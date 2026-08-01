/*
Copyright 2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

pub mod list_provider;
pub mod query_provider;
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use s3_vectors::S3Vectors;
use snafu::prelude::*;

use crate::s3_vectors::{S3VectorIdentifier, S3VectorsTable, list_index_names};

/// The separator used between the base index name and spill sequence number.
const SPILL_SEPARATOR: &str = "-";

/// Maximum sequence number for spill indexes (00-99).
pub const MAX_SPILL_SEQUENCE: u8 = 99;

/// Represents a spill index with its base name and sequence number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillIndex {
    /// The base index name (without spill suffix).
    pub base_name: String,
    /// The spill sequence number (0-99).
    pub sequence: u8,
}

#[derive(Debug, PartialEq, Snafu)]
pub enum Error {
    #[snafu(display(
        "Invalid spill index name format: '{name}'. Expected format: base_name{SPILL_SEPARATOR}sequence_number"
    ))]
    InvalidSpillIndexFormat { name: String },

    #[snafu(display(
        "Spill sequence number {sequence} exceeds maximum allowed value of {MAX_SPILL_SEQUENCE}"
    ))]
    SequenceNumberTooLarge { sequence: u8 },

    #[snafu(display(
        "Spill sequence number could not be parsed from '{sequence_str}' in index name '{name}'"
    ))]
    InvalidSequenceNumber { sequence_str: String, name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl SpillIndex {
    fn format_name(&self) -> String {
        let Self {
            base_name,
            sequence,
        } = self;
        format!("{base_name}{SPILL_SEPARATOR}{sequence:02}")
    }

    /// Parses a spill index name into its components.
    pub fn parse(index_name: &str) -> Result<Option<Self>> {
        let parts: Vec<&str> = index_name.split(SPILL_SEPARATOR).collect();

        if parts.len() < 2 {
            return Ok(None);
        }

        let Some(sequence_str) = parts.last() else {
            return Ok(None);
        };

        let base_name_parts = &parts[..parts.len() - 1];
        let base_name = base_name_parts.join(SPILL_SEPARATOR);

        if sequence_str.len() != 2 {
            return Ok(None);
        }

        let sequence = sequence_str
            .parse::<u8>()
            .map_err(|_| Error::InvalidSequenceNumber {
                sequence_str: (*sequence_str).to_string(),
                name: index_name.to_string(),
            })?;

        if sequence > MAX_SPILL_SEQUENCE {
            return Err(Error::SequenceNumberTooLarge { sequence });
        }

        Ok(Some(Self {
            base_name,
            sequence,
        }))
    }

    /// Gets all spill index names that belong to the same virtual index.
    #[must_use]
    fn get_spill_indexes_for_virtual_index(base_name: &str, all_indexes: &[String]) -> Vec<String> {
        let mut spill_indexes = Vec::new();

        for index_name in all_indexes {
            if let Ok(Some(spill)) = Self::parse(index_name)
                && spill.base_name == base_name
            {
                spill_indexes.push(spill);
            }
        }

        // Sort by sequence number for consistent ordering
        spill_indexes.sort_by_key(|a| a.sequence);

        spill_indexes.into_iter().map(|i| i.format_name()).collect()
    }

    /// Gets all index names (main + spills) that belong to a virtual index.
    #[must_use]
    pub fn get_all_indexes_for_virtual_index(
        virtual_index_name: &str,
        all_indexes: &[String],
    ) -> Vec<String> {
        let base_name = if let Ok(Some(spill)) = Self::parse(virtual_index_name) {
            spill.base_name
        } else {
            virtual_index_name.to_string()
        };

        let mut result = Vec::new();

        // Add main index if it exists
        if all_indexes.contains(&base_name) {
            result.push(base_name.clone());
        }

        // Add all spill indexes
        let spill_indexes = Self::get_spill_indexes_for_virtual_index(&base_name, all_indexes);
        result.extend(spill_indexes);

        result
    }
}

/// Find the last spill number for a given virtual index.
pub async fn get_last_spill_index_for_virtual_index(
    client: &Arc<dyn S3Vectors + Send + Sync>,
    bucket_name: &str,
    virtual_index_name: &str,
) -> Result<u8, super::Error> {
    let all_indexes = list_index_names(client, bucket_name, virtual_index_name).await?;
    Ok(all_indexes
        .iter()
        .filter_map(|i| SpillIndex::parse(i).ok().flatten())
        .max_by_key(|s| s.sequence)
        .map(|s| s.sequence)
        .unwrap_or_default())
}

/// Returns the current index identifier, accounting for spilling.
#[must_use]
pub fn current_index(idx: &S3VectorIdentifier, spill_index: &Arc<AtomicU8>) -> S3VectorIdentifier {
    let spill_num = spill_index.load(Ordering::SeqCst);
    if spill_num == 0 {
        idx.clone()
    } else {
        match idx {
            S3VectorIdentifier::Index {
                bucket_name,
                index_name,
            } => S3VectorIdentifier::Index {
                bucket_name: bucket_name.clone(),
                index_name: format!("{index_name}.{spill_num:02}"),
            },
            S3VectorIdentifier::IndexArn(_) => idx.clone(),
        }
    }
}

/// Returns the next index identifier, incrementing the spill index
///
/// # Errors
/// Returns an error if there is no next index
pub fn next_index(
    idx: &S3VectorIdentifier,
    spill_index: &Arc<AtomicU8>,
) -> Result<S3VectorIdentifier, super::Error> {
    if spill_index
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
            if x >= MAX_SPILL_SEQUENCE {
                None
            } else {
                Some(x + 1)
            }
        })
        .is_err()
    {
        return Err(super::Error::MaxSpillAttemptsReached);
    }

    Ok(current_index(idx, spill_index))
}

pub(super) async fn all_spill_tables(
    table: &S3VectorsTable,
    spill_index: &Arc<AtomicU8>,
) -> Result<Vec<S3VectorsTable>, super::Error> {
    let current_index = current_index(&table.idx, spill_index);
    let (_, Some(bucket_name), Some(index_name)) = current_index.index_identifier_variables()
    else {
        // This should never happen
        return Ok(vec![]);
    };

    let spill_index_name = match SpillIndex::parse(&index_name) {
        Ok(Some(name)) => name.base_name,
        _ => index_name.clone(),
    };
    let all_index_names = list_index_names(&table.client, &bucket_name, &spill_index_name).await?;

    Ok(
        SpillIndex::get_all_indexes_for_virtual_index(&index_name, &all_index_names)
            .iter()
            .map(|spill_index_name| {
                table.clone().with_new_id(S3VectorIdentifier::Index {
                    bucket_name: bucket_name.clone(),
                    index_name: spill_index_name.clone(),
                })
            })
            .collect::<Vec<S3VectorsTable>>(),
    )
}

/// Every index (base + every spill index currently discoverable via `ListIndexes`) backing the
/// virtual index identified by `table.idx`.
///
/// Unlike [`all_spill_tables`], this doesn't take a writer's in-memory `spill_index` position —
/// which spill index a given key's vector actually landed in depends on write-time AWS quota
/// state and isn't recoverable from the key alone, so a delete broadcasts to every index this
/// finds rather than routing to one. `DeleteVectors` against a key absent from a given index is a
/// no-op, so broadcasting is safe; it costs one delete call per existing physical index.
pub async fn all_existing_spill_tables(
    table: &S3VectorsTable,
) -> Result<Vec<S3VectorsTable>, super::Error> {
    let (_, Some(bucket_name), Some(index_name)) = table.idx.index_identifier_variables() else {
        // ARN-identified indexes never spill (`current_index` always resolves back to the same
        // ARN) — the base table is the only possible target.
        return Ok(vec![table.clone()]);
    };

    let all_index_names = list_index_names(&table.client, &bucket_name, &index_name).await?;
    let mut names = SpillIndex::get_all_indexes_for_virtual_index(&index_name, &all_index_names);
    if !names.contains(&index_name) {
        // Always include the base index even if `ListIndexes` didn't return it (e.g. eventual
        // consistency) — matches the pre-broadcast behavior of always attempting the base delete.
        names.insert(0, index_name.clone());
    }

    Ok(names
        .into_iter()
        .map(|name| {
            table.clone().with_new_id(S3VectorIdentifier::Index {
                bucket_name: bucket_name.clone(),
                index_name: name,
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_spill_index() {
        let result = SpillIndex::parse("myindex-00")
            .expect("success")
            .expect("success");
        assert_eq!(result.base_name, "myindex");
        assert_eq!(result.sequence, 0);

        let result = SpillIndex::parse("myindex-42")
            .expect("success")
            .expect("success");
        assert_eq!(result.base_name, "myindex");
        assert_eq!(result.sequence, 42);

        let result = SpillIndex::parse("test_index-99")
            .expect("success")
            .expect("success");
        assert_eq!(result.base_name, "test_index");
        assert_eq!(result.sequence, 99);
    }

    #[test]
    fn test_parse_partitioned_spill_index() {
        let result = SpillIndex::parse("myindex.hash1.hash2.hash3-01")
            .expect("success")
            .expect("success");
        assert_eq!(result.base_name, "myindex.hash1.hash2.hash3");
        assert_eq!(result.sequence, 1);

        let result = SpillIndex::parse("dataset.col.expr.val-05")
            .expect("success")
            .expect("success");
        assert_eq!(result.base_name, "dataset.col.expr.val");
        assert_eq!(result.sequence, 5);
    }

    #[test]
    fn test_parse_invalid_sequence() {
        assert!(SpillIndex::parse("myindex").expect("success").is_none());
        assert!(SpillIndex::parse("myindex.1").expect("success").is_none());
        assert!(SpillIndex::parse("myindex.123").expect("success").is_none());
        assert!(SpillIndex::parse("myindex.abc").expect("success").is_none());
        let result = SpillIndex::parse("myindex-aa");
        result.expect_err("Should error on invalid spill index format");
    }

    #[test]
    fn test_get_spill_indexes_for_virtual_index() {
        let all_indexes = vec![
            "myindex".to_string(),
            "myindex-01".to_string(),
            "myindex-02".to_string(),
            "other-01".to_string(),
            "myindex-10".to_string(),
        ];

        let result = SpillIndex::get_spill_indexes_for_virtual_index("myindex", &all_indexes);
        assert_eq!(
            result,
            vec![
                "myindex-01".to_string(),
                "myindex-02".to_string(),
                "myindex-10".to_string(),
            ]
        );
    }

    #[test]
    fn test_get_all_indexes_for_virtual_index() {
        let all_indexes = vec![
            "myindex".to_string(),
            "myindex-01".to_string(),
            "myindex-02".to_string(),
            "other-01".to_string(),
        ];

        // From main index
        let result = SpillIndex::get_all_indexes_for_virtual_index("myindex", &all_indexes);
        assert_eq!(
            result,
            vec![
                "myindex".to_string(),
                "myindex-01".to_string(),
                "myindex-02".to_string(),
            ]
        );

        // From spill index
        let result = SpillIndex::get_all_indexes_for_virtual_index("myindex-01", &all_indexes);
        assert_eq!(
            result,
            vec![
                "myindex".to_string(),
                "myindex-01".to_string(),
                "myindex-02".to_string(),
            ]
        );
    }

    use crate::s3_vectors::{
        MetadataColumns, S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Constraint, Constraints};
    use s3_vectors::{
        CreateIndexInput, CreateVectorBucketInput, DataType as S3DataType, DistanceMetric,
        S3Vectors, mock::MockClient,
    };

    fn test_table(
        client: Arc<dyn S3Vectors + Send + Sync>,
        idx: S3VectorIdentifier,
    ) -> S3VectorsTable {
        S3VectorsTable {
            idx: Arc::new(idx),
            client,
            schema: Arc::new(Schema::new(vec![
                Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
                Field::new_fixed_size_list(
                    S3_VECTOR_EMBEDDING_NAME,
                    Field::new("item", DataType::Float32, false),
                    3,
                    false,
                ),
            ])),
            constraints: Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]),
            dimension: 3,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        }
    }

    async fn create_index(client: &Arc<dyn S3Vectors + Send + Sync>, index_name: &str) {
        client
            .create_vector_bucket(
                &CreateVectorBucketInput::builder()
                    .vector_bucket_name("test-bucket")
                    .build()
                    .expect("valid input"),
            )
            .await
            .ok();
        client
            .create_index(
                &CreateIndexInput::builder()
                    .index_name(index_name)
                    .vector_bucket_name("test-bucket")
                    .data_type(S3DataType::Float32)
                    .dimension(3)
                    .distance_metric(DistanceMetric::Cosine)
                    .build()
                    .expect("valid input"),
            )
            .await
            .expect("create_index should succeed");
    }

    #[tokio::test]
    async fn all_existing_spill_tables_discovers_base_and_every_spill() {
        let mock_client = Arc::new(MockClient::new());
        let client = Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>;
        for name in ["virtual-index", "virtual-index-01", "virtual-index-02"] {
            create_index(&client, name).await;
        }
        // An unrelated index sharing a prefix must not be swept in.
        create_index(&client, "virtual-index-other-05").await;

        let table = test_table(
            client,
            S3VectorIdentifier::Index {
                bucket_name: "test-bucket".to_string(),
                index_name: "virtual-index".to_string(),
            },
        );

        let tables = all_existing_spill_tables(&table)
            .await
            .expect("should discover indexes");
        let names: Vec<String> = tables
            .iter()
            .map(|t| {
                t.idx
                    .index_identifier_variables()
                    .2
                    .expect("index-backed identifier")
            })
            .collect();
        assert_eq!(
            names,
            vec![
                "virtual-index".to_string(),
                "virtual-index-01".to_string(),
                "virtual-index-02".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn all_existing_spill_tables_no_spillover_returns_just_the_base() {
        let mock_client = Arc::new(MockClient::new());
        let client = Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>;
        create_index(&client, "virtual-index").await;

        let table = test_table(
            client,
            S3VectorIdentifier::Index {
                bucket_name: "test-bucket".to_string(),
                index_name: "virtual-index".to_string(),
            },
        );

        let tables = all_existing_spill_tables(&table)
            .await
            .expect("should discover indexes");
        let names: Vec<String> = tables
            .iter()
            .map(|t| {
                t.idx
                    .index_identifier_variables()
                    .2
                    .expect("index-backed identifier")
            })
            .collect();
        assert_eq!(names, vec!["virtual-index".to_string()]);
    }

    #[tokio::test]
    async fn all_existing_spill_tables_arn_identifier_returns_just_the_base() {
        let mock_client = Arc::new(MockClient::new());
        let client = Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>;

        let table = test_table(
            client,
            S3VectorIdentifier::IndexArn(
                "arn:aws:s3vectors:us-east-1:123:index/virtual".to_string(),
            ),
        );

        let tables = all_existing_spill_tables(&table)
            .await
            .expect("ARN identifiers never spill, so this must not error");
        assert_eq!(tables.len(), 1);
    }
}
