/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::cmp::min;
use std::slice;
use std::{any::Any, collections::HashSet, path::PathBuf, sync::Arc};

use crate::metadata::{MetadataColumn, MetadataColumns};
use arrow::{array::RecordBatch, datatypes::DataType};
use arrow_schema::Field;
use async_trait::async_trait;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use runtime_datafusion_index::Index;
use snafu::ResultExt;
use tantivy::schema::DocParsingError;
use tantivy::{TantivyDocument, TantivyError};
use tokio::sync::RwLock;

use crate::aggregation::write_to_json_string;
use crate::generation::text_search::query::FullTextSearchQuery;
use crate::generation::text_search::util::{array_to_terms, with_json_subset_column};
use crate::generation::text_search::{
    FailedToInsertDataIntoIndexSnafu, FullTextSearchFieldIndex, IndexCreationSnafu,
    InvalidIndexingSnafu, TextSearchIndexingSnafu,
};
use crate::generation::util::get_primary_keys;
use crate::index::SearchIndex;

/// The minimum number of bytes to support writing to in-memory [`tantivy::Index`].
pub static MINIMUM_MEMORY_BUDGET_FOR_MEMORY_INDEX: usize = 15_000_000;
pub static INDEX_UNIQUE_FIELD_NAME: &str = "__spice.unique_field";

#[derive(Clone)]
pub struct FullTextDatabaseIndex {
    pub search_fields: Vec<String>,
    pub primary_key: Vec<String>,
    pub base_table: Arc<dyn TableProvider>,
    pub index: Arc<RwLock<tantivy::Index>>,
    /// FTS indexes don't have additional metadata columns beyond primary keys and search fields
    pub metadata_columns: MetadataColumns,
}

impl std::fmt::Debug for FullTextDatabaseIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FullTextDatabaseIndex")
            .field("base_table", &self.base_table)
            .field("search_fields", &self.search_fields)
            .field("primary_key", &self.primary_key)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Index for FullTextDatabaseIndex {
    fn name(&self) -> &'static str {
        "full_text"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn required_columns(&self) -> Vec<String> {
        // Return both the primary key and search fields, deduplicated.
        let mut required_columns = HashSet::new();
        required_columns.extend(self.primary_key.iter().cloned());
        required_columns.extend(self.search_fields.iter().cloned());
        required_columns.into_iter().collect()
    }

    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if let Err(e) = self.update_index(batches.as_slice()).await {
            tracing::error!("Failed to update full text search index: {e}");
            return Err(DataFusionError::External(Box::new(e)));
        }
        Ok(batches)
    }
}

impl FullTextDatabaseIndex {
    pub async fn try_new(
        inner: Arc<dyn TableProvider>,
        search_fields: Vec<String>,
        primary_key_override: Option<Vec<String>>,
        directory: Option<PathBuf>,
    ) -> Result<Self, super::Error> {
        let pks = Self::validate_primary_key(&inner, primary_key_override).await?;
        let tantivy_schema =
            Self::create_tantivy_schema(&inner, search_fields.as_slice(), pks.as_slice())?;

        let index = if let Some(path) = &directory {
            match tantivy::Index::create_in_dir(path, tantivy_schema) {
                Ok(idx) => idx,
                Err(TantivyError::IndexAlreadyExists) => {
                    tantivy::index::Index::open_in_dir(path).context(TextSearchIndexingSnafu)?
                }
                Err(e) => return Err(e).context(TextSearchIndexingSnafu),
            }
        } else {
            tantivy::Index::create_in_ram(tantivy_schema)
        };

        let metadata_columns = Self::derive_metadata_columns(&inner, &index, &pks);
        Ok(Self {
            base_table: inner,
            search_fields,
            index: Arc::new(RwLock::new(index)),
            primary_key: pks,
            metadata_columns,
        })
    }

    async fn validate_primary_key(
        inner: &Arc<dyn TableProvider>,
        primary_key_override: Option<Vec<String>>,
    ) -> Result<Vec<String>, super::Error> {
        // Use 'primary_key_override', fallback to underlying in table.
        let pks = match (primary_key_override, get_primary_keys(inner).await) {
            (Some(pks), _) => pks,
            (None, Ok(pks)) => {
                if pks.is_empty() {
                    return Err(super::Error::NoPrimaryKey);
                }

                pks
            }
            (None, Err(e)) => {
                return Err(super::Error::FailedToRetrievePrimaryKey { source: e });
            }
        };

        // INDEX_UNIQUE_FIELD_NAME is a reserved field name.
        if pks.contains(&INDEX_UNIQUE_FIELD_NAME.to_string()) {
            return Err(super::Error::PrimaryKeyInvalidName {
                column: INDEX_UNIQUE_FIELD_NAME.to_string(),
            });
        }
        Ok(pks)
    }

    /// Get all [`Field`]s in Tantivy [`tantivy::Index`] that are in base table, and not primary keys.
    /// These are non-filterable [`MetadataColumn`]s, since we can retrieve the data
    /// from the [`tantivy::Index`] without access to the base [`TableProvider`].
    fn derive_metadata_columns(
        base_table: &Arc<dyn TableProvider>,
        index: &tantivy::Index,
        primary_key: &[String],
    ) -> MetadataColumns {
        let base_schema = base_table.schema();

        index
            .schema()
            .fields()
            .filter_map(|(_, fe)| {
                let name = fe.name();
                if primary_key.contains(&name.to_string()) {
                    return None;
                }

                let (_, f) = base_schema.column_with_name(name)?;
                Some(MetadataColumn::NonFilterable(Arc::new(f.clone())))
            })
            .collect::<Vec<_>>()
            .into()
    }

    pub fn full_text_search_field_index(
        &self,
        search_field: &str,
    ) -> Result<FullTextSearchFieldIndex, super::Error> {
        let index_read = self
            .index
            .try_read()
            .map_err(|_| super::Error::TemporarilyFailedToAccessSearchIndex {})?;

        let mut search_index = FullTextSearchFieldIndex::try_new(
            &index_read,
            search_field.to_string(),
            self.primary_key.clone(),
            Some(self.metadata_columns.all_names()),
        )?;
        search_index.add_type_hints(&self.underlying_table().schema());
        Ok(search_index)
    }

    /// Given a [`RecordBatch`] of new data, find all [`Term`]s we need to delete. These terms are
    /// an exact match on either a primary key (if one primary key column), or `INDEX_UNIQUE_FIELD_NAME`.
    fn existing_terms_to_delete(
        &self,
        index_schema: &tantivy::schema::Schema,
        rb: &[RecordBatch],
    ) -> Result<Vec<tantivy::Term>, super::Error> {
        let Some(pk) = self.primary_key.first() else {
            // Should not occur, but no primary key implies none must be deleted.
            return Ok(vec![]);
        };

        let (pk_field, pk) = if self.primary_key.len() == 1 {
            let Some((pk_field, _)) = index_schema.find_field(pk.as_str()) else {
                return Err(super::Error::FailedToRetrieveDataFromIndex {
                    source: TantivyError::FieldNotFound(pk.clone()),
                });
            };
            (pk_field, pk.clone())
        } else {
            // Primary key has multiple columns. Therefore tantivy::Index has derived field `INDEX_UNIQUE_FIELD_NAME`.
            let Some((pk_field, _)) = index_schema.find_field(INDEX_UNIQUE_FIELD_NAME) else {
                return Err(super::Error::InvalidIndexingError {
                    source: Box::from(TantivyError::FieldNotFound(pk.clone())),
                    context: format!(
                        "Full text search has multiple primary key columns, so the column '{INDEX_UNIQUE_FIELD_NAME}' should be present, but is not.",
                    ),
                });
            };
            (pk_field, INDEX_UNIQUE_FIELD_NAME.to_string())
        };

        Ok(rb
            .iter()
            .filter_map(|r| r.column_by_name(pk.as_str()))
            .map(|arr| array_to_terms(pk_field, arr))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| super::Error::FailedToRetrieveDataFromSource {
                source: DataFusionError::ArrowError(Box::new(e), None),
            })?
            .into_iter()
            .flatten()
            .collect())
    }

    /// Update the underlying [`tantivy::Index`] with new data from [`RecordBatch`]s. Additional
    /// columns present will be ignored.
    ///
    /// If there is a multi-column primary key (as specified by [`Self::primary_key`]), an additional column is used in the [`tantivy::Index`] for unique lookup (required since updates = deletion -> insertion).
    async fn update_index(&self, rb: &[RecordBatch]) -> Result<(), super::Error> {
        // Construct column for `INDEX_UNIQUE_FIELD_NAME` if needed.
        let rb = if self.primary_key.len() > 1 {
            rb.iter()
                .map(|r| with_json_subset_column(r, &self.primary_key, INDEX_UNIQUE_FIELD_NAME))
                .collect::<Result<Vec<RecordBatch>, _>>()
                .context(InvalidIndexingSnafu {
                    context: "An error occured creating the a unique column for the full text search index".to_string(),
                })?
        } else {
            rb.to_vec()
        };

        let index_writable = self.index.write().await;
        // Updates in tantivy are a deletion then insertion.
        let mut index_writer: tantivy::IndexWriter = index_writable
            .writer(MINIMUM_MEMORY_BUDGET_FOR_MEMORY_INDEX)
            .context(IndexCreationSnafu)?;

        // Deletion.
        for t in self.existing_terms_to_delete(&index_writable.schema(), &rb)? {
            index_writer.delete_term(t);
        }

        // Insertion.
        let doc_json = write_to_json_string(&rb).context(InvalidIndexingSnafu {
            context: "Failed to write data to intermediate JSON string for indexing".to_string(),
        })?;
        let docs = parse_json_array(&index_writable.schema(), doc_json.as_str())
            .context(FailedToInsertDataIntoIndexSnafu)?;
        for doc in docs {
            index_writer.add_document(doc).context(IndexCreationSnafu)?;
        }
        index_writer
            .commit()
            .context(FailedToInsertDataIntoIndexSnafu)?;
        Ok(())
    }

    #[must_use]
    pub fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    #[must_use]
    pub fn underlying_table(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.base_table)
    }

    /// Construct a new [`FullTextDatabaseIndex`] with an updated [`TableProvider`].
    ///
    /// No Checks are done to confirm compatibility between the current index and the provided [`TableProvider`].
    #[must_use]
    pub fn with_new_base(&self, base_table: Arc<dyn TableProvider>) -> Self {
        Self {
            search_fields: self.search_fields.clone(),
            primary_key: self.primary_key.clone(),
            index: Arc::clone(&self.index),
            base_table,
            metadata_columns: self.metadata_columns.clone(),
        }
    }

    fn create_tantivy_schema(
        base_table: &Arc<dyn TableProvider>,
        search_fields: &[String],
        primary_key: &[String],
    ) -> Result<tantivy::schema::Schema, super::Error> {
        let schema = base_table.schema();
        let mut schema_builder = tantivy::schema::Schema::builder();
        for p in primary_key {
            if search_fields.contains(p) {
                // Added below, tokenized.
                continue;
            }
            let Some((_, field)) = schema.column_with_name(p) else {
                return Err(super::Error::PrimaryKeyNotFound { column: p.clone() });
            };
            match field.data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    schema_builder.add_f64_field(
                        p.as_str(),
                        tantivy::schema::STORED | tantivy::schema::INDEXED,
                    );
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    schema_builder.add_u64_field(
                        p.as_str(),
                        tantivy::schema::STORED | tantivy::schema::INDEXED,
                    );
                }
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    schema_builder.add_i64_field(
                        p.as_str(),
                        tantivy::schema::STORED | tantivy::schema::INDEXED,
                    );
                }
                DataType::Boolean => {
                    schema_builder.add_bool_field(
                        p.as_str(),
                        tantivy::schema::STORED | tantivy::schema::INDEXED,
                    );
                }

                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    // [`tantivy::schema::STRING`] means we won't tokenize, important for primary key lookup via [`TermQuery`].
                    schema_builder.add_text_field(
                        p.as_str(),
                        tantivy::schema::STORED | tantivy::schema::STRING,
                    );
                }
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                    schema_builder.add_bytes_field(
                        p.as_str(),
                        tantivy::schema::STORED | tantivy::schema::INDEXED,
                    );
                }
                dt => {
                    return Err(super::Error::PrimaryKeyInvalidType {
                        data_type: dt.clone(),
                        column: p.clone(),
                    });
                }
            }
        }

        // If we need `INDEX_UNIQUE_FIELD_NAME`, add to schema.
        if primary_key.len() > 1 {
            schema_builder.add_text_field(INDEX_UNIQUE_FIELD_NAME, tantivy::schema::STRING);
        }

        for s in search_fields {
            schema_builder.add_text_field(s, tantivy::schema::TEXT | tantivy::schema::STORED);
        }
        Ok(schema_builder.build())
    }

    #[must_use]
    pub fn column_is_part_of_pk(&self, column: &str) -> bool {
        self.primary_key.contains(&column.to_string())
    }
}

#[async_trait]
impl SearchIndex for FullTextDatabaseIndex {
    /// Currently multi-column uses of [`FullTextDatabaseIndex`] do either:
    ///   1. `TextSearchTableFunc` chooses a column from its UDTF params and overrides `self.search_fields` at query time.
    ///   2. `as_candidate_generations` in `crates/runtime/src/search/full_text/mod.rs` creates [`FullTextSearchFieldIndex`].
    fn search_column(&self) -> String {
        // For FTS, return the first search field as the primary search column
        self.search_fields.first().cloned().unwrap_or_default()
    }

    fn primary_fields(&self) -> Vec<Field> {
        // Convert primary key names to Field objects by looking them up in the base table schema
        let schema = self.base_table.schema();
        self.primary_key
            .iter()
            .filter_map(|pk_name| {
                schema
                    .column_with_name(pk_name)
                    .map(|(_, field)| (*field).clone())
            })
            .collect()
    }

    fn metadata_columns(&self) -> &MetadataColumns {
        &self.metadata_columns
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        self.update_index(slice::from_ref(&record)).await.boxed()?;
        Ok(record)
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        let field_index = self
            .full_text_search_field_index(&self.search_column())
            .boxed()
            .map_err(DataFusionError::External)?;

        Ok(Arc::new(
            LogicalPlanBuilder::scan(
                self.name(),
                Arc::new(DefaultTableSource::new(Arc::new(FullTextSearchQuery {
                    index: field_index,
                    query: query.to_string(),
                    pre_limit: None,
                }))),
                None,
            )?
            .build()?,
        ))
    }
}

/// An implementation of [`TantivyDocument::parse_json`] that can parse a JSON array of JSON
/// objects that will deserialize to [`TantivyDocument`].
fn parse_json_array(
    schema: &tantivy::schema::Schema,
    doc_json: &str,
) -> Result<Vec<TantivyDocument>, TantivyError> {
    let json_obj: Vec<serde_json::Map<String, serde_json::Value>> = serde_json::from_str(doc_json)
        .map_err(|_| {
            Into::<TantivyError>::into(DocParsingError::InvalidJson(
                doc_json[0..min(20, doc_json.len())].to_string(),
            ))
        })?;

    Ok(json_obj
        .into_iter()
        .map(|obj| TantivyDocument::from_json_object(schema, obj))
        .collect::<Result<Vec<_>, _>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::datasource::{MemTable, TableProvider};
    use runtime_datafusion_index::Index;

    fn create_test_table() -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("content", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    "test content 1",
                    "test content 2",
                    "test content 3",
                ])),
            ],
        )
        .expect("Failed to create test batch");

        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("Failed to create test table"))
    }

    #[tokio::test]
    async fn test_compute_index_returns_batches_unchanged() {
        let table = create_test_table();
        let search_fields = vec!["content".to_string()];
        let primary_key = Some(vec!["id".to_string()]);

        let index = FullTextDatabaseIndex::try_new(table, search_fields, primary_key, None)
            .await
            .expect("Failed to create index");

        let input_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("content", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![4, 5])),
                Arc::new(StringArray::from(vec!["new content 1", "new content 2"])),
            ],
        )
        .expect("Failed to create input batch");

        let input_batches = vec![input_batch.clone()];
        let result_batches = index
            .compute_index(input_batches.clone())
            .await
            .expect("Failed to compute index");

        assert_eq!(input_batches.len(), result_batches.len());

        for (input, result) in input_batches.iter().zip(result_batches.iter()) {
            assert_eq!(input.schema(), result.schema());
            assert_eq!(input.num_rows(), result.num_rows());
            assert_eq!(input.num_columns(), result.num_columns());

            for col_idx in 0..input.num_columns() {
                let input_col = input.column(col_idx);
                let result_col = result.column(col_idx);
                assert_eq!(input_col, result_col);
            }
        }
    }
}
