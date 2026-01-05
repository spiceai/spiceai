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

//! Helpers for working with Cayenne deletion vectors.
//!
//! The deletion-vector pipeline is shared by multiple code paths (SQL `DELETE`,
//! runtime-triggered maintenance, and upcoming `on_conflict`/upsert flows).  This
//! module consolidates the logic for writing deletion-vector files and producing
//! the corresponding catalog metadata so callers can focus on selecting which rows
//! to remove.

use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, BinaryArray, Int64Array};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use chrono::Utc;
use uuid::Uuid;

use crate::catalog::{CatalogError, CatalogResult};
use crate::metadata::{DeleteFile, TableMetadata};

/// Directory under the table snapshot where deletion vectors are stored.
const DELETION_DIR_NAME: &str = "deletions";
/// File extension used for deletion-vector files.
const DELETION_FILE_EXTENSION: &str = "arrow";
/// File format recorded in the catalog for deletion vectors.
const DELETION_FILE_FORMAT: &str = "arrow_ipc";

/// Identifies rows for deletion using either position-based IDs or primary key-based keys.
///
/// # Deletion Strategies
///
/// - **Position-based (`row_ids`)**: Uses row position within the table. Requires consistent
///   ordering between delete and read operations (ensured by `CoalescePartitionsExec`).
///   Used when no primary key is defined.
///
/// - **Key-based (`row_keys`)**: Uses the byte representation of primary key columns
///   (via Arrow's `RowConverter`). Position-independent and survives data reorganization.
///   Used when a primary key is defined.
#[derive(Debug)]
pub enum DeletionIdentifier {
    /// Position-based row IDs (for tables without primary key)
    PositionBased(Vec<i64>),
    /// Primary key-based row keys (for tables with primary key)
    KeyBased(Vec<Box<[u8]>>),
}

impl DeletionIdentifier {
    /// Returns `true` if there are no rows to delete.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::PositionBased(ids) => ids.is_empty(),
            Self::KeyBased(keys) => keys.is_empty(),
        }
    }

    /// Returns the number of rows to delete.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::PositionBased(ids) => ids.len(),
            Self::KeyBased(keys) => keys.len(),
        }
    }
}

/// Specification describing a deletion-vector file that should be produced.
///
/// A single deletion-vector file applies to one virtual data file (identified by
/// `source_data_file_path` for position-based deletions) and contains the logical
/// row IDs to mark as deleted.
#[derive(Debug)]
pub struct DeletionVectorWriteSpec {
    /// Row identifiers (position-based or key-based)
    pub identifiers: DeletionIdentifier,
}

impl DeletionVectorWriteSpec {
    /// Create a new specification with position-based row IDs.
    #[must_use]
    pub fn new(row_ids: Vec<i64>) -> Self {
        Self {
            identifiers: DeletionIdentifier::PositionBased(row_ids),
        }
    }

    /// Create a new specification with key-based row keys.
    #[must_use]
    pub fn new_key_based(row_keys: Vec<Box<[u8]>>) -> Self {
        Self {
            identifiers: DeletionIdentifier::KeyBased(row_keys),
        }
    }

    /// Returns `true` if there are no row IDs to write.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.identifiers.is_empty()
    }
}

/// Result of writing a deletion-vector file.
#[derive(Debug)]
pub struct DeletionVectorWriteResult {
    /// Metadata entry that should be registered with the catalog.
    pub delete_file: DeleteFile,
    /// The deletion identifiers that were written (position-based or key-based).
    pub identifiers: DeletionIdentifier,
    /// Filesystem path where the deletion-vector file was written.
    pub path: PathBuf,
}

/// Writes deletion-vector files for a specific Cayenne table snapshot.
#[derive(Debug)]
pub struct DeletionVectorWriter<'a> {
    table: &'a TableMetadata,
}

impl<'a> DeletionVectorWriter<'a> {
    /// Create a new writer bound to the provided table metadata.
    #[must_use]
    pub fn new(table: &'a TableMetadata) -> Self {
        Self { table }
    }

    /// Write deletion-vector files for the supplied specifications.
    ///
    /// Callers are responsible for registering the returned [`DeleteFile`] metadata
    /// with the catalog. Empty specifications are skipped automatically.
    ///
    /// # Errors
    ///
    /// Returns an error if row IDs are negative, if Arrow record batches cannot be
    /// constructed, or if any filesystem/IO operations fail.
    pub async fn write(
        &self,
        specs: Vec<DeletionVectorWriteSpec>,
    ) -> CatalogResult<Vec<DeletionVectorWriteResult>> {
        let mut results = Vec::with_capacity(specs.len());

        for spec in specs {
            if spec.is_empty() {
                continue;
            }

            let deletion_dir = self.table_snapshot_deletion_dir();
            tokio::fs::create_dir_all(&deletion_dir)
                .await
                .map_err(|source| CatalogError::IoError { source })?;

            let file_path = Self::deletion_file_path(&deletion_dir);

            let (batch, schema, count, identifiers) = match spec.identifiers {
                DeletionIdentifier::PositionBased(mut row_ids) => {
                    // Validate no negative row IDs
                    if row_ids.iter().any(|row_id| *row_id < 0) {
                        return Err(CatalogError::NegativeRowId {
                            row_ids: format!(
                                "{:?}",
                                row_ids
                                    .iter()
                                    .filter(|row_id| **row_id < 0)
                                    .copied()
                                    .collect::<Vec<_>>()
                            ),
                        });
                    }
                    row_ids.sort_unstable();
                    row_ids.dedup();
                    let count = row_ids.len();
                    let schema = position_based_deletion_schema();
                    let batch = build_position_based_batch(&schema, &row_ids)?;
                    (
                        batch,
                        schema,
                        count,
                        DeletionIdentifier::PositionBased(row_ids),
                    )
                }
                DeletionIdentifier::KeyBased(mut row_keys) => {
                    // Sort and deduplicate keys
                    row_keys.sort();
                    row_keys.dedup();
                    let count = row_keys.len();
                    let schema = key_based_deletion_schema();
                    let batch = build_key_based_batch(&schema, &row_keys)?;
                    (batch, schema, count, DeletionIdentifier::KeyBased(row_keys))
                }
            };

            let file_size_bytes =
                write_deletion_file(&file_path, Arc::clone(&schema), batch).await?;

            // Determine deletion type from identifiers
            let deletion_type = match &identifiers {
                DeletionIdentifier::PositionBased(_) => {
                    crate::metadata::DeletionType::PositionBased
                }
                DeletionIdentifier::KeyBased(_) => crate::metadata::DeletionType::KeyBased,
            };

            let delete_file = build_delete_file(
                self.table,
                &file_path,
                count,
                file_size_bytes,
                deletion_type,
            )?;

            results.push(DeletionVectorWriteResult {
                delete_file,
                identifiers,
                path: file_path,
            });
        }

        Ok(results)
    }

    fn table_snapshot_deletion_dir(&self) -> PathBuf {
        let base = Path::new(&self.table.path);
        let snapshot_path = base.join(&self.table.current_snapshot_id);

        snapshot_path.join(DELETION_DIR_NAME)
    }

    fn deletion_file_path(deletion_dir: &Path) -> PathBuf {
        let file_name = format!("delete_{}.{}", Uuid::now_v7(), DELETION_FILE_EXTENSION);
        deletion_dir.join(file_name)
    }
}

/// Build a deletion batch for position-based row IDs.
fn build_position_based_batch(schema: &SchemaRef, row_ids: &[i64]) -> CatalogResult<RecordBatch> {
    let deleted_at = Utc::now().timestamp_micros();

    let row_id_array = Int64Array::from(row_ids.to_vec());
    let deleted_at_array = Int64Array::from(vec![deleted_at; row_ids.len()]);

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(row_id_array) as Arc<dyn Array>,
            Arc::new(deleted_at_array),
        ],
    )
    .map_err(|err| CatalogError::InvalidOperation {
        message: "Failed to build position-based deletion batch.".to_string(),
        source: Box::new(err),
    })
}

/// Build a deletion batch for key-based row keys (primary key bytes).
fn build_key_based_batch(schema: &SchemaRef, row_keys: &[Box<[u8]>]) -> CatalogResult<RecordBatch> {
    let deleted_at = Utc::now().timestamp_micros();

    // Convert Box<[u8]> to &[u8] for BinaryArray
    let key_refs: Vec<&[u8]> = row_keys.iter().map(AsRef::as_ref).collect();
    let row_key_array = BinaryArray::from(key_refs);
    let deleted_at_array = Int64Array::from(vec![deleted_at; row_keys.len()]);

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(row_key_array) as Arc<dyn Array>,
            Arc::new(deleted_at_array),
        ],
    )
    .map_err(|err| CatalogError::InvalidOperation {
        message: "Failed to build key-based deletion batch.".to_string(),
        source: Box::new(err),
    })
}

async fn write_deletion_file(
    file_path: &Path,
    schema: SchemaRef,
    batch: RecordBatch,
) -> CatalogResult<u64> {
    let output_path = file_path.to_path_buf();
    let schema_for_write = schema;
    let batch_for_write = batch;

    tokio::task::spawn_blocking(move || -> CatalogResult<u64> {
        use arrow::ipc::writer::FileWriter;

        let file = std::fs::File::create(&output_path)
            .map_err(|source| CatalogError::IoError { source })?;
        let mut writer = FileWriter::try_new(file, &schema_for_write).map_err(|err| {
            CatalogError::InvalidOperation {
                message: "Failed to initialize deletion vector writer.".to_string(),
                source: Box::new(err),
            }
        })?;
        writer
            .write(&batch_for_write)
            .map_err(|err| CatalogError::InvalidOperation {
                message: "Failed to write deletion vector batch.".to_string(),
                source: Box::new(err),
            })?;
        writer
            .finish()
            .map_err(|err| CatalogError::InvalidOperation {
                message: "Failed to finish deletion vector file.".to_string(),
                source: Box::new(err),
            })?;

        let metadata =
            std::fs::metadata(&output_path).map_err(|source| CatalogError::IoError { source })?;

        Ok(metadata.len())
    })
    .await
    .map_err(|source| CatalogError::TaskJoin { source })?
}

fn build_delete_file(
    table: &TableMetadata,
    file_path: &Path,
    delete_count: usize,
    file_size_bytes: u64,
    deletion_type: crate::metadata::DeletionType,
) -> CatalogResult<DeleteFile> {
    let delete_count_i64 =
        i64::try_from(delete_count).map_err(|err| CatalogError::InvalidOperation {
            message: format!("Deletion count overflow ({delete_count})."),
            source: Box::new(err),
        })?;
    let file_size_i64 =
        i64::try_from(file_size_bytes).map_err(|err| CatalogError::InvalidOperation {
            message: format!("Deletion vector file too large ({file_size_bytes} bytes)."),
            source: Box::new(err),
        })?;

    Ok(DeleteFile {
        delete_file_id: 0,
        table_id: table.table_id,
        source_data_file_path: None, // Set by caller for position-based deletions
        path: file_path.to_string_lossy().to_string(),
        path_is_relative: false,
        format: DELETION_FILE_FORMAT.to_string(),
        delete_count: delete_count_i64,
        file_size_bytes: file_size_i64,
        deletion_type,
        // Sequence number is set by the caller after getting the current sequence from catalog
        sequence_number: table.current_sequence_number,
    })
}

/// Schema for position-based deletion vectors (tables without primary key).
fn position_based_deletion_schema() -> SchemaRef {
    use arrow::datatypes::{DataType, Field, Schema};

    Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("deleted_at", DataType::Int64, false),
    ]))
}

/// Schema for key-based deletion vectors (tables with primary key).
fn key_based_deletion_schema() -> SchemaRef {
    use arrow::datatypes::{DataType, Field, Schema};

    Arc::new(Schema::new(vec![
        Field::new("row_key", DataType::Binary, false),
        Field::new("deleted_at", DataType::Int64, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::ipc::reader::FileReader;
    use std::sync::Arc;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn build_table_metadata(temp_dir: &TempDir) -> TableMetadata {
        TableMetadata {
            table_id: 42,
            table_uuid: Uuid::now_v7().to_string(),
            table_name: "test_table".to_string(),
            path: temp_dir.path().to_string_lossy().to_string(),
            path_is_relative: false,
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            current_snapshot_id: Uuid::now_v7().to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
            current_sequence_number: 0,
        }
    }

    #[tokio::test]
    async fn writes_deletion_vector_and_returns_metadata() {
        let temp_dir = TempDir::new().expect("temp dir");
        let table_metadata = build_table_metadata(&temp_dir);
        let writer = DeletionVectorWriter::new(&table_metadata);

        let specs = vec![DeletionVectorWriteSpec::new(vec![3, 1, 3, 2])];
        let results = writer.write(specs).await.expect("write deletion vector");

        assert_eq!(results.len(), 1);
        let result = &results[0];

        // Extract the row IDs from the result
        let row_ids = match &result.identifiers {
            DeletionIdentifier::PositionBased(ids) => ids.clone(),
            DeletionIdentifier::KeyBased(_) => panic!("Expected position-based identifiers"),
        };

        assert_eq!(row_ids, vec![1, 2, 3]);
        assert_eq!(result.delete_file.table_id, table_metadata.table_id);
        assert_eq!(
            result.delete_file.delete_count,
            i64::try_from(row_ids.len()).expect("convert delete count")
        );
        assert_eq!(result.delete_file.format, DELETION_FILE_FORMAT);

        let file = std::fs::File::open(&result.path).expect("open deletion file");
        let reader = FileReader::try_new(file, None).expect("create reader");
        let batches: Vec<_> = reader
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("read batches");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), row_ids.len());

        let row_ids_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row_id column");
        let read_row_ids: Vec<_> = (0..row_ids_col.len())
            .map(|idx| row_ids_col.value(idx))
            .collect();
        assert_eq!(read_row_ids, row_ids);
    }

    #[tokio::test]
    async fn skips_empty_specs() {
        let temp_dir = TempDir::new().expect("temp dir");
        let table_metadata = build_table_metadata(&temp_dir);
        let writer = DeletionVectorWriter::new(&table_metadata);

        let results = writer
            .write(vec![
                DeletionVectorWriteSpec::new(vec![]),
                DeletionVectorWriteSpec::new(vec![0]),
            ])
            .await
            .expect("write deletion vector");

        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn rejects_negative_row_ids() {
        let temp_dir = TempDir::new().expect("temp dir");
        let table_metadata = build_table_metadata(&temp_dir);
        let writer = DeletionVectorWriter::new(&table_metadata);

        let err = writer
            .write(vec![DeletionVectorWriteSpec::new(vec![1, -5])])
            .await
            .expect_err("negative row ids should fail");

        matches!(err, CatalogError::InvalidOperation { .. });
    }
}
