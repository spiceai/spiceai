/*
Copyright 2025-2026 The Spice.ai OSS Authors

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

//! Deletion vector I/O for Cayenne tables.
//!
//! This module handles reading and writing deletion vector files using Arrow IPC format.
//! Deletion vectors are used to mark rows as deleted without rewriting data files.
//!
//! # File Format
//!
//! Deletion vectors are stored as Arrow IPC files with one of two schemas:
//!
//! - **Position-based** (for tables without primary key):
//!   - `row_id: UInt64` - File-local row position (0-indexed)
//!   - `deleted_at: Int64` - Deletion timestamp (microseconds)
//!
//! - **Key-based** (for tables with primary key):
//!   - `row_key: Binary` - Primary key bytes (via Arrow's `RowConverter`)
//!   - `deleted_at: Int64` - Deletion timestamp (microseconds)

use std::collections::HashMap;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use arrow::array::{Array, BinaryArray, Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::FileReader;
use arrow_schema::SchemaRef;
use chrono::Utc;
use roaring::RoaringBitmap;
use uuid::Uuid;

use crate::catalog::{CatalogError, CatalogResult};
use crate::metadata::{DeleteFile, DeletionType, TableMetadata};

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
/// - **Position-based (`row_ids`)**: Uses row position within a specific data file.
///   File-local positions (0 to N-1) ensure correct deletion regardless of scan order.
///   Used when no primary key is defined.
///
/// - **Key-based (`row_keys`)**: Uses the byte representation of primary key columns
///   (via Arrow's `RowConverter`). Position-independent and survives data reorganization.
///   Used when a primary key is defined.
#[derive(Debug)]
pub enum DeletionIdentifier {
    /// Position-based row IDs for a specific data file (tables without primary key).
    /// The file path identifies which data file these row positions belong to.
    PositionBased {
        file_path: String,
        row_ids: Vec<u64>,
    },
    /// Primary key-based row keys (for tables with primary key).
    KeyBased(Vec<Box<[u8]>>),
}

impl DeletionIdentifier {
    /// Returns `true` if there are no rows to delete.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::PositionBased { row_ids, .. } => row_ids.is_empty(),
            Self::KeyBased(keys) => keys.is_empty(),
        }
    }
}

/// Specification describing a deletion-vector file that should be produced.
///
/// For position-based deletions, the file path is embedded in the `DeletionIdentifier`.
/// For key-based deletions, the deletion applies to the entire table.
#[derive(Debug)]
pub struct DeletionVectorWriteSpec {
    /// Row identifiers (position-based with file path, or key-based)
    pub identifiers: DeletionIdentifier,
}

impl DeletionVectorWriteSpec {
    /// Create a new specification with position-based row IDs for a specific data file.
    ///
    /// The row IDs should be file-local positions (0 to N-1 within the specified file).
    #[must_use]
    pub fn new_position_based(file_path: String, row_ids: Vec<u64>) -> Self {
        Self {
            identifiers: DeletionIdentifier::PositionBased { file_path, row_ids },
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

// ============================================================================
// Writer
// ============================================================================

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

            let (batch, schema, count, identifiers, source_data_file_path) = match spec.identifiers
            {
                DeletionIdentifier::PositionBased {
                    file_path: source_file,
                    mut row_ids,
                } => {
                    row_ids.sort_unstable();
                    row_ids.dedup();
                    let count = row_ids.len();
                    let schema = position_based_deletion_schema();
                    let batch = build_position_based_batch(&schema, &row_ids)?;
                    (
                        batch,
                        schema,
                        count,
                        DeletionIdentifier::PositionBased {
                            file_path: source_file.clone(),
                            row_ids,
                        },
                        Some(source_file),
                    )
                }
                DeletionIdentifier::KeyBased(mut row_keys) => {
                    // Sort and deduplicate keys
                    row_keys.sort();
                    row_keys.dedup();
                    let count = row_keys.len();
                    let schema = key_based_deletion_schema();
                    let batch = build_key_based_batch(&schema, &row_keys)?;
                    (
                        batch,
                        schema,
                        count,
                        DeletionIdentifier::KeyBased(row_keys),
                        None,
                    )
                }
            };

            let file_size_bytes =
                write_deletion_file(&file_path, Arc::clone(&schema), batch).await?;

            // Determine deletion type from identifiers
            let deletion_type = match &identifiers {
                DeletionIdentifier::PositionBased { .. } => DeletionType::PositionBased,
                DeletionIdentifier::KeyBased(_) => DeletionType::KeyBased,
            };

            let delete_file = build_delete_file(
                self.table,
                &file_path,
                count,
                file_size_bytes,
                deletion_type,
                source_data_file_path,
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

// ============================================================================
// Reader
// ============================================================================

/// Read deletion vectors from files, detecting whether each file is position-based or key-based
/// from its schema, and return separate collections for each type.
///
/// For position-based deletions, returns a map of source data file path to the `RoaringBitmap`
/// of file-local row positions. This enables correct deletion filtering regardless of file
/// scan order.
///
/// # Blocking I/O Warning
///
/// This function performs **blocking file system I/O** operations and must be called
/// from within `tokio::task::spawn_blocking`.
///
/// # Returns
///
/// A tuple of `(per_file_row_ids, key_based_row_keys_with_sequence)`.
/// - `per_file_row_ids`: Map of source data file path -> `RoaringBitmap` of deleted row positions
/// - `key_based_row_keys_with_sequence`: Map of PK bytes -> max delete sequence number
///
/// # Errors
///
/// Returns an error if any deletion vector file cannot be read or parsed.
#[expect(clippy::type_complexity)]
pub fn detect_deletion_type_and_read(
    delete_files: Vec<DeleteFile>,
) -> datafusion_common::Result<(HashMap<String, RoaringBitmap>, HashMap<Box<[u8]>, i64>)> {
    let mut per_file_row_ids: HashMap<String, RoaringBitmap> = HashMap::new();
    let mut deleted_row_keys: HashMap<Box<[u8]>, i64> = HashMap::new();
    let file_count = delete_files.len();

    tracing::debug!(
        "detect_deletion_type_and_read: processing {} delete files",
        file_count
    );

    // Track overflow occurrences to log once at the end
    let mut overflow_count: u64 = 0;
    let mut first_overflow_id: Option<u64> = None;

    for delete_file in delete_files {
        let path = std::path::Path::new(&delete_file.path);
        tracing::debug!("detect_deletion_type_and_read: reading file {:?}", path);

        let file = std::fs::File::open(path).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to open deletion vector file {}: {e}",
                path.display()
            ))
        })?;

        let reader = FileReader::try_new(file, None).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to read deletion vector file {}: {e}",
                path.display()
            ))
        })?;

        // Detect type from schema: first column name determines type
        // "row_id" (Int64) = position-based, "row_key" (Binary) = key-based
        let schema = reader.schema();
        let first_field = schema.field(0);
        let is_key_based = matches!(first_field.data_type(), DataType::Binary);

        // Get the sequence number for this delete file (for sequence-based ordering)
        let file_sequence = delete_file.sequence_number;

        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Failed to read batch from deletion vector: {e}"
                ))
            })?;

            if is_key_based {
                // Key-based: extract Binary row_key column
                let row_key_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Expected BinaryArray for row_key column".to_string(),
                        )
                    })?;

                for i in 0..row_key_array.len() {
                    if !row_key_array.is_null(i) {
                        let key = row_key_array.value(i).to_vec().into_boxed_slice();
                        // Track max delete sequence for each PK
                        deleted_row_keys
                            .entry(key)
                            .and_modify(|seq| *seq = (*seq).max(file_sequence))
                            .or_insert(file_sequence);
                    }
                }
            } else {
                // Position-based: extract UInt64 row_id column
                // Use source_data_file_path to group deletions by their originating data file
                let source_file = delete_file.source_data_file_path.clone().unwrap_or_else(|| {
                    tracing::warn!(
                        "Position-based deletion vector at {:?} has no source_data_file_path - using empty key",
                        path
                    );
                    String::new()
                });

                let row_id_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::Execution(
                            "Expected UInt64Array for row_id column".to_string(),
                        )
                    })?;

                // Bulk insert row IDs - schema guarantees no nulls (nullable: false)
                let bitmap = per_file_row_ids.entry(source_file).or_default();
                let values = row_id_array.values();
                for &row_id in values {
                    if let Ok(row_id_u32) = u32::try_from(row_id) {
                        bitmap.insert(row_id_u32);
                    } else {
                        if first_overflow_id.is_none() {
                            first_overflow_id = Some(row_id);
                        }
                        overflow_count += 1;
                    }
                }
            }
        }
    }

    if overflow_count > 0 {
        tracing::warn!(
            "Skipped {} row ID(s) that exceed u32::MAX (first: {}) - table should be compacted",
            overflow_count,
            first_overflow_id.unwrap_or(0)
        );
    }

    let total_position_based: u64 = per_file_row_ids.values().map(RoaringBitmap::len).sum();
    tracing::debug!(
        "Loaded {} position-based deletions across {} files + {} key-based deleted rows from {} deletion vector files",
        total_position_based,
        per_file_row_ids.len(),
        deleted_row_keys.len(),
        file_count
    );

    Ok((per_file_row_ids, deleted_row_keys))
}

// ============================================================================
// Helpers
// ============================================================================

/// Build a deletion batch for position-based row IDs.
fn build_position_based_batch(schema: &SchemaRef, row_ids: &[u64]) -> CatalogResult<RecordBatch> {
    let deleted_at = Utc::now().timestamp_micros();

    let row_id_array = UInt64Array::from(row_ids.to_vec());
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
    deletion_type: DeletionType,
    source_data_file_path: Option<String>,
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
        source_data_file_path,
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
static POSITION_BASED_DELETION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::UInt64, false),
        Field::new("deleted_at", DataType::Int64, false),
    ]))
});

/// Schema for key-based deletion vectors (tables with primary key).
static KEY_BASED_DELETION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("row_key", DataType::Binary, false),
        Field::new("deleted_at", DataType::Int64, false),
    ]))
});

/// Returns the schema for position-based deletion vectors.
fn position_based_deletion_schema() -> SchemaRef {
    Arc::clone(&POSITION_BASED_DELETION_SCHEMA)
}

/// Returns the schema for key-based deletion vectors.
fn key_based_deletion_schema() -> SchemaRef {
    Arc::clone(&KEY_BASED_DELETION_SCHEMA)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::ipc::reader::FileReader;
    use tempfile::TempDir;

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

        let specs = vec![DeletionVectorWriteSpec::new_position_based(
            "test_file.vortex".to_string(),
            vec![3, 1, 3, 2],
        )];
        let results = writer.write(specs).await.expect("write deletion vector");

        assert_eq!(results.len(), 1);
        let result = &results[0];

        // Extract the row IDs from the result
        let row_ids = match &result.identifiers {
            DeletionIdentifier::PositionBased { row_ids, .. } => row_ids.clone(),
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
            .downcast_ref::<UInt64Array>()
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
                DeletionVectorWriteSpec::new_position_based("empty.vortex".to_string(), vec![]),
                DeletionVectorWriteSpec::new_position_based("test.vortex".to_string(), vec![0]),
            ])
            .await
            .expect("write deletion vector");

        assert_eq!(results.len(), 1);
    }
}
