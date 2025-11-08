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

use arrow::array::{Array, Int64Array};
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

/// Specification describing a deletion-vector file that should be produced.
///
/// A single deletion-vector file applies to one virtual data file (identified by
/// `data_file_id`) and contains the logical row IDs to mark as deleted.
#[derive(Debug)]
pub struct DeletionVectorWriteSpec {
    /// Catalog identifier for the virtual data file the deletion vector applies to.
    pub data_file_id: i64,
    /// Logical row IDs that should be marked as deleted.
    pub row_ids: Vec<i64>,
}

impl DeletionVectorWriteSpec {
    /// Create a new specification.
    #[must_use]
    pub fn new(data_file_id: i64, row_ids: Vec<i64>) -> Self {
        Self {
            data_file_id,
            row_ids,
        }
    }

    /// Returns `true` if there are no row IDs to write.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_ids.is_empty()
    }
}

/// Result of writing a deletion-vector file.
#[derive(Debug)]
pub struct DeletionVectorWriteResult {
    /// Metadata entry that should be registered with the catalog.
    pub delete_file: DeleteFile,
    /// Row IDs that were written to the deletion-vector file (sorted, deduplicated).
    pub row_ids: Vec<i64>,
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

        for mut spec in specs {
            if spec.is_empty() {
                continue;
            }

            if spec.row_ids.iter().any(|row_id| *row_id < 0) {
                return Err(CatalogError::InvalidOperation {
                    message: format!(
                        "Deletion vectors require non-negative row IDs, table: {}",
                        self.table.table_name
                    ),
                });
            }

            spec.row_ids.sort_unstable();
            spec.row_ids.dedup();

            let deletion_dir = self.table_snapshot_deletion_dir();
            tokio::fs::create_dir_all(&deletion_dir)
                .await
                .map_err(|source| CatalogError::IoError { source })?;

            let file_path = Self::deletion_file_path(&deletion_dir);
            let schema = deletion_vector_schema();
            let batch = build_deletion_batch(&schema, &spec.row_ids)?;

            let file_size_bytes =
                write_deletion_file(&file_path, Arc::clone(&schema), batch).await?;

            let delete_file = build_delete_file(
                self.table,
                &file_path,
                spec.data_file_id,
                spec.row_ids.len(),
                file_size_bytes,
            )?;

            results.push(DeletionVectorWriteResult {
                delete_file,
                row_ids: spec.row_ids,
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

fn build_deletion_batch(schema: &SchemaRef, row_ids: &[i64]) -> CatalogResult<RecordBatch> {
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
        message: format!("Failed to build deletion-vector batch: {err}"),
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
                message: format!("Failed to initialize deletion vector writer: {err}"),
            }
        })?;
        writer
            .write(&batch_for_write)
            .map_err(|err| CatalogError::InvalidOperation {
                message: format!("Failed to write deletion vector batch: {err}"),
            })?;
        writer
            .finish()
            .map_err(|err| CatalogError::InvalidOperation {
                message: format!("Failed to finish deletion vector file: {err}"),
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
    data_file_id: i64,
    delete_count: usize,
    file_size_bytes: u64,
) -> CatalogResult<DeleteFile> {
    let delete_count_i64 =
        i64::try_from(delete_count).map_err(|err| CatalogError::InvalidOperation {
            message: format!("Deletion count overflow ({delete_count}): {err}"),
        })?;
    let file_size_i64 =
        i64::try_from(file_size_bytes).map_err(|err| CatalogError::InvalidOperation {
            message: format!("Deletion vector file too large ({file_size_bytes} bytes): {err}"),
        })?;

    Ok(DeleteFile {
        delete_file_id: 0,
        table_id: table.table_id,
        data_file_id,
        path: file_path.to_string_lossy().to_string(),
        path_is_relative: false,
        format: DELETION_FILE_FORMAT.to_string(),
        delete_count: delete_count_i64,
        file_size_bytes: file_size_i64,
    })
}

fn deletion_vector_schema() -> SchemaRef {
    use arrow::datatypes::{DataType, Field, Schema};

    Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
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
            current_snapshot_id: Uuid::now_v7().to_string(),
            partition_column: None,
            vortex_config: crate::metadata::VortexConfig::default(),
        }
    }

    #[tokio::test]
    async fn writes_deletion_vector_and_returns_metadata() {
        let temp_dir = TempDir::new().expect("temp dir");
        let table_metadata = build_table_metadata(&temp_dir);
        let writer = DeletionVectorWriter::new(&table_metadata);

        let specs = vec![DeletionVectorWriteSpec::new(0, vec![3, 1, 3, 2])];
        let results = writer.write(specs).await.expect("write deletion vector");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.row_ids, vec![1, 2, 3]);
        assert_eq!(result.delete_file.table_id, table_metadata.table_id);
        assert_eq!(result.delete_file.data_file_id, 0);
        assert_eq!(
            result.delete_file.delete_count,
            i64::try_from(result.row_ids.len()).expect("convert delete count")
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
        assert_eq!(batch.num_rows(), result.row_ids.len());

        let row_ids_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row_id column");
        let row_ids: Vec<_> = (0..row_ids_col.len())
            .map(|idx| row_ids_col.value(idx))
            .collect();
        assert_eq!(row_ids, result.row_ids);
    }

    #[tokio::test]
    async fn skips_empty_specs() {
        let temp_dir = TempDir::new().expect("temp dir");
        let table_metadata = build_table_metadata(&temp_dir);
        let writer = DeletionVectorWriter::new(&table_metadata);

        let results = writer
            .write(vec![
                DeletionVectorWriteSpec::new(0, vec![]),
                DeletionVectorWriteSpec::new(1, vec![0]),
            ])
            .await
            .expect("write deletion vector");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].delete_file.data_file_id, 1);
    }

    #[tokio::test]
    async fn rejects_negative_row_ids() {
        let temp_dir = TempDir::new().expect("temp dir");
        let table_metadata = build_table_metadata(&temp_dir);
        let writer = DeletionVectorWriter::new(&table_metadata);

        let err = writer
            .write(vec![DeletionVectorWriteSpec::new(0, vec![1, -5])])
            .await
            .expect_err("negative row ids should fail");

        matches!(err, CatalogError::InvalidOperation { .. });
    }
}
