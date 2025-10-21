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

//! Arrow/Feather backend for Pepper catalog using DataFusion.
//!
//! This backend stores catalog metadata in Arrow format (IPC/Feather files)
//! using DataFusion for querying and manipulation.

use super::backend::CatalogBackend;
use super::catalog::{CatalogError, CatalogResult};
use super::metadata::{CreateTableOptions, DataFile, DeleteFile, TableMetadata, TableStats};
use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_ipc::writer::FileWriter;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Arrow/Feather-based backend for Pepper metadata catalog.
///
/// Uses DataFusion to query and manipulate Arrow IPC files for metadata storage.
pub(crate) struct ArrowBackend {
    base_path: PathBuf,
}

impl ArrowBackend {
    /// Create a new Arrow backend with the given base directory path.
    pub fn new(base_path: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    /// Get the path to the metadata file.
    fn metadata_file(&self) -> PathBuf {
        self.base_path.join("pepper_metadata.arrow")
    }

    /// Get the path to the tables file.
    fn tables_file(&self) -> PathBuf {
        self.base_path.join("pepper_table.arrow")
    }

    /// Get the path to the data files file.
    fn data_files_file(&self) -> PathBuf {
        self.base_path.join("pepper_data_file.arrow")
    }

    /// Get the path to the delete files file.
    fn delete_files_file(&self) -> PathBuf {
        self.base_path.join("pepper_delete_file.arrow")
    }

    /// Schema for the metadata table.
    fn metadata_schema() -> Schema {
        Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ])
    }

    /// Schema for the tables table.
    fn tables_schema() -> Schema {
        Schema::new(vec![
            Field::new("table_id", DataType::Int64, false),
            Field::new("table_uuid", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("path_is_relative", DataType::Boolean, false),
            Field::new("schema_json", DataType::Utf8, false),
            Field::new("primary_key_json", DataType::Utf8, true),
        ])
    }

    /// Schema for the data files table.
    fn data_files_schema() -> Schema {
        Schema::new(vec![
            Field::new("data_file_id", DataType::Int64, false),
            Field::new("table_id", DataType::Int64, false),
            Field::new("file_order", DataType::Int64, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("path_is_relative", DataType::Boolean, false),
            Field::new("file_format", DataType::Utf8, false),
            Field::new("record_count", DataType::Int64, false),
            Field::new("file_size_bytes", DataType::Int64, false),
            Field::new("row_id_start", DataType::Int64, false),
        ])
    }

    /// Schema for the delete files table.
    fn delete_files_schema() -> Schema {
        Schema::new(vec![
            Field::new("delete_file_id", DataType::Int64, false),
            Field::new("table_id", DataType::Int64, false),
            Field::new("data_file_id", DataType::Int64, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("path_is_relative", DataType::Boolean, false),
            Field::new("format", DataType::Utf8, false),
            Field::new("delete_count", DataType::Int64, false),
            Field::new("file_size_bytes", DataType::Int64, false),
        ])
    }

    /// Write an empty Arrow file with the given schema if it doesn't exist.
    async fn init_file(path: &Path, schema: Schema) -> CatalogResult<()> {
        if path.exists() {
            return Ok(());
        }

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Create empty record batch
        let empty_batch = RecordBatch::new_empty(Arc::new(schema));

        // Write to file using blocking task
        let path_owned = path.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let file =
                std::fs::File::create(&path_owned).map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to create file {}: {e}", path_owned.display()),
                })?;

            let mut writer = FileWriter::try_new(file, &empty_batch.schema()).map_err(|e| {
                CatalogError::InvalidOperation {
                    message: format!("Failed to create Arrow writer: {e}"),
                }
            })?;

            writer
                .write(&empty_batch)
                .map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to write Arrow batch: {e}"),
                })?;

            writer
                .finish()
                .map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to finish Arrow writer: {e}"),
                })?;

            Ok::<(), CatalogError>(())
        })
        .await??;

        Ok(())
    }

    /// Initialize metadata file with default values.
    async fn init_metadata_file(&self) -> CatalogResult<()> {
        let path = self.metadata_file();
        if path.exists() {
            return Ok(());
        }

        let schema = Arc::new(Self::metadata_schema());

        // Create initial metadata with next IDs
        let keys = StringArray::from(vec!["next_catalog_id", "next_file_id"]);
        let values = Int64Array::from(vec![1, 1]);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
        )
        .map_err(|e| CatalogError::InvalidOperation {
            message: format!("Failed to create metadata batch: {e}"),
        })?;

        // Write to file
        let path_owned = path.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = path_owned.parent() {
                std::fs::create_dir_all(parent).map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to create directory: {e}"),
                })?;
            }

            let file =
                std::fs::File::create(&path_owned).map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to create file {}: {e}", path_owned.display()),
                })?;

            let mut writer = FileWriter::try_new(file, &batch.schema()).map_err(|e| {
                CatalogError::InvalidOperation {
                    message: format!("Failed to create Arrow writer: {e}"),
                }
            })?;

            writer
                .write(&batch)
                .map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to write Arrow batch: {e}"),
                })?;

            writer
                .finish()
                .map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to finish Arrow writer: {e}"),
                })?;

            Ok::<(), CatalogError>(())
        })
        .await??;

        Ok(())
    }

    /// Read the next catalog ID and increment it atomically.
    async fn get_and_increment_catalog_id(&self) -> CatalogResult<i64> {
        let metadata_path = self.metadata_file();

        // Read current metadata directly from Arrow IPC file
        let batches = tokio::task::spawn_blocking({
            let metadata_path = metadata_path.clone();
            move || {
                let file = std::fs::File::open(&metadata_path).map_err(|e| {
                    CatalogError::InvalidOperation {
                        message: format!("Failed to open metadata file: {e}"),
                    }
                })?;

                let reader = arrow_ipc::reader::FileReader::try_new(file, None).map_err(|e| {
                    CatalogError::InvalidOperation {
                        message: format!("Failed to create Arrow reader: {e}"),
                    }
                })?;

                let mut batches = Vec::new();
                for batch_result in reader {
                    batches.push(batch_result.map_err(|e| CatalogError::InvalidOperation {
                        message: format!("Failed to read batch: {e}"),
                    })?);
                }

                Ok::<Vec<RecordBatch>, CatalogError>(batches)
            }
        })
        .await??;

        // Find next_catalog_id value
        let mut next_id = 1i64;
        for batch in &batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CatalogError::InvalidOperation {
                    message: "Invalid metadata key column type".to_string(),
                })?;
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| CatalogError::InvalidOperation {
                    message: "Invalid metadata value column type".to_string(),
                })?;

            for i in 0..batch.num_rows() {
                if keys.value(i) == "next_catalog_id" {
                    next_id = values.value(i);
                    break;
                }
            }
        }

        // Update metadata with new next_catalog_id
        let schema = Arc::new(Self::metadata_schema());
        let new_keys = StringArray::from(vec!["next_catalog_id", "next_file_id"]);
        let new_values = Int64Array::from(vec![next_id + 1, 1]); // Simplified for now

        let new_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(new_keys) as ArrayRef,
                Arc::new(new_values) as ArrayRef,
            ],
        )
        .map_err(|e| CatalogError::InvalidOperation {
            message: format!("Failed to create new metadata batch: {e}"),
        })?;

        // Write updated metadata
        let path_owned = metadata_path.clone();
        tokio::task::spawn_blocking(move || {
            let file =
                std::fs::File::create(&path_owned).map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to create metadata file: {e}"),
                })?;

            let mut writer = FileWriter::try_new(file, &new_batch.schema()).map_err(|e| {
                CatalogError::InvalidOperation {
                    message: format!("Failed to create Arrow writer: {e}"),
                }
            })?;

            writer
                .write(&new_batch)
                .map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to write metadata batch: {e}"),
                })?;

            writer
                .finish()
                .map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to finish Arrow writer: {e}"),
                })?;

            Ok::<(), CatalogError>(())
        })
        .await??;

        Ok(next_id)
    }

    /// Append a record batch to an Arrow file.
    async fn append_to_file(path: &Path, batch: RecordBatch) -> CatalogResult<()> {
        let path_owned = path.to_path_buf();

        tokio::task::spawn_blocking(move || {
            // Read existing batches
            let mut existing_batches = Vec::new();
            if path_owned.exists() {
                let file = std::fs::File::open(&path_owned).map_err(|e| {
                    CatalogError::InvalidOperation {
                        message: format!("Failed to open file: {e}"),
                    }
                })?;

                let reader = arrow_ipc::reader::FileReader::try_new(file, None).map_err(|e| {
                    CatalogError::InvalidOperation {
                        message: format!("Failed to create Arrow reader: {e}"),
                    }
                })?;

                for batch_result in reader {
                    existing_batches.push(batch_result.map_err(|e| {
                        CatalogError::InvalidOperation {
                            message: format!("Failed to read batch: {e}"),
                        }
                    })?);
                }
            }

            // Add new batch
            existing_batches.push(batch.clone());

            // Write all batches
            let file =
                std::fs::File::create(&path_owned).map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to create file: {e}"),
                })?;

            let mut writer = FileWriter::try_new(file, &batch.schema()).map_err(|e| {
                CatalogError::InvalidOperation {
                    message: format!("Failed to create Arrow writer: {e}"),
                }
            })?;

            for b in &existing_batches {
                writer
                    .write(b)
                    .map_err(|e| CatalogError::InvalidOperation {
                        message: format!("Failed to write batch: {e}"),
                    })?;
            }

            writer
                .finish()
                .map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to finish Arrow writer: {e}"),
                })?;

            Ok::<(), CatalogError>(())
        })
        .await??;

        Ok(())
    }
}

#[async_trait]
impl CatalogBackend for ArrowBackend {
    async fn init(&self) -> CatalogResult<()> {
        // Create base directory
        if !self.base_path.exists() {
            tokio::fs::create_dir_all(&self.base_path).await?;
        }

        // Initialize all metadata files
        self.init_metadata_file().await?;
        Self::init_file(&self.tables_file(), Self::tables_schema()).await?;
        Self::init_file(&self.data_files_file(), Self::data_files_schema()).await?;
        Self::init_file(&self.delete_files_file(), Self::delete_files_schema()).await?;

        Ok(())
    }

    async fn create_table(&self, options: CreateTableOptions) -> CatalogResult<i64> {
        // Get next table ID
        let table_id = self.get_and_increment_catalog_id().await?;

        // Generate UUID
        let table_uuid = uuid::Uuid::now_v7().to_string();

        // Serialize schema using Arrow IPC format
        let schema_json = {
            use arrow_ipc::writer::IpcWriteOptions;
            let write_options = IpcWriteOptions::default();
            let arrow_flight::IpcMessage(schema_bytes) =
                arrow_flight::SchemaAsIpc::new(options.schema.as_ref(), &write_options)
                    .try_into()
                    .map_err(
                        |e: arrow_schema::ArrowError| CatalogError::InvalidOperation {
                            message: format!("Failed to serialize schema: {e}"),
                        },
                    )?;

            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                schema_bytes.as_ref(),
            )
        };

        let primary_key_json = if options.primary_key.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&options.primary_key).map_err(|e| {
                CatalogError::InvalidOperation {
                    message: format!("Failed to serialize primary key: {e}"),
                }
            })?)
        };

        // Create record batch for the new table
        let schema = Arc::new(Self::tables_schema());
        let table_ids = Int64Array::from(vec![table_id]);
        let table_uuids = StringArray::from(vec![table_uuid.as_str()]);
        let table_names = StringArray::from(vec![options.table_name.as_str()]);
        let paths = StringArray::from(vec![options.base_path.as_str()]);
        let path_is_relative = BooleanArray::from(vec![false]);
        let schema_jsons = StringArray::from(vec![schema_json.as_str()]);
        let primary_key_jsons = StringArray::from(vec![primary_key_json.as_deref()]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(table_ids) as ArrayRef,
                Arc::new(table_uuids) as ArrayRef,
                Arc::new(table_names) as ArrayRef,
                Arc::new(paths) as ArrayRef,
                Arc::new(path_is_relative) as ArrayRef,
                Arc::new(schema_jsons) as ArrayRef,
                Arc::new(primary_key_jsons) as ArrayRef,
            ],
        )
        .map_err(|e| CatalogError::InvalidOperation {
            message: format!("Failed to create table batch: {e}"),
        })?;

        // Append to tables file
        Self::append_to_file(&self.tables_file(), batch).await?;

        Ok(table_id)
    }

    async fn get_table(&self, table_name: &str) -> CatalogResult<TableMetadata> {
        let tables_path = self.tables_file();

        if !tables_path.exists() {
            return Err(CatalogError::TableNotFound {
                table_name: table_name.to_string(),
            });
        }

        // Read tables directly from Arrow IPC file
        let batches = tokio::task::spawn_blocking({
            let tables_path = tables_path.clone();
            move || {
                let file = std::fs::File::open(&tables_path).map_err(|e| {
                    CatalogError::InvalidOperation {
                        message: format!("Failed to open tables file: {e}"),
                    }
                })?;

                let reader = arrow_ipc::reader::FileReader::try_new(file, None).map_err(|e| {
                    CatalogError::InvalidOperation {
                        message: format!("Failed to create Arrow reader: {e}"),
                    }
                })?;

                let mut batches = Vec::new();
                for batch_result in reader {
                    batches.push(batch_result.map_err(|e| CatalogError::InvalidOperation {
                        message: format!("Failed to read batch: {e}"),
                    })?);
                }

                Ok::<Vec<RecordBatch>, CatalogError>(batches)
            }
        })
        .await??;

        if batches.is_empty() {
            return Err(CatalogError::TableNotFound {
                table_name: table_name.to_string(),
            });
        }

        // Find the table by name
        for batch in &batches {
            let table_names = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CatalogError::InvalidOperation {
                    message: "Invalid table_name column type".to_string(),
                })?;

            // Look for matching table name
            for row_idx in 0..batch.num_rows() {
                if table_names.value(row_idx) == table_name {
                    // Extract table metadata from this row
                    return Self::extract_table_metadata_from_batch(batch, row_idx);
                }
            }
        }

        Err(CatalogError::TableNotFound {
            table_name: table_name.to_string(),
        })
    }

    async fn get_table_by_id(&self, table_id: i64) -> CatalogResult<TableMetadata> {
        Err(CatalogError::TableNotFound {
            table_name: format!("id:{table_id}"),
        })
    }

    async fn list_tables(&self) -> CatalogResult<Vec<TableMetadata>> {
        Ok(vec![])
    }

    async fn drop_table(&self, _table_name: &str) -> CatalogResult<()> {
        Err(CatalogError::InvalidOperation {
            message: "Not yet implemented".to_string(),
        })
    }

    async fn add_data_file(&self, _data_file: DataFile) -> CatalogResult<i64> {
        Err(CatalogError::InvalidOperation {
            message: "Not yet implemented".to_string(),
        })
    }

    async fn get_data_files(&self, _table_id: i64) -> CatalogResult<Vec<DataFile>> {
        Ok(vec![])
    }

    async fn add_delete_file(&self, _delete_file: DeleteFile) -> CatalogResult<i64> {
        Err(CatalogError::InvalidOperation {
            message: "Not yet implemented".to_string(),
        })
    }

    async fn get_delete_files(&self, _data_file_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        Ok(vec![])
    }

    async fn get_table_delete_files(&self, _table_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        Ok(vec![])
    }

    async fn get_table_stats(&self, _table_id: i64) -> CatalogResult<TableStats> {
        Ok(TableStats::default())
    }

    async fn begin_transaction(&self) -> CatalogResult<()> {
        Ok(())
    }

    async fn commit_transaction(&self) -> CatalogResult<()> {
        Ok(())
    }

    async fn rollback_transaction(&self) -> CatalogResult<()> {
        Ok(())
    }
}

impl ArrowBackend {
    /// Helper to extract table metadata from a record batch row.
    fn extract_table_metadata_from_batch(
        batch: &RecordBatch,
        row_idx: usize,
    ) -> CatalogResult<TableMetadata> {
        let table_id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| CatalogError::InvalidOperation {
                message: "Invalid table_id column type".to_string(),
            })?
            .value(row_idx);

        let table_uuid = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::InvalidOperation {
                message: "Invalid table_uuid column type".to_string(),
            })?
            .value(row_idx)
            .to_string();

        let table_name = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::InvalidOperation {
                message: "Invalid table_name column type".to_string(),
            })?
            .value(row_idx)
            .to_string();

        let path = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::InvalidOperation {
                message: "Invalid path column type".to_string(),
            })?
            .value(row_idx)
            .to_string();

        let path_is_relative = batch
            .column(4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| CatalogError::InvalidOperation {
                message: "Invalid path_is_relative column type".to_string(),
            })?
            .value(row_idx);

        let schema_json = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::InvalidOperation {
                message: "Invalid schema_json column type".to_string(),
            })?
            .value(row_idx);

        let primary_key_json_arr = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::InvalidOperation {
                message: "Invalid primary_key_json column type".to_string(),
            })?;

        let primary_key_json = if primary_key_json_arr.is_null(row_idx) {
            None
        } else {
            Some(primary_key_json_arr.value(row_idx).to_string())
        };

        // Deserialize schema
        let schema = {
            use base64::Engine;
            use bytes::Bytes;

            let schema_bytes = base64::engine::general_purpose::STANDARD
                .decode(schema_json)
                .map_err(|e| CatalogError::InvalidOperation {
                    message: format!("Failed to decode schema: {e}"),
                })?;

            let ipc_message = arrow_flight::IpcMessage(Bytes::from(schema_bytes));
            arrow_schema::Schema::try_from(ipc_message).map_err(|e| {
                CatalogError::InvalidOperation {
                    message: format!("Failed to deserialize schema: {e}"),
                }
            })?
        };

        let schema = Arc::new(schema);

        // Parse primary key
        let primary_key = if let Some(ref pk_json) = primary_key_json {
            serde_json::from_str(pk_json).unwrap_or_default()
        } else {
            vec![]
        };

        Ok(TableMetadata {
            table_id,
            table_uuid,
            table_name,
            path,
            path_is_relative,
            schema,
            primary_key,
        })
    }

    async fn get_table_by_id(&self, table_id: i64) -> CatalogResult<TableMetadata> {
        Err(CatalogError::TableNotFound {
            table_name: format!("id:{table_id}"),
        })
    }

    async fn list_tables(&self) -> CatalogResult<Vec<TableMetadata>> {
        Ok(vec![])
    }

    async fn drop_table(&self, _table_name: &str) -> CatalogResult<()> {
        Err(CatalogError::InvalidOperation {
            message: "Not yet implemented".to_string(),
        })
    }

    async fn add_data_file(&self, _data_file: DataFile) -> CatalogResult<i64> {
        Err(CatalogError::InvalidOperation {
            message: "Not yet implemented".to_string(),
        })
    }

    async fn get_data_files(&self, _table_id: i64) -> CatalogResult<Vec<DataFile>> {
        Ok(vec![])
    }

    async fn add_delete_file(&self, _delete_file: DeleteFile) -> CatalogResult<i64> {
        Err(CatalogError::InvalidOperation {
            message: "Not yet implemented".to_string(),
        })
    }

    async fn get_delete_files(&self, _data_file_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        Ok(vec![])
    }

    async fn get_table_delete_files(&self, _table_id: i64) -> CatalogResult<Vec<DeleteFile>> {
        Ok(vec![])
    }

    async fn get_table_stats(&self, _table_id: i64) -> CatalogResult<TableStats> {
        Ok(TableStats::default())
    }

    async fn begin_transaction(&self) -> CatalogResult<()> {
        Ok(())
    }

    async fn commit_transaction(&self) -> CatalogResult<()> {
        Ok(())
    }

    async fn rollback_transaction(&self) -> CatalogResult<()> {
        Ok(())
    }
}
