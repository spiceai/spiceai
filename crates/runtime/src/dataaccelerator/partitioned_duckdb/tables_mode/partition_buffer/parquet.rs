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

//! Parquet-based partition buffer implementation.
//!
//! This buffer writes partition data to temporary Parquet files on disk,
//! providing memory-efficient handling of large datasets.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::{WriterProperties, WriterVersion};
use snafu::prelude::*;
use tokio::sync::mpsc::Sender;

use super::{PartitionBuffer, PartitionData};

#[derive(Debug, Snafu)]
enum ParquetBufferError {
    #[snafu(display("Failed to create temporary directory '{}': {source}", path.display()))]
    TempDirCreation {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to create partition directory '{}': {source}", path.display()))]
    PartitionDirCreation {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to create Parquet file '{}': {source}", path.display()))]
    ParquetFileCreation {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to write to Parquet file '{}': {source}", path.display()))]
    ParquetWrite {
        path: PathBuf,
        source: datafusion::parquet::errors::ParquetError,
    },

    #[snafu(display("Failed to close Parquet writer for '{}': {source}", path.display()))]
    ParquetClose {
        path: PathBuf,
        source: datafusion::parquet::errors::ParquetError,
    },

    #[snafu(display("Failed to send partition data for '{partition}': {source}"))]
    ChannelSend {
        partition: String,
        source: tokio::sync::mpsc::error::SendError<(String, PartitionData)>,
    },

    #[snafu(display("Failed to get system time: {source}"))]
    SystemTime { source: std::time::SystemTimeError },

    #[snafu(display(
        "Partition sink for '{partition_id}' not found - should exist after creation",
    ))]
    PartitionSinkNotFound { partition_id: String },

    #[snafu(display("Parquet writer not available for partition sink"))]
    WriterNotAvailable,
}

impl From<ParquetBufferError> for DataFusionError {
    fn from(err: ParquetBufferError) -> Self {
        DataFusionError::Execution(err.to_string())
    }
}

/// Parquet-based partition buffer that writes data to temporary files.
/// When the configured row threshold is reached, the file is closed and its path
/// is sent for ingestion.
#[derive(Debug)]
pub struct ParquetPartitionBuffer {
    /// Channel sender for communicating with `DuckDB` writer (None when finished)
    sender: Option<Sender<(String, PartitionData)>>,
    /// Arrow schema for all partitions
    schema: SchemaRef,
    /// Map of partition ID to its corresponding sink
    partition_sinks: HashMap<String, PartitionSink>,
    /// Row count tracking per partition
    row_counts: HashMap<String, usize>,
    /// Threshold for flushing partition data
    rows_per_partition_threshold: usize,
    /// Base temporary directory for all partition files
    temp_dir: PathBuf,
}

impl ParquetPartitionBuffer {
    /// Create a new Parquet partition buffer.
    ///
    /// # Arguments
    /// * `sender` - Channel for sending partition data to `DuckDB` writer
    /// * `schema` - Arrow schema for the data
    /// * `rows_per_partition_threshold` - Number of rows after which to flush a partition
    /// * `base_temp_dir` - Base directory for temporary files
    /// * `table_name` - Name of the table for organizing temp files
    ///
    /// # Errors
    /// Returns an error if the temporary directory cannot be created.
    pub fn new(
        sender: Sender<(String, PartitionData)>,
        schema: SchemaRef,
        rows_per_partition_threshold: usize,
        base_temp_dir: &std::path::Path,
        table_name: &str,
    ) -> Result<Self, DataFusionError> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context(SystemTimeSnafu)?
            .as_millis();

        let temp_dir = base_temp_dir.join(format!("{table_name}/{timestamp}"));

        std::fs::create_dir_all(&temp_dir).context(TempDirCreationSnafu {
            path: temp_dir.clone(),
        })?;

        tracing::debug!(
            "Created temporary directory for Parquet partition buffer: {}",
            temp_dir.display()
        );

        Ok(Self {
            sender: Some(sender),
            schema,
            partition_sinks: HashMap::new(),
            row_counts: HashMap::new(),
            rows_per_partition_threshold,
            temp_dir,
        })
    }

    /// Process a single batch for a partition.
    ///
    /// This method writes the batch to the partition's Parquet file and
    /// checks if the threshold has been reached to trigger a flush.
    async fn process_batch(
        &mut self,
        partition_id: String,
        batch: RecordBatch,
    ) -> Result<(), DataFusionError> {
        let batch_row_count = batch.num_rows();

        // Ensure partition sink exists
        if !self.partition_sinks.contains_key(&partition_id) {
            let sink = PartitionSink::new(&self.temp_dir, &partition_id, &self.schema)?;
            self.partition_sinks.insert(partition_id.clone(), sink);
        }

        // Write batch to Parquet file
        let sink =
            self.partition_sinks
                .get_mut(&partition_id)
                .context(PartitionSinkNotFoundSnafu {
                    partition_id: partition_id.clone(),
                })?;
        sink.write_batch(&batch)?;

        // Update row count
        let current_rows = self.row_counts.entry(partition_id.clone()).or_insert(0);
        *current_rows += batch_row_count;

        // Check if threshold is reached
        if *current_rows >= self.rows_per_partition_threshold {
            self.flush_partition(&partition_id, true).await?;
        }

        Ok(())
    }

    /// Flush a specific partition's data to the channel.
    async fn flush_partition(
        &mut self,
        partition_id: &str,
        continue_writing: bool,
    ) -> Result<(), DataFusionError> {
        if let Some(sink) = self.partition_sinks.get_mut(partition_id) {
            let file_path = sink.flush(continue_writing)?;
            let num_rows = self.row_counts.get(partition_id).unwrap_or(&0);

            tracing::debug!(
                "Flushing partition '{partition_id}' with {num_rows} rows to file: {}",
                file_path.display()
            );

            // Only send if we still have a sender (haven't finished yet)
            if let Some(sender) = &self.sender {
                sender
                    .send((
                        partition_id.to_string(),
                        PartitionData::ParquetFile(file_path),
                    ))
                    .await
                    .context(ChannelSendSnafu {
                        partition: partition_id,
                    })?;
            } else {
                tracing::warn!(
                    "Attempted to flush partition '{partition_id}' after sender was dropped",
                );
            }

            // Reset row count for this partition since we flushed the file
            self.row_counts.insert(partition_id.to_string(), 0);
        }

        Ok(())
    }
}

#[async_trait]
impl PartitionBuffer for ParquetPartitionBuffer {
    async fn process(
        &mut self,
        partition_id: String,
        batches: Vec<RecordBatch>,
    ) -> datafusion::common::Result<()> {
        for batch in batches {
            self.process_batch(partition_id.clone(), batch).await?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> datafusion::common::Result<()> {
        let partition_ids: Vec<String> = self.partition_sinks.keys().cloned().collect();
        for partition_id in partition_ids {
            if let Some(&row_count) = self.row_counts.get(&partition_id)
                && row_count > 0
            {
                self.flush_partition(&partition_id, false).await?;
            }
        }

        // Drop the sender to signal completion
        if let Some(sender) = self.sender.take() {
            drop(sender);
            tracing::debug!("Dropped sender for ParquetPartitionBuffer after finish");
        }

        Ok(())
    }
}

impl Drop for ParquetPartitionBuffer {
    fn drop(&mut self) {
        // Clean up temporary directory
        if self.temp_dir.exists() {
            if let Err(e) = std::fs::remove_dir_all(&self.temp_dir) {
                tracing::warn!(
                    "Failed to clean up temporary directory '{path}': {source}",
                    path = self.temp_dir.display(),
                    source = e
                );
            } else {
                tracing::debug!(
                    "Cleaned up temporary directory: {path}",
                    path = self.temp_dir.display()
                );
            }
        }
    }
}

/// Manages writing to a single partition's Parquet files.
#[derive(Debug)]
struct PartitionSink {
    /// Directory containing this partition's files
    partition_dir: PathBuf,
    /// Current file index for generating unique names
    file_index: usize,
    /// Total rows written to current file
    rows_written: u64,
    /// Current Parquet writer (None when no file is open)
    writer: Option<ArrowWriter<std::fs::File>>,
    /// Schema for validation and writer creation
    schema: SchemaRef,
}

impl PartitionSink {
    /// Create a new partition sink.
    ///
    /// # Arguments
    /// * `base_dir` - Base temporary directory
    /// * `partition_key` - Unique identifier for this partition
    /// * `schema` - Arrow schema for the data
    fn new(
        base_dir: &Path,
        partition_key: &str,
        schema: &SchemaRef,
    ) -> Result<Self, DataFusionError> {
        // Create partition-specific directory
        let partition_dir = base_dir.join(format!("partition_{partition_key}"));
        std::fs::create_dir_all(&partition_dir).context(PartitionDirCreationSnafu {
            path: partition_dir.clone(),
        })?;

        let mut sink = Self {
            partition_dir,
            file_index: 0,
            rows_written: 0,
            writer: None,
            schema: Arc::clone(schema),
        };

        // Create the first file
        sink.roll()?;

        Ok(sink)
    }

    /// Create a new Parquet file and associated writer.
    fn roll(&mut self) -> Result<(), DataFusionError> {
        self.file_index += 1;
        let file_path = self.current_file_path();

        let properties = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            // Use SNAPPY compression for good balance of speed and size
            .set_compression(Compression::SNAPPY)
            // Use default row group size (1024 * 1024)
            .set_max_row_group_size(
                datafusion::parquet::file::properties::DEFAULT_MAX_ROW_GROUP_SIZE,
            )
            // Use default write batch size (1024)
            .set_write_batch_size(datafusion::parquet::file::properties::DEFAULT_WRITE_BATCH_SIZE)
            .build();

        let file = std::fs::File::create(&file_path).context(ParquetFileCreationSnafu {
            path: file_path.clone(),
        })?;

        let writer = ArrowWriter::try_new(file, Arc::clone(&self.schema), Some(properties))
            .context(ParquetWriteSnafu {
                path: file_path.clone(),
            })?;

        self.writer = Some(writer);
        self.rows_written = 0;

        tracing::trace!("Created new Parquet file: {}", file_path.display());

        Ok(())
    }

    /// Write a record batch to the current file.
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let writer = self.writer.as_mut().context(WriterNotAvailableSnafu)?;

        writer.write(batch).context(ParquetWriteSnafu {
            path: self.current_file_path(),
        })?;

        self.rows_written += batch.num_rows() as u64;

        Ok(())
    }

    /// Flush current file and start a new one, returning the path of the flushed file
    fn flush(&mut self, continue_writing: bool) -> Result<PathBuf, DataFusionError> {
        // Get the path of the file we're about to close
        let current_file_path = self.current_file_path();

        // Close previous writer if it exists
        if let Some(mut writer) = self.writer.take() {
            writer.finish().context(ParquetCloseSnafu {
                path: self.current_file_path(),
            })?;
        }

        if continue_writing {
            // Start a new file only if we are continuing to write
            self.roll()?;
        }

        Ok(current_file_path)
    }

    /// Get the path of the current file.
    fn current_file_path(&self) -> PathBuf {
        self.partition_dir
            .join(format!("part-{:05}.parquet", self.file_index))
    }
}
