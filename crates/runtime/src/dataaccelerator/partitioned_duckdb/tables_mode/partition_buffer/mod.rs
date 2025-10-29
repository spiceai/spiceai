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

//! Partition buffer implementations for `DuckDB` partitioned table mode.
//!
//! This module provides different buffering strategies for partitioned data:
//! - Memory-based buffering for smaller datasets
//! - Parquet file-based buffering for larger datasets with better memory efficiency and faster ingestion.
//!

use std::fmt;
use std::path::PathBuf;

use arrow::array::RecordBatch;
use async_trait::async_trait;

pub mod config;
pub mod factory;
pub mod memory;
pub mod parquet;

// Re-export main types
pub use config::PartitionBufferConfig;
pub use factory::PartitionBufferFactory;

/// Data type for partition buffer channels.
///
/// This enum allows the buffer to send either in-memory batches (Memory buffer)
/// or file paths (Parquet buffer) to the `DuckDB` writer.
#[derive(Debug)]
pub enum PartitionData {
    /// In-memory record batches (used by `MemoryPartitionBuffer`)
    Batches(Vec<RecordBatch>),
    /// File path to a Parquet file (used by `ParquetPartitionBuffer`)
    ParquetFile(PathBuf),
}

/// `PartitionBuffer` accumulates data for each partition, tracking the number of rows.
/// When the number of buffered rows for a partition reaches a configured threshold, the buffer is flushed
/// and the data is sent to `DuckDB` for ingestion. This enables efficient faster writes per partition.
#[async_trait]
pub trait PartitionBuffer: Send + fmt::Debug {
    /// Process batches for a specific partition.
    async fn process(
        &mut self,
        partition_id: String,
        batches: Vec<RecordBatch>,
    ) -> datafusion::common::Result<()>;

    /// Complete writing by flushing all remaining buffered data and closing the sender.
    async fn finish(&mut self) -> datafusion::common::Result<()>;
}
