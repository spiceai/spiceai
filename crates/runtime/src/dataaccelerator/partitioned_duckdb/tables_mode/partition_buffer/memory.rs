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

use std::collections::HashMap;
use std::fmt;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use tokio::sync::mpsc::Sender;

use crate::dataaccelerator::partitioned_duckdb::tables_mode::partition_buffer::PartitionData;

use super::PartitionBuffer;

/// In-memory implementation of partition buffer.
///
/// This implementation maintains Vec<RecordBatch> per partition in memory
/// and flushes them when row thresholds are reached.
pub struct MemoryPartitionBuffer {
    sender: Option<Sender<(String, PartitionData)>>,
    buffers: HashMap<String, Vec<RecordBatch>>,
    row_counts: HashMap<String, usize>,
    rows_per_partition_threshold: usize,
}

impl MemoryPartitionBuffer {
    /// Create a new memory-based partition buffer.
    pub fn new(
        sender: Sender<(String, PartitionData)>,
        rows_per_partition_threshold: usize,
    ) -> Self {
        Self {
            sender: Some(sender),
            buffers: HashMap::new(),
            row_counts: HashMap::new(),
            rows_per_partition_threshold,
        }
    }

    /// Flush all buffered data for a specific partition.
    async fn flush_partition(&mut self, partition_id: &str) -> datafusion::common::Result<()> {
        if let Some(partition_batches) = self.buffers.remove(partition_id) {
            if !partition_batches.is_empty() {
                // Only send if we still have a sender (haven't finished yet)
                if let Some(sender) = &self.sender {
                    sender.send((partition_id.to_string(), PartitionData::Batches(partition_batches))).await
                        .map_err(|e| DataFusionError::Execution(format!(
                            "Unable to send combined RecordBatch for partition {partition_id} to DuckDB writer: {e}"
                        )))?;
                } else {
                    tracing::warn!(
                        "Attempted to flush partition '{}' after sender was dropped",
                        partition_id
                    );
                }
            }
            self.row_counts.remove(partition_id);
        }
        Ok(())
    }
}

#[async_trait]
impl PartitionBuffer for MemoryPartitionBuffer {
    /// Add batches to the specified partition buffer. If threshold is reached, flush that partition.
    async fn process(
        &mut self,
        partition_id: String,
        batches: Vec<RecordBatch>,
    ) -> datafusion::common::Result<()> {
        let total_batch_rows: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();

        // Add all batches to partition buffer
        self.buffers
            .entry(partition_id.clone())
            .or_default()
            .extend(batches);

        // Update row count for this partition
        let current_rows = self.row_counts.entry(partition_id.clone()).or_default();
        *current_rows += total_batch_rows;

        // Check if we should flush this partition's buffer
        if *current_rows >= self.rows_per_partition_threshold {
            self.flush_partition(&partition_id).await?;
        }

        Ok(())
    }

    /// Complete writing by flushing all remaining buffered data and closing the sender.
    async fn finish(&mut self) -> datafusion::common::Result<()> {
        let partition_ids: Vec<String> = self.buffers.keys().cloned().collect();
        for partition_id in partition_ids {
            self.flush_partition(&partition_id).await?;
        }

        // Drop the sender to signal completion
        tracing::debug!("Dropping sender for memory partition buffer");
        self.sender = None;

        Ok(())
    }
}

impl fmt::Debug for MemoryPartitionBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryPartitionBuffer")
            .field(
                "rows_per_partition_threshold",
                &self.rows_per_partition_threshold,
            )
            .finish_non_exhaustive()
    }
}
