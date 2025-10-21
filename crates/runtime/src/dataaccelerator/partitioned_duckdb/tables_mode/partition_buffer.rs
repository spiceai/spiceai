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

use std::collections::HashMap;

use arrow::array::RecordBatch;
use datafusion::error::DataFusionError;
use tokio::sync::mpsc::Sender;

/// Buffers data per partition before flushing them to `DuckDB`.
///
/// `PartitionBuffer` accumulates `RecordBatch` for each partition, tracking the number of rows.
/// When the number of buffered rows for a partition reaches a configured threshold, the buffer is flushed
/// and the data is sent to `DuckDB` for ingestion. This enables efficient faster writes per partition.
pub struct PartitionBuffer {
    sender: Sender<(String, Vec<RecordBatch>)>,
    buffers: HashMap<String, Vec<RecordBatch>>,
    row_counts: HashMap<String, usize>,
    rows_per_partition_threshold: usize,
}

impl PartitionBuffer {
    pub fn new(
        sender: Sender<(String, Vec<RecordBatch>)>,
        rows_per_partition_threshold: usize,
    ) -> Self {
        Self {
            sender,
            buffers: HashMap::new(),
            row_counts: HashMap::new(),
            rows_per_partition_threshold,
        }
    }

    /// Add batches to the specified partition buffer. If threshold is reached, flush that partition.
    pub async fn process(
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

    /// Flush all buffered data for a specific partition
    async fn flush_partition(&mut self, partition_id: &str) -> datafusion::common::Result<()> {
        if let Some(partition_batches) = self.buffers.remove(partition_id) {
            if !partition_batches.is_empty() {
                self.sender.send((partition_id.to_string(), partition_batches)).await
                    .map_err(|e| DataFusionError::Execution(format!(
                        "Unable to send combined RecordBatch for partition {partition_id} to DuckDB writer: {e}"
                    )))?;
            }
            self.row_counts.remove(partition_id);
        }
        Ok(())
    }

    /// Flush all remaining buffered data for all partitions
    pub async fn flush_all(&mut self) -> datafusion::common::Result<()> {
        let partition_ids: Vec<String> = self.buffers.keys().cloned().collect();
        for partition_id in partition_ids {
            self.flush_partition(&partition_id).await?;
        }
        Ok(())
    }
}
