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

use arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use tokio::sync::mpsc::Sender;

use super::PartitionData;
use super::config::PartitionBufferType;
use super::memory::MemoryPartitionBuffer;
use super::parquet::ParquetPartitionBuffer;

/// Factory for creating partition buffers based on configuration.
pub struct PartitionBufferFactory;

impl PartitionBufferFactory {
    /// Create a partition buffer based on the configuration.
    pub fn create_buffer(
        config: &super::PartitionBufferConfig,
        batch_sender: Sender<(String, PartitionData)>,
        schema: SchemaRef,
        table_name: &str,
    ) -> Result<Box<dyn super::PartitionBuffer>, DataFusionError> {
        match config.buffer_type {
            PartitionBufferType::Memory => Ok(Box::new(MemoryPartitionBuffer::new(
                batch_sender,
                config.rows_per_partition_threshold,
            ))),
            PartitionBufferType::Parquet => Ok(Box::new(ParquetPartitionBuffer::new(
                batch_sender,
                schema,
                config.rows_per_partition_threshold,
                &config.temp_dir,
                table_name,
            )?)),
        }
    }
}
