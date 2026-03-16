/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::time::SystemTime;
use std::{collections::HashMap, sync::Arc};

use datafusion::sql::TableReference;
use object_store::ObjectStore;
use object_store_occ::{InsertResult, ObjectState, WriteResult};
use snafu::prelude::*;

use crate::cluster::partition::metadata::PartitionValue;

use super::metadata::{PartitionMetadata, TablePartitionMetadata};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to access partition metadata for table {table}: {source}"))]
    MetadataAccess {
        table: String,
        source: object_store_occ::Error,
    },

    #[snafu(display("Failed to get current time: {source}"))]
    SystemTime { source: std::time::SystemTimeError },

    #[snafu(display("Partition {partition} not found in table {table}"))]
    PartitionNotFound { table: String, partition: String },

    #[snafu(display("No partition metadata found for table {table}"))]
    TableMetadataNotFound { table: String },

    #[snafu(display("Concurrent modification detected for table {table}"))]
    ConcurrentModification { table: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

static PARTITION_PREFIX: &str = "accelerations/partitions/";

/// Manages partition metadata for accelerated tables in object storage.
///
/// Uses optimistic concurrency control to safely coordinate partition assignments
/// across multiple schedulers without locks.
#[derive(Debug)]
pub struct PartitionManager {
    state: ObjectState<TablePartitionMetadata>,
}

#[derive(Debug, Clone)]
pub struct AllocationResult {
    pub previously_assigned: Vec<PartitionValue>,
    pub newly_assigned: Vec<PartitionValue>,
}

impl AllocationResult {
    #[must_use]
    pub fn all_assigned(self) -> Vec<PartitionValue> {
        let mut all = self.previously_assigned;
        all.extend(self.newly_assigned);
        all
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.previously_assigned.len() + self.newly_assigned.len()
    }
}

impl PartitionManager {
    /// Creates a new partition manager with the given object store.
    ///
    /// All partition metadata will be stored under the "partitions/" prefix.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            state: ObjectState::new(store).with_prefix(PARTITION_PREFIX),
        }
    }

    #[must_use]
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.state = self.state.with_prefix(prefix);
        self
    }

    /// Get partition metadata for a table from object store.
    pub async fn get_table_metadata(
        &self,
        table: &TableReference,
    ) -> Result<Option<TablePartitionMetadata>> {
        let key = table.to_string();
        self.state
            .get(&key)
            .await
            .context(MetadataAccessSnafu { table: key })
    }

    /// Get partition metadata from local cache (may be stale).
    #[must_use]
    pub fn get_cached_table_metadata(
        &self,
        table: &TableReference,
    ) -> Option<TablePartitionMetadata> {
        let key = table.to_string();
        self.state.get_cached(&key)
    }

    /// Initialize a blank partition metadata file for a table.
    ///
    /// For acceleration table partitions this acts as a temporary lock during scheduler startup (other
    ///  schedulers will not re-initialize, since a file exists, even if blank). If the file already
    /// exists, this is a no-op and returns `Ok(false)`.
    pub async fn initialize_blank_metadata(&self, table: &TableReference) -> Result<bool> {
        self.initialize_metadata(table, Vec::new()).await
    }

    /// Initialize partition metadata for a table with the given partition expression SQL strings.
    ///
    /// If the file already exists, this is a no-op and returns `Ok(false)`.
    pub async fn initialize_metadata(
        &self,
        table: &TableReference,
        partition_expressions: Vec<String>,
    ) -> Result<bool> {
        // If its cached, can avoid insert operation. Optimisation to reduce object store calls.
        if self.get_cached_table_metadata(table).is_some() {
            return Ok(false);
        }
        let key = table.to_string();
        let now_ms = now_ms()?;
        let mut metadata = TablePartitionMetadata::blank(table.to_string(), now_ms);
        metadata.partition_expressions = partition_expressions;

        match self
            .state
            .insert(&key, &metadata)
            .await
            .context(MetadataAccessSnafu { table: key.clone() })?
        {
            InsertResult::Ok => Ok(true),
            InsertResult::AlreadyExists => Ok(false),
        }
    }

    /// Update partition metadata with discovered partitions, all marked as unassigned.
    ///
    /// This replaces the partitions list with the provided partition values.
    /// If `partition_expressions` is non-empty, it also sets the SQL expression strings
    /// (only when currently empty, to avoid overwriting values set during table creation).
    pub async fn set_unassigned_partitions(
        &self,
        table: &TableReference,
        partition_values: Vec<HashMap<String, String>>,
        partition_expressions: Vec<String>,
    ) -> Result<()> {
        let key = table.to_string();
        let now_ms = now_ms()?;

        let mut metadata = self
            .get_table_metadata(table)
            .await?
            .unwrap_or_else(|| TablePartitionMetadata::blank(table.to_string(), now_ms));

        metadata.partitions = partition_values
            .into_iter()
            .map(PartitionMetadata::new)
            .collect();
        metadata.updated_at = now_ms;
        if metadata.partition_expressions.is_empty() {
            metadata.partition_expressions = partition_expressions;
        }

        self.write_metadata(&key, metadata).await
    }

    /// Allocates unassigned partitions to an executor.
    ///
    /// Uses OCC to atomically update metadata.
    pub async fn allocate_partitions(
        &self,
        table: &TableReference,
        executor_id: &str,
        limit: usize,
    ) -> Result<AllocationResult> {
        let key = table.to_string();
        let mut backoff = util::fibonacci_backoff::FibonacciBackoffBuilder::new()
            .max_retries(Some(5))
            .build();

        loop {
            let now_ms = now_ms()?;
            let mut metadata = self
                .get_table_metadata(table)
                .await?
                .ok_or_else(|| Error::TableMetadataNotFound { table: key.clone() })?;

            let mut result = AllocationResult {
                newly_assigned: vec![],
                previously_assigned: metadata
                    .partitions
                    .iter()
                    .filter_map(|p| {
                        if p.is_assigned_to(executor_id) {
                            Some(p.partition_value.clone())
                        } else {
                            None
                        }
                    })
                    .collect(),
            };
            let mut changes = false;

            for partition in &mut metadata.partitions {
                if result.count() >= limit {
                    break;
                }

                if !partition.is_assigned() {
                    partition.assign_to(executor_id.to_string(), now_ms);
                    result
                        .newly_assigned
                        .push(partition.partition_value.clone());
                    changes = true;
                }
            }

            if !changes {
                return Ok(result);
            }

            metadata.updated_at = now_ms;

            match self.write_metadata(&key, metadata).await {
                Ok(()) => return Ok(result),
                Err(Error::ConcurrentModification { .. }) => {
                    if let Some(delay) = backoff.next_duration() {
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(Error::ConcurrentModification { table: key.clone() });
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Assigns a partition to an executor in the metadata store.
    pub async fn assign_partition(
        &self,
        table: &TableReference,
        partition_value: &PartitionValue,
        executor_id: &str,
    ) -> Result<()> {
        let key = table.to_string();
        let mut backoff = util::fibonacci_backoff::FibonacciBackoffBuilder::new()
            .max_retries(Some(5))
            .build();

        loop {
            let now_ms = now_ms()?;
            let mut metadata = self
                .get_table_metadata(table)
                .await?
                .ok_or_else(|| Error::TableMetadataNotFound { table: key.clone() })?;

            let mut updated = false;
            for partition in &mut metadata.partitions {
                if partition.partition_value == *partition_value {
                    partition.assign_to(executor_id.to_string(), now_ms);
                    updated = true;
                    break;
                }
            }

            if !updated {
                return Err(Error::PartitionNotFound {
                    table: key.clone(),
                    partition: format!("{partition_value:?}"),
                });
            }

            metadata.updated_at = now_ms;

            match self.write_metadata(&key, metadata).await {
                Ok(()) => return Ok(()),
                Err(Error::ConcurrentModification { .. }) => {
                    if let Some(delay) = backoff.next_duration() {
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(Error::ConcurrentModification { table: key.clone() });
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// List all tables with partition metadata.
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        self.state.list_keys().await.context(MetadataAccessSnafu {
            table: String::from("<list>"),
        })
    }

    /// Refresh the local cache from object store.
    pub async fn refresh(&self) -> Result<()> {
        self.state.refresh().await.context(MetadataAccessSnafu {
            table: String::from("<refresh>"),
        })
    }

    /// Adds a new partition to a table's metadata and assigns it to an executor
    /// in a single OCC write. If the partition already exists, it is assigned
    /// (or left as-is if already assigned to the same executor).
    pub async fn add_and_assign_partition(
        &self,
        table: &TableReference,
        partition_value: &PartitionValue,
        executor_id: &str,
    ) -> Result<()> {
        let key = table.to_string();
        let mut backoff = util::fibonacci_backoff::FibonacciBackoffBuilder::new()
            .max_retries(Some(5))
            .build();

        loop {
            let now_ms = now_ms()?;
            let mut metadata = self
                .get_table_metadata(table)
                .await?
                .ok_or_else(|| Error::TableMetadataNotFound { table: key.clone() })?;

            // Find existing partition or add a new one.
            let partition = metadata
                .partitions
                .iter_mut()
                .find(|p| p.partition_value == *partition_value);

            if let Some(p) = partition {
                if p.is_assigned_to(executor_id) {
                    return Ok(());
                }
                p.assign_to(executor_id.to_string(), now_ms);
            } else {
                let mut new_partition = PartitionMetadata::new(partition_value.clone());
                new_partition.assign_to(executor_id.to_string(), now_ms);
                metadata.add_partition(new_partition);
            }

            metadata.updated_at = now_ms;

            match self.write_metadata(&key, metadata).await {
                Ok(()) => return Ok(()),
                Err(Error::ConcurrentModification { .. }) => {
                    if let Some(delay) = backoff.next_duration() {
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(Error::ConcurrentModification { table: key.clone() });
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Write metadata using `insert_or_update` with conflict handling.
    pub(crate) async fn write_metadata(
        &self,
        key: &str,
        metadata: TablePartitionMetadata,
    ) -> Result<()> {
        match self
            .state
            .insert_or_update(key, &metadata)
            .await
            .context(MetadataAccessSnafu {
                table: key.to_string(),
            })? {
            WriteResult::Inserted | WriteResult::Updated => Ok(()),
            WriteResult::Conflict { .. } => Err(Error::ConcurrentModification {
                table: key.to_string(),
            }),
        }
    }
}

#[expect(clippy::result_large_err)]
fn now_ms() -> Result<u128> {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .context(SystemTimeSnafu)
}
