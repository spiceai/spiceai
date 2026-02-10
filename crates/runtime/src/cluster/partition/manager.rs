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

#[expect(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to access partition metadata for table {table}: {source}"))]
    MetadataAccess {
        table: String,
        source: object_store_occ::Error,
    },

    #[snafu(display("Failed to get current time: {source}"))]
    TimeError { source: std::time::SystemTimeError },

    #[snafu(display("Partition {partition} not found in table {table}"))]
    PartitionNotFound { table: String, partition: String },

    #[snafu(display("Concurrent modification detected for table {table}"))]
    ConcurrentModification { table: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

static PARTITION_PREFIX: &str = "accelerations/partitions/";

/// Manages partition metadata for accelerated tables in object storage.
///
/// Uses optimistic concurrency control to safely coordinate partition assignments
/// across multiple schedulers without locks.
pub struct PartitionManager {
    state: ObjectState<TablePartitionMetadata>,
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
    /// This acts as a temporary lock during scheduler startup. If the file already exists,
    /// this is a no-op and returns `Ok(false)`.
    pub async fn initialize_blank_metadata(&self, table: &TableReference) -> Result<bool> {
        let key = table.to_string();
        let now_ms = now_ms()?;
        let metadata = TablePartitionMetadata::blank(table.to_string(), now_ms);

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
    pub async fn set_unassigned_partitions(
        &self,
        table: &TableReference,
        partition_values: Vec<HashMap<String, String>>,
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

        self.write_metadata(&key, metadata).await
    }

    /// Allocates unassigned partitions to an executor.
    ///
    /// Returns the list of allocated partitions.
    /// Uses OCC to atomically update metadata.
    pub async fn allocate_partitions(
        &self,
        table: &TableReference,
        executor_id: &str,
        limit: usize,
    ) -> Result<Vec<PartitionValue>> {
        let key = table.to_string();
        let mut backoff = util::fibonacci_backoff::FibonacciBackoffBuilder::new()
            .max_retries(Some(5))
            .build();

        loop {
            let now_ms = now_ms()?;
            let mut metadata =
                self.get_table_metadata(table)
                    .await?
                    .ok_or_else(|| Error::PartitionNotFound {
                        table: key.clone(),
                        partition: "any".to_string(),
                    })?;

            let mut allocated: Vec<_> = metadata
                .partitions
                .iter()
                .filter_map(|p| {
                    if p.is_assigned_to(executor_id) {
                        Some(p.partition_value.clone())
                    } else {
                        None
                    }
                })
                .collect();
            let mut changes = false;

            for partition in &mut metadata.partitions {
                if allocated.len() >= limit {
                    break;
                }

                if !partition.is_assigned() {
                    partition.assign_to(executor_id.to_string(), now_ms);
                    allocated.push(partition.partition_value.clone());
                    changes = true;
                }
            }

            if !changes {
                return Ok(allocated);
            }

            metadata.updated_at = now_ms;

            match self.write_metadata(&key, metadata).await {
                Ok(()) => return Ok(allocated),
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

    /// Write metadata using `insert_or_update` with conflict handling.
    async fn write_metadata(&self, key: &str, metadata: TablePartitionMetadata) -> Result<()> {
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
        .map_err(|source| Error::TimeError { source })
}
