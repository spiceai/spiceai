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

/// Result of copying partition assignments from one table to another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyAssignmentsResult {
    /// Assignments were copied from the source table.
    Copied {
        /// Number of partition values with executor assignments that were copied.
        partition_count: usize,
    },
    /// Source table had no partition metadata (unpartitioned or newly created).
    NoSourceMetadata,
    /// Source table had partition metadata but no assigned partitions.
    NoAssignments,
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
        let metadata =
            TablePartitionMetadata::new(table.to_string(), now_ms, partition_expressions);

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

        let mut metadata = self.get_table_metadata(table).await?.unwrap_or_else(|| {
            TablePartitionMetadata::new(table.to_string(), now_ms, partition_expressions)
        });

        metadata.partitions = partition_values
            .into_iter()
            .map(PartitionMetadata::new)
            .collect();
        metadata.updated_at = now_ms;

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

    /// Adds new partitions to a table's metadata and assigns each to its
    /// respective executor in a single OCC write. If a partition already exists,
    /// it is assigned (or left as-is if already assigned to the same executor).
    ///
    /// `assignments` is a list of (`partition_value`, `executor_id`) tuples.
    pub async fn add_and_assign_partitions(
        &self,
        table: &TableReference,
        assignments: &[(&PartitionValue, &str)],
    ) -> Result<()> {
        if assignments.is_empty() {
            return Ok(());
        }

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

            let mut changes = false;

            for &(partition_value, executor_id) in assignments {
                let existing = metadata
                    .partitions
                    .iter_mut()
                    .find(|p| p.partition_value == *partition_value);

                if let Some(p) = existing {
                    if !p.is_assigned_to(executor_id) {
                        p.assign_to(executor_id.to_string(), now_ms);
                        changes = true;
                    }
                } else {
                    let mut new_partition = PartitionMetadata::new(partition_value.clone());
                    new_partition.assign_to(executor_id.to_string(), now_ms);
                    metadata.add_partition(new_partition);
                    changes = true;
                }
            }

            if !changes {
                return Ok(());
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

    /// Copy partition-to-executor assignments from one table to another.
    ///
    /// Creates (or overwrites) the target table's metadata with the same
    /// partition expressions, partition values, and executor assignments
    /// as the source table. This ensures `DoPut` write-through routes data
    /// to the same executors for both tables.
    ///
    /// If the source table has no partition metadata, this is a no-op because
    /// the source exists but is unpartitioned and there is nothing to copy.
    pub async fn copy_assignments(
        &self,
        source_table: &TableReference,
        target_table: &TableReference,
    ) -> Result<CopyAssignmentsResult> {
        let Some(source_metadata) = self.get_table_metadata(source_table).await? else {
            return Ok(CopyAssignmentsResult::NoSourceMetadata);
        };

        let assigned_count = source_metadata
            .partitions
            .iter()
            .filter(|p| p.is_assigned())
            .count();

        let now_ms = now_ms()?;
        let mut target_metadata = source_metadata;
        target_metadata.table_name = target_table.to_string();
        target_metadata.updated_at = now_ms;

        let target_key = target_table.to_string();
        self.write_metadata(&target_key, target_metadata).await?;

        if assigned_count == 0 {
            Ok(CopyAssignmentsResult::NoAssignments)
        } else {
            Ok(CopyAssignmentsResult::Copied {
                partition_count: assigned_count,
            })
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

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn test_manager() -> PartitionManager {
        PartitionManager::new(Arc::new(InMemory::new())).with_prefix("test/")
    }

    #[tokio::test]
    async fn copy_assignments_copies_metadata() {
        let pm = test_manager();
        let source = TableReference::parse_str("catalog.schema.source");
        let target = TableReference::parse_str("catalog.schema.target");

        // Initialize source with partition expressions
        pm.initialize_metadata(&source, vec!["region".to_string()])
            .await
            .expect("should initialize");

        // Add a partition and assign it
        let pv = HashMap::from([("region".to_string(), "us-east-1".to_string())]);
        pm.set_unassigned_partitions(&source, vec![pv], vec![])
            .await
            .expect("should set partitions");
        let partition_value: PartitionValue =
            HashMap::from([("region".to_string(), "us-east-1".to_string())]);
        pm.assign_partition(&source, &partition_value, "executor-1")
            .await
            .expect("should assign");

        // Copy assignments
        let result = pm
            .copy_assignments(&source, &target)
            .await
            .expect("should copy");
        assert_eq!(
            result,
            CopyAssignmentsResult::Copied {
                partition_count: 1
            }
        );

        // Verify target has the same metadata
        let target_meta = pm
            .get_table_metadata(&target)
            .await
            .expect("should get")
            .expect("should exist");

        assert_eq!(target_meta.table_name, target.to_string());
        assert_eq!(
            target_meta.partition_expressions,
            vec!["region".to_string()]
        );
        assert_eq!(target_meta.partitions.len(), 1);
        assert!(target_meta.partitions[0].is_assigned_to("executor-1"));
    }

    #[tokio::test]
    async fn copy_assignments_noop_when_source_missing() {
        let pm = test_manager();
        let source = TableReference::parse_str("catalog.schema.missing");
        let target = TableReference::parse_str("catalog.schema.target");

        // Missing source metadata is a no-op (source exists but is unpartitioned).
        let result = pm
            .copy_assignments(&source, &target)
            .await
            .expect("should be a no-op for missing source metadata");
        assert_eq!(result, CopyAssignmentsResult::NoSourceMetadata);

        // Target should have no metadata since nothing was copied.
        let target_meta = pm.get_table_metadata(&target).await.expect("should get");
        assert!(target_meta.is_none());
    }

    #[tokio::test]
    async fn copy_assignments_overwrites_existing_target() {
        let pm = test_manager();
        let source = TableReference::parse_str("catalog.schema.source");
        let target = TableReference::parse_str("catalog.schema.target");

        // Initialize both
        pm.initialize_metadata(&source, vec!["region".to_string()])
            .await
            .expect("should initialize source");
        pm.initialize_metadata(&target, vec!["old_expr".to_string()])
            .await
            .expect("should initialize target");

        // Add partition to source
        let pv = HashMap::from([("region".to_string(), "eu-west-1".to_string())]);
        pm.set_unassigned_partitions(&source, vec![pv], vec![])
            .await
            .expect("should set partitions");

        // Copy should overwrite target
        let result = pm
            .copy_assignments(&source, &target)
            .await
            .expect("should copy");
        assert_eq!(result, CopyAssignmentsResult::NoAssignments);

        let target_meta = pm
            .get_table_metadata(&target)
            .await
            .expect("should get")
            .expect("should exist");

        assert_eq!(target_meta.table_name, target.to_string());
        assert_eq!(
            target_meta.partition_expressions,
            vec!["region".to_string()]
        );
        assert_eq!(target_meta.partitions.len(), 1);
    }
}
