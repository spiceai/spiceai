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

use crate::metadata::{
    PartitionMetadata, PartitionValue, TablePartitionMetadata, normalized_table_name,
};

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
pub struct PartitionStore {
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

impl PartitionStore {
    /// Creates a new partition store with the given object store.
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
        let key = normalized_table_name(table);
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
        let key = normalized_table_name(table);
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
        let key = normalized_table_name(table);
        let now_ms = now_ms()?;
        let metadata = TablePartitionMetadata::new(table, now_ms, partition_expressions);

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
        let now_ms = now_ms()?;

        let mut metadata = self
            .get_table_metadata(table)
            .await?
            .unwrap_or_else(|| TablePartitionMetadata::new(table, now_ms, partition_expressions));

        metadata.partitions = partition_values
            .into_iter()
            .map(PartitionMetadata::new)
            .collect();
        metadata.updated_at = now_ms;

        self.write_metadata(table, metadata).await
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
        let key = normalized_table_name(table);
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

            match self.write_metadata(table, metadata).await {
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
        let key = normalized_table_name(table);
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

            match self.write_metadata(table, metadata).await {
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

        let key = normalized_table_name(table);
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

            match self.write_metadata(table, metadata).await {
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
        target_metadata.table_name = normalized_table_name(target_table);
        target_metadata.updated_at = now_ms;

        self.write_metadata(target_table, target_metadata).await?;

        if assigned_count == 0 {
            Ok(CopyAssignmentsResult::NoAssignments)
        } else {
            Ok(CopyAssignmentsResult::Copied {
                partition_count: assigned_count,
            })
        }
    }

    /// Write metadata using `insert_or_update` with conflict handling.
    pub async fn write_metadata(
        &self,
        table: &TableReference,
        metadata: TablePartitionMetadata,
    ) -> Result<()> {
        let key = normalized_table_name(table);
        match self
            .state
            .insert_or_update(&key, &metadata)
            .await
            .context(MetadataAccessSnafu { table: key.clone() })?
        {
            WriteResult::Inserted | WriteResult::Updated => Ok(()),
            WriteResult::Conflict { .. } => Err(Error::ConcurrentModification { table: key }),
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
    use runtime_datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

    fn in_memory_store() -> PartitionStore {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        PartitionStore::new(store)
    }

    fn test_store() -> PartitionStore {
        PartitionStore::new(Arc::new(InMemory::new())).with_prefix("test/")
    }

    fn table(name: &str) -> TableReference {
        TableReference::parse_str(name)
    }

    fn partition_value(key: &str, val: &str) -> PartitionValue {
        HashMap::from([(key.to_string(), val.to_string())])
    }

    /// Verifies that `add_and_assign_partitions` correctly adds new partitions
    /// and assigns them to executors in a single operation. The pre-refresh
    /// partition discovery path (`PartitionService::discover_and_assign_for_table`)
    /// goes through the service's discover → add → assign → notify flow rather
    /// than this helper; this test exercises the helper's direct add-and-assign
    /// behavior in isolation.
    #[tokio::test]
    async fn test_add_and_assign_new_partitions() {
        let pm = in_memory_store();
        let tbl = table("my_table");

        // Initialize empty metadata
        pm.initialize_metadata(&tbl, vec!["bucket(3, id)".to_string()])
            .await
            .expect("init");

        // Add and assign 3 new partitions to 2 executors
        let p0 = partition_value("bucket(3, id)", "0");
        let p1 = partition_value("bucket(3, id)", "1");
        let p2 = partition_value("bucket(3, id)", "2");

        let assignments = vec![
            (&p0, "executor-1"),
            (&p1, "executor-2"),
            (&p2, "executor-1"),
        ];

        pm.add_and_assign_partitions(&tbl, &assignments)
            .await
            .expect("add and assign");

        // Verify all partitions are present and correctly assigned
        let metadata = pm
            .get_table_metadata(&tbl)
            .await
            .expect("get metadata")
            .expect("should exist");

        assert_eq!(metadata.partitions.len(), 3);

        let find_partition = |val: &str| -> &PartitionMetadata {
            metadata
                .partitions
                .iter()
                .find(|p| p.partition_value.get("bucket(3, id)") == Some(&val.to_string()))
                .expect("partition not found")
        };

        let p0_meta = find_partition("0");
        assert!(p0_meta.is_assigned_to("executor-1"));
        assert!(!p0_meta.is_assigned_to("executor-2"));

        let p1_meta = find_partition("1");
        assert!(p1_meta.is_assigned_to("executor-2"));
        assert!(!p1_meta.is_assigned_to("executor-1"));

        let p2_meta = find_partition("2");
        assert!(p2_meta.is_assigned_to("executor-1"));
        assert!(!p2_meta.is_assigned_to("executor-2"));
    }

    /// Verifies that `add_and_assign_partitions` is idempotent when called with
    /// already-existing partitions that are already assigned.
    #[tokio::test]
    async fn test_add_and_assign_idempotent() {
        let pm = in_memory_store();
        let tbl = table("my_table");

        pm.initialize_metadata(&tbl, vec!["bucket(2, id)".to_string()])
            .await
            .expect("init");

        let p0 = partition_value("bucket(2, id)", "0");

        // First call: add and assign
        let assignments = vec![(&p0, "executor-1")];
        pm.add_and_assign_partitions(&tbl, &assignments)
            .await
            .expect("first call");

        // Second call: same partition, same executor - should be a no-op
        pm.add_and_assign_partitions(&tbl, &assignments)
            .await
            .expect("second call (idempotent)");

        let metadata = pm
            .get_table_metadata(&tbl)
            .await
            .expect("get metadata")
            .expect("should exist");

        assert_eq!(metadata.partitions.len(), 1);
        assert!(metadata.partitions[0].is_assigned_to("executor-1"));
        // Should only have one executor entry
        assert_eq!(metadata.partitions[0].assigned_executors.len(), 1);
    }

    /// Verifies that new partitions can be added alongside existing partitions.
    /// This simulates the scenario where a refresh discovers new partition values
    /// that were not present during the initial partition management cycle.
    #[tokio::test]
    async fn test_add_new_partitions_alongside_existing() {
        let pm = in_memory_store();
        let tbl = table("my_table");

        pm.initialize_metadata(&tbl, vec!["region".to_string()])
            .await
            .expect("init");

        // First: add the initial partition
        let p_east = partition_value("region", "us-east");
        pm.add_and_assign_partitions(&tbl, &[(&p_east, "executor-1")])
            .await
            .expect("initial assignment");

        // Verify: 1 partition assigned
        let metadata = pm
            .get_table_metadata(&tbl)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(metadata.partitions.len(), 1);

        // Now simulate what happens during a refresh: a NEW partition value is discovered
        let p_west = partition_value("region", "us-west");
        pm.add_and_assign_partitions(&tbl, &[(&p_west, "executor-2")])
            .await
            .expect("add new partition before refresh");

        // Verify: 2 partitions, each assigned to a different executor
        let metadata = pm
            .get_table_metadata(&tbl)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(metadata.partitions.len(), 2);

        let east = metadata
            .partitions
            .iter()
            .find(|p| p.partition_value.get("region") == Some(&"us-east".to_string()))
            .expect("us-east");
        assert!(east.is_assigned_to("executor-1"));

        let west = metadata
            .partitions
            .iter()
            .find(|p| p.partition_value.get("region") == Some(&"us-west".to_string()))
            .expect("us-west");
        assert!(west.is_assigned_to("executor-2"));
    }

    /// Verifies that `add_and_assign_partitions` with an empty assignments list
    /// is a no-op (does not error or modify metadata).
    #[tokio::test]
    async fn test_add_and_assign_empty_is_noop() {
        let pm = in_memory_store();
        let tbl = table("my_table");

        pm.initialize_metadata(&tbl, vec!["col".to_string()])
            .await
            .expect("init");

        let empty: Vec<(&PartitionValue, &str)> = vec![];
        pm.add_and_assign_partitions(&tbl, &empty)
            .await
            .expect("empty is ok");

        let metadata = pm
            .get_table_metadata(&tbl)
            .await
            .expect("get")
            .expect("exists");
        assert!(metadata.partitions.is_empty());
    }

    #[tokio::test]
    async fn copy_assignments_copies_metadata() {
        let pm = test_store();
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
        assert_eq!(result, CopyAssignmentsResult::Copied { partition_count: 1 });

        // Verify target has the same metadata
        let target_meta = pm
            .get_table_metadata(&target)
            .await
            .expect("should get")
            .expect("should exist");

        assert_eq!(target_meta.table_name, normalized_table_name(&target));
        assert_eq!(
            target_meta.partition_expressions,
            vec!["region".to_string()]
        );
        assert_eq!(target_meta.partitions.len(), 1);
        assert!(target_meta.partitions[0].is_assigned_to("executor-1"));
    }

    #[tokio::test]
    async fn copy_assignments_noop_when_source_missing() {
        let pm = test_store();
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
        let pm = test_store();
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

        assert_eq!(target_meta.table_name, normalized_table_name(&target));
        assert_eq!(
            target_meta.partition_expressions,
            vec!["region".to_string()]
        );
        assert_eq!(target_meta.partitions.len(), 1);
    }

    #[test]
    fn table_key_normalizes_bare_partial_full() {
        let bare = TableReference::bare("my_table");
        let partial = TableReference::partial(SPICE_DEFAULT_SCHEMA, "my_table");
        let full = TableReference::full(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA, "my_table");

        assert_eq!(
            normalized_table_name(&bare),
            normalized_table_name(&full),
            "bare and full should produce the same key"
        );
        assert_eq!(
            normalized_table_name(&partial),
            normalized_table_name(&full),
            "partial and full should produce the same key"
        );
        assert_eq!(
            normalized_table_name(&bare),
            normalized_table_name(&partial),
            "bare and partial should produce the same key"
        );
    }

    #[test]
    fn table_key_distinguishes_different_tables() {
        let a = TableReference::bare("table_a");
        let b = TableReference::bare("table_b");
        assert_ne!(normalized_table_name(&a), normalized_table_name(&b));
    }

    #[test]
    fn table_key_distinguishes_different_schemas() {
        let default_schema = TableReference::bare("my_table");
        let other_schema = TableReference::partial("other_schema", "my_table");
        assert_ne!(
            normalized_table_name(&default_schema),
            normalized_table_name(&other_schema)
        );
    }

    #[tokio::test]
    async fn fully_qualified_and_bare_resolve_to_same_partition() {
        let pm = test_store();
        let bare = TableReference::bare("my_table");
        let full = TableReference::full(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA, "my_table");

        // Initialize with bare name
        pm.initialize_metadata(&bare, vec!["org_id".to_string()])
            .await
            .expect("should initialize");

        // Set partitions using bare name
        let pv = HashMap::from([("org_id".to_string(), "test_org_name".to_string())]);
        pm.set_unassigned_partitions(&bare, vec![pv], vec![])
            .await
            .expect("should set partitions");

        // Look up using fully qualified name — should find the same metadata
        let meta_via_full = pm
            .get_table_metadata(&full)
            .await
            .expect("should get")
            .expect("fully qualified lookup should find metadata stored with bare name");

        assert_eq!(meta_via_full.partitions.len(), 1);

        // Assign using fully qualified name
        let partition_value: PartitionValue =
            HashMap::from([("org_id".to_string(), "test_org_name".to_string())]);
        pm.assign_partition(&full, &partition_value, "executor-1")
            .await
            .expect("should assign via fully qualified ref");

        // Verify assignment is visible via bare name
        let meta_via_bare = pm
            .get_table_metadata(&bare)
            .await
            .expect("should get")
            .expect("bare lookup should see assignment made with fully qualified name");

        assert!(meta_via_bare.partitions[0].is_assigned_to("executor-1"));
    }
}
