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

//! Partition metadata wrappers backed by [`ClusterStateStore`].
//!
//! The cluster's accelerated and catalog/federated partition metadata
//! both live as submaps inside the single `cluster.json` document. This
//! module exposes a [`PartitionStore`] type parameterised by
//! [`PartitionScope`] so that call sites get a typed, scope-specific
//! handle without duplicating method bodies. Both type aliases point at
//! the same struct; the only difference is the scope passed at construction.

use std::sync::Arc;
use std::{collections::HashMap, time::SystemTime};

use datafusion::sql::TableReference;
use snafu::prelude::*;

use crate::cluster_state::{
    ClusterStateStore, MutateError, MutateOk, MutationOutcome, PartitionScope,
};
use crate::metadata::{
    PartitionMetadata, PartitionValue, TablePartitionMetadata, normalized_table_name,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to access partition metadata for table {table}: {source}"))]
    MetadataAccess { table: String, source: MutateError },

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

/// A single (table, partition, executor) assignment, used by the
/// batched [`PartitionStore::apply_assignments`] API.
#[derive(Debug, Clone)]
pub struct AssignmentRequest {
    pub table: TableReference,
    pub partition_value: PartitionValue,
    pub executor_id: String,
}

/// Partition metadata store bound to one [`PartitionScope`] of the
/// shared cluster document.
///
/// Construct via [`PartitionStore::accelerations`] or
/// [`PartitionStore::catalog`].
#[derive(Debug, Clone)]
pub struct PartitionStore {
    cluster: Arc<ClusterStateStore>,
    scope: PartitionScope,
}

/// Type alias used at scheduler/executor wiring sites for clarity.
pub type AccelerationsPartitions = PartitionStore;
/// Type alias used at scheduler/executor wiring sites for clarity.
pub type CatalogPartitions = PartitionStore;

impl PartitionStore {
    #[must_use]
    pub fn new(cluster: Arc<ClusterStateStore>, scope: PartitionScope) -> Self {
        Self { cluster, scope }
    }

    /// Convenience constructor for the acceleration scope.
    #[must_use]
    pub fn accelerations(cluster: Arc<ClusterStateStore>) -> Self {
        Self::new(cluster, PartitionScope::Acceleration)
    }

    /// Convenience constructor for the catalog/federated scope.
    #[must_use]
    pub fn catalog(cluster: Arc<ClusterStateStore>) -> Self {
        Self::new(cluster, PartitionScope::Catalog)
    }

    #[must_use]
    pub fn scope(&self) -> PartitionScope {
        self.scope
    }

    #[must_use]
    pub fn cluster_state(&self) -> &Arc<ClusterStateStore> {
        &self.cluster
    }

    /// Get partition metadata for a table from object store.
    ///
    /// # Errors
    ///
    /// Returns an error if the cluster state read fails.
    pub async fn get_table_metadata(
        &self,
        table: &TableReference,
    ) -> Result<Option<TablePartitionMetadata>> {
        let key = normalized_table_name(table);
        let snap = self
            .cluster
            .read()
            .await
            .context(MetadataAccessSnafu { table: key.clone() })?;
        Ok(self.scope.map(&snap).get(&key).cloned())
    }

    /// Get partition metadata from the most recent in-memory snapshot.
    /// Cheap; no IO; never returns stale-deletes (snapshot is whole-doc).
    #[must_use]
    pub fn get_cached_table_metadata(
        &self,
        table: &TableReference,
    ) -> Option<TablePartitionMetadata> {
        let key = normalized_table_name(table);
        let snap = self.cluster.read_cached()?;
        self.scope.map(&snap).get(&key).cloned()
    }

    /// Initialize partition metadata for a table with the given partition expression SQL strings.
    ///
    /// Returns `Ok(true)` if a new entry was created, `Ok(false)` if one already existed.
    ///
    /// # Errors
    ///
    /// Returns an error if the cluster state mutation fails.
    pub async fn initialize_metadata(
        &self,
        table: &TableReference,
        partition_expressions: Vec<String>,
    ) -> Result<bool> {
        if self.get_cached_table_metadata(table).is_some() {
            return Ok(false);
        }

        let key = normalized_table_name(table);
        let now_ms = now_ms()?;
        let metadata = TablePartitionMetadata::new(table, now_ms, partition_expressions);
        let scope = self.scope;
        let key_for_closure = key.clone();
        let metadata_for_closure = metadata.clone();
        let mut created = false;
        let res = self
            .cluster
            .mutate(|state| {
                let map = scope.map_mut(state);
                if map.contains_key(&key_for_closure) {
                    created = false;
                    MutationOutcome::NoChange
                } else {
                    map.insert(key_for_closure.clone(), metadata_for_closure.clone());
                    created = true;
                    MutationOutcome::Apply
                }
            })
            .await
            .context(MetadataAccessSnafu { table: key.clone() })?;
        match res {
            MutateOk::Committed => Ok(created),
            MutateOk::AlreadySatisfied => Ok(false),
        }
    }

    /// Update partition metadata with discovered partitions, all marked as unassigned.
    ///
    /// # Errors
    ///
    /// Returns an error if the cluster state mutation fails or encounters a concurrent modification.
    pub async fn set_unassigned_partitions(
        &self,
        table: &TableReference,
        partition_values: Vec<HashMap<String, Option<String>>>,
        partition_expressions: Vec<String>,
    ) -> Result<()> {
        let key = normalized_table_name(table);
        let scope = self.scope;
        let table_clone = table.clone();
        self.cluster
            .mutate(|state| {
                let now_ms = match crate::cluster_state::now_ms() {
                    Ok(v) => u128::from(v),
                    Err(e) => return MutationOutcome::Abort(e),
                };
                let map = scope.map_mut(state);
                let entry = map.entry(key.clone()).or_insert_with(|| {
                    TablePartitionMetadata::new(&table_clone, now_ms, partition_expressions.clone())
                });
                if entry.partition_expressions.is_empty() && !partition_expressions.is_empty() {
                    entry
                        .partition_expressions
                        .clone_from(&partition_expressions);
                }
                entry.partitions = partition_values
                    .iter()
                    .cloned()
                    .map(PartitionMetadata::new)
                    .collect();
                entry.updated_at = now_ms;
                MutationOutcome::Apply
            })
            .await
            .map_err(|e| match e {
                MutateError::ConcurrentModification { .. } => {
                    Error::ConcurrentModification { table: key }
                }
                other => Error::MetadataAccess {
                    table: key,
                    source: other,
                },
            })?;
        Ok(())
    }

    /// Allocates unassigned partitions to an executor.
    ///
    /// # Errors
    ///
    /// Returns an error if the table metadata is not found, or if the cluster state mutation fails.
    pub async fn allocate_partitions(
        &self,
        table: &TableReference,
        executor_id: &str,
        limit: usize,
    ) -> Result<AllocationResult> {
        let key = normalized_table_name(table);
        let scope = self.scope;

        let mut captured: Option<AllocationResult> = None;
        let executor = executor_id.to_string();
        let key_for_err = key.clone();
        let res = self
            .cluster
            .mutate(|state| {
                let now_ms = match crate::cluster_state::now_ms() {
                    Ok(v) => u128::from(v),
                    Err(e) => return MutationOutcome::Abort(e),
                };
                let map = scope.map_mut(state);
                let Some(metadata) = map.get_mut(&key) else {
                    return MutationOutcome::Abort(MutateError::Conflict {
                        message: format!("no partition metadata for table {key}"),
                    });
                };

                let previously_assigned: Vec<PartitionValue> = metadata
                    .partitions
                    .iter()
                    .filter(|p| p.is_assigned_to(&executor))
                    .map(|p| p.partition_value.clone())
                    .collect();
                let mut newly_assigned: Vec<PartitionValue> = Vec::new();
                let mut total = previously_assigned.len();
                let mut changes = false;
                for partition in &mut metadata.partitions {
                    if total >= limit {
                        break;
                    }
                    if !partition.is_assigned() {
                        partition.assign_to(executor.clone(), now_ms);
                        newly_assigned.push(partition.partition_value.clone());
                        total += 1;
                        changes = true;
                    }
                }

                let result = AllocationResult {
                    previously_assigned,
                    newly_assigned,
                };
                captured = Some(result);

                if changes {
                    metadata.updated_at = now_ms;
                    MutationOutcome::Apply
                } else {
                    MutationOutcome::NoChange
                }
            })
            .await
            .map_err(|e| match e {
                MutateError::ConcurrentModification { .. } => Error::ConcurrentModification {
                    table: key_for_err.clone(),
                },
                MutateError::Conflict { message } if message.contains("no partition metadata") => {
                    Error::TableMetadataNotFound {
                        table: key_for_err.clone(),
                    }
                }
                other => Error::MetadataAccess {
                    table: key_for_err.clone(),
                    source: other,
                },
            })?;

        let _ = res;
        captured.ok_or_else(|| Error::TableMetadataNotFound { table: key_for_err })
    }

    /// Assigns a single partition to an executor. Most callers should
    /// prefer [`Self::apply_assignments`] for batching.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition or table metadata is not found, or if the mutation fails.
    pub async fn assign_partition(
        &self,
        table: &TableReference,
        partition_value: &PartitionValue,
        executor_id: &str,
    ) -> Result<()> {
        let assignments = vec![AssignmentRequest {
            table: table.clone(),
            partition_value: partition_value.clone(),
            executor_id: executor_id.to_string(),
        }];
        let mut not_found: Option<(String, String)> = None;
        self.apply_assignments_inner(&assignments, &mut not_found)
            .await?;
        if let Some((tbl, part)) = not_found {
            return Err(Error::PartitionNotFound {
                table: tbl,
                partition: part,
            });
        }
        Ok(())
    }

    /// Atomically apply many `(table, partition, executor)` assignments
    /// in a single cluster-document write. Existing assignments to the
    /// same `(table, partition)` are overwritten with the new executor.
    /// Partition rows that don't yet exist are created so that callers
    /// can use this both for "first-time assignment" and "reassign".
    ///
    /// # Errors
    ///
    /// Returns an error if any table metadata is not found or if the cluster state mutation fails.
    pub async fn apply_assignments(&self, assignments: &[AssignmentRequest]) -> Result<()> {
        let mut not_found: Option<(String, String)> = None;
        self.apply_assignments_inner(assignments, &mut not_found)
            .await
    }

    async fn apply_assignments_inner(
        &self,
        assignments: &[AssignmentRequest],
        not_found_out: &mut Option<(String, String)>,
    ) -> Result<()> {
        if assignments.is_empty() {
            return Ok(());
        }
        let scope = self.scope;
        let assignments_owned: Vec<_> = assignments.to_vec();
        // Captured by the mutator on the failure path so the surfaced
        // error can name the exact table (and partition) that broke the
        // batch instead of guessing from `assignments[0]`.
        let mut missing_key: Option<(String, String)> = None;

        let res = self
            .cluster
            .mutate(|state| {
                let now_ms = match crate::cluster_state::now_ms() {
                    Ok(v) => u128::from(v),
                    Err(e) => return MutationOutcome::Abort(e),
                };
                let map = scope.map_mut(state);
                let mut changes = false;
                for assignment in &assignments_owned {
                    let key = normalized_table_name(&assignment.table);
                    let Some(metadata) = map.get_mut(&key) else {
                        missing_key =
                            Some((key.clone(), format!("{:?}", assignment.partition_value)));
                        return MutationOutcome::Abort(MutateError::Conflict {
                            message: format!("no partition metadata for table {key}"),
                        });
                    };
                    if let Some(partition) = metadata
                        .partitions
                        .iter_mut()
                        .find(|p| p.partition_value == assignment.partition_value)
                    {
                        if !partition.is_assigned_to(&assignment.executor_id) {
                            partition.assigned_executors.clear();
                            partition.assign_to(assignment.executor_id.clone(), now_ms);
                            changes = true;
                        }
                    } else {
                        let mut p = PartitionMetadata::new(assignment.partition_value.clone());
                        p.assign_to(assignment.executor_id.clone(), now_ms);
                        metadata.add_partition(p);
                        changes = true;
                    }
                    metadata.updated_at = now_ms;
                }
                if changes {
                    MutationOutcome::Apply
                } else {
                    MutationOutcome::NoChange
                }
            })
            .await;

        match res {
            Ok(_) => Ok(()),
            Err(MutateError::ConcurrentModification { .. }) => {
                let table = missing_key
                    .as_ref()
                    .map(|(t, _)| t.clone())
                    .or_else(|| assignments.first().map(|a| normalized_table_name(&a.table)))
                    .unwrap_or_default();
                Err(Error::ConcurrentModification { table })
            }
            Err(MutateError::Conflict { .. }) => {
                if let Some((tbl, part)) = missing_key {
                    *not_found_out = Some((tbl.clone(), part));
                    Err(Error::TableMetadataNotFound { table: tbl })
                } else {
                    Err(Error::MetadataAccess {
                        table: String::from("<batch>"),
                        source: MutateError::Conflict {
                            message: "unexpected conflict from mutator".to_string(),
                        },
                    })
                }
            }
            Err(other) => Err(Error::MetadataAccess {
                table: missing_key.map_or_else(|| String::from("<batch>"), |(t, _)| t),
                source: other,
            }),
        }
    }

    /// Adds new partitions to a table's metadata and assigns each to
    /// its respective executor in a single OCC write.
    ///
    /// # Errors
    ///
    /// Returns an error if the cluster state mutation fails (see [`Self::apply_assignments`]).
    pub async fn add_and_assign_partitions(
        &self,
        table: &TableReference,
        assignments: &[(&PartitionValue, &str)],
    ) -> Result<()> {
        if assignments.is_empty() {
            return Ok(());
        }
        let requests: Vec<AssignmentRequest> = assignments
            .iter()
            .map(|(pv, executor)| AssignmentRequest {
                table: table.clone(),
                partition_value: (*pv).clone(),
                executor_id: (*executor).to_string(),
            })
            .collect();
        self.apply_assignments(&requests).await
    }

    /// Replace this table's metadata wholesale (atomic OCC write). Used
    /// by callers that compute the new metadata externally and just need
    /// to persist it.
    ///
    /// # Errors
    ///
    /// Returns an error if the cluster state mutation fails.
    pub async fn write_metadata(
        &self,
        table: &TableReference,
        metadata: TablePartitionMetadata,
    ) -> Result<()> {
        let key = normalized_table_name(table);
        let scope = self.scope;
        let key_for_err = key.clone();
        self.cluster
            .mutate(|state| {
                let map = scope.map_mut(state);
                map.insert(key.clone(), metadata.clone());
                MutationOutcome::Apply
            })
            .await
            .map_err(|e| match e {
                MutateError::ConcurrentModification { .. } => {
                    Error::ConcurrentModification { table: key_for_err }
                }
                other => Error::MetadataAccess {
                    table: key_for_err,
                    source: other,
                },
            })?;
        Ok(())
    }

    /// List all tables (in this scope) with partition metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the cluster state read fails with an unexpected error.
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        match self.cluster.read().await {
            Ok(snap) => Ok(self.scope.map(&snap).keys().cloned().collect()),
            Err(MutateError::ClusterDocMissing { .. }) => Ok(Vec::new()),
            Err(other) => Err(Error::MetadataAccess {
                table: String::from("<list>"),
                source: other,
            }),
        }
    }

    /// Refresh the local cache from object store.
    ///
    /// # Errors
    ///
    /// Returns an error if the cluster state read fails with an unexpected error.
    pub async fn refresh(&self) -> Result<()> {
        match self.cluster.read().await {
            Ok(_) | Err(MutateError::ClusterDocMissing { .. }) => Ok(()),
            Err(other) => Err(Error::MetadataAccess {
                table: String::from("<refresh>"),
                source: other,
            }),
        }
    }

    /// Copy partition assignments from one table to another atomically
    /// (read + write happen inside a single OCC mutation).
    ///
    /// # Errors
    ///
    /// Returns an error if the cluster state mutation fails.
    pub async fn copy_assignments(
        &self,
        source_table: &TableReference,
        target_table: &TableReference,
    ) -> Result<CopyAssignmentsResult> {
        let scope = self.scope;
        let source_key = normalized_table_name(source_table);
        let target_key = normalized_table_name(target_table);

        let mut outcome: Option<CopyAssignmentsResult> = None;
        let res = self
            .cluster
            .mutate(|state| {
                let now_ms = match crate::cluster_state::now_ms() {
                    Ok(v) => u128::from(v),
                    Err(e) => return MutationOutcome::Abort(e),
                };
                let map = scope.map_mut(state);
                let Some(source_meta) = map.get(&source_key).cloned() else {
                    outcome = Some(CopyAssignmentsResult::NoSourceMetadata);
                    return MutationOutcome::NoChange;
                };
                let assigned_count = source_meta
                    .partitions
                    .iter()
                    .filter(|p| p.is_assigned())
                    .count();
                let mut target_meta = source_meta;
                target_meta.table_name.clone_from(&target_key);
                target_meta.updated_at = now_ms;
                map.insert(target_key.clone(), target_meta);
                outcome = Some(if assigned_count == 0 {
                    CopyAssignmentsResult::NoAssignments
                } else {
                    CopyAssignmentsResult::Copied {
                        partition_count: assigned_count,
                    }
                });
                MutationOutcome::Apply
            })
            .await
            .map_err(|e| match e {
                MutateError::ConcurrentModification { .. } => Error::ConcurrentModification {
                    table: target_key.clone(),
                },
                other => Error::MetadataAccess {
                    table: target_key.clone(),
                    source: other,
                },
            })?;
        let _ = res;
        Ok(outcome.unwrap_or(CopyAssignmentsResult::NoSourceMetadata))
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
    use crate::cluster_state::ClusterStateStore;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use runtime_datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

    async fn test_store() -> PartitionStore {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = Arc::new(ClusterStateStore::new(store, ""));
        cs.bootstrap().await.expect("bootstrap");
        PartitionStore::accelerations(cs)
    }

    fn table(name: &str) -> TableReference {
        TableReference::parse_str(name)
    }

    fn partition_value(key: &str, val: &str) -> PartitionValue {
        HashMap::from([(key.to_string(), Some(val.to_string()))])
    }

    #[tokio::test]
    async fn test_add_and_assign_new_partitions() {
        let pm = test_store().await;
        let tbl = table("my_table");

        pm.initialize_metadata(&tbl, vec!["bucket(3, id)".to_string()])
            .await
            .expect("init");

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
                .find(|p| p.partition_value.get("bucket(3, id)") == Some(&Some(val.to_string())))
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

    #[tokio::test]
    async fn test_add_and_assign_idempotent() {
        let pm = test_store().await;
        let tbl = table("my_table");

        pm.initialize_metadata(&tbl, vec!["bucket(2, id)".to_string()])
            .await
            .expect("init");

        let p0 = partition_value("bucket(2, id)", "0");

        let assignments = vec![(&p0, "executor-1")];
        pm.add_and_assign_partitions(&tbl, &assignments)
            .await
            .expect("first call");

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
        assert_eq!(metadata.partitions[0].assigned_executors.len(), 1);
    }

    #[tokio::test]
    async fn test_add_new_partitions_alongside_existing() {
        let pm = test_store().await;
        let tbl = table("my_table");

        pm.initialize_metadata(&tbl, vec!["region".to_string()])
            .await
            .expect("init");

        let p_east = partition_value("region", "us-east");
        pm.add_and_assign_partitions(&tbl, &[(&p_east, "executor-1")])
            .await
            .expect("initial assignment");

        let metadata = pm
            .get_table_metadata(&tbl)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(metadata.partitions.len(), 1);

        let p_west = partition_value("region", "us-west");
        pm.add_and_assign_partitions(&tbl, &[(&p_west, "executor-2")])
            .await
            .expect("add new partition before refresh");

        let metadata = pm
            .get_table_metadata(&tbl)
            .await
            .expect("get")
            .expect("exists");
        assert_eq!(metadata.partitions.len(), 2);

        let east = metadata
            .partitions
            .iter()
            .find(|p| p.partition_value.get("region") == Some(&Some("us-east".to_string())))
            .expect("us-east");
        assert!(east.is_assigned_to("executor-1"));

        let west = metadata
            .partitions
            .iter()
            .find(|p| p.partition_value.get("region") == Some(&Some("us-west".to_string())))
            .expect("us-west");
        assert!(west.is_assigned_to("executor-2"));
    }

    #[tokio::test]
    async fn test_add_and_assign_empty_is_noop() {
        let pm = test_store().await;
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
        let pm = test_store().await;
        let source = TableReference::parse_str("catalog.schema.source");
        let target = TableReference::parse_str("catalog.schema.target");

        pm.initialize_metadata(&source, vec!["region".to_string()])
            .await
            .expect("should initialize");

        let pv = HashMap::from([("region".to_string(), Some("us-east-1".to_string()))]);
        pm.set_unassigned_partitions(&source, vec![pv], vec![])
            .await
            .expect("should set partitions");
        let partition_value: PartitionValue =
            HashMap::from([("region".to_string(), Some("us-east-1".to_string()))]);
        pm.assign_partition(&source, &partition_value, "executor-1")
            .await
            .expect("should assign");

        let result = pm
            .copy_assignments(&source, &target)
            .await
            .expect("should copy");
        assert_eq!(result, CopyAssignmentsResult::Copied { partition_count: 1 });

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
        let pm = test_store().await;
        let source = TableReference::parse_str("catalog.schema.missing");
        let target = TableReference::parse_str("catalog.schema.target");

        let result = pm
            .copy_assignments(&source, &target)
            .await
            .expect("should be a no-op for missing source metadata");
        assert_eq!(result, CopyAssignmentsResult::NoSourceMetadata);

        let target_meta = pm.get_table_metadata(&target).await.expect("should get");
        assert!(target_meta.is_none());
    }

    #[tokio::test]
    async fn copy_assignments_overwrites_existing_target() {
        let pm = test_store().await;
        let source = TableReference::parse_str("catalog.schema.source");
        let target = TableReference::parse_str("catalog.schema.target");

        pm.initialize_metadata(&source, vec!["region".to_string()])
            .await
            .expect("should initialize source");
        pm.initialize_metadata(&target, vec!["old_expr".to_string()])
            .await
            .expect("should initialize target");

        let pv = HashMap::from([("region".to_string(), Some("eu-west-1".to_string()))]);
        pm.set_unassigned_partitions(&source, vec![pv], vec![])
            .await
            .expect("should set partitions");

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

        assert_eq!(normalized_table_name(&bare), normalized_table_name(&full));
        assert_eq!(
            normalized_table_name(&partial),
            normalized_table_name(&full)
        );
        assert_eq!(
            normalized_table_name(&bare),
            normalized_table_name(&partial)
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
        let pm = test_store().await;
        let bare = TableReference::bare("my_table");
        let full = TableReference::full(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA, "my_table");

        pm.initialize_metadata(&bare, vec!["org_id".to_string()])
            .await
            .expect("should initialize");

        let pv = HashMap::from([("org_id".to_string(), "test_org_name".to_string())]);
        pm.set_unassigned_partitions(&bare, vec![pv], vec![])
            .await
            .expect("should set partitions");

        let meta_via_full = pm
            .get_table_metadata(&full)
            .await
            .expect("should get")
            .expect("fully qualified lookup should find metadata stored with bare name");

        assert_eq!(meta_via_full.partitions.len(), 1);

        let partition_value: PartitionValue =
            HashMap::from([("org_id".to_string(), "test_org_name".to_string())]);
        pm.assign_partition(&full, &partition_value, "executor-1")
            .await
            .expect("should assign via fully qualified ref");

        let meta_via_bare = pm
            .get_table_metadata(&bare)
            .await
            .expect("should get")
            .expect("bare lookup should see assignment made with fully qualified name");

        assert!(meta_via_bare.partitions[0].is_assigned_to("executor-1"));
    }

    #[tokio::test]
    async fn acceleration_and_catalog_share_no_state() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cs = Arc::new(ClusterStateStore::new(store, ""));
        cs.bootstrap().await.expect("bootstrap");
        let acc = PartitionStore::accelerations(Arc::clone(&cs));
        let cat = PartitionStore::catalog(Arc::clone(&cs));
        let table = TableReference::bare("t");

        acc.initialize_metadata(&table, vec!["region".to_string()])
            .await
            .expect("init");
        let pv = HashMap::from([("region".to_string(), "us-east-1".to_string())]);
        acc.set_unassigned_partitions(&table, vec![pv.clone()], vec![])
            .await
            .expect("set");
        acc.assign_partition(&table, &pv, "exec-1")
            .await
            .expect("assign");

        // Catalog scope should not see the acceleration assignment.
        assert!(cat.get_table_metadata(&table).await.expect("get").is_none());
    }

    #[tokio::test]
    async fn apply_assignments_writes_once_for_many_partitions() {
        let pm = test_store().await;
        let table = table("t");
        pm.initialize_metadata(&table, vec!["region".to_string()])
            .await
            .expect("init");
        let values: Vec<HashMap<String, Option<String>>> = (0..50)
            .map(|i| HashMap::from([("region".to_string(), Some(format!("r-{i}")))]))
            .collect();
        pm.set_unassigned_partitions(&table, values.clone(), vec![])
            .await
            .expect("set");

        let requests: Vec<AssignmentRequest> = values
            .iter()
            .map(|pv| AssignmentRequest {
                table: table.clone(),
                partition_value: pv.clone(),
                executor_id: "exec-1".to_string(),
            })
            .collect();
        pm.apply_assignments(&requests).await.expect("apply");

        let meta = pm
            .get_table_metadata(&table)
            .await
            .expect("get")
            .expect("present");
        for p in meta.partitions {
            assert!(p.is_assigned_to("exec-1"));
        }
    }
}
