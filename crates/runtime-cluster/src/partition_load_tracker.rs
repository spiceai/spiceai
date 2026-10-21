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

//! Aggregates `PartitionsLoaded` acks from executors on the scheduler side
//! so the scheduler can tell when an accelerated table is actually ready to
//! serve queries.
//!
//! Each entry says "for this (table, executor), the executor has finished
//! loading exactly these partition expression byte sequences." The bytes
//! match the encoding the scheduler used when sending `UpdatePartitions`
//! (`Expr::to_bytes()` over the AND-combined partition predicate produced
//! by [`crate::partition_value_to_bytes`]). Sorting the predicate's
//! key entries before serialization makes the bytes a stable identifier
//! independent of `HashMap` iteration order, which is what lets the
//! scheduler match acks against assigned partitions by byte equality.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use datafusion::common::TableReference;
use tokio::sync::RwLock;

use crate::context::PartitionExprResolver;
use crate::executor_registry::LoadedPartitions;
use crate::metadata::{
    PartitionKey, TablePartitionMetadata, partition_key, partition_value_to_bytes,
};

/// Identifier for an executor on the scheduler side (matches the executor's
/// advertise address).
pub type ExecutorId = String;

/// One executor process's report that it has loaded exactly `partitions` of a
/// table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadedAck {
    /// The process that sent the report; empty from executors that send none.
    pub process_id: String,
    /// Orders the process's reports for the table: a report with a lower
    /// sequence describes an older load. 0 from executors that send none.
    pub load_sequence: u64,
    /// The partition expression bytes of the loaded partitions.
    pub partitions: HashSet<Bytes>,
}

#[derive(Default, Debug)]
pub struct PartitionLoadTracker {
    /// (table, `executor_id`) -> the executor's latest report for the table.
    loaded: RwLock<HashMap<TableReference, HashMap<ExecutorId, LoadedAck>>>,
}

impl PartitionLoadTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the loaded-partition set for `(table, executor_id)` with the
    /// new snapshot from the executor. The snapshot is authoritative — a
    /// missing partition implies the executor is no longer holding it
    /// loaded. A report older than the one recorded from the same process is
    /// ignored; returns whether `ack` was recorded.
    pub async fn replace(
        &self,
        table: TableReference,
        executor_id: ExecutorId,
        ack: LoadedAck,
    ) -> bool {
        let mut guard = self.loaded.write().await;
        let acks = guard.entry(table).or_default();
        if let Some(recorded) = acks.get(&executor_id)
            && recorded.process_id == ack.process_id
            && ack.load_sequence != 0
            && ack.load_sequence < recorded.load_sequence
        {
            return false;
        }
        acks.insert(executor_id, ack);
        true
    }

    /// The latest report of each executor for `table`.
    pub async fn acks(&self, table: &TableReference) -> HashMap<ExecutorId, LoadedAck> {
        self.loaded
            .read()
            .await
            .get(table)
            .cloned()
            .unwrap_or_default()
    }

    /// For each executor that reported on `table`, the process that reported
    /// and the partitions of `metadata` it has loaded, by
    /// [`partition_key`](crate::metadata::partition_key).
    pub async fn loaded_partitions(
        &self,
        table: &TableReference,
        metadata: &TablePartitionMetadata,
        resolver: &dyn PartitionExprResolver,
    ) -> HashMap<ExecutorId, LoadedPartitions> {
        let mut keys: HashMap<Bytes, PartitionKey> =
            HashMap::with_capacity(metadata.partitions.len());
        for p in &metadata.partitions {
            match partition_value_to_bytes(p.partition_value.clone(), table, resolver).await {
                Ok(bytes) => {
                    keys.insert(bytes, partition_key(&p.partition_value));
                }
                Err(err) => {
                    tracing::debug!(
                        "Failed to encode partition bytes for coverage of {table}: {err}"
                    );
                }
            }
        }
        self.acks(table)
            .await
            .into_iter()
            .map(|(executor_id, ack)| {
                let partitions = ack
                    .partitions
                    .iter()
                    .filter_map(|bytes| keys.get(bytes).cloned())
                    .collect();
                (
                    executor_id,
                    LoadedPartitions {
                        process_id: ack.process_id,
                        partitions,
                    },
                )
            })
            .collect()
    }

    /// Drops all loaded entries for `executor_id`. Called when the executor
    /// disconnects so a reassignment isn't masked by stale acks.
    pub async fn drop_executor(&self, executor_id: &str) {
        let mut guard = self.loaded.write().await;
        for table_map in guard.values_mut() {
            table_map.remove(executor_id);
        }
    }

    /// Returns the tables that have at least one executor ack recorded.
    /// Used by the scheduler's periodic readiness sweep to re-evaluate acks
    /// that arrived before the table's partition metadata was seeded (e.g.
    /// acks replayed by executors that connected during scheduler startup).
    pub async fn acked_tables(&self) -> Vec<TableReference> {
        let guard = self.loaded.read().await;
        guard
            .iter()
            .filter(|(_, executors)| !executors.is_empty())
            .map(|(table, _)| table.clone())
            .collect()
    }

    /// Returns true when every partition in `metadata` has at least one
    /// of its `assigned_executors` reporting the partition's bytes as
    /// loaded. Returns false on any unassigned partition or encoding
    /// failure.
    ///
    /// A table whose source legitimately contains zero partitions is
    /// considered loaded once the metadata has been written at least
    /// once (`updated_at > 0`) — there is nothing to wait for. A table
    /// that has never been reconciled (`updated_at == 0` with no
    /// partitions) returns false; the next reconcile cycle will populate
    /// metadata.
    pub async fn is_table_loaded(
        &self,
        table: &TableReference,
        metadata: &TablePartitionMetadata,
        resolver: &dyn PartitionExprResolver,
    ) -> bool {
        if metadata.partitions.is_empty() {
            return metadata.updated_at > 0;
        }

        // Pre-compute the canonical partition bytes for every assigned
        // partition before acquiring the read lock. `partition_value_to_bytes`
        // can run user-provided expr parsing (slow / async), and holding
        // `self.loaded.read()` across that await would block writers
        // (`replace` / `drop_executor`) for the duration.
        let mut required: Vec<(&[String], bytes::Bytes)> =
            Vec::with_capacity(metadata.partitions.len());
        for p in &metadata.partitions {
            if p.assigned_executors.is_empty() {
                return false;
            }
            match partition_value_to_bytes(p.partition_value.clone(), table, resolver).await {
                Ok(b) => required.push((p.assigned_executors.as_slice(), b)),
                Err(err) => {
                    tracing::debug!(
                        "Failed to encode partition bytes for readiness check on {table}: {err}"
                    );
                    return false;
                }
            }
        }

        let guard = self.loaded.read().await;
        let Some(table_map) = guard.get(table) else {
            return false;
        };
        for (assigned_executors, bytes) in &required {
            let covered = assigned_executors.iter().any(|exec| {
                table_map
                    .get(exec)
                    .is_some_and(|ack| ack.partitions.contains(bytes))
            });
            if !covered {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use datafusion::error::DataFusionError;
    use datafusion_expr::{Expr, col};

    use crate::metadata::{PartitionMetadata, TablePartitionMetadata};

    /// Deterministic resolver for tests: returns the partition expression
    /// as a plain column reference so `partition_value_to_bytes` produces a
    /// stable byte sequence (`col(expr) = lit(value)`) without needing a
    /// real `DataFusion` table provider.
    struct ColumnResolver;

    #[async_trait]
    impl PartitionExprResolver for ColumnResolver {
        async fn try_parse_expr(
            &self,
            _tbl: &TableReference,
            expr: &str,
        ) -> Result<Expr, DataFusionError> {
            Ok(col(expr))
        }
    }

    /// Returns the bytes the executor would ack for the given partition value,
    /// matching the encoding scheduler-side `is_table_loaded` checks against.
    async fn ack_bytes(value: &[(&str, &str)], table: &TableReference) -> Bytes {
        let pv: crate::metadata::PartitionValue = value
            .iter()
            .map(|(k, v)| ((*k).to_string(), Some((*v).to_string())))
            .collect();
        crate::metadata::partition_value_to_bytes(pv, table, &ColumnResolver)
            .await
            .expect("partition_value_to_bytes")
    }

    fn loaded(partitions: HashSet<Bytes>) -> LoadedAck {
        LoadedAck {
            partitions,
            ..LoadedAck::default()
        }
    }

    fn partition(value: &[(&str, &str)], assigned_executors: Vec<&str>) -> PartitionMetadata {
        let pv: crate::metadata::PartitionValue = value
            .iter()
            .map(|(k, v)| ((*k).to_string(), Some((*v).to_string())))
            .collect();
        PartitionMetadata {
            partition_value: pv,
            assigned_executors: assigned_executors
                .into_iter()
                .map(ToString::to_string)
                .collect(),
            last_assigned_at: None,
        }
    }

    #[tokio::test]
    async fn drop_executor_clears_acks_across_tables() {
        let tracker = PartitionLoadTracker::new();
        let table_a = TableReference::parse_str("a");
        let table_b = TableReference::parse_str("b");

        let bytes = HashSet::from([Bytes::from_static(b"part-1")]);
        tracker
            .replace(table_a.clone(), "exec-1".to_string(), loaded(bytes.clone()))
            .await;
        tracker
            .replace(table_b.clone(), "exec-1".to_string(), loaded(bytes))
            .await;
        tracker
            .replace(
                table_a.clone(),
                "exec-2".to_string(),
                loaded(HashSet::from([Bytes::from_static(b"part-2")])),
            )
            .await;

        tracker.drop_executor("exec-1").await;

        let guard = tracker.loaded.read().await;
        let acks_a = guard
            .get(&table_a)
            .expect("table_a entry should exist after replace()");
        assert!(acks_a.get("exec-1").is_none());
        assert!(acks_a.get("exec-2").is_some());
        let acks_other = guard
            .get(&table_b)
            .expect("table_b entry should exist after replace()");
        assert!(acks_other.get("exec-1").is_none());
    }

    /// A report of an older load from the same process does not replace a
    /// newer one; a new process's report replaces the old process's.
    #[tokio::test]
    async fn an_older_load_report_does_not_replace_a_newer_one() {
        let tracker = PartitionLoadTracker::new();
        let table = TableReference::parse_str("t");
        let newer = LoadedAck {
            process_id: "p1".into(),
            load_sequence: 2,
            partitions: HashSet::from([Bytes::from_static(b"part-1")]),
        };
        let older = LoadedAck {
            process_id: "p1".into(),
            load_sequence: 1,
            partitions: HashSet::new(),
        };
        assert!(
            tracker
                .replace(table.clone(), "exec-1".into(), newer.clone())
                .await
        );
        assert!(!tracker.replace(table.clone(), "exec-1".into(), older).await);
        assert_eq!(tracker.acks(&table).await["exec-1"], newer);

        let restarted = LoadedAck {
            process_id: "p2".into(),
            load_sequence: 1,
            partitions: HashSet::new(),
        };
        assert!(
            tracker
                .replace(table.clone(), "exec-1".into(), restarted.clone())
                .await
        );
        assert_eq!(tracker.acks(&table).await["exec-1"], restarted);
    }

    #[tokio::test]
    async fn acked_tables_lists_tables_with_recorded_acks() {
        let tracker = PartitionLoadTracker::new();
        assert!(tracker.acked_tables().await.is_empty());

        let table = TableReference::parse_str("t");
        // An empty ack (zero-partition dataset) still counts as an ack.
        tracker
            .replace(table.clone(), "exec-1".to_string(), loaded(HashSet::new()))
            .await;
        assert_eq!(tracker.acked_tables().await, vec![table.clone()]);

        // Dropping the only acking executor leaves the table without acks.
        tracker.drop_executor("exec-1").await;
        assert!(tracker.acked_tables().await.is_empty());
    }

    #[tokio::test]
    async fn is_table_loaded_when_every_partition_is_covered_across_executors() {
        let tracker = PartitionLoadTracker::new();
        let table = TableReference::parse_str("t");

        let p1 = ack_bytes(&[("date", "2024-01-01")], &table).await;
        let p2 = ack_bytes(&[("date", "2024-01-02")], &table).await;

        // Each executor acks only its own assigned partition; coverage is
        // satisfied when *some* assigned executor reports it.
        tracker
            .replace(
                table.clone(),
                "exec-1".into(),
                loaded(HashSet::from([p1.clone()])),
            )
            .await;
        tracker
            .replace(
                table.clone(),
                "exec-2".into(),
                loaded(HashSet::from([p2.clone()])),
            )
            .await;

        let metadata = TablePartitionMetadata {
            table_name: "t".into(),
            partitions: vec![
                partition(&[("date", "2024-01-01")], vec!["exec-1", "exec-2"]),
                partition(&[("date", "2024-01-02")], vec!["exec-1", "exec-2"]),
            ],
            schema_version: 1,
            updated_at: 1,
            partition_expressions: vec!["date".into()],
            unregistered_at_ms: None,
        };

        assert!(
            tracker
                .is_table_loaded(&table, &metadata, &ColumnResolver)
                .await
        );
    }

    #[tokio::test]
    async fn is_table_loaded_returns_false_when_a_partition_is_uncovered() {
        let tracker = PartitionLoadTracker::new();
        let table = TableReference::parse_str("t");

        let p1 = ack_bytes(&[("date", "2024-01-01")], &table).await;

        // Only the first partition has an ack; the second is uncovered.
        tracker
            .replace(table.clone(), "exec-1".into(), loaded(HashSet::from([p1])))
            .await;

        let metadata = TablePartitionMetadata {
            table_name: "t".into(),
            partitions: vec![
                partition(&[("date", "2024-01-01")], vec!["exec-1"]),
                partition(&[("date", "2024-01-02")], vec!["exec-1"]),
            ],
            schema_version: 1,
            updated_at: 1,
            partition_expressions: vec!["date".into()],
            unregistered_at_ms: None,
        };

        assert!(
            !tracker
                .is_table_loaded(&table, &metadata, &ColumnResolver)
                .await
        );
    }

    #[tokio::test]
    async fn is_table_loaded_handles_empty_partitions_via_updated_at() {
        let tracker = PartitionLoadTracker::new();
        let table = TableReference::parse_str("t");

        // updated_at == 0: metadata never reconciled, can't be loaded yet.
        let unreconciled = TablePartitionMetadata {
            table_name: "t".into(),
            partitions: vec![],
            schema_version: 1,
            updated_at: 0,
            partition_expressions: vec![],
            unregistered_at_ms: None,
        };
        assert!(
            !tracker
                .is_table_loaded(&table, &unreconciled, &ColumnResolver)
                .await
        );

        // updated_at > 0 with zero partitions: legitimately empty source,
        // there is nothing to load.
        let reconciled = TablePartitionMetadata {
            updated_at: 1,
            ..unreconciled
        };
        assert!(
            tracker
                .is_table_loaded(&table, &reconciled, &ColumnResolver)
                .await
        );
    }
}
