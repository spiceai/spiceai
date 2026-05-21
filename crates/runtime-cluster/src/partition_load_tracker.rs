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
use datafusion::sql::TableReference;
use tokio::sync::RwLock;

use crate::context::PartitionExprResolver;
use crate::metadata::{TablePartitionMetadata, partition_value_to_bytes};

/// Identifier for an executor on the scheduler side (matches the executor's
/// advertise address).
pub type ExecutorId = String;

#[derive(Default, Debug)]
pub struct PartitionLoadTracker {
    /// (table, `executor_id`) -> set of partition expression bytes the
    /// executor has acked as loaded.
    loaded: RwLock<HashMap<TableReference, HashMap<ExecutorId, HashSet<Bytes>>>>,
}

impl PartitionLoadTracker {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the loaded-partition set for `(table, executor_id)` with the
    /// new snapshot from the executor. The snapshot is authoritative — a
    /// missing partition implies the executor is no longer holding it
    /// loaded.
    pub async fn replace(
        &self,
        table: TableReference,
        executor_id: ExecutorId,
        partition_expr_bytes: HashSet<Bytes>,
    ) {
        let mut guard = self.loaded.write().await;
        guard
            .entry(table)
            .or_default()
            .insert(executor_id, partition_expr_bytes);
    }

    /// Drops all loaded entries for `executor_id`. Called when the executor
    /// disconnects so a reassignment isn't masked by stale acks.
    pub async fn drop_executor(&self, executor_id: &str) {
        let mut guard = self.loaded.write().await;
        for table_map in guard.values_mut() {
            table_map.remove(executor_id);
        }
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
            let covered = assigned_executors
                .iter()
                .any(|exec| table_map.get(exec).is_some_and(|set| set.contains(bytes)));
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

    #[tokio::test]
    async fn drop_executor_clears_acks_across_tables() {
        let tracker = PartitionLoadTracker::new();
        let table_a = TableReference::parse_str("a");
        let table_b = TableReference::parse_str("b");

        let bytes = HashSet::from([Bytes::from_static(b"part-1")]);
        tracker
            .replace(table_a.clone(), "exec-1".to_string(), bytes.clone())
            .await;
        tracker
            .replace(table_b.clone(), "exec-1".to_string(), bytes)
            .await;
        tracker
            .replace(
                table_a.clone(),
                "exec-2".to_string(),
                HashSet::from([Bytes::from_static(b"part-2")]),
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
}
