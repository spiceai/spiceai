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

//! The order in which executor processes first registered under each executor
//! id, kept beside the cluster document as `executor_processes.json`. Every
//! scheduler reads the same order, so they agree on which process under an id
//! is the latest, whatever the clocks of the hosts the processes started on read.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use object_store_occ::{Change, ConflictRetry, ModifyError, ObjectState};
use serde::{Deserialize, Serialize};

use crate::cluster_state::ClusterStateStore;

/// Processes remembered per executor id, so a replaced process that is still
/// running is recognized when it connects again.
const PROCESSES_KEPT: usize = 9;

/// How long an executor's entry outlives the last process that registered
/// under it, while no process under it is connected.
const ENTRY_RETENTION: Duration = Duration::from_hours(24);

const KEY: &str = "executor_processes";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct ProcessOrder {
    /// The generation the next new process gets. It never decreases, so a
    /// process that registers after its executor's entry was removed still
    /// sorts after every process seen before it.
    next_generation: u64,
    #[serde(default)]
    executors: HashMap<String, ExecutorEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecutorEntry {
    /// When the last new process registered, by the recording scheduler's
    /// clock (milliseconds since the Unix epoch). Used only to remove entries.
    admitted_at_ms: u64,
    /// The latest processes, oldest first.
    processes: Vec<AdmittedProcess>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AdmittedProcess {
    process_id: String,
    generation: u64,
}

/// Where a process stands in its executor's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Admission {
    /// Orders the process against the other processes under its executor id:
    /// a later process has a greater generation.
    pub generation: u64,
    /// Whether a later process has registered under the executor id.
    pub superseded: bool,
}

/// Records the order executor processes register in.
pub struct ExecutorProcessOrder {
    state: ObjectState<ProcessOrder>,
    retry: ConflictRetry,
}

impl std::fmt::Debug for ExecutorProcessOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorProcessOrder")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl ExecutorProcessOrder {
    /// The order stored beside `cluster`'s document.
    #[must_use]
    pub fn new(cluster: &ClusterStateStore) -> Self {
        let prefix = cluster
            .path()
            .as_ref()
            .rsplit_once('/')
            .map(|(dir, _)| format!("{dir}/"))
            .unwrap_or_default();
        Self {
            state: ObjectState::new(Arc::clone(cluster.object_store())).with_prefix(prefix),
            retry: ConflictRetry::default(),
        }
    }

    /// Returns where `process_id` stands under `executor_id`, recording it as
    /// the latest process when it registers for the first time.
    ///
    /// A new process also removes the entries of other executors whose last
    /// process registered more than [`ENTRY_RETENTION`] before `now_ms`,
    /// unless `is_connected` says a process under the id is connected.
    ///
    /// # Errors
    ///
    /// Returns an error if the order cannot be read or written.
    pub async fn admit(
        &self,
        executor_id: &str,
        process_id: &str,
        now_ms: u64,
        is_connected: impl Fn(&str) -> bool,
    ) -> Result<Admission, ModifyError<Infallible>> {
        let retention_ms = u64::try_from(ENTRY_RETENTION.as_millis()).unwrap_or(u64::MAX);
        self.state
            .modify(KEY, &self.retry, |order| {
                let mut order = order.unwrap_or_default();
                if let Some(entry) = order.executors.get(executor_id)
                    && let Some(position) = entry
                        .processes
                        .iter()
                        .position(|process| process.process_id == process_id)
                {
                    return Ok(Change::Keep(Admission {
                        generation: entry.processes[position].generation,
                        superseded: position + 1 < entry.processes.len(),
                    }));
                }
                order.executors.retain(|id, entry| {
                    id == executor_id
                        || is_connected(id)
                        || now_ms.saturating_sub(entry.admitted_at_ms) <= retention_ms
                });
                let generation = order.next_generation;
                order.next_generation += 1;
                let entry = order
                    .executors
                    .entry(executor_id.to_string())
                    .or_insert_with(|| ExecutorEntry {
                        admitted_at_ms: now_ms,
                        processes: Vec::new(),
                    });
                entry.admitted_at_ms = now_ms;
                entry.processes.push(AdmittedProcess {
                    process_id: process_id.to_string(),
                    generation,
                });
                if entry.processes.len() > PROCESSES_KEPT {
                    entry.processes.remove(0);
                }
                Ok(Change::Write(
                    order,
                    Admission {
                        generation,
                        superseded: false,
                    },
                ))
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use super::*;

    fn order_over(store: &Arc<dyn ObjectStore>) -> ExecutorProcessOrder {
        ExecutorProcessOrder::new(&ClusterStateStore::new(Arc::clone(store), "prefix"))
    }

    async fn admit(order: &ExecutorProcessOrder, executor_id: &str, process_id: &str) -> Admission {
        order
            .admit(executor_id, process_id, 1_000, |_| false)
            .await
            .expect("admit")
    }

    /// Processes are ordered by when they first register, and every scheduler
    /// reading the same state sees that order.
    #[tokio::test]
    async fn processes_are_ordered_by_first_registration() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first_scheduler = order_over(&store);
        let second_scheduler = order_over(&store);

        let later_clock = admit(&first_scheduler, "exec-a", "run-2").await;
        let earlier_clock = admit(&first_scheduler, "exec-a", "run-1").await;
        assert!(earlier_clock.generation > later_clock.generation);
        assert!(!earlier_clock.superseded);

        assert_eq!(
            admit(&second_scheduler, "exec-a", "run-2").await,
            Admission {
                generation: later_clock.generation,
                superseded: true,
            }
        );
        assert_eq!(
            admit(&second_scheduler, "exec-a", "run-1").await,
            earlier_clock
        );
    }

    /// Removing an idle executor's entry keeps the generations increasing, so a
    /// process that registers afterwards is still the latest one.
    #[tokio::test]
    async fn idle_entries_are_removed_without_reusing_generations() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let order = order_over(&store);
        let retention_ms = u64::try_from(ENTRY_RETENTION.as_millis()).expect("retention");

        let old = admit(&order, "exec-a", "run-1").await;
        admit(&order, "exec-b", "run-1").await;
        order
            .admit("exec-c", "run-1", 1_000 + retention_ms + 1, |id| {
                id == "exec-b"
            })
            .await
            .expect("admit");

        let stored = order.state.get(KEY).await.expect("read").expect("stored");
        let mut ids: Vec<_> = stored.executors.keys().cloned().collect();
        ids.sort();
        assert_eq!(ids, ["exec-b", "exec-c"]);

        let returning = admit(&order, "exec-a", "run-2").await;
        assert!(returning.generation > old.generation);
        assert!(!returning.superseded);
    }

    /// Only the latest processes are remembered.
    #[tokio::test]
    async fn only_the_latest_processes_are_kept() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let order = order_over(&store);
        for run in 0..=PROCESSES_KEPT {
            admit(&order, "exec-a", &format!("run-{run}")).await;
        }
        let stored = order.state.get(KEY).await.expect("read").expect("stored");
        let kept: Vec<_> = stored.executors["exec-a"]
            .processes
            .iter()
            .map(|process| process.process_id.as_str())
            .collect();
        assert_eq!(kept.len(), PROCESSES_KEPT);
        assert_eq!(kept.first(), Some(&"run-1"));
    }
}
