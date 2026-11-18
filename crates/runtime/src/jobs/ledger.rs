/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Each caller's async query budget, shared by every scheduler of a cluster.
//!
//! A caller runs up to [`Limits::running`] queries at once, queues up to [`Limits::pending`]
//! more, and keeps up to [`Limits::stored_bytes`] of results. The counts live in one ledger
//! object per caller in the job store, changed only by conditional writes, so every scheduler
//! sees and updates the same counts without a coordination service. A ledger is a summary of the
//! caller's job states: the recovery sweep reconciles it with them, so a scheduler that stops
//! between writing a job and its ledger leaves nothing behind for long.

use std::collections::BTreeMap;

use object_store::UpdateVersion;
use serde::{Deserialize, Serialize};

/// The most bytes one async query may store: 100 GiB.
pub const MAX_JOB_RESULT_BYTES: u64 = 100 << 30;

/// Bytes a running query writes between updates of its ledger entry. The stored-results budget
/// can therefore lag by up to this much for each running query; the exact size is recorded when
/// the query finishes.
pub const WRITTEN_BYTES_BATCH: u64 = 1 << 30;

/// The per-caller limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Queries a caller may have running at once.
    pub running: usize,
    /// Queries a caller may have waiting to run.
    pub pending: usize,
    /// Result bytes a caller may keep: unexpired results plus what its running queries wrote.
    pub stored_bytes: u64,
}

impl Default for Limits {
    /// The product limits: 64 running, 1,000 pending, and 6.25 TiB (64 × 100 GiB) stored.
    fn default() -> Self {
        Self {
            running: 64,
            pending: 1_000,
            stored_bytes: 64 * MAX_JOB_RESULT_BYTES,
        }
    }
}

/// A caller's running and pending queries and stored results.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ledger {
    /// Queries holding a running slot, by job id.
    #[serde(default)]
    pub running: BTreeMap<String, RunningEntry>,
    /// Queries waiting for a slot, oldest first.
    #[serde(default)]
    pub pending: Vec<PendingEntry>,
    /// Results kept until they expire, by job id.
    #[serde(default)]
    pub stored: BTreeMap<String, StoredEntry>,
    /// The version read, for the conditional write.
    #[serde(skip)]
    pub version: Option<UpdateVersion>,
}

/// A query holding a running slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunningEntry {
    /// The scheduler that starts or drives it.
    pub node: String,
    /// Result bytes it has written, updated every [`WRITTEN_BYTES_BATCH`].
    #[serde(default)]
    pub written: u64,
}

/// A query waiting for a running slot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingEntry {
    pub job_id: String,
    /// The scheduler that accepted it, and the only one that can start it under its caller.
    pub node: String,
}

/// A result kept until it expires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredEntry {
    pub bytes: u64,
    pub expires_at_ms: u64,
}

/// How a submission was admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// It holds a running slot: start it now.
    Run,
    /// It waits in the caller's queue.
    Pending,
}

/// Why a submission was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The caller already has `pending` queries waiting.
    QueueFull { pending: usize },
    /// The caller's stored results are at its budget; `oldest_expiry_ms` is when its oldest
    /// result expires, if it has one.
    ResultBudget {
        stored_bytes: u64,
        oldest_expiry_ms: Option<u64>,
    },
}

impl Ledger {
    /// Drops results that expired by `now_ms`. Returns whether any was dropped.
    pub fn prune_expired(&mut self, now_ms: u64) -> bool {
        let before = self.stored.len();
        self.stored.retain(|_, entry| entry.expires_at_ms > now_ms);
        self.stored.len() != before
    }

    /// Unexpired result bytes plus what running queries have written.
    #[must_use]
    pub fn stored_bytes(&self) -> u64 {
        let stored = self
            .stored
            .values()
            .fold(0u64, |total, entry| total.saturating_add(entry.bytes));
        self.running
            .values()
            .fold(stored, |total, entry| total.saturating_add(entry.written))
    }

    /// Admits `job_id`, accepted by `node`: it runs when a running slot is free and nothing is
    /// queued ahead of it, waits when the queue has room, and is refused when the caller's
    /// stored results are at the budget or its queue is full.
    pub fn admit(
        &mut self,
        job_id: &str,
        node: &str,
        limits: Limits,
        now_ms: u64,
    ) -> Result<Admission, Refusal> {
        self.prune_expired(now_ms);
        let stored_bytes = self.stored_bytes();
        if stored_bytes >= limits.stored_bytes {
            return Err(Refusal::ResultBudget {
                stored_bytes,
                oldest_expiry_ms: self.stored.values().map(|e| e.expires_at_ms).min(),
            });
        }
        if self.running.len() < limits.running && self.pending.is_empty() {
            self.running.insert(
                job_id.to_string(),
                RunningEntry {
                    node: node.to_string(),
                    written: 0,
                },
            );
            return Ok(Admission::Run);
        }
        if self.pending.len() >= limits.pending {
            return Err(Refusal::QueueFull {
                pending: self.pending.len(),
            });
        }
        self.pending.push(PendingEntry {
            job_id: job_id.to_string(),
            node: node.to_string(),
        });
        Ok(Admission::Pending)
    }

    /// Releases `job_id`'s running slot or queue place, and keeps its result until it expires
    /// when it has one. Returns whether the ledger changed.
    pub fn finish(&mut self, job_id: &str, stored: Option<StoredEntry>) -> bool {
        let mut changed = self.running.remove(job_id).is_some();
        let pending = self.pending.len();
        self.pending.retain(|entry| entry.job_id != job_id);
        changed |= self.pending.len() != pending;
        if let Some(stored) = stored
            && self.stored.insert(job_id.to_string(), stored) != Some(stored)
        {
            changed = true;
        }
        changed
    }

    /// Moves the oldest queued queries accepted by `node` to running while slots are free,
    /// stopping at the first queued query another scheduler accepted, so the caller's queries
    /// start in submission order. Returns the job ids `node` must now start.
    pub fn promote(&mut self, node: &str, limits: Limits) -> Vec<String> {
        let mut promoted = Vec::new();
        while self.running.len() < limits.running
            && self.pending.first().is_some_and(|entry| entry.node == node)
        {
            let entry = self.pending.remove(0);
            self.running.insert(
                entry.job_id.clone(),
                RunningEntry {
                    node: entry.node,
                    written: 0,
                },
            );
            promoted.push(entry.job_id);
        }
        promoted
    }

    /// [`Ledger::finish`] and then [`Ledger::promote`] for `node`, in one write: returns whether
    /// the ledger changed and the job ids `node` must now start.
    pub fn finish_and_promote(
        &mut self,
        job_id: &str,
        stored: Option<StoredEntry>,
        node: &str,
        limits: Limits,
    ) -> (bool, Vec<String>) {
        let finished = self.finish(job_id, stored);
        let promoted = self.promote(node, limits);
        (finished || !promoted.is_empty(), promoted)
    }

    /// Records that `job_id`, running, has written `written` result bytes. Returns whether the
    /// ledger changed.
    pub fn record_written(&mut self, job_id: &str, written: u64) -> bool {
        match self.running.get_mut(job_id) {
            Some(entry) if entry.written != written => {
                entry.written = written;
                true
            }
            _ => false,
        }
    }

    /// Moves `job_id`'s running slot or queue place to `node`, which took it over. Returns
    /// whether the ledger changed.
    pub fn take_over(&mut self, job_id: &str, node: &str) -> bool {
        let mut changed = false;
        if let Some(entry) = self.running.get_mut(job_id)
            && entry.node != node
        {
            entry.node = node.to_string();
            changed = true;
        }
        for entry in &mut self.pending {
            if entry.job_id == job_id && entry.node != node {
                entry.node = node.to_string();
                changed = true;
            }
        }
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMITS: Limits = Limits {
        running: 2,
        pending: 2,
        stored_bytes: 100,
    };

    fn stored(bytes: u64, expires_at_ms: u64) -> StoredEntry {
        StoredEntry {
            bytes,
            expires_at_ms,
        }
    }

    /// A caller runs up to the running limit, then queues up to the queue limit, then is
    /// refused; finishing a query starts the oldest queued one of the scheduler that accepted it.
    #[test]
    fn runs_then_queues_then_refuses_and_starts_queued_queries_in_order() {
        let mut ledger = Ledger::default();
        assert_eq!(ledger.admit("a", "n1", LIMITS, 0), Ok(Admission::Run));
        assert_eq!(ledger.admit("b", "n1", LIMITS, 0), Ok(Admission::Run));
        assert_eq!(ledger.admit("c", "n2", LIMITS, 0), Ok(Admission::Pending));
        assert_eq!(ledger.admit("d", "n1", LIMITS, 0), Ok(Admission::Pending));
        assert_eq!(
            ledger.admit("e", "n1", LIMITS, 0),
            Err(Refusal::QueueFull { pending: 2 })
        );

        assert!(ledger.finish("a", None));
        // `c` is first and was accepted by n2: n1 must not start `d` ahead of it.
        assert_eq!(ledger.promote("n1", LIMITS), Vec::<String>::new());
        assert_eq!(ledger.promote("n2", LIMITS), vec!["c".to_string()]);
        assert!(ledger.finish("b", None));
        assert_eq!(ledger.promote("n1", LIMITS), vec!["d".to_string()]);
        // A new submission waits behind nothing and runs once a slot is free.
        assert_eq!(ledger.admit("e", "n1", LIMITS, 0), Ok(Admission::Pending));
    }

    /// Stored results, and what running queries wrote, count against the budget until they
    /// expire; a refusal names the oldest expiry.
    #[test]
    fn stored_results_count_until_they_expire() {
        let mut ledger = Ledger::default();
        assert_eq!(ledger.admit("a", "n1", LIMITS, 0), Ok(Admission::Run));
        assert!(ledger.record_written("a", 40));
        assert!(ledger.finish("a", Some(stored(60, 1_000))));
        assert_eq!(ledger.admit("b", "n1", LIMITS, 10), Ok(Admission::Run));
        assert!(ledger.record_written("b", 40));
        assert_eq!(ledger.stored_bytes(), 100);
        assert_eq!(
            ledger.admit("c", "n1", LIMITS, 10),
            Err(Refusal::ResultBudget {
                stored_bytes: 100,
                oldest_expiry_ms: Some(1_000),
            })
        );
        // Once `a` expires its 60 bytes are free again.
        assert_eq!(ledger.admit("c", "n1", LIMITS, 1_000), Ok(Admission::Run));
        assert_eq!(ledger.stored_bytes(), 40);
    }

    #[test]
    fn a_taken_over_query_keeps_its_place() {
        let mut ledger = Ledger::default();
        assert_eq!(ledger.admit("a", "n1", LIMITS, 0), Ok(Admission::Run));
        assert_eq!(ledger.admit("b", "n1", LIMITS, 0), Ok(Admission::Run));
        assert_eq!(ledger.admit("c", "n1", LIMITS, 0), Ok(Admission::Pending));
        assert!(ledger.take_over("c", "n2"));
        assert!(ledger.finish("a", None));
        assert_eq!(ledger.promote("n2", LIMITS), vec!["c".to_string()]);
    }

    #[test]
    fn product_limits() {
        let limits = Limits::default();
        assert_eq!(limits.running, 64);
        assert_eq!(limits.pending, 1_000);
        assert_eq!(limits.stored_bytes, 6_871_947_673_600); // 6.25 TiB
    }
}
