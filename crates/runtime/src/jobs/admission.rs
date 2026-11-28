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

//! Each caller's async query budget on this scheduler, kept in memory.
//!
//! On each scheduler a caller runs up to [`Limits::running`] queries at once, queues up to
//! [`Limits::pending`] more, which start in submission order, and keeps up to
//! [`Limits::stored_bytes`] of results written by that scheduler's queries. The budget belongs
//! to the scheduler: in a cluster of N schedulers a caller may hold up to N times each limit.
//!
//! Nothing here is written to the job store, so a submission makes the same writes it would
//! make without a budget. A scheduler that restarts starts with an empty budget, and a query it
//! takes over from a scheduler that stopped counts against it once taken over.

use std::collections::{BTreeMap, HashMap, VecDeque};

/// The most bytes one async query may store: 100 GiB.
pub const MAX_JOB_RESULT_BYTES: u64 = 100 << 30;

/// Stored results are counted by the minute they expire in, so a caller's budget holds at most
/// one entry per minute of the result TTL however many results it keeps. A result counts until
/// the end of its minute.
pub const STORED_BUCKET_MS: u64 = 60_000;

/// The per-caller limits on one scheduler.
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

/// A finished query's result, kept until it expires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// The caller's stored results are at its budget; `oldest_expiry_ms` is the end of the
    /// minute its oldest result expires in, if it has one.
    ResultBudget {
        stored_bytes: u64,
        oldest_expiry_ms: Option<u64>,
    },
}

/// One caller's queries and results on this scheduler.
#[derive(Debug, Default)]
struct Caller {
    /// Queries holding a running slot, with the result bytes each has written so far.
    running: HashMap<String, u64>,
    /// Queries waiting for a slot, oldest first.
    pending: VecDeque<String>,
    /// Bytes of results kept until they expire, by the end of the minute they expire in
    /// (milliseconds since the Unix epoch).
    stored: BTreeMap<u64, u64>,
}

impl Caller {
    fn prune_expired(&mut self, now_ms: u64) {
        self.stored.retain(|&ends_at_ms, _| ends_at_ms > now_ms);
    }

    /// Unexpired result bytes plus what running queries have written.
    fn stored_bytes(&self) -> u64 {
        let stored = self
            .stored
            .values()
            .fold(0u64, |total, bytes| total.saturating_add(*bytes));
        self.running
            .values()
            .fold(stored, |total, written| total.saturating_add(*written))
    }

    fn keep_stored(&mut self, stored: StoredEntry) {
        if stored.bytes == 0 {
            return;
        }
        let ends_at_ms = stored
            .expires_at_ms
            .div_ceil(STORED_BUCKET_MS)
            .saturating_mul(STORED_BUCKET_MS);
        let bytes = self.stored.entry(ends_at_ms).or_default();
        *bytes = bytes.saturating_add(stored.bytes);
    }

    /// Moves the oldest queued queries to running while slots are free.
    fn promote(&mut self, limits: Limits) -> Vec<String> {
        let mut promoted = Vec::new();
        while self.running.len() < limits.running {
            let Some(job_id) = self.pending.pop_front() else {
                break;
            };
            self.running.insert(job_id.clone(), 0);
            promoted.push(job_id);
        }
        promoted
    }

    fn is_empty(&self) -> bool {
        self.running.is_empty() && self.pending.is_empty() && self.stored.is_empty()
    }
}

/// Every caller's async query budget on this scheduler.
#[derive(Debug)]
pub struct Budgets {
    limits: Limits,
    callers: parking_lot::Mutex<HashMap<String, Caller>>,
}

impl Budgets {
    #[must_use]
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            callers: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    /// Admits `job_id` for `owner`: it runs when a running slot is free and nothing waits ahead
    /// of it, waits when the queue has room, and is refused when the caller's stored results are
    /// at the budget or its queue is full.
    pub fn admit(&self, owner: &str, job_id: &str, now_ms: u64) -> Result<Admission, Refusal> {
        let limits = self.limits;
        let mut callers = self.callers.lock();
        let caller = callers.entry(owner.to_string()).or_default();
        caller.prune_expired(now_ms);
        let stored_bytes = caller.stored_bytes();
        let refusal = if stored_bytes >= limits.stored_bytes {
            Refusal::ResultBudget {
                stored_bytes,
                oldest_expiry_ms: caller.stored.keys().next().copied(),
            }
        } else if caller.running.len() < limits.running && caller.pending.is_empty() {
            caller.running.insert(job_id.to_string(), 0);
            return Ok(Admission::Run);
        } else if caller.pending.len() >= limits.pending {
            Refusal::QueueFull {
                pending: caller.pending.len(),
            }
        } else {
            caller.pending.push_back(job_id.to_string());
            return Ok(Admission::Pending);
        };
        if caller.is_empty() {
            callers.remove(owner);
        }
        Err(refusal)
    }

    /// Releases `job_id`'s running slot or queue place and, when it held a running slot, keeps
    /// `stored` until it expires. A query this scheduler no longer holds is not counted again.
    /// Returns the queued queries that now hold a running slot, oldest first, for the caller to
    /// start.
    pub fn finish(&self, owner: &str, job_id: &str, stored: Option<StoredEntry>) -> Vec<String> {
        let limits = self.limits;
        let mut callers = self.callers.lock();
        let Some(caller) = callers.get_mut(owner) else {
            return Vec::new();
        };
        let ran = caller.running.remove(job_id).is_some();
        if !ran {
            caller.pending.retain(|queued| queued != job_id);
        }
        if ran && let Some(stored) = stored {
            caller.keep_stored(stored);
        }
        let promoted = caller.promote(limits);
        if caller.is_empty() {
            callers.remove(owner);
        }
        promoted
    }

    /// Records that `job_id`, running, has written `written` result bytes so far.
    pub fn record_written(&self, owner: &str, job_id: &str, written: u64) {
        if let Some(entry) = self
            .callers
            .lock()
            .get_mut(owner)
            .and_then(|caller| caller.running.get_mut(job_id))
        {
            *entry = written;
        }
    }

    /// Admits `job_id`, taken over from a scheduler that stopped, as a running query when a slot
    /// is free here; otherwise it stays where it is until a later sweep finds room. Returns
    /// whether it holds a slot.
    pub fn take_over_running(&self, owner: &str, job_id: &str) -> bool {
        let limits = self.limits;
        let mut callers = self.callers.lock();
        let caller = callers.entry(owner.to_string()).or_default();
        if caller.running.contains_key(job_id) {
            return true;
        }
        if caller.running.len() < limits.running && caller.pending.is_empty() {
            caller.running.insert(job_id.to_string(), 0);
            return true;
        }
        if caller.is_empty() {
            callers.remove(owner);
        }
        false
    }

    /// Admits `job_id`, a queued query taken over from a scheduler that stopped, behind this
    /// scheduler's own queued queries, or to run when a slot is free and nothing waits. `None`
    /// when the queue is full, so it stays where it is until a later sweep finds room.
    pub fn take_over_pending(&self, owner: &str, job_id: &str) -> Option<Admission> {
        let limits = self.limits;
        let mut callers = self.callers.lock();
        let caller = callers.entry(owner.to_string()).or_default();
        if caller.running.contains_key(job_id) {
            return Some(Admission::Run);
        }
        if caller.pending.iter().any(|queued| queued == job_id) {
            return Some(Admission::Pending);
        }
        if caller.running.len() < limits.running && caller.pending.is_empty() {
            caller.running.insert(job_id.to_string(), 0);
            return Some(Admission::Run);
        }
        if caller.pending.len() < limits.pending {
            caller.pending.push_back(job_id.to_string());
            return Some(Admission::Pending);
        }
        None
    }

    /// Whether this scheduler holds `job_id` as running or queued for `owner`.
    #[must_use]
    pub fn holds(&self, owner: &str, job_id: &str) -> bool {
        self.callers.lock().get(owner).is_some_and(|caller| {
            caller.running.contains_key(job_id) || caller.pending.iter().any(|q| q == job_id)
        })
    }

    /// How many queries `owner` has running and waiting on this scheduler.
    #[cfg(test)]
    #[must_use]
    pub fn counts(&self, owner: &str) -> (usize, usize) {
        self.callers.lock().get(owner).map_or((0, 0), |caller| {
            (caller.running.len(), caller.pending.len())
        })
    }

    /// Counts `stored` against `owner`'s budget until it expires, as a finished query's result.
    #[cfg(test)]
    pub fn keep_stored(&self, owner: &str, stored: StoredEntry) {
        self.callers
            .lock()
            .entry(owner.to_string())
            .or_default()
            .keep_stored(stored);
    }

    /// The bytes counted against `owner`'s budget: unexpired results plus what its running
    /// queries wrote.
    #[cfg(test)]
    #[must_use]
    pub fn stored_bytes(&self, owner: &str, now_ms: u64) -> u64 {
        let mut callers = self.callers.lock();
        callers.get_mut(owner).map_or(0, |caller| {
            caller.prune_expired(now_ms);
            caller.stored_bytes()
        })
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
    /// refused; finishing a query starts the oldest queued one.
    #[test]
    fn runs_then_queues_then_refuses_and_starts_queued_queries_in_order() {
        let budgets = Budgets::new(LIMITS);
        assert_eq!(budgets.admit("o", "a", 0), Ok(Admission::Run));
        assert_eq!(budgets.admit("o", "b", 0), Ok(Admission::Run));
        assert_eq!(budgets.admit("o", "c", 0), Ok(Admission::Pending));
        assert_eq!(budgets.admit("o", "d", 0), Ok(Admission::Pending));
        assert_eq!(
            budgets.admit("o", "e", 0),
            Err(Refusal::QueueFull { pending: 2 })
        );
        // Another caller is unaffected.
        assert_eq!(budgets.admit("other", "x", 0), Ok(Admission::Run));

        assert_eq!(budgets.finish("o", "a", None), vec!["c".to_string()]);
        assert_eq!(budgets.counts("o"), (2, 1));
        // A new submission waits behind the queued one rather than taking a slot first.
        assert_eq!(budgets.admit("o", "e", 0), Ok(Admission::Pending));
        assert_eq!(budgets.finish("o", "b", None), vec!["d".to_string()]);
        assert_eq!(budgets.finish("o", "c", None), vec!["e".to_string()]);
        assert_eq!(budgets.counts("o"), (2, 0));
    }

    /// Cancelling a queued query frees its place without starting anything.
    #[test]
    fn a_cancelled_queued_query_frees_its_place() {
        let budgets = Budgets::new(LIMITS);
        for job in ["a", "b", "c", "d"] {
            budgets.admit("o", job, 0).expect("admitted");
        }
        assert_eq!(budgets.finish("o", "c", None), Vec::<String>::new());
        assert_eq!(budgets.counts("o"), (2, 1));
        assert_eq!(budgets.admit("o", "e", 0), Ok(Admission::Pending));
        assert_eq!(budgets.finish("o", "a", None), vec!["d".to_string()]);
    }

    /// Stored results, and what running queries wrote, count against the budget until the end
    /// of the minute they expire in; a refusal names that time.
    #[test]
    fn stored_results_count_until_the_minute_they_expire_in_ends() {
        let budgets = Budgets::new(LIMITS);
        assert_eq!(budgets.admit("o", "a", 0), Ok(Admission::Run));
        budgets.record_written("o", "a", 40);
        assert_eq!(
            budgets.finish("o", "a", Some(stored(60, 61_000))),
            Vec::<String>::new()
        );
        assert_eq!(budgets.admit("o", "b", 10), Ok(Admission::Run));
        budgets.record_written("o", "b", 40);
        assert_eq!(budgets.stored_bytes("o", 10), 100);
        assert_eq!(
            budgets.admit("o", "c", 61_000),
            Err(Refusal::ResultBudget {
                stored_bytes: 100,
                oldest_expiry_ms: Some(120_000),
            })
        );
        // Once `a`'s minute has ended its 60 bytes are free again.
        assert_eq!(budgets.admit("o", "c", 120_000), Ok(Admission::Run));
        assert_eq!(budgets.stored_bytes("o", 120_000), 40);
    }

    /// However many results a caller keeps, its budget holds one entry for each minute they
    /// expire in; a query is counted once, and one the scheduler does not hold not at all.
    #[test]
    fn results_expiring_in_one_minute_share_one_entry() {
        let budgets = Budgets::new(Limits::default());
        for n in 0..1_000u64 {
            let job = format!("j{n}");
            assert_eq!(budgets.admit("o", &job, 0), Ok(Admission::Run));
            budgets.finish("o", &job, Some(stored(10, 60_001 + n)));
        }
        assert_eq!(
            budgets
                .callers
                .lock()
                .get("o")
                .map(|caller| caller.stored.clone()),
            Some(BTreeMap::from([(120_000, 10_000)]))
        );
        budgets.finish("o", "j0", Some(stored(10, 60_001)));
        budgets.finish("o", "unknown", Some(stored(10, 60_001)));
        assert_eq!(budgets.stored_bytes("o", 0), 10_000);
    }

    /// A query taken over from a scheduler that stopped takes a running slot only when one is
    /// free here, and a taken-over queued query waits behind this scheduler's own.
    #[test]
    fn taken_over_queries_count_here_only_within_the_limits() {
        let budgets = Budgets::new(LIMITS);
        assert!(budgets.take_over_running("o", "r1"));
        assert_eq!(budgets.admit("o", "a", 0), Ok(Admission::Run));
        assert!(!budgets.take_over_running("o", "r2"));
        assert_eq!(budgets.counts("o"), (2, 0));
        assert_eq!(budgets.admit("o", "b", 0), Ok(Admission::Pending));
        assert_eq!(
            budgets.take_over_pending("o", "q1"),
            Some(Admission::Pending)
        );
        assert_eq!(budgets.take_over_pending("o", "q2"), None);
        assert_eq!(budgets.finish("o", "r1", None), vec!["b".to_string()]);
        assert_eq!(budgets.finish("o", "a", None), vec!["q1".to_string()]);
    }

    /// A caller with nothing running, queued or stored leaves no entry behind.
    #[test]
    fn an_idle_caller_is_forgotten() {
        let budgets = Budgets::new(LIMITS);
        budgets.admit("o", "a", 0).expect("admitted");
        budgets.finish("o", "a", None);
        assert!(budgets.callers.lock().is_empty());
        assert_eq!(budgets.admit("o", "b", 0), Ok(Admission::Run));
        assert_eq!(
            budgets.finish("o", "b", Some(stored(5, 60_000))),
            Vec::<String>::new()
        );
        assert_eq!(
            budgets.callers.lock().len(),
            1,
            "stored results keep the caller"
        );
    }

    #[test]
    fn product_limits() {
        let limits = Limits::default();
        assert_eq!(limits.running, 64);
        assert_eq!(limits.pending, 1_000);
        assert_eq!(limits.stored_bytes, 6_871_947_673_600); // 6.25 TiB
    }
}
