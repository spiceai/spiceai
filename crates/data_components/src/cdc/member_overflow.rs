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

//! Pump-local hold-back queues that stop one slow CDC member stalling its peers.
//!
//! A shared source (a Postgres replication slot, a `MySQL` binlog dump) decodes the
//! stream once and fans changes out to every member. Delivery used to be strictly
//! synchronous: a full member channel blocked the pump, which stopped it reading
//! the connection, so every member advanced at the slowest member's rate.
//!
//! Holding envelopes back per member lets the pump keep reading and serving the
//! other members. Two properties make it safe:
//!
//! * **Per-member order is preserved.** A member's channel is fed from exactly
//!   one place at a time — the caller's `try_send` fast path applies *only* while
//!   that member's queue is empty; once anything is held back, every later
//!   envelope for that member appends to the queue and reaches the channel
//!   through [`MemberOverflow::flush`], oldest first.
//! * **Memory is bounded.** The caller enforces a cap and falls back to a
//!   blocking send past it, so a member that never drains still back-pressures
//!   the source rather than growing the hold-back without limit.
//!
//! Holding an envelope is equivalent to having it in flight for ack purposes: the
//! caller marks it delivered *before* queueing, so the ack floor cannot advance
//! past an envelope that is merely held. Discarding held envelopes (on reconnect,
//! or when a member detaches) therefore cannot lose data — the floor still holds
//! that member's position and the replay re-delivers from it.

use std::collections::VecDeque;
use std::hash::Hash;

use rustc_hash::FxHashMap;
use tokio::sync::mpsc;

use super::{ChangeEnvelope, StreamError};

/// An envelope as it travels from the pump to a member's channel.
pub type MemberEnvelope = std::result::Result<ChangeEnvelope, StreamError>;

/// Outcome of pushing held-back envelopes toward a member's channel.
#[derive(Debug, PartialEq, Eq)]
pub enum FlushOutcome {
    /// The member's queue drained completely.
    Drained,
    /// The channel filled; the remainder stays queued.
    Full,
    /// The receiver is gone — the caller should detach the member.
    ReceiverGone,
}

/// Per-member hold-back queues, keyed by whatever identifies a member to its pump.
pub struct MemberOverflow<K: Eq + Hash + Clone> {
    queues: FxHashMap<K, VecDeque<MemberEnvelope>>,
    /// Held across all members, maintained alongside `queues` so the cap is O(1)
    /// to test on the hot path.
    total: usize,
}

impl<K: Eq + Hash + Clone> Default for MemberOverflow<K> {
    fn default() -> Self {
        Self {
            queues: FxHashMap::default(),
            total: 0,
        }
    }
}

impl<K: Eq + Hash + Clone> MemberOverflow<K> {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    #[must_use]
    pub fn total(&self) -> usize {
        self.total
    }

    /// How many envelopes are held for `key`. A non-zero value is what forces the
    /// caller off the `try_send` fast path, preserving per-member order.
    #[must_use]
    pub fn held_for(&self, key: &K) -> usize {
        self.queues.get(key).map_or(0, VecDeque::len)
    }

    /// Queue an envelope behind whatever is already held for `key`.
    pub fn push(&mut self, key: &K, envelope: MemberEnvelope) {
        self.queues
            .entry(key.clone())
            .or_default()
            .push_back(envelope);
        self.total = self.total.saturating_add(1);
    }

    /// The member holding the most — the one pinning the shared budget, and so
    /// the one worth draining first when the cap is reached.
    #[must_use]
    pub fn deepest(&self) -> Option<K> {
        self.queues
            .iter()
            .max_by_key(|(_, q)| q.len())
            .map(|(k, _)| k.clone())
    }

    /// Pop the oldest held envelope for `key`, for a caller that will deliver it
    /// with a blocking send.
    pub fn pop_front(&mut self, key: &K) -> Option<MemberEnvelope> {
        let queue = self.queues.get_mut(key)?;
        let item = queue.pop_front();
        if item.is_some() {
            self.total = self.total.saturating_sub(1);
        }
        if queue.is_empty() {
            self.queues.remove(key);
        }
        item
    }

    /// Drop everything held for `key` (it detached, or its receiver is gone).
    pub fn drop_member(&mut self, key: &K) {
        if let Some(q) = self.queues.remove(key) {
            self.total = self.total.saturating_sub(q.len());
        }
    }

    /// Drop everything. Used on reconnect: the replay restarts at the minimum
    /// committed position across members and re-delivers these commits, so held
    /// envelopes are stale rather than lost.
    pub fn clear(&mut self) {
        self.queues.clear();
        self.total = 0;
    }

    /// Every member currently holding envelopes.
    #[must_use]
    pub fn members(&self) -> Vec<K> {
        self.queues.keys().cloned().collect()
    }

    /// Non-blocking drain toward one member's channel, oldest first.
    pub fn flush_member(&mut self, key: &K, sender: &mpsc::Sender<MemberEnvelope>) -> FlushOutcome {
        let Some(queue) = self.queues.get_mut(key) else {
            return FlushOutcome::Drained;
        };
        let mut outcome = FlushOutcome::Drained;
        // `total` is decremented at POP, not at successful send, so the closed
        // case — where the popped envelope is discarded rather than delivered or
        // requeued — cannot leave a phantom count pinning the shared budget.
        while let Some(envelope) = queue.pop_front() {
            self.total = self.total.saturating_sub(1);
            match sender.try_send(envelope) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    queue.push_front(returned);
                    self.total = self.total.saturating_add(1);
                    outcome = FlushOutcome::Full;
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    outcome = FlushOutcome::ReceiverGone;
                    break;
                }
            }
        }
        if matches!(outcome, FlushOutcome::Drained | FlushOutcome::ReceiverGone) {
            let drained = self.queues.remove(key).map_or(0, |q| q.len());
            self.total = self.total.saturating_sub(drained);
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Distinguishable envelopes: the error payload carries an identity, so a
    /// test can assert the ORDER items arrive in — which identical ready-signal
    /// envelopes cannot express.
    fn tagged(n: usize) -> MemberEnvelope {
        Err(StreamError::External(format!("env-{n}")))
    }

    fn tag_of(e: &MemberEnvelope) -> String {
        match e {
            Err(StreamError::External(s)) => s.clone(),
            _ => "?".to_string(),
        }
    }

    #[tokio::test]
    async fn flush_delivers_oldest_first() {
        let (tx, mut rx) = mpsc::channel::<MemberEnvelope>(2);
        let mut o: MemberOverflow<&str> = MemberOverflow::default();
        for n in 0..4 {
            o.push(&"m", tagged(n));
        }

        // Only two slots: the two OLDEST must land, in order.
        assert_eq!(o.flush_member(&"m", &tx), FlushOutcome::Full);
        assert_eq!(tag_of(&rx.try_recv().expect("first")), "env-0");
        assert_eq!(tag_of(&rx.try_recv().expect("second")), "env-1");
        assert_eq!(o.held_for(&"m"), 2, "the newer two stay queued");

        // Freeing room delivers the remainder, still in order.
        assert_eq!(o.flush_member(&"m", &tx), FlushOutcome::Drained);
        assert_eq!(tag_of(&rx.try_recv().expect("third")), "env-2");
        assert_eq!(tag_of(&rx.try_recv().expect("fourth")), "env-3");
        assert!(o.is_empty());
    }

    #[tokio::test]
    async fn a_free_member_is_unaffected_by_a_blocked_peer() {
        let (slow_tx, _slow_rx) = mpsc::channel::<MemberEnvelope>(1);
        let (fast_tx, mut fast_rx) = mpsc::channel::<MemberEnvelope>(4);
        slow_tx.try_send(tagged(99)).expect("prefill slow");

        let mut o: MemberOverflow<&str> = MemberOverflow::default();
        o.push(&"slow", tagged(0));
        o.push(&"fast", tagged(1));

        assert_eq!(o.flush_member(&"slow", &slow_tx), FlushOutcome::Full);
        assert_eq!(o.flush_member(&"fast", &fast_tx), FlushOutcome::Drained);
        assert_eq!(o.held_for(&"slow"), 1, "blocked member keeps its backlog");
        assert_eq!(o.held_for(&"fast"), 0);
        assert_eq!(
            tag_of(&fast_rx.try_recv().expect("fast got its own")),
            "env-1"
        );
    }

    #[tokio::test]
    async fn a_closed_receiver_is_reported_and_its_backlog_dropped() {
        let (tx, rx) = mpsc::channel::<MemberEnvelope>(1);
        drop(rx);
        let mut o: MemberOverflow<&str> = MemberOverflow::default();
        o.push(&"gone", tagged(0));
        o.push(&"gone", tagged(1));
        assert_eq!(o.flush_member(&"gone", &tx), FlushOutcome::ReceiverGone);
        assert_eq!(o.held_for(&"gone"), 0);
        assert!(o.is_empty(), "a dead member must not pin the shared budget");
    }

    #[test]
    fn totals_track_pushes_pops_and_drops() {
        let mut o: MemberOverflow<&str> = MemberOverflow::default();
        o.push(&"a", tagged(0));
        o.push(&"a", tagged(1));
        o.push(&"b", tagged(2));
        assert_eq!(o.total(), 3);
        assert_eq!(o.deepest().expect("deepest"), "a");
        assert_eq!(tag_of(&o.pop_front(&"a").expect("pop")), "env-0");
        assert_eq!(o.total(), 2);
        o.drop_member(&"a");
        assert_eq!(o.total(), 1);
        o.clear();
        assert!(o.is_empty());
    }
}
