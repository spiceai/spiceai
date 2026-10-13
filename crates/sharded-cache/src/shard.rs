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

//! One shard of the cache: a `HashMap` plus intrusive region lists.
//!
//! # Policies
//!
//! - **LRU / LFU:** a single list (`probation`) holds every resident. Hits
//!   promote in place (non-destructive). LFU stores a per-entry frequency
//!   counter used at eviction time.
//! - **W-TinyLFU:** three lists — admission **window** (LRU), main
//!   **probation**, and main **protected** (SLRU). New keys enter the window;
//!   the window LRU competes with the probation LRU via the Count-Min Sketch
//!   before entering the main space. A hit on probation promotes into
//!   protected.
//!
//! # Expiry order
//!
//! Every resident is also threaded onto a second intrusive list, in the order
//! its TTL clock last started (`inserted_at`): appended on insert, moved to
//! the newest end when a replace restarts the clock, unlinked on removal. All
//! entries share one TTL and `inserted_at` is sampled under the shard lock, so
//! this list is sorted oldest-first and the expired entries are exactly a
//! prefix of it. Expiry pops that prefix ([`Shard::expire_older_than`]) and
//! never visits a live entry, rather than walking the recency lists, which
//! hits reorder and which therefore say nothing about age.
//!
//! The list only decides when an entry is *reclaimed*. Whether it is *served*
//! is still decided by its own `inserted_at` on every read, so even a clock
//! that stepped backwards would delay reclaiming an entry by that step, never
//! return an expired one.

use crate::EvictionPolicy;
use crate::hasher::IdentityBuildHasher;
use crate::sketch::CountMinSketch;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Which SLRU / window segment an entry currently occupies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Region {
    /// Admission window (W-TinyLFU only).
    Window,
    /// Main-space probation (also the sole list for LRU / LFU).
    Probation,
    /// Main-space protected (W-TinyLFU only).
    Protected,
}

/// When an entry's TTL clock started: nanoseconds since its shard's `epoch`.
///
/// Eight bytes where an `Instant` takes sixteen, which is what keeps a slot
/// to one 64-byte cache line now that it also carries the expiry-order links.
/// A hit reads one slot under the shard lock, so a slot that straddled two
/// lines would cost the contended hit path a second miss. Nanoseconds lose
/// nothing against `Instant` on the platforms Spice runs on, and `i64` spans
/// 292 years either side of the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Stamp(i64);

/// Whether `ttl` has run out for an entry stamped `at`, as of `now`:
/// `now - at >= ttl`, with a negative age read as zero, as
/// `Instant::saturating_duration_since` reads it. Every expiry decision —
/// serving a hit, admitting over a resident, reclaiming — goes through here.
fn ttl_elapsed(now: Stamp, at: Stamp, ttl: Duration) -> bool {
    let age = u128::try_from(now.0.saturating_sub(at.0)).unwrap_or(0);
    age >= ttl.as_nanos()
}

/// A node's link in the expiry-order list: an `Option<u32>` packed into four
/// bytes, with `u32::MAX` as `None`. No slot reaches that index — the cache
/// cannot hold `u32::MAX` entries.
///
/// The packing holds the list's cost to 8 bytes per entry rather than 16. The
/// recency links stay `Option<u32>`: they are relinked on every promoted hit,
/// and converting through the sentinel there measurably slowed that path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Link(u32);

impl Link {
    const NONE: Self = Self(u32::MAX);

    fn get(self) -> Option<u32> {
        (self != Self::NONE).then_some(self.0)
    }
}

impl From<Option<u32>> for Link {
    fn from(idx: Option<u32>) -> Self {
        idx.map_or(Self::NONE, Self)
    }
}

/// Oldest and newest ends of a shard's expiry-order list.
#[derive(Debug, Default, Clone, Copy)]
struct ExpiryEnds {
    oldest: Option<u32>,
    newest: Option<u32>,
}

#[derive(Debug, Default, Clone, Copy)]
struct ListEnds {
    head: Option<u32>,
    tail: Option<u32>,
}

pub(crate) struct Shard<V> {
    /// Origin of every [`Stamp`] in this shard.
    epoch: Instant,
    map: HashMap<u64, u32, IdentityBuildHasher>,
    slots: Vec<Slot<V>>,
    free: Vec<u32>,
    window: ListEnds,
    probation: ListEnds,
    protected: ListEnds,
    /// Every resident, oldest `inserted_at` first. See the module docs.
    expiry: ExpiryEnds,
    weight: u64,
    window_weight: u64,
    protected_weight: u64,
    sketch: Option<CountMinSketch>,
    policy: EvictionPolicy,
}

enum Slot<V> {
    Occupied(Node<V>),
    Vacant,
}

struct Node<V> {
    key: u64,
    /// Shared handle so a hit can `Arc::clone` under a short shard lock and
    /// clone the fat `V` only after unlock.
    value: Arc<V>,
    inserted_at: Stamp,
    weight: u64,
    region: Region,
    /// Saturating hit count for [`EvictionPolicy::Lfu`].
    freq: u16,
    prev: Option<u32>,
    next: Option<u32>,
    /// Neighbour in the expiry-order list with an older (or equal) `inserted_at`.
    older: Link,
    /// Neighbour in the expiry-order list with a newer (or equal) `inserted_at`.
    newer: Link,
}

pub(crate) fn into_owned<V: Clone>(value: Arc<V>) -> V {
    Arc::try_unwrap(value).unwrap_or_else(|shared| (*shared).clone())
}

pub(crate) enum GetOutcome<V> {
    /// Live hit: `Arc` handle cloned under the shard lock (no list surgery).
    Hit(Arc<V>),
    Miss,
    Expired {
        /// Still an `Arc` so expiry under the shard lock never deep-clones `V`.
        value: Arc<V>,
        weight: u64,
    },
}

#[derive(Clone, Copy, Default)]
pub(crate) struct WeightDelta {
    pub(crate) added: u64,
    pub(crate) removed: u64,
    pub(crate) window_added: u64,
    pub(crate) window_removed: u64,
    pub(crate) protected_added: u64,
    pub(crate) protected_removed: u64,
}

impl WeightDelta {
    pub(crate) fn net(self) -> i128 {
        i128::from(self.added) - i128::from(self.removed)
    }

    pub(crate) fn window_net(&self) -> i128 {
        i128::from(self.window_added) - i128::from(self.window_removed)
    }

    pub(crate) fn protected_net(&self) -> i128 {
        i128::from(self.protected_added) - i128::from(self.protected_removed)
    }
}

/// Result of [`Shard::replace_if`].
///
/// A declined replace hands `value` back rather than dropping it: the caller
/// holds the shard mutex (and the invalidate gate) across this call, and `V`
/// can own a whole query result, so its `Drop` belongs outside both.
pub(crate) enum ReplaceOutcome<V> {
    /// The resident was rewritten in place. `old` is the value it displaced.
    Replaced { delta: WeightDelta, old: Arc<V> },
    /// No resident, or the predicate rejected the one found.
    Declined { value: V },
}

impl<V> Shard<V> {
    pub(crate) fn new(policy: EvictionPolicy) -> Self {
        Self {
            epoch: Instant::now(),
            map: HashMap::with_hasher(IdentityBuildHasher),
            slots: Vec::new(),
            free: Vec::new(),
            window: ListEnds::default(),
            probation: ListEnds::default(),
            protected: ListEnds::default(),
            expiry: ExpiryEnds::default(),
            weight: 0,
            window_weight: 0,
            protected_weight: 0,
            sketch: matches!(policy, EvictionPolicy::TinyLfu).then(CountMinSketch::new),
            policy,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    /// `at` as a [`Stamp`] of this shard, saturating at the ends of `i64`.
    fn stamp(&self, at: Instant) -> Stamp {
        match at.checked_duration_since(self.epoch) {
            Some(since) => Stamp(i64::try_from(since.as_nanos()).unwrap_or(i64::MAX)),
            None => Stamp(
                i64::try_from(self.epoch.duration_since(at).as_nanos()).map_or(i64::MIN, |n| -n),
            ),
        }
    }

    pub(crate) fn contains(&self, key: u64) -> bool {
        self.map.contains_key(&key)
    }

    /// The resident at `key` when it is still inside `ttl`, otherwise `None`.
    ///
    /// An expired resident is absent here so a conditional insert can replace
    /// it. Expiry matches [`Self::get`]: `now - inserted_at >= ttl`.
    pub(crate) fn peek_live(&self, key: u64, now: Instant, ttl: Duration) -> Option<&V> {
        let &idx = self.map.get(&key)?;
        let now = self.stamp(now);
        match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) if !ttl_elapsed(now, node.inserted_at, ttl) => {
                Some(node.value.as_ref())
            }
            _ => None,
        }
    }

    pub(crate) fn window_weight(&self) -> u64 {
        self.window_weight
    }

    pub(crate) fn protected_weight(&self) -> u64 {
        self.protected_weight
    }

    #[expect(dead_code)]
    pub(crate) fn tail_key(&self) -> Option<u64> {
        self.region_tail_key(self.primary_region())
    }

    pub(crate) fn region_tail_key(&self, region: Region) -> Option<u64> {
        let idx = self.ends(region).tail?;
        match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => Some(node.key),
            _ => None,
        }
    }

    pub(crate) fn peek_weight(&self, key: u64) -> Option<u64> {
        let idx = *self.map.get(&key)?;
        match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => Some(node.weight),
            _ => None,
        }
    }

    pub(crate) fn peek_freq(&self, key: u64) -> Option<u16> {
        let idx = *self.map.get(&key)?;
        match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => Some(node.freq),
            _ => None,
        }
    }

    pub(crate) fn peek_region(&self, key: u64) -> Option<Region> {
        let idx = *self.map.get(&key)?;
        match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => Some(node.region),
            _ => None,
        }
    }

    pub(crate) fn peek_region_tail(&self, region: Region) -> Option<(u64, u64)> {
        let idx = self.ends(region).tail?;
        match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => Some((node.key, node.weight)),
            _ => None,
        }
    }

    /// Walk the entire probation list and return the lowest-frequency resident
    /// (true LFU within this shard). Ties keep the colder (closer-to-tail) key
    /// because the walk starts at the LRU end.
    /// `exclude` is the key admission is protecting from self-eviction, if any.
    pub(crate) fn peek_lfu_victim(&self, exclude: Option<u64>) -> Option<(u64, u64, u16)> {
        let mut best: Option<(u64, u64, u16)> = None;
        let mut cursor = self.probation.tail;
        while let Some(idx) = cursor {
            let Some(Slot::Occupied(node)) = self.slots.get(idx as usize) else {
                break;
            };
            if exclude == Some(node.key) {
                cursor = node.prev;
                continue;
            }
            let take = best.is_none_or(|(_, _, freq)| node.freq < freq);
            if take {
                best = Some((node.key, node.weight, node.freq));
            }
            cursor = node.prev;
        }
        best
    }

    pub(crate) fn tail_is_expired(&self, now: Instant, ttl: Duration) -> bool {
        self.region_tail_is_expired(self.primary_region(), now, ttl)
            || (matches!(self.policy, EvictionPolicy::TinyLfu)
                && (self.region_tail_is_expired(Region::Window, now, ttl)
                    || self.region_tail_is_expired(Region::Protected, now, ttl)))
    }

    fn region_tail_is_expired(&self, region: Region, now: Instant, ttl: Duration) -> bool {
        let Some(idx) = self.ends(region).tail else {
            return false;
        };
        match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => ttl_elapsed(self.stamp(now), node.inserted_at, ttl),
            _ => false,
        }
    }

    pub(crate) fn increment_sketch(&mut self, key: u64) {
        if let Some(sketch) = self.sketch.as_mut() {
            sketch.increment(key);
        }
    }

    pub(crate) fn sketch_estimate(&self, key: u64) -> u8 {
        self.sketch
            .as_ref()
            .map_or(0, |sketch| sketch.estimate(key))
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = u64> + '_ {
        self.map.keys().copied()
    }

    /// Keys in expiry order, oldest TTL origin first.
    #[cfg(test)]
    pub(crate) fn keys_oldest_first(&self) -> Vec<u64> {
        let mut keys = Vec::with_capacity(self.map.len());
        let mut cursor = self.expiry.oldest;
        while let Some(idx) = cursor {
            let Some(Slot::Occupied(node)) = self.slots.get(idx as usize) else {
                break;
            };
            keys.push(node.key);
            cursor = node.newer.get();
        }
        keys
    }

    /// Keys most-recently-used first. For W-TinyLFU: protected, then probation,
    /// then window (each list MRU→LRU).
    pub(crate) fn keys_mru_first(&self) -> Vec<u64> {
        let mut keys = Vec::with_capacity(self.map.len());
        match self.policy {
            EvictionPolicy::TinyLfu => {
                self.append_list_mru(&mut keys, Region::Protected);
                self.append_list_mru(&mut keys, Region::Probation);
                self.append_list_mru(&mut keys, Region::Window);
            }
            EvictionPolicy::Lru | EvictionPolicy::Lfu => {
                self.append_list_mru(&mut keys, Region::Probation);
            }
        }
        keys
    }

    fn append_list_mru(&self, keys: &mut Vec<u64>, region: Region) {
        let mut cursor = self.ends(region).head;
        while let Some(idx) = cursor {
            let Some(Slot::Occupied(node)) = self.slots.get(idx as usize) else {
                break;
            };
            keys.push(node.key);
            cursor = node.next;
        }
    }

    fn primary_region(&self) -> Region {
        let _ = self.policy;
        Region::Probation
    }

    fn ends(&self, region: Region) -> &ListEnds {
        match region {
            Region::Window => &self.window,
            Region::Probation => &self.probation,
            Region::Protected => &self.protected,
        }
    }

    fn ends_mut(&mut self, region: Region) -> &mut ListEnds {
        match region {
            Region::Window => &mut self.window,
            Region::Probation => &mut self.probation,
            Region::Protected => &mut self.protected,
        }
    }

    fn admit_region(&self) -> Region {
        match self.policy {
            EvictionPolicy::TinyLfu => Region::Window,
            EvictionPolicy::Lru | EvictionPolicy::Lfu => Region::Probation,
        }
    }
}

impl<V: Clone> Shard<V> {
    /// Test helper: move an entry's TTL origin `by` further into the past.
    ///
    /// Returns `false` when `key` is absent, or when the monotonic clock's
    /// origin is itself less than `by` old, so a caller asserts on the rewind
    /// rather than going on to test an entry that was never aged.
    #[cfg(test)]
    pub(crate) fn rewind_inserted_at(&mut self, key: u64, by: Duration) -> bool {
        let Some(&idx) = self.map.get(&key) else {
            return false;
        };
        let Some(Slot::Occupied(node)) = self.slots.get_mut(idx as usize) else {
            return false;
        };
        let Some(earlier) = i64::try_from(by.as_nanos())
            .ok()
            .and_then(|by| node.inserted_at.0.checked_sub(by))
            .map(Stamp)
        else {
            return false;
        };
        node.inserted_at = earlier;
        // Keep the expiry-order list sorted: move the entry behind the newest
        // resident that is still no younger than it.
        self.expiry_unlink(idx);
        let mut cursor = self.expiry.newest;
        while let Some(other) = cursor {
            match self.slots.get(other as usize) {
                Some(Slot::Occupied(node)) if node.inserted_at > earlier => {
                    cursor = node.older.get();
                }
                _ => break,
            }
        }
        self.expiry_link_after(idx, cursor);
        true
    }

    /// Short-lock hit path: look up, bump freq, `Arc::clone` the value handle.
    /// Does **not** relink LRU/LFU/W-TinyLFU lists — callers enqueue a touch and
    /// apply promotions via [`Self::apply_touch`] off the get latency path.
    pub(crate) fn get(&mut self, key: u64, now: Instant, ttl: Duration) -> GetOutcome<V> {
        let Some(&idx) = self.map.get(&key) else {
            return GetOutcome::Miss;
        };
        let expired = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => ttl_elapsed(self.stamp(now), node.inserted_at, ttl),
            _ => return GetOutcome::Miss,
        };
        if expired {
            let weight = match self.slots.get(idx as usize) {
                Some(Slot::Occupied(node)) => node.weight,
                _ => 0,
            };
            self.map.remove(&key);
            let Some(value) = self.take_value_and_free(idx) else {
                return GetOutcome::Miss;
            };
            return GetOutcome::Expired { value, weight };
        }
        let value = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => Arc::clone(&node.value),
            _ => return GetOutcome::Miss,
        };
        if let Some(Slot::Occupied(node)) = self.slots.get_mut(idx as usize) {
            node.freq = node.freq.saturating_add(1);
        }
        GetOutcome::Hit(value)
    }

    /// Apply a buffered hit: region relink / probation→protected (W-TinyLFU).
    /// No-op if `key` is gone. Does not bump `freq` (already counted on get).
    pub(crate) fn apply_touch(&mut self, key: u64) {
        let Some(&idx) = self.map.get(&key) else {
            return;
        };
        self.on_hit(idx);
    }

    pub(crate) fn insert(
        &mut self,
        key: u64,
        value: V,
        weight: u64,
        now: Instant,
    ) -> (WeightDelta, Option<Arc<V>>) {
        if let Some(&idx) = self.map.get(&key) {
            return self.replace(idx, value, weight, now);
        }
        let region = self.admit_region();
        let inserted_at = self.stamp(now);
        let idx = self.alloc(Node {
            key,
            value: Arc::new(value),
            inserted_at,
            weight,
            region,
            freq: 0,
            prev: None,
            next: None,
            older: Link::NONE,
            newer: Link::NONE,
        });
        self.map.insert(key, idx);
        self.push_front(idx, region);
        self.expiry_push_newest(idx);
        self.weight = self.weight.saturating_add(weight);
        let mut delta = WeightDelta {
            added: weight,
            removed: 0,
            window_added: 0,
            window_removed: 0,
            protected_added: 0,
            protected_removed: 0,
        };
        if region == Region::Window {
            self.window_weight = self.window_weight.saturating_add(weight);
            delta.window_added = weight;
        }
        (delta, None)
    }

    pub(crate) fn remove(&mut self, key: u64) -> Option<(Arc<V>, u64)> {
        let idx = self.map.remove(&key)?;
        let weight = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => node.weight,
            _ => 0,
        };
        let value = self.take_value_and_free(idx)?;
        Some((value, weight))
    }

    pub(crate) fn evict_region_lru(&mut self, region: Region) -> Option<(u64, Arc<V>, u64)> {
        let idx = self.ends(region).tail?;
        let (key, weight) = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => (node.key, node.weight),
            _ => return None,
        };
        self.map.remove(&key);
        let value = self.take_value_and_free(idx)?;
        Some((key, value, weight))
    }

    /// Move `key` from window → probation after a successful `TinyLFU` admit.
    /// Returns false if the key is gone or not in the window.
    pub(crate) fn move_window_to_probation(&mut self, key: u64) -> bool {
        let Some(&idx) = self.map.get(&key) else {
            return false;
        };
        let (weight, region) = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => (node.weight, node.region),
            _ => return false,
        };
        if region != Region::Window {
            return false;
        }
        self.unlink(idx);
        if let Some(Slot::Occupied(node)) = self.slots.get_mut(idx as usize) {
            node.region = Region::Probation;
        }
        self.window_weight = self.window_weight.saturating_sub(weight);
        self.push_front(idx, Region::Probation);
        true
    }

    /// Move a probation hit into protected. Caller must demote if over cap.
    #[expect(dead_code)]
    pub(crate) fn move_probation_to_protected(&mut self, key: u64) -> Option<u64> {
        let &idx = self.map.get(&key)?;
        let (weight, region) = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => (node.weight, node.region),
            _ => return None,
        };
        if region != Region::Probation {
            return None;
        }
        self.unlink(idx);
        if let Some(Slot::Occupied(node)) = self.slots.get_mut(idx as usize) {
            node.region = Region::Protected;
        }
        self.protected_weight = self.protected_weight.saturating_add(weight);
        self.push_front(idx, Region::Protected);
        Some(weight)
    }

    /// Demote protected LRU into probation MRU. Returns demoted weight.
    pub(crate) fn demote_protected_lru_to_probation(&mut self) -> Option<(u64, u64)> {
        let idx = self.protected.tail?;
        let (key, weight) = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => (node.key, node.weight),
            _ => return None,
        };
        self.unlink(idx);
        if let Some(Slot::Occupied(node)) = self.slots.get_mut(idx as usize) {
            node.region = Region::Probation;
        }
        self.protected_weight = self.protected_weight.saturating_sub(weight);
        self.push_front(idx, Region::Probation);
        Some((key, weight))
    }

    /// Drop matching entries without promoting survivors. Returns `(values, weight)`.
    pub(crate) fn invalidate_matching<F>(&mut self, predicate: F) -> (Vec<Arc<V>>, u64)
    where
        F: Fn(&V) -> bool,
    {
        let matched = self.collect_matching(|node| predicate(node.value.as_ref()));
        self.remove_indices(matched)
    }

    /// Remove every resident whose TTL has run out at `now`.
    ///
    /// Costs one step per expired entry plus one: the expired residents are
    /// the oldest prefix of the expiry-order list.
    pub(crate) fn expire_older_than(&mut self, now: Instant, ttl: Duration) -> (Vec<Arc<V>>, u64) {
        let (values, weight, _) = self.expire_older_than_at_most(now, ttl, usize::MAX);
        (values, weight)
    }

    /// [`Self::expire_older_than`], stopping after `limit` removals so a
    /// caller can release the shard lock between batches. The final `bool`
    /// is `true` when the limit was reached and expired residents may remain.
    ///
    /// Entries stamped after `now` are never removed, even under a zero TTL
    /// that counts them as expired: a caller that holds `now` fixed across
    /// batches then reclaims only what the shard held when it started, however
    /// fast the shard is refilled between batches.
    pub(crate) fn expire_older_than_at_most(
        &mut self,
        now: Instant,
        ttl: Duration,
        limit: usize,
    ) -> (Vec<Arc<V>>, u64, bool) {
        let now = self.stamp(now);
        let mut values = Vec::new();
        let mut weight: u64 = 0;
        while values.len() < limit {
            let Some(idx) = self.expiry.oldest else {
                return (values, weight, false);
            };
            let key = match self.slots.get(idx as usize) {
                Some(Slot::Occupied(node))
                    if node.inserted_at <= now && ttl_elapsed(now, node.inserted_at, ttl) =>
                {
                    node.key
                }
                _ => return (values, weight, false),
            };
            self.map.remove(&key);
            if let Some(Slot::Occupied(node)) = self.take_slot(idx) {
                weight = weight.saturating_add(node.weight);
                values.push(node.value);
            }
        }
        (values, weight, true)
    }

    pub(crate) fn take_all(&mut self) -> (Vec<Arc<V>>, u64) {
        let weight = self.weight;
        let mut values = Vec::with_capacity(self.map.len());
        self.map.clear();
        self.window = ListEnds::default();
        self.probation = ListEnds::default();
        self.protected = ListEnds::default();
        self.expiry = ExpiryEnds::default();
        self.weight = 0;
        self.window_weight = 0;
        self.protected_weight = 0;
        // Keep indices already in `free` (vacant slots) and add those that
        // were occupied, so a later insert reuses storage instead of
        // appending forever after churn + clear.
        for (i, slot) in self.slots.iter_mut().enumerate() {
            if let Slot::Occupied(node) = std::mem::replace(slot, Slot::Vacant) {
                values.push(node.value);
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "slot index fits in u32; the cache cannot hold u32::MAX entries"
                )]
                self.free.push(i as u32);
            }
        }
        (values, weight)
    }

    fn collect_matching<F>(&self, mut pred: F) -> Vec<(u64, u32)>
    where
        F: FnMut(&Node<V>) -> bool,
    {
        let mut matched = Vec::new();
        let regions = match self.policy {
            EvictionPolicy::TinyLfu => [Region::Window, Region::Probation, Region::Protected],
            EvictionPolicy::Lru | EvictionPolicy::Lfu => {
                [Region::Probation, Region::Probation, Region::Probation]
            }
        };
        let mut seen_probation = false;
        for region in regions {
            if matches!(self.policy, EvictionPolicy::Lru | EvictionPolicy::Lfu) {
                if seen_probation {
                    break;
                }
                seen_probation = true;
            }
            let mut cursor = self.ends(region).head;
            while let Some(idx) = cursor {
                let Some(Slot::Occupied(node)) = self.slots.get(idx as usize) else {
                    break;
                };
                let next = node.next;
                if pred(node) {
                    matched.push((node.key, idx));
                }
                cursor = next;
            }
        }
        matched
    }

    fn remove_indices(&mut self, matched: Vec<(u64, u32)>) -> (Vec<Arc<V>>, u64) {
        let mut values = Vec::with_capacity(matched.len());
        let mut weight: u64 = 0;
        for (key, idx) in matched {
            self.map.remove(&key);
            if let Some(Slot::Occupied(node)) = self.take_slot(idx) {
                weight = weight.saturating_add(node.weight);
                values.push(node.value);
            }
        }
        (values, weight)
    }

    fn on_hit(&mut self, idx: u32) {
        let region = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => node.region,
            _ => return,
        };
        match (self.policy, region) {
            (EvictionPolicy::TinyLfu, Region::Probation) => {
                // Promote into protected; demotion of protected LRU is handled
                // by the cache when the global protected cap is exceeded.
                let weight = match self.slots.get(idx as usize) {
                    Some(Slot::Occupied(node)) => node.weight,
                    _ => return,
                };
                self.unlink(idx);
                if let Some(Slot::Occupied(node)) = self.slots.get_mut(idx as usize) {
                    node.region = Region::Protected;
                }
                self.protected_weight = self.protected_weight.saturating_add(weight);
                self.push_front(idx, Region::Protected);
            }
            (_, region) => self.promote(idx, region),
        }
    }
}

impl<V: Clone> Shard<V> {
    fn replace(
        &mut self,
        idx: u32,
        value: V,
        weight: u64,
        now: Instant,
    ) -> (WeightDelta, Option<Arc<V>>) {
        let now = self.stamp(now);
        let (old_weight, old_value, region) = match self.slots.get_mut(idx as usize) {
            Some(Slot::Occupied(node)) => {
                let old_weight = node.weight;
                let old_value = std::mem::replace(&mut node.value, Arc::new(value));
                let region = node.region;
                node.weight = weight;
                node.inserted_at = now;
                (old_weight, old_value, region)
            }
            _ => {
                return (
                    WeightDelta {
                        added: 0,
                        removed: 0,
                        window_added: 0,
                        window_removed: 0,
                        protected_added: 0,
                        protected_removed: 0,
                    },
                    None,
                );
            }
        };
        self.weight = self
            .weight
            .saturating_sub(old_weight)
            .saturating_add(weight);
        let mut delta = WeightDelta {
            added: weight,
            removed: old_weight,
            window_added: 0,
            window_removed: 0,
            protected_added: 0,
            protected_removed: 0,
        };
        if region == Region::Window {
            self.window_weight = self
                .window_weight
                .saturating_sub(old_weight)
                .saturating_add(weight);
            delta.window_added = weight;
            delta.window_removed = old_weight;
        } else if region == Region::Protected {
            self.protected_weight = self
                .protected_weight
                .saturating_sub(old_weight)
                .saturating_add(weight);
            delta.protected_added = weight;
            delta.protected_removed = old_weight;
        }
        self.promote(idx, region);
        // The clock restarted at `now`, the newest time this shard has seen.
        self.expiry_unlink(idx);
        self.expiry_push_newest(idx);
        (delta, Some(old_value))
    }

    /// Conditionally rewrite a resident. Returns `None` if `key` is absent,
    /// `Some((false, ..))` if the predicate rejected, or `Some((true, delta, old))`
    /// when the value was replaced. When `keep_ttl` is set, `inserted_at` is left
    /// unchanged so the remaining lifetime is preserved.
    pub(crate) fn replace_if<F>(
        &mut self,
        key: u64,
        value: V,
        weight: u64,
        now: Instant,
        keep_ttl: bool,
        should_replace: F,
    ) -> ReplaceOutcome<V>
    where
        F: FnOnce(&V) -> bool,
    {
        let Some(&idx) = self.map.get(&key) else {
            return ReplaceOutcome::Declined { value };
        };
        let accept = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => should_replace(node.value.as_ref()),
            _ => return ReplaceOutcome::Declined { value },
        };
        if !accept {
            return ReplaceOutcome::Declined { value };
        }
        let now = self.stamp(now);
        let (old_weight, old_value, region) = match self.slots.get_mut(idx as usize) {
            Some(Slot::Occupied(node)) => {
                let old_weight = node.weight;
                let old_value = std::mem::replace(&mut node.value, Arc::new(value));
                let region = node.region;
                node.weight = weight;
                if !keep_ttl {
                    node.inserted_at = now;
                }
                (old_weight, old_value, region)
            }
            _ => return ReplaceOutcome::Declined { value },
        };
        self.weight = self
            .weight
            .saturating_sub(old_weight)
            .saturating_add(weight);
        let mut delta = WeightDelta {
            added: weight,
            removed: old_weight,
            window_added: 0,
            window_removed: 0,
            protected_added: 0,
            protected_removed: 0,
        };
        if region == Region::Window {
            self.window_weight = self
                .window_weight
                .saturating_sub(old_weight)
                .saturating_add(weight);
            delta.window_added = weight;
            delta.window_removed = old_weight;
        } else if region == Region::Protected {
            self.protected_weight = self
                .protected_weight
                .saturating_sub(old_weight)
                .saturating_add(weight);
            delta.protected_added = weight;
            delta.protected_removed = old_weight;
        }
        // In-place rewrite: do not bump recency (promotion would restart LRU order).
        if !keep_ttl {
            // The clock restarted at `now`, the newest time this shard has seen.
            self.expiry_unlink(idx);
            self.expiry_push_newest(idx);
        }
        ReplaceOutcome::Replaced {
            delta,
            old: old_value,
        }
    }

    fn promote(&mut self, idx: u32, region: Region) {
        if self.ends(region).head == Some(idx) {
            return;
        }
        self.unlink(idx);
        self.push_front(idx, region);
    }

    fn push_front(&mut self, idx: u32, region: Region) {
        let head = self.ends(region).head;
        if let Some(Slot::Occupied(node)) = self.slots.get_mut(idx as usize) {
            node.prev = None;
            node.next = head;
            node.region = region;
        }
        if let Some(head_idx) = head
            && let Some(Slot::Occupied(node)) = self.slots.get_mut(head_idx as usize)
        {
            node.prev = Some(idx);
        }
        let ends = self.ends_mut(region);
        ends.head = Some(idx);
        if ends.tail.is_none() {
            ends.tail = Some(idx);
        }
    }

    fn unlink(&mut self, idx: u32) {
        let (prev, next, region) = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => (node.prev, node.next, node.region),
            _ => return,
        };
        if let Some(prev_idx) = prev
            && let Some(Slot::Occupied(node)) = self.slots.get_mut(prev_idx as usize)
        {
            node.next = next;
        } else {
            self.ends_mut(region).head = next;
        }
        if let Some(next_idx) = next
            && let Some(Slot::Occupied(node)) = self.slots.get_mut(next_idx as usize)
        {
            node.prev = prev;
        } else {
            self.ends_mut(region).tail = prev;
        }
        if let Some(Slot::Occupied(node)) = self.slots.get_mut(idx as usize) {
            node.prev = None;
            node.next = None;
        }
    }

    /// Append `idx` at the newest end of the expiry-order list. Callers
    /// guarantee its `inserted_at` is no older than the current newest
    /// resident's: both are sampled under this shard's lock, and `Instant` is
    /// monotonic.
    fn expiry_push_newest(&mut self, idx: u32) {
        self.expiry_link_after(idx, self.expiry.newest);
    }

    /// Link the unlinked `idx` directly after `after` in the expiry-order
    /// list, or at the oldest end when `after` is `None`.
    fn expiry_link_after(&mut self, idx: u32, after: Option<u32>) {
        let newer = match after {
            None => self.expiry.oldest,
            Some(after) => match self.slots.get(after as usize) {
                Some(Slot::Occupied(node)) => node.newer.get(),
                _ => return,
            },
        };
        if let Some(Slot::Occupied(node)) = self.slots.get_mut(idx as usize) {
            node.older = after.into();
            node.newer = newer.into();
        }
        match after.and_then(|i| self.slots.get_mut(i as usize)) {
            Some(Slot::Occupied(node)) => node.newer = Link(idx),
            _ => self.expiry.oldest = Some(idx),
        }
        match newer.and_then(|i| self.slots.get_mut(i as usize)) {
            Some(Slot::Occupied(node)) => node.older = Link(idx),
            _ => self.expiry.newest = Some(idx),
        }
    }

    fn expiry_unlink(&mut self, idx: u32) {
        let (older, newer) = match self.slots.get_mut(idx as usize) {
            Some(Slot::Occupied(node)) => (
                std::mem::replace(&mut node.older, Link::NONE).get(),
                std::mem::replace(&mut node.newer, Link::NONE).get(),
            ),
            _ => return,
        };
        match older.and_then(|i| self.slots.get_mut(i as usize)) {
            Some(Slot::Occupied(node)) => node.newer = newer.into(),
            _ => self.expiry.oldest = newer,
        }
        match newer.and_then(|i| self.slots.get_mut(i as usize)) {
            Some(Slot::Occupied(node)) => node.older = older.into(),
            _ => self.expiry.newest = older,
        }
    }

    fn alloc(&mut self, node: Node<V>) -> u32 {
        if let Some(idx) = self.free.pop() {
            self.slots[idx as usize] = Slot::Occupied(node);
            idx
        } else {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "slot index fits in u32; the cache cannot hold u32::MAX entries"
            )]
            let idx = self.slots.len() as u32;
            self.slots.push(Slot::Occupied(node));
            idx
        }
    }

    fn take_value_and_free(&mut self, idx: u32) -> Option<Arc<V>> {
        match self.take_slot(idx)? {
            // Keep the Arc until the caller releases the shard lock — cloning
            // a large V under the mutex stalls concurrent hits on this shard.
            Slot::Occupied(node) => Some(node.value),
            Slot::Vacant => None,
        }
    }

    fn take_slot(&mut self, idx: u32) -> Option<Slot<V>> {
        self.unlink(idx);
        self.expiry_unlink(idx);
        let slot = self.slots.get_mut(idx as usize)?;
        let taken = std::mem::replace(slot, Slot::Vacant);
        if let Slot::Occupied(ref node) = taken {
            self.weight = self.weight.saturating_sub(node.weight);
            match node.region {
                Region::Window => {
                    self.window_weight = self.window_weight.saturating_sub(node.weight);
                }
                Region::Protected => {
                    self.protected_weight = self.protected_weight.saturating_sub(node.weight);
                }
                Region::Probation => {}
            }
            self.free.push(idx);
        }
        Some(taken)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EvictionPolicy;
    use std::time::Duration;

    fn shard() -> Shard<u32> {
        Shard::new(EvictionPolicy::Lru)
    }

    fn tinylfu_shard() -> Shard<u32> {
        Shard::new(EvictionPolicy::TinyLfu)
    }

    #[test]
    fn get_promotes_without_dropping_the_entry() {
        let mut shard = shard();
        let now = Instant::now();
        shard.insert(1, 10, 1, now);
        shard.insert(2, 20, 1, now);
        assert_eq!(shard.keys_mru_first(), vec![2, 1]);

        let got = shard.get(1, now, Duration::from_mins(1));
        assert!(matches!(got, GetOutcome::Hit(ref v) if **v == 10));
        assert_eq!(shard.len(), 2, "a hit must not remove the entry");
        // Promote is buffered: apply explicitly (cache drains off the get path).
        shard.apply_touch(1);
        assert_eq!(shard.keys_mru_first(), vec![1, 2]);
    }

    #[test]
    fn tail_is_expired_uses_insert_time() {
        let mut shard = shard();
        let inserted = Instant::now();
        shard.insert(1, 10, 5, inserted);
        assert!(!shard.tail_is_expired(inserted, Duration::from_secs(1)));
        assert!(shard.tail_is_expired(inserted + Duration::from_secs(2), Duration::from_secs(1)));
        assert_eq!(shard.peek_weight(1), Some(5));
        assert_eq!(shard.peek_region_tail(Region::Probation), Some((1, 5)));
    }

    #[test]
    fn expired_get_removes_the_entry() {
        let mut shard = shard();
        let inserted = Instant::now();
        shard.insert(1, 10, 5, inserted);
        let later = inserted + Duration::from_secs(2);
        let got = shard.get(1, later, Duration::from_secs(1));
        assert!(matches!(got, GetOutcome::Expired { weight: 5, .. }));
        assert_eq!(shard.len(), 0);
        assert_eq!(shard.weight, 0);
    }

    #[test]
    fn take_all_reuses_vacant_and_occupied_slots() {
        let mut shard = shard();
        let now = Instant::now();
        for i in 0..100u64 {
            shard.insert(i, u32::try_from(i).expect("fits u32"), 1, now);
        }
        for i in 0..50u64 {
            shard.remove(i);
        }
        assert_eq!(shard.slots.len(), 100);
        assert_eq!(shard.free.len(), 50);

        let (_values, weight) = shard.take_all();
        assert_eq!(weight, 50);
        assert_eq!(shard.slots.len(), 100);
        assert_eq!(
            shard.free.len(),
            100,
            "vacant indices plus newly vacated occupied ones must stay reusable"
        );

        for i in 0..100u64 {
            shard.insert(i + 1_000, 1, 1, now);
        }
        assert_eq!(
            shard.slots.len(),
            100,
            "a refill after churn + clear must not append new slots"
        );
        assert!(shard.free.is_empty());
        assert_eq!(shard.len(), 100);
    }

    #[test]
    fn tinylfu_inserts_into_the_window() {
        let mut shard = tinylfu_shard();
        let now = Instant::now();
        shard.insert(1, 10, 5, now);
        assert_eq!(shard.peek_region(1), Some(Region::Window));
        assert_eq!(shard.window_weight(), 5);
        assert!(shard.move_window_to_probation(1));
        assert_eq!(shard.peek_region(1), Some(Region::Probation));
        assert_eq!(shard.window_weight(), 0);
    }

    #[test]
    fn tinylfu_probation_hit_promotes_to_protected() {
        let mut shard = tinylfu_shard();
        let now = Instant::now();
        shard.insert(1, 10, 5, now);
        assert!(shard.move_window_to_probation(1));
        let got = shard.get(1, now, Duration::from_mins(1));
        assert!(matches!(got, GetOutcome::Hit(ref v) if **v == 10));
        assert_eq!(
            shard.peek_region(1),
            Some(Region::Probation),
            "get must not relink; touch apply promotes"
        );
        shard.apply_touch(1);
        assert_eq!(shard.peek_region(1), Some(Region::Protected));
        assert_eq!(shard.protected_weight(), 5);
    }

    #[test]
    fn peek_lfu_victim_finds_cold_mru_not_just_tail() {
        let mut shard = Shard::new(crate::EvictionPolicy::Lfu);
        let now = Instant::now();
        // Insert 20 residents. Leave the MRU (key 19) at freq 0 and bump every
        // older key so a tail-only sample of 16 would miss the true coldest.
        for i in 0..20u64 {
            shard.insert(i, i, 1, now);
        }
        for key in 0..19u64 {
            let _ = shard.get(key, now, Duration::from_mins(1));
            shard.apply_touch(key);
        }
        // key 19 is MRU with freq 0; keys 0..18 were touched (freq >= 1) and
        // promoted toward MRU, so the LRU tail is a freq-1 key.
        let victim = shard.peek_lfu_victim(None).expect("victim");
        assert_eq!(
            victim.0, 19,
            "full LFU scan must find the cold MRU, not a hotter LRU-tail sample"
        );
        assert_eq!(victim.2, 0);
    }

    /// A slot is one cache line on 64-bit targets: a contended hit reads one
    /// slot under the shard lock, and a slot that straddled two lines measurably
    /// lengthened the tail of hits on other keys of that shard.
    #[test]
    fn a_slot_fits_one_cache_line() {
        assert_eq!(std::mem::size_of::<Slot<u32>>(), 64);
        assert_eq!(std::mem::size_of::<Slot<String>>(), 64);
    }

    /// `ttl_elapsed` over stamps must decide exactly what the `Instant`
    /// arithmetic it replaced decides: `now.saturating_duration_since(at) >= ttl`.
    #[test]
    fn ttl_elapsed_agrees_with_instant_arithmetic() {
        let shard = shard();
        let epoch = shard.epoch;
        let ns = Duration::from_nanos;
        let mut rng = Rng(7);
        let mut instants = vec![epoch, epoch + ns(1), epoch + Duration::from_hours(48)];
        // Before the epoch too, where the platform's clock allows it.
        instants.extend(epoch.checked_sub(Duration::from_secs(5)));
        for _ in 0..200 {
            instants.push(epoch + ns(rng.below(10_000_000_000)));
            if let Some(before) = epoch.checked_sub(ns(rng.below(1_000_000_000))) {
                instants.push(before);
            }
        }
        let ttls = [
            Duration::ZERO,
            ns(1),
            Duration::from_secs(1),
            Duration::from_mins(48 * 60 + 1),
            Duration::from_secs(u64::MAX),
            Duration::MAX,
        ];
        let mut checked = 0;
        for &at in &instants {
            for &now in &instants {
                // Exact boundaries: the age equal to the TTL, and one nanosecond short.
                let age = now.saturating_duration_since(at);
                let boundary = [age, age.saturating_sub(ns(1)), age + ns(1)];
                for ttl in ttls.iter().copied().chain(boundary) {
                    assert_eq!(
                        ttl_elapsed(shard.stamp(now), shard.stamp(at), ttl),
                        now.saturating_duration_since(at) >= ttl,
                        "at {at:?}, now {now:?}, ttl {ttl:?}"
                    );
                    checked += 1;
                }
            }
        }
        assert!(checked > 100_000, "only {checked} cases ran");
    }

    #[test]
    fn expiry_pops_only_the_expired_prefix_of_insert_order() {
        let mut shard = shard();
        let t0 = Instant::now();
        let ttl = Duration::from_secs(10);
        for key in 0..6u64 {
            shard.insert(key, 0, 1, t0 + Duration::from_secs(key));
        }
        // Hits reorder recency but must not reorder expiry.
        for key in [0, 1, 2] {
            let _ = shard.get(key, t0, ttl);
            shard.apply_touch(key);
        }
        // Restarting key 1's clock moves it behind key 5.
        shard.insert(1, 0, 1, t0 + Duration::from_secs(6));
        // A kept TTL leaves key 2 where it was.
        assert!(matches!(
            shard.replace_if(2, 0, 1, t0 + Duration::from_secs(7), true, |_| true),
            ReplaceOutcome::Replaced { .. }
        ));
        assert_eq!(shard.keys_oldest_first(), vec![0, 2, 3, 4, 5, 1]);

        // At t0+13s, entries inserted at or before t0+3s are expired.
        let at = t0 + Duration::from_secs(13);
        let (values, weight, more) = shard.expire_older_than_at_most(at, ttl, 2);
        assert_eq!((values.len(), weight, more), (2, 2, true));
        assert_eq!(shard.keys_oldest_first(), vec![3, 4, 5, 1]);
        let (values, weight, more) = shard.expire_older_than_at_most(at, ttl, 2);
        assert_eq!((values.len(), weight, more), (1, 1, false));
        assert_eq!(shard.keys_oldest_first(), vec![4, 5, 1]);
        let mut left: Vec<u64> = shard.keys().collect();
        left.sort_unstable();
        assert_eq!(left, vec![1, 4, 5]);
    }

    /// Expiry order, checked against two oracles over seeded random operation
    /// histories: a plain model of each resident's TTL origin, and the full
    /// recency-list walk expiry used before it had its own list.
    #[test]
    fn expiry_order_matches_a_model_over_random_histories() {
        for seed in 0..64u64 {
            for policy in [
                EvictionPolicy::Lru,
                EvictionPolicy::Lfu,
                EvictionPolicy::TinyLfu,
            ] {
                run_expiry_history(seed, policy);
            }
        }
    }

    /// splitmix64, so every history is reproducible from its seed.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one operation per match arm keeps the history readable"
    )]
    fn run_expiry_history(seed: u64, policy: EvictionPolicy) {
        use std::collections::BTreeMap;

        let ctx = format!("seed {seed}, policy {policy:?}");
        let mut rng = Rng(seed);
        let mut shard: Shard<u32> = Shard::new(policy);
        // key -> (inserted_at, weight)
        let mut model: BTreeMap<u64, (Instant, u64)> = BTreeMap::new();
        let ttl = Duration::from_secs(50);
        let t0 = Instant::now();
        let mut now = t0;
        let key_space = 1 + rng.below(40);

        for step in 0..600 {
            let ctx = format!("{ctx}, step {step}");
            // Time mostly creeps forward and sometimes stands still, so equal
            // timestamps are exercised too.
            now += Duration::from_millis(rng.below(4) * 500);
            let key = rng.below(key_space);
            let weight = 1 + rng.below(9);
            match rng.below(12) {
                0..=3 => {
                    shard.insert(key, 0, weight, now);
                    model.insert(key, (now, weight));
                }
                4 => {
                    let keep_ttl = rng.below(2) == 0;
                    let outcome = shard.replace_if(key, 0, weight, now, keep_ttl, |_| true);
                    if let Some(entry) = model.get_mut(&key) {
                        assert!(matches!(outcome, ReplaceOutcome::Replaced { .. }), "{ctx}");
                        entry.1 = weight;
                        if !keep_ttl {
                            entry.0 = now;
                        }
                    } else {
                        assert!(matches!(outcome, ReplaceOutcome::Declined { .. }), "{ctx}");
                    }
                }
                5 => {
                    let removed = shard.remove(key).map(|(_, w)| w);
                    assert_eq!(removed, model.remove(&key).map(|(_, w)| w), "{ctx}");
                }
                6 => {
                    let outcome = shard.get(key, now, ttl);
                    match model.get(&key).copied() {
                        None => assert!(matches!(outcome, GetOutcome::Miss), "{ctx}"),
                        Some((at, w)) if now.saturating_duration_since(at) >= ttl => {
                            assert!(
                                matches!(outcome, GetOutcome::Expired { weight, .. } if weight == w),
                                "{ctx}"
                            );
                            model.remove(&key);
                        }
                        Some(_) => {
                            assert!(matches!(outcome, GetOutcome::Hit(_)), "{ctx}");
                            shard.apply_touch(key);
                        }
                    }
                }
                7 => {
                    let region = match rng.below(3) {
                        0 => Region::Window,
                        1 => Region::Probation,
                        _ => Region::Protected,
                    };
                    if let Some((evicted, _, w)) = shard.evict_region_lru(region) {
                        assert_eq!(model.remove(&evicted).map(|(_, w)| w), Some(w), "{ctx}");
                    }
                }
                8 => {
                    // Region moves relink the recency lists only.
                    let _ = shard.move_window_to_probation(key);
                    let _ = shard.demote_protected_lru_to_probation();
                }
                9 | 10 => {
                    let limit = usize::try_from(1 + rng.below(6)).expect("small");
                    let mut expected: Vec<u64> = shard
                        .collect_matching(|n| ttl_elapsed(shard.stamp(now), n.inserted_at, ttl))
                        .into_iter()
                        .map(|(k, _)| k)
                        .collect();
                    expected.sort_unstable();
                    let mut modeled: Vec<u64> = model
                        .iter()
                        .filter(|(_, (at, _))| now.saturating_duration_since(*at) >= ttl)
                        .map(|(k, _)| *k)
                        .collect();
                    modeled.sort_unstable();
                    assert_eq!(
                        expected, modeled,
                        "{ctx}: the recency walk disagrees with the model"
                    );
                    let before: Vec<u64> = shard.keys_oldest_first();
                    let (values, weight, more) = shard.expire_older_than_at_most(now, ttl, limit);
                    assert_eq!(values.len(), expected.len().min(limit), "{ctx}");
                    assert_eq!(more, values.len() == limit, "{ctx}");
                    // The removed entries are the oldest ones, in order.
                    let removed = &before[..values.len()];
                    let mut removed_sorted = removed.to_vec();
                    removed_sorted.sort_unstable();
                    for key in &removed_sorted {
                        assert!(expected.binary_search(key).is_ok(), "{ctx}: {key} was live");
                    }
                    let modeled_weight: u64 = removed
                        .iter()
                        .map(|k| model.remove(k).map_or(0, |(_, w)| w))
                        .sum();
                    assert_eq!(weight, modeled_weight, "{ctx}");
                }
                _ => {
                    if rng.below(8) == 0 {
                        let _ = shard.take_all();
                        model.clear();
                    }
                }
            }
            assert_expiry_list_consistent(&shard, &model, &ctx);
        }
    }

    fn assert_expiry_list_consistent(
        shard: &Shard<u32>,
        model: &std::collections::BTreeMap<u64, (Instant, u64)>,
        ctx: &str,
    ) {
        let mut seen = Vec::new();
        let mut prev = None;
        let mut cursor = shard.expiry.oldest;
        let mut last: Option<Stamp> = None;
        while let Some(idx) = cursor {
            let Some(Slot::Occupied(node)) = shard.slots.get(idx as usize) else {
                panic!("{ctx}: expiry list reaches vacant slot {idx}");
            };
            assert_eq!(
                node.older.get(),
                prev,
                "{ctx}: broken back link at {}",
                node.key
            );
            assert!(
                last.is_none_or(|t| t <= node.inserted_at),
                "{ctx}: expiry list out of order at {}",
                node.key
            );
            assert_eq!(
                model.get(&node.key).map(|&(at, w)| (shard.stamp(at), w)),
                Some((node.inserted_at, node.weight)),
                "{ctx}: resident {} disagrees with the model",
                node.key
            );
            last = Some(node.inserted_at);
            seen.push(node.key);
            assert!(seen.len() <= model.len(), "{ctx}: expiry list has a cycle");
            prev = cursor;
            cursor = node.newer.get();
        }
        assert_eq!(shard.expiry.newest, prev, "{ctx}: newest end is stale");
        seen.sort_unstable();
        let keys: Vec<u64> = model.keys().copied().collect();
        assert_eq!(seen, keys, "{ctx}: expiry list and residents differ");
        assert_eq!(shard.len(), model.len(), "{ctx}");
        assert_eq!(
            shard.weight,
            model.values().map(|(_, w)| w).sum::<u64>(),
            "{ctx}"
        );
    }
}
