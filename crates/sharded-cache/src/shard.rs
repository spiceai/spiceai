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

use crate::EvictionPolicy;
use crate::hasher::IdentityBuildHasher;
use crate::sketch::CountMinSketch;
use std::collections::HashMap;
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

#[derive(Debug, Default, Clone, Copy)]
struct ListEnds {
    head: Option<u32>,
    tail: Option<u32>,
}

pub(crate) struct Shard<V> {
    map: HashMap<u64, u32, IdentityBuildHasher>,
    slots: Vec<Slot<V>>,
    free: Vec<u32>,
    window: ListEnds,
    probation: ListEnds,
    protected: ListEnds,
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
    value: V,
    inserted_at: Instant,
    weight: u64,
    region: Region,
    /// Saturating hit count for [`EvictionPolicy::Lfu`].
    freq: u16,
    prev: Option<u32>,
    next: Option<u32>,
}

pub(crate) enum GetOutcome<V> {
    Hit(V),
    Miss,
    Expired { value: V, weight: u64 },
}

#[derive(Clone, Copy)]
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

impl<V> Shard<V> {
    pub(crate) fn new(policy: EvictionPolicy) -> Self {
        Self {
            map: HashMap::with_hasher(IdentityBuildHasher),
            slots: Vec::new(),
            free: Vec::new(),
            window: ListEnds::default(),
            probation: ListEnds::default(),
            protected: ListEnds::default(),
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

    #[expect(dead_code)]
    pub(crate) fn contains(&self, key: u64) -> bool {
        self.map.contains_key(&key)
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

    #[expect(dead_code)]
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

    pub(crate) fn peek_tail(&self) -> Option<(u64, u64)> {
        self.peek_region_tail(self.primary_region())
    }

    pub(crate) fn peek_region_tail(&self, region: Region) -> Option<(u64, u64)> {
        let idx = self.ends(region).tail?;
        match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => Some((node.key, node.weight)),
            _ => None,
        }
    }

    pub(crate) fn peek_lfu_victim(&self) -> Option<(u64, u64, u16)> {
        let mut best: Option<(u64, u64, u16)> = None;
        let mut cursor = self.probation.tail;
        while let Some(idx) = cursor {
            let Some(Slot::Occupied(node)) = self.slots.get(idx as usize) else {
                break;
            };
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
            Some(Slot::Occupied(node)) => now.saturating_duration_since(node.inserted_at) >= ttl,
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
    /// Non-destructive get: clone the value and promote in place.
    pub(crate) fn get(&mut self, key: u64, now: Instant, ttl: Duration) -> GetOutcome<V> {
        let Some(&idx) = self.map.get(&key) else {
            return GetOutcome::Miss;
        };
        let expired = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => now.saturating_duration_since(node.inserted_at) >= ttl,
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
            Some(Slot::Occupied(node)) => node.value.clone(),
            _ => return GetOutcome::Miss,
        };
        if let Some(Slot::Occupied(node)) = self.slots.get_mut(idx as usize) {
            node.freq = node.freq.saturating_add(1);
        }
        self.on_hit(idx);
        GetOutcome::Hit(value)
    }

    pub(crate) fn insert(
        &mut self,
        key: u64,
        value: V,
        weight: u64,
        now: Instant,
    ) -> (WeightDelta, Option<V>) {
        if let Some(&idx) = self.map.get(&key) {
            return self.replace(idx, value, weight, now);
        }
        let region = self.admit_region();
        let idx = self.alloc(Node {
            key,
            value,
            inserted_at: now,
            weight,
            region,
            freq: 0,
            prev: None,
            next: None,
        });
        self.map.insert(key, idx);
        self.push_front(idx, region);
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

    pub(crate) fn remove(&mut self, key: u64) -> Option<(V, u64)> {
        let idx = self.map.remove(&key)?;
        let weight = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => node.weight,
            _ => 0,
        };
        let value = self.take_value_and_free(idx)?;
        Some((value, weight))
    }

    pub(crate) fn evict_lru(&mut self) -> Option<(u64, V, u64)> {
        self.evict_region_lru(self.primary_region())
    }

    pub(crate) fn evict_region_lru(&mut self, region: Region) -> Option<(u64, V, u64)> {
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
    pub(crate) fn invalidate_matching<F>(&mut self, predicate: F) -> (Vec<V>, u64)
    where
        F: Fn(&V) -> bool,
    {
        let matched = self.collect_matching(|node| predicate(&node.value));
        self.remove_indices(matched)
    }

    pub(crate) fn expire_older_than(&mut self, now: Instant, ttl: Duration) -> (Vec<V>, u64) {
        let matched =
            self.collect_matching(|node| now.saturating_duration_since(node.inserted_at) >= ttl);
        self.remove_indices(matched)
    }

    pub(crate) fn take_all(&mut self) -> (Vec<V>, u64) {
        let weight = self.weight;
        let mut values = Vec::with_capacity(self.map.len());
        self.map.clear();
        self.window = ListEnds::default();
        self.probation = ListEnds::default();
        self.protected = ListEnds::default();
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

    fn remove_indices(&mut self, matched: Vec<(u64, u32)>) -> (Vec<V>, u64) {
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

impl<V> Shard<V> {
    fn replace(
        &mut self,
        idx: u32,
        value: V,
        weight: u64,
        now: Instant,
    ) -> (WeightDelta, Option<V>) {
        let (old_weight, old_value, region) = match self.slots.get_mut(idx as usize) {
            Some(Slot::Occupied(node)) => {
                let old_weight = node.weight;
                let old_value = std::mem::replace(&mut node.value, value);
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
        (delta, Some(old_value))
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

    fn take_value_and_free(&mut self, idx: u32) -> Option<V> {
        match self.take_slot(idx)? {
            Slot::Occupied(node) => Some(node.value),
            Slot::Vacant => None,
        }
    }

    fn take_slot(&mut self, idx: u32) -> Option<Slot<V>> {
        self.unlink(idx);
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
        assert!(matches!(got, GetOutcome::Hit(10)));
        assert_eq!(shard.len(), 2, "a hit must not remove the entry");
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
        assert_eq!(shard.peek_tail(), Some((1, 5)));
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
        assert!(matches!(got, GetOutcome::Hit(10)));
        assert_eq!(shard.peek_region(1), Some(Region::Protected));
        assert_eq!(shard.protected_weight(), 5);
    }
}
