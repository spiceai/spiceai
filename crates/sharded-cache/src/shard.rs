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

//! One shard of the cache: a `HashMap` plus an intrusive LRU list.
//!
//! Recency is list position. A hit unlinks the node and relinks it at the
//! head without removing it from the map, so a concurrent reader of the same
//! key cannot observe a hole.

use crate::EvictionPolicy;
use crate::hasher::IdentityBuildHasher;
use crate::sketch::CountMinSketch;
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub(crate) struct Shard<V> {
    map: HashMap<u64, u32, IdentityBuildHasher>,
    slots: Vec<Slot<V>>,
    free: Vec<u32>,
    /// Most-recently-used node.
    head: Option<u32>,
    /// Least-recently-used node.
    tail: Option<u32>,
    weight: u64,
    sketch: Option<CountMinSketch>,
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
    prev: Option<u32>,
    next: Option<u32>,
}

pub(crate) enum GetOutcome<V> {
    Hit(V),
    Miss,
    Expired { value: V, weight: u64 },
}

pub(crate) struct WeightDelta {
    pub(crate) added: u64,
    pub(crate) removed: u64,
}

impl WeightDelta {
    pub(crate) fn net(self) -> i128 {
        i128::from(self.added) - i128::from(self.removed)
    }
}

impl<V> Shard<V> {
    pub(crate) fn new(policy: EvictionPolicy) -> Self {
        Self {
            map: HashMap::with_hasher(IdentityBuildHasher),
            slots: Vec::new(),
            free: Vec::new(),
            head: None,
            tail: None,
            weight: 0,
            sketch: matches!(policy, EvictionPolicy::TinyLfu).then(CountMinSketch::new),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn contains(&self, key: u64) -> bool {
        self.map.contains_key(&key)
    }

    pub(crate) fn tail_key(&self) -> Option<u64> {
        let idx = self.tail?;
        match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => Some(node.key),
            _ => None,
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

    /// Keys most-recently-used first. Used to assert that a scan did not
    /// rewrite recency.
    pub(crate) fn keys_mru_first(&self) -> Vec<u64> {
        let mut keys = Vec::with_capacity(self.map.len());
        let mut cursor = self.head;
        while let Some(idx) = cursor {
            let Some(Slot::Occupied(node)) = self.slots.get(idx as usize) else {
                break;
            };
            keys.push(node.key);
            cursor = node.next;
        }
        keys
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
        self.promote(idx);
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
        let idx = self.alloc(Node {
            key,
            value,
            inserted_at: now,
            weight,
            prev: None,
            next: None,
        });
        self.map.insert(key, idx);
        self.push_front(idx);
        self.weight = self.weight.saturating_add(weight);
        (
            WeightDelta {
                added: weight,
                removed: 0,
            },
            None,
        )
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
        let idx = self.tail?;
        let (key, weight) = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => (node.key, node.weight),
            _ => return None,
        };
        self.map.remove(&key);
        let value = self.take_value_and_free(idx)?;
        Some((key, value, weight))
    }

    /// Drop matching entries without promoting survivors. Returns `(values, weight)`.
    pub(crate) fn invalidate_matching<F>(&mut self, predicate: F) -> (Vec<V>, u64)
    where
        F: Fn(&V) -> bool,
    {
        let mut matched = Vec::new();
        let mut cursor = self.head;
        while let Some(idx) = cursor {
            let (next, key, is_match) = match self.slots.get(idx as usize) {
                Some(Slot::Occupied(node)) => (node.next, node.key, predicate(&node.value)),
                _ => break,
            };
            if is_match {
                matched.push((key, idx));
            }
            cursor = next;
        }
        let mut values = Vec::with_capacity(matched.len());
        let mut weight: u64 = 0;
        for (key, idx) in matched {
            self.map.remove(&key);
            if let Some(Slot::Occupied(node)) = self.take_slot(idx) {
                weight = weight.saturating_add(node.weight);
                self.weight = self.weight.saturating_sub(node.weight);
                values.push(node.value);
            }
        }
        (values, weight)
    }

    pub(crate) fn expire_older_than(&mut self, now: Instant, ttl: Duration) -> (Vec<V>, u64) {
        let mut matched = Vec::new();
        let mut cursor = self.head;
        while let Some(idx) = cursor {
            let (next, key, expired) = match self.slots.get(idx as usize) {
                Some(Slot::Occupied(node)) => (
                    node.next,
                    node.key,
                    now.saturating_duration_since(node.inserted_at) >= ttl,
                ),
                _ => break,
            };
            if expired {
                matched.push((key, idx));
            }
            cursor = next;
        }
        let mut values = Vec::with_capacity(matched.len());
        let mut weight: u64 = 0;
        for (key, idx) in matched {
            self.map.remove(&key);
            if let Some(Slot::Occupied(node)) = self.take_slot(idx) {
                weight = weight.saturating_add(node.weight);
                self.weight = self.weight.saturating_sub(node.weight);
                values.push(node.value);
            }
        }
        (values, weight)
    }

    pub(crate) fn take_all(&mut self) -> (Vec<V>, u64) {
        let weight = self.weight;
        let mut values = Vec::with_capacity(self.map.len());
        self.map.clear();
        self.head = None;
        self.tail = None;
        self.weight = 0;
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
}

impl<V> Shard<V> {
    fn replace(
        &mut self,
        idx: u32,
        value: V,
        weight: u64,
        now: Instant,
    ) -> (WeightDelta, Option<V>) {
        let (old_weight, old_value) = match self.slots.get_mut(idx as usize) {
            Some(Slot::Occupied(node)) => {
                let old_weight = node.weight;
                let old_value = std::mem::replace(&mut node.value, value);
                node.weight = weight;
                node.inserted_at = now;
                (old_weight, old_value)
            }
            _ => {
                return (
                    WeightDelta {
                        added: 0,
                        removed: 0,
                    },
                    None,
                );
            }
        };
        self.weight = self
            .weight
            .saturating_sub(old_weight)
            .saturating_add(weight);
        self.promote(idx);
        (
            WeightDelta {
                added: weight,
                removed: old_weight,
            },
            Some(old_value),
        )
    }

    fn promote(&mut self, idx: u32) {
        if self.head == Some(idx) {
            return;
        }
        self.unlink(idx);
        self.push_front(idx);
    }

    fn push_front(&mut self, idx: u32) {
        if let Some(Slot::Occupied(node)) = self.slots.get_mut(idx as usize) {
            node.prev = None;
            node.next = self.head;
        }
        if let Some(head) = self.head
            && let Some(Slot::Occupied(node)) = self.slots.get_mut(head as usize)
        {
            node.prev = Some(idx);
        }
        self.head = Some(idx);
        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    fn unlink(&mut self, idx: u32) {
        let (prev, next) = match self.slots.get(idx as usize) {
            Some(Slot::Occupied(node)) => (node.prev, node.next),
            _ => return,
        };
        if let Some(prev_idx) = prev
            && let Some(Slot::Occupied(node)) = self.slots.get_mut(prev_idx as usize)
        {
            node.next = next;
        } else {
            self.head = next;
        }
        if let Some(next_idx) = next
            && let Some(Slot::Occupied(node)) = self.slots.get_mut(next_idx as usize)
        {
            node.prev = prev;
        } else {
            self.tail = prev;
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
        self.unlink(idx);
        match std::mem::replace(self.slots.get_mut(idx as usize)?, Slot::Vacant) {
            Slot::Occupied(node) => {
                self.weight = self.weight.saturating_sub(node.weight);
                self.free.push(idx);
                Some(node.value)
            }
            Slot::Vacant => None,
        }
    }

    fn take_slot(&mut self, idx: u32) -> Option<Slot<V>> {
        self.unlink(idx);
        let slot = self.slots.get_mut(idx as usize)?;
        let taken = std::mem::replace(slot, Slot::Vacant);
        if matches!(taken, Slot::Occupied(_)) {
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
}
