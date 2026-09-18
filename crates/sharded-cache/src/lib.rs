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

//! Sharded in-memory cache with LRU and `TinyLFU` eviction.
//!
//! # Sharding
//!
//! The cache is split into [`NUM_SHARDS`] (16) independent shards. A key maps
//! to shard **`key % 16`** ([`shard_index`]). Keys are pre-hashed `u64` values;
//! each shard's map uses an identity hasher so they are not hashed again.
//!
//! # Get is non-destructive
//!
//! [`ShardedCache::get`] never removes an entry to serve a hit. A hit clones
//! the value and updates LRU recency in place. Two concurrent hits on the same
//! key both succeed. This is the hard gate against `Pingora`'s remove-and-re-admit
//! path (spiceai/spiceai#12985).
//!
//! # Table invalidation
//!
//! [`ShardedCache::invalidate_matching`] scans each shard in place and does not
//! promote survivors, so a refresh cannot rewrite recency as scan order.

mod hasher;
mod shard;
mod sketch;

use parking_lot::Mutex;
use shard::{GetOutcome, Shard};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub use hasher::{IdentityBuildHasher, IdentityHasher};

/// Number of shards. Keys map to shard `key % NUM_SHARDS`.
pub const NUM_SHARDS: usize = 16;

/// Maps a cache key to its shard: `key % NUM_SHARDS`.
#[inline]
#[must_use]
pub fn shard_index(key: u64) -> usize {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "shard index only needs the low bits of the u64 key"
    )]
    {
        (key as usize) % NUM_SHARDS
    }
}

/// Why an entry left the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionReason {
    /// The cache exceeded `max_weight` and reclaimed an entry.
    Size,
    /// The entry outlived its TTL.
    Expired,
    /// A caller asked that matching entries be dropped (table invalidation).
    Invalidated,
}

/// Admission / eviction policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// Least-recently-used: the coldest entry is the next size-eviction victim.
    #[default]
    Lru,
    /// `TinyLFU` admission over LRU eviction: a new key that would push the cache
    /// over `max_weight` is admitted only if it is at least as frequent as the
    /// current LRU victim.
    TinyLfu,
}

/// Called when an entry is evicted for size, expiry, or invalidation.
///
/// Explicit [`ShardedCache::remove`] and [`ShardedCache::clear`] do not notify.
pub trait EvictionListener: Send + Sync + 'static {
    fn on_evict(reason: EvictionReason);
}

/// Listener that records nothing.
#[derive(Debug, Default)]
pub struct NoopListener;

impl EvictionListener for NoopListener {
    fn on_evict(_reason: EvictionReason) {}
}

#[repr(align(64))]
struct CachePadded<T>(T);

/// Sharded cache keyed by pre-hashed `u64` values.
pub struct ShardedCache<V, L: EvictionListener = NoopListener> {
    shards: Box<[CachePadded<Mutex<Shard<V>>>; NUM_SHARDS]>,
    max_weight: u64,
    ttl: Duration,
    policy: EvictionPolicy,
    weight: AtomicU64,
    /// Serializes overflow trimming so two concurrent inserts cannot each
    /// evict a victim after a single removal would already restore the budget.
    trim: Mutex<()>,
    _listener: std::marker::PhantomData<L>,
}

impl<V: Clone + Send + 'static, L: EvictionListener> ShardedCache<V, L> {
    /// Create a cache with a byte budget of `max_weight` and a per-entry TTL.
    #[must_use]
    pub fn new(max_weight: u64, ttl: Duration, policy: EvictionPolicy) -> Self {
        Self {
            shards: Box::new(core::array::from_fn(|_| {
                CachePadded(Mutex::new(Shard::new(policy)))
            })),
            max_weight,
            ttl,
            policy,
            weight: AtomicU64::new(0),
            trim: Mutex::new(()),
            _listener: std::marker::PhantomData,
        }
    }

    /// Insert `value` under `key` with the given byte `weight`.
    ///
    /// A value heavier than `max_weight` is admitted and then size-evicted, so
    /// it is not retained. `TinyLFU` may reject a *new* key that would exceed
    /// the budget if it is less frequent than the current LRU victim.
    pub fn insert(&self, key: u64, value: V, weight: usize) {
        let weight = u64::try_from(weight).unwrap_or(u64::MAX);
        let now = Instant::now();
        let shard_idx = shard_index(key);
        let mut shard = self.shards[shard_idx].0.lock();

        if matches!(self.policy, EvictionPolicy::TinyLfu) {
            shard.increment_sketch(key);
            if self.should_reject_tinylfu(&shard, key, weight) {
                return;
            }
        }

        let delta = shard.insert(key, value, weight, now);
        // Publish the weight before releasing the shard so a concurrent
        // remove of this key cannot subtract before the matching add.
        self.apply_delta(delta.net());
        drop(shard);
        self.evict_to_limit(shard_idx);
    }

    /// Clone the value for `key` if it is present and unexpired.
    ///
    /// Never removes a live entry. An expired entry is dropped and reported
    /// as [`EvictionReason::Expired`].
    pub fn get(&self, key: &u64) -> Option<V> {
        let now = Instant::now();
        let mut shard = self.shards[shard_index(*key)].0.lock();
        if matches!(self.policy, EvictionPolicy::TinyLfu) {
            shard.increment_sketch(*key);
        }
        match shard.get(*key, now, self.ttl) {
            GetOutcome::Hit(value) => Some(value),
            GetOutcome::Miss => None,
            GetOutcome::Expired { weight } => {
                self.sub_weight(weight);
                drop(shard);
                L::on_evict(EvictionReason::Expired);
                None
            }
        }
    }

    /// Remove `key` if present. This is not an eviction and is not reported.
    pub fn remove(&self, key: &u64) -> Option<V> {
        let mut shard = self.shards[shard_index(*key)].0.lock();
        let (value, weight) = shard.remove(*key)?;
        self.sub_weight(weight);
        drop(shard);
        Some(value)
    }

    /// Drop every entry. Not reported as evictions.
    pub fn clear(&self) {
        // Lock order is shard 0..N so a concurrent `clear` cannot deadlock.
        let mut guards: Vec<_> = self.shards.iter().map(|s| s.0.lock()).collect();
        let mut removed: u64 = 0;
        for guard in &mut guards {
            let (_, weight) = guard.take_all();
            removed = removed.saturating_add(weight);
        }
        self.sub_weight(removed);
        drop(guards);
    }

    /// Keys currently held, in shard order. May include entries that have
    /// expired but have not been observed yet.
    #[must_use]
    pub fn iter_keys(&self) -> Vec<u64> {
        let mut keys = Vec::new();
        for shard in self.shards.iter() {
            keys.extend(shard.0.lock().keys());
        }
        keys
    }

    /// Number of entries, including unobserved expired ones.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.0.lock().len()).sum()
    }

    /// Whether [`Self::len`] is zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Sum of admitted weights.
    #[must_use]
    pub fn weighted_size(&self) -> u64 {
        self.weight.load(Ordering::Relaxed)
    }

    /// Expire stale entries and evict down to `max_weight`.
    pub fn run_pending_tasks(&self) {
        let now = Instant::now();
        for shard_idx in 0..NUM_SHARDS {
            let mut shard = self.shards[shard_idx].0.lock();
            let (expired, weight) = shard.expire_older_than(now, self.ttl);
            self.sub_weight(weight);
            drop(shard);
            for _ in expired {
                L::on_evict(EvictionReason::Expired);
            }
        }
        self.evict_to_limit(0);
    }

    /// Drop every entry whose value satisfies `predicate`.
    ///
    /// Survivors are not promoted. Returns how many entries were removed.
    ///
    /// Each shard is unlocked before the next is scanned, so a write that
    /// lands in an already-walked shard can survive this return. That is the
    /// same window Pingora and Moka leave. SQL results close it with a
    /// table-change clock on the write path; search results do not.
    pub fn invalidate_matching<F>(&self, predicate: F) -> usize
    where
        F: Fn(&V) -> bool,
    {
        let mut removed = 0;
        for shard_idx in 0..NUM_SHARDS {
            let mut shard = self.shards[shard_idx].0.lock();
            let (values, weight) = shard.invalidate_matching(&predicate);
            self.sub_weight(weight);
            drop(shard);
            removed += values.len();
            for _ in values {
                L::on_evict(EvictionReason::Invalidated);
            }
        }
        removed
    }

    /// Keys most-recently-used first within each shard, concatenated in shard
    /// order. Test helper for recency contracts.
    #[must_use]
    pub fn keys_in_lru_order(&self) -> Vec<u64> {
        let mut keys = Vec::new();
        for shard in self.shards.iter() {
            keys.extend(shard.0.lock().keys_mru_first());
        }
        keys
    }

    /// Reject a *new* key when this shard is already over budget *and* the
    /// candidate is less frequent than this shard's LRU tail. Eviction for
    /// `TinyLFU` also starts on this shard, so admission and the victim match.
    fn should_reject_tinylfu(&self, shard: &Shard<V>, key: u64, weight: u64) -> bool {
        if shard.contains(key) {
            return false;
        }
        if self.weight.load(Ordering::Relaxed).saturating_add(weight) <= self.max_weight {
            return false;
        }
        let Some(victim) = shard.tail_key() else {
            return false;
        };
        shard.sketch_estimate(key) < shard.sketch_estimate(victim)
    }

    fn evict_to_limit(&self, prefer: usize) {
        if self.weight.load(Ordering::Relaxed) <= self.max_weight {
            return;
        }
        let _trim = self.trim.lock();
        while self.weight.load(Ordering::Relaxed) > self.max_weight {
            let progressed = match self.policy {
                // `TinyLFU` admission compares against the inserting shard's
                // tail, so trim that shard first. A one-shot on an empty shard
                // is then self-evicted instead of displacing a hot key elsewhere.
                EvictionPolicy::TinyLfu => self.evict_prefer_then_others(prefer),
                // LRU must not self-evict a just-admitted sole resident while
                // an older victim exists on another shard.
                EvictionPolicy::Lru => self.evict_others_then_prefer(prefer),
            };
            if !progressed {
                break;
            }
        }
    }

    fn evict_prefer_then_others(&self, prefer: usize) -> bool {
        if self.evict_one_from(prefer) {
            return true;
        }
        self.evict_first_other(prefer)
    }

    fn evict_others_then_prefer(&self, prefer: usize) -> bool {
        if self.evict_first_other(prefer) {
            return true;
        }
        self.evict_one_from(prefer)
    }

    fn evict_first_other(&self, prefer: usize) -> bool {
        (0..NUM_SHARDS)
            .filter(|&shard_idx| shard_idx != prefer)
            .any(|shard_idx| self.evict_one_from(shard_idx))
    }

    fn evict_one_from(&self, shard_idx: usize) -> bool {
        let mut shard = self.shards[shard_idx].0.lock();
        let now = Instant::now();
        let (expired, expired_weight) = shard.expire_older_than(now, self.ttl);
        self.sub_weight(expired_weight);
        if !expired.is_empty() {
            drop(shard);
            for _ in expired {
                L::on_evict(EvictionReason::Expired);
            }
            return true;
        }
        let Some((_key, _value, weight)) = shard.evict_lru() else {
            return false;
        };
        self.sub_weight(weight);
        drop(shard);
        L::on_evict(EvictionReason::Size);
        true
    }

    fn apply_delta(&self, net: i128) {
        if net > 0 {
            let add = u64::try_from(net).unwrap_or(u64::MAX);
            self.weight.fetch_add(add, Ordering::Relaxed);
        } else if net < 0 {
            let sub = u64::try_from(-net).unwrap_or(u64::MAX);
            self.sub_weight(sub);
        }
    }

    fn sub_weight(&self, amount: u64) {
        // Saturating subtract without a CAS loop: weight only decreases here
        // alongside a matching shard removal, so underflow would be a bug in
        // the pairing, not a race worth spinning on.
        let _ = self
            .weight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(amount))
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestValue {
        data: String,
        size: usize,
    }

    impl TestValue {
        fn new(data: &str) -> Self {
            Self {
                size: data.len(),
                data: data.to_string(),
            }
        }

        fn with_size(data: &str, size: usize) -> Self {
            Self {
                data: data.to_string(),
                size,
            }
        }
    }

    fn cache(max_weight: u64, ttl: Duration) -> ShardedCache<TestValue> {
        ShardedCache::new(max_weight, ttl, EvictionPolicy::Lru)
    }

    #[test]
    fn shard_index_is_key_mod_16() {
        for key in 0..64u64 {
            assert_eq!(shard_index(key), usize::try_from(key % 16).expect("0..16"));
        }
        assert_eq!(shard_index(u64::MAX), 15);
    }

    #[test]
    fn keys_zero_through_fifteen_land_on_distinct_shards() {
        let seen: Vec<usize> = (0..16).map(shard_index).collect();
        let mut unique = seen.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique, seen, "key i must map to shard i for i in 0..16");
    }

    #[test]
    fn insert_get_round_trip() {
        let cache = cache(1024, Duration::from_mins(1));
        cache.insert(1, TestValue::new("hello"), 5);
        assert_eq!(cache.get(&1).map(|v| v.data), Some("hello".to_string()));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.weighted_size(), 5);
    }

    #[test]
    fn get_is_non_destructive() {
        let cache = cache(1024, Duration::from_mins(1));
        cache.insert(7, TestValue::new("stay"), 4);
        assert!(cache.get(&7).is_some());
        assert!(cache.get(&7).is_some());
        assert_eq!(cache.len(), 1, "hits must not drop the entry");
        assert_eq!(cache.weighted_size(), 4);
    }

    #[test]
    fn ttl_expires_on_get() {
        let cache = cache(1024, Duration::from_millis(30));
        cache.insert(1, TestValue::new("ephemeral"), 9);
        assert!(cache.get(&1).is_some());
        std::thread::sleep(Duration::from_millis(50));
        assert!(cache.get(&1).is_none());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.weighted_size(), 0);
    }

    #[test]
    fn weight_tracks_insert_overwrite_and_remove() {
        let cache = cache(1024, Duration::from_mins(1));
        cache.insert(1, TestValue::with_size("a", 100), 100);
        cache.insert(2, TestValue::with_size("b", 200), 200);
        assert_eq!(cache.weighted_size(), 300);
        cache.insert(1, TestValue::with_size("A", 50), 50);
        assert_eq!(cache.weighted_size(), 250);
        cache.remove(&2);
        assert_eq!(cache.weighted_size(), 50);
    }

    #[test]
    fn insert_evicts_until_the_cache_fits() {
        let cache = cache(100, Duration::from_mins(1));
        for i in 0..50u64 {
            cache.insert(i, TestValue::with_size("x", 100), 100);
        }
        assert!(cache.weighted_size() <= 100);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn a_value_heavier_than_max_weight_is_not_retained() {
        let cache = cache(100, Duration::from_mins(1));
        cache.insert(1, TestValue::with_size("huge", 500), 500);
        assert!(cache.get(&1).is_none());
        assert_eq!(cache.weighted_size(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn concurrent_gets_of_one_key_all_hit() {
        let cache = Arc::new(cache(4096, Duration::from_mins(1)));
        cache.insert(42, TestValue::new("shared"), 6);

        let misses = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let cache = Arc::clone(&cache);
            let misses = Arc::clone(&misses);
            handles.push(std::thread::spawn(move || {
                for _ in 0..1_000 {
                    if cache.get(&42).is_none() {
                        misses.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().expect("thread panicked");
        }
        assert_eq!(
            misses.load(Ordering::Relaxed),
            0,
            "a live key must not report a miss to a concurrent hit"
        );
        assert!(cache.get(&42).is_some());
    }

    #[test]
    fn invalidate_matching_does_not_promote_survivors() {
        let cache = cache(1024, Duration::from_mins(1));
        for key in [16, 32, 48, 64, 80] {
            cache.insert(key, TestValue::new(&format!("v{key}")), 1);
        }
        assert_eq!(
            cache.get(&16).map(|v| v.data),
            Some("v16".to_string()),
            "read the oldest so recency no longer matches insertion order"
        );
        assert_eq!(cache.keys_in_lru_order(), vec![16, 80, 64, 48, 32]);

        let removed = cache.invalidate_matching(|value| value.data == "v48");
        assert_eq!(removed, 1);
        assert_eq!(
            cache.keys_in_lru_order(),
            vec![16, 80, 64, 32],
            "survivors must keep the recency they had before the scan"
        );
    }

    #[test]
    fn tinylfu_put_and_get() {
        let cache: ShardedCache<TestValue> =
            ShardedCache::new(1024, Duration::from_mins(1), EvictionPolicy::TinyLfu);
        cache.insert(1, TestValue::new("tiny"), 4);
        assert_eq!(cache.get(&1).map(|v| v.data), Some("tiny".to_string()));
    }

    #[test]
    fn tinylfu_keeps_a_hot_key_over_a_one_shot() {
        let cache: ShardedCache<TestValue> =
            ShardedCache::new(100, Duration::from_mins(1), EvictionPolicy::TinyLfu);
        let hot = 16u64;
        cache.insert(hot, TestValue::with_size("hot", 100), 100);
        for _ in 0..64 {
            assert!(cache.get(&hot).is_some());
        }
        for one_shot in (32..48).step_by(16) {
            cache.insert(
                one_shot,
                TestValue::with_size(&format!("c{one_shot}"), 100),
                100,
            );
        }
        assert!(
            cache.get(&hot).is_some(),
            "`TinyLFU` must not admit one-shot keys over a frequently read resident"
        );
    }

    struct CountingListener;
    static SIZE_EVICTIONS: AtomicU64 = AtomicU64::new(0);

    impl EvictionListener for CountingListener {
        fn on_evict(reason: EvictionReason) {
            if reason == EvictionReason::Size {
                SIZE_EVICTIONS.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[test]
    fn size_eviction_is_reported() {
        SIZE_EVICTIONS.store(0, Ordering::Relaxed);
        let cache: ShardedCache<TestValue, CountingListener> =
            ShardedCache::new(250, Duration::from_mins(1), EvictionPolicy::Lru);
        for i in 0..3u64 {
            cache.insert(i, TestValue::with_size("x", 100), 100);
        }
        assert!(cache.weighted_size() <= 250);
        assert!(
            SIZE_EVICTIONS.load(Ordering::Relaxed) > 0,
            "crossing max_weight must report a size eviction"
        );
    }

    #[test]
    fn insert_evicts_an_older_entry_on_another_shard() {
        let cache = cache(100, Duration::from_mins(1));
        cache.insert(0, TestValue::with_size("old", 60), 60);
        cache.insert(1, TestValue::with_size("new", 60), 60);
        assert!(
            cache.get(&1).is_some(),
            "the just-admitted key must not self-evict while an older victim exists"
        );
        assert!(
            cache.get(&0).is_none(),
            "the older entry on another shard is the LRU victim"
        );
        assert!(cache.weighted_size() <= 100);
    }

    #[test]
    fn clear_after_partial_remove_reuses_the_budget() {
        let cache = cache(10_000, Duration::from_mins(1));
        for i in 0..200u64 {
            cache.insert(i, TestValue::with_size("x", 10), 10);
        }
        for i in 0..100u64 {
            cache.remove(&i);
        }
        cache.clear();
        assert_eq!(cache.weighted_size(), 0);
        for i in 0..200u64 {
            cache.insert(i + 1_000, TestValue::with_size("y", 10), 10);
        }
        assert_eq!(cache.len(), 200);
        assert_eq!(cache.weighted_size(), 2_000);
    }

    #[test]
    fn tinylfu_does_not_evict_a_hot_key_on_another_shard() {
        let cache: ShardedCache<TestValue> =
            ShardedCache::new(100, Duration::from_mins(1), EvictionPolicy::TinyLfu);
        let hot = 0u64;
        cache.insert(hot, TestValue::with_size("hot", 100), 100);
        for _ in 0..64 {
            assert!(cache.get(&hot).is_some());
        }
        cache.insert(1, TestValue::with_size("one", 100), 100);
        assert!(
            cache.get(&hot).is_some(),
            "`TinyLFU` must not evict a hot resident to admit a one-shot on another shard"
        );
        assert!(
            cache.get(&1).is_none(),
            "the one-shot on an empty shard should be the overflow victim"
        );
    }

    #[test]
    fn concurrent_overflow_inserts_leave_one_resident() {
        let cache = Arc::new(cache(100, Duration::from_mins(1)));
        let left = Arc::clone(&cache);
        let right = Arc::clone(&cache);
        let a = std::thread::spawn(move || {
            left.insert(0, TestValue::with_size("a", 100), 100);
        });
        let b = std::thread::spawn(move || {
            right.insert(1, TestValue::with_size("b", 100), 100);
        });
        a.join().expect("thread panicked");
        b.join().expect("thread panicked");
        assert!(cache.weighted_size() <= 100);
        assert_eq!(
            cache.len(),
            1,
            "two concurrent overflow inserts must not each evict a victim"
        );
    }

    #[test]
    fn concurrent_insert_remove_leaves_zero_weight_when_empty() {
        let cache = Arc::new(cache(10_000_000, Duration::from_mins(1)));
        let mut handles = Vec::new();
        for thread_id in 0..8u64 {
            let cache = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for i in 0..2_000u64 {
                    let key = thread_id.saturating_mul(10_000) + (i % 64);
                    cache.insert(key, TestValue::with_size("x", 10), 10);
                    if i.is_multiple_of(3) {
                        cache.remove(&key);
                    } else if i.is_multiple_of(2) {
                        cache.insert(key, TestValue::with_size("y", 7), 7);
                    }
                }
            }));
        }
        for handle in handles {
            handle.join().expect("thread panicked");
        }
        for key in cache.iter_keys() {
            cache.remove(&key);
        }
        assert_eq!(cache.len(), 0);
        assert_eq!(
            cache.weighted_size(),
            0,
            "every insert's weight must be published before a concurrent remove can subtract"
        );
    }
}
