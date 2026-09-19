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

//! Sharded in-memory cache with LRU, LFU, and W-`TinyLFU` eviction.
//!
//! # Sharding
//!
//! The cache is split into [`NUM_SHARDS`] (16) independent shards. A key maps
//! to shard **`key % 16`** ([`shard_index`]). Keys are pre-hashed `u64` values;
//! each shard's map uses an identity hasher so they are not hashed again.
//!
//! # Get is non-destructive (short-lock + buffered promote)
//!
//! [`ShardedCache::get`] never removes an entry to serve a hit. Under the shard
//! `parking_lot::Mutex` a hit only looks up, bumps frequency / sketch metadata,
//! and `Arc::clone`s the value handle — then unlocks. The fat `V` clone happens
//! after unlock. Region relinks (LRU / LFU / W-TinyLFU) are enqueued into a
//! per-shard touch buffer and applied from [`ShardedCache::run_pending_tasks`],
//! [`ShardedCache::insert`], and a best-effort drain on the get path (Moka-like
//! buffered ops: concurrent touch order may be slightly looser; differential
//! value agreement still holds). Two concurrent hits on the same key both
//! succeed. This is the hard gate against `Pingora`'s remove-and-re-admit path
//! (spiceai/spiceai#12985).
//!
//! # Table invalidation
//!
//! [`ShardedCache::invalidate_matching`] scans each shard in place and does not
//! promote survivors, so a refresh cannot rewrite recency as scan order. A
//! write-epoch plus `invalidate_gate` handshake re-scans until stable so an
//! insert into an already-walked shard cannot survive the return.

mod hasher;
mod shard;
mod sketch;

use parking_lot::{Mutex, RwLock};
use shard::{GetOutcome, Shard};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
    /// Least-recently-used *within each shard*. Overflow trim walks other
    /// shards from a rotating hand and evicts the first non-empty tail, so
    /// the victim is not chosen by a global LRU timestamp comparison.
    #[default]
    Lru,
    /// Least-frequently-used. Each hit increments a per-entry counter; overflow
    /// trim removes the **global** lowest-frequency resident (full per-shard
    /// scan under the trim lock). Other shards are tried first so a just-
    /// admitted key is not self-evicted. Ties keep the colder list position.
    Lfu,
    /// Full W-`TinyLFU` (Caffeine/Moka shape): a ~1% LRU **window**, a ~99%
    /// SLRU **main** space split into ~20% **probation** / ~80% **protected**,
    /// and a Count-Min Sketch. New keys enter the window; the window LRU
    /// competes with the probation LRU on frequency before entering main. A
    /// hit on probation promotes into protected; protected overflow demotes
    /// back to probation.
    ///
    /// Segment caps (`window_cap` / `protected_cap`) and the matching weight
    /// counters are **cache-wide**, derived from the global `max_weight`, not
    /// per-shard. Sharding still partitions the lists and locks; only the
    /// capacity accounting is shared.
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

/// Soft cap on buffered touches per shard. Excess oldest touches are dropped
/// (Moka-like): eviction heuristics tolerate lost promotions under overload.
const TOUCH_DRAIN_THRESHOLD: usize = 64;
const TOUCH_BUFFER_CAP: usize = 1024;
/// Region order the guaranteed-progress fallback reclaims from: coldest main
/// space first, then new admissions, and only then the protected segment.
const FALLBACK_EVICT_ORDER: [shard::Region; 3] = [
    shard::Region::Probation,
    shard::Region::Window,
    shard::Region::Protected,
];

#[repr(align(64))]
struct TouchBuffer {
    keys: Mutex<Vec<u64>>,
}

#[repr(align(64))]
struct CachePadded<T>(T);

/// Sharded cache keyed by pre-hashed `u64` values.
pub struct ShardedCache<V, L: EvictionListener = NoopListener> {
    shards: Box<[CachePadded<Mutex<Shard<V>>>; NUM_SHARDS]>,
    /// Per-shard buffered touches (key ids). Separate from the shard data
    /// mutex so get can unlock the map before enqueueing / draining promotes.
    touch_buffers: Box<[CachePadded<TouchBuffer>; NUM_SHARDS]>,
    max_weight: u64,
    ttl: Duration,
    policy: EvictionPolicy,
    weight: AtomicU64,
    /// Bytes currently in the W-TinyLFU admission window (~1% cap).
    window_weight: AtomicU64,
    /// Bytes currently in the W-TinyLFU protected segment (~80% of main).
    protected_weight: AtomicU64,
    /// Serializes overflow trimming so two concurrent inserts cannot each
    /// evict a victim after a single removal would already restore the budget.
    trim: Mutex<()>,
    /// Bumped after a write publishes a new resident (`insert` / successful
    /// `replace_if`). [`Self::invalidate_matching`] re-scans while this moves.
    write_epoch: AtomicU64,
    /// Shared by writers (`read`) and the stable-check at the end of
    /// [`Self::invalidate_matching`] (`write`) so an in-flight insert cannot
    /// publish between the last scan and return.
    invalidate_gate: RwLock<()>,
    /// Rotating start shard for cross-shard eviction / demotion / expired-tail
    /// scans so shard 0 does not permanently absorb eviction pressure.
    hand: AtomicUsize,
    /// Test-only: wait after publishing weight and before overflow trim so a
    /// two-thread test can force both inserts to observe the overflow.
    #[cfg(test)]
    after_publish: Mutex<Option<std::sync::Arc<std::sync::Barrier>>>,
    /// Test-only: run at the start of each overflow-trim iteration, and again
    /// after `TinyLFU` snapshots a tail and before that tail is unlinked.
    #[cfg(test)]
    before_size_victim: Mutex<Option<std::sync::Arc<dyn Fn() + Send + Sync>>>,
    /// Test-only: run after a size victim is chosen and before its budget is
    /// claimed, so a concurrent `remove` can restore the limit in the window
    /// that used to unlink-then-rollback.
    #[cfg(test)]
    before_claim_size: Mutex<Option<std::sync::Arc<dyn Fn() + Send + Sync>>>,
    /// Test-only: run after each shard is invalidated and unlocked, so a test
    /// can insert into an already-scanned shard during the multi-shard walk.
    #[cfg(test)]
    after_invalidate_shard: Mutex<Option<std::sync::Arc<dyn Fn(usize) + Send + Sync>>>,
    _listener: std::marker::PhantomData<L>,
}

impl<V: Clone + Send + Sync + 'static, L: EvictionListener> ShardedCache<V, L> {
    /// Create a cache with a byte budget of `max_weight` and a per-entry TTL.
    #[must_use]
    pub fn new(max_weight: u64, ttl: Duration, policy: EvictionPolicy) -> Self {
        Self {
            shards: Box::new(core::array::from_fn(|_| {
                CachePadded(Mutex::new(Shard::new(policy)))
            })),
            touch_buffers: Box::new(core::array::from_fn(|_| {
                CachePadded(TouchBuffer {
                    keys: Mutex::new(Vec::new()),
                })
            })),
            max_weight,
            ttl,
            policy,
            weight: AtomicU64::new(0),
            window_weight: AtomicU64::new(0),
            protected_weight: AtomicU64::new(0),
            trim: Mutex::new(()),
            write_epoch: AtomicU64::new(0),
            invalidate_gate: RwLock::new(()),
            hand: AtomicUsize::new(0),
            #[cfg(test)]
            after_publish: Mutex::new(None),
            #[cfg(test)]
            before_size_victim: Mutex::new(None),
            #[cfg(test)]
            before_claim_size: Mutex::new(None),
            #[cfg(test)]
            after_invalidate_shard: Mutex::new(None),
            _listener: std::marker::PhantomData,
        }
    }

    /// Insert `value` under `key` with the given byte `weight`.
    ///
    /// A value heavier than `max_weight` is rejected before admission so it
    /// cannot flush unrelated residents and then self-evict. If `key` already
    /// has a resident, that stale generation is removed. Under W-`TinyLFU` the
    /// key enters the admission window; overflow trim may reject it if the
    /// window LRU loses the frequency comparison against the probation victim.
    pub fn insert(&self, key: u64, value: V, weight: usize) {
        let weight = u64::try_from(weight).unwrap_or(u64::MAX);
        let shard_idx = shard_index(key);
        // Hold the invalidate gate across publish so `invalidate_matching`
        // cannot observe a stable epoch while this write is still invisible.
        {
            let _gate = self.invalidate_gate.read();
            // Apply deferred get-path promotes before admission so eviction sees
            // up-to-date region / frequency state for this shard.
            self.drain_touches_blocking(shard_idx);
            let mut shard = self.shards[shard_idx].0.lock();
            if weight > self.max_weight {
                // Reject under the shard lock. A concurrent fitting insert of the
                // same key must not land between the weight check and this remove,
                // or an uncacheable admit would delete a newer valid result.
                let before_window = shard.window_weight();
                let before_protected = shard.protected_weight();
                let removed = shard.remove(key);
                if let Some((_, old_weight)) = &removed {
                    self.sub_weight(*old_weight);
                    self.sync_segment_weights_after_removal(
                        before_window,
                        shard.window_weight(),
                        before_protected,
                        shard.protected_weight(),
                    );
                }
                // Release the shard before dropping V (stale and/or rejected) so a
                // costly or re-entrant Drop cannot stall this shard.
                drop(shard);
                drop(removed);
                drop(value);
                return;
            }
            // Sample TTL after the shard lock so wait time is not charged to the
            // entry (and so TinyLFU can expire this shard before admission).
            let now = Instant::now();

            let mut expired = Vec::new();
            if matches!(self.policy, EvictionPolicy::TinyLfu) {
                let before_window = shard.window_weight();
                let before_protected = shard.protected_weight();
                let (values, expired_weight) = shard.expire_older_than(now, self.ttl);
                if expired_weight > 0 {
                    self.sub_weight(expired_weight);
                }
                self.sync_segment_weights_after_removal(
                    before_window,
                    shard.window_weight(),
                    before_protected,
                    shard.protected_weight(),
                );
                expired = values;
                shard.increment_sketch(key);
            }

            let (delta, replaced) = shard.insert(key, value, weight, now);
            // Publish the weight before releasing the shard so a concurrent
            // remove of this key cannot subtract before the matching add.
            self.apply_delta(&delta);
            drop(shard);
            self.note_write();
            drop(replaced);
            for _ in expired {
                L::on_evict(EvictionReason::Expired);
            }
            #[cfg(test)]
            self.wait_after_publish();
        }
        self.evict_to_limit(shard_idx, Some(key));
    }

    /// Return a shared handle for `key` if it is present and unexpired.
    ///
    /// Never removes a live entry. An expired entry is dropped and reported
    /// as [`EvictionReason::Expired`].
    ///
    /// # Short lock + buffered promote
    ///
    /// Under the shard mutex this only looks up, bumps LFU/sketch metadata,
    /// and `Arc::clone`s the resident handle — then unlocks. Callers that need
    /// an owned `V` clone outside this path (or keep the `Arc`). LRU / LFU /
    /// W-TinyLFU region relinks are recorded in a per-shard touch buffer and
    /// applied from [`Self::run_pending_tasks`], [`Self::insert`], and a
    /// best-effort drain on this path. Concurrent touch order may be slightly
    /// looser (Moka-like buffered ops); differential value agreement still
    /// holds. Demotion / trim stay off the get hot path (`try_lock` / after
    /// unlock).
    pub fn get(&self, key: &u64) -> Option<std::sync::Arc<V>> {
        let shard_idx = shard_index(*key);
        let handle = {
            let mut shard = self.shards[shard_idx].0.lock();
            let now = Instant::now();
            if matches!(self.policy, EvictionPolicy::TinyLfu) {
                shard.increment_sketch(*key);
            }
            let before_window = shard.window_weight();
            let before_protected = shard.protected_weight();
            match shard.get(*key, now, self.ttl) {
                GetOutcome::Hit(handle) => handle,
                GetOutcome::Miss => return None,
                GetOutcome::Expired { value, weight } => {
                    self.sub_weight(weight);
                    self.sync_segment_weights_after_removal(
                        before_window,
                        shard.window_weight(),
                        before_protected,
                        shard.protected_weight(),
                    );
                    drop(shard);
                    drop(value);
                    L::on_evict(EvictionReason::Expired);
                    return None;
                }
            }
        };
        self.record_touch(shard_idx, *key);
        // Promote is off the get latency path: best-effort drain only once
        // the per-shard buffer reaches TOUCH_DRAIN_THRESHOLD (`try_lock`).
        // Insert and `run_pending_tasks` drain the rest (Moka-like buffered ops).
        self.maybe_drain_touches(shard_idx);
        Some(handle)
    }

    /// Replace the resident at `key` when `should_replace` accepts it.
    ///
    /// When `keep_ttl` is true the existing insertion timestamp is preserved so
    /// the entry's remaining lifetime does not restart.
    ///
    /// A replacement heavier than `max_weight` is refused before the shard is
    /// locked, for the reason [`Self::insert`] rejects one: admitting it would
    /// trim unrelated residents on the way to self-evicting. Unlike `insert`,
    /// the resident already under `key` is left in place — a replacement that
    /// cannot be held is not a reason to drop the copy that can be.
    pub fn replace_if<F>(
        &self,
        key: u64,
        value: V,
        weight: usize,
        keep_ttl: bool,
        should_replace: F,
    ) -> bool
    where
        F: FnOnce(&V) -> bool,
    {
        let weight = u64::try_from(weight).unwrap_or(u64::MAX);
        if weight > self.max_weight {
            return false;
        }
        let shard_idx = shard_index(key);
        let replaced = {
            let _gate = self.invalidate_gate.read();
            self.drain_touches_blocking(shard_idx);
            let mut shard = self.shards[shard_idx].0.lock();
            let now = Instant::now();
            let Some((replaced, delta, old)) =
                shard.replace_if(key, value, weight, now, keep_ttl, should_replace)
            else {
                return false;
            };
            if replaced {
                self.apply_delta(&delta);
                self.note_write();
            }
            drop(shard);
            drop(old);
            replaced
        };
        if replaced && self.weight.load(Ordering::Relaxed) > self.max_weight {
            self.evict_to_limit(shard_idx, Some(key));
        }
        replaced
    }

    /// Remove `key` if present. This is not an eviction and is not reported.
    pub fn remove(&self, key: &u64) -> Option<V> {
        let mut shard = self.shards[shard_index(*key)].0.lock();
        let before_window = shard.window_weight();
        let before_protected = shard.protected_weight();
        let (value, weight) = shard.remove(*key)?;
        self.sub_weight(weight);
        self.sync_segment_weights_after_removal(
            before_window,
            shard.window_weight(),
            before_protected,
            shard.protected_weight(),
        );
        drop(shard);
        Some(value)
    }

    /// Drop every entry. Not reported as evictions.
    pub fn clear(&self) {
        // Lock order is shard 0..N so a concurrent `clear` cannot deadlock.
        // Keep the removed values alive until every shard lock is released:
        // a `Drop` that re-enters this cache would otherwise deadlock, and
        // dropping Arrow-backed values would extend the all-shards hold.
        let mut guards: Vec<_> = self.shards.iter().map(|s| s.0.lock()).collect();
        let mut values = Vec::new();
        let mut removed: u64 = 0;
        for guard in &mut guards {
            let (shard_values, weight) = guard.take_all();
            removed = removed.saturating_add(weight);
            values.extend(shard_values);
        }
        self.sub_weight(removed);
        self.window_weight.store(0, Ordering::Relaxed);
        self.protected_weight.store(0, Ordering::Relaxed);
        for buf in self.touch_buffers.iter() {
            buf.0.keys.lock().clear();
        }
        // Release every shard before dropping values. A value destructor that
        // re-enters the cache would otherwise deadlock on these non-reentrant
        // locks, and large Arrow-backed values would extend the hold.
        drop(guards);
        drop(values);
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

    /// Test helper: global W-TinyLFU window segment weight.
    #[cfg(test)]
    #[must_use]
    pub fn window_weight_for_test(&self) -> u64 {
        self.window_weight.load(Ordering::Relaxed)
    }

    /// Test helper: global W-TinyLFU protected segment weight.
    #[cfg(test)]
    #[must_use]
    pub fn protected_weight_for_test(&self) -> u64 {
        self.protected_weight.load(Ordering::Relaxed)
    }

    /// Expire stale entries, apply buffered promotes, and evict down to `max_weight`.
    ///
    /// Drains every shard's touch buffer so region relinks deferred from
    /// [`Self::get`] are applied before expiry / size maintenance.
    pub fn run_pending_tasks(&self) {
        for shard_idx in 0..NUM_SHARDS {
            self.drain_touches_blocking(shard_idx);
        }
        for shard_idx in 0..NUM_SHARDS {
            let mut shard = self.shards[shard_idx].0.lock();
            let now = Instant::now();
            let before_window = shard.window_weight();
            let before_protected = shard.protected_weight();
            let (expired, weight) = shard.expire_older_than(now, self.ttl);
            self.sub_weight(weight);
            self.sync_segment_weights_after_removal(
                before_window,
                shard.window_weight(),
                before_protected,
                shard.protected_weight(),
            );
            drop(shard);
            for _ in expired {
                L::on_evict(EvictionReason::Expired);
            }
        }
        self.evict_to_limit(0, None);
    }

    /// Drop every entry whose value satisfies `predicate`.
    ///
    /// Survivors are not promoted. Returns how many entries were removed.
    ///
    /// Shards are still unlocked between steps of a single pass (so readers
    /// and writers on other shards are not stalled for the whole walk), but
    /// the pass re-runs until `write_epoch` is stable under `invalidate_gate`.
    /// An insert into an already-scanned shard therefore cannot survive this
    /// return: either it published before the stable check (rescan removes
    /// it) or it is blocked on the gate until after return (post-invalidation
    /// write).
    pub fn invalidate_matching<F>(&self, predicate: F) -> usize
    where
        F: Fn(&V) -> bool,
    {
        let mut removed = 0;
        loop {
            let start = self.write_epoch.load(Ordering::Acquire);
            removed += self.invalidate_matching_once(&predicate);
            // Wait for in-flight writers and block new publishes before the
            // epoch comparison so nothing can land between "stable" and return.
            let _gate = self.invalidate_gate.write();
            if self.write_epoch.load(Ordering::Acquire) == start {
                return removed;
            }
        }
    }

    fn invalidate_matching_once<F>(&self, predicate: &F) -> usize
    where
        F: Fn(&V) -> bool,
    {
        let mut removed = 0;
        for shard_idx in 0..NUM_SHARDS {
            let mut shard = self.shards[shard_idx].0.lock();
            let before_window = shard.window_weight();
            let before_protected = shard.protected_weight();
            let (values, weight) = shard.invalidate_matching(predicate);
            self.sub_weight(weight);
            self.sync_segment_weights_after_removal(
                before_window,
                shard.window_weight(),
                before_protected,
                shard.protected_weight(),
            );
            drop(shard);
            #[cfg(test)]
            self.run_after_invalidate_shard(shard_idx);
            removed += values.len();
            for _ in values {
                L::on_evict(EvictionReason::Invalidated);
            }
        }
        removed
    }

    /// Record that a resident was published so [`Self::invalidate_matching`]
    /// can detect a concurrent write and rescan.
    fn note_write(&self) {
        self.write_epoch.fetch_add(1, Ordering::Release);
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

    /// Overflow trim. The outer weight check is a fast path; the limit is
    /// rechecked under `trim` before each victim so two concurrent inserts
    /// cannot each evict after one removal would restore the budget.
    ///
    /// Removed values and listener callbacks are applied only after `trim` is
    /// released. A `Drop` or listener that re-enters [`Self::insert`] would
    /// otherwise deadlock on this non-reentrant mutex.
    fn needs_overflow_trim(&self) -> bool {
        if self.weight.load(Ordering::Relaxed) > self.max_weight {
            return true;
        }
        matches!(self.policy, EvictionPolicy::TinyLfu)
            && (self.window_weight.load(Ordering::Relaxed) > self.window_cap()
                || self.protected_weight.load(Ordering::Relaxed) > self.protected_cap())
    }

    fn evict_to_limit(&self, prefer: usize, admitted: Option<u64>) {
        if !self.needs_overflow_trim() {
            return;
        }
        let mut evicted = Vec::new();
        {
            let _trim = self.trim.lock();
            while self.needs_overflow_trim() {
                #[cfg(test)]
                self.run_before_size_victim();
                // Apply deferred get-path promotes before choosing a victim.
                self.drain_all_touches();
                if !self.needs_overflow_trim() {
                    break;
                }
                // Reclaim expired tails before a live size victim. Peeking 16
                // tails is bounded; `expire_older_than` runs only on shards
                // whose tail is already stale, not a full-cache scan.
                if self.expire_expired_tails(&mut evicted) {
                    continue;
                }
                let progressed = match self.policy {
                    EvictionPolicy::TinyLfu => self.evict_wtinylfu_one(admitted, &mut evicted),
                    EvictionPolicy::Lfu => self.evict_lfu_one(prefer, admitted, &mut evicted),
                    // LRU must not self-evict a just-admitted sole resident while
                    // an older victim exists on another shard.
                    EvictionPolicy::Lru => self.evict_others_then_prefer(prefer, &mut evicted),
                };
                if !progressed {
                    break;
                }
            }
        }
        for (value, reason) in evicted {
            drop(value);
            L::on_evict(reason);
        }
    }

    /// Window capacity: ~1% of `max_weight` (at least 1 byte).
    fn window_cap(&self) -> u64 {
        (self.max_weight / 100).max(1)
    }

    /// Protected capacity: ~80% of the main space (main = total − window).
    fn protected_cap(&self) -> u64 {
        let main = self.max_weight.saturating_sub(self.window_cap());
        (main.saturating_mul(80) / 100).max(1)
    }

    /// Full W-TinyLFU overflow step:
    /// 1. If the window is over its cap (or the cache is over budget), take the
    ///    window LRU as a candidate and the probation LRU as the victim.
    /// 2. Admit the candidate into probation when its CMS estimate is ≥ the
    ///    victim's; otherwise reject the candidate.
    /// 3. Demote protected → probation when the protected segment is over cap.
    /// 4. If still over budget, size-evict a probation (then window) tail.
    fn evict_wtinylfu_one(
        &self,
        _admitted: Option<u64>,
        evicted: &mut Vec<(V, EvictionReason)>,
    ) -> bool {
        // Prefer draining an oversized window via admission before cold size trim.
        if (self.window_weight.load(Ordering::Relaxed) > self.window_cap()
            || self.weight.load(Ordering::Relaxed) > self.max_weight)
            && self.wtinylfu_admit_or_reject_window_candidate(evicted)
        {
            return true;
        }
        if self.protected_weight.load(Ordering::Relaxed) > self.protected_cap()
            && self.demote_one_protected(evicted)
        {
            // Demotion is not a size eviction; keep looping so a later step
            // can reclaim bytes if still over budget.
            return true;
        }
        // Size-evict from main (probation) first, then the window.
        for _ in 0..NUM_SHARDS {
            if self.weight.load(Ordering::Relaxed) <= self.max_weight {
                return false;
            }
            if let Some((shard_idx, key, _)) =
                self.lowest_freq_region_tail(shard::Region::Probation)
            {
                #[cfg(test)]
                self.run_before_size_victim();
                if self.remove_tail_for_size(shard_idx, key, shard::Region::Probation, evicted) {
                    return true;
                }
                continue;
            }
            break;
        }
        for _ in 0..NUM_SHARDS {
            if self.weight.load(Ordering::Relaxed) <= self.max_weight {
                return false;
            }
            if let Some((shard_idx, key, _)) = self.lowest_freq_region_tail(shard::Region::Window) {
                #[cfg(test)]
                self.run_before_size_victim();
                if self.remove_tail_for_size(shard_idx, key, shard::Region::Window, evicted) {
                    return true;
                }
                continue;
            }
            break;
        }
        // Guaranteed-progress fallback: if every TinyLFU snapshot was promoted
        // off its tail before unlink, fall back to numeric-order LRU so insert
        // cannot return above max_weight.
        if self.weight.load(Ordering::Relaxed) > self.max_weight {
            return self.evict_others_then_prefer(0, evicted);
        }
        false
    }

    /// Window LRU vs probation LRU `TinyLFU` comparison.
    ///
    /// When the cache still fits `max_weight`, the window LRU is moved into
    /// probation (no resident leaves). Only when the total is over budget does
    /// the candidate compete with the probation victim on CMS frequency.
    fn wtinylfu_admit_or_reject_window_candidate(
        &self,
        evicted: &mut Vec<(V, EvictionReason)>,
    ) -> bool {
        let Some((cand_shard, cand_key, cand_freq)) =
            self.lowest_freq_region_tail(shard::Region::Window)
        else {
            return false;
        };
        // After the snapshot is published and before the candidate is unlinked
        // or moved, so a concurrent get can promote / change the region.
        #[cfg(test)]
        self.run_before_size_victim();
        // Concurrent gets (incl. the test hook) may have buffered promotes on
        // any shard; apply them before frequency compare / victim choice.
        self.drain_all_touches();

        let total_over = self.weight.load(Ordering::Relaxed) > self.max_weight;
        let victim = self.lowest_freq_region_tail(shard::Region::Probation);

        if !total_over {
            // Drain window toward its cap without discarding anyone.
            return self.promote_window_tail(cand_shard, cand_key);
        }

        match victim {
            None => self.promote_window_tail(cand_shard, cand_key),
            Some((vic_shard, vic_key, vic_freq)) => {
                if cand_freq >= vic_freq {
                    // Evict victim, then promote candidate into probation.
                    if !self.remove_tail_for_size(
                        vic_shard,
                        vic_key,
                        shard::Region::Probation,
                        evicted,
                    ) {
                        // Victim gone or claim failed; try promoting anyway if
                        // the budget was restored, else reject nothing this pass.
                        if self.weight.load(Ordering::Relaxed) <= self.max_weight {
                            return self.promote_window_key(cand_shard, cand_key);
                        }
                        return false;
                    }
                    let _ = self.promote_window_key(cand_shard, cand_key);
                    true
                } else if cand_key == vic_key {
                    // Same entry cannot be both; just promote.
                    self.promote_window_tail(cand_shard, cand_key)
                } else {
                    // Reject the window candidate.
                    self.remove_tail_for_size(cand_shard, cand_key, shard::Region::Window, evicted)
                }
            }
        }
    }

    /// Move a window resident into probation, re-snapshotting the window tail
    /// when a concurrent hit moves `key` off it.
    ///
    /// [`Self::promote_window_key`] refuses a stale snapshot so the caller can
    /// retry. Handing that `false` straight back to [`Self::evict_to_limit`]
    /// reads as "no progress" and breaks the trim loop, leaving the window
    /// above `window_cap` until some later operation happens to trim it. Retry
    /// against the current tail instead, bounded by the shard count.
    fn promote_window_tail(&self, shard_idx: usize, key: u64) -> bool {
        if self.promote_window_key(shard_idx, key) {
            return true;
        }
        for _ in 0..NUM_SHARDS {
            let Some((next_shard, next_key, _)) =
                self.lowest_freq_region_tail(shard::Region::Window)
            else {
                return false;
            };
            if self.promote_window_key(next_shard, next_key) {
                return true;
            }
        }
        false
    }

    fn promote_window_key(&self, shard_idx: usize, key: u64) -> bool {
        let mut shard = self.shards[shard_idx].0.lock();
        // `drain_all_touches` may have run after the window-tail snapshot was
        // taken; a concurrent hit can move that key off the tail. Only promote
        // when `key` is still the current window LRU — otherwise return false
        // so the caller re-snapshots and retries.
        let Some(tail_key) = shard.region_tail_key(shard::Region::Window) else {
            return false;
        };
        if tail_key != key {
            return false;
        }
        if shard.peek_region(key) != Some(shard::Region::Window) {
            return false;
        }
        let weight = shard.peek_weight(key).unwrap_or(0);
        if shard.move_window_to_probation(key) {
            let _ =
                self.window_weight
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(weight))
                    });
            return true;
        }
        false
    }

    fn demote_one_protected(&self, _evicted: &mut Vec<(V, EvictionReason)>) -> bool {
        // Pick any shard with a protected tail (rotating hand).
        let start = self.next_hand_start();
        for offset in 0..NUM_SHARDS {
            let shard_idx = (start + offset) % NUM_SHARDS;
            let mut shard = self.shards[shard_idx].0.lock();
            let Some((key, weight)) = shard.demote_protected_lru_to_probation() else {
                continue;
            };
            let _ = key;
            self.protected_weight.fetch_sub(weight, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Record a get-path touch for later region relink.
    fn record_touch(&self, shard_idx: usize, key: u64) {
        let mut buf = self.touch_buffers[shard_idx].0.keys.lock();
        if buf.len() >= TOUCH_BUFFER_CAP {
            // Drop oldest under overload (Moka-like); value agreement unaffected.
            let overflow = buf.len() + 1 - TOUCH_BUFFER_CAP;
            buf.drain(0..overflow);
        }
        buf.push(key);
    }

    fn maybe_drain_touches(&self, shard_idx: usize) {
        let pending = self.touch_buffers[shard_idx].0.keys.lock().len();
        if pending < TOUCH_DRAIN_THRESHOLD {
            return;
        }
        self.drain_touches_try(shard_idx);
    }

    fn take_touch_batch(&self, shard_idx: usize) -> Vec<u64> {
        let mut buf = self.touch_buffers[shard_idx].0.keys.lock();
        std::mem::take(&mut *buf)
    }

    fn requeue_touches(&self, shard_idx: usize, keys: &[u64]) {
        if keys.is_empty() {
            return;
        }
        let mut buf = self.touch_buffers[shard_idx].0.keys.lock();
        // Prefer newer touches if requeue would exceed the soft cap.
        let room = TOUCH_BUFFER_CAP.saturating_sub(buf.len());
        if room == 0 {
            return;
        }
        let start = keys.len().saturating_sub(room);
        buf.extend_from_slice(&keys[start..]);
    }

    fn apply_touch_batch(&self, shard_idx: usize, keys: &[u64]) {
        if keys.is_empty() {
            return;
        }
        let mut shard = self.shards[shard_idx].0.lock();
        let before_protected = shard.protected_weight();
        for &key in keys {
            shard.apply_touch(key);
        }
        let after_protected = shard.protected_weight();
        if after_protected > before_protected {
            self.protected_weight
                .fetch_add(after_protected - before_protected, Ordering::Relaxed);
        } else if before_protected > after_protected {
            self.protected_weight
                .fetch_sub(before_protected - after_protected, Ordering::Relaxed);
        }
        drop(shard);
        if matches!(self.policy, EvictionPolicy::TinyLfu) {
            self.demote_protected_if_over_cap();
        }
    }

    fn drain_touches_try(&self, shard_idx: usize) {
        let batch = self.take_touch_batch(shard_idx);
        if batch.is_empty() {
            return;
        }
        let Some(mut shard) = self.shards[shard_idx].0.try_lock() else {
            self.requeue_touches(shard_idx, &batch);
            return;
        };
        let before_protected = shard.protected_weight();
        for &key in &batch {
            shard.apply_touch(key);
        }
        let after_protected = shard.protected_weight();
        if after_protected > before_protected {
            self.protected_weight
                .fetch_add(after_protected - before_protected, Ordering::Relaxed);
        } else if before_protected > after_protected {
            self.protected_weight
                .fetch_sub(before_protected - after_protected, Ordering::Relaxed);
        }
        drop(shard);
        if matches!(self.policy, EvictionPolicy::TinyLfu) {
            self.demote_protected_if_over_cap();
        }
    }

    fn drain_touches_blocking(&self, shard_idx: usize) {
        let batch = self.take_touch_batch(shard_idx);
        self.apply_touch_batch(shard_idx, &batch);
    }

    fn drain_all_touches(&self) {
        for shard_idx in 0..NUM_SHARDS {
            self.drain_touches_blocking(shard_idx);
        }
    }

    /// Demote protected → probation while the global protected segment is over cap.
    ///
    /// Uses `try_lock` on `trim` so a hit that promotes into protected cannot
    /// deadlock with an in-flight `insert` that already holds `trim` and is
    /// waiting on this shard.
    fn demote_protected_if_over_cap(&self) {
        // Cheap shared load first: steady-state hits must not RMW the
        // cache-wide `trim` mutex when protected is already under cap.
        if self.protected_weight.load(Ordering::Relaxed) <= self.protected_cap() {
            return;
        }
        let Some(_trim) = self.trim.try_lock() else {
            return;
        };
        while self.protected_weight.load(Ordering::Relaxed) > self.protected_cap() {
            let mut progressed = false;
            for shard_idx in 0..NUM_SHARDS {
                if self.protected_weight.load(Ordering::Relaxed) <= self.protected_cap() {
                    break;
                }
                let mut shard = self.shards[shard_idx].0.lock();
                let Some((_key, weight)) = shard.demote_protected_lru_to_probation() else {
                    continue;
                };
                self.protected_weight.fetch_sub(weight, Ordering::Relaxed);
                progressed = true;
            }
            if !progressed {
                break;
            }
        }
    }

    /// LFU overflow: remove the lowest-frequency resident, preferring other
    /// shards first so a just-admitted key is not self-evicted while an older
    /// colder victim exists. Retries when a concurrent hit raises the selected
    /// key's frequency between snapshot and unlink.
    fn evict_lfu_one(
        &self,
        prefer: usize,
        admitted: Option<u64>,
        evicted: &mut Vec<(V, EvictionReason)>,
    ) -> bool {
        for _ in 0..NUM_SHARDS {
            // Every shard is compared on every pass, because `caching_policy:
            // lfu` promises the global lowest hit-count resident. Only the key
            // just admitted is held back — excluding its whole shard would pass
            // over the coldest resident whenever it happens to live there — and
            // even that is reconsidered when nothing else can be reclaimed.
            let victim = self
                .lowest_freq_entry(admitted)
                .or_else(|| self.lowest_freq_entry(None));
            let Some((shard_idx, key, freq)) = victim else {
                return false;
            };
            if self.remove_lfu_key_for_size(shard_idx, key, freq, evicted) {
                return true;
            }
            // Selected key got hotter under a concurrent hit; reselect.
        }
        // Every attempt lost its revalidation race. Fall back to the same
        // guaranteed-progress path W-`TinyLFU` uses, so an insert cannot
        // return with the cache still above `max_weight`.
        self.evict_others_then_prefer(prefer, evicted)
    }

    /// Lowest-frequency entry across every shard (full per-shard scan). When
    /// `exclude_key` is set, that one key is passed over on the first pass so
    /// admission does not self-evict; the caller retries with `None` when
    /// nothing else can be reclaimed. Shard walk starts at the rotating hand.
    fn lowest_freq_entry(&self, exclude_key: Option<u64>) -> Option<(usize, u64, u16)> {
        let mut best: Option<(usize, u64, u16)> = None;
        let start = self.next_hand_start();
        for offset in 0..NUM_SHARDS {
            let shard_idx = (start + offset) % NUM_SHARDS;
            let shard = self.shards[shard_idx].0.lock();
            let Some((key, _weight, freq)) = shard.peek_lfu_victim(exclude_key) else {
                continue;
            };
            let take = best.is_none_or(|(_, _, best_freq)| freq < best_freq);
            if take {
                best = Some((shard_idx, key, freq));
            }
        }
        best
    }

    /// The region-tail with the lowest home-shard sketch estimate. Shard walk
    /// starts at the rotating hand.
    fn lowest_freq_region_tail(&self, region: shard::Region) -> Option<(usize, u64, u8)> {
        let mut best: Option<(usize, u64, u8)> = None;
        let start = self.next_hand_start();
        for offset in 0..NUM_SHARDS {
            let shard_idx = (start + offset) % NUM_SHARDS;
            let shard = self.shards[shard_idx].0.lock();
            let Some(key) = shard.region_tail_key(region) else {
                continue;
            };
            let freq = shard.sketch_estimate(key);
            let take = best.is_none_or(|(_, _, best_freq)| freq < best_freq);
            if take {
                best = Some((shard_idx, key, freq));
            }
        }
        best
    }

    /// Expire every shard whose LRU tail is already stale. Bounded: one tail
    /// peek per shard, and a slot walk only on shards that have an expired tail.
    /// Shard walk starts at the rotating hand.
    fn expire_expired_tails(&self, evicted: &mut Vec<(V, EvictionReason)>) -> bool {
        let now = Instant::now();
        let mut progressed = false;
        let start = self.next_hand_start();
        for offset in 0..NUM_SHARDS {
            let shard_idx = (start + offset) % NUM_SHARDS;
            let mut shard = self.shards[shard_idx].0.lock();
            if !shard.tail_is_expired(now, self.ttl) {
                continue;
            }
            let before_window = shard.window_weight();
            let before_protected = shard.protected_weight();
            let (expired, weight) = shard.expire_older_than(now, self.ttl);
            if weight > 0 {
                self.sub_weight(weight);
            }
            self.sync_segment_weights_after_removal(
                before_window,
                shard.window_weight(),
                before_protected,
                shard.protected_weight(),
            );
            drop(shard);
            if expired.is_empty() {
                continue;
            }
            progressed = true;
            for value in expired {
                evicted.push((value, EvictionReason::Expired));
            }
        }
        progressed
    }

    /// Unlink an LFU victim after revalidating its frequency under the shard
    /// lock. A concurrent hit can raise the snapshot frequency; when that
    /// happens we return false so [`Self::evict_lfu_one`] reselects.
    fn remove_lfu_key_for_size(
        &self,
        shard_idx: usize,
        key: u64,
        expected_freq: u16,
        evicted: &mut Vec<(V, EvictionReason)>,
    ) -> bool {
        // Test hook: simulate a concurrent hit after selection, before drain.
        #[cfg(test)]
        self.run_before_size_victim();
        self.drain_touches_blocking(shard_idx);
        let mut shard = self.shards[shard_idx].0.lock();
        let now = Instant::now();
        if shard.tail_is_expired(now, self.ttl) {
            let before_window = shard.window_weight();
            let before_protected = shard.protected_weight();
            let (expired, expired_weight) = shard.expire_older_than(now, self.ttl);
            if expired_weight > 0 {
                self.sub_weight(expired_weight);
            }
            self.sync_segment_weights_after_removal(
                before_window,
                shard.window_weight(),
                before_protected,
                shard.protected_weight(),
            );
            if !expired.is_empty() {
                drop(shard);
                for value in expired {
                    evicted.push((value, EvictionReason::Expired));
                }
                return true;
            }
        }
        if self.weight.load(Ordering::Relaxed) <= self.max_weight {
            return false;
        }
        let Some(current_freq) = shard.peek_freq(key) else {
            return false;
        };
        if current_freq > expected_freq {
            // Concurrent hit made this key hotter than the selection snapshot.
            return false;
        }
        // Also refuse if another resident on this shard is now strictly colder.
        if let Some((_, _, victim_freq)) = shard.peek_lfu_victim(Some(key))
            && victim_freq < current_freq
        {
            return false;
        }
        let before_window = shard.window_weight();
        let before_protected = shard.protected_weight();
        let Some(weight) = shard.peek_weight(key) else {
            return false;
        };
        if !self.claim_size_eviction(weight) {
            return false;
        }
        let Some((value, _)) = shard.remove(key) else {
            self.weight.fetch_add(weight, Ordering::Relaxed);
            return false;
        };
        self.sync_segment_weights_after_removal(
            before_window,
            shard.window_weight(),
            before_protected,
            shard.protected_weight(),
        );
        drop(shard);
        evicted.push((value, EvictionReason::Size));
        true
    }

    /// Unlink this shard's region tail only when it is still `expected`.
    /// A concurrent get may have buffered a promote for this key; we drain
    /// that shard's touch buffer before re-checking the tail so a completed
    /// hit is visible (Moka-like: promote is deferred, but applied before
    /// size unlink). Returning false lets the caller reselect.
    fn remove_tail_for_size(
        &self,
        shard_idx: usize,
        expected: u64,
        region: shard::Region,
        evicted: &mut Vec<(V, EvictionReason)>,
    ) -> bool {
        // Apply deferred get-path promotes before trusting the snapshot tail.
        self.drain_touches_blocking(shard_idx);
        let mut shard = self.shards[shard_idx].0.lock();
        let now = Instant::now();
        if shard.tail_is_expired(now, self.ttl) {
            let before_window = shard.window_weight();
            let before_protected = shard.protected_weight();
            let (expired, expired_weight) = shard.expire_older_than(now, self.ttl);
            if expired_weight > 0 {
                self.sub_weight(expired_weight);
            }
            self.sync_segment_weights_after_removal(
                before_window,
                shard.window_weight(),
                before_protected,
                shard.protected_weight(),
            );
            if !expired.is_empty() {
                drop(shard);
                for value in expired {
                    evicted.push((value, EvictionReason::Expired));
                }
                return true;
            }
        }
        let over_budget = self.weight.load(Ordering::Relaxed) > self.max_weight;
        let over_window = matches!(self.policy, EvictionPolicy::TinyLfu)
            && self.window_weight.load(Ordering::Relaxed) > self.window_cap();
        if !over_budget && !over_window {
            return false;
        }
        if shard.region_tail_key(region) != Some(expected) {
            return false;
        }
        let before_window = shard.window_weight();
        let before_protected = shard.protected_weight();
        let Some((_, weight)) = shard.peek_region_tail(region) else {
            return false;
        };
        if over_budget {
            if !self.claim_size_eviction(weight) {
                return false;
            }
        } else {
            // Window-cap trim only: total budget already fits.
            self.sub_weight(weight);
        }
        let Some((_, value, _)) = shard.evict_region_lru(region) else {
            self.weight.fetch_add(weight, Ordering::Relaxed);
            return false;
        };
        self.sync_segment_weights_after_removal(
            before_window,
            shard.window_weight(),
            before_protected,
            shard.protected_weight(),
        );
        drop(shard);
        evicted.push((value, EvictionReason::Size));
        true
    }

    /// Subtract `victim_weight` only while the cache is still over budget.
    /// A concurrent `remove` / `clear` / invalidation can restore the limit
    /// after the `while` check; a failed claim means this victim stays.
    fn claim_size_eviction(&self, victim_weight: u64) -> bool {
        #[cfg(test)]
        self.run_before_claim_size();
        loop {
            let current = self.weight.load(Ordering::Relaxed);
            if current <= self.max_weight {
                return false;
            }
            if self
                .weight
                .compare_exchange_weak(
                    current,
                    current.saturating_sub(victim_weight),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    fn evict_others_then_prefer(
        &self,
        prefer: usize,
        evicted: &mut Vec<(V, EvictionReason)>,
    ) -> bool {
        if self.evict_first_other(prefer, evicted) {
            return true;
        }
        self.evict_one_from(prefer, evicted)
    }

    /// Evict from the first other shard (rotating hand start) that has a
    /// victim. This is not a global LRU comparison: within one pass the hand
    /// order decides which non-`prefer` tail is tried first.
    fn evict_first_other(&self, prefer: usize, evicted: &mut Vec<(V, EvictionReason)>) -> bool {
        let start = self.next_hand_start();
        (0..NUM_SHARDS)
            .map(|offset| (start + offset) % NUM_SHARDS)
            .filter(|&shard_idx| shard_idx != prefer)
            .any(|shard_idx| self.evict_one_from(shard_idx, evicted))
    }

    /// Advance and return the rotating start shard for a cross-shard pass.
    fn next_hand_start(&self) -> usize {
        self.hand.fetch_add(1, Ordering::Relaxed) % NUM_SHARDS
    }

    fn evict_one_from(&self, shard_idx: usize, evicted: &mut Vec<(V, EvictionReason)>) -> bool {
        let mut shard = self.shards[shard_idx].0.lock();
        let now = Instant::now();
        // `expire_expired_tails` already reclaimed every expired-tail shard
        // before size eviction. Only walk this shard when its tail is still
        // stale (or became stale after that pass); otherwise skip the O(n)
        // mid-list scan — those entries stay until get / checkpoint.
        if shard.tail_is_expired(now, self.ttl) {
            let before_window = shard.window_weight();
            let before_protected = shard.protected_weight();
            let (expired, expired_weight) = shard.expire_older_than(now, self.ttl);
            self.sub_weight(expired_weight);
            self.sync_segment_weights_after_removal(
                before_window,
                shard.window_weight(),
                before_protected,
                shard.protected_weight(),
            );
            if !expired.is_empty() {
                drop(shard);
                for value in expired {
                    evicted.push((value, EvictionReason::Expired));
                }
                return true;
            }
        }
        if self.weight.load(Ordering::Relaxed) <= self.max_weight {
            return false;
        }
        let before_window = shard.window_weight();
        let before_protected = shard.protected_weight();
        // This is the guaranteed-progress fallback, so it must be able to
        // reclaim from whichever region still holds residents: the probation
        // list alone, which is all this used to read, can be empty under
        // W-`TinyLFU` while the window or protected segments are not.
        let Some((region, weight)) = FALLBACK_EVICT_ORDER
            .into_iter()
            .find_map(|region| shard.peek_region_tail(region).map(|(_, w)| (region, w)))
        else {
            return false;
        };
        if !self.claim_size_eviction(weight) {
            return false;
        }
        let Some((_, value, _)) = shard.evict_region_lru(region) else {
            self.weight.fetch_add(weight, Ordering::Relaxed);
            return false;
        };
        self.sync_segment_weights_after_removal(
            before_window,
            shard.window_weight(),
            before_protected,
            shard.protected_weight(),
        );
        drop(shard);
        evicted.push((value, EvictionReason::Size));
        true
    }

    fn apply_delta(&self, delta: &shard::WeightDelta) {
        let net = delta.net();
        if net > 0 {
            let add = u64::try_from(net).unwrap_or(u64::MAX);
            self.weight.fetch_add(add, Ordering::Relaxed);
        } else if net < 0 {
            let sub = u64::try_from(-net).unwrap_or(u64::MAX);
            self.sub_weight(sub);
        }
        Self::apply_segment_delta(&self.window_weight, delta.window_net());
        Self::apply_segment_delta(&self.protected_weight, delta.protected_net());
    }

    fn apply_segment_delta(counter: &AtomicU64, net: i128) {
        if net > 0 {
            let add = u64::try_from(net).unwrap_or(u64::MAX);
            counter.fetch_add(add, Ordering::Relaxed);
        } else if net < 0 {
            let sub = u64::try_from(-net).unwrap_or(u64::MAX);
            let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(sub))
            });
        }
    }

    fn sync_segment_weights_after_removal(
        &self,
        before_window: u64,
        after_window: u64,
        before_protected: u64,
        after_protected: u64,
    ) {
        let window_lost = before_window.saturating_sub(after_window);
        let protected_lost = before_protected.saturating_sub(after_protected);
        if window_lost > 0 {
            let _ =
                self.window_weight
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(window_lost))
                    });
        }
        if protected_lost > 0 {
            let _ = self.protected_weight.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| Some(current.saturating_sub(protected_lost)),
            );
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

    #[cfg(test)]
    fn wait_after_publish(&self) {
        let barrier = {
            let guard = self.after_publish.lock();
            guard.as_ref().map(std::sync::Arc::clone)
        };
        if let Some(barrier) = barrier {
            barrier.wait();
        }
    }

    #[cfg(test)]
    fn set_after_publish_barrier(&self, barrier: std::sync::Arc<std::sync::Barrier>) {
        *self.after_publish.lock() = Some(barrier);
    }

    #[cfg(test)]
    fn run_before_size_victim(&self) {
        let hook = {
            let guard = self.before_size_victim.lock();
            guard.as_ref().map(std::sync::Arc::clone)
        };
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn set_before_size_victim<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self.before_size_victim.lock() = Some(std::sync::Arc::new(hook));
    }

    #[cfg(test)]
    fn run_before_claim_size(&self) {
        let hook = {
            let guard = self.before_claim_size.lock();
            guard.as_ref().map(std::sync::Arc::clone)
        };
        if let Some(hook) = hook {
            hook();
        }
    }

    #[cfg(test)]
    fn set_before_claim_size<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self.before_claim_size.lock() = Some(std::sync::Arc::new(hook));
    }

    #[cfg(test)]
    fn run_after_invalidate_shard(&self, shard_idx: usize) {
        let hook = {
            let guard = self.after_invalidate_shard.lock();
            guard.as_ref().map(std::sync::Arc::clone)
        };
        if let Some(hook) = hook {
            hook(shard_idx);
        }
    }

    #[cfg(test)]
    fn set_after_invalidate_shard<F>(&self, hook: F)
    where
        F: Fn(usize) + Send + Sync + 'static,
    {
        *self.after_invalidate_shard.lock() = Some(std::sync::Arc::new(hook));
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
        assert_eq!(
            cache.get(&1).map(|v| v.data.clone()),
            Some("hello".to_string())
        );
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
            cache.get(&16).map(|v| v.data.clone()),
            Some("v16".to_string()),
            "read the oldest so recency no longer matches insertion order"
        );
        cache.run_pending_tasks();
        assert_eq!(cache.keys_in_lru_order(), vec![16, 80, 64, 48, 32]);

        let removed = cache.invalidate_matching(|value| value.data == "v48");
        assert_eq!(removed, 1);
        assert_eq!(
            cache.keys_in_lru_order(),
            vec![16, 80, 64, 32],
            "survivors must keep the recency they had before the scan"
        );
    }

    /// A matching insert into an already-scanned shard during the multi-shard
    /// walk must not survive `invalidate_matching`'s return. Without the
    /// write-epoch / gate rescan, the hook below leaves key 0 resident.
    #[test]
    fn invalidate_matching_rescans_insert_into_already_scanned_shard() {
        let cache = Arc::new(cache(1024, Duration::from_mins(1)));
        // Seed a match on shard 1 so the walk continues past shard 0.
        cache.insert(1, TestValue::new("stale"), 1);
        let cache_for_hook = Arc::clone(&cache);
        let inserted = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        cache.set_after_invalidate_shard(move |shard_idx| {
            if shard_idx == 0
                && !inserted.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                // Publish into the shard that was just unlocked. The first
                // pass has already left shard 0; the epoch bump forces a
                // rescan that must drop this entry before return. Insert
                // only once so the rescan itself does not loop forever.
                cache_for_hook.insert(0, TestValue::new("stale"), 1);
            }
        });
        let removed = cache.invalidate_matching(|value| value.data == "stale");
        assert!(
            removed >= 2,
            "seed on shard 1 plus mid-scan insert on shard 0 must both be removed, got {removed}"
        );
        assert!(
            cache.get(&0).is_none(),
            "matching entry inserted into an already-scanned shard must not survive invalidation"
        );
        assert!(
            cache.get(&1).is_none(),
            "seed matching entry must be removed"
        );
    }

    #[test]
    fn tinylfu_put_and_get() {
        let cache: ShardedCache<TestValue> =
            ShardedCache::new(1024, Duration::from_mins(1), EvictionPolicy::TinyLfu);
        cache.insert(1, TestValue::new("tiny"), 4);
        assert_eq!(
            cache.get(&1).map(|v| v.data.clone()),
            Some("tiny".to_string())
        );
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
    fn insert_rejects_a_value_heavier_than_the_budget() {
        let cache = cache(100, Duration::from_mins(1));
        cache.insert(0, TestValue::with_size("small", 60), 60);
        cache.insert(1, TestValue::with_size("huge", 500), 500);
        assert_eq!(
            cache.len(),
            1,
            "an uncacheable insert must not flush unrelated entries"
        );
        assert!(
            cache.get(&0).is_some(),
            "the resident 60-byte entry must survive a 500-byte reject"
        );
        assert!(
            cache.get(&1).is_none(),
            "a value heavier than max_weight must not be retained"
        );
        assert_eq!(cache.weighted_size(), 60);
    }

    #[test]
    fn oversized_insert_removes_a_stale_generation_of_the_same_key() {
        let cache = cache(100, Duration::from_mins(1));
        cache.insert(1, TestValue::with_size("old", 40), 40);
        cache.insert(1, TestValue::with_size("huge", 500), 500);
        assert!(
            cache.get(&1).is_none(),
            "rejecting an oversized replacement must not leave the stale generation"
        );
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.weighted_size(), 0);
    }

    #[test]
    fn overflow_trim_drops_values_after_releasing_the_trim_lock() {
        #[derive(Clone)]
        struct ReenterInsertOnDrop {
            cache: Option<Arc<ShardedCache<ReenterInsertOnDrop>>>,
        }
        impl Drop for ReenterInsertOnDrop {
            fn drop(&mut self) {
                // Nested insert overflows and must take `trim`. Deadlocks if
                // this Drop runs while the outer trim guard is still held.
                if let Some(cache) = self.cache.take() {
                    cache.insert(99, ReenterInsertOnDrop { cache: None }, 80);
                }
            }
        }

        let cache = Arc::new(ShardedCache::<ReenterInsertOnDrop>::new(
            100,
            Duration::from_mins(1),
            EvictionPolicy::Lru,
        ));
        cache.insert(
            0,
            ReenterInsertOnDrop {
                cache: Some(Arc::clone(&cache)),
            },
            60,
        );

        let cache_for_thread = Arc::clone(&cache);
        let (done, waiter) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            cache_for_thread.insert(1, ReenterInsertOnDrop { cache: None }, 60);
            done.send(()).expect("overflow-trim completion signal");
        });
        waiter.recv_timeout(Duration::from_secs(2)).expect(
            "overflow trim deadlocked: Drop re-entered insert while the trim lock was held",
        );
        assert!(
            cache.weighted_size() <= 100,
            "re-entrant insert must still respect the budget"
        );
    }

    #[test]
    fn clear_drops_values_after_releasing_shard_locks() {
        #[derive(Clone)]
        struct ReenterOnDrop {
            cache: Arc<ShardedCache<ReenterOnDrop>>,
            dropped: Arc<AtomicU64>,
        }
        impl Drop for ReenterOnDrop {
            fn drop(&mut self) {
                // `parking_lot::Mutex` is not reentrant. Re-entering `len`
                // deadlocks if any shard lock is still held.
                let _ = self.cache.len();
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }

        let cache = Arc::new(ShardedCache::new(
            1024,
            Duration::from_mins(1),
            EvictionPolicy::Lru,
        ));
        let dropped = Arc::new(AtomicU64::new(0));
        cache.insert(
            1,
            ReenterOnDrop {
                cache: Arc::clone(&cache),
                dropped: Arc::clone(&dropped),
            },
            1,
        );

        let finished = Arc::new(AtomicU64::new(0));
        let cache_for_clear = Arc::clone(&cache);
        let finished_for_clear = Arc::clone(&finished);
        std::thread::spawn(move || {
            cache_for_clear.clear();
            finished_for_clear.store(1, Ordering::Relaxed);
        });

        let start = Instant::now();
        while finished.load(Ordering::Relaxed) == 0 {
            assert!(
                start.elapsed() < Duration::from_secs(2),
                "clear deadlocked because values were dropped while shard locks were held"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        assert!(cache.is_empty());
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
    fn tinylfu_admits_a_frequent_key_over_a_cold_resident_on_another_shard() {
        let cache: ShardedCache<TestValue> =
            ShardedCache::new(100, Duration::from_mins(1), EvictionPolicy::TinyLfu);
        let cold = 0u64;
        let hot = 1u64;
        cache.insert(cold, TestValue::with_size("cold", 100), 100);
        for _ in 0..64 {
            assert!(
                cache.get(&hot).is_none(),
                "probes increment the sketch before the key is admitted"
            );
        }
        cache.insert(hot, TestValue::with_size("hot", 100), 100);
        assert!(
            cache.get(&hot).is_some(),
            "`TinyLFU` must admit a frequently probed key by evicting a colder resident on another shard"
        );
        assert!(
            cache.get(&cold).is_none(),
            "the colder cross-shard LRU tail is the overflow victim"
        );
    }

    #[test]
    fn concurrent_overflow_inserts_leave_one_resident() {
        for trial in 0..20 {
            let cache = Arc::new(cache(100, Duration::from_mins(1)));
            let barrier = Arc::new(std::sync::Barrier::new(2));
            cache.set_after_publish_barrier(Arc::clone(&barrier));
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
            assert!(
                cache.weighted_size() <= 100,
                "trial {trial}: weight must stay at or under the budget"
            );
            assert!(
                cache.weighted_size() > 0,
                "trial {trial}: overflow trim must not remove every resident"
            );
            assert_eq!(
                cache.len(),
                1,
                "trial {trial}: two concurrent overflow inserts must not each evict a victim"
            );
        }
    }

    #[test]
    fn overflow_trim_does_not_evict_after_a_remove_restores_the_budget() {
        let cache = Arc::new(cache(100, Duration::from_mins(1)));
        cache.insert(0, TestValue::with_size("a", 60), 60);
        let cache_for_hook = Arc::clone(&cache);
        cache.set_before_size_victim(move || {
            cache_for_hook.remove(&0);
        });
        cache.insert(1, TestValue::with_size("b", 60), 60);
        assert!(
            cache.get(&1).is_some(),
            "B must stay once A's removal has already restored the 100-byte budget"
        );
        assert!(
            cache.get(&0).is_none(),
            "the hook removes A before the overflow victim is taken"
        );
        assert_eq!(cache.weighted_size(), 60);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn failed_size_claim_does_not_renew_ttl_or_recency() {
        let cache = Arc::new(cache(100, Duration::from_millis(200)));
        cache.insert(0, TestValue::with_size("old", 40), 40);
        cache.insert(16, TestValue::with_size("newer", 20), 20);
        assert_eq!(
            cache.keys_in_lru_order(),
            vec![16, 0],
            "key 16 is MRU and key 0 is the same-shard LRU tail"
        );

        let cache_for_hook = Arc::clone(&cache);
        cache.set_before_claim_size(move || {
            cache_for_hook.remove(&1);
        });
        std::thread::sleep(Duration::from_millis(80));
        cache.insert(1, TestValue::with_size("overflow", 60), 60);

        assert_eq!(
            cache.keys_in_lru_order(),
            vec![16, 0],
            "a failed size claim must leave the victim at its original LRU position"
        );
        assert!(
            cache.get(&0).is_some(),
            "the victim must still be present immediately after the failed claim"
        );
        std::thread::sleep(Duration::from_millis(150));
        assert!(
            cache.get(&0).is_none(),
            "a failed size claim must not reset the victim's TTL origin"
        );
    }

    #[test]
    fn tinylfu_does_not_evict_a_promoted_snapshot_victim() {
        let cache = Arc::new(ShardedCache::<TestValue>::new(
            100,
            Duration::from_mins(1),
            EvictionPolicy::TinyLfu,
        ));
        cache.insert(0, TestValue::with_size("tail", 40), 40);
        cache.insert(16, TestValue::with_size("mru", 40), 40);
        assert_eq!(
            cache.keys_in_lru_order(),
            vec![16, 0],
            "key 16 is MRU and key 0 is the same-shard LRU tail"
        );

        let calls = AtomicU64::new(0);
        let cache_for_hook = Arc::clone(&cache);
        cache.set_before_size_victim(move || {
            // Call 0 is the evict-loop entry. Call 1 is after TinyLFU snapshots
            // the tail and before that key is unlinked.
            if calls.fetch_add(1, Ordering::Relaxed) == 1 {
                assert!(
                    cache_for_hook.get(&0).is_some(),
                    "the snapshot victim must still be present so get can promote it"
                );
            }
        });
        cache.insert(1, TestValue::with_size("new", 40), 40);

        assert!(
            cache.get(&0).is_some(),
            "a get that promoted the snapshot victim off the tail must keep that key"
        );
        assert!(
            cache.get(&1).is_some(),
            "the overflow insert must stay once the real tail can still be reclaimed"
        );
        assert!(
            cache.get(&16).is_none(),
            "the key that remains the LRU tail after the promotion is the overflow victim"
        );
        assert_eq!(cache.weighted_size(), 80);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn tinylfu_reclaims_an_expired_cross_shard_tail_before_a_live_victim() {
        let cache: ShardedCache<TestValue> =
            ShardedCache::new(100, Duration::from_millis(100), EvictionPolicy::TinyLfu);
        let expired = 0u64;
        let live = 1u64;
        let candidate = 2u64;
        cache.insert(expired, TestValue::with_size("expired", 50), 50);
        for _ in 0..64 {
            assert!(cache.get(&expired).is_some());
        }
        std::thread::sleep(Duration::from_millis(70));
        cache.insert(live, TestValue::with_size("live", 50), 50);
        std::thread::sleep(Duration::from_millis(50));
        cache.insert(candidate, TestValue::with_size("new", 10), 10);

        assert!(
            cache.get(&expired).is_none(),
            "the expired cross-shard tail must be reclaimed before a live size victim"
        );
        assert!(
            cache.get(&live).is_some(),
            "a live resident must not be size-evicted while an expired tail still holds budget"
        );
        assert!(cache.get(&candidate).is_some());
        assert!(cache.weighted_size() <= 100);
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

    #[test]
    fn lfu_evicts_the_lowest_frequency_key() {
        let cache: ShardedCache<TestValue> =
            ShardedCache::new(100, Duration::from_mins(1), EvictionPolicy::Lfu);
        cache.insert(0, TestValue::with_size("cold", 50), 50);
        cache.insert(1, TestValue::with_size("hot", 50), 50);
        for _ in 0..16 {
            assert!(cache.get(&1).is_some());
        }
        // One get on cold so it is not zero, but still colder than hot.
        assert!(cache.get(&0).is_some());
        cache.insert(2, TestValue::with_size("new", 50), 50);
        assert!(cache.get(&0).is_none(), "LFU must evict the colder key");
        assert!(cache.get(&1).is_some(), "hot key must survive LFU eviction");
        assert!(cache.get(&2).is_some());
    }

    #[test]
    fn lfu_evicts_the_cold_resident_on_the_inserting_shard_over_a_hot_one_elsewhere() {
        // `caching_policy: lfu` promises the global lowest hit-count resident.
        // Skipping the whole inserting shard on the first pass breaks that: the
        // shard holding the coldest key is excluded, so a far hotter resident
        // elsewhere is evicted instead. Only the just-admitted key needs
        // protecting from self-eviction, not everything sharing its shard.
        let cache: ShardedCache<TestValue> =
            ShardedCache::new(100, Duration::from_mins(1), EvictionPolicy::Lfu);
        // Keys 0 and 16 share shard 0; key 1 is alone on shard 1.
        cache.insert(0, TestValue::with_size("cold", 40), 40);
        cache.insert(1, TestValue::with_size("hot", 40), 40);
        for _ in 0..32 {
            assert!(cache.get(&1).is_some());
        }

        // Admitting on shard 0 makes shard 0 the `prefer` shard.
        cache.insert(16, TestValue::with_size("new", 40), 40);

        assert!(
            cache.get(&1).is_some(),
            "a freq-32 resident must not be evicted while a freq-0 resident exists"
        );
        assert!(
            cache.get(&0).is_none(),
            "the coldest resident is the victim even when it shares the inserting shard"
        );
        assert!(
            cache.get(&16).is_some(),
            "the just-admitted key must not be self-evicted"
        );
    }

    #[test]
    fn replace_if_refuses_a_value_heavier_than_the_budget() {
        // Admitting an oversized replacement makes the cache over-budget, and
        // overflow trim walks other shards first — so one uncacheable value
        // flushes unrelated residents on its way to self-evicting.
        let cache = cache(100, Duration::from_mins(1));
        cache.insert(0, TestValue::with_size("unrelated", 60), 60);
        cache.insert(1, TestValue::with_size("target", 20), 20);

        let replaced = cache.replace_if(1, TestValue::with_size("huge", 500), 500, false, |_| true);

        assert!(
            !replaced,
            "a replacement heavier than max_weight must be refused"
        );
        assert!(
            cache.get(&0).is_some(),
            "an unrelated resident must not be flushed by a refused replacement"
        );
        assert_eq!(
            cache.get(&1).map(|v| v.data.clone()),
            Some("target".to_string()),
            "the resident already under the key must survive a refused replacement"
        );
        assert_eq!(cache.weighted_size(), 80);
    }

    #[test]
    fn wtinylfu_drains_an_oversized_window_after_a_concurrent_touch() {
        // Total weight fits `max_weight`, but the window is over `window_cap`.
        // A concurrent hit moves the snapshotted window candidate off the tail,
        // so `promote_window_key` refuses it; reporting that refusal as "no
        // progress" breaks the trim loop and strands the window over cap.
        let cache = Arc::new(ShardedCache::<TestValue>::new(
            1_000,
            Duration::from_mins(1),
            EvictionPolicy::TinyLfu,
        ));
        let window_cap = 1_000 / 100;
        // Weight 8 so one resident fits the 10-byte window (no trim) and two do
        // not — the trim then runs with two keys still in the window, which is
        // what gives the refused candidate a different tail to fall back to.
        // Keys 0 and 16 share a shard, so that shard holds the only window tail.
        cache.insert(0, TestValue::with_size("a", 8), 8);
        assert_eq!(
            cache.window_weight_for_test(),
            8,
            "one 8-byte resident must sit in the window under the 10-byte cap"
        );

        let fired = AtomicU64::new(0);
        let cache_for_hook = Arc::clone(&cache);
        cache.set_before_size_victim(move || {
            // Call 0 is the trim-loop entry; call 1 is after the window tail is
            // snapshotted and before it is moved — the racy window.
            if fired.fetch_add(1, Ordering::Relaxed) == 1 {
                assert!(
                    cache_for_hook.get(&0).is_some(),
                    "the snapshot candidate must still be present so get can move it"
                );
            }
        });

        cache.insert(16, TestValue::with_size("b", 8), 8);

        assert_eq!(
            cache.len(),
            2,
            "nothing is over budget, so nothing is evicted"
        );
        assert!(
            cache.window_weight_for_test() <= window_cap,
            "an oversized window must drain to its {window_cap}-byte cap even when a \
             concurrent touch moves the snapshot candidate, got {}",
            cache.window_weight_for_test()
        );
    }

    #[test]
    fn wtinylfu_window_promotes_into_probation_under_budget() {
        let cache: ShardedCache<TestValue> =
            ShardedCache::new(10_000, Duration::from_mins(1), EvictionPolicy::TinyLfu);
        cache.insert(0, TestValue::with_size("a", 40), 40);
        cache.insert(16, TestValue::with_size("b", 40), 40);
        // Both fit; window drain moves them to probation without discarding.
        assert!(cache.get(&0).is_some());
        assert!(cache.get(&16).is_some());
        assert_eq!(cache.len(), 2);
        assert!(cache.weighted_size() <= 10_000);
    }

    #[test]
    fn tinylfu_expires_a_hot_same_shard_resident_before_admission() {
        let cache: ShardedCache<TestValue> =
            ShardedCache::new(100, Duration::from_millis(30), EvictionPolicy::TinyLfu);
        let old = 16u64;
        let new = 32u64;
        cache.insert(old, TestValue::with_size("old", 100), 100);
        for _ in 0..64 {
            assert!(cache.get(&old).is_some());
        }
        std::thread::sleep(Duration::from_millis(50));
        cache.insert(new, TestValue::with_size("new", 100), 100);
        assert!(
            cache.get(&new).is_some(),
            "an expired same-shard resident must not block TinyLFU admission"
        );
        assert!(cache.get(&old).is_none());
    }
    #[test]
    fn run_pending_tasks_syncs_window_and_protected_weights_on_expire() {
        let cache: ShardedCache<TestValue> =
            ShardedCache::new(10_000, Duration::from_millis(30), EvictionPolicy::TinyLfu);
        // One-byte window resident (key 0 → shard 0).
        cache.insert(0, TestValue::with_size("w", 1), 1);
        assert_eq!(cache.weighted_size(), 1);
        assert_eq!(
            cache.window_weight_for_test(),
            1,
            "fresh TinyLFU insert must land in the window segment"
        );
        std::thread::sleep(Duration::from_millis(50));
        cache.run_pending_tasks();
        assert_eq!(cache.weighted_size(), 0);
        assert_eq!(
            cache.window_weight_for_test(),
            0,
            "expire path must clear global window_weight with total weight"
        );
        assert_eq!(cache.protected_weight_for_test(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn lfu_evicts_global_lowest_freq_including_cold_mru() {
        // Keys on distinct shards (0, 1, 2). Full-scan LFU must prefer the
        // colder resident over the hot one when overflowing.
        let cache: ShardedCache<TestValue> =
            ShardedCache::new(100, Duration::from_mins(1), EvictionPolicy::Lfu);
        cache.insert(0, TestValue::with_size("cold", 50), 50);
        cache.insert(1, TestValue::with_size("hot", 50), 50);
        for _ in 0..16 {
            assert!(cache.get(&1).is_some());
        }
        assert!(cache.get(&0).is_some());
        cache.insert(2, TestValue::with_size("new", 50), 50);
        assert!(
            cache.get(&0).is_none(),
            "LFU must evict the colder cross-shard key"
        );
        assert!(cache.get(&1).is_some());
        assert!(cache.get(&2).is_some());
    }

    #[test]
    fn lfu_revalidates_frequency_before_unlink() {
        // Select A (freq 0) as victim. The before-unlink hook raises A's
        // frequency while B stays colder; revalidation must reselect B.
        let cache = std::sync::Arc::new(ShardedCache::<TestValue>::new(
            100,
            Duration::from_mins(1),
            EvictionPolicy::Lfu,
        ));
        cache.insert(0, TestValue::with_size("a", 50), 50);
        cache.insert(1, TestValue::with_size("b", 50), 50);
        // Warm B once so A (freq 0) is the colder snapshot victim. The hook
        // then warms A past B between selection and unlink.
        assert!(cache.get(&1).is_some());
        let cache_for_hook = std::sync::Arc::clone(&cache);
        let hits = std::sync::atomic::AtomicUsize::new(0);
        let hits = std::sync::Arc::new(hits);
        let hits_for_hook = std::sync::Arc::clone(&hits);
        cache.set_before_size_victim(move || {
            // Call 0 is the outer trim loop (before selection). Call 1 is
            // inside remove_lfu_key_for_size after A was snapshotted — raise A
            // there. Later reselect passes no-op.
            if hits_for_hook.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 1 {
                for _ in 0..4 {
                    let _ = cache_for_hook.get(&0);
                }
            }
        });
        cache.insert(2, TestValue::with_size("new", 50), 50);
        assert!(
            cache.get(&0).is_some(),
            "A must survive after concurrent hits raised its frequency"
        );
        assert!(
            cache.get(&1).is_none(),
            "B (still colder) must be the reselected victim"
        );
        assert!(cache.get(&2).is_some());
    }
}
