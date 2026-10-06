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

//! Microbenchmarks for the shared Postgres replication ack path
//! ([`data_components::postgres_replication::shared`]): the `old_*` arms mirror
//! the pre-change mutex-`HashMap` `AckTable` (eager `recompute` on every
//! commit/credit, one shared mutex taken per op) and the `new_*` arms mirror the
//! per-member cache-line-isolated `AckSlot` design (a lock-free `advance_monotonic`
//! on the member's own atomic; the slot-level floor recomputed lazily under a
//! read lock at each boundary). The ack internals are crate-private, so — exactly
//! like `shared_pump_bench` — both arms are mirrored here; the `new_*` code is a
//! line-faithful copy of the real `AckSlot`/`advance_monotonic`/sweep so there is
//! no drift, and `--baseline`-comparing arms gives the per-op reduction.
//!
//! Groups (all at 7 members — the CH-benCHmark shared-slot width):
//! 1. `ack/commit_uncontended` — one committer, no contention.
//! 2. `ack/commit_contended` — 7 committer threads + 1 pump thread, wall time.
//! 3. `ack/boundary_flush_lsn` — the per-boundary floor recompute/sweep.
//! 4. `ack/checkpoint_drain` — O(committers) sequential vs O(1) folded drain.
//!
//! ## Converting bench deltas into projected run savings
//!
//! ```text
//! projected_savings_s = Σ_i  count_i × (old_ns_i − new_ns_i) / 1e9
//! ```
//!
//! with SF-1000 1-slot per-600s counts (each ≈ 5.5M): consumer commits (× the
//! old recompute each), pump `already_committed`, `deliver`, `credit_idle` (each
//! an old lock + sweep + recompute), and boundary `flush_lsn`. The
//! `commit_contended` delta is the honest multiplier for the pump-side counts,
//! since the pump shares the lock with 7 consumers in vivo. Coalescing also
//! changes `count`: consumer commits 5.5M → ~460k (one per burst), a second
//! scenario row the formula should reflect.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex, RwLock};

use criterion::{Criterion, criterion_group, criterion_main};

/// Representative shared-slot fan-out width (the CH-benCHmark topology).
const MEMBERS: usize = 7;

type MemberKey = (String, String);

fn advance_monotonic(flush: &AtomicU64, to: u64) {
    let mut current = flush.load(Ordering::Relaxed);
    loop {
        if to <= current {
            return;
        }
        match flush.compare_exchange(current, to, Ordering::Release, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

fn keys(n: usize) -> Vec<MemberKey> {
    (0..n)
        .map(|i| ("public".to_string(), format!("t{i}")))
        .collect()
}

// ----------------------------------------------------------------------------
// Old arm: mutex-`HashMap` `AckTable` with eager `recompute`.
// ----------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct OldEntry {
    committed: u64,
    delivered: u64,
    live: bool,
    streaming: bool,
}

struct OldAck {
    entries: Mutex<HashMap<MemberKey, OldEntry>>,
    shared_flush: AtomicU64,
}

impl OldAck {
    fn with_streaming(n: usize) -> Self {
        let mut entries = HashMap::new();
        for k in keys(n) {
            entries.insert(
                k,
                OldEntry {
                    committed: 0,
                    delivered: 0,
                    live: true,
                    streaming: true,
                },
            );
        }
        Self {
            entries: Mutex::new(entries),
            shared_flush: AtomicU64::new(0),
        }
    }

    fn already_committed(&self, key: &MemberKey, lsn: u64) -> bool {
        self.entries
            .lock()
            .expect("lock")
            .get(key)
            .is_some_and(|e| e.committed >= lsn)
    }

    fn deliver(&self, key: &MemberKey, lsn: u64) {
        let mut entries = self.entries.lock().expect("lock");
        if let Some(e) = entries.get_mut(key) {
            e.delivered = e.delivered.max(lsn);
        }
    }

    fn commit(&self, key: &MemberKey, lsn: u64) {
        {
            let mut entries = self.entries.lock().expect("lock");
            if let Some(e) = entries.get_mut(key) {
                e.committed = e.committed.max(lsn);
            }
        }
        self.recompute();
    }

    fn credit_idle(&self, upto: u64) {
        {
            let mut entries = self.entries.lock().expect("lock");
            for e in entries.values_mut() {
                if e.live && e.streaming && e.delivered == e.committed {
                    let lsn = e.committed.max(upto);
                    e.committed = lsn;
                    e.delivered = lsn;
                }
            }
        }
        self.recompute();
    }

    fn recompute(&self) {
        let floor = {
            let entries = self.entries.lock().expect("lock");
            entries.values().map(|e| e.committed).min()
        };
        if let Some(floor) = floor {
            advance_monotonic(&self.shared_flush, floor);
        }
    }

    fn flush_lsn(&self) -> u64 {
        self.shared_flush.load(Ordering::Acquire)
    }
}

// ----------------------------------------------------------------------------
// New arm: per-member cache-line-isolated `AckSlot` (line-faithful mirror of
// the real implementation).
// ----------------------------------------------------------------------------

const LIVE: u8 = 0b001;
const STREAMING: u8 = 0b100;

#[repr(align(64))]
struct NewSlot {
    committed: AtomicU64,
    delivered: AtomicU64,
    state: AtomicU8,
}

impl NewSlot {
    fn streaming() -> Arc<Self> {
        Arc::new(Self {
            committed: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            state: AtomicU8::new(LIVE | STREAMING),
        })
    }
    fn committed(&self) -> u64 {
        self.committed.load(Ordering::Acquire)
    }
    fn delivered(&self) -> u64 {
        self.delivered.load(Ordering::Acquire)
    }
    fn commit(&self, lsn: u64) {
        advance_monotonic(&self.committed, lsn);
    }
    fn deliver(&self, lsn: u64) {
        advance_monotonic(&self.delivered, lsn);
    }
    fn already_committed(&self, lsn: u64) -> bool {
        self.committed.load(Ordering::Acquire) >= lsn
    }
}

struct NewAck {
    members: RwLock<HashMap<MemberKey, Arc<NewSlot>>>,
    shared_flush: AtomicU64,
}

impl NewAck {
    fn with_streaming(n: usize) -> Self {
        let mut members = HashMap::new();
        for k in keys(n) {
            members.insert(k, NewSlot::streaming());
        }
        Self {
            members: RwLock::new(members),
            shared_flush: AtomicU64::new(0),
        }
    }

    fn slot(&self, key: &MemberKey) -> Arc<NewSlot> {
        Arc::clone(self.members.read().expect("lock").get(key).expect("member"))
    }

    fn credit_idle(&self, upto: u64) {
        for slot in self.members.read().expect("lock").values() {
            let s = slot.state.load(Ordering::Acquire);
            if s & (LIVE | STREAMING) == (LIVE | STREAMING) && slot.delivered() == slot.committed()
            {
                advance_monotonic(&slot.committed, upto);
                advance_monotonic(&slot.delivered, upto);
            }
        }
    }

    fn flush_lsn(&self) -> u64 {
        let floor = self
            .members
            .read()
            .expect("lock")
            .values()
            .map(|slot| slot.committed())
            .min();
        if let Some(floor) = floor {
            advance_monotonic(&self.shared_flush, floor);
        }
        self.shared_flush.load(Ordering::Acquire)
    }
}

// ----------------------------------------------------------------------------
// 1. Uncontended commit.
// ----------------------------------------------------------------------------

fn bench_commit_uncontended(c: &mut Criterion) {
    let key = ("public".to_string(), "t0".to_string());
    let mut group = c.benchmark_group("ack/commit_uncontended");

    let old = OldAck::with_streaming(MEMBERS);
    let mut lsn = 0u64;
    group.bench_function("old_mutex_recompute", |b| {
        b.iter(|| {
            lsn += 1;
            old.commit(&key, lsn);
        });
    });

    let new = NewAck::with_streaming(MEMBERS);
    let slot = new.slot(&key);
    let mut lsn = 0u64;
    group.bench_function("new_atomic_advance", |b| {
        b.iter(|| {
            lsn += 1;
            slot.commit(lsn);
        });
    });

    group.finish();
}

// ----------------------------------------------------------------------------
// 2. Contended commit — the headline number. 7 committer threads on their own
//    member + 1 pump thread cycling already_committed/deliver/credit_idle.
// ----------------------------------------------------------------------------

/// Commits each of the 7 committer threads performs per measured iteration.
const OPS_PER_COMMITTER: u64 = 2_000;

fn bench_commit_contended(c: &mut Criterion) {
    let mut group = c.benchmark_group("ack/commit_contended");
    let member_keys = keys(MEMBERS);

    group.bench_function("old_mutex_recompute", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let ack = Arc::new(OldAck::with_streaming(MEMBERS));
                total += run_contended_old(&ack, &member_keys);
            }
            total
        });
    });

    group.bench_function("new_atomic_advance", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let ack = Arc::new(NewAck::with_streaming(MEMBERS));
                total += run_contended_new(&ack, &member_keys);
            }
            total
        });
    });

    group.finish();
}

fn run_contended_old(ack: &Arc<OldAck>, member_keys: &[MemberKey]) -> std::time::Duration {
    // 7 committers + 1 pump, released together by a barrier. The pump cycles
    // already_committed/deliver/credit_idle (sharing the one mutex with all 7
    // committers) until every committer has finished; wall time spans the
    // barrier release to all-joined.
    let barrier = Arc::new(Barrier::new(MEMBERS + 1));
    let remaining = Arc::new(std::sync::atomic::AtomicUsize::new(MEMBERS));
    let start = Arc::new(std::sync::OnceLock::new());
    std::thread::scope(|s| {
        for key in member_keys {
            let ack = Arc::clone(ack);
            let barrier = Arc::clone(&barrier);
            let remaining = Arc::clone(&remaining);
            let key = key.clone();
            s.spawn(move || {
                barrier.wait();
                for lsn in 1..=OPS_PER_COMMITTER {
                    ack.deliver(&key, lsn);
                    ack.commit(&key, lsn);
                }
                remaining.fetch_sub(1, Ordering::Release);
            });
        }
        let ack = Arc::clone(ack);
        let barrier = Arc::clone(&barrier);
        let remaining = Arc::clone(&remaining);
        let start_cell = Arc::clone(&start);
        let probe = member_keys[0].clone();
        s.spawn(move || {
            barrier.wait();
            let _ = start_cell.set(std::time::Instant::now());
            let mut upto = 0u64;
            while remaining.load(Ordering::Acquire) > 0 {
                upto += 1;
                let _ = ack.already_committed(&probe, upto);
                ack.credit_idle(upto);
            }
        });
    });
    start
        .get()
        .map_or(std::time::Duration::ZERO, std::time::Instant::elapsed)
}

fn run_contended_new(ack: &Arc<NewAck>, member_keys: &[MemberKey]) -> std::time::Duration {
    let barrier = Arc::new(Barrier::new(MEMBERS + 1));
    let remaining = Arc::new(std::sync::atomic::AtomicUsize::new(MEMBERS));
    let start = Arc::new(std::sync::OnceLock::new());
    std::thread::scope(|s| {
        for key in member_keys {
            let slot = ack.slot(key);
            let barrier = Arc::clone(&barrier);
            let remaining = Arc::clone(&remaining);
            s.spawn(move || {
                barrier.wait();
                for lsn in 1..=OPS_PER_COMMITTER {
                    slot.deliver(lsn);
                    slot.commit(lsn);
                }
                remaining.fetch_sub(1, Ordering::Release);
            });
        }
        let ack = Arc::clone(ack);
        let barrier = Arc::clone(&barrier);
        let remaining = Arc::clone(&remaining);
        let start_cell = Arc::clone(&start);
        let probe = ack.slot(&member_keys[0]);
        s.spawn(move || {
            barrier.wait();
            let _ = start_cell.set(std::time::Instant::now());
            let mut upto = 0u64;
            while remaining.load(Ordering::Acquire) > 0 {
                upto += 1;
                let _ = probe.already_committed(upto);
                ack.credit_idle(upto);
            }
        });
    });
    start
        .get()
        .map_or(std::time::Duration::ZERO, std::time::Instant::elapsed)
}

// ----------------------------------------------------------------------------
// 3. Per-boundary floor recompute/sweep.
// ----------------------------------------------------------------------------

fn bench_boundary_flush_lsn(c: &mut Criterion) {
    let mut group = c.benchmark_group("ack/boundary_flush_lsn");

    let old = OldAck::with_streaming(MEMBERS);
    // Prime distinct committed values so the min sweep is real work.
    for (i, k) in keys(MEMBERS).iter().enumerate() {
        old.commit(k, 100 + i as u64);
    }
    group.bench_function("old_recompute", |b| {
        b.iter(|| {
            old.recompute();
            std::hint::black_box(old.flush_lsn())
        });
    });

    let new = NewAck::with_streaming(MEMBERS);
    for (i, k) in keys(MEMBERS).iter().enumerate() {
        new.slot(k).commit(100 + i as u64);
    }
    group.bench_function("new_readlock_sweep", |b| {
        b.iter(|| std::hint::black_box(new.flush_lsn()));
    });

    group.finish();
}

// ----------------------------------------------------------------------------
// 4. Checkpoint drain — O(committers) sequential vs O(1) folded.
// ----------------------------------------------------------------------------

/// Deferred epochs held between Cayenne checkpoints.
const EPOCHS: usize = 128;
/// Measured envelopes-per-burst (one committer each) per epoch.
const COMMITTERS_PER_EPOCH: usize = 43;

fn bench_checkpoint_drain(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("ack/checkpoint_drain");

    // Every committer targets one member's slot (a single dataset's deferred
    // queue), so the fold collapses the whole prefix to one max-LSN commit. The
    // (epoch, flush_to) pairs stand in for the queued committers.
    let build_committers = || -> Vec<(usize, u64)> {
        let mut v = Vec::with_capacity(EPOCHS * COMMITTERS_PER_EPOCH);
        let mut lsn = 0u64;
        for epoch in 0..EPOCHS {
            for _ in 0..COMMITTERS_PER_EPOCH {
                lsn += 1;
                v.push((epoch, lsn));
            }
        }
        v
    };

    let old_slot = NewSlot::streaming();
    let committers = build_committers();
    group.bench_function("old_sequential", |b| {
        b.to_async(&rt).iter(|| {
            let slot = Arc::clone(&old_slot);
            let committers = committers.clone();
            async move {
                // Old drain: await one commit per committer (≈5,500 calls).
                for (_epoch, lsn) in committers {
                    slot.commit(lsn);
                    // Model the awaited async commit boundary.
                    tokio::task::yield_now().await;
                }
                std::hint::black_box(slot.committed())
            }
        });
    });

    let new_slot = NewSlot::streaming();
    let committers = build_committers();
    group.bench_function("new_folded", |b| {
        b.to_async(&rt).iter(|| {
            let slot = Arc::clone(&new_slot);
            let committers = committers.clone();
            async move {
                // New drain: fold the whole prefix (same slot ⇒ max LSN) then a
                // single awaited commit.
                let folded = committers.iter().map(|(_, lsn)| *lsn).max().unwrap_or(0);
                slot.commit(folded);
                tokio::task::yield_now().await;
                std::hint::black_box(slot.committed())
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_commit_uncontended,
    bench_commit_contended,
    bench_boundary_flush_lsn,
    bench_checkpoint_drain
);
criterion_main!(benches);
