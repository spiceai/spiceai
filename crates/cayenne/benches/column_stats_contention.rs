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

//! Regression bench: lock pattern in
//! `ColumnStatsAccumulator::update`
//! (`crates/cayenne/src/provider/table.rs:259-303`).
//!
//! The current `ColumnStatsAccumulator`
//! (`crates/cayenne/src/provider/table.rs:214-228`) holds **two** separate
//! `std::sync::Mutex`es:
//!
//! ```ignore
//! columns: std::sync::Mutex<Vec<vortex::array::stats::StatsSet>>,
//! columns_seeded: std::sync::Mutex<Vec<bool>>,
//! ```
//!
//! `update()` acquires both — `columns` then `columns_seeded` — on every
//! `RecordBatch` from the write hot path (called from the streaming wrapper
//! at `crates/cayenne/src/provider/table.rs:2790`). Multi-partition writers
//! share a single `Arc<ColumnStatsAccumulator>`
//! (`table.rs:2782`), so each writer task serializes through *the same two
//! mutexes* on every batch. Per-batch fixed cost is the floor; under
//! contention it becomes the throughput ceiling.
//!
//! `ColumnStatsAccumulator` is `pub(crate)`, so this bench is a shape bench
//! — it models the exact `std::sync::Mutex<Vec<_>>` + `std::sync::Mutex<Vec<_>>`
//! pattern, with the same per-batch body shape (read columns slice, branch
//! on per-column seeded flag, mutate both vectors). Same precedent as
//! `listing_fence_overhead.rs` which benches the synchronization pattern
//! rather than the concrete `ListingTable` it guards.
//!
//! ## Three lanes
//!
//! - `current_two_locks/<threads>` — mirrors today's structure. Each
//!   thread locks `columns`, then locks `columns_seeded`, does per-column
//!   work, drops both guards. Models the production pattern.
//! - `single_combined_lock/<threads>` — merges the two `Mutex<Vec<_>>`
//!   fields into one `Mutex<State>` where `State` owns both vectors.
//!   One atomic acquisition per batch instead of two. Same contention
//!   profile, smaller per-call constant.
//! - `per_thread_then_merge/<threads>` — each thread accumulates into a
//!   thread-local accumulator with no synchronization at all; a single
//!   final merge folds them together. Models the structural fix. Wall
//!   time should scale near-linearly with thread count down to the
//!   merge cost.
//!
//! ## How to read
//!
//! `cargo bench --bench column_stats_contention -p cayenne`. For threads=8
//! and `BATCHES_PER_THREAD=512`:
//!
//! - `current_two_locks/8` is the regression baseline. As threads
//!   increases, time stays nearly flat — i.e. the lock is the bottleneck.
//! - `single_combined_lock/8` should be ~2× faster than `current_two_locks/8`
//!   (one atomic CAS instead of two) but still serial.
//! - `per_thread_then_merge/8` should be ~Nx faster on an N-core box,
//!   because the threads truly run in parallel.
//!
//! Use the gap between `current_two_locks` and `per_thread_then_merge`
//! to size the headroom from migrating to per-partition accumulators.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Mutex;
use std::sync::{Arc, atomic::AtomicI64};
use std::thread;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Number of stat columns. Picked to match a typical accelerated table —
/// most production schemas have 4-32 columns. Per-column work scales with
/// this value linearly inside the locked critical section.
const NUM_COLUMNS: usize = 8;

/// Per-thread batch count. Large enough to amortize thread spawn overhead;
/// small enough that the bench stays in the millisecond range.
const BATCHES_PER_THREAD: usize = 512;

/// Concurrency levels straddling the typical writer-partition count
/// (`target_partitions` defaults to logical CPU count).
const THREAD_COUNTS: &[usize] = &[1, 4, 8, 16];

/// Stand-in for the work that
/// `crate::stats::column_stats_to_stats_set(...)` plus
/// `existing.merge_unordered(...)` does per column inside the locked
/// critical section. The exact wall-clock value does not matter for the
/// contention story; what matters is that there is *some* nonzero work
/// inside the lock, so contention is observable rather than instantaneous.
#[inline(never)]
fn per_column_work(state: u64, batch_contribution: u64) -> u64 {
    // A few non-trivial integer ops so the optimizer cannot fold this
    // into a single instruction. `black_box` keeps both inputs alive.
    let a = black_box(state).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let b = black_box(batch_contribution).wrapping_add(0xDEAD_BEEF_CAFE_BABE);
    a ^ b.rotate_left(13)
}

// ---------------------------------------------------------------------------
// Lane 1: current_two_locks — exact mirror of `ColumnStatsAccumulator`.
// ---------------------------------------------------------------------------

struct CurrentTwoLocks {
    columns: Mutex<Vec<u64>>,
    columns_seeded: Mutex<Vec<bool>>,
    row_count: AtomicI64,
}

impl CurrentTwoLocks {
    fn new() -> Self {
        Self {
            columns: Mutex::new(vec![0u64; NUM_COLUMNS]),
            columns_seeded: Mutex::new(vec![false; NUM_COLUMNS]),
            row_count: AtomicI64::new(0),
        }
    }

    fn update(&self, batch_rows: i64, batch_contribution: u64) {
        let mut cols = self.columns.lock().expect("cols poisoned");
        let mut seeded = self.columns_seeded.lock().expect("seeded poisoned");
        self.row_count
            .fetch_add(batch_rows, std::sync::atomic::Ordering::Relaxed);
        for i in 0..NUM_COLUMNS {
            let next = per_column_work(cols[i], batch_contribution);
            if seeded[i] {
                cols[i] = next;
            } else {
                cols[i] = next;
                seeded[i] = true;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Lane 2: single_combined_lock — one Mutex owning both vectors.
// ---------------------------------------------------------------------------

struct CombinedState {
    columns: Vec<u64>,
    columns_seeded: Vec<bool>,
}

struct SingleCombinedLock {
    state: Mutex<CombinedState>,
    row_count: AtomicI64,
}

impl SingleCombinedLock {
    fn new() -> Self {
        Self {
            state: Mutex::new(CombinedState {
                columns: vec![0u64; NUM_COLUMNS],
                columns_seeded: vec![false; NUM_COLUMNS],
            }),
            row_count: AtomicI64::new(0),
        }
    }

    fn update(&self, batch_rows: i64, batch_contribution: u64) {
        let mut state = self.state.lock().expect("state poisoned");
        self.row_count
            .fetch_add(batch_rows, std::sync::atomic::Ordering::Relaxed);
        for i in 0..NUM_COLUMNS {
            let next = per_column_work(state.columns[i], batch_contribution);
            if state.columns_seeded[i] {
                state.columns[i] = next;
            } else {
                state.columns[i] = next;
                state.columns_seeded[i] = true;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Lane 3: per_thread_then_merge — thread-local accumulators, one merge at
// the end. Models the structural fix (per-partition accumulators that
// finalize into the shared one).
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct LocalAcc {
    columns: Vec<u64>,
    columns_seeded: Vec<bool>,
    row_count: i64,
}

impl LocalAcc {
    fn new() -> Self {
        Self {
            columns: vec![0u64; NUM_COLUMNS],
            columns_seeded: vec![false; NUM_COLUMNS],
            row_count: 0,
        }
    }

    fn update(&mut self, batch_rows: i64, batch_contribution: u64) {
        self.row_count = self.row_count.saturating_add(batch_rows);
        for i in 0..NUM_COLUMNS {
            let next = per_column_work(self.columns[i], batch_contribution);
            if self.columns_seeded[i] {
                self.columns[i] = next;
            } else {
                self.columns[i] = next;
                self.columns_seeded[i] = true;
            }
        }
    }

    fn merge(&mut self, other: &LocalAcc) {
        self.row_count = self.row_count.saturating_add(other.row_count);
        for i in 0..NUM_COLUMNS {
            if other.columns_seeded[i] {
                let next = per_column_work(self.columns[i], other.columns[i]);
                self.columns[i] = next;
                self.columns_seeded[i] = self.columns_seeded[i] || true;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Drivers.
// ---------------------------------------------------------------------------

fn run_current(threads: usize) {
    let acc = Arc::new(CurrentTwoLocks::new());
    thread::scope(|s| {
        for t in 0..threads {
            let acc = Arc::clone(&acc);
            s.spawn(move || {
                for b in 0..BATCHES_PER_THREAD {
                    acc.update(1024, (t as u64).wrapping_mul(b as u64 + 1));
                }
            });
        }
    });
    black_box(acc.row_count.load(std::sync::atomic::Ordering::Relaxed));
}

fn run_combined(threads: usize) {
    let acc = Arc::new(SingleCombinedLock::new());
    thread::scope(|s| {
        for t in 0..threads {
            let acc = Arc::clone(&acc);
            s.spawn(move || {
                for b in 0..BATCHES_PER_THREAD {
                    acc.update(1024, (t as u64).wrapping_mul(b as u64 + 1));
                }
            });
        }
    });
    black_box(acc.row_count.load(std::sync::atomic::Ordering::Relaxed));
}

fn run_per_thread(threads: usize) {
    let final_acc = Arc::new(Mutex::new(LocalAcc::new()));
    thread::scope(|s| {
        for t in 0..threads {
            let final_acc = Arc::clone(&final_acc);
            s.spawn(move || {
                let mut local = LocalAcc::new();
                for b in 0..BATCHES_PER_THREAD {
                    local.update(1024, (t as u64).wrapping_mul(b as u64 + 1));
                }
                final_acc.lock().expect("final acc").merge(&local);
            });
        }
    });
    black_box(final_acc.lock().expect("final").row_count);
}

fn bench_column_stats_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_stats_contention");
    for &t in THREAD_COUNTS {
        let work_units = u64::try_from(t * BATCHES_PER_THREAD).unwrap_or(u64::MAX);
        group.throughput(Throughput::Elements(work_units));

        group.bench_with_input(BenchmarkId::new("current_two_locks", t), &t, |b, &t| {
            b.iter(|| run_current(t));
        });

        group.bench_with_input(BenchmarkId::new("single_combined_lock", t), &t, |b, &t| {
            b.iter(|| run_combined(t));
        });

        group.bench_with_input(BenchmarkId::new("per_thread_then_merge", t), &t, |b, &t| {
            b.iter(|| run_per_thread(t));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_column_stats_contention);
criterion_main!(benches);
