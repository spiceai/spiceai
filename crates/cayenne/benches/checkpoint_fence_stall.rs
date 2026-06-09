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

//! Regression bench: scan stall during inline-memtable checkpoint.
//!
//! Older versions of `CayenneTableProvider::checkpoint_inlined_data` held the
//! `listing_fence.write()` guard across `clear_inlined_metadata_after_checkpoint`,
//! which issued two sequential awaited metastore DELETEs:
//!
//! ```ignore
//! {
//!     let _fence = self.listing_fence.write().await;
//!     self.clear_inlined_metadata_after_checkpoint().await?;  // 2 awaits
//!     self.refresh_listing_table_under_held_fence()?;
//! }
//!
//! async fn clear_inlined_metadata_after_checkpoint(&self) -> Result<()> {
//!     self.catalog.clear_inlined_data(&id).await?;     // round trip 1
//!     self.catalog.clear_inlined_deletes(&id).await?;  // round trip 2
//!     ...
//! }
//! ```
//!
//! Every concurrent scan acquiring `listing_fence.read().await` blocked for
//! the full duration of those two round trips. On a remote metastore
//! (Turso wire RTT ~10 ms, managed PostgreSQL ~10-30 ms) two sequential
//! round trips meant every reader stalled 20-60 ms per checkpoint —
//! recurring at the inline-flush cadence.
//!
//! The production path now folds the two DELETEs into a single
//! `execute_transaction_batch_helper` call ([`crate::cayenne_catalog::CayenneCatalog::clear_inlined_data_and_deletes`],
//! at `cayenne_catalog.rs:1748`) — one BEGIN + two DELETEs + one COMMIT in
//! one wire round-trip. The listing-fence bracket holds for only one RTT
//! now; the in-process cost of the bracket is unchanged but the wire-bound
//! term halves.
//!
//! ## What this bench measures
//!
//! Two lanes, identical fence-bracket pattern, identical "refresh
//! listing table" no-op, identical lock primitive (`tokio::sync::RwLock`
//! — same primitive used by `listing_fence`).
//!
//! Per-call metastore work is simulated by `tokio::time::sleep(rtt)`. Real
//! `InMemory` round-trip time is below the timer resolution, so the sleep is
//! the *only* meaningful work — exactly the model we want because it isolates
//! the sequential-vs-batched pattern from any confounding compute.
//!
//! - `checkpoint_fence_stall/two_sequential_deletes_baseline/<rtt>` —
//!   `fence.write().await; sleep(rtt).await; sleep(rtt).await; drop(fence);`
//!   Mirrors the older two-DELETE shape.
//! - `checkpoint_fence_stall/single_batch_delete/<rtt>` — current behavior:
//!   `fence.write().await; sleep(rtt).await; drop(fence);` Single batched
//!   DELETE.
//!
//! ## How to read
//!
//! `cargo bench --bench checkpoint_fence_stall -p cayenne`. The
//! `two_sequential_deletes_baseline` lane is ~2× the duration of
//! `single_batch_delete`. Because the lock is held for the whole duration,
//! **the duration of the baseline lane is also the worst-case scan tail
//! latency one checkpoint would cause under the older code** — every
//! concurrent reader stalled that long. The bench output makes the
//! tail-latency floor visible at three RTTs that cover production
//! deployments:
//!
//! - `rtt_1ms` — local SQLite with `fsync` (best case).
//! - `rtt_10ms` — same-zone network metastore (typical Turso / managed
//!   Postgres).
//! - `rtt_30ms` — cross-region network metastore.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

/// Stand-in for the in-process work
/// `refresh_listing_table_under_held_fence` does after the metastore
/// returns. Real cost is sub-microsecond (`ArcSwap::store` + invalidate
/// the DataFusion list-files cache); we keep the symbol so both lanes
/// pay the same constant overhead.
#[inline(never)]
fn refresh_listing_table_no_op() {
    black_box(0u64);
}

/// Simulated round-trip times spanning the three realistic deployment
/// profiles. Local in-process SQLite without `fsync` (< 100 µs) is not
/// included — at that scale the bench duration is dominated by lock
/// acquisition overhead and the regression is not visible.
const RTTS: &[(&str, Duration)] = &[
    ("rtt_1ms", Duration::from_millis(1)),
    ("rtt_10ms", Duration::from_millis(10)),
    ("rtt_30ms", Duration::from_millis(30)),
];

async fn two_sequential_deletes_baseline(fence: &RwLock<()>, rtt: Duration) {
    let _guard = fence.write().await;
    // clear_inlined_data — first metastore round trip.
    tokio::time::sleep(rtt).await;
    // clear_inlined_deletes — second metastore round trip.
    tokio::time::sleep(rtt).await;
    refresh_listing_table_no_op();
}

async fn single_batch_delete(fence: &RwLock<()>, rtt: Duration) {
    let _guard = fence.write().await;
    // clear_inlined_data_and_deletes — single transaction, one round trip.
    tokio::time::sleep(rtt).await;
    refresh_listing_table_no_op();
}

fn bench_checkpoint_fence_stall(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let fence = Arc::new(RwLock::new(()));

    let mut group = c.benchmark_group("checkpoint_fence_stall");
    for &(label, rtt) in RTTS {
        let fence_a = Arc::clone(&fence);
        group.bench_with_input(
            BenchmarkId::new("two_sequential_deletes_baseline", label),
            &rtt,
            |b, &rtt| {
                let fence = Arc::clone(&fence_a);
                b.to_async(&rt)
                    .iter(|| async { two_sequential_deletes_baseline(&fence, rtt).await });
            },
        );

        let fence_b = Arc::clone(&fence);
        group.bench_with_input(
            BenchmarkId::new("single_batch_delete", label),
            &rtt,
            |b, &rtt| {
                let fence = Arc::clone(&fence_b);
                b.to_async(&rt)
                    .iter(|| async { single_batch_delete(&fence, rtt).await });
            },
        );
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Moonshot lever 1+2: mem-tier checkpoint encode + BEGIN IMMEDIATE commit moved
// OUTSIDE the listing fence.
//
// `checkpoint_mem_tier` (cdc_durability: memory) previously held
// `listing_fence.write()` across the Vortex ENCODE and the metastore COMMIT, so
// every concurrent CDC append (which also takes `listing_fence.write()`) stalled
// for that full duration. The two-phase rewrite runs encode + commit off the
// fence and takes the fence only for the in-memory swap.
//
// This bench measures the FENCE-HELD duration — the exact stall a concurrent
// append sees — under each model. Work is modeled by `sleep`: an encode (~8 ms
// for a small delta) plus a metastore commit round-trip at three deployment
// RTTs. Single-task and deterministic (no inter-task race): the timed span is
// purely the fence-held section.
//
//   - `encode_commit_under_fence/<rtt>` (OLD): fence held across encode + commit
//     + swap  ⇒ stall ≈ encode + commit.
//   - `swap_only_under_fence/<rtt>`     (NEW): encode + commit happen first, off
//     the fence; fence held for the swap only  ⇒ stall ≈ µs.
//
// The ratio of the two lanes is the per-checkpoint append-stall reduction, which
// recurs at the background checkpoint cadence (default 1 s) for every memory-mode
// table — so a 10–30 ms stall removed per checkpoint is 10–30 ms of ingest
// throughput reclaimed per table per second.
const ENCODE: Duration = Duration::from_millis(8);

#[inline(never)]
fn swap_no_op() {
    // Stand-in for the under-fence work the new path keeps: ArcSwap publish of
    // the snapshot pointer + deletion cache, clear the flushed epoch, refresh the
    // listing table. All in-process; real cost is sub-millisecond.
    black_box(0u64);
}

async fn encode_commit_under_fence(fence: &RwLock<()>, commit_rtt: Duration) -> Duration {
    let started = Instant::now();
    let _guard = fence.write().await;
    tokio::time::sleep(ENCODE).await; // Vortex encode — UNDER the fence (old)
    tokio::time::sleep(commit_rtt).await; // BEGIN IMMEDIATE commit — UNDER the fence (old)
    swap_no_op();
    started.elapsed()
}

async fn swap_only_under_fence(fence: &RwLock<()>, commit_rtt: Duration) -> Duration {
    tokio::time::sleep(ENCODE).await; // encode — OUTSIDE the fence (new)
    tokio::time::sleep(commit_rtt).await; // commit — OUTSIDE the fence (new)
    let started = Instant::now();
    let _guard = fence.write().await; // fence taken ONLY for the swap
    swap_no_op();
    started.elapsed()
}

fn bench_mem_tier_checkpoint_fence_stall(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let fence = Arc::new(RwLock::new(()));

    let mut group = c.benchmark_group("mem_tier_checkpoint_fence_stall");
    for &(label, rtt) in RTTS {
        let fence_a = Arc::clone(&fence);
        group.bench_with_input(
            BenchmarkId::new("encode_commit_under_fence", label),
            &rtt,
            |b, &rtt| {
                let fence = Arc::clone(&fence_a);
                b.to_async(&rt).iter_custom(|iters| {
                    let fence = Arc::clone(&fence);
                    async move {
                        let mut held = Duration::ZERO;
                        for _ in 0..iters {
                            held += encode_commit_under_fence(&fence, rtt).await;
                        }
                        // The fence-held span must cover at least the encode +
                        // commit it serializes — guards against the model silently
                        // measuring nothing.
                        assert!(
                            held >= ENCODE.saturating_mul(u32::try_from(iters).unwrap_or(u32::MAX)),
                            "under-fence lane must hold the fence across the encode"
                        );
                        held
                    }
                });
            },
        );

        let fence_b = Arc::clone(&fence);
        group.bench_with_input(
            BenchmarkId::new("swap_only_under_fence", label),
            &rtt,
            |b, &rtt| {
                let fence = Arc::clone(&fence_b);
                b.to_async(&rt).iter_custom(|iters| {
                    let fence = Arc::clone(&fence);
                    async move {
                        let mut held = Duration::ZERO;
                        for _ in 0..iters {
                            held += swap_only_under_fence(&fence, rtt).await;
                        }
                        held
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_checkpoint_fence_stall,
    bench_mem_tier_checkpoint_fence_stall
);
criterion_main!(benches);
