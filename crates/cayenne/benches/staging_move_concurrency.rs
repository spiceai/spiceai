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

//! Regression bench: per-file serial latency of the S3 staged-file move
//! during the CDC pipelined finalize barrier.
//!
//! Older versions of `CayenneTableProvider::move_staging_files_s3` moved
//! files out of `_staging/<id>/` into the live snapshot directory with two
//! serial loops:
//!
//! ```ignore
//! // Phase 1: copy
//! for meta in &objects {
//!     store.copy(&meta.location, &target_path).await?;
//!     copied_locations.push(meta.location.clone());
//! }
//! // Phase 2: delete staging originals
//! for location in &copied_locations {
//!     store.delete(location).await?;
//! }
//! ```
//!
//! Both phases iterated serially with `.await` between each S3 round trip.
//! The move runs under `apply_under_barrier` which holds `visibility_lock`
//! plus the `listing_fence` write guard across the entire move — so every
//! concurrent scan that reaches `listing_fence.read().await` blocked until
//! the move completed. For a CDC burst that produced `N` Vortex files, the
//! held-fence time included `2 · N · RTT_s3` (copy RTT + delete RTT per
//! file). On S3 with ~10–30 ms per op, a 64-file burst stalled every reader
//! for ~1.3–3.8 s.
//!
//! The production path now drives both phases through
//! `stream::iter(...).try_for_each_concurrent(OBJECT_STORE_MOVE_CONCURRENCY, ...)`
//! ([`provider/table.rs:2551-2591`], with
//! `OBJECT_STORE_MOVE_CONCURRENCY = 16`). The fence-held time is now
//! `RTT_s3 · (N / parallelism) + RTT_s3 · (N / parallelism)` — for
//! `parallelism=16` and N=64, ~8 RTTs total instead of 128.
//!
//! ## What this bench measures
//!
//! Two lanes, identical work — move `N` 4 KiB objects between two
//! `object_store::memory::InMemory` prefixes. Per-op latency is simulated
//! by `tokio::time::sleep(SIMULATED_S3_RTT)` immediately before each
//! `copy` / `delete`. This isolates the scheduling pattern (serial loop
//! vs `try_for_each_concurrent`) from real-network jitter.
//!
//! - `staging_move/serial_baseline/<N>` — mirrors the older serial loop in
//!   `move_staging_files_s3`. Time grows linearly with `N`.
//! - `staging_move/concurrent/<N>` — current behavior:
//!   `try_for_each_concurrent(16)` over both phases. Time grows as
//!   `N / 16` (one RTT batch + a tail).
//!
//! Both lanes use the same store, the same byte payload, and the same
//! source/destination paths so the only difference is dispatch pattern.
//!
//! ## How to read the report
//!
//! After `cargo bench --bench staging_move_concurrency -p cayenne`:
//!
//! - Look at `staging_move/serial_baseline/64` vs
//!   `staging_move/concurrent/64`. The ratio is approximately
//!   `min(64, 16) * 2 / ceil(64 / 16) * 2` ≈ 16×. That ratio is the
//!   reduction in fence-held time the production fix delivered.
//! - The `serial_baseline` lane is the **regression to track**: if a
//!   future change reintroduces a serial loop here, this gap reappears.
//! - The `concurrent` lane is the current production floor.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use object_store::ObjectStoreExt;
use object_store::ObjectStore;
use object_store::PutPayload;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectStorePath;
use tokio::runtime::Runtime;

/// Simulated S3 per-op round trip. 50 µs keeps each iteration in the low
/// millisecond range so Criterion can collect samples quickly while still
/// dominating any in-process `InMemory::copy` cost (sub-microsecond).
const SIMULATED_S3_RTT: Duration = Duration::from_micros(50);

/// File counts straddle the small-burst / large-burst boundary. 16 is a
/// typical CDC append (a few small Vortex files); 256 is a fan-out burst
/// or a partitioned table.
const FILE_COUNTS: &[usize] = &[16, 64, 256];

/// Concurrency for the achievable-concurrent lane. Matches a reasonable
/// `buffer_unordered` width for S3 — large enough to saturate, small
/// enough to avoid hammering the underlying store with thousands of
/// in-flight requests.
const CONCURRENCY: usize = 16;

/// Tiny payload — the cost we are measuring is dispatch, not bandwidth.
const PAYLOAD_BYTES: usize = 4 * 1024;

fn payload() -> PutPayload {
    PutPayload::from(vec![0u8; PAYLOAD_BYTES])
}

fn staging_path(i: usize) -> ObjectStorePath {
    ObjectStorePath::from(format!("_staging/burst-1/data-{i:06}.vortex"))
}

fn target_path(i: usize) -> ObjectStorePath {
    ObjectStorePath::from(format!("current/data-{i:06}.vortex"))
}

/// Seed `n` staging files in a fresh `InMemory` store. The cost of this
/// setup is deliberately outside Criterion's measurement window via
/// `iter_batched`.
async fn seed_store(n: usize) -> Arc<InMemory> {
    let store = Arc::new(InMemory::new());
    for i in 0..n {
        store
            .put(&staging_path(i), payload())
            .await
            .expect("seed put");
    }
    store
}

/// Mirrors `CayenneTableProvider::move_staging_files_s3`
/// (`crates/cayenne/src/provider/table.rs:2122-2221`): Phase 1 copies
/// every staged file to the target prefix serially, then Phase 2 deletes
/// each staged original serially. Each `.await` represents one S3 round
/// trip held under the listing-fence write guard.
async fn serial_copy_then_delete(store: Arc<InMemory>, n: usize) {
    let mut copied = Vec::with_capacity(n);
    // Phase 1: copy.
    for i in 0..n {
        let src = staging_path(i);
        let dst = target_path(i);
        tokio::time::sleep(SIMULATED_S3_RTT).await;
        store.copy(&src, &dst).await.expect("copy");
        copied.push(src);
    }
    // Phase 2: delete.
    for src in &copied {
        tokio::time::sleep(SIMULATED_S3_RTT).await;
        store.delete(src).await.expect("delete");
    }
}

/// Achievable pattern: `buffer_unordered` across both phases. Same
/// two-phase ordering as the serial variant (Phase 2 only begins after
/// Phase 1 fully drains) so crash-safety semantics are preserved.
async fn concurrent_copy_then_delete(store: Arc<InMemory>, n: usize) {
    // Phase 1: copy.
    let store_phase1 = Arc::clone(&store);
    stream::iter(0..n)
        .map(|i| {
            let store = Arc::clone(&store_phase1);
            async move {
                let src = staging_path(i);
                let dst = target_path(i);
                tokio::time::sleep(SIMULATED_S3_RTT).await;
                store.copy(&src, &dst).await
            }
        })
        .buffer_unordered(CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await
        .expect("phase 1 copy");

    // Phase 2: delete.
    let store_phase2 = Arc::clone(&store);
    stream::iter(0..n)
        .map(|i| {
            let store = Arc::clone(&store_phase2);
            async move {
                let src = staging_path(i);
                tokio::time::sleep(SIMULATED_S3_RTT).await;
                store.delete(&src).await
            }
        })
        .buffer_unordered(CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await
        .expect("phase 2 delete");
}

fn bench_staging_move(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    let mut group = c.benchmark_group("staging_move");
    // Throughput per file makes the per-file scheduling cost legible in
    // Criterion's report.
    for &n in FILE_COUNTS {
        group.throughput(Throughput::Elements(u64::try_from(n).unwrap_or(u64::MAX)));

        // Setup runs inside the async body — `iter_batched` with a sync
        // closure cannot use `Runtime::block_on` because it executes inside
        // the runtime that `to_async` has already entered. The per-iteration
        // seed cost is `n` cheap `InMemory::put` calls (no simulated RTT)
        // and is identical across both lanes, so it does not skew the
        // serial-vs-concurrent ratio that this bench measures.
        group.bench_with_input(BenchmarkId::new("serial_baseline", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let store = seed_store(n).await;
                serial_copy_then_delete(black_box(store), black_box(n)).await;
            });
        });

        group.bench_with_input(BenchmarkId::new("concurrent", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let store = seed_store(n).await;
                concurrent_copy_then_delete(black_box(store), black_box(n)).await;
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_staging_move);
criterion_main!(benches);
