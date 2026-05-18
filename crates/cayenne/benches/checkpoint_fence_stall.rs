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
//! `CayenneTableProvider::checkpoint_inlined_data`
//! (`crates/cayenne/src/provider/table.rs:5740-5830`) ends by holding the
//! `listing_fence.write()` guard
//! (`table.rs:5823-5827`) across `clear_inlined_metadata_after_checkpoint`
//! (`table.rs:5832-5841`), which issues **two** sequential awaited
//! metastore DELETEs:
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
//! Every concurrent scan acquiring `listing_fence.read().await`
//! (`table.rs:6989`) blocks for the full duration of those two round
//! trips. The in-source comment at `table.rs:5819` claims this is
//! "microseconds in the typical case", which is only true on co-located
//! SQLite without `fsync`. On a remote metastore (Turso wire RTT ~10 ms,
//! managed PostgreSQL ~10-30 ms) two sequential round trips mean every
//! reader stalls 20-60 ms per checkpoint. Sustained inline ingestion
//! triggers `checkpoint_inlined_data` whenever
//! `inline_flush_max_bytes` / `inline_flush_max_rows` /
//! `inline_flush_max_segments` is crossed — typically several times per
//! minute at production ingest rates — so this is a recurring tail-latency
//! source, not a one-time cost.
//!
//! The fix is to fold the two DELETEs into a single metastore
//! transaction: `clear_inlined_data_and_deletes` issues one BEGIN +
//! two DELETEs + one COMMIT in one wire round-trip. The listing-fence
//! bracket then holds for only one RTT instead of two — the in-process
//! cost of the bracket is unchanged but the wire-bound term halves.
//!
//! ## What this bench measures
//!
//! Two lanes, identical fence-bracket pattern, identical "refresh
//! listing table" no-op, identical lock primitive (`tokio::sync::RwLock`
//! — same primitive used by `listing_fence` at `table.rs:880`).
//!
//! Per-call metastore work is simulated by `tokio::time::sleep(rtt)`.
//! Real `InMemory` round-trip time is below the timer resolution, so the
//! sleep is the *only* meaningful work — exactly the model we want
//! because it isolates the sequential-vs-batched pattern from any
//! confounding compute.
//!
//! - `checkpoint_fence_stall/current_two_sequential_deletes/<rtt>` —
//!   `fence.write().await; sleep(rtt).await; sleep(rtt).await; drop(fence);`
//!   Mirrors today's two-DELETE shape.
//! - `checkpoint_fence_stall/achievable_single_batch_delete/<rtt>` —
//!   `fence.write().await; sleep(rtt).await; drop(fence);` Single
//!   batched DELETE.
//!
//! ## How to read
//!
//! `cargo bench --bench checkpoint_fence_stall -p cayenne`. The
//! `current_two_sequential_deletes` lane is ~2× the duration of
//! `achievable_single_batch_delete`. Because the lock is held for the
//! whole duration, **the duration of the current lane is also the
//! worst-case scan tail latency caused by one checkpoint** — every
//! concurrent reader stalls that long. The bench output makes the
//! tail-latency floor visible at three RTTs that cover production
//! deployments:
//!
//! - `rtt_1ms` — local SQLite with `fsync` (best case).
//! - `rtt_10ms` — same-zone network metastore (typical Turso / managed
//!   Postgres).
//! - `rtt_30ms` — cross-region network metastore.
//!
//! Use the `current_two_sequential_deletes/rtt_30ms` value as the
//! upper bound on how long a scan can hang during one checkpoint.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

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

async fn current_two_sequential_deletes(fence: &RwLock<()>, rtt: Duration) {
    let _guard = fence.write().await;
    // clear_inlined_data — first metastore round trip.
    tokio::time::sleep(rtt).await;
    // clear_inlined_deletes — second metastore round trip.
    tokio::time::sleep(rtt).await;
    refresh_listing_table_no_op();
}

async fn achievable_single_batch_delete(fence: &RwLock<()>, rtt: Duration) {
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
            BenchmarkId::new("current_two_sequential_deletes", label),
            &rtt,
            |b, &rtt| {
                let fence = Arc::clone(&fence_a);
                b.to_async(&rt)
                    .iter(|| async { current_two_sequential_deletes(&fence, rtt).await });
            },
        );

        let fence_b = Arc::clone(&fence);
        group.bench_with_input(
            BenchmarkId::new("achievable_single_batch_delete", label),
            &rtt,
            |b, &rtt| {
                let fence = Arc::clone(&fence_b);
                b.to_async(&rt)
                    .iter(|| async { achievable_single_batch_delete(&fence, rtt).await });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_checkpoint_fence_stall);
criterion_main!(benches);
