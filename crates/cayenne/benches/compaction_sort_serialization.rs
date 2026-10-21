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

//! Regression bench: compaction throughput cliff when `sort_columns` is
//! configured.
//!
//! `CayenneTableProvider::rewrite_current_snapshot_for_compaction`
//! (`crates/cayenne/src/provider/table.rs:4810-4889`) hard-codes
//! `target_partitions = 1` when `sort_columns` is set on the table:
//!
//! ```ignore
//! let target_partitions = if self.context.has_sort_columns() {
//!     stream = self.sort_stream(stream)?;
//!     1                                        // ← single writer
//! } else {
//!     ctx.state().config().target_partitions() // ← parallel writers
//! };
//! ```
//!
//! The trade-off is real: sorted output produces tight per-file zone maps,
//! which makes downstream OLAP queries dramatically faster on the
//! sort-column predicate. But the compaction rewrite itself loses all
//! writer parallelism: a 300M-row `order_line` table that finishes
//! compaction in minutes without sort_columns takes much longer with
//! sort_columns because a single Vortex writer thread serially encodes
//! every row.
//!
//! This was the question raised in the May 15 2026 SF100 retest:
//! *"How common is it to define sort columns on large tables in production
//! Cayenne deployments? Is the unsorted configuration representative of
//! typical usage?"* The 30× bootstrap improvement that report measured is
//! configuration-specific — production deployments that need `sort_columns`
//! for OLAP query performance pay the K× cliff this bench captures.
//!
//! The fix is a parallel sort-merge: range-partition the input by sort
//! key, sort each partition in parallel, and write each partition through
//! its own Vortex writer. Final output is split across K files (matching
//! today's `target_partitions=K` model) and each file is internally
//! sorted, so per-file zone maps stay tight. DataFusion already has
//! `SortPreservingMergeExec` for the merge layer; what is missing is the
//! `range-partition before sort` rewrite for the compaction path
//! specifically.
//!
//! ## What this bench measures
//!
//! Pure shape — no Vortex, no Cayenne setup. Models the
//! `target_partitions=1` (sorted) vs `target_partitions=K` (unsorted)
//! cliff on a synthetic stream of N rows.
//!
//! Per-row "write work" is simulated by a small CPU-bound function
//! (`xor`, `wrapping_mul`, `memcpy`) so the parallelism story is
//! observable as wall-clock speedup. The exact per-row cost does not
//! matter — only the ratio between lanes.
//!
//! Three lanes per `N_rows`:
//!
//! - `serial_sort_then_write/N` — mirrors today's sort_columns
//!   compaction path. Allocates a `Vec<Row>` of all rows, sorts it by
//!   the synthetic sort key, then processes every row on one thread.
//!   Time = sort + N · per-row-work.
//! - `parallel_write_unsorted/N` — mirrors today's unsorted compaction
//!   path. Round-robins N rows across `K = num_cpus.min(16)` worker
//!   threads. No sort. Time = N · per-row-work / K.
//! - `parallel_sort_then_merge_write/N` — models the proposed fix.
//!   Range-partitions input across K threads, sorts each partition in
//!   parallel, then each thread writes its partition. Time = sort/K +
//!   N · per-row-work / K. Total output is sorted within each partition
//!   (no global merge needed for compaction since each Vortex file is
//!   independently zone-mapped).
//!
//! ## How to read
//!
//! `cargo bench --bench compaction_sort_serialization -p cayenne`. At
//! `N_rows = 4_000_000` on a multi-core box:
//!
//! - `serial_sort_then_write` is the regression baseline. Slope is
//!   bounded by single-thread throughput.
//! - `parallel_write_unsorted` is the headroom **without** sort_columns
//!   — the K× speedup over serial.
//! - `parallel_sort_then_merge_write` is the headroom **with** the
//!   proposed fix — should approach `parallel_write_unsorted` minus the
//!   per-partition sort cost (O((N/K) log (N/K))).
//!
//! The gap between `serial_sort_then_write` and
//! `parallel_sort_then_merge_write` is what production deployments using
//! `sort_columns` could reclaim at compaction time. For
//! N = 4_000_000 rows and K = 16, the gap should be ~10-14× (sort itself
//! is sub-linear; the dominant savings come from parallel write work).

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::thread;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Total input rows. Three sizes to show the curve:
/// - 100 K: a small compaction (background tier 0).
/// - 1 M:   a medium compaction (tier 1 / 2).
/// - 4 M:   a large compaction (tier 3, single-partition production
///   `order_line` at SF10).
const ROW_COUNTS: &[usize] = &[100_000, 1_000_000, 4_000_000];

/// Worker count for parallel lanes. Capped at 16 so the bench runs in
/// reasonable time across hardware shapes; production picks
/// `SessionConfig::target_partitions()`, typically `num_cpus`.
fn worker_count() -> usize {
    std::thread::available_parallelism()
        .map_or(4, |n| n.get())
        .min(16)
}

/// Synthetic row: 16 bytes of payload + an i64 sort key. Width is
/// representative of a narrow CDC row (PK + small payload).
#[derive(Clone)]
struct Row {
    sort_key: i64,
    _payload: [u8; 16],
}

fn make_row(idx: usize) -> Row {
    // Scrambled sort key so the input is unsorted but deterministic.
    let scrambled = (idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let sort_key = scrambled as i64;
    let mut payload = [0u8; 16];
    payload[..8].copy_from_slice(&scrambled.to_le_bytes());
    Row {
        sort_key,
        _payload: payload,
    }
}

/// Simulated per-row work that a Vortex writer does: a few non-trivial
/// integer ops + a memcpy. Inline-never so the optimizer cannot hoist
/// it out of the loop or fuse it across rows.
#[inline(never)]
fn per_row_work(row: &Row, acc: u64) -> u64 {
    let mut sink = [0u8; 16];
    sink.copy_from_slice(&row._payload);
    let mixed = u64::from_le_bytes(sink[..8].try_into().expect("8 bytes"))
        .wrapping_mul(0x9E37_79B9_7F4A_7C15);
    acc.wrapping_add(mixed ^ row.sort_key as u64).rotate_left(7)
}

fn generate_rows(n: usize) -> Vec<Row> {
    (0..n).map(make_row).collect()
}

/// Lane A: serial sort + single writer (today's sort_columns path).
fn serial_sort_then_write(n: usize) -> u64 {
    let mut rows = generate_rows(n);
    rows.sort_unstable_by_key(|r| r.sort_key);

    let mut acc = 0u64;
    for row in &rows {
        acc = per_row_work(row, acc);
    }
    black_box(&rows);
    acc
}

/// Lane B: parallel writer, no sort (today's unsorted path).
fn parallel_write_unsorted(n: usize) -> u64 {
    let rows = generate_rows(n);
    let k = worker_count();
    let chunk = n.div_ceil(k);

    let total: u64 = thread::scope(|s| {
        let mut handles = Vec::with_capacity(k);
        let rows_ref = &rows;
        for w in 0..k {
            let start = w * chunk;
            let end = (start + chunk).min(n);
            if start >= end {
                break;
            }
            handles.push(s.spawn(move || {
                let mut acc = 0u64;
                for row in &rows_ref[start..end] {
                    acc = per_row_work(row, acc);
                }
                acc
            }));
        }
        handles.into_iter().map(|h| h.join().expect("join")).sum()
    });
    black_box(&rows);
    total
}

/// Lane C: parallel sort + parallel writer (proposed fix). Range-
/// partition by sort key bucket, sort each partition in parallel, write
/// in parallel. Each output partition is independently sorted, which is
/// sufficient for Cayenne's per-file zone maps.
fn parallel_sort_then_merge_write(n: usize) -> u64 {
    let rows = generate_rows(n);
    let k = worker_count();

    // Range-partition by the high bits of sort_key. For our scrambled
    // input the bucket distribution is approximately uniform — same
    // shape as a real range-partition over a high-cardinality column.
    let mut buckets: Vec<Vec<Row>> = (0..k).map(|_| Vec::with_capacity(n / k + 1)).collect();
    let bits = (k as u64).next_power_of_two().trailing_zeros();
    for row in rows {
        let key = row.sort_key as u64;
        let bucket = ((key >> (64 - bits)) as usize).min(k - 1);
        buckets[bucket].push(row);
    }

    let total: u64 = thread::scope(|s| {
        let mut handles = Vec::with_capacity(k);
        for bucket in buckets {
            handles.push(s.spawn(move || {
                let mut local = bucket;
                local.sort_unstable_by_key(|r| r.sort_key);
                let mut acc = 0u64;
                for row in &local {
                    acc = per_row_work(row, acc);
                }
                black_box(&local);
                acc
            }));
        }
        handles.into_iter().map(|h| h.join().expect("join")).sum()
    });
    total
}

fn bench_compaction_sort_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction_sort_serialization");
    for &n in ROW_COUNTS {
        group.throughput(Throughput::Elements(u64::try_from(n).unwrap_or(u64::MAX)));

        group.bench_with_input(
            BenchmarkId::new("serial_sort_then_write", n),
            &n,
            |b, &n| {
                b.iter(|| serial_sort_then_write(black_box(n)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("parallel_write_unsorted", n),
            &n,
            |b, &n| {
                b.iter(|| parallel_write_unsorted(black_box(n)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("parallel_sort_then_merge_write", n),
            &n,
            |b, &n| {
                b.iter(|| parallel_sort_then_merge_write(black_box(n)));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_compaction_sort_serialization);
criterion_main!(benches);
