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

//! Regression bench: per-scan cost of cloning cached `Statistics` for the
//! optimizer in `CayenneTableProvider::cached_table_statistics_for_optimizer`
//! (`crates/cayenne/src/provider/table.rs:3304-3331`).
//!
//! The optimizer hot path runs once per `TableProvider::statistics()` call,
//! which DataFusion makes for every scan and for several physical-optimizer
//! rules (column pruning, partition pruning, join order, exact-join-filter
//! sizing). The current implementation:
//!
//! ```ignore
//! let stats = stats.clone();                       // O(num_columns) deep clone
//! if has_pending_visibility_changes {
//!     Some(Self::statistics_to_inexact(stats))     // O(num_columns) re-build
//! } else {
//!     Some(stats)
//! }
//! ```
//!
//! Each `ColumnStatistics` carries up to five `Precision<ScalarValue>` fields
//! (`null_count`, `min_value`, `max_value`, `sum_value`, `distinct_count`)
//! plus `byte_size`. Cloning a `Precision<ScalarValue>` heap-allocates for
//! variable-width scalars (Utf8, Binary, List, Struct, decimal-256, …). On
//! pending overlays the entire `Vec` is consumed by `into_iter`,
//! `to_inexact` is called on every field of every column, and the
//! `Statistics` is rebuilt.
//!
//! For a 256-column table that is the practical ceiling that the recent
//! `TABLE_STATISTICS_FULL_COLUMN_SYNC_LIMIT = 256` workaround
//! (commit `2d5ced3d7f`) was chosen to bound. The workaround returns
//! top-level stats only (`num_rows`, `total_byte_size`, empty
//! `column_statistics`) for wider tables — preserving the planner from a
//! per-scan clone cliff at the cost of losing column min/max information
//! that the optimizer needs for partition pruning, exact-join-filter
//! sizing, and join-order cost models.
//!
//! Two unresolved concerns this bench surfaces:
//!
//! 1. **Cliff above 256**: tables with 257+ columns silently lose all
//!    column-level statistics for optimizer planning. A plan that would
//!    have pruned 95% of files on a 200-column table can degenerate to a
//!    full scan on a 300-column table for the same query shape.
//! 2. **Cost below 256**: even at 100-200 columns the per-scan clone is a
//!    measurable fraction of planning latency on overlay-active tables
//!    (writes still pending, inline rows present). Reused across every
//!    optimizer rule that calls `statistics()`, the cost compounds.
//!
//! The TigerStyle remedy is to share the cached `Statistics` by `Arc` and
//! lazy-transform only when an overlay is active (or never, if callers can
//! accept a `Cow<'_, Statistics>`-style API). One allocation per write,
//! not per scan.
//!
//! ## What this bench measures
//!
//! Pure CPU shape — no Cayenne setup, no metastore, no DataFusion planner.
//! Models the per-scan body of `cached_table_statistics_for_optimizer` at
//! four column counts that bracket the workaround threshold:
//!
//! - 64 columns:  typical narrow table.
//! - 200 columns: just under the workaround threshold; still pays the clone.
//! - 256 columns: at the threshold; still pays the clone (workaround
//!   triggers at `> 256`, i.e. 257+).
//! - 1024 columns: well past the threshold; pays the workaround's
//!   top-level path and loses column stats entirely.
//!
//! Three lanes per width:
//!
//! - `full_clone_no_overlay/<cols>` — mirrors today's no-overlay path
//!   (`stats.clone()` then return). Wall time is the deep `Vec<ColumnStatistics>`
//!   clone.
//! - `full_clone_with_overlay/<cols>` — mirrors today's overlay path
//!   (`stats.clone()` then `statistics_to_inexact`). Wall time is the
//!   clone plus the per-column `to_inexact` rebuild — i.e. the path
//!   taken on inserts-pending-checkpoint and pending-deletion tables.
//! - `top_level_only/<cols>` — mirrors the wide-table workaround
//!   (`top_level_statistics_only`). Wall time is two `Precision` clones.
//!   Used at 1024 columns to model the workaround floor.
//!
//! ## How to read
//!
//! `cargo bench --bench cached_table_statistics_wide -p cayenne`.
//!
//! - `full_clone_with_overlay/256` — per-scan tax on an overlay-active
//!   200-column table. At 10K scans/sec on the read path, multiplying by
//!   this number gives the planner-side CPU floor.
//! - The ratio `full_clone_with_overlay/256` vs `top_level_only/256` is
//!   the headroom from sharing stats via `Arc` (or moving the workaround
//!   lower). Per-call clone dominates; the per-column copy is the
//!   wallclock weight.
//! - The jump between `full_clone_with_overlay/64` and
//!   `full_clone_with_overlay/256` is the symbol-of-cost the workaround
//!   was sized to dodge.

#![allow(clippy::expect_used)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};

/// Column counts bracketing the wide-table workaround threshold of 256.
const COLUMN_COUNTS: &[usize] = &[64, 200, 256, 1024];

/// Build a `Statistics` shaped like a metastore-loaded snapshot: every
/// column has an exact min/max as `ScalarValue::Utf8` (the cliff is
/// variable-width allocator-bound, not memcpy-bound — Int64 stats are
/// faster but the production mix is dominated by string/decimal/timestamp
/// columns whose `ScalarValue` clones heap-allocate).
fn build_stats(num_columns: usize) -> Statistics {
    let mut column_statistics = Vec::with_capacity(num_columns);
    for i in 0..num_columns {
        column_statistics.push(ColumnStatistics {
            null_count: Precision::Exact(0),
            min_value: Precision::Exact(ScalarValue::Utf8(Some(format!("min_value_{i:06}")))),
            max_value: Precision::Exact(ScalarValue::Utf8(Some(format!("max_value_{i:06}")))),
            sum_value: Precision::Absent,
            distinct_count: Precision::Exact(1_024),
            byte_size: Precision::Exact(8_192),
        });
    }

    Statistics {
        num_rows: Precision::Exact(1_000_000),
        total_byte_size: Precision::Exact(64 * 1024 * 1024),
        column_statistics,
    }
}

/// Mirrors `column_statistics_to_inexact` in
/// `crates/cayenne/src/provider/table.rs:3364-3373`. Reproduced inline
/// because the method is private to `CayenneTableProvider`.
fn column_statistics_to_inexact(stats: ColumnStatistics) -> ColumnStatistics {
    ColumnStatistics {
        null_count: stats.null_count.to_inexact(),
        max_value: stats.max_value.to_inexact(),
        min_value: stats.min_value.to_inexact(),
        sum_value: stats.sum_value.to_inexact(),
        distinct_count: stats.distinct_count.to_inexact(),
        byte_size: stats.byte_size.to_inexact(),
    }
}

/// Mirrors `statistics_to_inexact` in
/// `crates/cayenne/src/provider/table.rs:3352-3362`.
fn statistics_to_inexact(stats: Statistics) -> Statistics {
    Statistics {
        num_rows: stats.num_rows.to_inexact(),
        total_byte_size: stats.total_byte_size.to_inexact(),
        column_statistics: stats
            .column_statistics
            .into_iter()
            .map(column_statistics_to_inexact)
            .collect(),
    }
}

/// Mirrors `top_level_statistics_only` in
/// `crates/cayenne/src/provider/table.rs:3333-3350`. The wide-table
/// workaround: returns an empty `column_statistics` and clones only the
/// two top-level `Precision` fields.
fn top_level_statistics_only(stats: &Statistics, inexact: bool) -> Statistics {
    let num_rows = if inexact {
        stats.num_rows.clone().to_inexact()
    } else {
        stats.num_rows.clone()
    };
    let total_byte_size = if inexact {
        stats.total_byte_size.clone().to_inexact()
    } else {
        stats.total_byte_size.clone()
    };

    Statistics {
        num_rows,
        total_byte_size,
        column_statistics: Vec::new(),
    }
}

fn bench_full_clone_no_overlay(c: &mut Criterion) {
    let mut group = c.benchmark_group("cached_table_statistics_full_clone_no_overlay");
    for &n in COLUMN_COUNTS {
        let stats = build_stats(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let cloned = stats.clone();
                black_box(cloned);
            });
        });
    }
    group.finish();
}

fn bench_full_clone_with_overlay(c: &mut Criterion) {
    let mut group = c.benchmark_group("cached_table_statistics_full_clone_with_overlay");
    for &n in COLUMN_COUNTS {
        let stats = build_stats(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let cloned = stats.clone();
                let inexact = statistics_to_inexact(cloned);
                black_box(inexact);
            });
        });
    }
    group.finish();
}

fn bench_top_level_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("cached_table_statistics_top_level_only");
    for &n in COLUMN_COUNTS {
        let stats = build_stats(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let top_only = top_level_statistics_only(&stats, true);
                black_box(top_only);
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_full_clone_no_overlay,
    bench_full_clone_with_overlay,
    bench_top_level_only,
);
criterion_main!(benches);
