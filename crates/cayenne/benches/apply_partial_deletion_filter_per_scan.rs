// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Regression bench: per-scan-per-protected-snapshot O(M) rebuild cost in
//! [`crate::provider::table::CayenneTableProvider::apply_partial_deletion_filter`]
//! (`crates/cayenne/src/provider/table.rs:7100-7173`).
//!
//! Every call to `scan_protected_snapshots` walks every protected snapshot
//! and, for each, hits `apply_partial_deletion_filter` which today does:
//!
//! ```ignore
//! let filtered_deletions: HashMap<i64, i64> = deleted_pk_values
//!     .entries()
//!     .iter()
//!     .filter(|(_, seq)| **seq > min_delete_seq_to_apply)
//!     .map(|(&pk, &seq)| (pk, seq))
//!     .collect();                                   // O(M_total) iter + O(M_filtered) alloc
//! ...
//! DeletionIndex::from_map(filtered_deletions)       // O(M_filtered) bloom rebuild
//! ```
//!
//! For N protected snapshots and a deletion cache of M total entries each
//! scan pays `O(N · (M_total + M_filtered))` allocator + hashing work
//! before any data is read. With the existing warn-at-N≥4 threshold
//! (`provider/table.rs:6980-6986`) this is bounded but real: 4 snapshots
//! at 100 K entries each ≈ 1.6 MB of HashMap allocator traffic + 4 fresh
//! bloom filter rebuilds per scan.
//!
//! The TigerStyle remedy is to apply the `min_seq` filter at probe time
//! (one `seq > min_seq` comparison per matched PK in
//! `Int64PkDeletionFilterStream::poll_next`) and reuse the existing
//! `DeletionIndex` instance across protected snapshots. The probe path is
//! already cheap because of the bloom prefilter — adding an integer
//! comparison after a confirmed map hit is a constant per match, not per
//! cached entry.
//!
//! ## What this bench measures
//!
//! Pure shape — no Cayenne setup, no metastore. Two lanes per
//! `(deletion_cache_size, protected_snapshot_count)`:
//!
//! - `current_rebuild_per_snapshot` — for each protected snapshot,
//!   filter+collect+from_map. Mirrors the body of
//!   `apply_partial_deletion_filter`. Cost scales as O(N · M).
//! - `probe_time_filter` — model: share one `Arc<DeletionIndex>` across
//!   all snapshots, do nothing extra at scan-plan time. Cost is O(N) just
//!   to count the snapshots; the per-snapshot work is amortized into the
//!   probe loop which is not measured here (that cost is a constant per
//!   probe, regardless of how many snapshots exist).
//!
//! The gap visualizes the per-plan-build overhead the proposed fix would
//! eliminate. The probe-time work added by the fix is *not* captured by
//! this bench — but `deletion_index_probe.rs` already measures the per-row
//! probe cost, and adding one `seq > min_seq` comparison per matched
//! probe is well below the bloom check cost, so the swap is a net win
//! whenever `N · M_plan_build_cost > M_extra_probe_cost`.
//!
//! `cargo bench --bench apply_partial_deletion_filter_per_scan -p cayenne`.

#![expect(clippy::expect_used)]
#![expect(clippy::cast_possible_wrap)]

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use cayenne::provider::deletion_index::DeletionIndex;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

/// Total deletion-cache sizes to test. Bracket realistic shapes:
/// - 1 K       — fresh table, a few PK deletes accumulated.
/// - 10 K      — typical operational state between compactions.
/// - 100 K     — long-lived table absorbing many deletes.
const CACHE_SIZES: &[usize] = &[1_000, 10_000, 100_000];

/// Protected snapshot counts. The warn-at-4 threshold in
/// `scan_protected_snapshots` triggers above this; 8 stresses past it.
const SNAPSHOT_COUNTS: &[usize] = &[1, 4, 8];

/// Build a deletion cache with `size` entries. Sequence numbers are spread
/// uniformly across `1..=size` so the `min_seq` filter retains roughly half
/// the entries for the test's chosen cutoff — matches the typical "older
/// half of deletes don't apply" shape that protected snapshots produce.
fn build_deletion_cache(size: usize) -> Arc<DeletionIndex> {
    let mut entries = HashMap::with_capacity(size);
    for i in 0..size {
        entries.insert(i as i64, i as i64 + 1);
    }
    Arc::new(DeletionIndex::from_map(entries))
}

/// Lane A: mirrors `apply_partial_deletion_filter` body for every protected
/// snapshot. Each iteration filter+collect+rebuild for N snapshots.
fn run_full_rebuild(cache: &Arc<DeletionIndex>, snapshot_count: usize, min_seq: i64) {
    for _ in 0..snapshot_count {
        let filtered: HashMap<i64, i64> = cache
            .entries()
            .iter()
            .filter(|(_, seq)| **seq > min_seq)
            .map(|(&pk, &seq)| (pk, seq))
            .collect();
        let index = DeletionIndex::from_map(filtered);
        black_box(index);
    }
}

/// Lane B: models the proposed fix — share the existing `Arc<DeletionIndex>`
/// across protected snapshots and defer the `min_seq` filter to probe time.
/// Plan-build cost collapses to N Arc clones.
fn run_probe_time_filter(cache: &Arc<DeletionIndex>, snapshot_count: usize, _min_seq: i64) {
    for _ in 0..snapshot_count {
        let shared = Arc::clone(cache);
        black_box(shared);
    }
}

fn bench_apply_partial_deletion_filter_per_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_partial_deletion_filter_per_scan");
    group.sample_size(10);

    for &cache_size in CACHE_SIZES {
        let cache = build_deletion_cache(cache_size);
        // Cutoff that retains ~half the entries — typical shape from
        // protected-snapshot-creation-time max-delete-sequence captures.
        let min_seq = (cache_size as i64) / 2;

        for &snapshot_count in SNAPSHOT_COUNTS {
            // Throughput = total entries touched across all snapshots, so
            // the bench reports per-entry plan-build cost.
            let entries_touched = u64::try_from(cache_size * snapshot_count).unwrap_or(u64::MAX);
            group.throughput(Throughput::Elements(entries_touched));

            let id = format!("M={cache_size}/N={snapshot_count}");
            let cache_a = Arc::clone(&cache);
            group.bench_with_input(
                BenchmarkId::new("current_rebuild_per_snapshot", &id),
                &snapshot_count,
                |b, &snapshot_count| {
                    b.iter(|| {
                        run_full_rebuild(&cache_a, snapshot_count, min_seq);
                    });
                },
            );
            let cache_b = Arc::clone(&cache);
            group.bench_with_input(
                BenchmarkId::new("probe_time_filter", &id),
                &snapshot_count,
                |b, &snapshot_count| {
                    b.iter(|| {
                        run_probe_time_filter(&cache_b, snapshot_count, min_seq);
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_apply_partial_deletion_filter_per_scan);
criterion_main!(benches);
