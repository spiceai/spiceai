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
//! Every call to `scan_protected_snapshots` walks every protected snapshot.
//! Older `apply_partial_deletion_filter` code rebuilt a filtered deletion
//! index for each protected snapshot:
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
//! before any data is read. Four snapshots at 100 K entries each mean about
//! 1.6 MB of `HashMap` allocator traffic plus four fresh bloom filter rebuilds
//! per scan.
//!
//! The production path now applies the `min_seq` filter at probe time (one
//! `seq > min_seq` comparison per matched PK in
//! `Int64PkDeletionFilterStream::poll_next`) and reuses the existing
//! `DeletionIndex` instance across protected snapshots.
//!
//! ## What this bench measures
//!
//! Pure shape — no Cayenne setup, no metastore. Two lanes per
//! `(deletion_cache_size, protected_snapshot_count)`:
//!
//! - `rebuild_per_snapshot_baseline` — for each protected snapshot,
//!   filter+collect+from_map. Mirrors the body of
//!   the old `apply_partial_deletion_filter`. Cost scales as O(N · M).
//! - `probe_time_filter` — current behavior: share one `Arc<DeletionIndex>` across
//!   all snapshots, do nothing extra at scan-plan time. Cost is O(N) just
//!   to count the snapshots; the per-snapshot work is amortized into the
//!   probe loop which is not measured here (that cost is a constant per
//!   probe, regardless of how many snapshots exist).
//!
//! The gap visualizes the per-plan-build overhead avoided by probe-time
//! filtering. The probe-time work is not captured by this bench, but
//! `deletion_index_probe.rs` measures the per-row probe cost.
//!
//! `cargo bench --bench apply_partial_deletion_filter_per_scan -p cayenne`.

#![expect(clippy::expect_used)]

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

/// Protected snapshot counts. The default snapshot-maintenance count trigger is 8,
/// so this brackets the normal operating range and the trigger boundary.
const SNAPSHOT_COUNTS: &[usize] = &[1, 4, 8];

/// Build a deletion cache with `size` entries. Sequence numbers are spread
/// uniformly across `1..=size` so the `min_seq` filter retains roughly half
/// the entries for the test's chosen cutoff — matches the typical "older
/// half of deletes don't apply" shape that protected snapshots produce.
fn build_deletion_cache(size: usize) -> Arc<DeletionIndex> {
    let mut entries = HashMap::with_capacity(size);
    for i in 0..size {
        let seq = i64::try_from(i).expect("cache size should fit in i64");
        entries.insert(seq, seq + 1);
    }
    Arc::new(DeletionIndex::from_map(entries))
}

/// Lane A: mirrors `apply_partial_deletion_filter` body for every protected
/// snapshot. Each iteration filter+collect+rebuild for N snapshots.
fn run_full_rebuild(cache: &Arc<DeletionIndex>, snapshot_count: usize, min_seq: i64) {
    for _ in 0..snapshot_count {
        let filtered: HashMap<i64, i64> = cache
            .iter_entries()
            .filter_map(|(pk, entry)| {
                entry
                    .delete_sequence()
                    .filter(|seq| *seq > min_seq)
                    .map(|seq| (pk, seq))
            })
            .collect();
        let index = DeletionIndex::from_map(filtered);
        black_box(index);
    }
}

/// Lane B: current behavior — share the existing `Arc<DeletionIndex>` across
/// protected snapshots and defer the `min_seq` filter to probe time. Plan-build
/// cost collapses to N Arc clones.
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
        let min_seq = i64::try_from(cache_size).expect("cache size should fit in i64") / 2;

        for &snapshot_count in SNAPSHOT_COUNTS {
            // Throughput = total entries touched across all snapshots, so
            // the bench reports per-entry plan-build cost.
            let entries_touched = u64::try_from(cache_size * snapshot_count).unwrap_or(u64::MAX);
            group.throughput(Throughput::Elements(entries_touched));

            let id = format!("M={cache_size}/N={snapshot_count}");
            let cache_a = Arc::clone(&cache);
            group.bench_with_input(
                BenchmarkId::new("rebuild_per_snapshot_baseline", &id),
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

// ============================================================================
// Rank-1 mechanism: a threshold-bearing (protected-snapshot) probe skips the
// huge low-seq base run and walks only the small recent runs. This isolates the
// per-probe GET cost a protected scan pays on bloom-hit (base-resident) rows.
// ============================================================================

/// Base (bootstrap) run sizes — the accumulated low-seq deletions a protected
/// snapshot must NOT pay to walk. Large entries leave L2/L3 so the avoided base
/// GET is a real DRAM miss (the mechanism the lever targets).
const BASE_RUN_SIZES: &[usize] = &[100_000, 1_000_000, 5_000_000];
/// Recent applicable deletions living in `active` (seqs above the cutoff).
const RECENT_DELETES: usize = 2_000;
/// Bloom-hit probes per iteration (base-resident PKs — the GET fires).
const PROBE_COUNT: usize = 4_096;

/// A long-lived index under a protected snapshot: a big low-seq base run
/// (PKs `0..base`, seqs `1..=base`) plus a small recent `active` tier
/// (seqs `> base`). Returns `(index, S = base)` so a protected probe skips the
/// base run (`max_delete_seq = base <= S`) and walks only `active`.
fn build_base_plus_recent(base: usize) -> (Arc<DeletionIndex>, i64) {
    let mut entries = HashMap::with_capacity(base);
    for i in 0..base {
        let pk = i64::try_from(i).expect("base size fits in i64");
        entries.insert(pk, pk + 1); // delete_seq = pk + 1, so max_delete_seq = base
    }
    let mut idx = DeletionIndex::from_map(entries);
    let cutoff = i64::try_from(base).expect("base size fits in i64");
    let recent: Vec<(i64, i64)> = (0..RECENT_DELETES)
        .map(|j| {
            let off = i64::try_from(j).expect("recent count fits in i64");
            (cutoff + off, cutoff + 1 + off) // new PKs, seqs strictly above the cutoff
        })
        .collect();
    idx = idx.extend_max_deletes(recent);
    (Arc::new(idx), cutoff)
}

/// Lane A (main scan / pre-rank-1 behaviour): `get` fuses every run, so each
/// bloom-hit pays the base-run GET. Lane B (protected snapshot / rank-1):
/// `get_with_min_seq(Some(S))` skips the base run and walks only `active`.
fn bench_probe_run_skip(c: &mut Criterion) {
    let mut group = c.benchmark_group("deletion_probe_run_skip");
    group.sample_size(10);

    for &base in BASE_RUN_SIZES {
        let (cache, cutoff) = build_base_plus_recent(base);
        // Spread probes across the whole base to defeat prefetch (DRAM misses).
        let step = (base / PROBE_COUNT).max(1);
        let pks: Vec<i64> = (0..PROBE_COUNT)
            .map(|k| i64::try_from((k * step) % base).expect("pk fits in i64"))
            .collect();
        group.throughput(Throughput::Elements(PROBE_COUNT as u64));
        let id = format!("base={base}");

        let (ca, pa) = (Arc::clone(&cache), pks.clone());
        group.bench_with_input(BenchmarkId::new("fuse_all_none", &id), &base, |b, _| {
            b.iter(|| {
                let mut hits = 0_u64;
                for &pk in &pa {
                    if black_box(ca.get(pk)).is_some() {
                        hits += 1;
                    }
                }
                black_box(hits);
            });
        });

        let (cb, pb) = (Arc::clone(&cache), pks.clone());
        group.bench_with_input(BenchmarkId::new("skip_base_some_s", &id), &base, |b, _| {
            b.iter(|| {
                let mut applied = 0_u64;
                for &pk in &pb {
                    if black_box(cb.get_with_min_seq(pk, Some(cutoff))).is_some() {
                        applied += 1;
                    }
                }
                black_box(applied);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_apply_partial_deletion_filter_per_scan,
    bench_probe_run_skip
);
criterion_main!(benches);
