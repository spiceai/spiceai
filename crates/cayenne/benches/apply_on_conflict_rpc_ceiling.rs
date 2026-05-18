// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Regression bench: per-upsert metastore RPC count in
//! [`crate::provider::table::CayenneTableProvider::apply_on_conflict_deletions`]
//! (`crates/cayenne/src/provider/table.rs:4262-4479`).
//!
//! The on-conflict path runs a 3+ RPC sequence on every upsert that
//! produces deletion vectors, with **no transaction** wrapping any of
//! them:
//!
//! 1. `catalog.increment_sequence_number(table_id)` — 1 RPC
//! 2. `DeletionVectorWriter::write(specs)` — writes deletion-vector
//!    files to disk (NOT counted here; we measure metastore RPCs only)
//! 3. For each `DeletionVectorWriteResult`:
//!    `catalog.add_delete_file(result.delete_file)` — 1 RPC per file
//! 4. `catalog.add_insert_records_batch(...)` — 1 RPC
//!
//! For a typical PK-mode upsert that produces 1-2 delete files the
//! cumulative cost is 3-4 metastore RPCs per upsert. The bigger issue is
//! that **none of these are wrapped in a transaction**, so a crash
//! between step 3 and step 4 leaves the catalog with delete-file records
//! at `delete_sequence` but no insert-record at `insert_sequence`. On
//! restart the new row is then permanently hidden by the deletion filter
//! (the comment at `provider/table.rs:4389-4391` explicitly acknowledges
//! this durability requirement).
//!
//! The fix mirrors the established pattern for atomic multi-row catalog
//! work — see `commit_inlined_mutation` (`cayenne_catalog.rs`) and the
//! `commit_compaction_in_txn` / `commit_overwrite_in_txn` family. A new
//! `commit_on_conflict_deletions` trait method opens one transaction,
//! INSERTs every `cayenne_delete_file` row, INSERTs every insert-record
//! row (chunked under SQLite's 32 K-param cap, as
//! `add_insert_records_batch_in_chunks` already does internally), and
//! commits. Crash anywhere before commit → catalog state unchanged → the
//! upsert is fully re-driveable from the calling write path.
//!
//! Counted RPC totals:
//!
//! | path                                       | RPCs per upsert | atomic? |
//! |--------------------------------------------|-----------------|---------|
//! | today (`apply_on_conflict_deletions`)     | `2 + delete_files` | no   |
//! | proposed (`commit_on_conflict_deletions`) | `2` (one for `increment_sequence`, one txn for the rest) | yes |
//!
//! ## What this bench measures
//!
//! Pure shape — same `tokio::sync::Mutex<()>` + `tokio::time::sleep(rtt)`
//! pattern as `stats_persistence_rpc_ceiling.rs` and
//! `metastore_connection_contention.rs`. No real SQLite, no Cayenne
//! setup. Two lanes per `(delete_files_per_upsert, upsert_count, RTT)`:
//!
//! - `current_n_plus_two_rpc` — each upsert: 1 RPC (increment) + N RPCs
//!   (add_delete_file × N) + 1 RPC (add_insert_records_batch). Total =
//!   `(N + 2)` RPCs per upsert. Mirrors today's body.
//! - `proposed_single_txn` — each upsert: 1 RPC (increment) + 1 RPC
//!   (single transaction batch). Total = 2 RPCs per upsert.
//!
//! The synthetic models the metastore-pool RTT delivered cost — the
//! real wall-time gap will track the synthetic gap proportionally,
//! amplified by the fact that the transaction batch is a single
//! `execute_batch` call (one parse + one round trip) instead of N
//! separate prepared-statement round trips.
//!
//! `cargo bench --bench apply_on_conflict_rpc_ceiling -p cayenne`.

#![expect(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::sync::Mutex;

/// Per-RPC simulated round trip — mirrors the other RPC-ceiling benches.
const RTTS: &[(&str, Duration)] = &[
    ("rtt_1ms", Duration::from_millis(1)),
    ("rtt_10ms", Duration::from_millis(10)),
];

/// Delete files per upsert. Typical PK-mode upsert produces 1-2 files
/// (one per touched virtual file). Larger counts model partitioned or
/// wide-fan-out upserts.
const DELETE_FILES_PER_UPSERT: &[usize] = &[1, 2, 4];

/// Upsert count per iteration. 32 keeps the worst-case shape
/// (`N=4`, `rtt_10ms`, current) under 2.5 s per iteration so Criterion
/// `--quick` produces a multi-sample distribution. Pattern lifted from
/// `stats_persistence_rpc_ceiling.rs:128`.
const UPSERTS_PER_ITERATION: usize = 32;

/// One simulated metastore RPC. Same shape as other RPC-ceiling benches.
async fn one_rpc(pool: &Mutex<()>, rtt: Duration) {
    let _guard = pool.lock().await;
    tokio::time::sleep(rtt).await;
}

/// Lane A: mirrors today's `apply_on_conflict_deletions`. Each upsert
/// pays `2 + delete_files` separate RPCs.
async fn run_current(pool: &Arc<Mutex<()>>, upserts: usize, delete_files: usize, rtt: Duration) {
    for _ in 0..upserts {
        // 1. increment_sequence_number
        one_rpc(pool, rtt).await;
        // 3. add_delete_file × N
        for _ in 0..delete_files {
            one_rpc(pool, rtt).await;
        }
        // 4. add_insert_records_batch
        one_rpc(pool, rtt).await;
    }
}

/// Lane B: models the proposed `commit_on_conflict_deletions` —
/// `increment_sequence_number` + one transaction batch.
async fn run_proposed(pool: &Arc<Mutex<()>>, upserts: usize, _delete_files: usize, rtt: Duration) {
    for _ in 0..upserts {
        // 1. increment_sequence_number
        one_rpc(pool, rtt).await;
        // 2. one transaction batch covering every delete_file row and
        //    every insert_records row. `execute_batch` is one round
        //    trip regardless of how many statements it contains.
        one_rpc(pool, rtt).await;
    }
}

fn bench_apply_on_conflict_rpc_ceiling(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("apply_on_conflict_rpc_ceiling");
    group.sample_size(10);

    for &(rtt_label, rtt) in RTTS {
        for &delete_files in DELETE_FILES_PER_UPSERT {
            let upserts_total = u64::try_from(UPSERTS_PER_ITERATION).unwrap_or(u64::MAX);
            group.throughput(Throughput::Elements(upserts_total));

            let id = format!("delete_files={delete_files}/{rtt_label}");
            let pool_a = Arc::new(Mutex::new(()));
            group.bench_with_input(
                BenchmarkId::new("current_n_plus_two_rpc", &id),
                &delete_files,
                |b, &delete_files| {
                    b.to_async(&rt).iter(|| {
                        let pool = Arc::clone(&pool_a);
                        async move {
                            run_current(&pool, UPSERTS_PER_ITERATION, delete_files, rtt).await;
                            black_box(pool);
                        }
                    });
                },
            );
            let pool_b = Arc::new(Mutex::new(()));
            group.bench_with_input(
                BenchmarkId::new("proposed_single_txn", &id),
                &delete_files,
                |b, &delete_files| {
                    b.to_async(&rt).iter(|| {
                        let pool = Arc::clone(&pool_b);
                        async move {
                            run_proposed(&pool, UPSERTS_PER_ITERATION, delete_files, rtt).await;
                            black_box(pool);
                        }
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_apply_on_conflict_rpc_ceiling);
criterion_main!(benches);
