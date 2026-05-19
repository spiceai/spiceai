// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Regression bench: per-upsert metastore call count in
//! [`crate::provider::table::CayenneTableProvider::apply_on_conflict_deletions`].
//!
//! Older versions of the on-conflict path ran a non-atomic 3+ metastore-call
//! sequence on every upsert that produces deletion vectors:
//!
//! 1. `catalog.increment_sequence_number(table_id)` — 1 call
//! 2. `DeletionVectorWriter::write(specs)` — writes deletion-vector
//!    files to disk (NOT counted here; we measure metastore calls only)
//! 3. For each `DeletionVectorWriteResult`:
//!    `catalog.add_delete_file(result.delete_file)` — 1 call per file
//! 4. `catalog.add_insert_records_batch(...)` — 1 call per insert-record chunk
//!
//! For a typical PK-mode upsert that produces 1-2 delete files the cumulative
//! cost was 3-4 metastore calls per upsert. None of those calls were wrapped
//! in a transaction, so a crash between step 3 and step 4 could leave the
//! catalog with delete-file records at `delete_sequence` but no insert-record
//! at `insert_sequence`, permanently hiding the new row on restart.
//!
//! The production path now calls `commit_on_conflict_deletions`
//! ([`crate::catalog::MetadataCatalog::commit_on_conflict_deletions`], wired in
//! at `provider/table.rs:4506-4526`) which opens one transaction, INSERTs every
//! `cayenne_delete_file` row, INSERTs every insert-record row (chunked under
//! SQLite's 32 K-param cap, as `add_insert_records_batch_in_chunks` already
//! does internally), and commits. Crash anywhere before commit → catalog state
//! unchanged → the upsert is fully re-driveable from the calling write path.
//!
//! Counted metastore-call totals for the one-insert-record-chunk case:
//!
//! | path                                            | calls per upsert     | atomic? |
//! |-------------------------------------------------|----------------------|---------|
//! | older (`apply_on_conflict_deletions`, no txn)   | `2 + delete_files`   | no      |
//! | current (`commit_on_conflict_deletions` in txn) | `4 + delete_files`   | yes     |
//!
//! ## What this bench measures
//!
//! Pure shape — same `tokio::sync::Mutex<()>` + `tokio::time::sleep(call_latency)`
//! pattern as `stats_persistence_rpc_ceiling.rs`. No real SQLite, no Cayenne
//! setup. Two lanes per `(delete_files_per_upsert, upsert_count, call_latency)`:
//!
//! - `no_txn_baseline` — each upsert: 1 call (increment) + N calls
//!   (add_delete_file × N) + 1 call (add_insert_records_batch). Total =
//!   `(N + 2)` calls per upsert. Mirrors the older non-atomic path.
//! - `atomic_txn_calls` — current behavior. Each upsert: 1 call (increment) +
//!   1 call (begin transaction) + N calls (delete-file INSERTs) + 1 call
//!   (insert-record chunk INSERT) + 1 call (commit). Total = `(N + 4)` calls
//!   per upsert for this bench's single-chunk setup.
//!
//! The bench keeps the call-count tradeoff visible — atomicity costs a
//! constant 2 extra calls per upsert in exchange for closing the crash window.
//!
//! `cargo bench --bench apply_on_conflict_rpc_ceiling -p cayenne`.

#![expect(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tokio::sync::Mutex;

/// Per-call simulated metastore latency — mirrors the other RPC-ceiling benches.
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

/// One simulated metastore call. Same shape as other RPC-ceiling benches.
async fn one_metastore_call(pool: &Mutex<()>, rtt: Duration) {
    let _guard = pool.lock().await;
    tokio::time::sleep(rtt).await;
}

/// Lane A: mirrors the older non-atomic `apply_on_conflict_deletions`. Each
/// upsert pays `2 + delete_files` separate metastore calls.
async fn run_current(pool: &Arc<Mutex<()>>, upserts: usize, delete_files: usize, rtt: Duration) {
    for _ in 0..upserts {
        // 1. increment_sequence_number
        one_metastore_call(pool, rtt).await;
        // 3. add_delete_file × N
        for _ in 0..delete_files {
            one_metastore_call(pool, rtt).await;
        }
        // 4. add_insert_records_batch
        one_metastore_call(pool, rtt).await;
    }
}

/// Lane B: current behavior — `commit_on_conflict_deletions` as implemented:
/// `increment_sequence_number`, `BEGIN`, per-delete-file `INSERT`, one
/// insert-record chunk `INSERT`, then `COMMIT`.
async fn run_proposed(pool: &Arc<Mutex<()>>, upserts: usize, delete_files: usize, rtt: Duration) {
    for _ in 0..upserts {
        // 1. increment_sequence_number
        one_metastore_call(pool, rtt).await;
        // 2. begin_transaction
        one_metastore_call(pool, rtt).await;
        // 3. delete-file INSERT × N
        for _ in 0..delete_files {
            one_metastore_call(pool, rtt).await;
        }
        // 4. one insert-record chunk INSERT
        one_metastore_call(pool, rtt).await;
        // 5. commit
        one_metastore_call(pool, rtt).await;
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
                BenchmarkId::new("no_txn_baseline", &id),
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
                BenchmarkId::new("atomic_txn_calls", &id),
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
