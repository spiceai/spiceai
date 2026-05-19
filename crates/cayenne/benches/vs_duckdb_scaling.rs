// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Concurrency scaling benchmarks: does throughput scale with cores?
//!
//! The single-thread `vs_duckdb_*` benches measure per-query latency. This
//! bench measures *throughput under concurrency* — the metric that matters
//! for 64-core deployments where Spice runs both ingestion (CDC / refresh)
//! and query traffic in parallel.
//!
//! Two workloads are exercised, each across `[1, 2, 4, 8, 16, 32, 64]`
//! concurrency levels:
//!
//! * **`vs_duckdb_scaling_reads`** — N concurrent `SELECT COUNT(*)` queries
//!   against one pre-loaded 1M-row table. Cayenne uses one long-lived warm
//!   `SessionContext` shared across the N reader tasks (mirrors a running
//!   Spice runtime). DuckDB uses one `Connection` per task via
//!   `try_clone()`. The bench shows whether reader-side scaling is bounded
//!   by mutex contention or runs cleanly to the core count.
//!
//! * **`vs_duckdb_scaling_writes`** — sustained insert throughput driven by
//!   N concurrent background writer tasks against one table. Each writer
//!   issues an insert, the runner waits N inserts then samples; criterion's
//!   `throughput` metric reports rows/sec. Measures whether the per-table
//!   write path scales with concurrent writers, or whether the per-table
//!   `write_lock` at `provider/table.rs:1131` serialises them.
//!
//! `BenchmarkId::new("…", N)` naming makes the throughput-vs-N curve visible
//! directly in criterion's HTML output and parse-able from the text logs.

#![cfg(feature = "duckdb-bench")]
#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_precision_loss)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use duckdb::Connection;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

use common::{
    CayenneFixture, cayenne_insert, cayenne_query_warm, duckdb_insert_parquet,
    duckdb_insert_rows, make_batch, schema, setup_cayenne, setup_duckdb, warm_session_for,
    write_parquet,
};

/// Concurrency levels measured. Spans the typical core counts we care about
/// (1, 2, 4, 8, 16, 32) up to the 64-core goal stated by the user.
const CONCURRENCIES: &[usize] = &[1, 2, 4, 8, 16, 32, 64];

/// Rows in the pre-loaded read fixture. Single fixed value because the
/// variable we're studying is concurrency, not data size.
const READ_ROWS: usize = 1_048_576;

/// Rows per insert batch in the write-scaling workload. Small batch so each
/// `cayenne_insert` call is short (~1–10 ms) and the per-iteration sample
/// captures concurrency overhead rather than batch I/O cost.
const WRITE_BATCH_ROWS: usize = 1_024;

// ---------------------------------------------------------------------------
// vs_duckdb_scaling_reads — N concurrent COUNT(*) against ONE table
// ---------------------------------------------------------------------------

/// Owns both the pre-loaded Cayenne table and the matching DuckDB table.
/// Built once at bench start; reused across every concurrency level so the
/// data-load cost is paid exactly once.
struct ReadFixtures {
    cayenne: Arc<CayenneFixture>,
    duckdb_conn: Arc<Connection>,
    _duckdb_temp: tempfile::TempDir,
    _parquet_dir: tempfile::TempDir,
}

async fn build_read_fixtures() -> ReadFixtures {
    let cayenne_fixture = setup_cayenne("scaling_read").await;
    let batch = make_batch(schema(), 0, READ_ROWS);
    let _ = cayenne_insert(&cayenne_fixture.table, batch.clone()).await;

    let duckdb_fixture = setup_duckdb("scaling_read");
    let parquet_dir = tempfile::tempdir().expect("parquet dir");
    let parquet_path = parquet_dir.path().join("scaling_read.parquet");
    write_parquet(&batch, &parquet_path);
    duckdb_insert_parquet(&duckdb_fixture.conn, "scaling_read", &parquet_path);

    ReadFixtures {
        cayenne: Arc::new(cayenne_fixture),
        duckdb_conn: Arc::new(duckdb_fixture.conn),
        _duckdb_temp: duckdb_fixture._temp_dir,
        _parquet_dir: parquet_dir,
    }
}

fn bench_read_scaling(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_scaling_reads");
    group.sample_size(10);

    let fixtures = rt.block_on(build_read_fixtures());
    // One warm session per Cayenne table, shared across all reader tasks for
    // the entire bench run. Mirrors a long-lived Spice daemon holding a
    // single SessionContext and serving many concurrent queries.
    let warm_ctx = Arc::new(warm_session_for(&fixtures.cayenne.table));

    for &concurrency in CONCURRENCIES {
        group.throughput(Throughput::Elements(concurrency as u64));

        let wc = Arc::clone(&warm_ctx);
        group.bench_with_input(
            BenchmarkId::new("cayenne_warm", concurrency),
            &concurrency,
            |b, &n| {
                b.iter(|| {
                    rt.block_on(async {
                        let mut join_set = JoinSet::new();
                        for _ in 0..n {
                            let wc = Arc::clone(&wc);
                            join_set.spawn(async move {
                                let batches =
                                    cayenne_query_warm(&wc, "SELECT COUNT(*) FROM t").await;
                                black_box(batches);
                            });
                        }
                        while let Some(result) = join_set.join_next().await {
                            result.expect("cayenne reader task");
                        }
                    });
                });
            },
        );

        let conn = Arc::clone(&fixtures.duckdb_conn);
        group.bench_with_input(
            BenchmarkId::new("duckdb", concurrency),
            &concurrency,
            |b, &n| {
                b.iter(|| {
                    let mut handles = Vec::with_capacity(n);
                    for _ in 0..n {
                        // duckdb-rs `Connection::try_clone` produces a separate
                        // cursor sharing the same database; concurrent reads
                        // are MVCC-safe.
                        let conn_clone = conn.try_clone().expect("duckdb connection clone");
                        handles.push(std::thread::spawn(move || {
                            let mut stmt = conn_clone
                                .prepare("SELECT COUNT(*) FROM scaling_read")
                                .expect("prepare");
                            let mut rows = stmt.query([]).expect("query");
                            let row = rows.next().expect("next").expect("row");
                            let value: i64 = row.get(0).expect("count");
                            black_box(value);
                        }));
                    }
                    for handle in handles {
                        handle.join().expect("duckdb reader thread");
                    }
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// vs_duckdb_scaling_writes — N background writers sustaining inserts
// ---------------------------------------------------------------------------

/// Sustained-throughput Cayenne writer. Spawns one async task on the runtime
/// that pumps `cayenne_insert` calls in a tight loop, counting completions.
/// Dropping the writer stops the loop and joins the task — modeled after
/// `CayenneBgWriter` in `vs_duckdb_concurrent.rs`, which is the
/// known-working concurrent-write pattern in this crate.
struct CayenneBgWriter {
    stop: Arc<std::sync::atomic::AtomicBool>,
    handle: Option<tokio::task::JoinHandle<u64>>,
    rt_handle: tokio::runtime::Handle,
}

impl CayenneBgWriter {
    fn spawn(
        rt: &Runtime,
        fixture: &CayenneFixture,
        counter: Arc<AtomicUsize>,
        writer_id: i64,
    ) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let table = Arc::clone(&fixture.table);
        let handle = rt.spawn(async move {
            let mut written = 0u64;
            // Each writer uses a disjoint id range so concurrent writers
            // never produce overlapping primary key candidates — keeps
            // behavior deterministic across concurrency levels.
            let id_stride = (WRITE_BATCH_ROWS as i64) * 1024 * 1024;
            let mut cursor = writer_id * id_stride;
            while !stop_clone.load(Ordering::Relaxed) {
                let batch = make_batch(schema(), cursor, WRITE_BATCH_ROWS);
                cursor += WRITE_BATCH_ROWS as i64;
                if cayenne_insert(&table, batch).await > 0 {
                    written += 1;
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
            written
        });
        Self {
            stop,
            handle: Some(handle),
            rt_handle: rt.handle().clone(),
        }
    }
}

impl Drop for CayenneBgWriter {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            // Block the bench thread on writer settling. The bench thread is
            // not a runtime worker, so block_on is safe here.
            let _ = self.rt_handle.block_on(handle);
        }
    }
}

/// Sustained-throughput DuckDB writer. Same shape as `CayenneBgWriter` but
/// uses a dedicated OS thread and a fresh DuckDB connection opened from the
/// fixture's on-disk file. Each thread holds its own connection — DuckDB
/// serializes commits internally, but we want concurrent writers to expose
/// any reader-side contention on the shared database.
struct DuckDbBgWriter {
    stop: Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<u64>>,
}

impl DuckDbBgWriter {
    fn spawn(db_path: &std::path::Path, counter: Arc<AtomicUsize>, writer_id: i64) -> Self {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let db_path = db_path.to_path_buf();
        let handle = std::thread::spawn(move || {
            let conn = Connection::open(&db_path).expect("bg duckdb open");
            let id_stride = (WRITE_BATCH_ROWS as i64) * 1024 * 1024;
            let mut cursor = writer_id * id_stride;
            let mut written = 0u64;
            while !stop_clone.load(Ordering::Relaxed) {
                let batch = make_batch(schema(), cursor, WRITE_BATCH_ROWS);
                cursor += WRITE_BATCH_ROWS as i64;
                duckdb_insert_rows(&conn, "scaling_write", &batch);
                written += 1;
                counter.fetch_add(1, Ordering::Relaxed);
            }
            written
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for DuckDbBgWriter {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn bench_write_scaling(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_scaling_writes");
    group.sample_size(10);

    for &concurrency in CONCURRENCIES {
        group.throughput(Throughput::Elements(
            (concurrency as u64) * (WRITE_BATCH_ROWS as u64),
        ));

        // --- Cayenne lane ---
        let cayenne_fixture = rt.block_on(setup_cayenne("scaling_write"));
        let cayenne_counter = Arc::new(AtomicUsize::new(0));
        let mut cayenne_writers: Vec<CayenneBgWriter> = (0..concurrency)
            .map(|i| {
                CayenneBgWriter::spawn(
                    &rt,
                    &cayenne_fixture,
                    Arc::clone(&cayenne_counter),
                    i as i64,
                )
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("cayenne", concurrency),
            &concurrency,
            |b, &_n| {
                b.iter(|| {
                    let start = cayenne_counter.load(Ordering::Relaxed);
                    // Wait for one full pass (each writer contributes ≥1
                    // insert) so a single iteration captures the full
                    // concurrency cost — not just the latency of one of N
                    // writers.
                    let target = start + concurrency;
                    while cayenne_counter.load(Ordering::Relaxed) < target {
                        std::hint::spin_loop();
                    }
                    black_box(());
                });
            },
        );

        // Explicit drop order: writers first (stops the loops + joins the
        // tasks), then the fixture (its TempDir cleans up the data).
        cayenne_writers.drain(..);
        drop(cayenne_fixture);

        // --- DuckDB lane ---
        let duckdb_fixture = setup_duckdb("scaling_write");
        let duckdb_counter = Arc::new(AtomicUsize::new(0));
        let duckdb_db_path = duckdb_fixture.db_path();
        let mut duckdb_writers: Vec<DuckDbBgWriter> = (0..concurrency)
            .map(|i| {
                DuckDbBgWriter::spawn(
                    &duckdb_db_path,
                    Arc::clone(&duckdb_counter),
                    i as i64,
                )
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("duckdb", concurrency),
            &concurrency,
            |b, &_n| {
                b.iter(|| {
                    let start = duckdb_counter.load(Ordering::Relaxed);
                    let target = start + concurrency;
                    while duckdb_counter.load(Ordering::Relaxed) < target {
                        std::hint::spin_loop();
                    }
                    black_box(());
                });
            },
        );

        duckdb_writers.drain(..);
        drop(duckdb_fixture);
    }

    group.finish();
}

criterion_group!(benches, bench_read_scaling, bench_write_scaling);
criterion_main!(benches);
