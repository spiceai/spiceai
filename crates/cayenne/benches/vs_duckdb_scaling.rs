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
//! Three workloads are exercised, each across `[1, 2, 4, 8, 16, 32, 64, 128]`
//! concurrency levels (64 covers the stated single-machine goal, 128 covers
//! oversubscription on common 64-core SMT-2 boxes) and across **both
//! Cayenne metastore backends**
//! (`Sqlite` and `Turso`, the latter compiled in when the `turso` feature
//! is enabled — see `CAYENNE_LANES`). DuckDB runs once per workload as the
//! external baseline.
//!
//! * **`vs_duckdb_scaling_reads`** — N concurrent `SELECT COUNT(*)` queries
//!   against one pre-loaded 1M-row table. Cayenne uses one long-lived warm
//!   `SessionContext` shared across the N reader tasks (mirrors a running
//!   Spice runtime). DuckDB opens one fresh `Connection` per OS thread
//!   from the shared on-disk database path. The bench shows whether
//!   reader-side scaling is bounded by mutex contention or runs cleanly
//!   to the core count. Cayenne lane ids are `cayenne_warm` /
//!   `cayenne_turso_warm`.
//!
//! * **`vs_duckdb_scaling_writes`** — sustained insert throughput driven by
//!   N concurrent background writer tasks against one table via the generic
//!   `CayenneTableProvider::insert_into` path. Each writer pumps inserts in
//!   a loop, the bench iter waits for N completions to pass before sampling.
//!   Measures whether the per-table write path scales with concurrent
//!   writers, or whether the per-table `write_lock` at
//!   `provider/table.rs:1131` serialises them.
//!
//! * **`vs_duckdb_scaling_cdc`** — same shape as writes, but pumping
//!   `CayenneTableProvider::write_cdc_append_stream` + `finish()` (the
//!   production CDC pipelined path used by `refresh_mode: changes`) instead
//!   of `insert_into`. Compares against DuckDB INSERT as the closest analog
//!   since DuckDB has no CDC-pipelined entry point of its own.
//!
//! Workers in every lane are pooled per concurrency level (spawned once,
//! signaled per iteration via `mpsc` channels), so worker-spawn cost
//! falls outside criterion's timed region. The bench thread blocks on
//! bounded-timeout receives (`tokio::time::timeout` over
//! `tokio::sync::mpsc` for the async Cayenne reader lane,
//! `std::sync::mpsc::Receiver::recv_timeout` via `wait_for_completions`
//! for the threaded DuckDB and write/CDC lanes) — no CPU-burning
//! `spin_loop`, no AtomicUsize counter, and a stalled or panicked worker
//! surfaces as a labeled panic naming the bench, lane, and completion
//! number instead of hanging indefinitely.
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use duckdb::Connection;
use tokio::runtime::Runtime;

/// Bounded-timeout wait for `n` writer-completion signals. Replaces the
/// earlier `AtomicUsize` + `std::hint::spin_loop` pattern that burned an
/// entire core on the bench thread and could hang indefinitely if a writer
/// panicked. `recv_timeout` is blocking (no CPU burn), and surfaces a
/// useful failure message when a writer stalls or dies.
fn wait_for_completions(rx: &mpsc::Receiver<()>, n: usize, ctx: &str) {
    // 30s per iteration is generous — even the slowest write workload below
    // (DuckDB at 64-way concurrency) takes ~95 ms per iteration. A timeout
    // this far above the steady-state means a real fault, not noise.
    let per_iter_timeout = Duration::from_secs(30);
    for i in 0..n {
        rx.recv_timeout(per_iter_timeout).unwrap_or_else(|err| {
            panic!(
                "{ctx}: stalled waiting for writer completion {}/{n} after {per_iter_timeout:?} ({err})",
                i + 1
            )
        });
    }
}

use common::{
    CAYENNE_LANES, CayenneFixture, cayenne_cdc_write, cayenne_insert, cayenne_query_warm,
    duckdb_insert_parquet, duckdb_insert_rows, make_batch, schema, setup_cayenne_for, setup_duckdb,
    warm_session_for, write_parquet,
};

/// Concurrency levels measured. Spans the typical core counts we care about
/// (1 → 32) up through the 64-core baseline goal and out to 128 so the
/// per-iteration curve covers oversubscription on common 64-core deployments
/// (every workload runs 1× hyperthread + 1× extra-task-per-core headroom).
const CONCURRENCIES: &[usize] = &[1, 2, 4, 8, 16, 32, 64, 128];

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

/// Pre-built read-side state shared across all Cayenne metastore lanes and
/// concurrency levels. The DuckDB side stores the on-disk database path
/// rather than a shared `Connection` because `duckdb::Connection` is not
/// `Send` in this codebase (`vs_duckdb_concurrent.rs:108` follows the same
/// pattern — each thread calls `Connection::open(&db_path)` so every thread
/// owns its own connection). Reader threads do the same.
struct DuckDbReadFixture {
    db_path: std::path::PathBuf,
    _temp: tempfile::TempDir,
    _parquet_dir: tempfile::TempDir,
}

/// Build the DuckDB side once, returning a path other threads can open
/// fresh connections against. The corresponding Cayenne fixture is built
/// per-lane inside `bench_read_scaling` so each metastore backend gets
/// its own independent metastore + data dir.
fn build_duckdb_read_fixture(batch: &arrow::array::RecordBatch) -> DuckDbReadFixture {
    let duckdb_fixture = setup_duckdb("scaling_read");
    let parquet_dir = tempfile::tempdir().expect("parquet dir");
    let parquet_path = parquet_dir.path().join("scaling_read.parquet");
    write_parquet(batch, &parquet_path);
    duckdb_insert_parquet(&duckdb_fixture.conn, "scaling_read", &parquet_path);
    DuckDbReadFixture {
        db_path: duckdb_fixture.db_path(),
        _temp: duckdb_fixture._temp_dir,
        _parquet_dir: parquet_dir,
    }
}

/// Persistent Cayenne reader worker. Spawned once per concurrency level,
/// drives one read on every iteration when its dedicated `go_rx` channel
/// receives a tick, signals completion through the shared `done_tx`.
/// Per-worker `go` channels (not a broadcast or single shared channel)
/// avoid lost-permit and multi-consumer-receiver issues — the bench
/// thread sends exactly one tick to each worker per iteration.
struct CayenneReader {
    handle: tokio::task::JoinHandle<()>,
}

impl CayenneReader {
    fn spawn(
        rt: &Runtime,
        warm_ctx: Arc<datafusion::prelude::SessionContext>,
        mut go_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
        done_tx: tokio::sync::mpsc::UnboundedSender<()>,
    ) -> Self {
        let handle = rt.spawn(async move {
            // `go_rx.recv()` returns None when the bench thread drops its
            // sender — that's the teardown signal, exit the loop cleanly.
            while go_rx.recv().await.is_some() {
                let batches = cayenne_query_warm(&warm_ctx, "SELECT COUNT(*) FROM t").await;
                black_box(batches);
                // `send` errors only if the bench has dropped its receiver
                // mid-iteration; benign on teardown.
                let _ = done_tx.send(());
            }
        });
        Self { handle }
    }
}

/// Persistent DuckDB reader worker. OS thread with the same channel shape
/// as `CayenneReader` — its own `go_rx` for per-worker ticks, a shared
/// `done_tx` for completion signals. Each worker opens its own
/// `Connection` from the shared on-disk database (DuckDB connections are
/// not `Send` in this codebase, see `vs_duckdb_concurrent.rs:108` for the
/// same pattern). Reused across every criterion sample so the
/// thread-spawn cost is paid exactly once per concurrency level.
struct DuckDbReader {
    handle: std::thread::JoinHandle<()>,
}

impl DuckDbReader {
    fn spawn(
        db_path: std::path::PathBuf,
        go_rx: mpsc::Receiver<()>,
        done_tx: mpsc::Sender<()>,
    ) -> Self {
        let handle = std::thread::spawn(move || {
            let conn = Connection::open(&db_path).expect("duckdb reader connection open");
            // `recv()` returns Err when the bench drops its sender — exit
            // cleanly on teardown.
            while go_rx.recv().is_ok() {
                let mut stmt = conn
                    .prepare("SELECT COUNT(*) FROM scaling_read")
                    .expect("prepare");
                let mut rows = stmt.query([]).expect("query");
                let row = rows.next().expect("next").expect("row");
                let value: i64 = row.get(0).expect("count");
                black_box(value);
                let _ = done_tx.send(());
            }
        });
        Self { handle }
    }
}

fn bench_read_scaling(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_scaling_reads");
    group.sample_size(10);

    let batch = make_batch(schema(), 0, READ_ROWS);
    let duckdb_fixture = build_duckdb_read_fixture(&batch);

    // For each Cayenne metastore (Sqlite, optionally Turso): set up the
    // table once, spawn worker pools per concurrency level, run the
    // BenchmarkId-prefixed by `lane.lane()`.
    for &lane in CAYENNE_LANES {
        let lane_label = format!("{}_warm", lane.lane());
        let cayenne_fixture = Arc::new(rt.block_on(async {
            let f = setup_cayenne_for("scaling_read", lane).await;
            let _ = cayenne_insert(&f.table, batch.clone()).await;
            f
        }));
        // One warm session per Cayenne table, shared across all reader
        // tasks for this lane. Mirrors a long-lived Spice daemon holding
        // a single SessionContext and serving many concurrent queries.
        let warm_ctx = Arc::new(warm_session_for(&cayenne_fixture.table));

        for &concurrency in CONCURRENCIES {
            group.throughput(Throughput::Elements(concurrency as u64));

            // --- Cayenne pool for this lane: N persistent tokio tasks.
            //     Per-worker `go_tx`, shared `done_rx`. Worker spawn
            //     cost is paid once per (lane, concurrency) pair, not
            //     once per criterion sample.
            let (cayenne_done_tx, mut cayenne_done_rx) =
                tokio::sync::mpsc::unbounded_channel::<()>();
            let mut cayenne_go_txs: Vec<tokio::sync::mpsc::UnboundedSender<()>> =
                Vec::with_capacity(concurrency);
            let mut cayenne_workers: Vec<CayenneReader> = Vec::with_capacity(concurrency);
            for _ in 0..concurrency {
                let (go_tx, go_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
                cayenne_go_txs.push(go_tx);
                cayenne_workers.push(CayenneReader::spawn(
                    &rt,
                    Arc::clone(&warm_ctx),
                    go_rx,
                    cayenne_done_tx.clone(),
                ));
            }
            // Drop the bench's spare done sender so `done_rx.recv()`
            // returns None once all workers drop their senders during
            // teardown.
            drop(cayenne_done_tx);

            let bench_ctx = format!("vs_duckdb_scaling_reads/{lane_label}");
            group.bench_with_input(
                BenchmarkId::new(&lane_label, concurrency),
                &concurrency,
                |b, &_n| {
                    b.iter(|| {
                        rt.block_on(async {
                            for tx in &cayenne_go_txs {
                                tx.send(()).expect("cayenne go-tx (worker exited?)");
                            }
                            // Bounded per-completion timeout. If any
                            // reader task panics or stalls, surface a
                            // clear error instead of hanging the bench
                            // thread indefinitely.
                            for i in 0..concurrency {
                                match tokio::time::timeout(
                                    Duration::from_secs(30),
                                    cayenne_done_rx.recv(),
                                )
                                .await
                                {
                                    Ok(Some(())) => {}
                                    Ok(None) => panic!(
                                        "{bench_ctx}: done channel closed at completion {}/{concurrency} (worker exited?)",
                                        i + 1
                                    ),
                                    Err(_) => panic!(
                                        "{bench_ctx}: stalled waiting for reader completion {}/{concurrency} after 30s",
                                        i + 1
                                    ),
                                }
                            }
                        });
                    });
                },
            );

            // Tear down Cayenne workers for this (lane, concurrency).
            cayenne_go_txs.clear();
            for w in cayenne_workers.drain(..) {
                rt.block_on(async { w.handle.await })
                    .expect("cayenne reader task");
            }
        }

        drop(cayenne_fixture);
    }

    // --- DuckDB lane: run once across all concurrencies (not per
    //     Cayenne metastore — the DuckDB fixture is independent of
    //     Cayenne's metastore choice). Same per-worker `go` channel +
    //     shared `done` channel shape.
    for &concurrency in CONCURRENCIES {
        group.throughput(Throughput::Elements(concurrency as u64));

        let (duckdb_done_tx, duckdb_done_rx) = mpsc::channel::<()>();
        let mut duckdb_go_txs: Vec<mpsc::Sender<()>> = Vec::with_capacity(concurrency);
        let mut duckdb_workers: Vec<DuckDbReader> = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let (go_tx, go_rx) = mpsc::channel::<()>();
            duckdb_go_txs.push(go_tx);
            duckdb_workers.push(DuckDbReader::spawn(
                duckdb_fixture.db_path.clone(),
                go_rx,
                duckdb_done_tx.clone(),
            ));
        }
        drop(duckdb_done_tx);

        group.bench_with_input(
            BenchmarkId::new("duckdb", concurrency),
            &concurrency,
            |b, &_n| {
                b.iter(|| {
                    for tx in &duckdb_go_txs {
                        tx.send(()).expect("duckdb go-tx (worker exited?)");
                    }
                    wait_for_completions(
                        &duckdb_done_rx,
                        concurrency,
                        "vs_duckdb_scaling_reads/duckdb",
                    );
                });
            },
        );

        // Tear down DuckDB workers for this concurrency.
        duckdb_go_txs.clear();
        for w in duckdb_workers.drain(..) {
            w.handle.join().expect("duckdb reader thread");
        }
    }

    drop(duckdb_fixture);
    group.finish();
}

// ---------------------------------------------------------------------------
// vs_duckdb_scaling_writes — N background writers sustaining inserts
// ---------------------------------------------------------------------------

/// Sustained-throughput Cayenne writer. Spawns one async task on the runtime
/// that pumps `cayenne_insert` calls in a tight loop, signaling each
/// completion through `tx`. Dropping the writer stops the loop and joins
/// the task — modeled after `CayenneBgWriter` in `vs_duckdb_concurrent.rs`.
struct CayenneBgWriter {
    stop: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<u64>>,
    rt_handle: tokio::runtime::Handle,
}

impl CayenneBgWriter {
    fn spawn(rt: &Runtime, fixture: &CayenneFixture, tx: mpsc::Sender<()>, writer_id: i64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
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
                    // Sender drop after stop is the normal teardown path;
                    // ignore the error in that case.
                    let _ = tx.send(());
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
    fn spawn(
        db_path: &std::path::Path,
        table_name: &str,
        tx: mpsc::Sender<()>,
        writer_id: i64,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let db_path = db_path.to_path_buf();
        let table_name = table_name.to_string();
        let handle = std::thread::spawn(move || {
            let conn = Connection::open(&db_path).expect("bg duckdb open");
            let id_stride = (WRITE_BATCH_ROWS as i64) * 1024 * 1024;
            let mut cursor = writer_id * id_stride;
            let mut written = 0u64;
            while !stop_clone.load(Ordering::Relaxed) {
                let batch = make_batch(schema(), cursor, WRITE_BATCH_ROWS);
                cursor += WRITE_BATCH_ROWS as i64;
                duckdb_insert_rows(&conn, &table_name, &batch);
                written += 1;
                let _ = tx.send(());
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

    // For each Cayenne metastore lane: run the full concurrency sweep.
    // Each (lane, concurrency) point gets a fresh fixture because writes
    // accumulate state on disk and we want every measurement to start
    // from an empty table.
    for &lane in CAYENNE_LANES {
        let lane_label = lane.lane().to_string();
        for &concurrency in CONCURRENCIES {
            group.throughput(Throughput::Elements(
                (concurrency as u64) * (WRITE_BATCH_ROWS as u64),
            ));

            // --- Cayenne lane ---
            let cayenne_fixture = rt.block_on(setup_cayenne_for("scaling_write", lane));
            // Each writer holds a clone of `cayenne_tx`; bench iter reads N
            // completions from `cayenne_rx` via `wait_for_completions`
            // (blocking, bounded timeout, surfaces a useful error if any
            // writer stalls or panics).
            let (cayenne_tx, cayenne_rx) = mpsc::channel::<()>();
            let cayenne_writers: Vec<CayenneBgWriter> = (0..concurrency)
                .map(|i| {
                    CayenneBgWriter::spawn(&rt, &cayenne_fixture, cayenne_tx.clone(), i as i64)
                })
                .collect();
            // Drop the spare sender so the channel closes naturally when
            // all writers exit on the teardown signal.
            drop(cayenne_tx);

            let bench_ctx = format!("vs_duckdb_scaling_writes/{lane_label}");
            group.bench_with_input(
                BenchmarkId::new(&lane_label, concurrency),
                &concurrency,
                |b, &_n| {
                    b.iter(|| {
                        // Drain `concurrency` total completed inserts from
                        // the shared channel — total system throughput
                        // across N writers, not per-writer fairness. One
                        // fast writer can contribute multiple of the N
                        // counted messages in an iteration; by design for
                        // a throughput-under-concurrency measurement.
                        // criterion's `Throughput::Elements` divides by
                        // `concurrency * WRITE_BATCH_ROWS` to report
                        // per-worker throughput.
                        wait_for_completions(&cayenne_rx, concurrency, &bench_ctx);
                    });
                },
            );

            // Explicit drop order: writers first (stops the loops + joins
            // the tasks), then the fixture (its TempDir cleans up the
            // data).
            drop(cayenne_writers);
            drop(cayenne_fixture);
        }
    }

    // --- DuckDB lane: run once per concurrency level (DuckDB has no
    //     metastore-backend dimension; one independent measurement
    //     across all Cayenne lanes is the right shape).
    for &concurrency in CONCURRENCIES {
        group.throughput(Throughput::Elements(
            (concurrency as u64) * (WRITE_BATCH_ROWS as u64),
        ));

        // --- DuckDB lane ---
        let duckdb_fixture = setup_duckdb("scaling_write");
        let duckdb_db_path = duckdb_fixture.db_path();
        let (duckdb_tx, duckdb_rx) = mpsc::channel::<()>();
        let duckdb_writers: Vec<DuckDbBgWriter> = (0..concurrency)
            .map(|i| {
                DuckDbBgWriter::spawn(
                    &duckdb_db_path,
                    "scaling_write",
                    duckdb_tx.clone(),
                    i as i64,
                )
            })
            .collect();
        drop(duckdb_tx);

        group.bench_with_input(
            BenchmarkId::new("duckdb", concurrency),
            &concurrency,
            |b, &_n| {
                b.iter(|| {
                    wait_for_completions(
                        &duckdb_rx,
                        concurrency,
                        "vs_duckdb_scaling_writes/duckdb",
                    );
                });
            },
        );

        drop(duckdb_writers);
        drop(duckdb_fixture);
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// vs_duckdb_scaling_cdc — sustained CDC pipelined writes
// ---------------------------------------------------------------------------

/// Sustained-throughput Cayenne CDC writer. Same shape as `CayenneBgWriter`
/// but pumps `write_cdc_append_stream` + `finish()` (the production CDC
/// pipelined path used by `refresh_mode: changes`) instead of the generic
/// `insert_into`. Lets us see whether the Stage-A / Stage-B split actually
/// translates into higher sustained throughput vs the regular write path.
struct CayenneCdcBgWriter {
    stop: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<u64>>,
    rt_handle: tokio::runtime::Handle,
}

impl CayenneCdcBgWriter {
    fn spawn(rt: &Runtime, fixture: &CayenneFixture, tx: mpsc::Sender<()>, writer_id: i64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let table = Arc::clone(&fixture.table);
        let handle = rt.spawn(async move {
            let mut written = 0u64;
            let id_stride = (WRITE_BATCH_ROWS as i64) * 1024 * 1024;
            let mut cursor = writer_id * id_stride;
            while !stop_clone.load(Ordering::Relaxed) {
                let batch = make_batch(schema(), cursor, WRITE_BATCH_ROWS);
                cursor += WRITE_BATCH_ROWS as i64;
                if cayenne_cdc_write(&table, batch).await > 0 {
                    written += 1;
                    let _ = tx.send(());
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

impl Drop for CayenneCdcBgWriter {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = self.rt_handle.block_on(handle);
        }
    }
}

fn bench_cdc_scaling(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_scaling_cdc");
    group.sample_size(10);

    // For each Cayenne metastore lane: run the full concurrency sweep on
    // the CDC pipelined path. Same structure as `bench_write_scaling`,
    // but the Cayenne writer pumps `write_cdc_append_stream + finish()`
    // instead of `insert_into`. Lane id is `cayenne_cdc` (Sqlite) or
    // `cayenne_turso_cdc` (Turso).
    for &lane in CAYENNE_LANES {
        let lane_label = format!("{}_cdc", lane.lane());
        for &concurrency in CONCURRENCIES {
            group.throughput(Throughput::Elements(
                (concurrency as u64) * (WRITE_BATCH_ROWS as u64),
            ));

            let cayenne_fixture = rt.block_on(setup_cayenne_for("scaling_cdc", lane));
            let (cayenne_tx, cayenne_rx) = mpsc::channel::<()>();
            let cayenne_writers: Vec<CayenneCdcBgWriter> = (0..concurrency)
                .map(|i| {
                    CayenneCdcBgWriter::spawn(&rt, &cayenne_fixture, cayenne_tx.clone(), i as i64)
                })
                .collect();
            drop(cayenne_tx);

            let bench_ctx = format!("vs_duckdb_scaling_cdc/{lane_label}");
            group.bench_with_input(
                BenchmarkId::new(&lane_label, concurrency),
                &concurrency,
                |b, &_n| {
                    b.iter(|| {
                        wait_for_completions(&cayenne_rx, concurrency, &bench_ctx);
                    });
                },
            );

            drop(cayenne_writers);
            drop(cayenne_fixture);
        }
    }

    // --- DuckDB INSERT lane (closest analog — DuckDB has no CDC pipelined
    //     equivalent; the comparison answers "if you ran CDC on DuckDB by
    //     just doing inserts, how fast would it be?"). Runs once per
    //     concurrency level, independent of Cayenne's metastore choice.
    for &concurrency in CONCURRENCIES {
        group.throughput(Throughput::Elements(
            (concurrency as u64) * (WRITE_BATCH_ROWS as u64),
        ));

        let duckdb_fixture = setup_duckdb("scaling_cdc");
        let duckdb_db_path = duckdb_fixture.db_path();
        let (duckdb_tx, duckdb_rx) = mpsc::channel::<()>();
        let duckdb_writers: Vec<DuckDbBgWriter> = (0..concurrency)
            .map(|i| {
                DuckDbBgWriter::spawn(&duckdb_db_path, "scaling_cdc", duckdb_tx.clone(), i as i64)
            })
            .collect();
        drop(duckdb_tx);

        group.bench_with_input(
            BenchmarkId::new("duckdb", concurrency),
            &concurrency,
            |b, &_n| {
                b.iter(|| {
                    wait_for_completions(&duckdb_rx, concurrency, "vs_duckdb_scaling_cdc/duckdb");
                });
            },
        );

        drop(duckdb_writers);
        drop(duckdb_fixture);
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_read_scaling,
    bench_write_scaling,
    bench_cdc_scaling
);
criterion_main!(benches);
