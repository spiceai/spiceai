// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Fleet CDC: sustained CDC apply across N tables concurrently.
//!
//! `vs_duckdb_scaling_cdc` pumps ONE table with N writers; this bench pumps
//! N TABLES with one CDC writer each — the real HTAP topology (CH-benCH is
//! 14 CDC datasets, each its own accelerator with its own metastore). The
//! distinction matters because per-table write concurrency is sized in
//! isolation: with T tables the effective encoder demand is the SUM across
//! tables, which oversubscribes the box in exactly the way a single-table
//! bench structurally cannot see. This curve is the micro-proxy guarding
//! the global write-budget work.
//!
//! - **Cayenne**: each table is a fully independent fixture (own metastore,
//!   own data dir — the per-dataset topology spiced creates). One persistent
//!   writer task per table pumps `write_cdc_append_stream + finish()` (the
//!   production `refresh_mode: changes` path) with a long-lived per-writer
//!   `SessionContext`, batches of `WRITE_BATCH_ROWS` appends per tick.
//! - **DuckDB**: one file-backed database with N tables; one writer thread
//!   per table, each with its own `Connection`. DuckDB serializes writers
//!   internally — the single-global-writer architecture Cayenne's
//!   per-table encoders are traded against.
//!
//! Criterion `Throughput::Elements(N × WRITE_BATCH_ROWS)` makes the report
//! read as aggregate rows/sec; a flat (or falling) elements-per-second curve
//! as N grows is the oversubscription signal.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use duckdb::Connection;
use tokio::runtime::Runtime;

use common::{
    CAYENNE_LANES, CayenneFixture, cayenne_cdc_write, duckdb_insert_rows, make_batch, schema,
    setup_cayenne_for, setup_duckdb,
};

/// Table-count sweep. 14 ≈ CH-benCH's table fleet; 16 keeps the axis on a
/// power-of-two grid while covering it.
const TABLE_COUNTS: &[usize] = &[1, 2, 4, 8, 16];

/// Rows per CDC batch per table per tick — matches
/// `vs_duckdb_scaling`'s WRITE_BATCH_ROWS so the N=1 lane of this bench is
/// directly comparable to that bench's concurrency=1 CDC lane.
const WRITE_BATCH_ROWS: usize = 1_024;

/// Batches pre-written into every table (both engines) before its lane is
/// timed. A fresh table's per-batch cost is not stationary — snapshots and
/// metadata accumulate as batches land, so lanes with cheaper iterations
/// would silently run MORE iterations and age their tables further than
/// expensive lanes, corrupting the cross-N comparison (a first cut of this
/// bench produced a non-monotonic garbage curve exactly this way). The
/// preload parks every table well past the steep start of that cost curve
/// so marginal aging during the (bounded, flat-sampled) measurement is
/// small and comparable across lanes. Preload ids are negative so timed
/// writes (cursor 0..) never overlap them.
const PRELOAD_BATCHES: usize = 64;

/// Per-completion wait bound: a stalled writer surfaces as a labeled panic
/// instead of a hung bench (same rationale as `vs_duckdb_scaling`).
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(30);

/// Persistent CDC writer task bound to ONE Cayenne table. Receives a tick,
/// writes one batch through the production CDC path, signals done.
struct CayenneTableWriter {
    handle: tokio::task::JoinHandle<()>,
}

impl CayenneTableWriter {
    fn spawn(
        rt: &Runtime,
        fixture: Arc<CayenneFixture>,
        mut go_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
        done_tx: tokio::sync::mpsc::UnboundedSender<()>,
    ) -> Self {
        let handle = rt.spawn(async move {
            // One long-lived session per writer — the long-running refresh
            // loop shape (see `cayenne_cdc_write` docs for why per-batch
            // SessionContext construction would dominate).
            let ctx = datafusion::prelude::SessionContext::new();
            let task_ctx = ctx.task_ctx();
            let mut cursor: i64 = 0;
            while go_rx.recv().await.is_some() {
                let batch = make_batch(schema(), cursor, WRITE_BATCH_ROWS);
                cursor += WRITE_BATCH_ROWS as i64;
                let rows = cayenne_cdc_write(&fixture.table, &task_ctx, batch).await;
                assert!(rows > 0, "cdc write acknowledged zero rows");
                let _ = done_tx.send(());
            }
        });
        Self { handle }
    }
}

/// Persistent CDC-analog writer thread bound to ONE DuckDB table in the
/// shared database file. Own `Connection` (connections are not `Send`).
struct DuckDbTableWriter {
    handle: std::thread::JoinHandle<()>,
}

impl DuckDbTableWriter {
    fn spawn(
        db_path: std::path::PathBuf,
        table_name: String,
        go_rx: mpsc::Receiver<()>,
        done_tx: mpsc::Sender<()>,
    ) -> Self {
        let handle = std::thread::spawn(move || {
            let conn = Connection::open(&db_path).expect("duckdb writer connection open");
            let mut cursor: i64 = 0;
            while go_rx.recv().is_ok() {
                let batch = make_batch(schema(), cursor, WRITE_BATCH_ROWS);
                cursor += WRITE_BATCH_ROWS as i64;
                duckdb_insert_rows(&conn, &table_name, &batch);
                let _ = done_tx.send(());
            }
        });
        Self { handle }
    }
}

fn bench_cdc_multitable(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_cdc_multitable");
    // Flat sampling + bounded measurement: every sample runs the same
    // iteration count and the lane stops aging its tables after a few
    // hundred batches — see PRELOAD_BATCHES for why this matters.
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(4));

    for &lane in CAYENNE_LANES {
        let lane_label = format!("{}_cdc", lane.lane());

        for &tables in TABLE_COUNTS {
            group.throughput(Throughput::Elements((tables * WRITE_BATCH_ROWS) as u64));

            // N independent fixtures: own metastore + data dir each, the
            // per-dataset topology spiced creates for a CDC fleet.
            let fixtures: Vec<Arc<CayenneFixture>> = (0..tables)
                .map(|i| {
                    Arc::new(rt.block_on(setup_cayenne_for(
                        &format!("cdc_multi_{i}"),
                        lane,
                    )))
                })
                .collect();

            // Preload: park every table past the steep start of the
            // per-batch cost curve (negative id range; timed writes use
            // cursor 0.. and never overlap).
            rt.block_on(async {
                let preload_ctx = datafusion::prelude::SessionContext::new();
                let task_ctx = preload_ctx.task_ctx();
                let preload_start = -((PRELOAD_BATCHES * WRITE_BATCH_ROWS) as i64);
                for fixture in &fixtures {
                    for batch_idx in 0..PRELOAD_BATCHES {
                        let start = preload_start + (batch_idx * WRITE_BATCH_ROWS) as i64;
                        let batch = make_batch(schema(), start, WRITE_BATCH_ROWS);
                        let rows = cayenne_cdc_write(&fixture.table, &task_ctx, batch).await;
                        assert!(rows > 0, "preload cdc write acknowledged zero rows");
                    }
                }
            });

            let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
            let mut go_txs: Vec<tokio::sync::mpsc::UnboundedSender<()>> =
                Vec::with_capacity(tables);
            let mut workers: Vec<CayenneTableWriter> = Vec::with_capacity(tables);
            for fixture in &fixtures {
                let (go_tx, go_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
                go_txs.push(go_tx);
                workers.push(CayenneTableWriter::spawn(
                    &rt,
                    Arc::clone(fixture),
                    go_rx,
                    done_tx.clone(),
                ));
            }
            drop(done_tx);

            let bench_ctx = format!("vs_duckdb_cdc_multitable/{lane_label}");
            group.bench_with_input(
                BenchmarkId::new(&lane_label, tables),
                &tables,
                |b, &n| {
                    b.iter(|| {
                        rt.block_on(async {
                            for tx in &go_txs {
                                tx.send(()).expect("cayenne cdc go-tx (worker exited?)");
                            }
                            for i in 0..n {
                                match tokio::time::timeout(COMPLETION_TIMEOUT, done_rx.recv())
                                    .await
                                {
                                    Ok(Some(())) => {}
                                    Ok(None) => panic!(
                                        "{bench_ctx}: done channel closed at completion {}/{n} \
                                         (worker exited?)",
                                        i + 1
                                    ),
                                    Err(_) => panic!(
                                        "{bench_ctx}: stalled waiting for writer completion \
                                         {}/{n} after {COMPLETION_TIMEOUT:?}",
                                        i + 1
                                    ),
                                }
                            }
                        });
                    });
                },
            );

            // Teardown: dropping the go senders ends each worker loop; join
            // them before the fixtures are torn down.
            drop(go_txs);
            for worker in workers {
                let _ = rt.block_on(worker.handle);
            }
            drop(fixtures);
        }
    }

    // --- DuckDB: one database file, N tables, one writer thread per table.
    for &tables in TABLE_COUNTS {
        group.throughput(Throughput::Elements((tables * WRITE_BATCH_ROWS) as u64));

        let fixture = setup_duckdb("cdc_multi_0");
        for i in 1..tables {
            fixture
                .conn
                .execute_batch(&format!(
                    "CREATE TABLE cdc_multi_{i} (id BIGINT, name VARCHAR NOT NULL, \
                     value BIGINT NOT NULL);"
                ))
                .expect("duckdb create table");
        }

        // Preload to the same per-table batch count as the Cayenne lanes.
        {
            let preload_start = -((PRELOAD_BATCHES * WRITE_BATCH_ROWS) as i64);
            for i in 0..tables {
                let table_name = format!("cdc_multi_{i}");
                for batch_idx in 0..PRELOAD_BATCHES {
                    let start = preload_start + (batch_idx * WRITE_BATCH_ROWS) as i64;
                    let batch = make_batch(schema(), start, WRITE_BATCH_ROWS);
                    duckdb_insert_rows(&fixture.conn, &table_name, &batch);
                }
            }
        }

        let (done_tx, done_rx) = mpsc::channel::<()>();
        let mut go_txs: Vec<mpsc::Sender<()>> = Vec::with_capacity(tables);
        let mut workers: Vec<DuckDbTableWriter> = Vec::with_capacity(tables);
        for i in 0..tables {
            let (go_tx, go_rx) = mpsc::channel::<()>();
            go_txs.push(go_tx);
            workers.push(DuckDbTableWriter::spawn(
                fixture.db_path(),
                format!("cdc_multi_{i}"),
                go_rx,
                done_tx.clone(),
            ));
        }
        drop(done_tx);

        let bench_ctx = format!("vs_duckdb_cdc_multitable/duckdb tables={tables}");
        group.bench_with_input(BenchmarkId::new("duckdb", tables), &tables, |b, &n| {
            b.iter(|| {
                for tx in &go_txs {
                    tx.send(()).expect("duckdb go-tx (worker exited?)");
                }
                for i in 0..n {
                    done_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap_or_else(|err| {
                        panic!(
                            "{bench_ctx}: stalled waiting for writer completion {}/{n} \
                             after {COMPLETION_TIMEOUT:?} ({err})",
                            i + 1
                        )
                    });
                }
            });
        });

        drop(go_txs);
        for worker in workers {
            let _ = worker.handle.join();
        }
        drop(fixture);
    }

    group.finish();
}

criterion_group!(benches, bench_cdc_multitable);
criterion_main!(benches);
