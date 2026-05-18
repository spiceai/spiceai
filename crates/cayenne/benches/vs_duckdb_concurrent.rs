// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Scan-under-write contention: Cayenne vs DuckDB.
//!
//! For each lane, the bench preloads `BASE_ROWS` rows, then spawns a
//! background writer that loops forever appending small bursts of new rows.
//! In the timed region, the foreground runs a scan query repeatedly. Criterion
//! reports scan latency *under* sustained write pressure, which is what the
//! Spice CH-benCH retest report measured at the system level (Finding 2:
//! mixed OLAP performance under concurrent write load).
//!
//! - Cayenne: background appends run on the Tokio runtime; each burst goes
//!   through the full append path (acquire write_lock, write Vortex files,
//!   refresh listing, persist stats, commit catalog metadata).
//! - DuckDB:  background appends run on a dedicated `std::thread` with its
//!   own `Connection` to the same file-backed DB. DuckDB serializes writes
//!   internally; concurrent scans see snapshot-isolated state.
//!
//! Background lifecycle is RAII: dropping a `RunningWriter` signals the
//! writer to stop and joins it. This guarantees clean teardown between
//! benchmark groups.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use duckdb::Connection;
use tokio::runtime::Runtime;

use common::{
    CAYENNE_LANES, CayenneFixture, DuckDbFixture, Metastore, cayenne_insert, cayenne_query,
    duckdb_insert_parquet, duckdb_insert_rows, duckdb_query_scalar, make_batch, schema,
    setup_cayenne_for, setup_duckdb, write_parquet,
};

const BASE_ROWS: usize = 50_000;
const BG_BURST_ROWS: usize = 64;

const CAYENNE_SCAN_SQL: &str = "SELECT SUM(value) FROM t WHERE id BETWEEN 1000 AND 11000";
const DUCKDB_SCAN_SQL: &str =
    "SELECT SUM(value) FROM concurrent_bench WHERE id BETWEEN 1000 AND 11000";

/// Background writer handle for the Cayenne lane. Drop to stop + join.
struct CayenneBgWriter {
    stop: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
    rt_handle: tokio::runtime::Handle,
}

impl CayenneBgWriter {
    fn spawn(rt: &Runtime, fixture: &CayenneFixture) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let table = Arc::clone(&fixture.table);
        let handle = rt.spawn(async move {
            let mut cursor = BASE_ROWS as i64;
            while !stop_clone.load(Ordering::Relaxed) {
                let batch = make_batch(schema(), cursor, BG_BURST_ROWS);
                cursor += BG_BURST_ROWS as i64;
                let _ = cayenne_insert(&table, batch).await;
            }
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
            // Block the bench thread on the background task settling. Using
            // `block_on` here is safe because Criterion's bench thread is not
            // the runtime worker — the runtime owns its own threads.
            let _ = self.rt_handle.block_on(handle);
        }
    }
}

/// Background writer handle for the DuckDB lane. Drop to stop + join.
struct DuckDbBgWriter {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl DuckDbBgWriter {
    fn spawn(fixture: &DuckDbFixture) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let db_path = fixture.db_path();
        let handle = std::thread::spawn(move || {
            let conn = Connection::open(&db_path).expect("bg duckdb open");
            let mut cursor = BASE_ROWS as i64;
            while !stop_clone.load(Ordering::Relaxed) {
                let batch = make_batch(schema(), cursor, BG_BURST_ROWS);
                cursor += BG_BURST_ROWS as i64;
                duckdb_insert_rows(&conn, "concurrent_bench", &batch);
            }
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

async fn load_cayenne(lane: Metastore) -> CayenneFixture {
    let fixture = setup_cayenne_for("concurrent_bench", lane).await;
    let _ = cayenne_insert(&fixture.table, make_batch(schema(), 0, BASE_ROWS)).await;
    fixture
}

fn load_duckdb(parquet_path: &std::path::Path) -> DuckDbFixture {
    let fixture = setup_duckdb("concurrent_bench");
    duckdb_insert_parquet(&fixture.conn, "concurrent_bench", parquet_path);
    fixture
}

fn bench_concurrent(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_concurrent");
    // Lower sample size — each iteration runs against a moving table
    // (background writer keeps appending) and the goal is the relative
    // delta vs `vs_duckdb_scan`, not absolute precision.
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let parquet_dir = tempfile::tempdir().expect("parquet dir");
    let parquet_path = parquet_dir.path().join("base.parquet");
    write_parquet(&make_batch(schema(), 0, BASE_ROWS), &parquet_path);

    for &lane in CAYENNE_LANES {
        let lane_label = lane.lane();
        let fixture = rt.block_on(load_cayenne(lane));
        let bg = CayenneBgWriter::spawn(&rt, &fixture);

        let table = Arc::clone(&fixture.table);
        group.bench_function(BenchmarkId::new(lane_label, "scan_under_write"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let batches = cayenne_query(&table, CAYENNE_SCAN_SQL).await;
                    black_box(batches);
                });
            });
        });

        // Explicit drop order: stop the background writer before the
        // fixture, so the writer doesn't try to insert into a torn-down
        // table during cleanup.
        drop(bg);
        drop(fixture);
    }

    let duckdb_fixture = load_duckdb(&parquet_path);
    let bg = DuckDbBgWriter::spawn(&duckdb_fixture);
    group.bench_function(BenchmarkId::new("duckdb", "scan_under_write"), |b| {
        b.iter(|| {
            let v = duckdb_query_scalar(&duckdb_fixture.conn, DUCKDB_SCAN_SQL);
            black_box(v);
        });
    });
    drop(bg);
    drop(duckdb_fixture);

    group.finish();
}

criterion_group!(benches, bench_concurrent);
criterion_main!(benches);
