// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Scan latency under MAINTENANCE: Cayenne vs chDB (embedded ClickHouse).
//!
//! Sibling of `vs_duckdb_scan_under_compaction`. The CH-benCH system the
//! Cayenne perf work targets runs hours of scans concurrent with compaction;
//! this bench measures the scan-under-maintenance interference profile against
//! the embedded-ClickHouse reference.
//!
//! ## Lane shapes — and why chDB's differs
//!
//! - **Cayenne**: byte-identical to the `vs_duckdb_scan_under_compaction`
//!   Cayenne lane — a PK-upsert fixture with `inline_max_rows: 0` and a small
//!   compaction trigger, plus an RAII background task that appends a burst then
//!   drives `compact_protected_snapshots_subset` on a *separate* tokio task,
//!   genuinely concurrent with the foreground timed scans.
//! - **chDB**: chDB exposes ONE process-global ClickHouse engine and its
//!   `Session` is `Send + !Sync`, so a second session (or a background thread
//!   sharing the session) cannot run merges *concurrently* with the foreground
//!   scan the way DuckDB/Cayenne can. The honest analog is a single-threaded
//!   **interleave**: each timed iteration runs the scan and then drives one
//!   `INSERT … ; OPTIMIZE TABLE … FINAL` maintenance step on the same session.
//!   This measures scan latency on an engine that is also continually merging,
//!   but the merge is *between* scans, not *during* one. The numbers are still
//!   comparable as "scan cost on a table under active maintenance churn"; they
//!   are NOT a true concurrent-contention measurement. The plan doc calls this
//!   out, and the merge count is asserted > 0 and printed so a reader can see
//!   how much maintenance the scans overlapped.
//!
//! VALIDITY: after the timed region each lane asserts its maintenance actually
//! ran (Cayenne merge count / chDB optimize count > 0) — a bench whose
//! maintenance never fired would just re-measure an idle scan.
//!
//! Lanes:
//! - `cayenne`       — Cayenne with the SQLite metastore (default)
//! - `cayenne_turso` — Cayenne with the Turso metastore (--features turso)
//! - `chdb`          — chDB (embedded ClickHouse), MergeTree engine

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_chdb_helpers/chdb_common.rs"]
mod chdb_common;
#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::execution::runtime_env::RuntimeEnv;
use tokio::runtime::Runtime;

use chdb_common::setup_chdb_from_parquet;
use common::{
    CAYENNE_LANES, CayenneFixture, Metastore, cayenne_insert, cayenne_query, make_batch, schema,
    setup_cayenne_custom, write_parquet,
};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

/// Total preloaded rows, written as PRELOAD_SNAPSHOTS separate inserts so the
/// Cayenne table starts with a populated protected-snapshot tier.
const BASE_ROWS: usize = 49_152;
const PRELOAD_SNAPSHOTS: usize = 12;

/// Rows appended per maintenance cycle (each cycle = one merge trigger).
const BG_BURST_ROWS: usize = 2_048;

/// Cayenne: merge as soon as a tier holds this many runs.
const COMPACTION_TRIGGER: usize = 4;

/// Same scan as `vs_duckdb_scan_under_compaction` so the chDB number reads
/// directly against the Cayenne and DuckDB scan-under-maintenance numbers.
const CAYENNE_SCAN_SQL: &str = "SELECT SUM(value) FROM t WHERE id BETWEEN 1000 AND 11000";
const CHDB_SCAN_SQL: &str =
    "SELECT SUM(value) FROM compaction_bench WHERE id BETWEEN 1000 AND 11000";

async fn load_cayenne(lane: Metastore) -> CayenneFixture {
    let vortex_config = cayenne::metadata::VortexConfig {
        inline_max_rows: 0,
        compaction_trigger_protected_snapshots: COMPACTION_TRIGGER,
        compaction_background_interval_ms: 3_600_000,
        ..cayenne::metadata::VortexConfig::default()
    };
    let fixture = setup_cayenne_custom(
        "compaction_bench",
        lane,
        vec!["id".to_string()],
        Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        schema(),
        vortex_config,
        Arc::new(RuntimeEnv::default()),
    )
    .await;
    let rows_per_snapshot = BASE_ROWS / PRELOAD_SNAPSHOTS;
    for snapshot in 0..PRELOAD_SNAPSHOTS {
        let start = (snapshot * rows_per_snapshot) as i64;
        let _ = cayenne_insert(
            &fixture.table,
            make_batch(schema(), start, rows_per_snapshot),
        )
        .await;
    }
    fixture
}

/// Background compactor for the Cayenne lane (identical to the DuckDB sibling):
/// append a burst (new snapshot), then drive a subset-compaction pass on a
/// separate tokio task. Drop to stop + join.
struct CayenneBgCompactor {
    stop: Arc<AtomicBool>,
    merges: Arc<AtomicUsize>,
    handle: Option<tokio::task::JoinHandle<()>>,
    rt_handle: tokio::runtime::Handle,
}

impl CayenneBgCompactor {
    fn spawn(rt: &Runtime, fixture: &CayenneFixture) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let merges = Arc::new(AtomicUsize::new(0));
        let stop_clone = Arc::clone(&stop);
        let merges_clone = Arc::clone(&merges);
        let table = Arc::clone(&fixture.table);
        let handle = rt.spawn(async move {
            let mut cursor = BASE_ROWS as i64;
            while !stop_clone.load(Ordering::Relaxed) {
                let batch = make_batch(schema(), cursor, BG_BURST_ROWS);
                cursor += BG_BURST_ROWS as i64;
                let _ = cayenne_insert(&table, batch).await;
                match table.compact_protected_snapshots_subset(usize::MAX).await {
                    Ok(true) => {
                        merges_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(false) => {}
                    Err(e) => panic!("background compaction failed: {e}"),
                }
            }
        });
        Self {
            stop,
            merges,
            handle: Some(handle),
            rt_handle: rt.handle().clone(),
        }
    }

    /// Clone of the merge counter; read AFTER drop+join (an in-flight pass can
    /// still complete during stop+join).
    fn merges_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.merges)
    }
}

impl Drop for CayenneBgCompactor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            if let Err(join_err) = self.rt_handle.block_on(handle) {
                if join_err.is_panic() && !std::thread::panicking() {
                    std::panic::resume_unwind(join_err.into_panic());
                }
            }
        }
    }
}

fn bench_scan_under_compaction(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_chdb_scan_under_compaction");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let parquet_dir = tempfile::tempdir().expect("parquet dir");
    let parquet_path = parquet_dir.path().join("base.parquet");
    write_parquet(&make_batch(schema(), 0, BASE_ROWS), &parquet_path);

    // --- Cayenne lanes (true concurrent background compaction) ---
    for &lane in CAYENNE_LANES {
        let lane_label = lane.lane();
        let fixture = rt.block_on(load_cayenne(lane));
        let bg = CayenneBgCompactor::spawn(&rt, &fixture);

        let table = Arc::clone(&fixture.table);
        group.bench_function(BenchmarkId::new(lane_label, "scan_under_compaction"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let batches = cayenne_query(&table, CAYENNE_SCAN_SQL).await;
                    black_box(batches);
                });
            });
        });

        let merges_counter = bg.merges_counter();
        drop(bg);
        drop(fixture);
        let merges = merges_counter.load(Ordering::Relaxed);
        eprintln!(
            "vs_chdb_scan_under_compaction: {lane_label} background subset merges = {merges}"
        );
        assert!(
            merges > 0,
            "{lane_label}: background compactor never merged — the bench did not \
             measure scan-under-compaction (check trigger/threshold config)"
        );
    }

    // --- chDB lane (single-thread interleave: scan, then one OPTIMIZE step) ---
    //
    // chDB's single process-global engine + `Session: !Sync` rule out a truly
    // concurrent background merger (see the module docs). The OPTIMIZE step
    // runs between scans on the same session; the optimize counter proves
    // maintenance fired during the timed region.
    let chdb_fixture = setup_chdb_from_parquet("compaction_bench", &parquet_path);
    let base_count = chdb_fixture.query_scalar("SELECT COUNT(*) FROM compaction_bench");
    assert_eq!(
        base_count, BASE_ROWS as i64,
        "chdb loaded {base_count} base rows, expected {BASE_ROWS} — bench would measure \
         an under-loaded table"
    );

    let optimizes = AtomicUsize::new(0);
    let cursor = std::cell::Cell::new(BASE_ROWS as i64);
    group.bench_function(BenchmarkId::new("chdb", "scan_under_compaction"), |b| {
        b.iter(|| {
            let v = chdb_fixture.query_scalar(CHDB_SCAN_SQL);
            black_box(v);
            // One maintenance step per iteration: append a burst + OPTIMIZE
            // FINAL. This keeps the engine merging across the timed region.
            let next = chdb_fixture.append_burst_and_optimize(cursor.get(), BG_BURST_ROWS);
            cursor.set(next);
            optimizes.fetch_add(1, Ordering::Relaxed);
        });
    });

    let optimize_count = optimizes.load(Ordering::Relaxed);
    eprintln!(
        "vs_chdb_scan_under_compaction: chdb interleaved OPTIMIZE FINAL passes = {optimize_count}"
    );
    assert!(
        optimize_count > 0,
        "chdb: no OPTIMIZE FINAL ran — the bench did not measure scan-under-maintenance"
    );
    drop(chdb_fixture);

    group.finish();
}

criterion_group!(benches, bench_scan_under_compaction);
criterion_main!(benches);
