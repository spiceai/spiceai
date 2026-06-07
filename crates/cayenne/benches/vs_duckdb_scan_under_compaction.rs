// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Scan latency under background COMPACTION: Cayenne vs DuckDB.
//!
//! Sibling of `vs_duckdb_concurrent` (scan-under-*write*). Compaction is the
//! heavier background writer: it re-reads and re-encodes whole snapshot
//! subsets and contends with readers on the listing-refresh / snapshot-publish
//! fence — a different (and worse) interference profile than small appends.
//! The CH-benCH system runs measured exactly this state for hours at a time;
//! no paired micro-bench covered it.
//!
//! Lane shape (RAII background worker, foreground timed scans — the
//! `vs_duckdb_concurrent` pattern):
//!
//! - **Cayenne**: a PK upsert fixture (protected snapshots — the
//!   compactor's input — are a conflict-resolution construct; an append-only
//!   table never produces any) pinning `inline_max_rows: 0` and a small
//!   compaction trigger so every insert lands as a protected snapshot. The
//!   background task loops: append a burst (creates a fresh snapshot), then
//!   drive `compact_protected_snapshots_subset(usize::MAX)` — the same
//!   entry point the background maintenance loop calls — counting merges
//!   that actually happened.
//! - **DuckDB**: the background thread loops: insert a burst, then force a
//!   `CHECKPOINT` — DuckDB's analogous reader-contending maintenance write.
//!
//! VALIDITY: after the timed region each lane asserts its background
//! maintenance actually ran (merge count / checkpoint count > 0) — a bench
//! whose compactor never fired would just re-measure scan-under-append.
//! The counts are printed to stderr so a run can also be sanity-checked for
//! *how much* compaction the scans overlapped.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::execution::runtime_env::RuntimeEnv;
use duckdb::Connection;
use tokio::runtime::Runtime;

use common::{
    CAYENNE_LANES, CayenneFixture, DuckDbFixture, Metastore, cayenne_insert, cayenne_query,
    duckdb_insert_parquet, duckdb_insert_rows, duckdb_query_scalar, make_batch, schema,
    setup_cayenne_custom, setup_duckdb_pk, write_parquet,
};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

/// Total preloaded rows, written as PRELOAD_SNAPSHOTS separate inserts so the
/// table starts with a populated protected-snapshot tier for the compactor.
const BASE_ROWS: usize = 49_152;
const PRELOAD_SNAPSHOTS: usize = 12;

/// Rows appended per background cycle (each append = one new snapshot).
const BG_BURST_ROWS: usize = 2_048;

/// Merge as soon as a tier holds this many runs — keeps the compactor busy.
const COMPACTION_TRIGGER: usize = 4;

/// Same scan as `vs_duckdb_concurrent`, so "scan under compaction" reads
/// directly against "scan under write" and idle-scan numbers.
const CAYENNE_SCAN_SQL: &str = "SELECT SUM(value) FROM t WHERE id BETWEEN 1000 AND 11000";
const DUCKDB_SCAN_SQL: &str =
    "SELECT SUM(value) FROM compaction_bench WHERE id BETWEEN 1000 AND 11000";

async fn load_cayenne(lane: Metastore) -> CayenneFixture {
    let vortex_config = cayenne::metadata::VortexConfig {
        // Everything lands in Vortex files immediately — protected snapshots
        // are what the compactor consumes.
        inline_max_rows: 0,
        compaction_trigger_protected_snapshots: COMPACTION_TRIGGER,
        // Effectively disable the interval loop: the background task below
        // drives compaction deterministically instead.
        compaction_background_interval_ms: 3_600_000,
        ..cayenne::metadata::VortexConfig::default()
    };
    // PK + upsert table: protected snapshots — the compactor's input — are
    // a conflict-resolution construct, so a plain append-only table never
    // produces any and `compact_protected_snapshots_subset` is a no-op
    // (the validity gate below caught exactly that on a non-PK draft of
    // this bench: merges = 0).
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

/// Background compactor for the Cayenne lane: append a burst (new snapshot),
/// then drive a subset-compaction pass. Drop to stop + join.
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

    /// Clone of the merge counter. Read it AFTER dropping the compactor —
    /// the background task can complete one more in-flight pass during the
    /// stop+join, so a pre-drop read can under-report (or, read as 0, fail
    /// the validity gate spuriously).
    fn merges_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.merges)
    }
}

impl Drop for CayenneBgCompactor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            if let Err(join_err) = self.rt_handle.block_on(handle) {
                // Propagate a background-compactor panic at the source —
                // swallowed, it would resurface later as a misleading
                // `merges = 0` validity failure (or as silently invalid
                // numbers). Skip while already unwinding: panicking inside
                // a panic-triggered drop aborts the process and eats the
                // original message.
                if join_err.is_panic() && !std::thread::panicking() {
                    std::panic::resume_unwind(join_err.into_panic());
                }
            }
        }
    }
}

/// Background checkpointer for the DuckDB lane: insert a burst, then force a
/// `CHECKPOINT` — the closest DuckDB analog to a maintenance rewrite that
/// contends with readers. Drop to stop + join.
struct DuckDbBgCheckpointer {
    stop: Arc<AtomicBool>,
    checkpoints: Arc<AtomicUsize>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl DuckDbBgCheckpointer {
    fn spawn(fixture: &DuckDbFixture) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let checkpoints = Arc::new(AtomicUsize::new(0));
        let stop_clone = Arc::clone(&stop);
        let checkpoints_clone = Arc::clone(&checkpoints);
        let db_path = fixture.db_path();
        let handle = std::thread::spawn(move || {
            let conn = Connection::open(&db_path).expect("bg duckdb open");
            let mut cursor = BASE_ROWS as i64;
            while !stop_clone.load(Ordering::Relaxed) {
                let batch = make_batch(schema(), cursor, BG_BURST_ROWS);
                cursor += BG_BURST_ROWS as i64;
                duckdb_insert_rows(&conn, "compaction_bench", &batch);
                // FORCE CHECKPOINT would abort concurrent transactions;
                // plain CHECKPOINT contends with (but doesn't kill) readers —
                // the apples-to-apples maintenance pressure.
                conn.execute_batch("CHECKPOINT;")
                    .expect("duckdb checkpoint");
                checkpoints_clone.fetch_add(1, Ordering::Relaxed);
            }
        });
        Self {
            stop,
            checkpoints,
            handle: Some(handle),
        }
    }

    /// Clone of the checkpoint counter — read after drop+join, see
    /// [`CayenneBgCompactor::merges_counter`].
    fn checkpoints_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.checkpoints)
    }
}

impl Drop for DuckDbBgCheckpointer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            if let Err(panic_payload) = handle.join() {
                // Same rationale as CayenneBgCompactor::drop — surface the
                // background panic at the source instead of as a misleading
                // checkpoint-validity failure later.
                if !std::thread::panicking() {
                    std::panic::resume_unwind(panic_payload);
                }
            }
        }
    }
}

fn bench_scan_under_compaction(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_scan_under_compaction");
    // Like vs_duckdb_concurrent: the table is moving (background writer +
    // compactor), so the signal is the delta vs the idle-scan bench, not
    // absolute precision.
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    let parquet_dir = tempfile::tempdir().expect("parquet dir");
    let parquet_path = parquet_dir.path().join("base.parquet");
    write_parquet(&make_batch(schema(), 0, BASE_ROWS), &parquet_path);

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

        // VALIDITY GATE: if no merge ever fired, the bench measured
        // scan-under-append, not scan-under-compaction. Read the counter
        // only after drop(bg) joins the task — an in-flight pass can still
        // complete (and increment) during the stop+join.
        let merges_counter = bg.merges_counter();
        drop(bg);
        drop(fixture);
        let merges = merges_counter.load(Ordering::Relaxed);
        eprintln!("scan_under_compaction: {lane_label} background subset merges = {merges}");
        assert!(
            merges > 0,
            "{lane_label}: background compactor never merged — the bench did not \
             measure scan-under-compaction (check trigger/threshold config)"
        );
    }

    // PK table for parity with the Cayenne upsert fixture.
    let duckdb_fixture = setup_duckdb_pk("compaction_bench");
    duckdb_insert_parquet(&duckdb_fixture.conn, "compaction_bench", &parquet_path);
    let bg = DuckDbBgCheckpointer::spawn(&duckdb_fixture);
    group.bench_function(BenchmarkId::new("duckdb", "scan_under_compaction"), |b| {
        b.iter(|| {
            let v = duckdb_query_scalar(&duckdb_fixture.conn, DUCKDB_SCAN_SQL);
            black_box(v);
        });
    });
    // Same post-join read discipline as the Cayenne lane above.
    let checkpoints_counter = bg.checkpoints_counter();
    drop(bg);
    drop(duckdb_fixture);
    let checkpoints = checkpoints_counter.load(Ordering::Relaxed);
    eprintln!("scan_under_compaction: duckdb background checkpoints = {checkpoints}");
    assert!(
        checkpoints > 0,
        "duckdb: background checkpointer never ran — the bench did not measure \
         scan-under-checkpoint"
    );

    group.finish();
}

criterion_group!(benches, bench_scan_under_compaction);
criterion_main!(benches);
