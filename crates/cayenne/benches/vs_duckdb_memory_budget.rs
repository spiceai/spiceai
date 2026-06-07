// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Peak memory under a fixed budget: Cayenne vs DuckDB.
//!
//! Every other `vs_duckdb_*` bench runs Cayenne on `RuntimeEnv::default()` —
//! an UNLIMITED memory pool — so none of them can see whether Cayenne lives
//! within the budget an operator configures via `runtime.query.memory_limit`.
//! This bench closes that gap: each lane runs the same workload at the same
//! configured budget on both engines —
//!
//! - **Cayenne**: a `TrackConsumersPool<GreedyMemoryPool>` sized to the
//!   budget (exactly how spiced builds the production pool — see
//!   `crates/runtime/src/datafusion/builder.rs`), wrapped in a high-water
//!   tracker, wired into BOTH the table's `RuntimeEnv` (write path) and the
//!   query `SessionContext` (scan path), mirroring spiced's single shared
//!   `RuntimeEnv`.
//! - **DuckDB**: `SET memory_limit='<budget>'` on the same file-backed DB.
//!
//! Three workloads per budget: a filtered scan-aggregate, a GROUP BY with
//! 16Ki groups, and a PK upsert from parquet (the CDC replace shape).
//!
//! Reported signals, per (engine, workload, budget):
//! - criterion wall-clock (latency at the budget);
//! - `pool high-water` lines on stderr — the maximum bytes the DataFusion
//!   pool had reserved at any point during the lane (Cayenne), best-effort
//!   `duckdb_memory()` usage (DuckDB);
//! - a hard assert that Cayenne's pool high-water never exceeds the budget
//!   (the operator contract — exceeding it is a bug, not a slow lane);
//! - a lane that cannot complete at a budget (ResourcesExhausted) is
//!   SKIPPED with an explanatory stderr line rather than panicking — "needs
//!   more than 256 MiB for this workload" is a finding, not a crash.
//!
//! Pool high-water ≈ 0 while the workload clearly allocates (e.g. the upsert
//! lane) is itself a signal: that work is running OFF-pool, invisible to the
//! operator's budget — the known "ingest caches sized off total RAM" class
//! of findings. For process-level peak RSS, wrap the bench invocation in
//! `/usr/bin/time -l` (macOS) or `/usr/bin/time -v` (Linux).
//!
//! READING THE NUMBERS: budgets run sequentially (256MiB → 2GiB → 16GiB), so
//! slow systemic drift (thermal, page cache) accumulates toward the later
//! budgets — compare ENGINES WITHIN a budget, not one engine across budgets.
//! The high-water lines are order-robust; the wall-clock lanes are the
//! within-budget pairing.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]

#[path = "vs_duckdb_helpers/common.rs"]
mod common;

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::common::Result as DataFusionResult;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryLimit, MemoryPool, MemoryReservation, TrackConsumersPool,
};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionContext;
use duckdb::Connection;
use tokio::runtime::Runtime;

use common::{
    CayenneFixture, Metastore, cayenne_insert, make_batch, make_batch_grouped, schema,
    setup_cayenne_custom, warm_session_with_runtime, write_parquet,
};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

/// Budgets from the audit skill's three reference points: embedded /
/// dev-laptop / production-server.
const MEMORY_BUDGETS: &[(&str, usize)] = &[
    ("256MiB", 256 * 1024 * 1024),
    ("2GiB", 2 * 1024 * 1024 * 1024),
    ("16GiB", 16 * 1024 * 1024 * 1024),
];

/// Rows in the pre-loaded fixture. Matches `vs_duckdb_scaling`'s READ_ROWS
/// so latency lanes are comparable across benches.
const BASE_ROWS: usize = 1_048_576;

/// Distinct GROUP BY keys — the existing `vs_duckdb_groupby` high-cardinality
/// lane, here re-run under a budgeted pool.
const GROUPS: usize = 16_384;

/// Rows per upsert batch (CDC replace shape: same ids re-written).
const UPSERT_ROWS: usize = 16_384;

const SCAN_SQL: &str = "SELECT SUM(value) FROM t WHERE id BETWEEN 1000 AND 11000";
const GROUPBY_SQL: &str = "SELECT name, SUM(value) FROM t GROUP BY name";

// ---------------------------------------------------------------------------
// High-water memory pool
// ---------------------------------------------------------------------------

/// Wraps the production pool shape (`TrackConsumersPool<GreedyMemoryPool>`)
/// and records the maximum total reservation ever observed. DataFusion's
/// `MemoryPool::reserved()` only reports the CURRENT reservation; the budget
/// question is about the peak.
#[derive(Debug)]
struct HighWaterPool {
    inner: TrackConsumersPool<GreedyMemoryPool>,
    high_water: AtomicUsize,
}

impl HighWaterPool {
    fn new(budget_bytes: usize) -> Self {
        let topn = NonZeroUsize::new(5).expect("non-zero top-n");
        Self {
            inner: TrackConsumersPool::new(GreedyMemoryPool::new(budget_bytes), topn),
            high_water: AtomicUsize::new(0),
        }
    }

    fn record_high_water(&self) {
        let reserved = self.inner.reserved();
        self.high_water.fetch_max(reserved, Ordering::Relaxed);
    }

    fn high_water_bytes(&self) -> usize {
        self.high_water.load(Ordering::Relaxed)
    }

    fn reset_high_water(&self) {
        // Seed with the CURRENT reservation, not 0: the high-water only
        // advances on grow/try_grow, so a pool holding steady-state
        // reservations that don't grow further during the measured window
        // would otherwise read 0 — hiding held memory (e.g. table-side
        // caches sized during a pre-flight upsert and retained across the
        // timed iterations).
        self.high_water
            .store(self.inner.reserved(), Ordering::Relaxed);
    }
}

impl MemoryPool for HighWaterPool {
    fn register(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &datafusion::execution::memory_pool::MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.record_high_water();
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DataFusionResult<()> {
        self.inner.try_grow(reservation, additional)?;
        self.record_high_water();
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

/// A budgeted Cayenne fixture: the table AND the warm query session share one
/// `RuntimeEnv` built on a [`HighWaterPool`] — the spiced topology.
struct BudgetedCayenne {
    fixture: CayenneFixture,
    warm_ctx: SessionContext,
    pool: Arc<HighWaterPool>,
}

async fn setup_budgeted_cayenne(
    table_name: &str,
    budget_bytes: usize,
    with_pk: bool,
) -> BudgetedCayenne {
    let pool = Arc::new(HighWaterPool::new(budget_bytes));
    let runtime_env: Arc<RuntimeEnv> = RuntimeEnvBuilder::default()
        .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
        .build_arc()
        .expect("budgeted runtime env");

    let (primary_key, on_conflict) = if with_pk {
        (
            vec!["id".to_string()],
            Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
        )
    } else {
        (vec![], None)
    };

    let fixture = setup_cayenne_custom(
        table_name,
        Metastore::Sqlite,
        primary_key,
        on_conflict,
        schema(),
        cayenne::metadata::VortexConfig::default(),
        Arc::clone(&runtime_env),
    )
    .await;
    let warm_ctx = warm_session_with_runtime(&fixture.table, runtime_env);
    BudgetedCayenne {
        fixture,
        warm_ctx,
        pool,
    }
}

/// Run a query, returning the engine error instead of panicking — a budget
/// too small for the workload is a reportable outcome, not a bench crash.
async fn try_query(ctx: &SessionContext, sql: &str) -> DataFusionResult<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// Upsert from parquet through the BUDGETED session. The shared
/// `cayenne_insert_from_parquet` helper builds its own
/// `SessionContext::new()` — an unbudgeted pool — and
/// `CayenneTableProvider::insert_into` draws execution memory from the
/// SESSION's runtime env, so routing this lane through the helper would
/// bypass the budget the bench exists to enforce (only the table-internal
/// reservations would land on the budgeted pool). Errors are returned, not
/// panicked, so a too-small budget can be reported as a skip.
async fn try_upsert_from_parquet(
    ctx: &SessionContext,
    table: &Arc<cayenne::CayenneTableProvider>,
    parquet_path: &std::path::Path,
) -> DataFusionResult<u64> {
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::ParquetReadOptions;
    use datafusion_expr::dml::InsertOp;

    let parquet_path = parquet_path.to_string_lossy().into_owned();
    let df = ctx
        .read_parquet::<&str>(parquet_path.as_str(), ParquetReadOptions::default())
        .await?;
    let input_exec = df.create_physical_plan().await?;
    let insert_plan = table
        .insert_into(&ctx.state(), input_exec, InsertOp::Append)
        .await?;
    let results = datafusion::physical_plan::collect(insert_plan, ctx.task_ctx()).await?;
    Ok(results
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .map_or(0, |rows| rows.value(0)))
}

fn eprintln_lane_memory(engine: &str, workload: &str, budget_label: &str, bytes: u64) {
    eprintln!(
        "memory_budget: {engine}/{workload}@{budget_label} pool high-water = {bytes} bytes \
         ({:.1} MiB)",
        bytes as f64 / (1024.0 * 1024.0)
    );
}

/// Best-effort DuckDB memory usage via `duckdb_memory()` (sum of
/// `memory_usage_bytes` across its internal allocators).
fn duckdb_memory_bytes(conn: &Connection) -> Option<i64> {
    conn.prepare("SELECT COALESCE(SUM(memory_usage_bytes), 0) FROM duckdb_memory()")
        .ok()?
        .query_row([], |row| row.get::<_, i64>(0))
        .ok()
}

// ---------------------------------------------------------------------------
// Bench
// ---------------------------------------------------------------------------

fn bench_memory_budget(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("vs_duckdb_memory_budget");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    let base_batch = make_batch_grouped(schema(), 0, BASE_ROWS, GROUPS);
    let parquet_dir = tempfile::tempdir().expect("parquet dir");
    let base_parquet = parquet_dir.path().join("base.parquet");
    write_parquet(&base_batch, &base_parquet);
    // Upsert source: re-writes ids 0..UPSERT_ROWS (CDC replace shape).
    let upsert_parquet = parquet_dir.path().join("upsert.parquet");
    write_parquet(&make_batch(schema(), 0, UPSERT_ROWS), &upsert_parquet);

    for &(budget_label, budget_bytes) in MEMORY_BUDGETS {
        // --- Cayenne read lanes (scan + groupby) on one budgeted fixture.
        {
            let lane = rt.block_on(setup_budgeted_cayenne("memory_budget", budget_bytes, false));
            let preload = rt.block_on(cayenne_insert(&lane.fixture.table, base_batch.clone()));
            assert!(preload > 0, "cayenne preload must insert rows");
            lane.pool.reset_high_water();

            for (workload, sql) in [("scan_filter_sum", SCAN_SQL), ("groupby_16k", GROUPBY_SQL)] {
                // Pre-flight once: a budget the workload cannot fit is a
                // finding to report, not a panic.
                if let Err(e) = rt.block_on(try_query(&lane.warm_ctx, sql)) {
                    eprintln!("memory_budget: cayenne/{workload}@{budget_label} SKIPPED — {e}");
                    continue;
                }
                lane.pool.reset_high_water();
                group.bench_function(
                    BenchmarkId::new(format!("cayenne_{workload}"), budget_label),
                    |b| {
                        b.iter(|| {
                            rt.block_on(async {
                                let batches = try_query(&lane.warm_ctx, sql)
                                    .await
                                    .expect("pre-flighted query failed mid-bench");
                                black_box(batches);
                            });
                        });
                    },
                );
                let high_water = lane.pool.high_water_bytes() as u64;
                eprintln_lane_memory("cayenne", workload, budget_label, high_water);
                assert!(
                    high_water <= budget_bytes as u64,
                    "cayenne/{workload}@{budget_label}: pool high-water {high_water} bytes \
                     exceeds the configured budget {budget_bytes} — operator contract violated"
                );
            }
        }

        // --- Cayenne upsert lane on a budgeted PK fixture. The upsert runs
        //     through the lane's BUDGETED warm session (see
        //     `try_upsert_from_parquet`) so both the query-side decode and
        //     the table-side write reservations draw on the same budgeted
        //     pool — the spiced topology.
        {
            let lane = rt.block_on(setup_budgeted_cayenne(
                "memory_budget_upsert",
                budget_bytes,
                true,
            ));
            let preload = rt.block_on(cayenne_insert(&lane.fixture.table, base_batch.clone()));
            assert!(preload > 0, "cayenne upsert preload must insert rows");
            // Pre-flight once: a budget the upsert cannot fit is a finding
            // to report, not a panic.
            if let Err(e) = rt.block_on(try_upsert_from_parquet(
                &lane.warm_ctx,
                &lane.fixture.table,
                &upsert_parquet,
            )) {
                eprintln!("memory_budget: cayenne/upsert_16k@{budget_label} SKIPPED — {e}");
            } else {
                lane.pool.reset_high_water();
                group.bench_function(BenchmarkId::new("cayenne_upsert_16k", budget_label), |b| {
                    b.iter(|| {
                        rt.block_on(async {
                            let rows = try_upsert_from_parquet(
                                &lane.warm_ctx,
                                &lane.fixture.table,
                                &upsert_parquet,
                            )
                            .await
                            .expect("pre-flighted upsert failed mid-bench");
                            black_box(rows);
                        });
                    });
                });
                let high_water = lane.pool.high_water_bytes() as u64;
                eprintln_lane_memory("cayenne", "upsert_16k", budget_label, high_water);
                assert!(
                    high_water <= budget_bytes as u64,
                    "cayenne/upsert_16k@{budget_label}: pool high-water {high_water} bytes \
                     exceeds the configured budget {budget_bytes} — operator contract violated"
                );
            }
        }

        // --- DuckDB lanes at the same budget.
        {
            let fixture = common::setup_duckdb("memory_budget");
            fixture
                .conn
                .execute_batch(&format!("SET memory_limit='{budget_label}';"))
                .expect("duckdb SET memory_limit");
            common::duckdb_insert_parquet(&fixture.conn, "memory_budget", &base_parquet);

            for (workload, sql) in [
                (
                    "scan_filter_sum",
                    "SELECT SUM(value) FROM memory_budget WHERE id BETWEEN 1000 AND 11000",
                ),
                (
                    "groupby_16k",
                    "SELECT name, SUM(value) FROM memory_budget GROUP BY name",
                ),
            ] {
                group.bench_function(
                    BenchmarkId::new(format!("duckdb_{workload}"), budget_label),
                    |b| {
                        b.iter(|| {
                            let mut stmt = fixture.conn.prepare(sql).expect("duckdb prepare");
                            let mut rows = stmt.query([]).expect("duckdb query");
                            let mut n: i64 = 0;
                            while let Some(_row) = rows.next().expect("duckdb row") {
                                n += 1;
                            }
                            black_box(n);
                        });
                    },
                );
                if let Some(bytes) = duckdb_memory_bytes(&fixture.conn) {
                    eprintln_lane_memory("duckdb", workload, budget_label, bytes.max(0) as u64);
                }
            }
        }

        // --- DuckDB upsert lane at the same budget (PK table).
        {
            let fixture = common::setup_duckdb_pk("memory_budget_upsert");
            fixture
                .conn
                .execute_batch(&format!("SET memory_limit='{budget_label}';"))
                .expect("duckdb SET memory_limit");
            common::duckdb_insert_parquet(&fixture.conn, "memory_budget_upsert", &base_parquet);

            group.bench_function(BenchmarkId::new("duckdb_upsert_16k", budget_label), |b| {
                b.iter(|| {
                    common::duckdb_upsert_parquet(
                        &fixture.conn,
                        "memory_budget_upsert",
                        &upsert_parquet,
                    );
                });
            });
            if let Some(bytes) = duckdb_memory_bytes(&fixture.conn) {
                eprintln_lane_memory("duckdb", "upsert_16k", budget_label, bytes.max(0) as u64);
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_memory_budget);
criterion_main!(benches);
