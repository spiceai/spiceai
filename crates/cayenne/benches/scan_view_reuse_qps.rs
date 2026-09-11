// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Scan-view reuse at 1000 QPS across every production reuse mode.
//!
//! Lanes (same file-backed table, no writes during the load):
//! * `recapture_every_scan` — [`ScanViewReuse::WithinLag(Duration::ZERO)`].
//!   Capture on every scan (`CAYENNE_SCAN_VIEW_FRESHNESS_MS=0`).
//! * `until_invalidated` — [`ScanViewReuse::UntilInvalidated`]. `full` /
//!   `append` / `snapshot` / `caching`, and writable `changes`.
//! * `changes_within_lag` — [`ScanViewReuse::WithinLag(1s)`]. Read-only
//!   `refresh_mode: changes`. On a quiet table this should match
//!   `until_invalidated` (cache hits, no recapture).
//!
//! Workloads (both on `WHERE id = ?`, file-backed, no SQL parse):
//! * `scan_plan` — `TableProvider::scan` only. Isolates scan-view capture /
//!   cache (the metastore-bypass). Headline 1000 QPS arm.
//! * `scan_collect` — scan + execute. End-to-end query including Vortex.
//! * Closed-loop max QPS (3 s, 32 workers) for both shapes.
//! * Criterion per-query latency of both shapes.
//!
//! `cargo bench -p cayenne --bench scan_view_reuse_qps`

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::{
    CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog,
    ScanViewReuse,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{col, lit};
use datafusion_physical_plan::collect;
use tokio::runtime::Runtime;
use tokio::sync::Semaphore;

/// Rows in the file-backed table. Large enough to be a real snapshot, small
/// enough that a PK lookup is setup-dominated (Vortex prunes to one row).
const ROWS: usize = 131_072;

/// Offer rate for the open-loop load.
const TARGET_QPS: u64 = 1000;

/// How long each load arm runs.
const LOAD_DURATION: Duration = Duration::from_secs(3);

/// Concurrent in-flight cap (open-loop) / worker count (closed-loop).
const CONCURRENCY: usize = 32;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn make_batch(start_id: i64, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows as i64).map(|i| start_id + i).collect();
    let names: Vec<String> = ids.iter().map(|id| format!("name_{id}")).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 100).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch")
}

async fn cayenne_insert(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> u64 {
    let ctx = SessionContext::new();
    let schema = Arc::clone(batch.schema_ref());
    let input_exec =
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory exec");
    let insert_plan = table
        .insert_into(&ctx.state(), input_exec, InsertOp::Append)
        .await
        .expect("insert plan");
    let results = collect(insert_plan, ctx.task_ctx())
        .await
        .expect("insert collect");
    results
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .map_or(0, |rows| rows.value(0))
}

struct ReuseFixture {
    _temp_dir: tempfile::TempDir,
    table: Arc<CayenneTableProvider>,
    ctx: Arc<SessionContext>,
    target_id: i64,
}

struct LoadReport {
    lane: &'static str,
    kind: &'static str,
    completed: u64,
    errors: u64,
    duration: Duration,
    achieved_qps: f64,
    p50_us: u64,
    p99_us: u64,
    p999_us: u64,
}

async fn setup_reuse_table(reuse: ScanViewReuse) -> ReuseFixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = temp_dir.path().join("catalog.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.display())).expect("catalog"),
    );
    catalog.init().await.expect("catalog init");
    let runtime_env = Arc::new(RuntimeEnv::default());

    let table = Arc::new(
        CayenneTableProviderBuilder::new(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            runtime_env,
        )
        .with_scan_view_reuse(reuse)
        .create(CreateTableOptions {
            table_name: "scan_view_qps".to_string(),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        })
        .await
        .expect("create table"),
    );
    // Production path: offload ScanView builds and run the idle evictor.
    table.init_scan_view_cache();

    let _ = cayenne_insert(&table, make_batch(0, ROWS)).await;
    let ctx = Arc::new(SessionContext::new());
    let target_id = (ROWS / 2) as i64;

    // Populate the scan-view cache (and Vortex footer cache) before any timed load.
    for _ in 0..16 {
        black_box(pk_scan_collect(&table, &ctx, target_id).await);
    }

    ReuseFixture {
        _temp_dir: temp_dir,
        table,
        ctx,
        target_id,
    }
}

fn pk_filters(target_id: i64) -> [datafusion_expr::Expr; 1] {
    [col("id").eq(lit(target_id))]
}

/// Plan-only PK lookup. Times scan-view capture vs cache; no Vortex execute.
async fn pk_scan_plan(
    table: &Arc<CayenneTableProvider>,
    ctx: &SessionContext,
    target_id: i64,
) {
    let filters = pk_filters(target_id);
    let projection = vec![2];
    let plan = table
        .scan(&ctx.state(), Some(&projection), &filters, None)
        .await
        .expect("scan");
    black_box(plan);
}

/// PK lookup through `TableProvider::scan` + collect — hits the scan-view
/// cache, then executes. Skips SQL parse/plan.
async fn pk_scan_collect(
    table: &Arc<CayenneTableProvider>,
    ctx: &SessionContext,
    target_id: i64,
) -> Vec<RecordBatch> {
    let filters = pk_filters(target_id);
    let projection = vec![2];
    let plan = table
        .scan(&ctx.state(), Some(&projection), &filters, None)
        .await
        .expect("scan");
    collect(plan, ctx.task_ctx()).await.expect("collect")
}

fn percentile_us(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn finish_report(
    lane: &'static str,
    kind: &'static str,
    completed: u64,
    errors: u64,
    duration: Duration,
    mut latencies_us: Vec<u64>,
) -> LoadReport {
    latencies_us.sort_unstable();
    let secs = duration.as_secs_f64().max(1e-9);
    LoadReport {
        lane,
        kind,
        completed,
        errors,
        duration,
        achieved_qps: completed as f64 / secs,
        p50_us: percentile_us(&latencies_us, 0.50),
        p99_us: percentile_us(&latencies_us, 0.99),
        p999_us: percentile_us(&latencies_us, 0.999),
    }
}

async fn run_one(
    table: &Arc<CayenneTableProvider>,
    ctx: &SessionContext,
    target_id: i64,
    collect_rows: bool,
) {
    if collect_rows {
        black_box(pk_scan_collect(table, ctx, target_id).await);
    } else {
        pk_scan_plan(table, ctx, target_id).await;
    }
}

/// Open-loop: launch lookups at `TARGET_QPS`, cap in-flight at `CONCURRENCY`.
async fn open_loop_1000qps(
    fixture: &ReuseFixture,
    lane: &'static str,
    collect_rows: bool,
) -> LoadReport {
    let sem = Arc::new(Semaphore::new(CONCURRENCY));
    let interval = Duration::from_nanos(1_000_000_000 / TARGET_QPS);
    let start = Instant::now();
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::with_capacity(
        (TARGET_QPS as usize) * LOAD_DURATION.as_secs() as usize + 64,
    )));
    let errors = Arc::new(AtomicU64::new(0));
    let mut joins = Vec::new();
    let mut next_launch = start;

    while start.elapsed() < LOAD_DURATION {
        let permit = Arc::clone(&sem)
            .acquire_owned()
            .await
            .expect("semaphore");
        let table = Arc::clone(&fixture.table);
        let ctx = Arc::clone(&fixture.ctx);
        let target_id = fixture.target_id;
        let latencies = Arc::clone(&latencies);
        joins.push(tokio::spawn(async move {
            let t0 = Instant::now();
            run_one(&table, &ctx, target_id, collect_rows).await;
            latencies
                .lock()
                .expect("latencies")
                .push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
            drop(permit);
        }));
        next_launch += interval;
        let now = Instant::now();
        if next_launch > now {
            tokio::time::sleep(next_launch - now).await;
        }
    }

    for join in joins {
        if join.await.is_err() {
            errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    let latencies_us = latencies.lock().expect("latencies").clone();
    finish_report(
        lane,
        if collect_rows {
            "open_loop_1000qps_collect"
        } else {
            "open_loop_1000qps_scan_plan"
        },
        latencies_us.len() as u64,
        errors.load(Ordering::Relaxed),
        start.elapsed(),
        latencies_us,
    )
}

/// Closed-loop: `CONCURRENCY` workers issue lookups as fast as they can.
async fn closed_loop_max_qps(
    fixture: &ReuseFixture,
    lane: &'static str,
    collect_rows: bool,
) -> LoadReport {
    let stop_at = Instant::now() + LOAD_DURATION;
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let errors = Arc::new(AtomicU64::new(0));
    let mut joins = Vec::with_capacity(CONCURRENCY);

    for _ in 0..CONCURRENCY {
        let table = Arc::clone(&fixture.table);
        let ctx = Arc::clone(&fixture.ctx);
        let target_id = fixture.target_id;
        let latencies = Arc::clone(&latencies);
        joins.push(tokio::spawn(async move {
            while Instant::now() < stop_at {
                let t0 = Instant::now();
                run_one(&table, &ctx, target_id, collect_rows).await;
                latencies
                    .lock()
                    .expect("latencies")
                    .push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
            }
        }));
    }

    let start = Instant::now();
    for join in joins {
        if join.await.is_err() {
            errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    let latencies_us = latencies.lock().expect("latencies").clone();
    finish_report(
        lane,
        if collect_rows {
            "closed_loop_max_qps_collect"
        } else {
            "closed_loop_max_qps_scan_plan"
        },
        latencies_us.len() as u64,
        errors.load(Ordering::Relaxed),
        start.elapsed(),
        latencies_us,
    )
}

fn print_report(report: &LoadReport) {
    eprintln!(
        "scan_view_reuse_qps  lane={:<22} kind={:<28} qps={:>8.1}  p50={:>7}µs  p99={:>7}µs  p999={:>7}µs  n={}  errors={}  dur_ms={}",
        report.lane,
        report.kind,
        report.achieved_qps,
        report.p50_us,
        report.p99_us,
        report.p999_us,
        report.completed,
        report.errors,
        report.duration.as_millis()
    );
}

fn bench_scan_view_reuse_qps(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");

    eprintln!(
        "scan_view_reuse_qps  setup: {ROWS} rows, target {TARGET_QPS} QPS, {CONCURRENCY} in-flight, {}s arms",
        LOAD_DURATION.as_secs()
    );

    let recapture = rt.block_on(setup_reuse_table(ScanViewReuse::WithinLag(
        Duration::ZERO,
    )));
    let cached = rt.block_on(setup_reuse_table(ScanViewReuse::UntilInvalidated));
    let changes_lag = rt.block_on(setup_reuse_table(ScanViewReuse::WithinLag(
        Duration::from_secs(1),
    )));
    let lanes: [(&str, &ReuseFixture); 3] = [
        ("recapture_every_scan", &recapture),
        ("until_invalidated", &cached),
        ("changes_within_lag", &changes_lag),
    ];

    // Headline loads — printed so a `cargo bench` log is the before/after
    // artifact, not only criterion's per-iter time. `scan_plan` isolates the
    // cache; `collect` is the end-to-end query (Vortex execute included).
    for collect_rows in [false, true] {
        let label = if collect_rows { "collect" } else { "scan_plan" };
        let mut closed_qps = Vec::new();
        for (lane, fixture) in lanes {
            let open = rt.block_on(open_loop_1000qps(fixture, lane, collect_rows));
            print_report(&open);
            let closed = rt.block_on(closed_loop_max_qps(fixture, lane, collect_rows));
            print_report(&closed);
            closed_qps.push((lane, closed.achieved_qps));
        }
        if let (Some((_, recapture_qps)), Some((_, until_qps)), Some((_, lag_qps))) = (
            closed_qps.first().copied(),
            closed_qps.get(1).copied(),
            closed_qps.get(2).copied(),
        ) {
            eprintln!(
                "scan_view_reuse_qps  closed_loop_max_qps/{label}  recapture={recapture_qps:.1}  until_invalidated={until_qps:.1} ({:.2}x)  changes_within_lag={lag_qps:.1} ({:.2}x)",
                until_qps / recapture_qps.max(1e-9),
                lag_qps / recapture_qps.max(1e-9)
            );
        }
    }

    let mut latency = c.benchmark_group("scan_view_reuse_pk_lookup");
    latency.sample_size(30);

    for collect_rows in [false, true] {
        let shape = if collect_rows { "collect" } else { "scan_plan" };
        for (lane, fixture) in lanes {
            let table = Arc::clone(&fixture.table);
            let ctx = Arc::clone(&fixture.ctx);
            let target_id = fixture.target_id;
            latency.bench_function(BenchmarkId::new(shape, lane), |b| {
                b.to_async(&rt).iter(|| {
                    let table = Arc::clone(&table);
                    let ctx = Arc::clone(&ctx);
                    async move {
                        run_one(&table, &ctx, target_id, collect_rows).await;
                    }
                });
            });
        }
    }
    latency.finish();

    let mut batch = c.benchmark_group("scan_view_reuse_32way");
    batch.sample_size(10);
    batch.throughput(Throughput::Elements(CONCURRENCY as u64));

    for collect_rows in [false, true] {
        let shape = if collect_rows { "collect" } else { "scan_plan" };
        for (lane, fixture) in lanes {
            let table = Arc::clone(&fixture.table);
            let ctx = Arc::clone(&fixture.ctx);
            let target_id = fixture.target_id;
            batch.bench_function(BenchmarkId::new(shape, lane), |b| {
                b.to_async(&rt).iter(|| {
                    let table = Arc::clone(&table);
                    let ctx = Arc::clone(&ctx);
                    async move {
                        let mut joins = Vec::with_capacity(CONCURRENCY);
                        for _ in 0..CONCURRENCY {
                            let table = Arc::clone(&table);
                            let ctx = Arc::clone(&ctx);
                            joins.push(tokio::spawn(async move {
                                run_one(&table, &ctx, target_id, collect_rows).await;
                            }));
                        }
                        for join in joins {
                            join.await.expect("worker");
                        }
                    }
                });
            });
        }
    }
    batch.finish();
}

criterion_group!(benches, bench_scan_view_reuse_qps);
criterion_main!(benches);
