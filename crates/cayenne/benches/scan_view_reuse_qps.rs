// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Scan-view reuse at 10K QPS across every production refresh mode.
//!
//! Lanes (same file-backed table, no writes during the load):
//! * `recapture_every_scan` — [`ScanViewReuse::WithinLag(Duration::ZERO)`].
//!   Capture on every scan (`CAYENNE_SCAN_VIEW_FRESHNESS_MS=0`). Baseline.
//! * `full` — [`ScanViewReuse::UntilInvalidated`]. Production `refresh_mode: full`.
//! * `append` — [`ScanViewReuse::UntilInvalidated`]. Production `refresh_mode: append`.
//! * `changes` — [`ScanViewReuse::WithinLag(1s)`]. Production read-only
//!   `refresh_mode: changes`. On a quiet table this should match `full`/`append`
//!   (cache hits, no recapture).
//!
//! Workloads (both on `WHERE id = ?`, file-backed):
//! * `scan_plan` — `TableProvider::scan` only. Isolates scan-view capture /
//!   cache (the metastore-bypass). Headline 10K QPS arm.
//! * `scan_collect` — `DataFusion` `read_table` + filter + collect. End-to-end
//!   executed PK lookup (planner `FilterExec`, Vortex pushdown). Collect
//!   asserts the returned `value` is `id * 100` — throughput on the wrong
//!   row is counted as an error, not a QPS result.
//! * Closed-loop max QPS (3 s, 32 workers) for both shapes.
//! * Each arm reports `metastore_queries` (must stay 0 on a warm reuse hit).
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

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
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
const TARGET_QPS: u64 = 10_000;

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
    catalog: Arc<CayenneCatalog>,
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
    metastore_queries: u64,
}

async fn setup_reuse_table(reuse: ScanViewReuse) -> ReuseFixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = temp_dir.path().join("catalog.db");
    let catalog =
        Arc::new(CayenneCatalog::new(format!("sqlite://{}", db_path.display())).expect("catalog"));
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
    // Fail setup if the seeded PK lookup is already wrong — a throughput number
    // on incorrect rows is not a QPS result.
    let expected = expected_pk_value(target_id);
    for _ in 0..16 {
        let batches = pk_scan_collect(&table, &ctx, target_id).await;
        assert!(
            pk_lookup_value_is(&batches, expected),
            "warmup PK lookup for id={target_id} must return value={expected}"
        );
        black_box(batches);
    }

    ReuseFixture {
        _temp_dir: temp_dir,
        catalog,
        table,
        ctx,
        target_id,
    }
}

fn pk_filters(target_id: i64) -> [datafusion_expr::Expr; 1] {
    [col("id").eq(lit(target_id))]
}

/// Plan-only PK lookup. Times scan-view capture vs cache; no Vortex execute.
async fn pk_scan_plan(table: &Arc<CayenneTableProvider>, ctx: &SessionContext, target_id: i64) {
    let filters = pk_filters(target_id);
    let projection = vec![2];
    let plan = table
        .scan(&ctx.state(), Some(&projection), &filters, None)
        .await
        .expect("scan");
    black_box(plan);
}

/// Seeded `value` for `id` (`make_batch` writes `id * 100`).
const fn expected_pk_value(id: i64) -> i64 {
    id * 100
}

/// True iff `batches` is exactly one non-null `value` row equal to `expected`.
/// The scan projects column 2 (`value`), so the result's column 0 is that field.
fn pk_lookup_value_is(batches: &[RecordBatch], expected: i64) -> bool {
    let mut rows = 0_usize;
    for batch in batches {
        let Some(values) = batch.column(0).as_any().downcast_ref::<Int64Array>() else {
            return false;
        };
        for i in 0..values.len() {
            if values.is_null(i) || values.value(i) != expected {
                return false;
            }
            rows += 1;
        }
    }
    rows == 1
}

/// Executed PK lookup through the `DataFusion` planner. Direct
/// `TableProvider::scan` + collect does not apply data-column filters
/// (pushdown is Inexact), so a raw scan collect returns every `value` in
/// the file. `read_table` + filter adds the post-scan `FilterExec` the
/// physical optimizer then pushes into Vortex.
async fn pk_scan_collect(
    table: &Arc<CayenneTableProvider>,
    ctx: &SessionContext,
    target_id: i64,
) -> Vec<RecordBatch> {
    ctx.read_table(Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("read_table")
        .filter(col("id").eq(lit(target_id)))
        .expect("pk filter")
        .select_columns(&["value"])
        .expect("project value")
        .collect()
        .await
        .expect("collect")
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
    metastore_queries: u64,
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
        metastore_queries,
    }
}

/// Returns `false` when a collect returns the wrong row (counted as an error
/// by the load arms). Plan-only scans have no result to check.
async fn run_one(
    table: &Arc<CayenneTableProvider>,
    ctx: &SessionContext,
    target_id: i64,
    collect_rows: bool,
) -> bool {
    if collect_rows {
        let batches = pk_scan_collect(table, ctx, target_id).await;
        let ok = pk_lookup_value_is(&batches, expected_pk_value(target_id));
        black_box(&batches);
        ok
    } else {
        pk_scan_plan(table, ctx, target_id).await;
        true
    }
}

/// Open-loop: launch lookups at `TARGET_QPS`, cap in-flight at `CONCURRENCY`.
async fn open_loop_qps(
    fixture: &ReuseFixture,
    lane: &'static str,
    collect_rows: bool,
) -> LoadReport {
    let queries_before = fixture.catalog.metastore_query_count();
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
        let permit = Arc::clone(&sem).acquire_owned().await.expect("semaphore");
        let table = Arc::clone(&fixture.table);
        let ctx = Arc::clone(&fixture.ctx);
        let target_id = fixture.target_id;
        let latencies = Arc::clone(&latencies);
        let errors = Arc::clone(&errors);
        joins.push(tokio::spawn(async move {
            let t0 = Instant::now();
            let ok = run_one(&table, &ctx, target_id, collect_rows).await;
            if ok {
                latencies
                    .lock()
                    .expect("latencies")
                    .push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
            } else {
                errors.fetch_add(1, Ordering::Relaxed);
            }
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
    let metastore_queries = fixture
        .catalog
        .metastore_query_count()
        .saturating_sub(queries_before);
    finish_report(
        lane,
        if collect_rows {
            "open_loop_10kqps_collect"
        } else {
            "open_loop_10kqps_scan_plan"
        },
        latencies_us.len() as u64,
        errors.load(Ordering::Relaxed),
        start.elapsed(),
        latencies_us,
        metastore_queries,
    )
}

/// Closed-loop: `CONCURRENCY` workers issue lookups as fast as they can.
async fn closed_loop_max_qps(
    fixture: &ReuseFixture,
    lane: &'static str,
    collect_rows: bool,
) -> LoadReport {
    let queries_before = fixture.catalog.metastore_query_count();
    let stop_at = Instant::now() + LOAD_DURATION;
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let errors = Arc::new(AtomicU64::new(0));
    let mut joins = Vec::with_capacity(CONCURRENCY);

    for _ in 0..CONCURRENCY {
        let table = Arc::clone(&fixture.table);
        let ctx = Arc::clone(&fixture.ctx);
        let target_id = fixture.target_id;
        let latencies = Arc::clone(&latencies);
        let errors = Arc::clone(&errors);
        joins.push(tokio::spawn(async move {
            while Instant::now() < stop_at {
                let t0 = Instant::now();
                let ok = run_one(&table, &ctx, target_id, collect_rows).await;
                if ok {
                    latencies
                        .lock()
                        .expect("latencies")
                        .push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
                } else {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
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
    let metastore_queries = fixture
        .catalog
        .metastore_query_count()
        .saturating_sub(queries_before);
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
        metastore_queries,
    )
}

fn print_report(report: &LoadReport) {
    eprintln!(
        "scan_view_reuse_qps  lane={:<22} kind={:<28} qps={:>8.1}  p50={:>7}µs  p99={:>7}µs  p999={:>7}µs  n={}  errors={}  metastore_queries={}  dur_ms={}",
        report.lane,
        report.kind,
        report.achieved_qps,
        report.p50_us,
        report.p99_us,
        report.p999_us,
        report.completed,
        report.errors,
        report.metastore_queries,
        report.duration.as_millis()
    );
}

fn bench_scan_view_reuse_qps(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");

    eprintln!(
        "scan_view_reuse_qps  setup: {ROWS} rows, target {TARGET_QPS} QPS, {CONCURRENCY} in-flight, {}s arms",
        LOAD_DURATION.as_secs()
    );

    let recapture = rt.block_on(setup_reuse_table(ScanViewReuse::WithinLag(Duration::ZERO)));
    let full = rt.block_on(setup_reuse_table(ScanViewReuse::UntilInvalidated));
    let append = rt.block_on(setup_reuse_table(ScanViewReuse::UntilInvalidated));
    let changes = rt.block_on(setup_reuse_table(ScanViewReuse::WithinLag(
        Duration::from_secs(1),
    )));
    let lanes: [(&str, &ReuseFixture); 4] = [
        ("recapture_every_scan", &recapture),
        ("full", &full),
        ("append", &append),
        ("changes", &changes),
    ];

    // Headline loads — printed so a `cargo bench` log is the before/after
    // artifact, not only criterion's per-iter time. `scan_plan` isolates the
    // cache; `collect` is the end-to-end query (Vortex execute included).
    for collect_rows in [false, true] {
        let label = if collect_rows { "collect" } else { "scan_plan" };
        let mut closed_qps = Vec::new();
        for (lane, fixture) in lanes {
            let open = rt.block_on(open_loop_qps(fixture, lane, collect_rows));
            print_report(&open);
            let closed = rt.block_on(closed_loop_max_qps(fixture, lane, collect_rows));
            print_report(&closed);
            closed_qps.push((lane, closed.achieved_qps, closed.metastore_queries));
        }
        if closed_qps.len() == 4 {
            let recapture_qps = closed_qps[0].1;
            eprintln!(
                "scan_view_reuse_qps  closed_loop_max_qps/{label}  recapture={recapture_qps:.1}  full={:.1} ({:.2}x, meta={})  append={:.1} ({:.2}x, meta={})  changes={:.1} ({:.2}x, meta={})",
                closed_qps[1].1,
                closed_qps[1].1 / recapture_qps.max(1e-9),
                closed_qps[1].2,
                closed_qps[2].1,
                closed_qps[2].1 / recapture_qps.max(1e-9),
                closed_qps[2].2,
                closed_qps[3].1,
                closed_qps[3].1 / recapture_qps.max(1e-9),
                closed_qps[3].2,
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
                        assert!(
                            run_one(&table, &ctx, target_id, collect_rows).await,
                            "PK lookup returned the wrong row"
                        );
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
                                assert!(
                                    run_one(&table, &ctx, target_id, collect_rows).await,
                                    "PK lookup returned the wrong row"
                                );
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
