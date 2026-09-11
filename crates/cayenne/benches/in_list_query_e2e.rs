/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]

//! End-to-end query time for `IN` / `NOT IN` over a Cayenne table.
//!
//! A large `IN` list in ordinary SQL is the one path that reaches Vortex's
//! `list_contains` kernel without a cap: `DataFusion` sinks the predicate into
//! the Cayenne Vortex scan (no `FilterExec` survives, asserted below), and the
//! scan then evaluates it per batch. Hash-join dynamic filters are capped at 150
//! distinct values and declined by the opener, and the key-delete tombstone
//! filter reaches only listing-time file pruning — so this is what a kernel
//! change is actually worth to a query.
//!
//! Three key types are swept because each reaches the kernel differently: `i64`
//! keys the set on the value, `utf8` on the element bytes, and `f64` on the
//! bits, which is what makes `NaN` match itself and the two signed zeros not
//! match each other. A `utf8` column is also the one whose stored form the
//! kernel can read without decompressing, so its arm carries both the probe and
//! that.
//!
//! Both polarities are measured because they load the scan differently: `IN`
//! keeps M rows out of `ROWS`, so the scan can skip decoding payload for
//! everything else, while `NOT IN` keeps nearly all of them and pays full
//! decode on top of the membership test.
//!
//! The batch size is taken from `IN_LIST_BENCH_BATCH_SIZE` (default 8192).
//! Whatever a hashed kernel spends per batch on the list itself — reading M
//! scalars out of it and keying a set — is paid once per batch and amortized
//! over the rows in it, so running the same sweep at a larger batch size
//! measures how much of the query time that per-batch work is. That is the
//! ceiling available from hoisting the set into the expression, without having
//! to build the hoist to find out.
//!
//! `plan_only` measures parsing and planning the same statement without
//! executing it. At the top of the sweep the statement carries 32768 literals,
//! and without that arm its cost would be read as scan time.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{column_reference::ColumnReference, on_conflict::OnConflict};
use tempfile::TempDir;
use tokio::runtime::Runtime;

const ROWS: usize = 1_048_576;
/// Written in four inserts, so the table is several Vortex files — the shape
/// where a per-file cost is paid more than once.
const ROWS_PER_INSERT: usize = 262_144;

/// 150 is `DataFusion`'s hash-join `InList` cap and 2048 Cayenne's tombstone
/// cap; neither bounds a user's `IN` list, so the sweep runs past both.
const LIST_LENS: &[usize] = &[1, 32, 150, 512, 2048, 8192, 32_768];

/// The `utf8`/`f64` arms stop here rather than running the full sweep: `f64` is
/// deliberately never probed — equality makes `NaN` match nothing while a
/// value-keyed set would make it match itself — so its kernel stays
/// O(rows x elements) and the top of the integer sweep would cost minutes per
/// sample.
const NON_INTEGER_MAX_LIST_LEN: usize = 8192;

struct Fixture {
    table: Arc<CayenneTableProvider>,
    _dir: TempDir,
}

fn bench_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v1", DataType::Int64, false),
        Field::new("v2", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("ratio", DataType::Float64, false),
    ]))
}

fn make_batch(schema: &Arc<Schema>, start: i64, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (start..start + rows as i64).collect();
    let v1: Vec<i64> = ids.iter().map(|id| id * 2).collect();
    let v2: Vec<i64> = ids.iter().map(|id| id * 7 % 1_000).collect();
    // Unique per row, so an `IN` list of M labels matches M rows exactly, the
    // same shape the integer arms sweep.
    let labels: Vec<String> = ids.iter().map(|id| format!("k{id:09}")).collect();
    let ratios: Vec<f64> = ids.iter().map(|id| *id as f64 * 1.5).collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(v1)),
            Arc::new(Int64Array::from(v2)),
            Arc::new(StringArray::from(labels)),
            Arc::new(Float64Array::from(ratios)),
        ],
    )
    .expect("batch")
}

async fn append_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) {
    let ctx = SessionContext::new();
    let schema = Arc::clone(batch.schema_ref());
    let input_exec =
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory exec");
    let insert_plan = table
        .insert_into(&ctx.state(), input_exec, InsertOp::Append)
        .await
        .expect("insert plan");
    datafusion_physical_plan::collect(insert_plan, ctx.task_ctx())
        .await
        .expect("insert collect");
}

async fn setup() -> Fixture {
    let dir = tempfile::tempdir().expect("temp dir");
    let data_path = dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = dir.path().join("catalog.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy())).expect("catalog"),
    );
    catalog.init().await.expect("catalog init");

    let schema = bench_schema();
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "in_list_bench".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string()
                ]))),
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig {
                    // Materialize as Vortex files rather than inline rows, so the
                    // scan is the path under test.
                    inline_max_rows: 0,
                    inline_max_bytes: 0,
                    inline_max_buffer_bytes: 0,
                    ..VortexConfig::default()
                },
            },
            ctx.runtime_env(),
        )
        .await
        .expect("create table"),
    );

    let mut start = 0i64;
    while (start as usize) < ROWS {
        let rows = ROWS_PER_INSERT.min(ROWS - start as usize);
        append_batch(&table, make_batch(&schema, start, rows)).await;
        start += rows as i64;
    }

    Fixture { table, _dir: dir }
}

/// `list_len` ids spread evenly over `0..ROWS`, so each matches exactly one row
/// and the matches are scattered — no zone holds them all, which is the shape a
/// membership test has to actually evaluate.
fn list_ids(list_len: usize) -> Vec<i64> {
    let step = (ROWS / list_len).max(1);
    (0..list_len).map(|i| (i * step) as i64).collect()
}

fn batch_size() -> usize {
    std::env::var("IN_LIST_BENCH_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8192)
}

/// One session for the whole bench. A fresh `SessionContext` per iteration
/// rebuilds the whole `SessionState` — UDF registry, analyzer and optimizer
/// rule lists, catalog — which a long-running Spice runtime pays once, not per
/// query, and which would otherwise sit inside every number below including
/// `plan_only`.
fn ctx_for(table: &Arc<CayenneTableProvider>) -> SessionContext {
    let mut config = datafusion::prelude::SessionConfig::new();
    config.options_mut().execution.batch_size = batch_size();
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    ctx
}

/// (matched rows, payload sum) — both asserted, so a faster run that returned
/// the wrong rows fails instead of looking like an improvement.
fn run_query(rt: &Runtime, ctx: &SessionContext, sql: &str) -> (i64, i64) {
    rt.block_on(async {
        let batches = ctx
            .sql(sql)
            .await
            .expect("sql")
            .collect()
            .await
            .expect("collect");
        let col = |idx: usize| -> i64 {
            batches
                .iter()
                .flat_map(|b| {
                    b.column(idx)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("aggregate is Int64")
                        .iter()
                        .flatten()
                        .collect::<Vec<_>>()
                })
                .sum()
        };
        (col(0), col(1))
    })
}

/// Asserts the predicate reached the Vortex scan itself, by requiring every
/// file-reading `DataSourceExec` to carry it. Without this the bench could be
/// timing DataFusion's hashed `InListExpr` above the scan and not the Vortex
/// kernel at all.
///
/// "No `FilterExec` anywhere" is the wrong test: Cayenne's scan is a union
/// whose empty branches (`files_scanned=0` over `EmptyExec`) keep a `FilterExec`
/// that has no source to be pushed into.
fn assert_pushed_into_scan(rt: &Runtime, ctx: &SessionContext, sql: &str) {
    let plan = rt.block_on(async {
        let batches = ctx
            .sql(&format!("EXPLAIN ANALYZE {sql}"))
            .await
            .expect("explain")
            .collect()
            .await
            .expect("explain collect");
        arrow::util::pretty::pretty_format_batches(&batches)
            .expect("format")
            .to_string()
    });
    let scans: Vec<&str> = plan
        .lines()
        .filter(|l| l.contains("DataSourceExec"))
        .collect();
    assert!(
        !scans.is_empty(),
        "expected at least one Vortex scan in the plan:\n{plan}"
    );
    assert!(
        scans.iter().all(|l| l.contains("predicate")),
        "every Vortex scan must carry the pushed predicate:\n{plan}"
    );
    // How many batches the scan emitted, which is how many times a per-batch
    // cost is paid. Reported so a batch-size sweep can be checked to have
    // actually changed it rather than assumed to have.
    let batches: Vec<&str> = scans
        .iter()
        .filter_map(|l| l.split("output_batches=").nth(1))
        .map(|b| b.split(',').next().unwrap_or(b).trim())
        .collect();
    eprintln!(
        "BATCHES[size={}] scan output_batches={batches:?}",
        batch_size()
    );
}

fn bench_in_list_queries(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");
    let fixture = rt.block_on(setup());
    let table = &fixture.table;

    let total_sum: i64 = (0..ROWS as i64).map(|id| id * 2).sum();

    let ctx = ctx_for(table);
    let mut group = c.benchmark_group("in_list_query_e2e");
    group.sample_size(10);

    for &list_len in LIST_LENS {
        let ids = list_ids(list_len);
        let matched_sum: i64 = ids.iter().map(|&id| id * 2).sum();
        let rendered = |f: &dyn Fn(i64) -> String| {
            ids.iter().map(|&id| f(id)).collect::<Vec<_>>().join(", ")
        };

        // `id` and `label` are probed — integers by value, strings by their
        // bytes. `ratio` is not, so the `f64` arm shows what the falsifier
        // alone is worth, and what excluding floats from the probe costs.
        let mut arms = vec![
            ("i64", "id".to_string(), rendered(&|id| id.to_string())),
            ("utf8", "label".to_string(), rendered(&|id| format!("'k{id:09}'"))),
            (
                "f64",
                "ratio".to_string(),
                rendered(&|id| format!("{:?}", id as f64 * 1.5)),
            ),
        ];
        if list_len > NON_INTEGER_MAX_LIST_LEN {
            arms.retain(|(name, _, _)| *name == "i64");
        }

        for (name, column, list) in &arms {
            let in_sql = format!("SELECT count(*), sum(v1) FROM t WHERE {column} IN ({list})");
            let not_in_sql =
                format!("SELECT count(*), sum(v1) FROM t WHERE {column} NOT IN ({list})");

            assert_pushed_into_scan(&rt, &ctx, &in_sql);
            assert_pushed_into_scan(&rt, &ctx, &not_in_sql);
            assert_eq!(
                run_query(&rt, &ctx, &in_sql),
                (list_len as i64, matched_sum),
                "{name} IN must match one row per list value for M={list_len}"
            );
            assert_eq!(
                run_query(&rt, &ctx, &not_in_sql),
                (ROWS as i64 - list_len as i64, total_sum - matched_sum),
                "{name} NOT IN must keep every other row for M={list_len}"
            );

            group.bench_with_input(
                BenchmarkId::new(format!("in_list_{name}"), list_len),
                &list_len,
                |b, _| b.iter(|| black_box(run_query(&rt, &ctx, &in_sql))),
            );
            group.bench_with_input(
                BenchmarkId::new(format!("not_in_list_{name}"), list_len),
                &list_len,
                |b, _| b.iter(|| black_box(run_query(&rt, &ctx, &not_in_sql))),
            );
        }

        // Parse + plan of the integer statement, so the literal-handling cost
        // is separable from scan time rather than silently inside it.
        let in_sql = format!(
            "SELECT count(*), sum(v1) FROM t WHERE id IN ({})",
            arms[0].2
        );
        group.bench_with_input(BenchmarkId::new("plan_only", list_len), &list_len, |b, _| {
            b.iter(|| {
                let plan = rt.block_on(async { ctx.sql(&in_sql).await.expect("sql") });
                black_box(plan);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_in_list_queries);
criterion_main!(benches);
