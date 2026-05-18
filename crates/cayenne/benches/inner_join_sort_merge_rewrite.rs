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

//! Regression bench: latency cliff caused by `CayenneAntiJoinSortMergeRewriter`
//! firing on `Inner`-joins above the 10M-row build-side threshold.
//!
//! When the same-source inner-join build side exceeds
//! [`crate::ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS`] (10M), the rewriter at
//! `crates/cayenne/src/optimizer_rules.rs:360-430` replaces the
//! `HashJoinExec` with `SortMergeJoinExec` + explicit `SortExec` inputs on
//! both sides. The rationale is correctness/safety: `HashJoinExec`'s build
//! side is non-spillable, so a large hash-table can OOM the runtime.
//!
//! But the rewrite is *expensive when the original hash-join would have fit
//! in memory*:
//!
//! - Both inputs are fully materialized and sorted (`SortExec` × 2,
//!   `O(N log N)` time per side, plus full-row width in memory or on
//!   spill files).
//! - The sort-merge merge pass walks both inputs end-to-end.
//! - Total cost is typically 5–10× the pure-hash-join cost when the
//!   build side fits, and uses several times more peak memory because
//!   `SortExec` materializes both sides instead of just one hash table.
//!
//! TPC-DS at SF10+ has multiple fact tables above the 10M threshold
//! (`store_sales` ~29M at SF10, `web_sales` ~7M at SF10 grows to
//! ~72M at SF100, `catalog_sales` ~14M at SF10, `inventory` ~117M at SF10),
//! so the rewriter fires on most fact-side inner joins at production scale
//! factors. End-to-end TPC-DS-on-Cayenne shows substantial query-time and
//! memory regressions as a result. The `pairs.yaml` testoperator manifest
//! at `tools/testoperator/dispatch/perf-cayenne-vs-duckdb/` carries the
//! end-to-end benchmark; this Criterion bench is the focused per-rule
//! reproducer.
//!
//! ## What this bench measures
//!
//! Two lanes, identical query shape — a self-join over an int64 key column,
//! aggregating the row count. The only difference is the preloaded table
//! size:
//!
//! - `below_threshold/<N>` for `N < ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS` —
//!   the rule does not fire; `HashJoinExec` runs unchanged.
//! - `above_threshold/<N>` for `N > ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS` —
//!   the rule fires; `SortMergeJoinExec` with `SortExec` inputs runs
//!   instead.
//!
//! Because the row counts straddle the threshold by a small margin, the
//! raw data-size delta between the two lanes is modest (~2–3x), but the
//! query-time delta should be much larger if the rewrite is the
//! regression. Criterion's report makes that cliff visible.
//!
//! ## How to read the report
//!
//! After running `cargo bench --bench inner_join_sort_merge_rewrite -p cayenne`,
//! look at `inner_join_sort_merge_rewrite/below_threshold/<N>` versus
//! `inner_join_sort_merge_rewrite/above_threshold/<N>`. If the rewriter is
//! the cause of the TPC-DS regression, the time-per-row in the
//! `above_threshold` lane will be **significantly higher** than in the
//! `below_threshold` lane — disproportionate to the modest table-size
//! delta.
//!
//! A future fix (raise the threshold, make it memory-pool-aware, gate on
//! `cayenne_sort_merge_min_rows`, or split inner-join handling from
//! anti/semi-join handling) should bring the `above_threshold` curve back
//! into line with the `below_threshold` curve, scaled by raw data volume.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Just below the rewriter's 10M-row gate
/// (`ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS`).
const BELOW_THRESHOLD_ROWS: usize = 5_000_000;
/// Just above the rewriter's 10M-row gate — small margin so the data-size
/// delta vs the below-lane is modest.
const ABOVE_THRESHOLD_ROWS: usize = 12_000_000;

/// Insert chunk size — chosen large enough that per-burst overhead is
/// amortized but small enough that preloading 12M rows keeps the in-flight
/// batch under a few hundred MB.
const PRELOAD_CHUNK: usize = 100_000;

struct BenchTable {
    _temp_dir: TempDir,
    table: Arc<CayenneTableProvider>,
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn make_batch(start: i64, rows: usize) -> RecordBatch {
    let ids = (start..start + rows as i64).collect::<Vec<_>>();
    let values = ids.iter().map(|id| id * 100).collect::<Vec<_>>();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch")
}

async fn append_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> u64 {
    let ctx = SessionContext::new();
    let input_schema = Arc::clone(batch.schema_ref());
    let input_exec =
        MemorySourceConfig::try_new_exec(&[vec![batch]], input_schema, None).expect("memory exec");
    let insert_plan = table
        .insert_into(&ctx.state(), input_exec, InsertOp::Append)
        .await
        .expect("insert plan");
    let results = datafusion_physical_plan::collect(insert_plan, ctx.task_ctx())
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

async fn setup_table(table_name: &str, rows: usize) -> BenchTable {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = temp_dir.path().join("bench.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy())).expect("catalog"),
    );
    catalog.init().await.expect("catalog init");

    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: table_name.to_string(),
                schema: schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
            ctx.runtime_env(),
        )
        .await
        .expect("table"),
    );

    let mut written: usize = 0;
    while written < rows {
        let this_chunk = PRELOAD_CHUNK.min(rows - written);
        let batch = make_batch(written as i64, this_chunk);
        let n = append_batch(&table, batch).await;
        assert_eq!(n as usize, this_chunk);
        written += this_chunk;
    }

    BenchTable {
        _temp_dir: temp_dir,
        table,
    }
}

/// Run a self-equi-join on `id` aggregating into a single row count. The
/// shape mirrors a TPC-DS fact-table self-join (e.g. `store_sales`
/// joined back to itself by `ss_ticket_number`) — the inner-join build
/// side is the same Cayenne-backed scan as the probe side, so the
/// rewriter's same-source precondition fires.
async fn run_self_join(table: &Arc<CayenneTableProvider>) -> i64 {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");

    let df = ctx
        .sql(
            "SELECT COUNT(*) FROM t AS a INNER JOIN t AS b ON a.id = b.id \
             WHERE a.value > 0 AND b.value > 0",
        )
        .await
        .expect("sql");

    let batches = df.collect().await.expect("collect");
    batches
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
        })
        .map(|arr| arr.value(0))
        .unwrap_or(0)
}

fn bench_inner_join_sort_merge_rewrite(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("inner_join_sort_merge_rewrite");
    // The preload is multi-second; cap samples to keep bench wall-time
    // bounded while still resolving the regression cliff.
    group.sample_size(10);

    // Preload each lane ONCE before the timing loop. Query lanes are
    // pure reads, so the same fixture can be reused across all samples.
    let below = Arc::new(rt.block_on(setup_table("below_bench", BELOW_THRESHOLD_ROWS)));
    let above = Arc::new(rt.block_on(setup_table("above_bench", ABOVE_THRESHOLD_ROWS)));

    {
        let below = Arc::clone(&below);
        group.bench_with_input(
            BenchmarkId::new("below_threshold", BELOW_THRESHOLD_ROWS),
            &BELOW_THRESHOLD_ROWS,
            |b, &_| {
                b.iter(|| {
                    rt.block_on(async {
                        let n = run_self_join(&below.table).await;
                        black_box(n);
                    });
                });
            },
        );
    }

    {
        let above = Arc::clone(&above);
        group.bench_with_input(
            BenchmarkId::new("above_threshold", ABOVE_THRESHOLD_ROWS),
            &ABOVE_THRESHOLD_ROWS,
            |b, &_| {
                b.iter(|| {
                    rt.block_on(async {
                        let n = run_self_join(&above.table).await;
                        black_box(n);
                    });
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_inner_join_sort_merge_rewrite);
criterion_main!(benches);
