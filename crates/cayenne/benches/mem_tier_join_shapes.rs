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

//! Join-shape latency over a memory-mode Cayenne table, with and without a
//! resident in-memory CDC tier.
//!
//! Hypothesis (SF-100 CH-benCH, `cdc_durability: memory`): query shapes that
//! reference a CDC table MORE THAN ONCE (semi/anti-join subqueries — q20/q21/
//! q2/q11/q15) degrade 4–200x when a RAM tier is resident, while single-scan
//! shapes stay at file-mode parity (q20: 322ms file vs 64.9s memory; q1/q10/
//! q18 at parity). Three union-related explanations were already REFUTED
//! (dynamic-filter routing, statistics fallback, Cayenne-rule detection), so
//! this bench measures the shapes directly to localize the cost: if the
//! degradation reproduces here, the in-process loop replaces 50-minute remote
//! runs for the fix iteration.
//!
//! Lanes: `{single_scan, semi_join, triple_scan}` x
//! `{file_only, overlay_fresh, overlay_upsert}`:
//! - `file_only`      — all rows durably checkpointed; tier empty.
//! - `overlay_fresh`  — a resident tier of NEW rows whose `item_id` lies
//!                      outside every query's domain (results identical).
//! - `overlay_upsert` — a resident tier of VALUE-IDENTICAL upserts of existing
//!                      rows: tombstones + replacement rows engage the full
//!                      merge-on-read machinery, results still identical.
//!
//! Bench discipline (Tiger Style): setup outside `b.iter`; every loop bounded;
//! every `expect` carries a message; every lane ASSERTS the query result equals
//! the file-only expected value (a fast wrong answer is worse than a slow right
//! one).

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog, SlotAdvancer};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

const ITEMS: i64 = 10_000;
const STOCK_ROWS: i64 = 100_000; // 10 stock rows per item
const OLINE_ROWS: i64 = 400_000; // 40 order lines per item
const SUPP_ROWS: i64 = 1_000;
const OVERLAY_FRESH_ROWS: i64 = 20_000; // item_id >= ITEMS: outside every query domain
const OVERLAY_UPSERT_ROWS: i64 = 20_000; // value-identical rewrites of existing pks
const INSERT_CHUNK: i64 = 50_000;

const SINGLE_SCAN_SQL: &str = "SELECT count(*), sum(ol_qty) FROM oline_b WHERE ol_item_id < 5000";
// q20-shaped: IN-subquery (semi join) whose inner is a stock x order_line join
// with GROUP BY + HAVING over an aggregate of the second CDC table.
const SEMI_JOIN_SQL: &str = "SELECT count(*) FROM supp_b WHERE su_key IN \
     (SELECT s_item_id % 1000 FROM stock_b JOIN oline_b ON ol_item_id = s_item_id \
      GROUP BY s_item_id, s_qty HAVING 200 * s_qty > sum(ol_qty))";
// q21-shaped: three references to the SAME CDC table (EXISTS + NOT EXISTS),
// bounded by an item_id range so the bench stays in seconds.
const TRIPLE_SCAN_SQL: &str = "SELECT count(*) FROM oline_b l1 \
     WHERE l1.ol_item_id < 200 \
       AND EXISTS (SELECT 1 FROM oline_b l2 \
                   WHERE l2.ol_item_id = l1.ol_item_id AND l2.ol_qty > l1.ol_qty) \
       AND NOT EXISTS (SELECT 1 FROM oline_b l3 \
                       WHERE l3.ol_item_id = l1.ol_item_id AND l3.ol_qty > l1.ol_qty + 50)";

#[derive(Clone, Copy, PartialEq, Eq)]
enum TierState {
    FileOnly,
    OverlayFresh,
    OverlayUpsert,
}

impl TierState {
    fn label(self) -> &'static str {
        match self {
            Self::FileOnly => "file_only",
            Self::OverlayFresh => "overlay_fresh",
            Self::OverlayUpsert => "overlay_upsert",
        }
    }
}

struct NoopAdvancer;

#[async_trait::async_trait]
impl SlotAdvancer for NoopAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

fn fact_schema(item_col: &str, qty_col: &str) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(item_col, DataType::Int64, false),
        Field::new(qty_col, DataType::Int64, false),
    ]))
}

/// Deterministic fact batch: row `i` gets `item_id = i % ITEMS`,
/// `qty = (i * 7) % 100` — seedless but fixed, identical across states.
fn fact_batch(schema: &Arc<Schema>, start_id: i64, rows: i64, item_offset: i64) -> RecordBatch {
    assert!(rows > 0, "fact_batch needs rows");
    let ids: Vec<i64> = (start_id..start_id + rows).collect();
    let items: Vec<i64> = ids.iter().map(|i| (i % ITEMS) + item_offset).collect();
    let qtys: Vec<i64> = ids.iter().map(|i| (i * 7) % 100).collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(items)),
            Arc::new(Int64Array::from(qtys)),
        ],
    )
    .expect("fact batch construction")
}

async fn insert_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) {
    let ctx = SessionContext::new();
    let schema = batch.schema();
    let input_exec = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
        &[vec![batch]],
        schema,
        None,
    )
    .expect("memory exec for insert");
    let insert_plan = table
        .insert_into(
            &ctx.state(),
            input_exec,
            datafusion_expr::dml::InsertOp::Append,
        )
        .await
        .expect("insert_into plan");
    datafusion_physical_plan::collect(insert_plan, ctx.task_ctx())
        .await
        .expect("insert collect");
}

async fn create_fact_table(
    catalog: &Arc<dyn MetadataCatalog>,
    data_dir: &str,
    name: &str,
    schema: Arc<Schema>,
    total_rows: i64,
) -> Arc<CayenneTableProvider> {
    let vortex_config = VortexConfig {
        cdc_durability: CdcDurability::Memory,
        deletion_mode: DeletionMode::Key,
        // Disable every tier self-flush so the bench fully controls residency:
        // no byte-cap spill, no age spill, no periodic tick (not spawned here).
        cdc_mem_tier_max_bytes: 0,
        cdc_mem_tier_max_age_ms: 0,
        cdc_mem_tier_min_flush_bytes: 0,
        ..VortexConfig::default()
    };
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(catalog),
            CreateTableOptions {
                table_name: name.to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: format!("{data_dir}/{name}"),
                partition_column: None,
                vortex_config,
            },
            Arc::new(RuntimeEnv::default()),
        )
        .await
        .expect("create fact table"),
    );

    let mut start = 0_i64;
    while start < total_rows {
        let rows = INSERT_CHUNK.min(total_rows - start);
        insert_batch(&table, fact_batch(&schema, start, rows, 0)).await;
        start += rows;
    }
    // Flush everything durable: the file branch is the baseline for all states.
    table
        .checkpoint_inlined_data()
        .await
        .expect("checkpoint base rows durable");
    table
}

async fn append_overlay(
    table: &Arc<CayenneTableProvider>,
    schema: &Arc<Schema>,
    state: TierState,
    fresh_item_offset: i64,
) {
    table.install_slot_advancer(Arc::new(NoopAdvancer));
    let ctx = SessionContext::new();
    let batch = match state {
        TierState::FileOnly => return,
        // Fresh rows OUTSIDE the query domain: new pks, item_id shifted by a
        // PER-TABLE offset so the two tables' overlays can never join EACH
        // OTHER either (same offset on both let overlay-stock x overlay-oline
        // form new semi-join groups — caught by the correctness gate).
        TierState::OverlayFresh => fact_batch(
            schema,
            OLINE_ROWS + STOCK_ROWS, // pks beyond every base table's range
            OVERLAY_FRESH_ROWS,
            fresh_item_offset,
        ),
        // Value-identical upserts of EXISTING pks: tombstones + replacements
        // engage merge-on-read fully while query results stay unchanged.
        TierState::OverlayUpsert => fact_batch(schema, 0, OVERLAY_UPSERT_ROWS, 0),
    };
    let stream_schema = batch.schema();
    let stream = datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(
        stream_schema,
        futures::stream::iter([Ok::<_, datafusion_common::DataFusionError>(batch)]),
    );
    let write = table
        .write_cdc_append_stream(Box::pin(stream), &ctx.task_ctx())
        .await
        .expect("CDC append to RAM tier");
    assert!(
        write.in_memory_epoch().is_some(),
        "overlay write must engage the RAM tier (advancer armed, memory mode) — \
         a durable fallback would mean the bench measures the wrong state"
    );
}

/// Background value-identical upsert stream: rewrites existing pks with their
/// SAME values at a steady cadence, so every query result stays byte-identical
/// while the tier version / structural epoch churn at live-CDC rates. Returns
/// a stop flag; the task exits within one cadence of it being set (bounded:
/// also hard-capped at MAX_APPEND_TICKS).
fn spawn_append_load(
    rt: &tokio::runtime::Runtime,
    table: Arc<CayenneTableProvider>,
    schema: Arc<Schema>,
) -> Arc<std::sync::atomic::AtomicBool> {
    const APPEND_ROWS: i64 = 2_000;
    const CADENCE: std::time::Duration = std::time::Duration::from_millis(50);
    const MAX_APPEND_TICKS: u32 = 10_000;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_task = Arc::clone(&stop);
    rt.spawn(async move {
        let ctx = SessionContext::new();
        let mut tick = 0u32;
        let mut start_pk = 0i64;
        while !stop_task.load(std::sync::atomic::Ordering::Relaxed) && tick < MAX_APPEND_TICKS {
            let batch = fact_batch(&schema, start_pk, APPEND_ROWS, 0);
            start_pk = (start_pk + APPEND_ROWS) % (OLINE_ROWS / 2);
            let stream_schema = batch.schema();
            let stream = datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(
                stream_schema,
                futures::stream::iter([Ok::<_, datafusion_common::DataFusionError>(batch)]),
            );
            table
                .write_cdc_append_stream(Box::pin(stream), &ctx.task_ctx())
                .await
                .expect("background value-identical upsert");
            tokio::time::sleep(CADENCE).await;
            tick += 1;
        }
    });
    stop
}

struct Lane {
    ctx: SessionContext,
    oline: Arc<CayenneTableProvider>,
    oline_schema: Arc<Schema>,
    expected_single: Vec<RecordBatch>,
    expected_semi: Vec<RecordBatch>,
    expected_triple: Vec<RecordBatch>,
}

async fn run_sql(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .expect("plan query")
        .collect()
        .await
        .expect("run query")
}

fn batches_equal(a: &[RecordBatch], b: &[RecordBatch]) -> bool {
    let fmt = |batches: &[RecordBatch]| {
        arrow::util::pretty::pretty_format_batches(batches)
            .expect("format batches")
            .to_string()
    };
    fmt(a) == fmt(b)
}

async fn setup_lane(state: TierState, expected: Option<&Lane>) -> (Lane, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let base = temp_dir.path().to_str().expect("utf8 temp path");
    let metadata_dir = format!("{base}/metadata");
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db")).expect("catalog"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog init");

    let oline_schema = fact_schema("ol_item_id", "ol_qty");
    let stock_schema = fact_schema("s_item_id", "s_qty");
    let oline = create_fact_table(
        &catalog,
        base,
        "oline_b",
        Arc::clone(&oline_schema),
        OLINE_ROWS,
    )
    .await;
    let stock = create_fact_table(
        &catalog,
        base,
        "stock_b",
        Arc::clone(&stock_schema),
        STOCK_ROWS,
    )
    .await;
    append_overlay(&oline, &oline_schema, state, 3 * ITEMS).await;
    append_overlay(&stock, &stock_schema, state, ITEMS).await;

    let ctx = SessionContext::new();
    let oline_handle = Arc::clone(&oline);
    ctx.register_table("oline_b", oline)
        .expect("register oline");
    ctx.register_table("stock_b", stock)
        .expect("register stock");
    let supp_schema = Arc::new(Schema::new(vec![Field::new(
        "su_key",
        DataType::Int64,
        false,
    )]));
    let supp_batch = RecordBatch::try_new(
        Arc::clone(&supp_schema),
        vec![Arc::new(Int64Array::from(
            (0..SUPP_ROWS).collect::<Vec<_>>(),
        ))],
    )
    .expect("supplier batch");
    ctx.register_table(
        "supp_b",
        Arc::new(MemTable::try_new(supp_schema, vec![vec![supp_batch]]).expect("supp table")),
    )
    .expect("register supp");

    let lane = Lane {
        oline: oline_handle,
        oline_schema,
        expected_single: run_sql(&ctx, SINGLE_SCAN_SQL).await,
        expected_semi: run_sql(&ctx, SEMI_JOIN_SQL).await,
        expected_triple: run_sql(&ctx, TRIPLE_SCAN_SQL).await,
        ctx,
    };
    // Every overlay state must produce IDENTICAL results to file_only — the
    // overlay rows are out-of-domain or value-identical by construction. This
    // is the correctness gate that makes the latency comparison meaningful.
    if let Some(reference) = expected {
        assert!(
            batches_equal(&lane.expected_single, &reference.expected_single),
            "single-scan result diverged in state {} — overlay construction broken",
            state.label()
        );
        assert!(
            batches_equal(&lane.expected_semi, &reference.expected_semi),
            "semi-join result diverged in state {} — overlay construction broken",
            state.label()
        );
        assert!(
            batches_equal(&lane.expected_triple, &reference.expected_triple),
            "triple-scan result diverged in state {} — overlay construction broken",
            state.label()
        );
    }
    (lane, temp_dir)
}

fn bench_mem_tier_join_shapes(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let (file_lane, _g1) = rt.block_on(setup_lane(TierState::FileOnly, None));
    let (fresh_lane, _g2) = rt.block_on(setup_lane(TierState::OverlayFresh, Some(&file_lane)));
    let (upsert_lane, _g3) = rt.block_on(setup_lane(TierState::OverlayUpsert, Some(&file_lane)));

    // One EXPLAIN per state for the semi-join shape (plan-diff inspection;
    // outside the timed region).
    for (state, lane) in [
        (TierState::FileOnly, &file_lane),
        (TierState::OverlayFresh, &fresh_lane),
        (TierState::OverlayUpsert, &upsert_lane),
    ] {
        let plan = rt.block_on(run_sql(&lane.ctx, &format!("EXPLAIN {SEMI_JOIN_SQL}")));
        eprintln!(
            "===== semi_join physical plan [{}] =====\n{}",
            state.label(),
            arrow::util::pretty::pretty_format_batches(&plan).expect("format explain")
        );
    }

    let mut group = c.benchmark_group("mem_tier_join_shapes");
    group.sample_size(10);
    for (shape, sql) in [
        ("single_scan", SINGLE_SCAN_SQL),
        ("semi_join", SEMI_JOIN_SQL),
        ("triple_scan", TRIPLE_SCAN_SQL),
    ] {
        for (state, lane) in [
            (TierState::FileOnly, &file_lane),
            (TierState::OverlayFresh, &fresh_lane),
            (TierState::OverlayUpsert, &upsert_lane),
        ] {
            let expected = match shape {
                "single_scan" => &lane.expected_single,
                "semi_join" => &lane.expected_semi,
                _ => &lane.expected_triple,
            };
            group.bench_with_input(
                BenchmarkId::new(shape, state.label()),
                &sql,
                |bencher, &sql| {
                    bencher.to_async(&rt).iter(|| async {
                        let batches = run_sql(&lane.ctx, sql).await;
                        assert!(
                            batches_equal(&batches, expected),
                            "{shape}/{} returned a different result mid-bench",
                            state.label()
                        );
                        black_box(batches);
                    });
                },
            );
        }
    }
    group.finish();

    // ---- live-load lanes: the SAME queries while value-identical upserts
    // stream into oline_b (tier-version churn at live-CDC cadence). Results
    // remain byte-identical, so the correctness gate still applies. This is
    // the in-process probe for the LOAD-COUPLED component of the SF-100
    // slow-set (q20 322ms -> 65s reproduced nowhere at rest).
    let stop = spawn_append_load(
        &rt,
        Arc::clone(&upsert_lane.oline),
        Arc::clone(&upsert_lane.oline_schema),
    );
    let mut live = c.benchmark_group("mem_tier_join_shapes_under_appends");
    live.sample_size(10);
    for (shape, sql) in [
        ("single_scan", SINGLE_SCAN_SQL),
        ("semi_join", SEMI_JOIN_SQL),
    ] {
        let expected = match shape {
            "single_scan" => &upsert_lane.expected_single,
            _ => &upsert_lane.expected_semi,
        };
        live.bench_with_input(
            BenchmarkId::new(shape, "overlay_upsert_live"),
            &sql,
            |bencher, &sql| {
                bencher.to_async(&rt).iter(|| async {
                    let batches = run_sql(&upsert_lane.ctx, sql).await;
                    assert!(
                        batches_equal(&batches, expected),
                        "{shape}/overlay_upsert_live returned a different result mid-bench"
                    );
                    black_box(batches);
                });
            },
        );
    }
    live.finish();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
}

criterion_group!(benches, bench_mem_tier_join_shapes);
criterion_main!(benches);
