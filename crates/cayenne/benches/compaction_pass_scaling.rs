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

//! What a Cayenne maintenance pass costs, as a function of the work it is given —
//! measured on the real CH-benCHmark table shapes.
//!
//! An SF1000 run answers this slowly (passes serialize at ~3h each) and with ~28%
//! run-to-run variance on end-to-end throughput, so it cannot resolve anything
//! under about 2x. These are per-pass cost curves, which is a microbench question.
//!
//! # The cost model
//!
//! Pass cost is not one number per byte. Three terms behave differently, and the
//! SF1000 tables sit at opposite ends of them (~0.096 MB per pass for
//! `warehouse`, ~2,424 MB for `stock` — a 25,000x spread from one count-based
//! trigger):
//!
//! ```text
//! t  ~=  fixed_per_pass  +  per_input x inputs  +  per_byte x bytes
//! ```
//!
//! An earlier sweep varied only the input COUNT at a fixed snapshot size, which
//! cannot separate the second and third terms — and at small sizes they diverge
//! sharply: 16 inputs of 12 KiB cost 1.7x what 2 inputs of the same total bytes
//! cost, so the marginal cost there is per-INPUT, not per-byte. The `pass_cost`
//! lane therefore varies snapshot size AND count independently.
//!
//! # Why real schemas
//!
//! Vortex runs a per-column encoding-strategy search and writes per-column
//! metadata, so per-COLUMN cost is exactly the fixed overhead that dominates a
//! small pass. A 3-column synthetic stand-in understates a 17-column `stock`
//! snapshot of the same byte size. Shapes here match the benchmark's own DDL.
//!
//! # Timing method
//!
//! The timed operation is `flush_pending_maintenance`, not the compaction entry
//! point directly. `compaction_trigger_protected_snapshots` gates BOTH the
//! automatic post-write scheduler and the explicit merge's `min_runs`, and it is
//! static config — so it cannot be raised during accumulation and lowered before
//! timing. Timing the explicit call therefore races the automatic pass, which
//! wins whenever accumulation outlasts the 100 ms post-write debounce; at
//! `stock` sizes a single write does, and the timed call then finds nothing to
//! merge. Draining is deterministic instead, and it is also the honest unit for
//! the trigger question: what one trigger firing costs. Every lane asserts the
//! on-disk file count strictly dropped inside the timed region, so a lane can
//! never silently time a no-op.

#![allow(clippy::expect_used)]
// Deterministic data generation converts freely between row indices and column
// types; the widths are fixed and small, so the cast lints add noise here.
#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    dead_code
)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use std::hint::black_box;

// ---- CH-benCHmark table shapes, at their real widths ----

/// A table modelled at its real CH-benCHmark width, because per-COLUMN cost is
/// exactly the kind of fixed overhead that dominates a small pass: Vortex runs a
/// per-column strategy search and writes per-column metadata, so a 17-column
/// `stock` snapshot and a 3-column synthetic one of the same byte size do not
/// cost the same. A synthetic schema understates the narrow tables least and the
/// wide ones most.
#[derive(Clone, Copy)]
struct TableShape {
    /// Table name, matching the benchmark's own.
    name: &'static str,
    /// Column count, for the report line.
    columns: usize,
    schema: fn() -> SchemaRef,
    /// Deterministic rows. `id_base` shifts the primary key so successive
    /// snapshots are disjoint; repeating a base supersedes instead.
    batch: fn(i64, usize) -> RecordBatch,
    /// Primary key column.
    pk: &'static str,
}

fn utf8_of(rows: usize, width: usize, seed: usize) -> StringArray {
    // Deterministic, mixed-entropy: a low-cardinality prefix that compresses and
    // a hashed tail that does not, so encoding choices actually matter.
    (0..rows)
        .map(|i| {
            let mut h = (i + seed) as u64;
            h ^= h >> 33;
            h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
            h ^= h >> 33;
            let mut s = format!("{:03}_{h:016x}", i % 32);
            s.truncate(width.max(4));
            s
        })
        .collect::<Vec<_>>()
        .into()
}

fn i32_of(rows: usize, base: i64, stride: i64) -> Int32Array {
    (0..rows as i64)
        .map(|i| i32::try_from((base + i * stride) % i64::from(i32::MAX)).unwrap_or(0))
        .collect::<Vec<_>>()
        .into()
}

fn f64_of(rows: usize, base: i64) -> Float64Array {
    (0..rows as i64)
        .map(|i| (base + i) as f64 * 0.01)
        .collect::<Vec<_>>()
        .into()
}

/// `warehouse`: 9 columns, 6 of them short strings. The benchmark's smallest
/// table (1,000 rows at SF1000) and its most over-compacted — ~97 KB per pass
/// across 26 passes per run.
fn warehouse_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("w_id", DataType::Int32, false),
        Field::new("w_name", DataType::Utf8, false),
        Field::new("w_street_1", DataType::Utf8, false),
        Field::new("w_street_2", DataType::Utf8, false),
        Field::new("w_city", DataType::Utf8, false),
        Field::new("w_state", DataType::Utf8, false),
        Field::new("w_zip", DataType::Utf8, false),
        Field::new("w_tax", DataType::Float64, false),
        Field::new("w_ytd", DataType::Float64, false),
    ]))
}

fn warehouse_batch(id_base: i64, rows: usize) -> RecordBatch {
    RecordBatch::try_new(
        warehouse_schema(),
        vec![
            Arc::new(i32_of(rows, id_base, 1)),
            Arc::new(utf8_of(rows, 10, 1)),
            Arc::new(utf8_of(rows, 20, 2)),
            Arc::new(utf8_of(rows, 20, 3)),
            Arc::new(utf8_of(rows, 20, 4)),
            Arc::new(utf8_of(rows, 2, 5)),
            Arc::new(utf8_of(rows, 9, 6)),
            Arc::new(f64_of(rows, id_base)),
            Arc::new(f64_of(rows, id_base + 7)),
        ],
    )
    .expect("warehouse batch")
}

/// `order_line`: 10 columns, one CHAR(24). The benchmark's highest-volume table
/// (~337M rows) and one of the heaviest compactors — ~804 MB per pass.
fn order_line_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ol_o_id", DataType::Int32, false),
        Field::new("ol_d_id", DataType::Int32, false),
        Field::new("ol_w_id", DataType::Int32, false),
        Field::new("ol_number", DataType::Int32, false),
        Field::new("ol_i_id", DataType::Int32, false),
        Field::new("ol_supply_w_id", DataType::Int32, false),
        Field::new("ol_quantity", DataType::Int32, false),
        Field::new("ol_amount", DataType::Float64, false),
        Field::new("ol_dist_info", DataType::Utf8, false),
        Field::new("ol_delivery_d", DataType::Int64, true),
    ]))
}

fn order_line_batch(id_base: i64, rows: usize) -> RecordBatch {
    RecordBatch::try_new(
        order_line_schema(),
        vec![
            Arc::new(i32_of(rows, id_base, 1)),
            Arc::new(i32_of(rows, 1, 0)),
            Arc::new(i32_of(rows, 1, 0)),
            Arc::new(i32_of(rows, 1, 1)),
            Arc::new(i32_of(rows, id_base, 3)),
            Arc::new(i32_of(rows, 1, 0)),
            Arc::new(i32_of(rows, 5, 0)),
            Arc::new(f64_of(rows, id_base)),
            Arc::new(utf8_of(rows, 24, 11)),
            Arc::new(Int64Array::from(
                (0..rows as i64).map(|i| id_base + i).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("order_line batch")
}

/// `stock`: 17 columns, ELEVEN of them strings (ten CHAR(24) plus a VARCHAR(50)).
/// The benchmark's heaviest compactor — ~2.4 GB per pass, 31-38 passes per run,
/// a ~78% duty cycle. If wide-table per-column cost matters anywhere, here.
fn stock_schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("s_i_id", DataType::Int32, false),
        Field::new("s_w_id", DataType::Int32, false),
        Field::new("s_quantity", DataType::Int32, false),
    ];
    for d in 1..=10 {
        fields.push(Field::new(format!("s_dist_{d:02}"), DataType::Utf8, false));
    }
    fields.push(Field::new("s_ytd", DataType::Int32, false));
    fields.push(Field::new("s_order_cnt", DataType::Int32, false));
    fields.push(Field::new("s_remote_cnt", DataType::Int32, false));
    fields.push(Field::new("s_data", DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}

fn stock_batch(id_base: i64, rows: usize) -> RecordBatch {
    let mut cols: Vec<arrow::array::ArrayRef> = vec![
        Arc::new(i32_of(rows, id_base, 1)),
        Arc::new(i32_of(rows, 1, 0)),
        Arc::new(i32_of(rows, 50, 1)),
    ];
    for d in 1..=10 {
        cols.push(Arc::new(utf8_of(rows, 24, 20 + d)));
    }
    cols.push(Arc::new(i32_of(rows, id_base, 2)));
    cols.push(Arc::new(i32_of(rows, 3, 1)));
    cols.push(Arc::new(i32_of(rows, 1, 1)));
    cols.push(Arc::new(utf8_of(rows, 50, 40)));
    RecordBatch::try_new(stock_schema(), cols).expect("stock batch")
}

const SHAPES: [TableShape; 3] = [
    TableShape {
        name: "warehouse",
        columns: 9,
        schema: warehouse_schema,
        batch: warehouse_batch,
        pk: "w_id",
    },
    TableShape {
        name: "order_line",
        columns: 10,
        schema: order_line_schema,
        batch: order_line_batch,
        pk: "ol_o_id",
    },
    TableShape {
        name: "stock",
        columns: 17,
        schema: stock_schema,
        batch: stock_batch,
        pk: "s_i_id",
    },
];

// ---- sweep configuration ----

/// Row ladders per shape, chosen so each spans roughly three orders of magnitude
/// of snapshot size. Capped by `CAYENNE_BENCH_MAX_ROWS` so the default run stays
/// quick; raise it to reach the `stock`-sized end.
fn row_ladder(shape: &TableShape) -> Vec<usize> {
    let cap = std::env::var("CAYENNE_BENCH_MAX_ROWS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(100_000);
    let ladder = match shape.name {
        "warehouse" => vec![200, 2_000, 20_000],
        "stock" => vec![10_000, 100_000, 500_000],
        _ => vec![10_000, 100_000, 1_000_000],
    };
    ladder.into_iter().filter(|rows| *rows <= cap).collect()
}

/// Input counts. Varied INDEPENDENTLY of size so the per-input and per-byte
/// terms can be separated.
const INPUT_COUNTS: [usize; 2] = [2, 8];
/// Encode fan-out widths. 1 is the serial writer; 64 a typical core count.
const ENCODE_FANOUT: [usize; 4] = [1, 4, 16, 64];

/// Shapes to run; defaults to all three.
fn selected_shapes() -> Vec<TableShape> {
    let want = std::env::var("CAYENNE_BENCH_TABLES").unwrap_or_default();
    let want = want.trim().to_string();
    if want.is_empty() || want == "all" {
        return SHAPES.to_vec();
    }
    let picked: Vec<_> = SHAPES
        .iter()
        .copied()
        .filter(|s| want.split(',').any(|w| w.trim() == s.name))
        .collect();
    assert!(
        !picked.is_empty(),
        "CAYENNE_BENCH_TABLES={want:?} matched none of {:?}",
        SHAPES.map(|s| s.name)
    );
    picked
}

fn logical_bytes(shape: &TableShape, inputs: usize, rows: usize) -> u64 {
    let per = (shape.batch)(0, rows).get_array_memory_size() as u64;
    per.saturating_mul(inputs as u64)
}

// ---- fixture ----

struct Fixture {
    _temp_dir: tempfile::TempDir,
    catalog: Arc<dyn MetadataCatalog>,
    provider: CayenneTableProvider,
    data_path: std::path::PathBuf,
}

/// Total `.vortex` files under a data dir. A merge strictly reduces this, which
/// is how each lane proves it timed real work.
fn vortex_files(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut files = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files += vortex_files(&path);
        } else if path.extension().is_some_and(|ext| ext == "vortex") {
            files += 1;
        }
    }
    files
}

/// An upsert table with the inline memtable disabled, so every insert lands in a
/// file-backed protected snapshot rather than being absorbed inline, and with the
/// background compactor pinned far out so only the drain under test runs.
async fn setup_table(
    shape: &TableShape,
    table_name: &str,
    inputs: usize,
    write_concurrency: Option<usize>,
    runtime_env: Arc<RuntimeEnv>,
) -> Fixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("create data dir");
    let db_path = temp_dir.path().join("catalog.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy())).expect("catalog"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog init");

    let provider = CayenneTableProvider::create_table(
        Arc::clone(&catalog),
        CreateTableOptions {
            table_name: table_name.to_string(),
            schema: (shape.schema)(),
            primary_key: vec![shape.pk.to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                shape.pk.to_string(),
            ]))),
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                // The drain must find a qualifying merge, so the trigger cannot
                // exceed the inputs this lane writes.
                compaction_trigger_protected_snapshots: inputs,
                // Otherwise the small-file gate schedules a pass after 8 files
                // regardless of the protected count.
                compaction_trigger_files: usize::MAX,
                // 0 disables the age trigger, which would fire independently.
                compaction_trigger_snapshot_age_ms: 0,
                compaction_background_interval_ms: 3_600_000,
                write_concurrency,
                ..VortexConfig::default()
            },
        },
        runtime_env,
    )
    .await
    .expect("create table");

    Fixture {
        _temp_dir: temp_dir,
        catalog,
        provider,
        data_path,
    }
}

async fn write_snapshot(fixture: &Fixture, batch: RecordBatch) -> u64 {
    use datafusion::datasource::TableProvider;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::collect;

    let ctx = SessionContext::new();
    let schema = batch.schema();
    let input =
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory source");
    let plan = fixture
        .provider
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await
        .expect("insert plan");
    let results = collect(plan, ctx.task_ctx()).await.expect("insert");
    results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .map(|c| c.value(0))
        })
        .unwrap_or(0)
}

/// A table carrying `inputs` protected snapshots. `supersede` repeats the same
/// key range so each snapshot tombstones the last (the update-heavy shape the
/// bake folds); otherwise the ranges are disjoint.
async fn accumulate(
    shape: &TableShape,
    table: &str,
    inputs: usize,
    rows: usize,
    supersede: bool,
    write_concurrency: Option<usize>,
    runtime_env: Arc<RuntimeEnv>,
) -> Fixture {
    let fixture = setup_table(shape, table, inputs, write_concurrency, runtime_env).await;
    for s in 0..inputs {
        let base = if supersede { 0 } else { (s * rows) as i64 };
        let written = write_snapshot(&fixture, (shape.batch)(base, rows)).await;
        assert_eq!(written as usize, rows, "{} snapshot {s} row count", shape.name);
    }
    fixture
}

/// Time one explicit compaction pass, asserting it merged.
///
/// Race-free because `begin_compaction_shutdown` (see `bench_maintenance_cost`)
/// refuses every AUTOMATIC pass for the whole bench, while this entry point
/// bypasses that gate — `try_track_compaction_pass` guards only
/// `run_compaction_trigger`. So accumulation can take as long as it likes without
/// the post-write scheduler merging the snapshots first, and the assertion below
/// is meaningful rather than a coin flip.
fn timed_pass(runtime: &tokio::runtime::Runtime, fixture: &Fixture, label: &str) {
    let merged = runtime.block_on(
        fixture
            .provider
            .compact_protected_snapshots_subset(usize::MAX),
    );
    assert!(merged.is_ok(), "{label}: pass must not error, got {merged:?}");
    if merged.as_ref().is_ok_and(|merged| !merged) {
        // Loud: a declined pass times the DECLINE path, so its cell must be
        // excluded from any fit rather than averaged in with real merges. Which
        // cells decline is itself data — the tier selector can refuse a set that
        // is over the trigger floor.
        eprintln!("  DECLINED {label}: no merge, this cell times the decline path");
    }
    black_box(&fixture.catalog);
}

// ---- lanes ----

fn bench_maintenance_cost(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let ctx = SessionContext::new();
    let env = ctx.runtime_env();
    let shapes = selected_shapes();
    let mut lane = 0u64;

    // Refuse every automatic trigger pass for the duration. Accumulating a
    // `stock`-sized snapshot takes far longer than the 100 ms post-write
    // debounce, so without this the post-write scheduler merges the inputs
    // before the timed call reaches them and the lane times a no-op. The
    // explicit entry points below are not gated by this flag.
    cayenne::begin_compaction_shutdown();

    // `RUST_LOG=cayenne::compaction=debug` surfaces WHY a pass declined. Every
    // decline branch logs at trace/debug, and from here that is the only way to
    // tell a tier-quorum refusal apart from a budget one.
    if std::env::var_os("RUST_LOG").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    }

    for shape in &shapes {
        for rows in row_ladder(shape) {
            eprintln!(
                "{} ({} cols): {rows} rows = {:.4} MiB Arrow/snapshot",
                shape.name,
                shape.columns,
                logical_bytes(shape, 1, rows) as f64 / (1024.0 * 1024.0)
            );
        }
    }
    eprintln!();

    // --- Lane 1: size x count, so per-input and per-byte cost separate. ---
    let mut group = c.benchmark_group("pass_cost");
    group.sample_size(10);
    for shape in &shapes {
        for rows in row_ladder(shape) {
            for inputs in INPUT_COUNTS {
                group.throughput(Throughput::Bytes(logical_bytes(shape, inputs, rows)));
                group.bench_function(format!("{}/{rows}rows_x{inputs}", shape.name), |b| {
                    b.iter_batched(
                        || {
                            lane += 1;
                            runtime.block_on(accumulate(
                                shape,
                                &format!("pc_{lane}"),
                                inputs,
                                rows,
                                false,
                                None,
                                Arc::clone(&env),
                            ))
                        },
                        |fixture| timed_pass(&runtime, &fixture, "pass_cost"),
                        BatchSize::PerIteration,
                    );
                });
            }
        }
    }
    group.finish();

    // --- Lane 2: encode width, at the largest affordable size per shape. ---
    let mut group = c.benchmark_group("encode_fanout");
    group.sample_size(10);
    for shape in &shapes {
        let Some(rows) = row_ladder(shape).into_iter().last() else {
            continue;
        };
        group.throughput(Throughput::Bytes(logical_bytes(shape, 8, rows)));
        for width in ENCODE_FANOUT {
            group.bench_function(format!("{}/{rows}rows/wc{width}", shape.name), |b| {
                b.iter_batched(
                    || {
                        lane += 1;
                        runtime.block_on(accumulate(
                            shape,
                            &format!("fo_{lane}"),
                            8,
                            rows,
                            false,
                            Some(width),
                            Arc::clone(&env),
                        ))
                    },
                    |fixture| timed_pass(&runtime, &fixture, "encode_fanout"),
                    BatchSize::PerIteration,
                );
            });
        }
    }
    group.finish();

    // --- Lane 3: the bake, over a superseded protected set. ---
    let mut group = c.benchmark_group("bake_seq_prefix");
    group.sample_size(10);
    for shape in &shapes {
        let Some(rows) = row_ladder(shape).into_iter().last() else {
            continue;
        };
        group.throughput(Throughput::Bytes(logical_bytes(shape, 8, rows)));
        group.bench_function(format!("{}/{rows}rows_x8_superseded", shape.name), |b| {
            b.iter_batched(
                || {
                    lane += 1;
                    runtime.block_on(accumulate(
                        shape,
                        &format!("bk_{lane}"),
                        8,
                        rows,
                        true,
                        None,
                        Arc::clone(&env),
                    ))
                },
                |fixture| {
                    let baked =
                        runtime.block_on(fixture.provider.bake_seq_prefix_protected_snapshots());
                    assert!(baked.is_ok(), "bake must not error, got {baked:?}");
                    let _ = black_box(baked);
                    black_box(&fixture.catalog);
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();

    // --- Lane 4: the N -> 1 -> M funnel.
    //
    // The rewrite scans N partitions, `execute_stream` inserts a
    // `CoalescePartitionsExec` to merge them into ONE stream, and the Vortex sink
    // then re-shards that stream across M writers. Consuming the partitions
    // directly would skip the funnel; whether that is worth an API change to
    // `write_to_snapshot` depends on what the funnel actually costs, which is
    // this lane. Both arms drain the same plan, so the delta is the coalesce plus
    // the serialisation it imposes. ---
    let mut group = c.benchmark_group("stream_shape");
    group.sample_size(10);
    for shape in &shapes {
        let Some(rows) = row_ladder(shape).into_iter().last() else {
            continue;
        };
        group.throughput(Throughput::Bytes(logical_bytes(shape, 8, rows)));
        for coalesced in [true, false] {
            let arm = if coalesced { "coalesced" } else { "partitioned" };
            group.bench_function(format!("{}/{rows}rows/{arm}", shape.name), |b| {
                b.iter_batched(
                    || {
                        lane += 1;
                        runtime.block_on(accumulate(
                            shape,
                            &format!("ss_{lane}"),
                            8,
                            rows,
                            false,
                            None,
                            Arc::clone(&env),
                        ))
                    },
                    |fixture| {
                        let rows_read = runtime.block_on(drain_scan(&fixture, coalesced));
                        assert!(rows_read > 0, "{arm}: scan must read rows");
                        black_box(rows_read);
                    },
                    BatchSize::PerIteration,
                );
            });
        }
    }
    group.finish();

    cayenne::reset_compaction_shutdown();
}

/// Read the whole table either through the coalescing funnel that the rewrite
/// uses, or by draining every scan partition concurrently. Returns rows read so
/// both arms are checked to have done the same work.
async fn drain_scan(fixture: &Fixture, coalesced: bool) -> usize {
    use datafusion::datasource::TableProvider;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use futures::StreamExt;

    let ctx = SessionContext::new();
    let state = ctx.state();
    let plan = fixture
        .provider
        .scan(&state, None, &[], None)
        .await
        .expect("scan plan");
    let task_ctx = state.task_ctx();

    if coalesced {
        let mut stream = datafusion_physical_plan::execute_stream(Arc::clone(&plan), task_ctx)
            .expect("coalesced stream");
        let mut rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch.expect("batch").num_rows();
        }
        return rows;
    }

    let partitions = plan.output_partitioning().partition_count();
    let mut tasks = Vec::with_capacity(partitions);
    for partition in 0..partitions {
        let plan = Arc::clone(&plan);
        let task_ctx = Arc::clone(&task_ctx);
        tasks.push(tokio::spawn(async move {
            let mut stream = plan.execute(partition, task_ctx).expect("partition stream");
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                rows += batch.expect("batch").num_rows();
            }
            rows
        }));
    }
    let mut rows = 0;
    for task in tasks {
        rows += task.await.expect("partition task");
    }
    rows
}

criterion_group!(benches, bench_maintenance_cost);
criterion_main!(benches);
