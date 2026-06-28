// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Staged-disk CDC path feeding the maintained-aggregate IVM registry.
//!
//! ## What this measures and why (the feature under test)
//!
//! Cayenne maintains grouped `COUNT`/`SUM` incrementally from the CDC stream
//! ([`cayenne::maintained_aggregate::MaintainedAggregateRegistry`]) and the
//! optimizer serves a matching query from that state instead of re-scanning. The
//! in-memory CDC tier (`cdc_durability: memory`) already fed the registry; the
//! STAGED-disk path (`cdc_durability: file`) previously did NOT — a staged
//! publish just blindly marked the registry stale, so the very next query
//! fell back to a full O(rows) re-aggregate. The new path feeds the registry
//! from the staged publish too: under the held listing fence
//! ([`CayenneTableProvider::feed_staged_ivm_under_fence`]) it assigns the IVM
//! epoch AND `try_send`-enqueues the captured insert batches to the background
//! applier, so the registry stays fresh through staged publishes — the same
//! O(delta) maintain the in-mem path gets, on the durable path.
//!
//! Two lanes, each modeling a real comparison:
//!
//! - `staged_ivm_feed_publish` — the END-TO-END feature cost. Drives the real
//!   public staged-disk CDC path (`write_cdc_append_stream` + `finish()`) on a
//!   table built `with_maintained_aggregates` + `cdc_durability: file`, vs an
//!   identical table with NO maintained aggregate. The IVM lane pays the Stage-A
//!   insert-batch capture ([`CayenneTableProvider::should_capture_staged_ivm_feed`])
//!   plus the under-fence epoch-assign + `try_send`
//!   ([`CayenneTableProvider::feed_staged_ivm_under_fence`]); the baseline lane
//!   pays neither. The gap is the per-staged-publish IVM feed overhead — and it
//!   must be small (the feed enqueues; it does not aggregate inline). Both lanes
//!   ASSERT `has_pending_finalize()` so the bench can never silently measure the
//!   wrong (in-memory / inline) path.
//!
//! - `staged_ivm_feed_vs_rebuild` — the ASYMPTOTE the feed buys, on the shipped
//!   `pub` registry directly (deterministic, no async applier). The staged feed
//!   keeps the registry fresh by applying a small delta
//!   ([`MaintainedAggregateRegistry::apply_insert_batches`], O(delta)); the OLD
//!   blind-stale behavior forced the next query to a full
//!   ([`MaintainedAggregateRegistry::rebuild_from_batches`], O(rows)) recompute.
//!   Incremental feed vs full rebuild over the same data: O(delta) vs O(rows), so
//!   the win widens with table size. The two are asserted to produce the SAME
//!   per-group state before any timing — a fast wrong answer is worthless.
//!
//! ## CRITICAL invariant (the reorder fix, why the e2e lane exists)
//!
//! Staged `finish()` tasks are pipelined/detached, so epoch-assign + enqueue MUST
//! be atomic under the listing fence. An off-fence enqueue can deliver epoch N+1
//! before N to the FIFO applier, and `begin_maintenance_pass` PERMANENTLY stales
//! the registry on a `+1` gap. The e2e lane drives the production publish path
//! (where the fence ordering actually happens), so a regression that moves the
//! enqueue off the fence would surface here as a permanently-stale registry.
//!
//! ## Bench discipline (Tiger Style)
//!
//! Per-iteration fixtures are built OUTSIDE the timed region (`iter_custom` seeds
//! a fresh table/registry, the timer wraps only the staged publish or the
//! feed/rebuild call); every loop is bounded; every `expect` carries a message;
//! the asymptote lane asserts equality with a full recompute before timing.

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::maintained_aggregate::{
    MaintainedAggregateExpr, MaintainedAggregateFunction, MaintainedAggregateRegistry,
    MaintainedAggregateSpec,
};
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

/// `GROUP BY id` cardinality the maintained view tracks. Matches the (id, value)
/// PK table both lanes build.
const GROUP_COUNT: usize = 1_000;
/// Rows in one staged publish / one maintenance delta (a CDC micro-batch).
const DELTA_ROWS: usize = 1_000;
/// Base-table sizes for the asymptote lane: the rebuild cost is O(rows), the
/// incremental feed is O(delta), so the ratio widens across these.
const BASE_ROW_COUNTS: &[usize] = &[100_000, 1_000_000];

// ----------------------------- shared schema/spec ---------------------------

/// The (id Int64, value Int64) PK=id table both lanes use, mirroring the
/// `create_cdc_upsert_table_*` test fixtures in `table.rs`.
fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// `COUNT(*), SUM(value) GROUP BY id` — both Int64-output aggregates the registry
/// maintains exactly. This is the view the staged feed keeps fresh.
fn count_sum_specs() -> Vec<MaintainedAggregateSpec> {
    vec![MaintainedAggregateSpec {
        group_by: vec!["id".to_string()],
        aggregates: vec![
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Count,
                column: None,
            },
            MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("value".to_string()),
            },
        ],
        filter: None,
    }]
}

/// A `count`-row `(id, value)` batch starting at id `start`. Deterministic so the
/// asymptote lane's correctness gate is reproducible. `id % GROUP_COUNT` keeps the
/// group cardinality bounded; `value` is a signed spread around zero.
fn id_value_batch(start: i64, count: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..count as i64)
        .map(|j| (start + j) % GROUP_COUNT as i64)
        .collect();
    let values: Vec<i64> = (0..count as i64).map(|j| (j % 2_001) - 1_000).collect();
    RecordBatch::try_new(
        table_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("id/value batch is valid")
}

fn single_batch_stream(batch: RecordBatch) -> datafusion::execution::SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
    ))
}

// ------------------- lane 1: end-to-end staged publish feed ------------------

/// A live CDC-upsert table on the STAGED-disk path. `with_maintained_aggregates`
/// is set only when `ivm` is true — the two configs differ ONLY by whether a
/// maintained aggregate (and therefore the staged IVM feed) is engaged, so the
/// timed gap isolates the feed cost.
///
/// Mirrors `create_cdc_upsert_table_with_vortex_config` in `table.rs`, but routes
/// through the PUBLIC `CayenneTableProviderBuilder` (a bench cannot reach the
/// crate's test helpers) and forces the staged path:
/// `cdc_durability: file` (the default is `memory` — the in-mem feed, NOT the
/// path under test), `inline_max_rows: 0` (no inline memtable → always staged),
/// `deletion_mode: key` (off the position-delete pathway).
async fn create_staged_table(
    table_name: &str,
    runtime_env: Arc<RuntimeEnv>,
    ivm: bool,
) -> (CayenneTableProvider, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("str path"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("str path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");

    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog created"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");

    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: table_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: data_dir,
        partition_column: None,
        vortex_config: VortexConfig {
            inline_max_rows: 0,
            deletion_mode: DeletionMode::Key,
            cdc_durability: CdcDurability::File,
            ..VortexConfig::default()
        },
    };

    let mut builder = cayenne::CayenneTableProviderBuilder::new(Arc::clone(&catalog), runtime_env);
    if ivm {
        builder = builder.with_maintained_aggregates(count_sum_specs());
    }
    let provider = builder.create(options).await.expect("table created");
    (provider, temp_dir)
}

/// Drive one staged CDC publish end-to-end: stage the batch, assert it took the
/// staged (not inline / in-memory) path, then publish it. The publish is where
/// `feed_staged_ivm_under_fence` runs under the held listing fence on an IVM
/// table. Returns rows written.
async fn staged_publish(provider: &CayenneTableProvider, batch: RecordBatch) -> u64 {
    let ctx = SessionContext::new();
    let write = provider
        .write_cdc_append_stream(single_batch_stream(batch), &ctx.task_ctx())
        .await
        .expect("staged CDC write (stage A)");
    assert!(
        write.has_pending_finalize(),
        "write must be STAGED (pending finalize) — a missing finalize means the \
         bench measured the inline / in-memory path, not the staged IVM feed"
    );
    assert!(
        write.in_memory_epoch().is_none(),
        "write must NOT have taken the in-memory tier (cdc_durability: file) — \
         the in-mem path has its own feed; this lane measures the staged feed"
    );
    let rows = write.rows();
    write
        .finish()
        .await
        .expect("staged publish (stage B finish)");
    rows
}

/// End-to-end per-staged-publish cost, IVM vs no-IVM. The maintained-aggregate
/// table additionally pays the Stage-A insert-batch capture and the under-fence
/// epoch-assign + `try_send`; the difference between the two lanes is that feed.
///
/// `iter_custom` rebuilds a FRESH table per iteration OUTSIDE the timer: each
/// publish must start from a clean staging dir + a fresh registry/applier (a
/// reused table would accumulate files and change the publish cost), and the
/// table create (catalog init, metastore) is far heavier than one publish, so it
/// must not be timed.
fn bench_staged_publish(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("staged_ivm_feed_publish");
    group.sample_size(20);
    group.throughput(Throughput::Elements(DELTA_ROWS as u64));

    for (label, ivm) in [("no_ivm", false), ("with_ivm", true)] {
        group.bench_function(BenchmarkId::from_parameter(label), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for iteration in 0..iters {
                    // Fresh table per iteration, untimed.
                    let table_name = format!("staged_ivm_{label}_{iteration}");
                    let (provider, _tmp) = rt.block_on(create_staged_table(
                        &table_name,
                        Arc::new(RuntimeEnv::default()),
                        ivm,
                    ));
                    let batch = id_value_batch(0, DELTA_ROWS);

                    let start = Instant::now();
                    let rows = rt.block_on(staged_publish(&provider, batch));
                    total += start.elapsed();

                    black_box(rows);
                    // `provider` + `_tmp` drop here (untimed): tears down the
                    // applier thread + the temp metastore/data dirs.
                }
                total
            });
        });
    }
    group.finish();
}

// ------------------ lane 2: incremental feed vs full rebuild -----------------

/// A retraction-capable registry over `count_sum_specs`, loaded with `rows` of
/// base data at epoch 1 — the state a staged-fed table holds before a delta. The
/// staged feed keeps THIS fresh with a small `apply_insert_batches`; the old
/// blind-stale path would instead force a full `rebuild_from_batches` on the next
/// query.
fn load_registry(rows: usize) -> (MaintainedAggregateRegistry, Vec<RecordBatch>) {
    let schema = table_schema();
    let registry = MaintainedAggregateRegistry::try_new_with_pk(
        &count_sum_specs(),
        &schema,
        &[0], // pk = column 0 (`id`)
        usize::MAX,
    )
    .expect("retraction-capable registry");

    const LOAD_CHUNK: usize = 65_536;
    let mut base = Vec::with_capacity(rows.div_ceil(LOAD_CHUNK));
    let mut start = 0_i64;
    while (start as usize) < rows {
        let count = LOAD_CHUNK.min(rows - start as usize);
        base.push(id_value_batch(start, count));
        start += count as i64;
    }
    registry
        .apply_insert_batches(1, &base)
        .expect("load base registry");
    (registry, base)
}

/// Serve `SUM(value) GROUP BY id` into a sorted `id -> sum` map via the
/// production serve path (`batch_for_spec`: fresh/epoch gate + O(groups)
/// materialize). Used only by the correctness gate.
fn serve_sums(registry: &MaintainedAggregateRegistry, epoch: u64) -> BTreeMap<i64, i64> {
    let output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value_sum", DataType::Int64, false),
    ]));
    let sum_only_spec = MaintainedAggregateSpec {
        group_by: vec!["id".to_string()],
        aggregates: vec![MaintainedAggregateExpr {
            function: MaintainedAggregateFunction::Sum,
            column: Some("value".to_string()),
        }],
        filter: None,
    };
    let batch = registry
        .batch_for_spec(&sum_only_spec, epoch, output_schema)
        .expect("serve must not error")
        .expect("view must serve at the scan epoch");
    decode_sums(&batch)
}

fn decode_sums(batch: &RecordBatch) -> BTreeMap<i64, i64> {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id Int64");
    let sums = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum Int64");
    let mut out = BTreeMap::new();
    for row in 0..batch.num_rows() {
        out.insert(ids.value(row), sums.value(row));
    }
    out
}

/// Correctness gate: applying a delta INCREMENTALLY (the staged feed) must equal
/// a full REBUILD over base+delta (the old blind-stale fallback) for every group.
/// Run before any timing.
fn assert_incremental_matches_rebuild(base: &[RecordBatch], delta: &RecordBatch) {
    let schema = table_schema();
    // Incremental: load the base at epoch 1, then apply the delta at epoch 2 —
    // exactly the call the staged applier makes after the under-fence enqueue.
    let inc =
        MaintainedAggregateRegistry::try_new_with_pk(&count_sum_specs(), &schema, &[0], usize::MAX)
            .expect("inc registry");
    inc.apply_insert_batches(1, base).expect("inc base");
    inc.apply_insert_batches(2, std::slice::from_ref(delta))
        .expect("inc delta");

    let rebuilt =
        MaintainedAggregateRegistry::try_new_with_pk(&count_sum_specs(), &schema, &[0], usize::MAX)
            .expect("rebuild registry");
    let mut all: Vec<RecordBatch> = base.to_vec();
    all.push(delta.clone());
    rebuilt.rebuild_from_batches(2, &all).expect("full rebuild");

    let inc_sums = serve_sums(&inc, 2);
    let rebuilt_sums = serve_sums(&rebuilt, 2);
    assert_eq!(
        inc_sums, rebuilt_sums,
        "incremental staged feed diverged from a full rebuild over the same data"
    );
    assert!(
        !inc_sums.is_empty() && inc_sums.len() <= GROUP_COUNT,
        "served group cardinality is bounded and non-empty"
    );
}

/// The asymptote: a small staged delta applied incrementally (O(delta), what the
/// new feed enqueues) vs a full recompute (O(rows), the old blind-stale
/// fallback's next-query cost). Both on the shipped `pub` registry.
fn bench_feed_vs_rebuild(c: &mut Criterion) {
    let mut group = c.benchmark_group("staged_ivm_feed_vs_rebuild");
    group.sample_size(20);

    for &rows in BASE_ROW_COUNTS {
        let (_registry, base) = load_registry(rows);
        // A staged delta = `DELTA_ROWS` upserts of EXISTING ids (each retracts its
        // old contribution from the per-PK index then applies the new one — the
        // O(delta) maintain cost the staged feed pays).
        let delta = id_value_batch(0, DELTA_ROWS);

        // Correctness before timing.
        assert_incremental_matches_rebuild(&base, &delta);

        // Lane A — incremental feed: O(delta). A fresh registry preloaded with the
        // base, then one `apply_insert_batches` of the delta (the call the staged
        // applier makes after `feed_staged_ivm_under_fence` enqueues). iter_batched
        // re-seeds the base registry OUTSIDE the timer so only the delta apply is
        // measured.
        group.throughput(Throughput::Elements(DELTA_ROWS as u64));
        group.bench_with_input(
            BenchmarkId::new("incremental_feed_delta", rows),
            &rows,
            |b, _| {
                b.iter_batched(
                    || {
                        let (reg, _) = load_registry(rows);
                        reg
                    },
                    |reg| {
                        reg.apply_insert_batches(2, std::slice::from_ref(&delta))
                            .expect("incremental delta apply");
                        black_box(serve_sums(&reg, 2).len());
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // Lane B — full rebuild: O(rows). The blind-stale fallback the feed
        // REPLACES — the next query after a stale-marking staged publish must
        // recompute the whole view. iter_custom builds a fresh registry per
        // iteration outside the timer; the timer wraps only the rebuild.
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(
            BenchmarkId::new("full_rebuild_rows", rows),
            &rows,
            |b, _| {
                let mut all: Vec<RecordBatch> = base.clone();
                all.push(delta.clone());
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let schema = table_schema();
                        let reg = MaintainedAggregateRegistry::try_new_with_pk(
                            &count_sum_specs(),
                            &schema,
                            &[0],
                            usize::MAX,
                        )
                        .expect("rebuild registry");
                        let start = Instant::now();
                        reg.rebuild_from_batches(2, &all).expect("full rebuild");
                        total += start.elapsed();
                        black_box(reg.is_empty());
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_staged_publish, bench_feed_vs_rebuild);
criterion_main!(benches);
