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

//! A/B anchor for lever L2 — eager-subset mem-tier overlay maintenance.
//!
//! In `cdc_durability: memory`, every CDC append maintains an incrementally
//! filtered "visible overlay" of the in-RAM tier under `listing_fence.write()`.
//! For the `RowConverterBased` (composite-PK) strategy — every chbench heavy
//! table (stock/customer/order_line) — the old maintenance hardwired
//! `affected = true` for EVERY overlay entry whenever an append carried ANY
//! row-key tombstone, because there is no cheap range prune for opaque row
//! keys. So an upsert that conflicts with bulk-loaded *file* data re-filtered
//! the WHOLE accumulated overlay on every append: O(tier) work, under the
//! fence, that grows as the tier grows between checkpoints.
//!
//! Lever L2 (`build_mem_tombstones_inlined_only`) feeds overlay maintenance
//! ONLY the RAM-resident (`deleted_inlined_*`) tombstones. A file-resident
//! conflict — the dominant case, a hit against cold bulk data not re-touched
//! this checkpoint window — then carries an EMPTY overlay delta, so no entry is
//! re-filtered and maintenance is O(1).
//!
//! ## What this bench measures
//!
//! The wall time to apply `K` file-conflicting upsert slices to a composite-PK
//! memory-mode table whose RAM tier grows (no checkpoint between slices). Each
//! slice re-upserts a DISJOINT block of file-resident keys, so every conflict
//! stays file-resident (never re-hits a key already pulled into the RAM tier)
//! and the overlay grows by exactly one entry per slice.
//!
//! - **Old (full-union delta):** slice `i` re-filters the `i` prior overlay
//!   entries ⇒ total ≈ O(K²) (super-linear in K).
//! - **New (inlined-only delta, current code):** slice `i`'s delta is empty ⇒
//!   O(1) maintenance ⇒ total ≈ O(K) (linear in K).
//!
//! Run `cargo bench --bench mem_tier_overlay_append -p cayenne`. The A/B is the
//! `K`-scaling shape: the new code's per-slice cost is flat as K grows; the old
//! code's grows with K. Setup (bulk load + checkpoint to a file) is UNTIMED via
//! `iter_custom`; only the K conflicting appends are measured.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog, SlotAdvancer};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

/// Rows per slice (one CDC batch / one overlay entry).
const SLICE: i64 = 1_000;
/// Number of accumulated slices to apply (the tier depth axis). Kept modest so
/// the old code's O(K²) lane stays tractable; the linear-vs-quadratic SHAPE
/// across these three points is the A/B signal, not the absolute K.
const SLICE_COUNTS: &[i64] = &[16, 64, 128];

struct NoopAdvancer;
#[async_trait::async_trait]
impl SlotAdvancer for NoopAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

/// Composite-PK `(a, b, value)` schema → forces the `RowConverterBased`
/// strategy (the path L2 changes).
fn composite_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Block `i` is the `SLICE` keys `(a=i, b=0..SLICE)`; `value` is `generation`
/// so a re-upsert at a later generation supersedes the prior copy.
fn block_batch(schema: &Arc<Schema>, block: i64, generation: i64) -> RecordBatch {
    let a: Vec<i64> = (0..SLICE).map(|_| block).collect();
    let b: Vec<i64> = (0..SLICE).collect();
    let value: Vec<i64> = (0..SLICE).map(|_| generation).collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(a)),
            Arc::new(Int64Array::from(b)),
            Arc::new(Int64Array::from(value)),
        ],
    )
    .expect("block batch construction")
}

async fn upsert_block(
    table: &Arc<CayenneTableProvider>,
    schema: &Arc<Schema>,
    block: i64,
    generation: i64,
) {
    let ctx = SessionContext::new();
    let batch = block_batch(schema, block, generation);
    let stream = RecordBatchStreamAdapter::new(
        batch.schema(),
        futures::stream::iter([Ok::<_, datafusion_common::DataFusionError>(batch)]),
    );
    let write = table
        .write_cdc_append_stream(Box::pin(stream), &ctx.task_ctx())
        .await
        .expect("CDC upsert to RAM tier");
    assert!(
        write.in_memory_epoch().is_some(),
        "upsert must engage the RAM tier (memory mode armed) — a durable fallback \
         would mean the bench measures the wrong path"
    );
    // Memory-mode append publishes via the RAM tier swap; nothing to finalize.
    drop(write);
}

/// Build a fresh composite-PK memory-mode table holding `blocks * SLICE` keys
/// durably in a FILE (RAM tier empty, keyset all `FileUnlocated`). All tier
/// self-flush is disabled so the bench fully controls residency.
async fn fresh_file_resident_table(
    blocks: i64,
) -> (Arc<CayenneTableProvider>, Arc<Schema>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", dir.path().to_str().expect("path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db")).expect("catalog"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("init catalog");

    let schema = composite_schema();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog),
            CreateTableOptions {
                table_name: "overlay_append".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["a".to_string(), "b".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "a".to_string(),
                    "b".to_string(),
                ]))),
                base_path: format!("{}/data", dir.path().to_str().expect("path")),
                partition_column: None,
                vortex_config: VortexConfig {
                    cdc_durability: CdcDurability::Memory,
                    deletion_mode: DeletionMode::Key,
                    // Disable every tier self-flush so no spill/tick perturbs the
                    // timed window — the bench drives all residency by hand.
                    cdc_mem_tier_max_bytes: 0,
                    cdc_mem_tier_max_age_ms: 0,
                    cdc_mem_tier_min_flush_bytes: 0,
                    ..VortexConfig::default()
                },
            },
            Arc::new(RuntimeEnv::default()),
        )
        .await
        .expect("create composite-pk memory table"),
    );
    table.install_slot_advancer(Arc::new(NoopAdvancer));

    // Bulk-load every block as NEW keys (gen 0), then flush to a durable file so
    // the subsequent upserts are FILE conflicts (FileUnlocated in the keyset).
    for block in 0..blocks {
        upsert_block(&table, &schema, block, 0).await;
    }
    table
        .checkpoint_mem_tier()
        .await
        .expect("flush bulk-loaded blocks to a durable file");
    (table, schema, dir)
}

fn bench_overlay_append(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("mem_tier_overlay_append");
    group.sample_size(10);
    for &k in SLICE_COUNTS {
        group.throughput(Throughput::Elements((k * SLICE) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(k), &k, |bencher, &k| {
            bencher.to_async(&rt).iter_custom(|iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    // UNTIMED: fresh table with `k` file-resident blocks, RAM empty.
                    let (table, schema, _dir) = fresh_file_resident_table(k).await;

                    // TIMED: re-upsert each disjoint file-resident block once
                    // (gen 1). Every conflict is file-resident, so the new code's
                    // overlay delta is empty (O(1)); the old code re-filtered the
                    // whole growing overlay each time (O(tier)).
                    let started = Instant::now();
                    for block in 0..k {
                        upsert_block(&table, &schema, block, 1).await;
                    }
                    total += started.elapsed();
                    black_box(&table);
                }
                total
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_overlay_append);
criterion_main!(benches);
