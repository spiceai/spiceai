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

//! Bench: the SEAL (cheap durable slot-advance) vs the BAKE (full protected-
//! snapshot checkpoint) on the SAME active-piece delta.
//!
//! Motivation: the source replication slot advances only when the mem-tier delta
//! is made durable. A **bake** (Vortex encode + protected-snapshot publish under
//! the listing fence) is too expensive — and too read-amplifying — to run every
//! few seconds, which is why the age cap sits at 10–15 s and replication/freshness
//! lag tracks it. A **seal** makes the SAME delta durable via ONE unpublished
//! inline-corpus commit (no Vortex encode, no listing fence, no protected snapshot,
//! no read amplification) and fires the slot advancer, so the slot can advance
//! every `cdc_mem_tier_seal_age_ms` (default 2 s) for sub-3 s freshness. This bench
//! quantifies seal-cost ≪ bake-cost on identical deltas — the ratio is the headroom
//! that lets the seal cadence be far tighter than the bake cadence.
//!
//! Bench discipline (Tiger Style): the table + delta are built in the untimed
//! `iter_batched` setup; only the durable flush is timed. Every loop is bounded,
//! every `expect` carries a message, and the delta is asserted resident in RAM
//! (`in_memory_epoch`) before the timed flush so both lanes measure the same state.

#![allow(clippy::expect_used)]

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog, SlotAdvancer};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

struct NoopAdvancer;

#[async_trait::async_trait]
impl SlotAdvancer for NoopAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

fn id_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn id_batch(schema: &Arc<Schema>, start: i64, rows: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + rows).collect();
    RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(Int64Array::from(ids))])
        .expect("id batch construction")
}

/// Build a fresh memory-mode upsert table with ALL self-flush disabled (so the
/// bench alone drives the durable flush), arm a no-op advancer, and append
/// `delta_rows` rows to the RAM tier. Asserts the write engaged the RAM tier so
/// both lanes measure a flush of the SAME resident delta. Returns the provider +
/// its temp dir (kept alive for the timed closure).
async fn table_with_delta(
    delta_rows: i64,
    tag: &str,
) -> (Arc<CayenneTableProvider>, tempfile::TempDir) {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", temp_dir.path().to_str().expect("temp path"));
    let data_dir = format!("{}/data", temp_dir.path().to_str().expect("temp path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
    let connection_string = format!("sqlite://{metadata_dir}/cayenne.db");
    let catalog = Arc::new(CayenneCatalog::new(connection_string).expect("catalog"))
        as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("init catalog");

    let schema = id_schema();
    let vortex_config = VortexConfig {
        cdc_durability: CdcDurability::Memory,
        deletion_mode: DeletionMode::Key,
        // Disable every tier self-flush so the bench fully controls residency.
        cdc_mem_tier_max_bytes: 0,
        cdc_mem_tier_max_age_ms: 0,
        cdc_mem_tier_min_flush_bytes: 0,
        // Sealing is driven explicitly here, not by the (unspawned) tick.
        cdc_mem_tier_seal_age_ms: 0,
        ..VortexConfig::default()
    };
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog),
            CreateTableOptions {
                table_name: format!("seal_bench_{tag}"),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: format!("{data_dir}/t"),
                partition_column: None,
                vortex_config,
            },
            Arc::new(RuntimeEnv::default()),
        )
        .await
        .expect("create memory-mode table"),
    );
    table.install_slot_advancer(Arc::new(NoopAdvancer));

    let ctx = SessionContext::new();
    let batch = id_batch(&schema, 0, delta_rows);
    let stream = datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(
        batch.schema(),
        futures::stream::iter([Ok::<_, datafusion_common::DataFusionError>(batch)]),
    );
    let write = table
        .write_cdc_append_stream(Box::pin(stream), &ctx.task_ctx())
        .await
        .expect("CDC append to RAM tier");
    assert!(
        write.in_memory_epoch().is_some(),
        "the delta must land in the RAM mem tier (memory mode + armed) — a durable \
         fallback would mean the bench measures the wrong state"
    );
    (table, temp_dir)
}

fn bench_durable_flush(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("mem_tier_durable_flush");
    for &delta_rows in &[1_000_i64, 20_000] {
        group.throughput(Throughput::Elements(
            u64::try_from(delta_rows).expect("positive"),
        ));

        // SEAL lane: shadow the active delta into the unpublished inline corpus +
        // advance the slot (no Vortex, no fence, no protected snapshot).
        group.bench_with_input(
            BenchmarkId::new("seal", delta_rows),
            &delta_rows,
            |b, &n| {
                b.iter_batched(
                    || rt.block_on(table_with_delta(n, "seal")),
                    |(table, _tmp)| {
                        rt.block_on(async {
                            black_box(table.seal_mem_tier_durable().await.expect("seal"));
                        });
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // BAKE lane: the full protected-snapshot checkpoint of the SAME delta.
        group.bench_with_input(
            BenchmarkId::new("bake", delta_rows),
            &delta_rows,
            |b, &n| {
                b.iter_batched(
                    || rt.block_on(table_with_delta(n, "bake")),
                    |(table, _tmp)| {
                        rt.block_on(async {
                            black_box(table.checkpoint_mem_tier().await.expect("bake"));
                        });
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_durable_flush);
criterion_main!(benches);
