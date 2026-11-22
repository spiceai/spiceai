/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Compares change-sink CDC apply with a first-load append of the same keyed rows into Cayenne.

#![expect(
    clippy::expect_used,
    reason = "Benchmark setup and writes must succeed"
)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use accelerator_cayenne::CayenneAccelerator;
use arrow::{
    array::{Float64Array, Int64Array, ListArray, StringArray, StructArray},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use cayenne::{
    CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog,
    metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig},
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use data_accelerator_api::DataAccelerator;
use data_components::cdc::{ChangeBatch as CdcBatch, changes_schema};
use datafusion::{
    common::TableReference,
    datasource::{TableProvider, memory::MemorySourceConfig},
    execution::context::SessionContext,
    logical_expr::dml::InsertOp,
    physical_plan::collect,
};
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use runtime_acceleration::change_sink::{
    ChangeBatch, ChangeSink, ChangeSinkContext, DurabilityObserver, Recovery, StorageDurability,
    WriteOptions,
};
use tokio::runtime::Handle;

const BATCH_COUNT: usize = 64;
const BATCH_ROWS: usize = 8192;
const PRIMARY_KEYS: [&str; 4] = ["ol_w_id", "ol_d_id", "ol_o_id", "ol_number"];

// The synthetic source retains every input, so its durability callbacks do no I/O.
// Installing this observer lets replayable CDC use the in-memory append path.
struct RetainedInput;

#[async_trait]
impl DurabilityObserver for RetainedInput {
    async fn on_durable(&self, _fence: u64) {}

    async fn record_position(
        &self,
        _fence: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

struct Fixture {
    sink: ChangeSink,
    ctx: SessionContext,
    table: Arc<CayenneTableProvider>,
    _dir: tempfile::TempDir,
}

impl Fixture {
    async fn new(schema: &SchemaRef) -> Self {
        let ctx = util::session_state::session_context();
        let dir = tempfile::tempdir().expect("table directory");
        let catalog: Arc<dyn MetadataCatalog> = Arc::new(
            CayenneCatalog::new(format!(
                "sqlite://{}",
                dir.path().join("catalog.db").display()
            ))
            .expect("catalog"),
        );
        catalog.init().await.expect("initialize catalog");
        let table = Arc::new(
            CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
                .create(CreateTableOptions {
                    table_name: "order_line".into(),
                    schema: Arc::clone(schema),
                    primary_key: PRIMARY_KEYS.iter().map(|key| (*key).into()).collect(),
                    on_conflict: Some(OnConflict::Upsert(ColumnReference::new(
                        PRIMARY_KEYS.iter().map(|key| (*key).into()).collect(),
                    ))),
                    base_path: dir.path().join("data").to_string_lossy().into_owned(),
                    partition_column: None,
                    vortex_config: VortexConfig {
                        cdc_durability: CdcDurability::Memory,
                        deletion_mode: DeletionMode::Key,
                        cdc_mem_tier_checkpoint_interval_ms: 0,
                        compaction_background_interval_ms: 0,
                        ..VortexConfig::default()
                    },
                })
                .await
                .expect("create order_line table"),
        );
        // This public hook binds CayenneChangeSinkBackend and constructs a sink
        // with capacity 2; the backend's module is private to the accelerator.
        let sink = CayenneAccelerator::new()
            .change_sink(
                ChangeSinkContext::new(
                    TableReference::bare("order_line"),
                    Arc::clone(&table) as Arc<dyn TableProvider>,
                ),
                &Handle::current(),
                2,
            )
            .await
            .expect("bind Cayenne change sink")
            .expect("Cayenne provides a change sink");
        sink.set_durability_observer(Arc::new(RetainedInput));
        Self {
            sink,
            ctx,
            table,
            _dir: dir,
        }
    }
}

fn order_line_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ol_w_id", DataType::Int64, false),
        Field::new("ol_d_id", DataType::Int64, false),
        Field::new("ol_o_id", DataType::Int64, false),
        Field::new("ol_number", DataType::Int64, false),
        Field::new("ol_i_id", DataType::Int64, false),
        Field::new("ol_amount", DataType::Float64, false),
        Field::new("ol_dist_info", DataType::Utf8, false),
    ]))
}

fn order_line_batches(schema: &SchemaRef) -> Vec<RecordBatch> {
    (0..BATCH_COUNT)
        .map(|batch_index| {
            let start = i64::try_from(batch_index * BATCH_ROWS).expect("batch start fits i64");
            let end = start + i64::try_from(BATCH_ROWS).expect("batch size fits i64");
            let rows = start..end;
            // Ten lines per order, 3000 orders per district, ten districts per
            // warehouse. The composite key stays distinct across all batches.
            RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        rows.clone().map(|r| r / 300_000 + 1),
                    )),
                    Arc::new(Int64Array::from_iter_values(
                        rows.clone().map(|r| r / 30_000 % 10 + 1),
                    )),
                    Arc::new(Int64Array::from_iter_values(
                        rows.clone().map(|r| r / 10 % 3000 + 1),
                    )),
                    Arc::new(Int64Array::from_iter_values(
                        rows.clone().map(|r| r % 10 + 1),
                    )),
                    Arc::new(Int64Array::from_iter_values(rows.map(|r| r % 100_000 + 1))),
                    Arc::new(Float64Array::from(vec![12.5; BATCH_ROWS])),
                    Arc::new(StringArray::from(vec![
                        "abcdefghijklmnopqrstuvwx";
                        BATCH_ROWS
                    ])),
                ],
            )
            .expect("order_line rows")
        })
        .collect()
}

fn change_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Vec<CdcBatch> {
    batches
        .iter()
        .map(|data| {
            let keys = ListArray::try_new(
                Arc::new(Field::new("item", DataType::Utf8, false)),
                OffsetBuffer::from_lengths(std::iter::repeat_n(PRIMARY_KEYS.len(), BATCH_ROWS)),
                Arc::new(StringArray::from(
                    (0..BATCH_ROWS)
                        .flat_map(|_| PRIMARY_KEYS)
                        .collect::<Vec<_>>(),
                )),
                None,
            )
            .expect("primary key names");
            let record = RecordBatch::try_new(
                Arc::new(changes_schema(schema)),
                vec![
                    Arc::new(StringArray::from(vec!["c"; BATCH_ROWS])),
                    Arc::new(keys),
                    Arc::new(StructArray::from(data.clone())),
                ],
            )
            .expect("CDC record");
            CdcBatch::try_new(record).expect("CDC batch")
        })
        .collect()
}

fn bench_cdc_apply(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cpu_budget::cpu_budget().main_runtime_worker_threads())
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let schema = order_line_schema();
    let rows = order_line_batches(&schema);
    let batches = change_batches(&schema, &rows);
    let total_rows = u64::try_from(BATCH_COUNT * BATCH_ROWS).expect("row count fits u64");
    let mut group = c.benchmark_group("cdc_apply");
    group.sample_size(10);
    group.throughput(Throughput::Elements(total_rows));
    group.bench_function("order_line_bootstrap_inmem", |b| {
        b.to_async(&runtime).iter_custom(|iterations| {
            let schema = &schema;
            let batches = &batches;
            async move {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let fixture = Fixture::new(schema).await;
                    let inputs: Vec<_> = batches.iter().cloned().map(ChangeBatch::cdc).collect();
                    let mut durability = [StorageDurability::Durable; BATCH_COUNT];
                    let start = Instant::now();
                    for (batch, durability) in inputs.into_iter().zip(&mut durability) {
                        let receipt = fixture
                            .sink
                            .reserve()
                            .await
                            .expect("sink capacity")
                            .submit(
                                batch,
                                WriteOptions {
                                    recovery: Recovery::Replayable,
                                    ..WriteOptions::default()
                                },
                            )
                            .expect("admit CDC batch")
                            .wait()
                            .await
                            .expect("CDC receipt");
                        receipt.published().await.expect("published CDC rows");
                        *durability = receipt.durability;
                    }
                    elapsed += start.elapsed();
                    // Check the intended path and visible row count outside timing.
                    for durability in durability {
                        assert!(
                            matches!(durability, StorageDurability::Deferred(_)),
                            "benchmark requires in-memory CDC appends, got {durability:?}"
                        );
                    }
                    assert_eq!(
                        fixture
                            .ctx
                            .read_table(Arc::clone(&fixture.table) as Arc<dyn TableProvider>)
                            .expect("scan order_line")
                            .count()
                            .await
                            .expect("visible row count"),
                        BATCH_COUNT * BATCH_ROWS
                    );
                    fixture.sink.begin_close().wait().await.expect("drain sink");
                    drop(fixture);
                }
                elapsed
            }
        });
    });
    group.bench_function("order_line_bootstrap_first_load", |b| {
        b.to_async(&runtime).iter_custom(|iterations| {
            let schema = &schema;
            let batches = &rows;
            async move {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let fixture = Fixture::new(schema).await;
                    let state = fixture.ctx.state();
                    // Without this permission, CDC memory durability does not
                    // divert a plain append to write_cdc_append_stream.
                    assert!(
                        state
                            .config()
                            .get_extension::<cayenne::RebuildableWrite>()
                            .is_none(),
                        "first-load benchmark requires an unmarked write session"
                    );
                    assert!(
                        fixture.table.takes_versioned_append().await,
                        "first-load benchmark requires an eligible empty file-backed table"
                    );
                    // One partition feeds all 64 batches through a single append.
                    let input = MemorySourceConfig::try_new_exec(
                        &[batches.clone()],
                        Arc::clone(schema),
                        None,
                    )
                    .expect("first-load input");
                    let task_ctx = fixture.ctx.task_ctx();
                    let start = Instant::now();
                    let plan = fixture
                        .table
                        .insert_into(&state, input, InsertOp::Append)
                        .await
                        .expect("first-load insert plan");
                    collect(plan, task_ctx).await.expect("first-load append");
                    elapsed += start.elapsed();

                    assert_eq!(
                        fixture
                            .ctx
                            .read_table(Arc::clone(&fixture.table) as Arc<dyn TableProvider>)
                            .expect("scan order_line")
                            .count()
                            .await
                            .expect("visible row count"),
                        BATCH_COUNT * BATCH_ROWS
                    );
                    fixture
                        .ctx
                        .register_table(
                            "order_line",
                            Arc::clone(&fixture.table) as Arc<dyn TableProvider>,
                        )
                        .expect("register order_line");
                    let duplicates = fixture
                        .ctx
                        .sql(
                            "SELECT ol_w_id, ol_d_id, ol_o_id, ol_number FROM order_line \
                             GROUP BY ol_w_id, ol_d_id, ol_o_id, ol_number HAVING count(*) > 1",
                        )
                        .await
                        .expect("duplicate-key query")
                        .collect()
                        .await
                        .expect("duplicate-key rows");
                    assert_eq!(
                        duplicates.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        0,
                        "first load must not duplicate any composite primary key"
                    );
                    fixture.sink.begin_close().wait().await.expect("drain sink");
                    drop(fixture);
                }
                elapsed
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_cdc_apply);
criterion_main!(benches);
