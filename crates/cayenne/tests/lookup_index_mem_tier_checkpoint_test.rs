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

//! The in-memory CDC tier's checkpoint writes a protected snapshot like every
//! other writer, so that snapshot must carry its own write-time secondary index.
//! Both of its encode paths are covered: one shard (a single coordinated
//! encode) and several shards (one encode per shard, concurrently, into the same
//! snapshot directory). A lookup must be answered from the index — never
//! `unbuilt` — and return exactly the latest version of its key.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::lookup_index::LookupIndexCounters;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog, SlotAdvancer};

use datafusion::datasource::TableProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

const NAME: &str = "mem_cdc";
const ROWS: i64 = 4_000;

struct NoopSlotAdvancer;

#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Rows `ids`, each carrying `value = id * multiplier`.
fn rows(ids: std::ops::Range<i64>, multiplier: i64) -> RecordBatch {
    let ids: Vec<i64> = ids.collect();
    let values: Vec<i64> = ids.iter().map(|id| id * multiplier).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("fixture batch")
}

/// A key-deletion upsert table on the in-memory CDC tier with `shards` PK-hash
/// shards, indexed on its key, with background compaction parked so the test
/// owns when snapshots are written.
async fn open(fixture: &common::TestFixture, shards: usize) -> Arc<CayenneTableProvider> {
    let runtime_env = Arc::new(RuntimeEnv::default());
    let vortex_config = VortexConfig {
        cdc_durability: CdcDurability::Memory,
        cdc_mem_tier_shards: shards,
        deletion_mode: DeletionMode::Key,
        compaction_background_interval_ms: 3_600_000,
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(&vortex_config, Arc::clone(&runtime_env), NAME);
    let options = CreateTableOptions {
        table_name: NAME.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog: Arc<dyn MetadataCatalog> = Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let table = Arc::new(
        CayenneTableProviderBuilder::new(catalog, runtime_env)
            .with_context(context)
            .with_secondary_indexes(vec![vec!["id".to_string()]])
            .create(options)
            .await
            .expect("create table"),
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    table
}

/// CDC-applies `batch` through the in-memory tier.
async fn cdc_upsert(table: &Arc<CayenneTableProvider>, batch: RecordBatch) {
    let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        batch.schema(),
        futures::stream::iter([Ok(batch)]),
    ));
    let ctx = SessionContext::new();
    let write = table
        .write_cdc_append_stream(stream, &ctx.task_ctx())
        .await
        .expect("cdc apply");
    if write.has_pending_finalize() {
        write.finish().await.expect("cdc finalize");
    }
}

async fn sql(table: &Arc<CayenneTableProvider>, query: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table(NAME, Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    ctx.sql(query)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("run")
}

/// Every `value` the lookup of `id` returns.
async fn lookup(table: &Arc<CayenneTableProvider>, id: i64) -> Vec<i64> {
    let batches = sql(table, &format!("SELECT value FROM {NAME} WHERE id = {id}")).await;
    let mut found = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value is i64");
        for row in 0..column.len() {
            found.push(column.value(row));
        }
    }
    found
}

async fn explain(table: &Arc<CayenneTableProvider>, query: &str) -> String {
    let batches = sql(table, &format!("EXPLAIN ANALYZE {query}")).await;
    arrow::util::pretty::pretty_format_batches(&batches)
        .expect("format plan")
        .to_string()
}

fn counters(table: &Arc<CayenneTableProvider>) -> LookupIndexCounters {
    table
        .lookup_index_counters()
        .expect("the table declares an index")
}

/// The current snapshot's own index is built in the background by the first
/// lookup; waits for it so the counters below describe the checkpoint's
/// write-time index alone.
async fn await_current_index(table: &Arc<CayenneTableProvider>) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        lookup(table, 0).await;
        if counters(table).index_bytes > 0 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the current snapshot's index was never published: {:?}",
            counters(table)
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Writes `ROWS` keys, checkpoints them, upserts half of them again and
/// checkpoints again — two protected snapshots written by the mem-tier
/// checkpoint, the second superseding rows of the first — then checks that
/// every lookup is served by the index and returns only the latest version.
async fn checkpointed_mem_tier_rows_are_indexed(shards: usize) {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let table = open(&fixture, shards).await;

    cdc_upsert(&table, rows(0..ROWS, 1)).await;
    assert!(
        table.checkpoint_mem_tier().await.expect("first checkpoint") > 0,
        "the first checkpoint flushed nothing"
    );
    cdc_upsert(&table, rows(0..ROWS / 2, 10)).await;
    assert!(
        table.checkpoint_mem_tier().await.expect("second checkpoint") > 0,
        "the second checkpoint flushed nothing"
    );

    let full_scan = format!("SELECT value FROM {NAME} WHERE value >= 0");
    let plan = explain(&table, &full_scan).await;
    assert!(
        plan.contains("snapshots_scanned=2"),
        "expected the two checkpoint snapshots:\n{plan}"
    );
    // Several shards encode one file each into the same snapshot, so the index
    // build must have been fed by several concurrent writers.
    let files: usize = plan
        .split("files_scanned=")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|n| n.parse().ok())
        .expect("files_scanned in the plan");
    if shards > 1 {
        assert!(
            files > 2,
            "a {shards}-shard checkpoint wrote {files} file(s) across two snapshots; expected one per shard:\n{plan}"
        );
    }

    await_current_index(&table).await;
    let before = counters(&table);
    for id in [0, 1, ROWS / 2 - 1, ROWS / 2, ROWS - 1] {
        let expected = if id < ROWS / 2 { id * 10 } else { id };
        assert_eq!(
            lookup(&table, id).await,
            [expected],
            "lookup of {id} returned the wrong rows ({shards} shard(s))"
        );
    }
    assert!(lookup(&table, 10 * ROWS).await.is_empty());
    let after = counters(&table);

    assert_eq!(
        after.unbuilt, before.unbuilt,
        "a checkpoint snapshot was read without an index ({shards} shard(s)): {after:?}"
    );
    assert_eq!(
        after.snapshot_mismatch, before.snapshot_mismatch,
        "an index was refused for its own snapshot ({shards} shard(s)): {after:?}"
    );
    assert!(
        after.selected > before.selected
            && after.access_plans_attached > before.access_plans_attached,
        "no lookup was served by row selection ({shards} shard(s)): {after:?}"
    );
    let plan = explain(&table, &format!("SELECT value FROM {NAME} WHERE id = {}", ROWS - 1)).await;
    assert!(
        plan.contains("snapshots_scanned=1, files_scanned=1"),
        "the lookup read snapshots that cannot hold its key ({shards} shard(s)):\n{plan}"
    );
}

/// One shard: the checkpoint's single coordinated encode.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_shard_checkpoint_snapshots_are_indexed() {
    checkpointed_mem_tier_rows_are_indexed(1).await;
}

/// Several shards: one encode per shard, concurrently, into one snapshot
/// directory, all reporting to one index build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sharded_checkpoint_snapshots_are_indexed() {
    checkpointed_mem_tier_rows_are_indexed(4).await;
}
