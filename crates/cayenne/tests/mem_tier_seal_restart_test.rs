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

//! Regression test for #16278: a restart after a mem-tier seal must serve each
//! key once, also when no protected snapshot exists.
//!
//! A full rewrite folds every protected snapshot into the current snapshot. A
//! CDC upsert then supersedes a stored row in the mem tier, and a seal makes it
//! durable as an inline shadow row. The restart drops the process before any
//! checkpoint, so the seal is all the durable state there is, and the source
//! does not replay what the seal acknowledged. A seal that stamped a re-insert
//! on the superseded key made the restored current-snapshot scan serve the old
//! row beside the shadow. This cannot be driven through a Spicepod: nothing
//! there forces a full rewrite to fold every protected snapshot, or a crash
//! between a seal and the next checkpoint.

#![expect(clippy::expect_used, reason = "test setup and exact row assertions")]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog, SlotAdvancer};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

use crate::common;

const TABLE: &str = "t";

struct NoopSlotAdvancer;
#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

fn schema(id_type: &DataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", id_type.clone(), false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn batch(id_type: &DataType, ids: &[i64], values: &[i64]) -> RecordBatch {
    let ids: ArrayRef = match id_type {
        DataType::Utf8 => Arc::new(StringArray::from_iter_values(
            ids.iter().map(|id| format!("key-{id}")),
        )),
        _ => Arc::new(Int64Array::from(ids.to_vec())),
    };
    RecordBatch::try_new(
        schema(id_type),
        vec![ids, Arc::new(Int64Array::from(values.to_vec()))],
    )
    .expect("batch")
}

/// Every row the table serves, as `(id, value)` sorted by both.
async fn served(table: &Arc<CayenneTableProvider>) -> Vec<(String, i64)> {
    let ctx = SessionContext::new();
    ctx.register_table(TABLE, Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    let batches = ctx
        .sql(&format!(
            "SELECT CAST(id AS VARCHAR), value FROM {TABLE} ORDER BY 1, 2"
        ))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("scan");
    let mut rows = Vec::new();
    for batch in &batches {
        let column = arrow::compute::cast(batch.column(0), &DataType::Utf8).expect("cast id");
        let ids = column.as_any().downcast_ref::<StringArray>().expect("ids");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("values");
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row).to_string(), values.value(row)));
        }
    }
    rows
}

fn expected(id_type: &DataType, rows: &[(i64, i64)]) -> Vec<(String, i64)> {
    let mut rows: Vec<_> = rows
        .iter()
        .map(|&(id, value)| {
            let id = match id_type {
                DataType::Utf8 => format!("key-{id}"),
                _ => id.to_string(),
            };
            (id, value)
        })
        .collect();
    rows.sort();
    rows
}

/// A full rewrite, an upsert of key 0 in the mem tier, a seal and a restart.
async fn seal_after_full_rewrite_then_restart(id_type: DataType, shards: usize) {
    let case = format!("id {id_type}, {shards} shard(s)");
    let tmp = tempfile::tempdir().expect("tempdir");
    let catalog = || async {
        let catalog = cayenne::CayenneCatalog::new(format!(
            "sqlite://{}",
            tmp.path().join("catalog.db").display()
        ))
        .expect("catalog");
        catalog.init().await.expect("init");
        Arc::new(catalog) as Arc<dyn MetadataCatalog>
    };
    let options = CreateTableOptions {
        table_name: TABLE.to_string(),
        schema: schema(&id_type),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: tmp.path().join("data").to_string_lossy().into_owned(),
        partition_column: None,
        vortex_config: VortexConfig {
            cdc_durability: CdcDurability::Memory,
            deletion_mode: DeletionMode::Key,
            // Seals and checkpoints run only when the test calls them.
            cdc_mem_tier_min_flush_bytes: 0,
            cdc_mem_tier_shards: shards,
            compaction_background_interval_ms: 3_600_000,
            // The durable write lands in Vortex files rather than inline.
            inline_max_rows: 0,
            ..VortexConfig::default()
        },
    };
    let table = Arc::new(
        CayenneTableProvider::create_table(
            catalog().await,
            options,
            SessionContext::new().runtime_env(),
        )
        .await
        .expect("create"),
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    common::insert_batch(table.as_ref(), batch(&id_type, &[0, 1], &[1, 2]))
        .await
        .expect("durable insert");
    table
        .flush_pending_maintenance()
        .await
        .expect("drain maintenance");
    table
        .sort_and_rewrite_data(64 << 20)
        .await
        .expect("full rewrite");
    let protected = table
        .catalog()
        .get_all_snapshot_sequences(table.table_id())
        .await
        .expect("snapshots");
    assert!(
        protected.is_empty(),
        "{case}: the rewrite folded every protected snapshot: {protected:?}"
    );
    assert_eq!(
        served(&table).await,
        expected(&id_type, &[(0, 1), (1, 2)]),
        "{case}: before the upsert"
    );

    let ctx = SessionContext::new();
    let write = table
        .write_cdc_append_stream(
            common::single_batch_stream(batch(&id_type, &[0], &[10])),
            &ctx.task_ctx(),
        )
        .await
        .expect("CDC upsert");
    assert!(
        write.in_memory_epoch().is_some(),
        "{case}: the upsert lands in the mem tier"
    );
    if write.has_pending_finalize() {
        write.finish().await.expect("finish the upsert");
    }
    let sealed = table.seal_mem_tier_durable().await.expect("seal");
    assert_eq!(sealed, 1, "{case}: the seal shadows the upserted row");
    assert_eq!(
        served(&table).await,
        expected(&id_type, &[(0, 10), (1, 2)]),
        "{case}: live after the seal"
    );

    // Restart: the process stops before any checkpoint.
    drop(table);
    let reopened = Arc::new(
        CayenneTableProviderBuilder::new(catalog().await, SessionContext::new().runtime_env())
            .open(TABLE)
            .await
            .expect("reopen"),
    );
    assert_eq!(
        served(&reopened).await,
        expected(&id_type, &[(0, 10), (1, 2)]),
        "{case}: after the restart each key is served once, with its new value"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_after_full_rewrite_then_restart_int64() {
    seal_after_full_rewrite_then_restart(DataType::Int64, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_after_full_rewrite_then_restart_utf8() {
    seal_after_full_rewrite_then_restart(DataType::Utf8, 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seal_after_full_rewrite_then_restart_sharded() {
    seal_after_full_rewrite_then_restart(DataType::Int64, 4).await;
}
