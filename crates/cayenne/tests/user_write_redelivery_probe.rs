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

#![allow(clippy::expect_used)]

//! Baseline probe, no `retention_sql`: a local committed user write (SQL
//! `DELETE`, SQL `UPDATE`, transaction upsert) over a key whose newest version
//! lives only in the armed `cdc_durability: memory` RAM tier, then a crash (no
//! further checkpoint) and the source's re-delivery of IDENTICAL events.
//!
//! Two re-delivery windows:
//! - `unacked`: the events this table's slot advancer never acknowledged (the
//!   tier event, unless the write's own path checkpointed it).
//! - `resume_lag`: every event since the source's saved resume point, which can
//!   lag the in-process acknowledgement (Postgres keepalive status update,
//!   watermark writer, MySQL sidecar): here, the whole history, including
//!   events already checkpointed.
//!
//! Target (CTO, 2026-11-11): re-delivery of an identical source event must not
//! overwrite a later local committed mutation, so every case must end at the
//! view the user write committed. Uses only APIs present at 9aa41988c.

use crate::common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{
    CayenneTableProvider, CayenneTableProviderBuilder, CayenneTransaction, MetadataCatalog,
    SlotAdvancer,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::*;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

test_with_backends!(redelivery_unacked_after_user_delete_impl);
test_with_backends!(redelivery_unacked_after_user_update_impl);
test_with_backends!(redelivery_unacked_after_user_transaction_impl);
test_with_backends!(redelivery_resume_lag_after_user_delete_impl);
test_with_backends!(redelivery_resume_lag_after_user_update_impl);
test_with_backends!(redelivery_resume_lag_after_user_transaction_impl);

/// Source history: two checkpointed batches, then one RAM-only batch.
const DURABLE_A: [(i64, i64); 2] = [(1, 1), (3, -3)];
const DURABLE_B: [(i64, i64); 1] = [(2, 2)];
const TIER: [(i64, i64); 1] = [(3, 3)];

#[derive(Clone, Copy, Debug)]
enum Write {
    Delete,
    Update,
    Transaction,
}

#[derive(Clone, Copy, Debug)]
enum Window {
    Unacked,
    ResumeLag,
}

struct CountingAdvancer(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl SlotAdvancer for CountingAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn batch(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

fn stream(rows: &[(i64, i64)]) -> SendableRecordBatchStream {
    let b = batch(rows);
    Box::pin(RecordBatchStreamAdapter::new(
        b.schema(),
        futures::stream::iter([Ok(b)]),
    ))
}

async fn cdc(table: &CayenneTableProvider, rows: &[(i64, i64)]) -> TestResult<()> {
    let ctx = SessionContext::new();
    let write = table
        .write_cdc_append_stream(stream(rows), &ctx.task_ctx())
        .await?;
    if write.has_pending_finalize() {
        write.finish().await?;
    }
    Ok(())
}

async fn rows(table: &Arc<CayenneTableProvider>) -> TestResult<Vec<(i64, i64)>> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)?;
    let batches = ctx.sql("SELECT id, value FROM t").await?.collect().await?;
    let mut out = Vec::new();
    for b in &batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let values = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v");
        out.extend((0..b.num_rows()).map(|r| (ids.value(r), values.value(r))));
    }
    out.sort_unstable();
    Ok(out)
}

fn options(fixture: &common::TestFixture, name: &str) -> CreateTableOptions {
    CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cdc_durability: CdcDurability::Memory,
            deletion_mode: DeletionMode::Key,
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
    }
}

async fn probe(fixture: common::TestFixture, write: Write, window: Window) -> TestResult<()> {
    let name = format!("redeliver_{write:?}_{window:?}").to_lowercase();
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let acks = Arc::new(AtomicUsize::new(0));
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog),
            options(&fixture, &name),
            SessionContext::new().runtime_env(),
        )
        .await?,
    );
    table.install_slot_advancer(Arc::new(CountingAdvancer(Arc::clone(&acks))));

    cdc(&table, &DURABLE_A).await?;
    table.checkpoint_mem_tier().await?;
    cdc(&table, &DURABLE_B).await?;
    table.checkpoint_mem_tier().await?;
    let acks_before_tier = acks.load(Ordering::SeqCst);
    cdc(&table, &TIER).await?;
    assert_eq!(
        acks.load(Ordering::SeqCst),
        acks_before_tier,
        "{name}: the tier event is RAM-only and unacknowledged before the user write"
    );
    assert_eq!(
        rows(&table).await?,
        vec![(1, 1), (2, 2), (3, 3)],
        "{name}: seeded"
    );

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&table) as Arc<dyn TableProvider>)?;
    match write {
        Write::Delete => {
            ctx.sql("DELETE FROM t WHERE id = 3")
                .await?
                .collect()
                .await?;
        }
        Write::Update => {
            ctx.sql("UPDATE t SET value = 30 WHERE id = 3")
                .await?
                .collect()
                .await?;
        }
        Write::Transaction => {
            let txn = CayenneTransaction::new();
            let token = table.transaction_write_token().await;
            txn.register(
                table.table_id().to_string(),
                token,
                table.clone_for_write_operations(),
            );
            let staged = table
                .begin_staged_upsert_occ(token, stream(&[(3, 30)]), 1)
                .await?;
            txn.set_staged(table.table_id(), staged);
            txn.commit().await?;
        }
    }
    let committed = match write {
        Write::Delete => vec![(1, 1), (2, 2)],
        Write::Update | Write::Transaction => vec![(1, 1), (2, 2), (3, 30)],
    };
    assert_eq!(
        rows(&table).await?,
        committed,
        "{name}: live after the user write"
    );
    let tier_acked = acks.load(Ordering::SeqCst) > acks_before_tier;
    println!("{name}: tier acknowledged by the user write's own path: {tier_acked}");

    // Crash: the old instance is left running (nothing flushes), a fresh
    // process opens the catalog and the source re-delivers identical events.
    let restarted = Arc::new(
        CayenneTableProviderBuilder::new(Arc::clone(&catalog), SessionContext::new().runtime_env())
            .open(&name)
            .await?,
    );
    restarted.install_slot_advancer(Arc::new(CountingAdvancer(Arc::new(AtomicUsize::new(0)))));
    let after_open = rows(&restarted).await?;
    println!("{name}: reopened before re-delivery: {after_open:?}");
    match window {
        Window::Unacked => {
            if !tier_acked {
                cdc(&restarted, &TIER).await?;
            }
        }
        Window::ResumeLag => {
            cdc(&restarted, &DURABLE_A).await?;
            cdc(&restarted, &DURABLE_B).await?;
            cdc(&restarted, &TIER).await?;
        }
    }
    let replayed = rows(&restarted).await?;
    println!("{name}: after re-delivery: {replayed:?}");
    assert_eq!(
        replayed, committed,
        "{name}: identical re-delivery must not overwrite the committed user write (tier acked: {tier_acked})"
    );
    Ok(())
}

async fn redelivery_unacked_after_user_delete_impl(f: common::TestFixture) -> TestResult<()> {
    probe(f, Write::Delete, Window::Unacked).await
}
async fn redelivery_unacked_after_user_update_impl(f: common::TestFixture) -> TestResult<()> {
    probe(f, Write::Update, Window::Unacked).await
}
async fn redelivery_unacked_after_user_transaction_impl(f: common::TestFixture) -> TestResult<()> {
    probe(f, Write::Transaction, Window::Unacked).await
}
async fn redelivery_resume_lag_after_user_delete_impl(f: common::TestFixture) -> TestResult<()> {
    probe(f, Write::Delete, Window::ResumeLag).await
}
async fn redelivery_resume_lag_after_user_update_impl(f: common::TestFixture) -> TestResult<()> {
    probe(f, Write::Update, Window::ResumeLag).await
}
async fn redelivery_resume_lag_after_user_transaction_impl(
    f: common::TestFixture,
) -> TestResult<()> {
    probe(f, Write::Transaction, Window::ResumeLag).await
}
