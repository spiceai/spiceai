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

//! Regression test for #12007: zero-row CDC readiness heartbeats (#11777)
//! must not force the durable CDC path on a memory-durability Cayenne table.
//!
//! Lag-based readiness makes CDC connectors emit a zero-row heartbeat
//! envelope roughly every second on a caught-up source. The heartbeat's
//! committer is a no-op, but a no-op committer does not support deferral, so
//! before the fix every heartbeat flipped the coalesced burst to
//! `requires_durable_cdc_path` — forcing a mem-tier checkpoint (which acks
//! the deferred source committers) once per heartbeat. Under load those
//! forced checkpoints raced Cayenne's pipelined Stage-B staged-append
//! finalize and its staged-WAL crash recovery, duplicating rows.
//!
//! The invariant verified here: applying a readiness heartbeat marks the
//! dataset Ready but does NOT drain (ack) the deferred commit queue — the
//! source slot advances only when a real durability event covers the RAM
//! tier, never because a heartbeat arrived.

#![cfg(not(windows))]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use data_components::cdc::{
    ChangeBatch, ChangeEnvelope, CommitChange, CommitError, StreamError, build_heartbeat_envelope,
    changes_schema, now_unix_ms,
};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use futures::StreamExt;
use runtime::accelerated::refresh::Refresh;
use runtime::accelerated::refresh_task::RefreshTaskBuilder;
use runtime::federated::FederatedTable;
use runtime::status::RuntimeStatus;
use tempfile::TempDir;
use tokio::runtime::Handle;
use tokio::sync::{Mutex as TokioMutex, Notify, RwLock};

/// All-nullable data schema, matching what CDC sources produce (and what the
/// heartbeat envelope normalizes to), so heartbeats and change batches share
/// one schema.
fn data_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// Records committed envelope ids; supports deferral like a real replayable
/// source committer (a Postgres replication slot LSN ack), so memory-mode
/// Cayenne defers it behind the covering checkpoint.
struct DeferrableRecordingCommitter {
    id: i64,
    commits: Arc<TokioMutex<Vec<i64>>>,
}

#[async_trait]
impl CommitChange for DeferrableRecordingCommitter {
    async fn commit(&self) -> Result<(), CommitError> {
        self.commits.lock().await.push(self.id);
        Ok(())
    }

    fn supports_deferral(&self) -> bool {
        true
    }
}

/// Build a single-row create-op `ChangeEnvelope` for `(id, name)`.
fn make_create_envelope(id: i64, name: &str, commits: Arc<TokioMutex<Vec<i64>>>) -> ChangeEnvelope {
    let data = data_schema();
    let wrapper = changes_schema(&data);

    let op_array: ArrayRef = Arc::new(StringArray::from(vec!["c"]));

    let pk_field = Arc::new(Field::new("item", DataType::Utf8, false));
    let pk_values = StringArray::from(vec!["id"]);
    let pk_array: ArrayRef = Arc::new(
        ListArray::try_new(
            pk_field,
            OffsetBuffer::new(vec![0i32, 1].into()),
            Arc::new(pk_values),
            None,
        )
        .expect("ListArray"),
    );

    let id_array: ArrayRef = Arc::new(Int64Array::from(vec![id]));
    let name_array: ArrayRef = Arc::new(StringArray::from(vec![Some(name)]));
    let data_array: ArrayRef = Arc::new(StructArray::from(vec![
        (Arc::new(Field::new("id", DataType::Int64, true)), id_array),
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            name_array,
        ),
    ]));

    let record = RecordBatch::try_new(Arc::new(wrapper), vec![op_array, pk_array, data_array])
        .expect("wrapper RecordBatch");
    let batch = ChangeBatch::try_new(record).expect("ChangeBatch");

    ChangeEnvelope::new(
        Box::new(DeferrableRecordingCommitter { id, commits }),
        batch,
        false,
    )
}

/// Construct a memory-durability (CDC mem-tier) Cayenne table provider backed
/// by a temp `SQLite` metastore and temp data directory, mirroring the
/// production `cdc_durability: memory` chbench configuration.
async fn setup_memory_mode_cayenne(table_name: &str) -> (TempDir, Arc<CayenneTableProvider>) {
    let temp = TempDir::new().expect("temp dir");
    let data_path = temp.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = temp.path().join("test.db");
    let conn = format!("sqlite://{}", db_path.to_string_lossy());

    let catalog = Arc::new(CayenneCatalog::new(conn).expect("CayenneCatalog::new"));
    catalog.init().await.expect("catalog init");

    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(
        Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: table_name.to_string(),
            schema: data_schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                cdc_durability: CdcDurability::Memory,
                deletion_mode: DeletionMode::Key,
                ..VortexConfig::default()
            },
        },
        ctx.runtime_env(),
    )
    .await
    .expect("create_table");

    assert!(
        table.is_cdc_memory_mode(),
        "test requires a memory-durability CDC tier"
    );
    (temp, Arc::new(table))
}

fn make_refresh_task(
    accelerator: Arc<dyn TableProvider>,
    table_name: &str,
) -> runtime::accelerated::refresh_task::RefreshTask {
    let federated = Arc::new(FederatedTable::new_unchecked(Arc::clone(&accelerator)));
    RefreshTaskBuilder::new(
        RuntimeStatus::new(),
        TableReference::bare(table_name),
        federated,
        None,
        accelerator,
        Handle::current(),
        Arc::new(tokio::sync::Mutex::new(())),
    )
    .build()
}

/// Count the rows currently visible via `TableProvider::scan`.
async fn count_rows(provider: &Arc<CayenneTableProvider>) -> usize {
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan");
    let batches = collect(plan, ctx.task_ctx()).await.expect("collect");
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Poll `condition` until it returns true or `timeout` elapses; returns
/// whether the condition was met. Bounded wait — no fixed sleeps as readiness
/// gates.
async fn wait_until<F, Fut>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if condition().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeat_must_not_force_mem_tier_checkpoint_or_ack_deferred_commits() {
    let (_temp, table) = setup_memory_mode_cayenne("cdc_heartbeat_no_checkpoint").await;
    let task = make_refresh_task(
        Arc::clone(&table) as Arc<dyn TableProvider>,
        "cdc_heartbeat_no_checkpoint",
    );

    let commits: Arc<TokioMutex<Vec<i64>>> = Arc::new(TokioMutex::new(Vec::new()));
    let initial_load = Arc::new(AtomicBool::new(false));
    let ready_notify = Arc::new(Notify::new());

    // Channel-driven stream so envelope timing is under test control.
    let (tx, rx) = futures::channel::mpsc::unbounded::<Result<ChangeEnvelope, StreamError>>();

    let stream_task = {
        let notify = Arc::clone(&ready_notify);
        let load = Arc::clone(&initial_load);
        let refresh = Arc::new(RwLock::new(Refresh::default()));
        tokio::spawn(async move {
            task.start_changes_stream(refresh, rx.boxed(), None, Some(notify), load)
                .await
        })
    };

    // Subscribe to the ready notification BEFORE the heartbeat is sent so the
    // notify_waiters signal cannot be missed.
    let ready_seen = {
        let notify = Arc::clone(&ready_notify);
        tokio::spawn(async move {
            let waiter = notify.notified();
            tokio::pin!(waiter);
            tokio::time::timeout(Duration::from_secs(10), &mut waiter)
                .await
                .is_ok()
        })
    };
    tokio::task::yield_now().await;

    // 1. A real change lands in the mem tier; its committer is DEFERRED
    //    behind the covering checkpoint (memory durability), so it must not
    //    have committed yet.
    tx.unbounded_send(Ok(make_create_envelope(1, "row-1", Arc::clone(&commits))))
        .expect("send row envelope");
    assert!(
        wait_until(Duration::from_secs(10), || async {
            count_rows(&table).await == 1
        })
        .await,
        "the CDC row must become visible via the mem tier"
    );
    assert!(
        commits.lock().await.is_empty(),
        "premise: a memory-durability write defers its source committer behind \
         the covering checkpoint (nothing durable happened yet)"
    );

    // 2. A readiness heartbeat arrives (as connectors emit every ~1s on a
    //    caught-up source). It must mark the dataset Ready...
    let heartbeat = build_heartbeat_envelope(&data_schema(), now_unix_ms(), true)
        .expect("heartbeat envelope builds");
    tx.unbounded_send(Ok(heartbeat)).expect("send heartbeat");

    assert!(
        ready_seen.await.expect("ready waiter task must finish"),
        "the heartbeat's ready flag must flip the dataset Ready"
    );

    // 3. ...but it must NOT force a mem-tier checkpoint: the deferred source
    //    committer stays queued (un-acked). Before the #12007 fix, the
    //    heartbeat's non-deferrable no-op committer forced
    //    `requires_durable_cdc_path`, checkpointing the mem tier and acking
    //    the deferred committer right here.
    assert!(
        commits.lock().await.is_empty(),
        "a readiness heartbeat must not force a mem-tier checkpoint that acks \
         deferred source commits (#12007)"
    );

    // The row remains visible; the heartbeat wrote nothing.
    assert_eq!(
        count_rows(&table).await,
        1,
        "heartbeat must not change data"
    );

    drop(tx);
    stream_task
        .await
        .expect("changes stream task join")
        .expect("changes stream should end cleanly");
}
