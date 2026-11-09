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

//! A keyed partition's deferred append publishes an overlay: a protected
//! snapshot holding only the append's rows, committed by its sequence record
//! in the cross-partition transaction, with the partition's current snapshot
//! left in place (#14947). These tests drive the building blocks the
//! partitioned append coordinator (`accelerator-cayenne`) composes, in its
//! order, and reopen the table from a new catalog connection, as a restart does:
//! after a normal commit, after a crash before the catalog commit, and after a
//! crash between the catalog commit and the in-memory publication.

#![expect(clippy::expect_used, reason = "test code")]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{
    CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog,
    PreparedStagedAppend,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion_common::DataFusionError;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

const TABLE: &str = "partition_0";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn stream(rows: &[(i64, i64)]) -> SendableRecordBatchStream {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
        ],
    )
    .expect("batch");
    Box::pin(RecordBatchStreamAdapter::new(
        schema(),
        futures::stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
    ))
}

async fn catalog(dir: &Path) -> Arc<CayenneCatalog> {
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", dir.join("catalog.db").display()))
            .expect("catalog"),
    );
    catalog.init().await.expect("init catalog");
    catalog
}

/// One partition of a keyed partitioned acceleration.
async fn keyed_partition(dir: &Path, deletion_mode: DeletionMode) -> Arc<CayenneTableProvider> {
    let catalog = catalog(dir).await;
    Arc::new(
        CayenneTableProviderBuilder::new(
            catalog as Arc<dyn MetadataCatalog>,
            SessionContext::new().runtime_env(),
        )
        .create(CreateTableOptions {
            table_name: TABLE.to_string(),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: dir.join("data").to_string_lossy().into_owned(),
            partition_column: None,
            vortex_config: VortexConfig {
                deletion_mode,
                compaction_background_interval_ms: 0,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("create partition"),
    )
}

/// Reopen the partition through a new catalog connection.
async fn reopen(dir: &Path) -> Arc<CayenneTableProvider> {
    let catalog = catalog(dir).await;
    Arc::new(
        CayenneTableProviderBuilder::new(
            catalog as Arc<dyn MetadataCatalog>,
            SessionContext::new().runtime_env(),
        )
        .open(TABLE)
        .await
        .expect("reopen partition"),
    )
}

async fn rows(table: &Arc<CayenneTableProvider>) -> Vec<(i64, i64)> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    let batches = ctx
        .sql("SELECT id, v FROM t ORDER BY id, v")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("rows");
    batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id")
                .clone();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("v")
                .clone();
            (0..batch.num_rows())
                .map(|row| (ids.value(row), values.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Where the coordinator's run stops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stop {
    /// Runs to the end.
    Never,
    /// The staged files are in the target, the catalog transaction never ran.
    BeforeCommit,
    /// The catalog transaction committed, nothing was published in memory.
    AfterCommit,
}

/// Run one partition's append through the coordinator's steps, in its order.
/// A stop forgets the receipt and its payload rather than dropping them, as a
/// crash would, so no cleanup runs.
async fn append(
    catalog: &CayenneCatalog,
    table: &CayenneTableProvider,
    rows: &[(i64, i64)],
    stop: Stop,
) -> bool {
    let mut prepared: PreparedStagedAppend = table
        .begin_deferred_snapshot_append(stream(rows), 1)
        .await
        .expect("stage append");
    let overlay = prepared.publishes_overlay();
    let fence = prepared.lock_listing_fence_write_owned().await;
    prepared
        .apply_under_held_barrier()
        .await
        .expect("move staged files");
    drop(fence);
    prepared
        .prepare_deferred_manifest()
        .await
        .expect("manifest");
    if stop == Stop::BeforeCommit {
        std::mem::forget(prepared);
        return overlay;
    }
    let mut on_conflict = prepared
        .take_prepared_on_conflict()
        .expect("a keyed partition's append carries on-conflict state");
    let fence = prepared.lock_listing_fence_write_owned().await;
    let mut txn = catalog.begin_transaction().await.expect("begin");
    catalog
        .apply_prepared_on_conflict_in_txn(&mut *txn, &mut on_conflict)
        .await
        .expect("on-conflict payload");
    catalog
        .clear_table_statistics_in_txn(&mut *txn, prepared.table_id())
        .await
        .expect("clear statistics");
    txn.commit().await.expect("commit");
    on_conflict.mark_catalog_committed();
    if stop == Stop::AfterCommit {
        std::mem::forget(on_conflict);
        std::mem::forget(fence);
        std::mem::forget(prepared);
        return overlay;
    }
    let sequence = on_conflict.snapshot_sequence();
    prepared.publish_on_conflict_under_held_fence(on_conflict);
    prepared.publish_validated_file_keys(Some(sequence));
    drop(fence);
    prepared
        .remove_committed_staging_wal()
        .await
        .expect("remove staging WAL");
    prepared.finish_deferred_snapshot_maintenance();
    prepared.finish().await.expect("finish");
    overlay
}

async fn current_snapshot(catalog: &CayenneCatalog) -> String {
    catalog
        .get_table(TABLE)
        .await
        .expect("table")
        .current_snapshot_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_overlay_keeps_one_row_per_key_and_the_current_snapshot() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let dir = tempfile::tempdir().expect("dir");
        let table = keyed_partition(dir.path(), mode).await;
        let catalog = catalog(dir.path()).await;
        let before = current_snapshot(&catalog).await;

        assert!(
            append(&catalog, &table, &[(1, 0), (2, 0), (3, 0)], Stop::Never).await,
            "{mode:?}: a keyed partition's append is an overlay"
        );
        assert!(append(&catalog, &table, &[(1, 1)], Stop::Never).await);
        assert!(append(&catalog, &table, &[(1, 2), (3, 2)], Stop::Never).await);

        assert_eq!(
            current_snapshot(&catalog).await,
            before,
            "{mode:?}: an overlay moves no pointer"
        );
        assert_eq!(rows(&table).await, [(1, 2), (2, 0), (3, 2)], "{mode:?}");
        assert_eq!(
            rows(&reopen(dir.path()).await).await,
            [(1, 2), (2, 0), (3, 2)],
            "{mode:?}: after a restart"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_rolls_back_an_overlay_the_catalog_never_committed() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let dir = tempfile::tempdir().expect("dir");
        let table = keyed_partition(dir.path(), mode).await;
        let catalog = catalog(dir.path()).await;
        append(&catalog, &table, &[(1, 0), (2, 0)], Stop::Never).await;

        append(&catalog, &table, &[(1, 1), (3, 1)], Stop::BeforeCommit).await;
        drop(table);

        let reopened = reopen(dir.path()).await;
        assert_eq!(rows(&reopened).await, [(1, 0), (2, 0)], "{mode:?}");
        // The rolled-back write leaves the partition writable.
        append(&catalog, &reopened, &[(2, 5)], Stop::Never).await;
        assert_eq!(rows(&reopened).await, [(1, 0), (2, 5)], "{mode:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_rolls_forward_an_overlay_the_catalog_committed() {
    for mode in [DeletionMode::Key, DeletionMode::Position] {
        let dir = tempfile::tempdir().expect("dir");
        let table = keyed_partition(dir.path(), mode).await;
        let catalog = catalog(dir.path()).await;
        append(&catalog, &table, &[(1, 0), (2, 0)], Stop::Never).await;

        append(&catalog, &table, &[(1, 1), (3, 1)], Stop::AfterCommit).await;
        drop(table);

        let reopened = reopen(dir.path()).await;
        assert_eq!(rows(&reopened).await, [(1, 1), (2, 0), (3, 1)], "{mode:?}");
        append(&catalog, &reopened, &[(3, 7)], Stop::Never).await;
        assert_eq!(rows(&reopened).await, [(1, 1), (2, 0), (3, 7)], "{mode:?}");
    }
}
