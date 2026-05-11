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

//! End-to-end test of the CDC apply pipeline (`RefreshTask::start_changes_stream`)
//! into a Cayenne accelerator using the data-inlining write path.
//!
//! This test exercises the full pipeline that production change-data-capture
//! datasets use: `ChangesStream` → bounded prefetch channel → coalesced burst
//! → `CayenneTableProvider::insert_into` → `CayenneDataSink` → metastore-inlined
//! data. It verifies (a) all rows land in the accelerator, (b) commits happen
//! in stream order, and (c) small bursts are stored as inlined data in the
//! Cayenne metastore (i.e. the data-inlining path was hit, not the file path).

#![cfg(not(windows))]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use arrow::array::{ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use data_components::cdc::{
    ChangeBatch, ChangeEnvelope, ChangesStream, CommitChange, CommitError, StreamError,
    changes_schema,
};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use futures::StreamExt;
use futures::stream as fstream;
use runtime::accelerated_table::refresh::Refresh;
use runtime::accelerated_table::refresh_task::RefreshTaskBuilder;
use runtime::federated_table::FederatedTable;
use runtime::status::RuntimeStatus;
use tempfile::TempDir;
use tokio::runtime::Handle;
use tokio::sync::{Mutex as TokioMutex, RwLock};

/// Schema used by all rows in this test: a numeric primary key and a string column.
fn data_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// Records each committed envelope id. Used to assert every envelope's
/// source-side offset is committed exactly once, in stream order.
struct RecordingCommitter {
    id: i64,
    commits: Arc<TokioMutex<Vec<i64>>>,
}

#[async_trait]
impl CommitChange for RecordingCommitter {
    async fn commit(&self) -> Result<(), CommitError> {
        self.commits.lock().await.push(self.id);
        Ok(())
    }
}

/// Build a single-row create-op `ChangeEnvelope` for `(id, name)`.
fn make_create_envelope(id: i64, name: &str, commits: Arc<TokioMutex<Vec<i64>>>) -> ChangeEnvelope {
    let data = data_schema();
    let wrapper = changes_schema(&data);

    let op_array: ArrayRef = Arc::new(StringArray::from(vec!["c"]));

    // primary_keys: List<Utf8> with one entry "id"
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

    // data: Struct<id, name>
    let id_array: ArrayRef = Arc::new(Int64Array::from(vec![id]));
    let name_array: ArrayRef = Arc::new(StringArray::from(vec![Some(name)]));
    let data_array: ArrayRef = Arc::new(StructArray::from(vec![
        (Arc::new(Field::new("id", DataType::Int64, false)), id_array),
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            name_array,
        ),
    ]));

    let record = RecordBatch::try_new(Arc::new(wrapper), vec![op_array, pk_array, data_array])
        .expect("wrapper RecordBatch");
    let batch = ChangeBatch::try_new(record).expect("ChangeBatch");

    ChangeEnvelope::new(Box::new(RecordingCommitter { id, commits }), batch, false)
}

/// Build a `ChangesStream` of N create-op envelopes sharing one commit log.
fn make_n_envelopes(n: usize, commits: &Arc<TokioMutex<Vec<i64>>>) -> ChangesStream {
    let envelopes: Vec<Result<ChangeEnvelope, StreamError>> = (0..n)
        .map(|i| {
            let id = i64::try_from(i).expect("usize fits in i64");
            Ok(make_create_envelope(
                id,
                &format!("row-{i}"),
                Arc::clone(commits),
            ))
        })
        .collect();
    fstream::iter(envelopes).boxed()
}

/// Construct a Cayenne table provider backed by a temp `SQLite` metastore and
/// temp data directory.
async fn setup_cayenne(
    table_name: &str,
) -> (
    TempDir,
    Arc<CayenneCatalog>,
    Arc<CayenneTableProvider>,
    Arc<Schema>,
) {
    let temp = TempDir::new().expect("temp dir");
    let data_path = temp.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = temp.path().join("test.db");
    let conn = format!("sqlite://{}", db_path.to_string_lossy());

    let catalog = Arc::new(CayenneCatalog::new(conn).expect("CayenneCatalog::new"));
    catalog.init().await.expect("catalog init");

    let schema = data_schema();
    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(
        Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: table_name.to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await
    .expect("create_table");

    (temp, catalog, Arc::new(table), schema)
}

/// Build a `RefreshTask` whose accelerator and federated source both point at
/// the given Cayenne table. The federated reference is unused by the changes
/// path but `RefreshTaskBuilder` requires one.
fn make_refresh_task(
    accelerator: Arc<dyn TableProvider>,
    table_name: &str,
) -> runtime::accelerated_table::refresh_task::RefreshTask {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cdc_into_cayenne_data_inlining_e2e() {
    // Sixteen envelopes: small enough that the Cayenne sink should
    // store them as inlined data in the metastore (well under the 1024-row
    // inlining threshold).
    const N: usize = 16;

    let (_temp, catalog, table, _schema) = setup_cayenne("cdc_inline_e2e").await;
    let table_id = catalog
        .get_table("cdc_inline_e2e")
        .await
        .expect("get_table")
        .table_id;

    let task = make_refresh_task(
        Arc::clone(&table) as Arc<dyn TableProvider>,
        "cdc_inline_e2e",
    );
    let commits = Arc::new(TokioMutex::new(Vec::new()));
    let stream = make_n_envelopes(N, &commits);

    let refresh = Arc::new(RwLock::new(Refresh::default()));
    task.start_changes_stream(
        refresh,
        stream,
        None,
        None,
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("start_changes_stream");

    // Every envelope's commit should have fired exactly once, in stream order.
    let expected_commits: Vec<i64> = (0..N)
        .map(|i| i64::try_from(i).expect("usize fits in i64"))
        .collect();
    assert_eq!(
        *commits.lock().await,
        expected_commits,
        "every envelope must be committed once, in stream order"
    );

    // All rows must be visible via the Cayenne provider's scan path.
    assert_eq!(count_rows(&table).await, N, "all CDC rows must land");

    // Data-inlining path: the Cayenne metastore should report the rows are
    // stored inline (not as on-disk Parquet/Vortex files).
    let inlined = catalog
        .get_inlined_data_count(&table_id)
        .await
        .expect("get_inlined_data_count");
    assert_eq!(
        inlined,
        i64::try_from(N).expect("N fits in i64"),
        "small CDC bursts must take the data-inlining path"
    );
}
