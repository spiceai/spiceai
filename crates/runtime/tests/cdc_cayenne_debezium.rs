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

//! End-to-end regression coverage for **Debezium (CDC over Kafka) into a Cayenne
//! accelerator** — the scenario tracked by
//! <https://github.com/spiceai/spiceai/issues/9688>.
//!
//! The Debezium data connector turns each Debezium change event into a
//! [`ChangeEnvelope`] carrying a `{op, primary_keys, data}` [`ChangeBatch`]
//! (`op` ∈ `c`/`u`/`d`/`r`; see [`data_components::debezium`]) and feeds a
//! [`ChangesStream`] to [`RefreshTask::start_changes_stream`] — exactly the same
//! envelope shape every other CDC connector (Postgres WAL, `DynamoDB` Streams,
//! `MongoDB` Change Streams) produces. From Cayenne's perspective the source is
//! irrelevant: these tests build envelopes in the precise wire shape the
//! Debezium connector emits and drive them through the *real* runtime apply
//! loop into a primary-keyed, upsert Cayenne table.
//!
//! This exercises the operations that distinguish full CDC support from plain
//! append — `UPDATE` (upsert) and `DELETE` (keyed tombstone) — and their
//! convergence within and across coalesced bursts. The companion
//! `cdc_cayenne_inline.rs` covers the create-only inlining path; the
//! `crates/cayenne/tests/*` suite covers upsert/delete at the provider level
//! below the changes stream. This file is the missing seam: Debezium-shaped
//! `c`/`u`/`d` envelopes end-to-end through `start_changes_stream` into Cayenne.
//!
//! Debezium requires Kafka message keys (a primary key) for every accelerator
//! other than Arrow (see `dataconnector::debezium`), so the table under test is
//! always primary-keyed with `OnConflict::Upsert`, mirroring how the runtime
//! configures a keyed `refresh_mode: changes` Cayenne dataset.

#![cfg(not(windows))]
#![recursion_limit = "256"]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use async_trait::async_trait;
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use data_components::cdc::{
    ChangeBatch, ChangeEnvelope, ChangesStream, CommitChange, CommitError, StreamError,
    changes_schema,
};
#[cfg(feature = "debezium")]
use data_components::debezium::arrow::changes::to_change_batch;
#[cfg(feature = "debezium")]
use data_components::debezium::change_event::ChangeEvent;
#[cfg(feature = "debezium")]
use data_components::schema_projection::SchemaProjection;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use futures::StreamExt;
use futures::stream as fstream;
use runtime::accelerated::refresh::Refresh;
use runtime::accelerated::refresh_task::RefreshTaskBuilder;
use runtime::federated::FederatedTable;
use runtime::status::RuntimeStatus;
use tempfile::TempDir;
use tokio::runtime::Handle;
use tokio::sync::{Mutex as TokioMutex, RwLock};

/// Schema of the table under test: an `Int64` primary key and a nullable string.
fn data_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// Records each committed envelope id so a test can assert every source offset
/// is committed exactly once, in stream order.
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

/// Build a single-row Debezium-style [`ChangeEnvelope`] for `(id, name)` with the
/// given operation (`"c"`, `"u"`, or `"d"`). `seq` is the source offset recorded
/// on commit. For a `"d"` (delete) Debezium carries the row's before-image; only
/// the primary key matters to the keyed apply path, but we populate `name` to
/// stay faithful to the wire shape.
fn make_envelope(
    op: &str,
    id: i64,
    name: Option<&str>,
    seq: i64,
    commits: &Arc<TokioMutex<Vec<i64>>>,
) -> ChangeEnvelope {
    let data = data_schema();
    let wrapper = changes_schema(&data);

    let op_array: ArrayRef = Arc::new(StringArray::from(vec![op]));

    // primary_keys: List<Utf8> with one entry "id" (the Kafka message key column).
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
    let name_array: ArrayRef = Arc::new(StringArray::from(vec![name]));
    let data_array: ArrayRef = Arc::new(StructArray::from(vec![
        (Arc::new(Field::new("id", DataType::Int64, false)), id_array),
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            name_array,
        ),
    ]));

    let record = RecordBatch::try_new(Arc::new(wrapper), vec![op_array, pk_array, data_array])
        .expect("wrapper RecordBatch");
    // Debezium stamps each batch with the upstream commit time (`source.ts_ms`)
    // via `ChangeBatch::with_source_commit_ts_ms`
    // (crates/data_components/src/debezium_kafka.rs); the Cayenne apply path reads
    // it back through `source_commit_ts_ms()` for the replication-lag signal. Stamp
    // a non-None, monotonic value derived from the source offset so that
    // propagation path is exercised rather than left `None`.
    let batch = ChangeBatch::try_new(record)
        .expect("ChangeBatch")
        .with_source_commit_ts_ms(Some(1_700_000_000_000 + seq));

    ChangeEnvelope::new(
        Box::new(RecordingCommitter {
            id: seq,
            commits: Arc::clone(commits),
        }),
        batch,
        // Mark every envelope dataset-ready, matching the real Debezium
        // connector, which emits `is_dataset_ready = true`
        // (crates/data_components/src/debezium_kafka.rs) so the runtime
        // transitions the dataset out of `AccelerationNotReady`.
        true,
    )
}

/// One step in a Debezium-style change stream: `(op, id, name, seq)`.
type Op<'a> = (&'a str, i64, Option<&'a str>, i64);

/// Build a [`ChangesStream`] from a list of `(op, id, name, seq)` steps that all
/// share one commit log.
fn stream_of(ops: &[Op<'_>], commits: &Arc<TokioMutex<Vec<i64>>>) -> ChangesStream {
    let envelopes: Vec<Result<ChangeEnvelope, StreamError>> = ops
        .iter()
        .map(|&(op, id, name, seq)| Ok(make_envelope(op, id, name, seq, commits)))
        .collect();
    fstream::iter(envelopes).boxed()
}

/// Construct a primary-keyed, upsert Cayenne table backed by a temp `SQLite`
/// metastore and temp data directory — the shape the runtime creates for a
/// keyed `refresh_mode: changes` Debezium dataset.
async fn setup_keyed_cayenne(
    table_name: &str,
) -> (TempDir, Arc<CayenneCatalog>, Arc<CayenneTableProvider>) {
    setup_keyed_cayenne_with_schema(table_name, data_schema()).await
}

/// Like [`setup_keyed_cayenne`] but with a caller-supplied data schema — e.g. the
/// decomposed `{id, data}` schema a `json_object` Debezium dataset exposes.
async fn setup_keyed_cayenne_with_schema(
    table_name: &str,
    schema: Arc<Schema>,
) -> (TempDir, Arc<CayenneCatalog>, Arc<CayenneTableProvider>) {
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
            schema,
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig::default(),
        },
        ctx.runtime_env(),
    )
    .await
    .expect("create_table");

    (temp, catalog, Arc::new(table))
}

/// Build a [`RefreshTask`] whose accelerator and (unused) federated source both
/// point at the given Cayenne table.
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

/// Drive one finite Debezium-style change stream to completion against `table`,
/// returning the recorded commit-offset sequence.
async fn apply_stream(
    table: &Arc<CayenneTableProvider>,
    table_name: &str,
    ops: &[Op<'_>],
) -> Vec<i64> {
    let task = make_refresh_task(Arc::clone(table) as Arc<dyn TableProvider>, table_name);
    let commits = Arc::new(TokioMutex::new(Vec::new()));
    let stream = stream_of(ops, &commits);
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
    commits.lock().await.clone()
}

/// Scan the table and return its rows as sorted `(id, name)` pairs.
async fn scan_rows(table: &Arc<CayenneTableProvider>) -> Vec<(i64, Option<String>)> {
    let ctx = SessionContext::new();
    let plan = table
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan");
    let batches = collect(plan, ctx.task_ctx()).await.expect("collect");

    let mut rows: Vec<(i64, Option<String>)> = Vec::new();
    for batch in &batches {
        let id_col = batch
            .column_by_name("id")
            .expect("id column")
            .as_primitive::<Int64Type>();
        let name_col = batch
            .column_by_name("name")
            .expect("name column")
            .as_string::<i32>();
        for row in 0..batch.num_rows() {
            let name = if name_col.is_null(row) {
                None
            } else {
                Some(name_col.value(row).to_string())
            };
            rows.push((id_col.value(row), name));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows
}

/// Insert, update (upsert), and delete inside a single stream must converge to
/// the correct final state. A coalesced burst is split into contiguous
/// same-operation sub-runs and applied in source order, so the `UPDATE` on `id=2`
/// replaces its row and the `DELETE` on `id=3` removes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debezium_insert_update_delete_converges() {
    let (_temp, _catalog, table) = setup_keyed_cayenne("dbz_iud").await;

    let commits = apply_stream(
        &table,
        "dbz_iud",
        &[
            ("c", 1, Some("alice"), 0),
            ("c", 2, Some("bob"), 1),
            ("c", 3, Some("carol"), 2),
            ("u", 2, Some("bob-v2"), 3), // UPDATE id=2
            ("d", 3, Some("carol"), 4),  // DELETE id=3 (before-image carried)
        ],
    )
    .await;

    assert_eq!(
        commits,
        vec![0, 1, 2, 3, 4],
        "every Debezium offset must be committed once, in stream order"
    );
    assert_eq!(
        scan_rows(&table).await,
        vec![
            (1, Some("alice".to_string())),
            (2, Some("bob-v2".to_string())),
        ],
        "id=2 must reflect the UPDATE and id=3 must be DELETEd"
    );
}

/// The realistic Debezium timeline: rows are created, the stream ends (offsets
/// committed, like a restart), then a later stream updates and deletes rows that
/// are already durable in the accelerator. This exercises the cross-burst
/// upsert/delete path against persisted Cayenne state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debezium_update_delete_across_streams() {
    let (_temp, _catalog, table) = setup_keyed_cayenne("dbz_cross").await;

    // First stream: snapshot/create three rows.
    apply_stream(
        &table,
        "dbz_cross",
        &[
            ("c", 10, Some("ten"), 0),
            ("c", 20, Some("twenty"), 1),
            ("c", 30, Some("thirty"), 2),
        ],
    )
    .await;
    assert_eq!(scan_rows(&table).await.len(), 3, "three rows after creates");

    // Second stream (e.g. after a restart): update one row, delete another.
    apply_stream(
        &table,
        "dbz_cross",
        &[
            ("u", 20, Some("twenty-v2"), 3),
            ("d", 30, Some("thirty"), 4),
        ],
    )
    .await;

    assert_eq!(
        scan_rows(&table).await,
        vec![
            (10, Some("ten".to_string())),
            (20, Some("twenty-v2".to_string())),
        ],
        "cross-stream UPDATE/DELETE must apply to already-persisted rows"
    );
}

/// At-least-once delivery means Debezium can redeliver events Spice already
/// applied (e.g. a crash before the source offset was acknowledged). Because the
/// keyed apply is primary-key idempotent, re-applying the same creates must not
/// duplicate rows — the exactly-once guarantee the connector relies on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debezium_redelivered_events_are_idempotent() {
    let (_temp, _catalog, table) = setup_keyed_cayenne("dbz_idem").await;

    let ops = [
        ("c", 1, Some("a"), 0),
        ("c", 2, Some("b"), 1),
        ("c", 3, Some("c"), 2),
    ];

    apply_stream(&table, "dbz_idem", &ops).await;
    assert_eq!(
        scan_rows(&table).await.len(),
        3,
        "three rows after first apply"
    );

    // Redeliver the identical events.
    apply_stream(&table, "dbz_idem", &ops).await;

    assert_eq!(
        scan_rows(&table).await,
        vec![
            (1, Some("a".to_string())),
            (2, Some("b".to_string())),
            (3, Some("c".to_string())),
        ],
        "redelivered creates must upsert in place, not duplicate primary keys"
    );
}

/// Delete-then-reinsert of the same primary key ("resurrection"). Sequence
/// numbering must let the later `INSERT` win over the earlier `DELETE` tombstone
/// rather than the row staying deleted — the delete/re-insert ordering Cayenne's
/// fused tombstone entries are designed to resolve.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debezium_delete_then_reinsert_resurrects_row() {
    let (_temp, _catalog, table) = setup_keyed_cayenne("dbz_resurrect").await;

    let commits = apply_stream(
        &table,
        "dbz_resurrect",
        &[
            ("c", 1, Some("v1"), 0),
            ("d", 1, Some("v1"), 1), // tombstone id=1
            ("c", 1, Some("v2"), 2), // re-insert id=1 with a new value
        ],
    )
    .await;

    assert_eq!(commits, vec![0, 1, 2]);
    assert_eq!(
        scan_rows(&table).await,
        vec![(1, Some("v2".to_string()))],
        "the re-insert after the delete must win; id=1 is present with v2"
    );
}

/// Build a Debezium `create` change event with the given `after` payload, in the
/// on-the-wire envelope shape the connector deserializes. The embedded `schema`
/// block is intentionally minimal: `to_change_batch` projects against the
/// caller-supplied table schema, not the event's self-description.
#[cfg(feature = "debezium")]
fn debezium_create_event(after: &serde_json::Value) -> ChangeEvent {
    let value = serde_json::json!({
        "schema": { "type": "struct", "fields": [], "optional": false, "name": "test.Envelope" },
        "payload": {
            "before": null,
            "after": after,
            "source": {
                "version": "x", "connector": "x", "name": "x", "ts_ms": 0,
                "snapshot": "false", "db": "x", "table": "x"
            },
            "op": "c",
            "ts_ms": 0,
            "transaction": null
        }
    });
    serde_json::from_value(value).expect("valid Debezium change event")
}

/// JSON nesting (`json_object`) end-to-end for Debezium: real Debezium change
/// events are decomposed by the connector's `to_change_batch` — the declared `id`
/// stays a top-level column while every other `after` field folds into one
/// sorted-key JSON catch-all `data` column — then applied through the runtime
/// changes stream into a keyed Cayenne accelerator and read back via SQL.
#[cfg(feature = "debezium")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn debezium_json_nesting_folds_into_catch_all() {
    // The decomposed schema the dataset exposes: static `id` + catch-all `data`.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, true),
    ]));
    let (_temp, _catalog, table) =
        setup_keyed_cayenne_with_schema("dbz_json_nest", Arc::clone(&schema)).await;

    // `id` is the declared static (primary-key) column; every other `after` field
    // folds into `data`.
    let projection = SchemaProjection::nesting(vec!["id".to_string()], "data".to_string());
    let pk = ["id".to_string()];

    let events = [
        debezium_create_event(
            &serde_json::json!({ "id": 1, "email": "alice@example.com", "age": 30 }),
        ),
        debezium_create_event(
            &serde_json::json!({ "id": 2, "email": "bob@example.com", "age": 25 }),
        ),
    ];

    let commits = Arc::new(TokioMutex::new(Vec::new()));
    let envelopes: Vec<Result<ChangeEnvelope, StreamError>> = events
        .iter()
        .enumerate()
        .map(|(seq, event)| {
            // The real decomposition: fold non-declared `after` fields into `data`.
            let batch = to_change_batch(&schema, &pk, event, Some(&projection))
                .expect("Debezium change event should decompose into a change batch");
            Ok(ChangeEnvelope::new(
                Box::new(RecordingCommitter {
                    id: i64::try_from(seq).expect("seq fits i64"),
                    commits: Arc::clone(&commits),
                }),
                batch,
                true,
            ))
        })
        .collect();
    let stream = fstream::iter(envelopes).boxed();

    let task = make_refresh_task(
        Arc::clone(&table) as Arc<dyn TableProvider>,
        "dbz_json_nest",
    );
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

    // Read the accelerated table back and assert the decomposition.
    let ctx = SessionContext::new();
    let plan = table
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan");
    let batches = collect(plan, ctx.task_ctx()).await.expect("collect");

    let mut rows: Vec<(i64, serde_json::Value)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .expect("id column")
            .as_primitive::<Int64Type>();
        let data = batch
            .column_by_name("data")
            .expect("catch-all data column")
            .as_string::<i32>();
        for row in 0..batch.num_rows() {
            let catch_all: serde_json::Value =
                serde_json::from_str(data.value(row)).expect("catch-all must be valid JSON");
            rows.push((ids.value(row), catch_all));
        }
    }
    rows.sort_by_key(|(id, _)| *id);

    assert_eq!(rows.len(), 2, "two decomposed rows");

    let (id0, data0) = &rows[0];
    assert_eq!(*id0, 1);
    assert_eq!(data0["email"], serde_json::json!("alice@example.com"));
    assert!(
        data0.get("age").is_some(),
        "non-declared `age` must fold into the catch-all"
    );
    assert!(
        data0.get("id").is_none(),
        "declared static `id` must not leak into the catch-all"
    );

    let (id1, data1) = &rows[1];
    assert_eq!(*id1, 2);
    assert_eq!(data1["email"], serde_json::json!("bob@example.com"));
    assert!(data1.get("id").is_none());
}
