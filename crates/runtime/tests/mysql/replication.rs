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
//! Integration tests for `MySQL` binlog replication.
//!
//! These exercise the end-to-end data path:
//! - Start a `MySQL` container (binary logging + ROW format + FULL row images
//!   are the `MySQL` 8+ defaults, so no special server flags are needed).
//! - Create a source table with a primary key.
//! - Kick off the replication stream via `data_components::mysql_replication`.
//! - Insert/update/delete/truncate rows and observe them arriving as
//!   `ChangeBatch`es, then drop the stream and resume from the persisted
//!   binlog position.
//!
//! The full `AcceleratedTable` refresh loop is deliberately skipped here —
//! that layer is covered by existing CDC apply tests. This focused test
//! proves the binlog→Arrow pipeline, mirroring `postgres/replication.rs`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use data_components::cdc::{ChangeEnvelope, ChangesStream, StreamError};
use data_components::cdc::{InitialSnapshotMode, InvalidCheckpointBehavior};
use data_components::mysql_replication::{
    PersistedPosition, PositionStore, ReplicationMetricsCollector, ReplicationParams,
    ReplicationStreamInput, StoreError, encode_checkpoint_schema_json, start_replication_stream,
};
use futures::StreamExt;
use mysql_async::prelude::Queryable;

use crate::init_tracing;
use crate::mysql::common;

// 13324/13325 (the purged-position test uses `+ 1`): distinct from the other
// MySQL suites (comments 13320, e2e 13322/13323, refresh_retry 13327,
// schema_inference 13328, rehydration 13337) so parallel test binaries
// never fight over a container.
const MYSQL_REPLICATION_PORT: u16 = 13324;

/// In-memory [`PositionStore`] standing in for the accelerator sidecar.
#[derive(Default)]
struct MemoryPositionStore {
    inner: Mutex<Option<PersistedPosition>>,
}

#[async_trait]
impl PositionStore for MemoryPositionStore {
    async fn load(&self) -> Result<Option<PersistedPosition>, StoreError> {
        Ok(self.inner.lock().expect("lock").clone())
    }
    async fn save(&self, position: &PersistedPosition) -> Result<(), StoreError> {
        *self.inner.lock().expect("lock") = Some(position.clone());
        Ok(())
    }
    async fn clear(&self) -> Result<(), StoreError> {
        *self.inner.lock().expect("lock") = None;
        Ok(())
    }
}

fn dataset_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::LargeUtf8, true),
    ]))
}

fn params_for(port: u16, server_id: u32) -> ReplicationParams {
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("localhost")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some(common::MYSQL_ROOT_PASSWORD))
        .db_name(Some("mysqldb"));
    ReplicationParams {
        opts: mysql_async::Opts::from(opts),
        server_id,
        snapshot_mode: InitialSnapshotMode::Auto,
        bootstrap_batch_size: 8192,
        // Short interval so idle heartbeats persist the position quickly and
        // the resume phase of the test doesn't have to wait.
        checkpoint_interval: Duration::from_secs(1),
        invalid_position_behavior: InvalidCheckpointBehavior::Error,
        ready_lag: Duration::from_secs(2),
    }
}

fn stream_input(
    port: u16,
    server_id: u32,
    store: Arc<dyn PositionStore>,
) -> ReplicationStreamInput {
    let schema = dataset_schema();
    let schema_json = serde_json::to_string(schema.as_ref())
        .expect("dataset schema must serialize for checkpoint meta");
    ReplicationStreamInput {
        dataset_name: "repl_users".into(),
        params: params_for(port, server_id),
        schema,
        primary_keys: vec!["id".into()],
        database: "mysqldb".into(),
        table: "repl_users".into(),
        position_store: store,
        schema_json: Some(schema_json),
        metrics: ReplicationMetricsCollector::new(),
    }
}

async fn setup_source_table(port: u16) -> Result<mysql_async::Pool, anyhow::Error> {
    let pool = common::get_mysql_conn(port)?;
    let mut conn = pool.get_conn().await?;
    conn.query_drop("CREATE TABLE IF NOT EXISTS repl_users (id INT PRIMARY KEY, name TEXT)")
        .await?;
    conn.query_drop("TRUNCATE TABLE repl_users").await?;
    conn.query_drop("INSERT INTO repl_users VALUES (1, 'Alice'), (2, 'Bob')")
        .await?;
    drop(conn);
    Ok(pool)
}

/// Pull the next envelope, panicking with `context` if the stream stalls.
async fn next_envelope(
    stream: &mut ChangesStream,
    context: &str,
) -> Result<ChangeEnvelope, anyhow::Error> {
    tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for {context}"))?
        .ok_or_else(|| anyhow::anyhow!("stream ended waiting for {context}"))?
        .map_err(|e: StreamError| anyhow::anyhow!("stream error waiting for {context}: {e}"))
}

fn ops_of(envelope: &ChangeEnvelope) -> Vec<String> {
    let ops = envelope
        .change_batch()
        .expect("built change batch")
        .record
        .column_by_name("op")
        .expect("op column")
        .as_string::<i32>();
    (0..envelope
        .change_batch()
        .expect("built change batch")
        .record
        .num_rows())
        .map(|i| ops.value(i).to_string())
        .collect()
}

fn ids_of(envelope: &ChangeEnvelope) -> Vec<i32> {
    let data = envelope
        .change_batch()
        .expect("built change batch")
        .record
        .column_by_name("data")
        .expect("data column")
        .as_struct();
    let ids = data
        .column_by_name("id")
        .expect("id column")
        .as_primitive::<arrow::datatypes::Int32Type>();
    (0..ids.len()).map(|i| ids.value(i)).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_then_stream_changes_then_resume() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::mysql_replication=debug,info"));

    let port = MYSQL_REPLICATION_PORT;
    let _container = common::start_mysql_docker_container(port).await?;
    let pool = setup_source_table(port).await?;
    let store: Arc<MemoryPositionStore> = Arc::new(MemoryPositionStore::default());

    let mut stream = start_replication_stream(stream_input(
        port,
        200_101,
        Arc::clone(&store) as Arc<dyn PositionStore>,
    ));

    // --- 1. Cold bootstrap: truncate barrier, snapshot rows, ready signal ---
    let envelope = next_envelope(&mut stream, "truncate barrier").await?;
    assert_eq!(ops_of(&envelope), vec!["t"]);
    envelope.commit().await?;

    let envelope = next_envelope(&mut stream, "snapshot rows").await?;
    assert_eq!(ops_of(&envelope), vec!["c", "c"]);
    assert_eq!(ids_of(&envelope), vec![1, 2]);
    assert!(!envelope.is_dataset_ready());
    envelope.commit().await?;

    let envelope = next_envelope(&mut stream, "ready signal").await?;
    assert_eq!(
        envelope
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        0
    );
    assert!(
        envelope.is_dataset_ready(),
        "post-snapshot envelope marks ready"
    );
    envelope.commit().await?;
    let bootstrap_position = store
        .load()
        .await
        .expect("store readable")
        .expect("ready commit persists the bootstrap position")
        .position;

    // --- 2. Live insert ---
    let mut conn = pool.get_conn().await?;
    conn.query_drop("INSERT INTO repl_users VALUES (3, 'Charlie')")
        .await?;
    let envelope = next_envelope(&mut stream, "insert envelope").await?;
    assert_eq!(ops_of(&envelope), vec!["c"]);
    assert_eq!(ids_of(&envelope), vec![3]);
    envelope.commit().await?;

    // --- 3. Live update ---
    conn.query_drop("UPDATE repl_users SET name = 'Alicia' WHERE id = 1")
        .await?;
    let envelope = next_envelope(&mut stream, "update envelope").await?;
    assert_eq!(ops_of(&envelope), vec!["u"]);
    assert_eq!(ids_of(&envelope), vec![1]);
    let data = envelope
        .change_batch()
        .expect("built change batch")
        .record
        .column_by_name("data")
        .expect("data")
        .as_struct();
    let names = data
        .column_by_name("name")
        .expect("name")
        .as_string::<i64>();
    assert_eq!(names.value(0), "Alicia");
    envelope.commit().await?;

    // --- 4. Primary-key update becomes delete(old) + update(new) ---
    conn.query_drop("UPDATE repl_users SET id = 20 WHERE id = 2")
        .await?;
    let envelope = next_envelope(&mut stream, "pk-change envelope").await?;
    assert_eq!(ops_of(&envelope), vec!["d", "u"]);
    assert_eq!(ids_of(&envelope), vec![2, 20]);
    envelope.commit().await?;

    // --- 5. Live delete ---
    conn.query_drop("DELETE FROM repl_users WHERE id = 20")
        .await?;
    let envelope = next_envelope(&mut stream, "delete envelope").await?;
    assert_eq!(ops_of(&envelope), vec!["d"]);
    assert_eq!(ids_of(&envelope), vec![20]);
    envelope.commit().await?;

    // --- 6. TRUNCATE arrives as a truncate op ---
    conn.query_drop("TRUNCATE TABLE repl_users").await?;
    let envelope = next_envelope(&mut stream, "truncate envelope").await?;
    assert_eq!(ops_of(&envelope), vec!["t"]);
    envelope.commit().await?;

    // --- 7. Idle checkpoint persists the acked position ---
    // The checkpointer runs inside the stream, so it only makes progress
    // while the stream is polled — which the runtime's apply loop does
    // continuously. Mirror that here: keep polling the (quiet) stream while
    // waiting for the persisted position to advance. No envelope arrives on
    // a quiet table, so each poll simply times out after driving the
    // stream's idle tick.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let _ = tokio::time::timeout(Duration::from_millis(250), stream.next()).await;
        let persisted = store
            .load()
            .await
            .expect("store readable")
            .expect("position persisted")
            .position;
        if persisted > bootstrap_position {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "idle checkpoint never advanced past the bootstrap position ({bootstrap_position})"
        );
    }

    // --- 8. Resume from the persisted position (no snapshot) ---
    drop(stream);
    conn.query_drop("INSERT INTO repl_users VALUES (5, 'Eve')")
        .await?;
    drop(conn);

    let mut stream = start_replication_stream(stream_input(
        port,
        200_102,
        Arc::clone(&store) as Arc<dyn PositionStore>,
    ));

    // First envelope on resume is the immediate ready signal — no truncate
    // and no snapshot batch.
    let envelope = next_envelope(&mut stream, "resume ready signal").await?;
    assert_eq!(
        envelope
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        0
    );
    assert!(envelope.is_dataset_ready());
    envelope.commit().await?;

    // The insert made while detached replays from the persisted position.
    // Depending on where the idle checkpoint landed, earlier already-applied
    // envelopes may replay first (at-least-once) — skip until id 5 arrives.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let envelope = next_envelope(&mut stream, "replayed insert envelope").await?;
        let ids = ids_of(&envelope);
        let ops = ops_of(&envelope);
        envelope.commit().await?;
        if ids.contains(&5) {
            let idx = ids.iter().position(|id| *id == 5).expect("id 5 present");
            assert_eq!(ops[idx], "c");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "never received the detached insert (id 5) after resume"
        );
    }

    pool.disconnect().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn purged_position_behavior() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::mysql_replication=debug,info"));

    let port = MYSQL_REPLICATION_PORT + 1;
    let _container = common::start_mysql_docker_container(port).await?;
    let pool = setup_source_table(port).await?;

    // A persisted position pointing at a binlog file the server never had —
    // the same shape as a real purge. Use a valid v2 checkpoint meta so the
    // resume path reaches the purged-file check (rather than failing early on
    // MissingCheckpointMeta).
    let mut layout_conn = pool.get_conn().await?;
    let layout = data_components::mysql_replication::setup::fetch_table_layout(
        &mut layout_conn,
        "mysqldb",
        "repl_users",
    )
    .await?;
    drop(layout_conn);
    let dataset_schema_json = serde_json::to_string(dataset_schema().as_ref())
        .expect("dataset schema must serialize for checkpoint meta");
    let stale_meta = encode_checkpoint_schema_json(Some(&dataset_schema_json), &layout)
        .expect("checkpoint meta must encode");
    let stale = PersistedPosition {
        position: data_components::mysql_replication::BinlogPosition::new("binlog.999999", 4),
        schema_json: Some(stale_meta),
    };

    // Default behavior (`error`): the stream surfaces an actionable error.
    let store: Arc<MemoryPositionStore> = Arc::new(MemoryPositionStore::default());
    store.save(&stale).await.expect("save stale position");
    let mut stream = start_replication_stream(stream_input(
        port,
        200_201,
        Arc::clone(&store) as Arc<dyn PositionStore>,
    ));
    let Err(err) = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await?
        .expect("stream yields an item")
    else {
        anyhow::bail!("stale position with `error` behavior must fail")
    };
    assert!(
        err.to_string().contains("rebootstrap"),
        "error must point at the recovery knob, got: {err}"
    );
    drop(stream);

    // `rebootstrap`: the stale position is dropped and a fresh snapshot runs.
    let store: Arc<MemoryPositionStore> = Arc::new(MemoryPositionStore::default());
    store.save(&stale).await.expect("save stale position");
    let mut input = stream_input(port, 200_202, Arc::clone(&store) as Arc<dyn PositionStore>);
    input.params.invalid_position_behavior = InvalidCheckpointBehavior::Restart;
    let mut stream = start_replication_stream(input);

    let envelope = next_envelope(&mut stream, "rebootstrap truncate barrier").await?;
    assert_eq!(ops_of(&envelope), vec!["t"]);
    let envelope = next_envelope(&mut stream, "rebootstrap snapshot").await?;
    assert_eq!(ops_of(&envelope), vec!["c", "c"]);
    let envelope = next_envelope(&mut stream, "rebootstrap ready").await?;
    assert!(envelope.is_dataset_ready());
    envelope.commit().await?;
    let repersisted = store
        .load()
        .await
        .expect("store readable")
        .expect("rebootstrap persists a fresh position")
        .position;
    assert_ne!(repersisted.file, "binlog.999999");

    pool.disconnect().await?;
    Ok(())
}
