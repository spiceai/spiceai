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
//! Integration tests for *shared* `MySQL` binlog replication: multiple
//! `refresh_mode: changes` datasets on the same connection are automatically
//! multiplexed onto one binlog dump connection and one `server_id` (no opt-in,
//! no group label — sharing is keyed by connection identity), with decoded
//! transactions routed per `(database, table)` to each member.
//!
//! Covers the acceptance criteria for the shared binlog connection (the `MySQL`
//! analog of `postgres/replication_shared.rs`):
//! - initial snapshot of multiple member tables on one shared dump
//! - per-table change routing (a table's changes reach only its member)
//! - one source transaction fanning out to multiple members
//! - a late-added member snapshots itself without disturbing the others
//! - restart-resume from the shared minimum position (the held ack floor
//!   replays each member's gap idempotently)
//! - a second dataset naming an already-subscribed source table is rejected
//!
//! Like `replication.rs`, these drive the library stream directly rather than
//! the full `AcceleratedTable` refresh loop, so per-table routing and the shared
//! ack floor can be asserted precisely from `ChangeBatch`es.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use data_components::cdc::{ChangeEnvelope, ChangesStream, StreamError};
use data_components::cdc::{InitialSnapshotMode, InvalidCheckpointBehavior};
use data_components::mysql_replication::{
    CursorType, PersistedPosition, PositionStore, ReplicationMetricsCollector, ReplicationParams,
    ReplicationStreamInput, StoreError, start_replication_stream,
};
use futures::StreamExt;
use mysql_async::prelude::Queryable;

use crate::init_tracing;
use crate::mysql::common;

// Distinct from the other MySQL suites (comments 13320, e2e 13321/13322/13323,
// per-dataset replication 13324(+1,+2,+5), refresh_retry 13327, schema_inference
// 13328, rehydration 13337) so parallel test binaries never fight over a
// container.
const MYSQL_SHARED_PORT: u16 = 13340;

/// In-memory [`PositionStore`] standing in for one member's accelerator sidecar.
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

/// Params for a shared-dump member. Sharing is always-on and keyed by
/// connection identity, so every dataset built with the same `port`/user/pass/
/// TLS and the same `server_id` coalesces onto one binlog dump. Tests pass one
/// `server_id` per connection group; a distinct `server_id` (or port) yields a
/// separate dump.
fn shared_params(port: u16, server_id: u32) -> ReplicationParams {
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
        // Short interval so the shared pump persists each member's position
        // quickly and the resume test doesn't have to wait.
        checkpoint_interval: Duration::from_secs(1),
        invalid_position_behavior: InvalidCheckpointBehavior::Error,
        ready_lag: Duration::from_secs(2),
    }
}

fn stream_input(
    port: u16,
    server_id: u32,
    table: &str,
    store: Arc<dyn PositionStore>,
) -> ReplicationStreamInput {
    let schema = dataset_schema();
    let schema_json = serde_json::to_string(schema.as_ref())
        .expect("dataset schema must serialize for checkpoint meta");
    ReplicationStreamInput {
        dataset_name: table.to_string(),
        params: shared_params(port, server_id),
        schema,
        primary_keys: vec!["id".into()],
        database: "mysqldb".into(),
        table: table.to_string(),
        position_store: store,
        schema_json: Some(schema_json),
        metrics: ReplicationMetricsCollector::new(),
    }
}

async fn exec(pool: &mysql_async::Pool, sql: &str) -> Result<(), anyhow::Error> {
    let mut conn = pool.get_conn().await?;
    conn.query_drop(sql)
        .await
        .map_err(|e| anyhow::anyhow!("mysql error running `{sql}`: {e}"))?;
    Ok(())
}

/// Create a fresh `(id INT PRIMARY KEY, name TEXT)` source table and seed it.
async fn setup_table(
    pool: &mysql_async::Pool,
    table: &str,
    rows: &[(i32, &str)],
) -> Result<(), anyhow::Error> {
    exec(
        pool,
        &format!("CREATE TABLE IF NOT EXISTS {table} (id INT PRIMARY KEY, name TEXT)"),
    )
    .await?;
    exec(pool, &format!("TRUNCATE TABLE {table}")).await?;
    for (id, name) in rows {
        exec(
            pool,
            &format!("INSERT INTO {table} VALUES ({id}, '{name}')"),
        )
        .await?;
    }
    Ok(())
}

/// Pull the next envelope, timing out with `context` if the stream stalls.
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

fn num_rows(envelope: &ChangeEnvelope) -> usize {
    envelope
        .change_batch()
        .expect("built change batch")
        .record
        .num_rows()
}

/// Pull the next envelope carrying rows, committing and skipping the zero-row
/// idle heartbeats that interleave on a caught-up live stream.
async fn next_change_envelope(
    stream: &mut ChangesStream,
    context: &str,
) -> Result<ChangeEnvelope, anyhow::Error> {
    let deadline = std::time::Instant::now() + Duration::from_mins(1);
    loop {
        let envelope = next_envelope(stream, context).await?;
        if num_rows(&envelope) > 0 {
            return Ok(envelope);
        }
        envelope.commit().await?;
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "only idle heartbeats arrived while waiting for {context}"
        );
    }
}

/// Poll until an envelope reports `is_dataset_ready`, committing everything seen
/// along the way (a caught-up idle heartbeat or a fresh in-lag commit).
async fn wait_for_ready(stream: &mut ChangesStream, context: &str) -> Result<(), anyhow::Error> {
    let deadline = std::time::Instant::now() + Duration::from_mins(1);
    loop {
        let envelope = next_envelope(stream, context).await?;
        if envelope.is_dataset_ready() {
            envelope.commit().await?;
            return Ok(());
        }
        envelope.commit().await?;
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "dataset never reached Ready while waiting for {context}"
        );
    }
}

fn ops_of(envelope: &ChangeEnvelope) -> Vec<String> {
    let batch = envelope.change_batch().expect("built change batch");
    let ops = batch
        .record
        .column_by_name("op")
        .expect("op column")
        .as_string::<i32>();
    (0..batch.record.num_rows())
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

/// Drain one member's cold-bootstrap head: the truncate barrier, the snapshot
/// batch (whose ids must equal `expected_ids`), and the zero-row boundary.
async fn drain_bootstrap(
    stream: &mut ChangesStream,
    what: &str,
    expected_ids: &[i32],
) -> Result<(), anyhow::Error> {
    let env = next_envelope(stream, &format!("{what} truncate barrier")).await?;
    assert_eq!(
        ops_of(&env),
        vec!["t"],
        "{what}: cold bootstrap opens with a truncate barrier"
    );
    env.commit().await?;

    let env = next_envelope(stream, &format!("{what} snapshot rows")).await?;
    assert_eq!(ids_of(&env), expected_ids, "{what}: snapshot ids");
    let expected_ops: Vec<String> = expected_ids.iter().map(|_| "c".to_string()).collect();
    assert_eq!(
        ops_of(&env),
        expected_ops,
        "{what}: snapshot rows are all creates"
    );
    env.commit().await?;

    let env = next_envelope(stream, &format!("{what} snapshot boundary")).await?;
    assert_eq!(
        num_rows(&env),
        0,
        "{what}: snapshot boundary carries no rows"
    );
    assert!(
        !env.is_dataset_ready(),
        "{what}: snapshot boundary is not ready; readiness is lag-based"
    );
    env.commit().await?;
    Ok(())
}

/// Drain one member's rebuild head: the single zero-row signal that asks the
/// consumer to replace the acceleration's contents from the source.
///
/// Unlike [`drain_bootstrap`] there are no rows to assert here, and that is the
/// point — a member whose acceleration already holds rows must not empty it and
/// stream a fresh snapshot into it, because every query for the length of that
/// re-read would be answered from an empty, then partially filled, table. The
/// re-read moves to the consumer's atomic overwrite
/// (`ChangeEnvelope::history_unavailable`), so the resulting contents are
/// asserted where that path is covered, not here.
/// The idle heartbeats a caught-up GTID stream interleaves are zero-row too, so
/// the signal is found by its flag rather than by position. Any envelope
/// carrying rows before it is the failure this guards: that is a truncate or a
/// snapshot batch, i.e. the acceleration being emptied and refilled.
async fn drain_rebuild(stream: &mut ChangesStream, what: &str) -> Result<(), anyhow::Error> {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let env = next_envelope(stream, &format!("{what} rebuild signal")).await?;
        anyhow::ensure!(
            num_rows(&env) == 0,
            "{what}: the acceleration must be replaced atomically, but rows arrived on the \
             member's channel before any rebuild signal (ops: {:?})",
            ops_of(&env)
        );
        if env.history_unavailable() {
            assert!(
                !env.is_dataset_ready(),
                "{what}: the rebuild signal is not ready; readiness is lag-based"
            );
            env.commit().await?;
            return Ok(());
        }
        env.commit().await?;
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "{what}: never received a rebuild signal"
        );
    }
}

/// Read the next change on `stream`, asserting it is a single row with `op`/`id`,
/// then commit it.
async fn expect_single_change(
    stream: &mut ChangesStream,
    what: &str,
    op: &str,
    id: i32,
) -> Result<(), anyhow::Error> {
    let env = next_change_envelope(stream, what).await?;
    assert_eq!(ops_of(&env), vec![op.to_string()], "{what}: op");
    assert_eq!(ids_of(&env), vec![id], "{what}: id");
    env.commit().await?;
    Ok(())
}

/// Multiplex + per-table routing + a single source transaction fanning out to
/// two members of one shared dump.
#[tokio::test(flavor = "multi_thread")]
async fn shared_group_multiplexes_and_routes_per_table() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::mysql_replication=debug,info"));

    let port = MYSQL_SHARED_PORT;
    let _container = common::start_mysql_docker_container(port).await?;
    let pool = common::get_mysql_conn(port)?;
    setup_table(&pool, "shared_a", &[(1, "a1"), (2, "a2")]).await?;
    setup_table(&pool, "shared_b", &[(1, "b1"), (2, "b2"), (3, "b3")]).await?;

    let server_id = 210_001;
    let store_a: Arc<dyn PositionStore> = Arc::new(MemoryPositionStore::default());
    let store_b: Arc<dyn PositionStore> = Arc::new(MemoryPositionStore::default());

    // Two datasets join the SAME group -> one shared dump, per-member snapshot.
    let mut stream_a = start_replication_stream(stream_input(
        port,
        server_id,
        "shared_a",
        Arc::clone(&store_a),
    ));
    drain_bootstrap(&mut stream_a, "member a", &[1, 2]).await?;

    let mut stream_b = start_replication_stream(stream_input(
        port,
        server_id,
        "shared_b",
        Arc::clone(&store_b),
    ));
    drain_bootstrap(&mut stream_b, "member b", &[1, 2, 3]).await?;

    wait_for_ready(&mut stream_a, "member a readiness").await?;
    wait_for_ready(&mut stream_b, "member b readiness").await?;

    // --- Per-table routing: each table's change reaches only its member. ---
    exec(&pool, "INSERT INTO shared_a VALUES (3, 'a3')").await?;
    expect_single_change(&mut stream_a, "a insert routed to a", "c", 3).await?;

    exec(&pool, "INSERT INTO shared_b VALUES (4, 'b4')").await?;
    // b's next change is its own insert (4) — a's insert (3) never routed here,
    // proving per-table routing.
    expect_single_change(&mut stream_b, "b insert routed to b", "c", 4).await?;

    // --- One source transaction touching both tables fans out per member. ---
    let mut txn = pool.get_conn().await?;
    txn.query_drop("START TRANSACTION").await?;
    txn.query_drop("INSERT INTO shared_a VALUES (5, 'a5')")
        .await?;
    txn.query_drop("INSERT INTO shared_b VALUES (6, 'b6')")
        .await?;
    txn.query_drop("COMMIT").await?;
    drop(txn);

    expect_single_change(&mut stream_a, "txn fan-out to a", "c", 5).await?;
    expect_single_change(&mut stream_b, "txn fan-out to b", "c", 6).await?;

    drop(stream_a);
    drop(stream_b);
    pool.disconnect().await?;
    Ok(())
}

/// A late-added member snapshots its own table on the running shared dump
/// without disturbing the member already streaming.
#[tokio::test(flavor = "multi_thread")]
async fn shared_group_late_join_snapshots_independently() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::mysql_replication=debug,info"));

    let port = MYSQL_SHARED_PORT + 1;
    let _container = common::start_mysql_docker_container(port).await?;
    let pool = common::get_mysql_conn(port)?;
    setup_table(&pool, "late_a", &[(1, "a1"), (2, "a2")]).await?;
    setup_table(&pool, "late_b", &[(1, "b1"), (2, "b2")]).await?;

    let server_id = 210_101;
    let store_a: Arc<dyn PositionStore> = Arc::new(MemoryPositionStore::default());
    let store_b: Arc<dyn PositionStore> = Arc::new(MemoryPositionStore::default());

    // Only member a joins first and reaches Ready.
    let mut stream_a = start_replication_stream(stream_input(
        port,
        server_id,
        "late_a",
        Arc::clone(&store_a),
    ));
    drain_bootstrap(&mut stream_a, "member a", &[1, 2]).await?;
    wait_for_ready(&mut stream_a, "member a readiness").await?;

    // A live change on a streams before b exists.
    exec(&pool, "INSERT INTO late_a VALUES (3, 'a3')").await?;
    expect_single_change(&mut stream_a, "pre-join a insert", "c", 3).await?;

    // Member b joins late: it snapshots its own table on the running dump.
    let mut stream_b = start_replication_stream(stream_input(
        port,
        server_id,
        "late_b",
        Arc::clone(&store_b),
    ));
    drain_bootstrap(&mut stream_b, "late member b", &[1, 2]).await?;
    wait_for_ready(&mut stream_b, "late member b readiness").await?;

    // After b's join (which forces a shared reconnect), a keeps streaming and b
    // receives its own changes.
    exec(&pool, "INSERT INTO late_a VALUES (7, 'a7')").await?;
    expect_single_change(&mut stream_a, "post-join a insert", "c", 7).await?;

    exec(&pool, "INSERT INTO late_b VALUES (8, 'b8')").await?;
    expect_single_change(&mut stream_b, "post-join b insert", "c", 8).await?;

    drop(stream_a);
    drop(stream_b);
    pool.disconnect().await?;
    Ok(())
}

/// Restart-resume: after both members persist their positions and the streams
/// are dropped, changes made while down replay from the shared minimum position
/// when both members rejoin the group.
#[tokio::test(flavor = "multi_thread")]
async fn shared_group_restart_resumes_from_min_position() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::mysql_replication=debug,info"));

    let port = MYSQL_SHARED_PORT + 2;
    let _container = common::start_mysql_docker_container(port).await?;
    let pool = common::get_mysql_conn(port)?;
    setup_table(&pool, "resume_a", &[(1, "a1"), (2, "a2")]).await?;
    setup_table(&pool, "resume_b", &[(1, "b1"), (2, "b2")]).await?;

    let store_a: Arc<dyn PositionStore> = Arc::new(MemoryPositionStore::default());
    let store_b: Arc<dyn PositionStore> = Arc::new(MemoryPositionStore::default());

    // --- Run 1: bootstrap both members, apply a live change to each, and let
    //            the shared pump persist their positions. ---
    {
        let mut stream_a = start_replication_stream(stream_input(
            port,
            210_201,
            "resume_a",
            Arc::clone(&store_a),
        ));
        drain_bootstrap(&mut stream_a, "run1 member a", &[1, 2]).await?;
        let mut stream_b = start_replication_stream(stream_input(
            port,
            210_201,
            "resume_b",
            Arc::clone(&store_b),
        ));
        drain_bootstrap(&mut stream_b, "run1 member b", &[1, 2]).await?;
        wait_for_ready(&mut stream_a, "run1 a readiness").await?;
        wait_for_ready(&mut stream_b, "run1 b readiness").await?;

        exec(&pool, "INSERT INTO resume_a VALUES (3, 'a3')").await?;
        expect_single_change(&mut stream_a, "run1 a insert", "c", 3).await?;
        exec(&pool, "INSERT INTO resume_b VALUES (4, 'b4')").await?;
        expect_single_change(&mut stream_b, "run1 b insert", "c", 4).await?;

        // Drive both streams until each member's position is persisted past its
        // bootstrap — the shared pump checkpoints on the interval only while the
        // (quiet) streams are polled, mirroring the runtime's apply loop.
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let _ = tokio::time::timeout(Duration::from_millis(250), stream_a.next()).await;
            let _ = tokio::time::timeout(Duration::from_millis(250), stream_b.next()).await;
            let a_ready = store_a.load().await.expect("store a").is_some();
            let b_ready = store_b.load().await.expect("store b").is_some();
            if a_ready && b_ready {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "shared pump never persisted both members' positions"
            );
        }
        drop(stream_a);
        drop(stream_b);
    }

    // --- Gap: changes made while no member is streaming. ---
    exec(&pool, "INSERT INTO resume_a VALUES (5, 'a5')").await?;
    exec(&pool, "INSERT INTO resume_b VALUES (6, 'b6')").await?;

    // --- Run 2: both members rejoin; the shared dump resumes from the min
    //            position and each member replays its own gap idempotently. ---
    let mut stream_a = start_replication_stream(stream_input(
        port,
        210_202,
        "resume_a",
        Arc::clone(&store_a),
    ));
    let mut stream_b = start_replication_stream(stream_input(
        port,
        210_202,
        "resume_b",
        Arc::clone(&store_b),
    ));

    // Each member re-sees its gap insert (earlier already-applied commits may
    // replay first under at-least-once — skip until the gap id arrives).
    wait_for_id(&mut stream_a, "resume replay a", 5).await?;
    wait_for_id(&mut stream_b, "resume replay b", 6).await?;

    drop(stream_a);
    drop(stream_b);
    pool.disconnect().await?;
    Ok(())
}

/// Poll a member's stream until an envelope carrying `target_id` arrives,
/// committing everything seen along the way.
async fn wait_for_id(
    stream: &mut ChangesStream,
    what: &str,
    target_id: i32,
) -> Result<(), anyhow::Error> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let env = next_change_envelope(stream, what).await?;
        let ids = ids_of(&env);
        env.commit().await?;
        if ids.contains(&target_id) {
            return Ok(());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "never received id {target_id} while waiting for {what}"
        );
    }
}

/// A second dataset naming an already-subscribed source table in the same group
/// is rejected — one source table backs at most one member per group.
#[tokio::test(flavor = "multi_thread")]
async fn shared_group_rejects_duplicate_source_table() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::mysql_replication=debug,info"));

    let port = MYSQL_SHARED_PORT + 3;
    let _container = common::start_mysql_docker_container(port).await?;
    let pool = common::get_mysql_conn(port)?;
    setup_table(&pool, "dup_a", &[(1, "a1")]).await?;

    let server_id = 210_301;

    let mut first = start_replication_stream(stream_input(
        port,
        server_id,
        "dup_a",
        Arc::new(MemoryPositionStore::default()),
    ));
    drain_bootstrap(&mut first, "first subscriber", &[1]).await?;
    wait_for_ready(&mut first, "first subscriber readiness").await?;

    // A second live subscription to the SAME table in the SAME group must error.
    let mut dup = start_replication_stream(stream_input(
        port,
        server_id,
        "dup_a",
        Arc::new(MemoryPositionStore::default()),
    ));
    let Err(err) = tokio::time::timeout(Duration::from_secs(30), dup.next())
        .await?
        .expect("duplicate subscription yields an item")
    else {
        anyhow::bail!("a duplicate source table on the same connection must be rejected")
    };
    assert!(
        err.to_string()
            .contains("already replicated by another dataset"),
        "error must name the duplicate-subscription cause, got: {err}"
    );

    drop(first);
    drop(dup);
    pool.disconnect().await?;
    Ok(())
}

/// Restore under GTID auto-positioning — the failover-safe path. On a
/// `gtid_mode = ON` source, a shared group bootstraps with GTID cursors
/// (`cursor_type = Gtid`, a persisted executed set), and after a restart resumes
/// via `COM_BINLOG_DUMP_GTID` from the intersection of members' executed sets,
/// replaying the gap with no dependence on binlog file+offset (which would not
/// survive a source promotion). This is the multi-member analog of the
/// per-dataset `replication_e2e::mysql_binlog_replication_gtid_resume_cayenne`.
#[tokio::test(flavor = "multi_thread")]
async fn shared_group_gtid_restore_resumes_via_executed_set() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::mysql_replication=debug,info"));

    let port = MYSQL_SHARED_PORT + 4;
    let _container = common::start_mysql_gtid_docker_container(port).await?;
    let pool = common::get_mysql_conn(port)?;
    // Confirm the source really issues GTIDs, else this would silently exercise
    // the file+offset path instead of GTID auto-positioning.
    {
        let mut conn = pool.get_conn().await?;
        let mode: String = conn
            .query_first::<String, _>("SELECT @@GLOBAL.gtid_mode")
            .await?
            .ok_or_else(|| anyhow::anyhow!("gtid_mode query returned no row"))?;
        anyhow::ensure!(
            mode == "ON",
            "container must run with gtid_mode = ON, got {mode}"
        );
    }
    setup_table(&pool, "gtid_a", &[(1, "a1"), (2, "a2")]).await?;
    setup_table(&pool, "gtid_b", &[(1, "b1"), (2, "b2")]).await?;

    let store_a: Arc<dyn PositionStore> = Arc::new(MemoryPositionStore::default());
    let store_b: Arc<dyn PositionStore> = Arc::new(MemoryPositionStore::default());

    // --- Run 1: bootstrap both members under GTID, apply a live change to each,
    //            and let the shared pump persist GTID checkpoints. ---
    {
        let mut stream_a =
            start_replication_stream(stream_input(port, 210_401, "gtid_a", Arc::clone(&store_a)));
        drain_bootstrap(&mut stream_a, "gtid run1 a", &[1, 2]).await?;
        let mut stream_b =
            start_replication_stream(stream_input(port, 210_401, "gtid_b", Arc::clone(&store_b)));
        drain_bootstrap(&mut stream_b, "gtid run1 b", &[1, 2]).await?;
        wait_for_ready(&mut stream_a, "gtid run1 a readiness").await?;
        wait_for_ready(&mut stream_b, "gtid run1 b readiness").await?;

        exec(&pool, "INSERT INTO gtid_a VALUES (3, 'a3')").await?;
        expect_single_change(&mut stream_a, "gtid run1 a insert", "c", 3).await?;
        exec(&pool, "INSERT INTO gtid_b VALUES (4, 'b4')").await?;
        expect_single_change(&mut stream_b, "gtid run1 b insert", "c", 4).await?;

        // Drive both streams until each member persists a checkpoint.
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let _ = tokio::time::timeout(Duration::from_millis(250), stream_a.next()).await;
            let _ = tokio::time::timeout(Duration::from_millis(250), stream_b.next()).await;
            if store_a.load().await.expect("store a").is_some()
                && store_b.load().await.expect("store b").is_some()
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "shared pump never persisted GTID checkpoints"
            );
        }

        // Both checkpoints must be GTID cursors carrying an executed set — proof
        // the shared pump positions by GTID (failover-safe), not file+offset.
        for (store, who) in [(&store_a, "a"), (&store_b, "b")] {
            let cp = store
                .load()
                .await
                .expect("store readable")
                .expect("member persisted a checkpoint");
            assert_eq!(
                cp.cursor_type,
                CursorType::Gtid,
                "member {who} must persist a GTID cursor"
            );
            let set = cp
                .gtid_set
                .expect("a GTID cursor must carry an executed set");
            assert!(
                !set.is_empty(),
                "member {who} executed set must be non-empty after live changes"
            );
        }

        drop(stream_a);
        drop(stream_b);
    }

    // --- Gap: changes made while no member is streaming. ---
    exec(&pool, "INSERT INTO gtid_a VALUES (5, 'a5')").await?;
    exec(&pool, "INSERT INTO gtid_b VALUES (6, 'b6')").await?;

    // --- Run 2: resume via GTID auto-positioning from the persisted executed
    //            sets. A fresh server_id (GTID resume is set-driven, not
    //            file+offset) matches a real process restart, which derives a new
    //            id — the gap must still replay with no gaps. ---
    let mut stream_a =
        start_replication_stream(stream_input(port, 210_402, "gtid_a", Arc::clone(&store_a)));
    let mut stream_b =
        start_replication_stream(stream_input(port, 210_402, "gtid_b", Arc::clone(&store_b)));
    wait_for_id(&mut stream_a, "gtid resume replay a", 5).await?;
    wait_for_id(&mut stream_b, "gtid resume replay b", 6).await?;

    drop(stream_a);
    drop(stream_b);
    pool.disconnect().await?;
    Ok(())
}

/// A single dataset on its own shared dump — the common production case where
/// sharing coalesces exactly one member. Verifies the always-on shared path
/// serves a lone member end-to-end: cold snapshot, then create/update/delete
/// each routed and committed. The multi-dataset fan-out is covered by
/// `shared_group_multiplexes_and_routes_per_table`.
#[tokio::test(flavor = "multi_thread")]
async fn shared_group_single_dataset_streams_snapshot_and_changes() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::mysql_replication=debug,info"));

    let port = MYSQL_SHARED_PORT + 5;
    let _container = common::start_mysql_docker_container(port).await?;
    let pool = common::get_mysql_conn(port)?;
    setup_table(&pool, "solo", &[(1, "s1"), (2, "s2")]).await?;

    let store: Arc<dyn PositionStore> = Arc::new(MemoryPositionStore::default());
    let mut stream =
        start_replication_stream(stream_input(port, 210_501, "solo", Arc::clone(&store)));
    drain_bootstrap(&mut stream, "solo member", &[1, 2]).await?;
    wait_for_ready(&mut stream, "solo member readiness").await?;

    // Live create/update/delete each route to the single member and commit.
    exec(&pool, "INSERT INTO solo VALUES (3, 's3')").await?;
    expect_single_change(&mut stream, "solo insert", "c", 3).await?;

    exec(&pool, "UPDATE solo SET name = 's1b' WHERE id = 1").await?;
    expect_single_change(&mut stream, "solo update", "u", 1).await?;

    exec(&pool, "DELETE FROM solo WHERE id = 2").await?;
    expect_single_change(&mut stream, "solo delete", "d", 2).await?;

    drop(stream);
    pool.disconnect().await?;
    Ok(())
}

/// A purged resume position with `invalid_checkpoint_behavior: restart` must
/// rebuild the member in place instead of fatally erroring — for BOTH the
/// file+offset and GTID positioning paths (a purge surfaces as `MySQL` error 1236
/// in both, and recovery captures the head differently per mode). Regression
/// test for issue #11968 (restart was a no-op): the running pump's purge handler
/// now honors `invalid_position_behavior` rather than always broadcasting the
/// fatal purge error and stopping.
///
/// The two modes reach the rebuild by different routes, which is why both are
/// covered: file mode detects the purge when resolving the start position, so
/// the signal is the member's stream head; GTID mode resumes and is rejected by
/// the running pump's `COM_BINLOG_DUMP_GTID`, so the signal is delivered
/// mid-stream through the member's live channel.
#[tokio::test(flavor = "multi_thread")]
async fn shared_group_purged_position_restart_rebuilds_file() -> Result<(), anyhow::Error> {
    run_purged_position_restart(MYSQL_SHARED_PORT + 6, 210_601, false).await
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_group_purged_position_restart_rebuilds_gtid() -> Result<(), anyhow::Error> {
    run_purged_position_restart(MYSQL_SHARED_PORT + 7, 210_701, true).await
}

/// Drive the purged-position restart recovery. `gtid` selects the positioning
/// mode (a `gtid_mode = ON` container vs the default file+offset container).
///
/// Structure: subscription 1 bootstraps and persists a resume checkpoint, then
/// is dropped; we purge that checkpoint's binlogs from the source; subscription
/// 2 (a distinct `server_id`, so a fully independent source with no teardown
/// race) resumes from the shared store and must rebuild the acceleration rather
/// than fatally erroring. A purge surfaces as `MySQL` error 1236 in both
/// modes — file mode via the resolve-time file check, GTID mode via the running
/// pump's `COM_BINLOG_DUMP_GTID` rejection (the path issue #11968 fixed).
async fn run_purged_position_restart(
    port: u16,
    server_id: u32,
    gtid: bool,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::mysql_replication=debug,info"));

    let _container = if gtid {
        common::start_mysql_gtid_docker_container(port).await?
    } else {
        common::start_mysql_docker_container(port).await?
    };
    let pool = common::get_mysql_conn(port)?;
    setup_table(&pool, "purge_a", &[(1, "a1"), (2, "a2")]).await?;

    // Shared across both subscriptions: subscription 2 resumes from the
    // checkpoint subscription 1 persists (which we purge from the source
    // in between).
    let store: Arc<dyn PositionStore> = Arc::new(MemoryPositionStore::default());
    let subscribe = |sid: u32| {
        let mut input = stream_input(port, sid, "purge_a", Arc::clone(&store));
        // The behavior under test: recover a purged position by re-snapshotting.
        input.params.invalid_position_behavior = InvalidCheckpointBehavior::Restart;
        start_replication_stream(input)
    };

    // Subscription 1: bootstrap. The snapshot boundary persists the resume
    // checkpoint (position + executed GTID set) to the store; then drop it.
    {
        let mut stream = subscribe(server_id);
        drain_bootstrap(&mut stream, "initial bootstrap", &[1, 2]).await?;
        drop(stream);
    }

    // Purge the persisted checkpoint from the source: rotate past it, with a
    // change in each purged file so a GTID resume needs a purged transaction,
    // then drop the older binlogs. The persisted file no longer exists (file
    // mode) and the executed set now references purged GTIDs (GTID mode).
    exec(&pool, "INSERT INTO purge_a VALUES (3, 'a3')").await?;
    exec(&pool, "FLUSH BINARY LOGS").await?;
    exec(&pool, "INSERT INTO purge_a VALUES (4, 'a4')").await?;
    exec(&pool, "FLUSH BINARY LOGS").await?;
    exec(&pool, "INSERT INTO purge_a VALUES (5, 'a5')").await?;
    let current_file: String = {
        let mut conn = pool.get_conn().await?;
        let logs: Vec<mysql_async::Row> = conn.query("SHOW BINARY LOGS").await?;
        logs.last()
            .and_then(|r| r.get::<String, _>(0))
            .ok_or_else(|| anyhow::anyhow!("SHOW BINARY LOGS returned no rows"))?
    };
    exec(&pool, &format!("PURGE BINARY LOGS TO '{current_file}'")).await?;

    // Subscription 2: resume from the purged checkpoint. With restart, the
    // member must hand the consumer an atomic rebuild instead of fataling — and
    // instead of emptying the acceleration and streaming a fresh snapshot into
    // it, which every query would observe as an empty, then partial, table.
    let mut stream = subscribe(server_id + 1);
    drain_rebuild(&mut stream, "rebuild after purge").await?;

    drop(stream);
    pool.disconnect().await?;
    Ok(())
}
