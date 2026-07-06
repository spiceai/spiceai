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

#![allow(clippy::expect_used)]
//! Integration tests for postgres logical replication.
//!
//! These exercise the end-to-end data path:
//! - Start a Postgres container with `wal_level=logical`.
//! - Create a source table with a primary key.
//! - Kick off the replication stream via `data_components::postgres_replication`.
//! - Insert/update/delete rows and observe them arriving as `ChangeBatch`es.
//!
//! We deliberately skip the full `AcceleratedTable` refresh loop here — that
//! layer is covered by existing refresh tests with its own mocks. This focused
//! test proves the WAL→Arrow pipeline without the acceleration complexity.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use data_components::postgres_replication::{
    ReplicationMetricsCollector, ReplicationParams, ReplicationStreamInput, config,
    start_replication_stream,
};
use futures::StreamExt;
use secrecy::SecretString;
use tokio_postgres::{NoTls, error::SqlState};

use crate::init_tracing;
use crate::postgres::common;

fn dataset_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn big_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("blob", DataType::Utf8, true),
    ]))
}

fn params_for(port: u16, slot_name: &str, publication_name: &str) -> ReplicationParams {
    ReplicationParams {
        host: "localhost".into(),
        port,
        user: "postgres".into(),
        password: SecretString::from(common::PG_PASSWORD.to_string()),
        database: "postgres".into(),
        sslmode: config::SslMode::Disable,
        sslrootcert: None,
        slot_name: slot_name.into(),
        publication_name: publication_name.into(),
        initial_snapshot: true,
        snapshot_on_resume: false,
        temporary_slot: false,
        status_interval: Duration::from_secs(1),
        bootstrap_batch_size: 8192,
        shared: false,
        member_channel_capacity:
            data_components::postgres_replication::shared::DEFAULT_MEMBER_CHANNEL_CAPACITY,
    }
}

async fn setup_source_table(port: u16) -> Result<tokio_postgres::Client, anyhow::Error> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("localhost")
        .port(port)
        .user("postgres")
        .password(common::PG_PASSWORD)
        .dbname("postgres");
    let (client, connection) = cfg.connect(NoTls).await?;
    tokio::spawn(async move {
        let _: Result<(), tokio_postgres::Error> = connection.await;
    });
    client
        .simple_query(
            "CREATE TABLE IF NOT EXISTS public.repl_users (id int PRIMARY KEY, name text)",
        )
        .await?;
    client.simple_query("TRUNCATE public.repl_users").await?;
    client
        .simple_query("INSERT INTO public.repl_users VALUES (1, 'Alice'), (2, 'Bob')")
        .await?;
    Ok(client)
}

async fn setup_big_table(port: u16) -> Result<tokio_postgres::Client, anyhow::Error> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("localhost")
        .port(port)
        .user("postgres")
        .password(common::PG_PASSWORD)
        .dbname("postgres");
    let (client, connection) = cfg.connect(NoTls).await?;
    tokio::spawn(async move {
        let _: Result<(), tokio_postgres::Error> = connection.await;
    });
    client
        .simple_query("CREATE TABLE IF NOT EXISTS public.repl_big (id int PRIMARY KEY, blob text)")
        .await?;
    client.simple_query("TRUNCATE public.repl_big").await?;
    // Seed one small row so the bootstrap has content and marks the dataset ready.
    client
        .simple_query("INSERT INTO public.repl_big VALUES (0, 'seed')")
        .await?;
    Ok(client)
}

#[tokio::test(flavor = "multi_thread")]
async fn bootstrap_then_stream_changes() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let source = setup_source_table(u16::try_from(port).expect("port fits in u16")).await?;

    let params = params_for(
        u16::try_from(port).expect("port fits in u16"),
        "spice_itest_slot_a",
        "spice_itest_pub_a",
    );
    let input = ReplicationStreamInput {
        dataset_name: "repl_users".into(),
        params,
        schema: dataset_schema(),
        primary_keys: vec!["id".into()],
        schema_name: "public".into(),
        table_name: "repl_users".into(),
        metrics: ReplicationMetricsCollector::new(),
    };

    let mut stream = start_replication_stream(input);

    // --- 1. Bootstrap envelope: two rows, op="c" ---
    let envelope = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("bootstrap envelope missing"))??;
    let (committer, change_batch, is_ready) = envelope.into_parts();
    let ops = change_batch
        .record
        .column_by_name("op")
        .expect("op column")
        .as_string::<i32>();
    assert_eq!(change_batch.record.num_rows(), 2);
    for i in 0..2 {
        assert_eq!(ops.value(i), "c");
    }
    assert!(is_ready, "last bootstrap envelope must mark dataset ready");
    // Exercise the commit path so the test mirrors runtime usage (bootstrap
    // commits are no-ops but regressions in the interface should break here).
    committer.commit().await?;

    // --- 2. Live insert ---
    source
        .simple_query("INSERT INTO public.repl_users VALUES (3, 'Charlie')")
        .await?;

    let envelope = tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("insert envelope missing"))??;
    let (committer, change_batch, _) = envelope.into_parts();
    let ops = change_batch
        .record
        .column_by_name("op")
        .expect("op")
        .as_string::<i32>();
    assert_eq!(change_batch.record.num_rows(), 1);
    assert_eq!(ops.value(0), "c");
    let data = change_batch.record.column_by_name("data").expect("data");
    let data_struct = data.as_struct();
    let id_col = data_struct
        .column_by_name("id")
        .expect("id")
        .as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(id_col.value(0), 3);
    committer.commit().await?;

    // --- 3. Live update ---
    source
        .simple_query("UPDATE public.repl_users SET name = 'Alicia' WHERE id = 1")
        .await?;
    let envelope = tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("update envelope missing"))??;
    let (committer, change_batch, _) = envelope.into_parts();
    let ops = change_batch
        .record
        .column_by_name("op")
        .expect("op")
        .as_string::<i32>();
    assert_eq!(ops.value(0), "u");
    committer.commit().await?;

    // --- 4. Live delete ---
    source
        .simple_query("DELETE FROM public.repl_users WHERE id = 2")
        .await?;
    let envelope = tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("delete envelope missing"))??;
    let (committer, change_batch, _) = envelope.into_parts();
    let ops = change_batch
        .record
        .column_by_name("op")
        .expect("op")
        .as_string::<i32>();
    assert_eq!(ops.value(0), "d");
    committer.commit().await?;

    // --- 5. Non-persistent accelerator: snapshot forced on slot resume. ---
    // Dropping the stream and re-subscribing with `snapshot_on_resume` (set
    // by the connector for memory-mode accelerators) must re-deliver the full
    // table as a snapshot instead of resuming snapshot-less over an empty
    // accelerator. Current rows: {1 (Alicia), 3 (Charlie)}.
    drop(stream);
    let mut params = params_for(
        u16::try_from(port).expect("port fits in u16"),
        "spice_itest_slot_a",
        "spice_itest_pub_a",
    );
    params.snapshot_on_resume = true;
    let input = ReplicationStreamInput {
        dataset_name: "repl_users".into(),
        params,
        schema: dataset_schema(),
        primary_keys: vec!["id".into()],
        schema_name: "public".into(),
        table_name: "repl_users".into(),
        metrics: ReplicationMetricsCollector::new(),
    };
    let mut stream = start_replication_stream(input);
    let envelope = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("forced resume snapshot missing"))??;
    let (committer, change_batch, is_ready) = envelope.into_parts();
    assert_eq!(
        change_batch.record.num_rows(),
        2,
        "snapshot_on_resume must deliver the full table on an existing slot"
    );
    assert!(is_ready, "forced resume snapshot must mark dataset ready");
    committer.commit().await?;

    Ok(())
}

/// End-to-end regression for the incremental zero-copy `FrameReader` read path
/// (the streaming reader in the vendored `pgwire-replication` fork), driven
/// against a real Postgres:
///
/// 1. **Large value** — a single ~4 MiB row produces one WAL `XLogData` message
///    far larger than a socket read / TCP segment, so `FrameReader` must
///    assemble the frame across many `read_buf` calls (its incremental,
///    geometrically-growing buffer path). We assert the value round-trips with
///    exact length and content — a framing off-by-one would corrupt it.
/// 2. **Burst** — 1000 rows in a single transaction arrive as many frames
///    buffered together, exercising the tight drain loop
///    (`has_buffered_frame` + `next`). We assert every row id arrives exactly
///    once.
#[tokio::test(flavor = "multi_thread")]
async fn large_value_and_burst_replicate_intact() -> Result<(), anyhow::Error> {
    // Declared before any statements to satisfy clippy::items_after_statements.
    const BIG_LEN: usize = 4 * 1024 * 1024;
    const FIRST_ID: i32 = 1000;
    const LAST_ID: i32 = 1999;
    const BURST_ROWS: usize = 1000; // LAST_ID - FIRST_ID + 1

    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port_u16 = u16::try_from(port).expect("port fits in u16");
    let source = setup_big_table(port_u16).await?;

    let params = params_for(port_u16, "spice_itest_slot_big", "spice_itest_pub_big");
    let input = ReplicationStreamInput {
        dataset_name: "repl_big".into(),
        params,
        schema: big_schema(),
        primary_keys: vec!["id".into()],
        schema_name: "public".into(),
        table_name: "repl_big".into(),
        metrics: ReplicationMetricsCollector::new(),
    };
    let mut stream = start_replication_stream(input);

    // Bootstrap: the seed row marks the dataset ready.
    let envelope = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("bootstrap envelope missing"))??;
    let (committer, change_batch, is_ready) = envelope.into_parts();
    assert_eq!(change_batch.record.num_rows(), 1);
    assert!(is_ready, "bootstrap must mark dataset ready");
    committer.commit().await?;

    // --- 1. Large value (~4 MiB): spans many socket reads, so the FrameReader
    // assembles one frame incrementally instead of in a single read. ---
    source
        .simple_query(&format!(
            "INSERT INTO public.repl_big VALUES (1, repeat('A', {BIG_LEN}))"
        ))
        .await?;

    let envelope = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("large-value envelope missing"))??;
    let (committer, change_batch, _) = envelope.into_parts();
    assert_eq!(change_batch.record.num_rows(), 1);
    let data_struct = change_batch
        .record
        .column_by_name("data")
        .expect("data")
        .as_struct();
    let blob_col = data_struct
        .column_by_name("blob")
        .expect("blob")
        .as_string::<i32>();
    let received = blob_col.value(0);
    assert_eq!(
        received.len(),
        BIG_LEN,
        "large value must round-trip with exact length"
    );
    assert!(
        received.bytes().all(|b| b == b'A'),
        "large value content must be intact"
    );
    committer.commit().await?;

    // --- 2. Burst: 1000 rows in one transaction => many frames buffered
    // together, exercising the tight drain loop. ---
    source
        .simple_query(&format!(
            "INSERT INTO public.repl_big SELECT g, 'x' FROM generate_series({FIRST_ID}, {LAST_ID}) g"
        ))
        .await?;

    let mut seen = std::collections::BTreeSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while seen.len() < BURST_ROWS {
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "timed out collecting burst; got {} of {BURST_ROWS}",
                    seen.len()
                )
            })?;
        let envelope = tokio::time::timeout(remaining, stream.next())
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "burst stream ended early; got {} of {BURST_ROWS}",
                    seen.len()
                )
            })??;
        let (committer, change_batch, _) = envelope.into_parts();
        let data_struct = change_batch
            .record
            .column_by_name("data")
            .expect("data")
            .as_struct();
        let ids = data_struct
            .column_by_name("id")
            .expect("id")
            .as_primitive::<arrow::datatypes::Int32Type>();
        for i in 0..change_batch.record.num_rows() {
            seen.insert(ids.value(i));
        }
        committer.commit().await?;
    }
    assert_eq!(seen.len(), BURST_ROWS, "all burst rows must arrive");
    assert_eq!(*seen.iter().next().expect("min id"), FIRST_ID);
    assert_eq!(*seen.iter().next_back().expect("max id"), LAST_ID);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn two_replicas_have_independent_slots() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let source = setup_source_table(u16::try_from(port).expect("port fits in u16")).await?;

    // Two independent consumers — same publication, distinct slots.
    let params_a = params_for(
        u16::try_from(port).expect("port fits in u16"),
        "spice_itest_slot_r1",
        "spice_itest_pub_r",
    );
    let params_b = params_for(
        u16::try_from(port).expect("port fits in u16"),
        "spice_itest_slot_r2",
        "spice_itest_pub_r",
    );

    let build_input = |p: ReplicationParams| ReplicationStreamInput {
        dataset_name: "repl_users".into(),
        params: p,
        schema: dataset_schema(),
        primary_keys: vec!["id".into()],
        schema_name: "public".into(),
        table_name: "repl_users".into(),
        metrics: ReplicationMetricsCollector::new(),
    };

    let mut stream_a = start_replication_stream(build_input(params_a));
    let mut stream_b = start_replication_stream(build_input(params_b));

    // Each replica should see its own bootstrap independently. Commit the
    // envelopes so the replication stream's LSN-ACK / back-pressure path
    // mirrors real runtime usage — otherwise confirmed_flush_lsn never
    // advances and the slot state can become inconsistent (source of flaky
    // WAL-retention / cleanup timing behavior).
    let env_a = tokio::time::timeout(Duration::from_secs(30), stream_a.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("bootstrap a missing"))??;
    let env_b = tokio::time::timeout(Duration::from_secs(30), stream_b.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("bootstrap b missing"))??;
    assert_eq!(env_a.change_batch.record.num_rows(), 2);
    assert_eq!(env_b.change_batch.record.num_rows(), 2);
    env_a.commit().await?;
    env_b.commit().await?;

    // A live insert should propagate to BOTH replicas.
    source
        .simple_query("INSERT INTO public.repl_users VALUES (4, 'Derek')")
        .await?;

    let live_a = tokio::time::timeout(Duration::from_secs(15), stream_a.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("live a missing"))??;
    let live_b = tokio::time::timeout(Duration::from_secs(15), stream_b.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("live b missing"))??;
    assert_eq!(live_a.change_batch.record.num_rows(), 1);
    assert_eq!(live_b.change_batch.record.num_rows(), 1);
    live_a.commit().await?;
    live_b.commit().await?;

    // Confirm each replica has its own slot in pg_replication_slots.
    let slots = source
        .query(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'spice_itest_slot_r%'",
            &[],
        )
        .await?;
    let names: Vec<String> = slots.iter().map(|r| r.get::<_, String>(0)).collect();
    assert!(names.contains(&"spice_itest_slot_r1".to_string()));
    assert!(names.contains(&"spice_itest_slot_r2".to_string()));

    // Clean up slots so subsequent runs don't trip on orphans.
    drop(stream_a);
    drop(stream_b);
    for slot in &["spice_itest_slot_r1", "spice_itest_slot_r2"] {
        drop_replication_slot_when_inactive(&source, slot).await?;
    }
    Ok(())
}

async fn drop_replication_slot_when_inactive(
    source: &tokio_postgres::Client,
    slot: &str,
) -> Result<(), anyhow::Error> {
    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(10);
    let mut last_error = None;

    while start_time.elapsed() <= timeout {
        match source
            .query("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) if error.code() == Some(&SqlState::UNDEFINED_OBJECT) => return Ok(()),
            Err(error) => last_error = Some(error.to_string()),
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(anyhow::anyhow!(
        "Timed out waiting to drop Postgres replication slot {slot}. Last error: {}",
        last_error.unwrap_or_else(|| "none".to_string())
    ))
}
