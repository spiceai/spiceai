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
use data_components::cdc::{AccelerationContents, ChangeEnvelope, ChangesStream};
use data_components::postgres_replication::{
    NoopAppliedLsnStore, PgOutputFormat, ReplicationMetricsCollector, ReplicationParams,
    ReplicationStreamInput, SchemaEvolutionPolicy, config, start_replication_stream,
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
        ephemeral_accelerator: false,
        acceleration: AccelerationContents::Unknown,
        status_interval: Duration::from_secs(1),
        bootstrap_batch_size: 8192,
        shared: false,
        member_channel_capacity:
            data_components::postgres_replication::shared::DEFAULT_MEMBER_CHANNEL_CAPACITY,
        pg_output_format: PgOutputFormat::Binary,
        unclaimed_reservation_grace:
            data_components::postgres_replication::shared::DEFAULT_UNCLAIMED_RESERVATION_GRACE,
        watermark_flush_interval:
            data_components::postgres_replication::shared::DEFAULT_WATERMARK_FLUSH_INTERVAL,
        ready_lag: Duration::from_secs(2),
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
    // Seed one small row so the bootstrap has content.
    client
        .simple_query("INSERT INTO public.repl_big VALUES (0, 'seed')")
        .await?;
    Ok(client)
}

fn num_rows(envelope: &ChangeEnvelope) -> usize {
    envelope
        .change_batch()
        .expect("built change batch")
        .record
        .num_rows()
}

/// Pull the next envelope, erroring with `context` if the stream stalls.
async fn next_envelope(
    stream: &mut ChangesStream,
    context: &str,
) -> Result<ChangeEnvelope, anyhow::Error> {
    tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for {context}"))?
        .ok_or_else(|| anyhow::anyhow!("stream ended waiting for {context}"))?
        .map_err(|e| anyhow::anyhow!("stream error waiting for {context}: {e}"))
}

/// Pull the next envelope that carries rows, committing and skipping any
/// zero-row idle heartbeats that interleave on the (now heartbeat-emitting)
/// live stream. Lag-based readiness emits a zero-row heartbeat on a caught-up
/// source, so a linear "the next envelope is my change" assertion is no longer
/// valid — real change batches always carry rows, heartbeats never do.
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
        // Zero-row heartbeat/boundary: commit (harmless no-op for a heartbeat,
        // advances the persisted position for the boundary) and keep going.
        envelope.commit().await?;
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "only idle heartbeats arrived while waiting for {context}"
        );
    }
}

/// Poll the stream until an envelope reports `is_dataset_ready`, committing
/// everything seen along the way. On a caught-up, quiet source this is the
/// source-attested idle heartbeat; on a busy source it is the first live commit
/// whose source-commit time is within `ready_lag` of now.
async fn wait_for_ready(
    stream: &mut ChangesStream,
    context: &str,
) -> Result<ChangeEnvelope, anyhow::Error> {
    let deadline = std::time::Instant::now() + Duration::from_mins(1);
    loop {
        let envelope = next_envelope(stream, context).await?;
        if envelope.is_dataset_ready() {
            return Ok(envelope);
        }
        envelope.commit().await?;
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "dataset never reached Ready while waiting for {context}"
        );
    }
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
        policy: SchemaEvolutionPolicy::Block,
        applied_lsn_store: Arc::new(NoopAppliedLsnStore),
    };

    let mut stream = start_replication_stream(input);

    // --- 1. Bootstrap envelope: two rows, op="c" ---
    let envelope = next_envelope(&mut stream, "bootstrap envelope").await?;
    let (committer, change_batch, is_ready, _) = envelope.into_parts().expect("build change batch");
    let ops = change_batch
        .record
        .column_by_name("op")
        .expect("op column")
        .as_string::<i32>();
    assert_eq!(change_batch.record.num_rows(), 2);
    for i in 0..2 {
        assert_eq!(ops.value(i), "c");
    }
    // The post-snapshot bootstrap envelope is a not-ready boundary now:
    // readiness is lag-based and arrives via the live/heartbeat path below.
    assert!(
        !is_ready,
        "bootstrap boundary must not signal ready; readiness is lag-based"
    );
    // Exercise the commit path so the test mirrors runtime usage (bootstrap
    // commits are no-ops but regressions in the interface should break here).
    committer.commit().await?;

    // Readiness catch-up: once the live/heartbeat path is within `ready_lag`,
    // the dataset flips Ready.
    wait_for_ready(&mut stream, "bootstrap readiness catch-up")
        .await?
        .commit()
        .await?;

    // --- 2. Live insert ---
    source
        .simple_query("INSERT INTO public.repl_users VALUES (3, 'Charlie')")
        .await?;

    let envelope = next_change_envelope(&mut stream, "insert envelope").await?;
    {
        let change_batch = envelope.change_batch().expect("build change batch");
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
    }
    envelope.commit().await?;

    // --- 3. Live update ---
    source
        .simple_query("UPDATE public.repl_users SET name = 'Alicia' WHERE id = 1")
        .await?;
    let envelope = next_change_envelope(&mut stream, "update envelope").await?;
    {
        let change_batch = envelope.change_batch().expect("build change batch");
        let ops = change_batch
            .record
            .column_by_name("op")
            .expect("op")
            .as_string::<i32>();
        assert_eq!(ops.value(0), "u");
    }
    envelope.commit().await?;

    // --- 4. Live delete ---
    source
        .simple_query("DELETE FROM public.repl_users WHERE id = 2")
        .await?;
    let envelope = next_change_envelope(&mut stream, "delete envelope").await?;
    {
        let change_batch = envelope.change_batch().expect("build change batch");
        let ops = change_batch
            .record
            .column_by_name("op")
            .expect("op")
            .as_string::<i32>();
        assert_eq!(ops.value(0), "d");
    }
    envelope.commit().await?;

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
        policy: SchemaEvolutionPolicy::Block,
        applied_lsn_store: Arc::new(NoopAppliedLsnStore),
    };
    let mut stream = start_replication_stream(input);
    let envelope = next_envelope(&mut stream, "forced resume snapshot").await?;
    let (committer, change_batch, is_ready, _) = envelope.into_parts().expect("build change batch");
    assert_eq!(
        change_batch.record.num_rows(),
        2,
        "snapshot_on_resume must deliver the full table on an existing slot"
    );
    // The snapshot final envelope is a not-ready boundary now; readiness is
    // lag-based and follows from the live/heartbeat path once caught up.
    assert!(
        !is_ready,
        "forced resume snapshot boundary must not signal ready; readiness is lag-based"
    );
    committer.commit().await?;
    wait_for_ready(&mut stream, "forced resume readiness catch-up")
        .await?
        .commit()
        .await?;

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
        policy: SchemaEvolutionPolicy::Block,
        applied_lsn_store: Arc::new(NoopAppliedLsnStore),
    };
    let mut stream = start_replication_stream(input);

    // Bootstrap: the seed row arrives as a not-ready boundary; readiness is
    // lag-based and follows from the live/heartbeat path once caught up.
    let envelope = next_envelope(&mut stream, "bootstrap envelope").await?;
    let (committer, change_batch, is_ready, _) = envelope.into_parts().expect("build change batch");
    assert_eq!(change_batch.record.num_rows(), 1);
    assert!(
        !is_ready,
        "bootstrap boundary must not signal ready; readiness is lag-based"
    );
    committer.commit().await?;
    wait_for_ready(&mut stream, "bootstrap readiness catch-up")
        .await?
        .commit()
        .await?;

    // --- 1. Large value (~4 MiB): spans many socket reads, so the FrameReader
    // assembles one frame incrementally instead of in a single read. ---
    source
        .simple_query(&format!(
            "INSERT INTO public.repl_big VALUES (1, repeat('A', {BIG_LEN}))"
        ))
        .await?;

    let envelope = next_change_envelope(&mut stream, "large-value envelope").await?;
    {
        let change_batch = envelope.change_batch().expect("build change batch");
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
    }
    envelope.commit().await?;

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
        let (committer, change_batch, _, _) = envelope.into_parts().expect("build change batch");
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
        policy: SchemaEvolutionPolicy::Block,
        applied_lsn_store: Arc::new(NoopAppliedLsnStore),
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
    assert_eq!(
        env_a
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        2
    );
    assert_eq!(
        env_b
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        2
    );
    env_a.commit().await?;
    env_b.commit().await?;

    // A live insert should propagate to BOTH replicas.
    source
        .simple_query("INSERT INTO public.repl_users VALUES (4, 'Derek')")
        .await?;

    let live_a = next_change_envelope(&mut stream_a, "live a").await?;
    let live_b = next_change_envelope(&mut stream_b, "live b").await?;
    assert_eq!(
        live_a
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        1
    );
    assert_eq!(
        live_b
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        1
    );
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

/// Arrow schema covering every column type that has a distinct binary decoder,
/// used by [`wide_column_types_binary_matches_text`].
fn wide_schema() -> SchemaRef {
    use arrow::datatypes::TimeUnit;
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v_bool", DataType::Boolean, true),
        Field::new("v_i2", DataType::Int16, true),
        Field::new("v_i8", DataType::Int64, true),
        Field::new("v_f4", DataType::Float32, true),
        Field::new("v_f8", DataType::Float64, true),
        Field::new("v_num", DataType::Decimal128(15, 2), true),
        Field::new("v_date", DataType::Date32, true),
        Field::new(
            "v_ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("v_text", DataType::Utf8, true),
    ]))
}

/// Every column type Postgres emits in pgoutput's *binary* format must decode to
/// exactly the same Arrow value as its text form. The runtime now requests
/// binary output by default, so this drives a wide-typed table end-to-end under
/// BOTH [`PgOutputFormat::Binary`] and [`PgOutputFormat::Text`] and asserts the
/// same decoded values for each — proving the binary decoders (including the
/// hand-written numeric/date/timestamp paths) match the text fallback, and
/// keeping the text path exercised now that binary is the default.
#[tokio::test(flavor = "multi_thread")]
async fn wide_column_types_binary_matches_text() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));
    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");

    // Same scenario, both wire formats. Distinct table/slot/publication per run
    // keeps them fully isolated.
    run_wide_types_scenario(port, PgOutputFormat::Binary, "bin").await?;
    run_wide_types_scenario(port, PgOutputFormat::Text, "txt").await?;
    Ok(())
}

async fn run_wide_types_scenario(
    port: u16,
    format: PgOutputFormat,
    tag: &str,
) -> Result<(), anyhow::Error> {
    use arrow::datatypes::{
        Date32Type, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
        TimestampNanosecondType,
    };

    let table = format!("repl_wide_{tag}");
    let slot = format!("spice_wide_slot_{tag}");
    let publication = format!("spice_wide_pub_{tag}");

    // Fresh, empty source table — we drive the live binary WAL decode path with
    // INSERT/UPDATE/DELETE (bootstrap is skipped below), not the snapshot path.
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("localhost")
        .port(port)
        .user("postgres")
        .password(common::PG_PASSWORD)
        .dbname("postgres");
    let (source, connection) = cfg.connect(NoTls).await?;
    tokio::spawn(async move {
        let _: Result<(), tokio_postgres::Error> = connection.await;
    });
    source
        .simple_query(&format!(
            "CREATE TABLE IF NOT EXISTS public.{table} (\
               id int4 PRIMARY KEY, v_bool boolean, v_i2 int2, v_i8 int8, v_f4 float4, \
               v_f8 float8, v_num numeric(15,2), v_date date, v_ts timestamp, v_text text)"
        ))
        .await?;
    source
        .simple_query(&format!("TRUNCATE public.{table}"))
        .await?;

    let mut params = params_for(port, &slot, &publication);
    // Skip bootstrap so the row arrives via the pgoutput WAL path (the decoder
    // under test), not the snapshot reader.
    params.initial_snapshot = false;
    params.pg_output_format = format;
    let input = ReplicationStreamInput {
        dataset_name: table.clone(),
        params,
        schema: wide_schema(),
        primary_keys: vec!["id".into()],
        schema_name: "public".into(),
        table_name: table.clone(),
        metrics: ReplicationMetricsCollector::new(),
        policy: SchemaEvolutionPolicy::Block,
        applied_lsn_store: Arc::new(NoopAppliedLsnStore),
    };
    let mut stream = start_replication_stream(input);

    // `initial_snapshot: false` emits no snapshot and no immediate ready signal
    // (the prelude is empty under lag-based readiness). Wait for the dataset to
    // reach Ready via the caught-up idle heartbeat — receiving that envelope
    // also proves the slot is established, so the INSERT below is captured on
    // the WAL path.
    wait_for_ready(&mut stream, &format!("skip-bootstrap readiness ({tag})"))
        .await?
        .commit()
        .await?;

    // --- INSERT: one row exercising every binary type decoder ---
    source
        .simple_query(&format!(
            "INSERT INTO public.{table} VALUES \
             (1, true, 12345, 9000000000, 1.5, 2.5, 172799.49, '2000-01-01', \
              '2000-01-01 00:00:00', 'hello world')"
        ))
        .await?;
    let env = next_change_envelope(&mut stream, &format!("insert envelope ({tag})")).await?;
    let (committer, batch, _, _) = env.into_parts().expect("build change batch");
    assert_eq!(
        batch
            .record
            .column_by_name("op")
            .expect("op")
            .as_string::<i32>()
            .value(0),
        "c",
        "insert op ({tag})"
    );
    let data = batch.record.column_by_name("data").expect("data");
    let data = data.as_struct();
    assert_eq!(
        data.column_by_name("id")
            .expect("id")
            .as_primitive::<Int32Type>()
            .value(0),
        1,
        "id ({tag})"
    );
    assert!(
        data.column_by_name("v_bool")
            .expect("v_bool")
            .as_boolean()
            .value(0),
        "v_bool ({tag})"
    );
    assert_eq!(
        data.column_by_name("v_i2")
            .expect("v_i2")
            .as_primitive::<Int16Type>()
            .value(0),
        12345,
        "v_i2 ({tag})"
    );
    assert_eq!(
        data.column_by_name("v_i8")
            .expect("v_i8")
            .as_primitive::<Int64Type>()
            .value(0),
        9_000_000_000,
        "v_i8 ({tag})"
    );
    assert!(
        (data
            .column_by_name("v_f4")
            .expect("v_f4")
            .as_primitive::<Float32Type>()
            .value(0)
            - 1.5)
            .abs()
            < f32::EPSILON,
        "v_f4 ({tag})"
    );
    assert!(
        (data
            .column_by_name("v_f8")
            .expect("v_f8")
            .as_primitive::<Float64Type>()
            .value(0)
            - 2.5)
            .abs()
            < f64::EPSILON,
        "v_f8 ({tag})"
    );
    // numeric(15,2) 172799.49 -> i128 scaled by 2.
    assert_eq!(
        data.column_by_name("v_num")
            .expect("v_num")
            .as_primitive::<Decimal128Type>()
            .value(0),
        17_279_949,
        "v_num ({tag})"
    );
    // date 2000-01-01 == the Postgres epoch: 10957 days after the Unix epoch.
    assert_eq!(
        data.column_by_name("v_date")
            .expect("v_date")
            .as_primitive::<Date32Type>()
            .value(0),
        10_957,
        "v_date ({tag})"
    );
    // timestamp 2000-01-01 00:00:00 in nanoseconds since the Unix epoch.
    assert_eq!(
        data.column_by_name("v_ts")
            .expect("v_ts")
            .as_primitive::<TimestampNanosecondType>()
            .value(0),
        946_684_800_000_000_000,
        "v_ts ({tag})"
    );
    assert_eq!(
        data.column_by_name("v_text")
            .expect("v_text")
            .as_string::<i32>()
            .value(0),
        "hello world",
        "v_text ({tag})"
    );
    committer.commit().await?;

    // --- UPDATE: change numeric / bool / text / int8 ---
    source
        .simple_query(&format!(
            "UPDATE public.{table} SET v_num = 1.00, v_bool = false, \
             v_text = 'updated', v_i8 = -1 WHERE id = 1"
        ))
        .await?;
    let env = next_change_envelope(&mut stream, &format!("update envelope ({tag})")).await?;
    let (committer, batch, _, _) = env.into_parts().expect("build change batch");
    assert_eq!(
        batch
            .record
            .column_by_name("op")
            .expect("op")
            .as_string::<i32>()
            .value(0),
        "u",
        "update op ({tag})"
    );
    let data = batch.record.column_by_name("data").expect("data");
    let data = data.as_struct();
    assert_eq!(
        data.column_by_name("v_num")
            .expect("v_num")
            .as_primitive::<Decimal128Type>()
            .value(0),
        100,
        "updated v_num ({tag})"
    );
    assert!(
        !data
            .column_by_name("v_bool")
            .expect("v_bool")
            .as_boolean()
            .value(0),
        "updated v_bool ({tag})"
    );
    assert_eq!(
        data.column_by_name("v_text")
            .expect("v_text")
            .as_string::<i32>()
            .value(0),
        "updated",
        "updated v_text ({tag})"
    );
    assert_eq!(
        data.column_by_name("v_i8")
            .expect("v_i8")
            .as_primitive::<Int64Type>()
            .value(0),
        -1,
        "updated v_i8 ({tag})"
    );
    committer.commit().await?;

    // --- DELETE ---
    source
        .simple_query(&format!("DELETE FROM public.{table} WHERE id = 1"))
        .await?;
    let env = next_change_envelope(&mut stream, &format!("delete envelope ({tag})")).await?;
    let (committer, batch, _, _) = env.into_parts().expect("build change batch");
    assert_eq!(
        batch
            .record
            .column_by_name("op")
            .expect("op")
            .as_string::<i32>()
            .value(0),
        "d",
        "delete op ({tag})"
    );
    committer.commit().await?;

    drop(stream);
    drop_replication_slot_when_inactive(&source, &slot).await?;
    Ok(())
}

/// Lag-based readiness must gate a resume that has a backlog to drain: a
/// replayed WAL commit whose source-commit time is older than `ready_lag` is
/// NOT ready, and the dataset only reaches Ready once the stream catches up to
/// the source head. The 3s sleep is intentional — the readiness threshold is a
/// time-based property, so time itself is under test here.
#[tokio::test(flavor = "multi_thread")]
async fn resume_with_stale_backlog_is_not_ready_until_caught_up() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = setup_source_table(port).await?;

    let slot = "spice_itest_slot_stale";
    let publication = "spice_itest_pub_stale";
    let build_input = || ReplicationStreamInput {
        dataset_name: "repl_users".into(),
        params: params_for(port, slot, publication),
        schema: dataset_schema(),
        primary_keys: vec!["id".into()],
        schema_name: "public".into(),
        table_name: "repl_users".into(),
        metrics: ReplicationMetricsCollector::new(),
        policy: SchemaEvolutionPolicy::Block,
        applied_lsn_store: Arc::new(NoopAppliedLsnStore),
    };

    // Cold bootstrap, then let the caught-up source reach Ready. Committing the
    // envelopes advances the slot's confirmed_flush_lsn so the resume below only
    // replays the gap written while detached.
    let mut stream = start_replication_stream(build_input());
    let boot = next_envelope(&mut stream, "bootstrap boundary").await?;
    assert!(
        !boot.is_dataset_ready(),
        "bootstrap boundary must not signal ready; readiness is lag-based"
    );
    boot.commit().await?;
    wait_for_ready(&mut stream, "initial readiness")
        .await?
        .commit()
        .await?;
    drop(stream);

    // Create a backlog that will be stale by the time it replays: insert a row,
    // then wait past `ready_lag` (2s) so its WAL-commit source time is old.
    source
        .simple_query("INSERT INTO public.repl_users VALUES (7, 'Grace')")
        .await?;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Resume on the same slot (snapshot-less): the replayed insert (id 7) is
    // older than `ready_lag`, so its commit envelope must NOT mark the dataset
    // ready. At-least-once means earlier already-applied changes may replay
    // first — skip until id 7 arrives.
    let mut stream = start_replication_stream(build_input());
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let envelope = next_change_envelope(&mut stream, "stale replayed insert").await?;
        let is_ready = envelope.is_dataset_ready();
        let ids: Vec<i32> = {
            let data = envelope
                .change_batch()
                .expect("built change batch")
                .record
                .column_by_name("data")
                .expect("data column");
            let id_col = data
                .as_struct()
                .column_by_name("id")
                .expect("id column")
                .as_primitive::<arrow::datatypes::Int32Type>();
            (0..id_col.len()).map(|i| id_col.value(i)).collect()
        };
        envelope.commit().await?;
        if ids.contains(&7) {
            assert!(
                !is_ready,
                "a replayed commit older than ready_lag must not mark the dataset ready"
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "never received the stale backlog insert (id 7) after resume"
        );
    }

    // Once caught up to the head, the dataset reaches Ready (a fresh idle
    // heartbeat stamped with the current source clock).
    wait_for_ready(&mut stream, "catch-up readiness")
        .await?
        .commit()
        .await?;

    drop(stream);
    drop_replication_slot_when_inactive(&source, slot).await?;
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
