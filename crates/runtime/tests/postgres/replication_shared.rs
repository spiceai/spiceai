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
//! Integration tests for *shared-slot* postgres logical replication: multiple
//! `refresh_mode: changes` datasets multiplexed onto one replication slot, one
//! publication, and one walsender, with per-table change routing.
//!
//! Covers the acceptance criteria for slot sharing:
//! - initial snapshot of multiple member tables
//! - interleaved insert/update/delete routing (including one transaction
//!   spanning two member tables)
//! - a late-added dataset (publication gains the table + point-in-time
//!   snapshot)
//! - restart-resume from `confirmed_flush_lsn` (held ack floor replays the
//!   gap)
//! - exactly one slot / publication / walsender for N datasets
//! - duplicate-table and publication-mismatch rejection

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use data_components::cdc::{ChangeEnvelope, ChangesStream};
use data_components::postgres_replication::{
    ReplicationMetricsCollector, ReplicationParams, ReplicationStreamInput, config,
    start_replication_stream,
};
use futures::StreamExt;
use secrecy::SecretString;
use tokio_postgres::{NoTls, error::SqlState};

use crate::init_tracing;
use crate::postgres::common;

const SLOT: &str = "spice_itest_shared_slot";
const PUBLICATION: &str = "spice_itest_shared_slot_pub";

fn dataset_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn shared_params(port: u16) -> ReplicationParams {
    ReplicationParams {
        host: "localhost".into(),
        port,
        user: "postgres".into(),
        password: SecretString::from(common::PG_PASSWORD.to_string()),
        database: "postgres".into(),
        sslmode: config::SslMode::Disable,
        sslrootcert: None,
        slot_name: SLOT.into(),
        publication_name: PUBLICATION.into(),
        initial_snapshot: true,
        snapshot_on_resume: false,
        temporary_slot: false,
        status_interval: Duration::from_secs(1),
        bootstrap_batch_size: 8192,
        shared: true,
    }
}

fn input_for(port: u16, table: &str) -> ReplicationStreamInput {
    input_with_schema(port, table, dataset_schema())
}

fn input_with_schema(port: u16, table: &str, schema: SchemaRef) -> ReplicationStreamInput {
    ReplicationStreamInput {
        dataset_name: table.to_string(),
        params: shared_params(port),
        schema,
        primary_keys: vec!["id".into()],
        schema_name: "public".into(),
        table_name: table.to_string(),
        metrics: ReplicationMetricsCollector::new(),
    }
}

/// Schema for the late-added table, exercising the non-scalar column support:
/// a `text[]` array (List), a Postgres ENUM (Dictionary), a `uuid` (Arrow
/// Utf8 but a non-text wire type), and a `numeric` (Decimal128).
fn rich_dataset_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("uid", DataType::Utf8, true),
        Field::new("score", DataType::Decimal128(10, 2), true),
        // GENERATED column: captured by the snapshot, omitted by pgoutput on
        // the WAL path (applied as NULL).
        Field::new("name_upper", DataType::Utf8, true),
    ]))
}

fn string_col_of(envelope: &ChangeEnvelope, column: &str, row: usize) -> String {
    envelope
        .change_batch
        .record
        .column_by_name("data")
        .expect("data column")
        .as_struct()
        .column_by_name(column)
        .unwrap_or_else(|| panic!("{column} column"))
        .as_string::<i32>()
        .value(row)
        .to_string()
}

fn score_of(envelope: &ChangeEnvelope, row: usize) -> i128 {
    envelope
        .change_batch
        .record
        .column_by_name("data")
        .expect("data column")
        .as_struct()
        .column_by_name("score")
        .expect("score column")
        .as_primitive::<arrow::datatypes::Decimal128Type>()
        .value(row)
}

fn tags_of(envelope: &ChangeEnvelope, row: usize) -> Vec<Option<String>> {
    let data = envelope
        .change_batch
        .record
        .column_by_name("data")
        .expect("data column");
    let list = data
        .as_struct()
        .column_by_name("tags")
        .expect("tags column")
        .as_list::<i32>()
        .value(row);
    let items = list.as_string::<i32>();
    (0..items.len())
        .map(|i| {
            if items.is_null(i) {
                None
            } else {
                Some(items.value(i).to_string())
            }
        })
        .collect()
}

fn status_of(envelope: &ChangeEnvelope, row: usize) -> String {
    let data = envelope
        .change_batch
        .record
        .column_by_name("data")
        .expect("data column");
    let dict = data
        .as_struct()
        .column_by_name("status")
        .expect("status column")
        .as_dictionary::<arrow::datatypes::Int8Type>();
    let values = dict.values().as_string::<i32>();
    values.value(dict.key(row).expect("status key")).to_string()
}

async fn pg_client(port: u16) -> Result<tokio_postgres::Client, anyhow::Error> {
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
    Ok(client)
}

async fn create_table(
    client: &tokio_postgres::Client,
    table: &str,
    rows: &[(i32, &str)],
) -> Result<(), anyhow::Error> {
    client
        .simple_query(&format!(
            "CREATE TABLE IF NOT EXISTS public.{table} (id int PRIMARY KEY, name text)"
        ))
        .await?;
    client
        .simple_query(&format!("TRUNCATE public.{table}"))
        .await?;
    for (id, name) in rows {
        client
            .execute(
                &format!("INSERT INTO public.{table} VALUES ($1, $2)"),
                &[id, &(*name).to_string()],
            )
            .await?;
    }
    Ok(())
}

async fn next_envelope(
    stream: &mut ChangesStream,
    what: &str,
) -> Result<ChangeEnvelope, anyhow::Error> {
    tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for {what}"))?
        .ok_or_else(|| anyhow::anyhow!("stream ended waiting for {what}"))?
        .map_err(|e| anyhow::anyhow!("stream error waiting for {what}: {e}"))
}

fn ops_of(envelope: &ChangeEnvelope) -> Vec<String> {
    let ops = envelope
        .change_batch
        .record
        .column_by_name("op")
        .expect("op column")
        .as_string::<i32>();
    (0..ops.len()).map(|i| ops.value(i).to_string()).collect()
}

fn ids_of(envelope: &ChangeEnvelope) -> Vec<i32> {
    let data = envelope
        .change_batch
        .record
        .column_by_name("data")
        .expect("data column");
    let ids = data
        .as_struct()
        .column_by_name("id")
        .expect("id column")
        .as_primitive::<arrow::datatypes::Int32Type>();
    (0..ids.len()).map(|i| ids.value(i)).collect()
}

/// Read one envelope, assert a single row with the given op + id, commit it.
async fn expect_single_change(
    stream: &mut ChangesStream,
    what: &str,
    op: &str,
    id: i32,
) -> Result<(), anyhow::Error> {
    let envelope = next_envelope(stream, what).await?;
    assert_eq!(ops_of(&envelope), vec![op.to_string()], "{what}: op");
    assert_eq!(ids_of(&envelope), vec![id], "{what}: id");
    envelope.commit().await?;
    Ok(())
}

async fn slot_count(client: &tokio_postgres::Client, slot: &str) -> Result<i64, anyhow::Error> {
    Ok(client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await?
        .get(0))
}

async fn publication_tables(
    client: &tokio_postgres::Client,
    publication: &str,
) -> Result<HashSet<String>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT tablename FROM pg_publication_tables WHERE pubname = $1",
            &[&publication],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Poll until exactly `expected` walsenders serve the slot (reconnects make
/// the instantaneous count racy).
async fn wait_for_walsender_count(
    client: &tokio_postgres::Client,
    slot: &str,
    expected: i64,
) -> Result<(), anyhow::Error> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut last = -1;
    while std::time::Instant::now() < deadline {
        last = client
            .query_one(
                "SELECT count(*) FROM pg_stat_replication r \
                 JOIN pg_replication_slots s ON s.active_pid = r.pid \
                 WHERE s.slot_name = $1",
                &[&slot],
            )
            .await?
            .get(0);
        if last == expected {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow::anyhow!(
        "expected {expected} walsender(s) for slot {slot}, last saw {last}"
    ))
}

async fn drop_replication_slot_when_inactive(
    source: &tokio_postgres::Client,
    slot: &str,
) -> Result<(), anyhow::Error> {
    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(15);
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

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    Err(anyhow::anyhow!(
        "Timed out waiting to drop Postgres replication slot {slot}. Last error: {}",
        last_error.unwrap_or_else(|| "none".to_string())
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_slot_multiplexes_multiple_datasets() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "shared_repl_a", &[(1, "a1"), (2, "a2")]).await?;
    create_table(&source, "shared_repl_b", &[(1, "b1"), (2, "b2"), (3, "b3")]).await?;

    // --- 1. Two datasets join the same slot; each gets its own snapshot. ---
    let mut stream_a = start_replication_stream(input_for(port, "shared_repl_a"));
    let boot_a = next_envelope(&mut stream_a, "bootstrap a").await?;
    assert_eq!(boot_a.change_batch.record.num_rows(), 2, "bootstrap a rows");
    assert!(boot_a.is_dataset_ready(), "bootstrap a must mark ready");
    boot_a.commit().await?;

    let mut stream_b = start_replication_stream(input_for(port, "shared_repl_b"));
    let boot_b = next_envelope(&mut stream_b, "bootstrap b").await?;
    assert_eq!(boot_b.change_batch.record.num_rows(), 3, "bootstrap b rows");
    assert!(boot_b.is_dataset_ready(), "bootstrap b must mark ready");
    boot_b.commit().await?;

    // --- 2. One slot, one publication covering both tables, one walsender. ---
    assert_eq!(slot_count(&source, SLOT).await?, 1, "exactly one slot");
    assert_eq!(
        publication_tables(&source, PUBLICATION).await?,
        HashSet::from(["shared_repl_a".to_string(), "shared_repl_b".to_string()]),
        "publication covers both member tables"
    );
    wait_for_walsender_count(&source, SLOT, 1).await?;

    // --- 3. Interleaved changes route to the right dataset. ---
    source
        .simple_query("INSERT INTO public.shared_repl_a VALUES (10, 'a10')")
        .await?;
    expect_single_change(&mut stream_a, "insert a", "c", 10).await?;

    source
        .simple_query("INSERT INTO public.shared_repl_b VALUES (20, 'b20')")
        .await?;
    expect_single_change(&mut stream_b, "insert b", "c", 20).await?;

    source
        .simple_query("UPDATE public.shared_repl_a SET name = 'a10x' WHERE id = 10")
        .await?;
    expect_single_change(&mut stream_a, "update a", "u", 10).await?;

    source
        .simple_query("DELETE FROM public.shared_repl_b WHERE id = 20")
        .await?;
    expect_single_change(&mut stream_b, "delete b", "d", 20).await?;

    // --- 4. A single transaction touching both tables fans out to both. ---
    source
        .simple_query(
            "BEGIN; \
             INSERT INTO public.shared_repl_a VALUES (11, 'a11'); \
             INSERT INTO public.shared_repl_b VALUES (21, 'b21'); \
             COMMIT;",
        )
        .await?;
    expect_single_change(&mut stream_a, "cross-txn a", "c", 11).await?;
    expect_single_change(&mut stream_b, "cross-txn b", "c", 21).await?;

    // --- 5. Late-added dataset: publication gains the table + snapshot. ---
    // The table also carries a `text[]` array and an ENUM column — the
    // non-scalar types the replication path must handle on both the snapshot
    // (`::text` fetch) and WAL (pgoutput text format) sides.
    source
        .simple_query(
            "DROP TABLE IF EXISTS public.shared_repl_c; \
             DROP TYPE IF EXISTS shared_repl_mood; \
             CREATE TYPE shared_repl_mood AS ENUM ('active', 'paused'); \
             CREATE TABLE public.shared_repl_c (\
                 id int PRIMARY KEY, name text, tags text[], status shared_repl_mood, \
                 uid uuid, score numeric(10,2), \
                 name_upper text GENERATED ALWAYS AS (upper(name)) STORED); \
             INSERT INTO public.shared_repl_c (id, name, tags, status, uid, score) \
                 VALUES (100, 'c100', '{x,\"y z\"}', 'active', \
                         '11111111-2222-3333-4444-555555555555', 10.50);",
        )
        .await?;
    let mut stream_c = start_replication_stream(input_with_schema(
        port,
        "shared_repl_c",
        rich_dataset_schema(),
    ));
    let boot_c = next_envelope(&mut stream_c, "bootstrap c").await?;
    assert_eq!(boot_c.change_batch.record.num_rows(), 1, "bootstrap c rows");
    assert!(boot_c.is_dataset_ready(), "bootstrap c must mark ready");
    assert_eq!(
        tags_of(&boot_c, 0),
        vec![Some("x".to_string()), Some("y z".to_string())],
        "bootstrap c array column"
    );
    assert_eq!(status_of(&boot_c, 0), "active", "bootstrap c enum column");
    assert_eq!(
        string_col_of(&boot_c, "uid", 0),
        "11111111-2222-3333-4444-555555555555",
        "bootstrap c uuid column (non-text wire type → Utf8)"
    );
    assert_eq!(score_of(&boot_c, 0), 1050, "bootstrap c numeric column");
    assert_eq!(
        string_col_of(&boot_c, "name_upper", 0),
        "C100",
        "bootstrap captures GENERATED column values"
    );
    boot_c.commit().await?;

    assert_eq!(
        slot_count(&source, SLOT).await?,
        1,
        "still exactly one slot"
    );
    assert_eq!(
        publication_tables(&source, PUBLICATION).await?.len(),
        3,
        "publication gained the late-added table"
    );

    source
        .simple_query(
            "INSERT INTO public.shared_repl_c (id, name, tags, status, uid, score) \
             VALUES (101, 'c101', ARRAY['blue', NULL], 'paused', \
                     'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', 99.99)",
        )
        .await?;
    let live_c = next_envelope(&mut stream_c, "insert c").await?;
    assert_eq!(ops_of(&live_c), vec!["c".to_string()], "insert c: op");
    assert_eq!(ids_of(&live_c), vec![101], "insert c: id");
    assert_eq!(
        tags_of(&live_c, 0),
        vec![Some("blue".to_string()), None],
        "WAL-path array column (with NULL element)"
    );
    assert_eq!(status_of(&live_c, 0), "paused", "WAL-path enum column");
    assert_eq!(
        string_col_of(&live_c, "uid", 0),
        "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "WAL-path uuid column"
    );
    assert_eq!(score_of(&live_c, 0), 9999, "WAL-path numeric column");
    {
        // pgoutput omits GENERATED columns; the change applies them as NULL
        // (instead of fatally detaching the member, which is the bug this
        // covers).
        let data = live_c
            .change_batch
            .record
            .column_by_name("data")
            .expect("data column");
        let name_upper = data
            .as_struct()
            .column_by_name("name_upper")
            .expect("name_upper column")
            .as_string::<i32>();
        assert!(
            name_upper.is_null(0),
            "GENERATED column must be NULL on replicated changes"
        );
    }
    live_c.commit().await?;

    // --- 5b. Unchanged-TOAST columns under REPLICA IDENTITY FULL. ---
    // Dedicated table: Postgres rejects UPDATEs outright when REPLICA
    // IDENTITY FULL covers an unpublished GENERATED column ("Replica
    // identity must not contain unpublished generated columns"), so this
    // stage cannot share shared_repl_c. Write an incompressible ~10KB value
    // (forced out-of-line TOAST), then update a DIFFERENT column: pgoutput
    // marks the blob "unchanged" in the new tuple even under RIF, and the
    // value must be recovered from the old tuple instead of fatally ending
    // the stream.
    source
        .simple_query(
            "DROP TABLE IF EXISTS public.shared_repl_d; \
             CREATE TABLE public.shared_repl_d (id int PRIMARY KEY, name text, blob text); \
             ALTER TABLE public.shared_repl_d REPLICA IDENTITY FULL; \
             INSERT INTO public.shared_repl_d VALUES (1, 'd1', NULL);",
        )
        .await?;
    let toast_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("blob", DataType::Utf8, true),
    ]));
    let mut stream_d =
        start_replication_stream(input_with_schema(port, "shared_repl_d", toast_schema));
    let boot_d = next_envelope(&mut stream_d, "bootstrap d").await?;
    assert_eq!(boot_d.change_batch.record.num_rows(), 1, "bootstrap d rows");
    boot_d.commit().await?;

    source
        .simple_query(
            "UPDATE public.shared_repl_d \
             SET blob = (SELECT string_agg(md5(g::text), '' ORDER BY g) \
                         FROM generate_series(1, 320) g) \
             WHERE id = 1",
        )
        .await?;
    let blob_write = next_envelope(&mut stream_d, "blob write").await?;
    assert_eq!(ops_of(&blob_write), vec!["u".to_string()], "blob write op");
    assert_eq!(
        string_col_of(&blob_write, "blob", 0).len(),
        320 * 32,
        "blob update carries the full value"
    );
    blob_write.commit().await?;

    source
        .simple_query("UPDATE public.shared_repl_d SET name = 'd1x' WHERE id = 1")
        .await?;
    let toast_update = next_envelope(&mut stream_d, "unchanged-TOAST update").await?;
    assert_eq!(
        ops_of(&toast_update),
        vec!["u".to_string()],
        "unchanged-TOAST update op"
    );
    assert_eq!(
        string_col_of(&toast_update, "name", 0),
        "d1x",
        "changed column updated"
    );
    assert_eq!(
        string_col_of(&toast_update, "blob", 0).len(),
        320 * 32,
        "unchanged TOASTed column must be filled from the old tuple, not NULLed or fatal"
    );
    toast_update.commit().await?;

    // No cross-routing: the next change for `a` arrives on stream_a with only
    // `a` rows, despite the c-traffic in between.
    source
        .simple_query("INSERT INTO public.shared_repl_a VALUES (12, 'a12')")
        .await?;
    expect_single_change(&mut stream_a, "insert a after c joined", "c", 12).await?;

    // --- 6. Restart-resume: drop every member, write into the gap, rejoin. ---
    drop(stream_a);
    drop(stream_b);
    drop(stream_c);

    source
        .simple_query("INSERT INTO public.shared_repl_a VALUES (13, 'a13')")
        .await?;

    let mut stream_a2 = start_replication_stream(input_for(port, "shared_repl_a"));
    // Resume path: no snapshot, just the immediate ready signal...
    let ready = next_envelope(&mut stream_a2, "rejoin ready signal").await?;
    assert_eq!(
        ready.change_batch.record.num_rows(),
        0,
        "rejoin must resume from the slot, not re-snapshot"
    );
    assert!(ready.is_dataset_ready());
    ready.commit().await?;

    // ...then WAL replay from the held confirmed_flush_lsn. At-least-once:
    // the gap insert (13) MUST arrive; commits already applied before the
    // restart (id 12) MAY be replayed — both are `a` rows only.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut saw_gap_row = false;
    while !saw_gap_row {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for gap row id=13 after rejoin"
        );
        let envelope = next_envelope(&mut stream_a2, "post-rejoin change").await?;
        for (op, id) in ops_of(&envelope).iter().zip(ids_of(&envelope)) {
            assert_eq!(op, "c", "post-rejoin replay op");
            assert!(
                id == 12 || id == 13,
                "post-rejoin envelope contained foreign or unexpected row id {id}"
            );
            saw_gap_row |= id == 13;
        }
        envelope.commit().await?;
    }

    // --- 6b. Non-persistent accelerator: snapshot forced on slot resume. ---
    // With `snapshot_on_resume` (set by the connector for memory-mode
    // accelerators), a rejoin must take a full snapshot instead of the
    // resume-ready signal — the accelerator booted empty and WAL replay
    // alone cannot reconstruct it.
    drop(stream_a2);
    let mut ephemeral = input_for(port, "shared_repl_a");
    ephemeral.params.snapshot_on_resume = true;
    let mut stream_a3 = start_replication_stream(ephemeral);
    let boot_a3 = next_envelope(&mut stream_a3, "forced resume snapshot").await?;
    // shared_repl_a currently holds ids {1, 2, 10, 11, 12, 13}.
    assert_eq!(
        boot_a3.change_batch.record.num_rows(),
        6,
        "snapshot_on_resume must deliver the full table on rejoin"
    );
    assert!(boot_a3.is_dataset_ready());
    assert!(
        ops_of(&boot_a3).iter().all(|op| op == "c"),
        "forced resume snapshot rows are op=c"
    );
    boot_a3.commit().await?;

    // --- 7. Misconfiguration is rejected with clear errors. ---
    // Duplicate table on the same slot:
    let mut dup = start_replication_stream(input_for(port, "shared_repl_a"));
    match tokio::time::timeout(Duration::from_secs(30), dup.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("duplicate subscribe yielded no item"))?
    {
        Ok(_) => anyhow::bail!("duplicate table on a shared slot must error"),
        Err(e) => assert!(
            e.to_string().contains("already subscribed"),
            "unexpected duplicate-table error: {e}"
        ),
    }

    // Publication mismatch:
    let mut mismatched = input_for(port, "shared_repl_b");
    mismatched.params.publication_name = "spice_itest_other_pub".into();
    let mut mismatch = start_replication_stream(mismatched);
    match tokio::time::timeout(Duration::from_secs(30), mismatch.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("mismatched subscribe yielded no item"))?
    {
        Ok(_) => anyhow::bail!("publication mismatch on a shared slot must error"),
        Err(e) => assert!(
            e.to_string().contains("publication"),
            "unexpected publication-mismatch error: {e}"
        ),
    }

    // --- Cleanup ---
    drop(stream_a3);
    drop(stream_d);
    drop(dup);
    drop(mismatch);
    drop_replication_slot_when_inactive(&source, SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}

/// `pg_replication_slot_scope: instance` shares one *generated* slot across
/// every changes-mode dataset on a source — distinct per replica via the
/// instance hash. This proves the shared multiplexer treats the generated
/// instance-scoped name (`config::instance_slot_name`) exactly like an explicit
/// one: two datasets collapse onto one slot / one publication / one walsender.
#[tokio::test(flavor = "multi_thread")]
async fn instance_scoped_slot_multiplexes_generated_name() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "inst_a", &[(1, "a1"), (2, "a2")]).await?;
    create_table(&source, "inst_b", &[(1, "b1")]).await?;

    // The exact name the connector generates for `pg_replication_slot_scope:
    // instance` on this (host, port, db, user). Both datasets compute the same
    // name and must multiplex onto it.
    let slot = config::instance_slot_name("localhost", port, "postgres", "postgres");
    assert!(slot.starts_with("spice_inst_"), "got slot `{slot}`");
    let publication = config::publication_name_for_slot(&slot);

    let input = |table: &str| ReplicationStreamInput {
        dataset_name: table.to_string(),
        params: ReplicationParams {
            host: "localhost".into(),
            port,
            user: "postgres".into(),
            password: SecretString::from(common::PG_PASSWORD.to_string()),
            database: "postgres".into(),
            sslmode: config::SslMode::Disable,
            sslrootcert: None,
            slot_name: slot.clone(),
            publication_name: publication.clone(),
            initial_snapshot: true,
            snapshot_on_resume: false,
            temporary_slot: false,
            status_interval: Duration::from_secs(1),
            bootstrap_batch_size: 8192,
            shared: true,
        },
        schema: dataset_schema(),
        primary_keys: vec!["id".into()],
        schema_name: "public".into(),
        table_name: table.to_string(),
        metrics: ReplicationMetricsCollector::new(),
    };

    let mut stream_a = start_replication_stream(input("inst_a"));
    let boot_a = next_envelope(&mut stream_a, "bootstrap inst_a").await?;
    assert_eq!(boot_a.change_batch.record.num_rows(), 2, "bootstrap inst_a");
    boot_a.commit().await?;

    let mut stream_b = start_replication_stream(input("inst_b"));
    let boot_b = next_envelope(&mut stream_b, "bootstrap inst_b").await?;
    assert_eq!(boot_b.change_batch.record.num_rows(), 1, "bootstrap inst_b");
    boot_b.commit().await?;

    // One slot, one publication covering both tables, one walsender.
    assert_eq!(
        slot_count(&source, &slot).await?,
        1,
        "two instance-scoped datasets share exactly one slot"
    );
    assert_eq!(
        publication_tables(&source, &publication).await?,
        HashSet::from(["inst_a".to_string(), "inst_b".to_string()]),
        "instance-scoped publication covers both member tables"
    );
    wait_for_walsender_count(&source, &slot, 1).await?;

    // Per-table routing over the shared generated slot.
    source
        .simple_query("INSERT INTO public.inst_a VALUES (10, 'a10')")
        .await?;
    expect_single_change(&mut stream_a, "insert inst_a", "c", 10).await?;
    source
        .simple_query("INSERT INTO public.inst_b VALUES (20, 'b20')")
        .await?;
    expect_single_change(&mut stream_b, "insert inst_b", "c", 20).await?;

    drop(stream_a);
    drop(stream_b);
    drop_replication_slot_when_inactive(&source, &slot).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {publication}"))
        .await?;
    Ok(())
}
