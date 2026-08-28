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
use data_components::cdc::{AccelerationContents, ChangeEnvelope, ChangesStream};
use data_components::postgres_replication::{
    AppliedLsn, AppliedLsnStore, NoopAppliedLsnStore, PgOutputFormat, RecordedPosition,
    ReplicationMetrics, ReplicationMetricsCollector, ReplicationParams, ReplicationStreamInput,
    SchemaEvolutionPolicy, config, start_replication_stream,
};
use futures::StreamExt;
use secrecy::SecretString;
use tokio_postgres::{NoTls, error::SqlState};

use crate::init_tracing;
use crate::postgres::common;

const SLOT: &str = "spice_itest_shared_slot";
const PUBLICATION: &str = "spice_itest_shared_slot_pub";

// A second, *independent* slot used by `shared_and_independent_slots_coexist` to
// run a non-shared dataset alongside the shared-slot group in one process.
const INDEP_SLOT: &str = "spice_itest_mix_indep_slot";
const INDEP_PUBLICATION: &str = "spice_itest_mix_indep_pub";

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
        ephemeral_accelerator: false,
        acceleration: AccelerationContents::Unknown,
        status_interval: Duration::from_secs(1),
        bootstrap_batch_size: 8192,
        shared: true,
        member_channel_capacity:
            data_components::postgres_replication::shared::DEFAULT_MEMBER_CHANNEL_CAPACITY,
        pg_output_format: PgOutputFormat::Binary,
        unclaimed_reservation_grace:
            data_components::postgres_replication::shared::DEFAULT_UNCLAIMED_RESERVATION_GRACE,
        // Short enough that the idle carry-forward is exercised within a test rather
        // than waited on; the production default is coarse on purpose.
        watermark_flush_interval: Duration::from_secs(1),
        ready_lag: Duration::from_secs(2),
    }
}

fn input_for(port: u16, table: &str) -> ReplicationStreamInput {
    input_with_schema(port, table, dataset_schema())
}

/// A dataset on its own dedicated slot/publication (`shared: false`), for the
/// mixed-mode coexistence test. Note `shared: false` now selects only
/// slot/publication *naming* (a per-dataset slot), not a separate apply path —
/// every dataset runs on the shared pump, this one just as a one-member source.
fn independent_input(port: u16, table: &str) -> ReplicationStreamInput {
    let mut params = shared_params(port);
    params.slot_name = INDEP_SLOT.into();
    params.publication_name = INDEP_PUBLICATION.into();
    params.shared = false;
    ReplicationStreamInput {
        dataset_name: table.to_string(),
        params,
        schema: dataset_schema(),
        primary_keys: vec!["id".into()],
        schema_name: "public".into(),
        table_name: table.to_string(),
        metrics: ReplicationMetricsCollector::new(),
        policy: SchemaEvolutionPolicy::Block,
        applied_lsn_store: Arc::new(NoopAppliedLsnStore),
        write_back_registry: None,
    }
}

/// An [`AppliedLsnStore`] held in memory, for exercising the watermark paths in
/// this suite.
///
/// The default `NoopAppliedLsnStore` reports that it records nothing, which
/// makes a missing watermark uninformative by design — so with it, none of the
/// gap detection below can be reached. This records positions like the real
/// sidecar does, and survives across `start_replication_stream` calls in one
/// test so a "restart" can read what the previous stream wrote.
#[derive(Default)]
struct InMemoryAppliedLsnStore {
    recorded: std::sync::Mutex<Option<AppliedLsn>>,
    /// Successful writes, so a test can assert on how *often* a position is
    /// recorded and not only on its value.
    saves: std::sync::atomic::AtomicUsize,
}

impl InMemoryAppliedLsnStore {
    fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// A store that already holds `lsn`, for modelling an acceleration whose
    /// recorded position is *behind* where the slot has got to — a dataset
    /// re-added after its reservation lapsed, or one whose last acknowledgement
    /// never reached its local record.
    fn seeded(lsn: u64) -> Arc<Self> {
        let store = Self::default();
        *store.recorded.lock().expect("watermark mutex") = Some(AppliedLsn { lsn });
        Arc::new(store)
    }

    fn recorded_lsn(&self) -> Option<u64> {
        self.recorded
            .lock()
            .expect("watermark mutex")
            .map(|applied| applied.lsn)
    }

    fn saves(&self) -> usize {
        self.saves.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl AppliedLsnStore for InMemoryAppliedLsnStore {
    async fn load(
        &self,
    ) -> std::result::Result<RecordedPosition, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self
            .recorded
            .lock()
            .expect("watermark mutex")
            .map_or(RecordedPosition::Absent, RecordedPosition::At))
    }

    async fn save(
        &self,
        applied: AppliedLsn,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        *self.recorded.lock().expect("watermark mutex") = Some(applied);
        self.saves.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }

    async fn clear(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        *self.recorded.lock().expect("watermark mutex") = None;
        Ok(())
    }
}

/// [`input_for`] with a watermark store that actually records, so the gap
/// decision is reachable. Pass the same store across two streams to model a
/// restart against acceleration files that survived.
fn input_with_watermark<S: AppliedLsnStore + 'static>(
    port: u16,
    table: &str,
    store: &Arc<S>,
) -> ReplicationStreamInput {
    let mut input = input_for(port, table);
    input.applied_lsn_store = Arc::clone(store) as Arc<dyn AppliedLsnStore>;
    input
}

/// [`input_with_watermark`] carrying what the runtime observed the accelerator to
/// hold, which is what tells a first load apart from one that may be carrying
/// rows the source has since deleted.
fn input_with_contents<S: AppliedLsnStore + 'static>(
    port: u16,
    table: &str,
    store: &Arc<S>,
    acceleration: AccelerationContents,
) -> ReplicationStreamInput {
    let mut input = input_with_watermark(port, table, store);
    input.params.acceleration = acceleration;
    input
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
        policy: SchemaEvolutionPolicy::Block,
        applied_lsn_store: Arc::new(NoopAppliedLsnStore),
        write_back_registry: None,
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
        .change_batch()
        .expect("built change batch")
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
        .change_batch()
        .expect("built change batch")
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
        .change_batch()
        .expect("built change batch")
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
        .change_batch()
        .expect("built change batch")
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

fn num_rows(envelope: &ChangeEnvelope) -> usize {
    envelope
        .change_batch()
        .expect("built change batch")
        .record
        .num_rows()
}

/// Pull the next envelope that carries rows, committing and skipping any
/// zero-row idle heartbeats that interleave on the (now heartbeat-emitting)
/// live stream. Lag-based readiness emits zero-row heartbeats on a caught-up
/// source, so "the next envelope is my change" is no longer valid — real change
/// batches always carry rows, heartbeats never do.
async fn next_change_envelope(
    stream: &mut ChangesStream,
    what: &str,
) -> Result<ChangeEnvelope, anyhow::Error> {
    let deadline = std::time::Instant::now() + Duration::from_mins(1);
    loop {
        let envelope = next_envelope(stream, what).await?;
        if num_rows(&envelope) > 0 {
            return Ok(envelope);
        }
        envelope.commit().await?;
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "only idle heartbeats arrived while waiting for {what}"
        );
    }
}

/// Poll the stream until an envelope reports `is_dataset_ready`, committing
/// everything seen along the way. On a caught-up, quiet source this is the
/// source-attested idle heartbeat; on a busy source it is the first live commit
/// whose source-commit time is within `ready_lag` of now.
async fn wait_for_ready(
    stream: &mut ChangesStream,
    what: &str,
) -> Result<ChangeEnvelope, anyhow::Error> {
    let deadline = std::time::Instant::now() + Duration::from_mins(1);
    loop {
        let envelope = next_envelope(stream, what).await?;
        if envelope.is_dataset_ready() {
            return Ok(envelope);
        }
        envelope.commit().await?;
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "dataset never reached Ready while waiting for {what}"
        );
    }
}

fn ops_of(envelope: &ChangeEnvelope) -> Vec<String> {
    let ops = envelope
        .change_batch()
        .expect("built change batch")
        .record
        .column_by_name("op")
        .expect("op column")
        .as_string::<i32>();
    (0..ops.len()).map(|i| ops.value(i).to_string()).collect()
}

fn ids_of(envelope: &ChangeEnvelope) -> Vec<i32> {
    let data = envelope
        .change_batch()
        .expect("built change batch")
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

/// Read one change envelope (skipping zero-row idle heartbeats), assert a
/// single row with the given op + id, commit it.
async fn expect_single_change(
    stream: &mut ChangesStream,
    what: &str,
    op: &str,
    id: i32,
) -> Result<(), anyhow::Error> {
    let envelope = next_change_envelope(stream, what).await?;
    assert_eq!(ops_of(&envelope), vec![op.to_string()], "{what}: op");
    assert_eq!(ids_of(&envelope), vec![id], "{what}: id");
    envelope.commit().await?;
    Ok(())
}

async fn slot_count(client: &tokio_postgres::Client) -> Result<i64, anyhow::Error> {
    Ok(client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&SLOT],
        )
        .await?
        .get(0))
}

async fn publication_tables(
    client: &tokio_postgres::Client,
) -> Result<HashSet<String>, anyhow::Error> {
    let rows = client
        .query(
            "SELECT tablename FROM pg_publication_tables WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Poll until exactly `expected` walsenders serve our slot (reconnects make
/// the instantaneous count racy).
async fn wait_for_walsender_count(
    client: &tokio_postgres::Client,
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
                &[&SLOT],
            )
            .await?
            .get(0);
        if last == expected {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow::anyhow!(
        "expected {expected} walsender(s) for slot {SLOT}, last saw {last}"
    ))
}

/// Poll until the store holds a recorded position.
///
/// The watermark is published by the snapshot-boundary envelope's committer
/// (`SnapshotWatermarkCommitter`), which hands it to the store to write rather
/// than writing it inline — so the position lands shortly after that commit
/// returns, not within it. Reading once races that write.
async fn wait_for_recorded_position(
    store: &Arc<InMemoryAppliedLsnStore>,
    what: &str,
) -> Result<(), anyhow::Error> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if matches!(store.load().await, Ok(RecordedPosition::At(_))) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(anyhow::anyhow!(
        "{what} recorded no position, so this case would not exercise a surviving one"
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
    assert_eq!(
        boot_a
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        2,
        "bootstrap a rows"
    );
    // Bootstrap boundary is not-ready now; readiness is lag-based and follows
    // from the caught-up live/heartbeat path.
    assert!(
        !boot_a.is_dataset_ready(),
        "bootstrap a boundary must not signal ready; readiness is lag-based"
    );
    boot_a.commit().await?;
    wait_for_ready(&mut stream_a, "bootstrap a readiness catch-up")
        .await?
        .commit()
        .await?;

    let mut stream_b = start_replication_stream(input_for(port, "shared_repl_b"));
    let boot_b = next_envelope(&mut stream_b, "bootstrap b").await?;
    assert_eq!(
        boot_b
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        3,
        "bootstrap b rows"
    );
    assert!(
        !boot_b.is_dataset_ready(),
        "bootstrap b boundary must not signal ready; readiness is lag-based"
    );
    boot_b.commit().await?;
    wait_for_ready(&mut stream_b, "bootstrap b readiness catch-up")
        .await?
        .commit()
        .await?;

    // --- 2. One slot, one publication covering both tables, one walsender. ---
    assert_eq!(slot_count(&source).await?, 1, "exactly one slot");
    assert_eq!(
        publication_tables(&source).await?,
        HashSet::from(["shared_repl_a".to_string(), "shared_repl_b".to_string()]),
        "publication covers both member tables"
    );
    wait_for_walsender_count(&source, 1).await?;

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
    assert_eq!(
        boot_c
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        1,
        "bootstrap c rows"
    );
    assert!(
        !boot_c.is_dataset_ready(),
        "bootstrap c boundary must not signal ready; readiness is lag-based"
    );
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
    wait_for_ready(&mut stream_c, "bootstrap c readiness catch-up")
        .await?
        .commit()
        .await?;

    assert_eq!(slot_count(&source).await?, 1, "still exactly one slot");
    assert_eq!(
        publication_tables(&source).await?.len(),
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
    let live_c = next_change_envelope(&mut stream_c, "insert c").await?;
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
            .change_batch()
            .expect("built change batch")
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
    assert_eq!(
        boot_d
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        1,
        "bootstrap d rows"
    );
    boot_d.commit().await?;

    source
        .simple_query(
            "UPDATE public.shared_repl_d \
             SET blob = (SELECT string_agg(md5(g::text), '' ORDER BY g) \
                         FROM generate_series(1, 320) g) \
             WHERE id = 1",
        )
        .await?;
    let blob_write = next_change_envelope(&mut stream_d, "blob write").await?;
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
    let toast_update = next_change_envelope(&mut stream_d, "unchanged-TOAST update").await?;
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
    // Resume path: no snapshot and no immediate ready signal (the prelude is
    // empty under lag-based readiness). The first output is the WAL replay from
    // the held confirmed_flush_lsn; readiness follows once the stream catches
    // up. At-least-once: the gap insert (13) MUST arrive; commits already
    // applied before the restart (id 12) MAY be replayed — both are `a` rows
    // only. Zero-row idle heartbeats may interleave and are skipped.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut saw_gap_row = false;
    while !saw_gap_row {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for gap row id=13 after rejoin"
        );
        let envelope = next_change_envelope(&mut stream_a2, "post-rejoin change").await?;
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

    // Once the replayed gap is drained, the resumed dataset reaches Ready via
    // the caught-up live/heartbeat path — the restart-resume readiness-catchup
    // guarantee.
    wait_for_ready(&mut stream_a2, "rejoin readiness catch-up")
        .await?
        .commit()
        .await?;

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
        boot_a3
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        6,
        "snapshot_on_resume must deliver the full table on rejoin"
    );
    // The forced-snapshot final envelope is a not-ready boundary now; readiness
    // is lag-based and follows from the caught-up live/heartbeat path.
    assert!(
        !boot_a3.is_dataset_ready(),
        "forced resume snapshot boundary must not signal ready; readiness is lag-based"
    );
    assert!(
        ops_of(&boot_a3).iter().all(|op| op == "c"),
        "forced resume snapshot rows are op=c"
    );
    boot_a3.commit().await?;
    wait_for_ready(&mut stream_a3, "forced resume readiness catch-up")
        .await?
        .commit()
        .await?;

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

/// Regression for #11290: a partitioned source table on a shared slot. Without
/// `publish_via_partition_root` on the publication, pgoutput attributes changes
/// to each *leaf* partition, whose relation name has no registered member — so
/// the shared router drops every post-snapshot change (and `credit_idle`
/// advances the ack floor past them). The dataset boots with a correct snapshot
/// and then silently never updates. The publication must therefore be created
/// with the option so changes are reported under the parent relation the
/// dataset subscribes to; the inserts/updates/deletes below all route through
/// leaf partitions and must still reach the stream.
#[tokio::test(flavor = "multi_thread")]
async fn shared_slot_partitioned_source_table_streams_changes() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    // Range-partitioned parent with two leaf partitions. The partition key is
    // part of the primary key (required by Postgres for a partitioned PK),
    // which also gives every leaf a replica identity for UPDATE/DELETE.
    source
        .simple_query(
            "CREATE TABLE public.shared_repl_part (id int, name text, PRIMARY KEY (id)) \
                 PARTITION BY RANGE (id); \
             CREATE TABLE public.shared_repl_part_lo \
                 PARTITION OF public.shared_repl_part FOR VALUES FROM (0) TO (100); \
             CREATE TABLE public.shared_repl_part_hi \
                 PARTITION OF public.shared_repl_part FOR VALUES FROM (100) TO (1000); \
             INSERT INTO public.shared_repl_part VALUES (1, 'lo1'), (150, 'hi1');",
        )
        .await?;

    // Snapshot of the parent covers rows across all partitions.
    let mut stream = start_replication_stream(input_for(port, "shared_repl_part"));
    let boot = next_envelope(&mut stream, "bootstrap partitioned").await?;
    assert_eq!(
        boot.change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        2,
        "bootstrap covers rows from both partitions"
    );
    // Bootstrap boundary is not-ready now; readiness is lag-based and follows
    // from the caught-up live/heartbeat path.
    assert!(
        !boot.is_dataset_ready(),
        "bootstrap boundary must not signal ready; readiness is lag-based"
    );
    boot.commit().await?;
    wait_for_ready(&mut stream, "bootstrap readiness catch-up")
        .await?
        .commit()
        .await?;

    // With publish_via_partition_root the publication lists the parent, not the
    // leaves — which is also what keeps `has_table` true across restarts.
    assert_eq!(
        publication_tables(&source).await?,
        HashSet::from(["shared_repl_part".to_string()]),
        "publication lists the partitioned parent, not its leaves"
    );
    assert_eq!(slot_count(&source).await?, 1, "exactly one slot");

    // The core regression: post-snapshot WAL changes must route to the dataset
    // even though Postgres applies them via leaf partitions — one row per
    // partition, covering insert/update/delete.
    source
        .simple_query("INSERT INTO public.shared_repl_part VALUES (50, 'lo50')")
        .await?;
    expect_single_change(&mut stream, "insert into low partition", "c", 50).await?;

    source
        .simple_query("INSERT INTO public.shared_repl_part VALUES (200, 'hi200')")
        .await?;
    expect_single_change(&mut stream, "insert into high partition", "c", 200).await?;

    source
        .simple_query("UPDATE public.shared_repl_part SET name = 'lo50x' WHERE id = 50")
        .await?;
    expect_single_change(&mut stream, "update in low partition", "u", 50).await?;

    source
        .simple_query("DELETE FROM public.shared_repl_part WHERE id = 200")
        .await?;
    expect_single_change(&mut stream, "delete in high partition", "d", 200).await?;

    // --- Cleanup ---
    drop(stream);
    drop_replication_slot_when_inactive(&source, SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}

/// Mixed deployment: a **shared-slot group** (two member datasets multiplexed
/// onto one slot) and an **independent-slot dataset** (its own slot) run in the
/// **same process against the same Postgres at the same time**. This is the gap
/// the per-mode tests leave open — each of those provisions its own database and
/// exercises a single mode. Here we prove the two modes coexist: distinct slots
/// and walsenders, no publication/slot-name interference, and each per-worker
/// `FrameReader` decoding its own stream correctly under concurrent load.
#[tokio::test(flavor = "multi_thread")]
async fn shared_and_independent_slots_coexist() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "mix_shared_a", &[(1, "a1")]).await?;
    create_table(&source, "mix_shared_b", &[(1, "b1")]).await?;
    create_table(&source, "mix_indep", &[(1, "i1")]).await?;

    // Shared-slot group: two members on ONE slot. Independent dataset: its OWN
    // slot — all three streaming in this one process against this one Postgres.
    let mut shared_a = start_replication_stream(input_for(port, "mix_shared_a"));
    let boot_a = next_envelope(&mut shared_a, "bootstrap shared_a").await?;
    assert_eq!(
        boot_a
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        1
    );
    boot_a.commit().await?;

    let mut shared_b = start_replication_stream(input_for(port, "mix_shared_b"));
    let boot_b = next_envelope(&mut shared_b, "bootstrap shared_b").await?;
    assert_eq!(
        boot_b
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        1
    );
    boot_b.commit().await?;

    let mut indep = start_replication_stream(independent_input(port, "mix_indep"));
    let boot_i = next_envelope(&mut indep, "bootstrap indep").await?;
    assert_eq!(
        boot_i
            .change_batch()
            .expect("built change batch")
            .record
            .num_rows(),
        1
    );
    boot_i.commit().await?;

    // Both slots exist side by side: one shared, one independent.
    let shared_slots: i64 = source
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&SLOT],
        )
        .await?
        .get(0);
    let indep_slots: i64 = source
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&INDEP_SLOT],
        )
        .await?
        .get(0);
    assert_eq!(shared_slots, 1, "shared slot must be present");
    assert_eq!(indep_slots, 1, "independent slot must be present");

    // Concurrent live changes to all three tables must route to the right
    // stream — the shared pump demultiplexes A vs B by relation, and the
    // independent slot delivers only its own table.
    source
        .simple_query("INSERT INTO public.mix_shared_a VALUES (2, 'a2')")
        .await?;
    source
        .simple_query("INSERT INTO public.mix_shared_b VALUES (2, 'b2')")
        .await?;
    source
        .simple_query("INSERT INTO public.mix_indep VALUES (2, 'i2')")
        .await?;

    expect_single_change(&mut shared_a, "shared_a live insert", "c", 2).await?;
    expect_single_change(&mut shared_b, "shared_b live insert", "c", 2).await?;
    expect_single_change(&mut indep, "indep live insert", "c", 2).await?;

    // --- Cleanup ---
    drop(shared_a);
    drop(shared_b);
    drop(indep);
    drop_replication_slot_when_inactive(&source, SLOT).await?;
    drop_replication_slot_when_inactive(&source, INDEP_SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {INDEP_PUBLICATION}"))
        .await?;
    Ok(())
}

/// Postgres `pg_lsn` text form of a numeric LSN, for comparisons in SQL.
fn lsn_text(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// Whether the slot has acknowledged everything up to `lsn` — the point past
/// which Postgres is free to recycle that WAL. Returns the verdict and the
/// slot's current `confirmed_flush_lsn` for the assertion message.
async fn slot_acked_past(
    client: &tokio_postgres::Client,
    lsn: &str,
) -> Result<(bool, String), anyhow::Error> {
    let row = client
        .query_one(
            "SELECT confirmed_flush_lsn >= $1::text::pg_lsn, confirmed_flush_lsn::text \
             FROM pg_replication_slots WHERE slot_name = $2",
            &[&lsn, &SLOT],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}

/// Regression for #11896: a durable acceleration whose CDC bootstrap was lost to
/// a crash before it became durable must be re-loaded, not resumed over.
///
/// The reported failure was that the slot's *existence* suppressed the
/// re-bootstrap: after a hard kill before the mem-tier seal, the acceleration was
/// empty, the slot was already there, `need_snapshot` was therefore false, and no
/// pass ever reconciled against the source — so the rows were gone permanently.
///
/// A crash before the seal is reproducible without killing anything: durability
/// is what gates the acknowledgement, so an unsealed bootstrap is exactly a
/// bootstrap whose envelopes were never committed. Dropping the stream without
/// committing leaves the same state a `SIGKILL` would — the slot exists, nothing
/// was acknowledged, and no position was recorded.
///
/// The acceleration must then be re-loaded (a fresh snapshot) or asked to rebuild.
/// Resuming is the failure.
#[tokio::test(flavor = "multi_thread")]
async fn a_bootstrap_lost_before_it_was_durable_is_reloaded_not_resumed()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "lost_bootstrap", &[(1, "alice"), (2, "bob")]).await?;

    // --- 1. First start: nothing has been recorded for this durable acceleration,
    // so it is asked to load from the source rather than resume. Nothing is
    // committed — the acknowledgement is what durability gates, so this leaves the
    // state a crash before the seal leaves behind. ---
    let store = InMemoryAppliedLsnStore::shared();
    let mut first = start_replication_stream(input_with_watermark(port, "lost_bootstrap", &store));
    let opening = next_envelope(&mut first, "first envelope before the crash").await?;
    anyhow::ensure!(
        opening.history_unavailable(),
        "a durable acceleration with nothing recorded must be asked to load from the source"
    );
    drop(opening); // never committed — nothing became durable, nothing was recorded
    drop(first);
    wait_for_walsender_count(&source, 0).await?;

    // The slot survives the crash, which is what used to suppress the re-load.
    assert_eq!(slot_count(&source).await?, 1, "the slot must persist");
    anyhow::ensure!(
        store.recorded_lsn().is_none(),
        "nothing may have been recorded: the acknowledgement that records a position is gated on \
         the durability this test is simulating the loss of"
    );

    // --- 2. Restart against the same (durable, now empty) acceleration. It must
    // be re-loaded rather than resumed. ---
    let mut restarted =
        start_replication_stream(input_with_watermark(port, "lost_bootstrap", &store));
    let envelope = next_envelope(&mut restarted, "first envelope after the restart").await?;
    let reloaded = envelope.history_unavailable() || num_rows(&envelope) == 2;
    anyhow::ensure!(
        reloaded,
        "a durable acceleration whose load was lost before it became durable was neither \
         re-snapshotted nor asked to rebuild — its rows are gone permanently (#11896). The slot's \
         existence must not suppress the re-load. First envelope carried {} row(s), \
         history_unavailable={}",
        num_rows(&envelope),
        envelope.history_unavailable()
    );
    envelope.commit().await?;

    drop(restarted);
    wait_for_walsender_count(&source, 0).await?;
    drop_replication_slot_when_inactive(&source, SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}

/// Take the slot away from a *running* stream, the way an operator does.
///
/// The pump reconnects on its own, so the drop has to win a race against it: a
/// slot an active walsender holds cannot be dropped (SQLSTATE 55006), and the
/// walsender comes back after each eviction. Evict, try to drop, repeat until the
/// drop lands — polling the actual condition rather than sleeping in the hope the
/// pump is between attempts.
async fn drop_slot_underneath_a_running_stream(
    source: &tokio_postgres::Client,
    slot: &str,
) -> Result<(), anyhow::Error> {
    let deadline = std::time::Instant::now() + Duration::from_mins(1);
    let mut last_error = None;
    while std::time::Instant::now() < deadline {
        source
            .execute(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                 WHERE slot_name = $1 AND active_pid IS NOT NULL",
                &[&slot],
            )
            .await?;
        match source
            .execute("SELECT pg_drop_replication_slot($1)", &[&slot])
            .await
        {
            Ok(_) => return Ok(()),
            // Already gone, which is the state this is trying to reach.
            Err(error) if error.code() == Some(&SqlState::UNDEFINED_OBJECT) => return Ok(()),
            Err(error) => last_error = Some(error.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(anyhow::anyhow!(
        "could not take slot {slot} away from the running stream. Last error: {}",
        last_error.unwrap_or_else(|| "none".to_string())
    ))
}

/// A replication slot lost while Spice is *running* must be recovered without a
/// restart.
///
/// Recovery for a slot that is missing or invalidated at *startup* already
/// existed, but it was only reachable through setup. Mid-stream,
/// `START_REPLICATION` fails permanently, that error was classified fatal, and the
/// stream stopped — the runtime stays healthy, so nothing necessarily restarts it,
/// and the dataset was down until an operator noticed.
///
/// The source deletes a row while the slot is gone, so the recovery is exercised
/// against the case that makes it a correctness fix and not only an availability
/// one: that deletion has no change event left to replay, and an acceleration that
/// merely resumed would keep the deleted row in every later query.
///
/// What this suite asserts is that the *request* to rebuild reaches the consumer,
/// once, on a stream that was never restarted — there is no accelerator here to
/// converge. The rebuild's own correctness (a streaming `InsertOp::Overwrite`
/// rather than an upsert over survivors) is the same path a startup rebuild takes
/// and is covered where that path is.
#[tokio::test(flavor = "multi_thread")]
async fn a_slot_lost_while_running_is_recovered_without_a_restart() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "slot_lost_live", &[(1, "alice"), (2, "bob")]).await?;

    // A durable acceleration observed to be empty, so the first load is an ordinary
    // snapshot bootstrap and any later rebuild is unambiguously this recovery.
    let store = InMemoryAppliedLsnStore::shared();
    let mut stream = start_replication_stream(input_with_contents(
        port,
        "slot_lost_live",
        &store,
        AccelerationContents::Empty,
    ));

    let bootstrap = next_envelope(&mut stream, "the initial snapshot").await?;
    anyhow::ensure!(
        !bootstrap.history_unavailable(),
        "the first load must be a snapshot bootstrap, so a rebuild later in this test can only \
         be the slot recovery"
    );
    assert_eq!(ids_of(&bootstrap), vec![1, 2], "the snapshot's rows");
    bootstrap.commit().await?;

    // Stream one live change, so a position is recorded and the acceleration has a
    // watermark for the lost slot to become unreachable from.
    source
        .execute("INSERT INTO public.slot_lost_live VALUES (3, 'carol')", &[])
        .await?;
    expect_single_change(&mut stream, "the pre-loss insert", "c", 3).await?;
    // The recorded position is what the replacement slot will be unable to reach,
    // so the loss has to happen after one exists. Written by a background task
    // after the commit above proved durable, so poll for it rather than assuming
    // the commit was enough.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while store.recorded_lsn().is_none() {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "no position was ever recorded, so there is no watermark for the replacement slot to \
             be unable to reach and this test would pass for the wrong reason"
        );
        let envelope = next_envelope(&mut stream, "an envelope carrying a position").await?;
        envelope.commit().await?;
    }

    // --- Lose the slot, and delete a row while it is gone. ---
    drop_slot_underneath_a_running_stream(&source, SLOT).await?;
    source
        .execute("DELETE FROM public.slot_lost_live WHERE id = 1", &[])
        .await?;

    // The same stream — never restarted — must be asked to rebuild.
    let mut rebuild_requests = 0;
    let deadline = std::time::Instant::now() + Duration::from_mins(2);
    loop {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the stream never recovered from losing its replication slot. Losing the slot must not \
             end the dataset: the slot can be replaced and the acceleration rebuilt from the \
             source, without a restart"
        );
        let envelope = next_envelope(&mut stream, "the rebuild request").await?;
        if envelope.history_unavailable() {
            rebuild_requests += 1;
            anyhow::ensure!(
                num_rows(&envelope) == 0,
                "the rebuild request is a signal, not data: the consumer replaces the \
                 acceleration's contents itself, so carrying rows here would append to what it is \
                 about to replace. Carried {} row(s)",
                num_rows(&envelope)
            );
        }
        envelope.commit().await?;
        // Committing the request is what releases the member's hold, so the loop
        // must not exit before that lands.
        if rebuild_requests > 0 {
            break;
        }
    }

    // A replacement slot exists, and exactly one: recovery must not leave the dead
    // slot behind, nor accumulate a slot per attempt.
    assert_eq!(
        slot_count(&source).await?,
        1,
        "recovery must leave exactly one slot behind"
    );

    // Streaming continues on the replacement, which is the half that used to need a
    // restart — and getting there is also what proves the rebuild was requested
    // *once*. Every envelope in between is inspected rather than skipped:
    // `next_change_envelope` would commit and discard a second zero-row rebuild
    // request along with the idle heartbeats, leaving a rebuild loop invisible here.
    source
        .execute("INSERT INTO public.slot_lost_live VALUES (4, 'dave')", &[])
        .await?;
    let deadline = std::time::Instant::now() + Duration::from_mins(2);
    let resumed = loop {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the insert made after the recovery never arrived, so streaming did not resume on the \
             replacement slot"
        );
        let envelope = next_envelope(&mut stream, "an insert after the recovery").await?;
        if envelope.history_unavailable() {
            rebuild_requests += 1;
        }
        anyhow::ensure!(
            rebuild_requests == 1,
            "the acceleration must be asked to rebuild once per lost slot, not once per reconnect \
             attempt — a rebuild loop re-reads the whole source table on a cycle. Saw \
             {rebuild_requests} requests"
        );
        if num_rows(&envelope) > 0 {
            break envelope;
        }
        envelope.commit().await?;
    };
    assert_eq!(ops_of(&resumed), vec!["c".to_string()]);
    assert_eq!(ids_of(&resumed), vec![4]);
    resumed.commit().await?;

    drop(stream);
    wait_for_walsender_count(&source, 0).await?;
    drop_replication_slot_when_inactive(&source, SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}

/// A durable acceleration holding no rows must load through the ordinary snapshot
/// bootstrap, not a rebuild.
///
/// Nothing has been recorded for it, and a missing watermark is normally treated
/// as evidence of a gap, because it cannot be told apart from one whose write
/// failed. Emptiness settles that: an acceleration with no rows has nothing that
/// could be stale and no deletion it could be missing, so there is nothing for a
/// rebuild to repair. Every durable CDC acceleration starts here exactly once, so
/// getting this wrong routes every first load through a full re-read of the
/// source (#13118).
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_acceleration_bootstraps_rather_than_rebuilding() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "fresh_empty", &[(1, "alice"), (2, "bob")]).await?;

    // A durable acceleration (the store records positions) that has recorded
    // nothing, observed to hold no rows — the state every first load is in.
    let store = InMemoryAppliedLsnStore::shared();
    let input = input_with_contents(port, "fresh_empty", &store, AccelerationContents::Empty);
    let metrics = ReplicationMetrics::new(Arc::clone(&input.metrics));
    let mut stream = start_replication_stream(input);

    let envelope = next_envelope(&mut stream, "first envelope of a fresh acceleration").await?;
    anyhow::ensure!(
        !envelope.history_unavailable(),
        "an acceleration observed to hold no rows was asked to rebuild from the source. There is \
         nothing in it that could be stale, so the rebuild repairs nothing and only re-reads the \
         whole table on what is the first load of every durable CDC acceleration (#13118)"
    );
    anyhow::ensure!(
        num_rows(&envelope) == 2,
        "the snapshot bootstrap must deliver the source rows; first envelope carried {} row(s)",
        num_rows(&envelope)
    );
    assert_eq!(
        ids_of(&envelope),
        vec![1, 2],
        "the bootstrap must carry the rows present at the source"
    );
    envelope.commit().await?;

    // `history_unavailable` being clear only says no rebuild was requested. The
    // snapshot bootstrap counts the rows it delivers and the rebuild path runs no
    // snapshot at all, so this is what tells "bootstrapped" apart from "loaded by
    // some other means".
    anyhow::ensure!(
        metrics.bootstrap_rows_total() > 0,
        "no rows were delivered by the snapshot bootstrap, so the acceleration was not loaded \
         through it"
    );

    drop(stream);
    wait_for_walsender_count(&source, 0).await?;
    drop_replication_slot_when_inactive(&source, SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}

/// An empty acceleration must still be **loaded** when no snapshot is going to
/// run, which means rebuilding it.
///
/// Emptiness says only that there is nothing stale to repair. It does not load
/// the table. When the slot already exists and its publication already carries
/// this table, none of `need_snapshot`'s conditions hold, so no snapshot runs —
/// and if the rebuild is skipped too, the member resumes from the slot's
/// position and every row committed before it is missing from the acceleration
/// permanently. That is silent, unrecoverable data loss: no change event for
/// those rows will ever be replayed.
///
/// Reached by starting a stream (creating the slot and publication), dropping
/// it, and rejoining against a *fresh* acceleration — the shape of a deleted or
/// relocated accelerator directory under a slot that outlived it.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_acceleration_is_still_loaded_when_no_snapshot_runs() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "slot_outlived", &[(1, "alice"), (2, "bob")]).await?;

    // First start: creates the slot and puts the table in the publication, and
    // commits so the slot holds a real position.
    let first_store = InMemoryAppliedLsnStore::shared();
    let mut first = start_replication_stream(input_with_contents(
        port,
        "slot_outlived",
        &first_store,
        AccelerationContents::Empty,
    ));
    next_envelope(&mut first, "first-start bootstrap")
        .await?
        .commit()
        .await?;
    drop(first);
    wait_for_walsender_count(&source, 0).await?;

    // Rows committed while nothing is streaming. A resume from the slot position
    // would carry these, but never the two rows that predate the slot.
    source
        .execute(
            "INSERT INTO public.slot_outlived (id, name) VALUES ($1, 'carol')",
            &[&3_i32],
        )
        .await?;

    // Rejoin with a brand-new acceleration: empty, nothing recorded, against the
    // surviving slot and publication. No snapshot can run in this state.
    let store = InMemoryAppliedLsnStore::shared();
    let input = input_with_contents(port, "slot_outlived", &store, AccelerationContents::Empty);
    let metrics = ReplicationMetrics::new(Arc::clone(&input.metrics));
    let mut rejoined = start_replication_stream(input);

    let envelope =
        next_envelope(&mut rejoined, "first envelope after the slot outlived it").await?;
    let loaded = envelope.history_unavailable() || metrics.bootstrap_rows_total() > 0;
    anyhow::ensure!(
        loaded,
        "an empty acceleration was neither rebuilt nor snapshotted when it rejoined a slot that \
         outlived it, so it resumed from the slot position and the rows committed before that \
         position are gone for good. Emptiness means there is nothing stale to repair — it does \
         not mean something else will load the table"
    );
    envelope.commit().await?;

    drop(rejoined);
    wait_for_walsender_count(&source, 0).await?;
    drop_replication_slot_when_inactive(&source, SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}

/// An acceleration that comes back **empty while its recorded position survived**
/// must be loaded, not resumed — the shape a `mode: file_update` recreate leaves
/// behind (#13546).
///
/// The recreate drops the accelerated table because the source schema changed
/// incompatibly, while the watermark sidecar lives in the same accelerator and is
/// not dropped with it. So the next start finds no rows and a position the slot
/// can still stream from, and every arm of the resume decision is individually
/// satisfied: the slot is valid, retention is intact, the position is this
/// source's. Resuming on it succeeds and the rows committed before that position
/// are never loaded by anything.
///
/// Distinct from [`an_empty_acceleration_is_still_loaded_when_no_snapshot_runs`],
/// which reaches the same "must be loaded" conclusion from a *missing* record. Here
/// the record is present and usable, which is the reason a resume looks safe.
///
/// The two rows written before the first start are what the assertion is about:
/// they precede the recorded position, so no reachable WAL carries them and only a
/// rebuild or a snapshot can put them back.
#[tokio::test(flavor = "multi_thread")]
async fn an_empty_acceleration_with_a_surviving_position_is_loaded_not_resumed()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "recreated", &[(1, "alice"), (2, "bob")]).await?;

    // First start: creates the slot and publication, and commits so a real
    // position is recorded in the store.
    let store = InMemoryAppliedLsnStore::shared();
    let mut first = start_replication_stream(input_with_contents(
        port,
        "recreated",
        &store,
        AccelerationContents::Empty,
    ));
    next_envelope(&mut first, "first-start bootstrap")
        .await?
        .commit()
        .await?;
    // The bootstrap stream is `snapshot.chain(boundary)`, and it is the zero-row
    // boundary — not the data envelope above — whose committer publishes the
    // watermark. Both rows fit one batch, so without polling this second envelope
    // the stream is dropped before any position is recorded.
    next_envelope(&mut first, "first-start snapshot boundary")
        .await?
        .commit()
        .await?;
    drop(first);
    wait_for_walsender_count(&source, 0).await?;

    // The recorded position is the whole point of this case: without it the
    // rejoin below is the already-covered missing-record case.
    wait_for_recorded_position(&store, "the first start").await?;

    // Rejoin on the SAME store — the position survived — while the acceleration
    // is observed empty, which is what the recreate left behind.
    let input = input_with_contents(port, "recreated", &store, AccelerationContents::Empty);
    let metrics = ReplicationMetrics::new(Arc::clone(&input.metrics));
    let mut rejoined = start_replication_stream(input);

    let envelope = next_envelope(&mut rejoined, "first envelope after the recreate").await?;
    let loaded = envelope.history_unavailable() || metrics.bootstrap_rows_total() > 0;
    anyhow::ensure!(
        loaded,
        "an emptied acceleration resumed from the position it recorded before it was emptied, so \
         every row committed below that position is missing from it for good. A recorded position \
         means those changes will never be resent — it does not mean the rows are here"
    );
    envelope.commit().await?;

    drop(rejoined);
    wait_for_walsender_count(&source, 0).await?;
    drop_replication_slot_when_inactive(&source, SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}

/// The other side of [`an_empty_acceleration_bootstraps_rather_than_rebuilding`]:
/// an acceleration that holds rows it cannot place must still be rebuilt.
///
/// This is the case the rebuild exists for. Rows are present, no position says
/// what they are current as of, and a row deleted at the source while the
/// acceleration was away produces no change row — appending a snapshot over the
/// top would upsert every surviving row and leave the deleted one behind
/// forever. Only re-reading the table removes it.
///
/// A probe that could not answer must land here too: not knowing whether rows are
/// present is not the same as knowing there are none, and only the latter is
/// proof.
#[tokio::test(flavor = "multi_thread")]
async fn an_unplaceable_acceleration_still_rebuilds() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "unplaceable", &[(1, "alice")]).await?;

    for (contents, described) in [
        (AccelerationContents::NonEmpty, "observed to hold rows"),
        (AccelerationContents::Unknown, "could not be read"),
    ] {
        let store = InMemoryAppliedLsnStore::shared();
        let input = input_with_contents(port, "unplaceable", &store, contents);
        let metrics = ReplicationMetrics::new(Arc::clone(&input.metrics));
        let mut stream = start_replication_stream(input);
        let envelope =
            next_envelope(&mut stream, "first envelope of an unplaceable acceleration").await?;
        anyhow::ensure!(
            envelope.history_unavailable(),
            "an acceleration whose contents {described}, with no recorded position, was resumed \
             rather than rebuilt. A row deleted at the source while it was away produces no \
             change row, so nothing but a re-read of the table would ever remove it (#12922)"
        );
        envelope.commit().await?;
        anyhow::ensure!(
            metrics.bootstrap_rows_total() == 0,
            "the rebuild replaces the acceleration's contents, so no snapshot may also be \
             appended over them"
        );

        drop(stream);
        wait_for_walsender_count(&source, 0).await?;
    }

    drop_replication_slot_when_inactive(&source, SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}

/// An applied-position store whose writes are slow, for showing that the commit path
/// does not wait on them.
#[derive(Default)]
struct SlowAppliedLsnStore {
    recorded: std::sync::Mutex<Option<AppliedLsn>>,
    saves: std::sync::atomic::AtomicUsize,
}

impl SlowAppliedLsnStore {
    const DELAY: Duration = Duration::from_millis(250);

    fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn saves(&self) -> usize {
        self.saves.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[async_trait::async_trait]
impl AppliedLsnStore for SlowAppliedLsnStore {
    async fn load(
        &self,
    ) -> std::result::Result<RecordedPosition, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self
            .recorded
            .lock()
            .expect("watermark mutex")
            .map_or(RecordedPosition::Absent, RecordedPosition::At))
    }

    async fn save(
        &self,
        applied: AppliedLsn,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tokio::time::sleep(Self::DELAY).await;
        *self.recorded.lock().expect("watermark mutex") = Some(applied);
        self.saves.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(())
    }

    async fn clear(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        *self.recorded.lock().expect("watermark mutex") = None;
        Ok(())
    }

    fn records_positions(&self) -> bool {
        true
    }
}

/// Committing publishes a position and returns; only the applied-position writer
/// touches the store. So a store that is slow to write must not make commits slow —
/// if it did, every dataset on a shared slot would apply changes at the speed of its
/// accelerator's bookkeeping writes.
///
/// The delay is fixed because store latency is the thing under test.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_position_store_does_not_slow_the_commit_path() -> Result<(), anyhow::Error> {
    const COMMITS: usize = 8;

    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "slow_store", &[(1, "one")]).await?;

    let store = SlowAppliedLsnStore::shared();
    let mut member = start_replication_stream(input_with_watermark(port, "slow_store", &store));
    next_envelope(&mut member, "bootstrap")
        .await?
        .commit()
        .await?;

    let mut committed = 0_usize;
    let mut spent_committing = Duration::ZERO;
    for id in 0..COMMITS {
        source
            .execute(
                "INSERT INTO public.slow_store (id, name) VALUES ($1, 'row')",
                &[&i32::try_from(id + 10).expect("id fits")],
            )
            .await?;
        if let Ok(envelope) = next_envelope(&mut member, "change").await {
            let started = std::time::Instant::now();
            envelope.commit().await?;
            spent_committing += started.elapsed();
            committed += 1;
        }
    }
    anyhow::ensure!(
        committed > 0,
        "no change was committed, so nothing about the commit path was measured"
    );

    let serialized = SlowAppliedLsnStore::DELAY * u32::try_from(committed).expect("fits");
    eprintln!(
        "commit path: {spent_committing:?} across {committed} commit(s); \
         waiting for each {:?} store write would have cost {serialized:?}; \
         store writes so far: {}",
        SlowAppliedLsnStore::DELAY,
        store.saves()
    );
    anyhow::ensure!(
        spent_committing * 2 < serialized,
        "commits are waiting on the applied-position store: {spent_committing:?} across \
         {committed} commit(s), against {serialized:?} if each had waited for its write. \
         Publishing the position must not put store I/O on the commit path"
    );

    drop(member);
    wait_for_walsender_count(&source, 0).await?;
    drop_replication_slot_when_inactive(&source, SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}

/// The counterpart to the rebuild tests: a dataset that simply restarts must
/// **resume**, not rebuild. Without this, a rule that classifies gaps too eagerly
/// passes every rebuild test while re-reading the whole table on every restart.
///
/// The case that makes this easy to get wrong is a *quiet* table. The slot's
/// acknowledgement is advanced for an idle member by the pump's keepalive crediting,
/// which routes no envelope and therefore records no watermark — so the slot's
/// `confirmed_flush_lsn` legitimately drifts ahead of the last position the
/// acceleration recorded as applied. That drift is not a gap: crediting only covers
/// WAL that contained no changes for this table. Treating it as one would rebuild a
/// healthy acceleration on every restart.
#[tokio::test(flavor = "multi_thread")]
async fn a_quiet_dataset_resumes_across_a_restart_rather_than_rebuilding()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "quiet", &[(1, "one")]).await?;

    // --- 1. Bootstrap and commit, so a position is recorded, then let the table go
    // quiet while a second table's traffic keeps the slot acknowledging forward. ---
    let store = InMemoryAppliedLsnStore::shared();
    let mut quiet = start_replication_stream(input_with_watermark(port, "quiet", &store));
    next_envelope(&mut quiet, "bootstrap quiet")
        .await?
        .commit()
        .await?;
    wait_for_ready(&mut quiet, "quiet readiness")
        .await?
        .commit()
        .await?;
    let recorded = store
        .recorded_lsn()
        .ok_or_else(|| anyhow::anyhow!("the member must record a position while attached"))?;

    // Drive the slot's acknowledgement past the quiet table's recorded position using
    // WAL that contains nothing for it. This is the benign drift the rule must not
    // mistake for a gap; the test asserts it was actually reached.
    let flush_every = input_for(port, "quiet").params.watermark_flush_interval;
    let drift_started = std::time::Instant::now();
    create_table(&source, "noisy", &[(1, "n1")]).await?;
    // The busy member records its position through its own commits. Counting its
    // writes against its commits is what shows they are coalesced by the single
    // writer rather than issued one per commit.
    let noisy_store = InMemoryAppliedLsnStore::shared();
    let mut noisy = start_replication_stream(input_with_watermark(port, "noisy", &noisy_store));
    next_envelope(&mut noisy, "bootstrap noisy")
        .await?
        .commit()
        .await?;

    let deadline = std::time::Instant::now() + Duration::from_mins(1);
    let mut drifted = false;
    let mut churn_id = 100;
    let mut noisy_commits = 0_usize;
    while std::time::Instant::now() < deadline {
        churn_id += 1;
        source
            .execute(
                "INSERT INTO public.noisy (id, name) VALUES ($1, 'churn')",
                &[&churn_id],
            )
            .await?;
        if let Ok(envelope) = next_envelope(&mut noisy, "noisy churn").await {
            envelope.commit().await?;
            noisy_commits += 1;
        }
        if let Ok(envelope) = next_envelope(&mut quiet, "quiet heartbeat").await {
            envelope.commit().await?;
        }
        let (past, _) = slot_acked_past(&source, &lsn_text(recorded)).await?;
        if past {
            drifted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::ensure!(
        drifted,
        "the test could not reach the state it exists to cover: the slot never acknowledged past \
         the quiet table's recorded position, so no drift was constructed"
    );

    // --- 2. Restart the quiet dataset against the position it recorded. The slot is
    // acknowledged past it, but only over WAL that held nothing for this table. ---
    drop(quiet);
    drop(noisy);
    wait_for_walsender_count(&source, 0).await?;

    // The carry-forward has to have actually run — otherwise this test would also
    // pass on a build that simply never rebuilds — and it has to be paced by
    // `watermark_flush_interval` rather than firing on every keepalive. The quiet
    // table sees no changes, so every save past its boundary envelope is a
    // carry-forward.
    let saves = store.saves();
    let elapsed_ticks = drift_started.elapsed().as_secs_f64() / flush_every.as_secs_f64();
    anyhow::ensure!(
        saves > 1,
        "the quiet table's recorded position was never carried forward ({saves} write(s) total), \
         so this test would pass for the wrong reason"
    );
    #[expect(
        clippy::cast_precision_loss,
        reason = "counter is small; the comparison is a loose bound"
    )]
    let saves_f = saves as f64;
    // Integration-test worker logs are dropped, so print the measurements.
    let noisy_saves = noisy_store.saves();
    eprintln!(
        "quiet-member position writes: {saves} over {elapsed_ticks:.1} flush interval(s) of {flush_every:?}"
    );
    eprintln!("busy-member position writes: {noisy_saves} for {noisy_commits} commit(s)");
    anyhow::ensure!(
        saves_f <= elapsed_ticks + 3.0,
        "the recorded position is being written far more often than once per flush interval \
         ({saves} writes over {elapsed_ticks:.1} intervals), which would put avoidable I/O on \
         every idle member of every shared slot"
    );

    let mut restarted = start_replication_stream(input_with_watermark(port, "quiet", &store));
    let envelope = next_envelope(&mut restarted, "first envelope after the restart").await?;
    anyhow::ensure!(
        !envelope.history_unavailable(),
        "a quiet dataset was asked to rebuild after an ordinary restart. The slot's acknowledgement \
         drifting ahead of the recorded position through idle crediting is not a gap, and treating \
         it as one re-reads the whole table on every restart"
    );
    envelope.commit().await?;

    drop(restarted);
    wait_for_walsender_count(&source, 0).await?;
    drop_replication_slot_when_inactive(&source, SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}

/// Regression for #11289 variant 2: a dataset removed from the Spicepod, whose
/// table stays in the publication, must not silently miss the changes committed
/// while it was gone once the slot's hold on its behalf lapses.
///
/// Reproducing this needs both halves of the protection to be absent, which is
/// why a simpler construction does not work:
///
///   * **A real restart.** A merely detached member keeps its `AckSlot`, and
///     `flush_lsn` is the minimum over *all* members including held ones — so its
///     frozen floor pins the slot and nothing can ack past it. Every stream has
///     to go away so the source, and its `AckTable`, are discarded.
///   * **A lapsed reservation.** On the next attach the slot reserves the floor
///     for every published table with no member, which protects the absent one
///     until the grace expires. `unclaimed_reservation_grace` is shortened here
///     because the behavior after expiry is otherwise unreachable in under five
///     minutes.
///
/// Once both are gone the surviving member's traffic carries the slot's
/// acknowledgement past the absent table's change. Rejoining must then either
/// deliver that change or ask for a rebuild — what it must never do is resume
/// into live traffic as if nothing were missing.
#[tokio::test(flavor = "multi_thread")]
async fn a_dataset_re_added_after_its_reservation_lapsed_does_not_silently_skip_changes()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "lapsed_mate", &[(1, "mate1")]).await?;
    create_table(&source, "lapsed_absent", &[(1, "absent1")]).await?;

    // Shorten the hold so expiry is reachable. It is read from the params of
    // whichever member opens the slot, so both members carry it.
    let brief_grace = Duration::from_secs(2);
    let short_grace = |mut input: ReplicationStreamInput| {
        input.params.unclaimed_reservation_grace = brief_grace;
        input
    };

    // --- 1. Both tables join, establishing publication membership, slot history,
    // and a recorded position for the table that is about to disappear. ---
    let absent_store = InMemoryAppliedLsnStore::shared();
    let mut mate = start_replication_stream(short_grace(input_for(port, "lapsed_mate")));
    next_envelope(&mut mate, "bootstrap mate")
        .await?
        .commit()
        .await?;

    let mut absent = start_replication_stream(short_grace(input_with_watermark(
        port,
        "lapsed_absent",
        &absent_store,
    )));
    next_envelope(&mut absent, "bootstrap absent")
        .await?
        .commit()
        .await?;
    wait_for_ready(&mut absent, "absent readiness")
        .await?
        .commit()
        .await?;
    let recorded = absent_store
        .recorded_lsn()
        .ok_or_else(|| anyhow::anyhow!("the member must record a position while attached"))?;

    // --- 2. A restart: every stream goes away, so the pump exits and the source
    // (with its AckTable and every held floor) is discarded. The slot and the
    // publication both persist, which is what makes this a restart. ---
    drop(mate);
    drop(absent);
    wait_for_walsender_count(&source, 0).await?;
    assert_eq!(slot_count(&source).await?, 1, "the slot must persist");

    // --- 3. The absent table changes while nothing is consuming it. ---
    source
        .simple_query("INSERT INTO public.lapsed_absent VALUES (2, 'missed-while-removed')")
        .await?;
    let missed_lsn: String = source
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);

    // --- 4. Only the surviving dataset comes back. Its attach reserves the floor
    // for the absent table; after the grace lapses, its own traffic carries the
    // slot's acknowledgement past the missed change. ---
    let mut mate = start_replication_stream(short_grace(input_for(port, "lapsed_mate")));

    // Committing the mate's envelopes — including its idle heartbeats — is what
    // carries the slot's acknowledgement forward; the reservation for the absent
    // table has to lapse first, which is why the grace is shortened above.
    let deadline = std::time::Instant::now() + Duration::from_mins(1);
    let mut acked_past = false;
    let mut churn_id = 100;
    while std::time::Instant::now() < deadline {
        churn_id += 1;
        source
            .execute(
                "INSERT INTO public.lapsed_mate (id, name) VALUES ($1, 'mate-churn')",
                &[&churn_id],
            )
            .await?;
        if let Ok(envelope) = next_envelope(&mut mate, "mate churn").await {
            envelope.commit().await?;
        }
        let (past, _) = slot_acked_past(&source, &missed_lsn).await?;
        if past {
            acked_past = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::ensure!(
        acked_past,
        "the test could not reach the state it exists to cover: the slot never acknowledged past \
         the absent table's change, so the reservation never lapsed"
    );

    // --- 5. Releasing the hold also drops the table from the publication, which
    // on its own would send a returning dataset through the initial-snapshot path
    // (`table_added = true`) and recover it for a reason that has nothing to do
    // with its recorded position. Put the table back with no member attached, so
    // the re-add sees `table_added = false` and recovery has to come from the
    // recorded position — either replaying from it, or reporting that the history
    // it needs is gone.
    source
        .simple_query(&format!(
            "ALTER PUBLICATION {PUBLICATION} ADD TABLE public.lapsed_absent"
        ))
        .await
        .ok();

    // --- 6. The dataset is re-added, carrying the position it recorded before it
    // left. Either outcome is correct; resuming into live traffic is not. ---
    let mut re_added = start_replication_stream(short_grace(input_with_watermark(
        port,
        "lapsed_absent",
        &InMemoryAppliedLsnStore::seeded(recorded),
    )));
    // Idle heartbeats carry no rows and are not an answer either way, so commit
    // past them and wait for one of the two acceptable outcomes: the missed change
    // replayed, or a report that the history needed to replay it is gone.
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    let mut recovered = false;
    while std::time::Instant::now() < deadline {
        let envelope = next_envelope(&mut re_added, "re-added dataset envelope").await?;
        recovered = envelope.history_unavailable() || ids_of(&envelope).contains(&2);
        envelope.commit().await?;
        if recovered {
            break;
        }
    }
    anyhow::ensure!(
        recovered,
        "a dataset re-added after its reservation lapsed neither received the change committed \
         while it was gone (id=2) nor asked to be rebuilt — it resumed as if nothing were missing \
         (#11289)"
    );

    drop(mate);
    drop(re_added);
    wait_for_walsender_count(&source, 0).await?;
    drop_replication_slot_when_inactive(&source, SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}

/// Regression for #12609: on a shared slot that is *resuming* — every member
/// rejoining without an initial snapshot, which is what a durable accelerator
/// does on every restart — the member that joins SECOND must still receive the
/// changes committed while the slot had no consumer.
///
/// The hazard is join order, not the restart itself. The first joiner starts
/// the pump, is promoted to streaming on connect, and, having nothing in
/// flight, is credited up to the server's WAL end by the next keepalive
/// (`AckTable::credit_idle`). A member registering after that is seated at the
/// shared floor, which is now that credited position — above changes to its own
/// table that nobody ever consumed. The floor only ever rises, so the pump's
/// reconnect cannot go back for them: the rows are acknowledged to Postgres
/// without being applied, no error is raised, and the accelerated table
/// silently disagrees with the source until something forces a re-snapshot.
///
/// Both tables get a row while the slot is idle. The first joiner's row proves
/// the resume replay works at all; the second joiner's row is the one that goes
/// missing. Idle heartbeats are what make the failure deterministic rather than
/// join-order luck: they are emitted from the same keepalive branch that
/// credits idle members, so seeing one after the first joiner has committed
/// everything proves the credit happened.
#[tokio::test(flavor = "multi_thread")]
async fn shared_slot_resume_delivers_gap_changes_to_the_second_joiner() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "resume_gap_first", &[(1, "f1")]).await?;
    create_table(&source, "resume_gap_second", &[(1, "s1")]).await?;

    // --- 1. Both datasets join the slot and snapshot, establishing the slot
    // history that makes the next boot a resume rather than a fresh join. ---
    let mut first = start_replication_stream(input_for(port, "resume_gap_first"));
    let boot_first = next_envelope(&mut first, "bootstrap first").await?;
    assert_eq!(num_rows(&boot_first), 1, "bootstrap first rows");
    boot_first.commit().await?;
    wait_for_ready(&mut first, "first readiness catch-up")
        .await?
        .commit()
        .await?;

    let mut second = start_replication_stream(input_for(port, "resume_gap_second"));
    let boot_second = next_envelope(&mut second, "bootstrap second").await?;
    assert_eq!(num_rows(&boot_second), 1, "bootstrap second rows");
    boot_second.commit().await?;
    wait_for_ready(&mut second, "second readiness catch-up")
        .await?
        .commit()
        .await?;

    // --- 2. Every member leaves: the pump exits and releases the slot, which
    // persists (this is a restart, not a teardown). ---
    drop(first);
    drop(second);
    wait_for_walsender_count(&source, 0).await?;
    assert_eq!(slot_count(&source).await?, 1, "the slot must persist");

    // --- 3. Both tables change while the slot has no consumer. ---
    source
        .simple_query("INSERT INTO public.resume_gap_first VALUES (2, 'f2-gap')")
        .await?;
    source
        .simple_query("INSERT INTO public.resume_gap_second VALUES (2, 's2-gap')")
        .await?;
    let gap_lsn: String = source
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);

    // --- 4. The first table rejoins alone, replays its gap row, and is then
    // credited to the WAL head — past the SECOND table's gap row, which no
    // member has consumed. ---
    let mut first_rejoined = start_replication_stream(input_for(port, "resume_gap_first"));
    expect_single_change(&mut first_rejoined, "gap row for the first joiner", "c", 2).await?;
    // Two idle heartbeats after that commit put the slot in exactly the state
    // the hazard needs. `credit_idle` runs on every keepalive and skips a
    // member with an uncommitted envelope, so the heartbeat following our
    // commit proves the first joiner was credited to the server's WAL end —
    // which is past both gap rows. (The first heartbeat may have been queued
    // before the commit landed; the second cannot have been.)
    for round in 1..=2 {
        wait_for_ready(
            &mut first_rejoined,
            &format!("first joiner idle heartbeat {round}"),
        )
        .await?
        .commit()
        .await?;
    }

    // Crediting the first joiner must not carry the SLOT's acknowledgement past
    // a change no member has consumed: below `confirmed_flush_lsn` Postgres is
    // free to recycle the WAL, which is what makes the loss unrecoverable.
    let (acked_past_gap, confirmed_flush) = slot_acked_past(&source, &gap_lsn).await?;
    anyhow::ensure!(
        !acked_past_gap,
        "slot {SLOT} acknowledged up to {confirmed_flush}, past a change (>= {gap_lsn}) owed to a \
         published table with no member — that WAL is now recyclable (#12609)"
    );

    // --- 5. The second table rejoins after its slot-mate has been credited.
    // Its gap row is still owed to it. ---
    let mut second_rejoined = start_replication_stream(input_for(port, "resume_gap_second"));
    let gap = next_change_envelope(&mut second_rejoined, "gap row for the second joiner")
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "a member joining a resuming shared slot second must still receive changes \
                 committed while the slot had no consumer (#12609): {e}"
            )
        })?;
    assert_eq!(
        ops_of(&gap),
        vec!["c".to_string()],
        "second joiner gap row op"
    );
    assert_eq!(
        ids_of(&gap),
        vec![2],
        "the second joiner must receive its gap change as a WAL replay, not as a fresh snapshot"
    );
    gap.commit().await?;

    // --- Cleanup ---
    drop(first_rejoined);
    drop(second_rejoined);
    drop_replication_slot_when_inactive(&source, SLOT).await?;
    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}

/// `drop_slot_after_shutdown` is what stops a non-persistent accelerator's slot
/// from pinning WAL on the source for the whole time Spice is down, so its
/// observable effect -- the slot actually disappearing -- is asserted against a
/// real server rather than inferred from unit-level logic.
///
/// Both halves of the contract are covered:
///
///   * while a walsender still holds the slot, `PostgreSQL` refuses the drop
///     (SQLSTATE `55006`); the call must retry within its budget and then give
///     up *leaving the slot intact*, never erroring or hanging; and
///   * once the stream is gone it must actually drop, tolerating the window in
///     which the server has not yet cleared the walsender.
#[tokio::test(flavor = "multi_thread")]
async fn drop_slot_after_shutdown_releases_an_inactive_slot() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("data_components::postgres_replication=debug,info"));

    let port = common::get_random_port()?;
    let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
    let port = u16::try_from(port).expect("port fits in u16");
    let source = pg_client(port).await?;

    create_table(&source, "drop_slot_tbl", &[(1, "a"), (2, "b")]).await?;

    // Bring the slot into existence the same way production does -- via a live
    // stream -- so the drop runs against a slot with a real walsender history.
    let mut stream = start_replication_stream(input_for(port, "drop_slot_tbl"));
    next_envelope(&mut stream, "bootstrap")
        .await?
        .commit()
        .await?;
    wait_for_ready(&mut stream, "readiness catch-up")
        .await?
        .commit()
        .await?;
    assert_eq!(slot_count(&source).await?, 1, "slot should exist");

    // The slot is held by a live walsender: the drop must fail closed. It
    // retries on 55006 for its budget, then returns without dropping -- and
    // without propagating an error, since shutdown must never block or fail on
    // the source.
    let params = shared_params(port);
    data_components::postgres_replication::slot::drop_slot_after_shutdown(&params).await;
    assert_eq!(
        slot_count(&source).await?,
        1,
        "an actively-held slot must survive the drop attempt"
    );

    // Release the stream and wait for the server to actually clear the
    // walsender before dropping for real. The drop's own 55006 retry covers a
    // brief overlap, but polling the observable condition here keeps the test
    // from depending on the pump tearing down inside that budget under load.
    drop(stream);
    wait_for_walsender_count(&source, 0).await?;
    data_components::postgres_replication::slot::drop_slot_after_shutdown(&params).await;
    assert_eq!(
        slot_count(&source).await?,
        0,
        "slot should be gone once no walsender holds it"
    );

    // Idempotent: a second call finds nothing (SQLSTATE 42704) and is a no-op
    // rather than an error -- shutdown can race with an external cleanup.
    data_components::postgres_replication::slot::drop_slot_after_shutdown(&params).await;
    assert_eq!(slot_count(&source).await?, 0, "second drop is a no-op");

    source
        .simple_query(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"))
        .await?;
    Ok(())
}
