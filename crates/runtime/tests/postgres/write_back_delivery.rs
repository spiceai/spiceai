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
//! Integration tests for durable write-back against a `PostgreSQL` source,
//! driven through a full `Runtime`: what a delivery leaves in the source, and
//! what the echo of that delivery must not put back into the accelerator.
//!
//! Which of the two delivery routes a write takes decides which question it can
//! answer. A write made **outside** a Cayenne transaction publishes to the
//! accelerator and is forwarded to the source by the write-back sink itself, in
//! a fire-and-forget background task: no delivery transaction is opened, no
//! transaction id is registered, and the change comes back over CDC like any
//! other source write. A write made **inside** a transaction stages instead,
//! and its commit marks the dirty keys that the delivery worker reconciles
//! through the connector-owned deliverer — which stamps the delivery with its
//! `xid8` and registers it before committing, so the pump can recognize and
//! drop the echo. Echo suppression is therefore reachable only from a write
//! made inside a transaction.
//!
//! Two devices make that outcome readable, defined once and shared by every
//! test: a source-side trigger that rewrites the delivered row so the source's
//! copy differs from the committed one ([`create_bumping_table`]), and a
//! sentinel row that orders the assertions behind the pump
//! ([`wait_for_sentinel`]).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use app::AppBuilder;
use runtime::Runtime;
use secrecy::ExposeSecret;
use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode, WriteMode};
use spicepod::component::dataset::replication::Replication;
use spicepod::component::{access::AccessMode, dataset::Dataset};
use spicepod::param::Params;
use tokio::time::sleep;
use tokio_postgres::Client;

use crate::cayenne::transaction::{describe, run_txn};
use crate::postgres::common;
use crate::utils::{
    register_test_connectors, run_query, runtime_ready_check, test_request_context,
};
use crate::{configure_test_datafusion, init_tracing};

/// How long to wait for a value to propagate in either direction — Spice → the
/// source through the delivery worker (which polls once a second), or the source
/// → Spice through the CDC stream.
const PROPAGATION_TIMEOUT: Duration = Duration::from_mins(1);
/// Poll interval while waiting.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

// ── source helpers ──────────────────────────────────────────────────────────

/// A client on the source, from the port [`common::get_random_port`] handed the
/// container.
async fn connect(port: usize) -> Result<Client, anyhow::Error> {
    common::connect(u16::try_from(port)?).await
}

async fn exec(client: &Client, sql: &str) -> Result<(), anyhow::Error> {
    client
        .simple_query(sql)
        .await
        .map_err(|e| anyhow!("postgres error running `{sql}`: {e}"))?;
    Ok(())
}

/// A single `int4` column from the source, or `None` when no row matched.
async fn source_value(client: &Client, sql: &str) -> Result<Option<i64>, anyhow::Error> {
    let rows = client
        .query(sql, &[])
        .await
        .map_err(|e| anyhow!("postgres error running `{sql}`: {e}"))?;
    match rows.first() {
        None => Ok(None),
        Some(row) => Ok(row.try_get::<_, Option<i32>>(0)?.map(i64::from)),
    }
}

// ── accelerator helpers ─────────────────────────────────────────────────────

/// A single integer scalar from a Spice query, or `None` for an empty result.
async fn accel_scalar(rt: &Arc<Runtime>, sql: &str) -> Result<Option<i64>, anyhow::Error> {
    let batches = run_query(rt, sql).await?;
    let Some(batch) = batches.iter().find(|b| b.num_rows() > 0) else {
        return Ok(None);
    };
    let column = batch.column(0);
    if let Some(ints) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return Ok(Some(ints.value(0)));
    }
    if let Some(ints) = column.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return Ok(Some(i64::from(ints.value(0))));
    }
    Err(anyhow!(
        "query `{sql}` returned an unexpected column type {} (expected an integer)",
        column.data_type()
    ))
}

/// Poll until `probe` yields `expected`, or fail naming the last value seen, so
/// a timeout says which direction of the round trip stalled.
async fn wait_for<F, Fut>(
    what: &str,
    expected: Option<i64>,
    mut probe: F,
) -> Result<(), anyhow::Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<i64>, anyhow::Error>>,
{
    let deadline = Instant::now() + PROPAGATION_TIMEOUT;
    loop {
        let last = probe().await?;
        if last == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out waiting for {what} to become {expected:?}; last saw {last:?}"
            ));
        }
        sleep(POLL_INTERVAL).await;
    }
}

/// Assert a Spice scalar equals `expected` right now, with no waiting — for use
/// after a barrier has already established that the pump is caught up.
async fn assert_accel(
    rt: &Arc<Runtime>,
    sql: &str,
    expected: i64,
    why: &str,
) -> Result<(), anyhow::Error> {
    let actual = accel_scalar(rt, sql).await?;
    if actual != Some(expected) {
        return Err(anyhow!(
            "`{sql}` was {actual:?}, expected {expected}: {why}"
        ));
    }
    Ok(())
}

// ── dataset construction ────────────────────────────────────────────────────

/// A durable-write-back Cayenne dataset over `public.{table}`: writable,
/// replicating, CDC-fed, upserting by a single-column primary key. The
/// accelerator is file-backed because the outstanding-xid registry lives in the
/// accelerator's store, so a memory-only accelerator would not persist it.
fn write_back_dataset(port: usize, table: &str, slot: &str, accel_dir: &Path) -> Dataset {
    cdc_dataset(port, table, Some(slot), accel_dir, WriteMode::WriteBack)
}

/// A `PostgreSQL`-sourced, Cayenne-file, CDC dataset. `write_mode` and the
/// presence of an explicit replication slot are the axes the control tests below
/// vary one at a time, to find which of them a stalled load depends on.
fn cdc_dataset(
    port: usize,
    table: &str,
    slot: Option<&str>,
    accel_dir: &Path,
    write_mode: WriteMode,
) -> Dataset {
    let mut dataset = Dataset::new(format!("postgres:public.{table}"), table.to_string());

    let mut params: HashMap<String, String> = common::get_pg_params(port)
        .into_iter()
        .map(|(k, v)| (k, v.expose_secret().to_string()))
        .collect();
    if let Some(slot) = slot {
        params.insert("pg_replication_slot".to_string(), slot.to_string());
    }
    dataset.params = Some(Params::from_string_map(params));

    let mut accel_params = HashMap::new();
    accel_params.insert(
        "cayenne_file_path".to_string(),
        accel_dir
            .join(format!("{table}_data"))
            .display()
            .to_string(),
    );
    accel_params.insert(
        "cayenne_metadata_dir".to_string(),
        accel_dir
            .join(format!("{table}_meta"))
            .display()
            .to_string(),
    );

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Changes),
        primary_key: Some("id".to_string()),
        on_conflict: [("id".to_string(), OnConflictBehavior::Upsert)]
            .into_iter()
            .collect(),
        write_mode,
        params: Some(Params::from_string_map(accel_params)),
        ..Acceleration::default()
    });
    // Write-back is local-first with asynchronous source durability, so the
    // runtime requires both the writable access mode and the explicit opt-in.
    if write_mode == WriteMode::WriteBack {
        dataset.access = AccessMode::ReadWrite;
        dataset.replication = Some(Replication { enabled: true });
    }

    dataset
}

async fn build_runtime(name: &str, datasets: Vec<Dataset>) -> Result<Arc<Runtime>, anyhow::Error> {
    register_test_connectors().await;
    let mut builder = AppBuilder::new(name);
    for dataset in datasets {
        builder = builder.with_dataset(dataset);
    }
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(builder.build()).build().await);

    // Phase markers go to stderr, not `tracing`: the harness installs its
    // subscriber as a thread-local default, so anything the loader logs from a
    // worker thread is dropped and a stall says only that it elapsed.
    eprintln!("[{name}] runtime built; loading components");
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(2)) => {
            // Deliberately does not read component statuses: that lock is held by
            // the load still in flight, so inspecting it here hangs instead of
            // reporting.
            return Err(anyhow!("timed out waiting for datasets to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }
    eprintln!("[{name}] components loaded; waiting for readiness");
    runtime_ready_check(&rt).await;
    eprintln!("[{name}] ready");
    Ok(rt)
}

fn tracing_filter() -> &'static str {
    "integration=debug,runtime=debug,connector_postgres=debug,\
     data_components::postgres_replication=debug,info"
}

// ── the trigger fixture and the waits the tests share ───────────────────────

/// The offset the source-side trigger stamps onto every `n`, so the row the
/// source stores is never the row Spice committed. Without that difference an
/// echo is unobservable: re-applying a row by primary key over the identical row
/// changes nothing.
const TRIGGER_BUMP: i64 = 1000;

/// The `n` every sentinel row carries. Bound once because a sentinel's `INSERT`
/// and the `TRIGGER_BUMP + n` the accelerator is then waited on for are written
/// at separate call sites: a mismatch between them does not fail an assertion,
/// it burns the whole [`PROPAGATION_TIMEOUT`] and reports as a stall.
const SENTINEL_N: i64 = 7;

/// Create `public.{table}` holding `(1, 10)`, then a `BEFORE INSERT OR UPDATE`
/// trigger that raises `n` by [`TRIGGER_BUMP`] once.
///
/// Guarded on `n < TRIGGER_BUMP` so the rewrite is idempotent, which it has to
/// be for the delivered value to be a stable oracle. The deliverer's upsert leg
/// issues `INSERT ... ON CONFLICT DO UPDATE`, which fires the `BEFORE INSERT`
/// trigger on the proposed row and then, when the row already exists, the
/// `BEFORE UPDATE` trigger on the result. An unguarded `n := n + TRIGGER_BUMP`
/// would stamp `n + 2 * TRIGGER_BUMP` on every delivery of an existing key, and
/// the worker replays a whole pass on any error, so a retried delivery would
/// leave the source on a value no wait here expects.
///
/// A trigger sees nothing of the deliverer's other leg: `deliver_deletes` issues
/// a `DELETE`, which this fixture cannot observe and no test here reaches (the
/// leg runs only for a key that is dirty and absent from the accelerator).
///
/// The seed row is written before the trigger exists, so the bootstrap snapshot
/// carries the plain value and only rows written afterwards are rewritten.
async fn create_bumping_table(source: &Client, table: &str) -> Result<(), anyhow::Error> {
    exec(
        source,
        &format!("CREATE TABLE public.{table} (id int PRIMARY KEY, n int NOT NULL)"),
    )
    .await?;
    exec(
        source,
        &format!("INSERT INTO public.{table} VALUES (1, 10)"),
    )
    .await?;
    exec(
        source,
        &format!(
            "CREATE FUNCTION bump_{table}() RETURNS trigger AS $$
             BEGIN
               IF NEW.n < {TRIGGER_BUMP} THEN NEW.n := NEW.n + {TRIGGER_BUMP}; END IF;
               RETURN NEW;
             END;
             $$ LANGUAGE plpgsql"
        ),
    )
    .await?;
    exec(
        source,
        &format!(
            "CREATE TRIGGER bump_{table}_trigger BEFORE INSERT OR UPDATE
             ON public.{table} FOR EACH ROW EXECUTE FUNCTION bump_{table}()"
        ),
    )
    .await?;
    Ok(())
}

/// Wait for the dataset's bootstrap snapshot — the one seed row.
async fn wait_for_bootstrap(rt: &Arc<Runtime>, table: &str) -> Result<(), anyhow::Error> {
    let sql = format!("SELECT count(*) FROM {table}");
    wait_for("the bootstrap snapshot", Some(1), || accel_scalar(rt, &sql)).await
}

/// Wait for the source to hold the trigger's rewrite of `n` at `id`, which is
/// what says a delivery of that row reached the source and committed there.
async fn wait_for_delivery(
    source: &Client,
    table: &str,
    id: i64,
    n: i64,
) -> Result<(), anyhow::Error> {
    let sql = format!("SELECT n FROM public.{table} WHERE id = {id}");
    wait_for(
        "the delivered row at the source",
        Some(TRIGGER_BUMP + n),
        || source_value(source, &sql),
    )
    .await
}

/// Wait for a sentinel row to reach the accelerator, carrying the trigger's
/// rewrite of [`SENTINEL_N`].
///
/// This is the barrier the echo assertions depend on, and the foreign-writer
/// control at the same time. The sentinel is written directly to the source
/// after a delivery has demonstrably committed there, so it commits later in the
/// WAL; logical replication is delivered in commit order, so the sentinel
/// becoming visible proves the pump has already processed the delivery's echo.
/// Its own value must survive, since Spice did not issue it.
async fn wait_for_sentinel(
    rt: &Arc<Runtime>,
    table: &str,
    sentinel_id: i64,
) -> Result<(), anyhow::Error> {
    let sql = format!("SELECT n FROM {table} WHERE id = {sentinel_id}");
    wait_for(
        "the sentinel in the accelerator",
        Some(TRIGGER_BUMP + SENTINEL_N),
        || accel_scalar(rt, &sql),
    )
    .await
}

// ── a source-side trigger outside a transaction ─────────────────────────────

/// A write made outside a transaction converges on the source's value: the
/// accelerator ends up holding the trigger's rewrite, not the row it committed.
///
/// The write is an ordinary `INSERT`, so it takes the sink's fire-and-forget
/// forward and no transaction id is registered for it (see the module docs).
/// Its rewrite therefore comes back as an unremarkable source change and is
/// applied — the opposite outcome to a transaction's delivery, and the reason
/// the two are worth pinning side by side.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_trigger_rewrite_reaches_the_accelerator() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(tracing_filter()));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
            let source = connect(port).await?;
            create_bumping_table(&source, "wb_trigger").await?;

            let accel = tempfile::tempdir()?;
            let rt = build_runtime(
                "write_back_source_trigger",
                vec![write_back_dataset(
                    port,
                    "wb_trigger",
                    "spice_wb_trigger_slot",
                    accel.path(),
                )],
            )
            .await?;
            wait_for_bootstrap(&rt, "wb_trigger").await?;

            // Spice commits 50 outside a transaction; the trigger rewrites it.
            run_query(&rt, "INSERT INTO wb_trigger (id, n) VALUES (2, 50)").await?;
            wait_for_delivery(&source, "wb_trigger", 2, 50).await?;

            exec(
                &source,
                &format!("INSERT INTO public.wb_trigger VALUES (9, {SENTINEL_N})"),
            )
            .await?;
            wait_for_sentinel(&rt, "wb_trigger", 9).await?;

            assert_accel(
                &rt,
                "SELECT n FROM wb_trigger WHERE id = 2",
                TRIGGER_BUMP + 50,
                "an unregistered delivery's rewrite reaches the accelerator",
            )
            .await?;
            assert_eq!(
                source_value(&source, "SELECT n FROM public.wb_trigger WHERE id = 2").await?,
                Some(TRIGGER_BUMP + 50),
                "the source holds the trigger's value"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}

// ── controls: which axis does a stalled load depend on? ─────────────────────

/// Control: the same Cayenne-file CDC dataset as the tests above, but a plain
/// replicated reader — no write-back, no writable access, no replication opt-in.
///
/// A declared `refresh_mode: changes` *dataset* has no other integration
/// coverage in a `--features postgres` build (`replication_tpch` is gated on
/// `duckdb`), so this is the baseline the write-back tests are read against: it
/// separates "this dataset shape does not come up" from "write-back does not
/// come up". Kept as a permanent test, not scaffolding — the baseline is worth
/// having on its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_cdc_dataset_bootstraps_and_follows_the_source() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(tracing_filter()));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
            let source = connect(port).await?;
            exec(
                &source,
                "CREATE TABLE public.cdc_plain (id int PRIMARY KEY, amount int NOT NULL)",
            )
            .await?;
            exec(&source, "INSERT INTO public.cdc_plain VALUES (1, 10)").await?;

            let accel = tempfile::tempdir()?;
            let rt = build_runtime(
                "cdc_plain",
                vec![cdc_dataset(
                    port,
                    "cdc_plain",
                    Some("spice_cdc_plain_slot"),
                    accel.path(),
                    WriteMode::WriteThrough,
                )],
            )
            .await?;

            wait_for_bootstrap(&rt, "cdc_plain").await?;

            // A source-side write reaches the accelerator over CDC.
            exec(&source, "INSERT INTO public.cdc_plain VALUES (2, 20)").await?;
            wait_for("the source insert", Some(20), || {
                accel_scalar(&rt, "SELECT amount FROM cdc_plain WHERE id = 2")
            })
            .await?;

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// Control: write-back, but with no explicit `pg_replication_slot` — the slot is
/// named per dataset instead of shared. Isolates the slot parameter from the
/// write-back path, since the tests above set both.
#[tokio::test(flavor = "multi_thread")]
async fn write_back_bootstraps_without_an_explicit_slot() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(tracing_filter()));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
            let source = connect(port).await?;
            exec(
                &source,
                "CREATE TABLE public.wb_noslot (id int PRIMARY KEY, amount int NOT NULL)",
            )
            .await?;
            exec(&source, "INSERT INTO public.wb_noslot VALUES (1, 10)").await?;

            let accel = tempfile::tempdir()?;
            let rt = build_runtime(
                "wb_noslot",
                vec![cdc_dataset(
                    port,
                    "wb_noslot",
                    None,
                    accel.path(),
                    WriteMode::WriteBack,
                )],
            )
            .await?;

            wait_for_bootstrap(&rt, "wb_noslot").await?;

            rt.shutdown().await;
            Ok(())
        })
        .await
}

// ── echo suppression: a transaction's delivery is not re-applied ────────────

/// Commit `(id, n)` into `table` inside a Cayenne transaction.
///
/// This is the write shape whose delivery runs through the connector-owned
/// deliverer: the write stages, the commit marks its dirty key, and the delivery
/// worker reconciles that key in a source transaction whose `xid8` it registers
/// before committing. A write outside `BEGIN`…`COMMIT` never marks a key and is
/// forwarded by the sink instead, registering nothing.
async fn commit_in_transaction(
    rt: &Arc<Runtime>,
    table: &str,
    id: i64,
    n: i64,
) -> Result<(), anyhow::Error> {
    let sql = format!("BEGIN; INSERT INTO {table} (id, n) VALUES ({id}, {n}); COMMIT;");
    run_txn(rt, &sql)
        .await
        .map_err(|e| anyhow!("`{sql}` failed: {}", describe(&e)))?;
    Ok(())
}

/// The echo of a transaction's own write-back delivery is dropped, while a
/// transaction Spice did not issue is applied.
///
/// The pump's filter and the registry's own lifecycle are unit-tested against a
/// synthetic stream (`postgres_replication::shared` and
/// `postgres_replication::xid_registry`), each side against ids of its own
/// choosing. What only a real source can exercise is that the two agree: the id
/// the deliverer reads from a live `PostgreSQL` and registers is the id that
/// server then reports for the same transaction in pgoutput, decoded through
/// the real connector, worker, pool, and slot.
///
/// Two things it deliberately does not establish, because a live source cannot
/// be made to show them. It cannot see the *ordering* of the registration
/// against the delivery's `COMMIT`: replication lags a commit by milliseconds,
/// so registering just after `COMMIT` would still beat the echo and pass here —
/// that ordering is an argued invariant of the deliverer, not a measured one.
/// And a container's transaction ids sit far below 2^32, so the epoch is zero
/// and the registry's low-32 projection is the identity; a projection bug shows
/// up only against a source that has wrapped, which
/// `xid_registry::contains_matches_low_32_bits` covers directly.
///
/// The trigger is what makes the outcome readable. Spice commits 50, the source
/// stores 1050, and the accelerator's value afterwards is the answer: 50 means
/// the echo was recognized and dropped, 1050 means it was re-applied over the
/// row the accelerator had committed.
///
/// Both branches of the deliverer's upsert are covered, because they reach the
/// source as different statements under different transaction ids: a key the
/// source does not hold takes the insert branch, and the second commit of the
/// same key takes `ON CONFLICT DO UPDATE`, whose echo is an UPDATE.
#[tokio::test(flavor = "multi_thread")]
async fn a_transactions_echo_is_dropped_while_a_foreign_write_lands() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(tracing_filter()));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
            let source = connect(port).await?;
            create_bumping_table(&source, "wb_echo").await?;

            let accel = tempfile::tempdir()?;
            let rt = build_runtime(
                "write_back_echo",
                vec![write_back_dataset(
                    port,
                    "wb_echo",
                    "spice_wb_echo_slot",
                    accel.path(),
                )],
            )
            .await?;
            wait_for_bootstrap(&rt, "wb_echo").await?;

            commit_in_transaction(&rt, "wb_echo", 2, 50).await?;
            wait_for_delivery(&source, "wb_echo", 2, 50).await?;

            exec(
                &source,
                &format!("INSERT INTO public.wb_echo VALUES (9, {SENTINEL_N})"),
            )
            .await?;
            wait_for_sentinel(&rt, "wb_echo", 9).await?;

            assert_accel(
                &rt,
                "SELECT n FROM wb_echo WHERE id = 2",
                50,
                "the accelerator must still hold the row it committed",
            )
            .await?;
            assert_accel(
                &rt,
                "SELECT count(*) FROM wb_echo",
                3,
                "dropping an echo must not drop a row",
            )
            .await?;
            assert_accel(
                &rt,
                "SELECT n FROM wb_echo WHERE id = 1",
                10,
                "a row no delivery touched must be left alone",
            )
            .await?;
            assert_eq!(
                source_value(&source, "SELECT n FROM public.wb_echo WHERE id = 2").await?,
                Some(TRIGGER_BUMP + 50),
                "the source must keep its own value: an echo is dropped, not rolled back"
            );

            // The same key again, so this delivery takes the upsert's conflict
            // branch instead of its insert branch: a different statement outcome
            // at the source, a different `xid8`, and an echo carrying an UPDATE
            // rather than an INSERT.
            commit_in_transaction(&rt, "wb_echo", 2, 60).await?;
            wait_for_delivery(&source, "wb_echo", 2, 60).await?;
            exec(
                &source,
                &format!("INSERT INTO public.wb_echo VALUES (11, {SENTINEL_N})"),
            )
            .await?;
            wait_for_sentinel(&rt, "wb_echo", 11).await?;
            assert_accel(
                &rt,
                "SELECT n FROM wb_echo WHERE id = 2",
                60,
                "the accelerator must still hold the row it committed over an existing key",
            )
            .await?;

            rt.shutdown().await;
            Ok(())
        })
        .await
}
