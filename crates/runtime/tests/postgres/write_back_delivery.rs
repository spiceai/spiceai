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
//! Integration tests for what durable write-back *delivers* to a `PostgreSQL`
//! source, driven through a full `Runtime`.
//!
//! These sit beside the echo-suppression suite and deliberately do not repeat
//! it: the question here is not whether an echo is dropped but whether the row
//! the source ends up holding is the row the accelerator committed.
//!
//! Both tests use the same WAL-ordering barrier the echo suite established,
//! because an upsert-keyed CDC apply makes a leaked echo largely idempotent and
//! a time-based assertion would be a guess: once a local write has demonstrably
//! reached the source, an *external* sentinel row is written directly to the
//! source, and the test waits for that sentinel to appear in the accelerator.
//! Logical replication is delivered in commit order, so a visible sentinel
//! proves the pump has already processed every earlier transaction — including
//! the echo of our own delivery. Only then is the table asserted.

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
use tokio_postgres::{Client, NoTls};

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

async fn connect(port: usize) -> Result<Client, anyhow::Error> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("localhost")
        .port(u16::try_from(port)?)
        .user("postgres")
        .password(common::PG_PASSWORD)
        .dbname("postgres");
    let (client, connection) = cfg.connect(NoTls).await?;
    tokio::spawn(async move {
        let _: Result<(), tokio_postgres::Error> = connection.await;
    });
    Ok(client)
}

async fn exec(client: &Client, sql: &str) -> Result<(), anyhow::Error> {
    client
        .simple_query(sql)
        .await
        .map_err(|e| anyhow!("postgres error running `{sql}`: {e}"))?;
    Ok(())
}

/// Terminate the walsender backend serving `slot`, forcing the still-running
/// pump to reconnect and resume from the slot's held `confirmed_flush` — the
/// same resume path a network blip takes, which replays every transaction the
/// ack floor had not yet passed (including an un-acked echo of our own
/// delivery).
async fn force_stream_reconnect(client: &Client, slot: &str) -> Result<(), anyhow::Error> {
    client
        .execute(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = $1 AND active_pid IS NOT NULL",
            &[&slot],
        )
        .await?;
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

/// A single `count(*)` from the source.
async fn source_count(client: &Client, sql: &str) -> Result<i64, anyhow::Error> {
    let row = client
        .query_one(sql, &[])
        .await
        .map_err(|e| anyhow!("postgres error running `{sql}`: {e}"))?;
    Ok(row.try_get::<_, i64>(0)?)
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

// ── UPDATE delivery ─────────────────────────────────────────────────────────

/// An UPDATE committed through Spice reaches the source, and a subsequent
/// write-back INSERT is not double-applied across a forced reconnect.
///
/// The write path for an update is its own code (`update_write_back`) and the
/// delivery worker reconciles it by upserting the row's *current* committed
/// value, so an update exercises a different pair of legs than the insert and
/// delete the echo suite drives. But that same upsert-by-primary-key apply
/// makes a leaked UPDATE echo unobservable: replaying `amount = 99` a second
/// time writes the identical value, so no row count or sum assertion over the
/// update alone can tell "echo dropped" from "echo re-applied". The INSERT of
/// a brand-new key below is the part of this test that actually exercises the
/// double-apply guarantee, because a leaked echo of a new row is a genuine
/// duplicate, not a no-op upsert — and it is driven across a forced walsender
/// reconnect so a replayed (not just a live) echo is covered too.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_back_update_reaches_the_source() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(tracing_filter()));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
            let source = connect(port).await?;
            let slot = "spice_wb_update_slot";
            exec(
                &source,
                "CREATE TABLE public.wb_update (id int PRIMARY KEY, amount int NOT NULL)",
            )
            .await?;
            exec(
                &source,
                "INSERT INTO public.wb_update VALUES (1, 10), (2, 20)",
            )
            .await?;

            let accel = tempfile::tempdir()?;
            let rt = build_runtime(
                "write_back_update",
                vec![write_back_dataset(port, "wb_update", slot, accel.path())],
            )
            .await?;
            wait_for("the bootstrap snapshot", Some(2), || {
                accel_scalar(&rt, "SELECT count(*) FROM wb_update")
            })
            .await?;

            // An UPDATE of a bootstrapped row: committed to the accelerator, then
            // delivered to the source as an upsert of its current value.
            run_query(&rt, "UPDATE wb_update SET amount = 99 WHERE id = 2").await?;
            wait_for("the updated row at the source", Some(99), || {
                source_value(&source, "SELECT amount FROM public.wb_update WHERE id = 2")
            })
            .await?;

            // Force the pump to reconnect and resume from the slot's held
            // confirmed_flush, replaying whatever the ack floor had not yet
            // passed — including a still-un-acked echo of the UPDATE above.
            force_stream_reconnect(&source, slot).await?;

            // A write-back INSERT of a brand-new key: unlike the UPDATE above,
            // a leaked echo of this event is a genuine duplicate row, so it can
            // actually distinguish suppression from re-application.
            run_query(&rt, "INSERT INTO wb_update (id, amount) VALUES (101, 13)").await?;
            wait_for("the inserted row at the source", Some(101), || {
                source_value(&source, "SELECT id FROM public.wb_update WHERE id = 101")
            })
            .await?;

            // Barrier: an external sentinel committed after the delivery. Once it
            // is visible here, the pump has processed past the delivery's echo.
            exec(&source, "INSERT INTO public.wb_update VALUES (100, 7)").await?;
            wait_for("the sentinel", Some(1), || {
                accel_scalar(&rt, "SELECT count(*) FROM wb_update WHERE id = 100")
            })
            .await?;

            assert_accel(
                &rt,
                "SELECT amount FROM wb_update WHERE id = 2",
                99,
                "the accelerator must still hold the value it committed",
            )
            .await?;
            assert_accel(
                &rt,
                "SELECT count(*) FROM wb_update",
                4,
                "a leaked echo of the new-key insert would add a duplicate row",
            )
            .await?;
            assert_accel(
                &rt,
                "SELECT sum(amount) FROM wb_update",
                129,
                "10 + 99 + 13 + 7: a re-applied insert echo would double-count",
            )
            .await?;
            // The source agrees, so nothing was delivered twice or lost.
            let source_sum =
                source_count(&source, "SELECT sum(amount)::int8 FROM public.wb_update").await?;
            assert_eq!(source_sum, 129, "the source holds the same total");

            rt.shutdown().await;
            Ok(())
        })
        .await
}

// ── a source-side trigger inside the delivery transaction ───────────────────

/// A source-side trigger that rewrites the delivered row converges: the
/// accelerator ends up holding the trigger's value, not the one it committed.
///
/// This is worth pinning because the obvious prediction is the opposite. The
/// trigger fires *inside* the delivery transaction, so the rewritten row is
/// written under the delivery's own transaction id — exactly what echo
/// suppression discards — which would leave the accelerator on its own value
/// forever. Measured, that is not what happens: the rewrite reaches the
/// accelerator and the two sides agree on the source's value.
///
/// The assertion is a bounded wait rather than an instantaneous read, so it
/// states the outcome without depending on whether the rewrite arrives as a
/// suppressed-then-refreshed value or an applied one. If suppression ever did
/// keep the rewrite out, this wait would time out and say so.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_trigger_rewrite_reaches_the_accelerator() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(tracing_filter()));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
            let source = connect(port).await?;
            exec(
                &source,
                "CREATE TABLE public.wb_trigger (id int PRIMARY KEY, amount int NOT NULL)",
            )
            .await?;
            // Seeded before the trigger exists, so the bootstrap snapshot is the
            // plain value and only delivered rows are rewritten.
            exec(&source, "INSERT INTO public.wb_trigger VALUES (1, 10)").await?;
            // Doubling makes a rewrite unmistakable rather than a plausible
            // off-by-one.
            exec(
                &source,
                "CREATE FUNCTION double_amount() RETURNS trigger AS $$
                 BEGIN NEW.amount := NEW.amount * 2; RETURN NEW; END;
                 $$ LANGUAGE plpgsql",
            )
            .await?;
            exec(
                &source,
                "CREATE TRIGGER double_amount_trigger BEFORE INSERT OR UPDATE
                 ON public.wb_trigger FOR EACH ROW EXECUTE FUNCTION double_amount()",
            )
            .await?;

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
            wait_for("the bootstrap snapshot", Some(1), || {
                accel_scalar(&rt, "SELECT count(*) FROM wb_trigger")
            })
            .await?;

            // Spice commits 50. The trigger stores 100.
            run_query(&rt, "INSERT INTO wb_trigger (id, amount) VALUES (2, 50)").await?;
            wait_for("the trigger's rewrite at the source", Some(100), || {
                source_value(&source, "SELECT amount FROM public.wb_trigger WHERE id = 2")
            })
            .await?;

            // Barrier: the sentinel is an external transaction, so it is not
            // suppressed. Its own value is doubled by the trigger too, which is
            // why the barrier waits on the row's presence and not its value.
            exec(&source, "INSERT INTO public.wb_trigger VALUES (100, 7)").await?;
            wait_for("the sentinel", Some(1), || {
                accel_scalar(&rt, "SELECT count(*) FROM wb_trigger WHERE id = 100")
            })
            .await?;

            // The trigger's rewrite reaches the accelerator: both sides end up on
            // the source's value, not the one Spice committed.
            wait_for(
                "the trigger's rewrite in the accelerator",
                Some(100),
                || accel_scalar(&rt, "SELECT amount FROM wb_trigger WHERE id = 2"),
            )
            .await?;
            // The sentinel is an external transaction, so its own rewrite is never
            // a suppression candidate — it confirms the stream is live rather than
            // stalled at the row above.
            assert_accel(
                &rt,
                "SELECT amount FROM wb_trigger WHERE id = 100",
                14,
                "an external transaction's rewrite reaches the accelerator",
            )
            .await?;
            assert_eq!(
                source_value(&source, "SELECT amount FROM public.wb_trigger WHERE id = 2").await?,
                Some(100),
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

            wait_for("the bootstrap snapshot", Some(1), || {
                accel_scalar(&rt, "SELECT count(*) FROM cdc_plain")
            })
            .await?;

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

            wait_for("the bootstrap snapshot", Some(1), || {
                accel_scalar(&rt, "SELECT count(*) FROM wb_noslot")
            })
            .await?;

            rt.shutdown().await;
            Ok(())
        })
        .await
}
