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
//! End-to-end integration tests for CDC echo suppression on `PostgreSQL` durable
//! write-back (#13348, implementing `cdc-echo-drop-xid-design.md`).
//!
//! These drive the **full Spice Runtime** — a `PostgreSQL` source with
//! `refresh_mode: changes`, a persistent Cayenne file accelerator, and
//! `write_mode: write_back` — so a local write is:
//!   1. committed to the accelerator, then
//!   2. delivered to the source by the connector-owned deliverer, which stamps
//!      the delivery transaction's id (`xid8`) and registers it, and
//!   3. echoed back over logical replication, where the CDC pump must drop the
//!      arbitrated table's changes for that transaction before they become Arrow.
//!
//! Because the accelerator applies UPDATE/INSERT as an upsert keyed on the
//! primary key, re-applying an echo of a write we already applied is largely
//! idempotent for the base rows — so these tests use a **WAL-ordering barrier**
//! to make the assertion deterministic rather than time-based: after a local
//! write has demonstrably reached the source, an *external* sentinel row is
//! written directly to the source, and the test waits until that sentinel is
//! visible in the accelerator. Because logical replication is delivered in
//! commit order, the sentinel becoming visible proves the pump has already
//! processed (and, for our own writes, dropped) every earlier transaction —
//! including the echo. Only then is the arbitrated table asserted to be correct.
//!
//! Scenarios implemented here (each maps to an acceptance criterion in the
//! design's PR 5):
//!   1. `echo_drop_end_to_end` — a local write's echo is not re-applied.
//!   2. `echo_still_dropped_after_restart` — the registry survives a restart.
//!   3. `echo_dropped_again_after_reconnect` — a forced walsender reconnect (WAL
//!      replay from the held `confirmed_flush`) does not re-admit the echo.
//!   4. `cascade_survives_per_relation_echo_drop` — a write-back DELETE with
//!      `ON DELETE CASCADE` into a second replicated table on the SAME slot
//!      drops only the arbitrated relation's echo; the child's cascade deletes
//!      still apply (the per-relation, not whole-transaction, requirement).
//!   6. `external_write_applies_normally` — a direct external write (a different,
//!      unregistered xid) applies to the accelerator unfiltered.
//!
//! Scenario 5 (fence ordering under memory durability) and scenario 7 (GC of
//! aborted / lost-unregister entries) are covered by the deterministic
//! lower-level tests the plan places them at — the pump's fake-stream unit tests
//! in `postgres_replication::shared` and the `XidRegistry` unit tests in
//! `postgres_replication::echo` — because they require control over the mem-tier
//! durability fence and injection of aborted / lost-unregister registry entries
//! that the black-box full-runtime harness does not expose. Scenarios 2 and 3
//! here likewise cannot deterministically pin the "un-acked at
//! restart/reconnect" precondition through the black box, so they assert the
//! invariant that holds either way (the write applies exactly once) and lean on
//! the pump unit tests for the precise ordering; see the PR description.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use app::AppBuilder;
use arrow::array::{Array, Int32Array, Int64Array};
use runtime::Runtime;
use runtime::config::Config;
use secrecy::ExposeSecret;
use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode, WriteMode};
use spicepod::component::access::AccessMode;
use spicepod::component::dataset::Dataset;
use spicepod::component::dataset::replication::Replication;
use spicepod::param::Params;
use tokio_postgres::{Client, NoTls};

use crate::postgres::common;
use crate::utils::{
    register_test_connectors, run_query, runtime_ready_check, test_request_context,
};
use crate::{configure_test_datafusion, init_tracing};

/// Bound on how long a change may take to propagate end-to-end (local write →
/// source delivery → echo → accelerator) before the test fails. Generous: it
/// covers the worker's idle poll plus a full replication round trip.
const PROPAGATION_TIMEOUT: Duration = Duration::from_secs(60);
/// Interval between polls of an eventually-consistent condition. Not a readiness
/// sleep — every wait polls the actual condition and reports the last observed
/// state on timeout.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

// ---------------------------------------------------------------------------
// Source (PostgreSQL) helpers
// ---------------------------------------------------------------------------

async fn connect(port: u16) -> Result<Client, anyhow::Error> {
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

async fn exec(client: &Client, sql: &str) -> Result<(), anyhow::Error> {
    client
        .simple_query(sql)
        .await
        .map_err(|e| anyhow!("postgres error running `{sql}`: {e}"))?;
    Ok(())
}

/// `SELECT count(*)` (a `bigint`) from the source, for a `WHERE`-scoped predicate.
async fn source_count(client: &Client, sql: &str) -> Result<i64, anyhow::Error> {
    let row = client
        .query_one(sql, &[])
        .await
        .map_err(|e| anyhow!("postgres error running `{sql}`: {e}"))?;
    Ok(row.get::<_, i64>(0))
}

/// Poll the source until `count_sql` returns `expected`, proving the write-back
/// worker has delivered (or a cascade has fired) on the source side. This
/// establishes the ordering the barrier relies on: the sentinel written after
/// this returns is guaranteed to be WAL-ordered after the delivery's echo.
async fn wait_source_count(
    client: &Client,
    count_sql: &str,
    expected: i64,
) -> Result<(), anyhow::Error> {
    let deadline = Instant::now() + PROPAGATION_TIMEOUT;
    let mut last = i64::MIN;
    loop {
        last = source_count(client, count_sql).await.unwrap_or(last);
        if last == expected {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out waiting for source `{count_sql}` to reach {expected}; last saw {last}"
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

// ---------------------------------------------------------------------------
// Accelerator (Spice runtime) helpers
// ---------------------------------------------------------------------------

/// Read the first column of the first non-empty batch as `i64`, accepting either
/// an `Int32` (a raw column) or `Int64` (a `count`/`sum` aggregate) result.
/// `None` when the query returned no rows.
async fn accel_scalar_i64(rt: &Arc<Runtime>, sql: &str) -> Result<Option<i64>, anyhow::Error> {
    let batches = run_query(rt, sql).await?;
    let Some(batch) = batches.iter().find(|b| b.num_rows() > 0) else {
        return Ok(None);
    };
    let column = batch.column(0);
    if let Some(a) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(a.value(0)));
    }
    if let Some(a) = column.as_any().downcast_ref::<Int32Array>() {
        return Ok(Some(i64::from(a.value(0))));
    }
    Err(anyhow!(
        "query `{sql}` returned an unexpected column type {} (want Int32 or Int64)",
        column.data_type()
    ))
}

/// Poll the accelerator until `sql` yields `expected` (as `i64`). Used both to
/// wait for the WAL-ordering barrier sentinel to appear and to assert a stable
/// end state has been reached.
async fn wait_accel_scalar(
    rt: &Arc<Runtime>,
    sql: &str,
    expected: i64,
) -> Result<(), anyhow::Error> {
    let deadline = Instant::now() + PROPAGATION_TIMEOUT;
    let mut last: Option<i64> = None;
    loop {
        last = accel_scalar_i64(rt, sql).await?;
        if last == Some(expected) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out waiting for accelerator `{sql}` to reach {expected}; last saw {last:?}"
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Assert `sql` currently yields exactly `expected`. Unlike [`wait_accel_scalar`]
/// this does not poll — call it only after a barrier has proven the pump is
/// caught up, so a wrong value is a real echo-suppression failure, not lag.
async fn assert_accel_scalar(
    rt: &Arc<Runtime>,
    sql: &str,
    expected: i64,
) -> Result<(), anyhow::Error> {
    let actual = accel_scalar_i64(rt, sql).await?;
    if actual != Some(expected) {
        return Err(anyhow!("accelerator `{sql}` = {actual:?}, want {expected}"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Dataset + runtime construction
// ---------------------------------------------------------------------------

/// Connector params for a `PostgreSQL` dataset pointed at the test container,
/// optionally pinned to an explicit (shared) replication slot so the test can
/// name the slot for restart / cascade wiring.
fn pg_params(port: usize, slot: Option<&str>) -> HashMap<String, String> {
    let mut params: HashMap<String, String> = common::get_pg_params(port)
        .into_iter()
        .map(|(k, v)| (k, v.expose_secret().to_string()))
        .collect();
    if let Some(slot) = slot {
        // An explicit `pg_replication_slot` makes the slot shareable, so two
        // datasets naming the same slot are multiplexed onto one pump (required
        // by the per-relation cascade test) and gives every test a known slot
        // name for restart / teardown.
        params.insert("pg_replication_slot".to_string(), slot.to_string());
    }
    params
}

/// Cayenne file-accelerator params rooted under `dir`. File mode is persistent —
/// required for durable write-back (the xid registry and applied-LSN watermark
/// live in the accelerator's own blob store) and for the restart test.
fn cayenne_params(dir: &std::path::Path) -> HashMap<String, String> {
    let mut params = HashMap::new();
    params.insert(
        "cayenne_file_path".to_string(),
        dir.join("data").display().to_string(),
    );
    params.insert(
        "cayenne_metadata_dir".to_string(),
        dir.join("meta").display().to_string(),
    );
    params
}

/// A `PostgreSQL`-sourced, Cayenne-file, CDC (`refresh_mode: changes`) dataset.
///
/// `write_back` toggles the durable federated write-back path: `true` gives the
/// full local-write → deliver → echo loop this suite exercises; `false` is a
/// plain replicated reader (used for the cascade test's child table).
fn cdc_dataset(
    port: usize,
    pg_table: &str,
    name: &str,
    pk: &str,
    dir: &std::path::Path,
    slot: Option<&str>,
    write_back: bool,
) -> Dataset {
    let mut dataset = Dataset::new(format!("postgres:{pg_table}"), name.to_string());
    dataset.params = Some(Params::from_string_map(pg_params(port, slot)));

    let on_conflict: HashMap<String, OnConflictBehavior> =
        std::iter::once((pk.to_string(), OnConflictBehavior::Upsert)).collect();

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Changes),
        primary_key: Some(pk.to_string()),
        on_conflict,
        params: Some(Params::from_string_map(cayenne_params(dir))),
        write_mode: if write_back {
            WriteMode::WriteBack
        } else {
            WriteMode::WriteThrough
        },
        ..Acceleration::default()
    });

    if write_back {
        // Local-first write-back is asynchronous source durability, so it
        // requires the explicit `replication.enabled` opt-in (and `ReadWrite`
        // so the runtime accepts local DML).
        dataset.access = AccessMode::ReadWrite;
        dataset.replication = Some(Replication { enabled: true });
    }
    dataset
}

/// Build a runtime for the given datasets and wait until it is ready. Reused
/// across a restart, so a second call with the same dataset configs resumes the
/// same accelerator files and replication slot.
async fn build_runtime(
    app_name: &str,
    datasets: Vec<Dataset>,
) -> Result<Arc<Runtime>, anyhow::Error> {
    register_test_connectors().await;
    let mut builder = AppBuilder::new(app_name);
    for ds in datasets {
        builder = builder.with_dataset(ds);
    }
    let app = builder.build();

    configure_test_datafusion();
    // Caching disabled so a single-shot assertion read (after the WAL-ordering
    // barrier) always observes the newest committed accelerator state rather
    // than a stale results-cache entry.
    let rt = Arc::new(
        Runtime::builder()
            .with_app(app)
            .with_runtime_config(Config::default().with_caching_disabled())
            .build()
            .await,
    );

    tokio::select! {
        () = tokio::time::sleep(Duration::from_secs(90)) => {
            return Err(anyhow!("timed out waiting for datasets to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }
    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// After a graceful shutdown, wait until no walsender holds `slot`, so a restart
/// can resume it without racing the old connection's teardown.
async fn wait_slot_inactive(client: &Client, slot: &str) -> Result<(), anyhow::Error> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let active: i64 = client
            .query_one(
                "SELECT count(*) FROM pg_replication_slots \
                 WHERE slot_name = $1 AND active_pid IS NOT NULL",
                &[&slot],
            )
            .await?
            .get(0);
        if active == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "slot {slot} still has an active walsender after shutdown"
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Terminate the walsender backend serving `slot`, forcing the still-running
/// pump to reconnect and resume from the slot's held `confirmed_flush` — the
/// same resume path a network blip takes, which replays every transaction the
/// ack floor had not yet passed.
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

// ---------------------------------------------------------------------------
// Scenario 1 — echo drop end-to-end
// ---------------------------------------------------------------------------

/// A local write is delivered to the source and echoed back over CDC; the echo
/// must NOT be re-applied. Uses the WAL-ordering barrier (an external sentinel
/// row) to make the assertion deterministic, then asserts the arbitrated table
/// carries the write exactly once — no duplicate row and no double-counted
/// `sum`, either of which would be the signature of a leaked echo.
#[tokio::test(flavor = "multi_thread")]
async fn echo_drop_end_to_end() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,connector_postgres=debug,\
         data_components::postgres_replication=debug,info",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
            let source = connect(u16::try_from(port)?).await?;

            exec(
                &source,
                "CREATE TABLE public.echo_orders (id int PRIMARY KEY, amount int NOT NULL)",
            )
            .await?;
            exec(
                &source,
                "INSERT INTO public.echo_orders VALUES (1, 10), (2, 20)",
            )
            .await?;

            let temp = tempfile::tempdir()?;
            let ds = cdc_dataset(
                port,
                "public.echo_orders",
                "echo_orders",
                "id",
                temp.path(),
                Some("spice_echo_e2e_slot"),
                true,
            );
            let rt = build_runtime("echo_e2e", vec![ds]).await?;

            // Bootstrap snapshot: the two seed rows.
            wait_accel_scalar(&rt, "SELECT count(*) FROM echo_orders", 2).await?;

            // Local write-back INSERT of a brand-new key. It lands in the
            // accelerator first, then the worker delivers it to the source,
            // which echoes it back over CDC.
            run_query(&rt, "INSERT INTO echo_orders (id, amount) VALUES (3, 30)").await?;

            // The delivery reached the source (worker ran, xid registered).
            wait_source_count(
                &source,
                "SELECT count(*) FROM public.echo_orders WHERE id = 3",
                1,
            )
            .await?;

            // Barrier: an external sentinel written AFTER the delivery. When it
            // is visible in the accelerator, the pump has processed past the
            // echo of id=3 (WAL commit order), so the echo has had its chance to
            // leak.
            exec(&source, "INSERT INTO public.echo_orders VALUES (100, 999)").await?;
            wait_accel_scalar(&rt, "SELECT count(*) FROM echo_orders WHERE id = 100", 1).await?;

            // The echo was dropped: the write applied exactly once.
            assert_accel_scalar(&rt, "SELECT count(*) FROM echo_orders", 4).await?;
            assert_accel_scalar(&rt, "SELECT count(*) FROM echo_orders WHERE id = 3", 1).await?;
            assert_accel_scalar(&rt, "SELECT amount FROM echo_orders WHERE id = 3", 30).await?;
            // A leaked echo re-applied as an append would double id=3's amount:
            // 10 + 20 + 30 + 999 = 1059, not 1089.
            assert_accel_scalar(&rt, "SELECT sum(amount) FROM echo_orders", 1059).await?;

            rt.shutdown().await;
            Ok(())
        })
        .await
}

// ---------------------------------------------------------------------------
// Scenario 2 — restart persistence
// ---------------------------------------------------------------------------

/// The outstanding-xid registry is persisted in the accelerator, so a restart
/// resumes the suppression set. Deliver a local write, shut down (the echo may
/// still be un-consumed by the slot), restart against the same accelerator files
/// and slot, and assert the echo is still dropped — the write is present exactly
/// once with no double-count after the restarted pump catches up.
#[tokio::test(flavor = "multi_thread")]
async fn echo_still_dropped_after_restart() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,connector_postgres=debug,\
         data_components::postgres_replication=debug,info",
    ));

    test_request_context()
        .scope(async {
            let slot = "spice_echo_restart_slot";
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
            let source = connect(u16::try_from(port)?).await?;

            exec(
                &source,
                "CREATE TABLE public.echo_restart (id int PRIMARY KEY, amount int NOT NULL)",
            )
            .await?;
            exec(
                &source,
                "INSERT INTO public.echo_restart VALUES (1, 10), (2, 20)",
            )
            .await?;

            // A stable temp dir shared across both runtime lifetimes so the
            // accelerator (and its registry) persist across the restart.
            let temp = tempfile::tempdir()?;
            let build_ds = || {
                cdc_dataset(
                    port,
                    "public.echo_restart",
                    "echo_restart",
                    "id",
                    temp.path(),
                    Some(slot),
                    true,
                )
            };

            let rt = build_runtime("echo_restart", vec![build_ds()]).await?;
            wait_accel_scalar(&rt, "SELECT count(*) FROM echo_restart", 2).await?;

            // Local write-back write; confirm it reached the source, then shut
            // down promptly so the echo is as likely as possible to be
            // un-consumed by the slot at restart time (the case persistence
            // exists to cover).
            run_query(&rt, "INSERT INTO echo_restart (id, amount) VALUES (3, 30)").await?;
            wait_source_count(
                &source,
                "SELECT count(*) FROM public.echo_restart WHERE id = 3",
                1,
            )
            .await?;
            rt.shutdown().await;
            drop(rt); // release the accelerator's file/metastore handles before restart
            wait_slot_inactive(&source, slot).await?;

            // Restart against the same accelerator files and slot: the registry
            // is reloaded and GC runs, then the pump resumes from the held
            // confirmed_flush and replays any un-acked echo.
            let rt = build_runtime("echo_restart", vec![build_ds()]).await?;

            // Barrier after the restart: an external sentinel proves the resumed
            // pump has processed past the (possibly replayed) echo.
            exec(&source, "INSERT INTO public.echo_restart VALUES (100, 999)").await?;
            wait_accel_scalar(&rt, "SELECT count(*) FROM echo_restart WHERE id = 100", 1).await?;

            assert_accel_scalar(&rt, "SELECT count(*) FROM echo_restart", 4).await?;
            assert_accel_scalar(&rt, "SELECT count(*) FROM echo_restart WHERE id = 3", 1).await?;
            assert_accel_scalar(&rt, "SELECT amount FROM echo_restart WHERE id = 3", 30).await?;
            assert_accel_scalar(&rt, "SELECT sum(amount) FROM echo_restart", 1059).await?;

            rt.shutdown().await;
            Ok(())
        })
        .await
}

// ---------------------------------------------------------------------------
// Scenario 4 — cascade survives a per-relation echo drop
// ---------------------------------------------------------------------------

/// The drop is per-relation, not per-transaction. A write-back DELETE on the
/// arbitrated parent cascades (`ON DELETE CASCADE`) into a second replicated
/// child table in the SAME source transaction. Both tables are members of one
/// shared replication slot, so the shared pump sees both relations under the
/// registered xid. It must drop only the parent's (echo) changes and still apply
/// the child's cascade deletes — a whole-transaction drop would silently strand
/// the child's rows forever.
#[tokio::test(flavor = "multi_thread")]
async fn cascade_survives_per_relation_echo_drop() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,connector_postgres=debug,\
         data_components::postgres_replication=debug,info",
    ));

    test_request_context()
        .scope(async {
            let slot = "spice_echo_cascade_slot";
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
            let source = connect(u16::try_from(port)?).await?;

            // Parent (the write-back target) + child with ON DELETE CASCADE.
            exec(
                &source,
                "CREATE TABLE public.echo_parent (id int PRIMARY KEY, label text)",
            )
            .await?;
            exec(
                &source,
                "CREATE TABLE public.echo_child (\
                     id int PRIMARY KEY, \
                     parent_id int NOT NULL REFERENCES public.echo_parent(id) ON DELETE CASCADE, \
                     note text)",
            )
            .await?;
            exec(
                &source,
                "INSERT INTO public.echo_parent VALUES (1, 'p1'), (2, 'p2')",
            )
            .await?;
            exec(
                &source,
                "INSERT INTO public.echo_child VALUES \
                 (10, 1, 'c10'), (11, 1, 'c11'), (20, 2, 'c20')",
            )
            .await?;

            let temp = tempfile::tempdir()?;
            // Both datasets on ONE shared slot so the pump multiplexes them and
            // the per-relation drop is exercised. The parent is write-back (the
            // arbitrated relation whose echo is registered/dropped); the child
            // is a plain replicated reader that must APPLY the cascade.
            let parent = cdc_dataset(
                port,
                "public.echo_parent",
                "echo_parent",
                "id",
                &temp.path().join("parent"),
                Some(slot),
                true,
            );
            let child = cdc_dataset(
                port,
                "public.echo_child",
                "echo_child",
                "id",
                &temp.path().join("child"),
                Some(slot),
                false,
            );
            let rt = build_runtime("echo_cascade", vec![parent, child]).await?;

            wait_accel_scalar(&rt, "SELECT count(*) FROM echo_parent", 2).await?;
            wait_accel_scalar(&rt, "SELECT count(*) FROM echo_child", 3).await?;

            // Local write-back DELETE of parent id=1. The worker's delete leg
            // deletes it on the source, whose ON DELETE CASCADE removes child
            // rows 10 and 11 in the same (registered) transaction.
            run_query(&rt, "DELETE FROM echo_parent WHERE id = 1").await?;

            // The cascade has fired on the source (child rows gone), which
            // establishes the ordering for the barrier below.
            wait_source_count(
                &source,
                "SELECT count(*) FROM public.echo_child WHERE parent_id = 1",
                0,
            )
            .await?;

            // Barrier: an external child insert AFTER the cascade. Its
            // visibility proves the pump processed the cascade transaction.
            exec(
                &source,
                "INSERT INTO public.echo_child VALUES (99, 2, 'sentinel')",
            )
            .await?;
            wait_accel_scalar(&rt, "SELECT count(*) FROM echo_child WHERE id = 99", 1).await?;

            // Parent echo dropped: parent id=1 gone, id=2 intact.
            assert_accel_scalar(&rt, "SELECT count(*) FROM echo_parent WHERE id = 1", 0).await?;
            assert_accel_scalar(&rt, "SELECT count(*) FROM echo_parent", 1).await?;

            // Child cascade APPLIED (the per-relation requirement): rows 10 and
            // 11 are gone, row 20 survives, sentinel 99 present. A
            // whole-transaction drop would have kept 10 and 11.
            assert_accel_scalar(
                &rt,
                "SELECT count(*) FROM echo_child WHERE parent_id = 1",
                0,
            )
            .await?;
            assert_accel_scalar(&rt, "SELECT count(*) FROM echo_child WHERE id = 20", 1).await?;
            assert_accel_scalar(&rt, "SELECT count(*) FROM echo_child", 2).await?;

            rt.shutdown().await;
            Ok(())
        })
        .await
}

// ---------------------------------------------------------------------------
// Scenario 6 — external / peer writes apply normally
// ---------------------------------------------------------------------------

/// Suppression is scoped to our OWN transactions. A direct external write to the
/// source is a different, unregistered xid and must apply to the accelerator
/// unfiltered — proving the filter set is not over-broad (which would silently
/// drop genuine source changes and diverge peers).
#[tokio::test(flavor = "multi_thread")]
async fn external_write_applies_normally() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,connector_postgres=debug,\
         data_components::postgres_replication=debug,info",
    ));

    test_request_context()
        .scope(async {
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
            let source = connect(u16::try_from(port)?).await?;

            exec(
                &source,
                "CREATE TABLE public.echo_ext (id int PRIMARY KEY, amount int NOT NULL)",
            )
            .await?;
            exec(
                &source,
                "INSERT INTO public.echo_ext VALUES (1, 10), (2, 20)",
            )
            .await?;

            let temp = tempfile::tempdir()?;
            let ds = cdc_dataset(
                port,
                "public.echo_ext",
                "echo_ext",
                "id",
                temp.path(),
                Some("spice_echo_ext_slot"),
                true,
            );
            let rt = build_runtime("echo_ext", vec![ds]).await?;
            wait_accel_scalar(&rt, "SELECT count(*) FROM echo_ext", 2).await?;

            // Do a local write-back write first, so the registry is non-empty
            // (an empty registry filters nothing, which would make this test
            // vacuous). Its echo is suppressed; that is scenario 1's concern.
            run_query(&rt, "INSERT INTO echo_ext (id, amount) VALUES (3, 30)").await?;
            wait_source_count(
                &source,
                "SELECT count(*) FROM public.echo_ext WHERE id = 3",
                1,
            )
            .await?;

            // External writes with a DIFFERENT, unregistered xid: an insert and
            // an update. Both must apply to the accelerator.
            exec(&source, "INSERT INTO public.echo_ext VALUES (4, 40)").await?;
            exec(
                &source,
                "UPDATE public.echo_ext SET amount = 100 WHERE id = 1",
            )
            .await?;

            wait_accel_scalar(&rt, "SELECT count(*) FROM echo_ext WHERE id = 4", 1).await?;
            wait_accel_scalar(&rt, "SELECT amount FROM echo_ext WHERE id = 1", 100).await?;

            // Final state: our own write (3) present once, both external writes
            // applied, nothing dropped or duplicated.
            assert_accel_scalar(&rt, "SELECT count(*) FROM echo_ext", 4).await?;
            assert_accel_scalar(&rt, "SELECT amount FROM echo_ext WHERE id = 4", 40).await?;
            assert_accel_scalar(&rt, "SELECT count(*) FROM echo_ext WHERE id = 3", 1).await?;
            // 100 (updated) + 20 + 30 + 40.
            assert_accel_scalar(&rt, "SELECT sum(amount) FROM echo_ext", 190).await?;

            rt.shutdown().await;
            Ok(())
        })
        .await
}

// ---------------------------------------------------------------------------
// Scenario 3 — reconnect replay
// ---------------------------------------------------------------------------

/// A reconnect must not re-admit an already-filtered echo. After a local write's
/// echo, the walsender is terminated so the pump resumes from the held
/// `confirmed_flush` and replays everything the ack floor had not yet passed —
/// including the echo, which must be dropped again on replay.
///
/// Whether the echo is un-acked at the moment of the reconnect is not
/// deterministically controllable through the black-box runtime (it depends on
/// the accelerator's durable-ack timing), so this asserts the invariant that
/// survives either case: the write applies exactly once across the reconnect.
/// The deterministic "reconnect exactly before the durable ack" ordering is the
/// pump's fake-stream unit test in `postgres_replication::shared`.
#[tokio::test(flavor = "multi_thread")]
async fn echo_dropped_again_after_reconnect() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,connector_postgres=debug,\
         data_components::postgres_replication=debug,info",
    ));

    test_request_context()
        .scope(async {
            let slot = "spice_echo_reconnect_slot";
            let port = common::get_random_port()?;
            let _container = common::start_postgres_docker_container_with_logical_wal(port).await?;
            let source = connect(u16::try_from(port)?).await?;

            exec(
                &source,
                "CREATE TABLE public.echo_reconnect (id int PRIMARY KEY, amount int NOT NULL)",
            )
            .await?;
            exec(
                &source,
                "INSERT INTO public.echo_reconnect VALUES (1, 10), (2, 20)",
            )
            .await?;

            let temp = tempfile::tempdir()?;
            let ds = cdc_dataset(
                port,
                "public.echo_reconnect",
                "echo_reconnect",
                "id",
                temp.path(),
                Some(slot),
                true,
            );
            let rt = build_runtime("echo_reconnect", vec![ds]).await?;
            wait_accel_scalar(&rt, "SELECT count(*) FROM echo_reconnect", 2).await?;

            // Local write-back write; confirm it reached the source, then force a
            // reconnect so the resumed pump replays from confirmed_flush.
            run_query(
                &rt,
                "INSERT INTO echo_reconnect (id, amount) VALUES (3, 30)",
            )
            .await?;
            wait_source_count(
                &source,
                "SELECT count(*) FROM public.echo_reconnect WHERE id = 3",
                1,
            )
            .await?;
            force_stream_reconnect(&source, slot).await?;

            // Barrier after the reconnect: an external sentinel proves the
            // resumed pump has processed past the replayed echo.
            exec(
                &source,
                "INSERT INTO public.echo_reconnect VALUES (100, 999)",
            )
            .await?;
            wait_accel_scalar(&rt, "SELECT count(*) FROM echo_reconnect WHERE id = 100", 1).await?;

            assert_accel_scalar(&rt, "SELECT count(*) FROM echo_reconnect", 4).await?;
            assert_accel_scalar(&rt, "SELECT count(*) FROM echo_reconnect WHERE id = 3", 1).await?;
            assert_accel_scalar(&rt, "SELECT amount FROM echo_reconnect WHERE id = 3", 30).await?;
            assert_accel_scalar(&rt, "SELECT sum(amount) FROM echo_reconnect", 1059).await?;

            rt.shutdown().await;
            Ok(())
        })
        .await
}
