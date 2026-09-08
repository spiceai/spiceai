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

//! Regression guard for Cayenne gated serializable transactions
//! (`BEGIN; SELECT assert(<gate>); UPDATE …; COMMIT;`).
//!
//! Every test drives the transport-neutral orchestrator
//! [`runtime::datafusion::query::run_transaction`] against an **accelerator-only**
//! Cayenne table (`on_conflict` upsert + `refresh_mode: full`) whose source is a
//! local CSV file — fully self-contained, no external Postgres or S3. This is the
//! configuration that routes gated writes through the staged commit path (see
//! `transaction::resolve_cayenne_staged`).
//!
//! The write path's sink reads the active [`cayenne::CayenneTransaction`] back
//! from the request context the orchestrator installed. In a test there is no
//! HTTP/Flight request, so [`run_txn`] wraps each call in a fresh
//! [`RequestContext`] scope — mirroring how `flightsql::prepared_statement_query`
//! `do_get` scopes the call. Without that scope the sink would publish
//! immediately and atomicity would silently NOT be exercised; the
//! [`test_txn_gate_fail_rolls_back`] canary fails loudly if staging is not wired.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use app::AppBuilder;
use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use runtime::Runtime;
use runtime::config::Config;
use runtime::datafusion::query::{TransactionError, run_transaction, transaction_statements};
use runtime_request_context::{Protocol, RequestContext, UserAgent};
use spicepod::acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode};
use spicepod::component::{access::AccessMode, dataset::Dataset};
use spicepod::param::Params;

use crate::utils::{runtime_ready_check, test_request_context};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Build an accelerator-only Cayenne dataset (`on_conflict: upsert` +
/// `refresh_mode: full` + a primary key) seeded from a local CSV file. This is
/// the exact shape `resolve_cayenne_staged` accepts as a transaction
/// participant, and it is fully self-contained (no external source).
fn make_txn_dataset(
    name: &str,
    csv_path: &std::path::Path,
    cayenne_data_dir: &std::path::Path,
    cayenne_metadata_dir: &std::path::Path,
) -> Dataset {
    let mut params = HashMap::new();
    params.insert(
        "cayenne_file_path".to_string(),
        cayenne_data_dir.display().to_string(),
    );
    params.insert(
        "cayenne_metadata_dir".to_string(),
        cayenne_metadata_dir.display().to_string(),
    );

    let mut on_conflict = HashMap::new();
    on_conflict.insert("id".to_string(), OnConflictBehavior::Upsert);

    let mut dataset = Dataset::new(format!("file://{}", csv_path.display()), name);
    dataset.access = AccessMode::ReadWrite;
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        params: Some(Params::from_string_map(params)),
        primary_key: Some("id".to_string()),
        on_conflict,
        ..Acceleration::default()
    });
    dataset
}

/// Write a two-column (`id VARCHAR`, `n BIGINT`) CSV seed file.
fn write_seed_csv(path: &std::path::Path, rows: &[(&str, i64)]) -> Result<(), String> {
    use std::fmt::Write as _;
    let mut body = String::from("id,n\n");
    for (id, n) in rows {
        writeln!(body, "{id},{n}").expect("writing to a String is infallible");
    }
    std::fs::write(path, body).map_err(|e| format!("failed to write seed CSV: {e}"))
}

/// Spin up a runtime for `app`, load components, and wait until ready. Caching is
/// disabled so out-of-transaction verification reads always observe the newest
/// committed state (the gate reads inside the transaction bypass the cache
/// regardless).
async fn build_ready_runtime(app: app::App) -> Result<Arc<Runtime>, String> {
    crate::configure_test_datafusion();
    let rt = Arc::new(
        Runtime::builder()
            .with_app(app)
            .with_runtime_config(Config::default().with_caching_disabled())
            .build()
            .await,
    );
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(1)) => {
            return Err("timed out waiting for components to load".to_string());
        }
        () = Arc::clone(&rt).load_components() => {}
    }
    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// Run a `BEGIN … COMMIT` body through the shared orchestrator.
///
/// Establishes a fresh [`RequestContext`] scope around the call so the
/// [`cayenne::CayenneTransaction`] the orchestrator installs on
/// `RequestContext::current` is the exact context the write path's sink reads
/// back (source 1 of `resolve_request_context`). This is the single most
/// important detail for actually exercising atomicity — see the module docs.
pub(crate) async fn run_txn(rt: &Runtime, sql: &str) -> Result<Vec<RecordBatch>, TransactionError> {
    let statements = transaction_statements(sql)
        .expect("test SQL must be a well-formed BEGIN…COMMIT transaction body");
    let context = Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str("spiceci-cayenne-txn"))
            .build(),
    );
    let df = rt.datafusion();
    context
        .scope(async move { run_transaction(&df, &statements, None, false).await })
        .await
        .map(|outcome| {
            outcome
                .result
                .map(|(batches, _)| batches)
                .unwrap_or_default()
        })
}

/// Human-readable rendering of a [`TransactionError`] (the type derives neither
/// `Debug` nor `Display`) for assertion messages.
pub(crate) fn describe(err: &TransactionError) -> String {
    match err {
        TransactionError::Rejected(m) => format!("Rejected({m})"),
        TransactionError::Plan(e) => format!("Plan({e})"),
        TransactionError::Query(e) => format!("Query({e})"),
        TransactionError::Stream(e) => format!("Stream({e})"),
        TransactionError::Conflict { table } => format!("Conflict({table})"),
        TransactionError::Publish(m) => format!("Publish({m})"),
    }
}

/// Read `n` for a row by primary key through the runtime query path; `None` if no
/// such row exists.
async fn read_n(rt: &Runtime, table: &str, id: &str) -> Option<i64> {
    let df = rt.datafusion();
    let sql = format!("SELECT n FROM {table} WHERE id = '{id}'");
    let batches = df
        .query_builder(&sql)
        .build()
        .run()
        .await
        .unwrap_or_else(|e| panic!("read query `{sql}` failed to plan: {e}"))
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .unwrap_or_else(|e| panic!("read query `{sql}` failed to execute: {e}"));
    let batch = batches.iter().find(|b| b.num_rows() > 0)?;
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("column `n` should be Int64");
    Some(col.value(0))
}

/// `SUM(n)` across `table`, as the quota tests' oracle. `COALESCE` so an empty
/// table reads 0 rather than NULL — the reservation gate is written the same way,
/// because a NULL gate aborts (see `test_txn_null_gate_fail_safe`) and a quota
/// system must admit its first reservation.
async fn read_sum(rt: &Runtime, table: &str) -> i64 {
    let df = rt.datafusion();
    let sql = format!("SELECT COALESCE(SUM(n), 0) FROM {table}");
    let batches = df
        .query_builder(&sql)
        .build()
        .run()
        .await
        .unwrap_or_else(|e| panic!("read query `{sql}` failed to plan: {e}"))
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .unwrap_or_else(|e| panic!("read query `{sql}` failed to execute: {e}"));
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("read query `{sql}` returned no rows"));
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("SUM(n) should be Int64")
        .value(0)
}

// ---------------------------------------------------------------------------
// Tests driving the run_transaction orchestrator
// ---------------------------------------------------------------------------

/// commit: a passing gate + an UPDATE publishes the write.
///
/// `BEGIN; SELECT assert((SELECT n …) < 5); UPDATE … n=n+1; COMMIT;` on `a=0`
/// leaves `a=1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_txn_commit_applies_update() -> Result<(), String> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
            let csv = temp.path().join("seed.csv");
            write_seed_csv(&csv, &[("a", 0)])?;
            let app = AppBuilder::new("test_txn_commit")
                .with_dataset(make_txn_dataset(
                    "t",
                    &csv,
                    &temp.path().join("data"),
                    &temp.path().join("meta"),
                ))
                .build();
            let rt = build_ready_runtime(app).await?;

            assert_eq!(read_n(&rt, "t", "a").await, Some(0), "seed value");

            let result = run_txn(
                &rt,
                "BEGIN; SELECT assert((SELECT n FROM t WHERE id='a') < 5); \
                 UPDATE t SET n=n+1 WHERE id='a'; COMMIT;",
            )
            .await;
            assert!(
                result.is_ok(),
                "transaction should commit, got {}",
                result.err().map(|e| describe(&e)).unwrap_or_default()
            );

            assert_eq!(
                read_n(&rt, "t", "a").await,
                Some(1),
                "committed UPDATE should have applied"
            );
            Ok(())
        })
        .await
}

/// gate-fail rollback (CANARY): a write staged before a failing gate must NOT be
/// published.
///
/// `BEGIN; UPDATE … n=99; SELECT assert(false); COMMIT;` returns an error and
/// leaves `a` unchanged. This is the canary for the whole feature: if the write
/// path is not reading the transaction back (staging not wired), the UPDATE
/// publishes immediately and the final assertion — that `a` is still `0` — fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_txn_gate_fail_rolls_back() -> Result<(), String> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
            let csv = temp.path().join("seed.csv");
            write_seed_csv(&csv, &[("a", 0)])?;
            let app = AppBuilder::new("test_txn_gate_fail")
                .with_dataset(make_txn_dataset(
                    "t",
                    &csv,
                    &temp.path().join("data"),
                    &temp.path().join("meta"),
                ))
                .build();
            let rt = build_ready_runtime(app).await?;

            let result = run_txn(
                &rt,
                "BEGIN; UPDATE t SET n=99 WHERE id='a'; SELECT assert(false); COMMIT;",
            )
            .await;
            assert!(result.is_err(), "a failing gate must abort the transaction");

            // The load-bearing assertion: the pre-gate UPDATE must have been
            // STAGED (not published), so the failed gate rolls it back.
            assert_eq!(
                read_n(&rt, "t", "a").await,
                Some(0),
                "gate-fail canary: staged write must NOT have published (a must stay 0)"
            );
            Ok(())
        })
        .await
}

/// cap enforcement: a `< cap` gate admits exactly `cap` commits, aborts the rest,
/// and leaves the counter pinned at `cap`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_txn_cap_enforcement() -> Result<(), String> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            const CAP: i64 = 5;
            const ATTEMPTS: usize = 8;

            let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
            let csv = temp.path().join("seed.csv");
            write_seed_csv(&csv, &[("a", 0)])?;
            let app = AppBuilder::new("test_txn_cap")
                .with_dataset(make_txn_dataset(
                    "t",
                    &csv,
                    &temp.path().join("data"),
                    &temp.path().join("meta"),
                ))
                .build();
            let rt = build_ready_runtime(app).await?;

            let sql = format!(
                "BEGIN; SELECT assert((SELECT n FROM t WHERE id='a') < {CAP}); \
                 UPDATE t SET n=n+1 WHERE id='a'; COMMIT;"
            );

            let mut commits = 0usize;
            let mut aborts = 0usize;
            for attempt in 0..ATTEMPTS {
                match run_txn(&rt, &sql).await {
                    Ok(_) => commits += 1,
                    // A failed `assert()` gate surfaces as the statement's execution
                    // error. It can arrive either eagerly from `.run()` (Query) or
                    // while draining the gate statement's stream (Stream) — the gate
                    // is a non-final statement — so accept both as a gate abort.
                    Err(err @ (TransactionError::Query(_) | TransactionError::Stream(_))) => {
                        assert!(
                            describe(&err).contains("assertion failed"),
                            "attempt {attempt}: expected a gate abort, got {}",
                            describe(&err)
                        );
                        aborts += 1;
                    }
                    Err(other) => {
                        return Err(format!(
                            "attempt {attempt}: unexpected error {}",
                            describe(&other)
                        ));
                    }
                }
            }

            let cap = usize::try_from(CAP).expect("CAP is non-negative");
            assert_eq!(commits, cap, "exactly `cap` commits should succeed");
            assert_eq!(aborts, ATTEMPTS - cap, "the rest must abort at the gate");
            assert_eq!(
                read_n(&rt, "t", "a").await,
                Some(CAP),
                "final counter must be pinned at the cap"
            );
            Ok(())
        })
        .await
}

/// quota reservation, sequential: an `assert(SUM + k <= QUOTA)` gate followed by
/// an `INSERT` admits exactly the reservations that fit and aborts the rest.
///
/// Distinct from [`test_txn_cap_enforcement`] in the two ways that matter. The
/// gate reads an **aggregate over every row** rather than one key, so the
/// transaction's read set is the whole table; and the write **inserts a new key**
/// rather than updating an existing one. Together those are the shape a
/// reservation system has — "is there room left, and if so take some" — and they
/// reach the staged commit path differently from a single-key counter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_txn_quota_reservation_admits_only_what_fits() -> Result<(), String> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            const QUOTA: i64 = 100;
            const RESERVATION: i64 = 30;
            const ATTEMPTS: usize = 5;
            // The seed already holds 10 of the quota, so three reservations of 30
            // fit exactly (10 + 90 = 100) and the fourth would exceed it.
            const SEEDED: i64 = 10;
            const EXPECTED_COMMITS: usize = 3;

            let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
            let csv = temp.path().join("seed.csv");
            write_seed_csv(&csv, &[("r0", SEEDED)])?;
            let app = AppBuilder::new("test_txn_quota_sequential")
                .with_dataset(make_txn_dataset(
                    "t",
                    &csv,
                    &temp.path().join("data"),
                    &temp.path().join("meta"),
                ))
                .build();
            let rt = build_ready_runtime(app).await?;
            assert_eq!(read_sum(&rt, "t").await, SEEDED, "seeded quota usage");

            let mut commits = 0usize;
            let mut aborts = 0usize;
            for attempt in 0..ATTEMPTS {
                // A distinct key per attempt: reserving is an insert, and reusing a
                // key would upsert over an earlier reservation instead of adding to
                // the total, quietly making the quota unreachable.
                let sql = format!(
                    "BEGIN; \
                     SELECT assert((SELECT COALESCE(SUM(n), 0) FROM t) + {RESERVATION} <= {QUOTA}); \
                     INSERT INTO t (id, n) VALUES ('r{attempt}_user', {RESERVATION}); COMMIT;"
                );
                match run_txn(&rt, &sql).await {
                    Ok(_) => commits += 1,
                    Err(err @ (TransactionError::Query(_) | TransactionError::Stream(_))) => {
                        assert!(
                            describe(&err).contains("assertion failed"),
                            "attempt {attempt}: expected a gate abort, got {}",
                            describe(&err)
                        );
                        aborts += 1;
                    }
                    Err(other) => {
                        return Err(format!(
                            "attempt {attempt}: unexpected error {}",
                            describe(&other)
                        ));
                    }
                }
            }

            assert_eq!(commits, EXPECTED_COMMITS, "only the reservations that fit commit");
            assert_eq!(aborts, ATTEMPTS - EXPECTED_COMMITS, "the rest abort at the gate");
            assert_eq!(
                read_sum(&rt, "t").await,
                QUOTA,
                "the admitted reservations must land exactly on the quota"
            );
            Ok(())
        })
        .await
}

/// quota reservation, concurrent: the quota is never oversubscribed.
///
/// This is the write-skew question, and the reason the aggregate gate deserves
/// its own test. Per-key optimistic concurrency compares the keys a transaction
/// touched, but every reservation here inserts a *different* key while reading
/// the same aggregate — so nothing about the keys reveals the conflict, and two
/// transactions that each saw room could each take it. Cayenne's answer is to
/// degrade the keyset and fall back to per-table conflict detection
/// (`mark_pk_keyset_occ_degraded`), which until now only a unit test covered.
///
/// The safety invariant is asserted strictly: committed reservations must never
/// exceed the quota. Liveness is asserted weakly — at least one must get through
/// — because a concurrent attempt may legitimately lose either at the gate or to
/// a conflict, and pinning the exact commit count would make the test a race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(not(target_os = "windows"))]
async fn test_txn_quota_holds_under_concurrent_reservations() -> Result<(), String> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            const QUOTA: i64 = 100;
            const RESERVATION: i64 = 30;
            const SEEDED: i64 = 10;
            // More contenders than can fit, so the gate and the conflict detector
            // are both under real pressure.
            const CONTENDERS: usize = 8;

            let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
            let csv = temp.path().join("seed.csv");
            write_seed_csv(&csv, &[("r0", SEEDED)])?;
            let app = AppBuilder::new("test_txn_quota_concurrent")
                .with_dataset(make_txn_dataset(
                    "t",
                    &csv,
                    &temp.path().join("data"),
                    &temp.path().join("meta"),
                ))
                .build();
            let rt = build_ready_runtime(app).await?;

            let mut tasks = Vec::with_capacity(CONTENDERS);
            for contender in 0..CONTENDERS {
                let rt = Arc::clone(&rt);
                tasks.push(tokio::spawn(async move {
                    let sql = format!(
                        "BEGIN; \
                         SELECT assert((SELECT COALESCE(SUM(n), 0) FROM t) + {RESERVATION} <= {QUOTA}); \
                         INSERT INTO t (id, n) VALUES ('u{contender}', {RESERVATION}); COMMIT;"
                    );
                    run_txn(&rt, &sql).await.is_ok()
                }));
            }

            let mut commits = 0usize;
            for task in tasks {
                if task.await.map_err(|e| format!("reservation task panicked: {e}"))? {
                    commits += 1;
                }
            }

            let total = read_sum(&rt, "t").await;
            assert!(
                total <= QUOTA,
                "the quota was oversubscribed: {commits} reservations of {RESERVATION} \
                 committed over a seeded {SEEDED}, leaving SUM(n)={total} against a \
                 quota of {QUOTA}"
            );
            assert!(
                commits >= 1,
                "no reservation got through at all (SUM(n)={total}); the gate or the \
                 conflict detector is rejecting everything"
            );
            Ok(())
        })
        .await
}

/// NULL-gate is fail-safe: comparing against a value from an absent row yields
/// NULL, and `assert(NULL)` aborts (no write published).
///
/// Deliberately uses a value comparison (`< 5`) against a missing row — NOT
/// `IS NOT NULL`, which is the known-deferred bug #11832.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_txn_null_gate_fail_safe() -> Result<(), String> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
            let csv = temp.path().join("seed.csv");
            write_seed_csv(&csv, &[("a", 0)])?;
            let app = AppBuilder::new("test_txn_null_gate")
                .with_dataset(make_txn_dataset(
                    "t",
                    &csv,
                    &temp.path().join("data"),
                    &temp.path().join("meta"),
                ))
                .build();
            let rt = build_ready_runtime(app).await?;

            let result = run_txn(
                &rt,
                "BEGIN; SELECT assert((SELECT n FROM t WHERE id='absent') < 5); \
                 UPDATE t SET n=n+1 WHERE id='a'; COMMIT;",
            )
            .await;
            assert!(
                result.is_err(),
                "a NULL gate comparison must abort (fail-safe)"
            );
            assert_eq!(
                read_n(&rt, "t", "a").await,
                Some(0),
                "NULL-gate abort must not publish the staged UPDATE"
            );
            Ok(())
        })
        .await
}

/// multi-table atomic: one transaction writing two accelerator-only Cayenne
/// tables that share a metastore commits both or neither. A failing gate leaves
/// both unchanged.
///
/// Both datasets share one `cayenne_metadata_dir`; the runtime keeps a single
/// `CayenneAccelerator` per engine, so both tables resolve to the same metastore
/// catalog and the commit fuses their publishes into one metastore transaction.
#[test]
#[cfg(not(target_os = "windows"))]
fn test_txn_multi_table_atomic() -> Result<(), String> {
    // The two-table fused commit plans/unparses deeper than the ~2 MiB default
    // test-thread stack in debug builds (single-table txns stay under it); run
    // the body on a dedicated large-stack thread, mirroring
    // `http::test_http_dynamic_request_headers_accelerated_view`.
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(16 * 1024 * 1024)
                .enable_all()
                .build()
                .map_err(|e| format!("failed to build tokio runtime: {e}"))?
                .block_on(run_test_txn_multi_table_atomic())
        })
        .map_err(|e| format!("failed to spawn test thread: {e}"))?
        .join()
        .map_err(|_| "test thread panicked".to_string())?
}

async fn run_test_txn_multi_table_atomic() -> Result<(), String> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
            let csv1 = temp.path().join("t1.csv");
            let csv2 = temp.path().join("t2.csv");
            write_seed_csv(&csv1, &[("a", 0)])?;
            write_seed_csv(&csv2, &[("a", 0)])?;

            // Shared data + metadata dirs => one shared Cayenne metastore.
            let data_dir = temp.path().join("data");
            let meta_dir = temp.path().join("meta");
            let app = AppBuilder::new("test_txn_multi_table")
                .with_dataset(make_txn_dataset("t1", &csv1, &data_dir, &meta_dir))
                .with_dataset(make_txn_dataset("t2", &csv2, &data_dir, &meta_dir))
                .build();
            let rt = build_ready_runtime(app).await?;

            assert_eq!(read_n(&rt, "t1", "a").await, Some(0), "t1 seed");
            assert_eq!(read_n(&rt, "t2", "a").await, Some(0), "t2 seed");

            // Both move together.
            let ok = run_txn(
                &rt,
                "BEGIN; SELECT assert((SELECT n FROM t1 WHERE id='a') < 5); \
                 UPDATE t1 SET n=n+1 WHERE id='a'; \
                 UPDATE t2 SET n=n+1 WHERE id='a'; COMMIT;",
            )
            .await;
            assert!(
                ok.is_ok(),
                "two-table commit should succeed, got {}",
                ok.err().map(|e| describe(&e)).unwrap_or_default()
            );
            assert_eq!(read_n(&rt, "t1", "a").await, Some(1), "t1 committed");
            assert_eq!(read_n(&rt, "t2", "a").await, Some(1), "t2 committed");

            // Failing gate after both writes stage => neither moves.
            let failed = run_txn(
                &rt,
                "BEGIN; UPDATE t1 SET n=n+1 WHERE id='a'; \
                 UPDATE t2 SET n=n+1 WHERE id='a'; SELECT assert(false); COMMIT;",
            )
            .await;
            assert!(failed.is_err(), "failing gate must abort the two-table txn");
            assert_eq!(
                read_n(&rt, "t1", "a").await,
                Some(1),
                "t1 must be unchanged"
            );
            assert_eq!(
                read_n(&rt, "t2", "a").await,
                Some(1),
                "t2 must be unchanged"
            );
            Ok(())
        })
        .await
}

// ---------------------------------------------------------------------------
// OCC conflict (deterministic stale-token commit)
// ---------------------------------------------------------------------------

/// OCC conflict: committing a staged write whose begin token was captured before
/// a conflicting commit surfaces a retryable conflict.
///
/// Forcing a genuine concurrent interleave through `run_transaction` cannot be
/// made deterministic (its begin-token capture and commit are one atomic call,
/// with no injectable pause between them), so — as the task permits — this
/// exercises the exact commit-time OCC re-check `run_transaction` relies on
/// ([`cayenne::CayenneTransaction::commit`], which `run_transaction` calls and
/// whose `Error::WriteConflict` it maps 1:1 to [`TransactionError::Conflict`])
/// with a deterministically stale token. A positive control (a fresh token
/// commits cleanly) proves the conflict is caused by the stale token, not a
/// broken harness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_txn_occ_conflict_stale_token() -> Result<(), anyhow::Error> {
    use cayenne::metadata::{CreateTableOptions, VortexConfig};
    use cayenne::provider::Error as CayenneError;
    use cayenne::{CayenneCatalog, CayenneTableProvider, CayenneTransaction, MetadataCatalog};
    use datafusion_table_providers::util::{
        column_reference::ColumnReference, on_conflict::OnConflict,
    };

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir()?;
            let cayenne_dir = temp.path().join("cayenne_occ");
            let metadata_db = temp.path().join("metadata_occ.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            let schema = occ_schema();
            let options = CreateTableOptions {
                table_name: "occ_t".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            };

            let catalog = Arc::new(CayenneCatalog::new(format!(
                "sqlite://{}",
                metadata_db.to_string_lossy()
            ))?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            let ctx = SessionContext::new();
            let provider =
                CayenneTableProvider::create_table(catalog_arc, options, ctx.runtime_env()).await?;
            ctx.register_table(
                "occ_t",
                Arc::new(provider.clone_for_write_operations())
                    as Arc<dyn datafusion::datasource::TableProvider>,
            )?;
            let table_id = provider.table_id().to_string();

            // Seed a=0.
            ctx.sql("INSERT INTO occ_t (id, n) VALUES ('a', 0)")
                .await?
                .collect()
                .await?;

            // Capture a begin token, then stage an upsert against it (off-lock).
            let stale_token = provider.transaction_write_token().await;
            let staged = provider
                .begin_staged_upsert_occ(stale_token, single_row_stream(&schema, "a", 99), 1)
                .await?;

            // Concurrently advance the same key PAST the captured token (a plain
            // upsert on the sync path), making the staged write's token stale.
            ctx.sql("INSERT INTO occ_t (id, n) VALUES ('a', 20)")
                .await?
                .collect()
                .await?;
            assert_eq!(
                row_a_value(&ctx, "occ_t").await?,
                Some(20),
                "advancing upsert applied"
            );
            let txn = CayenneTransaction::new();
            txn.register(
                table_id.clone(),
                stale_token,
                provider.clone_for_write_operations(),
            );
            txn.set_staged(&table_id, staged);
            match txn.commit().await {
                Err(CayenneError::WriteConflict { .. }) => {}
                Err(other) => {
                    return Err(anyhow::anyhow!(
                        "stale-token commit should conflict, got error: {other}"
                    ));
                }
                Ok(_) => {
                    return Err(anyhow::anyhow!(
                        "stale-token commit should have returned WriteConflict, but committed"
                    ));
                }
            }

            // The conflicting write (n=99) must have rolled back.
            assert_eq!(
                row_a_value(&ctx, "occ_t").await?,
                Some(20),
                "a lost OCC race must not publish (n must stay 20, not 99)"
            );
            Ok(())
        })
        .await
}

/// Inline-tier gated commit (regression for the fused-path inline-tombstone gap).
///
/// A gated upsert whose superseded row lives in the mem/inline tier (never
/// checkpointed to a file) must now commit: the fused commit path composes the
/// inline tombstone via trunk's deferred `durable_payload`. The earlier bespoke
/// multi-table path rejected this with "multi-table transaction on inline-mode
/// table ... unsupported" — this test guards against that regression returning.
#[tokio::test]
async fn test_txn_inline_tombstone_commits() -> Result<(), anyhow::Error> {
    use cayenne::metadata::{CreateTableOptions, VortexConfig};
    use cayenne::{CayenneCatalog, CayenneTableProvider, CayenneTransaction, MetadataCatalog};
    use datafusion_table_providers::util::{
        column_reference::ColumnReference, on_conflict::OnConflict,
    };

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp = tempfile::tempdir()?;
            let cayenne_dir = temp.path().join("cayenne_inline");
            let metadata_db = temp.path().join("metadata_inline.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            let schema = occ_schema();
            let options = CreateTableOptions {
                table_name: "inline_t".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            };
            let catalog = Arc::new(CayenneCatalog::new(format!(
                "sqlite://{}",
                metadata_db.to_string_lossy()
            ))?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            let ctx = SessionContext::new();
            let provider =
                CayenneTableProvider::create_table(catalog_arc, options, ctx.runtime_env()).await?;
            ctx.register_table(
                "inline_t",
                Arc::new(provider.clone_for_write_operations())
                    as Arc<dyn datafusion::datasource::TableProvider>,
            )?;
            let table_id = provider.table_id().to_string();

            // Seed a=0 and DO NOT checkpoint the mem/inline tier: the superseded
            // row the gated upsert replaces is an INLINE row, so its on-conflict
            // deletion is an inline tombstone.
            ctx.sql("INSERT INTO inline_t (id, n) VALUES ('a', 0)")
                .await?
                .collect()
                .await?;

            // Gated upsert a=0 -> a=10 through the fused commit path.
            let token = provider.transaction_write_token().await;
            let staged = provider
                .begin_staged_upsert_occ(token, single_row_stream(&schema, "a", 10), 1)
                .await?;
            let txn = CayenneTransaction::new();
            txn.register(
                table_id.clone(),
                token,
                provider.clone_for_write_operations(),
            );
            txn.set_staged(&table_id, staged);
            txn.commit().await.map_err(|e| {
                anyhow::anyhow!("inline-tier gated commit must succeed, not be rejected: {e}")
            })?;

            // The inline tombstone hid the old a=0; the upsert's a=10 is visible.
            assert_eq!(
                row_a_value(&ctx, "inline_t").await?,
                Some(10),
                "inline-tier gated upsert must publish a=10"
            );
            Ok(())
        })
        .await
}

/// Schema for the standalone OCC provider: `(id VARCHAR PK, n BIGINT)`.
fn occ_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("n", DataType::Int64, false),
    ]))
}

/// A single-row `SendableRecordBatchStream` to stage into a Cayenne upsert.
fn single_row_stream(schema: &SchemaRef, id: &str, n: i64) -> SendableRecordBatchStream {
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(StringArray::from(vec![id.to_string()])) as ArrayRef,
            Arc::new(Int64Array::from(vec![n])) as ArrayRef,
        ],
    )
    .expect("build staged batch");
    Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(schema),
        futures::stream::once(async move { Ok(batch) }),
    ))
}

/// Read `n` for `id='a'` from a standalone provider registered as `table`.
async fn row_a_value(ctx: &SessionContext, table: &str) -> Result<Option<i64>, anyhow::Error> {
    let batches = ctx
        .sql(&format!("SELECT n FROM {table} WHERE id = 'a'"))
        .await?
        .collect()
        .await?;
    let Some(batch) = batches.iter().find(|b| b.num_rows() > 0) else {
        return Ok(None);
    };
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("column `n` should be Int64"))?;
    Ok(Some(col.value(0)))
}

// ---------------------------------------------------------------------------
// Lightweight throughput bench
// ---------------------------------------------------------------------------

/// Throughput measurement: drive `K` sequential gated commits and report
/// commits/sec. Kept fast and deterministic (no external deps, no sleeps); the
/// asserted lower bound is intentionally loose — it guards against a catastrophic
/// regression (e.g. accidentally publishing synchronously per statement or an
/// O(n) commit), not against fine-grained perf drift.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(not(target_os = "windows"))]
async fn bench_txn_gated_commit_throughput() -> Result<(), String> {
    let _tracing = crate::init_tracing(Some("integration=info"));

    test_request_context()
        .scope(async {
            const K: i64 = 50;

            let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
            let csv = temp.path().join("seed.csv");
            write_seed_csv(&csv, &[("a", 0)])?;
            let app = AppBuilder::new("bench_txn_throughput")
                .with_dataset(make_txn_dataset(
                    "t",
                    &csv,
                    &temp.path().join("data"),
                    &temp.path().join("meta"),
                ))
                .build();
            let rt = build_ready_runtime(app).await?;

            // Gate cap well above K so every commit is admitted.
            let sql = format!(
                "BEGIN; SELECT assert((SELECT n FROM t WHERE id='a') < {}); \
                 UPDATE t SET n=n+1 WHERE id='a'; COMMIT;",
                K + 1
            );

            let start = Instant::now();
            for i in 0..K {
                run_txn(&rt, &sql)
                    .await
                    .map_err(|e| format!("commit {i} failed: {}", describe(&e)))?;
            }
            let elapsed = start.elapsed();
            let per_sec = f64::from(u32::try_from(K).unwrap_or(u32::MAX)) / elapsed.as_secs_f64();

            println!(
                "cayenne gated-commit throughput: {K} commits in {:.3}s = {per_sec:.1} commits/sec",
                elapsed.as_secs_f64()
            );

            assert_eq!(
                read_n(&rt, "t", "a").await,
                Some(K),
                "every gated commit should have applied exactly once"
            );
            assert!(
                per_sec > 0.5,
                "gated-commit throughput fell below the sanity floor: {per_sec:.2} commits/sec"
            );
            Ok(())
        })
        .await
}
