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

//! End-to-end regression coverage for per-principal isolation in the
//! caching acceleration mode.
//!
//! These tests deliberately exercise the *full* pipeline (auth principal ->
//! `RequestContext` -> `Query::run_internal` -> `TableProvider::scan` ->
//! `CachingAccelerationScanExec::execute` -> `DuckDB`) because the unit tests
//! in `accelerated_table::caching` cannot reach the two failure modes that
//! were caught only by manual end-to-end testing:
//!
//! 1. **Filters passed to `TableProvider::scan` are an optimization hint,
//!    not a contract.** An accelerator that returns `Inexact` /
//!    `Unsupported` (or that simply fails to push down a predicate) would
//!    let rows from another principal's namespace leak through to the
//!    caller. The fix is a strict `FilterExec` re-application on top of
//!    the accelerator scan; without it, `DuckDB` silently serves another
//!    principal's cached row.
//!
//! 2. **`DataFusion` does not propagate Tokio task-locals across
//!    `TableProvider::scan` and `ExecutionPlan::execute`.** Reading the
//!    cache namespace via `RequestContext::current()` in those code paths
//!    silently falls back to the global `INTERNAL_REQUEST_CONTEXT`
//!    (`Protocol::Internal`, no principal), collapsing every caller to
//!    `CacheNamespace::System`. The fix is to pull the request context
//!    out of the `SessionConfig` / `TaskContext` extension where
//!    `Query::run_internal` attaches it.
//!
//! Both regressions would silently turn the cross-principal test below
//! into a data-leak (alice's repeat upstream count would stay at 1, bob's
//! body would equal alice's), so this file is the canonical regression
//! gate for cache-namespace isolation.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use app::AppBuilder;
use app::spicepod::component::runtime::ApiKey;
use arrow::array::{Array, RecordBatch, StringArray};
use axum::{Router, routing::get};
use futures::TryStreamExt;
use runtime::Runtime;
use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
use runtime_request_context::{Protocol, RequestContext, UserAgent};
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::acceleration::get_params;
use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check},
};

/// Spawn a tiny HTTP server that returns a JSON array containing a
/// monotonically-increasing counter. The counter is bumped once per
/// successful request, so test code can read `counter.load(...)` to
/// determine how many upstream fetches actually occurred.
async fn start_per_principal_mock() -> (oneshot::Sender<()>, SocketAddr, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);
    let (tx, rx) = oneshot::channel::<()>();

    let app = Router::new().route(
        "/api/data",
        get(move || {
            let c = Arc::clone(&counter_clone);
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst) + 1;
                let body = format!("[{{\"served_at\":{n}}}]");
                ([("content-type", "application/json")], body)
            }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock listener");
    let addr = listener.local_addr().expect("local_addr");

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .unwrap_or_default();
    });

    (tx, addr, counter)
}

/// Build a `RequestContext` whose only principal is an API key.
///
/// The cache namespace is derived deterministically from the key bytes
/// (`apikey:<sha256[..16]>`), so two distinct key strings produce two
/// distinct namespaces and therefore two disjoint cache scopes.
fn principal_request_context(api_key: &str) -> Arc<RequestContext> {
    let ctx = Arc::new(
        RequestContext::builder(Protocol::Http)
            .with_user_agent(UserAgent::from_ua_str("spiceci/per-principal-test"))
            .build(),
    );
    let principal: AuthPrincipalRef = Arc::new(ApiKey::ReadOnly {
        key: api_key.to_string(),
    });
    ctx.set_auth_principal(principal)
        .expect("set_auth_principal");
    ctx
}

fn make_caching_dataset(base_url: &str, duckdb_file: &str) -> Dataset {
    let mut dataset = Dataset::new(base_url, "http_data");
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "json".to_string()),
            ("allowed_request_paths".to_string(), "/api/data".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Caching),
        params: get_params(&Mode::File, Some(duckdb_file.to_string()), "duckdb"),
        ..Acceleration::default()
    });
    dataset
}

/// Run a `SELECT content FROM http_data WHERE request_path = '/api/data'`
/// inside the given principal's request scope and return the parsed
/// `served_at` integer from the response body.
///
/// Goes through `Query::run_internal` (via `query_builder`) so the
/// principal's `RequestContext` is attached to the `SessionConfig` as
/// an extension; the accelerator scan path under test reads the cache
/// namespace from there. Bypassing this and going through the raw
/// `DataFrame` API would silently fall back to `INTERNAL_REQUEST_CONTEXT`
/// (`Protocol::Internal`, no principal) and collapse every caller to
/// `CacheNamespace::System`, defeating the test.
///
/// Sleeps briefly after collecting results so that the asynchronous
/// cache flush (writes flow through a channel to a background task)
/// has a chance to land before the next scan looks for the cached row.
///
/// Returning the integer (rather than asserting on it inline) lets the
/// caller compare the values from successive scans / different principals
/// and produce assertion messages with both sides visible.
async fn collect_served_at(rt: &Runtime, ctx: &Arc<RequestContext>) -> usize {
    let rt_clone = rt.clone();
    let ctx_clone = Arc::clone(ctx);
    let value = ctx_clone
        .scope(async move {
            let result = rt_clone
                .datafusion()
                .query_builder("SELECT content FROM http_data WHERE request_path = '/api/data'")
                .build()
                .run()
                .await
                .expect("query run");
            let batches: Vec<RecordBatch> = result
                .data
                .try_collect()
                .await
                .expect("collect scan results");
            assert!(!batches.is_empty(), "scan returned no batches");
            assert_eq!(
                batches[0].num_rows(),
                1,
                "scan returned wrong row count: {}",
                batches[0].num_rows()
            );

            let arr = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("content column should be Utf8");
            let body = arr.value(0);
            let v: serde_json::Value =
                serde_json::from_str(body).expect("response body is valid JSON");
            let obj = match &v {
                serde_json::Value::Object(_) => &v,
                serde_json::Value::Array(a) => a
                    .first()
                    .unwrap_or_else(|| panic!("empty JSON array in body: {body}")),
                _ => panic!("unexpected JSON shape in body: {body}"),
            };
            let value = obj
                .get("served_at")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_else(|| panic!("missing served_at in body: {body}"));
            usize::try_from(value).expect("served_at fits in usize")
        })
        .await;
    // Cache writes flow through an async channel to a background flush
    // task with a 500ms flush interval (`CACHE_WRITE_FLUSH_INTERVAL_MS`
    // in `accelerated_table::caching`). Wait long enough that the write
    // is guaranteed to have landed in the accelerator before the next
    // scan looks for the row.
    tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
    value
}

/// End-to-end isolation test for caching-mode acceleration with two
/// distinct API-key principals against an HTTP source backed by `DuckDB`
/// (file mode).
///
/// Walks the canonical four-step interleaving:
///
/// | Step | Caller | Upstream count delta | Body must equal |
/// |------|--------|----------------------|-----------------|
/// |  1   | alice  |        +1            | (records `alice1`) |
/// |  2   | alice  |         0            | `alice1` (cache hit) |
/// |  3   | bob    |        +1            | NOT `alice1` (separate ns) |
/// |  4   | bob    |         0            | bob's step-3 body  |
/// |  5   | alice  |         0            | `alice1` (untouched by bob) |
///
/// Either of the two regression vectors documented at the top of this
/// file would manifest as step 3 reusing alice's cached row (delta 0,
/// body equal to `alice1`) or step 5 returning bob's body (alice's
/// entry overwritten or filter not re-applied).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caching_accelerator_isolates_per_principal_e2e() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,runtime::accelerated_table::caching=debug",
    ));
    register_test_connectors().await;

    let (server_tx, addr, counter) = start_per_principal_mock().await;
    let base_url = format!("http://{addr}");

    let temp_dir = tempfile::tempdir()?;
    let duckdb_path = temp_dir
        .path()
        .join("per_principal_cache.duckdb")
        .to_string_lossy()
        .into_owned();

    // Setup runs under an admin (`Protocol::Internal`, no principal)
    // context; only the actual queries scope into alice / bob. This
    // mirrors the real runtime, where dataset registration, refresh, and
    // checkpoint code all run as `System` while incoming HTTP / Flight
    // requests carry their own per-call contexts.
    let admin_ctx = Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str("spiceci/per-principal-test-admin"))
            .build(),
    );

    let result: Result<(), anyhow::Error> = admin_ctx
        .scope(async {
            let dataset = make_caching_dataset(&base_url, &duckdb_path);
            let mut app = AppBuilder::new("test_caching_per_principal")
                .with_dataset(dataset)
                .build();

            // Disable the SQL results cache so this test measures *only*
            // the acceleration-layer cache; otherwise a duplicate query
            // could be served from the SQL results cache without ever
            // reaching the accelerator scan.
            if app.runtime.caching.sql_results.is_none() {
                app.runtime.caching.sql_results =
                    Some(spicepod::component::caching::SQLResultsCacheConfig::default());
            }
            if let Some(ref mut sql_cache) = app.runtime.caching.sql_results {
                sql_cache.enabled = false;
            }

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let load_rt = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg(
                        "Timed out waiting for caching dataset to load",
                    ));
                }
                () = load_rt.load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let alice = principal_request_context("alice-key-aaaaaaaaaaaaaaaaaaaa");
            let bob = principal_request_context("bob-key-bbbbbbbbbbbbbbbbbbbbbb");

            let upstream = || counter.load(Ordering::SeqCst);
            let baseline = upstream();

            // 1. Alice cold scan: must trigger exactly one upstream fetch.
            let alice1 = collect_served_at(&rt, &alice).await;
            assert_eq!(
                upstream() - baseline,
                1,
                "alice cold scan should fetch upstream exactly once \
                 (saw {} fetches)",
                upstream() - baseline,
            );

            // 2. Alice repeat: must serve from her own cached row,
            //    no upstream fetch.
            let alice2 = collect_served_at(&rt, &alice).await;
            assert_eq!(
                upstream() - baseline,
                1,
                "alice repeat must NOT fetch upstream \
                 (saw {} fetches; expected 1)",
                upstream() - baseline,
            );
            assert_eq!(
                alice1, alice2,
                "alice repeat must return alice's own cached body \
                 (alice1={alice1}, alice2={alice2})",
            );

            // 3. Bob cold scan, same SQL: must NOT inherit alice's
            //    cached row -> must trigger a new upstream fetch.
            //
            //    This is the cross-principal isolation gate. Either
            //    regression vector documented at the top of this file
            //    would let alice's row be served here, with the upstream
            //    counter staying at 1.
            let bob1 = collect_served_at(&rt, &bob).await;
            assert_eq!(
                upstream() - baseline,
                2,
                "bob cold scan must fetch upstream \
                 (cross-principal cache leak? saw {} fetches; expected 2)",
                upstream() - baseline,
            );
            assert_ne!(
                alice1, bob1,
                "bob must not see alice's cached body \
                 (alice1={alice1}, bob1={bob1})",
            );

            // 4. Bob repeat: must serve from bob's own cached row.
            let bob2 = collect_served_at(&rt, &bob).await;
            assert_eq!(
                upstream() - baseline,
                2,
                "bob repeat must NOT fetch upstream \
                 (saw {} fetches; expected 2)",
                upstream() - baseline,
            );
            assert_eq!(
                bob1, bob2,
                "bob repeat must return bob's own cached body \
                 (bob1={bob1}, bob2={bob2})",
            );

            // 5. Alice again, after bob: alice's cached row must still
            //    be intact (bob's writes went to a different namespace).
            let alice3 = collect_served_at(&rt, &alice).await;
            assert_eq!(
                upstream() - baseline,
                2,
                "alice repeat after bob must NOT fetch upstream \
                 (saw {} fetches; expected 2)",
                upstream() - baseline,
            );
            assert_eq!(
                alice1, alice3,
                "alice's cached body must survive bob's writes intact \
                 (alice1={alice1}, alice3={alice3})",
            );

            Ok(())
        })
        .await;

    let _ = server_tx.send(());
    result
}

/// User-facing schema must NOT expose the internal
/// `__spice_cache_namespace` storage column, even though it lives in
/// the underlying accelerator table for caching mode. This protects
/// users from accidentally depending on an internal column whose
/// presence and meaning are an implementation detail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caching_accelerator_hides_namespace_column_from_user_schema() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,runtime=debug"));
    register_test_connectors().await;

    let (server_tx, addr, _counter) = start_per_principal_mock().await;
    let base_url = format!("http://{addr}");

    let temp_dir = tempfile::tempdir()?;
    let duckdb_path = temp_dir
        .path()
        .join("hidden_column.duckdb")
        .to_string_lossy()
        .into_owned();

    let admin_ctx = Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str("spiceci/hidden-column-test"))
            .build(),
    );

    let result: Result<(), anyhow::Error> = admin_ctx
        .scope(async {
            let dataset = make_caching_dataset(&base_url, &duckdb_path);
            let mut app = AppBuilder::new("test_caching_hidden_column")
                .with_dataset(dataset)
                .build();
            if app.runtime.caching.sql_results.is_none() {
                app.runtime.caching.sql_results =
                    Some(spicepod::component::caching::SQLResultsCacheConfig::default());
            }
            if let Some(ref mut sql_cache) = app.runtime.caching.sql_results {
                sql_cache.enabled = false;
            }

            configure_test_datafusion();
            let rt = Runtime::builder().with_app(app).build().await;
            let load_rt = Arc::new(rt.clone());
            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg(
                        "Timed out waiting for caching dataset to load",
                    ));
                }
                () = load_rt.load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let table = rt
                .datafusion()
                .ctx
                .table("http_data")
                .await
                .expect("table http_data");
            let schema = table.schema();
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert!(
                !names.contains(&"__spice_cache_namespace"),
                "user-facing schema must NOT expose the internal \
                 __spice_cache_namespace column; got: {names:?}",
            );

            // Referencing the column in SQL must produce a normal
            // schema error (the reserved name behaves like any other
            // missing field; it is not silently resolved against the
            // hidden storage column).
            let err = rt
                .datafusion()
                .query_builder(
                    "SELECT __spice_cache_namespace FROM http_data \
                     WHERE request_path = '/api/data'",
                )
                .build()
                .run()
                .await
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default();
            assert!(
                err.contains("__spice_cache_namespace")
                    && (err.contains("No field") || err.contains("Schema error")),
                "expected a schema error for the reserved column; got: {err}",
            );

            Ok(())
        })
        .await;

    let _ = server_tx.send(());
    result
}
