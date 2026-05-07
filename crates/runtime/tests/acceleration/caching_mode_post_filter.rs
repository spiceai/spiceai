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

//! Regression coverage for caching-mode post-filter handling.
//!
//! Pins the (currently undocumented) behavior that
//! `refresh_mode: caching` always falls back to the federated source
//! when a query's full predicate eliminates every cached row, even
//! though `on_zero_results` defaults to `return_empty`. This works
//! because `CachingAccelerationScanExec` interprets a zero-row
//! accelerator scan as a cache miss and routes through
//! `handle_cache_miss` to fetch the source, regardless of the
//! configured `on_zero_results`.
//!
//! The corollary is that `on_zero_results` is effectively a no-op in
//! caching mode (the runtime emits a warning at dataset load time
//! noting this). The two tests below exercise both `return_empty`
//! (default) and `use_source` and assert identical observable
//! behavior, which is what makes this a regression gate: if either
//! the cache-miss-on-zero-rows heuristic in
//! `CachingAccelerationScanExec` or the `Inexact` filter pushdown
//! contract in `AcceleratedTable::supports_filters_pushdown` ever
//! changes, this test will fail and force a deliberate decision.
//!
//! Test mechanics: an in-process `axum` mock HTTP server with an
//! `AtomicUsize` request counter so the test can distinguish "served
//! from cache" (counter unchanged) from "fetched source" (counter
//! incremented).

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use app::AppBuilder;
use arrow::array::RecordBatch;
use axum::{Router, routing::get};
use futures::TryStreamExt;
use runtime::Runtime;
use runtime_request_context::{Protocol, RequestContext, UserAgent};
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode, ZeroResultsAction},
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

/// Spawn a tiny HTTP server that returns a JSON array containing one
/// row whose `served_at` value increases on every request. The shared
/// counter is bumped on each successful response so the test can
/// observe how many upstream fetches actually occurred.
///
/// The body always contains the literal `match-me` and never the
/// literal `nope-XYZ`, so a `content LIKE '%nope-XYZ%'` filter is
/// guaranteed to post-filter to empty regardless of how many times
/// the source has been hit.
async fn start_counting_mock() -> (oneshot::Sender<()>, SocketAddr, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);
    let (tx, rx) = oneshot::channel::<()>();

    let app = Router::new().route(
        "/api/data",
        get(move || {
            let c = Arc::clone(&counter_clone);
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst) + 1;
                let body = format!("[{{\"marker\":\"match-me\",\"served_at\":{n}}}]");
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

/// Build a caching-mode HTTP dataset backed by `DuckDB` (file mode).
///
/// Uses the default HTTP schema (no `columns:`/decomposition) so the
/// `content` column holds the raw JSON response body verbatim. The
/// tests post-filter on substrings of `content` to construct a
/// predicate the HTTP connector reports as `Unsupported` for
/// pushdown -- `DuckDB` still evaluates it during the accelerator scan,
/// which is precisely what makes the cache-miss-on-zero-rows
/// heuristic kick in.
///
/// `zero_results_action` is the value plumbed through to the
/// `AcceleratedTable`; in caching mode it is currently ignored at
/// scan time.
fn make_caching_dataset(
    base_url: &str,
    duckdb_file: &str,
    zero_results_action: ZeroResultsAction,
) -> Dataset {
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
        on_zero_results: zero_results_action,
        ..Acceleration::default()
    });
    dataset
}

/// Build the admin (`Protocol::Internal`, no principal) request
/// context that the dataset setup and queries scope into. The
/// caching pipeline's `RequestContext` extension lookup needs an
/// explicit scope so every query sees the same cache namespace
/// (`CacheNamespace::System` here); without one the lookup falls
/// back to the global `INTERNAL_REQUEST_CONTEXT`, which works in
/// production but can interact unpredictably with parallel test
/// runs.
fn admin_request_context() -> Arc<RequestContext> {
    Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str("spiceci/caching-post-filter-test"))
            .build(),
    )
}

/// Run a single SQL statement against the runtime via `query_builder`
/// (the same path as the public HTTP SQL endpoint) and return the
/// collected batches. Sleeps briefly afterward so the asynchronous
/// cache write has time to land before the next scan.
async fn run_sql(rt: &Runtime, ctx: &Arc<RequestContext>, sql: &str) -> Vec<RecordBatch> {
    let rt_clone = rt.clone();
    let ctx_clone = Arc::clone(ctx);
    let sql = sql.to_string();
    let batches = ctx_clone
        .scope(async move {
            let result = rt_clone
                .datafusion()
                .query_builder(&sql)
                .build()
                .run()
                .await
                .expect("query run");
            let collected: Vec<RecordBatch> = result
                .data
                .try_collect()
                .await
                .expect("collect scan results");
            collected
        })
        .await;
    // Cache writes flow through an async channel to a background flush
    // task; sleep long enough that the write has landed before the
    // next scan looks for the cached row.
    tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
    batches
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Drives the canonical four-step interleaving and asserts the same
/// observable behavior regardless of the configured
/// `on_zero_results`:
///
/// | Step | SQL                                                                | Upstream count delta | Rows |
/// |------|--------------------------------------------------------------------|----------------------|------|
/// |  1   | `WHERE request_path = '/api/data'`                                 |        +1            |  1   |
/// |  2   | `WHERE request_path = '/api/data'`                                 |         0            |  1   |
/// |  3   | `WHERE request_path = '/api/data' AND content LIKE '%nope-XYZ%'`   |        +1            |  0   |
/// |  4   | `WHERE request_path = '/api/data'`                                 |         0            |  2   |
///
/// Step 3 is the regression gate: the post-filter eliminates every
/// cached row, the accelerator scan returns zero rows, and
/// `CachingAccelerationScanExec` interprets that as a cache miss and
/// fetches the federated source. The fetched body STILL doesn't
/// contain the `nope-XYZ` substring, so the user-visible result is
/// empty -- but the upstream counter MUST increment.
///
/// Step 4's row count of 2 (not 1) is a deliberate observation, not
/// a bug: the source fetch in step 3 cached its response alongside
/// the original cold-scan row, so a subsequent partition-key-only
/// query now sees both. Step 4's *upstream count* assertion is the
/// real guarantee -- the fetch in step 3 didn't disturb the cache's
/// ability to serve subsequent partition-key queries without going
/// to source again.
async fn run_post_filter_scenario(
    label: &'static str,
    duckdb_file_name: &str,
    app_name: &str,
    zero_results_action: ZeroResultsAction,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some(
        "integration=debug,runtime=debug,runtime::accelerated_table=debug",
    ));
    register_test_connectors().await;

    let (server_tx, addr, counter) = start_counting_mock().await;
    let base_url = format!("http://{addr}");

    let temp_dir = tempfile::tempdir()?;
    let duckdb_path = temp_dir
        .path()
        .join(duckdb_file_name)
        .to_string_lossy()
        .into_owned();

    let admin = admin_request_context();
    let result: Result<(), anyhow::Error> = Arc::clone(&admin)
        .scope(async {
            let dataset = make_caching_dataset(&base_url, &duckdb_path, zero_results_action);
            let mut app = AppBuilder::new(app_name).with_dataset(dataset).build();

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

            let upstream = || counter.load(Ordering::SeqCst);
            let baseline = upstream();

            // 1. Cold scan on the partition key: must trigger exactly one upstream fetch.
            let q1 = run_sql(
                &rt,
                &admin,
                "SELECT content FROM http_data WHERE request_path = '/api/data'",
            )
            .await;
            assert_eq!(
                upstream() - baseline,
                1,
                "[{label}] step 1 cold scan should fetch upstream exactly once \
                 (saw {} fetches)",
                upstream() - baseline,
            );
            assert_eq!(total_rows(&q1), 1, "[{label}] step 1 should return 1 row");

            // 2. Repeat of the same partition-key query: served from
            //    cache, no upstream fetch.
            let q2 = run_sql(
                &rt,
                &admin,
                "SELECT content FROM http_data WHERE request_path = '/api/data'",
            )
            .await;
            assert_eq!(
                upstream() - baseline,
                1,
                "[{label}] step 2 repeat MUST NOT fetch upstream \
                 (saw {} fetches; expected 1)",
                upstream() - baseline,
            );
            assert_eq!(total_rows(&q2), 1, "[{label}] step 2 should still return 1 row");

            // 3. Same partition key, additional non-pushdown predicate
            //    that eliminates every cached row. Caching mode treats
            //    the resulting zero-row accelerator scan as a cache
            //    miss and MUST fetch the source -- regardless of the
            //    configured `on_zero_results` value.
            let q3 = run_sql(
                &rt,
                &admin,
                "SELECT content FROM http_data \
                 WHERE request_path = '/api/data' AND content LIKE '%nope-XYZ%'",
            )
            .await;
            assert_eq!(
                upstream() - baseline,
                2,
                "[{label}] step 3 post-filter-empty MUST fall back to source \
                 (saw {} fetches; expected 2)",
                upstream() - baseline,
            );
            assert_eq!(
                total_rows(&q3),
                0,
                "[{label}] step 3 should still post-filter to empty (substring not in source body)",
            );

            // 4. Repeat the original partition-key query: the fetch in
            //    step 3 must NOT have corrupted the partition-key cache
            //    entry -- still served from cache, no upstream fetch.
            let q4 = run_sql(
                &rt,
                &admin,
                "SELECT content FROM http_data WHERE request_path = '/api/data'",
            )
            .await;
            assert_eq!(
                upstream() - baseline,
                2,
                "[{label}] step 4 MUST NOT fetch upstream (saw {} fetches; expected 2)",
                upstream() - baseline,
            );
            assert_eq!(
                total_rows(&q4),
                2,
                "[{label}] step 4 should return 2 rows (1 from cold scan + 1 from step-3 source fetch); got {}",
                total_rows(&q4),
            );

            Ok(())
        })
        .await;

    let _ = server_tx.send(());
    result
}

/// Default (`on_zero_results: return_empty`) caching dataset: the
/// post-filter-empty case still falls back to source. This is the
/// surprising-but-correct behavior the runtime warns about at
/// dataset load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caching_default_falls_back_on_post_filter_zero_results() -> Result<(), anyhow::Error> {
    run_post_filter_scenario(
        "default-return_empty",
        "post_filter_default.duckdb",
        "test_caching_post_filter_default",
        ZeroResultsAction::ReturnEmpty,
    )
    .await
}

/// Explicit `on_zero_results: use_source` produces the same observable
/// behavior as the default. Pinning this guards against any future
/// change that wraps caching-mode scans with
/// `FallbackOnZeroResultsScanExec` -- such a wrap would cause q3 to
/// hit the source twice (once via the inner cache-miss path, once via
/// the outer fallback).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caching_use_source_falls_back_on_post_filter_zero_results() -> Result<(), anyhow::Error> {
    run_post_filter_scenario(
        "explicit-use_source",
        "post_filter_use_source.duckdb",
        "test_caching_post_filter_use_source",
        ZeroResultsAction::UseSource,
    )
    .await
}
