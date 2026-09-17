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

//! `caching_stale_if_error` as an RFC 5861 `stale-if-error=N` window, driven
//! through the wired path: Spicepod parsing, the accelerator's stored
//! `_fetched_at`, and the read path's decision when the origin fails.
//!
//! One mock origin answers `/items` with a JSON array until it is taken down,
//! after which every fetch fails at connect. The dataset caches with a short
//! `caching_ttl` and a finite `caching_stale_if_error`, and the same key is read
//! at three points: while fresh (served from cache without asking the origin),
//! past the TTL but inside the window (the fetch fails and the stale rows are
//! served instead), and past the window (the fetch failure reaches the client).
//! The unbounded `enabled` form is pinned alongside as the behavior the window
//! replaces.
//!
//! The origin is taken down for the send-error tests rather than switched to a
//! 5xx because a failing fetch is one failure mode the window is specified
//! against (#14126: the connector's timeouts propagate as errors) and it does
//! not depend on how the connector shapes an error response into rows. A
//! failing origin more commonly reaches the connector as a *successful* fetch
//! whose row carries a 5xx `response_status` instead (the connector accepts
//! the response once its own retries are exhausted) — a distinct code path
//! (`cache::batches_cacheable`) covered separately below, including on a
//! JSON-decomposed schema (`columns:` + `json_object: "*"`), where
//! `response_status` is not declared by the user and has to be forced into the
//! schema for the fallback to see it at all (#14156, #14157).
//!
//! The sleeps are deliberate: the TTL and the window are what is under test,
//! and both are kept to a few seconds.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use app::AppBuilder;
use arrow::array::{Array, StringArray, UInt16Array};
use axum::http::StatusCode;
use axum::{Router, routing::get};
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
    semantic::Column,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check},
};

/// How long an entry is fresh. Short, so the stale point is reached in seconds;
/// the eviction sweep derived from it still floors at a 30s interval, past the
/// end of the test, so nothing is evicted underneath these reads.
const TTL: Duration = Duration::from_secs(2);
/// The finite `stale-if-error` window, measured from the stale point.
const WINDOW: Duration = Duration::from_secs(6);
/// Read points, measured from the moment the entry is observed in the cache,
/// which is at or after its `_fetched_at`. 4s in is at least 2s past the TTL
/// and inside the 6s window with margin either side; 10s in is at least 8s
/// past the TTL and so past the window.
const INSIDE_WINDOW: Duration = Duration::from_secs(4);
const PAST_WINDOW: Duration = Duration::from_secs(10);
/// Rows in the good response — a multi-row entry, so the window is measured
/// over an entry rather than a single row.
const ROWS: usize = 3;

/// A mock origin serving `/items` as a JSON array of [`ROWS`] objects, counting
/// the requests that reach it, until it is taken down or switched to answer
/// with a fixed HTTP status (see [`Origin::set_status`]) — the shape a
/// connector-exhausted-retries failure actually takes, as distinct from a
/// send error from [`Origin::take_down`].
struct Origin {
    addr: SocketAddr,
    fetches: Arc<AtomicUsize>,
    status: Arc<AtomicU16>,
    shutdown: oneshot::Sender<()>,
}

impl Origin {
    async fn start() -> Self {
        let fetches = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&fetches);
        let status = Arc::new(AtomicU16::new(200));
        let status_for_handler = Arc::clone(&status);
        let (tx, rx) = oneshot::channel::<()>();

        let app = Router::new().route(
            "/items",
            get(move |uri: axum::http::Uri| {
                let counter = Arc::clone(&counter);
                let status = Arc::clone(&status_for_handler);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let current_status = status.load(Ordering::SeqCst);
                    if current_status != 200 {
                        let code =
                            StatusCode::from_u16(current_status).expect("a valid HTTP status code");
                        return (
                            code,
                            [("content-type", "text/plain")],
                            format!("origin fault: status {current_status}"),
                        );
                    }
                    let query = uri.query().unwrap_or_default().to_string();
                    let body = (1..=ROWS)
                        .map(|rank| format!(r#"{{"rank":{rank},"query":"{query}"}}"#))
                        .collect::<Vec<_>>()
                        .join(",");
                    (
                        StatusCode::OK,
                        [("content-type", "application/json")],
                        format!("[{body}]"),
                    )
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

        Self {
            addr,
            fetches,
            status,
            shutdown: tx,
        }
    }

    fn fetches(&self) -> usize {
        self.fetches.load(Ordering::SeqCst)
    }

    /// Switches the origin to answer every request with `status` instead of
    /// the good JSON body, without taking the server down — the shape a
    /// failing origin takes once the connector's own retries are exhausted:
    /// a *successful* fetch whose row carries the failing `response_status`
    /// (see `cache::batches_cacheable`), not a send error.
    fn set_status(&self, status: u16) {
        self.status.store(status, Ordering::SeqCst);
    }

    /// Stops the origin and returns once new connections to it are refused, so
    /// every later fetch fails at connect instead of racing the shutdown.
    async fn take_down(self) {
        self.shutdown.send(()).ok();
        let deadline = Instant::now() + Duration::from_secs(10);
        while tokio::net::TcpStream::connect(self.addr).await.is_ok() {
            assert!(
                Instant::now() < deadline,
                "the mock origin kept accepting connections after shutdown"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn caching_dataset(
    origin: &Origin,
    stale_if_error: &str,
    engine: Option<&str>,
    mode: Mode,
    extra_acceleration_params: Vec<(String, String)>,
) -> Dataset {
    let mut dataset = Dataset::new(format!("http://{}", origin.addr), "http_data");
    dataset.params = Some(Params::from_string_map(
        [
            ("file_format", "json"),
            ("allowed_request_paths", "/items"),
            ("request_query_filters", "enabled"),
            // The origin goes away on purpose. The connector's default three
            // retries with backoff would stretch each failing fetch by seconds
            // and blur the window edges this test measures.
            ("max_retries", "0"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect(),
    ));

    let mut params = vec![
        ("caching_ttl".to_string(), format!("{}s", TTL.as_secs())),
        (
            "caching_stale_if_error".to_string(),
            stale_if_error.to_string(),
        ),
    ];
    params.extend(extra_acceleration_params);

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: engine.map(str::to_string),
        mode,
        refresh_mode: Some(RefreshMode::Caching),
        params: Some(Params::from_string_map(params.into_iter().collect())),
        ..Acceleration::default()
    });
    dataset
}

/// Decomposes the origin's `{"rank":N,"query":"..."}` body into named
/// columns via `json_object: "*"`, the shape #14156/#14157 are about: the
/// user never declares `response_status`, so the runtime has to force it
/// into the schema itself for `caching_stale_if_error` to see anything.
fn decompose_into_named_columns(mut dataset: Dataset) -> Dataset {
    let mut extra = Column::new("extra");
    extra
        .metadata
        .insert("json_object".to_string(), serde_json::json!("*"));
    // `request_path` is declared (and filtered on below) purely so the
    // connector knows which of `allowed_request_paths` to fetch, matching
    // the undecomposed tests above — it is not part of what #14157 is about.
    dataset.columns = vec![
        Column::new("request_path"),
        Column::new("rank"),
        Column::new("query"),
        extra,
    ];
    dataset
}

async fn build_runtime(dataset: Dataset, name: &str) -> Arc<Runtime> {
    let mut app = AppBuilder::new(name).with_dataset(dataset).build();
    // The SQL results cache would answer the repeated reads below from its own
    // copy, hiding what the accelerator serves.
    if app.runtime.caching.sql_results.is_none() {
        app.runtime.caching.sql_results =
            Some(spicepod::component::caching::SQLResultsCacheConfig::default());
    }
    if let Some(ref mut sql_cache) = app.runtime.caching.sql_results {
        sql_cache.enabled = false;
    }

    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(2)) => {
            panic!("timed out waiting for datasets to load");
        }
        () = Arc::clone(&rt).load_components() => {}
    }
    runtime_ready_check(&rt).await;
    rt
}

/// One cache lookup for `query`, as a client would issue it: the
/// `response_status` of every served row, all 200 for the cached copy of the
/// good response — or the error, when the origin's failure reaches the client.
async fn fetch_statuses(rt: &Runtime, query: &str) -> Result<Vec<u16>, DataFusionError> {
    let batches = rt
        .datafusion()
        .ctx
        .table("http_data")
        .await?
        .filter(col("request_path").eq(lit("/items")))?
        .filter(col("request_query").eq(lit(query)))?
        .select(vec![col("response_status")])?
        .collect()
        .await?;
    Ok(batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt16Array>()
                .expect("response_status is UInt16")
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect())
}

/// One cache lookup against a [`decompose_into_named_columns`] dataset,
/// filtered on the decomposed `rank` column itself so the cache-key
/// derivation exercised is the decomposed-schema one, not the metadata-only
/// one the undecomposed tests above already cover. Returns the matching
/// rows' `rank` values: empty (not an error) if the row decomposed from the
/// origin's non-JSON error body instead of a good response (see
/// `decompose_json_row`) and so filtered out on `rank`, which is the
/// empty-result shape #14157 observed.
async fn fetch_decomposed_rank(rt: &Runtime, rank: usize) -> Result<Vec<String>, DataFusionError> {
    let batches = rt
        .datafusion()
        .ctx
        .table("http_data")
        .await?
        .filter(col("request_path").eq(lit("/items")))?
        .filter(col("rank").eq(lit(rank.to_string())))?
        .select(vec![col("rank")])?
        .collect()
        .await?;
    Ok(batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("rank is Utf8 (undeclared type defaults to Utf8)")
                .iter()
                .flatten()
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect())
}

/// Rows the accelerator currently holds. Unfiltered on purpose: a query carrying
/// request filters is a cache lookup and could trigger a fetch.
async fn cached_rows(rt: &Runtime) -> usize {
    let batches = rt
        .datafusion()
        .ctx
        .table("http_data")
        .await
        .expect("table")
        .collect()
        .await
        .expect("collect");
    batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum()
}

/// Polls `cached_rows` until it reaches `want`, returning the instant it did —
/// the reference the read points below are measured from.
async fn wait_for_cached_rows(
    rt: &Runtime,
    want: usize,
    timeout: Duration,
) -> Result<Instant, String> {
    let deadline = Instant::now() + timeout;
    let mut last = cached_rows(rt).await;
    while last != want && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        last = cached_rows(rt).await;
    }
    if last == want {
        return Ok(Instant::now());
    }
    Err(format!(
        "after {}s the acceleration held {last} row(s), expected {want}",
        timeout.as_secs()
    ))
}

/// Fills the cache from a healthy origin, takes the origin down, and reads the
/// same key inside and past the `stale-if-error` window. Returns what those two
/// reads served: the inside read must be the stale rows for every form under
/// test, so it is unwrapped here; the past read is what differs, so it is
/// returned as is.
async fn serve_inside_and_past_window(
    name: &str,
    stale_if_error: &str,
    engine: Option<&str>,
    mode: Mode,
    extra_acceleration_params: Vec<(String, String)>,
) -> Result<(Vec<u16>, Result<Vec<u16>, DataFusionError>), anyhow::Error> {
    let origin = Origin::start().await;
    let dataset = caching_dataset(
        &origin,
        stale_if_error,
        engine,
        mode,
        extra_acceleration_params,
    );
    let rt = build_runtime(dataset, name).await;

    // Fill: a miss fetches the good response and caches it.
    let first = fetch_statuses(&rt, "key=a").await?;
    assert_eq!(
        first,
        vec![200; ROWS],
        "the first read is the origin's good response"
    );
    let cached_at = wait_for_cached_rows(&rt, ROWS, Duration::from_secs(30))
        .await
        .map_err(|e| {
            anyhow::anyhow!("the response was never cached, so no stale read was possible: {e}")
        })?;
    let fetches_after_fill = origin.fetches();

    // Fresh: served from cache without asking the origin.
    assert_eq!(fetch_statuses(&rt, "key=a").await?, vec![200; ROWS]);
    assert_eq!(
        origin.fetches(),
        fetches_after_fill,
        "a fresh entry is served without a fetch"
    );

    origin.take_down().await;

    tokio::time::sleep_until(tokio::time::Instant::from_std(cached_at + INSIDE_WINDOW)).await;
    let inside = fetch_statuses(&rt, "key=a")
        .await
        .map_err(|e| anyhow::anyhow!("inside the window the read must not fail: {e}"))?;

    tokio::time::sleep_until(tokio::time::Instant::from_std(cached_at + PAST_WINDOW)).await;
    let past = fetch_statuses(&rt, "key=a").await;

    Ok((inside, past))
}

/// A finite `caching_stale_if_error: 6s` is `stale-if-error=6`: past the TTL the
/// stale rows stand in for an unreachable origin, and past the window the fetch
/// failure reaches the client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_finite_window_serves_stale_inside_it_and_propagates_the_failure_past_it()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    let (inside, past) = serve_inside_and_past_window(
        "caching_stale_if_error_finite",
        &format!("{}s", WINDOW.as_secs()),
        None,
        Mode::Memory,
        vec![],
    )
    .await?;

    assert_eq!(
        inside,
        vec![200; ROWS],
        "inside the window the stale rows are served in place of the fetch failure"
    );
    let err = past.expect_err("past the window the fetch failure reaches the client");
    eprintln!("past the window the client sees: {err}");
    Ok(())
}

/// The same window on `DuckDB`, which stores `_fetched_at` at microsecond
/// precision: the read path has to normalize the stored timestamp before it can
/// measure staleness, or a finite window could never prove an entry is inside
/// it and would fail closed on every read.
#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_finite_window_is_measured_from_a_microsecond_fetched_at_on_duckdb()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    let temp = tempfile::tempdir()?;
    let (inside, past) = serve_inside_and_past_window(
        "caching_stale_if_error_finite_duckdb",
        &format!("{}s", WINDOW.as_secs()),
        Some("duckdb"),
        Mode::File,
        vec![(
            "duckdb_file".to_string(),
            temp.path()
                .join("cache.duckdb")
                .to_string_lossy()
                .to_string(),
        )],
    )
    .await?;

    assert_eq!(
        inside,
        vec![200; ROWS],
        "a microsecond `_fetched_at` is read as inside the window"
    );
    let err = past.expect_err("and as past it once it is");
    eprintln!("past the window the client sees: {err}");
    Ok(())
}

/// `enabled` is `stale-if-error=∞`, the unbounded form the finite window
/// replaces: the stale rows are served however far past the TTL they are.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn enabled_serves_stale_with_no_bound() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    let (inside, past) = serve_inside_and_past_window(
        "caching_stale_if_error_enabled",
        "enabled",
        None,
        Mode::Memory,
        vec![],
    )
    .await?;

    assert_eq!(inside, vec![200; ROWS]);
    assert_eq!(
        past.expect("`enabled` never lets the fetch failure through while a copy is held"),
        vec![200; ROWS],
        "`enabled` keeps serving the stale rows where a finite window would have stopped"
    );
    Ok(())
}

/// The dominant failure mode: the origin never goes offline, it just starts
/// answering with a 5xx. The connector accepts that as a *successful* fetch
/// once its own retries are exhausted, so `caching_stale_if_error` has to
/// notice the `response_status` on an `Ok` batch (`cache::batches_cacheable`)
/// rather than only handling a transport `Err` — regression coverage for
/// #14156, where a stale allowlist rejected this exact 8-column schema and
/// left the fallback permanently unreachable for a real HTTP dataset.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_5xx_status_row_is_recognized_as_a_transient_failure_inside_the_window()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    let origin = Origin::start().await;
    let dataset = caching_dataset(
        &origin,
        &format!("{}s", WINDOW.as_secs()),
        None,
        Mode::Memory,
        vec![],
    );
    let rt = build_runtime(dataset, "caching_stale_if_error_5xx_status").await;

    let first = fetch_statuses(&rt, "key=a").await?;
    assert_eq!(
        first,
        vec![200; ROWS],
        "the first read is the origin's good response"
    );
    let cached_at = wait_for_cached_rows(&rt, ROWS, Duration::from_secs(30))
        .await
        .map_err(|e| {
            anyhow::anyhow!("the response was never cached, so no stale read was possible: {e}")
        })?;

    // The origin stays up and reachable — only its status changes. A
    // send-error test would never exercise the `Ok` batch this covers.
    origin.set_status(503);

    tokio::time::sleep_until(tokio::time::Instant::from_std(cached_at + INSIDE_WINDOW)).await;
    let inside = fetch_statuses(&rt, "key=a").await?;
    assert_eq!(
        inside,
        vec![200; ROWS],
        "inside the window the stale rows are served in place of the origin's 503"
    );

    tokio::time::sleep_until(tokio::time::Instant::from_std(cached_at + PAST_WINDOW)).await;
    let past = fetch_statuses(&rt, "key=a").await?;
    // The fault body isn't a JSON array like the good response, so it need not
    // decompose into `ROWS` rows the way the 200 response does — only that
    // every row served carries the origin's 503, not the stale 200 copy.
    assert!(
        !past.is_empty() && past.iter().all(|&status| status == 503),
        "past the window the origin's 503 must reach the client instead of the stale copy, got {past:?}"
    );
    Ok(())
}

/// The same 5xx-as-successful-fetch failure, on a dataset that decomposes the
/// JSON body into named `columns:` — the shape #14157 is about. The user
/// never declares `response_status`, so unless the runtime forces it into the
/// schema (`parse_http_json_nesting`), `cache::batches_cacheable` cannot see
/// it at all and the fallback silently never engages, no matter how #14156 is
/// fixed. Uses `enabled` rather than a finite window to isolate that
/// question from the window-boundary timing the tests above already cover.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_5xx_response_is_recognized_on_a_json_decomposed_dataset() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    let origin = Origin::start().await;
    let dataset = decompose_into_named_columns(caching_dataset(
        &origin,
        "enabled",
        None,
        Mode::Memory,
        vec![],
    ));
    let rt = build_runtime(dataset, "caching_stale_if_error_5xx_decomposed").await;

    let first = fetch_decomposed_rank(&rt, 1).await?;
    assert_eq!(
        first,
        vec!["1"],
        "the first read decomposes the origin's good response into named columns"
    );
    wait_for_cached_rows(&rt, ROWS, Duration::from_secs(30))
        .await
        .map_err(|e| {
            anyhow::anyhow!("the response was never cached, so no stale read was possible: {e}")
        })?;

    origin.set_status(503);
    tokio::time::sleep(TTL + Duration::from_secs(1)).await;

    let stale = fetch_decomposed_rank(&rt, 1).await?;
    assert_eq!(
        stale,
        vec!["1"],
        "the decomposed stale row must still be served, not an empty result: an origin \
        failure the fingerprint cannot see is silently indistinguishable from real data \
        decomposing to NULL, which is what #14157 observed as HTTP 200 with an empty body"
    );
    Ok(())
}
