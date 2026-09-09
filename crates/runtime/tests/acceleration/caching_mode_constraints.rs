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

//! How a declared key interacts with `refresh_mode: caching`.
//!
//! A cache entry is identified by the request it was fetched for
//! (`request_path`, `request_query`, `request_body`) plus the cache namespace,
//! and one entry can hold many rows: a JSON array body becomes one row per
//! element, each carrying the same request key. A dataset may additionally
//! declare an `acceleration.primary_key`, which is a claim about row uniqueness
//! in storage. The two coincide only when a request returns a single row.
//!
//! That gives three shapes worth pinning, all driven against a local mock:
//!
//! * no key, several rows per response - the default, and the only shape that
//!   needs no agreement between key and payload
//! * a key derived from the response body - unique per row, so several rows per
//!   response still work
//! * a key derived from the request columns - one row per request, which an
//!   array response contradicts
//!
//! Plus the two behaviours a declared key must not change: the eviction sweep
//! still has to enforce a budget, and a refreshed entry still has to shed the
//! rows the response stopped returning.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use app::AppBuilder;
use axum::{Router, routing::get};
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, IndexType, Mode, OnConflictBehavior, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check},
};

/// Serves `/items` as a JSON array of `rows` objects, counting the requests that
/// reach it. `rows` is shared so a test can shrink the response between fetches.
async fn start_mock(rows: usize) -> (oneshot::Sender<()>, SocketAddr, Arc<AtomicUsize>) {
    let (shutdown, addr, fetches, _) = start_resizable_mock(rows).await;
    (shutdown, addr, fetches)
}

/// As [`start_mock`], also handing back the row count so it can be changed.
async fn start_resizable_mock(
    rows: usize,
) -> (
    oneshot::Sender<()>,
    SocketAddr,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let fetches = Arc::new(AtomicUsize::new(0));
    let row_count = Arc::new(AtomicUsize::new(rows));
    let counter = Arc::clone(&fetches);
    let served_rows = Arc::clone(&row_count);
    let (tx, rx) = oneshot::channel::<()>();

    let app = Router::new().route(
        "/items",
        get(move |uri: axum::http::Uri| {
            let counter = Arc::clone(&counter);
            let served_rows = Arc::clone(&served_rows);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                let query = uri.query().unwrap_or_default().to_string();
                let n = served_rows.load(Ordering::SeqCst);
                let body = (1..=n)
                    .map(|rank| format!(r#"{{"rank":{rank},"query":"{query}"}}"#))
                    .collect::<Vec<_>>()
                    .join(",");
                ([("content-type", "application/json")], format!("[{body}]"))
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

    (tx, addr, fetches, row_count)
}

fn caching_dataset(
    addr: SocketAddr,
    engine: &str,
    primary_key: Option<&str>,
    acceleration_params: Vec<(String, String)>,
) -> Dataset {
    caching_dataset_with(addr, engine, primary_key, acceleration_params, false, false)
}

/// As [`caching_dataset`], additionally able to set `on_conflict: upsert` over
/// the declared key and an index on `request_query` -- the combination a caching
/// deployment reaches for to say "replace the row for this request".
fn caching_dataset_with(
    addr: SocketAddr,
    engine: &str,
    primary_key: Option<&str>,
    acceleration_params: Vec<(String, String)>,
    upsert_on_primary_key: bool,
    index_request_query: bool,
) -> Dataset {
    let mut dataset = Dataset::new(format!("http://{addr}"), "http_data");
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "json".to_string()),
            ("allowed_request_paths".to_string(), "/items".to_string()),
            ("request_query_filters".to_string(), "enabled".to_string()),
        ]
        .into_iter()
        .collect(),
    ));

    let mut on_conflict = HashMap::new();
    if upsert_on_primary_key && let Some(key) = primary_key {
        on_conflict.insert(key.to_string(), OnConflictBehavior::Upsert);
    }
    let mut indexes = HashMap::new();
    if index_request_query {
        indexes.insert("request_query".to_string(), IndexType::Enabled);
    }

    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Caching),
        primary_key: primary_key.map(ToString::to_string),
        on_conflict,
        indexes,
        params: Some(Params::from_string_map(
            acceleration_params.into_iter().collect(),
        )),
        ..Acceleration::default()
    });
    dataset
}

async fn build_runtime(dataset: Dataset, name: &str) -> Arc<Runtime> {
    let mut app = AppBuilder::new(name).with_dataset(dataset).build();
    // The SQL results cache would answer the repeated count queries below from
    // its own copy, hiding what the accelerator actually holds.
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

/// Rows the accelerator currently holds. Deliberately unfiltered: a query
/// carrying request filters would be a cache lookup and could trigger a fetch,
/// which is the opposite of what these assertions count.
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

/// Issues one cache lookup for `query`, the way a client would.
async fn fetch_key(rt: &Runtime, query: &str) {
    rt.datafusion()
        .ctx
        .table("http_data")
        .await
        .expect("table")
        .filter(col("request_path").eq(lit("/items")))
        .expect("path filter")
        .filter(col("request_query").eq(lit(query)))
        .expect("query filter")
        .collect()
        .await
        .expect("collect");
}

/// Polls `cached_rows` until it reaches `want`, reporting the last value seen
/// when it does not. The eviction sweep floors at a 30s interval, so this waits
/// for the condition rather than for a fixed duration.
async fn wait_for_cached_rows(rt: &Runtime, want: usize, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last = cached_rows(rt).await;
    while Instant::now() < deadline {
        if last == want {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        last = cached_rows(rt).await;
    }
    if last == want {
        return Ok(());
    }
    Err(format!(
        "after {}s the acceleration held {last} row(s), expected {want}",
        timeout.as_secs()
    ))
}

/// Drives lookups for `query` until the accelerator holds `want` rows.
///
/// A refresh is triggered by a lookup and completes in the background, so the
/// lookup that triggers it returns before the replacement has landed. Polling
/// alone would not do either: past the TTL it takes a lookup to start the
/// revalidation at all.
async fn wait_for_rows_driving_lookups(
    rt: &Runtime,
    query: &str,
    want: usize,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut last = cached_rows(rt).await;
    while Instant::now() < deadline {
        if last == want {
            return Ok(());
        }
        fetch_key(rt, query).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        last = cached_rows(rt).await;
    }
    if last == want {
        return Ok(());
    }
    Err(format!(
        "after {}s the acceleration held {last} row(s), expected {want}",
        timeout.as_secs()
    ))
}

/// Waits until `dataset` reports a component error, which a caching accelerator
/// does once a run of cache writes has been refused.
///
/// This is the completion signal for a refused write. Waiting a fixed time
/// instead would let an assertion that nothing was cached pass simply because no
/// write had been attempted yet.
async fn wait_for_dataset_error(
    rt: &Runtime,
    dataset: &str,
    timeout: Duration,
) -> Result<(), String> {
    let table = TableReference::bare(dataset.to_string());
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(status) = rt.status().get_dataset_status(&table)
            && status.error_message().is_some()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!(
        "dataset '{dataset}' never reported a cache-write failure within {}s",
        timeout.as_secs()
    ))
}

fn duckdb_params(dir: &std::path::Path, extra: Vec<(&str, &str)>) -> Vec<(String, String)> {
    let mut params = vec![(
        "duckdb_file".to_string(),
        dir.join("cache.duckdb").to_string_lossy().to_string(),
    )];
    params.extend(
        extra
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string())),
    );
    params
}

/// Regression test for #13976.
///
/// A caching accelerator with `caching_max_items` must evict down to its budget.
/// With a declared key the sweep's ranking aggregate was planned with a group key
/// widened to the whole stored row -- including the `Map` the HTTP connector
/// stores response headers in -- so it failed on every pass and the acceleration
/// grew without bound.
///
/// Cayenne rather than `DuckDB` because the defect is above both engines and
/// Cayenne had no coverage of the budgets at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// Cayenne is not built on Windows, so neither is a test that loads it.
#[cfg(all(feature = "sqlite", not(target_os = "windows")))]
async fn the_item_budget_is_enforced_on_cayenne_with_a_primary_key() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    let (shutdown, addr, _fetches) = start_mock(1).await;
    let temp = tempfile::tempdir()?;
    let registry = temp.path().join("cayenne").to_string_lossy().to_string();

    let dataset = caching_dataset(
        addr,
        "cayenne",
        Some("(request_query,request_path)"),
        vec![
            ("cayenne_file_path".to_string(), registry),
            ("caching_ttl".to_string(), "30s".to_string()),
            (
                "caching_stale_while_revalidate_ttl".to_string(),
                "1h".to_string(),
            ),
            ("caching_stale_if_error".to_string(), "enabled".to_string()),
            ("caching_max_items".to_string(), "1".to_string()),
        ],
    );
    let rt = build_runtime(dataset, "caching_budget_cayenne_pk").await;

    for key in ["key=a", "key=b", "key=c"] {
        fetch_key(&rt, key).await;
    }

    // Premise: the cache must actually fill before eviction can be observed.
    // Without this the test would pass vacuously on a build that cached nothing.
    wait_for_cached_rows(&rt, 3, Duration::from_secs(30))
        .await
        .map_err(|e| anyhow::anyhow!("cache never filled, so eviction was never exercised: {e}"))?;

    // Two sweeps' worth of headroom over the 30s floor.
    wait_for_cached_rows(&rt, 1, Duration::from_mins(2))
        .await
        .map_err(|e| anyhow::anyhow!("`caching_max_items: 1` was not enforced: {e}"))?;

    shutdown.send(()).ok();
    Ok(())
}

/// The default, unkeyed shape: a response holding several rows is cached whole
/// and served from cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_response_is_cached_without_a_primary_key() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    let (shutdown, addr, fetches) = start_mock(3).await;
    let temp = tempfile::tempdir()?;
    let dataset = caching_dataset(
        addr,
        "duckdb",
        None,
        duckdb_params(temp.path(), vec![("caching_ttl", "1h")]),
    );
    let rt = build_runtime(dataset, "caching_multi_row_unkeyed").await;

    fetch_key(&rt, "key=a").await;
    wait_for_cached_rows(&rt, 3, Duration::from_secs(30))
        .await
        .map_err(|e| {
            anyhow::anyhow!("all three rows should be cached under one request key: {e}")
        })?;

    let before = fetches.load(Ordering::SeqCst);
    fetch_key(&rt, "key=a").await;
    assert_eq!(
        before,
        fetches.load(Ordering::SeqCst),
        "the second query refetched from the origin, so nothing was served from cache"
    );

    shutdown.send(()).ok();
    Ok(())
}

/// A key derived from the response body is unique per row, so a response holding
/// several rows is still cached whole. The cache is still *looked up* by request
/// -- the declared key never takes part in that -- which is what lets a
/// body-derived key coexist with request-keyed refresh.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_row_response_is_cached_with_a_body_derived_primary_key()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    let (shutdown, addr, fetches) = start_mock(3).await;
    let temp = tempfile::tempdir()?;
    let dataset = caching_dataset(
        addr,
        "duckdb",
        // `content` holds the element's JSON, so it is distinct per row.
        Some("(request_path,content)"),
        duckdb_params(temp.path(), vec![("caching_ttl", "1h")]),
    );
    let rt = build_runtime(dataset, "caching_multi_row_body_key").await;

    fetch_key(&rt, "key=a").await;
    wait_for_cached_rows(&rt, 3, Duration::from_secs(30))
        .await
        .map_err(|e| anyhow::anyhow!("a key that is unique per row should cache every row: {e}"))?;

    let before = fetches.load(Ordering::SeqCst);
    fetch_key(&rt, "key=a").await;
    assert_eq!(
        before,
        fetches.load(Ordering::SeqCst),
        "the second query refetched from the origin, so nothing was served from cache"
    );

    shutdown.send(()).ok();
    Ok(())
}

/// A key over the request columns says one row per request. An array response
/// contradicts it, and the write is refused rather than truncated.
///
/// The assertion that matters is the *absence of a partial entry*: `on_conflict`
/// has variants (`upsert_dedup_by_row_id`) that would resolve the conflict by
/// keeping the last row, which would silently discard the rest of a search
/// result. Caching nothing is recoverable and visible in the logs; caching one
/// row of three is neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_derived_primary_key_refuses_a_multi_row_response() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    let (shutdown, addr, fetches) = start_mock(3).await;
    let temp = tempfile::tempdir()?;
    let dataset = caching_dataset(
        addr,
        "duckdb",
        Some("(request_query,request_path)"),
        duckdb_params(temp.path(), vec![("caching_ttl", "1h")]),
    );
    let rt = build_runtime(dataset, "caching_multi_row_request_key").await;

    // A run of refused writes is what makes the dataset report an error, and
    // that report is the signal the writes were attempted at all. Cache writes
    // are batched and flushed on an interval, and the run counts flushes rather
    // than requests, so the lookups are spaced past that interval instead of
    // issued back to back -- four in one flush is one refusal, not four.
    for key in ["key=a", "key=b", "key=c", "key=d"] {
        fetch_key(&rt, key).await;
        tokio::time::sleep(Duration::from_millis(900)).await;
    }
    wait_for_dataset_error(&rt, "http_data", Duration::from_mins(1))
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "the cache writes were never refused, so this test would \
             have asserted an empty accelerator that nothing had tried to fill: {e}"
            )
        })?;

    assert_eq!(
        cached_rows(&rt).await,
        0,
        "a response that violates the declared key must not be partially cached"
    );

    // And the entry is not served: a repeat lookup still reaches the origin.
    let before = fetches.load(Ordering::SeqCst);
    fetch_key(&rt, "key=a").await;
    assert!(
        fetches.load(Ordering::SeqCst) > before,
        "the lookup was served from cache, so something was cached after all"
    );

    shutdown.send(()).ok();
    Ok(())
}

/// A key over the request columns with `on_conflict: upsert` and an index -- the
/// shape a deployment writes to say "replace the row for this request" -- caches
/// a single-row response and serves it from cache.
///
/// The key holds here, because one request returns one row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_primary_key_with_on_conflict_upsert_still_caches() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    let (shutdown, addr, fetches) = start_mock(1).await;
    let temp = tempfile::tempdir()?;
    let dataset = caching_dataset_with(
        addr,
        "duckdb",
        Some("(request_query,request_path)"),
        duckdb_params(temp.path(), vec![("caching_ttl", "1h")]),
        true,
        true,
    );
    let rt = build_runtime(dataset, "caching_pk_on_conflict").await;

    fetch_key(&rt, "key=a").await;
    wait_for_cached_rows(&rt, 1, Duration::from_secs(30))
        .await
        .map_err(|e| anyhow::anyhow!("the response should be cached: {e}"))?;

    let before = fetches.load(Ordering::SeqCst);
    fetch_key(&rt, "key=a").await;
    assert_eq!(
        before,
        fetches.load(Ordering::SeqCst),
        "the second query refetched from the origin, so nothing was served from cache"
    );
    assert_eq!(
        cached_rows(&rt).await,
        1,
        "the entry should still hold exactly one row"
    );

    shutdown.send(()).ok();
    Ok(())
}

/// A refreshed entry holds what the response now returns, not what it used to.
///
/// Replacing an entry deletes the rows under its request key before appending
/// the new ones. A declared key must not change that: resolving the write as a
/// native upsert keyed on the constraint would append and update in place,
/// leaving a row the response no longer returns cached indefinitely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refreshed_entry_sheds_rows_the_response_no_longer_returns() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(None);
    register_test_connectors().await;

    let (shutdown, addr, _fetches, row_count) = start_resizable_mock(3).await;
    let temp = tempfile::tempdir()?;
    let dataset = caching_dataset(
        addr,
        "duckdb",
        Some("(request_path,content)"),
        duckdb_params(
            temp.path(),
            vec![
                ("caching_ttl", "1s"),
                ("caching_stale_while_revalidate_ttl", "1s"),
            ],
        ),
    );
    let rt = build_runtime(dataset, "caching_shed_on_refresh").await;

    fetch_key(&rt, "key=a").await;
    wait_for_cached_rows(&rt, 3, Duration::from_secs(30))
        .await
        .map_err(|e| {
            anyhow::anyhow!("cache never filled, so the refresh was not exercised: {e}")
        })?;

    // The origin now returns one fewer item. Drive lookups past the TTL so a
    // refresh replaces the entry.
    row_count.store(2, Ordering::SeqCst);
    wait_for_rows_driving_lookups(&rt, "key=a", 2, Duration::from_mins(1))
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "the row dropped from the response is still cached, so a stale row is being \
                 served: {e}"
            )
        })?;

    shutdown.send(()).ok();
    Ok(())
}
