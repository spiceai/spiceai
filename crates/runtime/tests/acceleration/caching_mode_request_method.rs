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

//! Regression coverage for #14768: in caching mode a GET and an explicit-empty
//! POST to the same path must not share a cache entry.
//!
//! The HTTP connector sends a GET when a query leaves `request_body`
//! unconstrained and a POST when it pins `request_body = ''`, but both
//! responses are stored with `request_body = ''`. The tests drive a real
//! runtime against an in-process origin that answers the two methods
//! differently, in both orders, and compare every cached answer with what the
//! origin returns for that method.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use app::AppBuilder;
use arrow::array::{Array, RecordBatch, StringArray};
use axum::{Router, extract::RawQuery, routing::get};
use futures::TryStreamExt;
use runtime::Runtime;
use runtime_request_context::{Protocol, RequestContext, UserAgent};
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check},
};

/// Origin requests seen so far, by method.
#[derive(Default)]
struct OriginCounts {
    get: AtomicUsize,
    post: AtomicUsize,
}

impl OriginCounts {
    fn snapshot(&self) -> (usize, usize) {
        (
            self.get.load(Ordering::SeqCst),
            self.post.load(Ordering::SeqCst),
        )
    }
}

/// Serve `/items`, answering `get-response` to a GET and `post-response` to a
/// POST, and count each method.
async fn start_origin() -> (oneshot::Sender<()>, SocketAddr, Arc<OriginCounts>) {
    let counts = Arc::new(OriginCounts::default());
    let get_counts = Arc::clone(&counts);
    let post_counts = Arc::clone(&counts);
    let (tx, rx) = oneshot::channel::<()>();

    let app = Router::new().route(
        "/items",
        get(move || {
            get_counts.get.fetch_add(1, Ordering::SeqCst);
            async { ([("content-type", "text/plain")], "get-response") }
        })
        .post(move || {
            post_counts.post.fetch_add(1, Ordering::SeqCst);
            async { ([("content-type", "text/plain")], "post-response") }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind origin listener");
    let addr = listener.local_addr().expect("origin local_addr");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .unwrap_or_default();
    });
    (tx, addr, counts)
}

fn http_dataset(base_url: &str, name: &str, accelerated: bool) -> Dataset {
    let mut dataset = Dataset::new(base_url, name);
    dataset.params = Some(Params::from_string_map(
        [
            ("file_format", "text"),
            ("allowed_request_paths", "/items"),
            ("request_query_filters", "enabled"),
            ("request_body_filters", "enabled"),
            ("max_retries", "0"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect(),
    ));
    if accelerated {
        let mut params = std::collections::HashMap::new();
        params.insert("caching_ttl".to_string(), "1h".to_string());
        dataset.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("arrow".to_string()),
            mode: Mode::Memory,
            refresh_mode: Some(RefreshMode::Caching),
            params: Some(Params::from_string_map(params)),
            ..Acceleration::default()
        });
    }
    dataset
}

fn admin_request_context() -> Arc<RequestContext> {
    Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str(
                "spiceci/caching-request-method-test",
            ))
            .build(),
    )
}

async fn run_sql(rt: &Runtime, ctx: &Arc<RequestContext>, sql: &str) -> Vec<RecordBatch> {
    let rt = rt.clone();
    let sql = sql.to_string();
    Arc::clone(ctx)
        .scope(async move {
            rt.datafusion()
                .query_builder(&sql)
                .build()
                .run()
                .await
                .expect("query run")
                .data
                .try_collect()
                .await
                .expect("collect query results")
        })
        .await
}

/// The `content` column of every returned row, in order.
fn contents(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column_by_name("content")
                .expect("content column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("content is Utf8");
            (0..column.len())
                .map(|i| column.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Poll until `table` holds `rows` cached rows, so a lookup that follows sees
/// the background cache write rather than racing it.
async fn wait_for_cached_rows(rt: &Runtime, ctx: &Arc<RequestContext>, table: &str, rows: i64) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut last = -1;
    while tokio::time::Instant::now() < deadline {
        let batches = run_sql(rt, ctx, &format!("SELECT count(*) AS c FROM {table}")).await;
        last = batches
            .first()
            .and_then(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .map(|a| a.value(0))
            })
            .unwrap_or(-1);
        if last >= rows {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{table} never held {rows} cached row(s); last count was {last}");
}

const GET_LOOKUP: &str = "WHERE request_path = '/items'";
const EMPTY_POST_LOOKUP: &str =
    "WHERE request_path = '/items' AND request_query = '' AND request_body = ''";

async fn start_runtime(base_url: &str, app_name: &str) -> Result<Runtime, anyhow::Error> {
    start_runtime_with(base_url, app_name, None).await
}

/// Start a runtime whose `cached` dataset uses `refresh_sql` when given.
async fn start_runtime_with(
    base_url: &str,
    app_name: &str,
    refresh_sql: Option<&str>,
) -> Result<Runtime, anyhow::Error> {
    let mut cached = http_dataset(base_url, "cached", true);
    if let Some(acceleration) = cached.acceleration.as_mut() {
        acceleration.refresh_sql = refresh_sql.map(str::to_string);
    }
    let mut app = AppBuilder::new(app_name)
        .with_dataset(cached)
        .with_dataset(http_dataset(base_url, "direct", false))
        .build();
    // Measure only the acceleration-layer cache.
    let sql_cache = app
        .runtime
        .caching
        .sql_results
        .get_or_insert_with(spicepod::component::caching::SQLResultsCacheConfig::default);
    sql_cache.enabled = false;

    configure_test_datafusion();
    let rt = Runtime::builder().with_app(app).build().await;
    let load_rt = Arc::new(rt.clone());
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
            return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
        }
        () = load_rt.load_components() => {}
    }
    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// A GET warms the cache; an explicit-empty POST lookup must still reach the
/// origin as a POST and return its response, as the unaccelerated dataset does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_caching_mode_explicit_empty_post_after_get() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug"));
    register_test_connectors().await;
    let (shutdown, addr, counts) = start_origin().await;
    let admin = admin_request_context();

    let result = async {
        let rt = start_runtime(&format!("http://{addr}"), "caching_get_then_post").await?;
        let (get0, post0) = counts.snapshot();

        let warm = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached {GET_LOOKUP}"),
        )
        .await;
        assert_eq!(contents(&warm), vec!["get-response"]);
        assert_eq!(counts.snapshot(), (get0 + 1, post0), "warm-up is one GET");
        wait_for_cached_rows(&rt, &admin, "cached", 1).await;

        let direct = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM direct {EMPTY_POST_LOOKUP}"),
        )
        .await;
        assert_eq!(contents(&direct), vec!["post-response"]);
        assert_eq!(counts.snapshot(), (get0 + 1, post0 + 1));

        let cached = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached {EMPTY_POST_LOOKUP}"),
        )
        .await;
        assert_eq!(
            contents(&cached),
            contents(&direct),
            "the cached explicit-empty POST must match the unaccelerated POST"
        );
        assert_eq!(
            counts.snapshot(),
            (get0 + 1, post0 + 2),
            "the explicit-empty POST must reach the origin as a POST"
        );

        // The GET entry is untouched and still served from the cache.
        let again = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached {GET_LOOKUP}"),
        )
        .await;
        assert_eq!(contents(&again), vec!["get-response"]);
        assert_eq!(
            counts.snapshot(),
            (get0 + 1, post0 + 2),
            "GET is a cache hit"
        );
        Ok(())
    }
    .await;
    shutdown.send(()).ok();
    result
}

/// An explicit-empty POST comes first; a later GET lookup must not be answered
/// with the POST response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_caching_mode_get_after_explicit_empty_post() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug"));
    register_test_connectors().await;
    let (shutdown, addr, counts) = start_origin().await;
    let admin = admin_request_context();

    let result = async {
        let rt = start_runtime(&format!("http://{addr}"), "caching_post_then_get").await?;
        let (get0, post0) = counts.snapshot();

        let post = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached {EMPTY_POST_LOOKUP}"),
        )
        .await;
        assert_eq!(contents(&post), vec!["post-response"]);
        assert_eq!(counts.snapshot(), (get0, post0 + 1));
        // Give a background cache write, if one was queued, time to land
        // before the GET lookup reads the cache.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let get = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached {GET_LOOKUP}"),
        )
        .await;
        assert_eq!(
            contents(&get),
            vec!["get-response"],
            "a GET lookup must not be served the explicit-empty POST response"
        );
        assert_eq!(
            counts.snapshot(),
            (get0 + 1, post0 + 1),
            "the GET reaches the origin"
        );
        Ok(())
    }
    .await;
    shutdown.send(()).ok();
    result
}

/// A POST with a body is cached; a later GET lookup on the same path must not
/// be answered with it, since the unaccelerated dataset sends a GET there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_caching_mode_get_after_post_with_body() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug"));
    register_test_connectors().await;
    let (shutdown, addr, counts) = start_origin().await;
    let admin = admin_request_context();

    let result = async {
        let rt = start_runtime(&format!("http://{addr}"), "caching_post_body_then_get").await?;
        let (get0, post0) = counts.snapshot();

        let post = run_sql(
            &rt,
            &admin,
            "SELECT content FROM cached WHERE request_path = '/items' AND request_body = 'x'",
        )
        .await;
        assert_eq!(contents(&post), vec!["post-response"]);
        assert_eq!(counts.snapshot(), (get0, post0 + 1));
        wait_for_cached_rows(&rt, &admin, "cached", 1).await;

        let get = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached {GET_LOOKUP}"),
        )
        .await;
        assert_eq!(
            contents(&get),
            vec!["get-response"],
            "a GET lookup must not be served a cached POST response"
        );
        assert_eq!(
            counts.snapshot(),
            (get0 + 1, post0 + 1),
            "the GET reaches the origin"
        );

        // The POST entry is still cached and still answers its own lookup.
        let post_again = run_sql(
            &rt,
            &admin,
            "SELECT content FROM cached WHERE request_path = '/items' AND request_body = 'x'",
        )
        .await;
        assert_eq!(contents(&post_again), vec!["post-response"]);
        assert_eq!(
            counts.snapshot(),
            (get0 + 1, post0 + 1),
            "the POST is a cache hit"
        );
        Ok(())
    }
    .await;
    shutdown.send(()).ok();
    result
}

/// A body predicate the connector cannot turn into a body — `<>` here — still
/// sends a GET, so the cached answer must match the unaccelerated one rather
/// than a cached POST that satisfies the predicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_caching_mode_get_with_non_body_predicate_after_post() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug"));
    register_test_connectors().await;
    let (shutdown, addr, counts) = start_origin().await;
    let admin = admin_request_context();

    let result = async {
        let rt = start_runtime(&format!("http://{addr}"), "caching_post_then_not_eq").await?;

        let post = run_sql(
            &rt,
            &admin,
            "SELECT content FROM cached WHERE request_path = '/items' AND request_body = 'x'",
        )
        .await;
        assert_eq!(contents(&post), vec!["post-response"]);
        wait_for_cached_rows(&rt, &admin, "cached", 1).await;
        let (get0, post0) = counts.snapshot();

        let lookup = "WHERE request_path = '/items' AND request_body <> 'z'";
        let direct = run_sql(&rt, &admin, &format!("SELECT content FROM direct {lookup}")).await;
        assert_eq!(
            contents(&direct),
            vec!["get-response"],
            "the predicate sends a GET"
        );
        let cached = run_sql(&rt, &admin, &format!("SELECT content FROM cached {lookup}")).await;
        assert_eq!(
            contents(&cached),
            contents(&direct),
            "the cached lookup must match the unaccelerated GET"
        );
        assert_eq!(
            counts.snapshot(),
            (get0 + 2, post0),
            "both lookups reach the origin as a GET"
        );
        Ok(())
    }
    .await;
    shutdown.send(()).ok();
    result
}

/// The accelerated table exposes the columns `refresh_sql` selects, in its
/// order, which differs from the source's; an explicit-empty POST, served
/// from the source, must still return the columns the query names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_caching_mode_explicit_empty_post_with_reordered_columns() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug"));
    register_test_connectors().await;
    let (shutdown, addr, counts) = start_origin().await;
    let admin = admin_request_context();

    let result = async {
        let rt = start_runtime_with(
            &format!("http://{addr}"),
            "caching_reordered_columns",
            Some("SELECT content, request_path, request_body FROM cached"),
        )
        .await?;
        let (get0, post0) = counts.snapshot();

        let rows = run_sql(
            &rt,
            &admin,
            "SELECT content, request_path FROM cached \
             WHERE request_path = '/items' AND request_body = ''",
        )
        .await;
        assert_eq!(contents(&rows), vec!["post-response"]);
        let paths: Vec<String> = rows
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name("request_path")
                    .expect("request_path column")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("request_path is Utf8");
                (0..column.len())
                    .map(|i| column.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(paths, vec!["/items"]);
        assert_eq!(
            counts.snapshot(),
            (get0, post0 + 1),
            "one POST to the origin"
        );
        Ok(())
    }
    .await;
    shutdown.send(()).ok();
    result
}

/// Serve `/pages` as a token-paginated JSON API: five items, two per page, the
/// next page named by `cursor`. Counts every page request.
async fn start_paginated_origin() -> (oneshot::Sender<()>, SocketAddr, Arc<AtomicUsize>) {
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&requests);
    let (tx, rx) = oneshot::channel::<()>();
    let app = Router::new().route(
        "/pages",
        get(move |RawQuery(query): RawQuery| {
            counter.fetch_add(1, Ordering::SeqCst);
            let page: usize = query
                .as_deref()
                .and_then(|q| q.strip_prefix("cursor="))
                .and_then(|v| v.parse().ok())
                .unwrap_or(1);
            let start = (page - 1) * 2;
            let end = (start + 2).min(5);
            let items: Vec<String> = (start..end)
                .map(|i| format!("{{\"item\":\"item-{i}\"}}"))
                .collect();
            let next = if end < 5 {
                (page + 1).to_string()
            } else {
                "null".to_string()
            };
            let body = format!("{{\"data\":[{}],\"next_cursor\":{next}}}", items.join(","));
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind paginated origin listener");
    let addr = listener.local_addr().expect("paginated origin local_addr");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .unwrap_or_default();
    });
    (tx, addr, requests)
}

fn paginated_dataset(base_url: &str, name: &str, accelerated: bool) -> Dataset {
    let mut dataset = Dataset::new(base_url, name);
    dataset.params = Some(Params::from_string_map(
        [
            ("file_format", "json"),
            ("allowed_request_paths", "/pages"),
            ("max_retries", "0"),
            ("pagination", "enabled"),
            ("pagination_next_pointer", "/next_cursor"),
            ("pagination_token_param", "cursor"),
            ("pagination_data_pointer", "/data"),
            ("pagination_max_pages", "10"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect(),
    ));
    if accelerated {
        dataset.acceleration = http_dataset(base_url, name, true).acceleration;
    }
    dataset
}

/// Every page of a paginated response is cached under the request body it was
/// fetched with (none, for a GET), so a repeated GET lookup is served all of
/// its pages from the cache, matching the unaccelerated dataset.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_caching_mode_paginated_get_serves_every_page() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug"));
    register_test_connectors().await;
    let (shutdown, addr, requests) = start_paginated_origin().await;
    let admin = admin_request_context();

    let result = async {
        let base_url = format!("http://{addr}");
        let mut app = AppBuilder::new("caching_paginated_get")
            .with_dataset(paginated_dataset(&base_url, "cached_pages", true))
            .with_dataset(paginated_dataset(&base_url, "direct_pages", false))
            .build();
        app.runtime
            .caching
            .sql_results
            .get_or_insert_with(spicepod::component::caching::SQLResultsCacheConfig::default)
            .enabled = false;
        configure_test_datafusion();
        let rt = Runtime::builder().with_app(app).build().await;
        let load_rt = Arc::new(rt.clone());
        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
            }
            () = load_rt.load_components() => {}
        }
        runtime_ready_check(&rt).await;

        let lookup = "WHERE request_path = '/pages' ORDER BY content";
        let mut expected: Vec<String> = (0..5)
            .map(|i| format!("{{\"item\":\"item-{i}\"}}"))
            .collect();
        expected.sort();

        let direct = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM direct_pages {lookup}"),
        )
        .await;
        assert_eq!(
            contents(&direct),
            expected,
            "the unaccelerated dataset reads every page"
        );

        let cold = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached_pages {lookup}"),
        )
        .await;
        assert_eq!(contents(&cold), expected, "a cold lookup reads every page");
        wait_for_cached_rows(&rt, &admin, "cached_pages", 5).await;

        let before_warm = requests.load(Ordering::SeqCst);
        let warm = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached_pages {lookup}"),
        )
        .await;
        assert_eq!(
            contents(&warm),
            expected,
            "a warm lookup is served every page"
        );
        assert_eq!(
            requests.load(Ordering::SeqCst),
            before_warm,
            "the warm lookup is a cache hit"
        );
        Ok(())
    }
    .await;
    shutdown.send(()).ok();
    result
}

/// A lookup whose only request predicate names no body — `<>` here, with no
/// path filter — still sends a GET to the dataset's URL, so it must not be
/// answered with a POST response cached for that URL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_caching_mode_body_only_non_body_predicate_after_post() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug"));
    register_test_connectors().await;
    let (shutdown, addr, counts) = start_origin().await;
    let admin = admin_request_context();

    let result = async {
        // The dataset URL is the endpoint itself, so a lookup needs no path.
        let rt = start_runtime(&format!("http://{addr}/items"), "caching_body_only_not_eq").await?;

        let post = run_sql(
            &rt,
            &admin,
            "SELECT content FROM cached WHERE request_body = 'x'",
        )
        .await;
        assert_eq!(contents(&post), vec!["post-response"]);
        wait_for_cached_rows(&rt, &admin, "cached", 1).await;
        let (get0, post0) = counts.snapshot();

        let lookup = "WHERE request_body <> 'z'";
        let direct = run_sql(&rt, &admin, &format!("SELECT content FROM direct {lookup}")).await;
        assert_eq!(
            contents(&direct),
            vec!["get-response"],
            "the predicate sends a GET"
        );
        let cached = run_sql(&rt, &admin, &format!("SELECT content FROM cached {lookup}")).await;
        assert_eq!(
            contents(&cached),
            contents(&direct),
            "the cached lookup must match the unaccelerated GET"
        );
        assert_eq!(
            counts.snapshot(),
            (get0 + 2, post0),
            "both lookups reach the origin as a GET"
        );
        Ok(())
    }
    .await;
    shutdown.send(()).ok();
    result
}

/// A cached GET entry that goes stale is refreshed in the background with the
/// request it was cached for — a GET — not replayed as an explicit-empty POST,
/// so the GET lookup that follows is still served the GET response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_caching_mode_periodic_refresh_replays_a_get_as_a_get() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug"));
    register_test_connectors().await;
    let (shutdown, addr, counts) = start_origin().await;
    let admin = admin_request_context();

    let result = async {
        let base_url = format!("http://{addr}");
        let mut cached = http_dataset(&base_url, "cached", true);
        if let Some(acceleration) = cached.acceleration.as_mut() {
            acceleration.params = Some(Params::from_string_map(
                [("caching_ttl".to_string(), "1s".to_string())]
                    .into_iter()
                    .collect(),
            ));
            acceleration.refresh_check_interval = Some("1s".to_string());
        }
        let mut app = AppBuilder::new("caching_periodic_refresh_get")
            .with_dataset(cached)
            .build();
        app.runtime
            .caching
            .sql_results
            .get_or_insert_with(spicepod::component::caching::SQLResultsCacheConfig::default)
            .enabled = false;
        configure_test_datafusion();
        let rt = Runtime::builder().with_app(app).build().await;
        let load_rt = Arc::new(rt.clone());
        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
                return Err(anyhow::Error::msg("Timed out waiting for datasets to load"));
            }
            () = load_rt.load_components() => {}
        }
        runtime_ready_check(&rt).await;
        let (get0, post0) = counts.snapshot();

        let warm = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached {GET_LOOKUP}"),
        )
        .await;
        assert_eq!(contents(&warm), vec!["get-response"]);
        wait_for_cached_rows(&rt, &admin, "cached", 1).await;

        // Poll until the background refresh has fetched the stale entry again.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let (get, post) = counts.snapshot();
            if get + post > get0 + post0 + 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "no background refresh reached the origin; requests (get, post) = {:?}",
                counts.snapshot()
            );
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        assert_eq!(
            counts.snapshot().1,
            post0,
            "the refresh of a cached GET must not reach the origin as a POST"
        );
        Ok(())
    }
    .await;
    shutdown.send(()).ok();
    result
}
