/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#![allow(clippy::expect_used)]

//! Regression coverage for #14881: in caching mode a lookup that sends no
//! query must not be served the response another lookup cached for a query on
//! the same path.
//!
//! The origin echoes its request line, so a cached answer names the request
//! that produced it. A lookup filtered on `request_path` alone sends no query
//! and must see the origin's answer for the queryless request, never the `q=a`
//! response, in both warm orders.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use app::AppBuilder;
use arrow::array::{Array, RecordBatch, StringArray};
use axum::Router;
use axum::extract::RawQuery;
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

/// Serve `/items`, answering `GET <path>` plus the query it received, and
/// count every request.
async fn start_origin() -> (oneshot::Sender<()>, SocketAddr, Arc<AtomicUsize>) {
    let counts = Arc::new(AtomicUsize::new(0));
    let request_counts = Arc::clone(&counts);
    let (tx, rx) = oneshot::channel::<()>();

    let app = Router::new().route(
        "/items",
        axum::routing::get(move |RawQuery(query): RawQuery| {
            request_counts.fetch_add(1, Ordering::SeqCst);
            async move {
                let line = match query {
                    Some(query) => format!("GET /items?{query}"),
                    None => "GET /items".to_string(),
                };
                ([("content-type", "text/plain")], line)
            }
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
            .with_user_agent(UserAgent::from_ua_str("spiceci/caching-request-query-test"))
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

const QUERY_LOOKUP: &str = "WHERE request_path = '/items' AND request_query = 'q=a'";
const QUERYLESS_LOOKUP: &str = "WHERE request_path = '/items'";

async fn start_runtime(base_url: &str, app_name: &str) -> Result<Runtime, anyhow::Error> {
    let mut app = AppBuilder::new(app_name)
        .with_dataset(http_dataset(base_url, "cached", true))
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

/// A query warms the cache; a queryless lookup on the same path must still
/// reach the origin and be answered with the queryless response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_caching_mode_queryless_lookup_after_query_lookup() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug"));
    register_test_connectors().await;
    let (shutdown, addr, counts) = start_origin().await;
    let admin = admin_request_context();

    let result = async {
        let rt = start_runtime(&format!("http://{addr}"), "caching_query_then_queryless").await?;
        let warm0 = counts.load(Ordering::SeqCst);

        let warm = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached {QUERY_LOOKUP}"),
        )
        .await;
        assert_eq!(contents(&warm), vec!["GET /items?q=a"]);
        assert_eq!(
            counts.load(Ordering::SeqCst),
            warm0 + 1,
            "the warm-up is one origin request"
        );
        wait_for_cached_rows(&rt, &admin, "cached", 1).await;

        let direct = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM direct {QUERYLESS_LOOKUP}"),
        )
        .await;
        assert_eq!(contents(&direct), vec!["GET /items"]);
        assert_eq!(counts.load(Ordering::SeqCst), warm0 + 2);

        let cached = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached {QUERYLESS_LOOKUP}"),
        )
        .await;
        assert_eq!(
            contents(&cached),
            contents(&direct),
            "the queryless cached lookup must match the unaccelerated queryless request"
        );
        assert_eq!(
            counts.load(Ordering::SeqCst),
            warm0 + 3,
            "the queryless lookup must reach the origin"
        );

        // The query entry is untouched and still served from the cache.
        let again = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached {QUERY_LOOKUP}"),
        )
        .await;
        assert_eq!(contents(&again), vec!["GET /items?q=a"]);
        assert_eq!(
            counts.load(Ordering::SeqCst),
            warm0 + 3,
            "the query lookup is a cache hit"
        );
        Ok(())
    }
    .await;
    shutdown.send(()).ok();
    result
}

/// A queryless lookup comes first; a later lookup for a query on the same path
/// must not be answered with the queryless response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_caching_mode_query_lookup_after_queryless_lookup() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug"));
    register_test_connectors().await;
    let (shutdown, addr, counts) = start_origin().await;
    let admin = admin_request_context();

    let result = async {
        let rt = start_runtime(&format!("http://{addr}"), "caching_queryless_then_query").await?;
        let warm0 = counts.load(Ordering::SeqCst);

        let queryless = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached {QUERYLESS_LOOKUP}"),
        )
        .await;
        assert_eq!(contents(&queryless), vec!["GET /items"]);
        assert_eq!(counts.load(Ordering::SeqCst), warm0 + 1);
        wait_for_cached_rows(&rt, &admin, "cached", 1).await;

        let direct = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM direct {QUERY_LOOKUP}"),
        )
        .await;
        assert_eq!(contents(&direct), vec!["GET /items?q=a"]);
        assert_eq!(counts.load(Ordering::SeqCst), warm0 + 2);

        let cached = run_sql(
            &rt,
            &admin,
            &format!("SELECT content FROM cached {QUERY_LOOKUP}"),
        )
        .await;
        assert_eq!(
            contents(&cached),
            contents(&direct),
            "the query cached lookup must match the unaccelerated query request"
        );
        assert_eq!(
            counts.load(Ordering::SeqCst),
            warm0 + 3,
            "the query lookup must reach the origin"
        );
        Ok(())
    }
    .await;
    shutdown.send(()).ok();
    result
}
