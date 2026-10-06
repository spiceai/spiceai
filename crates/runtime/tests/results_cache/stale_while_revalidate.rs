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

//! End-to-end coverage of stale-while-revalidate over the HTTP `/v1/sql`
//! surface, for queries that carry bound parameters.
//!
//! These drive the real server — request headers decide the cache key type, so
//! the `Spice-Cache-Key` route below is only reachable over HTTP — and assert on
//! the `Results-Cache-Status` response header a client actually reads.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use app::{App, AppBuilder};
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use serde_json::{Value, json};
use spicepod::component::{caching::SQLResultsCacheConfig, view::View};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// The value bound into `SQL`, and the row it must select.
const BOUND_ID: i64 = 3;
const BOUND_VAL: i64 = 155;

const SQL: &str = "SELECT val FROM swr_values WHERE id = $1";

/// A view rather than a dataset: the query only has to reach the results cache
/// through a `TableScan`, and a view needs no connector or credentials.
fn swr_values_view() -> View {
    let mut view = View::new("swr_values".to_string());
    view.sql =
        Some("SELECT * FROM (VALUES (1, 100), (2, 120), (3, 155)) AS t(id, val)".to_string());
    view
}

/// `item_ttl` is short so the clock alone ages the entry into the
/// stale-while-revalidate window, and the window is long so nothing expires out
/// of it while the test polls.
fn swr_app(name: &str, cache_key_type: spicepod::component::caching::CacheKeyType) -> App {
    AppBuilder::new(name)
        .with_sql_cache(SQLResultsCacheConfig {
            enabled: true,
            item_ttl: Some("1s".to_string()),
            stale_while_revalidate_ttl: Some("5m".to_string()),
            cache_key_type,
            ..Default::default()
        })
        .with_view(swr_values_view())
        .build()
}

/// Starts the runtime and its HTTP server, returning the base URL.
async fn start_runtime(app: App) -> (Arc<Runtime>, String) {
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let load_rt = Arc::clone(&rt);
    load_rt.load_components().await;
    runtime_ready_check(&rt).await;

    // Bind to pick free ports, then release them for the server to claim.
    let http_listener =
        std::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0)).expect("bind http port");
    let flight_listener =
        std::net::TcpListener::bind(SocketAddr::new(LOCALHOST, 0)).expect("bind flight port");
    let http_port = http_listener.local_addr().expect("http addr").port();
    let flight_port = flight_listener.local_addr().expect("flight addr").port();
    // Both reservations are held until both ports are known. Releasing the
    // first before binding the second lets the OS hand the same ephemeral port
    // back for it, and `start_servers` then cannot bind both endpoints -- which
    // surfaces only as this test timing out. tests/metrics.rs does the same.
    drop(http_listener);
    drop(flight_listener);

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

    let server_rt = Arc::clone(&rt);
    tokio::spawn(async move {
        let _ = server_rt
            .start_servers(api_config, None, EndpointAuth::no_auth())
            .await;
    });

    let base = format!("http://{LOCALHOST}:{http_port}");
    let health = format!("{base}/health");
    let client = reqwest::Client::new();
    let serving = wait_until_true(Duration::from_secs(30), || {
        let client = client.clone();
        let health = health.clone();
        async move {
            client
                .get(&health)
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
        }
    })
    .await;
    assert!(serving, "the HTTP server never started serving");

    (rt, base)
}

/// What one `/v1/sql` request reported: the `Results-Cache-Status` header, and
/// the rows.
struct SqlResponse {
    cache_status: String,
    rows: Vec<Value>,
}

impl SqlResponse {
    /// The single `val` the query selected, or a description of what came back
    /// instead. Kept as a `String` so an assertion can print the real shape.
    fn single_val(&self) -> String {
        match self.rows.as_slice() {
            [row] => row
                .get("val")
                .map_or_else(|| format!("{row}"), std::string::ToString::to_string),
            rows => format!("{rows:?}"),
        }
    }
}

async fn post_sql(
    client: &reqwest::Client,
    base: &str,
    client_cache_key: Option<&str>,
) -> SqlResponse {
    let mut request = client
        .post(format!("{base}/v1/sql"))
        .header("Content-Type", "application/json")
        .body(json!({ "sql": SQL, "parameters": [BOUND_ID] }).to_string());
    if let Some(key) = client_cache_key {
        request = request.header("Spice-Cache-Key", key);
    }

    let response = request.send().await.expect("request should be sent");
    let cache_status = response
        .headers()
        .get("Results-Cache-Status")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("ABSENT")
        .to_string();
    let status = response.status();
    let body = response.text().await.expect("response body should read");
    assert!(status.is_success(), "/v1/sql returned {status}: {body}");
    let rows = serde_json::from_str::<Vec<Value>>(&body)
        .unwrap_or_else(|e| panic!("response should be a JSON array of rows, got {body}: {e}"));

    SqlResponse { cache_status, rows }
}

/// Drives one stale-while-revalidate cycle and asserts the revalidation lands.
///
/// A revalidation that cannot be reconstructed fails silently — the stale entry
/// is simply served again — so what proves it landed is the status returning to
/// `HIT`: only a stored result makes the entry fresh again.
async fn assert_revalidation_lands(base: &str, client_cache_key: Option<&str>) {
    let client = reqwest::Client::new();

    let populated = post_sql(&client, base, client_cache_key).await;
    assert_eq!(
        populated.cache_status, "MISS",
        "the first request populates the cache"
    );
    assert_eq!(
        populated.single_val(),
        BOUND_VAL.to_string(),
        "binding {BOUND_ID} must select its own row"
    );

    // Age the entry past `item_ttl` into the stale-while-revalidate window. The
    // sleep is the behavior under test (TTL expiry), not a readiness wait.
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    let stale = post_sql(&client, base, client_cache_key).await;
    assert_eq!(
        stale.cache_status, "STALE",
        "an entry past item_ttl but inside the window is served stale"
    );
    assert_eq!(stale.single_val(), BOUND_VAL.to_string());

    // Poll for the background revalidation rather than sleeping a fixed
    // interval: a stored result makes the entry fresh again, so the status
    // returns to HIT once it lands.
    let mut last_val = stale.single_val();
    let mut last = stale.cache_status;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let response = post_sql(&client, base, client_cache_key).await;
        last_val = response.single_val();
        last = response.cache_status;
        if last == "HIT" {
            break;
        }
    }

    assert_eq!(
        last, "HIT",
        "the background revalidation never replaced the stale entry, so `{SQL}` is served from a \
         result older than item_ttl for the whole stale_while_revalidate_ttl window"
    );
    assert_eq!(
        last_val,
        BOUND_VAL.to_string(),
        "the revalidated entry must hold the row the request's own bound value selects"
    );
}

/// A stale-while-revalidate revalidation must re-bind the parameter values of
/// the query it is replacing.
///
/// Under `cache_key_type: sql` the stale hit is found on the raw-SQL key, before
/// any `LogicalPlan` exists, so the revalidation rebuilds the query from the SQL
/// text. That text still holds its placeholders: rebuilt without the values it
/// fails with `Placeholder '$1' was not provided a value for execution`, the
/// entry is never replaced, and every later request inside the window is both
/// served a stale result and charged another failing background query.
#[tokio::test]
async fn swr_revalidation_of_a_parameterized_query_rebinds_its_values() {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let (_rt, base) = start_runtime(swr_app(
                "swr_param_sql_key",
                spicepod::component::caching::CacheKeyType::Sql,
            ))
            .await;

            assert_revalidation_lands(&base, None).await;
        })
        .await;
}

/// The same defect on the default `cache_key_type: plan`, reached by a client
/// sending `Spice-Cache-Key`.
///
/// The client-supplied key is matched before a plan is built, so — exactly as
/// with raw-SQL keying — the revalidation has only the SQL text to rebuild from.
/// The default configuration is not by itself a defence.
#[tokio::test]
async fn swr_revalidation_of_a_client_keyed_parameterized_query_rebinds_its_values() {
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let (_rt, base) = start_runtime(swr_app(
                "swr_param_client_key",
                spicepod::component::caching::CacheKeyType::Plan,
            ))
            .await;

            assert_revalidation_lands(&base, Some("swr-param-client-key")).await;
        })
        .await;
}
