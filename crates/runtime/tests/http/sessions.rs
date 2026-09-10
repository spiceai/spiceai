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

//! SQL sessions over the wired HTTP and Flight endpoints.
//!
//! These have to be integration tests. What they cover — which context a request
//! lands in, and whether the ownership check runs on the path that actually
//! serves `EXECUTE` — is decided by middleware, the auth layer and the query
//! path acting together, and every one of those is stubbed out in a unit test.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use arrow::array::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt as _;
use rand::RngExt as _;
use reqwest::{Client, StatusCode};
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use runtime_auth::{FlightBasicAuth, HttpAuth, api_key::ApiKeyAuth};
use serde_json::Value;
use spicepod::component::caching::SQLResultsCacheConfig;
use spicepod::component::runtime::ApiKey;
use tonic::transport::Channel;

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, runtime_ready_check, test_request_context, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// Both endpoints of one runtime, so a session issued by either can be exercised
/// against the other.
struct TestRuntime {
    http_url: String,
    flight_url: String,
}

/// Starts a runtime whose HTTP and Flight endpoints accept `keys`, each of which
/// must carry its `:rw` suffix — `PREPARE` is refused outright to a read-only
/// principal, so a read-only key would make every assertion here pass for the
/// wrong reason.
async fn start(keys: &[&str]) -> Result<TestRuntime, anyhow::Error> {
    start_with_sql_cache(keys, None).await
}

/// [`start`], optionally with a SQL results cache configured.
async fn start_with_sql_cache(
    keys: &[&str],
    sql_cache: Option<SQLResultsCacheConfig>,
) -> Result<TestRuntime, anyhow::Error> {
    register_test_connectors().await;
    configure_test_datafusion();

    let mut rng = rand::rng();
    let http_port: u16 = rng.random_range(50000..60000);
    let flight_port: u16 = http_port + 1;

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

    let mut app_builder = app::AppBuilder::new("sessions_test");
    if let Some(sql_cache) = sql_cache {
        app_builder = app_builder.with_sql_cache(sql_cache);
    }

    let rt = Arc::new(
        Runtime::builder()
            .with_app(app_builder.build())
            .build()
            .await,
    );
    Arc::clone(&rt).load_components().await;
    runtime_ready_check(&rt).await;

    let auth = Arc::new(ApiKeyAuth::new(
        keys.iter().map(|key| ApiKey::parse_str(key)).collect(),
    ));
    let endpoint_auth = EndpointAuth::default()
        .with_http_auth(Arc::clone(&auth) as Arc<dyn HttpAuth + Send + Sync>)
        .with_flight_basic_auth(auth as Arc<dyn FlightBasicAuth + Send + Sync>);

    tokio::spawn(async move { Box::pin(rt.start_servers(api_config, None, endpoint_auth)).await });

    let http_url = format!("http://{LOCALHOST}:{http_port}");
    let health = format!("{http_url}/health");
    let client = Client::new();
    let started = wait_until_true(Duration::from_secs(30), || {
        let client = client.clone();
        let health = health.clone();
        async move {
            client
                .get(&health)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
        }
    })
    .await;
    anyhow::ensure!(started, "timed out waiting for the test runtime to start");

    Ok(TestRuntime {
        http_url,
        flight_url: format!("http://{LOCALHOST}:{flight_port}"),
    })
}

impl TestRuntime {
    /// Posts `sql` to `/v1/sql`, in `session` when one is given.
    async fn sql(
        &self,
        key: &str,
        session: Option<&str>,
        sql: &str,
    ) -> Result<(StatusCode, String), anyhow::Error> {
        let mut request = Client::new()
            .post(format!("{}/v1/sql", self.http_url))
            .bearer_auth(key)
            .body(sql.to_string());
        if let Some(session) = session {
            request = request.header("x-session-id", session);
        }

        let response = request.send().await?;
        let status = response.status();
        Ok((status, response.text().await?))
    }

    /// Posts `sql` and requires it to succeed, returning the `X-Cache` header
    /// the runtime answered with (`None` when the results cache did not apply).
    async fn sql_cache_header(
        &self,
        key: &str,
        session: Option<&str>,
        sql: &str,
    ) -> Result<Option<String>, anyhow::Error> {
        let mut request = Client::new()
            .post(format!("{}/v1/sql", self.http_url))
            .bearer_auth(key)
            .body(sql.to_string());
        if let Some(session) = session {
            request = request.header("x-session-id", session);
        }

        let response = request.send().await?;
        let status = response.status();
        let cache = response
            .headers()
            .get("x-cache")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        // Drain the body: the cache is written as the result stream is
        // consumed, so an entry only exists once the client has read the rows.
        let body = response.text().await?;
        anyhow::ensure!(status.is_success(), "`{sql}` failed with {status}: {body}");
        Ok(cache)
    }

    /// Posts `sql` and requires it to succeed, returning the response body.
    async fn sql_ok(
        &self,
        key: &str,
        session: Option<&str>,
        sql: &str,
    ) -> Result<String, anyhow::Error> {
        let (status, body) = self.sql(key, session, sql).await?;
        anyhow::ensure!(status.is_success(), "`{sql}` failed with {status}: {body}");
        Ok(body)
    }

    /// Performs a Flight SQL handshake and returns the session id it issues —
    /// the one way to get an explicit session id, now that HTTP mints none.
    async fn handshake_session(&self, key: &str) -> Result<String, anyhow::Error> {
        let channel = Channel::from_shared(self.flight_url.clone())?
            .connect()
            .await?;
        let mut client = FlightSqlServiceClient::new(channel);
        let token = client.handshake("", key).await?;
        Ok(String::from_utf8(token.to_vec())?)
    }

    /// Runs `sql` over Flight SQL in `session`, returning the rows.
    async fn flight_sql(
        &self,
        key: &str,
        session: &str,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, anyhow::Error> {
        let channel = Channel::from_shared(self.flight_url.clone())?
            .connect()
            .await?;
        let mut client = FlightSqlServiceClient::new(channel);
        client.set_header("authorization", format!("Bearer {key}"));
        client.set_header("x-session-id", session.to_string());

        let info = client.execute(sql.to_string(), None).await?;
        let ticket = info
            .endpoint
            .first()
            .and_then(|endpoint| endpoint.ticket.as_ref())
            .ok_or_else(|| anyhow::anyhow!("no ticket in the FlightInfo for `{sql}`"))?
            .clone();

        Ok(client.do_get(ticket).await?.try_collect().await?)
    }
}

/// The feature: a statement prepared in one request is executable by the next,
/// with no session to ask for and no header to send. The caller's principal is
/// what ties the requests together.
#[tokio::test]
async fn prepared_statements_span_requests() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let rt = start(&["k:rw"]).await?;

            rt.sql_ok("k", None, "PREPARE p AS SELECT 1 + 10 AS result")
                .await?;

            let body = rt.sql_ok("k", None, "EXECUTE p").await?;
            assert_eq!(
                serde_json::from_str::<Value>(&body)?,
                serde_json::json!([{ "result": 11 }]),
                "a later request executes the statement the earlier one prepared"
            );

            rt.sql_ok("k", None, "DEALLOCATE p").await?;

            let (status, body) = rt.sql("k", None, "EXECUTE p").await?;
            assert!(
                !status.is_success() && body.contains("'p' does not exist"),
                "a deallocated statement is gone: {status} {body}"
            );

            Ok(())
        })
        .await
}

/// The session is keyed on the principal, so two API keys never see each
/// other's statements even though neither asked for a session.
#[tokio::test]
async fn two_principals_do_not_share_prepared_statements() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let rt = start(&["a:rw", "b:rw"]).await?;

            rt.sql_ok("a", None, "PREPARE mine AS SELECT 'a data' AS v")
                .await?;

            let (status, body) = rt.sql("b", None, "EXECUTE mine").await?;
            assert!(
                !status.is_success() && body.contains("'mine' does not exist"),
                "another principal must not reach it: {status} {body}"
            );

            let body = rt.sql_ok("a", None, "EXECUTE mine").await?;
            assert_eq!(
                serde_json::from_str::<Value>(&body)?,
                serde_json::json!([{ "v": "a data" }])
            );

            Ok(())
        })
        .await
}

/// A stale or unrelated `x-session-id` must not fail the request. The header
/// name is one other systems use, and on HTTP the runtime never issues an id,
/// so a value it does not recognise says nothing about what the caller wants —
/// the request runs in the caller's own session.
#[tokio::test]
async fn an_unknown_session_id_does_not_fail_the_request() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let rt = start(&["k:rw"]).await?;
            let stale = "00000000-0000-4000-8000-000000000000";

            let body = rt.sql_ok("k", Some(stale), "SELECT 1 AS n").await?;
            assert_eq!(
                serde_json::from_str::<Value>(&body)?,
                serde_json::json!([{ "n": 1 }]),
                "an ordinary query is unaffected by a session id the runtime does not hold"
            );

            // And it is the caller's own session, so statements still carry over.
            rt.sql_ok("k", Some(stale), "PREPARE p AS SELECT 2 AS n")
                .await?;
            let body = rt.sql_ok("k", None, "EXECUTE p").await?;
            assert_eq!(
                serde_json::from_str::<Value>(&body)?,
                serde_json::json!([{ "n": 2 }])
            );

            Ok(())
        })
        .await
}

/// Regression: a session id that reaches another principal must not carry it
/// into the owner's prepared statements — reading them, or destroying them with
/// `DEALLOCATE`.
///
/// `EXECUTE` and `DEALLOCATE` are the cases that matter and the ones that used
/// to slip through: they are `LogicalPlan::Statement` plans, which took a
/// separate path to the session context from ordinary queries.
#[tokio::test]
async fn a_session_cannot_be_used_by_another_principal() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let rt = start(&["a:rw", "b:rw"]).await?;
            let session = rt.handshake_session("a").await?;

            rt.sql_ok(
                "a",
                Some(&session),
                "PREPARE victim AS SELECT 'a data' AS v",
            )
            .await?;

            for sql in ["EXECUTE victim", "DEALLOCATE victim", "SELECT 1"] {
                let (status, body) = rt.sql("b", Some(&session), sql).await?;
                assert_eq!(
                    status,
                    StatusCode::FORBIDDEN,
                    "`{sql}` from another principal must be refused, got {status}: {body}"
                );
            }

            let body = rt.sql_ok("a", Some(&session), "EXECUTE victim").await?;
            assert_eq!(
                serde_json::from_str::<Value>(&body)?,
                serde_json::json!([{ "v": "a data" }]),
                "the owner's statement survived every attempt"
            );

            Ok(())
        })
        .await
}

/// Regression: two principals naming the same id must not share a context.
/// They are authenticated, so each gets the session its own principal owns and
/// the id they chose is ignored.
#[tokio::test]
async fn two_principals_naming_the_same_id_do_not_share_a_session() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let rt = start(&["a:rw", "b:rw"]).await?;
            const GUESSABLE: &str = "shared-guessable-id";

            rt.sql_ok(
                "a",
                Some(GUESSABLE),
                "PREPARE squat AS SELECT 'a private' AS v",
            )
            .await?;

            let (status, body) = rt.sql("b", Some(GUESSABLE), "EXECUTE squat").await?;
            assert!(
                !status.is_success() && body.contains("'squat' does not exist"),
                "the second principal must not reach the first's statement: {status} {body}"
            );

            Ok(())
        })
        .await
}

/// One session, both protocols: a session created over HTTP carries its prepared
/// statements to Flight SQL and back, and its id authenticates on both.
#[tokio::test]
async fn a_session_is_shared_between_the_http_and_flight_endpoints() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let rt = start(&["k:rw"]).await?;
            let session = rt.handshake_session("k").await?;

            rt.sql_ok("k", Some(&session), "PREPARE shared AS SELECT 7 AS n")
                .await?;

            // Prepared over HTTP, executed over Flight.
            let rows = rt.flight_sql("k", &session, "EXECUTE shared").await?;
            let total: usize = rows.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(total, 1, "Flight sees the statement HTTP prepared");

            // Prepared over Flight, executed over HTTP.
            rt.flight_sql("k", &session, "PREPARE from_flight AS SELECT 8 AS n")
                .await?;
            let body = rt
                .sql_ok("k", Some(&session), "EXECUTE from_flight")
                .await?;
            assert_eq!(
                serde_json::from_str::<Value>(&body)?,
                serde_json::json!([{ "n": 8 }]),
                "HTTP sees the statement Flight prepared"
            );

            // The session id is a credential on the HTTP endpoint too, so a
            // client holding one needs no second token.
            let body = rt
                .sql_ok(&session, Some(&session), "EXECUTE shared")
                .await?;
            assert_eq!(
                serde_json::from_str::<Value>(&body)?,
                serde_json::json!([{ "n": 7 }])
            );

            Ok(())
        })
        .await
}
/// A prepared statement's results are cached like any other query's, and per
/// argument list.
///
/// This has to be an integration test: it is the wired HTTP path that carries
/// the session header, resolves the session, and reports the cache outcome in
/// `X-Cache`, and `EXECUTE` used to be exempt from the results cache entirely —
/// its plan is a `LogicalPlan::Statement`, which the cache refuses.
#[tokio::test]
async fn a_prepared_statement_is_cached_per_argument_list() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let rt = start_with_sql_cache(
                &["k:rw"],
                Some(SQLResultsCacheConfig {
                    enabled: true,
                    // Long enough that nothing expires mid-test, so a miss can
                    // only mean the entry was never stored.
                    item_ttl: Some("10m".to_string()),
                    ..Default::default()
                }),
            )
            .await?;
            let session = rt.handshake_session("k").await?;

            rt.sql_ok("k", Some(&session), "PREPARE p(BIGINT) AS SELECT $1 AS n")
                .await?;

            assert_eq!(
                rt.sql_cache_header("k", Some(&session), "EXECUTE p(1)")
                    .await?
                    .as_deref(),
                Some("Miss from spiceai"),
                "the first EXECUTE p(1) has nothing to serve; `None` here means \
                the results cache never applied to it at all"
            );
            assert_eq!(
                rt.sql_cache_header("k", Some(&session), "EXECUTE p(1)")
                    .await?
                    .as_deref(),
                Some("Hit from spiceai"),
                "repeating EXECUTE p(1) is served from the cache"
            );

            // A different argument list is a different query, so it must be a
            // different entry rather than a hit on p(1)'s rows.
            assert_eq!(
                rt.sql_cache_header("k", Some(&session), "EXECUTE p(2)")
                    .await?
                    .as_deref(),
                Some("Miss from spiceai"),
                "EXECUTE p(2) is keyed separately from EXECUTE p(1)"
            );
            let body = rt.sql_ok("k", Some(&session), "EXECUTE p(2)").await?;
            assert_eq!(
                serde_json::from_str::<Value>(&body)?,
                serde_json::json!([{ "n": 2 }]),
                "the cached EXECUTE p(2) entry must hold its own rows, not p(1)'s"
            );

            Ok(())
        })
        .await
}
