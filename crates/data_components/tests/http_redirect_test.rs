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

//! Credentials an HTTP dataset attaches must reach only the origin it was configured
//! with. Two servers listen on different ports of `127.0.0.1` — two origins — and are
//! reached through the hostnames `victim.test` and `attacker.test`, because the redirect
//! guard refuses a literal loopback address just as the runtime's does.

use std::{net::SocketAddr, sync::Arc};

use arrow::array::{Array, StringArray};
use data_components::http::{
    auth::{ClientAuthMethod, OAuth2Auth, OAuth2Config, OAuthGrant, TokenHeader},
    provider::{DEFAULT_MAX_BODY_BYTES, HttpTableProvider},
    redirect::RedirectGuard,
};
use datafusion::{error::DataFusionError, prelude::SessionContext};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use secrecy::SecretString;
use url::{Host, Url};
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path},
};

const API_KEY: &str = "api-key-secret";
const ACCESS_TOKEN: &str = "shpat_access_token_secret";
const STATIC_AUTHORIZATION: &str = "Bearer static-authorization-secret";
const ATTACKER_BODY: &str = r#"{"from":"attacker"}"#;
const VICTIM_BODY: &str = r#"{"from":"victim"}"#;

/// Every header the dataset attaches: two `http_headers` values (one under a name
/// `reqwest` already strips on a cross-origin hop, as a control) and the `OAuth2`
/// access token under a configured header name.
const CREDENTIAL_HEADERS: [&str; 3] = ["x-api-key", "authorization", "x-shopify-access-token"];

/// Blocks literal IP hosts, which is what the runtime's SSRF classifier does for the
/// loopback addresses these servers listen on.
fn is_ip_literal(target: &Url) -> bool {
    matches!(target.host(), Some(Host::Ipv4(_) | Host::Ipv6(_)))
}

const GUARD: RedirectGuard = RedirectGuard::new(is_ip_literal);

struct Origins {
    victim: MockServer,
    attacker: MockServer,
}

impl Origins {
    async fn start() -> Self {
        let victim = MockServer::start().await;
        let attacker = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": ACCESS_TOKEN,
                "token_type": "bearer",
                "expires_in": 3600,
            })))
            .mount(&victim)
            .await;
        Mock::given(path("/collect"))
            .respond_with(ResponseTemplate::new(200).set_body_string(ATTACKER_BODY))
            .mount(&attacker)
            .await;
        Self { victim, attacker }
    }

    fn victim_url(&self, path: &str) -> Url {
        Url::parse(&format!("http://victim.test:{}{path}", self.victim.address().port()))
            .expect("victim URL should parse")
    }

    fn attacker_url(&self) -> String {
        format!("http://attacker.test:{}/collect", self.attacker.address().port())
    }

    async fn redirect(&self, from: &str, status: u16, location: &str) {
        Mock::given(path(from))
            .respond_with(ResponseTemplate::new(status).insert_header("Location", location))
            .mount(&self.victim)
            .await;
    }

    /// A provider built the way the runtime builds one for a dataset with
    /// `http_headers` and `OAuth2` auth: one client carrying the guard's policy, and the
    /// same guard on the provider.
    async fn provider(&self, base_url: Url) -> HttpTableProvider {
        let client = reqwest::Client::builder()
            .redirect(GUARD.policy())
            .resolve("victim.test", *self.victim.address())
            .resolve("attacker.test", *self.attacker.address())
            .build()
            .expect("client should build");

        let auth = OAuth2Auth::try_new(
            OAuth2Config {
                token_url: format!("{}/oauth/token", self.victim.uri()),
                client_id: Some("client-id".to_string()),
                client_secret: Some(SecretString::from("client-secret")),
                scopes: None,
                client_auth: ClientAuthMethod::Body,
                header: TokenHeader::new("X-Shopify-Access-Token")
                    .expect("header name should be valid"),
            },
            OAuthGrant::ClientCredentials,
        )
        .await
        .expect("token exchange should succeed");

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static(API_KEY),
        );
        headers.insert(
            reqwest::header::AUTHORIZATION,
            HeaderValue::from_static(STATIC_AUTHORIZATION),
        );

        HttpTableProvider::new(base_url, client, "json".to_string(), false)
            .with_max_retries(0)
            .with_headers(headers)
            .with_auth(Arc::new(auth))
            .with_redirect_guard(GUARD)
    }
}

async fn requests_to(server: &MockServer, request_path: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .expect("request recording is on by default")
        .into_iter()
        .filter(|request| request.url.path() == request_path)
        .collect()
}

fn header<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request
        .headers
        .get(name)
        .map(|value| value.to_str().expect("header should be text"))
}

fn credential_headers(request: &Request) -> Vec<(&'static str, Option<&str>)> {
    CREDENTIAL_HEADERS
        .iter()
        .map(|name| (*name, header(request, name)))
        .collect()
}

fn sent_credentials() -> Vec<(&'static str, Option<&'static str>)> {
    vec![
        ("x-api-key", Some(API_KEY)),
        ("authorization", Some(STATIC_AUTHORIZATION)),
        ("x-shopify-access-token", Some(ACCESS_TOKEN)),
    ]
}

fn no_credentials() -> Vec<(&'static str, Option<&'static str>)> {
    CREDENTIAL_HEADERS.iter().map(|name| (*name, None)).collect()
}

async fn query(provider: HttpTableProvider, sql: &str) -> Result<Vec<String>, DataFusionError> {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider))
        .expect("table should register");
    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(batches
        .iter()
        .flat_map(|batch| {
            let content = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("content should be a string column");
            (0..content.len())
                .map(|row| content.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect())
}

/// The reported attack: the configured origin answers with a 302 to another origin. The
/// redirect is still followed, so the data arrives, but without a single credential.
#[tokio::test]
async fn cross_origin_redirect_reaches_the_target_without_credentials() {
    let origins = Origins::start().await;
    origins.redirect("/data", 302, &origins.attacker_url()).await;

    let rows = query(
        origins.provider(origins.victim_url("/data")).await,
        "SELECT content FROM t",
    )
    .await
    .expect("the redirect should be followed");
    assert_eq!(rows, vec![ATTACKER_BODY.to_string()]);

    let to_victim = requests_to(&origins.victim, "/data").await;
    assert_eq!(to_victim.len(), 1);
    assert_eq!(credential_headers(&to_victim[0]), sent_credentials());

    let to_attacker = requests_to(&origins.attacker, "/collect").await;
    assert_eq!(to_attacker.len(), 1);
    assert_eq!(credential_headers(&to_attacker[0]), no_credentials());
}

/// A redirect within the configured origin keeps every credential, on every hop.
#[tokio::test]
async fn same_origin_redirect_keeps_credentials() {
    let origins = Origins::start().await;
    origins.redirect("/data", 302, "/moved").await;
    origins.redirect("/moved", 301, "/final").await;
    Mock::given(path("/final"))
        .respond_with(ResponseTemplate::new(200).set_body_string(VICTIM_BODY))
        .mount(&origins.victim)
        .await;

    let rows = query(
        origins.provider(origins.victim_url("/data")).await,
        "SELECT content FROM t",
    )
    .await
    .expect("the redirect should be followed");
    assert_eq!(rows, vec![VICTIM_BODY.to_string()]);

    for hop in ["/data", "/moved", "/final"] {
        let requests = requests_to(&origins.victim, hop).await;
        assert_eq!(requests.len(), 1, "{hop}");
        assert_eq!(credential_headers(&requests[0]), sent_credentials(), "{hop}");
    }
    assert!(
        requests_to(&origins.attacker, "/collect").await.is_empty(),
        "nothing should reach the other origin"
    );
}

/// A 307 replays the method and the body. The body is the dataset's request, so a 307
/// to another origin is reported as a redirect that was not followed.
#[tokio::test]
async fn cross_origin_307_does_not_replay_the_request_body() {
    let origins = Origins::start().await;
    origins.redirect("/post", 307, &origins.attacker_url()).await;

    let provider = origins
        .provider(origins.victim_url("/"))
        .await
        .with_allowed_paths(vec!["/post".to_string()])
        .expect("allowed paths should be valid")
        .enable_body_filters(DEFAULT_MAX_BODY_BYTES);
    let error = query(
        provider,
        r#"SELECT content FROM t WHERE request_path = '/post' AND request_body = '{"secret":"body"}'"#,
    )
    .await
    .expect_err("a cross-origin 307 must not be followed");
    assert!(error.to_string().contains("307"), "{error}");

    let to_victim = requests_to(&origins.victim, "/post").await;
    assert_eq!(to_victim.len(), 1);
    assert_eq!(to_victim[0].body, br#"{"secret":"body"}"#);
    assert!(
        origins.attacker.received_requests().await.unwrap_or_default().is_empty(),
        "nothing should reach the other origin"
    );
}

/// A redirect to a blocked target is refused by the client's policy and never requested.
#[tokio::test]
async fn redirect_to_a_blocked_target_is_never_requested() {
    let origins = Origins::start().await;
    let blocked: SocketAddr = *origins.attacker.address();
    origins
        .redirect("/data", 302, &format!("http://{blocked}/collect"))
        .await;

    let error = query(
        origins.provider(origins.victim_url("/data")).await,
        "SELECT content FROM t",
    )
    .await
    .expect_err("a blocked redirect target must fail the request");
    assert!(
        error.to_string().contains("error following redirect"),
        "{error}"
    );
    assert!(
        origins.attacker.received_requests().await.unwrap_or_default().is_empty(),
        "the blocked target should receive nothing"
    );
}

/// The provider checks the guard itself before it re-issues a redirect, rather than
/// relying on the client's policy having checked the target first: here the client
/// follows no redirect at all, so the provider is the only check.
#[tokio::test]
async fn provider_checks_the_guard_before_reissuing_a_redirect() {
    let origins = Origins::start().await;
    let blocked: SocketAddr = *origins.attacker.address();
    origins
        .redirect("/data", 302, &format!("http://{blocked}/collect"))
        .await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .resolve("victim.test", *origins.victim.address())
        .build()
        .expect("client should build");
    let provider = HttpTableProvider::new(
        origins.victim_url("/data"),
        client,
        "json".to_string(),
        false,
    )
    .with_max_retries(0)
    .with_redirect_guard(GUARD);

    let error = query(provider, "SELECT content FROM t")
        .await
        .expect_err("a blocked redirect target must not be followed");
    assert!(error.to_string().contains("302"), "{error}");
    assert!(
        origins.attacker.received_requests().await.unwrap_or_default().is_empty(),
        "the blocked target should receive nothing"
    );
}

/// The token request carries the client secret in its form body, which a 307 replays, so
/// the token client does not follow a redirect off the token endpoint's origin.
#[tokio::test]
async fn token_request_is_not_replayed_to_another_origin() {
    let victim = MockServer::start().await;
    let attacker = MockServer::start().await;
    Mock::given(path("/oauth/token"))
        .respond_with(
            ResponseTemplate::new(307)
                .insert_header("Location", format!("{}/oauth/token", attacker.uri())),
        )
        .mount(&victim)
        .await;

    let error = OAuth2Auth::try_new(
        OAuth2Config {
            token_url: format!("{}/oauth/token", victim.uri()),
            client_id: Some("client-id".to_string()),
            client_secret: Some(SecretString::from("client-secret")),
            scopes: None,
            client_auth: ClientAuthMethod::Body,
            header: TokenHeader::new("X-Shopify-Access-Token").expect("header name should be valid"),
        },
        OAuthGrant::ClientCredentials,
    )
    .await
    .expect_err("a cross-origin redirect from the token endpoint must not be followed");
    assert!(error.to_string().contains("307"), "{error}");
    assert!(
        attacker.received_requests().await.unwrap_or_default().is_empty(),
        "the client secret should not reach the other origin"
    );
}

/// Once a `POST` has been turned into a bare `GET` by a cross-origin 302, there is no
/// body left to replay, so a later cross-origin 307 is followed like any other.
#[tokio::test]
async fn cross_origin_307_after_a_body_was_dropped_is_followed() {
    let origins = Origins::start().await;
    origins.redirect("/post", 302, &origins.attacker_url()).await;
    Mock::given(path("/collect"))
        .respond_with(
            ResponseTemplate::new(307)
                .insert_header("Location", origins.victim_url("/landing").as_str()),
        )
        .with_priority(1)
        .mount(&origins.attacker)
        .await;
    Mock::given(path("/landing"))
        .respond_with(ResponseTemplate::new(200).set_body_string(VICTIM_BODY))
        .mount(&origins.victim)
        .await;

    let provider = origins
        .provider(origins.victim_url("/"))
        .await
        .with_allowed_paths(vec!["/post".to_string()])
        .expect("allowed paths should be valid")
        .enable_body_filters(DEFAULT_MAX_BODY_BYTES);
    let rows = query(
        provider,
        r#"SELECT content FROM t WHERE request_path = '/post' AND request_body = '{"secret":"body"}'"#,
    )
    .await
    .expect("the bare GET's redirect should be followed");
    assert_eq!(rows, vec![VICTIM_BODY.to_string()]);

    let to_attacker = requests_to(&origins.attacker, "/collect").await;
    assert_eq!(to_attacker.len(), 1);
    assert_eq!(to_attacker[0].method.as_str(), "GET");
    assert!(to_attacker[0].body.is_empty());
    assert_eq!(credential_headers(&to_attacker[0]), no_credentials());

    let landing = requests_to(&origins.victim, "/landing").await;
    assert_eq!(landing.len(), 1);
    assert_eq!(landing[0].method.as_str(), "GET");
    assert_eq!(credential_headers(&landing[0]), no_credentials());
}
