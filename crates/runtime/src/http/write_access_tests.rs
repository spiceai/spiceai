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

//! Requests with malformed bodies sent through the runtime's own routes with API key
//! authentication: a caller without write access must be refused before a route reads or
//! parses its body, so the refusal never reveals the route's request schema.

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use http_body_util::BodyExt;
use runtime_auth::{HttpAuth, api_key::ApiKeyAuth, layer::http::AuthLayer};
use runtime_search::search_engine::{SearchEngine, parse_explicit_primary_keys};
use serde_json::Value;
use spicepod::component::runtime::{ApiKey, CorsConfig};
use tower::ServiceExt;

use crate::{Runtime, config::Config};

const READ_ONLY_KEY: &str = "read-only-test-key";
const READ_WRITE_KEY: &str = "read-write-test-key";
const WRITE_REFUSAL: &str = "API key does not allow write access";

/// Every route that needs write access, with whether its handler parses a JSON body.
fn write_routes() -> Vec<(Method, &'static str, bool)> {
    vec![
        (Method::GET, "/v1/sql/active", false),
        (
            Method::POST,
            "/v1/sql/00000000-0000-0000-0000-000000000000/cancel",
            false,
        ),
        (Method::POST, "/v1/datasets/missing/cdc", false),
        (Method::POST, "/v1/metrics", false),
        (Method::POST, "/v1/logs", false),
        (
            Method::POST,
            "/v1/datasets/missing/acceleration/refresh",
            true,
        ),
        (Method::PATCH, "/v1/datasets/missing/acceleration", true),
        (
            Method::POST,
            "/v1/datasets/missing/acceleration/snapshots/current",
            true,
        ),
        (
            Method::PUT,
            "/v1/datasets/missing/acceleration/snapshots/pin",
            true,
        ),
        (
            Method::DELETE,
            "/v1/datasets/missing/acceleration/snapshots/pin",
            false,
        ),
        (Method::POST, "/v1/packages/generate", true),
        (Method::POST, "/v1/queries/missing/cancel", false),
    ]
}

/// A body the JSON extractor rejects for its syntax (400) and one it rejects for its
/// `Content-Type` (415).
const MALFORMED_JSON: (&str, &str) = ("application/json", "{");
const WRONG_CONTENT_TYPE: (&str, &str) = ("text/plain", "not json");
/// Well-formed JSON with every field missing: a JSON route that requires a field answers 422
/// naming it.
const EMPTY_OBJECT: (&str, &str) = ("application/json", "{}");

/// The runtime's HTTP routes, with API key authentication when `auth` is set.
async fn routes(auth: bool) -> Router {
    let rt = Arc::new(
        Runtime::builder()
            .with_app(app::AppBuilder::new("write_access").build())
            .build()
            .await,
    );
    let search = Arc::new(SearchEngine::new(
        Arc::clone(&rt.df) as Arc<dyn runtime_query_engine::query_engine::QueryEngine>,
        parse_explicit_primary_keys(Arc::clone(&rt.app)).await,
        crate::search::util::RuntimeTableProviderExplorer,
    ));
    let auth_layer = auth.then(|| {
        AuthLayer::new(Arc::new(ApiKeyAuth::new(vec![
            ApiKey::parse_str(&format!("{READ_ONLY_KEY}:ro")),
            ApiKey::parse_str(&format!("{READ_WRITE_KEY}:rw")),
        ])) as Arc<dyn HttpAuth + Send + Sync>)
    });
    super::routes::routes(
        &rt,
        Arc::new(Config::new()),
        search,
        auth_layer,
        &CorsConfig::default(),
        false,
        #[cfg(feature = "mcp")]
        None,
    )
}

async fn send(
    routes: &Router,
    method: &Method,
    path: &str,
    api_key: Option<&str>,
    (content_type, body): (&str, &'static str),
) -> (StatusCode, String) {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, content_type);
    if let Some(api_key) = api_key {
        request = request.header("x-api-key", api_key);
    }
    let request = request.body(Body::from(body)).expect("request");
    let response = routes.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

#[tokio::test]
async fn read_only_caller_is_refused_before_the_body_is_parsed() {
    let routes = routes(true).await;
    for (method, path, _) in write_routes() {
        for body in [MALFORMED_JSON, WRONG_CONTENT_TYPE, EMPTY_OBJECT] {
            let (status, text) = send(&routes, &method, path, Some(READ_ONLY_KEY), body).await;
            let message = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|value| value["message"].as_str().map(ToString::to_string));
            assert_eq!(
                (status, message.as_deref()),
                (StatusCode::FORBIDDEN, Some(WRITE_REFUSAL)),
                "{method} {path} with a read-only key and a {} body {:?} answered {status}: {text}",
                body.0,
                body.1,
            );
        }
    }
}

#[tokio::test]
async fn anonymous_caller_is_refused_before_the_body_is_parsed() {
    let routes = routes(true).await;
    for (method, path, _) in write_routes() {
        for body in [MALFORMED_JSON, WRONG_CONTENT_TYPE, EMPTY_OBJECT] {
            let (status, text) = send(&routes, &method, path, None, body).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {path} without a key and a {} body {:?} answered {status}: {text}",
                body.0,
                body.1,
            );
        }
    }
}

/// A read-write caller gets exactly the answer the same request gets when the runtime has no
/// authentication, including the JSON extractor's 400, 415 and 422.
#[tokio::test]
async fn read_write_caller_gets_the_unauthenticated_answer() {
    let authenticated = routes(true).await;
    let unauthenticated = routes(false).await;
    for (method, path, parses_json) in write_routes() {
        for body in [MALFORMED_JSON, WRONG_CONTENT_TYPE, EMPTY_OBJECT] {
            let read_write = send(&authenticated, &method, path, Some(READ_WRITE_KEY), body).await;
            let anonymous = send(&unauthenticated, &method, path, None, body).await;
            assert_eq!(
                read_write, anonymous,
                "{method} {path} with a {} body {:?}: a read-write key and no authentication answered differently",
                body.0, body.1,
            );
            assert_ne!(
                read_write.0,
                StatusCode::FORBIDDEN,
                "{method} {path}: a read-write key was refused: {}",
                read_write.1
            );
            if parses_json {
                let expected = if body == MALFORMED_JSON {
                    StatusCode::BAD_REQUEST
                } else if body == WRONG_CONTENT_TYPE {
                    StatusCode::UNSUPPORTED_MEDIA_TYPE
                } else {
                    continue;
                };
                assert_eq!(
                    read_write.0, expected,
                    "{method} {path} with a {} body {:?}: {}",
                    body.0, body.1, read_write.1
                );
            }
        }
    }

    for (path, field) in [
        ("/v1/packages/generate", "from"),
        (
            "/v1/datasets/missing/acceleration/snapshots/current",
            "snapshot_id",
        ),
    ] {
        let answer = send(
            &authenticated,
            &Method::POST,
            path,
            Some(READ_WRITE_KEY),
            EMPTY_OBJECT,
        )
        .await;
        assert_eq!(
            answer,
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "Failed to deserialize the JSON body into the target type: missing field `{field}` at line 1 column 2"
                )
            ),
            "POST {path} with a read-write key and `{{}}`"
        );
    }
}

/// The write-access layer covers only the write routes: a read-only caller still reads.
#[tokio::test]
async fn read_only_caller_still_reads() {
    let routes = routes(true).await;
    let (status, text) = send(
        &routes,
        &Method::GET,
        "/v1/datasets",
        Some(READ_ONLY_KEY),
        ("application/json", ""),
    )
    .await;
    assert_eq!(
        (status, text.as_str()),
        (StatusCode::OK, "[]"),
        "GET /v1/datasets with a read-only key"
    );
}
