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
        parse_explicit_primary_keys(Arc::clone(&rt.app))
            .await
            .into(),
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

    let missing = |field: &str| {
        format!(
            "Failed to deserialize the JSON body into the target type: missing field `{field}` at line 1 column 2"
        )
    };
    for (path, expected_body) in [
        // `/v1/packages/generate` answers every error with a JSON `{"error": …}` body.
        (
            "/v1/packages/generate",
            serde_json::json!({ "error": missing("from") }).to_string(),
        ),
        (
            "/v1/datasets/missing/acceleration/snapshots/current",
            missing("snapshot_id"),
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
            (StatusCode::UNPROCESSABLE_ENTITY, expected_body),
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

/// Every response to the deprecated `POST /v1/packages/generate` carries its `Deprecation` and
/// `Link` headers, the refusals of the layers in front of its handler included: authentication
/// (401), write access (403) and the request body limit (413). Another route carries neither.
#[tokio::test]
async fn every_package_generate_response_carries_the_deprecation_headers() {
    const DEPRECATION: &str = "@1794873600";
    const LINK: &str = "<https://spiceai.org/docs/api/http/packages#deprecation>; rel=\"deprecation\"; type=\"text/html\"";
    // One byte over the 128 MiB limit on authenticated request bodies. The limit refuses a
    // declared length before it reads the body.
    let over_the_body_limit = (128 * 1024 * 1024 + 1).to_string();

    let routes = routes(true).await;
    let cases = [
        (
            "/v1/packages/generate",
            None,
            None,
            StatusCode::UNAUTHORIZED,
            Some((DEPRECATION, LINK)),
        ),
        (
            "/v1/packages/generate",
            Some("wrong-key"),
            None,
            StatusCode::UNAUTHORIZED,
            Some((DEPRECATION, LINK)),
        ),
        (
            "/v1/packages/generate",
            Some(READ_ONLY_KEY),
            None,
            StatusCode::FORBIDDEN,
            Some((DEPRECATION, LINK)),
        ),
        (
            "/v1/packages/generate",
            Some(READ_WRITE_KEY),
            None,
            StatusCode::BAD_REQUEST,
            Some((DEPRECATION, LINK)),
        ),
        (
            "/v1/packages/generate",
            Some(READ_WRITE_KEY),
            Some(over_the_body_limit.as_str()),
            StatusCode::PAYLOAD_TOO_LARGE,
            Some((DEPRECATION, LINK)),
        ),
        (
            "/v1/datasets/missing/acceleration/refresh",
            None,
            None,
            StatusCode::UNAUTHORIZED,
            None,
        ),
        (
            "/v1/datasets/missing/acceleration/refresh",
            Some(READ_WRITE_KEY),
            Some(over_the_body_limit.as_str()),
            StatusCode::PAYLOAD_TOO_LARGE,
            None,
        ),
    ];
    let mut answers = Vec::new();
    let mut expected = Vec::new();
    for (path, api_key, content_length, status, headers) in cases {
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(api_key) = api_key {
            request = request.header("x-api-key", api_key);
        }
        if let Some(content_length) = content_length {
            request = request.header(header::CONTENT_LENGTH, content_length);
        }
        let request = request.body(Body::from("{")).expect("request");
        let response = routes.clone().oneshot(request).await.expect("response");
        let value_of = |name: &str| {
            response
                .headers()
                .get(name)
                .map(|value| value.to_str().expect("an ASCII header").to_string())
        };
        let request =
            format!("POST {path} with key {api_key:?} and Content-Length {content_length:?}");
        answers.push((
            request.clone(),
            response.status(),
            value_of("deprecation"),
            value_of("link"),
        ));
        expected.push((
            request,
            status,
            headers.map(|(deprecation, _)| deprecation.to_string()),
            headers.map(|(_, link)| link.to_string()),
        ));
    }
    // Compared all at once, so a failure lists every request that answered differently.
    assert_eq!(answers, expected);
}
