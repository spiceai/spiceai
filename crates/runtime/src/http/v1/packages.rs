/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use axum::{
    Json,
    extract::rejection::JsonRejection,
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE, header::RETRY_AFTER},
    response::{IntoResponse, Response},
};
use object_store::{ObjectStore, path::Path};
use runtime_object_store::store::github::{GitHubRawObjectStore, TruncatedListing};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::{runtime::Handle, sync::Semaphore};

/// When `POST /v1/packages/generate` was deprecated: 2026-11-17T00:00:00Z, the date the
/// deprecation was decided. Sent as the `Deprecation` header (RFC 9745) on every response.
const PACKAGE_GENERATE_DEPRECATED_AT: u64 = 1_794_873_600;

/// The documentation a deprecated `POST /v1/packages/generate` points to, as its `Link` header.
const PACKAGE_GENERATE_DEPRECATION_LINK: &str = "<https://spiceai.org/docs/api/http/packages#deprecation>; rel=\"deprecation\"; type=\"text/html\"";

/// The `warn` line each call to the deprecated endpoint logs: when it is removed and what to use
/// instead.
fn deprecation_warning() -> &'static str {
    "`POST /v1/packages/generate` is deprecated and is removed in v2.6. Build the package from a checkout of the repository instead (for example, with `git archive`). See https://spiceai.org/docs/api/http/packages#deprecation"
}

/// Adds the `Deprecation` and `Link` headers every `POST /v1/packages/generate` response carries,
/// success or error.
pub(crate) fn add_deprecation_headers(headers: &mut http::HeaderMap) {
    headers.insert(
        http::HeaderName::from_static("deprecation"),
        HeaderValue::from_str(&format!("@{PACKAGE_GENERATE_DEPRECATED_AT}"))
            .unwrap_or_else(|_| HeaderValue::from_static("@1794873600")),
    );
    headers.insert(
        http::header::LINK,
        HeaderValue::from_static(PACKAGE_GENERATE_DEPRECATION_LINK),
    );
}

/// One package is built at a time on a runtime; a request that arrives while one is building is
/// answered `503` at once rather than waiting.
static PACKAGE_BUILD: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(1));

/// The longest a package build may take, from the moment it starts.
const PACKAGE_BUILD_DEADLINE: Duration = Duration::from_mins(5);

/// The `Retry-After` a request gets while another package is building.
const PACKAGE_BUILD_RETRY_AFTER_SECS: u64 = 30;

const INVALID_FROM: &str = "Invalid `from` field, specify a github source and retry (e.g. github:{org}/{repo}/{sha}/{path_to_spicepod.yaml})";

/// A response with the JSON error body every package-generation error uses.
fn json_error(status: StatusCode, message: impl std::fmt::Display) -> Response {
    (
        status,
        Json(serde_json::json!({"error": message.to_string()})),
    )
        .into_response()
}

/// The status and message for a package that could not be generated from `from`: `422` for a
/// package over its budget or a repository GitHub lists only in part, `500` otherwise.
fn package_error_response(from: &str, error: &package::Error) -> Response {
    if error.is_over_budget() {
        return json_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("Failed to generate the package from '{from}': {error}"),
        );
    }
    if let Some(truncated) = find_in_chain::<TruncatedListing>(error) {
        return json_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "Failed to generate the package from '{from}': {truncated} See https://spiceai.org/docs/api/http/packages"
            ),
        );
    }
    json_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("Failed to generate the package from '{from}': {error}"),
    )
}

/// The first error of type `T` in `error`'s source chain.
fn find_in_chain<'a, T: std::error::Error + 'static>(
    error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a T> {
    let mut current = Some(error);
    while let Some(e) = current {
        if let Some(found) = e.downcast_ref::<T>() {
            return Some(found);
        }
        current = e.source();
    }
    None
}

fn busy_message(from: &str) -> String {
    format!(
        "Failed to generate the package from '{from}': another package is being built on this runtime, and only one is built at a time. Retry after {PACKAGE_BUILD_RETRY_AFTER_SECS} seconds. See https://spiceai.org/docs/api/http/packages"
    )
}

fn deadline_message(from: &str) -> String {
    format!(
        "Failed to generate the package from '{from}': downloading its files from GitHub did not finish within 5 minutes, the most a package build may take. Reference fewer or smaller files, or retry if GitHub is slow. See https://spiceai.org/docs/api/http/packages"
    )
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GeneratePackageRequest {
    /// The GitHub source path in the format `github:{org}/{repo}/{sha}/{path_to_spicepod.yaml}`
    pub from: String,

    /// A key-value map of optional parameters (e.g., `github_token`)
    pub params: HashMap<String, String>,
}

/// Generate Package
///
/// Deprecated: removed in v2.6. Build the package from a checkout of the repository instead.
///
/// This endpoint generates a zip package from a specified GitHub source. A package holds at most
/// 10,000 files and 1 GiB, is built within 5 minutes, and one package is built at a time.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/packages/generate",
    operation_id = "generate_package",
    tag = "General",
    request_body(
        description = "Parameters required to generate a package",
        content((
            GeneratePackageRequest = "application/json",
            example = json!({
                "from": "github:myorg/myrepo/abc12345/spicepod.yaml",
                "params": {
                    "github_token": "ghp_exampleToken12345"
                }
            })
        ))
    ),
    responses(
        (status = 200, description = "Package generated successfully", content((
            Vec<u8> = "application/zip",
            example = "<binary zip file response>"
        ))),
        (status = 400, description = "Invalid request parameters", content((
            serde_json::Value = "application/json",
            example = json!({
                "error": "Invalid `from` field, specify a github source and retry (e.g. github:{org}/{repo}/{sha}/{path_to_spicepod.yaml})"
            })
        ))),
        (status = 422, description = "The package is over its budget (10,000 files, 1 GiB), or GitHub lists only part of the repository's files", content((
            serde_json::Value = "application/json",
            example = json!({
                "error": "Failed to generate the package from 'github:org/repo/abc123/spicepod.yaml': its files total more than 1 GiB, the most a package may hold, and dataset 'trips' ('file:data/trips/') alone adds 1.4 GiB. Reference fewer or smaller files in that dataset. See https://spiceai.org/docs/api/http/packages"
            })
        ))),
        (status = 500, description = "Internal server error", content((
            serde_json::Value = "application/json",
            example = json!({
                "error": "An unexpected error occurred"
            })
        ))),
        (status = 503, description = "Another package is being built on this runtime; retry after `Retry-After` seconds", content((
            serde_json::Value = "application/json",
            example = json!({
                "error": "Failed to generate the package from 'github:org/repo/abc123/spicepod.yaml': another package is being built on this runtime, and only one is built at a time. Retry after 30 seconds. See https://spiceai.org/docs/api/http/packages"
            })
        ))),
        (status = 504, description = "The package build did not finish within 5 minutes", content((
            serde_json::Value = "application/json",
            example = json!({
                "error": "Failed to generate the package from 'github:org/repo/abc123/spicepod.yaml': downloading its files from GitHub did not finish within 5 minutes, the most a package build may take. Reference fewer or smaller files, or retry if GitHub is slow. See https://spiceai.org/docs/api/http/packages"
            })
        )))
    )
))]
// The OpenAPI operation is marked deprecated from this attribute.
#[deprecated(
    since = "2.5.0",
    note = "removed in v2.6; build the package from a checkout of the repository instead"
)]
pub(crate) async fn generate(
    payload: Result<Json<GeneratePackageRequest>, JsonRejection>,
) -> Response {
    tracing::warn!(target: "spiced", "{}", deprecation_warning());

    let payload = match payload {
        Ok(Json(payload)) => payload,
        Err(rejection) => return json_error(rejection.status(), rejection.body_text()),
    };

    if !payload.from.starts_with("github:") {
        return json_error(StatusCode::BAD_REQUEST, INVALID_FROM);
    }

    let Some(from) = payload.from.split(':').nth(1) else {
        return json_error(StatusCode::BAD_REQUEST, INVALID_FROM);
    };

    let parts: Vec<&str> = from.splitn(4, '/').collect();
    let (Some(&org), Some(&repo), Some(&sha), Some(&path)) =
        (parts.first(), parts.get(1), parts.get(2), parts.get(3))
    else {
        return json_error(StatusCode::BAD_REQUEST, INVALID_FROM);
    };

    let github_token = payload.params.get("github_token").map(String::as_str);

    let store = match GitHubRawObjectStore::try_new(org, repo, sha, github_token, Handle::current())
    {
        Ok(store) => Arc::new(store) as Arc<dyn ObjectStore>,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, e),
    };

    let path = match Path::parse(path) {
        Ok(path) => path,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, e),
    };

    let Ok(_build) = PACKAGE_BUILD.try_acquire() else {
        let mut response = json_error(StatusCode::SERVICE_UNAVAILABLE, busy_message(&payload.from));
        response.headers_mut().insert(
            RETRY_AFTER,
            HeaderValue::from(PACKAGE_BUILD_RETRY_AFTER_SECS),
        );
        return response;
    };

    match tokio::time::timeout(PACKAGE_BUILD_DEADLINE, package::make_zip(&store, &path)).await {
        Err(_elapsed) => json_error(StatusCode::GATEWAY_TIMEOUT, deadline_message(&payload.from)),
        Ok(Err(e)) => package_error_response(&payload.from, &e),
        Ok(Ok(package_zip_bytes)) => (
            StatusCode::OK,
            [(CONTENT_TYPE, "application/zip")],
            package_zip_bytes,
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        serde_json::from_slice(&bytes).expect("a JSON body")
    }

    /// While a package is building, another request is answered `503` at once, with
    /// `Retry-After` and the JSON error body, rather than waiting for the build.
    #[tokio::test]
    #[expect(deprecated, reason = "tests the deprecated endpoint's handler")]
    async fn a_second_request_while_one_builds_gets_503() {
        let from = "github:org/repo/abc123/spicepod.yaml";
        let _building = PACKAGE_BUILD
            .try_acquire()
            .expect("no package is building in this test");
        let response = generate(Ok(Json(GeneratePackageRequest {
            from: from.to_string(),
            params: HashMap::new(),
        })))
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(RETRY_AFTER),
            Some(&HeaderValue::from_static("30"))
        );
        assert_eq!(
            body_json(response).await,
            serde_json::json!({"error": "Failed to generate the package from 'github:org/repo/abc123/spicepod.yaml': another package is being built on this runtime, and only one is built at a time. Retry after 30 seconds. See https://spiceai.org/docs/api/http/packages"})
        );
    }

    #[tokio::test]
    #[expect(deprecated, reason = "tests the deprecated endpoint's handler")]
    async fn an_invalid_source_gets_400_with_a_json_body() {
        let response = generate(Ok(Json(GeneratePackageRequest {
            from: "s3://bucket/spicepod.yaml".to_string(),
            params: HashMap::new(),
        })))
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            body_json(response).await,
            serde_json::json!({"error": INVALID_FROM})
        );
    }

    #[tokio::test]
    async fn a_truncated_listing_gets_422() {
        let error = package::Error::FailedToReadObject {
            source: object_store::Error::Generic {
                store: "GitHubRawObjectStore",
                source: Box::new(TruncatedListing {
                    org: "org".to_string(),
                    repo: "repo".to_string(),
                    rev: "abc123".to_string(),
                }),
            },
        };
        let response = package_error_response("github:org/repo/abc123/spicepod.yaml", &error);
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            body_json(response).await,
            serde_json::json!({"error": "Failed to generate the package from 'github:org/repo/abc123/spicepod.yaml': GitHub lists only part of the files of repository 'org/repo' at 'abc123', so the list would be incomplete. Reference individual files instead of a folder. See https://spiceai.org/docs/api/http/packages"})
        );
    }

    #[test]
    fn the_deprecation_warning_names_the_removal_release_and_the_replacement() {
        assert_eq!(
            deprecation_warning(),
            "`POST /v1/packages/generate` is deprecated and is removed in v2.6. Build the package from a checkout of the repository instead (for example, with `git archive`). See https://spiceai.org/docs/api/http/packages#deprecation"
        );
    }

    /// The headers every response carries, success or error.
    #[test]
    fn deprecation_headers() {
        let mut headers = http::HeaderMap::new();
        add_deprecation_headers(&mut headers);
        assert_eq!(
            headers.get("deprecation"),
            Some(&HeaderValue::from_static("@1794873600"))
        );
        assert_eq!(
            headers.get(http::header::LINK),
            Some(&HeaderValue::from_static(
                "<https://spiceai.org/docs/api/http/packages#deprecation>; rel=\"deprecation\"; type=\"text/html\""
            ))
        );
    }

    #[test]
    fn the_deadline_message_names_the_limit_and_the_fix() {
        assert_eq!(
            deadline_message("github:org/repo/abc123/spicepod.yaml"),
            "Failed to generate the package from 'github:org/repo/abc123/spicepod.yaml': downloading its files from GitHub did not finish within 5 minutes, the most a package build may take. Reference fewer or smaller files, or retry if GitHub is slow. See https://spiceai.org/docs/api/http/packages"
        );
    }
}
