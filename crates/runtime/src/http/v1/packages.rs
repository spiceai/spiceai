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

use crate::objectstore::github::GitHubRawObjectStore;
use axum::{
    http::{header::CONTENT_TYPE, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use object_store::{path::Path, ObjectStore};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

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
/// This endpoint generates a zip package from a specified GitHub source.
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
        (status = 500, description = "Internal server error", content((
            serde_json::Value = "application/json",
            example = json!({
                "error": "An unexpected error occurred"
            })
        )))
    )
))]
pub(crate) async fn generate(Json(payload): Json<GeneratePackageRequest>) -> Response {
    if !payload.from.starts_with("github:") {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                "Invalid `from` field, specify a github source and retry (e.g. github:{org}/{repo}/{sha}/{path_to_spicepod.yaml})",
            ),
        )
            .into_response();
    }

    let Some(from) = payload.from.split(':').nth(1) else {
        return (StatusCode::BAD_REQUEST, "Invalid `from` field, specify a github source and retry (e.g. github:{org}/{repo}/{sha}/{path_to_spicepod.yaml})")
            .into_response();
    };

    let parts: Vec<&str> = from.splitn(4, '/').collect();
    let (Some(&org), Some(&repo), Some(&sha), Some(&path)) =
        (parts.first(), parts.get(1), parts.get(2), parts.get(3))
    else {
        return (StatusCode::BAD_REQUEST, "Invalid `from` field, specify a github source and retry (e.g. github:{org}/{repo}/{sha}/{path_to_spicepod.yaml})")
        .into_response();
    };

    let github_token = payload.params.get("github_token").map(String::as_str);

    let store = match GitHubRawObjectStore::try_new(org, repo, sha, github_token) {
        Ok(store) => Arc::new(store) as Arc<dyn ObjectStore>,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    let path = match Path::parse(path) {
        Ok(path) => path,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let package_zip_bytes = match package::make_zip(&store, &path).await {
        Ok(bytes) => bytes,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/zip")],
        package_zip_bytes,
    )
        .into_response()
}
