/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::{collections::HashMap, sync::Arc};

use crate::objectstore::github::GitHubRawObjectStore;
use axum::{
    http::{header::CONTENT_TYPE, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use object_store::{path::Path, ObjectStore};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeneratePackageRequest {
    from: String,
    params: HashMap<String, String>,
}

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
