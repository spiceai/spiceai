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

use crate::datafusion::DataFusion;
use axum::{
    body::Bytes,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct GeneratePackageRequest {
    to: String, // github:{org}/{repo}/{sha}/{path_to_spicepod.yaml}
    // params:
    //   token: string
    params: HashMap<String, String>,
}
// struct PackageRequest {
//     org: String,
//     repo: String,
//     path: String,
//     token: Option<String>,
// }

pub(crate) async fn generate(
    Extension(df): Extension<Arc<DataFusion>>,
    Json(payload): Json<PackageRequest>,
) -> Response {
    let store = GitHubRawObjectStore::try_new(
        payload.org,
        payload.repo,
        payload.path,
        payload.token.as_ref(),
    );
}
