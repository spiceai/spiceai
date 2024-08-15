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
use axum::{
    extract::Path,
    extract::Query,
    http::status,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

/// [`OAuthResponse`] is the response returned via the OAuth redirect.
/// `<https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-auth-code-flow#successful-response>`
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) struct OAuthResponse {
    pub code: String,
    pub state: Option<String>,
}

/// The additional state provided in `/oauth2/v2.0/authorize`.
/// `<https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-auth-code-flow#request-an-authorization-code>`
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) struct SharepointState {
    pub datasets: Vec<String>,
}

impl SharepointState {
    pub(crate) fn new(from_state: Option<String>) -> Self {
        from_state
            .and_then(|state| serde_json::from_str(&state).ok())
            .unwrap_or_default()
    }
}

pub(crate) async fn get(
    Path(service): Path<String>,
    Query(params): Query<OAuthResponse>,
) -> Response {
    match service.as_str() {
        "sharepoint" => {
            println!("Sharing!!");
            let state = SharepointState::new(params.state);
            (
                status::StatusCode::OK,
                format!(
                    "Sharepoint is good. Code={}. State={:#?}!!",
                    params.code, state
                ),
            )
                .into_response()
        }
        s => (
            status::StatusCode::BAD_REQUEST,
            format!("Unsupported service {s}"),
        )
            .into_response(),
    }
}
