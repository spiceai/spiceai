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
use std::sync::Arc;

use axum::{
    Extension,
    response::{IntoResponse, Json, Response},
};
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::Runtime;

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct ListEvalElement {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub dataset: String,
    pub scorers: Vec<String>,
}

/// List Evals
///
/// Return all evals available to run in the runtime.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/evals",
    tag = "Evaluations",
    responses(
        (status = 200, description = "All evals available in the Spice runtime", body = [ListEvalElement],
            example = json!([{
                "name": "knows_math",
                "description": "Questions from first year, undergraduate math exams",
                "dataset": "math_exams",
                "scorers": ["match", "professor_logical_consistency"]
            }])
        )
    )
))]
pub(crate) async fn list(Extension(rt): Extension<Arc<Runtime>>) -> Response {
    let evals_lock = rt.evals.read().await;
    let evals: Vec<_> = evals_lock
        .iter()
        .map(|e| ListEvalElement {
            name: e.name.clone(),
            description: e.description.clone(),
            dataset: e.dataset.clone(),
            scorers: e.scorers.clone(),
        })
        .collect();

    (StatusCode::OK, Json(evals)).into_response()
}
