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
    extract::Path,
    response::{IntoResponse, Json, Response},
};
use axum_extra::TypedHeader;
use datafusion::sql::TableReference;
use headers_accept::Accept;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    Runtime,
    datafusion::request_context_extension::get_current_datafusion,
    model::{EvalScorerRegistry, LLMChatCompletionsModelStore, handle_eval_run, sql_query_for},
};
use runtime_request_context::{AsyncMarker, RequestContext};

#[cfg(feature = "openapi")]
use crate::model::EvalRunResponse;

use super::{ResponseMimeType, sql_to_http_response};

/// Input parameters to start an evaluation run for a given model.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct RunEval {
    pub model: String,
}

/// Run Eval (DEPRECATED)
///
/// This endpoint has been removed. Use the standalone `spice-eval` tool instead.
///
/// To run an evaluation:
/// ```bash
/// spice-eval run <eval_name> --model <model_name>
/// ```
/*
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/evals/{name}",
    operation_id = "post_eval",
    tag = "Evaluations",
    deprecated = true,
    params(
        ("Accept" = String, Header, description = "The format of the response, one of 'application/json' (default), 'text/csv' or 'text/plain'."),
    ),
    params(
        ("name" = String, Path, description = "Name of the evaluation to run")
    ),
    request_body(
        description = "Parameters to run the evaluation",
        content((RunEval = "application/json", example = json!({ "model": "example_model" })))
    ),
    responses(
        (status = 410, description = "Endpoint removed - use spice-eval tool instead")
    )
))]
pub(crate) async fn post(
    Extension(llms): Extension<Arc<RwLock<LLMChatCompletionsModelStore>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Extension(eval_scorer_registry): Extension<EvalScorerRegistry>,
    accept: Option<TypedHeader<Accept>>,
    Path(eval_name): Path<String>,
    Json(req): Json<RunEval>,
) -> Response {
    (
        StatusCode::GONE,
        "This endpoint has been removed. Use the standalone spice-eval tool instead. \
         Run: spice-eval run <eval_name> --model <model_name>",
    )
        .into_response()
}
*/

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
