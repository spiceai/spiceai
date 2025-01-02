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
    extract::Path,
    response::{IntoResponse, Json, Response},
    Extension,
};
use axum_extra::TypedHeader;
use datafusion::sql::TableReference;
use headers_accept::Accept;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    datafusion::DataFusion,
    model::{handle_eval_run, sql_query_for, EvalScorerRegistry, LLMModelStore},
    Runtime,
};

#[cfg(feature = "openapi")]
use crate::model::EvalRunResponse;

use super::{sql_to_http_response, ArrowFormat};

/// Input parameters to start an evaluation run for a given model.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct RunEval {
    pub model: String,
}

/// Run Eval
///
/// Evaluate a model against a eval spice specification
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/evals/{name}",
    operation_id = "post_eval",
    tag = "Evaluations",
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
        (status = 200, description = "Evaluation run successfully", content((
            EvalRunResponse = "application/json", example = json!({
                    "primary_key": "eval_12345",
                    "time_column": "2024-12-19T12:34:56Z",
                    "dataset": "my_dataset",
                    "model": "example_model",
                    "status": "completed",
                    "error_message": null,
                    "scorers": ["scorer1", "scorer2"],
                    "metrics": {
                        "scorer1/accuracy": 0.95,
                        "scorer2/accuracy": 0.93
                    }
                })
            ),
            ("text/csv", example = "primary_key,time_column,dataset,model,status,error_message,scorers,metrics\n\
                          eval_12345,2024-12-19T12:34:56Z,my_dataset,example_model,completed,,\"[\"\"scorer1\"\", \"\"scorer2\"\"]\",\"{\"\"scorer1/accuracy\"\":0.95, \"\"scorer2/accuracy\"\":0.93}\""
            ),
            ("text/plain", example = r#"+-------------+---------------------+-----------+---------------+-----------+----------------+------------------+---------------------------------------+
            | primary_key | time_column         | dataset   | model         | status    | error_message  |      scorers     | metrics                               |
            +-------------+---------------------+-----------+---------------+-----------+----------------+------------------+---------------------------------------+
            | eval_12345  | 2024-12-19T12:34:56Z| my_dataset| example_model | completed |                | scorer1, scorer2 | {"accuracy": 0.95, "precision": 0.93} |
            +-------------+---------------------+-----------+---------------+-----------+----------------+------------------+---------------------------------------+"#)
        )),
    )
))]
pub(crate) async fn post(
    Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
    Extension(df): Extension<Arc<DataFusion>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Extension(eval_scorer_registry): Extension<EvalScorerRegistry>,
    accept: Option<TypedHeader<Accept>>,
    Path(eval_name): Path<String>,
    Json(req): Json<RunEval>,
) -> Response {
    let model = req.model;

    let evals = rt.evals.read().await;
    let Some(eval) = evals.iter().find(|e| e.name == eval_name) else {
        return (
            StatusCode::NOT_FOUND,
            format!("eval '{eval_name}' not found"),
        )
            .into_response();
    };

    if !llms.read().await.contains_key(&model) {
        return (StatusCode::NOT_FOUND, format!("model '{model}' not found")).into_response();
    };

    if !df
        .has_table(&TableReference::parse_str(eval.dataset.as_str()))
        .await
    {
        return (
            StatusCode::NOT_FOUND,
            format!("dataset '{}' not found", eval.dataset),
        )
            .into_response();
    };

    match handle_eval_run(
        eval,
        model,
        Arc::clone(&df),
        Arc::clone(&llms),
        eval_scorer_registry,
    )
    .await
    {
        Ok(id) => {
            sql_to_http_response(
                Arc::clone(&df),
                sql_query_for(&id).as_str(),
                ArrowFormat::from_accept_header(accept.as_ref()),
            )
            .await
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    }
}

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
