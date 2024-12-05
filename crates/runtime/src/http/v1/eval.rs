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
use std::sync::Arc;

use axum::{
    extract::Path,
    response::{IntoResponse, Json, Response},
    Extension,
};
use datafusion::sql::TableReference;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    datafusion::DataFusion,
    model::{run_eval, LLMModelStore},
    Runtime,
};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RunEval {
    pub model: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) struct ModelResponse {}

pub(crate) async fn post(
    Extension(llms): Extension<Arc<RwLock<LLMModelStore>>>,
    Extension(df): Extension<Arc<DataFusion>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Path(eval_name): Path<String>,
    Json(req): Json<RunEval>,
) -> Response {
    let model = req.model;

    let llm_lock = llms.read().await;
    let Some(llm) = llm_lock.get(&model) else {
        return (StatusCode::NOT_FOUND, format!("model '{model}' not found")).into_response();
    };

    let evals = rt.evals.read().await;
    let Some(eval) = evals.iter().find(|e| e.name == eval_name) else {
        return (
            StatusCode::NOT_FOUND,
            format!("eval '{eval_name}' not found"),
        )
            .into_response();
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

    let scorers = rt.eval_scorers.read().await;
    match run_eval(eval, df, &**llm, &scorers).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")).into_response(),
    }
}
