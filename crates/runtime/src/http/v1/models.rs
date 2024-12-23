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

use app::App;
use axum::{
    extract::Query,
    http::status,
    response::{IntoResponse, Json, Response},
    Extension,
};
use csv::Writer;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{status::ComponentStatus, Runtime};

use super::Format;

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
pub struct ModelsQueryParams {
    /// The format of the response (e.g., `json` or `csv`).
    #[serde(default)]
    pub format: Format,

    /// If true, includes the status of each model in the response.
    #[serde(default)]
    pub status: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub(crate) struct ModelResponse {
    /// The name of the model
    pub name: String,

    /// The source from which the model was loaded (e.g., `openai`, `spiceai`)
    pub from: String,

    /// The datasets associated with this model, if any
    pub datasets: Option<Vec<String>>,

    /// The status of the model (e.g., `ready`, `initializing`, `error`)
    pub status: Option<ComponentStatus>,
}

/// List Models
///
/// List all models, both machine learning and language models, available in the runtime.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/models",
    operation_id = "get_models",
    tag = "AI",
    params(ModelsQueryParams),
    responses(
        (status = 200, description = "List of models in JSON format", content((
            ModelResponse = "application/json",
            example = json!([
                {
                    "name": "example_model_1",
                    "from": "openai",
                    "datasets": ["dataset_a", "dataset_b"],
                    "status": "ready"
                },
                {
                    "name": "example_model_2",
                    "from": "spiceai",
                    "datasets": null,
                    "status": "initializing"
                }
            ])
        ), (
            String = "text/csv",
            example = r#"
name,from,datasets,status
example_model_1,openai,"[\"dataset_a\", \"dataset_b\"]",ready
example_model_2,spiceai,,initializing
"#
        ))),
        (status = 500, description = "Internal server error occurred while processing models", content((
            serde_json::Value = "application/json",
            example = json!({
                "error": "App not initialized"
            })
        )))
    )
))]
pub(crate) async fn get(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(rt): Extension<Arc<Runtime>>,
    Query(params): Query<ModelsQueryParams>,
) -> Response {
    let statuses = if params.status {
        rt.status.get_model_statuses()
    } else {
        HashMap::default()
    };
    let resp = match app.read().await.as_ref() {
        Some(a) => a
            .models
            .iter()
            .map(|m| {
                let d = if m.datasets.is_empty() {
                    None
                } else {
                    Some(m.datasets.clone())
                };

                ModelResponse {
                    name: m.name.clone(),
                    from: m.from.clone(),
                    datasets: d,
                    status: if params.status {
                        statuses.get(&m.name).copied()
                    } else {
                        None
                    },
                }
            })
            .collect::<Vec<ModelResponse>>(),
        None => {
            return (
                status::StatusCode::INTERNAL_SERVER_ERROR,
                "App not initialized",
            )
                .into_response();
        }
    };

    match params.format {
        Format::Json => (status::StatusCode::OK, Json(resp)).into_response(),
        Format::Csv => match convert_details_to_csv(&resp) {
            Ok(csv) => (status::StatusCode::OK, csv).into_response(),
            Err(e) => {
                tracing::error!("Error converting to CSV: {e}");
                (status::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        },
    }
}

fn convert_details_to_csv(models: &[ModelResponse]) -> Result<String, Box<dyn std::error::Error>> {
    let mut w = Writer::from_writer(vec![]);
    for d in models {
        let _ = w.serialize(d);
    }
    w.flush()?;
    Ok(String::from_utf8(w.into_inner()?)?)
}
