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
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use app::App;
use axum::{
    Extension,
    extract::Query,
    http::status,
    response::{IntoResponse, Json, Response},
};
use csv::Writer;
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{Runtime, model::LLMResponsesModelStore, status::ComponentStatus};

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

    /// A comma-separated list of metadata fields to include in the response (e.g., `supports_responses_api`)
    #[serde(default)]
    pub metadata_fields: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub(crate) struct OpenAIModelResponse {
    object: String,
    data: Vec<OpenAIModel>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub(crate) struct Metadata {
    pub supports_responses_api: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) enum MetadataKeys {
    SupportsResponsesAPI,
}

impl TryFrom<&str> for MetadataKeys {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "supports_responses_api" => Ok(MetadataKeys::SupportsResponsesAPI),
            _ => Err(format!("Invalid metadata key: {value}")),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub(crate) struct OpenAIModel {
    /// The name of the model
    id: String,

    /// The type of the model (always `model`)
    object: String,

    /// The source from which the model was loaded (e.g., `openai`, `spiceai`)
    owned_by: String,

    /// The datasets associated with this model, if any
    datasets: Option<Vec<String>>,

    /// The status of the model (e.g., `ready`, `initializing`, `error`)
    status: Option<ComponentStatus>,

    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<Metadata>,
}

fn get_metadata_keys(params: &ModelsQueryParams) -> Result<Vec<MetadataKeys>, String> {
    let mut keys = Vec::new();
    for field in params.metadata_fields.split(',') {
        keys.push(MetadataKeys::try_from(field.trim())?);
    }
    Ok(keys)
}

fn generate_metadata(
    model_name: &str,
    metadata_keys: &Vec<MetadataKeys>,
    responses_models: &HashSet<String>,
) -> Option<Metadata> {
    if metadata_keys.is_empty() {
        return None;
    }

    let mut metadata = Metadata::default();
    for key in metadata_keys {
        match key {
            MetadataKeys::SupportsResponsesAPI => {
                metadata.supports_responses_api = responses_models.contains(model_name);
            }
        }
    }
    Some(metadata)
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
            OpenAIModelResponse = "application/json",
            example = json!({
                "object": "list",
                "data": [
                    {
                        "id": "gpt-4",
                        "object": "model",
                        "owned_by": "openai",
                        "datasets": null,
                        "status": "ready"
                    },
                    {
                        "id": "text-embedding-ada-002",
                        "object": "model",
                        "owned_by": "openai-internal",
                        "datasets": ["text-dataset-1", "text-dataset-2"],
                        "status": "ready"
                    }
                ]
            })
        ), (
            String = "text/csv",
            example = "
id,object,owned_by,datasets,status
gpt-4,model,openai,,ready
text-embedding-ada-002,model,openai-internal,\"text-dataset-1,text-dataset-2\",ready
"
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
    Extension(responses_models): Extension<Arc<RwLock<LLMResponsesModelStore>>>,
    Query(params): Query<ModelsQueryParams>,
) -> Response {
    let statuses = if params.status {
        rt.status.get_model_statuses()
    } else {
        HashMap::default()
    };

    let metadata_keys = match get_metadata_keys(&params) {
        Ok(keys) => keys,
        Err(err) => {
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };

    let responses_models = if metadata_keys.contains(&MetadataKeys::SupportsResponsesAPI) {
        let guard = responses_models.read().await;
        guard.keys().cloned().collect::<HashSet<String>>()
    } else {
        HashSet::default()
    };

    let mut models = match app.read().await.as_ref() {
        Some(a) => a
            .models
            .iter()
            .map(|m| {
                let d = if m.datasets.is_empty() {
                    None
                } else {
                    Some(m.datasets.clone())
                };
                OpenAIModel {
                    id: m.name.clone(),
                    object: "model".to_string(),
                    owned_by: m.from.clone(),
                    datasets: d,
                    status: statuses.get(&m.name).copied(),
                    metadata: generate_metadata(&m.name, &metadata_keys, &responses_models),
                }
            })
            .collect::<Vec<OpenAIModel>>(),
        None => {
            return (
                status::StatusCode::INTERNAL_SERVER_ERROR,
                "App not initialized",
            )
                .into_response();
        }
    };

    let worker_statuses = if params.status {
        rt.status.get_worker_statuses()
    } else {
        HashMap::default()
    };
    let worker_registry = rt.workers.read().await;
    let workers = worker_registry
        .iter()
        .filter_map(|(name, worker)| {
            Arc::clone(worker).as_model()?;
            Some(OpenAIModel {
                id: name.clone(),
                object: "model".to_string(),
                owned_by: "spiceai".to_string(),
                datasets: None,
                status: worker_statuses.get(name).copied(),
                metadata: generate_metadata(name, &metadata_keys, &responses_models),
            })
        })
        .collect::<Vec<OpenAIModel>>();
    models.extend(workers.into_iter());

    match params.format {
        Format::Json => (
            status::StatusCode::OK,
            Json(OpenAIModelResponse {
                object: "list".to_string(),
                data: models,
            }),
        )
            .into_response(),
        Format::Csv => match convert_details_to_csv(&models) {
            Ok(csv) => (status::StatusCode::OK, csv).into_response(),
            Err(e) => {
                tracing::error!("Error converting to CSV: {e}");
                (status::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        },
    }
}

fn convert_details_to_csv(models: &[OpenAIModel]) -> Result<String, Box<dyn std::error::Error>> {
    let mut w = Writer::from_writer(vec![]);
    for d in models {
        let _ = w.serialize(d);
    }
    w.flush()?;
    Ok(String::from_utf8(w.into_inner()?)?)
}
