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

use app::App;
use axum::{
    Extension,
    extract::Query,
    http::status,
    response::{IntoResponse, Json, Response},
};
use csv::Writer;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::Runtime;

use super::Format;

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
pub struct WorkersQueryParams {
    /// The format of the response (e.g., `json` or `csv`).
    #[serde(default)]
    pub format: Format,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub(crate) struct WorkerResponse {
    object: String,
    data: Vec<Worker>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub(crate) struct Worker {
    name: String,
    description: Option<String>,

    /// Use generic to avoid using `utoipa::ToSchema` in `spicepod` crate.
    models: Vec<serde_json::Value>,
}

impl From<&spicepod::component::worker::Worker> for Worker {
    fn from(value: &spicepod::component::worker::Worker) -> Self {
        let spicepod::component::worker::Worker {
            name,
            description,
            models,
            ..
        } = value;

        Worker {
            name: name.clone(),
            description: description.clone(),
            models: models
                .iter()
                .filter_map(|m| serde_json::to_value(m).ok()) // '.to_value' cannot fail as `.models` came from a serialized form (i.e. in spicepod.yaml).
                .collect(),
        }
    }
}

/// List Workers
///
/// Returns a list of workers in the system.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/workers",
    operation_id = "get_workers",
    params(WorkersQueryParams),
    responses(
        (status = 200, description = "List of workers in JSON format", content((
            WorkerResponse = "application/json",
            example = json!({
                "object": "list",
                "data": [
                    {
                        "from": "models:gpt-4o",
                        "name": "gpt-4o-researcher",
                        "role": "researcher"
                    },
                    {
                        "from": "models:gpt-4o",
                        "name": "gpt-4o-writer",
                        "role": "writer"
                    }
                ]
            })
        ), (
            String = "text/csv",
            example = "
from,name,role
models:gpt-4o,gpt-4o-researcher,researcher
models:gpt-4o,gpt-4o-writer,writer
"
        ))),
        (status = 500, description = "Internal server error occurred while processing workers", content((
            serde_json::Value = "application/json",
            example = json!({
                "error": "App not initialized"
            })
        )))
    )
))]

pub(crate) async fn get(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Extension(_rt): Extension<Arc<Runtime>>,
    Query(params): Query<WorkersQueryParams>,
) -> Response {
    let workers = match app.read().await.as_ref() {
        Some(a) => a.workers.iter().map(Into::into).collect::<Vec<Worker>>(),
        None => {
            return (
                status::StatusCode::INTERNAL_SERVER_ERROR,
                "App not initialized",
            )
                .into_response();
        }
    };

    match params.format {
        Format::Json => (
            status::StatusCode::OK,
            Json(WorkerResponse {
                object: "list".to_string(),
                data: workers,
            }),
        )
            .into_response(),
        Format::Csv => match convert_details_to_csv(&workers) {
            Ok(csv) => (status::StatusCode::OK, csv).into_response(),
            Err(e) => {
                tracing::error!("Error converting to CSV: {e}");
                (status::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        },
    }
}

fn convert_details_to_csv(workers: &[Worker]) -> Result<String, Box<dyn std::error::Error>> {
    let mut w = Writer::from_writer(vec![]);
    for worker in workers {
        let _ = w.serialize(worker);
    }
    w.flush()?;
    Ok(String::from_utf8(w.into_inner()?)?)
}
