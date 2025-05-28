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
    extract::Query,
    http::status,
    response::{IntoResponse, Json, Response},
};
use csv::Writer;
use serde::{Deserialize, Serialize};

use super::Format;
use crate::worker::{Worker, WorkerRegistry};

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
    data: Vec<WorkerResponseItem>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub(crate) struct WorkerResponseItem {
    name: String,
    description: Option<String>,
    r#type: String,
    is_llm: bool,
}

fn worker_details(worker: &Arc<dyn Worker>) -> WorkerResponseItem {
    WorkerResponseItem {
        name: worker.name().to_string(),
        description: worker.description().map(|d| d.to_string()),
        r#type: worker.role().to_string(),
        is_llm: Arc::clone(worker).as_model().is_some(),
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
                        "name": "round-robin",
                        "description": "Distributes requests between foo and bar models in a round-robin fashion.\n",
                        "type": "load_balance",
                        "is_llm": true
                    },
                    {
                        "name": "fallback",
                        "description": "Attempts bar first, then foo, then baz if previous models fail.\n",
                        "type": "load_balance",
                        "is_llm": true
                    }
                ]
            })
        ), (
            String = "text/csv",
            example = "
name,description,type,is_llm
round-robin,\"Distributes requests between foo and bar models in a round-robin fashion.\",load_balance,true
fallback,\"Attempts bar first, then foo, then baz if previous models fail.\",load_balance,true
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
    Extension(workers): Extension<WorkerRegistry>,
    Query(params): Query<WorkersQueryParams>,
) -> Response {
    let result = &*workers
        .read()
        .await
        .values()
        .map(worker_details)
        .collect::<Vec<_>>();

    match params.format {
        Format::Json => (
            status::StatusCode::OK,
            Json(WorkerResponse {
                object: "list".to_string(),
                data: result.to_vec(),
            }),
        )
            .into_response(),
        Format::Csv => match convert_details_to_csv(result) {
            Ok(csv) => (status::StatusCode::OK, csv).into_response(),
            Err(e) => {
                tracing::error!("Error converting to CSV: {e}");
                (status::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        },
    }
}

fn convert_details_to_csv(
    workers: &[WorkerResponseItem],
) -> Result<String, Box<dyn std::error::Error>> {
    let mut w = Writer::from_writer(vec![]);
    for worker in workers {
        let _ = w.serialize(worker);
    }
    w.flush()?;
    Ok(String::from_utf8(w.into_inner()?)?)
}
