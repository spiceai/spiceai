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

use super::Format;

#[derive(Debug, Deserialize)]
pub(crate) struct ModelsQueryParams {
    #[serde(default)]
    format: Format,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) struct ModelResponse {
    pub name: String,
    pub from: String,
    pub datasets: Option<Vec<String>>,
}

pub(crate) async fn get(
    Extension(app): Extension<Arc<RwLock<Option<App>>>>,
    Query(params): Query<ModelsQueryParams>,
) -> Response {
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
