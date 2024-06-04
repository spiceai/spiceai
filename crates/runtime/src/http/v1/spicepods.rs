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
    response::{IntoResponse, Response},
    Extension, Json,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use spicepod::Spicepod;
use tokio::sync::RwLock;

use super::{convert_entry_to_csv, Format};

#[derive(Debug, Deserialize)]
pub(crate) struct SpicepodQueryParams {
    #[allow(dead_code)]
    #[serde(default)]
    status: bool,

    #[serde(default)]
    format: Format,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) struct SpicepodCsvRow {
    pub name: String,

    pub version: String,

    #[serde(default)]
    pub datasets_count: usize,

    #[serde(default)]
    pub models_count: usize,

    #[serde(default)]
    pub dependencies_count: usize,
}

pub(crate) async fn get(
    Extension(app): Extension<Arc<RwLock<Option<App>>>>,
    Query(params): Query<SpicepodQueryParams>,
) -> Response {
    let Some(readable_app) = &*app.read().await else {
        return (
            status::StatusCode::INTERNAL_SERVER_ERROR,
            Json::<Vec<Spicepod>>(vec![]),
        )
            .into_response();
    };

    match params.format {
        Format::Json => {
            (status::StatusCode::OK, Json(readable_app.spicepods.clone())).into_response()
        }
        Format::Csv => {
            let resp: Vec<SpicepodCsvRow> = readable_app
                .spicepods
                .iter()
                .map(|spod| SpicepodCsvRow {
                    version: spod.version.to_string(),
                    name: spod.name.clone(),
                    models_count: spod.models.len(),
                    datasets_count: spod.datasets.len(),
                    dependencies_count: spod.dependencies.len(),
                })
                .collect_vec();
            match convert_entry_to_csv(&resp) {
                Ok(csv) => (status::StatusCode::OK, csv).into_response(),
                Err(e) => {
                    tracing::error!("Error converting to CSV: {e}");
                    (status::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                }
            }
        }
    }
}
