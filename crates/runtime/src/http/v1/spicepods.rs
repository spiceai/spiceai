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

#[derive(Default, Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
pub struct SpicepodQueryParams {
    /// The format of the response. Possible values are 'json' (default) or 'csv'.
    #[serde(default)]
    pub format: Format,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SpicepodCsvRow {
    /// The name of the spicepod
    pub name: String,

    /// The version of the spicepod
    pub version: String,

    /// The number of datasets in this spicepod
    #[serde(default)]
    pub datasets_count: usize,

    /// The number of models in this spicepod
    #[serde(default)]
    pub models_count: usize,

    /// The number of dependencies in this spicepod
    #[serde(default)]
    pub dependencies_count: usize,
}

/// List Spicepods
///
/// Get a list of spicepods and their details. In CSV format, it will return a summarised form.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/spicepods",
    operation_id = "get_spicepods",
    tag = "General",
    params(SpicepodQueryParams),
    responses(
        (status = 200, description = "List of spicepods", content((
            // Don't use Vec<Spicepod> here, to avoid propagating the utoipa::ToSchema trait
            Vec<serde_json::Value> = "application/json",
            example = json!([
                {
                    "name": "spicepod1",
                    "version": "v1.0.0",
                    "datasets_count": 3,
                    "models_count": 2,
                    "dependencies_count": 5
                },
                {
                    "name": "spicepod2",
                    "version": "v2.0.0",
                    "datasets_count": 4,
                    "models_count": 3,
                    "dependencies_count": 2
                }
            ])
        ), (
            String = "text/csv",
            example = "name,version,datasets_count,models_count,dependencies_count\nspicepod1,v1.0.0,3,2,5\nspicepod2,v2.0.0,4,3,2"
        ))),
        (status = 500, description = "Internal server error", content((
            String, example = "Internal server error"
        )))
    )
))]
pub(crate) async fn get(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
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
