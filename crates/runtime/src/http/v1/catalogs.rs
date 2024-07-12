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

use crate::{component::catalog::Catalog, LogErrors, Runtime};
use app::App;
use axum::{
    extract::Query,
    http::status,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tract_core::tract_data::itertools::Itertools;

use crate::datafusion::DataFusion;

use super::{convert_entry_to_csv, Format};

#[derive(Debug, Deserialize)]
pub(crate) struct CatalogFilter {
    provider: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CatalogQueryParams {
    #[serde(default)]
    format: Format,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) struct CatalogResponseItem {
    pub from: String,
    pub name: String,
}

pub(crate) async fn get(
    Extension(app): Extension<Arc<RwLock<Option<App>>>>,
    Extension(df): Extension<Arc<DataFusion>>,
    Query(filter): Query<CatalogFilter>,
    Query(params): Query<CatalogQueryParams>,
) -> Response {
    let app_lock = app.read().await;
    let Some(readable_app) = &*app_lock else {
        return (
            status::StatusCode::INTERNAL_SERVER_ERROR,
            Json::<Vec<CatalogResponseItem>>(vec![]),
        )
            .into_response();
    };

    let valid_catalogs = Runtime::get_valid_catalogs(readable_app, LogErrors(false));
    let catalogs: Vec<Catalog> = match filter.provider {
        Some(provider) => valid_catalogs
            .into_iter()
            .filter(|d| d.provider == provider)
            .collect(),
        None => valid_catalogs,
    };

    let resp = catalogs
        .iter()
        .map(|d| CatalogResponseItem {
            from: d.provider.clone(),
            name: d.name.clone(),
        })
        .collect_vec();

    match params.format {
        Format::Json => (status::StatusCode::OK, Json(resp)).into_response(),
        Format::Csv => match convert_entry_to_csv(&resp) {
            Ok(csv) => (status::StatusCode::OK, csv).into_response(),
            Err(e) => {
                tracing::error!("Error converting to CSV: {e}");
                (status::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        },
    }
}
