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

use crate::{component::catalog::Catalog, LogErrors, Runtime};
use app::App;
use axum::{
    extract::Query,
    http::status,
    response::{IntoResponse, Response},
    Extension, Json,
};
use axum_extra::TypedHeader;
use headers_accept::Accept;
use mediatype::{
    names::{APPLICATION, CSV, JSON, TEXT},
    MediaType,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tract_core::tract_data::itertools::Itertools;

use super::{convert_entry_to_csv, Format};

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
pub(crate) struct CatalogFilter {
    /// Filters catalogs by source (e.g., 'spiceai').
    from: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub(crate) struct CatalogResponseItem {
    pub from: String,
    pub name: String,
}

const APPLICATION_JSON: MediaType = MediaType::from_parts(APPLICATION, JSON, None, &[]);
const TEXT_CSV: MediaType = MediaType::from_parts(TEXT, CSV, None, &[]);
const ACCEPT_LIST: &[MediaType; 2] = &[APPLICATION_JSON, TEXT_CSV];

/// List Catalogs
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/catalogs",
    operation_id = "get_catalogs",
    tag = "Datasets",
    params(CatalogFilter),
    responses(
        (status = 200, description = "List of catalogs", content((
            CatalogResponseItem = "application/json",
            example = json!([
                {
                    "from": "spiceai",
                    "name": "spiceai"
                }
            ])
        ), (
            String = "text/csv",
            example = "
from,name
spiceai,spiceai
"
        ))),
        (status = 500, description = "Internal server error occurred while processing catalogs", content((
            serde_json::Value = "application/json",
            example = json!({
                "error": "An unexpected error occurred while processing the catalogs"
            })
        )))
    )
))]
pub(crate) async fn get(
    Extension(app): Extension<Arc<RwLock<Option<Arc<App>>>>>,
    Query(filter): Query<CatalogFilter>,
    accept: Option<TypedHeader<Accept>>,
) -> Response {
    let app_lock = app.read().await;
    let Some(readable_app) = app_lock.as_ref() else {
        return (
            status::StatusCode::INTERNAL_SERVER_ERROR,
            Json::<Vec<CatalogResponseItem>>(vec![]),
        )
            .into_response();
    };

    let valid_catalogs = Runtime::get_valid_catalogs(readable_app, LogErrors(false));
    let catalogs: Vec<Catalog> = match filter.from {
        Some(provider) => valid_catalogs
            .into_iter()
            .filter(|d| d.provider == provider)
            .collect(),
        None => valid_catalogs,
    };

    let resp = catalogs
        .iter()
        .map(|d| CatalogResponseItem {
            from: d.from.clone(),
            name: d.name.clone(),
        })
        .collect_vec();

    let mut format = Format::Json;
    if let Some(accept) = accept {
        if let Some(media_type) = accept.negotiate(ACCEPT_LIST.iter()) {
            if let ("text", "csv") = (media_type.ty.as_str(), media_type.subty.as_str()) {
                format = Format::Csv;
            }
        }
    }

    match format {
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
