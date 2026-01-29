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

use crate::{LogErrors, Runtime, component::catalog::Catalog};
use app::App;
use axum::{
    Extension, Json,
    extract::Query,
    http::status,
    response::{IntoResponse, Response},
};
use axum_extra::TypedHeader;
use headers_accept::Accept;
use mediatype::{
    MediaType,
    names::{APPLICATION, CSV, JSON, TEXT},
};
use serde::Deserialize;
use tokio::sync::RwLock;

use super::{Format, convert_entry_to_csv};

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
pub(crate) struct CatalogFilter {
    /// Filters catalogs by source (e.g., 'spiceai').
    from: Option<String>,
}

// Re-export shared type for backwards compatibility
pub use runtime_api_types::v1::CatalogInfo;
pub use runtime_api_types::v1::CatalogInfo as CatalogResponseItem;

const APPLICATION_JSON: MediaType = MediaType::from_parts(APPLICATION, JSON, None, &[]);
const TEXT_CSV: MediaType = MediaType::from_parts(TEXT, CSV, None, &[]);
const ACCEPT_LIST: &[MediaType; 2] = &[APPLICATION_JSON, TEXT_CSV];

/// List Catalogs
///
/// Returns a list of all registered catalogs (data sources). Catalogs provide metadata about schemas and tables available from external data sources.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/catalogs",
    operation_id = "get_catalogs",
    tag = "Catalogs",
    params(CatalogFilter),
    responses(
        (status = 200, description = "List of catalogs", content((
            CatalogInfo = "application/json",
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
    Extension(rt): Extension<Arc<Runtime>>,
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

    let valid_catalogs = rt.get_valid_catalogs(readable_app, LogErrors(false));
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
        .collect::<Vec<_>>();

    let mut format = Format::Json;
    if let Some(accept) = accept
        && let Some(media_type) = accept.negotiate(ACCEPT_LIST.iter())
        && (media_type.ty.as_str(), media_type.subty.as_str()) == ("text", "csv")
    {
        format = Format::Csv;
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
