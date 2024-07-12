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
pub(crate) struct CatalogFilter {
    provider: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) struct CatalogResponseItem {
    pub provider: String,
    pub name: Option<String>,
}

const APPLICATION_JSON: MediaType = MediaType::from_parts(APPLICATION, JSON, None, &[]);
const TEXT_CSV: MediaType = MediaType::from_parts(TEXT, CSV, None, &[]);
const ACCEPT_LIST: &[MediaType; 2] = &[APPLICATION_JSON, TEXT_CSV];

pub(crate) async fn get(
    Extension(app): Extension<Arc<RwLock<Option<App>>>>,
    Query(filter): Query<CatalogFilter>,
    accept: Option<TypedHeader<Accept>>,
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
            provider: d.provider.clone(),
            name: d.catalog_id.clone(),
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
