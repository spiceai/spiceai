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

use crate::{datafusion::is_spice_internal_schema, DataFusion};
use axum::{
    extract::{Path, Query},
    http::status,
    response::{IntoResponse, Response},
    Extension, Json,
};
use error::IcebergResponseError;
use namespace::{Namespace, NamespacePath};
use serde::{self, Deserialize, Serialize};

/// Check if a table exists.
///
/// This endpoint returns a 200 OK response if the table exists, otherwise it returns a 404 Not Found response.
#[cfg_attr(feature = "openapi", utoipa::path(
    head,
    path = "/v1/iceberg/namespaces/{namespace}/tables/{table}",
    operation_id = "head_table",
    tag = "Iceberg",
    responses(
        (status = 200, description = "Table exists"),
        (status = 404, description = "Table does not exist")
    )
))]
pub(crate) async fn head(
    Extension(datafusion): Extension<Arc<DataFusion>>,
    Path(namespace): Path<NamespacePath>,
    Path(table): Path<String>,
) -> Response {
    let namespace = Namespace::from(namespace);
    match get_child_namespaces_impl(&datafusion, &namespace) {
        Ok(_) => status::StatusCode::OK.into_response(),
        Err(e) => e.into_response(),
    }
}

/// Get a table.
///
/// This endpoint returns the table if it exists, otherwise it returns a 404 Not Found response.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/iceberg/namespaces/{namespace}/tables/{table}",
    operation_id = "get_table",
    tag = "Iceberg",
    responses(
        (status = 200, description = "Table exists"),
        (status = 404, description = "Table does not exist")
    )
))]
pub(crate) async fn get(
    Extension(datafusion): Extension<Arc<DataFusion>>,
    Path(namespace): Path<NamespacePath>,
    Path(table): Path<String>,
) -> Response {
    let namespace = Namespace::from(namespace);
    match get_child_namespaces_impl(&datafusion, &namespace) {
        Ok(_) => (
            status::StatusCode::OK,
            Json(NamespacesResponse {
                namespaces: vec![namespace],
            }),
        )
            .into_response(),
        Err(e) => e.into_response(),
    }
}
