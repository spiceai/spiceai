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

mod error;
mod namespace;

/// Get Iceberg Catalog API configuration.
///
/// This endpoint returns the Iceberg Catalog API configuration, including details about overrides, defaults, and available endpoints.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/iceberg/config",
    operation_id = "get_config",
    tag = "Iceberg",
    responses(
        (status = 200, description = "API configuration retrieved successfully", content((
            serde_json::Value = "application/json",
            example = json!({
                "overrides": {},
                "defaults": {},
                "endpoints": [
                    "GET /v1/iceberg/namespaces",
                    "HEAD /v1/iceberg/namespaces/{namespace}",
                    "GET /v1/iceberg/namespaces/{namespace}/tables",
                    "HEAD /v1/iceberg/namespaces/{namespace}/tables/{table}",
                    "GET /v1/iceberg/namespaces/{namespace}/tables/{table}"
                ]
            })
        )))
    )
))]
pub(crate) async fn get_config() -> &'static str {
    r#"{
  "overrides": {},
  "defaults": {},
  "endpoints": [
    "GET /v1/iceberg/namespaces",
    "HEAD /v1/iceberg/namespaces/{namespace}",
    "GET /v1/iceberg/namespaces/{namespace}/tables",
    "HEAD /v1/iceberg/namespaces/{namespace}/tables/{table}",
    "GET /v1/iceberg/namespaces/{namespace}/tables/{table}"
  ]
}"#
}

#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
pub(crate) struct ParentNamespaceQueryParams {
    /// The parent namespace from which to retrieve child namespaces.
    #[serde(default)]
    parent: Namespace,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct NamespacesResponse {
    namespaces: Vec<Namespace>,
}

/// Get a list of namespaces.
///
/// This endpoint retrieves namespaces available in the Iceberg catalog.
/// If a `parent` namespace is provided, it will list the child namespaces under the specified parent.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/iceberg/namespaces",
    operation_id = "get_iceberg_namespaces",
    tag = "Iceberg",
    params(ParentNamespaceQueryParams),
    responses(
        (status = 200, description = "Namespaces retrieved successfully", content((
            NamespacesResponse = "application/json",
            example = json!({
                "namespaces": [
                    { "parts": ["catalog_a"] },
                    { "parts": ["catalog_b", "schema_1"] }
                ]
            })
        ))),
        (status = 404, description = "Namespace not found", content((
            IcebergResponseError = "application/json",
            example = json!({
                "error": {
                    "message": "Namespace provided does not exist",
                    "type": "NoSuchNamespaceException",
                    "code": 404
                }
            })
        ))),
        (status = 400, description = "Bad request", content((
            IcebergResponseError = "application/json",
            example = json!({
                "error": {
                    "message": "Invalid namespace request",
                    "type": "BadRequestException",
                    "code": 400
                }
            })
        ))),
        (status = 500, description = "Internal server error", content((
            IcebergResponseError = "application/json",
            example = json!({
                "error": {
                    "message": "Internal Server Error: DF_SCHEMA_NOT_FOUND",
                    "type": "InternalServerError",
                    "code": 500
                }
            })
        )))
    )
))]
pub(crate) async fn get_namespaces(
    Extension(datafusion): Extension<Arc<DataFusion>>,
    Query(params): Query<ParentNamespaceQueryParams>,
) -> Response {
    match get_child_namespaces_impl(&datafusion, &params.parent) {
        Ok(namespaces) => (
            status::StatusCode::OK,
            Json(NamespacesResponse { namespaces }),
        )
            .into_response(),
        Err(e) => e.into_response(),
    }
}

/// Check if a namespace exists.
///
/// This endpoint returns a 200 OK response if the namespace exists, otherwise it returns a 404 Not Found response.
#[cfg_attr(feature = "openapi", utoipa::path(
    head,
    path = "/v1/iceberg/namespaces/{namespace}",
    operation_id = "head_namespace",
    tag = "Iceberg",
    responses(
        (status = 200, description = "Namespace exists"),
        (status = 404, description = "Namespace does not exist")
    )
))]
pub(crate) async fn head_namespace(
    Extension(datafusion): Extension<Arc<DataFusion>>,
    Path(namespace): Path<NamespacePath>,
) -> Response {
    let namespace = Namespace::from(namespace);
    match get_child_namespaces_impl(&datafusion, &namespace) {
        Ok(_) => status::StatusCode::OK.into_response(),
        Err(e) => e.into_response(),
    }
}

/// Check if a namespace exists.
///
/// This endpoint returns a 200 OK response if the namespace exists, otherwise it returns a 404 Not Found response.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/iceberg/namespaces/{namespace}",
    operation_id = "get_namespace",
    tag = "Iceberg",
    responses(
        (status = 200, description = "Namespace exists"),
        (status = 404, description = "Namespace does not exist")
    )
))]
pub(crate) async fn get_namespace(
    Extension(datafusion): Extension<Arc<DataFusion>>,
    Path(namespace): Path<NamespacePath>,
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

fn get_child_namespaces_impl(
    datafusion: &DataFusion,
    namespace: &Namespace,
) -> Result<Vec<Namespace>, IcebergResponseError> {
    let catalog_names = datafusion.ctx.catalog_names();

    match namespace.parts.len() {
        0 => {
            let namespaces = catalog_names
                .into_iter()
                .map(Namespace::from_single_part)
                .collect();
            Ok(namespaces)
        }
        1 => {
            // The user has specified a single namespace, so we want to return all of the schemas in that namespace
            let catalog_name = namespace.parts[0].clone();
            let Some(catalog) = datafusion.ctx.catalog(catalog_name.as_str()) else {
                return Err(IcebergResponseError::no_such_namespace(
                    "Namespace provided in the parent does not exist".to_string(),
                ));
            };

            let schema_names = catalog.schema_names().into_iter().filter(|schema_name| {
                !is_spice_internal_schema(catalog_name.as_str(), schema_name)
            });
            let namespaces = schema_names
                .map(|schema_name| Namespace::from_parts(vec![catalog_name.clone(), schema_name]))
                .collect();
            Ok(namespaces)
        }
        2 => {
            let catalog_name = namespace.parts[0].clone();
            let schema_name = namespace.parts[1].clone();
            let Some(catalog) = datafusion.ctx.catalog(catalog_name.as_str()) else {
                return Err(IcebergResponseError::no_such_namespace(
                    "Namespace provided in the parent does not exist".to_string(),
                ));
            };
            let mut schema_names = catalog.schema_names().into_iter().filter(|schema_name| {
                !is_spice_internal_schema(catalog_name.as_str(), schema_name)
            });
            if !schema_names.any(|s| s == schema_name) {
                return Err(IcebergResponseError::no_such_namespace(
                    "Namespace provided in the parent does not exist".to_string(),
                ));
            }

            // There are no namespaces under this schema, so we return an empty list
            Ok(vec![])
        }
        3.. => Err(IcebergResponseError::no_such_namespace(
            "Namespace provided in the parent does not exist".to_string(),
        )),
    }
}
