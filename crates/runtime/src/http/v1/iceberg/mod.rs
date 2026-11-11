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
use crate::{
    DataFusion,
    datafusion::{is_spice_internal_schema, request_context_extension::get_current_datafusion},
};
use axum::{
    Json,
    extract::{Path, Query},
    http::status,
    response::{IntoResponse, Response},
};
use error::IcebergResponseError;
use namespace::{Namespace, NamespacePath};
use runtime_request_context::{AsyncMarker, RequestContext};
use serde::{self, Deserialize, Serialize};

mod error;
pub mod namespace;
mod passthrough;
pub mod tables;

/// Get Iceberg API config
///
/// This endpoint returns the Iceberg Catalog API configuration, including details about overrides, defaults, and available endpoints.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/config",
    operation_id = "get_config",
    tag = "Iceberg",
    responses(
        (status = 200, description = "API configuration retrieved successfully", content((
            serde_json::Value = "application/json",
            example = json!({
                "overrides": {},
                "defaults": {},
                "endpoints": [
                    "GET /v1/{prefix}/namespaces",
                    "GET /v1/{prefix}/namespaces/{namespace}",
                    "HEAD /v1/{prefix}/namespaces/{namespace}",
                    "GET /v1/{prefix}/namespaces/{namespace}/tables",
                    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
                    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"
                ]
            })
        )))
    )
))]
pub(crate) async fn get_config() -> &'static str {
    CONFIG_RESPONSE
}

/// The Iceberg REST `ConfigResponse`.
///
/// `endpoints` uses the resource paths of the Iceberg REST spec, `{prefix}`
/// placeholder included: clients such as `PyIceberg` and Iceberg Java compare
/// these strings with their own endpoint constants and refuse any operation not
/// listed. No `prefix` override is returned, so clients call the paths without
/// one (`/v1/namespaces`), which is where the routes are served.
const CONFIG_RESPONSE: &str = r#"{
  "overrides": {},
  "defaults": {},
  "endpoints": [
    "GET /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"
  ]
}"#;

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

/// List Iceberg namespaces
///
/// This endpoint retrieves namespaces available in the Iceberg catalog.
/// If a `parent` namespace is provided, it will list the child namespaces under the specified parent.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/namespaces",
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
pub(crate) async fn get_namespaces(Query(params): Query<ParentNamespaceQueryParams>) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    match get_child_namespaces_impl(&df, &params.parent) {
        Ok(namespaces) => (
            status::StatusCode::OK,
            Json(NamespacesResponse { namespaces }),
        )
            .into_response(),
        Err(e) => e.into_response(),
    }
}

/// Check Namespace exists
///
/// This endpoint returns a 200 OK response if the namespace exists, otherwise it returns a 404 Not Found response.
#[cfg_attr(feature = "openapi", utoipa::path(
    head,
    path = "/v1/namespaces/{namespace}",
    operation_id = "head_namespace",
    tag = "Iceberg",
    responses(
        (status = 200, description = "Namespace exists"),
        (status = 404, description = "Namespace does not exist"),
        (status = 400, description = "Invalid namespace format")
    )
))]
pub(crate) async fn head_namespace(Path(namespace): Path<NamespacePath>) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    let namespace = Namespace::from(namespace);
    match get_child_namespaces_impl(&df, &namespace) {
        Ok(_) => status::StatusCode::OK.into_response(),
        Err(e) => e.into_response(),
    }
}

/// The Iceberg REST `GetNamespaceResponse`.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct GetNamespaceResponse {
    namespace: Namespace,
    /// Always `null`: Spice does not store namespace properties, and the spec
    /// asks a server without them to return `null` rather than `{}`.
    properties: Option<std::collections::HashMap<String, String>>,
}

/// Load an Iceberg namespace
///
/// Returns the namespace if it exists. Spice does not store namespace properties, so `properties` is `null`.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}",
    operation_id = "get_namespace",
    tag = "Iceberg",
    responses(
        (status = 200, description = "Namespace exists", content((
            GetNamespaceResponse = "application/json",
            example = json!({
                "namespace": ["spice", "public"],
                "properties": null
            })
        ))),
        (status = 404, description = "Namespace does not exist"),
        (status = 400, description = "Invalid namespace format")
    )
))]
pub(crate) async fn get_namespace(Path(namespace): Path<NamespacePath>) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    let namespace = Namespace::from(namespace);
    match get_child_namespaces_impl(&df, &namespace) {
        Ok(_) => (
            status::StatusCode::OK,
            Json(GetNamespaceResponse {
                namespace,
                properties: None,
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
                    "Namespace does not exist".to_string(),
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
                    "Namespace does not exist".to_string(),
                ));
            };
            let mut schema_names = catalog.schema_names().into_iter().filter(|schema_name| {
                !is_spice_internal_schema(catalog_name.as_str(), schema_name)
            });
            if !schema_names.any(|s| s == schema_name) {
                return Err(IcebergResponseError::no_such_namespace(
                    "Namespace does not exist".to_string(),
                ));
            }

            // There are no namespaces under this schema, so we return an empty list
            Ok(vec![])
        }
        3.. => Err(IcebergResponseError::bad_request(
            "Invalid namespace: maximum depth is 2 parts (catalog.schema)".to_string(),
        )),
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct TableIdentifier {
    namespace: Namespace,
    name: String,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct ListTablesResponse {
    identifiers: Vec<TableIdentifier>,
}

#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/tables",
    operation_id = "list_tables",
    tag = "Iceberg",
    responses(
        (status = 200, description = "Tables retrieved successfully", content((
            ListTablesResponse = "application/json",
            example = json!({
                "identifiers": [
                    { "namespace": { "parts": ["catalog_a"] }, "name": "table_1" },
                    { "namespace": { "parts": ["catalog_b", "schema_1"] }, "name": "table_2" }
                ]
            })
        ))),
        (status = 404, description = "Namespace does not exist"),
        (status = 400, description = "Invalid namespace format")
    )
))]
pub(crate) async fn list_tables(Path(namespace): Path<NamespacePath>) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    let namespace = Namespace::from(namespace);

    match namespace.parts.len() {
        // If only catalog is specified, return empty list
        1 => {
            let catalog_name = &namespace.parts[0];
            if df.ctx.catalog(catalog_name).is_none() {
                return IcebergResponseError::no_such_namespace(format!(
                    "Catalog '{catalog_name}' does not exist"
                ))
                .into_response();
            }
            (
                status::StatusCode::OK,
                Json(ListTablesResponse {
                    identifiers: Vec::new(),
                }),
            )
                .into_response()
        }

        // For catalog + schema, list the tables
        2 => {
            let catalog_name = &namespace.parts[0];
            let schema_name = &namespace.parts[1];

            if is_spice_internal_schema(catalog_name, schema_name) {
                return IcebergResponseError::no_such_namespace(
                    "Namespace does not exist".to_string(),
                )
                .into_response();
            }

            // Get the catalog
            let Some(catalog) = df.ctx.catalog(catalog_name) else {
                return IcebergResponseError::no_such_namespace(format!(
                    "Catalog '{catalog_name}' does not exist"
                ))
                .into_response();
            };

            // Get the schema and its tables
            let Some(schema) = catalog.schema(schema_name) else {
                return IcebergResponseError::no_such_namespace(format!(
                    "Schema '{schema_name}' does not exist in catalog '{catalog_name}'"
                ))
                .into_response();
            };
            let table_names = schema.table_names();

            // Convert to TableIdentifier format
            let identifiers: Vec<TableIdentifier> = table_names
                .into_iter()
                .map(|table_name| TableIdentifier {
                    namespace: namespace.clone(),
                    name: table_name,
                })
                .collect();

            (
                status::StatusCode::OK,
                Json(ListTablesResponse { identifiers }),
            )
                .into_response()
        }

        // For invalid namespace lengths (0 or > 2), return error
        _ => IcebergResponseError::bad_request(
            "Invalid namespace: must specify catalog and optionally schema".to_string(),
        )
        .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::{CONFIG_RESPONSE, GetNamespaceResponse, Namespace};

    /// `loadNamespaceMetadata` answers with the spec's `GetNamespaceResponse`,
    /// which `PyIceberg` validates: `namespace` is required, and `properties`
    /// is `null` for a server that does not store namespace properties.
    #[test]
    fn load_namespace_returns_get_namespace_response() {
        let body = serde_json::to_value(GetNamespaceResponse {
            namespace: Namespace::from_parts(vec!["spice".to_string(), "public".to_string()]),
            properties: None,
        })
        .expect("serializes");
        assert_eq!(
            body,
            serde_json::json!({ "namespace": ["spice", "public"], "properties": null })
        );
    }

    /// Iceberg REST clients refuse any operation whose endpoint the config does
    /// not list verbatim in the spec's form (`GET /v1/{prefix}/namespaces`), so
    /// the list has to name exactly the routes served, in that form.
    #[test]
    fn config_advertises_served_routes_in_the_spec_form() {
        let config: serde_json::Value =
            serde_json::from_str(CONFIG_RESPONSE).expect("the config response is JSON");
        let endpoints: Vec<&str> = config["endpoints"]
            .as_array()
            .expect("endpoints is an array")
            .iter()
            .map(|endpoint| endpoint.as_str().expect("each endpoint is a string"))
            .collect();

        assert_eq!(
            endpoints,
            [
                "GET /v1/{prefix}/namespaces",
                "GET /v1/{prefix}/namespaces/{namespace}",
                "HEAD /v1/{prefix}/namespaces/{namespace}",
                "GET /v1/{prefix}/namespaces/{namespace}/tables",
                "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
                "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            ]
        );
        assert_eq!(
            config["overrides"],
            serde_json::json!({}),
            "no prefix override"
        );
    }
}
