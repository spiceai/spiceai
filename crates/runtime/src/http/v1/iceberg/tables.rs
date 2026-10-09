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

use super::{
    error::{IcebergResponseError, InternalServerErrorCode},
    namespace::{Namespace, NamespacePath},
    passthrough::{not_served_as_iceberg_message, passthrough_source},
};
use crate::datafusion::is_spice_internal_schema;
use crate::datafusion::request_context_extension::get_current_datafusion;
use axum::{
    extract::Path,
    http::{header, status},
    response::{IntoResponse, Response},
};
use datafusion::common::TableReference;
use iceberg::spec::TableMetadata;
use runtime_request_context::{AsyncMarker, RequestContext};
use serde::Serialize;

/// Check if a table exists.
///
/// This endpoint returns a 200 OK response if the table exists, otherwise it returns a 404 Not Found response.
#[cfg_attr(feature = "openapi", utoipa::path(
    head,
    path = "/v1/namespaces/{namespace}/tables/{table}",
    operation_id = "head_table",
    tag = "Iceberg",
    responses(
        (status = 200, description = "Table exists"),
        (status = 404, description = "Table does not exist")
    )
))]
pub(crate) async fn head(Path((namespace, table)): Path<(NamespacePath, String)>) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    let namespace = Namespace::from(namespace);
    let Some(table_reference) = table_reference(&namespace, &table) else {
        return status::StatusCode::NOT_FOUND.into_response();
    };

    match df.get_table(&table_reference).await {
        Some(_) => status::StatusCode::OK.into_response(),
        None => status::StatusCode::NOT_FOUND.into_response(),
    }
}

/// The Iceberg REST `LoadTableResult`: the table's metadata and where it is
/// stored. No `config` or `storage-credentials`: a client reads the table's
/// files with its own credentials.
#[derive(Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct LoadTableResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_location: Option<String>,
    /// Iceberg table metadata, see `<https://iceberg.apache.org/spec/#table-metadata>`.
    #[cfg_attr(feature = "openapi", schema(value_type = Object))]
    metadata: TableMetadata,
}

/// Load a table.
///
/// A table Spice reads unchanged from an Iceberg table (a federated `iceberg:`
/// or `glue:` dataset, a table in an Iceberg or Glue catalog, or an Iceberg DDL
/// table) is returned as that Iceberg table: its current metadata and metadata
/// location, loaded from its catalog on every request, so an Iceberg client
/// reads the snapshot Spice's next query reads. A column added to the table
/// outside Spice reaches the client at once, while Spice keeps the columns the
/// table had when it was registered until it is reloaded. Any other table
/// (accelerated, with columns Spice computes such as embeddings, a view, or a
/// source that is not Iceberg) is refused with `400`, naming the reason: query
/// it with SQL through Spice instead.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/tables/{table}",
    operation_id = "get_table",
    tag = "Iceberg",
    params(
        ("namespace" = String, Path, description = "The namespace of the table."),
        ("table" = String, Path, description = "The name of the table.")
    ),
    responses(
        (status = 200, description = "The Iceberg table Spice reads the table from", body = LoadTableResponse),
        (status = 400, description = "The table cannot be read as an Iceberg table", content((
            IcebergResponseError = "application/json",
            example = json!({
                "error": {
                    "message": "Failed to load table 'spice.public.orders' as an Iceberg table: it is accelerated, so Spice answers queries from its acceleration rather than from an Iceberg table that Iceberg clients can read directly. Query it with SQL through Spice instead, over HTTP (`/v1/sql`) or Arrow Flight SQL. See: https://spiceai.org/docs/api/HTTP/get-table",
                    "type": "BadRequestException",
                    "code": 400
                }
            })
        ))),
        (status = 404, description = "Table does not exist", content((
            IcebergResponseError = "application/json",
            example = json!({
                "error": {
                    "message": "Table 'spice.public.orders' does not exist",
                    "type": "NoSuchTableException",
                    "code": 404
                }
            })
        ))),
        (status = 503, description = "The table's Iceberg catalog could not be reached", content((
            IcebergResponseError = "application/json"
        )))
    )
))]
pub(crate) async fn get(Path((namespace, table)): Path<(NamespacePath, String)>) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    let namespace = Namespace::from(namespace);
    let Some(table_reference) = table_reference(&namespace, &table) else {
        return IcebergResponseError::no_such_table(no_such_table_message(&namespace, &table))
            .into_response();
    };
    let Some(provider) = df.get_table(&table_reference).await else {
        return IcebergResponseError::no_such_table(no_such_table_message(&namespace, &table))
            .into_response();
    };

    let source = match passthrough_source(&provider) {
        Ok(source) => source,
        Err(reason) => {
            return IcebergResponseError::bad_request(not_served_as_iceberg_message(
                &table_reference,
                reason,
            ))
            .into_response();
        }
    };

    // The table the provider scans, loaded the way the provider loads it before
    // every scan, so a client sees the snapshot Spice's next read would use.
    let loaded = match source.catalog().load_table(source.table_ident()).await {
        Ok(loaded) => loaded,
        Err(e) if e.kind() == iceberg::ErrorKind::TableNotFound => {
            return IcebergResponseError::no_such_table(no_such_table_message(&namespace, &table))
                .into_response();
        }
        Err(e) => {
            tracing::warn!(
                "Failed to load table '{table_reference}' from its Iceberg catalog, so Iceberg clients cannot load it through Spice until the catalog answers. Queries through Spice read the same catalog. Cause: {e}"
            );
            return IcebergResponseError::service_unavailable(format!(
                "Failed to load table '{table_reference}' from its Iceberg catalog. Retry, or query it with SQL through Spice."
            ))
            .into_response();
        }
    };

    match load_table_body(&loaded) {
        Ok(body) => (
            status::StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(e) => {
            tracing::warn!(
                "Failed to serialize the Iceberg metadata of table '{table_reference}', so Iceberg clients cannot load it through Spice. Cause: {e}"
            );
            IcebergResponseError::internal(InternalServerErrorCode::InvalidTableMetadata)
                .into_response()
        }
    }
}

/// The `LoadTableResult` body for `table`.
pub(super) fn load_table_body(table: &iceberg::table::Table) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&LoadTableResponse {
        metadata_location: table.metadata_location().map(str::to_string),
        metadata: table.metadata().clone(),
    })
}

fn no_such_table_message(namespace: &Namespace, table: &str) -> String {
    format!(
        "Table '{}.{table}' does not exist",
        namespace.parts.join(".")
    )
}

fn table_reference(namespace: &Namespace, table: &str) -> Option<TableReference> {
    if namespace.parts.len() != 2 {
        return None;
    }

    let catalog = namespace.parts[0].as_str();
    let schema = namespace.parts[1].as_str();

    if is_spice_internal_schema(catalog, schema) {
        return None;
    }

    Some(TableReference::full(catalog, schema, table))
}
