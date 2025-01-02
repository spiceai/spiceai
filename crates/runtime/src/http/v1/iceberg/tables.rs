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

use std::collections::HashMap;
use std::sync::Arc;

use super::{
    error::{IcebergResponseError, InternalServerErrorCode},
    namespace::{Namespace, NamespacePath},
};
use crate::datafusion::is_spice_internal_schema;
use crate::DataFusion;
use arrow::datatypes::Schema as ArrowSchema;
use axum::{
    extract::Path,
    http::status,
    response::{IntoResponse, Response},
    Extension, Json,
};
use datafusion::sql::TableReference;
use iceberg::{arrow::arrow_schema_to_schema, spec::Schema};
use serde::{Serialize, Serializer};
use uuid::Uuid;

const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

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
    Path((namespace, table)): Path<(NamespacePath, String)>,
) -> Response {
    let namespace = Namespace::from(namespace);
    let Some(table_reference) = table_reference(&namespace, &table) else {
        return status::StatusCode::NOT_FOUND.into_response();
    };

    match datafusion.get_table(&table_reference).await {
        Some(_) => status::StatusCode::OK.into_response(),
        None => status::StatusCode::NOT_FOUND.into_response(),
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct LoadTableResponse {
    metadata: TableMetadata,
}

#[derive(Debug)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
enum TableFormatVersion {
    V2,
}

impl Serialize for TableFormatVersion {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            TableFormatVersion::V2 => serializer.serialize_u8(2),
        }
    }
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct TableMetadata {
    format_version: TableFormatVersion,
    #[cfg_attr(feature = "openapi", schema(value_type=String, example="2b9da507-2c07-4bb3-9f0b-8df66a5e9e53"))]
    table_uuid: Uuid,
    location: String,

    /// Iceberg schemas, see `<https://apache.github.io/iceberg/spec/#schemas>`.
    #[cfg_attr(feature = "openapi", schema(value_type=Type::Object, example="2b9da507-2c07-4bb3-9f0b-8df66a5e9e53"))]
    schemas: Vec<Schema>,
}

/// Get a table.
///
/// This endpoint returns the table if it exists, otherwise it returns a 404 Not Found response.
#[cfg_attr(feature = "openapi", utoipa::path(
    get,
    path = "/v1/iceberg/namespaces/{namespace}/tables/{table}",
    operation_id = "get_table",
    tag = "Iceberg",
    params(
        ("namespace" = String, Path, description = "The namespace of the table."),
        ("table" = String, Path, description = "The name of the table.")
    ),
    responses(
        (status = 200, description = "Table exists", body = LoadTableResponse),
        (status = 404, description = "Table does not exist"),
        (status = 500, description = "An internal server error occurred while getting the table", content((
            IcebergResponseError = "application/json",
            example = json!({
                "error": {
                    "message": "Request failed. An internal server error occurred while getting the table.",
                    "r#type": "InternalServerError",
                    "code": 500
                }
            })
        )))
    )
))]
pub(crate) async fn get(
    Extension(datafusion): Extension<Arc<DataFusion>>,
    Path((namespace, table)): Path<(NamespacePath, String)>,
) -> Response {
    let namespace = Namespace::from(namespace);
    let Some(table_reference) = table_reference(&namespace, &table) else {
        return status::StatusCode::NOT_FOUND.into_response();
    };

    let Some(table) = datafusion.get_table(&table_reference).await else {
        return status::StatusCode::NOT_FOUND.into_response();
    };

    let arrow_schema = table.schema();
    let arrow_schema = assign_field_ids(&arrow_schema);
    let iceberg_schema = match arrow_schema_to_schema(&arrow_schema) {
        Ok(schema) => schema,
        Err(e) => {
            tracing::debug!(
                "Error converting arrow schema to iceberg schema for {table_reference}: {e}"
            );
            return IcebergResponseError::internal(InternalServerErrorCode::InvalidSchema)
                .into_response();
        }
    };

    let metadata = TableMetadata {
        format_version: TableFormatVersion::V2,
        table_uuid: Uuid::new_v4(),
        location: format!("spice.ai/{table_reference}"),
        schemas: vec![iceberg_schema],
    };

    let response = LoadTableResponse { metadata };

    (status::StatusCode::OK, Json(response)).into_response()
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

/// Iceberg requires field IDs to be set, and the iceberg-rust crate expects them to be set in the
/// `PARQUET:field_id` metadata key.
fn assign_field_ids(schema: &ArrowSchema) -> ArrowSchema {
    let mut fields = vec![];
    for (i, field) in schema.fields.iter().enumerate() {
        let field = Arc::unwrap_or_clone(Arc::clone(field));
        fields.push(field.with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            format!("{i}"),
        )])));
    }
    ArrowSchema::new(fields)
}
