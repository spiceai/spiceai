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

//! HTTP endpoints for prepared statements (parameterized queries).
//!
//! This module provides HTTP API support for server-side prepared statements,
//! similar to the functionality available through gRPC/ADBC (Flight SQL).
//!
//! # Workflow
//!
//! 1. **Prepare**: `POST /v1/sql/prepare` - Create a prepared statement and get schema info
//! 2. **Execute**: `POST /v1/sql/execute` - Execute a prepared statement with parameters
//!
//! # Example
//!
//! ```bash
//! # Prepare a statement
//! curl -X POST http://localhost:8090/v1/sql/prepare \
//!   -H "Content-Type: application/json" \
//!   -d '{"sql": "SELECT * FROM users WHERE id = $1 AND name = $2"}'
//!
//! # Execute with parameters
//! curl -X POST http://localhost:8090/v1/sql/execute \
//!   -H "Content-Type: application/json" \
//!   -d '{"handle": "<handle>", "parameters": [1, "Alice"]}'
//! ```

use std::sync::Arc;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use axum_extra::TypedHeader;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use datafusion::common::ParamValues;
use headers_accept::Accept;
use serde::{Deserialize, Serialize};

use crate::datafusion::{
    param_utils,
    query::QueryBuilder,
    request_context_extension::get_current_datafusion,
};
use runtime_request_context::{AsyncMarker, RequestContext};

use super::{ResponseMimeType, sql_to_http_response};

/// Request to prepare a SQL statement.
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PrepareRequest {
    /// The SQL query to prepare. Can include placeholders like $1, $2, or :name.
    pub sql: String,
}

/// Response from preparing a SQL statement.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PrepareResponse {
    /// Opaque handle to reference this prepared statement in subsequent execute calls.
    pub handle: String,

    /// Schema of the result set (column names and types).
    pub dataset_schema: SchemaInfo,

    /// Schema of the parameters (placeholder names and inferred types).
    /// Will be empty if the query has no parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameter_schema: Option<SchemaInfo>,
}

/// Schema information for result set or parameters.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SchemaInfo {
    /// List of fields in the schema.
    pub fields: Vec<FieldInfo>,
}

/// Information about a single field (column or parameter).
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FieldInfo {
    /// Name of the field (column name or parameter name like "$1" or "name").
    pub name: String,
    /// Data type of the field.
    pub data_type: String,
    /// Whether the field is nullable.
    pub nullable: bool,
}

/// Request to execute a prepared statement.
#[derive(Debug, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ExecuteRequest {
    /// The handle returned from the prepare endpoint.
    pub handle: String,

    /// Parameters to bind to the prepared statement.
    /// Can be a JSON array for positional parameters ($1, $2, ...)
    /// or a JSON object for named parameters (:name, :foo).
    #[serde(default)]
    pub parameters: serde_json::Value,
}

/// Internal structure to serialize/deserialize prepared statement handle.
#[derive(Debug, Serialize, Deserialize)]
struct PreparedStatementHandle {
    sql: String,
}

impl PreparedStatementHandle {
    fn encode(&self) -> Result<String, postcard::Error> {
        let bytes = postcard::to_stdvec(self)?;
        Ok(URL_SAFE_NO_PAD.encode(bytes))
    }

    fn decode(handle: &str) -> Result<Self, PreparedStatementDecodeError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(handle)
            .map_err(|_| PreparedStatementDecodeError::InvalidBase64)?;

        // Use take_from_bytes to get both the result and any remaining bytes
        // This prevents handle tampering by ensuring all bytes are consumed
        let (result, remaining): (Self, &[u8]) = postcard::take_from_bytes(&bytes)
            .map_err(|_| PreparedStatementDecodeError::InvalidHandle)?;

        // Reject if there are any trailing bytes (tampering detection)
        if !remaining.is_empty() {
            return Err(PreparedStatementDecodeError::TrailingBytes);
        }

        Ok(result)
    }
}

#[derive(Debug)]
enum PreparedStatementDecodeError {
    InvalidBase64,
    InvalidHandle,
    TrailingBytes,
}

impl std::fmt::Display for PreparedStatementDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBase64 => write!(f, "Invalid prepared statement handle: malformed encoding"),
            Self::InvalidHandle => write!(f, "Invalid prepared statement handle: corrupted or expired"),
            Self::TrailingBytes => write!(f, "Invalid prepared statement handle: tampered or malformed"),
        }
    }
}

fn arrow_schema_to_schema_info(schema: &arrow_schema::Schema) -> SchemaInfo {
    SchemaInfo {
        fields: schema
            .fields()
            .iter()
            .map(|f| FieldInfo {
                name: f.name().clone(),
                data_type: format!("{:?}", f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect(),
    }
}

/// Prepare SQL Statement
///
/// Prepare a SQL statement and return schema information.
///
/// This endpoint analyzes the SQL query and returns:
/// - A handle to reference the prepared statement
/// - The schema of the result set (column names and types)
/// - The schema of any parameters in the query
///
/// The handle can be used with `/v1/sql/execute` to run the query with parameters.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/sql/prepare",
    operation_id = "post_sql_prepare",
    tag = "SQL",
    request_body(
        description = "SQL query to prepare",
        content(
            (
                PrepareRequest = "application/json",
                example = json!({
                    "sql": "SELECT * FROM users WHERE id = $1 AND name = $2"
                })
            )
        )
    ),
    responses(
        (status = 200, description = "Statement prepared successfully", body = PrepareResponse, example = json!({
            "handle": "kAARAElOU0VSVCBJTlRPIHVzZXJzIChpZCwgbmFtZSkgVkFMVUVTICgkMSwgJDIp",
            "dataset_schema": {
                "fields": [
                    {"name": "id", "data_type": "Int64", "nullable": false},
                    {"name": "name", "data_type": "Utf8", "nullable": true}
                ]
            },
            "parameter_schema": {
                "fields": [
                    {"name": "$1", "data_type": "Int64", "nullable": false},
                    {"name": "$2", "data_type": "Utf8", "nullable": false}
                ]
            }
        })),
        (status = 400, description = "Invalid SQL query", content((
            String,
            example = "SQL syntax error: unexpected token"
        ))),
        (status = 500, description = "Internal server error")
    )
))]
pub(crate) async fn prepare(Json(request): Json<PrepareRequest>) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    let query = QueryBuilder::new(&request.sql, Arc::clone(&df)).build();

    let (dataset_schema, parameter_schema) = match query.get_schema().await {
        Ok(schemas) => schemas,
        Err(e) => {
            tracing::debug!("Error preparing statement: {e}");
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    let handle = PreparedStatementHandle {
        sql: request.sql,
    };

    let encoded_handle = match handle.encode() {
        Ok(h) => h,
        Err(e) => {
            tracing::error!("Error encoding prepared statement handle: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to create prepared statement handle",
            )
                .into_response();
        }
    };

    let response = PrepareResponse {
        handle: encoded_handle,
        dataset_schema: arrow_schema_to_schema_info(&dataset_schema),
        parameter_schema: parameter_schema.map(|s| arrow_schema_to_schema_info(&s)),
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// Execute Prepared Statement
///
/// Execute a previously prepared SQL statement with the given parameters.
///
/// This endpoint executes a prepared statement created via `/v1/sql/prepare`.
/// Parameters can be provided as:
/// - A JSON array for positional parameters (`$1`, `$2`, `?`, etc.)
/// - A JSON object for named parameters (`:name`, `:foo`, etc.)
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/sql/execute",
    operation_id = "post_sql_execute",
    tag = "SQL",
    params(
        ("Accept" = String, Header, description = "The format of the response, one of 'application/json' (default), 'application/vnd.spiceai.sql.v1+json', 'text/csv' or 'text/plain'."),
    ),
    request_body(
        description = "Prepared statement handle and parameters",
        content(
            (
                ExecuteRequest = "application/json",
                example = json!({
                    "handle": "kAARAElOU0VSVCBJTlRPIHVzZXJzIChpZCwgbmFtZSkgVkFMVUVTICgkMSwgJDIp",
                    "parameters": [1, "Alice"]
                })
            ),
            (
                ExecuteRequest = "application/json",
                example = json!({
                    "handle": "kAARAElOU0VSVCBJTlRPIHVzZXJzIChpZCwgbmFtZSkgVkFMVUVTICgkMSwgJDIp",
                    "parameters": {"id": 1, "name": "Alice"}
                })
            )
        )
    ),
    responses(
        (status = 200, description = "Query executed successfully", content((
            Vec<serde_json::Value> = "application/json",
            example = json!([
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ])
        ))),
        (status = 400, description = "Invalid handle or parameters", content((
            String,
            example = "Invalid prepared statement handle: corrupted or expired"
        ))),
        (status = 500, description = "Internal server error")
    )
))]
pub(crate) async fn execute(
    accept: Option<TypedHeader<Accept>>,
    Json(request): Json<ExecuteRequest>,
) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    // Decode the handle
    let handle = match PreparedStatementHandle::decode(&request.handle) {
        Ok(h) => h,
        Err(e) => {
            tracing::debug!("Error decoding prepared statement handle: {e}");
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    // Convert parameters
    let parameters: Option<ParamValues> = if request.parameters.is_null()
        || (request.parameters.is_array()
            && request
                .parameters
                .as_array()
                .map_or(true, Vec::is_empty))
        || (request.parameters.is_object()
            && request
                .parameters
                .as_object()
                .map_or(true, serde_json::Map::is_empty))
    {
        None
    } else {
        match param_utils::convert_json_to_param_values(request.parameters) {
            Ok(p) => Some(p),
            Err(e) => {
                tracing::debug!("Error converting parameters: {e}");
                return (StatusCode::BAD_REQUEST, format!("Invalid parameters: {e}")).into_response();
            }
        }
    };

    sql_to_http_response(
        df,
        &handle.sql,
        parameters,
        ResponseMimeType::from_accept_header(accept.as_ref()),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepared_statement_handle_encode_decode() {
        let handle = PreparedStatementHandle {
            sql: "SELECT * FROM users WHERE id = $1".to_string(),
        };

        let encoded = handle.encode().expect("should encode");
        let decoded = PreparedStatementHandle::decode(&encoded).expect("should decode");

        assert_eq!(handle.sql, decoded.sql);
    }

    #[test]
    fn test_prepared_statement_handle_decode_invalid_base64() {
        let result = PreparedStatementHandle::decode("not-valid-base64!!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_prepared_statement_handle_decode_invalid_content() {
        // Valid base64 but invalid postcard content
        let invalid = URL_SAFE_NO_PAD.encode(b"not a valid postcard message");
        let result = PreparedStatementHandle::decode(&invalid);
        assert!(result.is_err());
    }

    #[test]
    fn test_prepared_statement_handle_decode_trailing_bytes_rejected() {
        // Encode a valid handle, then append extra bytes to simulate tampering
        let handle = PreparedStatementHandle {
            sql: "SELECT 1".to_string(),
        };
        let mut bytes = postcard::to_stdvec(&handle).expect("encode");
        bytes.extend_from_slice(b"TAMPERED");  // Append garbage bytes
        let tampered_encoded = URL_SAFE_NO_PAD.encode(&bytes);

        let result = PreparedStatementHandle::decode(&tampered_encoded);
        assert!(result.is_err(), "Tampered handle with trailing bytes should be rejected");
    }
}
