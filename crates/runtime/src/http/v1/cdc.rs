/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! HTTP CDC ingest endpoint for Debezium source plugins → Spice (no Kafka).
//!
//! `POST /v1/datasets/{name}/cdc`
//!
//! Accepts Debezium change events as:
//! - **JSON** (`application/json`, `application/vnd.debezium+json`) — single
//!   object, array, or NDJSON
//! - **Avro** (`application/avro`, `application/vnd.debezium+avro`) — Confluent
//!   wire format with `schema_registry_url`, or raw Avro with `X-Avro-Schema` /
//!   dataset `avro_schema` param
//!
//! The request blocks until the change batch is applied to the accelerator
//! (ack), so Debezium Server / Embedded sinks can commit offsets safely.

use axum::{
    Json,
    body::Bytes,
    extract::Path,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::json;

use super::require_write_access;

#[cfg(feature = "debezium")]
use crate::dataconnector::cdc_ingest::{self, Error as IngestError};
#[cfg(feature = "debezium")]
use axum::http::header;
#[cfg(any(feature = "debezium", feature = "openapi"))]
use serde::Serialize;

#[cfg(any(feature = "debezium", feature = "openapi"))]
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(
    not(feature = "debezium"),
    expect(
        dead_code,
        reason = "constructed only on the debezium ingest path; still referenced by the OpenAPI schema and serialization derives"
    )
)]
pub struct CdcIngestResponse {
    /// Number of change rows applied.
    pub applied: usize,
    /// Dataset that received the changes.
    pub dataset: String,
}

/// Ingest Debezium CDC changes
///
/// Push Debezium change events (JSON or Avro) directly into an accelerated
/// dataset configured with `from: cdc:…` and `refresh_mode: changes`. No Kafka
/// is required — use this as a Debezium Server / Embedded Engine sink.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/datasets/{name}/cdc",
    operation_id = "post_dataset_cdc",
    tag = "Datasets",
    params(
        ("name" = String, Path, description = "Dataset name (must match a `from: cdc:…` dataset)")
    ),
    request_body(
        description = "Debezium change event(s) as JSON or Avro",
        content(
            (serde_json::Value = "application/json"),
            (serde_json::Value = "application/vnd.debezium+json"),
            (Vec<u8> = "application/avro"),
            (Vec<u8> = "application/vnd.debezium+avro"),
        )
    ),
    responses(
        (status = 200, description = "Changes applied", body = CdcIngestResponse),
        (status = 400, description = "Invalid body or format"),
        (status = 404, description = "Dataset not registered for CDC ingest"),
        (status = 403, description = "Write access required"),
        (status = 504, description = "Timed out waiting for capacity or apply"),
        (status = 503, description = "Change stream stopped (dataset unloaded or reloading)"),
        (status = 501, description = "CDC ingest requires the debezium feature in this build"),
    )
))]
pub(crate) async fn post(Path(name): Path<String>, headers: HeaderMap, body: Bytes) -> Response {
    if let Some(resp) = require_write_access().await {
        return resp;
    }

    #[cfg(not(feature = "debezium"))]
    {
        let _ = (name, headers, body);
        (
            StatusCode::NOT_IMPLEMENTED,
            Json(json!({
                "message": "CDC ingest requires the debezium feature to be enabled in this spiced build"
            })),
        )
            .into_response()
    }

    #[cfg(feature = "debezium")]
    {
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());
        let avro_schema = headers
            .get("x-avro-schema")
            .and_then(|v| v.to_str().ok())
            .map(ToString::to_string);

        match cdc_ingest::ingest_http_body(&name, content_type, &body, avro_schema).await {
            Ok(applied) => (
                StatusCode::OK,
                Json(CdcIngestResponse {
                    applied,
                    dataset: name,
                }),
            )
                .into_response(),
            Err(e) => ingest_error_response(&e),
        }
    }
}

#[cfg(feature = "debezium")]
fn ingest_error_response(err: &IngestError) -> Response {
    let (status, message) = match err {
        IngestError::NotRegistered { .. } => (StatusCode::NOT_FOUND, err.to_string()),
        IngestError::UnsupportedFormat { .. }
        | IngestError::Decode { .. }
        | IngestError::ApplyFailed { .. } => (StatusCode::BAD_REQUEST, err.to_string()),
        IngestError::ChannelClosed { .. } => (StatusCode::SERVICE_UNAVAILABLE, err.to_string()),
        IngestError::ApplyTimeout { .. } => (StatusCode::GATEWAY_TIMEOUT, err.to_string()),
    };
    (status, Json(json!({ "message": message }))).into_response()
}
