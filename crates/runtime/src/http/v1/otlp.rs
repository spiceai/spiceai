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

//! OTLP/HTTP metrics receiver: `POST /v1/metrics` (#15060).
//!
//! Accepts `application/x-protobuf` and `application/json` bodies, optionally
//! `Content-Encoding: gzip`, and hands the decoded export to the same handler as the gRPC
//! receiver on the Flight port. The route sits on the authenticated HTTP router, so it has the
//! same API key, mTLS and request size limits as every other HTTP write.

use std::io::Read;
use std::sync::Arc;

use axum::{
    Extension,
    body::Bytes,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::MetricsService,
};
use prost::Message;

use super::require_write_access;
use crate::opentelemetry::Service as MetricsIngestService;

const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";
const JSON_CONTENT_TYPE: &str = "application/json";

/// Cap on a decompressed body, so a small gzip body cannot expand without bound. Matches the
/// authenticated router's limit on the compressed body.
pub(crate) const MAX_DECOMPRESSED_BODY_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Encoding {
    Protobuf,
    Json,
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, message.into()).into_response()
}

#[expect(
    clippy::result_large_err,
    reason = "the error is the HTTP response returned to the client as-is"
)]
fn content_encoding(headers: &HeaderMap) -> Result<Encoding, Response> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let mime = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match mime.as_str() {
        PROTOBUF_CONTENT_TYPE => Ok(Encoding::Protobuf),
        JSON_CONTENT_TYPE => Ok(Encoding::Json),
        _ => Err(error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!(
                "Unsupported Content-Type '{content_type}'. Send OTLP metrics as {PROTOBUF_CONTENT_TYPE} or {JSON_CONTENT_TYPE}."
            ),
        )),
    }
}

/// The body with any `Content-Encoding` removed.
#[expect(
    clippy::result_large_err,
    reason = "the error is the HTTP response returned to the client as-is"
)]
pub(crate) fn decompress(headers: &HeaderMap, body: Bytes) -> Result<Bytes, Response> {
    let encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match encoding.as_str() {
        "" | "identity" => Ok(body),
        "gzip" => {
            let mut decoded = Vec::new();
            let read = flate2::read::GzDecoder::new(body.as_ref())
                .take(MAX_DECOMPRESSED_BODY_BYTES + 1)
                .read_to_end(&mut decoded)
                .map_err(|e| {
                    error(
                        StatusCode::BAD_REQUEST,
                        format!("Failed to decompress gzip body: {e}"),
                    )
                })?;
            if read as u64 > MAX_DECOMPRESSED_BODY_BYTES {
                return Err(error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    format!(
                        "Decompressed body exceeds the {MAX_DECOMPRESSED_BODY_BYTES} byte limit."
                    ),
                ));
            }
            Ok(Bytes::from(decoded))
        }
        other => Err(error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            format!("Unsupported Content-Encoding '{other}'. Use gzip or no encoding."),
        )),
    }
}

/// Decodes an OTLP JSON export. The JSON mapping is decoded with `opentelemetry-proto` 0.33.1
/// and re-encoded to protobuf, which is wire-compatible with the types the receiver uses.
pub(crate) fn decode_json(body: &[u8]) -> Result<ExportMetricsServiceRequest, String> {
    let request: opentelemetry_proto_json::tonic::collector::metrics::v1::ExportMetricsServiceRequest =
        serde_json::from_slice(body).map_err(|e| format!("Invalid OTLP JSON body: {e}"))?;
    ExportMetricsServiceRequest::decode(request.encode_to_vec().as_slice())
        .map_err(|e| format!("Invalid OTLP JSON body: {e}"))
}

pub(crate) fn decode(
    encoding_kind: Encoding,
    body: &[u8],
) -> Result<ExportMetricsServiceRequest, String> {
    match encoding_kind {
        Encoding::Protobuf => ExportMetricsServiceRequest::decode(body)
            .map_err(|e| format!("Invalid OTLP protobuf body: {e}")),
        Encoding::Json => decode_json(body),
    }
}

fn encode_response(encoding_kind: Encoding, response: &ExportMetricsServiceResponse) -> Response {
    let (content_type, body) = match encoding_kind {
        Encoding::Protobuf => (PROTOBUF_CONTENT_TYPE, response.encode_to_vec()),
        Encoding::Json => {
            // OTLP JSON: camelCase field names, int64 as a string.
            let body = match &response.partial_success {
                Some(partial) => serde_json::json!({
                    "partialSuccess": {
                        "rejectedDataPoints": partial.rejected_data_points.to_string(),
                        "errorMessage": partial.error_message,
                    }
                }),
                None => serde_json::json!({}),
            };
            (JSON_CONTENT_TYPE, body.to_string().into_bytes())
        }
    };
    let mut response = (StatusCode::OK, body).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

fn status_code_for(code: tonic::Code) -> StatusCode {
    match code {
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Ingest OTLP metrics over HTTP
///
/// OpenTelemetry Protocol (OTLP/HTTP) metrics receiver. Accepts protobuf or JSON
/// `ExportMetricsServiceRequest` bodies, optionally gzip-compressed, and stores them like the
/// gRPC receiver on the Flight port.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/metrics",
    operation_id = "post_otlp_metrics",
    tag = "Datasets",
    request_body(
        description = "OTLP ExportMetricsServiceRequest",
        content(
            (Vec<u8> = "application/x-protobuf"),
            (serde_json::Value = "application/json"),
        )
    ),
    responses(
        (status = 200, description = "Export accepted, possibly with partial success"),
        (status = 400, description = "Invalid body, or every data point was rejected"),
        (status = 403, description = "Write access required"),
        (status = 413, description = "Body too large"),
        (status = 415, description = "Unsupported Content-Type or Content-Encoding"),
    )
))]
pub(crate) async fn post(
    Extension(service): Extension<Arc<MetricsIngestService>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(resp) = require_write_access().await {
        return resp;
    }
    handle(&service, &headers, body).await
}

pub(crate) async fn handle(
    service: &MetricsIngestService,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let encoding_kind = match content_encoding(headers) {
        Ok(kind) => kind,
        Err(resp) => return resp,
    };
    let body = match decompress(headers, body) {
        Ok(body) => body,
        Err(resp) => return resp,
    };
    let request = match decode(encoding_kind, &body) {
        Ok(request) => request,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    match service.export(tonic::Request::new(request)).await {
        Ok(response) => encode_response(encoding_kind, response.get_ref()),
        Err(status) => error(status_code_for(status.code()), status.message().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn json_body() -> &'static str {
        r#"{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"spiced"}}]},
            "scopeMetrics":[{"scope":{"name":"runtime"},"metrics":[{"name":"http.server.requests","unit":"{request}",
            "sum":{"aggregationTemporality":2,"isMonotonic":true,"dataPoints":[{"timeUnixNano":"1700000000000000000",
            "asInt":"42","attributes":[{"key":"route","value":{"stringValue":"/a"}}]}]}}]}]}]}"#
    }

    fn headers(content_type: &str, encoding: Option<&str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(content_type).expect("header"),
        );
        if let Some(encoding) = encoding {
            headers.insert(
                header::CONTENT_ENCODING,
                HeaderValue::from_str(encoding).expect("header"),
            );
        }
        headers
    }

    #[test]
    fn json_body_decodes_every_field_including_string_int64s() {
        let request = decode_json(json_body().as_bytes()).expect("decodes");
        let metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "http.server.requests");
        assert_eq!(metric.unit, "{request}");
        let Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(sum)) = &metric.data
        else {
            panic!("expected a sum");
        };
        assert_eq!(sum.aggregation_temporality, 2);
        assert!(sum.is_monotonic);
        let point = &sum.data_points[0];
        assert_eq!(point.time_unix_nano, 1_700_000_000_000_000_000);
        assert_eq!(
            point.value,
            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(42))
        );
        assert_eq!(point.attributes[0].key, "route");
    }

    #[test]
    fn protobuf_and_json_decode_to_the_same_request() {
        let from_json = decode_json(json_body().as_bytes()).expect("json");
        let protobuf = from_json.encode_to_vec();
        let from_protobuf = decode(Encoding::Protobuf, &protobuf).expect("protobuf");
        assert_eq!(from_json, from_protobuf);
    }

    #[test]
    fn gzip_bodies_are_decompressed() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(json_body().as_bytes()).expect("write");
        let compressed = encoder.finish().expect("finish");
        let body = decompress(
            &headers(JSON_CONTENT_TYPE, Some("gzip")),
            Bytes::from(compressed),
        )
        .expect("decompresses");
        assert_eq!(body.as_ref(), json_body().as_bytes());
    }

    #[test]
    fn content_type_parameters_are_ignored_and_unknown_types_rejected() {
        assert_eq!(
            content_encoding(&headers("application/json; charset=utf-8", None)).expect("json"),
            Encoding::Json
        );
        assert_eq!(
            content_encoding(&headers("application/x-protobuf", None)).expect("pb"),
            Encoding::Protobuf
        );
        let resp = content_encoding(&headers("text/plain", None)).expect_err("rejected");
        assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[test]
    fn unknown_content_encoding_is_rejected() {
        let resp = decompress(&headers(JSON_CONTENT_TYPE, Some("br")), Bytes::new())
            .expect_err("rejected");
        assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[test]
    fn invalid_bodies_are_bad_requests() {
        assert!(decode(Encoding::Json, b"{not json").is_err());
        assert!(decode(Encoding::Protobuf, &[0xff, 0xff, 0xff]).is_err());
    }

    #[test]
    fn json_partial_success_uses_the_otlp_json_mapping() {
        let response = encode_response(
            Encoding::Json,
            &ExportMetricsServiceResponse {
                partial_success: Some(
                    opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsPartialSuccess {
                        rejected_data_points: 3,
                        error_message: "Some data points were rejected".to_string(),
                    },
                ),
            },
        );
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            HeaderValue::from_static(JSON_CONTENT_TYPE)
        );
    }
}
