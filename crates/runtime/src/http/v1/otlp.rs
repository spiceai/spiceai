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
    body::{Body, Bytes},
    extract::{MatchedPath, Request},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::MetricsService,
};
use prost::Message;

use super::require_write_access;
use crate::opentelemetry::Service as MetricsIngestService;

/// The OTLP/HTTP metrics route.
pub(crate) const PATH: &str = "/v1/metrics";

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

/// `google.rpc.Status`, the body OTLP/HTTP requires on every 4xx/5xx response. `details` is
/// left out; it is optional.
#[derive(Clone, PartialEq, prost::Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

/// gRPC code for an HTTP error status, for the `code` of a `google.rpc.Status` body.
fn grpc_code_for(status: StatusCode) -> tonic::Code {
    match status {
        StatusCode::BAD_REQUEST | StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            tonic::Code::InvalidArgument
        }
        StatusCode::PAYLOAD_TOO_LARGE | StatusCode::TOO_MANY_REQUESTS => {
            tonic::Code::ResourceExhausted
        }
        StatusCode::SERVICE_UNAVAILABLE => tonic::Code::Unavailable,
        StatusCode::FORBIDDEN => tonic::Code::PermissionDenied,
        StatusCode::UNAUTHORIZED => tonic::Code::Unauthenticated,
        _ => tonic::Code::Internal,
    }
}

/// An error response whose body is a `google.rpc.Status` in the request's encoding, with the
/// matching `Content-Type`, as OTLP/HTTP requires.
fn status_error(
    encoding_kind: Encoding,
    status: StatusCode,
    code: tonic::Code,
    message: impl Into<String>,
) -> Response {
    let rpc_status = RpcStatus {
        code: i32::from(code),
        message: message.into(),
    };
    let (content_type, body) = match encoding_kind {
        Encoding::Protobuf => (PROTOBUF_CONTENT_TYPE, rpc_status.encode_to_vec()),
        Encoding::Json => (
            JSON_CONTENT_TYPE,
            serde_json::json!({ "code": rpc_status.code, "message": rpc_status.message })
                .to_string()
                .into_bytes(),
        ),
    };
    let mut response = (status, body).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response.extensions_mut().insert(OtlpEncoded);
    response
}

/// Marks a response whose body is already an OTLP message in the request's encoding, so
/// [`encode_layer_errors`] leaves it alone.
#[derive(Debug, Clone, Copy)]
struct OtlpEncoded;

/// Largest error body read back from a layer in front of the route to reuse as the message.
const MAX_LAYER_ERROR_BODY_BYTES: usize = 64 * 1024;

/// Middleware for `/v1/metrics` that re-encodes errors from the layers in front of the handler
/// (API key auth, the mTLS gate, the body limit, extractor rejections) as a `google.rpc.Status`
/// in the request's encoding, since OTLP/HTTP requires that body on every 4xx/5xx response,
/// including 401 and 403. Other routes, successes, responses the handler already encoded, and
/// requests with an unsupported Content-Type pass through unchanged.
pub(crate) async fn encode_layer_errors(req: Request<Body>, next: Next) -> Response {
    let is_otlp = req
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| req.uri().path() == PATH, |path| path.as_str() == PATH);
    if !is_otlp {
        return next.run(req).await;
    }
    let encoding_kind = content_encoding(req.headers()).ok();
    let response = next.run(req).await;
    let status = response.status();
    let Some(encoding_kind) = encoding_kind else {
        return response;
    };
    if !(status.is_client_error() || status.is_server_error())
        || response.extensions().get::<OtlpEncoded>().is_some()
    {
        return response;
    }
    let (parts, body) = response.into_parts();
    let message = layer_error_message(body).await.unwrap_or_else(|| {
        status
            .canonical_reason()
            .unwrap_or("Request failed")
            .to_string()
    });
    let mut encoded = status_error(encoding_kind, status, grpc_code_for(status), message);
    // Keep headers such as `WWW-Authenticate`; the body and its type are replaced.
    for (name, value) in &parts.headers {
        if *name != header::CONTENT_TYPE && *name != header::CONTENT_LENGTH {
            encoded.headers_mut().append(name, value.clone());
        }
    }
    encoded
}

/// The message of a layer's error body: a JSON `message` field, or the text itself.
async fn layer_error_message(body: Body) -> Option<String> {
    let bytes = axum::body::to_bytes(body, MAX_LAYER_ERROR_BODY_BYTES)
        .await
        .ok()?;
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes)
        && let Some(message) = value.get("message").and_then(serde_json::Value::as_str)
    {
        return Some(message.to_string());
    }
    let text = String::from_utf8_lossy(&bytes).trim().to_string();
    (!text.is_empty()).then_some(text)
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
///
/// An error is the HTTP status and message, which the caller encodes for the request.
pub(crate) fn decompress(headers: &HeaderMap, body: Bytes) -> Result<Bytes, (StatusCode, String)> {
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
                    (
                        StatusCode::BAD_REQUEST,
                        format!("Failed to decompress gzip body: {e}"),
                    )
                })?;
            if read as u64 > MAX_DECOMPRESSED_BODY_BYTES {
                return Err((
                    StatusCode::PAYLOAD_TOO_LARGE,
                    format!(
                        "Decompressed body exceeds the {MAX_DECOMPRESSED_BODY_BYTES} byte limit."
                    ),
                ));
            }
            Ok(Bytes::from(decoded))
        }
        other => Err((
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
    response.extensions_mut().insert(OtlpEncoded);
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
        (status = 401, description = "Missing or invalid API key"),
        (status = 403, description = "Write access required"),
        (status = 413, description = "Body too large"),
        (status = 415, description = "Unsupported Content-Type or Content-Encoding"),
        (status = 503, description = "Failed to write to the metrics store; retry the export"),
    )
))]
pub(crate) async fn post(
    Extension(service): Extension<Arc<MetricsIngestService>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(resp) = require_write_access().await {
        // Answer in the request's encoding when it has one, like every other OTLP error.
        let Ok(encoding_kind) = content_encoding(&headers) else {
            return resp;
        };
        return status_error(
            encoding_kind,
            StatusCode::FORBIDDEN,
            tonic::Code::PermissionDenied,
            "API key does not allow write access",
        );
    }
    handle(&service, &headers, body).await
}

/// Decompresses and decodes an export, as [`decompress`] and [`decode`] do, on the blocking
/// pool: a gzip body can inflate to [`MAX_DECOMPRESSED_BODY_BYTES`], and inflating and decoding
/// that much would hold an HTTP runtime thread that also answers `/health`. An error is the
/// HTTP status and message, which the caller encodes for the request.
async fn decode_body(
    encoding_kind: Encoding,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<ExportMetricsServiceRequest, (StatusCode, String)> {
    let headers = headers.clone();
    tokio::task::spawn_blocking(move || {
        let body = decompress(&headers, body)?;
        decode(encoding_kind, &body).map_err(|message| (StatusCode::BAD_REQUEST, message))
    })
    .await
    .unwrap_or_else(|e| {
        Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to decode the OTLP metrics export: {e}"),
        ))
    })
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
    // From here the request's encoding is known, so errors are `google.rpc.Status` bodies.
    // An unsupported Content-Type above has no encoding to answer in and stays plain text.
    let request = match decode_body(encoding_kind, headers, body).await {
        Ok(request) => request,
        Err((status, message)) => {
            return status_error(encoding_kind, status, grpc_code_for(status), message);
        }
    };
    match service.export(tonic::Request::new(request)).await {
        Ok(response) => encode_response(encoding_kind, response.get_ref()),
        Err(status) => status_error(
            encoding_kind,
            status_code_for(status.code()),
            status.code(),
            status.message(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicBool, Ordering};

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

    /// A gzip body can inflate to `MAX_DECOMPRESSED_BODY_BYTES`; inflating and decoding it must
    /// not hold the HTTP runtime's thread, which also answers `/health`.
    #[tokio::test(flavor = "current_thread")]
    async fn gzip_bodies_are_decoded_off_the_async_runtime() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder
            .write_all(&vec![0_u8; 16 * 1024 * 1024])
            .expect("write");
        let compressed = Bytes::from(encoder.finish().expect("finish"));
        let decoded = Arc::new(AtomicBool::new(false));
        let other_task = tokio::spawn({
            let decoded = Arc::clone(&decoded);
            // On this single-threaded runtime the task runs only when the handler yields.
            async move { decoded.load(Ordering::SeqCst) }
        });
        let (status, message) = decode_body(
            Encoding::Protobuf,
            &headers(PROTOBUF_CONTENT_TYPE, Some("gzip")),
            compressed,
        )
        .await
        .expect_err("zero bytes are not a protobuf export");
        decoded.store(true, Ordering::SeqCst);
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            message.starts_with("Invalid OTLP protobuf body:"),
            "{message}"
        );
        assert!(
            !other_task.await.expect("task"),
            "another task ran only after the body was decoded"
        );
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
        let (status, _) = decompress(&headers(JSON_CONTENT_TYPE, Some("br")), Bytes::new())
            .expect_err("rejected");
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[test]
    fn error_bodies_are_rpc_status_in_the_request_encoding() {
        let resp = status_error(
            Encoding::Protobuf,
            StatusCode::SERVICE_UNAVAILABLE,
            tonic::Code::Unavailable,
            "retry",
        );
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers()[header::CONTENT_TYPE],
            HeaderValue::from_static(PROTOBUF_CONTENT_TYPE)
        );
        let resp = status_error(
            Encoding::Json,
            StatusCode::BAD_REQUEST,
            tonic::Code::InvalidArgument,
            "bad",
        );
        assert_eq!(
            resp.headers()[header::CONTENT_TYPE],
            HeaderValue::from_static(JSON_CONTENT_TYPE)
        );
        let decoded = RpcStatus::decode(
            RpcStatus {
                code: i32::from(tonic::Code::Unavailable),
                message: "retry".to_string(),
            }
            .encode_to_vec()
            .as_slice(),
        )
        .expect("decodes");
        assert_eq!(decoded.code, 14);
    }

    #[test]
    fn invalid_bodies_are_bad_requests() {
        let json_error = decode(Encoding::Json, b"{not json").expect_err("malformed JSON");
        assert!(
            json_error.starts_with("Invalid OTLP JSON body"),
            "{json_error}"
        );
        let protobuf_error =
            decode(Encoding::Protobuf, &[0xff, 0xff, 0xff]).expect_err("malformed protobuf");
        assert!(
            protobuf_error.starts_with("Invalid OTLP protobuf body"),
            "{protobuf_error}"
        );
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

    /// A router whose layer in front of `/v1/metrics` rejects like the API key auth layer does:
    /// plain text, with a `WWW-Authenticate` header.
    fn rejecting_router() -> axum::Router {
        async fn reject_unauthenticated(_req: Request<Body>, _next: Next) -> Response {
            let mut response =
                (StatusCode::UNAUTHORIZED, "Missing or invalid API key").into_response();
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            response
        }
        axum::Router::new()
            .route(PATH, axum::routing::post(|| async { StatusCode::OK }))
            .route(
                "/v1/other",
                axum::routing::post(|| async { StatusCode::OK }),
            )
            .route_layer(axum::middleware::from_fn(reject_unauthenticated))
            .route_layer(axum::middleware::from_fn(encode_layer_errors))
    }

    async fn send(router: axum::Router, path: &str, content_type: &str) -> Response {
        use tower::ServiceExt;
        let request = axum::http::Request::post(path)
            .header(header::CONTENT_TYPE, content_type)
            .body(Body::empty())
            .expect("request");
        router.oneshot(request).await.expect("response")
    }

    #[tokio::test]
    async fn layer_errors_are_rpc_status_in_the_request_encoding() {
        let response = send(rejecting_router(), PATH, PROTOBUF_CONTENT_TYPE).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            HeaderValue::from_static(PROTOBUF_CONTENT_TYPE)
        );
        assert_eq!(
            response.headers()[header::WWW_AUTHENTICATE],
            HeaderValue::from_static("Bearer")
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let status = RpcStatus::decode(body.as_ref()).expect("google.rpc.Status");
        assert_eq!(status.code, i32::from(tonic::Code::Unauthenticated));
        assert_eq!(status.message, "Missing or invalid API key");

        let response = send(rejecting_router(), PATH, JSON_CONTENT_TYPE).await;
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(json["code"], i32::from(tonic::Code::Unauthenticated));
        assert_eq!(json["message"], "Missing or invalid API key");
    }

    #[tokio::test]
    async fn layer_errors_pass_through_off_route_or_without_an_encoding() {
        let other = send(rejecting_router(), "/v1/other", PROTOBUF_CONTENT_TYPE).await;
        assert_eq!(
            other.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("text/plain; charset=utf-8"))
        );
        let unsupported = send(rejecting_router(), PATH, "text/csv").await;
        assert_eq!(unsupported.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            unsupported.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("text/plain; charset=utf-8"))
        );
    }

    #[test]
    fn handler_encoded_errors_are_not_re_encoded() {
        let response = status_error(
            Encoding::Json,
            StatusCode::BAD_REQUEST,
            tonic::Code::InvalidArgument,
            "bad",
        );
        assert!(response.extensions().get::<OtlpEncoded>().is_some());
    }
}
