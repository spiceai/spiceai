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

//! Spice-side adjustments around rmcp's Streamable HTTP service on `/v1/mcp`.
//!
//! rmcp implements the transport. This module adds the checks that rmcp has
//! no setting for, and maps a few rmcp responses to the status codes and
//! bodies that the MCP specification and the MCP SDKs expect.

use std::{convert::Infallible, fmt::Display, sync::Arc};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, header};
use http_body::Frame;
use http_body_util::{BodyExt, BodyStream, Full, StreamBody, combinators::BoxBody};
use rmcp::{
    model::{ErrorCode, ProtocolVersion},
    transport::{
        common::http_header::{HEADER_MCP_PROTOCOL_VERSION, HEADER_SESSION_ID},
        streamable_http_server::{
            SessionManager, StreamableHttpService, session::local::LocalSessionManager,
        },
    },
};
use runtime_tools::mcp::server::RuntimeServer;
use serde::Deserialize;
use spicepod::component::runtime::{CorsConfig, McpOrigins};

pub(crate) type McpResponse = Response<BoxBody<Bytes, Infallible>>;

/// Same text rmcp uses, so clients see one message for a rejected `Origin`.
const ORIGIN_FORBIDDEN: &str = "Forbidden: Origin header is not allowed";

/// How the port of an allow-list entry is compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PortMatch {
    /// Any port matches. Used only for the localhost defaults.
    Any,
    /// The effective ports must be equal.
    Exact,
}

/// An RFC 6454 origin, parsed the way rmcp parses one.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WebOrigin {
    /// `Origin: null`, sent by opaque browser contexts.
    Null,
    Tuple(OriginTuple),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OriginTuple {
    scheme: String,
    host: String,
    /// The explicit port, else the default port of a known scheme.
    port: Option<u16>,
}

impl WebOrigin {
    fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        if value.eq_ignore_ascii_case("null") {
            return Some(Self::Null);
        }
        let uri = http::Uri::try_from(value).ok()?;
        let scheme = uri.scheme_str()?.to_ascii_lowercase();
        let authority = uri.authority()?;
        let port = authority.port_u16().or(match scheme.as_str() {
            "http" | "ws" => Some(80),
            "https" | "wss" => Some(443),
            _ => None,
        });
        Some(Self::Tuple(OriginTuple {
            host: authority
                .host()
                .trim_matches(['[', ']'])
                .to_ascii_lowercase(),
            scheme,
            port,
        }))
    }

    /// Whether `origin` matches this allow-list entry.
    fn matches(&self, origin: &Self, ports: PortMatch) -> bool {
        match (self, origin) {
            (Self::Null, Self::Null) => true,
            (Self::Tuple(allowed), Self::Tuple(origin)) => {
                allowed.scheme == origin.scheme
                    && allowed.host == origin.host
                    && (ports == PortMatch::Any || allowed.port == origin.port)
            }
            _ => false,
        }
    }
}

/// The `Origin` values `/v1/mcp` accepts.
///
/// rmcp treats an allow-list entry without a port as "any port". That suits
/// the localhost defaults, but an explicit `https://app.example.com` must not
/// accept `https://app.example.com:8443`, so Spice checks explicit entries
/// itself. The same policy decides the CORS headers on `/v1/mcp`.
#[derive(Debug, Clone)]
pub(crate) struct McpOriginPolicy {
    allowed: Arc<[WebOrigin]>,
    ports: PortMatch,
}

impl McpOriginPolicy {
    pub(crate) fn new(cors: &CorsConfig) -> Self {
        let origins = cors.mcp_origins();
        let ports = match origins {
            McpOrigins::LocalhostDefaults => PortMatch::Any,
            McpOrigins::Explicit(_) => PortMatch::Exact,
        };
        let allowed = origins
            .entries()
            .iter()
            .map(String::as_str)
            .filter_map(WebOrigin::parse)
            .collect();
        Self { allowed, ports }
    }

    /// `None` when `origin` is not a valid origin; rmcp answers those with 400.
    fn check(&self, origin: &HeaderValue) -> Option<bool> {
        let origin = WebOrigin::parse(origin.to_str().ok()?)?;
        Some(
            self.allowed
                .iter()
                .any(|allowed| allowed.matches(&origin, self.ports)),
        )
    }

    pub(crate) fn allows(&self, origin: &HeaderValue) -> bool {
        self.check(origin).unwrap_or(false)
    }
}

/// Whether an SSE stream may carry the empty-`data` priming events of
/// SEP-1699. They were added in `2025-11-25`; older SDKs try to parse the
/// empty `data` as JSON and log an error for each stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SsePriming {
    Supported,
    Unsupported,
}

impl SsePriming {
    /// `None` means no `MCP-Protocol-Version`, which the specification
    /// defines as `2025-03-26`.
    fn for_version(version: Option<&str>) -> Self {
        // Revisions are `YYYY-MM-DD` dates, so string order is release order.
        match version {
            Some(version) if version >= ProtocolVersion::V_2025_11_25.as_str() => Self::Supported,
            _ => Self::Unsupported,
        }
    }
}

/// The parts of a legacy `initialize` request body needed after rmcp answers.
#[derive(Deserialize)]
struct InitializeRequest {
    params: InitializeParams,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InitializeParams {
    protocol_version: String,
}

/// Request facts read before the request moves into rmcp.
struct RequestFacts {
    method: Method,
    session_id: Option<String>,
    protocol_version: Option<String>,
    json_content_type: bool,
}

impl RequestFacts {
    fn new(method: &Method, headers: &HeaderMap) -> Self {
        let header_str = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(ToString::to_string)
        };
        Self {
            method: method.clone(),
            session_id: header_str(HEADER_SESSION_ID),
            protocol_version: header_str(HEADER_MCP_PROTOCOL_VERSION),
            // Same test as rmcp's `Content-Type` check.
            json_content_type: headers
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("application/json")),
        }
    }
}

/// Wraps rmcp's [`StreamableHttpService`] for `/v1/mcp`.
#[derive(Clone)]
pub(crate) struct McpTransport {
    sessions: Arc<LocalSessionManager>,
    origins: McpOriginPolicy,
}

impl McpTransport {
    pub(crate) fn new(sessions: Arc<LocalSessionManager>, origins: McpOriginPolicy) -> Self {
        Self { sessions, origins }
    }

    pub(crate) async fn handle<B>(
        &self,
        service: &StreamableHttpService<RuntimeServer, LocalSessionManager>,
        request: Request<B>,
    ) -> McpResponse
    where
        B: http_body::Body + Send + 'static,
        B::Data: Send + 'static,
        B::Error: Display,
    {
        if let Some(origin) = request.headers().get(header::ORIGIN)
            && self.origins.check(origin) == Some(false)
        {
            tracing::debug!(?origin, "Rejected an MCP request with a disallowed Origin");
            return text_response(StatusCode::FORBIDDEN, ORIGIN_FORBIDDEN);
        }

        let facts = RequestFacts::new(request.method(), request.headers());
        match facts.method {
            Method::POST => self.handle_post(service, request, &facts).await,
            Method::DELETE => self.handle_delete(service, request, &facts).await,
            Method::GET => {
                let response = service.handle(request).await;
                if facts.session_id.is_some() {
                    filter_priming(
                        response,
                        SsePriming::for_version(facts.protocol_version.as_deref()),
                    )
                } else {
                    response
                }
            }
            _ => service.handle(request).await,
        }
    }

    async fn handle_post<B>(
        &self,
        service: &StreamableHttpService<RuntimeServer, LocalSessionManager>,
        request: Request<B>,
        facts: &RequestFacts,
    ) -> McpResponse
    where
        B: http_body::Body + Send + 'static,
        B::Data: Send + 'static,
        B::Error: Display,
    {
        // Keep the bytes: the 415 mapping and the `initialize` version read them.
        let (parts, body) = request.into_parts();
        let body = match body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                return text_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("Failed to read request body: {e}"),
                );
            }
        };
        let response = service
            .handle(Request::from_parts(parts, Full::new(body.clone())))
            .await;

        match response.status() {
            // rmcp: a legacy request other than `initialize` without
            // `Mcp-Session-Id`. The specification requires 400.
            StatusCode::UNPROCESSABLE_ENTITY => text_response(
                StatusCode::BAD_REQUEST,
                "Bad Request: Session ID is required. Send `initialize` first to create a session.",
            ),
            // rmcp: a JSON body that is not one JSON-RPC message. A wrong
            // `Content-Type` keeps its 415.
            StatusCode::UNSUPPORTED_MEDIA_TYPE if facts.json_content_type => {
                invalid_message_response(&body)
            }
            _ => {
                let priming = if facts.session_id.is_some() {
                    SsePriming::for_version(facts.protocol_version.as_deref())
                } else if response.headers().contains_key(HEADER_SESSION_ID) {
                    // A new legacy session: the version is in `initialize`.
                    let version = serde_json::from_slice::<InitializeRequest>(&body)
                        .ok()
                        .map(|init| init.params.protocol_version);
                    SsePriming::for_version(version.as_deref())
                } else {
                    // Sessionless streams have no priming event.
                    SsePriming::Supported
                };
                filter_priming(response, priming)
            }
        }
    }

    async fn handle_delete<B>(
        &self,
        service: &StreamableHttpService<RuntimeServer, LocalSessionManager>,
        request: Request<B>,
        facts: &RequestFacts,
    ) -> McpResponse
    where
        B: http_body::Body + Send + 'static,
        B::Data: Send + 'static,
        B::Error: Display,
    {
        let known = match facts.session_id.as_deref() {
            Some(id) => matches!(self.sessions.has_session(&id.into()).await, Ok(true)),
            None => false,
        };
        let response = service.handle(request).await;
        // rmcp answers 202 whether or not the session existed. SDKs accept
        // only 200 or 204, and the specification requires 404 for an unknown
        // session.
        if response.status() != StatusCode::ACCEPTED {
            return response;
        }
        if known {
            let (mut parts, body) = response.into_parts();
            parts.status = StatusCode::NO_CONTENT;
            Response::from_parts(parts, body)
        } else {
            text_response(StatusCode::NOT_FOUND, "Not Found: Session not found")
        }
    }
}

fn text_response(status: StatusCode, message: &str) -> McpResponse {
    let mut response = Response::new(Full::from(message.to_string()).boxed());
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// JSON-RPC error with a `null` id for a body that rmcp could not read as
/// one JSON-RPC message: `-32700` for invalid JSON, `-32600` for valid JSON
/// such as a batch.
fn invalid_message_response(body: &[u8]) -> McpResponse {
    let (code, message) = match serde_json::from_slice::<serde::de::IgnoredAny>(body) {
        Err(e) => (ErrorCode::PARSE_ERROR, format!("Parse error: {e}")),
        Ok(_) if body.trim_ascii_start().starts_with(b"[") => (
            ErrorCode::INVALID_REQUEST,
            "Invalid Request: JSON-RPC batches are not supported".to_string(),
        ),
        Ok(_) => (
            ErrorCode::INVALID_REQUEST,
            "Invalid Request: the body is not a JSON-RPC request, notification or response"
                .to_string(),
        ),
    };
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": null,
        "error": { "code": code.0, "message": message },
    });
    let mut response = Response::new(Full::from(body.to_string()).boxed());
    *response.status_mut() = StatusCode::BAD_REQUEST;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

/// Removes SSE priming events for clients that predate them.
fn filter_priming(response: McpResponse, priming: SsePriming) -> McpResponse {
    let is_sse = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"));
    if priming == SsePriming::Supported || !is_sse {
        return response;
    }
    let (parts, body) = response.into_parts();
    // Both `BodyStream` and `StreamBody` are a `Body` and a `Stream`, so
    // name the trait for each call.
    let frames = futures::StreamExt::filter(BodyStream::new(body), |frame| {
        futures::future::ready(!is_priming_event(frame))
    });
    Response::from_parts(parts, BodyExt::boxed(StreamBody::new(frames)))
}

/// rmcp writes each SSE event as one body frame, and a priming event is the
/// only event it writes with an empty `data` field.
fn is_priming_event(frame: &Result<Frame<Bytes>, Infallible>) -> bool {
    match frame {
        Ok(frame) => frame
            .data_ref()
            .is_some_and(|data| data.starts_with(b"data: \n") || data.starts_with(b"data:\n")),
        Err(never) => match *never {},
    }
}
