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

//! Streamable HTTP backend for MCP tool catalogs that bounds every response body.
//!
//! rmcp's `reqwest::Client` backend reads an `application/json` POST response,
//! and the body of a non-success response, into memory in full; its
//! `max_sse_event_size` only bounds SSE events. The MCP server picks the
//! response type, so through that backend it decides how much the client
//! buffers. [`BoundedHttpClient`] owns the POST path and stops reading a body
//! at [`MAX_TOOL_RESULT_SIZE`]. GET streams carry only SSE, so they are
//! delegated to rmcp, whose per-event limit the transport config sets to the
//! same value.
//!
//! The POST handling mirrors rmcp 3.3.0's `reqwest` backend
//! (`transport/common/reqwest/streamable_http_client.rs`); keep the two in step
//! when rmcp is upgraded.

use std::{
    borrow::Cow,
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Stream, StreamExt, stream::BoxStream};
use http::{
    HeaderName, HeaderValue, StatusCode,
    header::{ACCEPT, CONTENT_TYPE, WWW_AUTHENTICATE},
};
use rmcp::{
    model::{
        ClientJsonRpcMessage, ClientRequest, ErrorData, JsonRpcMessage, RequestId,
        ServerJsonRpcMessage,
    },
    transport::{
        common::http_header::{
            EVENT_STREAM_MIME_TYPE, HEADER_LAST_EVENT_ID, HEADER_MCP_PROTOCOL_VERSION,
            HEADER_SESSION_ID, JSON_MIME_TYPE,
        },
        streamable_http_client::{
            AuthRequiredError, InsufficientScopeError, SseError, StreamableHttpClient,
            StreamableHttpError, StreamableHttpPostResponse,
        },
    },
};
use snafu::Snafu;
use sse_stream::{Sse, SseStream};

use super::MAX_TOOL_RESULT_SIZE;

type HttpResult<T> = Result<T, StreamableHttpError<reqwest::Error>>;

/// The single-line message for a response that exceeded [`MAX_TOOL_RESULT_SIZE`].
///
/// It is both the `WARN` log line and the error the failed request surfaces,
/// so it never includes any of the response itself.
pub(crate) fn response_too_large_message(server_name: &str) -> String {
    format!(
        "MCP server '{server_name}' sent a response larger than the {} MiB limit, so the request failed and the response was discarded. Make the MCP tool return a smaller result, for example by paginating it. See https://spiceai.org/docs/components/tools/mcp",
        MAX_TOOL_RESULT_SIZE / (1024 * 1024)
    )
}

#[derive(Debug, Snafu)]
enum ResponseBodyError {
    #[snafu(display("{message}"))]
    TooLarge { message: String },
    #[snafu(display("{source}"))]
    Read { source: reqwest::Error },
}

/// Streamable HTTP client whose responses are never buffered past
/// [`MAX_TOOL_RESULT_SIZE`].
#[derive(Clone)]
pub(crate) struct BoundedHttpClient {
    inner: reqwest::Client,
    /// Spicepod name of the MCP tool catalog, for the over-limit message.
    server_name: Arc<str>,
}

impl BoundedHttpClient {
    pub(crate) fn new(server_name: &str) -> reqwest::Result<Self> {
        // The same settings rmcp's default client uses: no idle pooling (a
        // pooled connection whose previous body was not fully read stalls on
        // delayed ACK), and no redirects, so custom headers such as an API key
        // are never replayed to a redirect target.
        let inner = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            inner,
            server_name: server_name.into(),
        })
    }

    fn too_large(&self) -> String {
        let message = response_too_large_message(&self.server_name);
        tracing::warn!("{message}");
        message
    }

    /// The over-limit failure for a POST whose body was not read in full.
    ///
    /// A request gets a JSON-RPC error, so only that call fails. A transport
    /// error would reach the caller as a closed transport, which reconnects
    /// and sends the same request again.
    fn too_large_response(
        &self,
        request_id: Option<RequestId>,
        session_id: Option<String>,
    ) -> HttpResult<StreamableHttpPostResponse> {
        let message = self.too_large();
        match request_id {
            Some(id) => Ok(StreamableHttpPostResponse::Json(
                too_large_error(message, id),
                session_id,
            )),
            None => Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(
                message,
            ))),
        }
    }

    /// Reads the whole body, or returns `None` as soon as it is known to
    /// exceed [`MAX_TOOL_RESULT_SIZE`] — from `Content-Length` before reading
    /// anything, otherwise once the bytes received cross the limit.
    async fn read_body(
        &self,
        mut response: reqwest::Response,
    ) -> Result<Option<Vec<u8>>, reqwest::Error> {
        let declared = response.content_length();
        if declared.is_some_and(|len| len > MAX_TOOL_RESULT_SIZE as u64) {
            return Ok(None);
        }
        let capacity = declared.map_or(0, |len| usize::try_from(len).unwrap_or(0));
        let mut body = Vec::with_capacity(capacity);
        while let Some(chunk) = response.chunk().await? {
            if body.len() + chunk.len() > MAX_TOOL_RESULT_SIZE {
                return Ok(None);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Some(body))
    }
}

fn too_large_error(message: String, id: RequestId) -> ServerJsonRpcMessage {
    ServerJsonRpcMessage::error(ErrorData::internal_error(message, None), Some(id))
}

/// The SSE body of a POST, cut off once the bytes received exceed
/// [`MAX_TOOL_RESULT_SIZE`].
///
/// The limit applies to the whole stream, not to each event: a POST stream
/// carries the response to that one request (plus any progress notifications
/// before it), so its total is bounded by the size of that response.
///
/// rmcp reports an error inside a POST stream to the caller as a closed
/// transport, which reconnects and sends the request again. So when the POST
/// was a request, the stream invalidates any partial event and then sends a
/// JSON-RPC error for that request.
struct BoundedByteStream {
    inner: BoxStream<'static, reqwest::Result<Bytes>>,
    client: BoundedHttpClient,
    request_id: Option<RequestId>,
    received: usize,
    done: bool,
}

impl BoundedByteStream {
    fn over_limit(&mut self) -> Result<Bytes, ResponseBodyError> {
        self.done = true;
        // Drop the connection now rather than when rmcp drops the stream.
        self.inner = futures::stream::empty().boxed();
        let message = self.client.too_large();
        let Some(id) = self.request_id.take() else {
            return Err(ResponseBodyError::TooLarge { message });
        };
        let json = serde_json::to_string(&too_large_error(message.clone(), id))
            .map_err(|_| ResponseBodyError::TooLarge { message })?;
        // Do not begin with a blank line. Bytes already forwarded may hold a
        // complete `data:` field, and a blank line would dispatch it as a
        // success for this request. `data: x` continues that field so the
        // event is not JSON; the client skips it, and the next event is the
        // size error.
        Ok(Bytes::from(format!("data: x\n\ndata: {json}\n\n")))
    }
}

impl Stream for BoundedByteStream {
    type Item = Result<Bytes, ResponseBodyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        let item = match self.inner.poll_next_unpin(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(item) => item,
        };
        Poll::Ready(match item {
            Some(Ok(chunk)) => {
                self.received += chunk.len();
                if self.received > MAX_TOOL_RESULT_SIZE {
                    Some(self.over_limit())
                } else {
                    Some(Ok(chunk))
                }
            }
            Some(Err(source)) => Some(Err(ResponseBodyError::Read { source })),
            None => None,
        })
    }
}

fn apply_custom_headers(
    mut builder: reqwest::RequestBuilder,
    custom_headers: HashMap<HeaderName, HeaderValue>,
) -> HttpResult<reqwest::RequestBuilder> {
    // rmcp reserves these and injects `MCP-Protocol-Version` itself.
    const RESERVED: [&str; 3] = ["accept", HEADER_SESSION_ID, HEADER_LAST_EVENT_ID];
    for (name, value) in custom_headers {
        if !name
            .as_str()
            .eq_ignore_ascii_case(HEADER_MCP_PROTOCOL_VERSION)
            && RESERVED
                .iter()
                .any(|reserved| name.as_str().eq_ignore_ascii_case(reserved))
        {
            return Err(StreamableHttpError::ReservedHeaderConflict(
                name.to_string(),
            ));
        }
        builder = builder.header(name, value);
    }
    Ok(builder)
}

fn extract_scope_from_header(header: &str) -> Option<String> {
    let pos = header.to_ascii_lowercase().find("scope=")?;
    let value = &header[pos + "scope=".len()..];
    if let Some(quoted) = value.strip_prefix('"') {
        return quoted.find('"').map(|end| quoted[..end].to_string());
    }
    let end = value
        .find(|c: char| c == ',' || c == ';' || c.is_whitespace())
        .unwrap_or(value.len());
    (end > 0).then(|| value[..end].to_string())
}

fn www_authenticate(response: &reqwest::Response) -> HttpResult<Option<String>> {
    response
        .headers()
        .get(WWW_AUTHENTICATE)
        .map(|header| {
            header.to_str().map(ToString::to_string).map_err(|_| {
                StreamableHttpError::UnexpectedServerResponse(Cow::from(
                    "invalid www-authenticate header value",
                ))
            })
        })
        .transpose()
}

/// A sessionless `server/discover` rejected with a plain 4xx is how a legacy
/// server says it predates `2026-07-28`; turn it into the JSON-RPC error the
/// lifecycle layer falls back on.
fn legacy_discover_response(
    message: &ClientJsonRpcMessage,
    session_was_attached: bool,
    status: StatusCode,
    body: &str,
) -> Option<StreamableHttpPostResponse> {
    if session_was_attached
        || !status.is_client_error()
        || matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
    {
        return None;
    }
    let ClientJsonRpcMessage::Request(request) = message else {
        return None;
    };
    if !matches!(request.request, ClientRequest::DiscoverRequest(_)) {
        return None;
    }
    let error = ErrorData::invalid_request(
        format!("server/discover rejected with HTTP {status}: {body}"),
        None,
    );
    Some(StreamableHttpPostResponse::Json(
        ServerJsonRpcMessage::error(error, Some(request.id.clone())),
        None,
    ))
}

impl StreamableHttpClient for BoundedHttpClient {
    type Error = reqwest::Error;

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> HttpResult<StreamableHttpPostResponse> {
        let mut request = self
            .inner
            .post(uri.as_ref())
            .header(ACCEPT, [EVENT_STREAM_MIME_TYPE, JSON_MIME_TYPE].join(", "));
        if let Some(auth_header) = auth_header {
            request = request.bearer_auth(auth_header);
        }
        request = apply_custom_headers(request, custom_headers)?;
        let session_was_attached = session_id.is_some();
        if let Some(session_id) = session_id {
            request = request.header(HEADER_SESSION_ID, session_id.as_ref());
        }
        let response = request.json(&message).send().await?;

        let status = response.status();
        if status == StatusCode::UNAUTHORIZED
            && let Some(header) = www_authenticate(&response)?
        {
            return Err(StreamableHttpError::AuthRequired(AuthRequiredError::new(
                header,
            )));
        }
        if status == StatusCode::FORBIDDEN
            && let Some(header) = www_authenticate(&response)?
        {
            let scope = extract_scope_from_header(&header);
            return Err(StreamableHttpError::InsufficientScope(
                InsufficientScopeError::new(header, scope),
            ));
        }
        if matches!(status, StatusCode::ACCEPTED | StatusCode::NO_CONTENT) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if status == StatusCode::NOT_FOUND && session_was_attached {
            return Err(StreamableHttpError::SessionExpired);
        }
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .map(|ct| String::from_utf8_lossy(ct.as_bytes()).to_string());
        let session_id = response
            .headers()
            .get(HEADER_SESSION_ID)
            .and_then(|v| v.to_str().ok())
            .map(ToString::to_string);
        // Some servers answer a notification or response with an empty 200
        // instead of 202.
        if status.is_success()
            && response.content_length() == Some(0)
            && matches!(
                message,
                ClientJsonRpcMessage::Notification(_)
                    | ClientJsonRpcMessage::Response(_)
                    | ClientJsonRpcMessage::Error(_)
            )
        {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        let is_json = content_type
            .as_deref()
            .is_some_and(|ct| ct.as_bytes().starts_with(JSON_MIME_TYPE.as_bytes()));
        let request_id = match &message {
            ClientJsonRpcMessage::Request(request) => Some(request.id.clone()),
            _ => None,
        };

        if !status.is_success() {
            let body = match self.read_body(response).await {
                Ok(Some(body)) => String::from_utf8_lossy(&body).into_owned(),
                Ok(None) => return self.too_large_response(request_id, session_id),
                Err(_) => "<failed to read response body>".to_string(),
            };
            // A non-success response may still carry a JSON-RPC error worth
            // surfacing as the request's error.
            if is_json {
                if let Ok(message @ JsonRpcMessage::Error(_)) =
                    serde_json::from_str::<ServerJsonRpcMessage>(&body)
                {
                    return Ok(StreamableHttpPostResponse::Json(message, session_id));
                }
                tracing::warn!(
                    "MCP server '{}' answered HTTP {status} with a JSON body that is not a JSON-RPC error",
                    self.server_name
                );
            }
            if let Some(response) =
                legacy_discover_response(&message, session_was_attached, status, &body)
            {
                return Ok(response);
            }
            return Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(
                format!("HTTP {status}: {body}"),
            )));
        }

        match content_type.as_deref() {
            Some(ct) if ct.as_bytes().starts_with(EVENT_STREAM_MIME_TYPE.as_bytes()) => {
                let bytes = BoundedByteStream {
                    inner: response.bytes_stream().boxed(),
                    client: self.clone(),
                    request_id,
                    received: 0,
                    done: false,
                };
                Ok(StreamableHttpPostResponse::Sse(
                    SseStream::from_bytes_stream(bytes).boxed(),
                    session_id,
                ))
            }
            Some(_) if is_json => {
                let Some(body) = self.read_body(response).await? else {
                    return self.too_large_response(request_id, session_id);
                };
                // A malformed body (e.g. a 200 to a notification without an
                // `id`) is treated as accepted rather than failing.
                match serde_json::from_slice::<ServerJsonRpcMessage>(&body) {
                    Ok(message) => Ok(StreamableHttpPostResponse::Json(message, session_id)),
                    Err(e) => {
                        tracing::warn!(
                            "MCP server '{}' sent a JSON response that is not a JSON-RPC message, so it is treated as accepted: {e}",
                            self.server_name
                        );
                        Ok(StreamableHttpPostResponse::Accepted)
                    }
                }
            }
            _ => Err(StreamableHttpError::UnexpectedContentType(content_type)),
        }
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> HttpResult<()> {
        self.inner
            .delete_session(uri, session_id, auth_header, custom_headers)
            .await
    }

    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> HttpResult<BoxStream<'static, Result<Sse, SseError>>> {
        self.inner
            .get_stream_with_max_sse_event_size(
                uri,
                session_id,
                last_event_id,
                auth_header,
                custom_headers,
                MAX_TOOL_RESULT_SIZE,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use axum::{body::Body, extract::State, response::Response};
    use http::Request;
    use http_body_util::BodyExt;
    use rmcp::{
        ServerHandler,
        model::{
            CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ListToolsResult,
            PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
        },
        service::RequestContext,
        transport::streamable_http_server::{
            StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
        },
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;
    use crate::{
        catalog::SpiceToolCatalog,
        mcp::{MCPConfig, catalog::McpToolCatalog},
    };

    const SERVER_NAME: &str = "hostile";
    /// What the flooding tool would send if the client read it all.
    const FLOOD_SIZE: usize = 1024 * 1024 * 1024;
    const FLOOD_CHUNK: usize = 64 * 1024;
    static FLOOD_BYTES: [u8; FLOOD_CHUNK] = [b'X'; FLOOD_CHUNK];
    /// Generous room for what loopback socket buffers hold beyond what the
    /// client read; still a small fraction of `FLOOD_SIZE`.
    const MAX_FLOOD_BYTES_SENT: usize = 4 * MAX_TOOL_RESULT_SIZE;
    const CALL_TIMEOUT: Duration = Duration::from_secs(60);

    /// Lists `sized`, which returns `{"bytes": n}` bytes of text, and `flood`,
    /// which [`route`] answers itself.
    #[derive(Clone, Copy)]
    struct SizedResultServer;

    impl ServerHandler for SizedResultServer {
        fn get_info(&self) -> ServerInfo {
            ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
        }

        fn list_tools(
            &self,
            _request: Option<PaginatedRequestParams>,
            _context: RequestContext<rmcp::RoleServer>,
        ) -> impl Future<Output = Result<ListToolsResult, ErrorData>> + Send + '_ {
            let schema = Arc::new(serde_json::Map::new());
            std::future::ready(Ok(ListToolsResult {
                tools: vec![
                    Tool::new(
                        "sized",
                        "returns `bytes` bytes of text",
                        Arc::clone(&schema),
                    ),
                    Tool::new("flood", "never answered by this handler", schema),
                ],
                ..ListToolsResult::default()
            }))
        }

        fn call_tool(
            &self,
            request: CallToolRequestParams,
            _context: RequestContext<rmcp::RoleServer>,
        ) -> impl Future<Output = Result<CallToolResponse, ErrorData>> + Send + '_ {
            let bytes = request
                .arguments
                .as_ref()
                .and_then(|args| args.get("bytes"))
                .and_then(Value::as_u64)
                .and_then(|n| usize::try_from(n).ok())
                .unwrap_or(0);
            std::future::ready(Ok(CallToolResult::success(vec![ContentBlock::text(
                "X".repeat(bytes),
            )])
            .into()))
        }
    }

    #[derive(Clone)]
    struct Upstream {
        mcp: StreamableHttpService<SizedResultServer, LocalSessionManager>,
        flood_requests: Arc<AtomicUsize>,
        flood_bytes_sent: Arc<AtomicUsize>,
    }

    /// Forwards to the real MCP server, except a `tools/call` of `flood`,
    /// which gets a body of [`FLOOD_SIZE`] bytes in the shape its `mode`
    /// argument names. Chunks are generated as the client reads them, and
    /// counted in `flood_bytes_sent`; each such call counts in `flood_requests`.
    async fn route(State(upstream): State<Upstream>, request: Request<Body>) -> Response {
        let (parts, body) = request.into_parts();
        let body = body
            .collect()
            .await
            .expect("read the MCP request body")
            .to_bytes();
        let message: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        let is_flood = message["method"] == "tools/call" && message["params"]["name"] == "flood";
        if !is_flood {
            return upstream
                .mcp
                .oneshot(Request::from_parts(parts, Body::from(body)))
                .await
                .expect("the MCP service is infallible")
                .map(Body::new);
        }

        upstream.flood_requests.fetch_add(1, Ordering::SeqCst);
        let mode = message["params"]["arguments"]["mode"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let id = message["id"].to_string();
        if mode == "sse_sealed" {
            // A complete success, still unterminated, one byte under the cap.
            // The next chunk is the blank line that would dispatch it.
            let first = unterminated_sealed_event(&id);
            let sent = Arc::clone(&upstream.flood_bytes_sent);
            sent.fetch_add(first.len() + 2, Ordering::SeqCst);
            let body = futures::stream::iter([
                Ok::<_, std::io::Error>(Bytes::from(first)),
                Ok(Bytes::from_static(b"\n\n")),
            ]);
            return Response::builder()
                .header(CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(body))
                .expect("valid sealed response");
        }
        let (content_type, prefix) = if mode == "sse" {
            ("text/event-stream", "data: ")
        } else {
            ("application/json", "")
        };
        let prefix = format!(
            r#"{prefix}{{"jsonrpc":"2.0","id":{id},"result":{{"content":[{{"type":"text","text":""#
        );
        let sent = Arc::clone(&upstream.flood_bytes_sent);
        sent.fetch_add(prefix.len(), Ordering::SeqCst);
        let chunks = futures::stream::iter(0..FLOOD_SIZE / FLOOD_CHUNK).map(move |_| {
            sent.fetch_add(FLOOD_CHUNK, Ordering::SeqCst);
            Ok::<_, std::io::Error>(Bytes::from_static(&FLOOD_BYTES))
        });
        let body = futures::stream::once(async move { Ok(Bytes::from(prefix)) }).chain(chunks);
        let mut response = Response::builder().header(CONTENT_TYPE, content_type);
        if mode == "json_with_length" {
            response = response.header(http::header::CONTENT_LENGTH, FLOOD_SIZE.to_string());
        }
        response
            .body(Body::from_stream(body))
            .expect("valid flood response")
    }

    /// Serves [`route`] on a loopback port.
    async fn start_upstream(json_response: bool) -> (url::Url, Upstream) {
        let mcp = StreamableHttpService::new(
            || Ok(SizedResultServer),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .disable_allowed_hosts()
                .with_json_response(json_response),
        );
        let upstream = Upstream {
            mcp,
            flood_requests: Arc::new(AtomicUsize::new(0)),
            flood_bytes_sent: Arc::new(AtomicUsize::new(0)),
        };
        let app = axum::Router::new()
            .route("/mcp", axum::routing::any(route))
            .with_state(upstream.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback port");
        let addr = listener.local_addr().expect("listener address");
        tokio::spawn(async move { axum::serve(listener, app).await });
        let url = url::Url::parse(&format!("http://{addr}/mcp")).expect("valid url");
        (url, upstream)
    }

    async fn connect(url: url::Url) -> McpToolCatalog {
        McpToolCatalog::try_new(
            MCPConfig::StreamableHttp {
                url,
                auth_token: None,
                headers: HashMap::new(),
            },
            SERVER_NAME,
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("connect to the MCP server: {e}"))
    }

    async fn call(catalog: &McpToolCatalog, tool: &str, args: &Value) -> Result<Value, String> {
        let tool = catalog
            .get(tool)
            .await
            .unwrap_or_else(|| panic!("tool '{tool}' is listed"));
        tokio::time::timeout(CALL_TIMEOUT, tool.call(&args.to_string()))
            .await
            .expect("the tool call finishes instead of hanging")
            .map_err(|e| e.to_string())
    }

    /// Comment padding, then `data: {complete success}` with no trailing
    /// newline, totaling one byte under [`MAX_TOOL_RESULT_SIZE`].
    fn unterminated_sealed_event(id: &str) -> String {
        let tail = format!(
            "data: {{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":{{\"content\":[{{\"type\":\"text\",\"text\":\"SEALED_OK\"}}],\"isError\":false}}}}"
        );
        let pad = (MAX_TOOL_RESULT_SIZE - 1)
            .checked_sub(tail.len())
            .expect("the sealed event fits under the cap");
        let mut body = String::with_capacity(MAX_TOOL_RESULT_SIZE);
        body.push(':');
        body.extend(std::iter::repeat_n('x', pad - 2));
        body.push('\n');
        body.push_str(&tail);
        assert_eq!(
            body.len(),
            MAX_TOOL_RESULT_SIZE - 1,
            "pad leaves the event one byte under the cap"
        );
        assert!(body.ends_with(&tail) && !tail.ends_with('\n'));
        body
    }

    /// How the over-limit JSON-RPC error reaches the caller of the tool.
    fn too_large_call_error() -> String {
        format!(
            "Mcp error: -32603: {}",
            response_too_large_message(SERVER_NAME)
        )
    }

    fn text_result(bytes: usize) -> Value {
        json!([{"type": "text", "text": "X".repeat(bytes)}])
    }

    /// A normal result, and one at half the limit, arrive unchanged whether
    /// the server answers with JSON or SSE.
    #[tokio::test(flavor = "multi_thread")]
    async fn results_under_the_limit_are_returned_unchanged() {
        for json_response in [true, false] {
            let (url, _) = start_upstream(json_response).await;
            let catalog = connect(url).await;
            for bytes in [1024, MAX_TOOL_RESULT_SIZE / 2] {
                let result = call(&catalog, "sized", &json!({"bytes": bytes})).await;
                assert_eq!(
                    result.as_ref().map(|value| value == &text_result(bytes)),
                    Ok(true),
                    "a {bytes}-byte result (json_response={json_response}) must arrive unchanged"
                );
            }
        }
    }

    /// A finished success sits in the parser, one byte under the cap, and the
    /// next chunk is the blank line that would dispatch it. The call must
    /// return only the size error.
    #[tokio::test(flavor = "multi_thread")]
    async fn unterminated_success_is_not_dispatched_when_the_next_chunk_crosses_the_cap() {
        let (url, upstream) = start_upstream(true).await;
        let catalog = connect(url).await;
        let result = call(&catalog, "flood", &json!({"mode": "sse_sealed"})).await;
        assert_eq!(
            result,
            Err(too_large_call_error()),
            "the sealed success must not be returned"
        );
        assert_eq!(upstream.flood_requests.load(Ordering::SeqCst), 1);
    }

    /// regression test: a tool result of any size was read into memory in
    /// full and copied several times before reaching the caller.
    #[tokio::test(flavor = "multi_thread")]
    async fn oversized_result_fails_without_reading_the_whole_body() {
        for mode in ["json_chunked", "json_with_length", "sse"] {
            let (url, upstream) = start_upstream(true).await;
            let catalog = connect(url).await;

            let result = call(&catalog, "flood", &json!({"mode": mode})).await;
            assert_eq!(
                result,
                Err(too_large_call_error()),
                "mode={mode}: the call must fail with only the size-limit error"
            );
            assert_eq!(
                upstream.flood_requests.load(Ordering::SeqCst),
                1,
                "mode={mode}: the failure must not drop the connection and make the caller retry the call"
            );
            let sent = upstream.flood_bytes_sent.load(Ordering::SeqCst);
            assert!(
                sent > 0 && sent <= MAX_FLOOD_BYTES_SENT,
                "mode={mode}: the client must stop reading near the limit; the server sent {sent} of {FLOOD_SIZE} bytes"
            );

            assert_eq!(
                call(&catalog, "sized", &json!({"bytes": 1024})).await,
                Ok(text_result(1024)),
                "mode={mode}: the catalog must keep working after an over-limit result"
            );
        }
    }

    #[test]
    fn response_too_large_message_names_the_server_the_limit_and_the_fix() {
        assert_eq!(
            response_too_large_message("github"),
            "MCP server 'github' sent a response larger than the 16 MiB limit, so the request failed and the response was discarded. Make the MCP tool return a smaller result, for example by paginating it. See https://spiceai.org/docs/components/tools/mcp"
        );
    }
}
