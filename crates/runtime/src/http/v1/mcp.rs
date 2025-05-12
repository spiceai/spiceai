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
use futures::{StreamExt, TryStreamExt, stream::Stream};
// use mcp_server::{ByteTransport, Server, router::RouterService};
use rmcp::{
    ServiceExt,
    transport::sse_server::{SseServer, SseServerConfig},
};

use tokio_util::{codec::FramedRead, sync::CancellationToken};

use http::StatusCode;

use tokio::{
    io::{self, AsyncWriteExt},
    sync::Mutex,
};

use axum::{
    Extension,
    extract::Query,
    response::sse::{Event, Sse},
};
use std::{collections::HashMap, sync::Arc};

use crate::{Runtime, tools::mcp::server::RuntimeServer};

const FOUR_KB: usize = 1 << 12;

type C2SWriter = Arc<Mutex<io::WriteHalf<io::SimplexStream>>>;
type SessionId = Arc<str>;

#[derive(Clone, Default)]
pub struct McpState {
    txs: Arc<tokio::sync::RwLock<HashMap<SessionId, C2SWriter>>>,
}

impl McpState {
    pub(crate) async fn get(&self, session_id: &str) -> Option<C2SWriter> {
        let rg = self.txs.read().await;
        let writer = Arc::clone(rg.get(session_id)?);
        Some(writer)
    }
}

fn session_id() -> SessionId {
    Arc::from(format!("{:016x}", rand::random::<u128>()))
}

#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct PostEventQuery {
    pub session_id: String,
}

/// Openapi documentation for this endpoint is in [`crate::http::get_api_doc`].
pub(crate) async fn sse(
    Extension(rt): Extension<Arc<Runtime>>,
    Extension(mcp): Extension<Arc<McpState>>,
) -> Sse<impl Stream<Item = Result<Event, std::io::Error>>> {
    let session = session_id();
    tracing::trace!("New MCP connection with sessionid={session}");
    let (c2s_read, c2s_write) = tokio::io::simplex(FOUR_KB);
    let (s2c_read, s2c_write) = tokio::io::simplex(FOUR_KB);

    mcp.txs
        .write()
        .await
        .insert(Arc::clone(&session), Arc::new(Mutex::new(c2s_write)));
    {
        let session = Arc::clone(&session);
        tokio::spawn(async move {
            let service = RuntimeServer::from(&rt);
            let bytes_transport = ByteTransport::new(c2s_read, s2c_write);
            let ct = CancellationToken::new();
            service
                .serve_with_ct(bytes_transport, ct)
                .await
                .inspect_err(|e| tracing::error!(?e, "server run error"));
            mcp.txs.write().await.remove(&session);
        });
    }

    let stream = futures::stream::once(futures::future::ok(
        Event::default()
            .event("endpoint")
            .data(format!("?sessionId={session}")),
    ))
    .chain(
        FramedRead::new(
            s2c_read,
            crate::tools::mcp::server::codec::JsonRpcFrameCodec,
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        .and_then(move |bytes| match std::str::from_utf8(&bytes) {
            Ok(message) => futures::future::ok(Event::default().event("message").data(message)),
            Err(e) => futures::future::err(io::Error::new(io::ErrorKind::InvalidData, e)),
        }),
    );
    Sse::new(stream)
}
/// Send message to MCP server
///
/// Send message to the MCP endoint, for a given session.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v1/mcp/event",
        operation_id = "mcp_event",
        tag = "mcp",
        params(PostEventQuery),
        responses(
    (status = 202, description = "Message accepted. Response will stream via SSE."),
    (status = 404, description = "Session not found. No active session for the given `session_id`."),
    (status = 413, description = "Payload too large. Maximum allowed size is 4MB."),
    (status = 500, description = "Internal server error. An unexpected issue occurred."),
)
    )
)]
pub(crate) async fn event(
    Extension(mcp): Extension<Arc<McpState>>,
    Query(PostEventQuery { session_id }): Query<PostEventQuery>,
    body: String,
) -> Result<StatusCode, StatusCode> {
    const BODY_BYTES_LIMIT: usize = 1 << 22;
    tracing::trace!(
        "Received POST event in SSE session_id={session_id}. Event={}",
        body
    );
    let Some(writer) = mcp.get(session_id.as_str()).await else {
        return Err(StatusCode::NOT_FOUND);
    };

    let mut write_stream = writer.lock().await;
    if body.len() > BODY_BYTES_LIMIT {
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }
    write_stream
        .write_all(body.as_ref())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    write_stream
        .write_u8(b'\n')
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::ACCEPTED)
}

async fn post_event_handler(
    State(app): State<App>,
    Query(PostEventQuery { session_id }): Query<PostEventQuery>,
    Json(message): Json<ClientJsonRpcMessage>,
) -> Result<StatusCode, StatusCode> {
    tracing::debug!(session_id, ?message, "new client message");
    let tx = {
        let rg = app.txs.read().await;
        rg.get(session_id.as_str())
            .ok_or(StatusCode::NOT_FOUND)?
            .clone()
    };
    if tx.send(message).await.is_err() {
        tracing::error!("send message error");
        return Err(StatusCode::GONE);
    }
    Ok(StatusCode::ACCEPTED)
}

async fn sse_handler(
    State(app): State<App>,
) -> Result<Sse<impl Stream<Item = Result<Event, io::Error>>>, Response<String>> {
    let session = session_id();
    tracing::info!(%session, "sse connection");
    use tokio_stream::{StreamExt, wrappers::ReceiverStream};
    use tokio_util::sync::PollSender;
    let (from_client_tx, from_client_rx) = tokio::sync::mpsc::channel(64);
    let (to_client_tx, to_client_rx) = tokio::sync::mpsc::channel(64);
    app.txs
        .write()
        .await
        .insert(session.clone(), from_client_tx);
    let session = session.clone();
    let stream = ReceiverStream::new(from_client_rx);
    let sink = PollSender::new(to_client_tx);
    let transport = SseServerTransport {
        stream,
        sink,
        session_id: session.clone(),
        tx_store: app.txs.clone(),
    };
    let transport_send_result = app.transport_tx.send(transport);
    if transport_send_result.is_err() {
        tracing::warn!("send transport out error");
        let mut response =
            Response::new("fail to send out transport, it seems server is closed".to_string());
        *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        return Err(response);
    }
    let post_path = app.post_path.as_ref();
    let ping_interval = app.sse_ping_interval;
    let stream = futures::stream::once(futures::future::ok(
        Event::default()
            .event("endpoint")
            .data(format!("{post_path}?sessionId={session}")),
    ))
    .chain(ReceiverStream::new(to_client_rx).map(|message| {
        match serde_json::to_string(&message) {
            Ok(bytes) => Ok(Event::default().event("message").data(&bytes)),
            Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        }
    }));
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(ping_interval)))
}
