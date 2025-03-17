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
use futures::{stream::Stream, StreamExt, TryStreamExt};
use mcp_server::{router::RouterService, ByteTransport, Server};

use tokio_util::codec::FramedRead;

use http::StatusCode;

use tokio::{
    io::{self, AsyncWriteExt},
    sync::Mutex,
};

use axum::{
    extract::Query,
    response::sse::{Event, Sse},
    Extension,
};
use std::{collections::HashMap, sync::Arc};

use crate::{tools::mcp::server::RuntimeServer, Runtime};

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
            let server = Server::new(RouterService(RuntimeServer::from(&rt)));
            let bytes_transport = ByteTransport::new(c2s_read, s2c_write);
            let _result = server
                .run(bytes_transport)
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
