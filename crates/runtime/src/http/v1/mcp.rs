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
use http_body_util::StreamBody;
use mcp_server::{ByteTransport, Server};

use tokio_util::codec::FramedRead;
// use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use http::StatusCode;
use mcp_server::router::RouterService;

use tokio::{
    io::{self, AsyncWriteExt},
    sync::Mutex,
};

use std::{collections::HashMap, sync::Arc};

use axum::{
    body::Body,
    extract::{Path, Query, State},
    response::{
        sse::{Event, Sse},
        Response,
    },
    Extension,
};

use crate::{tools::mcp::server::RuntimeServer, Runtime};

const FOUR_KB: usize = 1 << 12;
type C2SWriter = Arc<Mutex<io::WriteHalf<io::SimplexStream>>>;
type SessionId = Arc<str>;

#[derive(Clone)]
pub struct McpState {
    txs: Arc<tokio::sync::RwLock<HashMap<SessionId, C2SWriter>>>,
    pub(crate) rt: Arc<Runtime>,
}

impl McpState {
    pub fn new(rt: Arc<Runtime>) -> Self {
        Self {
            txs: Default::default(),
            rt,
        }
    }
}

fn session_id() -> SessionId {
    let id = format!("{:016x}", rand::random::<u128>());
    Arc::from(id)
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PostEventQuery {
    pub session_id: String,
}

pub(crate) async fn sse(
    State(state): State<McpState>,
) -> Sse<impl Stream<Item = Result<Event, std::io::Error>>> {
    let session = session_id();
    let (c2s_read, c2s_write) = tokio::io::simplex(FOUR_KB);
    let (s2c_read, s2c_write) = tokio::io::simplex(FOUR_KB);
    state
        .txs
        .write()
        .await
        .insert(session.clone(), Arc::new(Mutex::new(c2s_write)));
    {
        let session = session.clone();
        tokio::spawn(async move {
            let server = Server::new(RouterService(RuntimeServer::from(state.rt.clone())));
            let bytes_transport = ByteTransport::new(c2s_read, s2c_write);
            let _result = server
                .run(bytes_transport)
                .await
                .inspect_err(|e| tracing::error!(?e, "server run error"));
            state.txs.write().await.remove(&session);
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

pub(crate) async fn event(
    State(state): State<McpState>,
    Query(PostEventQuery { session_id }): Query<PostEventQuery>,
    body: Body, // hyper::body::Incoming,
) -> Result<StatusCode, StatusCode> {
    // let body: Body = body.into();
    const BODY_BYTES_LIMIT: usize = 1 << 22;
    let write_stream = {
        let rg = state.txs.read().await;
        rg.get(session_id.as_str())
            .ok_or(StatusCode::NOT_FOUND)?
            .clone()
    };
    let mut write_stream = write_stream.lock().await;
    let mut body = body.into_data_stream();
    if let (_, Some(size)) = body.size_hint() {
        if size > BODY_BYTES_LIMIT {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
    }
    // calculate the body size
    let mut size = 0;
    while let Some(chunk) = body.next().await {
        let Ok(chunk) = chunk else {
            return Err(StatusCode::BAD_REQUEST);
        };
        size += chunk.len();
        if size > BODY_BYTES_LIMIT {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        write_stream
            .write_all(&chunk)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    }
    write_stream
        .write_u8(b'\n')
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::ACCEPTED)
}
