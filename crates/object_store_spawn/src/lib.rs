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

//! An `object_store` [`HttpConnector`] that performs request I/O on a dedicated Tokio runtime.
//!
//! Use it in place of `object_store::client::SpawnedReqwestConnector`. Both run each request on
//! the given runtime and pass the response body back to the reader over a channel. Two things
//! differ:
//!
//! - **The body is read at most [`RESPONSE_READ_AHEAD_BYTES`] ahead of its reader.** The
//!   `object_store` connector drains every body into an unbounded channel as fast as the network
//!   delivers it, so a reader slower than the network (a scan whose downstream is busy, a client
//!   fetching results slowly) holds the difference in memory, up to the whole object, for every
//!   response at once. Here the request task waits for the reader before it reads further, so the
//!   network is paced by the reader, as it is on a connector that does not spawn: the response
//!   stays in the socket and the sender's buffers rather than in this process.
//! - **The body stops at its first error.** Once a body outlives the request timeout
//!   (`ClientOptions::with_timeout`), reqwest returns a new timeout error on every later poll, so
//!   a task that forwards frames until the body ends never stops: it fills the channel with
//!   errors as fast as it can allocate them until the reader drops the body, which
//!   `object_store` does only after its retry backoff and the retry request complete.
//!
//! [`SpawnedReqwestConnector::with_destination_guard`] additionally refuses redirects to, and
//! connections to, the destinations a [`DestinationGuard`] blocks.

mod guard;

pub use guard::{
    DestinationGuard, DestinationRefused, REDIRECT_CHAIN_REFUSED, redirect_continues_off_origin,
};

use std::{
    future::poll_fn,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use bytes::Bytes;
use http_body::{Body, Frame};
use object_store::{
    ClientOptions,
    client::{
        HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
        HttpResponseBody, HttpService, ReqwestConnector,
    },
};
use snafu::Snafu;
use tokio::{
    runtime::Handle,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
};

/// How far the request task reads a response body ahead of the reader, in bytes.
///
/// Enough to keep the network busy while the reader works through what it has, and small
/// enough that a reader that stops reading holds no more than this per response. The task
/// holds at most one more frame while it waits for room, and a frame is no larger than one
/// read from the connection.
pub const RESPONSE_READ_AHEAD_BYTES: usize = 8 * 1024 * 1024;

/// What a frame costs against [`RESPONSE_READ_AHEAD_BYTES`]: its data, and at least one byte
/// so the number of queued frames is bounded too. A frame larger than the whole budget waits
/// until the reader has taken everything before it.
fn read_ahead_cost(frame: &Result<Frame<Bytes>, HttpError>) -> u32 {
    let bytes = frame
        .as_ref()
        .ok()
        .and_then(Frame::data_ref)
        .map_or(1, |data| data.len().max(1));
    u32::try_from(bytes.min(RESPONSE_READ_AHEAD_BYTES)).unwrap_or(u32::MAX)
}

type QueuedFrame = (Result<Frame<Bytes>, HttpError>, OwnedSemaphorePermit);

/// [`HttpConnector`] that sends every request with `reqwest` on `runtime`.
#[derive(Debug)]
pub struct SpawnedReqwestConnector {
    runtime: Handle,
    guard: Option<DestinationGuard>,
}

impl SpawnedReqwestConnector {
    #[must_use]
    pub fn new(runtime: Handle) -> Self {
        Self {
            runtime,
            guard: None,
        }
    }

    /// Refuses every redirect hop and resolved address that `guard` blocks.
    #[must_use]
    pub fn with_destination_guard(mut self, guard: DestinationGuard) -> Self {
        self.guard = Some(guard);
        self
    }
}

impl HttpConnector for SpawnedReqwestConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let client = match &self.guard {
            Some(guard) => HttpClient::new(guard::build_client(options, guard)?),
            None => ReqwestConnector::default().connect(options)?,
        };
        Ok(HttpClient::new(SpawnService {
            client,
            runtime: self.runtime.clone(),
        }))
    }
}

#[derive(Debug, Snafu)]
#[snafu(display("The HTTP request task stopped before it returned a response"))]
struct RequestTaskStopped;

#[derive(Debug)]
struct SpawnService {
    client: HttpClient,
    runtime: Handle,
}

#[async_trait]
impl HttpService for SpawnService {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let client = self.client.clone();
        let (parts_tx, parts_rx) = oneshot::channel();
        // Bounded in bytes rather than frames: every queued frame holds permits from
        // `read_ahead` until the reader takes it. The task waits for permits without
        // blocking `runtime`, so other requests' I/O carries on while this one waits.
        let read_ahead = Arc::new(Semaphore::new(RESPONSE_READ_AHEAD_BYTES));
        let (frames_tx, frames_rx) = mpsc::unbounded_channel::<QueuedFrame>();

        let worker = AbortOnDrop(self.runtime.spawn(async move {
            let response = match client.execute(req).await {
                Ok(response) => response,
                Err(e) => {
                    let _ = parts_tx.send(Err(guard::refusal(&e).unwrap_or(e)));
                    return;
                }
            };
            let (parts, mut body) = response.into_parts();
            if parts_tx.send(Ok(parts)).is_err() {
                return;
            }
            while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
                let failed = frame.is_err();
                // The semaphore is never closed, and the receiver going away aborts this
                // task, so a failed acquire only ends a body nobody is reading.
                let Ok(permit) = Arc::clone(&read_ahead)
                    .acquire_many_owned(read_ahead_cost(&frame))
                    .await
                else {
                    return;
                };
                if frames_tx.send((frame, permit)).is_err() || failed {
                    return;
                }
            }
        }));

        let parts = parts_rx
            .await
            .map_err(|_| HttpError::new(HttpErrorKind::Interrupted, RequestTaskStopped))??;

        Ok(HttpResponse::from_parts(
            parts,
            HttpResponseBody::new(SpawnedBody {
                frames: frames_rx,
                _worker: worker,
            }),
        ))
    }
}

/// Response body fed by the request task. Taking a frame returns its read-ahead permits to the
/// task; dropping the body aborts the task.
struct SpawnedBody {
    frames: mpsc::UnboundedReceiver<QueuedFrame>,
    _worker: AbortOnDrop,
}

impl Body for SpawnedBody {
    type Data = Bytes;
    type Error = HttpError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, HttpError>>> {
        self.frames
            .poll_recv(cx)
            .map(|queued| queued.map(|(frame, _permit)| frame))
    }
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
