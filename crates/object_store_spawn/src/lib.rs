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
//! the given runtime and pass the response body back over an unbounded channel. This connector
//! stops reading the body at its first error. Once a body outlives the request timeout
//! (`ClientOptions::with_timeout`), reqwest returns a new timeout error on every later poll, so a
//! task that forwards frames until the body ends never stops: it fills the channel with errors
//! as fast as it can allocate them until the reader drops the body, which `object_store` does
//! only after its retry backoff and the retry request complete.

use std::{
    future::poll_fn,
    pin::Pin,
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
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

/// [`HttpConnector`] that sends every request with `reqwest` on `runtime`.
#[derive(Debug)]
pub struct SpawnedReqwestConnector {
    runtime: Handle,
}

impl SpawnedReqwestConnector {
    #[must_use]
    pub fn new(runtime: Handle) -> Self {
        Self { runtime }
    }
}

impl HttpConnector for SpawnedReqwestConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let client = ReqwestConnector::default().connect(options)?;
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
        // Unbounded so that a slow reader never stalls I/O on `runtime`.
        let (frames_tx, frames_rx) = mpsc::unbounded_channel();

        let worker = AbortOnDrop(self.runtime.spawn(async move {
            let response = match client.execute(req).await {
                Ok(response) => response,
                Err(e) => {
                    let _ = parts_tx.send(Err(e));
                    return;
                }
            };
            let (parts, mut body) = response.into_parts();
            if parts_tx.send(Ok(parts)).is_err() {
                return;
            }
            while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
                let failed = frame.is_err();
                if frames_tx.send(frame).is_err() || failed {
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

/// Response body fed by the request task. Dropping it aborts the task.
struct SpawnedBody {
    frames: mpsc::UnboundedReceiver<Result<Frame<Bytes>, HttpError>>,
    _worker: AbortOnDrop,
}

impl Body for SpawnedBody {
    type Data = Bytes;
    type Error = HttpError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, HttpError>>> {
        self.frames.poll_recv(cx)
    }
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
