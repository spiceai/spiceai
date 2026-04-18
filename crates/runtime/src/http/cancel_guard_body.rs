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

//! Body wrapper that ties a `tokio_util::sync::DropGuard` to the lifetime of
//! a streaming HTTP body.
//!
//! Used by HTTP and Flight middleware so that cancellation fires when the
//! response body is dropped mid-stream (client disconnect during a streaming
//! SQL, SSE, or Flight DoGet response), and does NOT fire when the body
//! completes normally.

use std::pin::Pin;
use std::task::{Context, Poll};

use http_body::{Body, Frame, SizeHint};
use pin_project::pin_project;
use tokio_util::sync::DropGuard;

/// A body wrapper that holds a `DropGuard`. The guard is disarmed once
/// the inner body signals end-of-stream. If the wrapper is dropped before
/// end-of-stream (for example, because the client disconnected), the
/// guard fires and cancels the associated `CancellationToken`.
#[pin_project]
pub struct CancelGuardBody<B> {
    #[pin]
    inner: B,
    guard: Option<DropGuard>,
}

impl<B> CancelGuardBody<B> {
    pub fn new(inner: B, guard: DropGuard) -> Self {
        Self {
            inner,
            guard: Some(guard),
        }
    }
}

impl<B> Body for CancelGuardBody<B>
where
    B: Body,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        match this.inner.poll_frame(cx) {
            Poll::Ready(None) => {
                // Normal end-of-stream: disarm the guard so cancellation does
                // not fire when this wrapper is dropped.
                if let Some(guard) = this.guard.take() {
                    guard.disarm();
                }
                Poll::Ready(None)
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
