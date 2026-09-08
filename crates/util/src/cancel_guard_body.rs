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
//! SQL, SSE, or Flight `DoGet` response), and does NOT fire when the body
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

impl<B> CancelGuardBody<B>
where
    B: Body,
{
    pub fn new(inner: B, guard: DropGuard) -> Self {
        // If the inner body already reports end-of-stream (empty bodies,
        // HEAD responses, or callers that short-circuit based on
        // `Body::is_end_stream` without ever polling), the request is
        // effectively complete before the wrapper is dropped. Disarm the
        // guard upfront so its `Drop` impl does not spuriously cancel the
        // request token.
        let guard = if inner.is_end_stream() {
            guard.disarm();
            None
        } else {
            Some(guard)
        };
        Self { inner, guard }
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
        let mut inner = this.inner;
        match inner.as_mut().poll_frame(cx) {
            Poll::Ready(None) => {
                // Normal end-of-stream: disarm the guard so cancellation does
                // not fire when this wrapper is dropped.
                if let Some(guard) = this.guard.take() {
                    guard.disarm();
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(frame)) => {
                if inner.as_ref().get_ref().is_end_stream()
                    && let Some(guard) = this.guard.take()
                {
                    guard.disarm();
                }
                Poll::Ready(Some(frame))
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

#[cfg(test)]
mod tests {
    use super::CancelGuardBody;
    use std::convert::Infallible;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures::task::noop_waker_ref;
    use http_body::{Body, Frame, SizeHint};
    use tokio_util::sync::CancellationToken;

    #[derive(Debug)]
    struct StaticBody {
        frames: std::vec::IntoIter<Frame<&'static [u8]>>,
        end_stream: bool,
    }

    impl StaticBody {
        fn new(frames: Vec<Frame<&'static [u8]>>) -> Self {
            let end_stream = frames.is_empty();
            Self {
                frames: frames.into_iter(),
                end_stream,
            }
        }

        fn with_data_chunks(chunks: Vec<&'static [u8]>) -> Self {
            Self::new(chunks.into_iter().map(Frame::data).collect())
        }
    }

    impl Body for StaticBody {
        type Data = &'static [u8];
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            if let Some(frame) = self.frames.next() {
                Poll::Ready(Some(Ok(frame)))
            } else {
                self.end_stream = true;
                Poll::Ready(None)
            }
        }

        fn is_end_stream(&self) -> bool {
            self.end_stream
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::default()
        }
    }

    struct EndStreamAfterFinalFrameBody {
        frame: Option<Frame<&'static [u8]>>,
        end_stream: bool,
    }

    impl Body for EndStreamAfterFinalFrameBody {
        type Data = &'static [u8];
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            if let Some(frame) = self.frame.take() {
                self.end_stream = true;
                Poll::Ready(Some(Ok(frame)))
            } else {
                Poll::Ready(None)
            }
        }

        fn is_end_stream(&self) -> bool {
            self.end_stream
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::default()
        }
    }

    fn test_context() -> Context<'static> {
        Context::from_waker(noop_waker_ref())
    }

    #[test]
    fn cancels_when_dropped_before_end_of_stream() {
        let token = CancellationToken::new();
        let guard = token.clone().drop_guard();

        let body = StaticBody::with_data_chunks(vec![b"chunk-1", b"chunk-2"]);
        let mut guarded = Box::pin(CancelGuardBody::new(body, guard));
        let mut cx = test_context();

        let first = guarded.as_mut().poll_frame(&mut cx);
        assert!(matches!(first, Poll::Ready(Some(Ok(_)))));
        assert!(!token.is_cancelled());

        drop(guarded);

        assert!(token.is_cancelled());
    }

    #[test]
    fn does_not_cancel_after_normal_end_of_stream() {
        let token = CancellationToken::new();
        let guard = token.clone().drop_guard();

        let body = StaticBody::with_data_chunks(vec![b"chunk-1"]);
        let mut guarded = Box::pin(CancelGuardBody::new(body, guard));
        let mut cx = test_context();

        let first = guarded.as_mut().poll_frame(&mut cx);
        assert!(matches!(first, Poll::Ready(Some(Ok(_)))));
        assert!(!token.is_cancelled());

        let eos = guarded.as_mut().poll_frame(&mut cx);
        assert!(matches!(eos, Poll::Ready(None)));

        drop(guarded);

        assert!(!token.is_cancelled());
    }

    #[test]
    fn does_not_cancel_when_dropped_after_final_frame_without_polling_eos() {
        let token = CancellationToken::new();
        let guard = token.clone().drop_guard();

        let body = EndStreamAfterFinalFrameBody {
            frame: Some(Frame::data(&b"chunk-1"[..])),
            end_stream: false,
        };
        let mut guarded = Box::pin(CancelGuardBody::new(body, guard));
        let mut cx = test_context();

        let frame = guarded.as_mut().poll_frame(&mut cx);
        assert!(matches!(frame, Poll::Ready(Some(Ok(_)))));
        assert!(guarded.is_end_stream());
        assert!(!token.is_cancelled());

        drop(guarded);

        assert!(!token.is_cancelled());
    }

    #[test]
    fn does_not_cancel_for_empty_body_that_reports_end_of_stream() {
        let token = CancellationToken::new();
        let guard = token.clone().drop_guard();

        // Body reports end-of-stream immediately without ever being polled.
        let body = StaticBody {
            frames: Vec::new().into_iter(),
            end_stream: true,
        };
        let guarded = CancelGuardBody::new(body, guard);

        drop(guarded);

        assert!(!token.is_cancelled());
    }

    #[test]
    fn does_not_cancel_for_empty_body_after_end_of_stream_is_observed() {
        let token = CancellationToken::new();
        let guard = token.clone().drop_guard();

        let body = StaticBody::new(Vec::new());
        let mut guarded = Box::pin(CancelGuardBody::new(body, guard));
        let mut cx = test_context();

        let eos = guarded.as_mut().poll_frame(&mut cx);
        assert!(matches!(eos, Poll::Ready(None)));

        drop(guarded);

        assert!(!token.is_cancelled());
    }
}
