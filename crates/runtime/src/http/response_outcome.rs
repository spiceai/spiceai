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

//! Body wrapper that reports how a response body terminated.
//!
//! The response head is resolved before a streaming body produces its first
//! byte, so a status code alone cannot say whether the response succeeded: a
//! `/v1/sql` query that runs out of memory partway through a join has already
//! sent `200 OK`. This wrapper observes the end of the body instead and hands
//! the terminal outcome — plus the true end-to-end duration — to a callback.

use std::pin::Pin;
use std::task::{Context, Poll};

use http_body::{Body, Frame, SizeHint};
use pin_project::{pin_project, pinned_drop};

/// How a response body finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseOutcome {
    /// The body reached end-of-stream.
    Complete,
    /// The body yielded an error after the head was sent.
    Error,
    /// The body was dropped before end-of-stream, typically because the client
    /// disconnected mid-stream.
    Incomplete,
}

impl ResponseOutcome {
    pub(crate) fn as_label(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Error => "error",
            Self::Incomplete => "incomplete",
        }
    }
}

/// Wraps a response body and invokes `on_terminal` exactly once, with the
/// outcome the body reached.
#[pin_project(PinnedDrop)]
pub(crate) struct OutcomeTrackedBody<B, F>
where
    F: FnOnce(ResponseOutcome),
{
    #[pin]
    inner: B,
    on_terminal: Option<F>,
}

impl<B, F> OutcomeTrackedBody<B, F>
where
    B: Body,
    F: FnOnce(ResponseOutcome),
{
    pub(crate) fn new(inner: B, on_terminal: F) -> Self {
        // A body that already reports end-of-stream (an empty body, a HEAD
        // response, or a caller that short-circuits on `Body::is_end_stream`
        // without ever polling) is complete before it is ever polled.
        if inner.is_end_stream() {
            on_terminal(ResponseOutcome::Complete);
            return Self {
                inner,
                on_terminal: None,
            };
        }
        Self {
            inner,
            on_terminal: Some(on_terminal),
        }
    }
}

impl<B, F> Body for OutcomeTrackedBody<B, F>
where
    B: Body,
    F: FnOnce(ResponseOutcome),
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let mut inner = this.inner;
        let polled = inner.as_mut().poll_frame(cx);

        let outcome = match &polled {
            Poll::Ready(None) => Some(ResponseOutcome::Complete),
            Poll::Ready(Some(Err(_))) => Some(ResponseOutcome::Error),
            // A body may signal end-of-stream alongside its final frame rather
            // than on a follow-up poll; the caller is then free to stop polling.
            Poll::Ready(Some(Ok(_))) if inner.as_ref().get_ref().is_end_stream() => {
                Some(ResponseOutcome::Complete)
            }
            _ => None,
        };

        if let Some(outcome) = outcome
            && let Some(on_terminal) = this.on_terminal.take()
        {
            on_terminal(outcome);
        }

        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[pinned_drop]
impl<B, F> PinnedDrop for OutcomeTrackedBody<B, F>
where
    F: FnOnce(ResponseOutcome),
{
    fn drop(self: Pin<&mut Self>) {
        // Still armed at drop means the body never reached a terminal frame.
        if let Some(on_terminal) = self.project().on_terminal.take() {
            on_terminal(ResponseOutcome::Incomplete);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{OutcomeTrackedBody, ResponseOutcome};

    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    use futures::task::noop_waker_ref;
    use http_body::{Body, Frame, SizeHint};

    /// Records every outcome the wrapper reports, so a test can assert that it
    /// fires exactly once and with the right value.
    #[derive(Default)]
    struct Recorder(Arc<Mutex<Vec<ResponseOutcome>>>);

    impl Recorder {
        fn sink(&self) -> impl FnOnce(ResponseOutcome) + use<> {
            let outcomes = Arc::clone(&self.0);
            move |outcome| {
                outcomes
                    .lock()
                    .expect("outcome recorder mutex is not poisoned")
                    .push(outcome);
            }
        }

        fn recorded(&self) -> Vec<ResponseOutcome> {
            self.0
                .lock()
                .expect("outcome recorder mutex is not poisoned")
                .clone()
        }
    }

    /// Yields the queued frames, then `None`. `end_stream` only flips once the
    /// stream has been drained, matching a body whose length is not known ahead
    /// of time (the streaming `/v1/sql` shape).
    struct StreamingBody {
        frames: std::vec::IntoIter<Result<Frame<&'static [u8]>, &'static str>>,
        end_stream: bool,
    }

    impl StreamingBody {
        fn with_chunks(chunks: Vec<&'static [u8]>) -> Self {
            Self {
                frames: chunks
                    .into_iter()
                    .map(|c| Ok(Frame::data(c)))
                    .collect::<Vec<_>>()
                    .into_iter(),
                end_stream: false,
            }
        }

        /// A body that streams `chunks` and then fails, as a query does when it
        /// is refused memory partway through producing its result.
        fn failing_after(chunks: Vec<&'static [u8]>) -> Self {
            Self {
                frames: chunks
                    .into_iter()
                    .map(|c| Ok(Frame::data(c)))
                    .chain(std::iter::once(Err("resources exhausted")))
                    .collect::<Vec<_>>()
                    .into_iter(),
                end_stream: false,
            }
        }
    }

    impl Body for StreamingBody {
        type Data = &'static [u8];
        type Error = &'static str;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            if let Some(frame) = self.frames.next() {
                Poll::Ready(Some(frame))
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

    /// Reports end-of-stream alongside its final frame, so a caller that stops
    /// polling there never observes `Poll::Ready(None)`.
    struct EndStreamWithFinalFrameBody {
        frame: Option<Frame<&'static [u8]>>,
        end_stream: bool,
    }

    impl Body for EndStreamWithFinalFrameBody {
        type Data = &'static [u8];
        type Error = &'static str;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            match self.frame.take() {
                Some(frame) => {
                    self.end_stream = true;
                    Poll::Ready(Some(Ok(frame)))
                }
                None => Poll::Ready(None),
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

    /// Regression test for #12284: a streaming response that fails after its
    /// head was sent must not be reported as a success.
    #[test]
    fn reports_error_when_the_body_fails_mid_stream() {
        let recorder = Recorder::default();
        let body = StreamingBody::failing_after(vec![b"[{\"a\":1}"]);
        let mut tracked = Box::pin(OutcomeTrackedBody::new(body, recorder.sink()));
        let mut cx = test_context();

        let first = tracked.as_mut().poll_frame(&mut cx);
        assert!(matches!(first, Poll::Ready(Some(Ok(_)))));
        assert!(
            recorder.recorded().is_empty(),
            "no outcome before the body terminates"
        );

        let failure = tracked.as_mut().poll_frame(&mut cx);
        assert!(matches!(failure, Poll::Ready(Some(Err(_)))));
        assert_eq!(recorder.recorded(), vec![ResponseOutcome::Error]);

        drop(tracked);

        assert_eq!(
            recorder.recorded(),
            vec![ResponseOutcome::Error],
            "dropping after a terminal frame must not report a second outcome"
        );
    }

    #[test]
    fn reports_complete_when_the_body_reaches_end_of_stream() {
        let recorder = Recorder::default();
        let body = StreamingBody::with_chunks(vec![b"[", b"]"]);
        let mut tracked = Box::pin(OutcomeTrackedBody::new(body, recorder.sink()));
        let mut cx = test_context();

        for _ in 0..2 {
            assert!(matches!(
                tracked.as_mut().poll_frame(&mut cx),
                Poll::Ready(Some(Ok(_)))
            ));
        }
        assert!(recorder.recorded().is_empty());

        assert!(matches!(
            tracked.as_mut().poll_frame(&mut cx),
            Poll::Ready(None)
        ));
        drop(tracked);

        assert_eq!(recorder.recorded(), vec![ResponseOutcome::Complete]);
    }

    #[test]
    fn reports_incomplete_when_the_body_is_dropped_mid_stream() {
        let recorder = Recorder::default();
        let body = StreamingBody::with_chunks(vec![b"chunk-1", b"chunk-2"]);
        let mut tracked = Box::pin(OutcomeTrackedBody::new(body, recorder.sink()));
        let mut cx = test_context();

        assert!(matches!(
            tracked.as_mut().poll_frame(&mut cx),
            Poll::Ready(Some(Ok(_)))
        ));
        assert!(recorder.recorded().is_empty());

        drop(tracked);

        assert_eq!(recorder.recorded(), vec![ResponseOutcome::Incomplete]);
    }

    #[test]
    fn reports_complete_when_end_of_stream_arrives_with_the_final_frame() {
        let recorder = Recorder::default();
        let body = EndStreamWithFinalFrameBody {
            frame: Some(Frame::data(&b"chunk-1"[..])),
            end_stream: false,
        };
        let mut tracked = Box::pin(OutcomeTrackedBody::new(body, recorder.sink()));
        let mut cx = test_context();

        assert!(matches!(
            tracked.as_mut().poll_frame(&mut cx),
            Poll::Ready(Some(Ok(_)))
        ));
        assert_eq!(recorder.recorded(), vec![ResponseOutcome::Complete]);

        drop(tracked);

        assert_eq!(recorder.recorded(), vec![ResponseOutcome::Complete]);
    }

    #[test]
    fn reports_complete_for_a_body_that_is_empty_before_it_is_polled() {
        let recorder = Recorder::default();
        let body = StreamingBody {
            frames: Vec::new().into_iter(),
            end_stream: true,
        };
        let tracked = OutcomeTrackedBody::new(body, recorder.sink());

        assert_eq!(recorder.recorded(), vec![ResponseOutcome::Complete]);

        drop(tracked);

        assert_eq!(recorder.recorded(), vec![ResponseOutcome::Complete]);
    }

    #[test]
    fn outcome_labels_are_stable() {
        assert_eq!(ResponseOutcome::Complete.as_label(), "complete");
        assert_eq!(ResponseOutcome::Error.as_label(), "error");
        assert_eq!(ResponseOutcome::Incomplete.as_label(), "incomplete");
    }
}
