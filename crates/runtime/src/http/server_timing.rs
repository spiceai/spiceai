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

//! `Server-Timing` on `/v1/sql` responses.
//!
//! The handler marks its response with [`ServerTiming`]. The metrics layer,
//! which owns the clock behind the HTTP duration histograms, writes the value.
//! A buffered body is complete when the head is sent, so the head carries the
//! total. A streamed body is not, so its total can only follow the body as a
//! trailer. A partial number in the head would read as the total.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use http::{HeaderMap, HeaderValue, Version, header::TE};
use http_body::{Body, Frame, SizeHint};
use pin_project::pin_project;
use telemetry::timing::RecordedDuration;

use crate::server_timing::{SERVER_TIMING, field_value};

/// Where a `/v1/sql` response reports its server time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ServerTiming {
    /// The body is complete before the head is sent: the head carries the total.
    Header,
    /// The body streams, so the total is known only at its end and is sent as a
    /// trailer.
    Trailer,
}

/// Whether this request lets the server send trailer fields.
///
/// HTTP/2 always does. HTTP/1.1 does only when the client sent `TE: trailers`,
/// the same exact-match test hyper applies before writing them. Otherwise
/// hyper silently drops a trailer, so the response must not announce one.
pub(crate) fn request_accepts_trailers(version: Version, headers: &HeaderMap) -> bool {
    version == Version::HTTP_2 || headers.get(TE).is_some_and(|te| te == "trailers")
}

/// The `Trailer` response header value that announces the field. HTTP/1.1
/// hyper writes only the trailer fields the response head declared.
pub(crate) const TRAILER_DECLARATION: HeaderValue = HeaderValue::from_static("Server-Timing");

/// A body that, once `inner` has ended, sends one trailers frame carrying
/// `Server-Timing` with the value `recorded` holds.
///
/// `recorded` is set by the layer's end-of-body callback, which runs inside
/// `inner` (see `OutcomeTrackedBody`). It is set only when the body completed,
/// so a body that failed or was cut off sends no trailer.
#[pin_project]
pub(crate) struct ServerTimingTrailerBody<B> {
    #[pin]
    inner: B,
    recorded: RecordedDuration,
    state: TrailerState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TrailerState {
    /// `inner` may still yield frames.
    Streaming,
    /// `inner` reported end-of-stream with its last data frame, so the trailer
    /// goes out on the next poll.
    Pending,
    /// The trailer was sent, or will not be.
    Done,
}

impl<B> ServerTimingTrailerBody<B> {
    pub(crate) fn new(inner: B, recorded: RecordedDuration) -> Self {
        Self {
            inner,
            recorded,
            state: TrailerState::Streaming,
        }
    }
}

fn trailers(recorded: &RecordedDuration) -> Option<HeaderMap> {
    let milliseconds = recorded.get()?;
    let mut trailers = HeaderMap::with_capacity(1);
    trailers.insert(SERVER_TIMING, field_value(milliseconds));
    Some(trailers)
}

impl<B: Body> Body for ServerTimingTrailerBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        match *this.state {
            TrailerState::Done => Poll::Ready(None),
            TrailerState::Pending => {
                *this.state = TrailerState::Done;
                Poll::Ready(trailers(this.recorded).map(|t| Ok(Frame::trailers(t))))
            }
            TrailerState::Streaming => {
                let mut inner = this.inner;
                match inner.as_mut().poll_frame(cx) {
                    Poll::Ready(None) => {
                        *this.state = TrailerState::Done;
                        Poll::Ready(trailers(this.recorded).map(|t| Ok(Frame::trailers(t))))
                    }
                    Poll::Ready(Some(Ok(frame))) => {
                        // A body that brings its own trailers keeps them. The
                        // response bodies this wraps never do.
                        if frame.is_trailers() {
                            *this.state = TrailerState::Done;
                        } else if inner.as_ref().get_ref().is_end_stream() {
                            *this.state = TrailerState::Pending;
                        }
                        Poll::Ready(Some(Ok(frame)))
                    }
                    Poll::Ready(Some(Err(e))) => {
                        *this.state = TrailerState::Done;
                        Poll::Ready(Some(Err(e)))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        // Never end before the trailer has had its chance: a caller that stops
        // polling at end-of-stream would drop it.
        self.state == TrailerState::Done
    }

    fn size_hint(&self) -> SizeHint {
        // An exact size would make hyper send a `Content-Length` body, which
        // cannot carry trailers, so only the lower bound is passed on.
        let mut hint = SizeHint::new();
        hint.set_lower(self.inner.size_hint().lower());
        hint
    }
}
