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

//! `server-timing` on the `DoGet` that runs a query.
//!
//! A Flight query is usually two RPCs: `GetFlightInfo` plans it and returns a
//! ticket, and `DoGet` redeems the ticket and streams the result. The trailer
//! of a successful query `DoGet` reports the server time of both handlers,
//! never the time between the two calls. Each value is the
//! `flight_request_duration_ms` sample its handler recorded.
//!
//! `GetFlightInfo` hands its sample to `DoGet` through a small in-process map
//! keyed by the ticket it returned. A `DoGet` that finds nothing there reports
//! its own time only. That happens when another instance served the
//! `GetFlightInfo`, the client built the ticket itself, or the entry expired.
//!
//! The trailer is written into the trailers frame tonic already sends with
//! `grpc-status: 0`, by the request-context middleware's body wrapper. The
//! response stream itself is untouched, so every stream wrapper and metric
//! sees the same items and the same end it always did.

use std::{
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
    time::Duration,
};

use arrow_flight::FlightInfo;
use http::HeaderMap;
use http_body::{Body, Frame, SizeHint};
use pin_project::pin_project;
use runtime_request_context::{AsyncMarker, Extension, RequestContext};
use telemetry::timing::RecordedDuration;

use crate::server_timing::{SERVER_TIMING, field_value};

/// How many `GetFlightInfo` samples wait for their `DoGet` at once. An entry is
/// a 16-byte key and an `f64`, so the bound costs well under a megabyte.
const PENDING_CAPACITY: u64 = 10_000;

/// How long a `GetFlightInfo` sample waits for its `DoGet`. Clients redeem a
/// ticket right away. One that waits longer gets its `DoGet` time alone.
const PENDING_TTL: Duration = Duration::from_mins(5);

/// `GetFlightInfo` samples by the ticket they returned.
///
/// Keyed by a 128-bit digest of the full ticket bytes rather than the bytes
/// themselves: a ticket can carry a whole SQL statement, and the key only has
/// to tell this instance's outstanding tickets apart. Every `GetFlightInfo`
/// wraps its ticket with its own trace id, so two calls for the same statement
/// still get distinct keys.
static PENDING_GET_FLIGHT_INFO: LazyLock<moka::future::Cache<u128, f64>> = LazyLock::new(|| {
    moka::future::Cache::builder()
        .max_capacity(PENDING_CAPACITY)
        .time_to_live(PENDING_TTL)
        .build()
});

/// The digest a `DoGet` looks its ticket up by.
#[must_use]
pub(crate) fn ticket_digest(ticket: &[u8]) -> u128 {
    twox_hash::XxHash3_128::oneshot(ticket)
}

/// The timing of a query RPC, carried on its request context.
///
/// `metrics::track_flight_request` hands the RPC's `flight_request_duration_ms`
/// sample to it, so the value read back is the one the histogram received.
#[derive(Clone)]
pub(crate) struct QueryRpcTiming {
    sample: RecordedDuration,
    role: Role,
}

#[derive(Clone, Copy)]
enum Role {
    /// Plans a query and returns its ticket. Its sample goes to the `DoGet`
    /// that redeems the ticket and is not reported on its own, or a client
    /// adding up both RPCs would count the planning twice.
    GetFlightInfo,
    /// Runs the query. `get_flight_info_ms` is the sample of the
    /// `GetFlightInfo` that issued its ticket, when this instance served it.
    DoGet { get_flight_info_ms: Option<f64> },
}

impl Extension for QueryRpcTiming {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl QueryRpcTiming {
    /// Where the RPC's measurement reports its sample.
    pub(crate) fn sample(&self) -> RecordedDuration {
        self.sample.clone()
    }

    /// The `server-timing` total of a completed `DoGet`, in milliseconds.
    fn do_get_total(&self) -> Option<f64> {
        match self.role {
            Role::GetFlightInfo => None,
            Role::DoGet { get_flight_info_ms } => self
                .sample
                .get()
                .map(|do_get_ms| do_get_ms + get_flight_info_ms.unwrap_or(0.0)),
        }
    }
}

/// Marks the current `GetFlightInfo` as planning a query, so its sample is kept
/// for the `DoGet` that redeems its ticket. Call before the handler starts its
/// measurement.
pub(crate) async fn time_get_flight_info() {
    RequestContext::current(AsyncMarker::new().await).insert_extension(QueryRpcTiming {
        sample: RecordedDuration::default(),
        role: Role::GetFlightInfo,
    });
}

/// Keeps the sample of a completed query `GetFlightInfo` for every ticket it
/// returned.
///
/// `info` is the response as the client receives it, after
/// `get_flight_info::trace` wrapped each ticket, so the key is the exact bytes
/// the `DoGet` will present.
pub(crate) async fn remember_get_flight_info(request_context: &RequestContext, info: &FlightInfo) {
    let Some(timing) = request_context.extension::<QueryRpcTiming>() else {
        return;
    };
    if !matches!(timing.role, Role::GetFlightInfo) {
        return;
    }
    // Set when the handler's measurement dropped, which is before it returned.
    let Some(milliseconds) = timing.sample.get() else {
        return;
    };
    for ticket in info.endpoint.iter().filter_map(|e| e.ticket.as_ref()) {
        PENDING_GET_FLIGHT_INFO
            .insert(ticket_digest(&ticket.ticket), milliseconds)
            .await;
    }
}

/// Marks the current `DoGet` as running a query and claims the sample of the
/// `GetFlightInfo` that issued its ticket. Call before the handler starts its
/// measurement.
///
/// `ticket` is the digest of the ticket as the client presented it, before the
/// trace id was unwrapped from it.
pub(crate) async fn time_do_get(ticket: u128) {
    let get_flight_info_ms = PENDING_GET_FLIGHT_INFO.remove(&ticket).await;
    RequestContext::current(AsyncMarker::new().await).insert_extension(QueryRpcTiming {
        sample: RecordedDuration::default(),
        role: Role::DoGet { get_flight_info_ms },
    });
}

/// Adds `server-timing` to the trailers of a query `DoGet` that completed.
fn add_to_trailers(request_context: &RequestContext, trailers: &mut HeaderMap) {
    // A failed call carries its error status in this frame instead. Its time
    // is not a query's server time, and its measurement may not have recorded.
    if trailers
        .get("grpc-status")
        .is_none_or(|status| status.as_bytes() != b"0")
    {
        return;
    }
    let Some(total) = request_context
        .extension::<QueryRpcTiming>()
        .and_then(|timing| timing.do_get_total())
    else {
        return;
    };
    trailers.insert(SERVER_TIMING, field_value(total));
}

/// The Flight response body, with `server-timing` added to the trailers frame
/// of a query `DoGet` that completed.
///
/// tonic sends that frame after the response stream ends. Every measurement
/// has recorded by then: `TimedStream` records the `DoGet` sample when it sees
/// the end of the stream.
#[pin_project]
pub struct ServerTimingTrailers<B> {
    #[pin]
    inner: B,
    request_context: Arc<RequestContext>,
}

impl<B> ServerTimingTrailers<B> {
    pub(crate) fn new(inner: B, request_context: Arc<RequestContext>) -> Self {
        Self {
            inner,
            request_context,
        }
    }
}

impl<B: Body> Body for ServerTimingTrailers<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        match this.inner.poll_frame(cx) {
            Poll::Ready(Some(Ok(mut frame))) => {
                if let Some(trailers) = frame.trailers_mut() {
                    add_to_trailers(this.request_context, trailers);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            polled => polled,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
