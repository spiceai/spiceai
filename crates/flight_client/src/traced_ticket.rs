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

//! The opaque Flight ticket envelope for a runtime-generated trace id.

use arrow_flight::{Ticket, sql::Any};
use bytes::{BufMut, BytesMut};
use prost::Message;

pub const TRACED_TICKET_TYPE_URL: &str = "type.googleapis.com/spiceai.flight.TracedTicket";
const TRACE_ID_HEX_LEN: usize = 32;

/// Carries a normalized trace id alongside an otherwise byte-exact ticket.
///
/// Spice adopts this id before dispatching the inner command. Unlike a
/// client-pinned header, it joins the trace without reconciling stored rows.
/// `trace_id` must contain 32 hexadecimal characters, not all zero.
#[must_use]
pub fn wrap(ticket: &Ticket, trace_id: &str) -> Ticket {
    debug_assert_eq!(trace_id.len(), TRACE_ID_HEX_LEN, "trace ids are normalized");
    let mut value = BytesMut::with_capacity(TRACE_ID_HEX_LEN + ticket.ticket.len());
    value.put_slice(trace_id.as_bytes());
    value.put_slice(&ticket.ticket);
    Ticket {
        ticket: Any {
            type_url: TRACED_TICKET_TYPE_URL.to_string(),
            value: value.freeze(),
        }
        .encode_to_vec()
        .into(),
    }
}
