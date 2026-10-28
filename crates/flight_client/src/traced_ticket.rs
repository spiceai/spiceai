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

/// Identifies a ticket wrapped by [`wrap`].
///
/// Not a Flight SQL command: `DoGet` matches on it before command dispatch, so
/// it must not collide with a `type_url` the spec assigns.
pub const TRACED_TICKET_TYPE_URL: &str = "type.googleapis.com/spiceai.flight.TracedTicket";
const TRACE_ID_HEX_LEN: usize = 32;

/// Wraps `ticket` so it also carries `trace_id`, which must be normalized to 32
/// hexadecimal characters, not all zero.
///
/// The payload is the id's 32 hexadecimal characters followed by the original
/// ticket bytes. A fixed-width prefix rather than a nested message: the id is
/// one fixed-length field and the remainder is opaque, so the split needs no
/// schema, and the inner ticket is passed through byte-for-byte.
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
