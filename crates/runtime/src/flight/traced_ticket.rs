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

//! Carries a query's trace id from `GetFlightInfo` to `DoGet`.
//!
//! Flight SQL splits one query across two RPCs, and each is a request of its
//! own: separate headers, separate [`RequestContext`], separate trace id.
//! `GetFlightInfo` is the one that can answer the client — its `FlightInfo`
//! carries the `app_metadata` a driver can read — but `DoGet` is the one that
//! runs the query and logs its failure. An id returned by the first that the
//! second does not use names the planning call and correlates nothing.
//!
//! So `GetFlightInfo` resolves the id, returns it, and wraps the ticket it
//! hands out; `DoGet` unwraps it and adopts the id before the query starts.
//! The wrapper is transparent to clients, which treat a ticket as opaque bytes
//! and echo it back unread.
//!
//! An unwrapped ticket — one minted by an older runtime, or by a client that
//! built its own — still works: [`unwrap`] reports no id and the query numbers
//! itself as it always has.
//!
//! [`RequestContext`]: runtime_request_context::RequestContext

use std::sync::Arc;

use arrow_flight::{Ticket, sql::Any};
use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use runtime_request_context::{TRACE_ID_HEX_LEN, normalize_trace_id};

/// Identifies a ticket wrapped by [`wrap`].
///
/// Not a Flight SQL command: `DoGet` matches on it before command dispatch, so
/// it must not collide with a `type_url` the spec assigns.
const TRACED_TICKET_TYPE_URL: &str = "type.googleapis.com/spiceai.flight.TracedTicket";

/// Wraps `ticket` so it also carries `trace_id`.
///
/// The payload is the id's 32 hexadecimal characters followed by the original
/// ticket bytes. A fixed-width prefix rather than a nested message: the id is
/// one fixed-length field and the remainder is opaque, so the split needs no
/// schema, and the inner ticket is passed through byte-for-byte.
#[must_use]
pub(crate) fn wrap(ticket: Ticket, trace_id: &str) -> Ticket {
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

/// Splits a ticket produced by [`wrap`] back into its trace id and the ticket
/// underneath.
///
/// `None` for any ticket this runtime did not wrap, including one whose id is
/// unreadable — an unusable id costs correlation, never the query.
#[must_use]
pub(crate) fn unwrap(ticket: &Ticket) -> Option<(Arc<str>, Ticket)> {
    // Decoded from the `Bytes` rather than from a slice of it: prost copies a
    // `bytes` field out of a slice, and shares it out of a `Bytes`. Cloning
    // first is a refcount bump, and keeps the inner ticket below a view of the
    // wire buffer instead of a copy.
    let any = Any::decode(ticket.ticket.clone()).ok()?;
    if any.type_url != TRACED_TICKET_TYPE_URL {
        return None;
    }

    // A `type_url` this runtime writes and reads should never carry a payload
    // it cannot split, so say so rather than silently dropping the id.
    let Some((trace_id, inner)) = split_payload(&any.value) else {
        tracing::warn!(
            "Ignoring the trace id on a Flight ticket: expected {TRACE_ID_HEX_LEN} leading \
             hexadecimal characters, got {} bytes",
            any.value.len()
        );
        return None;
    };

    Some((trace_id, Ticket { ticket: inner }))
}

fn split_payload(value: &Bytes) -> Option<(Arc<str>, Bytes)> {
    if value.len() < TRACE_ID_HEX_LEN {
        return None;
    }
    let trace_id = normalize_trace_id(std::str::from_utf8(&value[..TRACE_ID_HEX_LEN]).ok()?)?;
    Some((trace_id, value.slice(TRACE_ID_HEX_LEN..)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

    fn ticket(bytes: &'static [u8]) -> Ticket {
        Ticket {
            ticket: Bytes::from_static(bytes),
        }
    }

    #[test]
    fn round_trips_the_id_and_the_ticket_underneath() {
        let inner = ticket(b"SELECT 1");
        let (trace_id, unwrapped) =
            unwrap(&wrap(inner.clone(), TRACE_ID)).expect("a wrapped ticket unwraps");

        assert_eq!(&*trace_id, TRACE_ID);
        assert_eq!(unwrapped, inner, "the inner ticket must survive byte-exact");
    }

    /// A ticket minted by an older runtime, or by a client that built its own,
    /// has to keep working — it just carries no id.
    #[test]
    fn reports_no_id_for_a_ticket_it_did_not_wrap() {
        assert!(unwrap(&ticket(b"SELECT 1")).is_none());

        let flightsql_command = Ticket {
            ticket: Any {
                type_url: "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
                    .to_string(),
                value: Bytes::from_static(b"whatever"),
            }
            .encode_to_vec()
            .into(),
        };
        assert!(unwrap(&flightsql_command).is_none());
    }

    /// An empty inner ticket is a legitimate payload — the id is the whole of
    /// it — and must not be mistaken for a truncated one.
    #[test]
    fn handles_an_empty_inner_ticket() {
        let (trace_id, unwrapped) = unwrap(&wrap(
            Ticket {
                ticket: Bytes::new(),
            },
            TRACE_ID,
        ))
        .expect("a wrapped empty ticket unwraps");

        assert_eq!(&*trace_id, TRACE_ID);
        assert!(unwrapped.ticket.is_empty());
    }

    /// A payload that cannot yield a usable id costs correlation, not the
    /// query: the ticket reads as untraced rather than failing.
    #[test]
    fn rejects_a_payload_that_cannot_carry_an_id() {
        for payload in [
            Bytes::from_static(b"too short"),
            // Right length, not hexadecimal.
            Bytes::from_static(b"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"),
            // Right length and hexadecimal, but the all-zero id correlates
            // nothing — every request sending it would share one id.
            Bytes::from_static(b"00000000000000000000000000000000"),
        ] {
            let malformed = Ticket {
                ticket: Any {
                    type_url: TRACED_TICKET_TYPE_URL.to_string(),
                    value: payload,
                }
                .encode_to_vec()
                .into(),
            };
            assert!(unwrap(&malformed).is_none());
        }
    }
}
