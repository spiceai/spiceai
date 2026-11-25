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

//! The Flight ticket for a Substrait plan whose literal values travel as
//! parameter bindings.
//!
//! A sidecar that sends the same query shape with different values sends the
//! same plan template every time; the values ride in the Substrait `Plan`'s own
//! `parameter_bindings`, one per `DynamicParameter` (`$N` in `DataFusion`, bound
//! by anchor `N - 1`). An upstream that knows this ticket decodes the template
//! once and binds each request's values into it.
//!
//! It is a ticket of its own rather than a [`CommandStatementSubstraitPlan`]
//! because an upstream that predates it would decode the plan, ignore the
//! bindings (`DataFusion` neither reads nor writes them), and fail on, or worse
//! run, a plan whose placeholders nobody bound. An unknown `type_url` is
//! refused with `Unimplemented` instead, which a client can fall back on.
//!
//! [`CommandStatementSubstraitPlan`]: arrow_flight::sql::CommandStatementSubstraitPlan

use arrow_flight::{Ticket, sql::Any};
use bytes::Bytes;
use prost::Message;

/// The `GetSqlInfo` code under which a Spice runtime reports the version of
/// the parameterized Substrait ticket it accepts: [`PARAMETERIZED_SUBSTRAIT_VERSION`]
/// when it accepts this one. Flight SQL reserves codes 0 to 9,999; custom
/// codes start at 10,000. A server that does not report the code, version 0,
/// does not accept the ticket. Another server's custom codes mean what that
/// server says, so the code is read only from a server whose
/// `FlightSqlServerName` is [`SPICE_SERVER_NAME`].
pub const SQL_INFO_PARAMETERIZED_SUBSTRAIT: u32 = 10_000;

/// The version of the parameterized Substrait ticket ([`wrap`]) this build
/// sends and accepts.
pub const PARAMETERIZED_SUBSTRAIT_VERSION: i32 = 1;

/// The `FlightSqlServerName` a Spice runtime reports in `GetSqlInfo`.
pub const SPICE_SERVER_NAME: &str = "Spice.ai";

/// Identifies a ticket made by [`wrap`].
///
/// Not a Flight SQL command: `DoGet` matches on it before command dispatch, so
/// it must not collide with a `type_url` the spec assigns.
pub const PARAMETERIZED_SUBSTRAIT_PLAN_TYPE_URL: &str =
    "type.googleapis.com/spiceai.flight.ParameterizedSubstraitPlan";

/// Wraps an encoded Substrait `Plan` whose `parameter_bindings` hold the values
/// of its dynamic parameters.
///
/// The plan is passed through byte-for-byte. To carry a trace id, wrap the
/// result with [`crate::traced_ticket::wrap`], as for a plain Substrait ticket:
/// the trace envelope is outermost, so an upstream adopts the id before it
/// looks at the plan.
#[must_use]
pub fn wrap(plan: Bytes) -> Ticket {
    Ticket {
        ticket: Any {
            type_url: PARAMETERIZED_SUBSTRAIT_PLAN_TYPE_URL.to_string(),
            value: plan,
        }
        .encode_to_vec()
        .into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traced_ticket::{TRACED_TICKET_TYPE_URL, wrap as wrap_traced};

    const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

    /// An upstream reads the plan back out of the `Any` exactly as sent, and a
    /// traced ticket nests it unchanged after the 32-character id.
    #[test]
    fn nests_inside_the_trace_envelope_byte_for_byte() {
        let plan = Bytes::from_static(b"\x0a\x03abc encoded plan");
        let ticket = wrap(plan.clone());

        let any = Any::decode(ticket.ticket.clone()).expect("the ticket is an Any");
        assert_eq!(any.type_url, PARAMETERIZED_SUBSTRAIT_PLAN_TYPE_URL);
        assert_eq!(any.value, plan);

        let traced = wrap_traced(&ticket, TRACE_ID);
        let outer = Any::decode(traced.ticket).expect("the traced ticket is an Any");
        assert_eq!(outer.type_url, TRACED_TICKET_TYPE_URL);
        assert_eq!(&outer.value[..32], TRACE_ID.as_bytes());
        assert_eq!(outer.value.slice(32..), ticket.ticket);
    }
}
