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

//! `FlightSQL` `CommandStatementSubstraitPlan` handlers.
//!
//! Per the `FlightSQL` spec, clients submit a serialized [`substrait::proto::Plan`]
//! wrapped in a [`SubstraitPlan`] message. The runtime decodes the plan, lowers
//! it to a `DataFusion` [`LogicalPlan`] via `datafusion-substrait`, and then runs
//! it through the same execution path as a SQL statement.

use std::sync::Arc;

use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    flight_service_server::FlightService,
    sql::{self, ProstMessageExt},
};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use prost::Message;
use tonic::{Request, Response, Status};

use crate::datafusion::DataFusion;
use crate::datafusion::query::QueryBuilder;
use crate::datafusion::request_context_extension::get_current_datafusion;
use crate::datafusion::sql_validator::validate_sql_query_read_only;
use crate::flight::{
    Service, handle_datafusion_error, is_auth_read_only,
    metrics::track_flight_request,
    to_tonic_err,
    util::{attach_cache_metadata, set_flightsql_protocol},
};
use runtime_request_context::{AsyncMarker, RequestContext};
use telemetry::timing::TimedStream;

/// Decodes the Substrait protobuf payload out of a
/// [`sql::CommandStatementSubstraitPlan`] and computes a stable cache key for
/// the plan bytes. Split from [`decode_plan`] so the validation/decoding
/// rules can be unit-tested without spinning up a `DataFusion` session.
fn decode_plan_proto(cmd: &sql::CommandStatementSubstraitPlan) -> Result<(Plan, String), Status> {
    let substrait = cmd.plan.as_ref().ok_or_else(|| {
        Status::invalid_argument("CommandStatementSubstraitPlan.plan is required")
    })?;

    if substrait.plan.is_empty() {
        return Err(Status::invalid_argument(
            "CommandStatementSubstraitPlan.plan.plan must not be empty",
        ));
    }

    let proto = Plan::decode(substrait.plan.as_ref())
        .map_err(|e| Status::invalid_argument(format!("Failed to decode Substrait plan: {e}")))?;

    // Cache key namespaced so it cannot collide with a SQL query string.
    let cache_key = format!("substrait:{}", sha256_hex(substrait.plan.as_ref()));
    Ok((proto, cache_key))
}

/// Lowers the wire representation of a Substrait plan to a `DataFusion`
/// [`LogicalPlan`]. Returns a synthetic cache key derived from the plan bytes
/// so that identical Substrait plans share results-cache entries.
async fn decode_plan(
    cmd: &sql::CommandStatementSubstraitPlan,
    datafusion: &Arc<DataFusion>,
) -> Result<(LogicalPlan, String), Status> {
    let (proto, cache_key) = decode_plan_proto(cmd)?;

    let session = datafusion.ctx.state();
    let plan = from_substrait_plan(&session, &proto)
        .await
        .map_err(map_substrait_error)?;

    Ok((plan, cache_key))
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        // `write!` formats directly into the preallocated `String` without
        // an intermediate allocation per byte.
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn map_substrait_error(e: DataFusionError) -> Status {
    match e {
        DataFusionError::Substrait(msg) => Status::invalid_argument(msg),
        DataFusionError::NotImplemented(msg) => {
            Status::unimplemented(format!("Substrait feature not supported: {msg}"))
        }
        other => handle_datafusion_error(other),
    }
}

/// Get a `FlightInfo` for executing a Substrait plan.
pub(crate) async fn get_flight_info(
    cmd: sql::CommandStatementSubstraitPlan,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    tracing::trace!("get_flight_info_substrait_plan");
    let _start = track_flight_request("get_flight_info", Some("statement_substrait_plan")).await;
    set_flightsql_protocol().await;

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    let (plan, cache_key) = decode_plan(&cmd, &datafusion).await?;

    let read_only = is_auth_read_only(&context);
    if read_only {
        validate_sql_query_read_only(&plan)
            .map_err(|e| Status::permission_denied(format!("Write access denied. {e}")))?;
    }

    let query = QueryBuilder::from_plan(plan, cache_key, Arc::clone(&datafusion))
        .read_only(read_only)
        .build();
    let (dataset_schema, _) = query.get_schema().await.map_err(handle_datafusion_error)?;
    let dataset_schema = arrow_tools::schema::expand_views_schema(&dataset_schema);

    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: cmd.as_any().encode_to_vec().into(),
    });

    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .try_with_schema(&dataset_schema)
        .map_err(to_tonic_err)?
        .with_descriptor(fd);

    Ok(Response::new(info))
}

/// Execute a Substrait plan and stream the results.
pub(crate) async fn do_get(
    cmd: sql::CommandStatementSubstraitPlan,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = track_flight_request("do_get", Some("statement_substrait_plan")).await;
    set_flightsql_protocol().await;

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    let (plan, cache_key) = decode_plan(&cmd, &datafusion).await?;

    if is_auth_read_only(&context) {
        validate_sql_query_read_only(&plan)
            .map_err(|e| Status::permission_denied(format!("Write access denied. {e}")))?;
    }

    let (output, from_cache) =
        Box::pin(Service::plan_to_flight_stream(datafusion, plan, cache_key)).await?;
    let timed_output = TimedStream::new(output, move || start);

    let mut response =
        Response::new(Box::pin(timed_output) as <Service as FlightService>::DoGetStream);
    attach_cache_metadata(&mut response, from_cache, &context);
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::sql::SubstraitPlan;
    use bytes::Bytes;
    use tonic::Code;

    fn cmd(plan: Option<SubstraitPlan>) -> sql::CommandStatementSubstraitPlan {
        sql::CommandStatementSubstraitPlan {
            plan,
            transaction_id: None,
        }
    }

    #[test]
    fn decode_plan_proto_missing_plan_returns_invalid_argument() {
        let err = decode_plan_proto(&cmd(None)).expect_err("missing plan must error");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("plan is required"), "{err}");
    }

    #[test]
    fn decode_plan_proto_empty_bytes_returns_invalid_argument() {
        let err = decode_plan_proto(&cmd(Some(SubstraitPlan {
            plan: Bytes::new(),
            version: String::new(),
        })))
        .expect_err("empty plan must error");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("must not be empty"), "{err}");
    }

    #[test]
    fn decode_plan_proto_invalid_bytes_returns_invalid_argument() {
        // 0x0a = field 1, wire-type 2 (length-delimited); 0x10 = "length 16
        // bytes follow" — but no further bytes are supplied, forcing prost
        // into a buffer-underflow decode error.
        let err = decode_plan_proto(&cmd(Some(SubstraitPlan {
            plan: Bytes::from_static(&[0x0a, 0x10]),
            version: String::new(),
        })))
        .expect_err("invalid bytes must error");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(
            err.message().contains("Failed to decode Substrait plan"),
            "{err}"
        );
    }

    #[test]
    fn decode_plan_proto_returns_namespaced_cache_key() {
        let bytes = Bytes::from_static(b"\x08\x00"); // valid empty Plan (version absent)
        let (_proto, key) = decode_plan_proto(&cmd(Some(SubstraitPlan {
            plan: bytes.clone(),
            version: String::new(),
        })))
        .expect("valid empty Plan should decode");
        assert!(key.starts_with("substrait:"));
        // Same bytes yield the same key.
        let (_proto2, key2) = decode_plan_proto(&cmd(Some(SubstraitPlan {
            plan: bytes,
            version: String::new(),
        })))
        .expect("decode");
        assert_eq!(key, key2);
    }

    #[test]
    fn sha256_hex_is_lowercase_64_chars() {
        let out = sha256_hex(b"");
        assert_eq!(out.len(), 64);
        assert!(
            out.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }
}
