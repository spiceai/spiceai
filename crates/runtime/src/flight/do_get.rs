/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use arrow_flight::{
    Ticket,
    flight_service_server::FlightService,
    sql::{Any, Command},
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    datafusion::request_context_extension::get_current_datafusion,
    flight::{
        metrics, traced_ticket,
        util::{attach_cache_metadata, attach_trace_id_metadata},
    },
};
use runtime_request_context::{AsyncMarker, RequestContext};
use telemetry::timing::TimedStream;

use super::{Service, flightsql, to_tonic_err};

pub(crate) async fn handle(
    request: Request<Ticket>,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    // `get_flight_info` answered the client with a trace id and wrapped the
    // ticket with it. Adopting it here, before any query runs, is what makes
    // that id name this execution — the work that can fail — rather than the
    // planning call the client could not have correlated on.
    let request = adopt_ticket_trace_id(request).await;

    // The id the query will actually run under, in the order the task resolves
    // it: a client that pinned one on this request outranks the ticket, so
    // reporting the ticket's would name an id no record carries.
    let context = RequestContext::current(AsyncMarker::new().await);
    let trace_id = context
        .client_trace_id()
        .or_else(|| context.propagated_trace_id())
        .cloned();

    let msg: Any = match Message::decode(&*request.get_ref().ticket) {
        Ok(msg) => msg,
        Err(_) => {
            return with_trace_id(Box::pin(do_get_simple(request)).await, trace_id.as_deref());
        }
    };

    let result = match Command::try_from(msg).map_err(to_tonic_err)? {
        Command::CommandStatementQuery(command) => {
            Box::pin(flightsql::statement_query::do_get(command)).await
        }
        Command::CommandPreparedStatementQuery(command) => {
            Box::pin(flightsql::prepared_statement_query::do_get(command)).await
        }
        Command::CommandPreparedStatementUpdate(command) => {
            Box::pin(flightsql::prepared_statement_update::do_get(command)).await
        }
        Command::CommandGetCatalogs(command) => flightsql::get_catalogs::do_get(command).await,
        Command::CommandGetDbSchemas(command) => flightsql::get_schemas::do_get(command).await,
        Command::CommandGetTables(command) => flightsql::get_tables::do_get(command).await,
        Command::CommandGetPrimaryKeys(command) => {
            flightsql::get_primary_keys::do_get(&command).await
        }
        Command::CommandGetTableTypes(command) => flightsql::get_table_types::do_get(command).await,
        Command::CommandGetSqlInfo(command) => flightsql::get_sql_info::do_get(command).await,
        Command::CommandStatementIngest(command) => {
            let _start = metrics::track_flight_request("do_get", None).await;
            Err(Status::unimplemented(format!(
                "StatementIngest is not yet implemented: {command:?}"
            )))
        }
        Command::CommandGetXdbcTypeInfo(command) => {
            Box::pin(flightsql::get_xdbc_type_info::do_get(command)).await
        }
        // Additional Commands not yet supported
        Command::CommandStatementUpdate(_cmd) => {
            let _start = metrics::track_flight_request("do_get", Some("statement_update")).await;
            // CommandStatementUpdate should be sent via DoPut, not DoGet
            Err(Status::invalid_argument(
                "CommandStatementUpdate should be sent via DoPut, not DoGet. See the FlightSQL specification.",
            ))
        }
        Command::CommandStatementSubstraitPlan(cmd) => {
            Box::pin(flightsql::statement_substrait_plan::do_get(cmd)).await
        }
        Command::CommandGetCrossReference(cmd) => {
            let _start = metrics::track_flight_request("do_get", Some("get_cross_reference")).await;
            tracing::debug!("CommandGetCrossReference not yet implemented: {cmd:?}");
            Err(Status::unimplemented(
                "CommandGetCrossReference is not yet implemented",
            ))
        }
        Command::CommandGetExportedKeys(cmd) => {
            let _start = metrics::track_flight_request("do_get", Some("get_exported_keys")).await;
            tracing::debug!("CommandGetExportedKeys not yet implemented: {cmd:?}");
            Err(Status::unimplemented(
                "CommandGetExportedKeys is not yet implemented",
            ))
        }
        Command::CommandGetImportedKeys(cmd) => {
            let _start = metrics::track_flight_request("do_get", Some("get_imported_keys")).await;
            tracing::debug!("CommandGetImportedKeys not yet implemented: {cmd:?}");
            Err(Status::unimplemented(
                "CommandGetImportedKeys is not yet implemented",
            ))
        }
        // Action commands (handled via do_action, not do_get)
        Command::ActionBeginSavepointRequest(_)
        | Command::ActionBeginSavepointResult(_)
        | Command::ActionBeginTransactionRequest(_)
        | Command::ActionBeginTransactionResult(_)
        | Command::ActionCancelQueryRequest(_)
        | Command::ActionCancelQueryResult(_)
        | Command::ActionClosePreparedStatementRequest(_)
        | Command::ActionCreatePreparedStatementRequest(_)
        | Command::ActionCreatePreparedStatementResult(_)
        | Command::ActionCreatePreparedSubstraitPlanRequest(_)
        | Command::ActionEndSavepointRequest(_)
        | Command::ActionEndTransactionRequest(_) => {
            let _start = metrics::track_flight_request("do_get", None).await;
            Err(Status::invalid_argument(
                "Action commands should be sent via do_action, not do_get",
            ))
        }
        // Result types (returned from do_put, not used in do_get)
        Command::DoPutPreparedStatementResult(_) | Command::DoPutUpdateResult(_) => {
            let _start = metrics::track_flight_request("do_get", None).await;
            Err(Status::invalid_argument(
                "Result types should not be sent to do_get",
            ))
        }
        // Ticket types (used in do_get, not part of Command routing)
        Command::TicketStatementQuery(_) => {
            let _start = metrics::track_flight_request("do_get", None).await;
            Err(Status::internal(
                "TicketStatementQuery should not reach this code path",
            ))
        }
        Command::Unknown(any) => {
            let _start = metrics::track_flight_request("do_get", None).await;
            Err(Status::unimplemented(format!(
                "Unknown command type: {}",
                any.type_url
            )))
        }
    };

    with_trace_id(result, trace_id.as_deref())
}

/// Records the trace id a `get_flight_info` wrapped into the ticket on the
/// request, and returns the request holding the ticket underneath.
///
/// A ticket without one — minted by an older runtime, or by a client that
/// built its own — passes through unchanged and the query numbers itself.
async fn adopt_ticket_trace_id(request: Request<Ticket>) -> Request<Ticket> {
    let Some((trace_id, inner)) = traced_ticket::unwrap(request.get_ref()) else {
        return request;
    };

    RequestContext::current(AsyncMarker::new().await).set_propagated_trace_id(trace_id);

    let (metadata, extensions, _) = request.into_parts();
    Request::from_parts(metadata, extensions, inner)
}

/// Returns the trace id alongside the result stream, for clients that read
/// gRPC response metadata.
///
/// The id is also in the `FlightInfo` this ticket came from, which is the only
/// place a Flight SQL JDBC caller can read it; this covers everything else.
fn with_trace_id<T>(
    result: Result<Response<T>, Status>,
    trace_id: Option<&str>,
) -> Result<Response<T>, Status> {
    match (result, trace_id) {
        (Ok(mut response), Some(trace_id)) => {
            attach_trace_id_metadata(&mut response, trace_id);
            Ok(response)
        }
        (result, _) => result,
    }
}

async fn do_get_simple(
    request: Request<Ticket>,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = metrics::track_flight_request("do_get", Some("sql_query")).await;

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    let ticket = request.into_inner();
    tracing::trace!("do_get_simple: {ticket:?}");
    match std::str::from_utf8(&ticket.ticket) {
        Ok(sql) => {
            let pre_parsed_plan =
                super::check_read_only_sql(&context, &datafusion, sql, None).await?;
            let (output, cache_status) = Box::pin(Service::sql_to_flight_stream(
                datafusion,
                sql,
                None,
                pre_parsed_plan,
            ))
            .await?;

            let timed_output = TimedStream::new(output, move || start);

            let mut response =
                Response::new(Box::pin(timed_output) as <Service as FlightService>::DoGetStream);

            attach_cache_metadata(&mut response, cache_status, &context);

            Ok(response)
        }
        Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e}"))),
    }
}
