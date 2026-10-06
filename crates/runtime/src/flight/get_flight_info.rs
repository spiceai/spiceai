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
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    sql::{Any, Command},
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    datafusion::request_context_extension::get_current_datafusion,
    flight::{metrics, traced_ticket, util::trace_id_app_metadata},
    task_history::correlation,
};
use runtime_request_context::{AsyncMarker, RequestContext};

use super::{Service, flightsql, to_tonic_err};

pub(crate) async fn handle(
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    // Resolved before any handler runs, so a failure while planning is logged
    // under the same id a successful call hands back. Every ticket this call
    // emits then carries it to `do_get` — the request that actually runs the
    // query — and the client reads it off the `FlightInfo`. Doing it here
    // rather than in each handler is what makes that true of every command
    // rather than of the ones someone remembered.
    let trace_id =
        correlation::publish_trace_id(&RequestContext::current(AsyncMarker::new().await));

    dispatch(request)
        .await
        .map(|response| response.map(|info| trace(info, &trace_id)))
}

/// Puts `trace_id` where a client can read it: in `app_metadata`, and on every
/// ticket the call hands out so the `do_get` that redeems one adopts it.
fn trace(info: FlightInfo, trace_id: &str) -> FlightInfo {
    FlightInfo {
        endpoint: info
            .endpoint
            .into_iter()
            .map(|endpoint| FlightEndpoint {
                ticket: endpoint.ticket.map(|t| traced_ticket::wrap(&t, trace_id)),
                ..endpoint
            })
            .collect(),
        ..info
    }
    .with_app_metadata(trace_id_app_metadata(trace_id))
}

async fn dispatch(request: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> {
    let Ok(message) = Any::decode(&*request.get_ref().cmd) else {
        return get_flight_info_simple(request).await;
    };

    // The arms below record per-command; a descriptor that is not a `Command`
    // reaches none of them, so it is recorded here.
    let command = match Command::try_from(message) {
        Ok(command) => command,
        Err(e) => {
            let _start = metrics::track_flight_request("get_flight_info", None).await;
            return Err(to_tonic_err(e));
        }
    };

    match command {
        Command::CommandStatementQuery(token) => {
            flightsql::statement_query::get_flight_info(token, request).await
        }
        Command::CommandPreparedStatementQuery(handle) => {
            flightsql::prepared_statement_query::get_flight_info(handle, request).await
        }
        Command::CommandPreparedStatementUpdate(handle) => {
            flightsql::prepared_statement_update::get_flight_info(handle, request).await
        }
        Command::CommandGetCatalogs(token) => {
            Ok(flightsql::get_catalogs::get_flight_info(token, request).await)
        }
        Command::CommandGetDbSchemas(token) => {
            Ok(flightsql::get_schemas::get_flight_info(&token, request).await)
        }
        Command::CommandGetTables(token) => {
            Ok(flightsql::get_tables::get_flight_info(&token, request).await)
        }
        Command::CommandGetSqlInfo(token) => {
            flightsql::get_sql_info::get_flight_info(&token, request).await
        }
        Command::CommandGetTableTypes(token) => {
            Ok(flightsql::get_table_types::get_flight_info(token, request).await)
        }
        Command::CommandGetPrimaryKeys(token) => {
            Ok(flightsql::get_primary_keys::get_flight_info(&token, request).await)
        }
        Command::CommandStatementIngest(_ingest_cmd) => {
            let _start = metrics::track_flight_request("get_flight_info", None).await;
            // CommandStatementIngest is handled via DoPut, not GetFlightInfo + DoGet
            // Return metadata indicating this is a write operation
            let fd = request.into_inner();
            let info = FlightInfo::new().with_descriptor(fd);
            Ok(Response::new(info))
        }
        Command::CommandGetXdbcTypeInfo(token) => {
            let _start =
                metrics::track_flight_request("get_flight_info", Some("get_xdbc_type_info")).await;
            Ok(Response::new(
                flightsql::get_xdbc_type_info::get_flight_info(token, request),
            ))
        }
        // Additional Commands not yet supported
        Command::CommandStatementUpdate(_cmd) => {
            let _start =
                metrics::track_flight_request("get_flight_info", Some("statement_update")).await;
            // Per FlightSQL spec, CommandStatementUpdate is primarily handled via DoPut.
            // Return minimal FlightInfo indicating this is a write operation.
            let fd = request.into_inner();
            let info = FlightInfo::new().with_descriptor(fd);
            Ok(Response::new(info))
        }
        Command::CommandStatementSubstraitPlan(cmd) => {
            flightsql::statement_substrait_plan::get_flight_info(cmd, request).await
        }
        Command::CommandGetCrossReference(cmd) => {
            let _start =
                metrics::track_flight_request("get_flight_info", Some("get_cross_reference")).await;
            tracing::debug!("CommandGetCrossReference not yet implemented: {cmd:?}");
            Err(Status::unimplemented(
                "CommandGetCrossReference is not yet implemented",
            ))
        }
        Command::CommandGetExportedKeys(cmd) => {
            let _start =
                metrics::track_flight_request("get_flight_info", Some("get_exported_keys")).await;
            tracing::debug!("CommandGetExportedKeys not yet implemented: {cmd:?}");
            Err(Status::unimplemented(
                "CommandGetExportedKeys is not yet implemented",
            ))
        }
        Command::CommandGetImportedKeys(cmd) => {
            let _start =
                metrics::track_flight_request("get_flight_info", Some("get_imported_keys")).await;
            tracing::debug!("CommandGetImportedKeys not yet implemented: {cmd:?}");
            Err(Status::unimplemented(
                "CommandGetImportedKeys is not yet implemented",
            ))
        }
        // Action commands (handled via do_action, not get_flight_info)
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
            let _start = metrics::track_flight_request("get_flight_info", None).await;
            Err(Status::invalid_argument(
                "Action commands should be sent via do_action, not get_flight_info",
            ))
        }
        // Result types (returned from do_put, not used in get_flight_info)
        Command::DoPutPreparedStatementResult(_) | Command::DoPutUpdateResult(_) => {
            let _start = metrics::track_flight_request("get_flight_info", None).await;
            Err(Status::invalid_argument(
                "Result types should not be sent to get_flight_info",
            ))
        }
        // Ticket types (used in do_get, not get_flight_info)
        Command::TicketStatementQuery(_) => {
            let _start = metrics::track_flight_request("get_flight_info", None).await;
            Err(Status::invalid_argument(
                "Ticket types should be sent via do_get, not get_flight_info",
            ))
        }
        Command::Unknown(any) => {
            let _start = metrics::track_flight_request("get_flight_info", None).await;
            Err(Status::unimplemented(format!(
                "Unknown command type: {}",
                any.type_url
            )))
        }
    }
}

async fn get_flight_info_simple(
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    tracing::trace!("get_flight_info_simple: {request:?}");
    let _start = metrics::track_flight_request("get_flight_info", Some("sql_query")).await;

    let fd = request.into_inner();

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    let sql: &str = std::str::from_utf8(&fd.cmd).map_err(to_tonic_err)?;
    let (arrow_schema, _) = Service::get_arrow_schema(datafusion, sql)
        .await
        .map_err(to_tonic_err)?;

    let info = FlightInfo {
        flight_descriptor: Some(fd.clone()),
        endpoint: vec![FlightEndpoint {
            ticket: Some(Ticket { ticket: fd.cmd }),
            ..Default::default()
        }],
        ..Default::default()
    }
    .try_with_schema(&arrow_schema)
    .map_err(to_tonic_err)?;

    Ok(Response::new(info))
}
