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
    flight::{metrics, util::attach_cache_metadata},
    timing::TimedStream,
};
use runtime_request_context::{AsyncMarker, RequestContext};

use super::{Service, flightsql, to_tonic_err};

pub(crate) async fn handle(
    request: Request<Ticket>,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let msg: Any = match Message::decode(&*request.get_ref().ticket) {
        Ok(msg) => msg,
        Err(_) => return Box::pin(do_get_simple(request)).await,
    };

    match Command::try_from(msg).map_err(to_tonic_err)? {
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
        Command::CommandStatementUpdate(cmd) => {
            let _start = metrics::track_flight_request("do_get", Some("statement_update")).await;
            tracing::debug!("CommandStatementUpdate not yet implemented: {cmd:?}");
            Err(Status::unimplemented(
                "CommandStatementUpdate is not yet implemented",
            ))
        }
        Command::CommandStatementSubstraitPlan(cmd) => {
            let _start =
                metrics::track_flight_request("do_get", Some("statement_substrait_plan")).await;
            tracing::debug!("CommandStatementSubstraitPlan not yet implemented: {cmd:?}");
            Err(Status::unimplemented(
                "CommandStatementSubstraitPlan is not yet implemented",
            ))
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
            let (output, cache_status) =
                Box::pin(Service::sql_to_flight_stream(datafusion, sql, None)).await?;

            let timed_output = TimedStream::new(output, move || start);

            let mut response =
                Response::new(Box::pin(timed_output) as <Service as FlightService>::DoGetStream);

            attach_cache_metadata(&mut response, cache_status, &context);

            Ok(response)
        }
        Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e}"))),
    }
}
