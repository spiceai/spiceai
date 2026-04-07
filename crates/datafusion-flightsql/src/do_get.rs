/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::sync::Arc;

use arrow_flight::{
    Ticket,
    flight_service_server::FlightService,
    sql::{Any, Command},
};
use datafusion::prelude::SessionContext;
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{FlightSqlService, flightsql, to_tonic_err};

pub(crate) async fn handle(
    ctx: Arc<SessionContext>,
    request: Request<Ticket>,
) -> Result<Response<<FlightSqlService as FlightService>::DoGetStream>, Status> {
    let msg: Any = match Message::decode(&*request.get_ref().ticket) {
        Ok(msg) => msg,
        Err(_) => return do_get_simple(ctx, request).await,
    };

    match Command::try_from(msg).map_err(to_tonic_err)? {
        Command::CommandStatementQuery(cmd) => flightsql::statement_query::do_get(ctx, cmd).await,
        Command::CommandPreparedStatementQuery(cmd) => {
            Box::pin(flightsql::prepared_statement_query::do_get(ctx, cmd)).await
        }
        Command::CommandPreparedStatementUpdate(cmd) => {
            Box::pin(flightsql::prepared_statement_update::do_get(ctx, cmd)).await
        }
        Command::CommandGetCatalogs(cmd) => flightsql::get_catalogs::do_get(&ctx, cmd),
        Command::CommandGetDbSchemas(cmd) => flightsql::get_schemas::do_get(&ctx, cmd),
        Command::CommandGetTables(cmd) => flightsql::get_tables::do_get(ctx, cmd).await,
        Command::CommandGetPrimaryKeys(cmd) => Ok(flightsql::get_primary_keys::do_get(&cmd)),
        Command::CommandGetTableTypes(cmd) => flightsql::get_table_types::do_get(cmd),
        Command::CommandGetSqlInfo(cmd) => flightsql::get_sql_info::do_get(cmd),
        Command::CommandGetXdbcTypeInfo(cmd) => flightsql::get_xdbc_type_info::do_get(cmd),
        Command::CommandStatementUpdate(_) => Err(Status::invalid_argument(
            "CommandStatementUpdate should be sent via DoPut, not DoGet",
        )),
        cmd => Err(Status::unimplemented(format!(
            "DoGet not implemented for command: {cmd:?}"
        ))),
    }
}

async fn do_get_simple(
    ctx: Arc<SessionContext>,
    request: Request<Ticket>,
) -> Result<Response<<FlightSqlService as FlightService>::DoGetStream>, Status> {
    let ticket = request.into_inner();
    let sql = std::str::from_utf8(&ticket.ticket)
        .map_err(|e| Status::invalid_argument(format!("invalid ticket: {e}")))?;

    tracing::trace!("do_get (plain SQL): {sql}");

    let output = FlightSqlService::sql_to_flight_stream(ctx, sql, None).await?;
    Ok(Response::new(
        Box::pin(output) as <FlightSqlService as FlightService>::DoGetStream
    ))
}
