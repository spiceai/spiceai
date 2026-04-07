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
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    sql::{Any, Command},
};
use datafusion::prelude::SessionContext;
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{FlightSqlService, flightsql, to_tonic_err};

pub(crate) async fn handle(
    ctx: Arc<SessionContext>,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let Ok(message) = Any::decode(&*request.get_ref().cmd) else {
        return get_flight_info_simple(ctx, request).await;
    };

    match Command::try_from(message).map_err(to_tonic_err)? {
        Command::CommandStatementQuery(cmd) => {
            flightsql::statement_query::get_flight_info(ctx, cmd, request).await
        }
        Command::CommandPreparedStatementQuery(handle) => {
            flightsql::prepared_statement_query::get_flight_info(ctx, handle, request).await
        }
        Command::CommandPreparedStatementUpdate(handle) => {
            flightsql::prepared_statement_update::get_flight_info(&handle, request)
        }
        Command::CommandGetCatalogs(cmd) => {
            Ok(flightsql::get_catalogs::get_flight_info(cmd, request))
        }
        Command::CommandGetDbSchemas(cmd) => {
            Ok(flightsql::get_schemas::get_flight_info(&cmd, request))
        }
        Command::CommandGetTables(cmd) => Ok(flightsql::get_tables::get_flight_info(&cmd, request)),
        Command::CommandGetSqlInfo(cmd) => flightsql::get_sql_info::get_flight_info(&cmd, request),
        Command::CommandGetTableTypes(cmd) => {
            Ok(flightsql::get_table_types::get_flight_info(cmd, request))
        }
        Command::CommandGetPrimaryKeys(cmd) => {
            Ok(flightsql::get_primary_keys::get_flight_info(&cmd, request))
        }
        Command::CommandGetXdbcTypeInfo(cmd) => Ok(Response::new(
            flightsql::get_xdbc_type_info::get_flight_info(cmd, request),
        )),
        Command::CommandStatementIngest(_) | Command::CommandStatementUpdate(_) => {
            // These are write operations handled via DoPut.
            let fd = request.into_inner();
            Ok(Response::new(FlightInfo::new().with_descriptor(fd)))
        }
        cmd => Err(Status::unimplemented(format!(
            "get_flight_info not supported for command: {cmd:?}"
        ))),
    }
}

async fn get_flight_info_simple(
    ctx: Arc<SessionContext>,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let fd = request.into_inner();
    let sql = std::str::from_utf8(&fd.cmd).map_err(|e| Status::invalid_argument(e.to_string()))?;

    let arrow_schema = FlightSqlService::get_arrow_schema(&ctx, sql).await?;

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
