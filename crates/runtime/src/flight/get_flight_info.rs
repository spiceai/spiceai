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

use std::sync::Arc;

use arrow_flight::{
    sql::{Any, Command},
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::flight::metrics;

use super::{flightsql, to_tonic_err, Service};

pub(crate) async fn handle(
    flight_svc: &Service,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let Ok(message) = Any::decode(&*request.get_ref().cmd) else {
        return get_flight_info_simple(flight_svc, request).await;
    };

    match Command::try_from(message).map_err(to_tonic_err)? {
        Command::CommandStatementQuery(token) => {
            flightsql::statement_query::get_flight_info(flight_svc, token, request).await
        }
        Command::CommandPreparedStatementQuery(handle) => {
            flightsql::prepared_statement_query::get_flight_info(flight_svc, handle, request).await
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
        _ => {
            let _start = metrics::track_flight_request("get_flight_info", None).await;
            Err(Status::unimplemented("Not yet implemented"))
        }
    }
}

async fn get_flight_info_simple(
    flight_svc: &Service,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    tracing::trace!("get_flight_info_simple: {request:?}");
    let _start = metrics::track_flight_request("get_flight_info", Some("sql_query")).await;

    let fd = request.into_inner();

    let sql: &str = std::str::from_utf8(&fd.cmd).map_err(to_tonic_err)?;
    let arrow_schema = Service::get_arrow_schema(Arc::clone(&flight_svc.datafusion), sql)
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
