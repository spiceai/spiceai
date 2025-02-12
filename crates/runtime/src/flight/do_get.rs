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
    flight_service_server::FlightService,
    sql::{Any, Command},
    Ticket,
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    flight::{metrics, util::attach_cache_metadata},
    timing::TimedStream,
};

use super::{flightsql, to_tonic_err, Service};

pub(crate) async fn handle(
    flight_svc: &Service,
    request: Request<Ticket>,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let msg: Any = match Message::decode(&*request.get_ref().ticket) {
        Ok(msg) => msg,
        Err(_) => return Box::pin(do_get_simple(flight_svc, request)).await,
    };

    match Command::try_from(msg).map_err(to_tonic_err)? {
        Command::CommandStatementQuery(command) => {
            Box::pin(flightsql::statement_query::do_get(flight_svc, command)).await
        }
        Command::CommandPreparedStatementQuery(command) => {
            Box::pin(flightsql::prepared_statement_query::do_get(
                flight_svc, command,
            ))
            .await
        }
        Command::CommandGetCatalogs(command) => {
            flightsql::get_catalogs::do_get(flight_svc, command).await
        }
        Command::CommandGetDbSchemas(command) => {
            flightsql::get_schemas::do_get(flight_svc, command).await
        }
        Command::CommandGetTables(command) => {
            flightsql::get_tables::do_get(flight_svc, command).await
        }
        Command::CommandGetPrimaryKeys(command) => {
            flightsql::get_primary_keys::do_get(flight_svc, &command).await
        }
        Command::CommandGetTableTypes(command) => flightsql::get_table_types::do_get(command).await,
        Command::CommandGetSqlInfo(command) => flightsql::get_sql_info::do_get(command).await,
        _ => {
            let _start = metrics::track_flight_request("do_get", None).await;
            Err(Status::unimplemented("Not yet implemented"))
        }
    }
}

async fn do_get_simple(
    flight_svc: &Service,
    request: Request<Ticket>,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = metrics::track_flight_request("do_get", Some("sql_query")).await;
    let datafusion = Arc::clone(&flight_svc.datafusion);
    let ticket = request.into_inner();
    tracing::trace!("do_get_simple: {ticket:?}");
    match std::str::from_utf8(&ticket.ticket) {
        Ok(sql) => {
            let (output, cache_status) =
                Box::pin(Service::sql_to_flight_stream(datafusion, sql)).await?;

            let timed_output = TimedStream::new(output, move || start);

            let mut response =
                Response::new(Box::pin(timed_output) as <Service as FlightService>::DoGetStream);

            attach_cache_metadata(&mut response, cache_status);

            Ok(response)
        }
        Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e}"))),
    }
}
