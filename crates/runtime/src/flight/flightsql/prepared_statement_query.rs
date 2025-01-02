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
    sql::{self, ProstMessageExt},
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    flight::{
        metrics, to_tonic_err,
        util::{attach_cache_metadata, set_flightsql_protocol},
        Service,
    },
    timing::TimedStream,
};

/// Create a prepared statement from given SQL statement.
pub(crate) async fn do_action_create_prepared_statement(
    flight_svc: &Service,
    statement: sql::ActionCreatePreparedStatementRequest,
) -> Result<sql::ActionCreatePreparedStatementResult, Status> {
    tracing::trace!("do_action_create_prepared_statement: {statement:?}");
    set_flightsql_protocol().await;
    let arrow_schema =
        Service::get_arrow_schema(Arc::clone(&flight_svc.datafusion), &statement.query)
            .await
            .map_err(to_tonic_err)?;

    let schema_bytes = Service::serialize_schema(&arrow_schema)?;

    Ok(sql::ActionCreatePreparedStatementResult {
        prepared_statement_handle: statement.query.into(),
        dataset_schema: schema_bytes,
        ..Default::default()
    })
}

pub(crate) async fn get_flight_info(
    flight_svc: &Service,
    handle: sql::CommandPreparedStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let _start =
        metrics::track_flight_request("get_flight_info", Some("prepared_statement_query")).await;
    set_flightsql_protocol().await;

    tracing::trace!("get_flight_info: {handle:?}");
    let sql = match std::str::from_utf8(&handle.prepared_statement_handle) {
        Ok(sql) => sql,
        Err(e) => {
            return Err(Status::invalid_argument(format!(
                "Invalid prepared statement handle: {e}"
            )))
        }
    };

    let arrow_schema = Service::get_arrow_schema(Arc::clone(&flight_svc.datafusion), sql)
        .await
        .map_err(to_tonic_err)?;

    tracing::trace!("get_flight_info_prepared_statement: arrow_schema={arrow_schema:?}");

    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: handle.as_any().encode_to_vec().into(),
    });

    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .try_with_schema(&arrow_schema)
        .map_err(to_tonic_err)?
        .with_descriptor(fd);

    Ok(Response::new(info))
}

pub(crate) async fn do_get(
    flight_svc: &Service,
    query: sql::CommandPreparedStatementQuery,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = metrics::track_flight_request("do_get", Some("prepared_statement_query")).await;
    set_flightsql_protocol().await;

    let datafusion = Arc::clone(&flight_svc.datafusion);
    tracing::trace!("do_get: {query:?}");
    match std::str::from_utf8(&query.prepared_statement_handle) {
        Ok(sql) => {
            let (output, from_cache) =
                Box::pin(Service::sql_to_flight_stream(datafusion, sql)).await?;
            let timed_output = TimedStream::new(output, move || start);

            let mut response =
                Response::new(Box::pin(timed_output) as <Service as FlightService>::DoGetStream);
            attach_cache_metadata(&mut response, from_cache);
            Ok(response)
        }
        Err(e) => Err(Status::invalid_argument(format!(
            "Invalid prepared statement handle: {e}"
        ))),
    }
}
