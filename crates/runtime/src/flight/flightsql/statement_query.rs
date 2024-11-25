/*
Copyright 2024 The Spice.ai OSS Authors

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
    datafusion::query::Protocol,
    flight::{metrics::track_flight_request, to_tonic_err, util::attach_cache_metadata, Service},
    timing::TimedStream,
};

/// Get a `FlightInfo` for executing a SQL query.
pub(crate) async fn get_flight_info(
    flight_svc: &Service,
    query: sql::CommandStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    tracing::trace!("get_flight_info: {query:?}");
    let _start = track_flight_request("get_flight_info", Some("statement_query"));

    let sql = query.query.as_str();

    let arrow_schema =
        Service::get_arrow_schema(Arc::clone(&flight_svc.datafusion), sql, Protocol::FlightSQL)
            .await
            .map_err(to_tonic_err)?;

    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: query.as_any().encode_to_vec().into(),
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
    cmd: sql::CommandStatementQuery,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = track_flight_request("do_get", Some("statement_query"));
    let datafusion = Arc::clone(&flight_svc.datafusion);
    tracing::trace!("do_get_statement: {cmd:?}");
    let (output, from_cache) = Box::pin(Service::sql_to_flight_stream(
        datafusion,
        &cmd.query,
        Protocol::FlightSQL,
    ))
    .await?;
    let timed_output = TimedStream::new(output, move || start);

    let mut response =
        Response::new(Box::pin(timed_output) as <Service as FlightService>::DoGetStream);
    attach_cache_metadata(&mut response, from_cache);
    Ok(response)
}
