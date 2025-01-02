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
    flight_service_server::FlightService,
    sql::{self, ProstMessageExt},
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{
    flight::{
        metrics, record_batches_to_flight_stream, to_tonic_err, util::set_flightsql_protocol,
        Service,
    },
    timing::TimedStream,
};

/// Get a `FlightInfo` for listing catalogs.
pub(crate) async fn get_flight_info(
    query: sql::CommandGetCatalogs,
    request: Request<FlightDescriptor>,
) -> Response<FlightInfo> {
    let _start = metrics::track_flight_request("get_flight_info", Some("get_catalogs")).await;
    set_flightsql_protocol().await;

    tracing::trace!("get_flight_info_catalogs");
    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: query.as_any().encode_to_vec().into(),
    });

    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .with_descriptor(fd);

    Response::new(info)
}

pub(crate) async fn do_get(
    flight_svc: &Service,
    query: sql::CommandGetCatalogs,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = metrics::track_flight_request("do_get", Some("get_catalogs")).await;
    set_flightsql_protocol().await;

    tracing::trace!("do_get_catalogs: {query:?}");
    let mut builder = query.into_builder();

    let catalog_names = flight_svc.datafusion.ctx.catalog_names();

    for catalog in catalog_names {
        builder.append(catalog);
    }

    let record_batch = builder.build().map_err(to_tonic_err)?;

    Ok(Response::new(Box::pin(TimedStream::new(
        record_batches_to_flight_stream(vec![record_batch]),
        move || start,
    ))
        as <Service as FlightService>::DoGetStream))
}
