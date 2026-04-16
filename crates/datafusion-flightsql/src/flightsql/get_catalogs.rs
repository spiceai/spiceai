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
    flight_service_server::FlightService,
    sql::{self, ProstMessageExt},
};
use datafusion::prelude::SessionContext;
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{FlightSqlService, record_batches_to_flight_stream, to_tonic_err};

pub(crate) fn get_flight_info(
    query: sql::CommandGetCatalogs,
    request: Request<FlightDescriptor>,
) -> Response<FlightInfo> {
    let fd = request.into_inner();
    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: query.as_any().encode_to_vec().into(),
    });
    Response::new(
        FlightInfo::new()
            .with_endpoint(endpoint)
            .with_descriptor(fd),
    )
}

pub(crate) fn do_get(
    ctx: &Arc<SessionContext>,
    query: sql::CommandGetCatalogs,
) -> Result<Response<<FlightSqlService as FlightService>::DoGetStream>, Status> {
    tracing::trace!("do_get get_catalogs: {query:?}");

    let mut builder = query.into_builder();
    for catalog in ctx.catalog_names() {
        builder.append(catalog);
    }
    let record_batch = builder.build().map_err(to_tonic_err)?;

    Ok(Response::new(Box::pin(record_batches_to_flight_stream(
        vec![record_batch],
    ))))
}
