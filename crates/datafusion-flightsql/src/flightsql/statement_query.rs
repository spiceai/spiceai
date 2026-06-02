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

use crate::{FlightSqlService, to_tonic_err};

pub(crate) async fn get_flight_info(
    ctx: Arc<SessionContext>,
    query: sql::CommandStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    tracing::trace!("get_flight_info statement_query: {:?}", query.query);

    let arrow_schema = FlightSqlService::get_arrow_schema(&ctx, &query.query).await?;
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
    ctx: Arc<SessionContext>,
    cmd: sql::CommandStatementQuery,
) -> Result<Response<<FlightSqlService as FlightService>::DoGetStream>, Status> {
    tracing::trace!("do_get statement_query: {:?}", cmd.query);

    let output = FlightSqlService::sql_to_flight_stream(ctx, &cmd.query, None).await?;
    Ok(Response::new(
        Box::pin(output) as <FlightSqlService as FlightService>::DoGetStream
    ))
}
