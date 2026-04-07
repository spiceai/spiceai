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
    FlightData,
    flight_service_server::FlightService,
    sql::{Any, Command},
};
use datafusion::prelude::SessionContext;
use prost::Message;
use tokio_stream::adapters::Peekable;
use tonic::{Request, Response, Status, Streaming};

use crate::{FlightSqlService, flightsql};

pub(crate) async fn handle(
    ctx: Arc<SessionContext>,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<<FlightSqlService as FlightService>::DoPutStream>, Status> {
    let mut streaming_flight: Peekable<Streaming<FlightData>> =
        tokio_stream::StreamExt::peekable(request.into_inner());

    let Some(Ok(first_message)) = streaming_flight.peek().await else {
        return Err(Status::invalid_argument("no flight data provided"));
    };

    let Some(fd) = &first_message.flight_descriptor else {
        return Err(Status::invalid_argument("no flight descriptor provided"));
    };

    let Ok(message) = Any::decode(&*fd.cmd) else {
        return do_put_raw(streaming_flight).await;
    };

    match Command::try_from(message).map_err(|e| Status::internal(format!("{e:?}")))? {
        Command::CommandPreparedStatementQuery(cmd) => {
            flightsql::prepared_statement_query::do_put_query(cmd, streaming_flight).await
        }
        Command::CommandPreparedStatementUpdate(cmd) => {
            flightsql::prepared_statement_update::do_put_update(cmd, streaming_flight).await
        }
        Command::CommandStatementUpdate(cmd) => {
            flightsql::statement_update::do_put(ctx, cmd).await
        }
        Command::CommandStatementIngest(_) => Err(Status::unimplemented(
            "CommandStatementIngest is not supported; use a SQL INSERT statement instead",
        )),
        _ => do_put_raw(streaming_flight).await,
    }
}

async fn do_put_raw(
    _streaming: Peekable<Streaming<FlightData>>,
) -> Result<Response<<FlightSqlService as FlightService>::DoPutStream>, Status> {
    Err(Status::unimplemented(
        "Raw path-based DoPut is not supported by this service",
    ))
}
