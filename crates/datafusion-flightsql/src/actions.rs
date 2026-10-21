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

//! `DoAction` / `ListActions` for Flight SQL prepared statements.
//!
//! Supported actions:
//! - `CreatePreparedStatement`
//! - `ClosePreparedStatement`

use std::sync::Arc;

use arrow_flight::{
    Action, ActionType,
    flight_service_server::FlightService,
    sql::{self, Any, ProstMessageExt},
};
use datafusion::prelude::SessionContext;
use futures::stream::BoxStream;
use prost::Message;
use tonic::{Request, Response, Status};

use crate::{FlightSqlService, flightsql::prepared_statement_query, to_tonic_err};

pub(crate) async fn do_action(
    ctx: Arc<SessionContext>,
    request: Request<Action>,
) -> Result<Response<<FlightSqlService as FlightService>::DoActionStream>, Status> {
    let action = request.into_inner();
    tracing::trace!("do_action: type={}", action.r#type);

    let msg: Any = Any::decode(&*action.body).map_err(to_tonic_err)?;

    match sql::Command::try_from(msg).map_err(to_tonic_err)? {
        sql::Command::ActionCreatePreparedStatementRequest(stmt) => {
            let result =
                prepared_statement_query::do_action_create_prepared_statement(ctx, stmt).await?;
            let output = futures::stream::iter(vec![Ok(arrow_flight::Result {
                body: result.as_any().encode_to_vec().into(),
            })]);
            Ok(Response::new(
                Box::pin(output) as <FlightSqlService as FlightService>::DoActionStream
            ))
        }
        sql::Command::ActionClosePreparedStatementRequest(handle) => {
            tracing::trace!("close_prepared_statement: {:?}", handle);
            // Prepared statement handles are self-contained serialised blobs;
            // there is nothing server-side to clean up.
            let output = futures::stream::empty();
            Ok(Response::new(
                Box::pin(output) as <FlightSqlService as FlightService>::DoActionStream
            ))
        }
        cmd => Err(Status::invalid_argument(format!(
            "unsupported action command: {cmd:?}"
        ))),
    }
}

pub(crate) fn list() -> Response<BoxStream<'static, Result<ActionType, Status>>> {
    let actions = vec![
        ActionType {
            r#type: "CreatePreparedStatement".to_string(),
            description: "Creates a reusable prepared statement resource on the server. \
                          Returns a handle identifying the prepared statement."
                .to_string(),
        },
        ActionType {
            r#type: "ClosePreparedStatement".to_string(),
            description: "Closes a prepared statement resource on the server.".to_string(),
        },
    ];
    Response::new(Box::pin(futures::stream::iter(actions.into_iter().map(Ok))))
}
