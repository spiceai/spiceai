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
use prost::Message;
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::{
    flight::{Service, flightsql::prepared_statement_query, metrics, to_tonic_err},
    timing::TimedStream,
};

use crate::datafusion::app_context_extension::AppContextExtension;
use crate::datafusion::request_context_extension::DataFusionContextExtension;
use crate::datafusion::secrets_context_extension::SecretsContextExtension;
use arrow_flight::{
    Action, ActionType as FlightActionType,
    flight_service_server::FlightService,
    sql::{self, Any, ProstMessageExt},
};
use runtime_proto::{ExecutorExpandSecretRequest, ExecutorExpandSecretResponse};
use runtime_request_context::{AsyncMarker, RequestContext};
use secrecy::ExposeSecret;

enum ActionType {
    CreatePreparedStatement,
    ClosePreparedStatement,
    GetAppDefinition,
    ExpandSecret,
    Unknown,
}

impl ActionType {
    fn from_str(s: &str) -> Self {
        match s {
            "CreatePreparedStatement" => ActionType::CreatePreparedStatement,
            "ClosePreparedStatement" => ActionType::ClosePreparedStatement,
            "GetAppDefinition" => ActionType::GetAppDefinition,
            "ExpandSecret" => ActionType::ExpandSecret,
            _ => ActionType::Unknown,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            ActionType::CreatePreparedStatement => "CreatePreparedStatement",
            ActionType::ClosePreparedStatement => "ClosePreparedStatement",
            ActionType::GetAppDefinition => "GetAppDefinition",
            ActionType::ExpandSecret => "ExpandSecret",
            ActionType::Unknown => "Unknown",
        }
    }
}

impl Display for ActionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub(crate) async fn list() -> Response<<Service as FlightService>::ListActionsStream> {
    let start = metrics::track_flight_request("list_actions", None).await;
    tracing::trace!("list_actions");
    let create_prepared_statement_action_type = FlightActionType {
        r#type: ActionType::CreatePreparedStatement.to_string(),
        description: "Creates a reusable prepared statement resource on the server.\n
            Request Message: ActionCreatePreparedStatementRequest\n
            Response Message: ActionCreatePreparedStatementResult"
            .into(),
    };
    let close_prepared_statement_action_type = FlightActionType {
        r#type: ActionType::ClosePreparedStatement.to_string(),
        description: "Closes a reusable prepared statement resource on the server.\n
            Request Message: ActionClosePreparedStatementRequest\n
            Response Message: N/A"
            .into(),
    };
    let get_app_definition_action_type = FlightActionType {
        r#type: ActionType::GetAppDefinition.to_string(),
        description:
            "Used in cluster mode to ask Spice for its App declaration for runtime dependencies.\n
            Request Message: N/A
            Response Message: app::App serialized as JSON bytes"
                .into(),
    };
    let expand_secret_action_type = FlightActionType {
        r#type: ActionType::ExpandSecret.to_string(),
        description: "Used in cluster mode to ask the scheduler for the value of a secret\n
            Request Message: ExecutorExpandSecretRequest
            Response Message: ExecutorExpandSecretResponse"
            .into(),
    };
    let actions: Vec<Result<FlightActionType, Status>> = vec![
        Ok(create_prepared_statement_action_type),
        Ok(close_prepared_statement_action_type),
        Ok(get_app_definition_action_type),
        Ok(expand_secret_action_type),
    ];

    let output = TimedStream::new(futures::stream::iter(actions), || start);

    Response::new(Box::pin(output) as <Service as FlightService>::ListActionsStream)
}

#[expect(clippy::too_many_lines)]
pub(crate) async fn do_action(
    request: Request<Action>,
) -> Result<Response<<Service as FlightService>::DoActionStream>, Status> {
    let action_type = ActionType::from_str(request.get_ref().r#type.as_str());

    let action_type_str = action_type.as_str().to_string();
    let start = metrics::track_flight_request("do_action", Some(&action_type_str)).await;

    let stream = match action_type {
        ActionType::CreatePreparedStatement => {
            tracing::trace!("do_action: CreatePreparedStatement");
            let any = Any::decode(&*request.get_ref().body).map_err(to_tonic_err)?;

            let cmd: sql::ActionCreatePreparedStatementRequest =
                any.unpack().map_err(to_tonic_err)?.ok_or_else(|| {
                    Status::invalid_argument(
                        "Unable to unpack ActionCreatePreparedStatementRequest.",
                    )
                })?;
            let stmt = prepared_statement_query::do_action_create_prepared_statement(cmd).await?;
            futures::stream::iter(vec![Ok(arrow_flight::Result {
                body: stmt.as_any().encode_to_vec().into(),
            })])
        }
        ActionType::ClosePreparedStatement => {
            tracing::trace!("do_action: ClosePreparedStatement");
            futures::stream::iter(vec![Ok(arrow_flight::Result::default())])
        }
        ActionType::GetAppDefinition => {
            tracing::trace!("do_action: GetAppDefinition");
            let context = RequestContext::current(AsyncMarker::new().await);
            let Some(app) = context
                .extension::<AppContextExtension>()
                .and_then(|a| a.app())
            else {
                return Err(Status::internal("App context not available"));
            };

            let bs = serde_json::to_vec(&app).map_err(to_tonic_err)?;
            let result = arrow_flight::Result::new(bs);
            futures::stream::iter(vec![Ok(result)])
        }
        ActionType::ExpandSecret => {
            tracing::trace!("do_action: ExpandSecret");

            let request = ExecutorExpandSecretRequest::decode(&*request.get_ref().body)
                .map_err(to_tonic_err)?;

            let span = tracing::span!(
                target: "task_history",
                tracing::Level::INFO,
                "cluster::expand_secret",
                executor_id = %request.executor_id,
                key = %request.key
            );
            let _guard = span.enter();

            let context = RequestContext::current(AsyncMarker::new().await);
            let Some(df) = context
                .extension::<DataFusionContextExtension>()
                .map(|df| df.datafusion())
            else {
                return Err(Status::internal("DataFusion context not available"));
            };

            let scheduler = {
                let Some(maybe_scheduler) = df.scheduler_server.read().ok() else {
                    return Err(Status::internal("Cluster scheduler context cannot be read"));
                };

                let Some(ref scheduler) = *maybe_scheduler else {
                    return Err(Status::internal("Cluster scheduler context not available"));
                };

                Arc::clone(scheduler)
            };

            let executor_state = scheduler
                .state
                .executor_manager
                .get_executor_state()
                .await
                .map_err(to_tonic_err)?;
            let executors = executor_state
                .into_iter()
                .map(|(e, _)| e.id)
                .collect::<HashSet<_>>();

            if !executors.contains(&request.executor_id) {
                return Err(Status::invalid_argument(format!(
                    "Executor {} is not a part of the cluster",
                    request.executor_id
                )));
            }

            tracing::debug!(
                "ExpandSecret: expanding secret {} for executor {}",
                request.key,
                request.executor_id
            );

            let Some(sctx) = context.extension::<SecretsContextExtension>() else {
                return Err(Status::internal("Secrets context not available"));
            };

            let secrets = sctx.secrets();
            let secrets = secrets.read().await;
            let Some(value) = secrets
                .get_secret(&request.key)
                .await
                .map_err(to_tonic_err)?
            else {
                tracing::error!(target: "task_history", "Secret not found");
                return Err(Status::invalid_argument(format!(
                    "Unable to read secret {}",
                    request.key
                )));
            };

            let exposed = value.expose_secret();
            let response = ExecutorExpandSecretResponse {
                key: request.key,
                value: exposed.to_string(),
            };

            tracing::debug!(target: "task_history", "Secret expanded successfully");

            let result = arrow_flight::Result::new(response.encode_to_vec());
            futures::stream::iter(vec![Ok(result)])
        }
        ActionType::Unknown => return Err(Status::invalid_argument("Unknown action type")),
    };

    Ok(Response::new(Box::pin(TimedStream::new(
        stream,
        move || start,
    ))))
}
