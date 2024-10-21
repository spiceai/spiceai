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

use crate::datafusion::query::{Protocol, QueryBuilder};
use crate::datafusion::DataFusion;
use crate::dataupdate::DataUpdate;
use crate::metrics as runtime_metrics;
use crate::timing::TimeMeasurement;
use crate::tls::TlsConfig;
use crate::user_agent_util::extract_user_agent;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::{Action, ActionType, Criteria, IpcMessage, PollInfo, SchemaResult};
use arrow_ipc::writer::IpcWriteOptions;
use bytes::Bytes;
use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::parser::ParserError;
use datafusion::sql::TableReference;
use futures::stream::{self, BoxStream, StreamExt};
use futures::{Stream, TryStreamExt};
use opentelemetry::KeyValue;
use secrecy::ExposeSecret;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::RwLock;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

mod actions;
mod do_exchange;
mod do_get;
mod do_put;
mod flightsql;
mod get_flight_info;
mod get_schema;
mod handshake;
mod metrics;
mod util;

use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaAsIpc, Ticket,
};

pub struct Service {
    datafusion: Arc<DataFusion>,
    channel_map: Arc<RwLock<HashMap<TableReference, Arc<Sender<DataUpdate>>>>>,
}

#[tonic::async_trait]
impl FlightService for Service {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let labels = Service::get_user_agent(&request);

        metrics::HANDSHAKE_REQUESTS.add(1, &labels);
        handshake::handle()
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let labels = Service::get_user_agent(&request);
        metrics::LIST_FLIGHTS_REQUESTS.add(1, &labels);
        tracing::trace!("list_flights - unimplemented");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let labels = Service::get_user_agent(&request);
        let _guard = TimeMeasurement::new(&metrics::GET_FLIGHT_INFO_REQUEST_DURATION_MS, &labels);
        metrics::GET_FLIGHT_INFO_REQUESTS.add(1, &labels);
        Box::pin(get_flight_info::handle(self, request)).await
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let labels = Service::get_user_agent(&request);
        metrics::GET_SCHEMA_REQUESTS.add(1, &labels);
        get_schema::handle(self, request).await
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let labels = Service::get_user_agent(&request);
        metrics::DO_GET_REQUESTS.add(1, &labels);
        Box::pin(do_get::handle(self, request)).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let labels = Service::get_user_agent(&request);
        metrics::DO_PUT_REQUESTS.add(1, &labels);
        do_put::handle(self, request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let labels = Service::get_user_agent(&request);
        metrics::DO_EXCHANGE_REQUESTS.add(1, &labels);
        do_exchange::handle(self, request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let labels = Service::get_user_agent(&request);
        metrics::DO_ACTION_REQUESTS.add(1, &labels);
        Box::pin(actions::do_action(self, request)).await
    }

    async fn list_actions(
        &self,
        request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let labels = Service::get_user_agent(&request);
        metrics::LIST_ACTIONS_REQUESTS.add(1, &labels);
        Ok(actions::list())
    }
}

impl Service {
    async fn get_arrow_schema(
        datafusion: Arc<DataFusion>,
        sql: &str,
        protocol: Protocol,
    ) -> Result<Schema, Status> {
        let query = QueryBuilder::new(sql, datafusion, protocol).build();

        let schema = query.get_schema().await.map_err(handle_datafusion_error)?;
        Ok(schema)
    }

    fn serialize_schema(schema: &Schema) -> Result<Bytes, Status> {
        let message: IpcMessage = SchemaAsIpc::new(schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(to_tonic_err)?;
        let IpcMessage(schema_bytes) = message;

        Ok(schema_bytes)
    }

    async fn sql_to_flight_stream(
        datafusion: Arc<DataFusion>,
        sql: &str,
        protocol: Protocol,
    ) -> Result<(BoxStream<'static, Result<FlightData, Status>>, Option<bool>), Status> {
        let query = QueryBuilder::new(sql, Arc::clone(&datafusion), protocol)
            .protocol(protocol)
            .build();

        let query_result = query.run().await.map_err(to_tonic_err)?;

        let schema = query_result.data.schema();
        let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
        let schema_as_ipc = SchemaAsIpc::new(&schema, &options);
        let schema_flight_data = FlightData::from(schema_as_ipc);

        let batches_stream = query_result
            .data
            .then(move |batch_result| {
                let options_clone = options.clone();
                async move {
                    let encoder = IpcDataGenerator::default();
                    let mut tracker = DictionaryTracker::new(false);

                    match batch_result {
                        Ok(batch) => {
                            let (flight_dictionaries, flight_batch) = encoder
                                .encoded_batch(&batch, &mut tracker, &options_clone)
                                .map_err(|e| Status::internal(e.to_string()))?;

                            let mut flights: Vec<FlightData> =
                                flight_dictionaries.into_iter().map(Into::into).collect();
                            flights.push(flight_batch.into());
                            Ok(flights)
                        }
                        Err(e) => match e {
                            DataFusionError::External(e) => {
                                if let Some(execution_error) =
                                    e.to_string().strip_prefix("Execution error: ")
                                {
                                    return Err(Status::invalid_argument(
                                        execution_error.to_string(),
                                    ));
                                }
                                Err(Status::invalid_argument(e.to_string()))
                            }
                            _ => Err(Status::invalid_argument(e.to_string())),
                        },
                    }
                }
            })
            .map(|result| {
                // Convert Result<Vec<FlightData>, Status> into Stream of Result<FlightData, Status>
                match result {
                    Ok(flights) => stream::iter(flights.into_iter().map(Ok)).left_stream(),
                    Err(e) => stream::once(async { Err(e) }).right_stream(),
                }
            })
            .flatten();

        let flights_stream = stream::once(async { Ok(schema_flight_data) }).chain(batches_stream);

        Ok((flights_stream.boxed(), query_result.from_cache))
    }

    fn get_request_labels<T>(req: &Request<T>) -> [KeyValue; 3] {
        let user_agent = req
            .metadata()
            .get("x-spice-user-agent")
            .map(|ua| ua.to_str().unwrap_or(""));
        let (user_agent, agent_version, agent_os) = extract_user_agent(user_agent);
        [
            KeyValue::new("user-agent", user_agent),
            KeyValue::new("user-agent-version", agent_version),
            KeyValue::new("user-agent-os", agent_os),
        ]
    }
}

fn record_batches_to_flight_stream(
    record_batches: Vec<RecordBatch>,
) -> impl Stream<Item = Result<FlightData, Status>> {
    FlightDataEncoderBuilder::new()
        .build(stream::iter(record_batches.into_iter().map(Ok)))
        .map_err(to_tonic_err)
}

#[allow(clippy::needless_pass_by_value)]
fn to_tonic_err<E>(e: E) -> Status
where
    E: std::fmt::Display + 'static,
{
    if let Some(status) = (&e as &dyn std::any::Any).downcast_ref::<Status>() {
        status.clone()
    } else {
        Status::internal(format!("{e}"))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn handle_datafusion_error(e: DataFusionError) -> Status {
    match e {
        DataFusionError::Plan(err_msg) | DataFusionError::Execution(err_msg) => {
            Status::invalid_argument(err_msg)
        }
        DataFusionError::SQL(sql_err, _) => match sql_err {
            ParserError::RecursionLimitExceeded => {
                Status::invalid_argument("Recursion limit exceeded")
            }
            ParserError::ParserError(err_msg) | ParserError::TokenizerError(err_msg) => {
                Status::invalid_argument(err_msg)
            }
        },
        DataFusionError::SchemaError(schema_err, _) => {
            Status::invalid_argument(format!("{schema_err}"))
        }
        _ => to_tonic_err(e),
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to register parquet file: {source}"))]
    RegisterParquet { source: crate::datafusion::Error },

    #[snafu(display("{source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to start Flight server: {source}"))]
    UnableToStartFlightServer { source: tonic::transport::Error },

    #[snafu(display("Unable to configure TLS on the Flight server: {source}"))]
    UnableToConfigureTls { source: tonic::transport::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn start(
    bind_address: std::net::SocketAddr,
    df: Arc<DataFusion>,
    tls_config: Option<Arc<TlsConfig>>,
) -> Result<()> {
    let service = Service {
        datafusion: Arc::clone(&df),
        channel_map: Arc::new(RwLock::new(HashMap::new())),
    };
    let svc = FlightServiceServer::new(service);

    tracing::info!("Spice Runtime Flight listening on {bind_address}");
    runtime_metrics::spiced_runtime::FLIGHT_SERVER_START.add(1, &[]);

    let mut server = Server::builder();

    if let Some(ref tls_config) = tls_config {
        let server_tls_config = ServerTlsConfig::new().identity(Identity::from_pem(
            tls_config.cert.expose_secret(),
            tls_config.key.expose_secret(),
        ));
        server = server
            .tls_config(server_tls_config)
            .context(UnableToConfigureTlsSnafu)?;
    }

    server
        .add_service(svc)
        .serve(bind_address)
        .await
        .context(UnableToStartFlightServerSnafu)?;

    Ok(())
}
