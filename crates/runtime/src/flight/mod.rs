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

#[cfg(feature = "cluster")]
use {
    crate::config::ClusterMode,
    ballista_core::serde::protobuf::scheduler_grpc_server::SchedulerGrpcServer,
    ballista_executor::flight_service::BallistaFlightService, std::net::ToSocketAddrs,
};

use crate::auth::EndpointAuth;
use crate::datafusion::DataFusion;
use crate::datafusion::error::{SpiceExternalError, find_datafusion_root};
use crate::datafusion::query::{self, QueryBuilder};
use crate::dataupdate::DataUpdate;
use crate::opentelemetry::create_metrics_service;
use crate::tls::{TlsConfig, server_with_tls_config};
use crate::{Runtime, metrics as runtime_metrics};
use app::App;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{Action, ActionType, Criteria, IpcMessage, PollInfo, PutResult, SchemaResult};
use arrow_flight::{
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, SchemaAsIpc,
    Ticket, flight_service_server::FlightServiceServer,
};
use arrow_ipc::writer::IpcWriteOptions;
use async_stream::try_stream;
use bytes::Bytes;
use cache::result::CacheStatus;
use datafusion::common::ParamValues;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use datafusion::sql::sqlparser::parser::ParserError;
use flight_client::Error as FlightClientError;
use futures::stream::{self, BoxStream, StreamExt};
use futures::{Stream, TryStreamExt};
use governor::{Quota, RateLimiter};
use metrics::track_flight_request;
use middleware::{RequestContextLayer, WriteRateLimitLayer};
use runtime_auth::{FlightBasicAuth, layer::flight::BasicAuthLayer};
use runtime_request_context::{AsyncMarker, RequestContext};
use snafu::prelude::*;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::broadcast::Sender;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
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
mod middleware;
mod util;

pub struct Service {
    channel_map: Arc<RwLock<HashMap<TableReference, Arc<Sender<DataUpdate>>>>>,
    basic_auth: Option<Arc<dyn FlightBasicAuth + Send + Sync>>,
}

impl Service {
    /// Creates a new Service with pre-allocated channel map capacity
    #[must_use]
    pub fn new(basic_auth: Option<Arc<dyn FlightBasicAuth + Send + Sync>>) -> Self {
        Self {
            // Pre-allocate for typical workloads (avoid reallocation)
            channel_map: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            basic_auth,
        }
    }
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
        let _start = track_flight_request("do_handshake", None).await;
        let response = handshake::handle(request.metadata(), self.basic_auth.as_ref()).await?;
        Ok(Self::wrap_response_stream_with_scope(response).await)
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let _start = track_flight_request("list_flights", None).await;
        tracing::trace!("list_flights - unimplemented");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Box::pin(get_flight_info::handle(request)).await
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        let _start = track_flight_request("poll_flight_info", None).await;
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let _start = track_flight_request("get_schema", None).await;
        get_schema::handle(request).await
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let _start = track_flight_request("do_get", None).await;
        let response = Box::pin(do_get::handle(request)).await?;
        Ok(Self::wrap_response_stream_with_scope(response).await)
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let _start = track_flight_request("do_put", None).await;
        let response = do_put::handle(request).await?;
        Ok(Self::wrap_response_stream_with_scope(response).await)
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let _start = track_flight_request("do_exchange", None).await;
        let response = do_exchange::handle(self, request).await?;
        Ok(Self::wrap_response_stream_with_scope(response).await)
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let _start = track_flight_request("do_action", None).await;
        let response = Box::pin(actions::do_action(request)).await?;
        Ok(Self::wrap_response_stream_with_scope(response).await)
    }

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let _start = track_flight_request("list_actions", None).await;
        let response = actions::list().await;
        Ok(Self::wrap_response_stream_with_scope(response).await)
    }
}

impl Service {
    async fn get_arrow_schema(
        datafusion: Arc<DataFusion>,
        sql: &str,
    ) -> Result<(Schema, Option<Schema>), Status> {
        let query = QueryBuilder::new(sql, datafusion).build();

        query.get_schema().await.map_err(handle_datafusion_error)
    }

    #[expect(clippy::result_large_err)]
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
        parameters: Option<ParamValues>,
    ) -> Result<(BoxStream<'static, Result<FlightData, Status>>, CacheStatus), Status> {
        let query_result = QueryBuilder::new(sql, Arc::clone(&datafusion))
            .parameters(parameters)
            .build()
            .run()
            .await
            .map_err(handle_query_error)?;

        // Reuse the same options for all messages
        let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
        let schema = query_result.data.schema();

        // Pre-compute schema flight data once
        let mut dict_tracker = DictionaryTracker::new(true); // Set to true to handle dictionaries
        let encoder = IpcDataGenerator::default();
        let data = IpcMessage(
            encoder
                .schema_to_bytes_with_dictionary_tracker(
                    schema.as_ref(),
                    &mut dict_tracker,
                    &options,
                )
                .ipc_message
                .into(),
        );
        let schema_flight_data = FlightData {
            data_header: data.0,
            ..Default::default()
        };

        let data_stream = query_result.data;
        let cache_status = query_result.cache_status;

        let flights_stream = try_stream! {
            yield schema_flight_data;

            // Use fused stream for better performance
            let mut data_stream = data_stream.fuse();

            while let Some(batch_result) = data_stream.next().await {
                match batch_result {
                    Ok(batch) => {
                        let (dicts, batch_data) = encoder
                            .encoded_batch(&batch, &mut dict_tracker, &options)
                            .map_err(|e| Status::internal(e.to_string()))?;

                        // Yield dictionaries first
                        for dict in dicts {
                            yield dict.into();
                        }
                        yield batch_data.into();
                    }
                    Err(e) => {
                        let e = find_datafusion_root(e);
                        Err(handle_datafusion_error(e))?;
                    }
                }
            }
        };

        Ok((flights_stream.boxed(), cache_status))
    }

    async fn wrap_response_stream_with_scope<S>(
        response: Response<S>,
    ) -> Response<BoxStream<'static, S::Item>>
    where
        S: Stream + Send + 'static,
        S::Item: Send + 'static,
    {
        // Get request context once, avoid repeated lookups
        let request_context = RequestContext::current(AsyncMarker::new().await);
        let (metadata, stream, extensions) = response.into_parts();
        let scoped_stream = request_context.scope_stream(stream);
        Response::from_parts(metadata, scoped_stream.boxed(), extensions)
    }
}

fn record_batches_to_flight_stream(
    record_batches: Vec<RecordBatch>,
) -> impl Stream<Item = Result<FlightData, Status>> {
    FlightDataEncoderBuilder::new()
        .build(stream::iter(record_batches.into_iter().map(Ok)))
        .map_err(to_tonic_err)
}

fn to_tonic_err<E>(e: E) -> Status
where
    E: std::fmt::Display + 'static,
{
    // Avoid cloning Status if already a Status
    if let Some(status) = (&e as &dyn std::any::Any).downcast_ref::<Status>() {
        // Create a new Status with the same code and message to avoid cloning the entire Status struct
        return Status::new(status.code(), status.message());
    }
    Status::internal(format!("{e}"))
}

fn handle_query_error(e: query::Error) -> Status {
    match e {
        query::Error::BindingParameters { source }
        | query::Error::UnableToExecuteQuery { source } => handle_datafusion_error(source),
        _ => to_tonic_err(e),
    }
}

fn handle_datafusion_error(e: DataFusionError) -> Status {
    match e {
        DataFusionError::Plan(err_msg) | DataFusionError::Execution(err_msg) => {
            Status::invalid_argument(err_msg)
        }
        DataFusionError::SQL(sql_err, _) => match *sql_err {
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
        DataFusionError::External(e) => {
            if let Some(e) = e.downcast_ref::<SpiceExternalError>() {
                match e {
                    SpiceExternalError::AccelerationNotReady { dataset_name } => {
                        // Pre-format message to avoid repeated allocation
                        Status::unavailable(format!(
                            "Acceleration not ready; loading initial data for {dataset_name}"
                        ))
                    }
                }
            } else if let Some(err) = e.downcast_ref::<FlightClientError>() {
                match err {
                    FlightClientError::ConnectionReset { source } => {
                        let mut error = Status::invalid_argument(source.to_string());
                        error.metadata_mut().insert("spiceai-retryable", 1.into());
                        error
                    }
                    _ => to_tonic_err(e),
                }
            } else if let Some(err) = e.downcast_ref::<llms::embeddings::Error>() {
                match err {
                    llms::embeddings::Error::RateLimited { .. } => {
                        Status::unavailable(err.to_string())
                    }
                    _ => to_tonic_err(e),
                }
            } else {
                to_tonic_err(e)
            }
        }
        DataFusionError::ResourcesExhausted(source) => Status::resource_exhausted(source),
        DataFusionError::Diagnostic(_, source) | DataFusionError::Context(_, source) => {
            handle_datafusion_error(*source)
        }
        DataFusionError::Shared(source) => {
            // Optimize: avoid string allocation for common case
            Status::internal(format!("Shared DataFusion error: {source}"))
        }
        DataFusionError::Collection(sources) => {
            // Handle first error efficiently without collecting all
            if let Some(first_error) = sources.into_iter().next() {
                handle_datafusion_error(first_error)
            } else {
                Status::internal("Several DataFusion errors occurred, but no details available")
            }
        }
        DataFusionError::NotImplemented(message) => {
            Status::invalid_argument(format!("Unsupported Query. {message}"))
        }
        DataFusionError::Internal(_)
        | DataFusionError::ArrowError(..)
        | DataFusionError::IoError(_)
        | DataFusionError::ObjectStore(_)
        | DataFusionError::ParquetError(_)
        | DataFusionError::Substrait(_)
        | DataFusionError::Configuration(_)
        | DataFusionError::ExecutionJoin(_) => to_tonic_err(e),
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

    #[snafu(display(
        "Address {addr} is already in use by another process. Either stop the existing process or change the address: https://spiceai.org/docs/cli/reference/run"
    ))]
    AddressAlreadyInUse { addr: String },

    #[cfg(feature = "cluster")]
    #[snafu(display(
        "The cluster scheduler is not initialized, preventing the flight service from starting."
    ))]
    ClusterSchedulerNotInitialized {},

    #[cfg(feature = "cluster")]
    #[snafu(display("The flight service has an insecure configuration: {message}"))]
    InsecureConfiguration { message: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

fn is_address_in_use_error(err: &tonic::transport::Error) -> bool {
    let mut source: Option<&dyn std::error::Error> = Some(err);
    while let Some(e) = source {
        if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
            return io_err.kind() == std::io::ErrorKind::AddrInUse;
        }
        source = e.source();
    }
    false
}

/// Starts flight service
/// # Panics
/// If running in clustered mode, will panic unless TLS is configured or user manually overrides
/// this safety check, as RPC will transmit sensitive information to executors.
pub async fn start(
    bind_address: std::net::SocketAddr,
    app: Option<Arc<App>>,
    rt: Arc<Runtime>,
    tls_config: Option<Arc<TlsConfig>>,
    endpoint_auth: EndpointAuth,
    rate_limits: Arc<RateLimits>,
    shutdown_signal: Option<CancellationToken>,
) -> Result<()> {
    let service = Service::new(endpoint_auth.flight_basic_auth.as_ref().map(Arc::clone));
    let spice_flight_service = FlightServiceServer::new(service)
        .max_decoding_message_size(flight_client::MAX_DECODING_MESSAGE_SIZE);

    let mut server = Server::builder();

    if let Some(ref tls_config) = tls_config {
        server = server_with_tls_config(server, tls_config).context(UnableToConfigureTlsSnafu)?;
    }

    #[cfg(feature = "cluster")]
    if tls_config.is_none()
        && rt.config.cluster.mode.is_some()
        && !rt.config.cluster.allow_insecure_connections
    {
        return Err(Error::InsecureConfiguration {
            message: "Refusing to start in clustered mode without a valid TLS configuration. \
            To acknowledge and override, pass --allow-insecure-connections as an argument to spiced.\
            Both schedulers and executors must share the same TLS configuration.".to_string(),
        });
    }

    #[cfg(feature = "cluster")]
    if rt.config.cluster.mode.is_some() && rt.df.cluster_config.cluster_api_key().is_none() {
        return Err(Error::InsecureConfiguration {
            message:
                "Refusing to start in clustered mode without configuring API key authentication.\
             Read the docs to learn how to declare one: https://spiceai.org/docs/api/auth"
                    .to_string(),
        });
    }

    let auth_layer = tower::ServiceBuilder::new()
        .layer(BasicAuthLayer::new(endpoint_auth.flight_basic_auth))
        .into_inner();

    // Create the OpenTelemetry MetricsService
    let otel_service = create_metrics_service(rt.datafusion());

    let mut server = server
        .layer(RequestContextLayer::new(app, rt.datafusion(), rt.secrets()))
        .layer(WriteRateLimitLayer::new(RateLimiter::direct(
            rate_limits.flight_write_limit,
        )))
        .layer(auth_layer);

    #[cfg(not(feature = "cluster"))]
    let server = server
        .add_service(spice_flight_service)
        .add_service(otel_service);

    #[cfg(feature = "cluster")]
    let server = match rt.config.cluster.mode {
        Some(ClusterMode::Scheduler) => {
            let Some(scheduler) = rt
                .df
                .scheduler_server
                .read()
                .ok()
                .and_then(|r| r.iter().next().cloned())
            else {
                return Err(Error::ClusterSchedulerNotInitialized {});
            };

            let scheduler_grpc_server = SchedulerGrpcServer::from_arc(scheduler);
            server
                .add_service(spice_flight_service)
                .add_service(scheduler_grpc_server)
                .add_service(otel_service)
        }
        Some(ClusterMode::Executor) => {
            let executor_flight = FlightServiceServer::new(BallistaFlightService::new())
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX);

            server
                .add_service(executor_flight)
                .add_service(otel_service)
        }
        _ => server
            .add_service(spice_flight_service)
            .add_service(otel_service),
    };

    // If running an executor, we may have resolved another port to bind if 50051 is taken
    // Cast truncation for port is OK: was originally widened to u32 because it's a u32 in
    // Ballista `ExecutorRegistration`
    #[expect(clippy::cast_possible_truncation)]
    #[cfg(feature = "cluster")]
    let bind_address = rt
        .df
        .executor
        .read()
        .ok()
        .and_then(|maybe_executor| {
            maybe_executor
                .as_ref()
                .and_then(|e| e.metadata.host.clone().map(|h| (h, e.metadata.port as u16)))
        })
        .and_then(|spec| {
            let (host, port) = &spec;
            tokio::task::block_in_place(|| match spec.to_socket_addrs() {
                Ok(sa) => Some(sa),
                Err(e) => {
                    tracing::error!("Unable to resolve bound executor host {host}:{port}: {e}");
                    None
                }
            })
        })
        .and_then(|mut addrs| addrs.next())
        .unwrap_or(bind_address);

    tracing::info!("Spice Runtime Flight listening on {bind_address}");
    runtime_metrics::spiced_runtime::FLIGHT_SERVER_START.add(1, &[]);

    if let Some(token) = shutdown_signal {
        server
            .serve_with_shutdown(bind_address, token.cancelled())
            .await
    } else {
        server.serve(bind_address).await
    }
    .map_err(|e| {
        if is_address_in_use_error(&e) {
            return Error::AddressAlreadyInUse {
                addr: bind_address.to_string(),
            };
        }
        Error::UnableToStartFlightServer { source: e }
    })?;

    tracing::debug!("Spice Runtime Flight stopped");

    Ok(())
}

pub struct RateLimits {
    pub flight_write_limit: Quota,
}

impl RateLimits {
    #[must_use]
    pub fn new() -> Self {
        RateLimits::default()
    }

    #[must_use]
    pub fn with_flight_write_limit(mut self, rate_limit: Quota) -> Self {
        self.flight_write_limit = rate_limit;
        self
    }
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            // Allow 100 Flight DoPut requests every 60 seconds by default
            flight_write_limit: Quota::per_minute(
                NonZeroU32::new(100).unwrap_or_else(|| unreachable!("100 is always non-zero")),
            ),
        }
    }
}
