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

use crate::auth::EndpointAuth;
use crate::datafusion::DataFusion;
use crate::datafusion::error::{SpiceExternalError, find_datafusion_root};
use crate::datafusion::query::{self, QueryBuilder};
use crate::datafusion::sql_validator::validate_sql_query_read_only;
use crate::dataupdate::DataUpdateBroadcaster;
use crate::opentelemetry::create_metrics_service;
use crate::tls::TlsConfig;
use crate::{Runtime, metrics as runtime_metrics};
use app::App;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator};
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
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::sqlparser::parser::ParserError;
use flight_client::Error as FlightClientError;
use futures::stream::{self, BoxStream, StreamExt};
use futures::{Stream, TryStreamExt};
use governor::{Quota, RateLimiter};
use metrics::track_flight_request;
use middleware::{RequestContextLayer, WriteRateLimitLayer};
use runtime_auth::{AuthRequestContext, FlightBasicAuth, layer::flight::BasicAuthLayer};
use runtime_request_context::{AsyncMarker, RequestContext};
use snafu::prelude::*;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

mod actions;
mod async_actions;
mod do_exchange;
mod do_get;
mod do_put;
mod flightsql;
mod get_flight_info;
mod get_schema;
mod handshake;
mod metrics;
pub mod middleware;
mod session;
pub(crate) mod session_auth;
mod util;

pub use session::SessionStore;

/// Sentinel value in [`FlightData::app_metadata`] that marks a message as a
/// keepalive heartbeat. Write-through forwarding tasks send these periodically
/// to prevent the executor's `DoPut` idle timeout from firing on streams that
/// receive data in bursts with long idle gaps between them.
pub use runtime_cluster::flight_config::{KEEPALIVE_APP_METADATA, do_put_idle_timeout};

pub struct Service {
    data_update_broadcaster: DataUpdateBroadcaster,
    basic_auth: Option<Arc<dyn FlightBasicAuth + Send + Sync>>,
    session_store: SessionStore,
}

impl Service {
    /// Creates a new Flight service using the shared data update broadcaster.
    #[must_use]
    pub fn new(
        basic_auth: Option<Arc<dyn FlightBasicAuth + Send + Sync>>,
        data_update_broadcaster: DataUpdateBroadcaster,
    ) -> Self {
        Self {
            data_update_broadcaster,
            basic_auth,
            session_store: SessionStore::new(),
        }
    }

    /// Returns a clone of the session store.
    #[must_use]
    pub fn session_store(&self) -> SessionStore {
        self.session_store.clone()
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
        let response = handshake::handle(
            request.metadata(),
            self.basic_auth.as_ref(),
            &self.session_store,
        )
        .await?;
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
        let read_only = crate::http::v1::current_principal_requires_read_only().await;
        let query = QueryBuilder::new(sql, datafusion)
            .read_only(read_only)
            .build();

        let (dataset_schema, parameter_schema) =
            query.get_schema().await.map_err(handle_datafusion_error)?;

        // The logical plan may report Utf8View/BinaryView, but the physical
        // execution (with `expand_views_at_output = true`) will produce
        // LargeUtf8/LargeBinary.  Align the advertised schema so that
        // `get_flight_info` and `do_get` are consistent.
        let dataset_schema = arrow_tools::schema::expand_views_schema(&dataset_schema);

        Ok((dataset_schema, parameter_schema))
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
        parameters: Option<ParamValues>,
        pre_parsed_plan: Option<LogicalPlan>,
    ) -> Result<(BoxStream<'static, Result<FlightData, Status>>, CacheStatus), Status> {
        let read_only = crate::http::v1::current_principal_requires_read_only().await;
        let query_result = if let Some(plan) = pre_parsed_plan {
            QueryBuilder::from_plan(plan, sql, Arc::clone(&datafusion))
                .parameters(parameters)
                .read_only(read_only)
                .build()
                .run()
                .await
                .map_err(handle_query_error)?
        } else {
            QueryBuilder::new(sql, Arc::clone(&datafusion))
                .parameters(parameters)
                .read_only(read_only)
                .build()
                .run()
                .await
                .map_err(handle_query_error)?
        };

        // Reuse the same options for all messages
        let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
        let schema = query_result.data.schema();

        // Pre-compute schema flight data once
        let mut dict_tracker = DictionaryTracker::new(true); // Set to true to handle dictionaries
        let mut compression_context = CompressionContext::default();
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
                            .encode(&batch, &mut dict_tracker, &options, &mut compression_context)
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

pub(crate) fn record_batches_to_flight_stream(
    record_batches: Vec<RecordBatch>,
) -> impl Stream<Item = Result<FlightData, Status>> {
    FlightDataEncoderBuilder::new()
        .build(stream::iter(record_batches.into_iter().map(Ok)))
        .map_err(to_tonic_err)
}

/// Returns `true` when the request has an authenticated principal that
/// lacks write permission (`"write"` or `"read_write"` group).
/// Returns `false` when there is no principal (auth not configured)
/// or the principal has write access.
pub(crate) fn is_auth_read_only(context: &RequestContext) -> bool {
    context.auth_principal().is_some_and(|principal| {
        !principal
            .groups()
            .iter()
            .any(|g| *g == "write" || *g == "read_write")
    })
}

/// If the current principal is read-only, validates that `sql` does not contain
/// any write operations (DDL, DML, COPY, write-capable extensions) and returns
/// the parsed [`LogicalPlan`] with parameters bound so callers can reuse it
/// without re-parsing.
///
/// Unlike `QueryBuilder::read_only`, this check does NOT disable the results cache —
/// read-only principals still benefit from cached SELECT results.
///
/// If the `sql` is guaranteed to be a write-based, [`is_auth_read_only`] is more efficient.
///
/// Returns:
/// - `Ok(Some(plan))` — principal is read-only, SQL is safe (plan reusable, params bound)
/// - `Ok(None)`       — principal has write access; no plan was parsed
/// - `Err(_)`         — principal is read-only and SQL contains a write operation
pub(crate) async fn check_read_only_sql(
    context: &RequestContext,
    datafusion: &Arc<DataFusion>,
    sql: &str,
    parameters: Option<&datafusion::common::ParamValues>,
) -> Result<Option<LogicalPlan>, Status> {
    if !is_auth_read_only(context) {
        return Ok(None);
    }
    let session = datafusion.ctx.state();
    let plan = datafusion
        .create_logical_plan(&session, sql)
        .await
        .map_err(|e| Status::invalid_argument(format!("Failed to parse SQL: {e}")))?;
    // Bind parameters to the plan so the returned plan is fully resolved.
    let plan = if let Some(params) = parameters {
        plan.with_param_values(params.clone())
            .map_err(|e| Status::invalid_argument(format!("Failed to bind parameters: {e}")))?
    } else {
        plan
    };
    if let Err(e) = validate_sql_query_read_only(&plan) {
        return Err(Status::permission_denied(format!(
            "Write access denied. {e}"
        )));
    }
    Ok(Some(plan))
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
        query::Error::QueryCancelled { .. } => Status::cancelled(e.to_string()),
        _ => to_tonic_err(e),
    }
}

fn handle_datafusion_error(e: DataFusionError) -> Status {
    if query::is_cancellation_error(&e) {
        return Status::cancelled(e.to_string());
    }
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
        | DataFusionError::Ffi(_)
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

    #[snafu(display("Unable to bind Flight TCP listener: {source}"))]
    UnableToBindFlightListener { source: std::io::Error },

    #[snafu(display("Unable to bind cluster TCP listener: {source}"))]
    UnableToBindClusterListener { source: std::io::Error },

    #[snafu(display(
        "Address {addr} is already in use by another process. Either stop the existing process or change the address: https://spiceai.org/docs/cli/reference/run"
    ))]
    AddressAlreadyInUse { addr: String },

    #[snafu(display(
        "The cluster scheduler is not initialized, preventing the flight service from starting."
    ))]
    ClusterSchedulerNotInitialized {},

    #[snafu(display("Unable to start internal cluster server: {source}"))]
    UnableToStartClusterServer { source: tonic::transport::Error },

    #[snafu(display("The flight service has an insecure configuration: {message}"))]
    InsecureConfiguration { message: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) fn is_address_in_use_error(err: &tonic::transport::Error) -> bool {
    let mut source: Option<&dyn std::error::Error> = Some(err);
    while let Some(e) = source {
        if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
            return io_err.kind() == std::io::ErrorKind::AddrInUse;
        }
        source = e.source();
    }
    false
}

/// Starts the Flight server.
///
/// # Errors
///
/// Returns an error if the server fails to bind to the specified address or if there are issues
/// with TLS setup.
///
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
    if matches!(
        rt.df.cluster_config.effective_role(),
        Some(crate::config::ClusterRole::Executor)
    ) {
        return Err(Error::InsecureConfiguration {
            message:
                "Executor flight server must be started via cluster::start_executor_flight_server"
                    .to_string(),
        });
    }

    let service = Service::new(
        endpoint_auth.flight_basic_auth.as_ref().map(Arc::clone),
        rt.datafusion().data_update_broadcaster(),
    );
    let session_store = service.session_store.clone();

    let flight_message_size = app
        .as_ref()
        .and_then(|a| a.runtime.flight.clone())
        .and_then(|f| f.max_message_size_bytes().transpose())
        .transpose()
        .map_err(|e| Error::InsecureConfiguration {
            message: format!(
                "Failed to parse spicepod value 'runtime.flight.max_message_size': {e}"
            ),
        })?;

    let spice_flight_service = FlightServiceServer::new(service)
        .max_decoding_message_size(
            flight_message_size.unwrap_or(flight_client::MAX_DECODING_MESSAGE_SIZE),
        )
        .max_encoding_message_size(
            flight_message_size.unwrap_or(flight_client::MAX_ENCODING_MESSAGE_SIZE),
        );

    let server = Server::builder();
    let session_aware_auth = session_auth::with_session_awareness(
        endpoint_auth.flight_basic_auth,
        session_store.clone(),
    );
    let auth_layer = tower::ServiceBuilder::new()
        .layer(BasicAuthLayer::new(session_aware_auth))
        .into_inner();

    // Create the OpenTelemetry MetricsService
    let otel_service = create_metrics_service(rt.datafusion());

    // Get job executor if available (cluster mode)
    let job_executor = rt.job_executor();
    let flight_write_rate_limit_enabled = rt.flight_write_rate_limit_enabled();

    let mut server = server
        .layer(
            RequestContextLayer::new(app, rt.datafusion(), session_store, rt.secrets())
                .with_job_executor(job_executor),
        )
        .layer(auth_layer)
        .layer(WriteRateLimitLayer::new(
            RateLimiter::direct(rate_limits.flight_write_limit),
            flight_write_rate_limit_enabled,
        ));

    let server = server
        .add_service(spice_flight_service)
        .add_service(otel_service);

    let serve_result = if let Some(ref tls_config) = tls_config {
        // TLS path: bind a TCP listener ourselves, run tokio-rustls per
        // connection so we can hot-swap the cert via the resolver, and feed
        // the resulting TlsStreams into tonic via serve_with_incoming. This
        // replaces the legacy `Server::tls_config(ServerTlsConfig::new()...)`
        // approach which baked the cert in once at startup.
        let listener = tokio::net::TcpListener::bind(bind_address)
            .await
            .map_err(|source| {
                if source.kind() == std::io::ErrorKind::AddrInUse {
                    Error::AddressAlreadyInUse {
                        addr: bind_address.to_string(),
                    }
                } else {
                    Error::UnableToBindFlightListener { source }
                }
            })?;
        // Bind succeeded; emit the started log + metric now so a failed
        // bind doesn't show up as a phantom "Flight listening" line.
        tracing::info!("Spice Runtime Flight listening on {bind_address}");
        runtime_metrics::spiced_runtime::FLIGHT_SERVER_START.add(1, &[]);
        let incoming = crate::tls::flight_incoming::tls_incoming(
            listener,
            Arc::clone(&tls_config.server_config),
        );
        if let Some(token) = shutdown_signal {
            server
                .serve_with_incoming_shutdown(incoming, token.cancelled())
                .await
        } else {
            server.serve_with_incoming(incoming).await
        }
    } else if let Some(token) = shutdown_signal {
        // Plain (no-TLS) path: tonic binds internally so we can't gate
        // the log on a successful bind without a refactor; the
        // is_address_in_use_error mapping below still surfaces
        // bind-time failures to the caller.
        tracing::info!("Spice Runtime Flight listening on {bind_address}");
        runtime_metrics::spiced_runtime::FLIGHT_SERVER_START.add(1, &[]);
        server
            .serve_with_shutdown(bind_address, token.cancelled())
            .await
    } else {
        tracing::info!("Spice Runtime Flight listening on {bind_address}");
        runtime_metrics::spiced_runtime::FLIGHT_SERVER_START.add(1, &[]);
        server.serve(bind_address).await
    };

    serve_result.map_err(|e| {
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
    /// Whether write rate limiting is enabled. When `false`, the rate limiter
    /// layer is still present but the check function always succeeds.
    flight_write_enabled: AtomicBool,
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

    #[must_use]
    pub fn with_flight_write_enabled(mut self, enabled: bool) -> Self {
        self.flight_write_enabled = AtomicBool::new(enabled);
        self
    }

    #[must_use]
    pub fn flight_write_enabled(&self) -> bool {
        self.flight_write_enabled.load(Ordering::Acquire)
    }

    pub fn set_flight_write_enabled(&self, enabled: bool) {
        self.flight_write_enabled.store(enabled, Ordering::Release);
    }
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            // Allow 100 Flight DoPut requests every 60 seconds by default
            flight_write_limit: Quota::per_minute(
                NonZeroU32::new(100).unwrap_or_else(|| unreachable!("100 is always non-zero")),
            ),
            flight_write_enabled: AtomicBool::new(true),
        }
    }
}
