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

use crate::Runtime;
use crate::auth::EndpointAuth;
use crate::datafusion::DataFusion;
use crate::datafusion::app_context_extension::AppContextExtension;
use crate::datafusion::error::{SpiceExternalError, find_datafusion_root};
use crate::datafusion::query::{self, QueryBuilder};
use crate::datafusion::sql_validator::validate_sql_query_read_only;
use crate::dataupdate::DataUpdateBroadcaster;
use crate::egress::EgressAccount;
use crate::opentelemetry::create_metrics_service;
use crate::tls::TlsConfig;
use app::{App, spicepod::component::runtime::FlightIpcCompression};
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use arrow::ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{Action, ActionType, Criteria, IpcMessage, PollInfo, PutResult, SchemaResult};
use arrow_flight::{
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, SchemaAsIpc,
    Ticket, flight_service_server::FlightServiceServer,
};
use arrow_ipc::{CompressionType, writer::IpcWriteOptions};
use bytes::Bytes;
use cache::result::{CacheStatus, query::QueryResult};
use datafusion::common::ParamValues;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::MemoryPool;
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
use std::future::Future;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{Instrument, Span};

mod actions;
mod async_actions;
mod do_exchange;
mod do_get;
mod do_put;
mod flightsql;
mod get_flight_info;
mod get_schema;
mod handshake;
pub(crate) mod metrics;
pub mod middleware;
mod mtls;
mod session;
pub(crate) mod session_auth;
mod traced_ticket;
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

/// The handler each RPC delegates to records its own `flight_requests` /
/// `flight_request_duration_ms` sample, so the sample carries a `command` label
/// and, where the response is a stream, spans the drain rather than the setup.
/// Starting a timer here as well would double every sample, so don't.
/// `list_flights` and `poll_flight_info` are the exceptions — they are
/// unimplemented, have no handler to delegate to, and record here.
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
        get_schema::handle(request).await
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let response = Box::pin(do_get::handle(request)).await?;
        Ok(Self::wrap_response_stream_with_scope(response).await)
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let response = do_put::handle(request).await?;
        Ok(Self::wrap_response_stream_with_scope(response).await)
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let response = do_exchange::handle(self, request).await?;
        Ok(Self::wrap_response_stream_with_scope(response).await)
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let response = Box::pin(actions::do_action(request)).await?;
        Ok(Self::wrap_response_stream_with_scope(response).await)
    }

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
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

    /// Construct a stream of [`FlightData`] for a given sql statement.
    ///
    /// This function does not perform any read-only validation itself. Callers
    /// are responsible for gating access:
    /// - For read-only principals, callers must pre-validate the SQL via
    ///   [`check_read_only_sql`] and pass the resulting plan as `read_only_plan`.
    ///   The original `sql` is then used only for caching, tracing, and
    ///   observability.
    /// - When `read_only_plan` is `None`, `sql` is executed directly and may be a
    ///   DDL/DML statement. Only callers that have already determined the
    ///   request is allowed to execute write statements should use this path.
    async fn sql_to_flight_stream(
        datafusion: Arc<DataFusion>,
        sql: &str,
        parameters: Option<ParamValues>,
        read_only_plan: Option<LogicalPlan>,
    ) -> Result<(BoxStream<'static, Result<FlightData, Status>>, CacheStatus), Status> {
        let query_builder = if let Some(plan) = read_only_plan {
            QueryBuilder::from_plan(plan, sql, Arc::clone(&datafusion))
        } else {
            QueryBuilder::new(sql, Arc::clone(&datafusion))
        };

        let query_result = query_builder
            .parameters(parameters)
            .build()
            .run()
            .await
            .map_err(handle_query_error)?;
        let context = RequestContext::current(AsyncMarker::new().await);
        let ipc_write_options = Self::ipc_write_options_for_context(&context)?;
        Ok(Self::query_result_to_flight_stream(
            query_result,
            ipc_write_options,
            datafusion.cpu_runtime().cloned(),
            &datafusion.ctx.runtime_env().memory_pool,
            context,
        ))
    }

    pub(crate) fn ipc_write_options_for_context(
        context: &RequestContext,
    ) -> Result<IpcWriteOptions, Status> {
        let ipc_compression = context
            .extension::<AppContextExtension>()
            .and_then(|app_ext| app_ext.app())
            .and_then(|app| {
                app.runtime
                    .flight
                    .as_ref()
                    .map(|flight| flight.ipc_compression)
            })
            .unwrap_or_default();

        Self::ipc_write_options(ipc_compression)
    }

    fn ipc_write_options(ipc_compression: FlightIpcCompression) -> Result<IpcWriteOptions, Status> {
        let compression = match ipc_compression {
            FlightIpcCompression::None => None,
            FlightIpcCompression::Lz4Frame => Some(CompressionType::LZ4_FRAME),
            FlightIpcCompression::Zstd => Some(CompressionType::ZSTD),
        };

        IpcWriteOptions::default()
            .try_with_compression(compression)
            .map_err(to_tonic_err)
    }

    fn query_result_to_flight_stream(
        query_result: QueryResult,
        ipc_write_options: IpcWriteOptions,
        cpu_runtime: Option<Handle>,
        memory_pool: &Arc<dyn MemoryPool>,
        request_context: Arc<RequestContext>,
    ) -> (BoxStream<'static, Result<FlightData, Status>>, CacheStatus) {
        // Reuse the same options for all messages.
        let options = ipc_write_options;
        let raw_schema = query_result.data.schema();

        let needs_view_cast = raw_schema
            .fields()
            .iter()
            .any(|field| matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView));
        // Expand Utf8View → LargeUtf8 and BinaryView → LargeBinary so the
        // schema header matches what we advertise in GetFlightInfo and what
        // clients (e.g. ADBC) expect after seeing that advertisement.
        let schema = if needs_view_cast {
            Arc::new(arrow_tools::schema::expand_views_schema(&raw_schema))
        } else {
            raw_schema
        };

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

        // Charge the encoded FlightData buffered for send against the query
        // memory pool so egress memory is visible to `runtime.query.memory_limit`
        // and applies back-pressure under real pressure.
        let account = EgressAccount::register(memory_pool, "flight_egress");

        // Encode on the dedicated CPU runtime when one is configured, otherwise on
        // the current (IO) runtime — the fallback when
        // `runtime.params.dedicated_thread_pool=disabled`. Either way encoding runs
        // as a task feeding a small bounded channel: it never blocks the tonic
        // response writer inline, the channel back-pressures so egress memory stays
        // bounded (and a slow client stalls execution), and the encode of batch N
        // overlaps the socket write of batch N-1, reducing transfer time. With a
        // dedicated CPU runtime that overlap is free; on the shared IO-runtime
        // fallback the spawn costs one scheduling hop before the first byte.
        let encode_runtime = cpu_runtime.unwrap_or_else(Handle::current);
        let (tx, rx) = mpsc::channel::<Result<FlightData, Status>>(FLIGHT_ENCODE_CHANNEL_CAPACITY);
        let span = Span::current();

        let encode_task = {
            let account = Arc::clone(&account);
            async move {
                // Reserve when a message enters the channel; the consumer
                // (`FlightEncodeStream`) releases it once handed to tonic.
                account.reserve(flight_data_size(&schema_flight_data)).await;
                if tx.send(Ok(schema_flight_data)).await.is_err() {
                    return;
                }

                let mut data_stream = data_stream.fuse();

                while let Some(batch_result) = data_stream.next().await {
                    match batch_result {
                        Ok(batch) => match encode_flight_batch(
                            batch,
                            needs_view_cast,
                            &schema,
                            &encoder,
                            &mut dict_tracker,
                            &options,
                            &mut compression_context,
                        ) {
                            Ok((dicts, batch_data)) => {
                                for dict in dicts {
                                    account.reserve(flight_data_size(&dict)).await;
                                    if tx.send(Ok(dict)).await.is_err() {
                                        return;
                                    }
                                }
                                account.reserve(flight_data_size(&batch_data)).await;
                                if tx.send(Ok(batch_data)).await.is_err() {
                                    return;
                                }
                            }
                            Err(status) => {
                                let _ = tx.send(Err(status)).await;
                                return;
                            }
                        },
                        Err(e) => {
                            let e = find_datafusion_root(e);
                            let _ = tx.send(Err(handle_datafusion_error(e))).await;
                            return;
                        }
                    }
                }
            }
        };

        let encode_handle =
            encode_runtime.spawn(request_context.scope(encode_task).instrument(span));

        let stream = FlightEncodeStream {
            receiver: ReceiverStream::new(rx),
            encode_handle: Some(encode_handle),
            account,
        };

        (stream.boxed(), cache_status)
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

/// Number of already-encoded `FlightData` messages buffered between the encode
/// task and the tonic response writer. Kept small so per-stream egress memory
/// stays bounded, while still letting the encode of batch N overlap the socket
/// write of batch N-1.
const FLIGHT_ENCODE_CHANNEL_CAPACITY: usize = 2;

/// Encode one [`RecordBatch`] into its Flight dictionary + record-batch
/// messages, applying the `Utf8View`/`BinaryView` → `Large*` cast when the
/// advertised schema was expanded.
fn encode_flight_batch(
    batch: RecordBatch,
    needs_view_cast: bool,
    schema: &Arc<Schema>,
    encoder: &IpcDataGenerator,
    dict_tracker: &mut DictionaryTracker,
    options: &IpcWriteOptions,
    compression_context: &mut CompressionContext,
) -> Result<(Vec<FlightData>, FlightData), Status> {
    // Cast view columns to match the expanded schema we advertised.
    let batch = if needs_view_cast {
        arrow_tools::schema::cast_view_columns(batch, schema)
            .map_err(|e| Status::internal(e.to_string()))?
    } else {
        batch
    };

    let (dicts, batch_data) = encoder
        .encode(&batch, dict_tracker, options, compression_context)
        .map_err(|e| Status::internal(e.to_string()))?;

    Ok((
        dicts.into_iter().map(Into::into).collect(),
        batch_data.into(),
    ))
}

/// Heap/wire bytes a single [`FlightData`] message occupies while buffered for
/// send — used to charge egress against the query memory pool.
fn flight_data_size(flight_data: &FlightData) -> usize {
    flight_data.data_header.len() + flight_data.data_body.len() + flight_data.app_metadata.len()
}

/// Response stream for the Flight encode pipeline. Wraps the receiver of
/// already-encoded [`FlightData`] and owns the encode task's [`JoinHandle`] so
/// that:
///   1. buffered messages are drained first, then a panic — or an unexpected
///      cancellation (e.g. runtime shutdown) — of the encode task surfaces as a
///      stream error instead of a silent truncation (which would look like a
///      successful short result), and
///   2. dropping the response stream (client disconnect) aborts the encode task,
///      which in turn drops the upstream execution stream.
///
/// Uses the same join-handle-backed approach as `RuntimeDriverStream` (execution
/// offload); unlike that stream it polls drain-first (point 1 above) rather than
/// observing the handle first.
struct FlightEncodeStream {
    receiver: ReceiverStream<Result<FlightData, Status>>,
    encode_handle: Option<JoinHandle<()>>,
    /// Egress reservation shared with the encode task. The encode task reserves
    /// each message's bytes before it enters the channel; we release them here
    /// as each message is handed to tonic. Dropping this (client disconnect)
    /// frees any still-buffered bytes via the reservation's `Drop`.
    account: Arc<EgressAccount>,
}

impl Stream for FlightEncodeStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Drain already-encoded messages first, so a panic/cancellation surfaces
        // only after the client has received everything the encode task sent.
        if let Some(item) = std::task::ready!(Pin::new(&mut this.receiver).poll_next(cx)) {
            if let Ok(flight_data) = &item {
                // Message handed to tonic — release its egress reservation.
                this.account.release(flight_data_size(flight_data));
            }
            return Poll::Ready(Some(item));
        }

        // Channel closed: the encode task has ended. Surface a panic — or an
        // unexpected cancellation that we did not trigger via `Drop::abort` (e.g.
        // runtime shutdown) — as a stream error rather than a silent end-of-stream
        // that would look like a successful short result. While the channel is
        // closed but the handle has not resolved yet, `ready!` yields `Pending` so
        // a panic is still reported instead of ending silently.
        let Some(handle) = this.encode_handle.as_mut() else {
            return Poll::Ready(None);
        };
        let result = std::task::ready!(Future::poll(Pin::new(handle), cx));
        this.encode_handle = None;
        match result {
            Ok(()) => Poll::Ready(None),
            Err(err) if err.is_panic() => Poll::Ready(Some(Err(Status::internal(format!(
                "Flight encode task panicked: {err}"
            ))))),
            Err(_) => Poll::Ready(Some(Err(Status::internal(
                "Flight encode task was cancelled before completing",
            )))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.receiver.size_hint()
    }
}

impl Drop for FlightEncodeStream {
    fn drop(&mut self) {
        if let Some(handle) = self.encode_handle.take()
            && !handle.is_finished()
        {
            handle.abort();
        }
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
    if let Some(FlightError::Tonic(status)) =
        (&e as &dyn std::any::Any).downcast_ref::<FlightError>()
    {
        return Status::new(status.code(), status.message());
    }
    Status::internal(format!("{e}"))
}

fn handle_query_error(e: query::Error) -> Status {
    match e {
        query::Error::BindingParameters { source }
        | query::Error::UnableToExecuteQuery { source } => handle_datafusion_error(source),
        query::Error::QueryCancelled { .. } => Status::cancelled(e.to_string()),
        query::Error::QueryTimedOut { .. } => Status::deadline_exceeded(e.to_string()),
        _ => to_tonic_err(e),
    }
}

/// Map a shared-orchestrator [`TransactionError`](query::TransactionError) to the
/// gRPC `Status` the `FlightSQL` transaction path returns. A `Conflict` is a
/// retryable optimistic-concurrency loss (`Aborted`).
pub(crate) fn transaction_error_to_status(error: query::TransactionError) -> Status {
    use query::TransactionError;
    match error {
        TransactionError::Rejected(message) => Status::invalid_argument(message),
        TransactionError::Plan(e) | TransactionError::Stream(e) => handle_datafusion_error(e),
        TransactionError::Query(e) => handle_query_error(e),
        TransactionError::Conflict { table } => Status::aborted(format!(
            "transaction write conflict on '{table}': a participant table changed since the transaction started; retry"
        )),
        TransactionError::Publish(message) => {
            Status::internal(format!("transaction publish failed: {message}"))
        }
    }
}

pub(crate) fn handle_datafusion_error(e: DataFusionError) -> Status {
    if query::is_cancellation_error(&e) {
        return Status::cancelled(e.to_string());
    }
    if query::is_timeout_error(&e) {
        return Status::deadline_exceeded(e.to_string());
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

    #[snafu(display(
        "The cluster executor is not initialized, preventing the flight service from starting."
    ))]
    ClusterExecutorNotInitialized {},

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

    let server = configure_flight_server_transport(Server::builder());
    let session_aware_auth = session_auth::with_session_awareness(
        endpoint_auth.flight_basic_auth,
        session_store.clone(),
    );
    let identity_source = endpoint_auth.identity_source;
    let auth_layer = tower::ServiceBuilder::new()
        .layer(BasicAuthLayer::new(session_aware_auth))
        .into_inner();

    // Create the OpenTelemetry MetricsService. Pass a weak runtime handle so ingest can
    // evolve an accelerated metric table's schema in place when new dimensions arrive.
    let query_engine: Arc<dyn runtime_query_engine::query_engine::QueryEngine> = rt.datafusion();
    let otel_service = create_metrics_service(query_engine, Some(Arc::downgrade(&rt)));

    // Get job executor if available (cluster mode)
    let job_executor = rt.job_executor();
    let flight_write_rate_limit_enabled = rt.flight_write_rate_limit_enabled();

    let mut server = server
        .layer(
            RequestContextLayer::new(rt.app(), rt.datafusion(), session_store, rt.secrets())
                .with_job_executor(job_executor),
        )
        // mTLS principal injection runs *after* RequestContextLayer
        // (which sets up the AuthRequestContext extension) and
        // *before* BasicAuthLayer (which short-circuits when a
        // principal is already present).
        .layer(mtls::MtlsLayer::new(identity_source))
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
            Arc::clone(&tls_config.flight_server_config),
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

pub(crate) fn configure_flight_server_transport(server: Server) -> Server {
    server
        .initial_stream_window_size(flight_client::HTTP2_INITIAL_STREAM_WINDOW_SIZE)
        .initial_connection_window_size(flight_client::HTTP2_INITIAL_CONNECTION_WINDOW_SIZE)
}

pub struct RateLimits {
    pub flight_write_limit: Quota,
    /// Whether write rate limiting is enabled. When `false`, the rate limiter
    /// layer is still present but the check function always succeeds.
    flight_write_enabled: AtomicBool,
    /// Rate limit applied to every request served by the `/metrics` HTTP endpoint
    /// (both local scrapes and `?scope=cluster` fan-out). It is independent of the
    /// data-path write limit so that clients can still retrieve observability data
    /// even when their data requests are rate-limited. Because this throttles all
    /// `/metrics` callers, lowering it to protect the expensive cluster fan-out
    /// will also throttle ordinary local Prometheus scrapes.
    pub metrics_endpoint_limit: Quota,
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
    pub fn with_metrics_endpoint_limit(mut self, rate_limit: Quota) -> Self {
        self.metrics_endpoint_limit = rate_limit;
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
            // Allow 100 /metrics HTTP requests every 60 seconds by default.
            // This is a separate limiter from the data-path write limit so that
            // clients can still retrieve observability data even when data
            // requests are rate-limited.
            metrics_endpoint_limit: Quota::per_minute(
                NonZeroU32::new(100).unwrap_or_else(|| unreachable!("100 is always non-zero")),
            ),
        }
    }
}
