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

use std::{collections::HashMap, sync::Arc};

use arrow::array::RecordBatch;
use arrow_flight::{
    FlightData, PutResult,
    flight_service_server::FlightService,
    sql::{Any, Command},
};
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow_schema::SchemaRef;
use arrow_tools::map_entries::{self, MapEntriesNormalizer};
use arrow_tools::schema::verify_schema;
use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter, sql::TableReference,
};
use opentelemetry::{KeyValue, Value};
use prost::Message as _;
use runtime_auth::AuthRequestContext;
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::{StreamExt as _, adapters::Peekable, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

use async_stream::stream;

use crate::{
    cluster::partition,
    config::ClusterRole,
    datafusion::{DataFusion, request_context_extension::get_current_datafusion},
    dataupdate::{StreamingDataUpdate, UpdateType},
};
use runtime_request_context::{AsyncMarker, RequestContext};
use telemetry::timing::TimedStream;

use super::{
    Service, flightsql, flightsql::prepared_statement_query, metrics,
    middleware::rate_limit::RateLimiterExtension,
};

pub(crate) async fn handle(
    request: Request<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoPutStream>, Status> {
    let rate_limit_check_fn = request
        .extensions()
        .get::<RateLimiterExtension>()
        .map(RateLimiterExtension::check_fn);

    let mut streaming_flight = request.into_inner().peekable();

    // We need to peek at the stream in case we branch below to prepared statements
    let Some(Ok(first_message)) = streaming_flight.peek().await else {
        let _start = metrics::track_flight_request("do_put", None).await;
        return Err(Status::invalid_argument("No flight data provided"));
    };
    let Some(fd) = &first_message.flight_descriptor else {
        let _start = metrics::track_flight_request("do_put", None).await;
        return Err(Status::invalid_argument("No flight descriptor provided"));
    };

    // Extract table path from FlightSQL commands if present
    let table_path_override = if let Ok(message) = Any::decode(&*fd.cmd) {
        let command = match Command::try_from(message) {
            Ok(command) => command,
            Err(e) => {
                let _start = metrics::track_flight_request("do_put", None).await;
                return Err(Status::internal(format!("{e:?}")));
            }
        };

        match command {
            Command::CommandPreparedStatementQuery(query) => {
                return prepared_statement_query::do_put_query(query, streaming_flight).await;
            }
            Command::CommandPreparedStatementUpdate(query) => {
                return flightsql::prepared_statement_update::do_put_update(
                    query,
                    streaming_flight,
                )
                .await;
            }
            Command::CommandStatementUpdate(cmd) => {
                return flightsql::statement_update::do_put(cmd).await;
            }
            Command::CommandStatementIngest(ingest_cmd) => {
                // Handle FlightSQL bulk ingestion command
                // Prefer descriptor path when command is under-qualified (table only).
                // This preserves fully-qualified paths forwarded by the scheduler.
                match (ingest_cmd.catalog.as_ref(), ingest_cmd.schema.as_ref()) {
                    (Some(catalog), Some(schema)) => Some(vec![
                        catalog.clone(),
                        schema.clone(),
                        ingest_cmd.table.clone(),
                    ]),
                    // If command is under-qualified, prefer descriptor path if present,
                    // because scheduler forwarding includes fully-qualified path parts.
                    (Some(catalog), None) => {
                        if fd.path.is_empty() {
                            Some(vec![catalog.clone(), ingest_cmd.table.clone()])
                        } else {
                            None
                        }
                    }
                    (None, Some(schema)) => {
                        if fd.path.is_empty() {
                            Some(vec![schema.clone(), ingest_cmd.table.clone()])
                        } else {
                            None
                        }
                    }
                    (None, None) => {
                        if fd.path.is_empty() {
                            Some(vec![ingest_cmd.table.clone()])
                        } else {
                            None
                        }
                    }
                }
            }
            _ => None,
        }
    } else {
        None
    };

    // Check if the request should be rate limited.
    if let Some(rate_limit_check) = rate_limit_check_fn
        && let Err(status) = rate_limit_check()
    {
        let _start = metrics::track_flight_request("do_put", None).await;
        return Err(status);
    }

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    match context.auth_principal() {
        Some(principal) => {
            if !principal
                .groups()
                .iter()
                .any(|group| *group == "write" || *group == "read_write")
            {
                let _start = metrics::track_flight_request("do_put", None).await;
                return Err(Status::permission_denied(
                    "Write access denied. Verify that authentication key used has write access and try again.",
                ));
            }
        }
        None => {
            if allow_scheduler_trusted_executor_write(&datafusion) {
                tracing::debug!(
                    "Allowing unauthenticated DoPut on executor in mTLS scheduler-trusted mode"
                );
            } else {
                let _start = metrics::track_flight_request("do_put", None).await;
                return Err(Status::unauthenticated(
                    "Flight DoPut requires authentication.\nFor auth details, visit https://spiceai.org/docs/api/auth",
                ));
            }
        }
    }

    // Since it is not a prepared statement we can take from the stream
    let Some(Ok(first_message)) = streaming_flight.next().await else {
        let _start = metrics::track_flight_request("do_put", None).await;
        return Err(Status::invalid_argument("No flight data provided"));
    };
    let Some(fd) = &first_message.flight_descriptor else {
        let _start = metrics::track_flight_request("do_put", None).await;
        return Err(Status::invalid_argument("No flight descriptor provided"));
    };

    // Use table path from FlightSQL command if available, otherwise use descriptor path
    let path_vec = table_path_override.as_ref().unwrap_or(&fd.path);

    if path_vec.is_empty() {
        let _start = metrics::track_flight_request("do_put", None).await;
        return Err(Status::invalid_argument("No path provided"));
    }

    let path = match path_vec.len() {
        3 => TableReference::full(
            path_vec[0].as_str(),
            path_vec[1].as_str(),
            path_vec[2].as_str(),
        ),
        2 => TableReference::partial(path_vec[0].as_str(), path_vec[1].as_str()),
        _ => TableReference::parse_str(&path_vec.join(".")),
    };
    let path = datafusion.normalize_table_reference(path);

    // Allocate path label once for request + per-batch metrics (avoids re-stringifying per batch).
    let path_label: Arc<str> = Arc::from(path.to_string());
    let start =
        metrics::track_flight_request_value("do_put", Some(Value::from(Arc::clone(&path_label))))
            .await;

    if !datafusion.is_writable(&path) && !datafusion.is_path_catalog_writable(&path) {
        return Err(Status::invalid_argument(format!(
            "Path doesn't exist or is not writable: {path}",
        )));
    }

    // Fast path: for scheduler -> executor Cayenne writes, split by partition
    // and forward to each executor.
    if let Some(executor_registry) = datafusion.executor_registry()
        && let Some(partition_expression) = datafusion.get_table_partition_expr(&path).await.map_err(|e| Status::internal(format!(
            "Failed to resolve partition expression for table `{path}` in distributed Cayenne write via Flight: {e}"
        )))?
        && matches!(
            datafusion.cluster_config.effective_role(),
            Some(ClusterRole::Scheduler)
        )
    {
        if !executor_registry.has_flight_sql_clients().await {
            return Err(Status::unavailable(
                "No executors available to write data to. Ensure that at least one executor is connected to the cluster and try again.",
            ));
        }

        let response = partition::write_through::forward_federated_partitioned_write(
            executor_registry,
            Arc::clone(&datafusion.ctx),
            datafusion.io_runtime.clone(),
            &path,
            first_message,
            streaming_flight,
            &[partition_expression],
        )
        .await;

        if let Err(e) = datafusion
            .caching()
            .invalidate_for_table(path.clone())
            .await
        {
            tracing::warn!(
                "Failed to invalidate caches for distributed Flight DoPut table {path}: {e}"
            );
        }

        return response.map_err(Into::into);
    }

    // In distributed mode, the scheduler must NEVER write data locally.
    // Writes should always be forwarded to executors via the partitioned write path above.
    // If we reached this point on the scheduler, the table is either not partitioned
    // or partition resolution failed — reject the write to prevent silent data misrouting.
    if matches!(
        datafusion.cluster_config.effective_role(),
        Some(ClusterRole::Scheduler)
    ) {
        return Err(Status::failed_precondition(format!(
            "Cannot write data to table `{path}` on the scheduler. Ensure the table has a partition expression configured for distributed writes.",
        )));
    }

    let schema = try_schema_from_flatbuffer_bytes(&first_message.data_header)
        .map_err(|e| Status::internal(format!("Failed to get schema from data header: {e}")))?;
    let schema = Arc::new(schema);

    // One stream carries one schema, so what its batches need is resolved once — and the dataset is
    // checked against the shape that will actually be written. See [`MapEntriesGuard`].
    let guard = MapEntriesGuard::for_declared(schema);

    let target_schema = datafusion
        .get_arrow_schema(path.clone())
        .await
        .map_err(|e| Status::internal(format!("Failed to get target dataset schema: {e}")))?;

    if let Err(e) = verify_schema(target_schema.fields(), guard.write_schema().fields()) {
        return Err(Status::invalid_argument(format!(
            "Schema validation error: the provided data schema does not match the expected schema for dataset `{path}`: {e}",
        )));
    }

    let first_message = first_message.clone();
    let response_stream = create_response_stream(
        path,
        path_label,
        guard,
        Arc::clone(&datafusion),
        streaming_flight,
        &first_message,
    );
    let response_stream = context.scope_stream(response_stream);

    let timed_stream = TimedStream::new(response_stream, move || start);

    Ok(Response::new(Box::pin(timed_stream)))
}

fn allow_scheduler_trusted_executor_write(datafusion: &DataFusion) -> bool {
    datafusion.cluster_config.effective_role() == Some(ClusterRole::Executor)
        && datafusion.cluster_config.tls_config().is_some()
}

/// Drains the messages remaining on the inbound flight stream after the write
/// sink has already completed, returning the number of discarded messages that
/// carried IPC data — i.e. client rows that were streamed but never written.
/// Keepalive heartbeats and schema-only or trailer messages carry no data and
/// are not counted, so a sink that completes exactly as the stream ends reports
/// zero discarded batches and the write is still acked as a success.
async fn drain_discarded_data_batches<S>(stream: &mut S) -> usize
where
    S: futures::Stream<Item = Result<FlightData, Status>> + Unpin,
{
    let mut discarded = 0usize;
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(data) => {
                // Keepalive heartbeats are tagged and never count as lost client
                // data, whatever they carry.
                if data.app_metadata.as_ref() == crate::flight::KEEPALIVE_APP_METADATA {
                    continue;
                }

                match declares_ipc_data(&data.data_header) {
                    Ok(carries_data) => {
                        if carries_data {
                            discarded += 1;
                        }
                    }
                    Err(e) => {
                        // A header with bytes that will not parse is a malformed
                        // message, not the absence of a batch. This count sizes a
                        // data-loss report, so it is counted: saying less was lost
                        // than was is the error that matters here.
                        tracing::error!(
                            "Could not read the IPC header of a message discarded after early write completion ({e}); counting it as discarded client data"
                        );
                        discarded += 1;
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    "Error reading remaining message after early write completion: {e}"
                );
            }
        }
    }
    discarded
}

/// What a `DoPut` stream's `MAP` columns need, resolved once from the client's schema message.
///
/// A client is free to declare a `MAP`'s `entries` field nullable, which the Arrow map layout
/// forbids. Every batch is therefore decoded under the client's own declarations and relabelled
/// afterwards, so that an entries array carrying nulls — the one shape relabelling cannot fix — is
/// refused rather than written under a declaration that says it holds none.
///
/// The two decisions the write makes about that live together here because they have to agree: the
/// schema the write stream advertises, and the shape of the batches pushed into it.
struct MapEntriesGuard {
    /// The client's own declaration. Batches are decoded under it — the IPC buffers are laid out
    /// the way it describes.
    declared: SchemaRef,
    normalizer: MapEntriesNormalizer,
}

impl MapEntriesGuard {
    fn for_declared(declared: SchemaRef) -> Self {
        let normalizer = MapEntriesNormalizer::for_schema(&declared);
        Self {
            declared,
            normalizer,
        }
    }

    /// The schema the write stream advertises: the one its batches carry once corrected.
    fn write_schema(&self) -> &SchemaRef {
        self.normalizer.schema()
    }

    /// Decodes one `FlightData` message and brings its `MAP` columns in line with the map layout.
    ///
    /// `Ok(None)` is a message that carries no batch, read from the IPC header: a schema-only
    /// message, or one with no header at all. A message whose header declares a record batch but
    /// will not decode is a malformed stream, and is reported with the decoder's own error rather
    /// than skipped — as is a header that will not parse at all, since neither tells the client
    /// anything if it is reported as an absent batch.
    fn decode(
        &self,
        message: &FlightData,
        dictionaries_by_id: &HashMap<i64, arrow::array::ArrayRef>,
        path: &TableReference,
    ) -> Result<Option<RecordBatch>, Status> {
        let declares_batch = declares_record_batch(&message.data_header).map_err(|e| {
            let message = decode_failure_message(path, &e);
            tracing::error!(dataset = %path, "{message}");
            Status::invalid_argument(message)
        })?;

        if !declares_batch {
            return Ok(None);
        }

        let batch = arrow_flight::utils::flight_data_to_arrow_batch(
            message,
            Arc::clone(&self.declared),
            dictionaries_by_id,
        )
        .map_err(|e| {
            let message = decode_failure_message(path, &e);
            tracing::error!(dataset = %path, "{message}");
            Status::invalid_argument(message)
        })?;

        self.normalizer.normalize(batch).map(Some).map_err(|e| {
            let message = map_entries_message(path, &e);
            tracing::error!(dataset = %path, "{message}");
            Status::invalid_argument(message)
        })
    }
}

/// What an IPC message's header declares, or `None` when the message carries no header bytes.
///
/// The header is the discriminator, not the body length: a batch of zero rows — and a batch
/// whose columns need no buffers — is sent with an empty body, so treating an empty body as
/// "no data" both drops rows the writer sent and under-counts the ones a failed write discarded.
///
/// A message with no header bytes at all declares nothing — Flight allows a metadata-only
/// message, and there is nothing there to misread. A header that has bytes but will not parse is
/// neither a declaration nor the absence of one: it is a malformed stream, and the `Err` is what
/// lets a caller report that parse failure instead of the "carries no batch" diagnosis a `false`
/// would produce, which names the wrong problem and hides the reason the IPC was rejected.
fn declared_message_header(data_header: &[u8]) -> Result<Option<arrow_ipc::MessageHeader>, String> {
    if data_header.is_empty() {
        return Ok(None);
    }

    arrow_ipc::root_as_message(data_header)
        .map(|message| Some(message.header_type()))
        .map_err(|e| e.to_string())
}

/// Whether an IPC message's header declares a record batch — the messages the write decodes.
fn declares_record_batch(data_header: &[u8]) -> Result<bool, String> {
    Ok(declared_message_header(data_header)? == Some(arrow_ipc::MessageHeader::RecordBatch))
}

/// Whether an IPC message's header declares data the write needed: a record batch, or a
/// dictionary the batches referencing it cannot be decoded without.
///
/// Wider than [`declares_record_batch`] because it answers a different question. That one asks
/// what to decode; this one asks what was lost. A dictionary message carries the values its
/// batch refers to, so a batch that references one carries nothing without it — a discarded
/// dictionary is discarded client data even though it is not itself a batch.
fn declares_ipc_data(data_header: &[u8]) -> Result<bool, String> {
    Ok(matches!(
        declared_message_header(data_header)?,
        Some(arrow_ipc::MessageHeader::RecordBatch | arrow_ipc::MessageHeader::DictionaryBatch)
    ))
}

fn create_response_stream(
    path: TableReference,
    path_label: Arc<str>,
    guard: MapEntriesGuard,
    df: Arc<DataFusion>,
    mut streaming_flight: Peekable<Streaming<FlightData>>,
    first_message: &FlightData,
) -> impl futures::Stream<Item = Result<PutResult, Status>> + use<> {
    let dictionaries_by_id = Arc::new(HashMap::new());
    tracing::debug!("Starting writing data into dataset: {path}");

    // Sometimes the first message only contains the schema and no data
    let first_batch = guard.decode(first_message, &dictionaries_by_id, &path);
    let write_schema = Arc::clone(guard.write_schema());

    stream! {
        // channel to propagate new record batches to the data writing stream
        let (batch_tx, batch_rx)= mpsc::channel::<Result<RecordBatch, DataFusionError>>(100);

        let write_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(write_schema, Box::new(ReceiverStream::new(batch_rx))));
        let streaming_update = StreamingDataUpdate::new(write_stream, UpdateType::Append);
        let path = path.clone();
        let mut write_future = Box::pin(df.write_streaming_data(&path, streaming_update));

        match first_batch {
            Ok(Some(first_batch)) => yield handle_record_batch(first_batch, &batch_tx, &path_label).await,
            Ok(None) => {}
            Err(status) => {
                yield Err(status);
                return;
            }
        }

        // Use a single pinned Sleep future that is reset on each received message,
        // rather than creating a new timer allocation on every loop iteration.
        let idle_timeout = crate::flight::do_put_idle_timeout();
        let deadline = tokio::time::sleep(idle_timeout);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                () = &mut deadline => {
                    tracing::error!(
                        dataset = %path,
                        "Timeout: no record batch received within {} seconds",
                        idle_timeout.as_secs()
                    );
                    yield Err(Status::deadline_exceeded(format!(
                        "Timeout: no record batch received within {} seconds",
                        idle_timeout.as_secs()
                    )));
                    break;
                }
                // Poll the writing task to check if it has completed with an error while processing the data
                write_result = &mut write_future => {
                    match write_result {
                        Ok(()) => {
                            // The write operation completed before the flight stream
                            // ended. This can happen when the data sink does not
                            // consume the input stream or finishes early. Drain
                            // the remaining messages, but if any of them carried
                            // record-batch data those client rows were never
                            // written — fail loudly instead of acking a silent
                            // partial ingest.
                            let discarded = drain_discarded_data_batches(&mut streaming_flight).await;
                            if discarded > 0 {
                                tracing::error!(
                                    dataset = %path,
                                    discarded_batches = discarded,
                                    "Write sink completed before the client finished streaming; {discarded} data batch(es) were not written",
                                );
                                yield Err(Status::data_loss(format!(
                                    "Write sink for dataset `{path}` finished before the client stream ended; {discarded} data batch(es) streamed by the client were not written",
                                )));
                                break;
                            }
                            tracing::warn!("Write operation completed before stream ended for dataset: {path}");
                            yield Ok(PutResult::default());
                            break;
                        }
                        Err(e) => {
                            tracing::error!("Write operation failed. Details included in the response.");
                            yield Err(Status::internal(format!("Write operation failed: {e}")));
                            break;
                        }
                    }
                },
                message = streaming_flight.next() => {
                    match message {
                        Some(Ok(message)) => {
                            // Reset the idle timeout on each received message
                            deadline.as_mut().reset(tokio::time::Instant::now() + idle_timeout);

                            // Skip keepalive messages — these are heartbeats from
                            // write-through forwarding to prevent the idle timeout.
                            if message.app_metadata.as_ref() == crate::flight::KEEPALIVE_APP_METADATA {
                                continue;
                            }

                            let new_batch = match guard.decode(&message, &dictionaries_by_id, &path) {
                                Ok(Some(new_batch)) => new_batch,
                                Ok(None) => {
                                    // Only a non-batch header reaches here: a message whose
                                    // header declares a record batch is decoded, empty body and
                                    // all, and one that will not decode is refused by `decode`
                                    // with the decoder's own error. Mid-stream, a schema message
                                    // is not the schema-only first message, so the stream has
                                    // gone out of step with what it declared.
                                    let message = format!(
                                        "Received an Arrow message that carries no record batch partway through the write to dataset '{path}', so the rest of the stream was not applied and any batch already accepted may have been. \
                                        Send every message after the schema as a record batch. \
                                        See: https://spiceai.org/docs/api/arrow-flight-sql"
                                    );
                                    tracing::error!(dataset = %path, "{message}");
                                    yield Err(Status::invalid_argument(message));
                                    break;
                                }
                                Err(status) => {
                                    yield Err(status);
                                    break;
                                }
                            };

                            // Only report errors; a success message is sent as the final step upon successful write completion.
                            //
                            // The send must race against polling `write_future`:
                            // `write_future` is an unspawned future driven solely by
                            // this loop, and the sink consumes `batch_rx` only while
                            // it is polled. Awaiting the send directly suspends this
                            // generator inside the branch when the channel is full,
                            // so the sink could never be polled again to drain it —
                            // a permanent lost-wakeup deadlock that froze all writes
                            // to an executor whenever the sink fell one channel's
                            // worth of batches behind the wire (e.g. during a slow
                            // metastore WAL checkpoint under heavy ingest). Keep the
                            // sink and the idle deadline polled while the send is
                            // pending so backpressure propagates instead of
                            // deadlocking.
                            let batch_send = handle_record_batch(new_batch, &batch_tx, &path_label);
                            let mut batch_send = std::pin::pin!(batch_send);
                            tokio::select! {
                                biased;
                                () = &mut deadline => {
                                    tracing::error!(
                                        dataset = %path,
                                        "Timeout: write sink did not accept a record batch within {} seconds",
                                        idle_timeout.as_secs()
                                    );
                                    yield Err(Status::deadline_exceeded(format!(
                                        "Timeout: write sink did not accept a record batch within {} seconds",
                                        idle_timeout.as_secs()
                                    )));
                                    break;
                                }
                                write_result = &mut write_future => {
                                    match write_result {
                                        Ok(()) => {
                                            // Sink finished while a batch was still pending. That
                                            // pending batch was not accepted by the sink, so it
                                            // counts as discarded along with anything left on the
                                            // wire; fail loudly rather than ack a partial ingest.
                                            let discarded = 1 + drain_discarded_data_batches(&mut streaming_flight).await;
                                            tracing::error!(
                                                dataset = %path,
                                                discarded_batches = discarded,
                                                "Write sink completed while a client batch was still pending; {discarded} data batch(es) were not written",
                                            );
                                            yield Err(Status::data_loss(format!(
                                                "Write sink for dataset `{path}` finished before the client stream ended; {discarded} data batch(es) streamed by the client were not written",
                                            )));
                                            break;
                                        }
                                        Err(e) => {
                                            tracing::error!("Write operation failed. Details included in the response.");
                                            yield Err(Status::internal(format!("Write operation failed: {e}")));
                                            break;
                                        }
                                    }
                                }
                                send_res = &mut batch_send => {
                                    if let Err(err) = send_res {
                                        yield Err(err);
                                        break;
                                    }
                                }
                            }
                        }
                        None => {
                            // End of the stream; signal that stream is completed and data write should be finalized
                            drop(batch_tx);
                            tracing::trace!("No more messages in the stream, finalizing write operation for path: {path}");

                            // Wait for the write operation to complete, logging a
                            // heartbeat while finalization is pending so a stuck
                            // write is visible instead of hanging silently.
                            let finalize_start = std::time::Instant::now();
                            let write_result = loop {
                                match tokio::time::timeout(std::time::Duration::from_secs(30), &mut write_future).await {
                                    Ok(res) => break res,
                                    Err(_) => {
                                        tracing::warn!(
                                            dataset = %path,
                                            waited_s = finalize_start.elapsed().as_secs(),
                                            "DoPut write finalization still pending",
                                        );
                                    }
                                }
                            };
                            if let Err(e) = write_result {
                                tracing::error!("Write operation failed. Details included in the response.");
                                yield Err(Status::internal(format!("Write operation failed: {e}")));
                                break;
                            }
                            tracing::debug!("Write operation completed successfully for dataset: {path}");
                            yield Ok(PutResult::default());
                            break;
                        }
                        Some(Err(e)) => {
                            tracing::error!("Error reading message: {e}");
                            yield Err(Status::internal(format!("Error reading message: {e}")));
                            break;
                        }
                    }
                }
            }
        };

        tracing::debug!("Finished writing data into dataset: {path}");
    }
}

/// Reports Arrow data a client sent that will not decode.
///
/// [`MapEntriesGuard::decode`] serves the first message of a write and every later one alike, so
/// this may assert only what holds for both: a first message that fails has applied nothing, while
/// a later one may follow batches the sink has already accepted. It therefore says the write did
/// not complete and leaves the rollback question to the callers that can answer it — claiming the
/// write "was not applied" would be false for every message after the first.
fn decode_failure_message(path: &TableReference, source: &impl std::fmt::Display) -> String {
    format!(
        "Failed to read the Arrow data sent for dataset '{path}' ({source}), so the write did not complete. \
         Send each message as an Arrow IPC record batch matching the schema the stream declared. \
         See: https://spiceai.org/docs/api/arrow-flight-sql"
    )
}

/// The failure a client sees when the Arrow data it streamed holds a `MAP` column that cannot be
/// brought in line with the Arrow map layout.
///
/// This is an append that has been consuming the client's stream, so it cannot say that nothing was
/// written: a batch accepted before the refusing one may already have reached the sink. It says what
/// holds for every batch the refusal can land on — the rest of the stream is not applied.
fn map_entries_message(path: &TableReference, source: &map_entries::Error) -> String {
    format!(
        "Failed to write to dataset '{path}' ({source}), so the rest of the stream was not applied and any batch already accepted may have been. \
         Send the MAP column with an `entries` field that is non-nullable and holds no null entries, as the Arrow map layout requires. \
         See: https://spiceai.org/docs/api/arrow-flight-sql"
    )
}

async fn handle_record_batch(
    batch: RecordBatch,
    batch_tx: &Sender<Result<RecordBatch, DataFusionError>>,
    path: &Arc<str>,
) -> Result<PutResult, Status> {
    tracing::trace!("Received batch with {} rows", batch.num_rows());

    let labels = [KeyValue::new("dataset", Arc::clone(path))];
    metrics::DO_PUT_ROWS_WRITTEN.add(batch.num_rows() as u64, &labels);
    metrics::DO_PUT_BYTES_WRITTEN.add(batch.get_array_memory_size() as u64, &labels);

    if let Err(e) = batch_tx.send(Ok(batch)).await {
        tracing::error!("Error sending record batch to write channel: {e}");
        return Err(Status::internal(format!(
            "Error sending record batch to write channel: {e}"
        )));
    }
    Ok(PutResult::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encodes `batch` the way a Flight client does and returns the messages after the leading
    /// schema message: the batch itself, with a real IPC header. Building the messages by hand
    /// would let the test agree with the code about a header layout neither shares with a client.
    fn encoded_messages(batch: &RecordBatch) -> Vec<FlightData> {
        let mut messages =
            arrow_flight::utils::batches_to_flight_data(&batch.schema(), vec![batch.clone()])
                .expect("encoding a batch as flight data");
        // The leading message declares the schema and carries no rows.
        messages.remove(0);
        messages
    }

    /// A batch of one `Int32` row — the ordinary case, whose body is non-empty.
    fn one_row_batch() -> RecordBatch {
        use arrow::array::{ArrayRef, Int32Array};
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .expect("building a one-row batch")
    }

    fn keepalive_message() -> FlightData {
        FlightData {
            app_metadata: crate::flight::KEEPALIVE_APP_METADATA.into(),
            // Even if a keepalive carried a body, it must never be counted as
            // discarded client data.
            data_body: (&b"heartbeat"[..]).into(),
            ..Default::default()
        }
    }

    /// Only data-bearing messages count as discarded; keepalives, empty
    /// trailers, and transient read errors are ignored.
    #[tokio::test]
    async fn drain_counts_only_data_bearing_messages() {
        let batch = one_row_batch();
        let mut messages: Vec<Result<FlightData, Status>> = vec![];
        messages.extend(encoded_messages(&batch).into_iter().map(Ok));
        messages.push(Ok(keepalive_message()));
        messages.extend(encoded_messages(&batch).into_iter().map(Ok));
        messages.push(Ok(FlightData::default())); // empty trailer message
        messages.push(Err(Status::internal("transient read error")));
        messages.extend(encoded_messages(&batch).into_iter().map(Ok));

        let mut stream = futures::stream::iter(messages);
        assert_eq!(drain_discarded_data_batches(&mut stream).await, 3);
    }

    /// The count sizes a data-loss report, so a batch of zero rows has to appear in it: the
    /// client streamed it and it was not written. Arrow encodes such a batch as a `RecordBatch`
    /// header with an empty body, so counting by body length reported it as nothing at all.
    #[tokio::test]
    async fn drain_counts_a_zero_row_batch() {
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let messages = encoded_messages(&RecordBatch::new_empty(schema));

        assert_eq!(messages.len(), 1, "expected a single batch message");
        assert!(
            messages[0].data_body.is_empty(),
            "a zero-row batch is expected to encode with an empty body; without that this case does not exercise the miscount"
        );
        // What the discriminator this replaces would have counted. Asserted so the case cannot
        // quietly stop distinguishing the two: it is the whole reason the case exists.
        assert_eq!(
            messages
                .iter()
                .filter(|data| !data.data_body.is_empty())
                .count(),
            0
        );

        let mut stream = futures::stream::iter(messages.into_iter().map(Ok::<_, Status>));
        assert_eq!(drain_discarded_data_batches(&mut stream).await, 1);
    }

    /// A dictionary message carries the values its batch refers to, so discarding one discards
    /// client data even though the message is not itself a batch. `Resend` because the default
    /// hydrates dictionaries into plain arrays and no dictionary message is sent at all.
    #[tokio::test]
    async fn drain_counts_a_dictionary_message() {
        use arrow::array::{ArrayRef, DictionaryArray};
        use arrow::datatypes::Int32Type;
        use arrow_flight::encode::{DictionaryHandling, FlightDataEncoderBuilder};
        use arrow_schema::{DataType, Field, Schema};
        use futures::TryStreamExt;

        let values: DictionaryArray<Int32Type> = vec!["a", "b", "a"].into_iter().collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "label",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values) as ArrayRef])
            .expect("building a dictionary-encoded batch");

        let mut messages: Vec<FlightData> = FlightDataEncoderBuilder::new()
            .with_dictionary_handling(DictionaryHandling::Resend)
            .build(futures::stream::iter(vec![Ok(batch)]))
            .try_collect()
            .await
            .expect("encoding a dictionary batch as flight data");
        // The leading message declares the schema and carries no rows.
        messages.remove(0);

        let dictionaries = messages
            .iter()
            .filter(|message| {
                arrow_ipc::root_as_message(&message.data_header)
                    .expect("parsing an encoded header")
                    .header_type()
                    == arrow_ipc::MessageHeader::DictionaryBatch
            })
            .count();
        assert_eq!(
            dictionaries, 1,
            "expected the encoder to emit one dictionary message"
        );

        let expected = messages.len();
        let mut stream = futures::stream::iter(messages.into_iter().map(Ok::<_, Status>));
        assert_eq!(drain_discarded_data_batches(&mut stream).await, expected);
    }

    /// A schema message declares no rows, so re-sending one after the sink completed loses
    /// nothing and must not be reported as a discarded batch.
    #[tokio::test]
    async fn drain_does_not_count_a_schema_message() {
        use arrow_schema::{DataType, Field, Schema};

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let schema_message = arrow_flight::utils::batches_to_flight_data(&schema, vec![])
            .expect("encoding a schema as flight data")
            .remove(0);

        assert!(
            !schema_message.data_header.is_empty(),
            "a schema message is expected to carry a header"
        );

        let mut stream = futures::stream::iter(vec![Ok::<_, Status>(schema_message)]);
        assert_eq!(drain_discarded_data_batches(&mut stream).await, 0);
    }

    /// A header with bytes that will not parse is a malformed message, not an absent batch.
    /// Under-reporting is the failure that matters for a data-loss count, so it counts.
    #[tokio::test]
    async fn drain_counts_a_message_whose_header_will_not_parse() {
        let malformed = FlightData {
            data_header: (&b"not an ipc message"[..]).into(),
            ..Default::default()
        };

        let mut stream = futures::stream::iter(vec![Ok::<_, Status>(malformed)]);
        assert_eq!(drain_discarded_data_batches(&mut stream).await, 1);
    }

    /// The failure a client sees when its `MAP` column cannot be brought in line with the Arrow
    /// map layout has to name the dataset, state what the write did and did not apply, give a
    /// remediation that covers the case it actually refuses, and point at the docs — a reword must
    /// not quietly drop any of the four.
    #[test]
    fn the_map_entries_refusal_names_the_dataset_the_impact_and_the_docs() {
        let source = arrow_tools::map_entries::Error::MapEntriesContainNulls {
            column: "attributes".to_string(),
        };
        let message =
            super::map_entries_message(&TableReference::partial("sales", "orders"), &source);

        assert!(message.contains("'sales.orders'"), "{message}");
        assert!(
            message.contains("the rest of the stream was not applied"),
            "{message}"
        );
        assert!(
            message.contains("may have been"),
            "an append cannot claim nothing was written: {message}"
        );
        assert!(message.contains("attributes"), "{message}");
        assert!(
            message.contains("holds no null entries"),
            "flipping the declaration does not fix the case this refuses: {message}"
        );
        assert!(
            message.contains("https://spiceai.org/docs/api/arrow-flight-sql"),
            "{message}"
        );
    }

    /// Builds a `MapArray` the way the Flight decoder does — straight from `ArrayData`, so
    /// neither of `MapArray::try_new`'s `entries` checks runs and a client's non-conforming
    /// declaration survives the decode.
    fn map_batch(entry_nulls: Option<arrow::buffer::NullBuffer>) -> RecordBatch {
        use arrow::array::{Array, ArrayData, ArrayRef, MapArray, StringArray, StructArray};
        use arrow::buffer::Buffer;
        use arrow_schema::{DataType, Field, Fields, Schema};

        let rows = entry_nulls
            .as_ref()
            .map_or(1, arrow::buffer::NullBuffer::len);
        let entry_fields: Fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into();
        let data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(entry_fields.clone()),
                true,
            )),
            false,
        );

        let keys: Vec<String> = (0..rows).map(|i| format!("k{i}")).collect();
        let values: Vec<String> = (0..rows).map(|i| format!("v{i}")).collect();
        let entries = StructArray::try_new(
            entry_fields,
            vec![
                Arc::new(StringArray::from(keys)) as ArrayRef,
                Arc::new(StringArray::from(values)) as ArrayRef,
            ],
            entry_nulls,
        )
        .expect("entries struct");

        let offsets: Vec<i32> = (0..=i32::try_from(rows).expect("row count")).collect();
        let data = ArrayData::builder(data_type.clone())
            .len(rows)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(entries.to_data())
            .build()
            .expect("map array data");

        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("m", data_type, true)])),
            vec![Arc::new(MapArray::from(data)) as ArrayRef],
        )
        .expect("map batch")
    }

    /// The schema message and the data message a client would put on the wire for `batch`.
    fn flight_messages(batch: &RecordBatch) -> Vec<FlightData> {
        arrow_flight::utils::batches_to_flight_data(batch.schema().as_ref(), vec![batch.clone()])
            .expect("encoding the batch")
    }

    fn conforming(schema: &arrow_schema::SchemaRef) -> bool {
        match schema.field(0).data_type() {
            arrow_schema::DataType::Map(entries, _) => !entries.is_nullable(),
            other => panic!("expected a Map column, got {other:?}"),
        }
    }

    /// Regression test for #13495: a batch of zero rows is still a batch. Arrow encodes one as a
    /// `RecordBatch` message with an empty body, so a decoder that discriminates on body length
    /// reports it as carrying no batch — and mid-stream this path refuses the whole write once
    /// that happens, after earlier batches have already been appended, which a retry duplicates.
    #[test]
    fn a_zero_row_batch_is_decoded_rather_than_read_as_a_schema_message() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let guard = MapEntriesGuard::for_declared(Arc::clone(&schema));
        let path = TableReference::bare("orders");
        let dictionaries = HashMap::new();

        let messages = flight_messages(&batch);
        assert!(
            messages
                .last()
                .expect("the encoding carries a batch message")
                .data_body
                .is_empty(),
            "premise of this test: Arrow sends a zero-row batch with an empty body"
        );

        let decoded: Vec<RecordBatch> = messages
            .iter()
            .filter_map(|message| {
                guard
                    .decode(message, &dictionaries, &path)
                    .expect("a zero-row batch decodes")
            })
            .collect();

        let [decoded] = decoded.as_slice() else {
            panic!("exactly one message carries a batch, got {}", decoded.len());
        };
        assert_eq!(decoded.num_rows(), 0);
        assert_eq!(&decoded.schema(), guard.write_schema());
    }

    /// Regression test for #13495: a client declaring a `MAP`'s `entries` nullable — which the
    /// Arrow map layout forbids — has the declaration corrected as each message is decoded, and
    /// the write stream advertises the corrected schema rather than the client's. The two have to
    /// agree: a stream that describes its batches with a type they no longer carry is a defect of
    /// its own.
    #[test]
    fn a_clients_nullable_map_entries_declaration_is_corrected_before_the_sink() {
        let batch = map_batch(None);
        let guard = MapEntriesGuard::for_declared(batch.schema());
        let path = TableReference::bare("orders");
        let dictionaries = HashMap::new();

        assert!(
            conforming(guard.write_schema()),
            "the write stream still advertises the client's non-conforming declaration"
        );

        let decoded: Vec<RecordBatch> = flight_messages(&batch)
            .iter()
            .filter_map(|message| {
                guard
                    .decode(message, &dictionaries, &path)
                    .expect("a nullable entries declaration is relabelled, not refused")
            })
            .collect();

        let [decoded] = decoded.as_slice() else {
            panic!("exactly one message carries a batch, got {}", decoded.len());
        };
        assert!(conforming(&decoded.schema()));
        assert_eq!(&decoded.schema(), guard.write_schema());
        assert_eq!(decoded.num_rows(), 1);
    }

    /// The one shape relabelling cannot fix is refused at the decode, so it never reaches the
    /// sink, and the refusal reaches the client as an argument error naming the column.
    #[test]
    fn a_map_whose_entries_carry_nulls_is_refused_before_the_sink() {
        let batch = map_batch(Some(arrow::buffer::NullBuffer::from(vec![true, false])));
        let guard = MapEntriesGuard::for_declared(batch.schema());
        let path = TableReference::bare("orders");
        let dictionaries = HashMap::new();

        let statuses: Vec<Status> = flight_messages(&batch)
            .iter()
            .filter_map(|message| guard.decode(message, &dictionaries, &path).err())
            .collect();

        let [status] = statuses.as_slice() else {
            panic!(
                "the data message must be refused, got {} refusals",
                statuses.len()
            );
        };
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status.message().contains("'orders'") && status.message().contains("'m'"),
            "the refusal must name the dataset and the column: {}",
            status.message()
        );
    }

    /// The decode failure is reported by a function shared with the first message of the write,
    /// so it must not claim a rollback it cannot guarantee — while still naming the dataset,
    /// giving a remediation, and pointing at the docs.
    #[test]
    fn the_decode_failure_names_the_dataset_without_promising_a_rollback() {
        let message = super::decode_failure_message(
            &TableReference::partial("sales", "orders"),
            &"unexpected end of stream",
        );

        assert!(message.contains("'sales.orders'"), "{message}");
        assert!(message.contains("unexpected end of stream"), "{message}");
        assert!(
            !message.contains("the write was not applied"),
            "a message shared with the mid-stream path cannot promise nothing was written: {message}"
        );
        assert!(
            message.contains("Send each message as an Arrow IPC record batch"),
            "{message}"
        );
        assert!(message.contains("https://spiceai.org/docs"), "{message}");
    }

    /// A schema message carries no batch. It is skipped rather than refused, which is what the
    /// write has always done with a first message that holds only the schema.
    #[test]
    fn a_message_carrying_no_batch_is_skipped() {
        let batch = map_batch(None);
        let guard = MapEntriesGuard::for_declared(batch.schema());
        let messages = flight_messages(&batch);
        let schema_message = messages.first().expect("a schema message");

        assert!(
            guard
                .decode(
                    schema_message,
                    &HashMap::new(),
                    &TableReference::bare("orders")
                )
                .expect("a schema message is not a failure")
                .is_none()
        );
    }

    /// A message with no IPC header is metadata-only, not malformed: there are no bytes there to
    /// misread, so it is skipped exactly as a schema message is.
    #[test]
    fn a_message_with_no_header_carries_no_batch() {
        let guard = MapEntriesGuard::for_declared(map_batch(None).schema());

        assert!(
            guard
                .decode(
                    &FlightData::default(),
                    &HashMap::new(),
                    &TableReference::bare("orders")
                )
                .expect("a message with no header is not a failure")
                .is_none()
        );
    }

    /// A header that has bytes but will not parse is a malformed stream. Reporting it as a message
    /// that merely carries no batch names the wrong problem and drops the reason the IPC was
    /// rejected, so it is refused with the parse failure the client can act on.
    #[test]
    fn a_malformed_header_is_refused_with_the_parse_failure() {
        let guard = MapEntriesGuard::for_declared(map_batch(None).schema());
        let malformed = FlightData {
            data_header: (&b"this is not a flatbuffer"[..]).into(),
            ..Default::default()
        };

        let status = guard
            .decode(&malformed, &HashMap::new(), &TableReference::bare("orders"))
            .expect_err("a header that will not parse is a malformed stream");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status.message().contains("Failed to read the Arrow data"),
            "the parse failure has to reach the client, not an absent-batch diagnosis: {}",
            status.message()
        );
        assert!(
            !status.message().contains("carries no record batch"),
            "a malformed header is not an absent batch: {}",
            status.message()
        );
    }
}
