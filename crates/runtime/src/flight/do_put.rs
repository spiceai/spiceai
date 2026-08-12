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
    utils::flight_data_to_arrow_batch,
};
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow_schema::SchemaRef;
use arrow_tools::schema::verify_schema;
use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter, sql::TableReference,
};
use opentelemetry::KeyValue;
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
        let _start = metrics::track_flight_request("do_put", None);
        return Err(Status::invalid_argument("No flight data provided"));
    };
    let Some(fd) = &first_message.flight_descriptor else {
        let _start = metrics::track_flight_request("do_put", None);
        return Err(Status::invalid_argument("No flight descriptor provided"));
    };

    // Extract table path from FlightSQL commands if present
    let table_path_override = if let Ok(message) = Any::decode(&*fd.cmd) {
        match Command::try_from(message).map_err(|e| Status::internal(format!("{e:?}")))? {
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
    if let Some(rate_limit_check) = rate_limit_check_fn {
        rate_limit_check()?;
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
                return Err(Status::unauthenticated(
                    "Flight DoPut requires authentication.\nFor auth details, visit https://spiceai.org/docs/api/auth",
                ));
            }
        }
    }

    // Since it is not a prepared statement we can take from the stream
    let Some(Ok(first_message)) = streaming_flight.next().await else {
        let _start = metrics::track_flight_request("do_put", None);
        return Err(Status::invalid_argument("No flight data provided"));
    };
    let Some(fd) = &first_message.flight_descriptor else {
        let _start = metrics::track_flight_request("do_put", None);
        return Err(Status::invalid_argument("No flight descriptor provided"));
    };

    // Use table path from FlightSQL command if available, otherwise use descriptor path
    let path_vec = table_path_override.as_ref().unwrap_or(&fd.path);

    if path_vec.is_empty() {
        let _start = metrics::track_flight_request("do_put", None);
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

    // Initializing tracking here so that both counter and duration have consistent path dimensions
    let start = metrics::track_flight_request("do_put", Some(&path.to_string())).await;

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

    let target_schema = datafusion
        .get_arrow_schema(path.clone())
        .await
        .map_err(|e| Status::internal(format!("Failed to get target dataset schema: {e}")))?;

    if let Err(e) = verify_schema(target_schema.fields(), schema.fields()) {
        return Err(Status::invalid_argument(format!(
            "Schema validation error: the provided data schema does not match the expected schema for dataset `{path}`: {e}",
        )));
    }

    let first_message = first_message.clone();
    let response_stream = create_response_stream(
        path,
        schema,
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
/// carried record-batch data — i.e. client rows that were streamed but never
/// written. Keepalive heartbeats and empty trailer messages do not carry data
/// and are not counted, so a sink that completes exactly as the stream ends
/// reports zero discarded batches and the write is still acked as a success.
async fn drain_discarded_data_batches<S>(stream: &mut S) -> usize
where
    S: futures::Stream<Item = Result<FlightData, Status>> + Unpin,
{
    let mut discarded = 0usize;
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(data) => {
                // A message carries record-batch (or dictionary) data when its
                // body is non-empty; keepalive heartbeats are tagged and never
                // count as lost client data.
                if data.app_metadata.as_ref() != crate::flight::KEEPALIVE_APP_METADATA
                    && !data.data_body.is_empty()
                {
                    discarded += 1;
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

fn create_response_stream(
    path: TableReference,
    schema: SchemaRef,
    df: Arc<DataFusion>,
    mut streaming_flight: Peekable<Streaming<FlightData>>,
    first_message: &FlightData,
) -> impl futures::Stream<Item = Result<PutResult, Status>> + use<> {
    let dictionaries_by_id = Arc::new(HashMap::new());
    tracing::debug!("Starting writing data into dataset: {path}");

    // Sometimes the first message only contains the schema and no data
    let first_batch = arrow_flight::utils::flight_data_to_arrow_batch(
        first_message,
        Arc::clone(&schema),
        &dictionaries_by_id,
    )
    .ok();

    stream! {
        // channel to propagate new record batches to the data writing stream
        let (batch_tx, batch_rx)= mpsc::channel::<Result<RecordBatch, DataFusionError>>(100);

        let write_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), Box::new(ReceiverStream::new(batch_rx))));
        let streaming_update = StreamingDataUpdate::new(write_stream, UpdateType::Append);
        let path = path.clone();
        let mut write_future = Box::pin(df.write_streaming_data(&path, streaming_update));

        if let Some(first_batch) = first_batch {
            yield handle_record_batch(first_batch, &batch_tx, &path.to_string()).await;
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

                            let new_batch = match flight_data_to_arrow_batch(
                                &message,
                                Arc::clone(&schema),
                                &dictionaries_by_id,
                            ) {
                                Ok(batches) => batches,
                                Err(e) => {
                                    tracing::error!("Failed to convert flight data to batches: {e}");
                                    yield Err(Status::internal(format!("Failed to convert flight data to batches: {e}")));
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
                            let path_str = path.to_string();
                            let batch_send = handle_record_batch(new_batch, &batch_tx, &path_str);
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

async fn handle_record_batch(
    batch: RecordBatch,
    batch_tx: &Sender<Result<RecordBatch, DataFusionError>>,
    path: &str,
) -> Result<PutResult, Status> {
    tracing::trace!("Received batch with {} rows", batch.num_rows());

    let labels = [KeyValue::new("dataset", path.to_string())];
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

    fn data_message(body: &'static [u8]) -> FlightData {
        FlightData {
            data_body: body.into(),
            ..Default::default()
        }
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
        let messages = vec![
            Ok(data_message(b"batch-1")),
            Ok(keepalive_message()),
            Ok(data_message(b"batch-2")),
            Ok(FlightData::default()), // empty trailer message
            Err(Status::internal("transient read error")),
            Ok(data_message(b"batch-3")),
        ];
        let mut stream = futures::stream::iter(messages);
        assert_eq!(drain_discarded_data_batches(&mut stream).await, 3);
    }

    /// A sink that completes exactly as the stream ends discards nothing, so
    /// the write is still acked as a success.
    #[tokio::test]
    async fn drain_empty_stream_reports_zero() {
        let mut stream = futures::stream::iter(Vec::<Result<FlightData, Status>>::new());
        assert_eq!(drain_discarded_data_batches(&mut stream).await, 0);
    }

    #[tokio::test]
    async fn drain_keepalive_and_trailers_only_reports_zero() {
        let messages = vec![
            Ok(keepalive_message()),
            Ok(FlightData::default()),
            Ok(keepalive_message()),
        ];
        let mut stream = futures::stream::iter(messages);
        assert_eq!(drain_discarded_data_batches(&mut stream).await, 0);
    }
}
