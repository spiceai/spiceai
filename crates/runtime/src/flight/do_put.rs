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

use std::{collections::HashMap, sync::Arc};

use arrow::array::RecordBatch;
use arrow_flight::{
    flight_service_server::FlightService, utils::flight_data_to_arrow_batch, FlightData, PutResult,
};
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter, sql::TableReference,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use async_stream::stream;

use crate::{
    dataupdate::{StreamingDataUpdate, UpdateType},
    timing::TimedStream,
};

use super::{metrics, Service};

pub(crate) async fn handle(
    flight_svc: &Service,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoPutStream>, Status> {
    let mut streaming_flight = request.into_inner();

    let Ok(Some(message)) = streaming_flight.message().await else {
        let _start = metrics::track_flight_request("do_put", None);
        return Err(Status::invalid_argument("No flight data provided"));
    };
    let Some(fd) = &message.flight_descriptor else {
        let _start = metrics::track_flight_request("do_put", None);
        return Err(Status::invalid_argument("No flight descriptor provided"));
    };
    if fd.path.is_empty() {
        let _start = metrics::track_flight_request("do_put", None);
        return Err(Status::invalid_argument("No path provided"));
    };

    let path = TableReference::parse_str(&fd.path.join("."));

    // Initializing tracking here so that both counter and duration have consistent path dimensions
    let start = metrics::track_flight_request("do_put", Some(&path.to_string())).await;

    if !flight_svc.datafusion.is_writable(&path) {
        return Err(Status::invalid_argument(format!(
            "Path doesn't exist or is not writable: {path}",
        )));
    };

    let schema = try_schema_from_flatbuffer_bytes(&message.data_header)
        .map_err(|e| Status::internal(format!("Failed to get schema from data header: {e}")))?;
    let schema = Arc::new(schema);
    let dictionaries_by_id = Arc::new(HashMap::new());

    // Sometimes the first message only contains the schema and no data
    let first_batch = arrow_flight::utils::flight_data_to_arrow_batch(
        &message,
        Arc::clone(&schema),
        &dictionaries_by_id,
    )
    .ok();

    let mut batches = vec![];
    if let Some(first_batch) = first_batch {
        batches.push(first_batch);
    }

    let df = Arc::clone(&flight_svc.datafusion);

    let response_stream = stream! {
        // channel to propogate new record batches to the data writing stream
        let (batch_tx, batch_rx)= mpsc::channel::<Result<RecordBatch, DataFusionError>>(100);

        let write_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), Box::new(ReceiverStream::new(batch_rx))));
        let streaming_update = StreamingDataUpdate::new(write_stream, UpdateType::Append);
        let path = path.clone();
        let mut write_future = Box::pin(df.write_streaming_data(&path, streaming_update));

        loop {
            tokio::select! {
                // Poll the writing task to check if it has completed with an error while processing the data
                write_result = &mut write_future => {
                    match write_result {
                        Ok(()) => unreachable!("Write operation should not complete successfully before the end of the stream"),
                        Err(e) => {
                            tracing::error!("Write operation failed: {e}");
                            yield Err(Status::internal(format!("Write operation failed: {e}")));
                            break;
                        }
                    }
                },
                message = streaming_flight.message() => {
                    match message {
                        Ok(Some(message)) => {
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

                            tracing::trace!("Received batch with {} rows", new_batch.num_rows());

                            if let Err(e) = batch_tx.send(Ok(new_batch)).await {
                                tracing::error!("Error sending record batch to write channel: {e}");
                                yield Err(Status::internal(format!("Error sending record batch to write channel: {e}")));
                            }
                            yield Ok(PutResult::default());
                        }
                        Ok(None) => {
                            // End of the stream; signal that stream is completed and data write should be finalized
                            drop(batch_tx);

                            // Wait for the write operation to complete
                            if let Err(e) = write_future.await {
                                tracing::error!("Write operation failed: {e}");
                                yield Err(Status::internal(format!("Write operation failed: {e}")));
                            }
                            break;
                        }
                        Err(e) => {
                            tracing::error!("Error reading message: {e}");
                            yield Err(Status::internal(format!("Error reading message: {e}")));
                            break;
                        }
                    }
                }
            }
        };

        tracing::trace!("Finished writing data to path: {path}");
    };

    let timed_stream = TimedStream::new(response_stream, move || start);

    Ok(Response::new(Box::pin(timed_stream)))
}
