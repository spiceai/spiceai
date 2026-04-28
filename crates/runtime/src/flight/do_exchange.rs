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

use std::sync::Arc;

use arrow::array::{ArrayBuilder, ArrayRef, ListBuilder, RecordBatch, StringBuilder, make_builder};
use arrow::array::{ListArray, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow_flight::{FlightData, SchemaAsIpc, flight_service_server::FlightService};
use arrow_ipc::writer::{self, CompressionContext, DictionaryTracker, IpcDataGenerator};
use async_stream::try_stream;
use data_components::cdc::changes_schema;
use datafusion::common::{Constraint, Constraints};
use datafusion::datasource::TableProvider;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use futures::{Stream, StreamExt};
use tokio::sync::{broadcast::error::RecvError, mpsc};
use tonic::{Request, Response, Status, Streaming};

use crate::datafusion::request_context_extension::get_current_datafusion;
use crate::dataupdate::{DataUpdate, UpdateType};
use runtime_request_context::{AsyncMarker, RequestContext};

use super::{Service, metrics};

const DO_EXCHANGE_UPDATE_BUFFER_CAPACITY: usize = 100;

pub(crate) async fn handle(
    flight_svc: &Service,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoExchangeStream>, Status> {
    let _start = metrics::track_flight_request("do_exchange", None);
    let mut streaming_request = request.into_inner();
    let req = streaming_request.next().await;
    let Some(subscription_request) = req else {
        return Err(Status::invalid_argument(
            "Need to send a FlightData message with a FlightDescriptor to subscribe to",
        ));
    };

    let subscription_request = match subscription_request {
        Ok(subscription_request) => subscription_request,
        Err(e) => {
            return Err(Status::invalid_argument(format!(
                "Unable to read subscription request: {e}",
            )));
        }
    };

    // TODO: Support multiple flight descriptors to subscribe to multiple data sources
    let Some(flight_descriptor) = subscription_request.flight_descriptor else {
        return Err(Status::invalid_argument(
            "Flight descriptor required to indicate which data to subscribe to",
        ));
    };

    if flight_descriptor.path.is_empty() {
        return Err(Status::invalid_argument(
            "Flight descriptor needs to specify a path to indicate which data to subscribe to",
        ));
    }

    let data_path = TableReference::parse_str(&flight_descriptor.path.join("."));

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    let Some(table_provider) = datafusion.get_table(&data_path).await else {
        return Err(Status::invalid_argument(format!(
            r#"Unknown dataset: "{data_path}""#,
        )));
    };

    let mut rx = flight_svc
        .data_update_broadcaster
        .subscribe(&data_path)
        .await;
    let (updates_tx, mut updates_rx) =
        mpsc::channel::<Result<DataUpdate, Status>>(DO_EXCHANGE_UPDATE_BUFFER_CAPACITY);
    let updates_data_path = data_path.clone();
    let _updates_task = tokio::spawn(async move {
        loop {
            let update = match rx.recv().await {
                Ok(data_update) => Ok(data_update),
                Err(RecvError::Lagged(skipped)) => Err(Status::data_loss(format!(
                    "DoExchange subscriber lagged behind and missed {skipped} data updates for dataset {updates_data_path}"
                ))),
                Err(RecvError::Closed) => break,
            };

            if updates_tx.send(update).await.is_err() {
                break;
            }
        }
    });

    let table_provider_stream = Arc::clone(&table_provider);
    let datafusion_stream = Arc::clone(&datafusion);

    let response_stream = try_stream! {
        let initial_snapshot_stream = initial_snapshot_stream(&datafusion_stream, Arc::clone(&table_provider_stream)).await?;
        let initial_snapshot_schema = initial_snapshot_stream.schema();
        let mut encoded_initial_update = Box::pin(encode_record_batch_stream(
            &table_provider_stream,
            initial_snapshot_schema,
            initial_snapshot_stream,
            UpdateType::Overwrite,
        ));
        while let Some(flight) = encoded_initial_update.next().await {
            yield flight?;
        }

        while let Some(data_update) = updates_rx.recv().await {
            let data_update = data_update?;
            let mut encoded_update = Box::pin(encode_data_update(&table_provider_stream, &data_update));
            while let Some(flight) = encoded_update.next().await {
                yield flight?;
            }
        }
    };

    Ok(Response::new(response_stream.boxed()))
}

async fn initial_snapshot_stream(
    datafusion: &Arc<crate::datafusion::DataFusion>,
    table_provider: Arc<dyn TableProvider>,
) -> Result<SendableRecordBatchStream, Status> {
    let df = datafusion
        .ctx
        .read_table(table_provider)
        .map_err(|source| Status::internal(format!("Unable to read initial snapshot: {source}")))?;
    df.execute_stream()
        .await
        .map_err(|source| Status::internal(format!("Unable to stream initial snapshot: {source}")))
}

fn encode_record_batch_stream(
    table_provider: &Arc<dyn TableProvider>,
    schema: SchemaRef,
    mut data: SendableRecordBatchStream,
    update_type: UpdateType,
) -> impl Stream<Item = Result<FlightData, Status>> + '_ {
    try_stream! {
        let mut encoder = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        let mut compression_context = CompressionContext::default();
        let write_options = writer::IpcWriteOptions::default();
        let mut schema_sent = false;

        if matches!(&update_type, UpdateType::Overwrite) {
            let truncate_batch = data_update_to_change_batch(table_provider, &schema, None, "t", 1)?;
            for flight in encode_change_batch_flights(
                &mut schema_sent,
                &mut encoder,
                &mut tracker,
                &mut compression_context,
                &write_options,
                &truncate_batch,
            )? {
                metrics::DO_EXCHANGE_DATA_UPDATES_SENT.add(1, &[]);
                yield flight;
            }
        }

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(|source| {
                Status::internal(format!("Unable to read initial snapshot batch: {source}"))
            })?;
            let change_batch = data_update_to_change_batch(
                table_provider,
                &schema,
                Some(&batch),
                change_operation_for_update(&update_type),
                0,
            )?;
            for flight in encode_change_batch_flights(
                &mut schema_sent,
                &mut encoder,
                &mut tracker,
                &mut compression_context,
                &write_options,
                &change_batch,
            )? {
                metrics::DO_EXCHANGE_DATA_UPDATES_SENT.add(1, &[]);
                yield flight;
            }
        }
    }
}

fn encode_data_update<'a>(
    table_provider: &'a Arc<dyn TableProvider>,
    data_update: &'a DataUpdate,
) -> impl Stream<Item = Result<FlightData, Status>> + 'a {
    try_stream! {
        let mut encoder = IpcDataGenerator::default();
        let mut tracker = DictionaryTracker::new(false);
        let mut compression_context = CompressionContext::default();
        let write_options = writer::IpcWriteOptions::default();
        let mut schema_sent = false;

        if data_update.data.is_empty() {
            let (operation, empty_row_count) =
                if matches!(data_update.update_type, UpdateType::Overwrite) {
                    ("t", 1)
                } else {
                    (change_operation_for_update(&data_update.update_type), 0)
                };
            let change_batch = data_update_to_change_batch(
                table_provider,
                &data_update.schema,
                None,
                operation,
                empty_row_count,
            )?;
            for flight in encode_change_batch_flights(
                &mut schema_sent,
                &mut encoder,
                &mut tracker,
                &mut compression_context,
                &write_options,
                &change_batch,
            )? {
                metrics::DO_EXCHANGE_DATA_UPDATES_SENT.add(1, &[]);
                yield flight;
            }
        } else {
            if matches!(data_update.update_type, UpdateType::Overwrite) {
                let truncate_batch =
                    data_update_to_change_batch(table_provider, &data_update.schema, None, "t", 1)?;
                for flight in encode_change_batch_flights(
                    &mut schema_sent,
                    &mut encoder,
                    &mut tracker,
                    &mut compression_context,
                    &write_options,
                    &truncate_batch,
                )? {
                    metrics::DO_EXCHANGE_DATA_UPDATES_SENT.add(1, &[]);
                    yield flight;
                }
            }

            for batch in &data_update.data {
                let change_batch = data_update_to_change_batch(
                    table_provider,
                    &data_update.schema,
                    Some(batch),
                    change_operation_for_update(&data_update.update_type),
                    0,
                )?;
                for flight in encode_change_batch_flights(
                    &mut schema_sent,
                    &mut encoder,
                    &mut tracker,
                    &mut compression_context,
                    &write_options,
                    &change_batch,
                )? {
                    metrics::DO_EXCHANGE_DATA_UPDATES_SENT.add(1, &[]);
                    yield flight;
                }
            }
        }
    }
}

fn change_operation_for_update(update_type: &UpdateType) -> &'static str {
    match update_type {
        UpdateType::Append => "c",
        UpdateType::Overwrite | UpdateType::Changes => "r",
    }
}

fn data_update_to_change_batch(
    table_provider: &Arc<dyn TableProvider>,
    schema: &SchemaRef,
    batch: Option<&RecordBatch>,
    operation: &str,
    empty_row_count: usize,
) -> Result<RecordBatch, Status> {
    let row_count = batch.map_or(empty_row_count, RecordBatch::num_rows);
    let op_array = StringArray::from(vec![operation; row_count]);
    let primary_keys = primary_keys_from_constraints(table_provider.constraints())
        .map(|primary_key_idx| get_primary_keys(schema, primary_key_idx));
    let primary_keys_array = get_primary_keys_array(primary_keys.as_deref(), row_count)?;
    let data_array = match batch {
        Some(batch) => StructArray::from(batch.clone()),
        None => empty_struct_array(schema, row_count),
    };

    let new_schema = Arc::new(changes_schema(schema.as_ref()));
    RecordBatch::try_new(
        new_schema,
        vec![
            Arc::new(op_array),
            Arc::new(primary_keys_array),
            Arc::new(data_array),
        ],
    )
    .map_err(|source| {
        Status::internal(format!(
            "Unable to convert data update into change event: {source}"
        ))
    })
}

fn empty_struct_array(schema: &SchemaRef, row_count: usize) -> StructArray {
    let columns = schema
        .fields()
        .iter()
        .map(|field| arrow::array::new_null_array(field.data_type(), row_count))
        .collect::<Vec<ArrayRef>>();
    StructArray::new(schema.fields().clone(), columns, None)
}

fn encode_change_batch_flights(
    schema_sent: &mut bool,
    encoder: &mut IpcDataGenerator,
    tracker: &mut DictionaryTracker,
    compression_context: &mut CompressionContext,
    write_options: &writer::IpcWriteOptions,
    change_batch: &RecordBatch,
) -> Result<Vec<FlightData>, Status> {
    let mut flights = Vec::new();

    if !*schema_sent {
        flights.push(FlightData::from(SchemaAsIpc::new(
            change_batch.schema().as_ref(),
            write_options,
        )));
        *schema_sent = true;
    }

    let (flight_dictionaries, flight_batch) = encoder
        .encode(change_batch, tracker, write_options, compression_context)
        .map_err(|source| Status::internal(format!("Unable to encode change event: {source}")))?;
    flights.extend(flight_dictionaries.into_iter().map(Into::into));
    flights.push(flight_batch.into());
    Ok(flights)
}

fn primary_keys_from_constraints(constraints: Option<&Constraints>) -> Option<&[usize]> {
    constraints?.iter().find_map(|constraint| match constraint {
        Constraint::PrimaryKey(primary_keys) => Some(primary_keys.as_slice()),
        Constraint::Unique(_) => None,
    })
}

fn get_primary_keys<'a>(schema: &'a SchemaRef, primary_key_idx: &[usize]) -> Vec<&'a str> {
    primary_key_idx
        .iter()
        .map(|idx| schema.field(*idx).name().as_str())
        .collect()
}

fn get_primary_keys_array(
    primary_keys: Option<&[&str]>,
    row_count: usize,
) -> Result<ListArray, Status> {
    let mut list_builder_generic = make_builder(
        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
        row_count,
    );
    let Some(list_builder) = list_builder_generic
        .as_any_mut()
        .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
    else {
        return Err(Status::internal(
            "Unable to build primary key array: expected list builder",
        ));
    };
    for _ in 0..row_count {
        let Some(str_builder) = list_builder
            .values()
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
        else {
            return Err(Status::internal(
                "Unable to build primary key array: expected string builder",
            ));
        };
        if let Some(primary_keys) = primary_keys {
            for key in primary_keys {
                str_builder.append_value(key);
            }
        }
        list_builder.append(true);
    }
    Ok(list_builder.finish())
}
