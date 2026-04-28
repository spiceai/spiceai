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

use std::{collections::VecDeque, sync::Arc};

use arrow::array::{
    ArrayBuilder, ListBuilder, RecordBatch, StringBuilder, make_builder, new_null_array,
};
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
use futures::StreamExt;
use tokio::sync::broadcast::error::RecvError;
use tonic::{Request, Response, Status, Streaming};

use crate::datafusion::request_context_extension::get_current_datafusion;
use crate::dataupdate::UpdateType;
use runtime_request_context::{AsyncMarker, RequestContext};

use super::{Service, metrics};

const MAX_PENDING_INITIAL_SNAPSHOT_UPDATES: usize = 1_024;

struct ChangeFlightEncoder {
    encoder: IpcDataGenerator,
    tracker: DictionaryTracker,
    compression_context: CompressionContext,
    write_options: writer::IpcWriteOptions,
    schema_sent: bool,
}

impl Default for ChangeFlightEncoder {
    fn default() -> Self {
        Self {
            encoder: IpcDataGenerator::default(),
            tracker: DictionaryTracker::new(false),
            compression_context: CompressionContext::default(),
            write_options: writer::IpcWriteOptions::default(),
            schema_sent: false,
        }
    }
}

impl ChangeFlightEncoder {
    fn encode(&mut self, record_batch: &RecordBatch) -> Result<Vec<FlightData>, Status> {
        let mut flights = Vec::new();
        if !self.schema_sent {
            flights.push(FlightData::from(SchemaAsIpc::new(
                record_batch.schema().as_ref(),
                &self.write_options,
            )));
            self.schema_sent = true;
        }

        let (flight_dictionaries, flight_batch) = self
            .encoder
            .encode(
                record_batch,
                &mut self.tracker,
                &self.write_options,
                &mut self.compression_context,
            )
            .map_err(|source| Status::internal(format!("Unable to encode batch: {source}")))?;

        flights.extend(flight_dictionaries.into_iter().map(Into::into));
        flights.push(flight_batch.into());

        Ok(flights)
    }
}

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
    let table_provider_stream = Arc::clone(&table_provider);
    let datafusion_stream = Arc::clone(&datafusion);
    let data_path_stream = data_path.clone();

    let response_stream = try_stream! {
        let mut encoder = ChangeFlightEncoder::default();
        macro_rules! yield_encoded_batch {
            ($change_batch:expr) => {{
                let flights = encoder.encode(&$change_batch)?;
                metrics::DO_EXCHANGE_DATA_UPDATES_SENT.add(flights.len() as u64, &[]);
                for flight in flights {
                    yield flight;
                }
            }};
        }
        macro_rules! yield_data_update {
            ($data_update:expr) => {{
                let data_update = $data_update;
                if data_update.update_type == UpdateType::Overwrite {
                    let truncate_batch = truncate_change_batch(&data_update.schema)?;
                    yield_encoded_batch!(truncate_batch);
                }

                for batch in &data_update.data {
                    let change_batch = record_batch_to_change_batch(
                        &table_provider_stream,
                        batch,
                    )?;
                    yield_encoded_batch!(change_batch);
                }
            }};
        }

        let mut initial_snapshot_stream = initial_snapshot_stream(
            &datafusion_stream,
            Arc::clone(&table_provider_stream),
        )
        .await?;
        let snapshot_schema = initial_snapshot_stream.schema();

        let truncate_batch = truncate_change_batch(&snapshot_schema)?;
        yield_encoded_batch!(truncate_batch);

        let mut pending_updates = VecDeque::new();
        let mut pending_updates_overflowed = false;
        let mut updates_closed = false;

        loop {
            tokio::select! {
                update = rx.recv(), if !updates_closed => {
                    match update {
                        Ok(data_update) => {
                            if pending_updates_overflowed {
                                continue;
                            }

                            if pending_updates.len() >= MAX_PENDING_INITIAL_SNAPSHOT_UPDATES {
                                pending_updates.clear();
                                pending_updates_overflowed = true;
                                tracing::warn!(
                                    dataset = %data_path_stream,
                                    max_pending_updates = MAX_PENDING_INITIAL_SNAPSHOT_UPDATES,
                                    "DoExchange subscriber received too many updates while streaming initial snapshot"
                                );
                            } else {
                                pending_updates.push_back(data_update);
                            }
                        }
                        Err(RecvError::Lagged(skipped_messages)) => {
                            pending_updates.clear();
                            pending_updates_overflowed = true;
                            tracing::warn!(
                                dataset = %data_path_stream,
                                skipped_messages,
                                "DoExchange subscriber lagged while streaming initial snapshot"
                            );
                        }
                        Err(RecvError::Closed) => updates_closed = true,
                    }
                }
                batch = initial_snapshot_stream.next() => {
                    let Some(batch) = batch else {
                        break;
                    };
                    let batch = batch.map_err(|source| {
                        Status::internal(format!("Unable to stream initial snapshot: {source}"))
                    })?;
                    let change_batch = record_batch_to_change_batch(
                        &table_provider_stream,
                        &batch,
                    )?;
                    yield_encoded_batch!(change_batch);
                }
            }
        }

        if pending_updates_overflowed {
            Err(Status::data_loss(format!(
                "DoExchange subscriber missed data updates while receiving the initial snapshot for dataset {data_path_stream}; resubscribe and reconcile state"
            )))?;
        }

        while let Some(data_update) = pending_updates.pop_front() {
            yield_data_update!(&data_update);
        }

        if !updates_closed {
            loop {
                match rx.recv().await {
                    Ok(data_update) => yield_data_update!(&data_update),
                    Err(RecvError::Lagged(skipped_messages)) => {
                        Err(Status::data_loss(format!(
                            "DoExchange subscriber fell behind and missed {skipped_messages} update(s) for dataset {data_path_stream}; resubscribe and reconcile state"
                        )))?;
                    }
                    Err(RecvError::Closed) => break,
                }
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

    df.execute_stream().await.map_err(|source| {
        Status::internal(format!(
            "Unable to execute initial snapshot stream: {source}"
        ))
    })
}

fn record_batch_to_change_batch(
    table_provider: &Arc<dyn TableProvider>,
    batch: &RecordBatch,
) -> Result<RecordBatch, Status> {
    let schema = batch.schema();
    let row_count = batch.num_rows();
    let op_array = StringArray::from(vec!["r"; row_count]);
    let primary_keys = get_primary_keys_from_constraints(&schema, table_provider.constraints())?;
    let primary_keys_array = match primary_keys.as_ref() {
        Some(primary_keys) => get_primary_keys_array(primary_keys, row_count),
        None => ListArray::new_null(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            row_count,
        ),
    };
    let data_array = StructArray::from(batch.clone());
    let change_schema = Arc::new(changes_schema(schema.as_ref()));

    RecordBatch::try_new(
        change_schema,
        vec![
            Arc::new(op_array),
            Arc::new(primary_keys_array),
            Arc::new(data_array),
        ],
    )
    .map_err(|source| {
        Status::internal(format!(
            "Unable to convert record batch into change event: {source}"
        ))
    })
}

fn truncate_change_batch(schema: &SchemaRef) -> Result<RecordBatch, Status> {
    let change_schema = Arc::new(changes_schema(schema.as_ref()));
    let op_array = StringArray::from(vec!["t"]);
    let primary_keys_array =
        ListArray::new_null(Arc::new(Field::new("item", DataType::Utf8, false)), 1);
    let data_array = new_null_array(&DataType::Struct(schema.fields().clone()), 1);

    RecordBatch::try_new(
        change_schema,
        vec![Arc::new(op_array), Arc::new(primary_keys_array), data_array],
    )
    .map_err(|source| {
        Status::internal(format!(
            "Unable to create initial snapshot truncate event: {source}"
        ))
    })
}

fn get_primary_keys_from_constraints<'a>(
    schema: &'a SchemaRef,
    constraints: Option<&Constraints>,
) -> Result<Option<Vec<&'a str>>, Status> {
    let Some(primary_key_indices) = constraints.and_then(|constraints| {
        constraints.iter().find_map(|constraint| match constraint {
            Constraint::PrimaryKey(primary_key_indices) => Some(primary_key_indices.as_slice()),
            Constraint::Unique(_) => None,
        })
    }) else {
        return Ok(None);
    };

    let mut primary_keys = Vec::with_capacity(primary_key_indices.len());
    for primary_key_index in primary_key_indices {
        let Some(field) = schema.fields().get(*primary_key_index) else {
            return Err(Status::internal(format!(
                "Primary key index {primary_key_index} is not present in schema"
            )));
        };
        primary_keys.push(field.name().as_str());
    }

    Ok(Some(primary_keys))
}

fn get_primary_keys_array(primary_keys: &[&str], row_count: usize) -> ListArray {
    let mut list_builder_generic = make_builder(
        &DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
        row_count,
    );
    let list_builder = list_builder_generic
        .as_any_mut()
        .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
        .unwrap_or_else(|| unreachable!("created above as a list builder"));
    for _ in 0..row_count {
        let str_builder = list_builder
            .values()
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap_or_else(|| unreachable!("created above as a string builder"));
        for key in primary_keys {
            str_builder.append_value(key);
        }
        list_builder.append(true);
    }
    list_builder.finish()
}
