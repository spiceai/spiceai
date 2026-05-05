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

use arrow::array::{ListArray, StringArray, StructArray};
use arrow::array::{ListBuilder, RecordBatch, StringBuilder, new_null_array};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow_flight::{FlightData, SchemaAsIpc, flight_service_server::FlightService};
use arrow_ipc::writer::{self, CompressionContext, DictionaryTracker, IpcDataGenerator};
use async_stream::try_stream;
use data_components::cdc::changes_schema;
use datafusion::common::{Constraint, Constraints};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use futures::StreamExt;
use tokio::sync::broadcast::error::RecvError;
use tonic::{Request, Response, Status, Streaming};

use crate::datafusion::request_context_extension::get_current_datafusion;
use crate::dataupdate::{DataUpdate, DataUpdateReceiver, UpdateType};
use runtime_request_context::{AsyncMarker, RequestContext};

use super::{Service, metrics};

const MAX_PENDING_INITIAL_SNAPSHOT_UPDATES: usize = 1_024;
const MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_BATCHES: usize = 128;
const MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_ROWS: usize = 1_000_000;
const MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_BYTES: usize = 128 * 1024 * 1024;

#[derive(Default)]
struct PendingInitialSnapshotUpdates {
    updates: VecDeque<DataUpdate>,
    batches: usize,
    rows: usize,
    bytes: usize,
}

impl PendingInitialSnapshotUpdates {
    fn push_back(&mut self, update: DataUpdate) -> bool {
        let (update_batches, update_rows, update_bytes) = update_stats(&update);
        let next_updates = self.updates.len().saturating_add(1);
        let next_batches = self.batches.saturating_add(update_batches);
        let next_rows = self.rows.saturating_add(update_rows);
        let next_bytes = self.bytes.saturating_add(update_bytes);

        if next_updates > MAX_PENDING_INITIAL_SNAPSHOT_UPDATES
            || next_batches > MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_BATCHES
            || next_rows > MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_ROWS
            || next_bytes > MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_BYTES
        {
            self.clear();
            return false;
        }

        self.batches = next_batches;
        self.rows = next_rows;
        self.bytes = next_bytes;
        self.updates.push_back(update);
        true
    }

    fn pop_front(&mut self) -> Option<DataUpdate> {
        let update = self.updates.pop_front()?;
        let (update_batches, update_rows, update_bytes) = update_stats(&update);
        self.batches = self.batches.saturating_sub(update_batches);
        self.rows = self.rows.saturating_sub(update_rows);
        self.bytes = self.bytes.saturating_sub(update_bytes);
        Some(update)
    }

    fn clear(&mut self) {
        self.updates.clear();
        self.batches = 0;
        self.rows = 0;
        self.bytes = 0;
    }
}

fn update_stats(update: &DataUpdate) -> (usize, usize, usize) {
    update.data.iter().fold(
        (0usize, 0usize, 0usize),
        |(batch_count, row_count, byte_count), batch| {
            (
                batch_count.saturating_add(1),
                row_count.saturating_add(batch.num_rows()),
                byte_count.saturating_add(batch.get_array_memory_size()),
            )
        },
    )
}

struct ChangeFlightEncoder {
    encoder: IpcDataGenerator,
    tracker: DictionaryTracker,
    compression_context: CompressionContext,
    write_options: writer::IpcWriteOptions,
    schema: Option<SchemaRef>,
}

impl Default for ChangeFlightEncoder {
    fn default() -> Self {
        Self {
            encoder: IpcDataGenerator::default(),
            tracker: DictionaryTracker::new(false),
            compression_context: CompressionContext::default(),
            write_options: writer::IpcWriteOptions::default(),
            schema: None,
        }
    }
}

impl ChangeFlightEncoder {
    fn encode(&mut self, record_batch: &RecordBatch) -> Result<Vec<FlightData>, Status> {
        let mut flights = Vec::new();
        let schema_changed = self
            .schema
            .as_ref()
            .is_none_or(|schema| schema.as_ref() != record_batch.schema().as_ref());

        if schema_changed {
            flights.push(FlightData::from(SchemaAsIpc::new(
                record_batch.schema().as_ref(),
                &self.write_options,
            )));
            self.schema = Some(record_batch.schema());
            self.tracker = DictionaryTracker::new(false);
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

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);
    let data_path = datafusion
        .normalize_table_reference(TableReference::parse_str(&flight_descriptor.path.join(".")));

    let Some(table_provider) = datafusion.get_table(&data_path).await else {
        return Err(Status::invalid_argument(format!(
            r#"Unknown dataset: "{data_path}""#,
        )));
    };

    let rx = flight_svc
        .data_update_broadcaster
        .subscribe(&data_path)
        .await;

    Ok(Response::new(do_exchange_response_stream(
        rx,
        table_provider,
        datafusion,
        data_path,
    )))
}

fn do_exchange_response_stream(
    mut rx: DataUpdateReceiver,
    table_provider_stream: Arc<dyn TableProvider>,
    datafusion_stream: Arc<crate::datafusion::DataFusion>,
    data_path_stream: TableReference,
) -> futures::stream::BoxStream<'static, Result<FlightData, Status>> {
    // The stream interleaves a fallible initial snapshot, broadcast updates, and IPC encoding without materializing the response.
    let response_stream = try_stream! {
        let mut encoder = ChangeFlightEncoder::default();
        enum InitialSnapshotEvent {
            DataUpdate(Result<DataUpdate, RecvError>),
            SnapshotBatch(Option<Result<RecordBatch, DataFusionError>>),
        }

        let mut initial_snapshot_stream = initial_snapshot_stream(
            &datafusion_stream,
            Arc::clone(&table_provider_stream),
        )
        .await?;
        let snapshot_schema = initial_snapshot_stream.schema();

        let truncate_batch = truncate_change_batch(&snapshot_schema)?;
        let flights = encode_and_count(&mut encoder, &truncate_batch)?;
        for flight in flights {
            yield flight;
        }

        let mut pending_updates = PendingInitialSnapshotUpdates::default();

        loop {
            let event = tokio::select! {
                update = rx.recv() => InitialSnapshotEvent::DataUpdate(update),
                batch = initial_snapshot_stream.next() => InitialSnapshotEvent::SnapshotBatch(batch),
            };

            match event {
                InitialSnapshotEvent::DataUpdate(Ok(data_update)) => {
                    if !pending_updates.push_back(data_update) {
                        tracing::warn!(
                            dataset = %data_path_stream,
                            max_pending_updates = MAX_PENDING_INITIAL_SNAPSHOT_UPDATES,
                            max_pending_batches = MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_BATCHES,
                            max_pending_rows = MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_ROWS,
                            max_pending_bytes = MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_BYTES,
                            "DoExchange subscriber received too much buffered update data while streaming initial snapshot"
                        );
                        Err(Status::data_loss(format!(
                            "DoExchange subscriber missed data updates while receiving the initial snapshot for dataset {data_path_stream}; resubscribe and reconcile state"
                        )))?;
                    }
                }
                InitialSnapshotEvent::DataUpdate(Err(RecvError::Lagged(skipped_messages))) => {
                    pending_updates.clear();
                    tracing::warn!(
                        dataset = %data_path_stream,
                        skipped_messages,
                        "DoExchange subscriber lagged while streaming initial snapshot"
                    );
                    Err(Status::data_loss(format!(
                        "DoExchange subscriber missed {skipped_messages} update(s) while receiving the initial snapshot for dataset {data_path_stream}; resubscribe and reconcile state"
                    )))?;
                }
                InitialSnapshotEvent::DataUpdate(Err(RecvError::Closed)) => {
                    Err(Status::data_loss(format!(
                        "DoExchange subscriber update stream closed while receiving the initial snapshot for dataset {data_path_stream}; resubscribe and reconcile state"
                    )))?;
                }
                InitialSnapshotEvent::SnapshotBatch(Some(batch)) => {
                    let batch = batch.map_err(|source| Status::internal(format!(
                        "Unable to stream initial snapshot: {source}"
                    )))?;
                    let change_batch = record_batch_to_change_batch(
                        &table_provider_stream,
                        &batch,
                    )?;
                    let flights = encode_and_count(&mut encoder, &change_batch)?;
                    for flight in flights {
                        yield flight;
                    }
                }
                InitialSnapshotEvent::SnapshotBatch(None) => break,
            }
        }

        while let Some(data_update) = pending_updates.pop_front() {
            if data_update.update_type == UpdateType::Overwrite {
                let truncate_batch = truncate_change_batch(&data_update.schema)?;
                let flights = encode_and_count(&mut encoder, &truncate_batch)?;
                for flight in flights {
                    yield flight;
                }
            }

            for batch in &data_update.data {
                let change_batch = record_batch_to_change_batch(&table_provider_stream, batch)?;
                let flights = encode_and_count(&mut encoder, &change_batch)?;
                for flight in flights {
                    yield flight;
                }
            }
        }

        loop {
            let data_update = match rx.recv().await {
                Ok(data_update) => data_update,
                Err(RecvError::Lagged(skipped_messages)) => {
                    Err(Status::data_loss(format!(
                        "DoExchange subscriber fell behind and missed {skipped_messages} update(s) for dataset {data_path_stream}; resubscribe and reconcile state"
                    )))?
                }
                Err(RecvError::Closed) => {
                    Err(Status::data_loss(format!(
                        "DoExchange subscriber update stream closed for dataset {data_path_stream}; resubscribe and reconcile state"
                    )))?
                }
            };

            if data_update.update_type == UpdateType::Overwrite {
                let truncate_batch = truncate_change_batch(&data_update.schema)?;
                let flights = encode_and_count(&mut encoder, &truncate_batch)?;
                for flight in flights {
                    yield flight;
                }
            }

            for batch in &data_update.data {
                let change_batch = record_batch_to_change_batch(&table_provider_stream, batch)?;
                let flights = encode_and_count(&mut encoder, &change_batch)?;
                for flight in flights {
                    yield flight;
                }
            }
        }
    };

    response_stream.boxed()
}

fn encode_and_count(
    encoder: &mut ChangeFlightEncoder,
    record_batch: &RecordBatch,
) -> Result<Vec<FlightData>, Status> {
    let flights = encoder.encode(record_batch)?;
    metrics::DO_EXCHANGE_DATA_UPDATES_SENT.add(flights.len() as u64, &[]);
    Ok(flights)
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
    let mut list_builder = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Utf8,
        false,
    )));
    for _ in 0..row_count {
        for key in primary_keys {
            list_builder.values().append_value(key);
        }
        list_builder.append(true);
    }
    list_builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BooleanArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn test_batch(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(test_schema(), vec![Arc::new(Int32Array::from(values))])
            .expect("test batch should be valid")
    }

    fn test_data_update(values: Vec<i32>, update_type: UpdateType) -> DataUpdate {
        let batch = test_batch(values);
        DataUpdate {
            schema: batch.schema(),
            data: vec![batch],
            update_type,
        }
    }

    #[test]
    fn test_get_primary_keys_array_repeats_keys_for_each_row() {
        let primary_keys = get_primary_keys_array(&["tenant", "id"], 2);

        assert_eq!(primary_keys.len(), 2);
        assert_eq!(primary_keys.value_type(), DataType::Utf8);

        for row_index in 0..primary_keys.len() {
            let values = primary_keys.value(row_index);
            let values = values
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("primary key list values should be strings");
            assert_eq!(values.len(), 2);
            assert_eq!(values.value(0), "tenant");
            assert_eq!(values.value(1), "id");
        }
    }

    #[test]
    fn test_truncate_change_batch_emits_truncate_operation() {
        let batch = truncate_change_batch(&test_schema()).expect("truncate batch should be valid");
        let op_column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("op column should be a string array");

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(op_column.value(0), "t");
    }

    #[test]
    fn test_pending_initial_snapshot_updates_preserves_order() {
        let mut pending_updates = PendingInitialSnapshotUpdates::default();
        assert!(pending_updates.push_back(test_data_update(vec![1], UpdateType::Append)));
        assert!(pending_updates.push_back(test_data_update(vec![2], UpdateType::Overwrite)));

        let first_update = pending_updates
            .pop_front()
            .expect("first pending update should exist");
        let second_update = pending_updates
            .pop_front()
            .expect("second pending update should exist");

        assert_eq!(first_update.update_type, UpdateType::Append);
        assert_eq!(second_update.update_type, UpdateType::Overwrite);
        assert!(pending_updates.pop_front().is_none());
    }

    #[test]
    fn test_pending_initial_snapshot_updates_rejects_too_many_batches() {
        let batch = test_batch(vec![]);
        let update = DataUpdate {
            schema: batch.schema(),
            data: vec![batch; MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_BATCHES + 1],
            update_type: UpdateType::Append,
        };
        let mut pending_updates = PendingInitialSnapshotUpdates::default();

        assert!(!pending_updates.push_back(update));
        assert!(pending_updates.pop_front().is_none());
    }

    #[test]
    fn test_pending_initial_snapshot_updates_rejects_too_many_rows() {
        let batch = RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int32Array::from(vec![
                0;
                MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_ROWS
                    + 1
            ]))],
        )
        .expect("large test batch should be valid");
        let update = DataUpdate {
            schema: batch.schema(),
            data: vec![batch],
            update_type: UpdateType::Append,
        };
        let mut pending_updates = PendingInitialSnapshotUpdates::default();

        assert!(!pending_updates.push_back(update));
        assert!(pending_updates.pop_front().is_none());
    }

    #[test]
    fn test_change_flight_encoder_resends_schema_when_schema_changes() {
        let mut encoder = ChangeFlightEncoder::default();
        let first_batch = test_batch(vec![1]);
        let second_batch = test_batch(vec![2]);
        let changed_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("active", DataType::Boolean, false),
        ]));
        let changed_batch = RecordBatch::try_new(
            changed_schema,
            vec![
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        )
        .expect("changed schema batch should be valid");

        let first_flights = encoder
            .encode(&first_batch)
            .expect("first batch should encode");
        let second_flights = encoder
            .encode(&second_batch)
            .expect("same-schema batch should encode");
        let changed_flights = encoder
            .encode(&changed_batch)
            .expect("changed-schema batch should encode");

        assert_eq!(first_flights.len(), 2);
        assert_eq!(second_flights.len(), 1);
        assert_eq!(changed_flights.len(), 2);
    }

    #[tokio::test]
    async fn test_do_exchange_response_stream_sends_snapshot_before_buffered_update() {
        use arrow_flight::{
            decode::{DecodedPayload, FlightDataDecoder},
            error::FlightError,
        };
        use datafusion::datasource::MemTable;
        use futures::TryStreamExt;
        use std::time::Duration;

        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let table_provider = Arc::new(
            MemTable::try_new(test_schema(), vec![vec![test_batch(vec![1])]])
                .expect("mem table should be created"),
        );
        let data_path = TableReference::bare("orders");
        let broadcaster = crate::dataupdate::DataUpdateBroadcaster::new();
        let rx = broadcaster.subscribe(&data_path).await;
        let response_stream =
            do_exchange_response_stream(rx, table_provider, datafusion, data_path.clone())
                .map_err(|status| FlightError::Tonic(Box::new(status)));

        broadcaster
            .publish(&data_path, test_data_update(vec![2], UpdateType::Append))
            .await;

        let batches = tokio::time::timeout(Duration::from_secs(5), async {
            let mut decoder = FlightDataDecoder::new(response_stream);
            let mut batches = Vec::new();

            while batches.len() < 3 {
                let message = decoder
                    .try_next()
                    .await
                    .expect("flight data should decode")
                    .expect("stream should yield enough batches");
                if let DecodedPayload::RecordBatch(batch) = message.payload {
                    batches.push(batch);
                }
            }

            batches
        })
        .await
        .expect("snapshot and buffered update should be emitted");

        assert_eq!(change_op(&batches[0]), "t");
        assert_eq!(change_op(&batches[1]), "r");
        assert_eq!(change_data_id(&batches[1]), 1);
        assert_eq!(change_op(&batches[2]), "r");
        assert_eq!(change_data_id(&batches[2]), 2);
    }

    fn change_op(batch: &RecordBatch) -> &str {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("operation column should be strings")
            .value(0)
    }

    fn change_data_id(batch: &RecordBatch) -> i32 {
        let data = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("data column should be a struct array");
        data.column_by_name("id")
            .expect("id field should exist")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id field should be int32")
            .value(0)
    }
}
