/*
Copyright 2026 The Spice.ai OSS Authors

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

//! `DoExchange` handler serving CDC-style subscriptions.
//!
//! On subscription this streams an initial snapshot of the requested table
//! (converted into "change" batches with an `op = "r"` operation), then
//! interleaves live updates supplied by a [`ChangeSource`]. Ported from the
//! Spice runtime's `do_exchange` implementation, decoupled from runtime
//! infrastructure: `DataUpdate`/`UpdateType` come from `runtime-query-engine`,
//! the initial snapshot is read via the [`QueryEngine`]'s `SessionContext`, and
//! the live feed is a generic [`BoxStream`] rather than a broadcast receiver.

use std::{collections::VecDeque, sync::Arc};

use arrow::array::{ListArray, StringArray, StructArray};
use arrow::array::{ListBuilder, RecordBatch, StringBuilder, new_null_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_flight::{
    FlightData, SchemaAsIpc, flight_service_server::FlightService,
};
use arrow_ipc::writer::{self, CompressionContext, DictionaryTracker, IpcDataGenerator};
use async_stream::try_stream;
use datafusion::common::{Constraint, Constraints};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::sql::TableReference;
use futures::StreamExt;
use futures::stream::BoxStream;
use runtime_query_engine::query_engine::{DataUpdate, QueryEngine, UpdateType};
use tonic::{Request, Response, Status, Streaming};

use crate::{ChangeSource, FlightSqlService};

const MAX_PENDING_INITIAL_SNAPSHOT_UPDATES: usize = 1_024;
const MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_BATCHES: usize = 128;
const MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_ROWS: usize = 1_000_000;
const MAX_PENDING_INITIAL_SNAPSHOT_UPDATE_BYTES: usize = 128 * 1024 * 1024;

/// The Arrow schema that represents a change event.
///
/// Inlined from `data_components::cdc::changes_schema` to avoid depending on
/// the heavy `data_components` crate.
fn changes_schema(table_schema: &Schema) -> Schema {
    Schema::new(vec![
        Field::new("op", DataType::Utf8, false),
        Field::new(
            "primary_keys",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
        ),
        Field::new(
            "data",
            DataType::Struct(table_schema.fields().clone()),
            true,
        ),
    ])
}

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
    engine: Arc<dyn QueryEngine>,
    change_source: Option<Arc<dyn ChangeSource>>,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<<FlightSqlService as FlightService>::DoExchangeStream>, Status> {
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

    let Some(table_provider) = engine.get_table(&data_path).await else {
        return Err(Status::invalid_argument(format!(
            r#"Unknown dataset: "{data_path}""#,
        )));
    };

    let Some(change_source) = change_source else {
        return Err(Status::unimplemented(
            "do_exchange requires a configured ChangeSource",
        ));
    };

    let update_stream = change_source.subscribe(&data_path).await;

    Ok(Response::new(do_exchange_response_stream(
        update_stream,
        table_provider,
        engine,
        data_path,
    )))
}

fn do_exchange_response_stream(
    mut update_stream: BoxStream<'static, Result<DataUpdate, Status>>,
    table_provider_stream: Arc<dyn TableProvider>,
    engine_stream: Arc<dyn QueryEngine>,
    data_path_stream: TableReference,
) -> BoxStream<'static, Result<FlightData, Status>> {
    // The stream interleaves a fallible initial snapshot, live updates, and IPC encoding without materializing the response.
    let response_stream = try_stream! {
        let mut encoder = ChangeFlightEncoder::default();
        enum InitialSnapshotEvent {
            DataUpdate(Option<Result<DataUpdate, Status>>),
            SnapshotBatch(Option<Result<RecordBatch, DataFusionError>>),
        }

        let mut initial_snapshot_stream = initial_snapshot_stream(
            &engine_stream,
            Arc::clone(&table_provider_stream),
        )
        .await?;
        let snapshot_schema = initial_snapshot_stream.schema();

        let truncate_batch = truncate_change_batch(&snapshot_schema)?;
        let flights = encoder.encode(&truncate_batch)?;
        for flight in flights {
            yield flight;
        }

        let mut pending_updates = PendingInitialSnapshotUpdates::default();

        loop {
            let event = tokio::select! {
                update = update_stream.next() => InitialSnapshotEvent::DataUpdate(update),
                batch = initial_snapshot_stream.next() => InitialSnapshotEvent::SnapshotBatch(batch),
            };

            match event {
                InitialSnapshotEvent::DataUpdate(Some(Ok(data_update))) => {
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
                InitialSnapshotEvent::DataUpdate(Some(Err(status))) => {
                    Err(status)?;
                }
                InitialSnapshotEvent::DataUpdate(None) => {
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
                    let flights = encoder.encode(&change_batch)?;
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
                let flights = encoder.encode(&truncate_batch)?;
                for flight in flights {
                    yield flight;
                }
            }

            for batch in &data_update.data {
                let change_batch = record_batch_to_change_batch(&table_provider_stream, batch)?;
                let flights = encoder.encode(&change_batch)?;
                for flight in flights {
                    yield flight;
                }
            }
        }

        loop {
            let data_update = match update_stream.next().await {
                Some(Ok(data_update)) => data_update,
                Some(Err(status)) => {
                    Err(status)?
                }
                None => {
                    Err(Status::data_loss(format!(
                        "DoExchange subscriber update stream closed for dataset {data_path_stream}; resubscribe and reconcile state"
                    )))?
                }
            };

            if data_update.update_type == UpdateType::Overwrite {
                let truncate_batch = truncate_change_batch(&data_update.schema)?;
                let flights = encoder.encode(&truncate_batch)?;
                for flight in flights {
                    yield flight;
                }
            }

            for batch in &data_update.data {
                let change_batch = record_batch_to_change_batch(&table_provider_stream, batch)?;
                let flights = encoder.encode(&change_batch)?;
                for flight in flights {
                    yield flight;
                }
            }
        }
    };

    response_stream.boxed()
}

async fn initial_snapshot_stream(
    engine: &Arc<dyn QueryEngine>,
    table_provider: Arc<dyn TableProvider>,
) -> Result<SendableRecordBatchStream, Status> {
    let df = engine
        .session_context()
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
