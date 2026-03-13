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

//! Forwards writes to each relevant executor based on their assigned partitions.
//! This is used for partitioned tables that are written to via the coordinator.

use std::{collections::HashMap, sync::Arc};

use arrow::array::RecordBatch;
use arrow_flight::{
    FlightData, FlightDescriptor, PutResult, flight_service_server::FlightService,
    utils::flight_data_to_arrow_batch,
};
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow_schema::SchemaRef;
use datafusion::sql::{ResolvedTableReference, TableReference};
use datafusion_expr::{Expr, execution_props::ExecutionProps};
use futures::TryStreamExt as _;
use runtime_request_context::{AsyncMarker, RequestContext};
use snafu::{ResultExt, Snafu};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::{StreamExt as _, adapters::Peekable, wrappers::ReceiverStream};
use tonic::{Response, Streaming};

use crate::{
    cluster::executor_registry::ExecutorRegistry,
    datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA},
    flight::Service as FlightSvc,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No partition metadata found for table {table}"))]
    NoPartitionMetadata { table: String },

    #[snafu(display("Failed to resolve partition expressions: {source}"))]
    ResolvePartitions {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to decode schema from FlightData header: {source}"))]
    DecodeSchema { source: arrow_schema::ArrowError },

    #[snafu(display("Failed to create DFSchema: {source}"))]
    CreateDFSchema {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Empty partition expressions for executor {executor_id}"))]
    EmptyPartitionExprs { executor_id: String },

    #[snafu(display("Failed to create physical filter for executor {executor_id}: {source}"))]
    CreatePhysicalFilter {
        executor_id: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("No FlightSQL client for executor {executor_id}"))]
    NoClient { executor_id: String },

    #[snafu(display("Failed to decode FlightData into RecordBatch: {source}"))]
    DecodeBatch { source: arrow_schema::ArrowError },

    #[snafu(display("Stream error while reading FlightData: {source}"))]
    StreamRead { source: tonic::Status },

    #[snafu(display("Filter evaluation failed for executor {executor_id}: {source}"))]
    FilterEval {
        executor_id: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to filter record batch: {source}"))]
    FilterBatch { source: arrow_schema::ArrowError },

    #[snafu(display("Failed to send batch to executor {executor_id}"))]
    SendBatch { executor_id: String },

    #[snafu(display("Executor forwarding task panicked: {source}"))]
    JoinTask { source: tokio::task::JoinError },

    #[snafu(display("DoPut to executor failed: {source}"))]
    DoPut { source: tonic::Status },

    #[snafu(display("Executor DoPut acknowledgement failed: {source}"))]
    DoPutAck { source: tonic::Status },

    #[snafu(display("Failed to encode forwarded Flight stream: {message}"))]
    Encode { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        match &err {
            Error::NoPartitionMetadata { .. } | Error::NoClient { .. } => {
                tonic::Status::not_found(err.to_string())
            }
            _ => tonic::Status::internal(err.to_string()),
        }
    }
}

/// Tuple of executor ID and its corresponding physical filter expression.
type ExecutorId = String;
type ExecutorFilter = (ExecutorId, Arc<dyn datafusion::physical_plan::PhysicalExpr>);

/// Forwards writes to executors, splitting record batches by partition
/// expression so each executor only receives the rows it is responsible for.
pub(crate) async fn forward_partitioned_write(
    executor_registry: &ExecutorRegistry,
    ctx: Arc<datafusion::prelude::SessionContext>,
    io_runtime: tokio::runtime::Handle,
    path: &TableReference,
    first_message: FlightData,
    mut streaming_flight: Peekable<Streaming<FlightData>>,
) -> Result<Response<<FlightSvc as FlightService>::DoPutStream>> {
    let table_partitions = executor_registry
        .partition_manager()
        .get_cached_table_metadata(path)
        .ok_or_else(|| Error::NoPartitionMetadata {
            table: path.to_string(),
        })?;
    let partitions_by_executor = table_partitions
        .all_executor_partitions(ctx)
        .context(ResolvePartitionsSnafu)?;

    let schema = Arc::new(
        try_schema_from_flatbuffer_bytes(&first_message.data_header).context(DecodeSchemaSnafu)?,
    );

    let executor_filters = build_executor_filters(&partitions_by_executor, &schema)?;

    let tbl = path
        .clone()
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);

    let (senders, join_handles) = spawn_executor_forwarding_tasks(
        executor_registry,
        executor_filters
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>()
            .as_slice(),
        &schema,
        tbl,
        &io_runtime,
    )
    .await?;

    // Decode and route the first message.
    let dictionaries_by_id = Arc::new(HashMap::new());
    if let Ok(batch) =
        flight_data_to_arrow_batch(&first_message, Arc::clone(&schema), &dictionaries_by_id)
        && batch.num_rows() > 0
    {
        route_batch_to_executors_by_partition(&batch, &executor_filters, &senders).await?;
    }

    // Decode and route the rest of the stream.
    while let Some(result) = streaming_flight.next().await {
        let batch = flight_data_to_arrow_batch(
            &result.context(StreamReadSnafu)?,
            Arc::clone(&schema),
            &dictionaries_by_id,
        )
        .context(DecodeBatchSnafu)?;
        if batch.num_rows() > 0 {
            route_batch_to_executors_by_partition(&batch, &executor_filters, &senders).await?;
        }
    }

    // Signal completion by dropping senders, then await all forwarding tasks.
    drop(senders);
    for handle in join_handles {
        handle.await.context(JoinTaskSnafu)??;
    }

    Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
        PutResult::default(),
    )]))))
}

/// Builds a physical filter expression per executor by OR-ing its partition expressions.
fn build_executor_filters(
    partitions_by_executor: &HashMap<String, Vec<Expr>>,
    schema: &SchemaRef,
) -> Result<Vec<ExecutorFilter>> {
    let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())
        .context(CreateDFSchemaSnafu)?;

    let mut filters = Vec::with_capacity(partitions_by_executor.len());
    for (executor_id, exprs) in partitions_by_executor {
        let combined =
            exprs
                .iter()
                .cloned()
                .reduce(Expr::or)
                .ok_or_else(|| Error::EmptyPartitionExprs {
                    executor_id: executor_id.clone(),
                })?;
        let physical = datafusion::physical_expr::create_physical_expr(
            &combined,
            &df_schema,
            &ExecutionProps::new(),
        )
        .context(CreatePhysicalFilterSnafu {
            executor_id: executor_id.clone(),
        })?;
        filters.push((executor_id.clone(), physical));
    }
    Ok(filters)
}

/// Opens a channel, per-executor, and spawns a `forward_batches_to_executor` task for each.
async fn spawn_executor_forwarding_tasks(
    executor_registry: &ExecutorRegistry,
    executors: &[ExecutorId],
    schema: &SchemaRef,
    tbl: ResolvedTableReference,
    io_runtime: &tokio::runtime::Handle,
) -> Result<(
    HashMap<String, Sender<RecordBatch>>,
    Vec<tokio::task::JoinHandle<Result<()>>>,
)> {
    let clients = executor_registry.flight_sql_clients.read().await;
    let mut senders: HashMap<String, Sender<RecordBatch>> = HashMap::new();
    let mut join_handles = Vec::new();

    let auth_header = RequestContext::current(AsyncMarker::new().await)
        .authorization_header()
        .map(str::to_string);

    for executor_id in executors {
        let client = clients
            .get(executor_id)
            .cloned()
            .ok_or_else(|| Error::NoClient {
                executor_id: executor_id.clone(),
            })?;

        let (tx, rx) = mpsc::channel::<RecordBatch>(64);
        senders.insert(executor_id.clone(), tx);

        join_handles.push(tokio::spawn(forward_batches_to_executor(
            client,
            rx,
            Arc::clone(schema),
            tbl.clone(),
            auth_header.clone(),
            io_runtime.clone(),
        )));
    }

    Ok((senders, join_handles))
}

/// Filters `batch` by each executor's partition expression and sends matching rows.
async fn route_batch_to_executors_by_partition(
    batch: &RecordBatch,
    executor_filters: &[ExecutorFilter],
    senders: &HashMap<ExecutorId, Sender<RecordBatch>>,
) -> Result<()> {
    for (executor_id, filter_expr) in executor_filters {
        let arr = filter_expr
            .evaluate(batch)
            .context(FilterEvalSnafu {
                executor_id: executor_id.clone(),
            })?
            .into_array(batch.num_rows())
            .context(FilterEvalSnafu {
                executor_id: executor_id.clone(),
            })?;
        let mask = arr
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| Error::FilterEval {
                executor_id: executor_id.clone(),
                source: datafusion::error::DataFusionError::Internal(
                    "Filter did not produce boolean array".to_string(),
                ),
            })?;

        let filtered =
            arrow::compute::filter_record_batch(batch, mask).context(FilterBatchSnafu)?;

        if filtered.num_rows() > 0 {
            if let Some(tx) = senders.get(executor_id) {
                tx.send(filtered).await.map_err(|_| Error::SendBatch {
                    executor_id: executor_id.clone(),
                })?;
            }
        }
    }
    Ok(())
}

/// Encodes `RecordBatch`es from `rx` as `FlightData` and sends them via `DoPut`
/// to a specific executor.
async fn forward_batches_to_executor(
    client: data_components::flightsql::FlightSqlClient,
    rx: mpsc::Receiver<RecordBatch>,
    schema: SchemaRef,
    tbl: ResolvedTableReference,
    auth_header: Option<String>,
    io_runtime: tokio::runtime::Handle,
) -> Result<()> {
    let (tx, flight_rx) = mpsc::channel::<arrow_flight::FlightData>(64);
    let (encode_result_tx, encode_result_rx) =
        tokio::sync::oneshot::channel::<std::result::Result<(), String>>();

    let encoder_schema = Arc::clone(&schema);
    io_runtime.spawn(async move {
        let batch_stream = ReceiverStream::new(rx).map(
            |b| -> std::result::Result<RecordBatch, arrow_flight::error::FlightError> { Ok(b) },
        );
        let flight_data_encoder = arrow_flight::encode::FlightDataEncoderBuilder::new()
            .with_schema(encoder_schema)
            .build(batch_stream);

        let mut flight_data_encoder = Box::pin(flight_data_encoder);
        let mut is_first = true;
        let fd: FlightDescriptor = arrow_flight::FlightDescriptor::new_path(vec![
            tbl.catalog.to_string(),
            tbl.schema.to_string(),
            tbl.table.to_string(),
        ]);
        loop {
            match flight_data_encoder.next().await {
                Some(Ok(mut fdata)) => {
                    if is_first {
                        fdata.flight_descriptor = Some(fd.clone());
                        is_first = false;
                    }
                    if tx.send(fdata).await.is_err() {
                        let _ = encode_result_tx.send(Ok(()));
                        break;
                    }
                }
                Some(Err(e)) => {
                    let _ = encode_result_tx.send(Err(e.to_string()));
                    break;
                }
                None => {
                    let _ = encode_result_tx.send(Ok(()));
                    break;
                }
            }
        }
    });

    let mut request = tonic::Request::new(ReceiverStream::new(flight_rx));
    if let Some(auth_value) = auth_header
        && let Ok(val) = auth_value.parse()
    {
        request.metadata_mut().insert("authorization", val);
    }

    let mut inner_client = client.into_inner();
    let response = inner_client.do_put(request).await.context(DoPutSnafu)?;

    response
        .into_inner()
        .try_collect::<Vec<_>>()
        .await
        .context(DoPutAckSnafu)?;

    match encode_result_rx.await {
        Ok(Ok(())) | Err(_) => Ok(()),
        Ok(Err(message)) => return Err(Error::Encode { message }),
    }
}
