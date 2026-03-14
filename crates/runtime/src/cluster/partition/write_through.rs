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
use datafusion_proto::bytes::Serializeable;
use futures::TryStreamExt as _;
use runtime_request_context::{AsyncMarker, RequestContext};
use snafu::{ResultExt, Snafu};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::{StreamExt as _, adapters::Peekable, wrappers::ReceiverStream};
use tonic::{Response, Streaming};

use crate::{
    cluster::{
        PartitionManager, executor_registry::ExecutorRegistry, partition::metadata::PartitionValue,
    },
    datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA},
    flight::Service as FlightSvc,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create partition metadata for table {table}"))]
    CreateMetadata {
        table: String,
        source: Box<super::manager::Error>,
    },

    #[snafu(display("Cannot find partition metadata for table {table}"))]
    FindMetadata { table: String },

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

    #[snafu(display("No executors available for new partition assignment"))]
    NoExecutorsAvailable,

    #[snafu(display("Failed to parse partition expression: {source}"))]
    ParsePartitionExpr {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to partition batch: {source}"))]
    PartitionBatch {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to serialize partition expression: {source}"))]
    SerializeExpr {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to persist partition assignment: {source}"))]
    PersistAssignment { source: Box<super::manager::Error> },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for tonic::Status {
    fn from(err: Error) -> Self {
        match &err {
            Error::NoClient { .. } => tonic::Status::not_found(err.to_string()),
            _ => tonic::Status::internal(err.to_string()),
        }
    }
}

/// Tuple of executor ID and its corresponding physical filter expression.
type ExecutorId = String;
type ExecutorFilter = (ExecutorId, Arc<dyn datafusion::physical_plan::PhysicalExpr>);

/// Forwards writes to executors, splitting record batches by partition
/// expression so each executor only receives the rows it is responsible for.
///
/// Rows with partition values not yet assigned to any executor are assigned
/// to the least-loaded executor. The assignment is persisted to the partition
/// metadata store before the rows are forwarded.
pub(crate) async fn forward_federated_partitioned_write(
    executor_registry: &ExecutorRegistry,
    ctx: Arc<datafusion::prelude::SessionContext>,
    io_runtime: tokio::runtime::Handle,
    path: &TableReference,
    first_message: FlightData,
    mut streaming_flight: Peekable<Streaming<FlightData>>,
    partition_by: &[Expr],
) -> Result<Response<<FlightSvc as FlightService>::DoPutStream>> {
    let partition_manager = executor_registry.federated_partition_manager();
    let table_partitions = match partition_manager.get_table_metadata(path).await {
        Ok(Some(metadata)) => metadata,
        Ok(None) => {
            partition_manager
                .initialize_blank_metadata(path)
                .await
                .map_err(|source| Error::CreateMetadata {
                    table: path.to_string(),
                    source: Box::new(source),
                })?;
            // Re-fetch from the object store rather than relying on the local
            // cache: another scheduler may have raced and created the metadata
            // first (AlreadyExists), leaving our cache empty.
            partition_manager
                .get_table_metadata(path)
                .await
                .map_err(|source| Error::CreateMetadata {
                    table: path.to_string(),
                    source: Box::new(source),
                })?
                .ok_or_else(|| Error::FindMetadata {
                    table: path.to_string(),
                })?
        }
        Err(e) => {
            return Err(Error::CreateMetadata {
                table: path.to_string(),
                source: Box::new(e),
            });
        }
    };
    let partitions_by_executor = table_partitions
        .all_executor_partitions(&ctx)
        .context(ResolvePartitionsSnafu)?;

    let schema = Arc::new(
        try_schema_from_flatbuffer_bytes(&first_message.data_header).context(DecodeSchemaSnafu)?,
    );

    let executor_filters = build_executor_filters(&partitions_by_executor, &schema)?;

    // Parse partition_by expressions into physical exprs for splitting unmatched rows.
    let partition_phys_exprs = build_partition_physical_exprs(partition_by, &schema)?;

    // Serialize the partition expressions for use as PartitionValue keys.
    let partition_expr_keys: Vec<String> = partition_phys_exprs
        .iter()
        .map(|(expr, _)| {
            expr.to_bytes()
                .map(|b| String::from_utf8_lossy(&b).to_string())
                .context(SerializeExprSnafu)
        })
        .collect::<Result<Vec<_>>>()?;

    let tbl = path
        .clone()
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);

    // Spawn forwarding tasks for ALL connected executors so we can route
    // new partitions to any executor, not just those with existing assignments.
    let all_executor_ids: Vec<ExecutorId> = {
        let clients = executor_registry.flight_sql_clients.read().await;
        clients.keys().cloned().collect()
    };

    let (senders, join_handles) = spawn_executor_forwarding_tasks(
        executor_registry,
        &all_executor_ids,
        &schema,
        tbl,
        &io_runtime,
    )
    .await?;

    let partition_manager = executor_registry.federated_partition_manager();

    // Decode and route the first message.
    let dictionaries_by_id = Arc::new(HashMap::new());
    if let Ok(batch) =
        flight_data_to_arrow_batch(&first_message, Arc::clone(&schema), &dictionaries_by_id)
        && batch.num_rows() > 0
    {
        route_batch_and_assign_unseen(
            &batch,
            &executor_filters,
            &senders,
            &partition_phys_exprs,
            &partition_expr_keys,
            &partitions_by_executor,
            &partition_manager,
            path,
        )
        .await?;
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
            route_batch_and_assign_unseen(
                &batch,
                &executor_filters,
                &senders,
                &partition_phys_exprs,
                &partition_expr_keys,
                &partitions_by_executor,
                &partition_manager,
                path,
            )
            .await?;
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

/// Routes a [`RecordBatch`] of data to one or more executors' [`Sender<RecordBatch>`], based on the executor filter
/// predicates, then assigns new partition predicate values to the least-loaded executor and forwards those rows accordingly.
#[expect(clippy::too_many_arguments)]
async fn route_batch_and_assign_unseen(
    batch: &RecordBatch,
    executor_filters: &[ExecutorFilter],
    senders: &HashMap<ExecutorId, Sender<RecordBatch>>,
    partition_phys_exprs: &[(Expr, Arc<dyn datafusion::physical_plan::PhysicalExpr>)],
    partition_expr_keys: &[String],
    partitions_by_executor: &HashMap<String, Vec<Expr>>,
    partition_manager: &Arc<PartitionManager>,
    path: &TableReference,
) -> Result<()> {
    // Route matched rows to known executors.
    let unmatched = route_matched_and_collect_unmatched(batch, executor_filters, senders).await?;

    if unmatched.num_rows() == 0 {
        return Ok(());
    }

    // Split unmatched rows by partition value and assign each to an executor.
    let physical_exprs: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>> =
        partition_phys_exprs
            .iter()
            .map(|(_, p)| Arc::clone(p))
            .collect();

    let partitioned =
        runtime_table_partition::insert::partition_batch_composite(&unmatched, &physical_exprs)
            .context(PartitionBatchSnafu)?;

    for (_key, (scalar_values, sub_batch)) in partitioned {
        if sub_batch.num_rows() == 0 {
            continue;
        }

        // Build the PartitionValue (HashMap<serialized_expr_key, serialized_lit_value>).
        let partition_value: PartitionValue = partition_expr_keys
            .iter()
            .zip(scalar_values.iter())
            .map(|(expr_key, scalar)| {
                let lit_expr = Expr::Literal(scalar.clone(), None);
                let lit_bytes = lit_expr.to_bytes().context(SerializeExprSnafu)?;
                Ok((
                    expr_key.clone(),
                    String::from_utf8_lossy(&lit_bytes).to_string(),
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        // Select least-loaded executor.
        let executor_id = select_least_loaded_executor(partitions_by_executor, senders)?;

        // Persist the assignment before forwarding.
        partition_manager
            .add_and_assign_partition(path, &partition_value, &executor_id)
            .await
            .map_err(|source| Error::PersistAssignment {
                source: Box::new(source),
            })?;

        tracing::debug!(
            table = %path,
            executor = %executor_id,
            "Assigned new partition and forwarding rows"
        );

        // Forward the rows.
        if let Some(tx) = senders.get(&executor_id) {
            tx.send(sub_batch).await.map_err(|_| Error::SendBatch {
                executor_id: executor_id.clone(),
            })?;
        }
    }

    Ok(())
}

/// Routes matched rows to known executors and returns the unmatched rows.
async fn route_matched_and_collect_unmatched(
    batch: &RecordBatch,
    executor_filters: &[ExecutorFilter],
    senders: &HashMap<ExecutorId, Sender<RecordBatch>>,
) -> Result<RecordBatch> {
    // Track which rows are matched by any executor.
    let mut any_matched = arrow::array::BooleanArray::from(vec![false; batch.num_rows()]);

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
        let raw_mask = arr
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| Error::FilterEval {
                executor_id: executor_id.clone(),
                source: datafusion::error::DataFusionError::Internal(
                    "Filter did not produce boolean array".to_string(),
                ),
            })?;

        // Coalesce NULLs to false so that rows where the partition predicate evaluates
        // to NULL (e.g. NULL partition column) are treated as unmatched rather than dropped.
        let mask = arrow::compute::prep_null_mask_filter(raw_mask);

        // Only mark rows as matched and forward if this executor has an active sender.
        // Otherwise rows would be marked matched but never forwarded, silently dropping data.
        if let Some(tx) = senders.get(executor_id) {
            let filtered =
                arrow::compute::filter_record_batch(batch, &mask).context(FilterBatchSnafu)?;

            if filtered.num_rows() > 0 {
                tx.send(filtered).await.map_err(|_| Error::SendBatch {
                    executor_id: executor_id.clone(),
                })?;
            }

            // OR into the cumulative matched mask only when we actually forwarded.
            any_matched = arrow::compute::or(&any_matched, &mask).context(FilterBatchSnafu)?;
        }
    }

    // Negate to get unmatched rows.
    let unmatched_mask = arrow::compute::not(&any_matched).context(FilterBatchSnafu)?;
    arrow::compute::filter_record_batch(batch, &unmatched_mask).context(FilterBatchSnafu)
}

/// Converts partition-by logical expressions into logical + physical expression pairs.
fn build_partition_physical_exprs(
    partition_by: &[Expr],
    schema: &SchemaRef,
) -> Result<Vec<(Expr, Arc<dyn datafusion::physical_plan::PhysicalExpr>)>> {
    let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())
        .context(CreateDFSchemaSnafu)?;

    partition_by
        .iter()
        .map(|e| {
            let physical = datafusion::physical_expr::create_physical_expr(
                e,
                &df_schema,
                &ExecutionProps::new(),
            )
            .context(ParsePartitionExprSnafu)?;
            Ok((e.clone(), physical))
        })
        .collect()
}

/// Selects the executor with the fewest currently assigned partitions.
fn select_least_loaded_executor(
    partitions_by_executor: &HashMap<String, Vec<Expr>>,
    senders: &HashMap<ExecutorId, Sender<RecordBatch>>,
) -> Result<ExecutorId> {
    senders
        .keys()
        .min_by_key(|id| partitions_by_executor.get(id.as_str()).map_or(0, Vec::len))
        .cloned()
        .ok_or(Error::NoExecutorsAvailable)
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
    // Resolve auth header and clone clients before holding the lock across spawns.
    let auth_header = RequestContext::current(AsyncMarker::new().await)
        .authorization_header()
        .map(str::to_string);

    let executor_clients: Vec<(ExecutorId, data_components::flightsql::FlightSqlClient)> = {
        let clients = executor_registry.flight_sql_clients.read().await;
        executors
            .iter()
            .map(|id| {
                let client = clients.get(id).cloned().ok_or_else(|| Error::NoClient {
                    executor_id: id.clone(),
                })?;
                Ok((id.clone(), client))
            })
            .collect::<Result<Vec<_>>>()?
    };

    let mut senders: HashMap<String, Sender<RecordBatch>> = HashMap::new();
    let mut join_handles = Vec::new();

    for (executor_id, client) in executor_clients {
        let (tx, rx) = mpsc::channel::<RecordBatch>(64);
        senders.insert(executor_id, tx);

        join_handles.push(io_runtime.spawn(forward_batches_to_executor(
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
        let mut flight_data_encoder = Box::pin(
            arrow_flight::encode::FlightDataEncoderBuilder::new()
                .with_schema(encoder_schema)
                .build(ReceiverStream::new(rx).map(
                    |b| -> std::result::Result<RecordBatch, arrow_flight::error::FlightError> {
                        Ok(b)
                    },
                )),
        );

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
        Ok(Err(message)) => Err(Error::Encode { message }),
    }
}
