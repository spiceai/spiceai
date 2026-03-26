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

use std::{collections::HashMap, pin::Pin, sync::Arc};

use arrow::array::RecordBatch;
use arrow_flight::{
    FlightData, FlightDescriptor, PutResult, flight_service_server::FlightService,
    utils::flight_data_to_arrow_batch,
};
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow_schema::{DataType, SchemaRef};
use byte_unit::rust_decimal::prelude::Zero;
use datafusion::{
    common::DFSchema,
    scalar::ScalarValue,
    sql::{ResolvedTableReference, TableReference},
};
use datafusion_expr::{Expr, execution_props::ExecutionProps, lit};
use futures::{Stream, TryStreamExt as _};
use runtime_request_context::{AsyncMarker, RequestContext};
use snafu::{ResultExt, Snafu};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::{StreamExt, adapters::Peekable, wrappers::ReceiverStream};
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

    #[snafu(display("Filter evaluation failed for executor {executor_id} and {filter}: {source}"))]
    FilterEval {
        executor_id: String,
        source: datafusion::error::DataFusionError,
        filter: String,
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

    #[snafu(display("Upstream execution error: {source}"))]
    UpstreamExecution {
        source: datafusion::error::DataFusionError,
    },
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
///
/// Batches are decoded and routed incrementally from the Flight stream to
/// avoid materializing the full payload in memory.
pub(crate) async fn forward_federated_partitioned_write(
    executor_registry: &ExecutorRegistry,
    ctx: Arc<datafusion::prelude::SessionContext>,
    io_runtime: tokio::runtime::Handle,
    path: &TableReference,
    first_message: FlightData,
    mut streaming_flight: Peekable<Streaming<FlightData>>,
    raw_partition_by: &[String],
) -> Result<Response<<FlightSvc as FlightService>::DoPutStream>> {
    let schema = Arc::new(
        try_schema_from_flatbuffer_bytes(&first_message.data_header).context(DecodeSchemaSnafu)?,
    );

    let dictionaries_by_id = Arc::new(HashMap::new());

    // Decode the first message and build a streaming iterator that yields
    // each subsequent FlightData message as a RecordBatch without buffering.
    let first_batch =
        flight_data_to_arrow_batch(&first_message, Arc::clone(&schema), &dictionaries_by_id).ok();

    let decode_schema = Arc::clone(&schema);
    let batch_stream = async_stream::try_stream! {
        if let Some(batch) = first_batch {
            if batch.num_rows() > 0 {
                yield batch;
            }
        }
        while let Some(result) = streaming_flight.next().await {
            let batch = flight_data_to_arrow_batch(
                &result.context(StreamReadSnafu)?,
                Arc::clone(&decode_schema),
                &dictionaries_by_id,
            )
            .context(DecodeBatchSnafu)?;
            if batch.num_rows() > 0 {
                yield batch;
            }
        }
    };

    forward_partitioned_batches(
        executor_registry,
        ctx,
        io_runtime,
        path,
        &schema,
        Box::pin(batch_stream),
        raw_partition_by,
    )
    .await?;

    Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
        PutResult::default(),
    )]))))
}

/// Core partition-aware batch routing logic shared by the Flight `DoPut` path
/// and the SQL `INSERT INTO` path.
///
/// Accepts an async stream of [`RecordBatch`] and routes each batch to the
/// correct executor as it arrives, avoiding full materialization in memory.
pub(crate) async fn forward_partitioned_batches(
    executor_registry: &ExecutorRegistry,
    ctx: Arc<datafusion::prelude::SessionContext>,
    io_runtime: tokio::runtime::Handle,
    path: &TableReference,
    schema: &SchemaRef,
    mut batches: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    raw_partition_by: &[String],
) -> Result<()> {
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
            partition_manager
                .get_cached_table_metadata(path)
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
    let target_schema = ctx
        .table_provider(path.clone())
        .await
        .context(CreateDFSchemaSnafu)?
        .schema();

    let partition_by = raw_partition_by
        .iter()
        .map(|p| ctx.parse_sql_expr(p, &DFSchema::try_from(Arc::clone(&target_schema))?))
        .collect::<Result<Vec<Expr>, _>>()
        .context(CreateDFSchemaSnafu)?;

    let mut partitions_by_executor = table_partitions
        .all_executor_partitions(&ctx, &target_schema)
        .context(ResolvePartitionsSnafu)?;

    let mut executor_filters = build_executor_filters(&partitions_by_executor, schema)?;

    // Parse partition_by expressions into physical exprs for splitting unmatched rows.
    let partition_phys_exprs = build_partition_physical_exprs(&partition_by, schema)?;

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
        &target_schema,
        tbl,
        &io_runtime,
    )
    .await?;

    let partition_manager = executor_registry.federated_partition_manager();

    // Route each batch through partition filters to the appropriate executor.
    let mut routing_error: Option<Error> = None;
    while let Some(batch_result) = StreamExt::next(&mut batches).await {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                routing_error = Some(e);
                break;
            }
        };
        if let Err(e) = route_batch_and_assign_unseen(
            &batch,
            &mut executor_filters,
            &senders,
            &partition_phys_exprs,
            raw_partition_by,
            &mut partitions_by_executor,
            &partition_manager,
            path,
        )
        .await
        {
            routing_error = Some(e);
            break;
        }
    }

    // Signal completion by dropping senders, then await all forwarding tasks.
    // Collect executor-side errors even when routing failed — the forwarding
    // tasks may hold the real error (e.g. DoPut rejection from the executor)
    // that caused the channel to close and triggered a SendBatch error.
    drop(senders);
    let mut executor_error: Option<Error> = None;
    for handle in join_handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if executor_error.is_none() {
                    executor_error = Some(e);
                }
            }
            Err(e) => {
                if executor_error.is_none() {
                    executor_error = Some(Error::JoinTask { source: e });
                }
            }
        }
    }

    // Prefer the executor-side error (root cause) over the routing error
    // (which is typically a SendBatch from a closed channel).
    if let Some(exec_err) = executor_error {
        return Err(exec_err);
    }
    if let Some(route_err) = routing_error {
        return Err(route_err);
    }

    Ok(())
}

/// Routes a [`RecordBatch`] of data to one or more executors' [`Sender<RecordBatch>`], based on the executor filter
/// predicates, then assigns new partition predicate values to the least-loaded executor and forwards those rows accordingly.
#[expect(clippy::too_many_arguments)]
async fn route_batch_and_assign_unseen(
    batch: &RecordBatch,
    // This is the boolean filter for each executor. It is the OR-combination of all partition predicates assigned to that executor.
    executor_filters: &mut Vec<ExecutorFilter>,
    senders: &HashMap<ExecutorId, Sender<RecordBatch>>,
    // Partition_by expressions, both logical and physical.
    partition_phys_exprs: &[(Expr, Arc<dyn datafusion::physical_plan::PhysicalExpr>)],
    // The original partition expression strings, used for constructing string representations of new partition predicates and values.
    partition_expr_keys: &[String],
    // For each executor, the PartitionValue boolean expressions it currently has.
    partitions_by_executor: &mut HashMap<String, Vec<Expr>>,
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

    // Collect non-empty partitioned sub-batches with their partition values.
    let entries: Vec<(
        Vec<datafusion::common::ScalarValue>,
        PartitionValue,
        RecordBatch,
    )> = partitioned
        .into_iter()
        .filter_map(|(_key, (scalar_values, sub_batch))| {
            if sub_batch.num_rows().is_zero() {
                return None;
            }
            let partition_value: PartitionValue = partition_expr_keys
                .iter()
                .zip(scalar_values.iter())
                .map(|(expr_key, scalar)| (expr_key.clone(), scalar_to_sql_literal(scalar)))
                .collect();
            Some((scalar_values, partition_value, sub_batch))
        })
        .collect();

    if entries.is_empty() {
        return Ok(());
    }

    // Assign an executor for each new partition value up front.
    let executor_ids =
        select_least_loaded_executors(partitions_by_executor, senders, entries.len())?;

    // Persist all assignments in a single OCC write.
    let assignments: Vec<(&PartitionValue, &str)> = entries
        .iter()
        .zip(executor_ids.iter())
        .map(|((_, pv, _), eid)| (pv, eid.as_str()))
        .collect();

    partition_manager
        .add_and_assign_partitions(path, &assignments)
        .await
        .map_err(|source| Error::PersistAssignment {
            source: Box::new(source),
        })?;

    // Update in-memory filters and forward rows for each partition.
    for ((scalar_values, _partition_value, sub_batch), executor_id) in
        entries.into_iter().zip(executor_ids.into_iter())
    {
        tracing::debug!(
            table = %path,
            executor = %executor_id,
            "Assigned new partition and forwarding rows"
        );

        // Update in-memory filters so subsequent batches route via the fast matched path
        // instead of re-entering the expensive unmatched → repartition → assign path.
        {
            let new_pred = partition_phys_exprs
                .iter()
                .zip(scalar_values.iter())
                .map(|((logical_expr, _), scalar)| logical_expr.clone().eq(lit(scalar.clone())))
                .reduce(Expr::and);

            if let Some(new_pred) = new_pred {
                partitions_by_executor
                    .entry(executor_id.clone())
                    .or_default()
                    .push(new_pred);

                // Rebuild physical filter for this executor from its full predicate list.
                let df_schema = DFSchema::try_from(batch.schema().as_ref().clone())
                    .context(CreateDFSchemaSnafu)?;
                let combined = partitions_by_executor[&executor_id]
                    .iter()
                    .cloned()
                    .reduce(Expr::or)
                    .ok_or(Error::EmptyPartitionExprs {
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

                if let Some(entry) = executor_filters
                    .iter_mut()
                    .find(|(id, _)| *id == executor_id)
                {
                    entry.1 = physical;
                } else {
                    executor_filters.push((executor_id.clone(), physical));
                }
            }
        }

        // Forward the rows.
        if let Some(tx) = senders.get(&executor_id) {
            tx.send(sub_batch).await.map_err(|_| Error::SendBatch {
                executor_id: executor_id.clone(),
            })?;
        }
    }

    Ok(())
}

fn scalar_to_sql_literal(scalar: &ScalarValue) -> String {
    if scalar.is_null() {
        return "NULL".to_string();
    }
    match scalar.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            // For string types, produce a properly quoted and escaped SQL literal.
            let value = scalar.to_string();
            let escaped = value.replace('\'', "''");
            format!("'{escaped}'")
        }
        _ => scalar.to_string(),
    }
}

/// Routes matched rows to known executors and returns the unmatched rows.
///
/// Partitions are non-overlapping, so each row matches at most one executor.
/// We progressively shrink the remaining batch as rows get matched, avoiding
/// redundant filter evaluations and data copies on already-routed rows.
async fn route_matched_and_collect_unmatched(
    batch: &RecordBatch,
    executor_filters: &[ExecutorFilter],
    senders: &HashMap<ExecutorId, Sender<RecordBatch>>,
) -> Result<RecordBatch> {
    let mut remaining = batch.clone();

    for (executor_id, filter_expr) in executor_filters {
        if remaining.num_rows() == 0 {
            break;
        }

        let arr = filter_expr
            .evaluate(&remaining)
            .context(FilterEvalSnafu {
                filter: filter_expr.to_string(),
                executor_id: executor_id.clone(),
            })?
            .into_array(remaining.num_rows())
            .context(FilterEvalSnafu {
                filter: filter_expr.to_string(),
                executor_id: executor_id.clone(),
            })?;
        let mask = arr
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| Error::FilterEval {
                filter: filter_expr.to_string(),
                executor_id: executor_id.clone(),
                source: datafusion::error::DataFusionError::Internal(
                    "Filter did not produce boolean array".to_string(),
                ),
            })?;

        let matched_count = mask.true_count();
        if matched_count == 0 {
            continue;
        }

        let filtered =
            arrow::compute::filter_record_batch(&remaining, mask).context(FilterBatchSnafu)?;
        if let Some(tx) = senders.get(executor_id) {
            tx.send(filtered).await.map_err(|_| Error::SendBatch {
                executor_id: executor_id.clone(),
            })?;
        }

        // If every remaining row was matched, nothing left to process.
        if matched_count == remaining.num_rows() {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }

        // Shrink remaining to only unmatched rows for subsequent executors.
        let negated = arrow::compute::not(mask).context(FilterBatchSnafu)?;
        remaining =
            arrow::compute::filter_record_batch(&remaining, &negated).context(FilterBatchSnafu)?;
    }

    Ok(remaining)
}

/// Parses partition-by SQL expression strings into logical + physical expression pairs.
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

/// Selects the least-loaded executor for each of `count` new partition values,
/// distributing them across executors by incrementally accounting for each assignment.
fn select_least_loaded_executors(
    partitions_by_executor: &HashMap<String, Vec<Expr>>,
    senders: &HashMap<ExecutorId, Sender<RecordBatch>>,
    count: usize,
) -> Result<Vec<ExecutorId>> {
    if senders.is_empty() {
        return Err(Error::NoExecutorsAvailable);
    }

    // Track load counts so each successive pick accounts for prior assignments.
    let mut load: HashMap<&str, usize> = senders
        .keys()
        .map(|id| {
            (
                id.as_str(),
                partitions_by_executor.get(id.as_str()).map_or(0, Vec::len),
            )
        })
        .collect();

    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        let executor_id = load
            .iter()
            .min_by_key(|&(_, &count)| count)
            .map(|(&id, _)| id.to_string())
            .ok_or(Error::NoExecutorsAvailable)?;
        *load
            .get_mut(executor_id.as_str())
            .ok_or(Error::NoExecutorsAvailable)? += 1;
        result.push(executor_id);
    }
    Ok(result)
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
    let adapt_schema = Arc::clone(&schema);
    io_runtime.spawn(async move {
        let mut flight_data_encoder = Box::pin(
            arrow_flight::encode::FlightDataEncoderBuilder::new()
                .with_schema(encoder_schema)
                .build(ReceiverStream::new(rx).map(
                    move |b| -> std::result::Result<RecordBatch, arrow_flight::error::FlightError> {
                        arrow_tools::record_batch::try_cast_to(b, Arc::clone(&adapt_schema))
                            .map_err(|e| {
                                arrow_flight::error::FlightError::Arrow(
                                    arrow::error::ArrowError::SchemaError(e.to_string()),
                                )
                            })
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
    let response = match inner_client.do_put(request).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("DoPut to executor failed: {e}");
            return Err(Error::DoPut { source: e });
        }
    };

    if let Err(e) = response.into_inner().try_collect::<Vec<_>>().await {
        tracing::error!("Executor DoPut acknowledgement failed: {e}");
        return Err(Error::DoPutAck { source: e });
    }

    match encode_result_rx.await {
        Ok(Ok(())) | Err(_) => Ok(()),
        Ok(Err(message)) => Err(Error::Encode { message }),
    }
}
