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

use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use arrow::array::{Array, RecordBatch};
use arrow_flight::{FlightData, FlightDescriptor, PutResult, utils::flight_data_to_arrow_batch};
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow_schema::{DataType, SchemaRef};
use datafusion::{
    common::DFSchema,
    scalar::ScalarValue,
    sql::{ResolvedTableReference, TableReference},
};
use datafusion_expr::{Expr, execution_props::ExecutionProps, lit};
use futures::{Stream, TryStreamExt as _, stream::BoxStream};
use runtime_datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use runtime_request_context::{AsyncMarker, RequestContext};
use snafu::{ResultExt, Snafu, ensure};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::{StreamExt, adapters::Peekable, wrappers::ReceiverStream};
use tonic::{Response, Streaming};

use crate::flight_config::{KEEPALIVE_APP_METADATA, do_put_idle_timeout};
use crate::{ExecutorRegistry, PartitionStore, PartitionValue, store};

/// Stream type used by Arrow Flight `DoPut` responses — matches what the runtime
/// crate's `FlightService` impl declares as its associated `DoPutStream` type.
pub type DoPutStream = BoxStream<'static, std::result::Result<PutResult, tonic::Status>>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create partition metadata for table {table}"))]
    CreateMetadata {
        table: String,
        source: Box<store::Error>,
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
    PersistAssignment { source: Box<store::Error> },

    #[snafu(display("Upstream execution error: {source}"))]
    UpstreamExecution {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Row count mismatch after partitioning unmatched rows for table {table}: expected {expected} but got {actual}"
    ))]
    PartitionRowCountMismatch {
        table: String,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("No sender for assigned executor {executor_id} for table {table}"))]
    NoSenderForExecutor { executor_id: String, table: String },
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
///
/// # Errors
///
/// Returns an error if schema decoding, batch routing, or the Flight response fails.
pub async fn forward_federated_partitioned_write(
    executor_registry: &ExecutorRegistry,
    ctx: Arc<datafusion::prelude::SessionContext>,
    io_runtime: tokio::runtime::Handle,
    path: &TableReference,
    first_message: FlightData,
    mut streaming_flight: Peekable<Streaming<FlightData>>,
    raw_partition_by: &[String],
) -> Result<Response<DoPutStream>> {
    let schema = Arc::new(
        try_schema_from_flatbuffer_bytes(&first_message.data_header).context(DecodeSchemaSnafu)?,
    );

    let dictionaries_by_id = Arc::new(HashMap::new());

    // Decode the first message and build a streaming iterator that yields
    // each subsequent FlightData message as a RecordBatch without buffering.
    let first_batch =
        maybe_read_first_batch(&first_message, Arc::clone(&schema), &dictionaries_by_id)?;

    let decode_schema = Arc::clone(&schema);
    let batch_stream = async_stream::try_stream! {
        if let Some(batch) = first_batch {
            yield batch;
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

/// If the first `FlightData` message contains a non-empty body, decode it as
/// the first `RecordBatch` to be forwarded.
/// The first `FlightData` message could be schema-only with an empty body, or
/// it could contain both schema and data; we support both cases.
fn maybe_read_first_batch(
    first_message: &FlightData,
    schema: SchemaRef,
    dictionaries_by_id: &Arc<HashMap<i64, Arc<dyn Array>>>,
) -> Result<Option<RecordBatch>> {
    if first_message.data_body.is_empty() {
        Ok(None)
    } else {
        let batch = flight_data_to_arrow_batch(first_message, schema, dictionaries_by_id)
            .context(DecodeBatchSnafu)?;
        Ok(Some(batch))
    }
}

/// Core partition-aware batch routing logic shared by the Flight `DoPut` path
/// and the SQL `INSERT INTO` path.
///
/// Accepts an async stream of [`RecordBatch`] and routes each batch to the
/// correct executor as it arrives, avoiding full materialization in memory.
///
/// # Errors
///
/// Returns an error if partition metadata lookup, batch forwarding to an executor, or
/// assignment persistence fails.
pub async fn forward_partitioned_batches(
    executor_registry: &ExecutorRegistry,
    ctx: Arc<datafusion::prelude::SessionContext>,
    io_runtime: tokio::runtime::Handle,
    path: &TableReference,
    schema: &SchemaRef,
    mut batches: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    raw_partition_by: &[String],
) -> Result<()> {
    let partition_store = executor_registry.federated_partition_store();
    let table_partitions = match partition_store.get_table_metadata(path).await {
        Ok(Some(metadata)) => metadata,
        Ok(None) => {
            partition_store
                .initialize_metadata(path, raw_partition_by.to_vec())
                .await
                .map_err(|source| Error::CreateMetadata {
                    table: path.to_string(),
                    source: Box::new(source),
                })?;
            partition_store
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
    let all_executor_ids: Vec<ExecutorId> = executor_registry
        .flight_sql_clients_snapshot()
        .await
        .into_keys()
        .collect();

    let (senders, join_handles) = spawn_executor_forwarding_tasks(
        executor_registry,
        &all_executor_ids,
        &target_schema,
        tbl,
        &io_runtime,
    )
    .await?;

    let partition_store = executor_registry.federated_partition_store();

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
            &partition_store,
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
    let metrics_node_id = executor_registry.node_id().map(str::to_string);
    let mut executor_error: Option<Error> = None;
    for (executor_id, handle) in join_handles {
        let outcome = handle.await;
        if let Some(node_id) = metrics_node_id.as_deref() {
            let status = match &outcome {
                Ok(Ok(())) => crate::metrics::WriteForwardStatus::Completed,
                _ => crate::metrics::WriteForwardStatus::Failed,
            };
            crate::metrics::record_partitioned_write_forward(node_id, &executor_id, status);
        }
        match outcome {
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
    partition_store: &Arc<PartitionStore>,
    path: &TableReference,
) -> Result<()> {
    // Partition rows by executor filter, collecting (executor_id, batch) pairs
    // without sending yet. All sends happen concurrently at the end to avoid
    // head-of-line blocking when one executor's channel is full.
    let (unmatched, mut pending_sends) = partition_matched_rows(batch, executor_filters, senders)?;
    if unmatched.num_rows() == 0 {
        send_all_concurrent(senders, pending_sends, path).await?;
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
            if sub_batch.num_rows() == 0 {
                return None;
            }
            let partition_value: PartitionValue = partition_expr_keys
                .iter()
                .zip(scalar_values.iter())
                .map(|(expr_key, scalar)| {
                    let val = if scalar.is_null() {
                        None
                    } else {
                        Some(scalar_to_sql_literal(scalar))
                    };
                    (expr_key.clone(), val)
                })
                .collect();
            Some((scalar_values, partition_value, sub_batch))
        })
        .collect();

    {
        let total_partitioned: usize = entries.iter().map(|(_, _, b)| b.num_rows()).sum();
        ensure!(
            total_partitioned == unmatched.num_rows(),
            PartitionRowCountMismatchSnafu {
                table: path.to_string(),
                expected: unmatched.num_rows(),
                actual: total_partitioned,
            }
        );
    }

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

    partition_store
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
                .map(|((logical_expr, _), scalar)| {
                    if scalar.is_null() {
                        logical_expr.clone().is_null()
                    } else {
                        logical_expr.clone().eq(lit(scalar.clone()))
                    }
                })
                .reduce(Expr::and);

            if let Some(new_pred) = new_pred {
                partitions_by_executor
                    .entry(executor_id.clone())
                    .or_default()
                    .push(new_pred);

                // Rebuild physical filter for this executor from its full predicate list.
                let df_schema = DFSchema::try_from(batch.schema().as_ref().clone())
                    .context(CreateDFSchemaSnafu)?;
                let combined = util::expr::combine_exprs_balanced(
                    partitions_by_executor[&executor_id].clone(),
                    Expr::or,
                )
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

        // Queue the rows for concurrent send.
        if !senders.contains_key(&executor_id) {
            return Err(Error::NoSenderForExecutor {
                executor_id,
                table: path.to_string(),
            });
        }

        pending_sends.push((executor_id.clone(), sub_batch));
    }

    // Send all pending batches (matched + newly assigned) concurrently.
    send_all_concurrent(senders, pending_sends, path).await?;

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

/// Sends all pending `(executor_id, batch)` pairs concurrently so that one
/// slow executor cannot block sends to the others (no head-of-line blocking).
async fn send_all_concurrent(
    senders: &HashMap<ExecutorId, Sender<RecordBatch>>,
    pending: Vec<(ExecutorId, RecordBatch)>,
    path: &TableReference,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }

    let futures: Vec<_> = pending
        .into_iter()
        .map(|(executor_id, batch)| {
            let tx = senders.get(&executor_id).cloned();
            async move {
                let Some(tx) = tx else {
                    return Err(Error::NoSenderForExecutor {
                        executor_id: executor_id.clone(),
                        table: path.to_string(),
                    });
                };
                tx.send(batch).await.map_err(|_| Error::SendBatch {
                    executor_id: executor_id.clone(),
                })?;
                Ok(())
            }
        })
        .collect();

    let results = futures::future::join_all(futures).await;
    for result in results {
        result?;
    }

    Ok(())
}

/// Partitions rows by executor filter, returning `(unmatched_rows, pending_sends)`.
///
/// Evaluates each executor's filter predicate against the batch and collects
/// matched rows into `pending_sends` without sending them. This is a pure
/// compute step — all sends happen concurrently afterwards in
/// [`send_all_concurrent`] to avoid head-of-line blocking.
fn partition_matched_rows(
    batch: &RecordBatch,
    executor_filters: &[ExecutorFilter],
    senders: &HashMap<ExecutorId, Sender<RecordBatch>>,
) -> Result<(RecordBatch, Vec<(ExecutorId, RecordBatch)>)> {
    let mut remaining = batch.clone();
    let mut pending_sends: Vec<(ExecutorId, RecordBatch)> = Vec::new();

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

        // If there is no active sender for this executor (e.g. it disconnected),
        // leave the matched rows in `remaining` so they are treated as unmatched
        // and re-assigned to a connected executor. This prevents silent data loss.
        if !senders.contains_key(executor_id) {
            tracing::warn!(
                executor_id,
                rows = matched_count,
                "Skipping send to disconnected executor; rows will be re-assigned"
            );
            continue;
        }

        let filtered =
            arrow::compute::filter_record_batch(&remaining, mask).context(FilterBatchSnafu)?;

        pending_sends.push((executor_id.clone(), filtered));

        // If every remaining row was matched, nothing left to process.
        if matched_count == remaining.num_rows() {
            return Ok((RecordBatch::new_empty(batch.schema()), pending_sends));
        }

        // Shrink remaining to only unmatched rows for subsequent executors.
        let negated = arrow::compute::not(mask).context(FilterBatchSnafu)?;
        remaining =
            arrow::compute::filter_record_batch(&remaining, &negated).context(FilterBatchSnafu)?;
    }

    Ok((remaining, pending_sends))
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
            util::expr::combine_exprs_balanced(exprs.clone(), Expr::or).ok_or_else(|| {
                Error::EmptyPartitionExprs {
                    executor_id: executor_id.clone(),
                }
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
///
/// Returns one `(executor_id, JoinHandle)` per spawned task so callers can pair
/// per-executor outcomes (used by partitioned-write metrics).
async fn spawn_executor_forwarding_tasks(
    executor_registry: &ExecutorRegistry,
    executors: &[ExecutorId],
    schema: &SchemaRef,
    tbl: ResolvedTableReference,
    io_runtime: &tokio::runtime::Handle,
) -> Result<(
    HashMap<String, Sender<RecordBatch>>,
    Vec<(ExecutorId, tokio::task::JoinHandle<Result<()>>)>,
)> {
    // Resolve auth header and clone clients before holding the lock across spawns.
    let auth_header = RequestContext::current(AsyncMarker::new().await)
        .authorization_header()
        .map(str::to_string);

    let executor_clients: Vec<(ExecutorId, data_components::flightsql::FlightSqlClient)> = {
        let clients = executor_registry.flight_sql_clients_snapshot().await;
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
        senders.insert(executor_id.clone(), tx);

        let executor_id_for_task = executor_id.clone();
        join_handles.push((
            executor_id,
            io_runtime.spawn(forward_batches_to_executor(
                client,
                rx,
                Arc::clone(schema),
                tbl.clone(),
                auth_header.clone(),
                io_runtime.clone(),
                executor_id_for_task,
            )),
        ));
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
    executor_id: String,
) -> Result<()> {
    let forward_start = std::time::Instant::now();
    let batches_forwarded = Arc::new(AtomicU64::new(0));
    let keepalives_sent = Arc::new(AtomicU64::new(0));
    let table_label = tbl.to_string();
    tracing::info!(
        executor = %executor_id,
        table = %table_label,
        "Executor forwarding task started",
    );
    let (tx, flight_rx) = mpsc::channel::<arrow_flight::FlightData>(64);
    let (encode_result_tx, encode_result_rx) =
        tokio::sync::oneshot::channel::<std::result::Result<(), String>>();

    let encoder_schema = Arc::clone(&schema);
    let adapt_schema = Arc::clone(&schema);

    // Keepalive interval: send a heartbeat at 1/3 of the executor idle timeout
    // so the executor never reaches its deadline while a write-through is active.
    // Clamp to a minimum non-zero duration to avoid a tight loop when the
    // idle timeout is very small (e.g. in tests with 1-2s timeouts).
    let keepalive_interval = (do_put_idle_timeout() / 3).max(std::time::Duration::from_millis(100));

    let encoder_batches = Arc::clone(&batches_forwarded);
    let encoder_keepalives = Arc::clone(&keepalives_sent);
    let encoder_handle = io_runtime.spawn(async move {
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

        let keepalive_sleep = tokio::time::sleep(keepalive_interval);
        tokio::pin!(keepalive_sleep);

        loop {
            tokio::select! {
                biased;
                data = flight_data_encoder.next() => {
                    match data {
                        Some(Ok(mut fdata)) => {
                            if is_first {
                                fdata.flight_descriptor = Some(fd.clone());
                                is_first = false;
                            }
                            // Reset keepalive timer after each real message.
                            keepalive_sleep.as_mut().reset(tokio::time::Instant::now() + keepalive_interval);
                            if tx.send(fdata).await.is_err() {
                                let _ = encode_result_tx.send(Ok(()));
                                return;
                            }
                            encoder_batches.fetch_add(1, Ordering::Relaxed);
                        }
                        Some(Err(e)) => {
                            let _ = encode_result_tx.send(Err(e.to_string()));
                            return;
                        }
                        None => {
                            let _ = encode_result_tx.send(Ok(()));
                            return;
                        }
                    }
                }
                () = &mut keepalive_sleep => {
                    // Only send keepalives after the first real FlightData
                    // (which carries the schema/descriptor) has been sent.
                    // Sending a keepalive before the schema would confuse
                    // the executor's DoPut handler.
                    if is_first {
                        keepalive_sleep.as_mut().reset(tokio::time::Instant::now() + keepalive_interval);
                        continue;
                    }
                    // No data for a while — send a keepalive to prevent the
                    // executor's DoPut idle timeout from firing.
                    let keepalive = arrow_flight::FlightData {
                        app_metadata: bytes::Bytes::from_static(KEEPALIVE_APP_METADATA),
                        ..Default::default()
                    };
                    if tx.send(keepalive).await.is_err() {
                        let _ = encode_result_tx.send(Ok(()));
                        return;
                    }
                    encoder_keepalives.fetch_add(1, Ordering::Relaxed);
                    keepalive_sleep.as_mut().reset(tokio::time::Instant::now() + keepalive_interval);
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

    let elapsed_ms = || u64::try_from(forward_start.elapsed().as_millis()).unwrap_or(u64::MAX);

    let mut inner_client = client.into_inner();
    let response = match inner_client.do_put(request).await {
        Ok(r) => r,
        Err(e) => {
            // Abort the encoder task so its `rx` is dropped promptly.
            // This prevents the routing loop from queuing data into a dead
            // channel and eventually stalling.
            encoder_handle.abort();
            tracing::error!(
                executor = %executor_id,
                table = %table_label,
                elapsed_ms = elapsed_ms(),
                batches = batches_forwarded.load(Ordering::Relaxed),
                keepalives = keepalives_sent.load(Ordering::Relaxed),
                error = %e,
                "`DoPut` to executor failed",
            );
            return Err(Error::DoPut { source: e });
        }
    };

    if let Err(e) = response.into_inner().try_collect::<Vec<_>>().await {
        encoder_handle.abort();
        tracing::error!(
            executor = %executor_id,
            table = %table_label,
            elapsed_ms = elapsed_ms(),
            batches = batches_forwarded.load(Ordering::Relaxed),
            keepalives = keepalives_sent.load(Ordering::Relaxed),
            error = %e,
            "Executor `DoPut` acknowledgement failed",
        );
        return Err(Error::DoPutAck { source: e });
    }

    tracing::info!(
        executor = %executor_id,
        table = %table_label,
        elapsed_ms = elapsed_ms(),
        batches = batches_forwarded.load(Ordering::Relaxed),
        keepalives = keepalives_sent.load(Ordering::Relaxed),
        "Executor forwarding task completed successfully",
    );

    match encode_result_rx.await {
        Ok(Ok(())) | Err(_) => Ok(()),
        Ok(Err(message)) => Err(Error::Encode { message }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, Schema};
    use arrow_flight::utils::batches_to_flight_data;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn encode_batch_to_flight_data(schema: &SchemaRef, batch: &RecordBatch) -> Vec<FlightData> {
        batches_to_flight_data(schema, vec![batch.clone()]).expect("encode flight data")
    }

    #[test]
    fn test_maybe_read_first_batch_empty_body_returns_none() {
        let schema = test_schema();
        let dictionaries_by_id = Arc::new(HashMap::new());

        // Build a FlightData with schema header but empty body (schema-only message).
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("should create batch");
        let flight_data = encode_batch_to_flight_data(&schema, &batch);

        // The schema-only message should have an empty data_body.
        assert!(
            flight_data[0].data_body.is_empty(),
            "schema message should have empty body"
        );

        let result =
            maybe_read_first_batch(&flight_data[0], Arc::clone(&schema), &dictionaries_by_id)
                .expect("should succeed");
        assert!(result.is_none(), "empty body should return None");
    }

    #[test]
    fn test_maybe_read_first_batch_with_data_returns_some() {
        let schema = test_schema();
        let dictionaries_by_id = Arc::new(HashMap::new());

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )
        .expect("should create batch");

        let flight_data = encode_batch_to_flight_data(&schema, &batch);
        assert!(
            !flight_data.is_empty(),
            "should have at least one data message"
        );

        let data_fd = flight_data
            .into_iter()
            .nth(1)
            .expect("should have data message");
        assert!(
            !data_fd.data_body.is_empty(),
            "data message should have non-empty body"
        );

        let result = maybe_read_first_batch(&data_fd, Arc::clone(&schema), &dictionaries_by_id)
            .expect("should succeed");

        let decoded = result.expect("non-empty body should return Some");
        assert_eq!(decoded.num_rows(), 3);
        assert_eq!(decoded.num_columns(), 1);

        let col = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("column should be Int32Array");
        assert_eq!(col.values().as_ref(), &[10, 20, 30]);
    }

    #[test]
    fn test_maybe_read_first_batch_single_row() {
        let schema = test_schema();
        let dictionaries_by_id = Arc::new(HashMap::new());

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![42]))],
        )
        .expect("should create batch");

        let flight_data = encode_batch_to_flight_data(&schema, &batch);
        let data_fd = flight_data
            .into_iter()
            .nth(1)
            .expect("should have data message");

        let result = maybe_read_first_batch(&data_fd, Arc::clone(&schema), &dictionaries_by_id)
            .expect("should succeed");

        let decoded = result.expect("should return Some for single row");
        assert_eq!(decoded.num_rows(), 1);
    }
}
