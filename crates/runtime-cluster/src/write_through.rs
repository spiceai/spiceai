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
use arrow_tools::{ipc, map_entries::MapEntriesNormalizer};
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

    #[snafu(display(
        "Failed to write to dataset '{table}': an Arrow message partway through the stream \
        could not be read ({message}), so the rest of the stream was not applied and any batch \
        already accepted may have been. \
        Check that the writing client emits valid Arrow IPC. \
        See: https://spiceai.org/docs/api/arrow-flight-sql"
    ))]
    UnreadableMessageHeader { table: String, message: String },

    #[snafu(display(
        "Failed to write to dataset '{table}': an Arrow message partway through the stream \
        carries no record batch, so the rest of the stream was not applied and any batch \
        already accepted may have been. \
        Send every message after the schema as a record batch. \
        See: https://spiceai.org/docs/api/arrow-flight-sql"
    ))]
    NonBatchMessage { table: String },

    #[snafu(display(
        "Failed to write to dataset '{table}': the first Arrow message of the stream carries \
        data that no record batch describes, so nothing was written. \
        Send the schema on its own, then the rows as record batch messages. \
        See: https://spiceai.org/docs/api/arrow-flight-sql"
    ))]
    FirstMessageBodyWithoutBatch { table: String },

    #[snafu(display("Stream error while reading FlightData: {source}"))]
    StreamRead { source: tonic::Status },

    #[snafu(display(
        "Failed to read the Arrow data sent for table '{table}' ({source}), so the rest of the stream was not applied and any batch already accepted may have been. \
        Send the MAP column with an `entries` field that is non-nullable and holds no null entries, as the Arrow map layout requires. \
        See: https://spiceai.org/docs/api/arrow-flight-sql"
    ))]
    MapEntriesNotNormalizable {
        table: String,
        source: arrow_tools::map_entries::Error,
    },

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
            // The client sent a MAP the Arrow layout does not allow. That is malformed input,
            // not a server fault, and the local DoPut path already reports it as such — routing
            // it through `internal` here would tell the caller to retry a write that can never
            // succeed.
            // Same reasoning for a stream that does not carry what Flight says it does: the
            // client sent it, and no retry of the same stream can succeed.
            Error::MapEntriesNotNormalizable { .. }
            | Error::UnreadableMessageHeader { .. }
            | Error::NonBatchMessage { .. }
            | Error::FirstMessageBodyWithoutBatch { .. } => {
                tonic::Status::invalid_argument(err.to_string())
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
    streaming_flight: Peekable<Streaming<FlightData>>,
    raw_partition_by: &[String],
) -> Result<Response<DoPutStream>> {
    let declared: SchemaRef = Arc::new(
        try_schema_from_flatbuffer_bytes(&first_message.data_header).context(DecodeSchemaSnafu)?,
    );

    // A client is free to declare a MAP's `entries` field nullable, which the Arrow map layout
    // forbids. This is the scheduler's own decode of the client stream, so the correction has to
    // happen here too: the partition expressions are evaluated against these batches before any
    // executor sees them. Each batch is decoded under the client's own declarations and relabelled
    // afterwards, so an entries array carrying nulls — the one shape relabelling cannot fix — is
    // refused rather than routed under a declaration that says it holds none. One stream carries
    // one schema, so what its batches need is resolved once.
    let normalizer = MapEntriesNormalizer::for_schema(&declared);
    let schema = Arc::clone(normalizer.schema());

    // Decode the first message and build a streaming iterator that yields
    // each subsequent FlightData message as a RecordBatch without buffering.
    let batch_stream = decode_client_batches(
        &first_message,
        streaming_flight,
        declared,
        normalizer,
        path.to_string(),
    )?;

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

/// Decodes a client's `DoPut` stream into the `RecordBatch`es the partition router forwards.
///
/// Generic over the inbound stream so the decode can be driven from a test with a stream of
/// hand-assembled messages; production passes the tonic `Streaming` the request arrived on.
///
/// Three kinds of message reach here that are not record batches, and each is answered the way
/// the runtime's own `DoPut` handler answers it, since the scheduler speaks the same protocol:
///
/// - a **keepalive** heartbeat is skipped. The scheduler emits these itself when it forwards to
///   an executor, so refusing one from a writer that follows the same convention would be the
///   scheduler rejecting its own protocol;
/// - a message that **declares no record batch** — a metadata-only trailer, or a schema
///   re-declared partway through — fails the write, because the stream has gone out of step
///   with what it declared and the rows already accepted may have been applied;
/// - a **header that will not parse** fails the write as a malformed stream.
///
/// Asking the batch decoder instead of the header is not the same question: it rejects all
/// three with the Arrow decoder's own wording, which names a flatbuffer range rather than
/// anything the writer can act on.
fn decode_client_batches<S>(
    first_message: &FlightData,
    mut messages: S,
    declared: SchemaRef,
    normalizer: MapEntriesNormalizer,
    table_name: String,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + 'static>
where
    S: Stream<Item = std::result::Result<FlightData, tonic::Status>> + Unpin + Send + 'static,
{
    let dictionaries_by_id = HashMap::new();
    let first_batch = maybe_read_first_batch(
        first_message,
        Arc::clone(&declared),
        &dictionaries_by_id,
        &table_name,
    )?;

    Ok(async_stream::try_stream! {
        if let Some(batch) = first_batch {
            yield normalizer
                .normalize(batch)
                .with_context(|_| MapEntriesNotNormalizableSnafu { table: table_name.clone() })?;
        }
        while let Some(result) = messages.next().await {
            let message = result.context(StreamReadSnafu)?;

            // The sentinel alone cannot decide this. On this path `app_metadata` is the
            // client's to set, so a message carrying data can wear it, and skipping on the
            // metadata alone discards that data while the write still reports success. A
            // heartbeat declares no IPC data at all, so requiring that too keeps the skip to
            // real heartbeats and sends anything data-bearing on to the check below, which
            // decodes it or fails loudly. The predicate is `declares_ipc_data` rather than
            // `declares_record_batch` because a dictionary is client data too: the batches
            // referring to it carry nothing without it, so a tagged dictionary must be refused
            // rather than dropped. The empty body is a floor under that: the header can only
            // ever add to what the body already establishes, never narrow it. `declares_ipc_data`
            // answers `false` for a schema message, a trailer, a `Tensor` and any IPC header a
            // later Arrow adds, so without the floor a sentinel-tagged message of those kinds
            // would take its body with it and the write would still report success. A real
            // heartbeat carries neither header nor body. `do_put.rs`'s discarded-message count
            // keeps the same header-vs-body floor, for the same reason -- though it applies the
            // floor only *after* its own sentinel check, which still skips unconditionally.
            if message.app_metadata.as_ref() == KEEPALIVE_APP_METADATA
                && message.data_body.is_empty()
                && !declares_ipc_data(&message.data_header, &table_name)?
            {
                continue;
            }

            if !declares_record_batch(&message.data_header, &table_name)? {
                // `ensure!` cannot be used inside the generator: it expands to a `return`,
                // which ends the stream rather than failing it.
                Err::<(), Error>(Error::NonBatchMessage {
                    table: table_name.clone(),
                })?;
            }

            let batch = flight_data_to_arrow_batch(
                &message,
                Arc::clone(&declared),
                &dictionaries_by_id,
            )
            .context(DecodeBatchSnafu)?;
            if batch.num_rows() > 0 {
                yield normalizer
                    .normalize(batch)
                    .with_context(|_| MapEntriesNotNormalizableSnafu { table: table_name.clone() })?;
            }
        }
    })
}

/// Whether a message's IPC header declares data the client sent — a record batch, or a
/// dictionary the batches referencing it cannot be decoded without — reporting an unreadable
/// header the same way [`declares_record_batch`] does.
fn declares_ipc_data(data_header: &[u8], table: &str) -> Result<bool> {
    ipc::declares_ipc_data(data_header).map_err(|message| Error::UnreadableMessageHeader {
        table: table.to_string(),
        message,
    })
}

/// Whether a message's IPC header declares a record batch, reporting an unreadable header as
/// this module's own failure so that what the writer sees names the dataset.
fn declares_record_batch(data_header: &[u8], table: &str) -> Result<bool> {
    ipc::declares_record_batch(data_header).map_err(|message| Error::UnreadableMessageHeader {
        table: table.to_string(),
        message,
    })
}

/// Decodes the first `FlightData` message as a `RecordBatch` when its header says it is one.
///
/// The header is the discriminator, not the body length. Arrow encodes a batch whose columns
/// need no buffers — an all-`Null` batch is the clear case — as a `RecordBatch` message with an
/// empty body, and such a batch can carry rows, so reading an empty body as "no batch" drops
/// rows the writer sent.
///
/// The caller's own precondition is what keeps that from being reachable here today: it derives
/// the stream's schema from this same header with `try_schema_from_flatbuffer_bytes`, which
/// fails on anything but a `Schema` message, so a first message that carries rows never reaches
/// this function — the write is refused earlier with a decode-schema error. Reading the header
/// rather than the body is what stops that from being load-bearing, since nothing in either
/// signature ties the two together.
///
/// A header that will not parse is a malformed stream rather than the absence of a batch, and
/// is reported as such.
fn maybe_read_first_batch(
    first_message: &FlightData,
    schema: SchemaRef,
    dictionaries_by_id: &HashMap<i64, Arc<dyn Array>>,
    table: &str,
) -> Result<Option<RecordBatch>> {
    if !declares_record_batch(&first_message.data_header, table)? {
        // The same floor the subsequent-message loop keeps: a body the client streamed is data
        // whatever the header above it declares, so reading the header alone must not narrow
        // what the body already establishes. Without this, a schema-headed first message with a
        // body answers `None` and those bytes are dropped while the write reports success --
        // which counting by body length, the test this replaced, got right.
        ensure!(
            first_message.data_body.is_empty(),
            FirstMessageBodyWithoutBatchSnafu { table }
        );
        return Ok(None);
    }

    let batch = flight_data_to_arrow_batch(first_message, schema, dictionaries_by_id)
        .context(DecodeBatchSnafu)?;
    Ok(Some(batch))
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
        entries.into_iter().zip(executor_ids)
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
    use arrow::array::{
        ArrayData, ArrayRef, Int32Array, MapArray, StringArray, StringDictionaryBuilder,
        StructArray,
    };
    use arrow::buffer::{Buffer, NullBuffer};
    use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
    use arrow_flight::utils::batches_to_flight_data;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn entry_fields() -> Fields {
        vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into()
    }

    /// A `MAP` declaring its `entries` field nullable — what a client that did not read the
    /// Arrow map layout sends, and the declaration this seam has to relabel.
    fn map_type(entries_nullable: bool) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(entry_fields()),
                entries_nullable,
            )),
            false,
        )
    }

    fn map_schema(entries_nullable: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "m",
            map_type(entries_nullable),
            true,
        )]))
    }

    /// Builds a `MapArray` through `ArrayData` rather than `MapArray::try_new`, the way the IPC
    /// reader does: neither `entries` check runs there, which is how a non-conforming map
    /// reaches this seam at all.
    fn map_batch(entries_nullable: bool, entry_nulls: Option<NullBuffer>) -> RecordBatch {
        let entries = StructArray::try_new(
            entry_fields(),
            vec![
                Arc::new(StringArray::from(vec!["k0", "k1"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("v0"), Some("v1")])) as ArrayRef,
            ],
            entry_nulls,
        )
        .expect("entries struct");

        let data = ArrayData::builder(map_type(entries_nullable))
            .len(2)
            .add_buffer(Buffer::from_slice_ref([0i32, 1, 2]))
            .add_child_data(entries.to_data())
            .build()
            .expect("map array data");

        RecordBatch::try_new(
            map_schema(entries_nullable),
            vec![Arc::new(MapArray::from(data)) as ArrayRef],
        )
        .expect("map batch")
    }

    fn encode_batch_to_flight_data(schema: &SchemaRef, batch: &RecordBatch) -> Vec<FlightData> {
        batches_to_flight_data(schema, vec![batch.clone()]).expect("encode flight data")
    }

    #[test]
    fn test_maybe_read_first_batch_empty_body_returns_none() {
        let schema = test_schema();
        let dictionaries_by_id = HashMap::new();

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

        let result = maybe_read_first_batch(
            &flight_data[0],
            Arc::clone(&schema),
            &dictionaries_by_id,
            "test.table",
        )
        .expect("should succeed");
        assert!(result.is_none(), "empty body should return None");
    }

    #[test]
    fn test_maybe_read_first_batch_with_data_returns_some() {
        let schema = test_schema();
        let dictionaries_by_id = HashMap::new();

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

        let result = maybe_read_first_batch(
            &data_fd,
            Arc::clone(&schema),
            &dictionaries_by_id,
            "test.table",
        )
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
        let dictionaries_by_id = HashMap::new();

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

        let result = maybe_read_first_batch(
            &data_fd,
            Arc::clone(&schema),
            &dictionaries_by_id,
            "test.table",
        )
        .expect("should succeed");

        let decoded = result.expect("should return Some for single row");
        assert_eq!(decoded.num_rows(), 1);
    }

    /// The scheduler `DoPut` path decodes and normalizes on its own seam, which is how the
    /// original runtime-side fix missed it. This drives that seam end to end: a stream that
    /// declares `entries` nullable must reach partition routing under a conformed schema.
    #[test]
    fn scheduler_seam_conforms_a_nullable_entries_declaration() {
        let declared = map_schema(true);
        let dictionaries_by_id = HashMap::new();

        let batch = map_batch(true, None);
        let flight_data = encode_batch_to_flight_data(&declared, &batch);
        let data_fd = flight_data
            .into_iter()
            .nth(1)
            .expect("should have a data message");

        let decoded = maybe_read_first_batch(
            &data_fd,
            Arc::clone(&declared),
            &dictionaries_by_id,
            "test.table",
        )
        .expect("decode should succeed")
        .expect("data message should carry a batch");

        let normalizer = MapEntriesNormalizer::for_schema(&declared);

        // What partition routing is handed must declare `entries` non-nullable, or it
        // describes the batches with a type they no longer carry.
        let DataType::Map(entries, _) = normalizer.schema().field(0).data_type() else {
            panic!("column should still be a MAP");
        };
        assert!(
            !entries.is_nullable(),
            "routing schema must declare entries non-nullable"
        );

        let conformed = normalizer
            .normalize(decoded)
            .expect("normalize should succeed");
        let conformed_schema = conformed.schema();
        let DataType::Map(entries, _) = conformed_schema.field(0).data_type() else {
            panic!("column should still be a MAP");
        };
        assert!(
            !entries.is_nullable(),
            "normalized batch must carry the conformed type"
        );
        assert_eq!(conformed.num_rows(), 2);
    }

    /// Entry nulls are the one shape relabelling cannot fix. The scheduler path must refuse
    /// them, and must present the refusal as malformed input rather than a server fault —
    /// `internal` would tell the client to retry a write that can never succeed.
    #[test]
    fn scheduler_seam_refuses_entry_nulls_as_invalid_argument() {
        let declared = map_schema(true);
        let dictionaries_by_id = HashMap::new();

        let batch = map_batch(true, Some(NullBuffer::from(vec![true, false])));
        let flight_data = encode_batch_to_flight_data(&declared, &batch);
        let data_fd = flight_data
            .into_iter()
            .nth(1)
            .expect("should have a data message");

        let decoded = maybe_read_first_batch(
            &data_fd,
            Arc::clone(&declared),
            &dictionaries_by_id,
            "test.table",
        )
        .expect("decode should succeed")
        .expect("data message should carry a batch");

        let err = MapEntriesNormalizer::for_schema(&declared)
            .normalize(decoded)
            .context(MapEntriesNotNormalizableSnafu {
                table: "sales.orders".to_string(),
            })
            .expect_err("entry nulls must be refused");

        assert!(
            matches!(err, Error::MapEntriesNotNormalizable { .. }),
            "expected MapEntriesNotNormalizable, got {err:?}"
        );

        let status = tonic::Status::from(err);
        assert_eq!(
            status.code(),
            tonic::Code::InvalidArgument,
            "malformed client input must not be reported as an internal error"
        );
        assert!(
            status.message().contains("'sales.orders'"),
            "message must name the table, quoted so a dotted identifier stays unambiguous: {}",
            status.message()
        );
    }

    /// The three-message shape a client actually sends: the schema, then batches. `region` is
    /// the partition column in the cluster tests, but nothing here routes, so any schema does.
    fn client_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("id", DataType::Int32, false),
        ]))
    }

    fn client_batch(regions: Vec<&str>, ids: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            client_schema(),
            vec![
                Arc::new(StringArray::from(regions)) as ArrayRef,
                Arc::new(Int32Array::from(ids)) as ArrayRef,
            ],
        )
        .expect("client batch")
    }

    fn keepalive() -> FlightData {
        FlightData {
            app_metadata: bytes::Bytes::from_static(KEEPALIVE_APP_METADATA),
            ..Default::default()
        }
    }

    /// Drives the production decode over `messages`, with `first` as the message the caller
    /// already took off the stream.
    async fn decode(
        first: FlightData,
        messages: Vec<FlightData>,
        schema: &SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        let stream = decode_client_batches(
            &first,
            futures::stream::iter(messages.into_iter().map(Ok::<_, tonic::Status>)),
            Arc::clone(schema),
            MapEntriesNormalizer::for_schema(schema),
            "test.s.events".to_string(),
        )?;
        Box::pin(stream).try_collect().await
    }

    /// A keepalive is a heartbeat, not data. The scheduler emits these itself when it forwards
    /// to an executor, and the executor's `DoPut` handler skips them; feeding one to the batch
    /// decoder instead fails the whole write with a flatbuffer range error.
    #[tokio::test]
    async fn a_keepalive_partway_through_the_stream_is_skipped() {
        let schema = client_schema();
        let mut messages = encode_batch_to_flight_data(&schema, &client_batch(vec!["US"], vec![1]));
        let first = messages.remove(0);
        let second = encode_batch_to_flight_data(&schema, &client_batch(vec!["EU"], vec![2]))
            .pop()
            .expect("a batch message");

        // What the decoder this replaces would have been handed. Asserted so the case cannot
        // quietly stop exercising the failure: it is the whole reason it exists.
        let dictionaries = Arc::new(HashMap::new());
        assert!(
            flight_data_to_arrow_batch(&keepalive(), Arc::clone(&schema), &dictionaries).is_err(),
            "a keepalive is expected to be undecodable as a batch"
        );

        let batches = decode(
            first,
            vec![messages.remove(0), keepalive(), second],
            &schema,
        )
        .await
        .expect("the keepalive should be skipped, not decoded");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    }

    /// On this path `app_metadata` is set by the Flight client — the function's own comment
    /// calls this "the scheduler's own decode of the client stream" — so the sentinel is not a
    /// private channel between our scheduler and our executor, and a writer is free to put it on
    /// a message that also carries a batch. Skipping on the metadata alone drops those rows and
    /// still reports the write a success: the same silent row loss the header discriminator
    /// exists to close, reopened by a different door.
    #[tokio::test]
    async fn a_data_bearing_message_wearing_the_keepalive_sentinel_is_not_skipped() {
        let schema = client_schema();
        let mut messages = encode_batch_to_flight_data(&schema, &client_batch(vec!["US"], vec![1]));
        let first = messages.remove(0);
        let mut second = encode_batch_to_flight_data(&schema, &client_batch(vec!["EU"], vec![2]))
            .pop()
            .expect("a batch message");
        second.app_metadata = bytes::Bytes::from_static(KEEPALIVE_APP_METADATA);

        // The fixture really does declare a batch: the sentinel is the only thing separating it
        // from the message the keepalive case sends. Asserted so the test cannot pass by
        // accidentally carrying nothing.
        assert!(
            declares_record_batch(&second.data_header, "test.s.events").expect("a readable header"),
            "the fixture must declare a record batch for this case to mean anything"
        );

        let batches = decode(first, vec![messages.remove(0), second], &schema)
            .await
            .expect("a data-bearing message should decode, not be skipped");

        // Asserted by value, not by count: a count alone passes on code that drops the tagged
        // row and duplicates the untagged one, which is the wrong behaviour wearing the right
        // total.
        let regions: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("the region column is a string")
                    .iter()
                    .map(|region| region.expect("no nulls in the fixture").to_string())
            })
            .collect();
        assert_eq!(
            regions,
            vec!["US".to_string(), "EU".to_string()],
            "the row carried by the sentinel-tagged message was dropped"
        );
    }

    /// A dictionary is client data too — the batches referring to it carry nothing without it —
    /// so a dictionary message wearing the sentinel must not be skipped either. This decoder
    /// does not ingest dictionaries, so what matters is that one is refused loudly rather than
    /// discarded while the write reports success.
    #[tokio::test]
    async fn a_dictionary_message_wearing_the_keepalive_sentinel_is_refused_not_skipped() {
        use arrow::ipc::writer::{
            CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
        };

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "region",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let mut values = StringDictionaryBuilder::<Int32Type>::new();
        values.append_value("US");
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(values.finish()) as ArrayRef],
        )
        .expect("a dictionary batch");

        // Encoded the way a writer does, so the dictionary message is a real one rather than a
        // header this test built to agree with itself.
        let generator = IpcDataGenerator::default();
        let options = IpcWriteOptions::default();
        let mut tracker = DictionaryTracker::new(false);
        let schema_message =
            generator.schema_to_bytes_with_dictionary_tracker(&schema, &mut tracker, &options);
        let (dictionaries, _encoded) = generator
            .encode(
                &batch,
                &mut tracker,
                &options,
                &mut CompressionContext::default(),
            )
            .expect("encoding a dictionary batch");
        let encoded_dictionary = dictionaries
            .into_iter()
            .next()
            .expect("a dictionary message");

        let first = FlightData {
            data_header: schema_message.ipc_message.into(),
            ..Default::default()
        };
        let dictionary = FlightData {
            data_header: encoded_dictionary.ipc_message.into(),
            data_body: encoded_dictionary.arrow_data.into(),
            app_metadata: bytes::Bytes::from_static(KEEPALIVE_APP_METADATA),
            ..Default::default()
        };
        assert!(
            ipc::declares_ipc_data(&dictionary.data_header).expect("a readable header")
                && !ipc::declares_record_batch(&dictionary.data_header).expect("a readable header"),
            "the fixture must be a dictionary message for this case to mean anything"
        );

        let err = decode(first, vec![dictionary], &schema)
            .await
            .expect_err("a tagged dictionary should be refused, not silently skipped");

        assert!(matches!(err, Error::NonBatchMessage { .. }), "{err:?}");
    }

    /// A message that declares no record batch means the stream has gone out of step with what
    /// it declared. It fails the write, and the message has to say which dataset and what to do
    /// — the Arrow decoder's own wording names a flatbuffer range instead.
    #[tokio::test]
    async fn a_message_declaring_no_batch_fails_the_write_with_an_actionable_message() {
        let schema = client_schema();
        let mut messages = encode_batch_to_flight_data(&schema, &client_batch(vec!["US"], vec![1]));
        let first = messages.remove(0);

        let err = decode(
            first,
            vec![messages.remove(0), FlightData::default()],
            &schema,
        )
        .await
        .expect_err("a trailer declaring no batch should fail the write");

        assert!(matches!(err, Error::NonBatchMessage { .. }), "{err:?}");
        let message = err.to_string();
        assert!(message.contains("'test.s.events'"), "{message}");
        assert!(message.contains("carries no record batch"), "{message}");
        assert!(
            message.contains("Send every message after the schema as a record batch"),
            "{message}"
        );
        assert!(
            message.contains("https://spiceai.org/docs/api/arrow-flight-sql"),
            "{message}"
        );
        assert_eq!(
            tonic::Status::from(err).code(),
            tonic::Code::InvalidArgument,
            "the client sent it, so no retry of the same stream can succeed"
        );
    }

    /// A header with bytes that will not parse is a malformed stream, not the absence of a
    /// batch, and is reported as its own problem.
    #[tokio::test]
    async fn an_unparseable_header_fails_the_write_as_malformed() {
        let schema = client_schema();
        let mut messages = encode_batch_to_flight_data(&schema, &client_batch(vec!["US"], vec![1]));
        let first = messages.remove(0);
        let garbage = FlightData {
            data_header: bytes::Bytes::from_static(&[0xff; 8]),
            ..Default::default()
        };

        let err = decode(first, vec![messages.remove(0), garbage], &schema)
            .await
            .expect_err("an unreadable header should fail the write");

        assert!(
            matches!(err, Error::UnreadableMessageHeader { .. }),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("'test.s.events'"), "{message}");
        assert!(message.contains("could not be read"), "{message}");
        assert!(
            message.contains("https://spiceai.org/docs/api/arrow-flight-sql"),
            "{message}"
        );
    }

    /// The header, not the body length, decides whether the first message carries a batch.
    ///
    /// Arrow encodes a batch whose columns need no buffers with an empty body, so a body-length
    /// discriminator reads this three-row batch as no batch at all and drops its rows. The
    /// caller's schema parse keeps that unreachable in production today — a first message that
    /// declares a record batch fails `try_schema_from_flatbuffer_bytes` before this runs — but
    /// nothing in either signature ties the two together, so the discriminator is guarded here.
    #[tokio::test]
    async fn a_buffer_free_first_message_is_decoded_by_its_header() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("n", DataType::Null, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::NullArray::new(3)) as ArrayRef],
        )
        .expect("null batch");
        let mut messages = encode_batch_to_flight_data(&schema, &batch);
        let first = messages.pop().expect("the batch message");

        assert!(
            first.data_body.is_empty(),
            "a batch whose columns need no buffers is expected to encode with an empty body; without that this case does not exercise the confusion"
        );

        let batches = decode(first, vec![], &schema)
            .await
            .expect("the first message declares a record batch");
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            3,
            "the rows the empty body would have dropped"
        );
    }

    /// The ordinary shape: a schema-only first message declares no batch and yields nothing.
    #[tokio::test]
    async fn a_schema_only_first_message_yields_no_batch_of_its_own() {
        let schema = client_schema();
        let mut messages = encode_batch_to_flight_data(&schema, &client_batch(vec!["US"], vec![1]));
        let first = messages.remove(0);
        assert!(first.data_body.is_empty());

        let batches = decode(first, vec![], &schema).await.expect("no batches");
        assert!(batches.is_empty());
    }

    /// A message wearing the keepalive sentinel whose header declares something other than IPC
    /// data — a schema re-declaration, a trailer, a `Tensor`, any header a later Arrow adds —
    /// but which carries a body, is client data, not a heartbeat.
    ///
    /// `declares_ipc_data` answers `false` for all of those, so the sentinel check alone would
    /// skip the message and take its body with it while the write still reported success: the
    /// exact silent-row-loss shape this PR exists to remove, reintroduced one layer up. The
    /// empty-body floor is what refuses it. `do_put.rs` keeps the same header-vs-body floor in
    /// its discarded-message count, but reaches it only past a sentinel check that still skips
    /// unconditionally, so on that one point the two receivers do not yet agree.
    ///
    /// A schema message is used because it is the shape a real client is likeliest to send; the
    /// arm it exercises is shared by every non-data header.
    #[tokio::test]
    async fn a_sentinel_tagged_message_carrying_a_body_is_not_skipped_as_a_heartbeat() {
        let schema = client_schema();
        let mut msgs = encode_batch_to_flight_data(&schema, &client_batch(vec!["US"], vec![1]));
        let first = msgs.remove(0);

        let mut tagged = batches_to_flight_data(
            &Schema::new(vec![Field::new("id", DataType::Int32, false)]),
            vec![],
        )
        .expect("encoding a schema as flight data")
        .remove(0);
        tagged.data_body = bytes::Bytes::from_static(b"rows the client sent");
        tagged.app_metadata = bytes::Bytes::from_static(KEEPALIVE_APP_METADATA);

        // Asserted so the case cannot quietly stop exercising the confusion: it needs a header
        // that parses into a non-data kind, and a body under it.
        assert_eq!(
            ipc::declares_ipc_data(&tagged.data_header),
            Ok(false),
            "the case needs a header that declares something other than IPC data"
        );
        assert!(
            !tagged.data_body.is_empty(),
            "the case needs a body; without one this is a real heartbeat"
        );

        let err = decode(first, vec![msgs.remove(0), tagged], &schema)
            .await
            .expect_err("a sentinel-tagged message carrying a body must not be skipped");

        assert!(matches!(err, Error::NonBatchMessage { .. }), "{err:?}");
        assert!(err.to_string().contains("'test.s.events'"), "{err}");
    }
    /// A first message whose header declares a schema but which carries a body is lost client
    /// data, not an absent batch.
    ///
    /// This is the half of the body/header swap that could *regress* rather than repair: reading
    /// the header alone answers `None` here, so those bytes would be dropped and an otherwise
    /// empty stream acknowledged as a complete write. Counting by body length -- the test this
    /// branch replaced -- got this case right, because a non-empty body reached
    /// `flight_data_to_arrow_batch`, which refuses a schema header. So the floor is a property
    /// to preserve, not a new one to add, and it matches what the subsequent-message loop does.
    #[tokio::test]
    async fn a_first_message_carrying_a_body_under_a_schema_header_is_refused() {
        let schema = client_schema();
        let mut first =
            encode_batch_to_flight_data(&schema, &client_batch(vec!["US"], vec![1])).remove(0);
        first.data_body = bytes::Bytes::from_static(b"rows the client sent");

        // Asserted so the case cannot quietly stop exercising the confusion.
        assert_eq!(
            ipc::declares_record_batch(&first.data_header),
            Ok(false),
            "the case needs the leading schema message, whose header declares no batch"
        );
        assert!(!first.data_body.is_empty(), "the case needs a body");

        let err = maybe_read_first_batch(&first, schema, &HashMap::new(), "test.s.events")
            .expect_err("a body no batch describes must not be silently dropped");

        assert!(
            matches!(err, Error::FirstMessageBodyWithoutBatch { .. }),
            "{err:?}"
        );
        let message = err.to_string();
        assert!(message.contains("'test.s.events'"), "{message}");
        assert!(message.contains("no record batch describes"), "{message}");
        assert!(
            message.contains("https://spiceai.org/docs/api/arrow-flight-sql"),
            "{message}"
        );
        assert_eq!(
            tonic::Status::from(err).code(),
            tonic::Code::InvalidArgument,
            "the client sent it, so no retry of the same stream can succeed"
        );
    }
}
