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

//! Federated task history table provider for cluster-wide task history queries.
//!
//! This module provides a `TableProvider` that queries task history across all
//! schedulers in a cluster, combining results from the local table and all peer
//! schedulers.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::compute::{SortOptions, sort_to_indices, take};
use arrow_ipc::reader::StreamReader;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::flightsql::FlightSqlClient;
use datafusion::catalog::Session;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, execute_stream};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use flight_client::cookie::CookieStore;
use futures::TryStreamExt;
use futures::future::join_all;
use runtime_proto::GetTaskHistoryRequest;
use runtime_proto::cluster_service_client::ClusterServiceClient;
use tokio::sync::RwLock;
use tonic::transport::{ClientTlsConfig, Endpoint};

use crate::cluster::SchedulerPeers;
use crate::datafusion::SPICE_RUNTIME_SCHEMA;
use crate::task_history::DEFAULT_TASK_HISTORY_TABLE;

/// How long a scan waits for one peer scheduler or executor, connection included.
/// The scan runs while its query holds a query admission permit, so a node that
/// accepts the request and never replies must not keep that permit.
const NODE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Fails `request` to `node` if it has not finished within [`NODE_REQUEST_TIMEOUT`].
async fn within_node_timeout(
    node: String,
    request: impl Future<Output = Result<Vec<RecordBatch>, (String, String)>>,
) -> Result<Vec<RecordBatch>, (String, String)> {
    tokio::time::timeout(NODE_REQUEST_TIMEOUT, request)
        .await
        .unwrap_or_else(|_| {
            Err((
                node,
                format!("no reply within {}s", NODE_REQUEST_TIMEOUT.as_secs()),
            ))
        })
}

/// A federated table provider that queries task history across all nodes in a cluster.
///
/// `scan()` contacts no other node. The plan it returns, when executed:
/// 1. Queries the local `task_history` table directly (via stored reference)
/// 2. Fans out to all peer schedulers via the `GetTaskHistory` RPC
/// 3. Fans out to all connected executors via `FlightSQL`
/// 4. Combines all results into a single result set sorted by `start_time`
///
/// If any peer or executor fails, or does not reply within [`NODE_REQUEST_TIMEOUT`],
/// the entire query fails with an error containing the identifiers of the failed nodes.
pub struct FederatedTaskHistoryTable {
    /// Schema for the `task_history` table (with `node_id` column)
    schema: SchemaRef,
    /// Local `task_history` table provider (direct reference to avoid recursion)
    local_table: Arc<dyn TableProvider>,
    /// Peer schedulers for fan-out queries
    scheduler_peers: Arc<RwLock<SchedulerPeers>>,
    /// `FlightSQL` clients for connected executors
    executor_flight_clients: Arc<RwLock<HashMap<String, FlightSqlClient>>>,
    /// TLS configuration for connecting to peer schedulers
    client_tls_config: Option<ClientTlsConfig>,
    /// This scheduler's advertise address (to exclude from peer queries)
    local_node_id: String,
}

impl std::fmt::Debug for FederatedTaskHistoryTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederatedTaskHistoryTable")
            .field("schema", &self.schema)
            .field("local_node_id", &self.local_node_id)
            .finish_non_exhaustive()
    }
}

impl FederatedTaskHistoryTable {
    /// Creates a new federated task history table.
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        local_table: Arc<dyn TableProvider>,
        scheduler_peers: Arc<RwLock<SchedulerPeers>>,
        executor_flight_clients: Arc<RwLock<HashMap<String, FlightSqlClient>>>,
        client_tls_config: Option<ClientTlsConfig>,
        local_node_id: String,
    ) -> Self {
        Self {
            schema,
            local_table,
            scheduler_peers,
            executor_flight_clients,
            client_tls_config,
            local_node_id,
        }
    }

    /// Executes the query against a single peer scheduler.
    async fn query_peer(
        peer_address: String,
        sql: String,
        client_tls_config: Option<ClientTlsConfig>,
    ) -> Result<Vec<RecordBatch>, (String, String)> {
        // Build the gRPC endpoint
        let endpoint_url = normalize_scheduler_endpoint(&peer_address, client_tls_config.is_some());
        let mut endpoint = Endpoint::from_shared(endpoint_url.clone())
            .map_err(|e| (peer_address.clone(), format!("invalid endpoint: {e}")))?;

        if let Some(tls_config) = client_tls_config {
            endpoint = endpoint
                .tls_config(tls_config)
                .map_err(|e| (peer_address.clone(), format!("TLS config error: {e}")))?;
        }

        // Connect to the peer
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| (peer_address.clone(), format!("connection failed: {e}")))?;

        let mut client = ClusterServiceClient::new(channel)
            .max_encoding_message_size(usize::MAX)
            .max_decoding_message_size(usize::MAX);

        // Execute the query
        let request = GetTaskHistoryRequest { sql };
        let response = client
            .get_task_history(request)
            .await
            .map_err(|e| (peer_address.clone(), format!("RPC failed: {e}")))?;

        let arrow_ipc = response.into_inner().arrow_ipc;

        // Decode the Arrow IPC response
        if arrow_ipc.is_empty() {
            return Ok(Vec::new());
        }

        let cursor = std::io::Cursor::new(arrow_ipc);
        let reader = StreamReader::try_new(cursor, None).map_err(|e| {
            (
                peer_address.clone(),
                format!("failed to read Arrow IPC: {e}"),
            )
        })?;

        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().map_err(|e| {
            (
                peer_address.clone(),
                format!("failed to collect batches: {e}"),
            )
        })?;

        Ok(batches)
    }

    /// Executes a query against a single executor via `FlightSQL`.
    async fn query_executor(
        executor_id: String,
        client: FlightSqlClient,
        sql: String,
        table_ref: String,
    ) -> Result<Vec<RecordBatch>, (String, String)> {
        let cookie_store = Arc::new(CookieStore::new());
        let batches: Vec<RecordBatch> =
            data_components::flightsql::query_to_stream(client, sql, cookie_store, table_ref)
                .try_collect()
                .await
                .map_err(|e| (executor_id, format!("FlightSQL query failed: {e}")))?;
        Ok(batches)
    }
}

#[async_trait]
impl TableProvider for FederatedTaskHistoryTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let table_ref = format!("\"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\"");
        let sql = build_peer_sql(&table_ref, filters, limit)?;
        // Keep the full schema here; the projection applies once the rows of every
        // node are combined.
        let local_plan = self.local_table.scan(state, None, filters, limit).await?;
        let projected_schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => Arc::clone(&self.schema),
        };
        Ok(Arc::new(FederatedTaskHistoryExec {
            read: FederatedRead {
                schema: Arc::clone(&self.schema),
                projection: projection.cloned(),
                local_plan,
                scheduler_peers: Arc::clone(&self.scheduler_peers),
                executor_flight_clients: Arc::clone(&self.executor_flight_clients),
                client_tls_config: self.client_tls_config.clone(),
                local_node_id: self.local_node_id.clone(),
                sql,
                table_ref,
            },
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
        }))
    }

    /// Delegate inserts to the local task history table.
    ///
    /// The federated table only federates reads across cluster nodes. Writes always
    /// go to the local table since each node manages its own task history.
    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.local_table.insert_into(state, input, insert_op).await
    }
}

/// What a [`FederatedTaskHistoryExec`] reads: this scheduler's rows through
/// `local_plan`, and `sql` from every peer scheduler and connected executor.
#[derive(Clone)]
struct FederatedRead {
    /// The full table schema; `projection` applies after the rows are combined.
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    local_plan: Arc<dyn ExecutionPlan>,
    scheduler_peers: Arc<RwLock<SchedulerPeers>>,
    executor_flight_clients: Arc<RwLock<HashMap<String, FlightSqlClient>>>,
    client_tls_config: Option<ClientTlsConfig>,
    local_node_id: String,
    sql: String,
    table_ref: String,
}

impl FederatedRead {
    /// Reads every node and returns the combined rows sorted by `start_time`.
    async fn rows(self, context: Arc<TaskContext>) -> DataFusionResult<Vec<RecordBatch>> {
        let peers = self.scheduler_peers.read().await;
        let peer_addresses =
            crate::cluster::remote_scheduler_addresses(&peers, &self.local_node_id);
        drop(peers);

        let executor_clients: Vec<(String, FlightSqlClient)> = {
            let clients = self.executor_flight_clients.read().await;
            clients
                .iter()
                .map(|(id, client)| (id.clone(), client.clone()))
                .collect()
        };

        tracing::debug!(
            "FederatedTaskHistoryTable executing federated query to {} peers and {} executors: {}",
            peer_addresses.len(),
            executor_clients.len(),
            self.sql,
        );

        let peer_futures: Vec<_> = peer_addresses
            .into_iter()
            .map(|addr| {
                within_node_timeout(
                    addr.clone(),
                    FederatedTaskHistoryTable::query_peer(
                        addr,
                        self.sql.clone(),
                        self.client_tls_config.clone(),
                    ),
                )
            })
            .collect();

        let executor_futures: Vec<_> = executor_clients
            .into_iter()
            .map(|(executor_id, client)| {
                within_node_timeout(
                    executor_id.clone(),
                    FederatedTaskHistoryTable::query_executor(
                        executor_id,
                        client,
                        self.sql.clone(),
                        self.table_ref.clone(),
                    ),
                )
            })
            .collect();

        let local_future = async {
            execute_stream(Arc::clone(&self.local_plan), context)
                .map_err(|e| ("local".to_string(), format!("execution failed: {e}")))?
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| ("local".to_string(), format!("collect failed: {e}")))
        };

        let (local_result, peer_results, executor_results) = futures::join!(
            local_future,
            join_all(peer_futures),
            join_all(executor_futures)
        );

        let mut all_batches = Vec::new();
        let mut failures = Vec::new();
        for result in std::iter::once(local_result)
            .chain(peer_results)
            .chain(executor_results)
        {
            match result {
                Ok(batches) => all_batches.extend(batches),
                Err((node, error)) => failures.push(format!("{node}: {error}")),
            }
        }

        if !failures.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Failed to collect cluster task history: nodes failed: [{}]",
                failures.join(", ")
            )));
        }

        let sorted = sort_batches_by_start_time(&all_batches, &self.schema)?;
        match &self.projection {
            Some(indices) => sorted
                .iter()
                .map(|batch| {
                    batch
                        .project(indices)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                })
                .collect(),
            None => Ok(sorted),
        }
    }
}

/// Reads `runtime.task_history` from every cluster node when it executes, so
/// that planning a query waits on no other node.
struct FederatedTaskHistoryExec {
    read: FederatedRead,
    properties: Arc<PlanProperties>,
}

impl std::fmt::Debug for FederatedTaskHistoryExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederatedTaskHistoryExec")
            .field("sql", &self.read.sql)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for FederatedTaskHistoryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FederatedTaskHistoryExec: sql={}", self.read.sql)
    }
}

impl ExecutionPlan for FederatedTaskHistoryExec {
    fn name(&self) -> &'static str {
        "FederatedTaskHistoryExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        )
            -> DataFusionResult<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> DataFusionResult<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "FederatedTaskHistoryExec has one partition; got {partition}"
            )));
        }
        let rows = futures::stream::once(self.read.clone().rows(context))
            .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), rows)))
    }
}

/// Sorts the collected record batches by the `start_time` column (ascending).
///
/// Concatenates all batches, sorts by `start_time`, and returns a single sorted batch.
/// Returns an empty vec if the input is empty, or a single concatenated (unsorted) batch
/// if the `start_time` column is missing from the schema.
fn sort_batches_by_start_time(
    batches: &[RecordBatch],
    schema: &SchemaRef,
) -> DataFusionResult<Vec<RecordBatch>> {
    let non_empty: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
    if non_empty.is_empty() {
        return Ok(Vec::new());
    }

    let concatenated = arrow::compute::concat_batches(schema, non_empty)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let Ok(start_time_idx) = schema.index_of("start_time") else {
        return Ok(vec![concatenated]);
    };

    let sort_column = concatenated.column(start_time_idx);
    let indices = sort_to_indices(
        sort_column,
        Some(SortOptions {
            descending: false,
            nulls_first: false,
        }),
        None,
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let sorted_columns: Vec<_> = concatenated
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None))
        .collect::<Result<_, _>>()
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let sorted_batch = RecordBatch::try_new(Arc::clone(schema), sorted_columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    Ok(vec![sorted_batch])
}

/// Builds the SQL query to send to peer schedulers and executors.
///
/// Uses `DataFusion`'s SQL unparser to convert filter expressions into valid SQL,
/// ensuring literals and identifiers are encoded correctly for remote execution.
///
/// Returns an error if any filter expression cannot be converted to SQL, to prevent
/// silently returning unfiltered (and therefore incorrect) results.
fn build_peer_sql(
    table_ref: &str,
    filters: &[Expr],
    limit: Option<usize>,
) -> DataFusionResult<String> {
    if filters.is_empty() {
        return Ok(if let Some(limit) = limit {
            format!("SELECT * FROM {table_ref} LIMIT {limit}")
        } else {
            format!("SELECT * FROM {table_ref}")
        });
    }

    let unparser = datafusion::sql::unparser::Unparser::default();
    let mut sql_parts = Vec::with_capacity(filters.len());
    for expr in filters {
        let sql_expr = unparser.expr_to_sql(expr).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to convert filter expression to SQL for federated task_history query: {e}"
            ))
        })?;
        sql_parts.push(sql_expr.to_string());
    }

    let where_clause = sql_parts.join(" AND ");

    Ok(if let Some(limit) = limit {
        format!("SELECT * FROM {table_ref} WHERE {where_clause} LIMIT {limit}")
    } else {
        format!("SELECT * FROM {table_ref} WHERE {where_clause}")
    })
}

/// Normalizes a scheduler endpoint address to a URL with scheme.
fn normalize_scheduler_endpoint(address: &str, tls_enabled: bool) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        return address.to_string();
    }

    let scheme = if tls_enabled { "https" } else { "http" };
    format!("{scheme}://{address}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    #[test]
    fn test_normalize_scheduler_endpoint() {
        assert_eq!(
            normalize_scheduler_endpoint("192.168.1.10:50052", false),
            "http://192.168.1.10:50052"
        );
        assert_eq!(
            normalize_scheduler_endpoint("192.168.1.10:50052", true),
            "https://192.168.1.10:50052"
        );
        assert_eq!(
            normalize_scheduler_endpoint("http://192.168.1.10:50052", true),
            "http://192.168.1.10:50052"
        );
        assert_eq!(
            normalize_scheduler_endpoint("https://192.168.1.10:50052", false),
            "https://192.168.1.10:50052"
        );
    }

    #[test]
    fn test_build_peer_sql() {
        let table_ref = "\"runtime\".\"task_history\"";

        // No filters, no limit
        assert_eq!(
            build_peer_sql(table_ref, &[], None).expect("no filters, no limit"),
            "SELECT * FROM \"runtime\".\"task_history\""
        );

        // With limit
        assert_eq!(
            build_peer_sql(table_ref, &[], Some(100)).expect("limit only"),
            "SELECT * FROM \"runtime\".\"task_history\" LIMIT 100"
        );
    }

    #[test]
    fn test_build_peer_sql_with_filters() {
        use datafusion::prelude::*;

        let table_ref = "\"runtime\".\"task_history\"";

        // Single filter — string literal should be properly quoted via SQL unparser
        let filter = col("status").eq(lit("completed"));
        assert_eq!(
            build_peer_sql(table_ref, &[filter], None).expect("single filter"),
            "SELECT * FROM \"runtime\".\"task_history\" WHERE (\"status\" = 'completed')"
        );

        // Filter with limit
        let filter = col("task_id").eq(lit("task-123"));
        assert_eq!(
            build_peer_sql(table_ref, &[filter], Some(50)).expect("filter with limit"),
            "SELECT * FROM \"runtime\".\"task_history\" WHERE (task_id = 'task-123') LIMIT 50"
        );

        // Multiple filters (combined with AND)
        let filter1 = col("status").eq(lit("running"));
        let filter2 = col("execution_time").gt(lit(100));
        assert_eq!(
            build_peer_sql(table_ref, &[filter1, filter2], None).expect("multiple filters"),
            "SELECT * FROM \"runtime\".\"task_history\" WHERE (\"status\" = 'running') AND (execution_time > 100)"
        );
    }

    #[tokio::test]
    async fn test_scan_with_sparse_projection_does_not_double_project_local_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col0", DataType::Utf8, false),
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Utf8, false),
            Field::new("col3", DataType::Utf8, false),
            Field::new("col4", DataType::Utf8, false),
            Field::new("col5", DataType::Utf8, false),
            Field::new("col6", DataType::Utf8, false),
            Field::new("col7", DataType::Utf8, false),
            Field::new("col8", DataType::Utf8, false),
            Field::new("col9", DataType::Utf8, false),
            Field::new("col10", DataType::Utf8, false),
        ]));

        let columns: Vec<ArrayRef> = (0..11)
            .map(|i| Arc::new(StringArray::from(vec![format!("value_{i}")])) as ArrayRef)
            .collect();

        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
            .expect("record batch with full schema should build");
        let local_table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
            .expect("local memtable should build");

        let table = FederatedTaskHistoryTable::new(
            Arc::clone(&schema),
            Arc::new(local_table),
            Arc::new(RwLock::new(SchedulerPeers::new())),
            Arc::new(RwLock::new(HashMap::new())),
            None,
            "local-scheduler".to_string(),
        );

        let ctx = SessionContext::new();
        let projection = vec![7, 10];
        let plan = table
            .scan(&ctx.state(), Some(&projection), &[], None)
            .await
            .expect("scan with sparse projection should succeed");

        let results = collect(plan, ctx.task_ctx())
            .await
            .expect("collecting projected federated results should succeed");

        assert_eq!(results.len(), 1, "expected a single result batch");
        assert_eq!(results[0].num_rows(), 1, "expected one row in result batch");
        assert_eq!(
            results[0].schema().fields().len(),
            2,
            "projection should return exactly two columns"
        );
        assert_eq!(results[0].schema().field(0).name(), "col7");
        assert_eq!(results[0].schema().field(1).name(), "col10");

        let projected_col_0 = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("projected column 0 should be a StringArray");
        let projected_col_1 = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("projected column 1 should be a StringArray");

        assert_eq!(projected_col_0.value(0), "value_7");
        assert_eq!(projected_col_1.value(0), "value_10");
    }

    /// Schedulers register under their instance identity, so this scheduler finds
    /// itself among its peers by advertise address and does not query itself.
    #[tokio::test]
    async fn the_local_scheduler_is_matched_by_advertise_address_not_identity() {
        let schema = Arc::new(Schema::new(vec![Field::new("col0", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["local"])) as ArrayRef],
        )
        .expect("record batch should build");
        let local_table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
            .expect("local memtable should build");
        let peers = SchedulerPeers::from([(
            "sched-a".to_string(),
            crate::cluster::SchedulerEntry {
                scheduler_id: "sched-a".to_string(),
                instance_id: uuid::Uuid::new_v4(),
                advertise_address: "127.0.0.1:50052".to_string(),
                grpc_address: "127.0.0.1:50051".to_string(),
                http_address: "127.0.0.1:8090".to_string(),
                started_at_ms: 0,
                ttl_ms: 30_000,
                build_version: "test".to_string(),
                labels: HashMap::new(),
            },
        )]);

        let table = FederatedTaskHistoryTable::new(
            Arc::clone(&schema),
            Arc::new(local_table),
            Arc::new(RwLock::new(peers)),
            Arc::new(RwLock::new(HashMap::new())),
            None,
            "127.0.0.1:50052".to_string(),
        );

        let ctx = SessionContext::new();
        let plan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("a scan that reaches no peer should succeed");
        let results = collect(plan, ctx.task_ctx())
            .await
            .expect("collecting the local rows should succeed");
        assert_eq!(results.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }

    /// Planning contacts no peer. A peer that accepts the connection and never
    /// replies fails the read at `NODE_REQUEST_TIMEOUT` instead of holding it,
    /// and its query's admission permit, for as long as the peer stays stuck.
    #[tokio::test(start_paused = true)]
    async fn a_peer_that_never_replies_fails_the_read_not_the_plan() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should have an address")
            .to_string();
        let _stuck_peer = tokio::spawn(async move {
            let mut connections = Vec::new();
            while let Ok((connection, _)) = listener.accept().await {
                connections.push(connection);
            }
        });
        let schema = Arc::new(Schema::new(vec![Field::new("col0", DataType::Utf8, false)]));
        let local_table = MemTable::try_new(Arc::clone(&schema), vec![vec![]])
            .expect("local memtable should build");
        let peers = SchedulerPeers::from([(
            "sched-b".to_string(),
            crate::cluster::SchedulerEntry {
                scheduler_id: "sched-b".to_string(),
                instance_id: uuid::Uuid::new_v4(),
                advertise_address: address.clone(),
                grpc_address: "127.0.0.1:50051".to_string(),
                http_address: "127.0.0.1:8090".to_string(),
                started_at_ms: 0,
                ttl_ms: 30_000,
                build_version: "test".to_string(),
                labels: HashMap::new(),
            },
        )]);
        let table = FederatedTaskHistoryTable::new(
            Arc::clone(&schema),
            Arc::new(local_table),
            Arc::new(RwLock::new(peers)),
            Arc::new(RwLock::new(HashMap::new())),
            None,
            "127.0.0.1:50052".to_string(),
        );

        let ctx = SessionContext::new();
        let planned_at = tokio::time::Instant::now();
        let plan = tokio::time::timeout(
            NODE_REQUEST_TIMEOUT,
            table.scan(&ctx.state(), None, &[], None),
        )
        .await
        .expect("planning should not wait on the peer")
        .expect("planning should succeed without the peer");
        assert_eq!(
            planned_at.elapsed(),
            Duration::ZERO,
            "planning should not wait on the peer"
        );

        let error = tokio::time::timeout(NODE_REQUEST_TIMEOUT * 10, collect(plan, ctx.task_ctx()))
            .await
            .expect("the read should end at the peer deadline, not wait for the peer")
            .expect_err("a peer that never replies should fail the read");
        assert!(
            error
                .to_string()
                .contains(&format!("{address}: no reply within 30s")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_sort_batches_by_start_time_sorts_across_batches() {
        use arrow::array::TimestampNanosecondArray;
        use arrow::datatypes::TimeUnit;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "start_time",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ]));

        // Batch 1: timestamps 300, 100 (unsorted)
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["c", "a"])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![300, 100]).with_timezone("UTC"))
                    as ArrayRef,
            ],
        )
        .expect("batch1");

        // Batch 2: timestamp 200 (interleaved between batch1 values)
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["b"])) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(vec![200]).with_timezone("UTC"))
                    as ArrayRef,
            ],
        )
        .expect("batch2");

        let sorted =
            sort_batches_by_start_time(&[batch1, batch2], &schema).expect("sort should succeed");

        assert_eq!(sorted.len(), 1, "should produce a single sorted batch");
        let batch = &sorted[0];
        assert_eq!(batch.num_rows(), 3);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id column");
        assert_eq!(ids.value(0), "a"); // start_time=100
        assert_eq!(ids.value(1), "b"); // start_time=200
        assert_eq!(ids.value(2), "c"); // start_time=300
    }

    #[test]
    fn test_sort_batches_empty_input() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "start_time",
            DataType::Utf8,
            false,
        )]));
        let result = sort_batches_by_start_time(&[], &schema).expect("empty input should succeed");
        assert!(result.is_empty());
    }
}
