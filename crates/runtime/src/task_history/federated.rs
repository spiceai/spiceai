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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

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
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::execution_plan::execute_stream;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
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

/// A federated table provider that queries task history across all nodes in a cluster.
///
/// When `scan()` is called, this provider:
/// 1. Queries the local `task_history` table directly (via stored reference)
/// 2. Fans out to all peer schedulers via the `GetTaskHistory` RPC
/// 3. Fans out to all connected executors via `FlightSQL`
/// 4. Combines all results into a single result set sorted by `start_time`
///
/// If any peer or executor fails, the entire query fails with an error containing
/// the identifiers of the failed nodes.
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
    ) -> Result<Vec<RecordBatch>, (String, String)> {
        let cookie_store = Arc::new(CookieStore::new());
        let batches: Vec<RecordBatch> =
            data_components::flightsql::query_to_stream(client, sql, cookie_store)
                .try_collect()
                .await
                .map_err(|e| (executor_id, format!("FlightSQL query failed: {e}")))?;
        Ok(batches)
    }

    /// Executes a scan directly on the local table provider.
    async fn query_local(
        local_table: Arc<dyn TableProvider>,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>, (String, String)> {
        let plan = local_table
            .scan(state, projection, filters, limit)
            .await
            .map_err(|e| ("local".to_string(), format!("scan failed: {e}")))?;

        let task_ctx = state.task_ctx();
        let stream = execute_stream(plan, task_ctx)
            .map_err(|e| ("local".to_string(), format!("execution failed: {e}")))?;

        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| ("local".to_string(), format!("collect failed: {e}")))?;

        Ok(batches)
    }
}

#[async_trait]
impl TableProvider for FederatedTaskHistoryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        // Get the list of peer schedulers
        let peers = self.scheduler_peers.read().await;
        let peer_addresses: Vec<String> = peers
            .keys()
            .filter(|addr| *addr != &self.local_node_id)
            .cloned()
            .collect();
        drop(peers);

        // Get the list of connected executors' FlightSQL clients
        let executor_clients: Vec<(String, FlightSqlClient)> = {
            let clients = self.executor_flight_clients.read().await;
            clients
                .iter()
                .map(|(id, client)| (id.clone(), client.clone()))
                .collect()
        };

        // Build the SQL query to send to peers and executors
        let table_ref = format!("\"{SPICE_RUNTIME_SCHEMA}\".\"{DEFAULT_TASK_HISTORY_TABLE}\"");
        let sql = build_peer_sql(&table_ref, filters, limit)?;

        tracing::debug!(
            "FederatedTaskHistoryTable executing federated query to {} peers and {} executors: {sql}",
            peer_addresses.len(),
            executor_clients.len(),
        );

        // Fan out to all peer schedulers in parallel
        let peer_futures: Vec<_> = peer_addresses
            .into_iter()
            .map(|addr| {
                let sql = sql.clone();
                let tls_config = self.client_tls_config.clone();
                async move { Self::query_peer(addr, sql, tls_config).await }
            })
            .collect();

        // Fan out to all executors via FlightSQL in parallel
        let executor_futures: Vec<_> = executor_clients
            .into_iter()
            .map(|(executor_id, client)| {
                let sql = sql.clone();
                async move { Self::query_executor(executor_id, client, sql).await }
            })
            .collect();

        // Execute local query directly on the stored table provider
        let local_table = Arc::clone(&self.local_table);
        let local_filters: Vec<Expr> = filters.to_vec();
        let local_limit = limit;

        // Query local batches with the full schema and apply projection once after
        // federating local + peer + executor results.
        let local_result =
            Self::query_local(local_table, state, None, &local_filters, local_limit).await;

        // Wait for all peer and executor results
        let (peer_results, executor_results) =
            futures::join!(join_all(peer_futures), join_all(executor_futures));

        // Collect all results, tracking failures
        let mut all_batches = Vec::new();
        let mut failures = Vec::new();

        match local_result {
            Ok(batches) => all_batches.extend(batches),
            Err((node, error)) => failures.push(format!("{node}: {error}")),
        }

        for result in peer_results {
            match result {
                Ok(batches) => all_batches.extend(batches),
                Err((node, error)) => failures.push(format!("{node}: {error}")),
            }
        }

        for result in executor_results {
            match result {
                Ok(batches) => all_batches.extend(batches),
                Err((node, error)) => failures.push(format!("{node}: {error}")),
            }
        }

        // If any node failed, return an error
        if !failures.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Failed to collect cluster task history: nodes failed: [{}]",
                failures.join(", ")
            )));
        }

        // Sort combined results by start_time
        let all_batches = sort_batches_by_start_time(&all_batches, &self.schema)?;

        // Build the execution plan from the collected batches. Keep the source
        // schema unprojected and let MemorySourceConfig apply projection once.
        let schema = Arc::clone(&self.schema);

        let memory_source = if all_batches.is_empty() {
            MemorySourceConfig::try_new(&[], Arc::clone(&schema), projection.cloned())?
        } else {
            MemorySourceConfig::try_new(&[all_batches], schema, projection.cloned())?
        };

        let exec = DataSourceExec::new(Arc::new(memory_source));
        Ok(Arc::new(exec))
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

    let concatenated = arrow::compute::concat_batches(schema, non_empty.into_iter())
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
