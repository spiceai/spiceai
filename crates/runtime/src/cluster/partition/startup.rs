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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use app::{App, spicepod::component::runtime::Scheduler as SchedulerConfig};

use datafusion::{
    execution::SessionStateBuilder, logical_expr::Expr, prelude::SessionContext,
    sql::TableReference,
};
use datafusion_proto::bytes::Serializeable;
use object_store::ObjectStore;
use object_store::prefix::PrefixStore;
use runtime_proto::{
    AllocateInitialPartitionsRequest, cluster_service_client::ClusterServiceClient,
};
use snafu::prelude::*;
use spicepod::partitioning::PartitionedBy;
use tonic::transport::Channel;

use super::PartitionManager;
use crate::{
    Runtime, accelerated_table::AcceleratedTable, cluster::partition::metadata::PartitionValue,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build object store for partition metadata: {source}"))]
    ObjectStoreBuild {
        source: crate::cluster::scheduler_registry::Error,
    },

    #[snafu(display("Failed to initialize partition metadata for table {table}: {source}"))]
    PartitionMetadataInit {
        table: String,
        source: super::manager::Error,
    },

    #[snafu(display("Failed to discover partitions for table {table}: {source}"))]
    PartitionDiscovery {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Scheduler configuration is missing state_location"))]
    MissingStateLocation,

    #[snafu(display("No schedulers available to request partition allocation"))]
    NoSchedulersAvailable,

    #[snafu(display("Failed to connect to scheduler at {url}: {source}"))]
    SchedulerConnection {
        url: String,
        source: tonic::transport::Error,
    },

    #[snafu(display("Failed to request partition allocation: {source}"))]
    PartitionAllocationRequest { source: tonic::Status },

    #[snafu(display("Failed to deserialize partition expression: {source}"))]
    PartitionExpressionDeserialization {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Failed to register table {table}: {source}"))]
    RegisterTable {
        table: String,
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Timed out waiting for table {table} to be registered"))]
    TableRegistrationTimeout { table: String },

    #[snafu(display("Table {table} is not an accelerated table"))]
    NotAcceleratedTable { table: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Builds an object store for partition metadata from scheduler configuration.
pub async fn build_partition_metadata_store(
    rt: &Runtime,
    config: &SchedulerConfig,
) -> Result<Arc<dyn ObjectStore>> {
    let (store, prefix) =
        crate::cluster::scheduler_registry::build_object_store(rt, &config.state_location, config)
            .await
            .context(ObjectStoreBuildSnafu)?;

    if prefix.is_empty() {
        Ok(store)
    } else {
        Ok(Arc::new(PrefixStore::new(store, prefix)))
    }
}

/// Initialize acceleration partition metadata for all accelerated tables on scheduler startup.
///
/// 1. Find all tables needing accelerated partitions
/// 2. For each table without partition metadata:
///    - Discover all required partitions from source
///    - Update with all partitions marked as unassigned
pub async fn initialize_partition_metadata(
    rt: &Arc<Runtime>,
    partition_manager: &PartitionManager,
) -> Result<()> {
    let Some(app) = rt.app().read().await.clone() else {
        tracing::warn!("No application found in runtime during partition metadata initialization");
        return Ok(());
    };
    let tables = accelerated_tables(&app);

    if tables.is_empty() {
        tracing::debug!("No accelerated tables with partitioning configured");
        return Ok(());
    }

    // Get existing tables from partition manager
    let existing_tables: HashSet<String> = partition_manager
        .list_tables()
        .await
        .context(PartitionMetadataInitSnafu {
            table: "<list>".to_string(),
        })?
        .into_iter()
        .collect();

    for (table, partitioning) in tables {
        let table_name = table.to_string();

        if existing_tables.contains(&table_name) {
            tracing::debug!(
                table = %table_name,
                "Partition metadata already exists, skipping initialization"
            );
            continue;
        }

        let partition_values = match table_partition_values(&table, &partitioning, rt).await {
            Ok(values) => values,
            Err(e) => {
                tracing::warn!(
                    table = %table_name,
                    error = %e,
                    "Failed to discover partition values, leaving blank metadata"
                );
                continue;
            }
        };

        match partition_manager
            .set_unassigned_partitions(&table, partition_values)
            .await
        {
            Ok(()) => {
                tracing::info!(
                    table = %table_name,
                    "Initialized partition metadata"
                );
            }
            Err(e) => {
                tracing::warn!(
                    table = %table_name,
                    error = %e,
                    "Failed to set unassigned partitions"
                );
            }
        }
    }

    Ok(())
}

/// Query the source table provider for partition values for a given table.
///
/// This builds a SQL query to get distinct values from the partition columns.
async fn table_partition_values(
    table: &TableReference,
    partitioning: &[PartitionedBy],
    rt: &Arc<Runtime>,
) -> Result<Vec<PartitionValue>> {
    let table_name = table.to_string();

    // Build SQL query to get distinct partition values
    // For single partition column: SELECT DISTINCT partition_col as  FROM table
    // For multiple columns: SELECT DISTINCT partition_col1, partition_col2, ... FROM table
    let partition_exprs: Vec<String> = partitioning
        .iter()
        .map(|p| {
            let PartitionedBy { name, expression } = p;
            format!("{expression} AS {name}")
        })
        .collect();

    if partition_exprs.is_empty() {
        return Ok(Vec::new());
    }

    let cols_str = partition_exprs.join(", ");
    let sql = format!("SELECT DISTINCT {cols_str} FROM {table_name}");

    tracing::debug!(
        table = %table_name,
        sql = %sql,
        "Querying for partition values"
    );

    let batches = execute_partition_discovery_query(rt, table, &sql).await?;

    // Convert record batches to partition value strings
    let mut partition_values = Vec::new();

    for batch in batches {
        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        for row_idx in 0..num_rows {
            // Build partition value string from column values
            let mut value_parts = HashMap::new();

            for col_idx in 0..num_cols {
                let column = batch.column(col_idx);
                let value_str = arrow::util::display::array_value_to_string(column, row_idx)
                    .boxed()
                    .context(PartitionDiscoverySnafu {
                        table: table_name.clone(),
                    })?;
                if let Some(pname) = partitioning.get(col_idx).map(|p| p.expression.clone()) {
                    value_parts.insert(pname, value_str);
                }
            }

            partition_values.push(value_parts);
        }
    }

    tracing::debug!(
        table = %table_name,
        partition_count = partition_values.len(),
        "Discovered partition values"
    );

    Ok(partition_values)
}

/// Wait for the [`TableReference`] to be registered in Runtime.
async fn wait_for_table(table: &TableReference, rt: &Arc<Runtime>) -> bool {
    for _ in 0..5 {
        if rt.datafusion().table_exists(table.clone()) {
            return true;
        }
        let () = tokio::time::sleep(Duration::from_secs(1)).await;
    }
    false
}

/// Executes a SQL query against the underlying table source of an accelerated dataset to discover partition values.
///
/// This function creates a temporary, isolated `SessionContext` to execute the query. It is critical
/// to query the *federated* table (the source) rather than the accelerated table itself, as the
/// acceleration will be empty (for schedulers).
async fn execute_partition_discovery_query(
    rt: &Arc<Runtime>,
    table: &TableReference,
    sql: &str,
) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    let table_name = table.to_string();

    // Wait for table to be registered.
    // TODO: we should call `initialize_partition_metadata` after all datasets registered.
    if !wait_for_table(table, rt).await {
        return Err(Error::TableRegistrationTimeout { table: table_name });
    }

    // Must get table source of `AcceleratedTable` to get true value of partition.
    let Some(acc) = rt.datafusion().get_table(table).await.and_then(|t| {
        t.as_any()
            .downcast_ref::<AcceleratedTable>()
            .map(AcceleratedTable::get_federated_table)
    }) else {
        return Err(Error::NotAcceleratedTable {
            table: table.to_string(),
        });
    };

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new_from_existing(rt.datafusion().ctx.state()).build(),
    );

    // Must deregister table in this context before registering source table.
    let _ = ctx.deregister_table(table.clone());
    ctx.register_table(table.clone(), acc.table_provider().await)
        .context(RegisterTableSnafu {
            table: table_name.clone(),
        })?;

    // Execute query
    let batches = ctx
        .sql(sql)
        .await
        .boxed()
        .context(PartitionDiscoverySnafu {
            table: table_name.clone(),
        })?
        .collect()
        .await
        .boxed()
        .context(PartitionDiscoverySnafu { table: table_name })?;

    Ok(batches)
}

/// Helper to find all tables with acceleration partitioning configured, along with their partitioning columns.
#[must_use]
pub fn accelerated_tables(app: &Arc<App>) -> HashMap<TableReference, Vec<PartitionedBy>> {
    let ds = app.datasets.iter().filter_map(|ds| {
        if let Some(acc) = &ds.acceleration
            && !acc.partition_by.is_empty()
        {
            return Some((
                TableReference::parse_str(&ds.name),
                acc.partition_by.clone(),
            ));
        }

        None
    });
    let views = app.views.iter().filter_map(|view| {
        if let Some(acc) = &view.acceleration
            && !acc.partition_by.is_empty()
        {
            return Some((
                TableReference::parse_str(&view.name),
                acc.partition_by.clone(),
            ));
        }

        None
    });
    ds.chain(views).collect()
}

/// Request initial partition allocations from a scheduler.
///
/// This is called by the executor on startup to get its assigned partitions.
pub async fn executor_request_initial_partitions(
    mut client: ClusterServiceClient<Channel>,
    executor_url: String,
) -> Result<HashMap<TableReference, Vec<Expr>>> {
    let response = client
        .allocate_initial_partitions(AllocateInitialPartitionsRequest {
            executor_id: executor_url,
        })
        .await
        .context(PartitionAllocationRequestSnafu)?
        .into_inner();

    let mut result = HashMap::new();

    for (table_name, partitions) in response.table_partitions {
        let table_ref = TableReference::parse_str(&table_name);
        let mut exprs = Vec::new();

        for item in partitions.items {
            let expr = Expr::from_bytes(&item).context(PartitionExpressionDeserializationSnafu)?;
            exprs.push(expr);
        }

        result.insert(table_ref, exprs);
    }

    Ok(result)
}
