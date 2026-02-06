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
use arrow::array::RecordBatch;
use datafusion::{
    execution::{SessionStateBuilder, runtime_env::RuntimeEnvBuilder},
    prelude::SessionContext,
    sql::TableReference,
};
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::prefix::PrefixStore;
use runtime_object_store::registry::SpiceObjectStoreRegistry;
use snafu::prelude::*;
use spicepod::partitioning::PartitionedBy;

use super::PartitionManager;
use crate::{Runtime, accelerated_table::AcceleratedTable};

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
) -> Result<Vec<HashMap<String, String>>> {
    let table_name = table.to_string();

    // Build SQL query to get distinct partition values
    // For single partition column: SELECT DISTINCT partition_col as  FROM table
    // For multiple columns: SELECT DISTINCT partition_col1, partition_col2, ... FROM table
    let partition_exprs: Vec<String> = partitioning
        .iter()
        .map(|p| match p {
            PartitionedBy { name, expression } => format!("{expression} AS {name}"),
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

    // Wait for table to be registered.
    // TODO: we should call `initialize_partition_metadata` after all datasets registered.
    if !wait_for_table(&table, &rt).await {
        return Err(Error::PartitionDiscovery {
            table: table.to_string(),
            source: Box::from(format!("table does not exist"))
                as Box<dyn std::error::Error + Send + Sync>,
        });
    }

    // Must get table source of `AcceleratedTable` to get true value of partition.
    let Some(acc) = rt.datafusion().get_table(&table).await.and_then(|t| {
        t.as_any()
            .downcast_ref::<AcceleratedTable>()
            .map(|acc| acc.get_federated_table())
    }) else {
        return Err(Error::PartitionDiscovery {
            table: table.to_string(),
            source: Box::from(format!("table is not an acceleration, somehow"))
                as Box<dyn std::error::Error + Send + Sync>,
        });
    };

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::default()
            .with_runtime_env(
                RuntimeEnvBuilder::default()
                    .with_object_store_registry(Arc::new(SpiceObjectStoreRegistry::new(
                        rt.tokio_io_runtime(),
                    )))
                    .build_arc()
                    .expect("msg"),
            )
            .build(),
    );
    ctx.register_table(table.clone(), acc.table_provider().await)
        .expect("");

    // Execute query
    let batches = ctx
        .sql(&sql)
        .await
        .boxed()
        .context(PartitionDiscoverySnafu {
            table: table_name.clone(),
        })?
        .collect()
        .await
        .boxed()
        .context(PartitionDiscoverySnafu {
            table: table_name.clone(),
        })?;

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
                if let Some(pname) = partitioning.get(col_idx).map(|p| p.name.clone()) {
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
        let _ = tokio::time::sleep(Duration::from_secs(1)).await;
    }
    return false;
}

/// Helper to find all tables with acceleration partitioning configured, along with their partitioning columns.
pub fn accelerated_tables(app: &Arc<App>) -> HashMap<TableReference, Vec<PartitionedBy>> {
    let ds = app.datasets.iter().filter_map(|ds| {
        if let Some(acc) = &ds.acceleration {
            if !acc.partition_by.is_empty() {
                return Some((
                    TableReference::parse_str(&ds.name),
                    acc.partition_by.clone(),
                ));
            }
        }
        None
    });
    let views = app.views.iter().filter_map(|view| {
        if let Some(acc) = &view.acceleration {
            if !acc.partition_by.is_empty() {
                return Some((
                    TableReference::parse_str(&view.name),
                    acc.partition_by.clone(),
                ));
            }
        }
        None
    });
    ds.chain(views).collect()
}
