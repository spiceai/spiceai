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

use std::{collections::HashMap, sync::Arc};

use app::App;

use datafusion::{
    execution::FunctionRegistry,
    logical_expr::Expr,
    sql::{ResolvedTableReference, TableReference},
};
use datafusion_proto::bytes::Serializeable;
use runtime_datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

use runtime_proto::{
    AllocateInitialPartitionsRequest, cluster_service_client::ClusterServiceClient,
};
use runtime_query_engine::query_engine::QueryEngine;
use snafu::prelude::*;
use spicepod::partitioning::PartitionedBy;
use tonic::transport::Channel;

use super::Result;
use crate::{
    cluster::partition::{
        MissingPartitionKeysSnafu, PartitionAllocationRequestSnafu,
        PartitionExpressionDeserializationSnafu, service::PartitionService,
    },
    datafusion::DataFusion,
};

/// Initialize acceleration partition metadata for all accelerated tables on scheduler startup.
///
/// Delegates to [`PartitionService::seed_table`] per table, which runs the
/// standard source-vs-store diff and writes new partitions as unassigned
/// (no assignment, no executor notification — executors typically haven't
/// connected yet at this point in startup).
///
/// Failures are logged per-table and do not abort the loop.
pub async fn initialize_partition_metadata(
    partition_service: &PartitionService,
    df: &Arc<DataFusion>,
    app: &Arc<App>,
) -> Result<()> {
    let tables = accelerated_tables(app);

    if tables.is_empty() {
        tracing::debug!("No accelerated tables with partitioning configured");
        return Ok(());
    }

    tracing::info!(
        table_count = tables.len(),
        "Initializing partition metadata for accelerated tables"
    );

    for (table, partitioning) in tables {
        if let Err(e) = partition_service
            .seed_table(&table, &partitioning, df.as_ref())
            .await
        {
            tracing::warn!(
                table = %table,
                error = %e,
                "Failed to initialize partition metadata"
            );
        }
    }

    Ok(())
}

/// Verify that all accelerated datasets and views have at least one `partition_by`
/// key configured, which is required for cluster partition assignment.
pub fn validate_partition_keys(app: &App) -> Result<()> {
    for ds in &app.datasets {
        if ds
            .acceleration
            .as_ref()
            .is_some_and(|acc| acc.partition_by.is_empty())
        {
            return MissingPartitionKeysSnafu {
                component_type: "dataset",
                name: ds.name.clone(),
            }
            .fail();
        }
    }
    for view in &app.views {
        if view
            .acceleration
            .as_ref()
            .is_some_and(|acc| acc.partition_by.is_empty())
        {
            return MissingPartitionKeysSnafu {
                component_type: "view",
                name: view.name.clone(),
            }
            .fail();
        }
    }
    Ok(())
}

/// Returns the first accelerated, partitioned table from `app` that isn't
/// yet registered in `df`'s `SessionContext`, or `None` if every such table
/// is ready.
///
/// Used as a readiness gate by scheduler paths whose partition-expression
/// serialization needs each accelerated table's schema to be in the catalog
/// — e.g. `allocate_initial_partitions` and `PartitionAssignmentTask::
/// run_assignment_cycle`. During the scheduler's own `load_datasets()`
/// startup window the answer is `Some(table)`; the caller should defer.
pub async fn first_unready_accelerated_table(
    app: &Arc<App>,
    df: &dyn QueryEngine,
) -> Option<TableReference> {
    // Collect without holding any external lock — the caller is expected to
    // pass an already-snapshotted `Arc<App>` so we don't hold an async
    // RwLock guard across the get_table awaits.
    let table_refs: Vec<TableReference> = accelerated_tables(app).into_keys().collect();
    for table_ref in table_refs {
        if df.get_table(&table_ref).await.is_none() {
            return Some(table_ref);
        }
    }
    None
}

/// Helper to find all tables with acceleration partitioning configured, along with their partitioning columns.
#[must_use]
pub fn accelerated_tables(app: &Arc<App>) -> HashMap<TableReference, Vec<PartitionedBy>> {
    let ds = app.datasets.iter().filter_map(|ds| {
        if let Some(acc) = &ds.acceleration
            && acc.enabled
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
            && acc.enabled
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
    registry: &(dyn FunctionRegistry + Send + Sync),
) -> Result<HashMap<ResolvedTableReference, Vec<Expr>>> {
    let response = client
        .allocate_initial_partitions(AllocateInitialPartitionsRequest { executor_url })
        .await
        .context(PartitionAllocationRequestSnafu)?
        .into_inner();

    let mut result = HashMap::new();

    for (table_name, partitions) in response.table_partitions {
        let resolved = TableReference::parse_str(&table_name)
            .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
        let mut exprs = Vec::new();

        for item in partitions.items {
            let expr = Expr::from_bytes_with_registry(&item, registry)
                .context(PartitionExpressionDeserializationSnafu)?;
            exprs.push(expr);
        }

        result.insert(resolved, exprs);
    }

    Ok(result)
}
