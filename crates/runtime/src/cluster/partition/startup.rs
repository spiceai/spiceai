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
};

use app::{App, spicepod::component::runtime::Scheduler as SchedulerConfig};

use datafusion::{logical_expr::Expr, sql::TableReference};
use datafusion_proto::bytes::Serializeable;
use object_store::ObjectStore;
use object_store::prefix::PrefixStore;
use runtime_proto::{
    AllocateInitialPartitionsRequest, cluster_service_client::ClusterServiceClient,
};
use snafu::prelude::*;
use spicepod::partitioning::PartitionedBy;
use tonic::transport::Channel;

use super::{PartitionManager, Result};
use crate::{
    Runtime,
    cluster::partition::{
        ObjectStoreBuildSnafu, PartitionAllocationRequestSnafu,
        PartitionExpressionDeserializationSnafu, PartitionMetadataInitSnafu, discovery,
    },
};

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

        let partition_values =
            match discovery::table_partition_values(&table, &partitioning, rt).await {
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
