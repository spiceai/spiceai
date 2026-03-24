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

mod discovery;
pub mod executor_selection;
mod manager;
mod metadata;
pub mod scheduler_task;
mod startup;
pub(crate) mod write_through;

use std::{collections::HashMap, sync::Arc};

use app::App;
use datafusion::sql::TableReference;
use datafusion_expr::Expr;
pub use manager::PartitionManager;
pub use metadata::{
    PartitionMetadata, PartitionValue, TablePartitionMetadata, partition_value_to_bytes,
};
use runtime_proto::BytesArray;
use snafu::{ResultExt, Snafu};
use spicepod::component::runtime;
pub use startup::{
    accelerated_tables, build_partition_metadata_store, executor_request_initial_partitions,
    initialize_partition_metadata, validate_partition_keys,
};

use crate::datafusion::DataFusion;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build object store for partition metadata: {source}"))]
    ObjectStoreBuild {
        source: crate::cluster::scheduler_registry::Error,
    },

    #[snafu(display("Failed to initialize partition metadata for table {table}: {source}"))]
    PartitionMetadataInit {
        table: String,
        source: Box<manager::Error>,
    },

    #[snafu(display("Failed to allocation partitions for table {table}: {source}"))]
    PartitionAllocation {
        table: String,
        source: Box<manager::Error>,
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

    #[snafu(display("Failed to serialize partition expression: {source}"))]
    PartitionExpressionSerialization {
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

    #[snafu(display(
        "Accelerated {component_type} '{name}' has no partition keys configured. Add 'partition_by' to its acceleration config to participate in cluster partition management."
    ))]
    MissingPartitionKeys {
        component_type: &'static str,
        name: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Extract partition filter expressions for a table from the assignments map.
/// Multiple assigned partitions are combined with OR (union semantics), then returned
/// as a single-element `Vec<Expr>` so that applying them via `.filter()` is correct.
/// Returns an empty `Vec` if no partitions are assigned.
#[expect(clippy::implicit_hasher)]
pub fn get_partition_filter_exprs(
    tbl: &TableReference,
    assignments: &HashMap<TableReference, Vec<Expr>>,
) -> Vec<Expr> {
    let partitions = assignments.get(tbl).cloned().unwrap_or_default();
    if partitions.is_empty() {
        return vec![];
    }
    // Combine multiple partition expressions with OR (union of partitions),
    // then wrap in a single-element Vec so `.filter()` applies it as one predicate.
    let combined = partitions
        .into_iter()
        .reduce(Expr::or)
        .unwrap_or_else(|| unreachable!("partitions is not empty"));
    vec![combined]
}

pub(crate) async fn allocation_initial_partitions(
    executor_id: &str,
    partition_manager: &Arc<PartitionManager>,
    app: &Arc<App>,
    df: &Arc<DataFusion>,
) -> Result<HashMap<String, BytesArray>> {
    let mut table_partitions: HashMap<String, BytesArray> = HashMap::new();

    let mut total_assigned: usize = 0;
    let max_partitions_per_executor = app.runtime.scheduler.as_ref().map_or(
        runtime::PartitionManagement::default().max_partitions_per_executor,
        runtime::Scheduler::max_partitions_per_executor,
    );

    // Find accelerated datasets with partitioning
    for table_ref in super::partition::accelerated_tables(app).keys() {
        if total_assigned >= max_partitions_per_executor {
            tracing::debug!(
                "Executor {executor_id} reached max_partitions_per_executor ({max_partitions_per_executor}) during initial allocation, skipping remaining tables"
            );
            return Ok(table_partitions);
        }
        let remaining = max_partitions_per_executor.saturating_sub(total_assigned);

        if partition_manager
            .get_cached_table_metadata(table_ref)
            .is_none()
        {
            tracing::info!(
                "No cached partition metadata for table {table_ref}. Scheduler likely has not finished discovering partitions for the table. Will not assign in initial allocation, but will get assigned on future assignments"
            );
            continue;
        }
        let result = partition_manager
            .allocate_partitions(table_ref, executor_id, remaining)
            .await
            .map_err(|e| Box::new(e))
            .context(PartitionAllocationSnafu {
                table: table_ref.to_string(),
            })?;

        let newly_assigned = result.newly_assigned.len();
        let partitions = result.all_assigned();
        if partitions.is_empty() {
            continue;
        }
        let mut items = Vec::with_capacity(partitions.len());
        for partition in &partitions {
            items.push(
                partition_value_to_bytes(partition.clone(), table_ref, df)
                    .await
                    .context(PartitionExpressionSerializationSnafu)?
                    .to_vec(),
            );
        }
        total_assigned += newly_assigned;
        table_partitions.insert(table_ref.to_string(), BytesArray { items });
    }

    Ok(table_partitions)
}
