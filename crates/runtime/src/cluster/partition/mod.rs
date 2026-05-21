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

pub(crate) mod discovery;
pub mod scheduler_task;
mod startup;

use std::collections::HashMap;

use datafusion::sql::ResolvedTableReference;
use datafusion_expr::Expr;
use snafu::Snafu;

// Re-export types that moved into the `runtime-cluster` crate so callers inside
// `runtime` can continue to import them from `crate::cluster::partition`.
pub use runtime_cluster::{
    CopyAssignmentsResult, PartitionMetadata, PartitionService, PartitionStore, PartitionValue,
    TablePartitionMetadata, partition_value_to_bytes,
};
pub use runtime_cluster::{executor_selection, service, store, write_through};

pub use startup::{
    accelerated_tables, executor_request_initial_partitions, first_unready_accelerated_table,
    initialize_partition_metadata, validate_partition_keys,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to initialize partition metadata for table {table}: {source}"))]
    PartitionMetadataInit {
        table: String,
        source: Box<runtime_cluster::store::Error>,
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

    #[snafu(display(
        "Accelerated {component_type} '{name}' has no partition keys configured. Add 'partition_by' to its acceleration config to participate in cluster partition assignment."
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
///
/// The caller is responsible for resolving the table reference to a
/// `ResolvedTableReference` (e.g. against the default catalog/schema) before
/// calling this function, so that bare, partial, and fully-qualified references
/// all produce the same key and match correctly against the assignments map.
#[expect(clippy::implicit_hasher)]
pub fn get_partition_filter_exprs(
    tbl: &ResolvedTableReference,
    assignments: &HashMap<ResolvedTableReference, Vec<Expr>>,
) -> Vec<Expr> {
    let partitions = assignments.get(tbl).cloned().unwrap_or_default();
    if partitions.is_empty() {
        return vec![];
    }
    // Combine multiple partition expressions with OR (union of partitions) using a
    // balanced tree to avoid O(n)-depth nesting that can exceed recursion limits
    // during expression traversal/serialization. Wrap in a single-element Vec so
    // `.filter()` applies it as one predicate.
    let combined = util::expr::combine_exprs_balanced(partitions, Expr::or)
        .unwrap_or_else(|| unreachable!("partitions is not empty"));
    vec![combined]
}
