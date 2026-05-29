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

/// Computes the per-executor [`Statistics`] for the slice of `table` this
/// executor has accelerated locally, returning the encoded statistics bytes and
/// the column names they're aligned to (so the coordinator can project column
/// stats onto a possibly-projected leaf scan by name).
///
/// Reports `num_rows` AND per-column min/max/null_count from the table
/// provider's scan-plan statistics (metadata-only, no row scan) — the column
/// stats are what let the coordinator's planner estimate join/aggregate output
/// cardinalities and pick hash-join build sides (q18 swap). Falls back to a
/// `COUNT(*)` (num_rows only) if the provider surfaces no scan statistics.
///
/// Note: in clustered mode an executor's accelerated table is registered
/// *non-partitioned* (`partition_by` is cleared once the executor has partition
/// assignments — see `init::dataset`), so this goes through the generic table
/// provider rather than a `PartitionTableProvider`.
///
/// Best-effort: returns `None` when the row count can't be determined.
///
/// [`Statistics`]: datafusion::common::Statistics
pub(crate) async fn local_executor_table_statistics(
    df: &crate::datafusion::DataFusion,
    table: &datafusion::sql::TableReference,
) -> Option<(Vec<u8>, Vec<String>)> {
    use datafusion::common::stats::Precision;

    let provider = df.get_table(table).await?;
    let schema = provider.schema();
    let column_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    // Primary: full scan-plan statistics (num_rows + per-column min/max).
    // No projection so column stats cover every column.
    let state = df.ctx.state();
    let scan_stats = match provider.scan(&state, None, &[], None).await {
        Ok(plan) => plan.partition_statistics(None).ok(),
        Err(_) => None,
    };

    let stats = match scan_stats {
        Some(s) if s.num_rows.get_value().is_some() => s,
        _ => {
            // Fallback: COUNT(*) → num_rows only, no column stats.
            let sql = format!("SELECT COUNT(*) AS n FROM {}", table.to_quoted_string());
            let n = async {
                let batches = df.ctx.sql(&sql).await.ok()?.collect().await.ok()?;
                let batch = batches.into_iter().find(|b| b.num_rows() > 0)?;
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()?;
                usize::try_from(col.value(0)).ok()
            }
            .await;
            match n {
                Some(n) => datafusion::common::Statistics::new_unknown(&schema)
                    .with_num_rows(Precision::Inexact(n)),
                None => {
                    tracing::debug!(table = %table, "No local statistics available for executor table");
                    return None;
                }
            }
        }
    };

    Some((runtime_cluster::encode_statistics(&stats), column_names))
}
