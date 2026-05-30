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
/// Reports `num_rows` AND per-column min/max — the column stats are what let the
/// coordinator's planner estimate join/aggregate output cardinalities and pick
/// hash-join build sides (q18 swap). Stats are sourced, in order:
///
/// 1. **Scan-plan statistics** (metadata-only). In append-only ("events") ingest
///    Cayenne collects per-column min/max from file footers, so the scan plan
///    carries everything we need with zero extra cost.
/// 2. **Cayenne metastore running-aggregate.** Under CDC ("changes") ingest
///    Cayenne disables footer-stat collection on the scan path while
///    position-based deletions are pending (the footer `num_rows` would
///    overcount logically-deleted rows), so the scan plan yields no column
///    bounds. The metastore aggregate — merged from footers on every write and
///    exposed via [`CayenneTableProvider::distributed_join_statistics`] — still
///    carries valid superset min/max (with an inexact row count), and reading it
///    is metadata-only (cached blob, no row scan).
/// 3. **`COUNT(*)`** (num_rows only) as a last resort when neither yields stats.
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
    let provider = df.get_table(table).await?;
    let schema = provider.schema();
    let column_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    // Primary: scan-plan statistics (num_rows + per-column min/max). No
    // projection so column stats cover every column.
    let state = df.ctx.state();
    let scan_stats = match provider.scan(&state, None, &[], None).await {
        Ok(plan) => plan.partition_statistics(None).ok(),
        Err(_) => None,
    };

    // Use scan-plan stats directly only when complete enough for join sizing:
    // num_rows present AND at least one column carries a min/max bound.
    if let Some(stats) = &scan_stats {
        if stats.num_rows.get_value().is_some() && has_any_column_bounds(stats) {
            return Some((runtime_cluster::encode_statistics(stats), column_names));
        }
    }

    // Secondary (CDC / no footer stats on the scan path): the Cayenne metastore
    // running-aggregate min/max, which survives pending deletions. Metadata-only.
    #[cfg(not(windows))]
    if let Some(cayenne) = provider
        .as_any()
        .downcast_ref::<cayenne::CayenneTableProvider>()
    {
        if let Some(stats) = cayenne.distributed_join_statistics() {
            if stats.num_rows.get_value().is_some() && has_any_column_bounds(&stats) {
                return Some((runtime_cluster::encode_statistics(&stats), column_names));
            }
        }
    }

    // Last resort: COUNT(*) → num_rows only, no column bounds.
    let stats = count_only_statistics(df, table, &schema).await.or_else(|| {
        tracing::debug!(table = %table, "No local statistics available for executor table");
        None
    })?;
    Some((runtime_cluster::encode_statistics(&stats), column_names))
}

/// True if any column in `stats` carries a usable min or max bound.
fn has_any_column_bounds(stats: &datafusion::common::Statistics) -> bool {
    use datafusion::common::stats::Precision;
    stats.column_statistics.iter().any(|c| {
        !matches!(c.min_value, Precision::Absent) || !matches!(c.max_value, Precision::Absent)
    })
}

/// `COUNT(*)` → num_rows-only statistics (no column bounds).
async fn count_only_statistics(
    df: &crate::datafusion::DataFusion,
    table: &datafusion::sql::TableReference,
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Option<datafusion::common::Statistics> {
    use datafusion::common::stats::Precision;

    let sql = format!("SELECT COUNT(*) AS n FROM {}", table.to_quoted_string());
    let batches = df.ctx.sql(&sql).await.ok()?.collect().await.ok()?;
    let batch = batches.into_iter().find(|b| b.num_rows() > 0)?;
    let n = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .and_then(|a| usize::try_from(a.value(0)).ok())?;
    Some(datafusion::common::Statistics::new_unknown(schema).with_num_rows(Precision::Inexact(n)))
}
