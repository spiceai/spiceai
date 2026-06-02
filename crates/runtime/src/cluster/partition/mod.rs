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
/// Reports `num_rows` AND per-column min/max (with an effective max encoding the
/// integer NDV) — these let the coordinator's planner estimate join/aggregate
/// output cardinalities and pick hash-join build sides (q18 swap).
///
/// Sourced from the **Cayenne metastore aggregate**, which is maintained
/// incrementally on the write path (a cheap byproduct of ingest) rather than
/// recomputed: a live `num_rows`, per-column min/max, and per-integer-column NDV
/// `HyperLogLog` sketches all merged on commit. The read is O(1) and always-fresh,
/// so there is no recurring full-table rescan and no need to guard against a
/// degraded result clobbering a richer one. Non-Cayenne providers (or a
/// pre-write cold aggregate) fall back to the scan plan's footer statistics.
///
/// Why the integer NDV matters: a join key whose values are *sparse* over a wide
/// domain (e.g. CDC `o_custkey` ranging to ~1e9 with only ~1M distinct) has a
/// min/max range far larger than its true NDV. Without a distinct count, the
/// planner's `max_distinct_count` falls back to the range / row count and badly
/// over-estimates the key's cardinality, under-estimating the join and preventing
/// the build-side swap. Encoding the NDV into the reported max
/// ([`encode_ndv_into_bounds`] / [`effective_max_for_ndv`]) makes the range-based
/// estimate accurate, and survives the per-executor `UnionExec` (which erases
/// `distinct_count` but preserves min/max). Dense (events) keys are unaffected.
///
/// Note: in clustered mode an executor's accelerated table is registered
/// *non-partitioned* (`partition_by` is cleared once the executor has partition
/// assignments — see `init::dataset`), so this goes through the generic table
/// provider rather than a `PartitionTableProvider`.
///
/// Best-effort: returns `None` when no statistics are available.
///
/// [`Statistics`]: datafusion::common::Statistics
pub(crate) async fn local_executor_table_statistics(
    df: &crate::datafusion::DataFusion,
    table: &datafusion::sql::TableReference,
) -> Option<(datafusion::common::Statistics, Vec<String>)> {
    let provider = df.get_table(table).await?;
    let schema = provider.schema();
    let column_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    // Primary: the Cayenne metastore aggregate, maintained incrementally on the
    // write path (live num_rows + per-column min/max + integer NDV as
    // distinct_count). It is always-fresh, O(1) to read, and — unlike
    // `TableProvider::statistics` — available even while position-based deletions
    // are pending (the CDC case). Encode NDV into the reported max so it survives
    // `UnionExec` over the per-executor scans.
    if let Some(cayenne) = provider
        .as_any()
        .downcast_ref::<cayenne::CayenneTableProvider>()
        && let Some(mut stats) = cayenne.optimizer_table_statistics()
        && stats.num_rows.get_value().is_some()
    {
        encode_ndv_into_bounds(&mut stats);
        return Some((stats, column_names));
    }

    // Fallback (non-Cayenne providers, or before any write has populated the
    // aggregate): scan-plan footer statistics — metadata-only, no aggregate
    // rescan. Used directly only when complete enough for join sizing: num_rows
    // present AND at least one column carries a min/max bound.
    let session_state = df.ctx.state();
    let scan_stats = match provider.scan(&session_state, None, &[], None).await {
        Ok(plan) => plan.partition_statistics(None).ok(),
        Err(_) => None,
    };
    if let Some(stats) = scan_stats
        && stats.num_rows.get_value().is_some()
        && has_any_column_bounds(&stats)
    {
        return Some((stats, column_names));
    }

    tracing::debug!(table = %table, "No local statistics available for executor table");
    None
}

/// Rewrite each integer column's reported max to the effective max
/// (`min(true_max, min + ndv)`, inexact) using the column's `distinct_count`.
/// This tightens the min/max *range* to the true NDV so the planner's
/// range-based distinct estimate is accurate for sparse keys after the
/// per-executor `UnionExec` (which erases `distinct_count` but preserves
/// min/max). See [`effective_max_for_ndv`] for the why.
fn encode_ndv_into_bounds(stats: &mut datafusion::common::Statistics) {
    use datafusion::common::stats::Precision;
    for col in &mut stats.column_statistics {
        let ndv = match col.distinct_count {
            Precision::Exact(n) | Precision::Inexact(n) => Some(n),
            Precision::Absent => None,
        };
        if ndv.is_some() {
            let true_max = std::mem::replace(&mut col.max_value, Precision::Absent);
            col.max_value = effective_max_for_ndv(&col.min_value, ndv, true_max);
        }
    }
}

/// True if any column in `stats` carries a usable min or max bound.
fn has_any_column_bounds(stats: &datafusion::common::Statistics) -> bool {
    use datafusion::common::stats::Precision;
    stats.column_statistics.iter().any(|c| {
        !matches!(c.min_value, Precision::Absent) || !matches!(c.max_value, Precision::Absent)
    })
}

/// Effective max for a column given its approximate distinct count: the tighter
/// of the true max and `min + ndv`, marked inexact. Returns `true_max` unchanged
/// when there's no ndv, no usable min, the column isn't an integer type, or the
/// synthetic value isn't tighter (dense keys). See the call site for why this
/// matters for sparse-key join sizing.
fn effective_max_for_ndv(
    min: &datafusion::common::stats::Precision<datafusion::common::ScalarValue>,
    ndv: Option<usize>,
    true_max: datafusion::common::stats::Precision<datafusion::common::ScalarValue>,
) -> datafusion::common::stats::Precision<datafusion::common::ScalarValue> {
    use datafusion::common::ScalarValue as SV;
    use datafusion::common::stats::Precision;

    let (Some(ndv), Some(min_sv)) = (ndv, min.get_value()) else {
        return true_max;
    };
    let ndv = i128::try_from(ndv).unwrap_or(i128::MAX);
    let add = |m: i128| -> i128 { m.saturating_add(ndv) };
    // Synthetic max = min + ndv, in the column's own integer type.
    let synthetic = match min_sv {
        SV::Int8(Some(m)) => i8::try_from(add(i128::from(*m)))
            .ok()
            .map(|v| SV::Int8(Some(v))),
        SV::Int16(Some(m)) => i16::try_from(add(i128::from(*m)))
            .ok()
            .map(|v| SV::Int16(Some(v))),
        SV::Int32(Some(m)) => i32::try_from(add(i128::from(*m)))
            .ok()
            .map(|v| SV::Int32(Some(v))),
        SV::Int64(Some(m)) => i64::try_from(add(i128::from(*m)))
            .ok()
            .map(|v| SV::Int64(Some(v))),
        SV::UInt8(Some(m)) => u8::try_from(add(i128::from(*m)))
            .ok()
            .map(|v| SV::UInt8(Some(v))),
        SV::UInt16(Some(m)) => u16::try_from(add(i128::from(*m)))
            .ok()
            .map(|v| SV::UInt16(Some(v))),
        SV::UInt32(Some(m)) => u32::try_from(add(i128::from(*m)))
            .ok()
            .map(|v| SV::UInt32(Some(v))),
        SV::UInt64(Some(m)) => u64::try_from(add(i128::from(*m)))
            .ok()
            .map(|v| SV::UInt64(Some(v))),
        _ => None,
    };
    match (synthetic, true_max.get_value()) {
        // Only tighten — never widen past the real max.
        (Some(syn), Some(tmax)) if &syn < tmax => Precision::Inexact(syn),
        _ => true_max,
    }
}
