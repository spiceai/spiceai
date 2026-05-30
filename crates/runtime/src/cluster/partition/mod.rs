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
/// Reports `num_rows` AND per-column min/max (and, for integer columns, an
/// approximate `distinct_count`) — these are what let the coordinator's planner
/// estimate join/aggregate output cardinalities and pick hash-join build sides
/// (q18 swap). Stats are sourced, in order:
///
/// 1. **Scan-plan statistics** (metadata-only). In append-only ("events") ingest
///    Cayenne collects per-column min/max from file footers, so the scan plan
///    carries num_rows + bounds with zero extra cost.
/// 2. **Aggregate query** fallback. Under CDC ("changes") ingest Cayenne disables
///    footer-stat collection while position-based deletions are pending, so the
///    scan plan yields no column bounds. Derive `num_rows` + per-column min/max
///    (+ `approx_distinct` for integer columns) with a single aggregate. Running
///    through the scan applies the deletion filter, so the values are exact for
///    the executor's live slice.
///
/// Why the integer `distinct_count` matters: a join key whose values are *sparse*
/// over a wide domain (e.g. CDC `o_custkey` ranging to ~1e9 with only ~1M
/// distinct) has a min/max range far larger than its true NDV. Without a distinct
/// count, the planner's `max_distinct_count` falls back to the range / row count
/// and badly over-estimates the key's cardinality, which under-estimates the join
/// and prevents the build-side swap. Reporting an approximate NDV lets the
/// planner size the join correctly. (The append-only case doesn't need this — its
/// keys are dense, so the min/max range already approximates the NDV.)
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

    // Fallback (CDC / no footer stats): aggregate query for num_rows + min/max
    // (+ integer NDV), degrading to a num_rows-only COUNT(*) if it fails.
    let stats = match aggregate_table_statistics(df, table, &schema).await {
        Some(stats) => stats,
        None => count_only_statistics(df, table, &schema).await.or_else(|| {
            tracing::debug!(table = %table, "No local statistics available for executor table");
            None
        })?,
    };

    Some((runtime_cluster::encode_statistics(&stats), column_names))
}

/// True if any column in `stats` carries a usable min or max bound.
fn has_any_column_bounds(stats: &datafusion::common::Statistics) -> bool {
    use datafusion::common::stats::Precision;
    stats.column_statistics.iter().any(|c| {
        !matches!(c.min_value, Precision::Absent) || !matches!(c.max_value, Precision::Absent)
    })
}

/// Whether `min`/`max` aggregates are supported for `dt` (scalar types only, so
/// the synthesized aggregate query can't fail on a nested/unsupported column).
fn supports_minmax(dt: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType;
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            | DataType::Float16 | DataType::Float32 | DataType::Float64
            | DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
            | DataType::Date32 | DataType::Date64
            | DataType::Time32(_) | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// Whether to collect an approximate `distinct_count` for `dt`. Restricted to
/// integer types: these are the join-key candidates (e.g. `*_custkey`,
/// `*_orderkey`) whose NDV can diverge sharply from their min/max range under
/// sparse-key CDC data, and `approx_distinct` (HyperLogLog) over one integer
/// column is cheap to fold into the existing aggregate scan.
fn supports_ndv(dt: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType;
    matches!(
        dt,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
    )
}

/// Quote an identifier for SQL (double-quoted, internal quotes doubled).
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Convert the single-row aggregate result at `array[0]` into a min/max bound,
/// treating null (empty/all-null column) as `Absent`.
fn scalar_bound(
    array: &datafusion::arrow::array::ArrayRef,
) -> datafusion::common::stats::Precision<datafusion::common::ScalarValue> {
    use datafusion::common::ScalarValue;
    use datafusion::common::stats::Precision;
    match ScalarValue::try_from_array(array, 0) {
        Ok(sv) if !sv.is_null() => Precision::Exact(sv),
        _ => Precision::Absent,
    }
}

/// Read a non-negative count from the single-row aggregate result at `array[0]`,
/// accepting either `Int64` (`COUNT`) or `UInt64` (`approx_distinct`).
fn count_at(array: &datafusion::arrow::array::ArrayRef) -> Option<usize> {
    use datafusion::arrow::array::{Int64Array, UInt64Array};
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return usize::try_from(a.value(0)).ok();
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
        return usize::try_from(a.value(0)).ok();
    }
    None
}

/// Derive `num_rows` + per-column min/max (+ integer `approx_distinct`) for the
/// executor's local slice of `table` via a single aggregate query. Returns `None`
/// if the query fails or yields no row.
async fn aggregate_table_statistics(
    df: &crate::datafusion::DataFusion,
    table: &datafusion::sql::TableReference,
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Option<datafusion::common::Statistics> {
    use datafusion::common::stats::Precision;
    use datafusion::common::{ColumnStatistics, Statistics};

    // COUNT(*) at column 0, then per supported column: min, max, and (for integer
    // columns) approx_distinct. `layout` records each column's result indices.
    let mut select_exprs: Vec<String> = vec!["COUNT(*) AS __sr_count".to_string()];
    // (schema field index, min idx, max idx, optional ndv idx) in the result batch.
    let mut layout: Vec<(usize, usize, usize, Option<usize>)> = Vec::new();
    let mut next = 1usize;
    for (idx, field) in schema.fields().iter().enumerate() {
        if !supports_minmax(field.data_type()) {
            continue;
        }
        let col = quote_ident(field.name());
        let (min_idx, max_idx) = (next, next + 1);
        select_exprs.push(format!("min({col}) AS __sr_min{idx}"));
        select_exprs.push(format!("max({col}) AS __sr_max{idx}"));
        next += 2;
        let ndv_idx = if supports_ndv(field.data_type()) {
            select_exprs.push(format!("approx_distinct({col}) AS __sr_ndv{idx}"));
            let n = next;
            next += 1;
            Some(n)
        } else {
            None
        };
        layout.push((idx, min_idx, max_idx, ndv_idx));
    }

    let sql = format!(
        "SELECT {} FROM {}",
        select_exprs.join(", "),
        table.to_quoted_string()
    );
    let batches = df.ctx.sql(&sql).await.ok()?.collect().await.ok()?;
    let batch = batches.into_iter().find(|b| b.num_rows() > 0)?;

    let num_rows = count_at(batch.column(0))?;

    // Per-column bounds for supported columns, keyed by schema field index.
    let mut bounds: HashMap<usize, ColumnStatistics> = HashMap::new();
    for (field_idx, min_idx, max_idx, ndv_idx) in layout {
        let distinct_count = ndv_idx
            .and_then(|i| count_at(batch.column(i)))
            .map_or(Precision::Absent, Precision::Inexact);
        bounds.insert(
            field_idx,
            ColumnStatistics {
                null_count: Precision::Absent,
                min_value: scalar_bound(batch.column(min_idx)),
                max_value: scalar_bound(batch.column(max_idx)),
                sum_value: Precision::Absent,
                distinct_count,
                byte_size: Precision::Absent,
            },
        );
    }

    // Align column_statistics positionally with the full schema (= column_names).
    let column_statistics = (0..schema.fields().len())
        .map(|i| {
            bounds
                .remove(&i)
                .unwrap_or_else(ColumnStatistics::new_unknown)
        })
        .collect();

    Some(Statistics {
        num_rows: Precision::Inexact(num_rows),
        total_byte_size: Precision::Absent,
        column_statistics,
    })
}

/// Last-resort fallback: `COUNT(*)` → num_rows only, no column bounds.
async fn count_only_statistics(
    df: &crate::datafusion::DataFusion,
    table: &datafusion::sql::TableReference,
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Option<datafusion::common::Statistics> {
    use datafusion::common::stats::Precision;

    let sql = format!("SELECT COUNT(*) AS n FROM {}", table.to_quoted_string());
    let batches = df.ctx.sql(&sql).await.ok()?.collect().await.ok()?;
    let batch = batches.into_iter().find(|b| b.num_rows() > 0)?;
    let n = count_at(batch.column(0))?;
    Some(datafusion::common::Statistics::new_unknown(schema).with_num_rows(Precision::Inexact(n)))
}
