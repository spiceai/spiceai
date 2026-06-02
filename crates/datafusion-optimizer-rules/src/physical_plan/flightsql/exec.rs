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

//! [`PartialAggregationFlightSqlExec`] — an [`ExecutionPlan`] that pushes a partial
//! aggregation into a `FlightSQL` query, replacing the original scan + local aggregation.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::aggregates::PhysicalGroupBy;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::sql::TableReference;

use data_components::flightsql::{
    FlightSqlClient, FlightSqlExec, query_to_stream, trace_parent_from_task_context,
};
use data_components::sql_expr::to_sql_preserving_precedence;
use flight_client::cookie::CookieStore;
use futures::StreamExt;

/// Aggregate functions whose partial state is the aggregate value itself (single state field).
pub(crate) static SIMPLE_PUSHDOWN_AGGREGATES: LazyLock<HashSet<&str>> =
    LazyLock::new(|| HashSet::from(["sum", "count", "min", "max"]));

/// Aggregate functions that require decomposition.
/// AVG's partial state is `(sum, count)` — two fields.
pub(crate) static DECOMPOSED_AGGREGATES: LazyLock<HashSet<&str>> =
    LazyLock::new(|| HashSet::from(["avg"]));

/// A SQL `SELECT` statement built from structured components.
///
/// Avoids ad-hoc string concatenation by collecting each clause into a dedicated
/// field, then rendering them all in [`SelectStatement::to_sql`].
struct SelectStatement {
    /// `SELECT` items, rendered as-is (e.g. `"col"`, `SUM("col") AS "__agg_0"`).
    select_items: Vec<String>,
    /// Fully-qualified table name for the `FROM` clause.
    from: String,
    /// Individual filter conditions joined with `AND` in the `WHERE` clause.
    where_clauses: Vec<String>,
    /// Column references for the `GROUP BY` clause.
    group_by_cols: Vec<String>,
}

impl SelectStatement {
    fn to_sql(&self) -> String {
        let mut sql = format!("SELECT {} FROM {}", self.select_items.join(", "), self.from);
        if !self.where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&self.where_clauses.join(" AND "));
        }
        if !self.group_by_cols.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&self.group_by_cols.join(", "));
        }
        sql
    }
}

/// Result of [`build_aggregate_sql`]: the SQL statement plus a mapping from each
/// output column position to the (deduplicated) remote column index.
///
/// When the same aggregate expression appears multiple times (e.g. `SUM(col)`
/// from an explicit SUM and from AVG decomposition), the SQL only contains it
/// once. The `column_mapping` tells [`execute`] which remote column to read
/// for each output position.
struct AggregateQuery {
    sql: String,
    /// `column_mapping[output_idx] = remote_col_idx`
    column_mapping: Vec<usize>,
}

/// A SQL fragment before deduplication, carrying the rendered SQL, a
/// dedup key (with outer CASTs stripped), and whether it needs an alias.
struct SqlFragment {
    /// The SQL expression to emit (e.g. `SUM("l_quantity")`).
    sql: String,
    /// Key used for deduplication. Outer CASTs are stripped so that
    /// `SUM(col)` and `SUM(CAST(col AS DOUBLE))` are recognized as duplicates.
    dedup_key: String,
    /// Whether this fragment needs an `AS "__agg_N"` alias.
    /// Group-by columns don't; aggregate expressions do.
    needs_alias: bool,
}

/// A deduplicated SQL select item, ready for final rendering.
struct SelectItem {
    /// The SQL expression to emit.
    sql: String,
    /// Whether this item needs an `AS "__agg_N"` alias.
    needs_alias: bool,
}

/// An [`ExecutionPlan`] that sends a SQL query with `GROUP BY` and aggregate functions
/// to a `FlightSQL` endpoint, producing output that matches the partial aggregate state
/// schema expected by a downstream `AggregateExec(mode=Final*)`.
///
/// Stores the structured components from the original `FlightSqlExec` (table reference,
/// filters, etc.) and the aggregate/group-by expressions from the `AggregateExec(Partial)`.
/// The final SQL is built at execution time from these structured fields.
#[derive(Clone)]
pub struct PartialAggregationFlightSqlExec {
    /// Table reference from the source `FlightSqlExec`.
    table_reference: TableReference,
    /// Filter expressions from the source `FlightSqlExec` (become the WHERE clause).
    source_filters: Vec<Expr>,
    /// Group-by expressions and their output names.
    group_by: PhysicalGroupBy,
    /// Aggregate function expressions.
    aggr_exprs: Vec<Arc<AggregateFunctionExpr>>,
    /// Schema of the output — matches the partial aggregate state fields.
    output_schema: SchemaRef,
    /// Column substitution map from an intermediate `ProjectionExec` (CSE).
    ///
    /// When `DataFusion`'s common subexpression elimination inserts a `ProjectionExec`
    /// between the `AggregateExec(Partial)` and the scan, the aggregate may reference
    /// synthetic columns like `__common_expr_1`. This map resolves each column index
    /// in the aggregate's expressions back to the original physical expression from
    /// the projection, so SQL generation can produce correct remote column references.
    ///
    /// Indexed by output column position of the `ProjectionExec`.
    column_substitutions: Vec<Arc<dyn PhysicalExpr>>,
    /// The `FlightSQL` client.
    client: FlightSqlClient,
    /// Cookie store for authentication propagation.
    cookie_store: Arc<CookieStore>,
    /// Cached plan properties.
    properties: PlanProperties,
    /// Optional W3C `traceparent` value inherited from the source
    /// `FlightSqlExec`. Forwarded as a gRPC metadata header on each
    /// outgoing call so executor-side spans chain back to the originating
    /// request.
    trace_parent: Option<String>,
    /// Output statistics, derived from the source scan's statistics at
    /// construction time (see [`PartialAggregationFlightSqlExec::new`]).
    statistics: Statistics,
}

impl PartialAggregationFlightSqlExec {
    /// Create from a source `FlightSqlExec` and the aggregate info from the
    /// `AggregateExec(Partial)` being replaced.
    pub fn new(
        source: &FlightSqlExec,
        group_by: PhysicalGroupBy,
        aggr_exprs: Vec<Arc<AggregateFunctionExpr>>,
        output_schema: SchemaRef,
        column_substitutions: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let statistics = Self::derive_statistics(source, &output_schema);
        Self {
            table_reference: source.table_reference().clone(),
            source_filters: source.filters().to_vec(),
            group_by,
            aggr_exprs,
            output_schema,
            column_substitutions,
            client: source.client().clone(),
            cookie_store: Arc::clone(source.cookie_store()),
            properties,
            trace_parent: source.trace_parent().map(str::to_string),
            statistics,
        }
    }

    /// Derive the output statistics of the pushed-down partial aggregate from
    /// the source scan's statistics.
    ///
    /// This mirrors `DataFusion`'s own grouped-`AggregateExec` estimate: a
    /// `GROUP BY` emits at most one row per input row, so the output row count
    /// is the input row count as an inexact upper bound. Column-level
    /// statistics are not carried through (the output columns are group keys
    /// and partial aggregate states, not the source columns). Without this, the
    /// pushed-down aggregate would be a statistics-less leaf and the planner
    /// could not compare it against a join's other side.
    fn derive_statistics(source: &FlightSqlExec, output_schema: &SchemaRef) -> Statistics {
        let stats = Statistics::new_unknown(output_schema);
        match source
            .partition_statistics(None)
            .ok()
            .and_then(|s| s.num_rows.get_value().copied())
        {
            Some(num_rows) => stats.with_num_rows(Precision::Inexact(num_rows)),
            None => stats,
        }
    }

    /// Build the SQL query with `GROUP BY` and aggregate functions from the stored fields.
    pub fn sql(&self) -> Result<String> {
        self.build_query().map(|q| q.sql)
    }

    /// Build the full [`AggregateQuery`] with SQL and column mapping.
    fn build_query(&self) -> Result<AggregateQuery> {
        build_aggregate_sql(
            &self.table_reference,
            &self.source_filters,
            &self.group_by,
            &self.aggr_exprs,
            &self.column_substitutions,
        )
    }
}

impl fmt::Debug for PartialAggregationFlightSqlExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.sql() {
            Ok(sql) => write!(f, "PartialAggregationFlightSqlExec sql={sql}"),
            Err(_) => write!(f, "PartialAggregationFlightSqlExec sql=<error>"),
        }
    }
}

impl DisplayAs for PartialAggregationFlightSqlExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match self.sql() {
            Ok(sql) => write!(f, "PartialAggregationFlightSqlExec sql={sql}"),
            Err(_) => write!(f, "PartialAggregationFlightSqlExec sql=<error>"),
        }
    }
}

impl ExecutionPlan for PartialAggregationFlightSqlExec {
    fn name(&self) -> &'static str {
        "PartialAggregationFlightSqlExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.statistics.clone())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        match partition {
            None | Some(0) => Ok(self.statistics.clone()),
            Some(_) => Ok(Statistics::new_unknown(&self.output_schema)),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "PartialAggregationFlightSqlExec only supports 1 partition, got partition {partition}"
            )));
        }
        let query = self.build_query()?;
        let target_schema = self.schema();
        let column_mapping = query.column_mapping;

        let mut client = self.client.clone();
        let trace_parent = self
            .trace_parent
            .clone()
            .or_else(|| trace_parent_from_task_context(&context));
        if let Some(value) = trace_parent {
            client.set_header("traceparent", value);
        }

        let stream = query_to_stream(client, query.sql, Arc::clone(&self.cookie_store)).map(
            move |result: std::result::Result<_, DataFusionError>| {
                result.and_then(|batch| remap_batch(&batch, &column_mapping, &target_schema))
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

// ─── Result remapping ────────────────────────────────────────────────

/// Remap columns from a deduplicated remote batch to the full output schema.
///
/// `column_mapping[output_idx]` gives the remote column index to read.
/// Columns are cast to the target type when they differ (e.g. remote `SUM`
/// returns `Decimal128` but the AVG state field expects `Float64`).
fn remap_batch(
    batch: &RecordBatch,
    column_mapping: &[usize],
    target_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let num_remote_cols = batch.num_columns();
    let columns: Vec<ArrayRef> = column_mapping
        .iter()
        .zip(target_schema.fields())
        .map(|(&remote_idx, target_field)| {
            if remote_idx >= num_remote_cols {
                return Err(DataFusionError::Execution(format!(
                    "Column mapping references index {remote_idx} but remote batch only has {num_remote_cols} columns"
                )));
            }
            let col = batch.column(remote_idx);
            if col.data_type() == target_field.data_type() {
                Ok(Arc::clone(col))
            } else {
                cast(col, target_field.data_type())
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(Arc::clone(target_schema), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

// ─── SQL generation ──────────────────────────────────────────────────

/// Build a SQL string from structured fields, deduplicating aggregate expressions
/// that are *exactly* identical as remote expressions (including any explicit CASTs)
/// and would therefore produce the same remote column name.
///
/// Returns the SQL and a column mapping from output position → remote column index.
///
/// `DataFusion` normalises aggregate names (for example `SUM(col)` and
/// `SUM(CAST(col AS DOUBLE))` may share the same internal name), which can cause
/// "duplicate field name" errors on the remote side. To avoid this, we compute
/// a deduplication key from the full remote aggregate expression, including CASTs,
/// and emit each unique remote aggregate expression only once. When multiple local
/// aggregates share the same remote expression, they are mapped to that single
/// remote column, and [`remap_batch`] is responsible for casting the resulting
/// column to the correct local target type when necessary.
fn build_aggregate_sql(
    table_reference: &TableReference,
    source_filters: &[Expr],
    group_by: &PhysicalGroupBy,
    aggr_exprs: &[Arc<AggregateFunctionExpr>],
    column_substitutions: &[Arc<dyn PhysicalExpr>],
) -> Result<AggregateQuery> {
    // Phase 1: collect all SQL fragments (before dedup) and their dedup keys.
    let mut all_fragments: Vec<SqlFragment> = Vec::new();
    let mut group_by_cols: Vec<String> = Vec::new();

    // Group-by columns / expressions — no alias needed. Generate SQL from the
    // physical expression so that non-trivial group-bys (e.g. date_trunc(...))
    // are correctly pushed down.
    for (expr, _name) in group_by.expr() {
        let expr_sql = physical_expr_to_sql(expr, column_substitutions).ok_or_else(|| {
            DataFusionError::Internal("Failed to convert group-by expression to SQL".to_string())
        })?;
        group_by_cols.push(expr_sql.clone());
        all_fragments.push(SqlFragment {
            sql: expr_sql.clone(),
            dedup_key: expr_sql,
            needs_alias: false,
        });
    }

    // Aggregate expressions
    for aggr in aggr_exprs {
        let func_name = aggr.fun().name();
        let exprs = aggr.expressions();

        if SIMPLE_PUSHDOWN_AGGREGATES.contains(func_name) {
            if func_name == "count" && exprs.is_empty() {
                all_fragments.push(SqlFragment {
                    sql: "COUNT(*)".to_string(),
                    dedup_key: "COUNT(*)".to_string(),
                    needs_alias: true,
                });
            } else if func_name == "count" {
                if exprs.len() != 1 {
                    return Err(DataFusionError::Internal(format!(
                        "Expected 1 argument for count, got {}",
                        exprs.len()
                    )));
                }
                let arg_sql =
                    physical_expr_to_sql(&exprs[0], column_substitutions).ok_or_else(|| {
                        DataFusionError::Internal("Failed to convert count arg to SQL".to_string())
                    })?;
                all_fragments.push(SqlFragment {
                    sql: format!("COUNT({arg_sql})"),
                    dedup_key: format!("COUNT({})", strip_outer_cast(&arg_sql)),
                    needs_alias: true,
                });
            } else {
                if exprs.len() != 1 {
                    return Err(DataFusionError::Internal(format!(
                        "Expected 1 argument for {func_name}, got {}",
                        exprs.len()
                    )));
                }
                let arg_sql =
                    physical_expr_to_sql(&exprs[0], column_substitutions).ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to convert {func_name} arg to SQL"
                        ))
                    })?;
                let upper = func_name.to_uppercase();
                all_fragments.push(SqlFragment {
                    sql: format!("{upper}({arg_sql})"),
                    dedup_key: format!("{upper}({})", strip_outer_cast(&arg_sql)),
                    needs_alias: true,
                });
            }
        } else if func_name == "avg" {
            if exprs.len() != 1 {
                return Err(DataFusionError::Internal(format!(
                    "Expected 1 argument for avg, got {}",
                    exprs.len()
                )));
            }
            let arg_sql =
                physical_expr_to_sql(&exprs[0], column_substitutions).ok_or_else(|| {
                    DataFusionError::Internal("Failed to convert avg arg to SQL".to_string())
                })?;
            // AVG decomposes into SUM + COUNT. The partial state has exactly
            // two fields, but their ORDER depends on the specific AVG accumulator
            // implementation (DataFusion's built-in uses `[count, sum]`, but
            // custom accumulators may use `[sum, count]`).
            //
            // We read the actual state fields from the aggregate expression and
            // emit SQL fragments in the matching order so that `column_mapping`
            // correctly routes remote columns to the output positions expected
            // by `AggregateExec(Final)`.
            let stripped = strip_outer_cast(&arg_sql);

            let state_fields = aggr.state_fields().map_err(|e| {
                DataFusionError::Internal(format!("Failed to get AVG state fields: {e}"))
            })?;
            if state_fields.len() != 2 {
                return Err(DataFusionError::Internal(format!(
                    "Expected 2 state fields for avg, got {}",
                    state_fields.len()
                )));
            }

            for field in &state_fields {
                let name_lower = field.name().to_lowercase();
                // Match on the `[count]` / `[sum]` suffix produced by `format_state_name`.
                if name_lower.ends_with("[count]") {
                    all_fragments.push(SqlFragment {
                        sql: format!("COUNT({arg_sql})"),
                        dedup_key: format!("COUNT({stripped})"),
                        needs_alias: true,
                    });
                } else if name_lower.ends_with("[sum]") {
                    // Use SUM(col) (no CAST) as the dedup key so it merges with an
                    // existing explicit SUM(col). The local side casts the Decimal
                    // result to Float64 via remap_batch.
                    all_fragments.push(SqlFragment {
                        sql: format!("SUM({arg_sql})"),
                        dedup_key: format!("SUM({stripped})"),
                        needs_alias: true,
                    });
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Unrecognized AVG state field '{}' — expected name ending with '[count]' or '[sum]'",
                        field.name()
                    )));
                }
            }
        } else {
            return Err(DataFusionError::Internal(format!(
                "Unsupported aggregate function for pushdown: {func_name}"
            )));
        }
    }

    // Phase 2: deduplicate by key, preserving order.
    let mut dedup_map: HashMap<String, usize> = HashMap::new();
    let mut unique_items: Vec<SelectItem> = Vec::new();
    let mut column_mapping: Vec<usize> = Vec::with_capacity(all_fragments.len());

    for frag in &all_fragments {
        if let Some(&idx) = dedup_map.get(&frag.dedup_key) {
            column_mapping.push(idx);
        } else {
            let idx = unique_items.len();
            dedup_map.insert(frag.dedup_key.clone(), idx);
            unique_items.push(SelectItem {
                sql: frag.sql.clone(),
                needs_alias: frag.needs_alias,
            });
            column_mapping.push(idx);
        }
    }

    // Phase 3: build SQL — alias only aggregate expressions, not plain columns
    let mut agg_alias_idx: usize = 0;
    let select_items: Vec<String> = unique_items
        .iter()
        .map(|item| {
            if item.needs_alias {
                let rendered = format!("{} AS \"__agg_{agg_alias_idx}\"", item.sql);
                agg_alias_idx += 1;
                rendered
            } else {
                item.sql.clone()
            }
        })
        .collect();

    // WHERE clause from source filters
    let where_clauses: Vec<String> = source_filters
        .iter()
        .map(|f| {
            to_sql_preserving_precedence(f).map_err(|e| DataFusionError::Internal(e.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;

    let stmt = SelectStatement {
        select_items,
        from: table_reference.to_quoted_string(),
        where_clauses,
        group_by_cols,
    };

    Ok(AggregateQuery {
        sql: stmt.to_sql(),
        column_mapping,
    })
}

/// Strip an outer `CAST(... AS ...)` wrapper from a SQL fragment, returning the
/// inner expression. If the fragment is not a CAST, return it unchanged.
///
/// This is used as a deduplication key: `DataFusion` normalises `SUM(col)` and
/// `SUM(CAST(col AS DOUBLE))` to the same internal aggregate, so we must treat
/// them as duplicates.
fn strip_outer_cast(sql: &str) -> &str {
    let s = sql.trim();
    if let Some(inner) = s.strip_prefix("CAST(") {
        // Find the matching closing paren, accounting for nested parens
        if let Some(as_pos) = find_top_level_as(inner) {
            // inner[..as_pos] is the expression inside CAST(expr AS type)
            return inner[..as_pos].trim();
        }
    }
    s
}

/// Find the position of the top-level ` AS ` keyword in a CAST body,
/// i.e. inside `CAST(expr AS type)`, skipping nested parentheses.
fn find_top_level_as(s: &str) -> Option<usize> {
    let mut depth = 0usize;
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i < len {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                if depth == 0 {
                    return None; // unbalanced
                }
                depth -= 1;
            }
            b' ' if depth == 0 => {
                // Check for " AS "
                if i + 4 <= len && bytes[i..i + 4].eq_ignore_ascii_case(b" AS ") {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Convert a physical expression to a SQL string fragment.
///
/// When `column_substitutions` is non-empty, a `Column` reference is first
/// resolved through the substitution map (indexed by column position). This
/// handles CSE-generated synthetic columns like `__common_expr_1` by inlining
/// the original expression. The substituted expression is then converted
/// recursively *without* substitutions, because its own column references
/// target the scan schema directly.
pub(super) fn physical_expr_to_sql(
    expr: &Arc<dyn PhysicalExpr>,
    column_substitutions: &[Arc<dyn PhysicalExpr>],
) -> Option<String> {
    use datafusion::physical_expr::expressions::{
        BinaryExpr, CastExpr, Column, Literal, NegativeExpr,
    };

    let any = expr.as_any();

    if let Some(col) = any.downcast_ref::<Column>() {
        // If there is a substitution for this column index, inline it.
        if let Some(subst) = column_substitutions.get(col.index()) {
            // Substituted expressions reference the scan schema directly,
            // so recurse without further substitutions.
            return physical_expr_to_sql(subst, &[]);
        }
        return Some(quote_ident(col.name()));
    }
    if let Some(lit) = any.downcast_ref::<Literal>() {
        return scalar_to_sql(lit.value());
    }
    if let Some(bin) = any.downcast_ref::<BinaryExpr>() {
        let left = physical_expr_to_sql(bin.left(), column_substitutions)?;
        let right = physical_expr_to_sql(bin.right(), column_substitutions)?;
        return Some(format!("({left} {op} {right})", op = bin.op()));
    }
    if let Some(cast) = any.downcast_ref::<CastExpr>() {
        let inner = physical_expr_to_sql(cast.expr(), column_substitutions)?;
        let dt = arrow_type_to_sql(cast.cast_type())?;
        return Some(format!("CAST({inner} AS {dt})"));
    }
    if let Some(neg) = any.downcast_ref::<NegativeExpr>() {
        let inner = physical_expr_to_sql(neg.arg(), column_substitutions)?;
        return Some(format!("(-{inner})"));
    }
    None
}

fn scalar_to_sql(value: &datafusion::scalar::ScalarValue) -> Option<String> {
    use datafusion::scalar::ScalarValue;
    match value {
        ScalarValue::Int8(Some(v)) => Some(v.to_string()),
        ScalarValue::Int16(Some(v)) => Some(v.to_string()),
        ScalarValue::Int32(Some(v)) => Some(v.to_string()),
        ScalarValue::Int64(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt8(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt16(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt32(Some(v)) => Some(v.to_string()),
        ScalarValue::UInt64(Some(v)) => Some(v.to_string()),
        ScalarValue::Float32(Some(v)) => Some(v.to_string()),
        ScalarValue::Float64(Some(v)) => Some(v.to_string()),
        ScalarValue::Decimal128(Some(v), _precision, scale) => {
            if *scale == 0 {
                Some(v.to_string())
            } else {
                // Render with the correct number of decimal places.
                // e.g. Decimal128(100, _, 2) → "1.00", Decimal128(-50, _, 2) → "-0.50"
                let scale_u32 = u32::from(scale.unsigned_abs());
                let divisor = i128::checked_pow(10, scale_u32)?;
                let divisor_u = divisor.unsigned_abs();
                let abs = v.unsigned_abs();
                let whole = abs / divisor_u;
                let frac = abs % divisor_u;
                let s = usize::from(scale.unsigned_abs());
                let sign = if *v < 0 { "-" } else { "" };
                Some(format!("{sign}{whole}.{frac:0>s$}"))
            }
        }
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => {
            let escaped = v.replace('\'', "''");
            Some(format!("'{escaped}'"))
        }
        ScalarValue::Boolean(Some(v)) => Some(v.to_string()),
        ScalarValue::Null => Some("NULL".to_string()),
        _ => None,
    }
}

fn arrow_type_to_sql(dt: &datafusion::arrow::datatypes::DataType) -> Option<String> {
    use datafusion::arrow::datatypes::DataType;
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => Some("INTEGER".to_string()),
        DataType::Int64 | DataType::UInt64 => Some("BIGINT".to_string()),
        DataType::Float32 => Some("FLOAT".to_string()),
        DataType::Float16 | DataType::Float64 => Some("DOUBLE".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some("VARCHAR".to_string()),
        DataType::Boolean => Some("BOOLEAN".to_string()),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            Some(format!("DECIMAL({p},{s})"))
        }
        _ => None,
    }
}

fn quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    /// Identity mapping: same types, same column order.
    #[test]
    fn test_remap_batch_identity() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![4, 5, 6])),
            ],
        )
        .expect("build batch");

        let mapping = vec![0, 1];
        let result = remap_batch(&batch, &mapping, &schema).expect("remap should succeed");

        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.num_rows(), 3);
        let col_a = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("should be Int64Array");
        assert_eq!(col_a.values(), &[1, 2, 3]);
    }

    /// Column reuse via dedup mapping: output has 3 columns but remote batch
    /// has only 2 — column 0 is read twice.
    #[test]
    fn test_remap_batch_column_reuse() {
        let remote_schema = Arc::new(Schema::new(vec![
            Field::new("sum_qty", DataType::Int64, false),
            Field::new("count_qty", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&remote_schema),
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .expect("build batch");

        // Output schema has 3 fields; column 0 from remote is reused at positions 0 and 2.
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("sum_qty_1", DataType::Int64, false),
            Field::new("count_qty", DataType::Int64, false),
            Field::new("sum_qty_2", DataType::Int64, false),
        ]));
        let mapping = vec![0, 1, 0];
        let result = remap_batch(&batch, &mapping, &target_schema).expect("remap should succeed");

        assert_eq!(result.num_columns(), 3);
        let col0 = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("should be Int64Array");
        let col2 = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("should be Int64Array");
        assert_eq!(col0.value(0), 100);
        assert_eq!(col2.value(0), 100);
    }

    /// Type casting: remote returns Int64, target expects Float64.
    #[test]
    fn test_remap_batch_type_cast() {
        let remote_schema = Arc::new(Schema::new(vec![Field::new(
            "sum_qty",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&remote_schema),
            vec![Arc::new(Int64Array::from(vec![42, 100]))],
        )
        .expect("build batch");

        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "sum_qty",
            DataType::Float64,
            true,
        )]));
        let mapping = vec![0];
        let result = remap_batch(&batch, &mapping, &target_schema).expect("remap should succeed");

        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("should be Float64Array");
        assert!((col.value(0) - 42.0).abs() < f64::EPSILON);
        assert!((col.value(1) - 100.0).abs() < f64::EPSILON);
    }

    /// Out-of-bounds column index should return an error, not panic.
    #[test]
    fn test_remap_batch_out_of_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .expect("build batch");

        let target_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, false)]));
        // Index 5 is out of bounds for a 1-column batch
        let mapping = vec![5];
        let result = remap_batch(&batch, &mapping, &target_schema);
        assert!(result.is_err(), "should error on out-of-bounds index");
        let err_msg = result
            .expect_err("should error on out-of-bounds index")
            .to_string();
        assert!(
            err_msg.contains("index 5"),
            "error should mention the bad index: {err_msg}"
        );
    }
}
