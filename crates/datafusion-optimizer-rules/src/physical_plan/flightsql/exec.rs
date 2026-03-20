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
//! aggregation into a FlightSQL query, replacing the original scan + local aggregation.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
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
    FlightSqlClient, FlightSqlExec, coerce_batch_to_schema, query_to_stream,
};
use datafusion_table_providers::sql::sql_provider_datafusion::expr as expr_to_sql;
use flight_client::cookie::CookieStore;
use futures::StreamExt;

/// Aggregate functions whose partial state is the aggregate value itself (single state field).
pub(crate) static SIMPLE_PUSHDOWN_AGGREGATES: LazyLock<HashSet<&str>> =
    LazyLock::new(|| HashSet::from(["sum", "count", "min", "max"]));

/// Aggregate functions that require decomposition.
/// AVG's partial state is `(sum, count)` — two fields.
pub(crate) static DECOMPOSED_AGGREGATES: LazyLock<HashSet<&str>> =
    LazyLock::new(|| HashSet::from(["avg"]));

/// An [`ExecutionPlan`] that sends a SQL query with `GROUP BY` and aggregate functions
/// to a FlightSQL endpoint, producing output that matches the partial aggregate state
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
    /// Schema of the `FlightSqlExec` input (used to resolve column references).
    input_schema: SchemaRef,
    /// Schema of the output — matches the partial aggregate state fields.
    output_schema: SchemaRef,
    /// The FlightSQL client.
    client: FlightSqlClient,
    /// Cookie store for authentication propagation.
    cookie_store: Arc<CookieStore>,
    /// Cached plan properties.
    properties: PlanProperties,
}

impl PartialAggregationFlightSqlExec {
    /// Create from a source `FlightSqlExec` and the aggregate info from the
    /// `AggregateExec(Partial)` being replaced.
    pub fn new(
        source: &FlightSqlExec,
        group_by: PhysicalGroupBy,
        aggr_exprs: Vec<Arc<AggregateFunctionExpr>>,
        input_schema: SchemaRef,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            table_reference: source.table_reference().clone(),
            source_filters: source.filters().to_vec(),
            group_by,
            aggr_exprs,
            input_schema,
            output_schema,
            client: source.client().clone(),
            cookie_store: Arc::clone(source.cookie_store()),
            properties,
        }
    }

    /// Build the SQL query with `GROUP BY` and aggregate functions from the stored fields.
    pub fn sql(&self) -> Result<String> {
        build_aggregate_sql(
            &self.table_reference,
            &self.source_filters,
            &self.group_by,
            &self.aggr_exprs,
            &self.input_schema,
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

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let sql = self.sql()?;
        let target_schema = self.schema();

        let stream =
            query_to_stream(self.client.clone(), sql, Arc::clone(&self.cookie_store)).map(
                move |result: std::result::Result<_, DataFusionError>| {
                    result.and_then(|batch| coerce_batch_to_schema(&batch, &target_schema))
                },
            );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

// ─── SQL generation ──────────────────────────────────────────────────

/// Build a SQL string from structured fields.
///
/// Produces: `SELECT group_cols, AGG(expr)... FROM table WHERE filters GROUP BY group_cols`
fn build_aggregate_sql(
    table_reference: &TableReference,
    source_filters: &[Expr],
    group_by: &PhysicalGroupBy,
    aggr_exprs: &[Arc<AggregateFunctionExpr>],
    input_schema: &SchemaRef,
) -> Result<String> {
    let mut select_parts: Vec<String> = Vec::new();
    let mut group_by_cols: Vec<String> = Vec::new();

    // Group-by columns
    for (_expr, name) in group_by.expr() {
        let quoted = quote_ident(name);
        select_parts.push(quoted.clone());
        group_by_cols.push(quoted);
    }

    // Aggregate expressions
    for aggr in aggr_exprs {
        let func_name = aggr.fun().name();
        let exprs = aggr.expressions();

        if SIMPLE_PUSHDOWN_AGGREGATES.contains(func_name) {
            if func_name == "count" && exprs.is_empty() {
                select_parts.push("COUNT(*)".to_string());
            } else if func_name == "count" {
                let arg_sql = physical_expr_to_sql(&exprs[0], input_schema).ok_or_else(|| {
                    DataFusionError::Internal("Failed to convert count arg to SQL".to_string())
                })?;
                select_parts.push(format!("COUNT({arg_sql})"));
            } else {
                let arg_sql = physical_expr_to_sql(&exprs[0], input_schema).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Failed to convert {func_name} arg to SQL"
                    ))
                })?;
                select_parts.push(format!("{}({arg_sql})", func_name.to_uppercase()));
            }
        } else if func_name == "avg" {
            let arg_sql = physical_expr_to_sql(&exprs[0], input_schema).ok_or_else(|| {
                DataFusionError::Internal("Failed to convert avg arg to SQL".to_string())
            })?;
            select_parts.push(format!("SUM(CAST({arg_sql} AS DOUBLE))"));
            select_parts.push(format!("COUNT({arg_sql})"));
        } else {
            return Err(DataFusionError::Internal(format!(
                "Unsupported aggregate function for pushdown: {func_name}"
            )));
        }
    }

    // WHERE clause from source filters
    let where_clause = if source_filters.is_empty() {
        String::new()
    } else {
        let filter_strs: Vec<String> = source_filters
            .iter()
            .map(|f| expr_to_sql::to_sql(f).map_err(|e| DataFusionError::Internal(e.to_string())))
            .collect::<Result<Vec<_>>>()?;
        format!(" WHERE {}", filter_strs.join(" AND "))
    };

    // GROUP BY clause
    let group_by_clause = if group_by_cols.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", group_by_cols.join(", "))
    };

    let table = table_reference.to_quoted_string();

    Ok(format!(
        "SELECT {} FROM {table}{where_clause}{group_by_clause}",
        select_parts.join(", ")
    ))
}

/// Convert a physical expression to a SQL string fragment.
pub(super) fn physical_expr_to_sql(
    expr: &Arc<dyn PhysicalExpr>,
    _schema: &SchemaRef,
) -> Option<String> {
    use datafusion::physical_expr::expressions::{
        BinaryExpr, CastExpr, Column, Literal, NegativeExpr,
    };

    let any = expr.as_any();

    if let Some(col) = any.downcast_ref::<Column>() {
        return Some(quote_ident(col.name()));
    }
    if let Some(lit) = any.downcast_ref::<Literal>() {
        return scalar_to_sql(lit.value());
    }
    if let Some(bin) = any.downcast_ref::<BinaryExpr>() {
        let left = physical_expr_to_sql(bin.left(), _schema)?;
        let right = physical_expr_to_sql(bin.right(), _schema)?;
        return Some(format!("({left} {op} {right})", op = bin.op()));
    }
    if let Some(cast) = any.downcast_ref::<CastExpr>() {
        let inner = physical_expr_to_sql(cast.expr(), _schema)?;
        let dt = arrow_type_to_sql(cast.cast_type())?;
        return Some(format!("CAST({inner} AS {dt})"));
    }
    if let Some(neg) = any.downcast_ref::<NegativeExpr>() {
        let inner = physical_expr_to_sql(neg.arg(), _schema)?;
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
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Some(format!("'{v}'")),
        ScalarValue::Boolean(Some(v)) => Some(v.to_string()),
        ScalarValue::Null => Some("NULL".to_string()),
        _ => None,
    }
}

fn arrow_type_to_sql(dt: &datafusion::arrow::datatypes::DataType) -> Option<String> {
    use datafusion::arrow::datatypes::DataType;
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Some("INTEGER".to_string()),
        DataType::Int64 => Some("BIGINT".to_string()),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Some("INTEGER".to_string()),
        DataType::UInt64 => Some("BIGINT".to_string()),
        DataType::Float32 => Some("FLOAT".to_string()),
        DataType::Float64 | DataType::Float16 => Some("DOUBLE".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Some("VARCHAR".to_string())
        }
        DataType::Boolean => Some("BOOLEAN".to_string()),
        DataType::Decimal128(p, s) => Some(format!("DECIMAL({p},{s})")),
        DataType::Decimal256(p, s) => Some(format!("DECIMAL({p},{s})")),
        _ => None,
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{name}\"")
}
