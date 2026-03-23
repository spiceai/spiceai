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

//! Physical optimizer rule that pushes partial aggregations into `FlightSqlExec` SQL queries.
//!
//! This rewrites patterns like:
//! ```text
//! AggregateExec(mode=Partial, gby=[col], aggr=[sum(x), count(*)])
//!   UnionExec
//!     FlightSqlExec sql="SELECT x, col FROM table"
//! ```
//! Into:
//! ```text
//! UnionExec
//!   PartialAggregationFlightSqlExec  (builds SQL at execution time)
//! ```

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};

use datafusion::physical_expr::PhysicalExpr;

use data_components::flightsql::FlightSqlExec;

use super::exec::{
    DECOMPOSED_AGGREGATES, PartialAggregationFlightSqlExec, SIMPLE_PUSHDOWN_AGGREGATES,
};

/// Pass-through [`ExecutionPlan`] nodes that can sit between an `AggregateExec` and
/// `FlightSqlExec` without blocking pushdown. Identified by [`ExecutionPlan::name`]
/// because the concrete types live in upstream crates not available for downcasting.
const PASS_THROUGH_EXEC_NAMES: &[&str] = &["CooperativeExec", "BytesProcessedExec"];

/// A [`PhysicalOptimizerRule`] that pushes `AggregateExec(mode=Partial)` operations
/// into `FlightSqlExec` SQL queries when the partial aggregate sits directly above
/// partitioned FlightSQL scans (possibly with pass-through nodes in between).
#[derive(Debug, Default)]
pub struct FlightSQLPartialAggregatePushdown {}

impl FlightSQLPartialAggregatePushdown {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

impl PhysicalOptimizerRule for FlightSQLPartialAggregatePushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(try_rewrite_partial_aggregate)
            .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "FlightSQLPartialAggregatePushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Attempt to rewrite an `AggregateExec(mode=Partial)` by pushing the aggregation
/// into each child `FlightSqlExec`'s SQL query.
fn try_rewrite_partial_aggregate(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() else {
        return Ok(Transformed::no(plan));
    };
    if *agg.mode() != AggregateMode::Partial {
        return Ok(Transformed::no(plan));
    }
    if !all_aggregates_pushable(agg) {
        return Ok(Transformed::no(plan));
    }

    let child = skip_repartition_roundrobin(agg.input());

    // Check for an intermediate ProjectionExec (inserted by CSE) between the
    // aggregate and the scan. If present, build a substitution map so that
    // synthetic columns like `__common_expr_1` can be inlined back to their
    // original expressions during SQL generation.
    let (scan_child, column_substitutions) = skip_projection(child);

    let flight_execs = collect_flight_execs(scan_child);
    let Some(flight_execs) = flight_execs else {
        return Ok(Transformed::no(plan));
    };
    if flight_execs.is_empty() {
        return Ok(Transformed::no(plan));
    }

    // Build the output schema: must match AggregateExec(Partial)'s output exactly
    let output_schema = build_output_schema(agg);

    let mut new_children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(flight_execs.len());
    for flight in &flight_execs {
        let Some(new_exec) =
            build_pushdown_exec(agg, flight, &output_schema, &column_substitutions)?
        else {
            return Ok(Transformed::no(plan));
        };
        new_children.push(new_exec);
    }

    let result: Arc<dyn ExecutionPlan> = if new_children.len() == 1 {
        new_children.remove(0)
    } else {
        UnionExec::try_new(new_children)?
    };

    Ok(Transformed::yes(result))
}
/// Returns `true` if all aggregate expressions in the [`AggregateExec`] are
/// eligible for pushdown: supported function, non-distinct, no FILTER clause.
fn all_aggregates_pushable(agg: &AggregateExec) -> bool {
    if agg.group_expr().has_grouping_set() {
        return false;
    }

    let filters = agg.filter_expr();
    for (i, aggr) in agg.aggr_expr().iter().enumerate() {
        let name = aggr.fun().name();
        if !SIMPLE_PUSHDOWN_AGGREGATES.contains(name) && !DECOMPOSED_AGGREGATES.contains(name) {
            return false;
        }
        if aggr.is_distinct() {
            return false;
        }
        if filters.get(i).is_some_and(|f| f.is_some()) {
            return false;
        }
    }
    true
}

// ─── Tree walking ────────────────────────────────────────────────────

/// Skip through a `RepartitionExec(RoundRobinBatch)` if present.
fn skip_repartition_roundrobin(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    if let Some(repart) = plan.as_any().downcast_ref::<RepartitionExec>() {
        if matches!(repart.partitioning(), Partitioning::RoundRobinBatch(_)) {
            return repart.input();
        }
    }
    plan
}

/// Skip through a `ProjectionExec` if present, returning a column substitution
/// map that maps each output column index to the projection's source expression.
///
/// DataFusion's common subexpression elimination (CSE) can insert a
/// `ProjectionExec` between `AggregateExec(Partial)` and the scan to
/// pre-compute shared sub-expressions (e.g. `l_extendedprice * (1 - l_discount)`
/// → `__common_expr_1`). The aggregate then references these synthetic columns.
///
/// The substitution map allows SQL generation to inline such synthetic columns
/// back to the original expressions over the scan schema.
fn skip_projection(
    plan: &Arc<dyn ExecutionPlan>,
) -> (&Arc<dyn ExecutionPlan>, Vec<Arc<dyn PhysicalExpr>>) {
    if let Some(proj) = plan.as_any().downcast_ref::<ProjectionExec>() {
        let substitutions: Vec<Arc<dyn PhysicalExpr>> =
            proj.expr().iter().map(|pe| Arc::clone(&pe.expr)).collect();
        return (proj.input(), substitutions);
    }
    (plan, Vec::new())
}

/// Collect `FlightSqlExec` references from a `UnionExec` or a single node.
///
/// Walks through known pass-through nodes (`CooperativeExec`, `BytesProcessedExec`)
/// identified by name (they are defined in upstream crates not available for downcasting).
/// Returns `None` if any child doesn't terminate at `FlightSqlExec`.
fn collect_flight_execs(plan: &Arc<dyn ExecutionPlan>) -> Option<Vec<&FlightSqlExec>> {
    if let Some(union_exec) = plan.as_any().downcast_ref::<UnionExec>() {
        let mut execs = Vec::with_capacity(union_exec.inputs().len());
        for child in union_exec.inputs() {
            execs.push(walk_to_flight_exec(child)?);
        }
        Some(execs)
    } else {
        Some(vec![walk_to_flight_exec(plan)?])
    }
}

/// Walk through single-input pass-through nodes to find a `FlightSqlExec`.
///
/// Recognised pass-through nodes (single input, no semantic change for pushdown):
/// - `FilterExec` — the underlying `FlightSqlExec` already carries the filter in
///   its SQL; the `FilterExec` is a redundant safety layer added by DataFusion.
/// - `RepartitionExec` — only shuffles partitions, no data change.
/// - Name-identified nodes in [`PASS_THROUGH_EXEC_NAMES`] (`CooperativeExec`,
///   `BytesProcessedExec`) — defined in upstream crates, not available for
///   downcasting.
///
/// Returns `None` if the chain contains a multi-input node or an unrecognised
/// node, or terminates at a non-`FlightSqlExec`.
fn walk_to_flight_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<&FlightSqlExec> {
    let mut current: &Arc<dyn ExecutionPlan> = plan;
    loop {
        if let Some(flight) = current.as_any().downcast_ref::<FlightSqlExec>() {
            return Some(flight);
        }

        let children = current.children();
        if children.len() != 1 {
            return None;
        }

        if current.as_any().downcast_ref::<FilterExec>().is_some()
            || current.as_any().downcast_ref::<RepartitionExec>().is_some()
            || PASS_THROUGH_EXEC_NAMES.contains(&current.name())
        {
            current = children[0];
        } else {
            return None;
        }
    }
}

// ─── Exec construction ───────────────────────────────────────────────

/// Build a `PartialAggregationFlightSqlExec` for one partition.
fn build_pushdown_exec(
    agg: &AggregateExec,
    flight: &FlightSqlExec,
    output_schema: &SchemaRef,
    column_substitutions: &[Arc<dyn PhysicalExpr>],
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let exec = PartialAggregationFlightSqlExec::new(
        flight,
        agg.group_expr().clone(),
        agg.aggr_expr().to_vec(),
        flight.schema(),
        Arc::clone(output_schema),
        column_substitutions.to_vec(),
    );

    // Validate that SQL can be built (fail fast during optimization, not at execution)
    if exec.sql().is_err() {
        return Ok(None);
    }

    Ok(Some(Arc::new(exec)))
}

/// Build the output schema for the pushdown.
/// Uses the `AggregateExec(Partial)`'s own output schema, which is the exact
/// schema that the parent `AggregateExec(FinalPartitioned)` expects.
/// This preserves field names and types so the schema-check validation passes.
fn build_output_schema(agg: &AggregateExec) -> SchemaRef {
    agg.schema()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::config::ConfigOptions;
    use datafusion::execution::TaskContext;
    use datafusion::functions_aggregate::average::avg_udaf;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
    use datafusion::physical_expr::expressions::{Column, Literal};
    use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
    };
    use datafusion::scalar::ScalarValue;
    use datafusion::sql::TableReference;
    use flight_client::cookie::CookieStore;
    use std::any::Any;
    use std::fmt;
    use tonic::transport::Channel;

    fn lineitem_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("l_quantity", DataType::Decimal128(15, 2), true),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), true),
            Field::new("l_discount", DataType::Decimal128(15, 2), true),
            Field::new("l_returnflag", DataType::Utf8View, true),
            Field::new("l_linestatus", DataType::Utf8View, true),
        ]))
    }

    fn dummy_client() -> data_components::flightsql::FlightSqlClient {
        let channel = Channel::from_static("http://[::1]:1").connect_lazy();
        let cookie_svc =
            flight_client::cookie::CookieService::new(channel, Arc::new(CookieStore::new()));
        FlightSqlServiceClient::new(cookie_svc)
    }

    fn make_flight_exec(
        schema: &SchemaRef,
        table_ref: &str,
        filters: &[datafusion::logical_expr::Expr],
    ) -> Arc<dyn ExecutionPlan> {
        let cookie_store = Arc::new(CookieStore::new());
        Arc::new(
            FlightSqlExec::new(
                None,
                schema,
                &TableReference::parse_str(table_ref),
                dummy_client(),
                filters,
                None,
                cookie_store,
            )
            .expect("build FlightSqlExec"),
        )
    }

    fn make_union(children: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        UnionExec::try_new(children).expect("build UnionExec")
    }

    fn make_group_by(names: &[(&str, usize)]) -> PhysicalGroupBy {
        PhysicalGroupBy::new_single(
            names
                .iter()
                .map(|&(name, idx)| {
                    (
                        Arc::new(Column::new(name, idx)) as Arc<dyn PhysicalExpr>,
                        name.to_string(),
                    )
                })
                .collect(),
        )
    }

    fn partial_aggregate(
        input: Arc<dyn ExecutionPlan>,
        group_by_indices: &[(usize, &str)],
        aggregates: Vec<Arc<AggregateFunctionExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let group_by = make_group_by(
            &group_by_indices
                .iter()
                .map(|&(idx, name)| (name, idx))
                .collect::<Vec<_>>(),
        );
        let filters: Vec<Option<Arc<dyn PhysicalExpr>>> = vec![None; aggregates.len()];
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            aggregates,
            filters,
            input,
            input_schema,
        )?))
    }

    fn plan_display(plan: &Arc<dyn ExecutionPlan>) -> String {
        datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string()
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        FlightSQLPartialAggregatePushdown::default().optimize(plan, &ConfigOptions::default())
    }

    // ─── Positive: pushdown happens ──────────────────────────────────

    #[tokio::test]
    async fn test_pushdown_sum_count_with_groupby() -> Result<()> {
        let schema = lineitem_schema();
        let union_input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
        ]);

        let sum_expr =
            AggregateExprBuilder::new(sum_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                .schema(Arc::clone(&schema))
                .alias("sum(l_quantity)")
                .build()?;
        let count_expr = AggregateExprBuilder::new(
            count_udaf(),
            vec![Arc::new(Literal::new(ScalarValue::Int64(Some(1))))],
        )
        .schema(Arc::clone(&schema))
        .alias("count(*)")
        .build()?;

        let agg = partial_aggregate(
            union_input,
            &[(3, "l_returnflag")],
            vec![Arc::new(sum_expr), Arc::new(count_expr)],
        )?;

        let optimized = optimize(agg)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        UnionExec
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0", COUNT(1) AS "__agg_1" FROM foo.foo.lineitem GROUP BY "l_returnflag"
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0", COUNT(1) AS "__agg_1" FROM foo.foo.lineitem GROUP BY "l_returnflag"
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn test_pushdown_global_aggregate_no_groupby() -> Result<()> {
        let schema = lineitem_schema();
        let union_input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
        ]);

        let sum_expr =
            AggregateExprBuilder::new(sum_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                .schema(Arc::clone(&schema))
                .alias("sum(l_quantity)")
                .build()?;

        let agg = partial_aggregate(union_input, &[], vec![Arc::new(sum_expr)])?;

        let optimized = optimize(agg)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        UnionExec
          PartialAggregationFlightSqlExec sql=SELECT SUM("l_quantity") AS "__agg_0" FROM foo.foo.lineitem
          PartialAggregationFlightSqlExec sql=SELECT SUM("l_quantity") AS "__agg_0" FROM foo.foo.lineitem
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn test_pushdown_min_max() -> Result<()> {
        let schema = lineitem_schema();
        let union_input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
        ]);

        let min_expr =
            AggregateExprBuilder::new(min_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                .schema(Arc::clone(&schema))
                .alias("min(l_quantity)")
                .build()?;
        let max_expr =
            AggregateExprBuilder::new(max_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                .schema(Arc::clone(&schema))
                .alias("max(l_quantity)")
                .build()?;

        let agg = partial_aggregate(
            union_input,
            &[(3, "l_returnflag")],
            vec![Arc::new(min_expr), Arc::new(max_expr)],
        )?;

        let optimized = optimize(agg)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        UnionExec
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", MIN("l_quantity") AS "__agg_0", MAX("l_quantity") AS "__agg_1" FROM foo.foo.lineitem GROUP BY "l_returnflag"
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", MIN("l_quantity") AS "__agg_0", MAX("l_quantity") AS "__agg_1" FROM foo.foo.lineitem GROUP BY "l_returnflag"
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn test_pushdown_avg_decomposition() -> Result<()> {
        let schema = lineitem_schema();
        let union_input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
        ]);

        let avg_expr =
            AggregateExprBuilder::new(avg_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                .schema(Arc::clone(&schema))
                .alias("avg(l_quantity)")
                .build()?;

        let agg = partial_aggregate(
            union_input,
            &[(3, "l_returnflag")],
            vec![Arc::new(avg_expr)],
        )?;

        let optimized = optimize(agg)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        UnionExec
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0", COUNT("l_quantity") AS "__agg_1" FROM foo.foo.lineitem GROUP BY "l_returnflag"
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0", COUNT("l_quantity") AS "__agg_1" FROM foo.foo.lineitem GROUP BY "l_returnflag"
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn test_pushdown_single_partition_no_union() -> Result<()> {
        let schema = lineitem_schema();
        let flight = make_flight_exec(&schema, "foo.foo.lineitem", &[]);

        let sum_expr =
            AggregateExprBuilder::new(sum_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                .schema(Arc::clone(&schema))
                .alias("sum(l_quantity)")
                .build()?;

        let agg = partial_aggregate(flight, &[(3, "l_returnflag")], vec![Arc::new(sum_expr)])?;

        let optimized = optimize(agg)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0" FROM foo.foo.lineitem GROUP BY "l_returnflag""#);

        Ok(())
    }

    #[tokio::test]
    async fn test_pushdown_preserves_filters_in_where() -> Result<()> {
        let schema = lineitem_schema();
        use datafusion::logical_expr::{col, lit};
        let filter = col("l_shipdate").gt(lit(ScalarValue::Int64(Some(100))));
        let union_input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", &[filter.clone()]),
            make_flight_exec(&schema, "foo.foo.lineitem", &[filter]),
        ]);

        let sum_expr =
            AggregateExprBuilder::new(sum_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                .schema(Arc::clone(&schema))
                .alias("sum(l_quantity)")
                .build()?;

        let agg = partial_aggregate(
            union_input,
            &[(3, "l_returnflag")],
            vec![Arc::new(sum_expr)],
        )?;

        let optimized = optimize(agg)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        UnionExec
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0" FROM foo.foo.lineitem WHERE "l_shipdate" > 100 GROUP BY "l_returnflag"
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0" FROM foo.foo.lineitem WHERE "l_shipdate" > 100 GROUP BY "l_returnflag"
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn test_pushdown_multiple_group_by_cols() -> Result<()> {
        let schema = lineitem_schema();
        let union_input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
        ]);

        let sum_expr =
            AggregateExprBuilder::new(sum_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                .schema(Arc::clone(&schema))
                .alias("sum(l_quantity)")
                .build()?;

        let agg = partial_aggregate(
            union_input,
            &[(3, "l_returnflag"), (4, "l_linestatus")],
            vec![Arc::new(sum_expr)],
        )?;

        let optimized = optimize(agg)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        UnionExec
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", "l_linestatus", SUM("l_quantity") AS "__agg_0" FROM foo.foo.lineitem GROUP BY "l_returnflag", "l_linestatus"
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", "l_linestatus", SUM("l_quantity") AS "__agg_0" FROM foo.foo.lineitem GROUP BY "l_returnflag", "l_linestatus"
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn test_pushdown_sum_and_avg_same_column_dedup() -> Result<()> {
        let schema = lineitem_schema();
        let union_input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
        ]);

        let sum_expr =
            AggregateExprBuilder::new(sum_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                .schema(Arc::clone(&schema))
                .alias("sum(l_quantity)")
                .build()?;
        let avg_expr =
            AggregateExprBuilder::new(avg_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                .schema(Arc::clone(&schema))
                .alias("avg(l_quantity)")
                .build()?;

        let agg = partial_aggregate(
            union_input,
            &[(3, "l_returnflag")],
            vec![Arc::new(sum_expr), Arc::new(avg_expr)],
        )?;

        let optimized = optimize(agg)?;
        // SUM("l_quantity") appears only once (deduped); AVG reuses it + adds COUNT
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        UnionExec
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0", COUNT("l_quantity") AS "__agg_1" FROM foo.foo.lineitem GROUP BY "l_returnflag"
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0", COUNT("l_quantity") AS "__agg_1" FROM foo.foo.lineitem GROUP BY "l_returnflag"
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn test_pushdown_through_projection_exec() -> Result<()> {
        use datafusion::physical_expr::expressions::BinaryExpr;
        use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};

        let schema = lineitem_schema();
        let union_input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
        ]);

        // Simulate CSE: ProjectionExec computes __common_expr_1 = l_extendedprice * l_discount
        let proj = Arc::new(ProjectionExec::try_new(
            vec![
                ProjectionExpr {
                    expr: Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("l_extendedprice", 1)),
                        datafusion::logical_expr::Operator::Multiply,
                        Arc::new(Column::new("l_discount", 2)),
                    )),
                    alias: "__common_expr_1".to_string(),
                },
                ProjectionExpr {
                    expr: Arc::new(Column::new("l_returnflag", 3)),
                    alias: "l_returnflag".to_string(),
                },
            ],
            union_input,
        )?) as Arc<dyn ExecutionPlan>;

        // Aggregate references __common_expr_1 at index 0 and l_returnflag at index 1
        let proj_schema = proj.schema();
        let sum_expr = AggregateExprBuilder::new(
            sum_udaf(),
            vec![Arc::new(Column::new("__common_expr_1", 0))],
        )
        .schema(Arc::clone(&proj_schema))
        .alias("sum(__common_expr_1)")
        .build()?;

        let agg = partial_aggregate(proj, &[(1, "l_returnflag")], vec![Arc::new(sum_expr)])?;

        let optimized = optimize(agg)?;
        // The CSE column __common_expr_1 is inlined back to ("l_extendedprice" * "l_discount")
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        UnionExec
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM(("l_extendedprice" * "l_discount")) AS "__agg_0" FROM foo.foo.lineitem GROUP BY "l_returnflag"
          PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM(("l_extendedprice" * "l_discount")) AS "__agg_0" FROM foo.foo.lineitem GROUP BY "l_returnflag"
        "#);

        Ok(())
    }

    // ─── Negative: pushdown does NOT happen ──────────────────────────

    /// A mock leaf exec that is NOT a `FlightSqlExec`.
    #[derive(Debug)]
    struct MockNonFlightExec {
        schema: SchemaRef,
        properties: PlanProperties,
    }

    impl MockNonFlightExec {
        fn new(schema: SchemaRef) -> Self {
            let props = PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                schema,
                properties: props,
            }
        }
    }

    impl DisplayAs for MockNonFlightExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "MockNonFlightExec")
        }
    }

    impl ExecutionPlan for MockNonFlightExec {
        fn name(&self) -> &'static str {
            "MockNonFlightExec"
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        fn properties(&self) -> &PlanProperties {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }
    }

    /// Assert that the optimizer does not change the plan.
    fn assert_no_pushdown(plan: Arc<dyn ExecutionPlan>) {
        let before = plan_display(&plan);
        let after = plan_display(&optimize(plan).expect("optimize should not error"));
        assert_eq!(before, after, "plan should be unchanged");
    }

    #[tokio::test]
    async fn test_no_pushdown_non_flightsql_child() -> Result<()> {
        let schema = lineitem_schema();
        let input = make_union(vec![Arc::new(MockNonFlightExec::new(Arc::clone(&schema)))]);
        assert_no_pushdown(partial_aggregate(
            input,
            &[(3, "l_returnflag")],
            vec![Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                    .schema(Arc::clone(&schema))
                    .alias("sum")
                    .build()?,
            )],
        )?);
        Ok(())
    }

    #[tokio::test]
    async fn test_no_pushdown_distinct_aggregate() -> Result<()> {
        let schema = lineitem_schema();
        let input = make_union(vec![make_flight_exec(&schema, "foo.foo.lineitem", &[])]);
        assert_no_pushdown(partial_aggregate(
            input,
            &[(3, "l_returnflag")],
            vec![Arc::new(
                AggregateExprBuilder::new(
                    count_udaf(),
                    vec![Arc::new(Column::new("l_quantity", 0))],
                )
                .schema(Arc::clone(&schema))
                .alias("cd")
                .distinct()
                .build()?,
            )],
        )?);
        Ok(())
    }

    #[tokio::test]
    async fn test_no_pushdown_final_mode() -> Result<()> {
        let schema = lineitem_schema();
        let input = make_union(vec![make_flight_exec(&schema, "foo.foo.lineitem", &[])]);
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("l_returnflag", 3)) as _,
            "l_returnflag".to_string(),
        )]);
        let agg: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            group_by,
            vec![Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                    .schema(Arc::clone(&schema))
                    .alias("sum")
                    .build()?,
            )],
            vec![None],
            input,
            schema,
        )?);
        assert_no_pushdown(agg);
        Ok(())
    }

    #[tokio::test]
    async fn test_no_pushdown_mixed_flight_and_non_flight() -> Result<()> {
        let schema = lineitem_schema();
        let input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
            Arc::new(MockNonFlightExec::new(Arc::clone(&schema))),
        ]);
        assert_no_pushdown(partial_aggregate(
            input,
            &[(3, "l_returnflag")],
            vec![Arc::new(
                AggregateExprBuilder::new(sum_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                    .schema(Arc::clone(&schema))
                    .alias("sum")
                    .build()?,
            )],
        )?);
        Ok(())
    }
}
