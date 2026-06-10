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
/// partitioned `FlightSQL` scans (possibly with pass-through nodes in between).
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

    fn name(&self) -> &'static str {
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

    // A RoundRobin RepartitionExec can also sit BELOW the CSE projection
    // (Aggregate → Projection → Repartition → Union → …Flight). This is the
    // common shape for aggregates over computed arguments — e.g.
    // `sum(l_extendedprice * (1 - l_discount))` (TPC-H Q1) — where CSE pulls the
    // computed expression into a projection that lands above the repartition.
    // Skip it here too so `collect_flight_execs` sees the `UnionExec` directly
    // instead of bailing on a multi-input node mid-walk. The repartition
    // preserves the scan schema, so the projection substitutions stay valid.
    let scan_child = skip_repartition_roundrobin(scan_child);

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
            build_pushdown_exec(agg, flight, &output_schema, &column_substitutions)
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
        if filters.get(i).is_some_and(Option::is_some) {
            return false;
        }
    }
    true
}

// ─── Tree walking ────────────────────────────────────────────────────

/// Skip through a `RepartitionExec(RoundRobinBatch)` if present.
fn skip_repartition_roundrobin(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    if let Some(repart) = plan.as_any().downcast_ref::<RepartitionExec>()
        && matches!(repart.partitioning(), Partitioning::RoundRobinBatch(_))
    {
        return repart.input();
    }
    plan
}

/// Skip through a `ProjectionExec` if present, returning a column substitution
/// map that maps each output column index to the projection's source expression.
///
/// `DataFusion`'s common subexpression elimination (CSE) can insert a
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
/// - `RepartitionExec` — only shuffles partitions, no data change.
/// - Name-identified nodes in [`PASS_THROUGH_EXEC_NAMES`] (`CooperativeExec`,
///   `BytesProcessedExec`) — defined in upstream crates, not available for
///   downcasting.
///
/// `FilterExec` is deliberately NOT pass-through. `FlightSQLTable` reports
/// filter pushdown as `Exact` (predicate absorbed into the scan SQL, no
/// `FilterExec` planned) or `Unsupported` (predicate stays in the plan as a
/// `FilterExec` and is NOT in the scan SQL) — it never reports `Inexact`. A
/// `FilterExec` here therefore always carries a predicate the pushed-down
/// aggregation SQL would lose, and the rewrite would silently aggregate over
/// unfiltered rows. Bail and keep the local partial aggregate instead.
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

        if current.as_any().downcast_ref::<RepartitionExec>().is_some()
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
) -> Option<Arc<dyn ExecutionPlan>> {
    let exec = PartialAggregationFlightSqlExec::new(
        flight,
        agg.group_expr().clone(),
        agg.aggr_expr().to_vec(),
        Arc::clone(output_schema),
        column_substitutions.to_vec(),
    );

    // Validate that SQL can be built (fail fast during optimization, not at execution)
    if exec.sql().is_err() {
        return None;
    }

    Some(Arc::new(exec))
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
    use datafusion::arrow::array::{
        Decimal128Array, Float64Array, RecordBatch, StringViewArray, UInt64Array,
    };
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
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, collect,
    };
    use datafusion::scalar::ScalarValue;
    use datafusion::sql::TableReference;
    use datafusion_datasource::memory::MemorySourceConfig;
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

    /// Build only the `AggregateExec(Partial)` — used by tests that only need to
    /// verify the optimizer rewrites the partial aggregate.
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

    /// Build the full two-phase aggregate plan that `DataFusion` produces:
    ///
    /// ```text
    /// AggregateExec(mode=Final)
    ///   CoalescePartitionsExec
    ///     AggregateExec(mode=Partial)
    ///       <input>
    /// ```
    ///
    /// Both aggregates share the same `input_schema` (the original scan schema),
    /// matching `DataFusion`'s own Partial→Final split convention.
    fn full_aggregate(
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
        let partial: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            group_by.clone(),
            aggregates.clone(),
            filters.clone(),
            input,
            Arc::clone(&input_schema),
        )?);
        let coalesce: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(partial));
        let final_agg: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            group_by.as_final(),
            aggregates,
            filters,
            coalesce,
            input_schema,
        )?);
        Ok(final_agg)
    }

    /// Build a `DataSourceExec` that returns pre-computed partial aggregate data.
    ///
    /// Each inner `Vec<RecordBatch>` represents one partition. Used to replace
    /// `PartialAggregationFlightSqlExec` nodes in optimized plans so the full
    /// `AggregateExec(Final)` can be executed with deterministic mock data.
    fn make_memory_exec(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
    ) -> Arc<dyn ExecutionPlan> {
        MemorySourceConfig::try_new_exec(partitions, schema, None)
            .expect("build MemorySourceConfig")
    }

    /// Walk an optimised plan tree and replace every
    /// `PartialAggregationFlightSqlExec` with a `DataSourceExec` fed by `data`.
    ///
    /// `data` is consumed round-robin: each encountered pushdown node gets the
    /// next entry. The replacement exec produces a single partition containing
    /// the provided batches, matching the output schema of the pushdown node.
    fn replace_pushdown_with_memory(
        plan: Arc<dyn ExecutionPlan>,
        data: &mut impl Iterator<Item = Vec<RecordBatch>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if plan
            .as_any()
            .downcast_ref::<PartialAggregationFlightSqlExec>()
            .is_some()
        {
            let schema = plan.schema();
            let partition_data = data.next().ok_or_else(|| {
                datafusion::common::DataFusionError::Internal(
                    "ran out of mock data for pushdown replacement".to_string(),
                )
            })?;
            return Ok(make_memory_exec(&[partition_data], schema));
        }
        let children = plan.children();
        if children.is_empty() {
            return Ok(plan);
        }
        let new_children: Vec<Arc<dyn ExecutionPlan>> = children
            .into_iter()
            .map(|c| replace_pushdown_with_memory(Arc::clone(c), data))
            .collect::<Result<_>>()?;
        plan.with_new_children(new_children)
    }

    fn plan_display(plan: &Arc<dyn ExecutionPlan>) -> String {
        let raw = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        // Strip trailing whitespace per line — `indent(true)` can right-pad
        // shorter lines, making snapshots sensitive to unrelated line-length changes.
        raw.lines()
            .map(str::trim_end)
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        FlightSQLPartialAggregatePushdown::default().optimize(plan, &ConfigOptions::default())
    }

    // ─── Positive: pushdown plan shape ───────────────────────────────

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

        let plan = full_aggregate(
            union_input,
            &[(3, "l_returnflag")],
            vec![Arc::new(sum_expr), Arc::new(count_expr)],
        )?;

        let optimized = optimize(plan)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[l_returnflag@0 as l_returnflag], aggr=[sum(l_quantity), count(*)]
          CoalescePartitionsExec
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

        let plan = full_aggregate(union_input, &[], vec![Arc::new(sum_expr)])?;

        let optimized = optimize(plan)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[], aggr=[sum(l_quantity)]
          CoalescePartitionsExec
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

        let plan = full_aggregate(
            union_input,
            &[(3, "l_returnflag")],
            vec![Arc::new(min_expr), Arc::new(max_expr)],
        )?;

        let optimized = optimize(plan)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[l_returnflag@0 as l_returnflag], aggr=[min(l_quantity), max(l_quantity)]
          CoalescePartitionsExec
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

        let plan = full_aggregate(
            union_input,
            &[(3, "l_returnflag")],
            vec![Arc::new(avg_expr)],
        )?;

        let optimized = optimize(plan)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[l_returnflag@0 as l_returnflag], aggr=[avg(l_quantity)]
          CoalescePartitionsExec
            UnionExec
              PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", COUNT("l_quantity") AS "__agg_0", SUM("l_quantity") AS "__agg_1" FROM foo.foo.lineitem GROUP BY "l_returnflag"
              PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", COUNT("l_quantity") AS "__agg_0", SUM("l_quantity") AS "__agg_1" FROM foo.foo.lineitem GROUP BY "l_returnflag"
        "#);

        Ok(())
    }

    /// Regression test: column names containing "count" or "sum" as a
    /// substring (e.g. `l_dis`**`count`**) must not confuse the AVG state
    /// field classification. Both `COUNT` and `SUM` must appear in the
    /// pushed-down SQL.
    #[tokio::test]
    async fn test_pushdown_avg_column_name_contains_count() -> Result<()> {
        let schema = lineitem_schema();
        let union_input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
        ]);

        // l_discount is at index 2 in lineitem_schema — its name contains "count"
        let avg_expr =
            AggregateExprBuilder::new(avg_udaf(), vec![Arc::new(Column::new("l_discount", 2))])
                .schema(Arc::clone(&schema))
                .alias("avg(l_discount)")
                .build()?;

        let plan = full_aggregate(
            union_input,
            &[(3, "l_returnflag")],
            vec![Arc::new(avg_expr)],
        )?;

        let optimized = optimize(plan)?;
        // Must produce both COUNT and SUM, not two COUNTs
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[l_returnflag@0 as l_returnflag], aggr=[avg(l_discount)]
          CoalescePartitionsExec
            UnionExec
              PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", COUNT("l_discount") AS "__agg_0", SUM("l_discount") AS "__agg_1" FROM foo.foo.lineitem GROUP BY "l_returnflag"
              PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", COUNT("l_discount") AS "__agg_0", SUM("l_discount") AS "__agg_1" FROM foo.foo.lineitem GROUP BY "l_returnflag"
        "#);

        Ok(())
    }

    /// Regression test: end-to-end correctness for `avg(l_discount)` where
    /// the column name contains "count" as a substring. Verifies the Final
    /// aggregate computes `SUM / COUNT`, not `COUNT / COUNT`.
    #[tokio::test]
    async fn test_result_avg_column_name_contains_count() -> Result<()> {
        let schema = lineitem_schema();
        let union_input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
        ]);

        // l_discount at index 2
        let avg_expr =
            AggregateExprBuilder::new(avg_udaf(), vec![Arc::new(Column::new("l_discount", 2))])
                .schema(Arc::clone(&schema))
                .alias("avg(l_discount)")
                .build()?;

        let plan = full_aggregate(union_input, &[], vec![Arc::new(avg_expr)])?;
        let optimized = optimize(plan)?;

        let partial_schema = optimized.children()[0].children()[0].schema();
        assert_eq!(
            partial_schema.fields().len(),
            2,
            "AVG partial state must have exactly 2 fields (count, sum), got schema: {partial_schema}"
        );

        // Partition 1: [0.04, 0.06, 0.10] → COUNT=3, SUM=0.20 (20 in cents)
        // Partition 2: [0.08, 0.02]        → COUNT=2, SUM=0.10 (10 in cents)
        // Correct AVG = 0.30 / 5 = 0.06
        // Wrong   AVG = COUNT/COUNT = 1.0  (if SUM field got COUNT value)
        let mut data = vec![
            vec![make_scalar_partial_batch(&partial_schema, 3, 0, 0.0, 20)?],
            vec![make_scalar_partial_batch(&partial_schema, 2, 0, 0.0, 10)?],
        ]
        .into_iter();
        let executable = replace_pushdown_with_memory(optimized, &mut data)?;
        let result = execute_plan(executable).await?;

        insta::assert_snapshot!(pretty(&result), @r"
        +-----------------+
        | avg(l_discount) |
        +-----------------+
        | 0.060000        |
        +-----------------+
        ");

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

        let plan = full_aggregate(flight, &[(3, "l_returnflag")], vec![Arc::new(sum_expr)])?;

        let optimized = optimize(plan)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[l_returnflag@0 as l_returnflag], aggr=[sum(l_quantity)]
          CoalescePartitionsExec
            PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0" FROM foo.foo.lineitem GROUP BY "l_returnflag"
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn test_pushdown_preserves_filters_in_where() -> Result<()> {
        use datafusion::logical_expr::{col, lit};

        let schema = lineitem_schema();
        let filter = col("l_shipdate").gt(lit(ScalarValue::Int64(Some(100))));
        let union_input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", std::slice::from_ref(&filter)),
            make_flight_exec(&schema, "foo.foo.lineitem", std::slice::from_ref(&filter)),
        ]);

        let sum_expr =
            AggregateExprBuilder::new(sum_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                .schema(Arc::clone(&schema))
                .alias("sum(l_quantity)")
                .build()?;

        let plan = full_aggregate(
            union_input,
            &[(3, "l_returnflag")],
            vec![Arc::new(sum_expr)],
        )?;

        let optimized = optimize(plan)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[l_returnflag@0 as l_returnflag], aggr=[sum(l_quantity)]
          CoalescePartitionsExec
            UnionExec
              PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0" FROM foo.foo.lineitem WHERE ("l_shipdate" > 100) GROUP BY "l_returnflag"
              PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0" FROM foo.foo.lineitem WHERE ("l_shipdate" > 100) GROUP BY "l_returnflag"
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

        let plan = full_aggregate(
            union_input,
            &[(3, "l_returnflag"), (4, "l_linestatus")],
            vec![Arc::new(sum_expr)],
        )?;

        let optimized = optimize(plan)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[sum(l_quantity)]
          CoalescePartitionsExec
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

        let plan = full_aggregate(
            union_input,
            &[(3, "l_returnflag")],
            vec![Arc::new(sum_expr), Arc::new(avg_expr)],
        )?;

        let optimized = optimize(plan)?;
        // SUM("l_quantity") appears only once (deduped); AVG reuses it + adds COUNT
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[l_returnflag@0 as l_returnflag], aggr=[sum(l_quantity), avg(l_quantity)]
          CoalescePartitionsExec
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

    /// A `FilterExec` above a federated scan holds a predicate that is NOT in
    /// the scan's SQL (`FlightSQLTable` pushdown is `Exact` or `Unsupported`,
    /// never `Inexact`). Pushing the partial aggregate through it would
    /// silently aggregate over unfiltered rows, so the rule must bail.
    #[tokio::test]
    async fn test_no_pushdown_filter_exec_above_scan() -> Result<()> {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::BinaryExpr;
        use datafusion::physical_plan::filter::FilterExec;

        let schema = lineitem_schema();
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("l_returnflag", 3)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Utf8View(Some("A".to_string())))),
        ));
        let filtered_scan = Arc::new(FilterExec::try_new(
            predicate,
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
        )?) as Arc<dyn ExecutionPlan>;
        let input = make_union(vec![filtered_scan]);
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
    async fn test_no_pushdown_unsupported_aggregate_function() -> Result<()> {
        use datafusion::functions_aggregate::variance::var_samp_udaf;

        let schema = lineitem_schema();
        let input = make_union(vec![make_flight_exec(&schema, "foo.foo.lineitem", &[])]);
        assert_no_pushdown(partial_aggregate(
            input,
            &[(3, "l_returnflag")],
            vec![Arc::new(
                AggregateExprBuilder::new(
                    var_samp_udaf(),
                    vec![Arc::new(Column::new("l_quantity", 0))],
                )
                .schema(Arc::clone(&schema))
                .alias("var_samp")
                .build()?,
            )],
        )?);
        Ok(())
    }

    // ─── Result correctness ─────────────────────────────────────────
    //
    // These tests build a full `Final → Coalesce → Partial → FlightSqlExec`
    // plan, optimize it, then swap each `PartialAggregationFlightSqlExec`
    // with an in-memory exec returning pre-aggregated data (simulating what
    // the remote would return). Executing the resulting plan through
    // `AggregateExec(Final)` verifies end-to-end correctness.
    //
    // Test data — two "remote partitions" over `l_quantity` (Decimal128(15,2)):
    //   Partition 1: [10.00, 20.00, 30.00]  (flag = "A")
    //   Partition 2: [40.00, 50.00]          (flag = "A")
    // Expected global aggregates:
    //   SUM   = 150.00
    //   COUNT = 5
    //   AVG   = 30.00
    //   MIN   = 10.00
    //   MAX   = 50.00

    /// Execute a plan and collect all result batches into one.
    async fn execute_plan(plan: Arc<dyn ExecutionPlan>) -> Result<RecordBatch> {
        let ctx = Arc::new(TaskContext::default());
        let batches = collect(plan, ctx).await?;
        datafusion::arrow::compute::concat_batches(&batches[0].schema(), &batches)
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))
    }

    /// Format a [`RecordBatch`] as a pretty-printed table string via Arrow.
    fn pretty(batch: &RecordBatch) -> String {
        arrow::util::pretty::pretty_format_batches(std::slice::from_ref(batch))
            .expect("pretty format")
            .to_string()
    }

    /// Format a [`RecordBatch`] as a pretty-printed table with rows sorted
    /// (excluding header/footer lines) for deterministic GROUP BY output.
    fn pretty_sorted(batch: &RecordBatch) -> String {
        let formatted = pretty(batch);
        let mut lines: Vec<&str> = formatted.trim().lines().collect();
        let n = lines.len();
        if n > 3 {
            lines[2..n - 1].sort_unstable();
        }
        lines.join("\n")
    }

    /// Build a single-row [`RecordBatch`] whose schema matches the partial
    /// aggregate state produced by `AggregateExec(Partial)`.
    ///
    /// Walks the schema field-by-field, filling each column from the
    /// matching `values` entry by [`DataType`].
    ///
    /// `values` maps each supported type to a single scalar value:
    ///   - `DataType::UInt64`         → `u64_val`
    ///   - `DataType::Int64`          → `i64_val`
    ///   - `DataType::Float64`        → `f64_val`
    ///   - `DataType::Decimal128(p,s)`→ `decimal_val` (raw i128, reused for every Decimal128 field)
    fn make_scalar_partial_batch(
        schema: &SchemaRef,
        u64_val: u64,
        i64_val: i64,
        f64_val: f64,
        decimal_val: i128,
    ) -> Result<RecordBatch> {
        let columns: Vec<Arc<dyn arrow::array::Array>> = schema
            .fields()
            .iter()
            .map(|field| -> Result<Arc<dyn arrow::array::Array>> {
                match field.data_type() {
                    DataType::UInt64 => Ok(Arc::new(UInt64Array::from(vec![u64_val]))),
                    DataType::Int64 => Ok(Arc::new(arrow::array::Int64Array::from(vec![i64_val]))),
                    DataType::Float64 => Ok(Arc::new(Float64Array::from(vec![f64_val]))),
                    DataType::Decimal128(p, s) => Ok(Arc::new(
                        Decimal128Array::from(vec![decimal_val])
                            .with_precision_and_scale(*p, *s)
                            .expect("decimal"),
                    )),
                    other => Err(datafusion::common::DataFusionError::Internal(format!(
                        "make_scalar_partial_batch: unsupported type for field {}: {other}",
                        field.name()
                    ))),
                }
            })
            .collect::<Result<_>>()?;
        RecordBatch::try_new(Arc::clone(schema), columns)
            .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))
    }

    #[tokio::test]
    async fn test_result_sum_global() -> Result<()> {
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

        let plan = full_aggregate(union_input, &[], vec![Arc::new(sum_expr)])?;
        insta::assert_snapshot!(plan_display(&plan), @r#"
        AggregateExec: mode=Final, gby=[], aggr=[sum(l_quantity)]
          CoalescePartitionsExec
            AggregateExec: mode=Partial, gby=[], aggr=[sum(l_quantity)]
              UnionExec
                FlightSqlExec sql=SELECT l_quantity, l_extendedprice, l_discount, l_returnflag, l_linestatus FROM foo.foo.lineitem
                FlightSqlExec sql=SELECT l_quantity, l_extendedprice, l_discount, l_returnflag, l_linestatus FROM foo.foo.lineitem
        "#);
        let optimized = optimize(plan)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[], aggr=[sum(l_quantity)]
          CoalescePartitionsExec
            UnionExec
              PartialAggregationFlightSqlExec sql=SELECT SUM("l_quantity") AS "__agg_0" FROM foo.foo.lineitem
              PartialAggregationFlightSqlExec sql=SELECT SUM("l_quantity") AS "__agg_0" FROM foo.foo.lineitem
        "#);

        // The partial state schema for SUM(Decimal128(15,2)) is a single
        // Decimal128 accumulator field. Read it from the pushdown node.
        let partial_schema = optimized.children()[0] // CoalescePartitionsExec
            .children()[0] // UnionExec (or single pushdown)
            .schema();

        let &DataType::Decimal128(p, s) = partial_schema.field(0).data_type() else {
            panic!(
                "expected Decimal128 partial state, got {:?}",
                partial_schema.field(0).data_type()
            );
        };

        // Partition 1: SUM([10.00, 20.00, 30.00]) = 60.00 → 6000 in Decimal128(*,2)
        // Partition 2: SUM([40.00, 50.00])         = 90.00 → 9000 in Decimal128(*,2)
        let p1 = RecordBatch::try_new(
            Arc::clone(&partial_schema),
            vec![Arc::new(
                Decimal128Array::from(vec![6000i128])
                    .with_precision_and_scale(p, s)
                    .expect("decimal"),
            )],
        )?;
        let p2 = RecordBatch::try_new(
            Arc::clone(&partial_schema),
            vec![Arc::new(
                Decimal128Array::from(vec![9000i128])
                    .with_precision_and_scale(p, s)
                    .expect("decimal"),
            )],
        )?;

        let mut data = vec![vec![p1], vec![p2]].into_iter();
        let executable = replace_pushdown_with_memory(optimized, &mut data)?;
        let result = execute_plan(executable).await?;

        insta::assert_snapshot!(pretty(&result), @r#"
        +-----------------+
        | sum(l_quantity) |
        +-----------------+
        | 150.00          |
        +-----------------+
        "#);

        Ok(())
    }

    #[tokio::test]
    async fn test_result_count_global() -> Result<()> {
        let schema = lineitem_schema();
        let union_input = make_union(vec![
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
            make_flight_exec(&schema, "foo.foo.lineitem", &[]),
        ]);

        let count_expr = AggregateExprBuilder::new(
            count_udaf(),
            vec![Arc::new(Literal::new(ScalarValue::Int64(Some(1))))],
        )
        .schema(Arc::clone(&schema))
        .alias("count(*)")
        .build()?;

        let plan = full_aggregate(union_input, &[], vec![Arc::new(count_expr)])?;
        insta::assert_snapshot!(plan_display(&plan), @r#"
        AggregateExec: mode=Final, gby=[], aggr=[count(*)]
          CoalescePartitionsExec
            AggregateExec: mode=Partial, gby=[], aggr=[count(*)]
              UnionExec
                FlightSqlExec sql=SELECT l_quantity, l_extendedprice, l_discount, l_returnflag, l_linestatus FROM foo.foo.lineitem
                FlightSqlExec sql=SELECT l_quantity, l_extendedprice, l_discount, l_returnflag, l_linestatus FROM foo.foo.lineitem
        "#);
        let optimized = optimize(plan)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[], aggr=[count(*)]
          CoalescePartitionsExec
            UnionExec
              PartialAggregationFlightSqlExec sql=SELECT COUNT(1) AS "__agg_0" FROM foo.foo.lineitem
              PartialAggregationFlightSqlExec sql=SELECT COUNT(1) AS "__agg_0" FROM foo.foo.lineitem
        "#);

        let partial_schema = optimized.children()[0].children()[0].schema();
        // COUNT partial state is Int64
        let p1 = RecordBatch::try_new(
            Arc::clone(&partial_schema),
            vec![Arc::new(arrow::array::Int64Array::from(vec![3i64]))],
        )?;
        let p2 = RecordBatch::try_new(
            Arc::clone(&partial_schema),
            vec![Arc::new(arrow::array::Int64Array::from(vec![2i64]))],
        )?;

        let mut data = vec![vec![p1], vec![p2]].into_iter();
        let executable = replace_pushdown_with_memory(optimized, &mut data)?;
        let result = execute_plan(executable).await?;

        insta::assert_snapshot!(pretty(&result), @r#"
        +----------+
        | count(*) |
        +----------+
        | 5        |
        +----------+
        "#);

        Ok(())
    }

    /// Validates that AVG does NOT "average the averages." The pushdown
    /// decomposes AVG into SUM + COUNT per partition; the Final aggregate
    /// must merge as `total_sum / total_count`, not `avg(partition_avgs)`.
    ///
    /// Partition 1: [10.00, 20.00, 30.00] → SUM=60.00, COUNT=3
    /// Partition 2: [40.00, 50.00]         → SUM=90.00, COUNT=2
    /// Correct AVG = 150.00 / 5 = 30.00
    /// Wrong   AVG = (20.00 + 45.00) / 2 = 32.50  (average of averages)
    #[tokio::test]
    async fn test_result_avg_does_not_average_averages() -> Result<()> {
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

        let plan = full_aggregate(union_input, &[], vec![Arc::new(avg_expr)])?;
        insta::assert_snapshot!(plan_display(&plan), @r#"
        AggregateExec: mode=Final, gby=[], aggr=[avg(l_quantity)]
          CoalescePartitionsExec
            AggregateExec: mode=Partial, gby=[], aggr=[avg(l_quantity)]
              UnionExec
                FlightSqlExec sql=SELECT l_quantity, l_extendedprice, l_discount, l_returnflag, l_linestatus FROM foo.foo.lineitem
                FlightSqlExec sql=SELECT l_quantity, l_extendedprice, l_discount, l_returnflag, l_linestatus FROM foo.foo.lineitem
        "#);
        let optimized = optimize(plan)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[], aggr=[avg(l_quantity)]
          CoalescePartitionsExec
            UnionExec
              PartialAggregationFlightSqlExec sql=SELECT COUNT("l_quantity") AS "__agg_0", SUM("l_quantity") AS "__agg_1" FROM foo.foo.lineitem
              PartialAggregationFlightSqlExec sql=SELECT COUNT("l_quantity") AS "__agg_0", SUM("l_quantity") AS "__agg_1" FROM foo.foo.lineitem
        "#);

        // AVG partial state is (count: UInt64, sum: <input_type>) — DataFusion's
        // Avg accumulator preserves the input type for the sum field.
        // For Decimal128(15,2) input, state is (UInt64, Decimal128(15,2)).
        let partial_schema = optimized.children()[0].children()[0].schema();
        assert_eq!(
            partial_schema.fields().len(),
            2,
            "AVG partial state must have exactly 2 fields (count, sum), got schema: {partial_schema}"
        );

        // Partition 1: COUNT=3, SUM=60.00 (6000 cents)
        // Partition 2: COUNT=2, SUM=90.00 (9000 cents)
        let mut data = vec![
            vec![make_scalar_partial_batch(&partial_schema, 3, 0, 0.0, 6000)?],
            vec![make_scalar_partial_batch(&partial_schema, 2, 0, 0.0, 9000)?],
        ]
        .into_iter();
        let executable = replace_pushdown_with_memory(optimized, &mut data)?;
        let result = execute_plan(executable).await?;

        insta::assert_snapshot!(pretty(&result), @r"
        +-----------------+
        | avg(l_quantity) |
        +-----------------+
        | 30.000000       |
        +-----------------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_result_min_max_global() -> Result<()> {
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

        let plan = full_aggregate(
            union_input,
            &[],
            vec![Arc::new(min_expr), Arc::new(max_expr)],
        )?;
        insta::assert_snapshot!(plan_display(&plan), @r#"
        AggregateExec: mode=Final, gby=[], aggr=[min(l_quantity), max(l_quantity)]
          CoalescePartitionsExec
            AggregateExec: mode=Partial, gby=[], aggr=[min(l_quantity), max(l_quantity)]
              UnionExec
                FlightSqlExec sql=SELECT l_quantity, l_extendedprice, l_discount, l_returnflag, l_linestatus FROM foo.foo.lineitem
                FlightSqlExec sql=SELECT l_quantity, l_extendedprice, l_discount, l_returnflag, l_linestatus FROM foo.foo.lineitem
        "#);
        let optimized = optimize(plan)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[], aggr=[min(l_quantity), max(l_quantity)]
          CoalescePartitionsExec
            UnionExec
              PartialAggregationFlightSqlExec sql=SELECT MIN("l_quantity") AS "__agg_0", MAX("l_quantity") AS "__agg_1" FROM foo.foo.lineitem
              PartialAggregationFlightSqlExec sql=SELECT MIN("l_quantity") AS "__agg_0", MAX("l_quantity") AS "__agg_1" FROM foo.foo.lineitem
        "#);

        let partial_schema = optimized.children()[0].children()[0].schema();
        // Partition 1: MIN=10.00 (1000), MAX=30.00 (3000)
        let p1 = RecordBatch::try_new(
            Arc::clone(&partial_schema),
            vec![
                Arc::new(
                    Decimal128Array::from(vec![1000i128])
                        .with_precision_and_scale(15, 2)
                        .expect("decimal"),
                ),
                Arc::new(
                    Decimal128Array::from(vec![3000i128])
                        .with_precision_and_scale(15, 2)
                        .expect("decimal"),
                ),
            ],
        )?;
        // Partition 2: MIN=40.00 (4000), MAX=50.00 (5000)
        let p2 = RecordBatch::try_new(
            Arc::clone(&partial_schema),
            vec![
                Arc::new(
                    Decimal128Array::from(vec![4000i128])
                        .with_precision_and_scale(15, 2)
                        .expect("decimal"),
                ),
                Arc::new(
                    Decimal128Array::from(vec![5000i128])
                        .with_precision_and_scale(15, 2)
                        .expect("decimal"),
                ),
            ],
        )?;

        let mut data = vec![vec![p1], vec![p2]].into_iter();
        let executable = replace_pushdown_with_memory(optimized, &mut data)?;
        let result = execute_plan(executable).await?;

        insta::assert_snapshot!(pretty(&result), @r#"
        +-----------------+-----------------+
        | min(l_quantity) | max(l_quantity) |
        +-----------------+-----------------+
        | 10.00           | 50.00           |
        +-----------------+-----------------+
        "#);

        Ok(())
    }

    /// End-to-end test with GROUP BY: two groups across two partitions.
    ///
    /// Partition 1: ("A", qty=10.00), ("A", qty=20.00), ("B", qty=30.00)
    /// Partition 2: ("A", qty=40.00), ("B", qty=50.00)
    ///
    /// Expected:
    ///   A: SUM=70.00  COUNT=3  AVG=70/3≈23.33
    ///   B: SUM=80.00  COUNT=2  AVG=80/2=40.00
    #[tokio::test]
    async fn test_result_grouped_sum_count_avg() -> Result<()> {
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
        let avg_expr =
            AggregateExprBuilder::new(avg_udaf(), vec![Arc::new(Column::new("l_quantity", 0))])
                .schema(Arc::clone(&schema))
                .alias("avg(l_quantity)")
                .build()?;

        let plan = full_aggregate(
            union_input,
            &[(3, "l_returnflag")],
            vec![Arc::new(sum_expr), Arc::new(count_expr), Arc::new(avg_expr)],
        )?;
        insta::assert_snapshot!(plan_display(&plan), @r#"
        AggregateExec: mode=Final, gby=[l_returnflag@0 as l_returnflag], aggr=[sum(l_quantity), count(*), avg(l_quantity)]
          CoalescePartitionsExec
            AggregateExec: mode=Partial, gby=[l_returnflag@3 as l_returnflag], aggr=[sum(l_quantity), count(*), avg(l_quantity)]
              UnionExec
                FlightSqlExec sql=SELECT l_quantity, l_extendedprice, l_discount, l_returnflag, l_linestatus FROM foo.foo.lineitem
                FlightSqlExec sql=SELECT l_quantity, l_extendedprice, l_discount, l_returnflag, l_linestatus FROM foo.foo.lineitem
        "#);
        let optimized = optimize(plan)?;
        insta::assert_snapshot!(plan_display(&optimized), @r#"
        AggregateExec: mode=Final, gby=[l_returnflag@0 as l_returnflag], aggr=[sum(l_quantity), count(*), avg(l_quantity)]
          CoalescePartitionsExec
            UnionExec
              PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0", COUNT(1) AS "__agg_1", COUNT("l_quantity") AS "__agg_2" FROM foo.foo.lineitem GROUP BY "l_returnflag"
              PartialAggregationFlightSqlExec sql=SELECT "l_returnflag", SUM("l_quantity") AS "__agg_0", COUNT(1) AS "__agg_1", COUNT("l_quantity") AS "__agg_2" FROM foo.foo.lineitem GROUP BY "l_returnflag"
        "#);

        let partial_schema = optimized.children()[0].children()[0].schema();

        // Partial state schema fields (in order):
        //   0: l_returnflag (Utf8View)     — group-by key
        //   1: sum(l_quantity) state       — Decimal128
        //   2: count(*) state              — Int64
        //   3: avg(l_quantity)[count]       — UInt64
        //   4: avg(l_quantity)[sum]         — Decimal128 (same type as input)
        //
        // Build partition 1: ("A": SUM=30.00, COUNT=2, AVG→{count=2, sum=30.00}),
        //                    ("B": SUM=30.00, COUNT=1, AVG→{count=1, sum=30.00})
        // Build partition 2: ("A": SUM=40.00, COUNT=1, AVG→{count=1, sum=40.00}),
        //                    ("B": SUM=50.00, COUNT=1, AVG→{count=1, sum=50.00})
        let build_batch = |flags: &[&str],
                           sums_cents: &[i128],
                           counts: &[i64],
                           avg_counts: &[u64],
                           avg_sums_cents: &[i128]|
         -> Result<RecordBatch> {
            let mut columns: Vec<Arc<dyn arrow::array::Array>> =
                Vec::with_capacity(partial_schema.fields().len());
            let mut decimal_idx = 0usize;
            for field in partial_schema.fields() {
                match field.data_type() {
                    DataType::Utf8View => {
                        columns.push(Arc::new(StringViewArray::from(
                            flags.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                        )));
                    }
                    DataType::Decimal128(p, s) => {
                        // First Decimal128 = SUM state, second = AVG sum state
                        let vals = if decimal_idx == 0 {
                            sums_cents
                        } else {
                            avg_sums_cents
                        };
                        columns.push(Arc::new(
                            Decimal128Array::from(vals.to_vec())
                                .with_precision_and_scale(*p, *s)
                                .expect("decimal"),
                        ));
                        decimal_idx += 1;
                    }
                    DataType::Int64 => {
                        columns.push(Arc::new(arrow::array::Int64Array::from(counts.to_vec())));
                    }
                    DataType::UInt64 => {
                        columns.push(Arc::new(UInt64Array::from(avg_counts.to_vec())));
                    }
                    other => {
                        return Err(datafusion::common::DataFusionError::Internal(format!(
                            "unexpected partial state type {}: {other}",
                            field.name()
                        )));
                    }
                }
            }
            RecordBatch::try_new(Arc::clone(&partial_schema), columns)
                .map_err(|e| datafusion::common::DataFusionError::ArrowError(Box::new(e), None))
        };

        let p1 = build_batch(
            &["A", "B"],
            &[3000, 3000], // SUM: 30.00, 30.00
            &[2, 1],       // COUNT(*)
            &[2, 1],       // AVG count
            &[3000, 3000], // AVG sum: 30.00, 30.00
        )?;
        let p2 = build_batch(
            &["A", "B"],
            &[4000, 5000], // SUM: 40.00, 50.00
            &[1, 1],       // COUNT(*)
            &[1, 1],       // AVG count
            &[4000, 5000], // AVG sum: 40.00, 50.00
        )?;

        let mut data = vec![vec![p1], vec![p2]].into_iter();
        let executable = replace_pushdown_with_memory(optimized, &mut data)?;
        let result = execute_plan(executable).await?;

        // Row order is non-deterministic with GROUP BY, so sort before snapshotting.
        insta::assert_snapshot!(pretty_sorted(&result), @r#"
        +--------------+-----------------+----------+-----------------+
        | l_returnflag | sum(l_quantity) | count(*) | avg(l_quantity) |
        +--------------+-----------------+----------+-----------------+
        | A            | 70.00           | 3        | 23.333333       |
        | B            | 80.00           | 2        | 40.000000       |
        +--------------+-----------------+----------+-----------------+
        "#);

        Ok(())
    }
}
