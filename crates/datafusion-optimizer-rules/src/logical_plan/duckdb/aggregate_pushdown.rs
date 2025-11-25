use crate::concrete;
use crate::logical_plan::duckdb::logical_pushdown_node::DuckDBLogicalPlanPushdownNode;
use crate::logical_plan::duckdb::{
    is_aggregate_plan_supported, is_duckdb_provider, is_plan_supported,
};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, ExprSchema};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use std::fmt::Debug;
use std::sync::Arc;

/// This looks for opportunities in the expressed logical plan to push down aggregates
/// directly into the SQL execution for `DuckDB` accelerated table providers (as indicated by `spice.accelerator`).
///
/// Schema metadata was chosen to "tag" scans in order to avoid a dependency on the runtime crate and
/// concrete adapter types. This also vastly simplifies testing.
#[derive(Debug)]
pub struct DuckDBAggregateLogicalPushdown {}

impl DuckDBAggregateLogicalPushdown {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    /// If this aggregate's root scan is from a `DuckDB` accelerated source, with supported expressions,
    /// wrap it in a marker node for pushdown rewriting during physical planning
    fn try_mark_pushdown(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        // Find a supported aggregate node
        let LogicalPlan::Aggregate(_) = plan else {
            return Ok(Transformed::no(plan));
        };

        if !is_plan_supported(&plan) {
            return Ok(Transformed::no(plan));
        }

        // Scan its children to ensure that there is a unary chain to an accelerated
        // DuckDB provider
        let mut found = false;

        let _ = plan.apply(|p| match p {
            LogicalPlan::TableScan(table_scan) if is_duckdb_provider(table_scan)? => {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            }
            other if other.inputs().len() > 1 => Ok(TreeNodeRecursion::Stop),
            _ => Ok(TreeNodeRecursion::Continue),
        })?;

        if found {
            Ok(Transformed::new(
                LogicalPlan::Extension(Extension {
                    node: DuckDBLogicalPlanPushdownNode::new(plan.clone()),
                }),
                true,
                TreeNodeRecursion::Jump,
            ))
        } else {
            Ok(Transformed::no(plan))
        }
    }

    /// Try to find a unary path to a marker node from the current node, then swap it
    fn try_percolate_marker_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let mut maybe_percolated = plan.transform_down(|p| {
            if p.inputs().len() > 1 || matches!(p, LogicalPlan::Analyze(_)) {
                return Ok(Transformed::new(p, false, TreeNodeRecursion::Stop));
            }

            let LogicalPlan::Extension(ref ext) = p else {
                return Ok(Transformed::no(p));
            };

            let Some(marker) = ext
                .node
                .as_any()
                .downcast_ref::<DuckDBLogicalPlanPushdownNode>()
            else {
                return Ok(Transformed::no(p));
            };

            Ok(Transformed::new(
                marker.input_plan.clone(),
                true,
                TreeNodeRecursion::Jump,
            ))
        })?;

        if maybe_percolated.transformed {
            maybe_percolated.tnr = TreeNodeRecursion::Jump;
            maybe_percolated.data =
                DuckDBLogicalPlanPushdownNode::from_input_plan(maybe_percolated.data);
        } else {
            maybe_percolated.tnr = TreeNodeRecursion::Continue;
        }

        Ok(maybe_percolated)
    }
}

impl OptimizerRule for DuckDBAggregateLogicalPushdown {
    fn name(&self) -> &'static str {
        "DuckDBAggregatePushdownOptimizerRule"
    }

    // This rule does its own recursion
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        println!("input schema {:?}", plan.schema().inner().metadata);

        // Mark all eligible nodes for DuckDB agg pushdown
        let maybe_marked_agg = plan.transform_down(|p| {
            if let LogicalPlan::Extension(ext) = &p
                && concrete!(ext.node, DuckDBLogicalPlanPushdownNode).is_some()
            {
                Ok(Transformed::new(p, false, TreeNodeRecursion::Jump))
            } else {
                Self::try_mark_pushdown(p)
            }
        })?;

        // If we didn't rewrite, bail out early
        if !maybe_marked_agg.transformed {
            return Ok(maybe_marked_agg);
        }

        // Try to push as much of the logical plan under the pushdown marker as possible. We
        // do this in two steps since the previous only operates on aggregate nodes (it is not
        // possible to walk up at the point in time of rewriting), and trying to account for all
        // invariants in one steps is difficult to follow
        maybe_marked_agg
            .data
            .transform_down(Self::try_percolate_marker_node)
    }
}

#[cfg(test)]
mod tests {
    use crate::concrete;
    use crate::logical_plan::duckdb::SPICE_ACCELERATOR_METADATA_KEY;
    use crate::logical_plan::duckdb::aggregate_pushdown::DuckDBAggregateLogicalPushdown;
    use crate::logical_plan::duckdb::logical_pushdown_node::DuckDBLogicalPlanPushdownNode;
    use datafusion::catalog::MemTable;
    use datafusion::common::Result;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::optimizer::OptimizerRule;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::{LogicalPlan, col, lit};
    use std::collections::HashMap;
    use std::sync::Arc;

    macro_rules! assert_marker {
        ($node:expr) => {
            let LogicalPlan::Extension(ext) = $node else {
                panic!("The marker node must be the child of an extension")
            };

            assert!(
                concrete!(ext.node, DuckDBLogicalPlanPushdownNode).is_some(),
                "Must cast to marker node type"
            );
        };
    }

    async fn make_fake_duck_table() -> Result<MemTable> {
        let ctx = SessionContext::new();
        let df = ctx
            .sql("select unnest(range(50)) as id")
            .await?
            .with_column("group_a", col("id") % lit(5))?
            .with_column("group_b", col("id") % lit(2))?;

        let mut metadata = HashMap::new();
        metadata.insert(
            SPICE_ACCELERATOR_METADATA_KEY.to_string(),
            "duckdb".to_string(),
        );

        let schema = df.schema().inner().as_ref().clone().with_metadata(metadata);
        let batches = df.collect().await?;

        MemTable::try_new(Arc::new(schema), vec![batches])
    }

    #[tokio::test]
    async fn test_mark_pushdown_simple() -> Result<()> {
        let ctx = SessionContext::new();
        let fake_duck_table = make_fake_duck_table().await?;
        ctx.register_table("sut", Arc::new(fake_duck_table))?;

        let optimizer = DuckDBAggregateLogicalPushdown::new();
        let plan = ctx
            .state()
            .create_logical_plan("select group_a, count(*) from sut group by group_a")
            .await?;

        let rewritten = optimizer.rewrite(plan, &ctx.state())?;
        assert!(
            rewritten.transformed,
            "This query must be fully pushed down"
        );
        assert_marker!(rewritten.data);

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_pushdown_union() -> Result<()> {
        let ctx = SessionContext::new();
        let fake_duck_table = make_fake_duck_table().await?;
        ctx.register_table("sut", Arc::new(fake_duck_table))?;

        let optimizer = DuckDBAggregateLogicalPushdown::new();
        let plan = ctx
            .state()
            .create_logical_plan(
                "
                select group_a, count(*) from sut group by group_a
                union
                select group_b, count(*) from sut group by group_b
            ",
            )
            .await?;

        let rewritten = optimizer.rewrite(plan, &ctx.state())?;
        assert!(rewritten.transformed, "This query must be rewritten");

        // Make sure each union has a marker node
        let traversal = rewritten.data.apply(|p| {
            if let LogicalPlan::Union(union) = p {
                for input in &union.inputs {
                    assert_marker!(input.as_ref());
                }

                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;

        assert!(matches!(traversal, TreeNodeRecursion::Stop));

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_pushdown_ineligible_join() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_table("sut_a", Arc::new(make_fake_duck_table().await?))?;
        ctx.register_table("sut_b", Arc::new(make_fake_duck_table().await?))?;

        let optimizer = DuckDBAggregateLogicalPushdown::new();

        // This query cannot be rewritten: the aggregate node input is against joined data, which
        // may not all be DuckDB, and we do not currently push down joins
        let plan = ctx
            .state()
            .create_logical_plan(
                "
                select sut_b.group_a, count(*) from
                sut_a join sut_b on sut_a.id = sut_b.id
                group by sut_b.group_a
            ",
            )
            .await?;

        let rewritten = optimizer.rewrite(plan, &ctx.state())?;
        println!("rewritten: {:#?}", rewritten);
        assert!(!rewritten.transformed, "This query must NOT be rewritten");

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_pushdown_with_projection_alias() -> Result<()> {
        let ctx = SessionContext::new();
        let fake_duck_table = make_fake_duck_table().await?;
        ctx.register_table("sut", Arc::new(fake_duck_table))?;

        let optimizer = DuckDBAggregateLogicalPushdown::new();

        // Test percolation: the alias should come from the pushed down DDL and not from a DF projection
        let plan = ctx
            .state()
            .create_logical_plan("select group_a, count(*) as cnt from sut group by group_a")
            .await?;

        let rewritten = optimizer.rewrite(plan, &ctx.state())?;
        assert!(
            rewritten.transformed,
            "This query must be rewritten with percolation"
        );

        assert_marker!(&rewritten.data);

        let LogicalPlan::Extension(ext) = rewritten.data else {
            panic!("Expected extension node");
        };

        let marker =
            concrete!(ext.node, DuckDBLogicalPlanPushdownNode).expect("Must be a marker node");

        let LogicalPlan::Projection(proj) = &marker.input_plan else {
            panic!(
                "Expected projection inside marker, got: {:?}",
                marker.input_plan
            );
        };

        assert!(
            proj.schema.field_names().contains(&"cnt".to_string()),
            "Projection should contain the 'cnt' alias"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_pushdown_unsupported_aggregate() -> Result<()> {
        let ctx = SessionContext::new();
        let fake_duck_table = make_fake_duck_table().await?;
        ctx.register_table("sut", Arc::new(fake_duck_table))?;

        let optimizer = DuckDBAggregateLogicalPushdown::new();

        // array_agg is not in SUPPORTED_AGG_FUNCTIONS, so this should not be rewritten
        let plan = ctx
            .state()
            .create_logical_plan("select array_agg(id) from sut")
            .await?;

        let rewritten = optimizer.rewrite(plan, &ctx.state())?;
        assert!(
            !rewritten.transformed,
            "Query with unsupported aggregate must NOT be rewritten"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_pushdown_with_where_clause() -> Result<()> {
        let ctx = SessionContext::new();
        let fake_duck_table = make_fake_duck_table().await?;
        ctx.register_table("sut", Arc::new(fake_duck_table))?;

        let optimizer = DuckDBAggregateLogicalPushdown::new();

        // Filters should be pushed below the marker node
        let plan = ctx
            .state()
            .create_logical_plan("select group_a, count(*) from sut where id > 10 group by group_a")
            .await?;

        let rewritten = optimizer.rewrite(plan, &ctx.state())?;
        assert!(
            rewritten.transformed,
            "Query with WHERE clause must be rewritten"
        );
        assert_marker!(&rewritten.data);

        let LogicalPlan::Extension(ext) = rewritten.data else {
            panic!("Expected extension node");
        };

        let marker =
            concrete!(ext.node, DuckDBLogicalPlanPushdownNode).expect("Must be a marker node");

        let mut found_filter = false;
        let _ = marker.input_plan.apply(|p| {
            if matches!(p, LogicalPlan::Filter(_)) {
                found_filter = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;

        assert!(found_filter, "Filter node must be inside the marker");

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_pushdown_multiple_aggregates() -> Result<()> {
        let ctx = SessionContext::new();
        let fake_duck_table = make_fake_duck_table().await?;
        ctx.register_table("sut", Arc::new(fake_duck_table))?;

        let optimizer = DuckDBAggregateLogicalPushdown::new();

        // Query with multiple aggregate functions
        let plan = ctx
            .state()
            .create_logical_plan(
                "select group_a, count(*), sum(id), avg(id), max(id), min(id) from sut group by group_a",
            )
            .await?;

        let rewritten = optimizer.rewrite(plan, &ctx.state())?;
        assert!(
            rewritten.transformed,
            "Query with multiple aggregates must be rewritten"
        );
        assert_marker!(rewritten.data);

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_pushdown_mixed_supported_unsupported_aggregates() -> Result<()> {
        let ctx = SessionContext::new();
        let fake_duck_table = make_fake_duck_table().await?;
        ctx.register_table("sut", Arc::new(fake_duck_table))?;

        let optimizer = DuckDBAggregateLogicalPushdown::new();

        // Query mixing supported (count) and unsupported (array_agg) aggregates
        // Should NOT be rewritten because array_agg is not supported
        let plan = ctx
            .state()
            .create_logical_plan(
                "select group_a, count(*), array_agg(id) from sut group by group_a",
            )
            .await?;

        let rewritten = optimizer.rewrite(plan, &ctx.state())?;
        assert!(
            !rewritten.transformed,
            "Query with any unsupported aggregate must NOT be rewritten"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_pushdown_complex_group_by() -> Result<()> {
        let ctx = SessionContext::new();
        let fake_duck_table = make_fake_duck_table().await?;
        ctx.register_table("sut", Arc::new(fake_duck_table))?;

        let optimizer = DuckDBAggregateLogicalPushdown::new();

        // Query with multiple GROUP BY columns
        let plan = ctx
            .state()
            .create_logical_plan(
                "select group_a, group_b, count(*) from sut group by group_a, group_b",
            )
            .await?;

        let rewritten = optimizer.rewrite(plan, &ctx.state())?;

        assert!(
            rewritten.transformed,
            "Query with complex GROUP BY must be rewritten"
        );
        assert_marker!(rewritten.data);

        Ok(())
    }

    #[tokio::test]
    async fn test_mark_pushdown_idempotency() -> Result<()> {
        let ctx = SessionContext::new();
        let fake_duck_table = make_fake_duck_table().await?;
        ctx.register_table("sut", Arc::new(fake_duck_table))?;

        let optimizer = DuckDBAggregateLogicalPushdown::new();

        let plan = ctx
            .state()
            .create_logical_plan("select group_a, count(*) from sut group by group_a")
            .await?;

        let rewritten_once = optimizer.rewrite(plan, &ctx.state())?;
        assert!(
            rewritten_once.transformed,
            "First rewrite must transform the plan"
        );

        let rewritten_twice = optimizer.rewrite(rewritten_once.data, &ctx.state())?;
        assert!(
            !rewritten_twice.transformed,
            "Second rewrite must not transform (already marked)"
        );

        Ok(())
    }
}
