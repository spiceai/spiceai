use super::*;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::common::stats::Precision;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::prelude::SessionContext;
use datafusion_common::Statistics;
use datafusion_expr::LogicalPlanBuilder;
use std::sync::Arc;

/// Wrapper around [`MemTable`] that exposes a fixed row count via
/// [`TableProvider::statistics`]. The cardinality gates in
/// [`skip_propagation_by_cardinality`] require stats to be present on the
/// dim side; without this wrapper, test tables backed by `MemTable` would
/// report `None` and propagation would be skipped.
#[derive(Debug)]
struct StatMemTable {
    inner: MemTable,
    num_rows: usize,
}

#[derive(Debug)]
struct NoStatsTable {
    inner: MemTable,
}

impl StatMemTable {
    fn try_new(
        schema: Arc<Schema>,
        batches: Vec<Vec<arrow::array::RecordBatch>>,
        num_rows: usize,
    ) -> Result<Self> {
        Ok(Self {
            inner: MemTable::try_new(schema, batches)?,
            num_rows,
        })
    }
}

impl NoStatsTable {
    fn try_new(schema: Arc<Schema>, batches: Vec<Vec<RecordBatch>>) -> Result<Self> {
        Ok(Self {
            inner: MemTable::try_new(schema, batches)?,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for StatMemTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(Statistics {
            num_rows: Precision::Exact(self.num_rows),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for NoStatsTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

fn rule() -> CayennePropagateFilterAcrossEquiJoinKeys {
    CayennePropagateFilterAcrossEquiJoinKeys::new_with_table_source_predicate(|_| true)
}

fn cross_join_rule() -> CayenneReassociateCrossJoin {
    CayenneReassociateCrossJoin::new_with_table_source_predicate(|_| true)
}

/// Build a [`LogicalPlan::TableScan`] backed by a [`StatMemTable`] that
/// reports `num_rows` via `TableProvider::statistics()`. Use this instead
/// of `datafusion_expr::builder::table_scan` in tests that need the
/// cardinality gates in [`skip_propagation_by_cardinality`] to pass.
fn stat_table_scan(name: &str, schema: &Arc<Schema>, num_rows: usize) -> Result<LogicalPlan> {
    let provider = Arc::new(StatMemTable::try_new(
        Arc::clone(schema),
        vec![vec![]],
        num_rows,
    )?);
    let source = Arc::new(DefaultTableSource::new(provider));
    LogicalPlanBuilder::scan(name, source, None)?.build()
}

fn make_ctx() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    // dim-like nation table — gains an `n_regionkey` so the multi-hop
    // `region ⋈ nation` propagation tests can join through it.
    let nation_schema = Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, true),
        Field::new("n_regionkey", DataType::Int64, false),
    ]));
    // dim-like region table for multi-hop tests.
    let region_schema = Arc::new(Schema::new(vec![
        Field::new("r_regionkey", DataType::Int64, false),
        Field::new("r_name", DataType::Utf8, true),
    ]));
    // fact-like supplier table
    let supplier_schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_nationkey", DataType::Int64, false),
    ]));
    // fact-like customer table for expression-equi-key no-op tests
    // (expression-derived nation mapping).
    let customer_schema = Arc::new(Schema::new(vec![
        Field::new("c_id", DataType::Int64, false),
        Field::new("c_state", DataType::Utf8, true),
    ]));
    // Dim tables use realistic small domains; fact tables are large enough
    // for the fact-to-dim key-domain ratio gate to allow pruning.
    ctx.register_table(
        "nation",
        Arc::new(StatMemTable::try_new(
            Arc::clone(&nation_schema),
            vec![vec![]],
            25,
        )?),
    )?;
    ctx.register_table(
        "region",
        Arc::new(StatMemTable::try_new(
            Arc::clone(&region_schema),
            vec![vec![]],
            5,
        )?),
    )?;
    ctx.register_table(
        "supplier",
        Arc::new(StatMemTable::try_new(
            Arc::clone(&supplier_schema),
            vec![vec![]],
            500_000,
        )?),
    )?;
    ctx.register_table(
        "customer",
        Arc::new(StatMemTable::try_new(
            Arc::clone(&customer_schema),
            vec![vec![]],
            500_000,
        )?),
    )?;
    Ok(ctx)
}

/// Walk a `LogicalPlan` to find the first `Join` and return whichever
/// side's plan tree contains a `SubqueryAlias` whose name starts with
/// [`PROPAGATED_FILTER_ALIAS_PREFIX`].
fn find_propagated_side(plan: &LogicalPlan) -> Option<&'static str> {
    let mut result: Option<&'static str> = None;
    let _ = plan.apply(|node| {
        if let LogicalPlan::Join(j) = node {
            if subtree_has_propagated_filter(j.left.as_ref()) {
                result = Some("left");
                return Ok(TreeNodeRecursion::Stop);
            }
            if subtree_has_propagated_filter(j.right.as_ref()) {
                result = Some("right");
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    result
}

fn count_propagated_filter_exprs(plan: &LogicalPlan) -> usize {
    let mut count = 0;
    let _ = plan.apply(|node| {
        if let LogicalPlan::Filter(f) = node {
            let _ = f.predicate.apply(|expr| {
                if let Expr::InSubquery(InSubquery { subquery, .. }) = expr
                    && let LogicalPlan::SubqueryAlias(alias) = subquery.subquery.as_ref()
                    && alias
                        .alias
                        .table()
                        .starts_with(PROPAGATED_FILTER_ALIAS_PREFIX)
                {
                    count += 1;
                }
                Ok(TreeNodeRecursion::Continue)
            });
        }
        Ok(TreeNodeRecursion::Continue)
    });
    count
}

#[test]
fn rule_metadata() {
    assert_eq!(
        rule().name(),
        "cayenne_propagate_filter_across_equi_join_keys"
    );
    assert_eq!(rule().apply_order(), Some(ApplyOrder::TopDown));
    assert_eq!(cross_join_rule().name(), "cayenne_reassociate_cross_join");
    assert_eq!(cross_join_rule().apply_order(), Some(ApplyOrder::BottomUp));
}

#[tokio::test]
async fn default_rule_skips_non_cayenne_table_scans() -> Result<()> {
    let ctx = make_ctx()?;
    let plan = ctx
        .sql(
            "SELECT s_suppkey FROM supplier, nation \
                 WHERE s_nationkey = n_nationkey AND n_name = 'CHINA'",
        )
        .await?
        .into_optimized_plan()?;

    let r = CayennePropagateFilterAcrossEquiJoinKeys::new();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
    assert!(
        !changed,
        "default rule must not rewrite non-Cayenne scans; plan was:\n{plan}"
    );
    Ok(())
}

#[tokio::test]
async fn non_inner_join_is_unchanged() -> Result<()> {
    // Use `IS NULL` on the right side so `eliminate_outer_join` doesn't
    // promote the LEFT JOIN to an INNER JOIN, otherwise we'd be testing
    // the wrong thing.
    let ctx = make_ctx()?;
    let plan = ctx
        .sql(
            "SELECT s_suppkey FROM supplier LEFT JOIN nation \
                 ON s_nationkey = n_nationkey WHERE n_name IS NULL",
        )
        .await?
        .into_optimized_plan()?;

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
    assert!(
        !changed,
        "LEFT JOIN must be skipped by the rule; plan was:\n{plan}"
    );
    Ok(())
}

/// Run the rule against every `LogicalPlan::Join` reachable from `plan`,
/// returning the transformed plan and a flag indicating whether at least
/// one invocation made a change.
///
/// Mirrors what `DataFusion`'s optimizer driver does for an
/// `ApplyOrder::TopDown` rule, but without spinning up the rest of the
/// rule pipeline — keeps the tests focused on this rule's behavior in
/// isolation.
fn apply_rule_to_all_joins(
    rule: &CayennePropagateFilterAcrossEquiJoinKeys,
    plan: LogicalPlan,
    cfg: &datafusion::optimizer::OptimizerContext,
) -> Result<(LogicalPlan, bool)> {
    let mut any_changed = false;
    let transformed = plan.transform_down(|node| {
        if matches!(node, LogicalPlan::Join(_)) {
            let r = rule.rewrite(node, cfg)?;
            if r.transformed {
                any_changed = true;
            }
            Ok(r)
        } else {
            Ok(Transformed::no(node))
        }
    })?;
    Ok((transformed.data, any_changed))
}

fn push_down_semi_join_rule() -> CayennePushDownSemiJoin {
    CayennePushDownSemiJoin::new_with_table_source_predicate(|_| true)
}

fn col(table: &str, column: &str) -> Expr {
    Expr::Column(Column::new(Some(table), column))
}

/// Schemas for a Q18-shaped `customer ⋈ orders ⋈ lineitem` semi-joined
/// against a qualifying-orderkey subquery (`sq`).
fn q18_schemas() -> (Arc<Schema>, Arc<Schema>, Arc<Schema>, Arc<Schema>) {
    (
        Arc::new(Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, true),
        ])),
        Arc::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
        ])),
        Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_quantity", DataType::Int64, false),
        ])),
        Arc::new(Schema::new(vec![Field::new(
            "sq_orderkey",
            DataType::Int64,
            false,
        )])),
    )
}

/// Build `LeftSemi( (customer ⋈ orders) ⋈ lineitem, sq )` on
/// `orders.o_orderkey = sq.sq_orderkey`. `cust_orders_join_type` lets a test
/// place an outer join on the path to `orders`; `orders_rows` drives the
/// size gate.
fn build_q18_semi_join(cust_orders_join_type: JoinType, orders_rows: usize) -> Result<LogicalPlan> {
    let (customer_s, orders_s, lineitem_s, sq_s) = q18_schemas();
    let customer = stat_table_scan("customer", &customer_s, 1_500_000)?;
    let orders = stat_table_scan("orders", &orders_s, orders_rows)?;
    let lineitem = stat_table_scan("lineitem", &lineitem_s, 60_000_000)?;
    let sq = stat_table_scan("sq", &sq_s, 5_000)?;

    let cust_orders = LogicalPlan::Join(Join::try_new(
        Arc::new(customer),
        Arc::new(orders),
        vec![(col("customer", "c_custkey"), col("orders", "o_custkey"))],
        None,
        cust_orders_join_type,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let three_way = LogicalPlan::Join(Join::try_new(
        Arc::new(cust_orders),
        Arc::new(lineitem),
        vec![(col("orders", "o_orderkey"), col("lineitem", "l_orderkey"))],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    Ok(LogicalPlan::Join(Join::try_new(
        Arc::new(three_way),
        Arc::new(sq),
        vec![(col("orders", "o_orderkey"), col("sq", "sq_orderkey"))],
        None,
        JoinType::LeftSemi,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?))
}

/// Return the table name under the left (kept) child of the first
/// `LeftSemi` join, if that child is a bare `TableScan`.
fn left_semi_landing_scan(plan: &LogicalPlan) -> Option<String> {
    let mut result = None;
    let _ = plan.apply(|node| {
        if let LogicalPlan::Join(join) = node
            && join.join_type == JoinType::LeftSemi
            && let LogicalPlan::TableScan(scan) = join.left.as_ref()
        {
            result = Some(scan.table_name.table().to_string());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    result
}

#[test]
fn semi_join_pushed_through_inner_joins_to_scan() -> Result<()> {
    let plan = build_q18_semi_join(JoinType::Inner, 15_000_000)?;
    let original_schema = Arc::clone(plan.schema());

    let transformed = push_down_semi_join_rule().rewrite(
        plan.clone(),
        &datafusion::optimizer::OptimizerContext::new(),
    )?;

    assert!(
        transformed.transformed,
        "Q18-shaped semi-join should push down to the orders scan; plan was:\n{plan}"
    );
    // Semi-join preserves the kept-side schema, so pushing it down must not
    // change the overall output schema.
    assert_eq!(transformed.data.schema(), &original_schema);
    // The outermost join is no longer the semi-join — it moved below.
    assert!(
        !matches!(&transformed.data, LogicalPlan::Join(j) if j.join_type == JoinType::LeftSemi),
        "top join should no longer be the semi-join; plan was:\n{}",
        transformed.data
    );
    // ...and it now sits directly over the orders scan.
    assert_eq!(
        left_semi_landing_scan(&transformed.data).as_deref(),
        Some("orders"),
        "semi-join should be planted on the orders scan; plan was:\n{}",
        transformed.data
    );
    Ok(())
}

#[test]
fn semi_join_already_on_bare_scan_is_unchanged() -> Result<()> {
    let (_, orders_s, _, sq_s) = q18_schemas();
    let orders = stat_table_scan("orders", &orders_s, 15_000_000)?;
    let sq = stat_table_scan("sq", &sq_s, 5_000)?;
    let plan = LogicalPlan::Join(Join::try_new(
        Arc::new(orders),
        Arc::new(sq),
        vec![(col("orders", "o_orderkey"), col("sq", "sq_orderkey"))],
        None,
        JoinType::LeftSemi,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    let transformed = push_down_semi_join_rule().rewrite(
        plan.clone(),
        &datafusion::optimizer::OptimizerContext::new(),
    )?;
    assert!(
        !transformed.transformed,
        "a semi-join already on a bare scan must be left unchanged (idempotence); plan was:\n{plan}"
    );
    Ok(())
}

#[test]
fn semi_join_not_pushed_through_outer_join() -> Result<()> {
    // A LEFT join sits between the semi-join and the orders scan: pushing the
    // semi-join past it could drop rows the outer join is meant to preserve,
    // so the rule must not descend.
    let plan = build_q18_semi_join(JoinType::Left, 15_000_000)?;
    let transformed = push_down_semi_join_rule().rewrite(
        plan.clone(),
        &datafusion::optimizer::OptimizerContext::new(),
    )?;
    assert!(
        !transformed.transformed,
        "semi-join must not be pushed through an outer join; plan was:\n{plan}"
    );
    Ok(())
}

#[test]
fn semi_join_not_pushed_when_no_cayenne_scan() -> Result<()> {
    // With a predicate that treats no source as Cayenne, there is no eligible
    // landing scan, so the rule leaves the plan untouched.
    let plan = build_q18_semi_join(JoinType::Inner, 15_000_000)?;
    let rule = CayennePushDownSemiJoin::new_with_table_source_predicate(|_| false);
    let transformed = rule.rewrite(
        plan.clone(),
        &datafusion::optimizer::OptimizerContext::new(),
    )?;
    assert!(
        !transformed.transformed,
        "no Cayenne landing scan => no pushdown; plan was:\n{plan}"
    );
    Ok(())
}

#[test]
fn semi_join_not_pushed_to_small_scan() -> Result<()> {
    // orders is below the size gate, so the pushdown can't recoup its cost.
    let plan = build_q18_semi_join(JoinType::Inner, 10_000)?;
    let transformed = push_down_semi_join_rule().rewrite(
        plan.clone(),
        &datafusion::optimizer::OptimizerContext::new(),
    )?;
    assert!(
        !transformed.transformed,
        "semi-join must not be pushed onto a small scan; plan was:\n{plan}"
    );
    Ok(())
}

#[test]
fn semi_join_with_expression_key_is_unchanged() -> Result<()> {
    // A non-column (key-transforming) semi-join key can't be traced to a
    // scan, so the rule bails.
    let (customer_s, orders_s, lineitem_s, sq_s) = q18_schemas();
    let customer = stat_table_scan("customer", &customer_s, 1_500_000)?;
    let orders = stat_table_scan("orders", &orders_s, 15_000_000)?;
    let lineitem = stat_table_scan("lineitem", &lineitem_s, 60_000_000)?;
    let sq = stat_table_scan("sq", &sq_s, 5_000)?;
    let cust_orders = LogicalPlan::Join(Join::try_new(
        Arc::new(customer),
        Arc::new(orders),
        vec![(col("customer", "c_custkey"), col("orders", "o_custkey"))],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let three_way = LogicalPlan::Join(Join::try_new(
        Arc::new(cust_orders),
        Arc::new(lineitem),
        vec![(col("orders", "o_orderkey"), col("lineitem", "l_orderkey"))],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let plan = LogicalPlan::Join(Join::try_new(
        Arc::new(three_way),
        Arc::new(sq),
        // expression key: o_orderkey + 1 (not a plain column)
        vec![(
            col("orders", "o_orderkey") + datafusion_expr::lit(1_i64),
            col("sq", "sq_orderkey"),
        )],
        None,
        JoinType::LeftSemi,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let transformed = push_down_semi_join_rule().rewrite(
        plan.clone(),
        &datafusion::optimizer::OptimizerContext::new(),
    )?;
    assert!(
        !transformed.transformed,
        "expression (non-column) semi-join key must not be pushed; plan was:\n{plan}"
    );
    Ok(())
}

#[test]
fn cross_join_reassociation_moves_b_c_join_under_cross() -> Result<()> {
    let supplier_schema = Arc::new(Schema::new(vec![Field::new(
        "su_suppkey",
        DataType::Int64,
        false,
    )]));
    let order_line_schema = Arc::new(Schema::new(vec![
        Field::new("ol_o_id", DataType::Int64, false),
        Field::new("ol_w_id", DataType::Int64, false),
        Field::new("ol_d_id", DataType::Int64, false),
        Field::new("ol_delivery_d", DataType::Int64, false),
    ]));
    let order_schema = Arc::new(Schema::new(vec![
        Field::new("o_id", DataType::Int64, false),
        Field::new("o_w_id", DataType::Int64, false),
        Field::new("o_d_id", DataType::Int64, false),
        Field::new("o_entry_d", DataType::Int64, false),
    ]));

    let supplier = stat_table_scan("supplier", &supplier_schema, 10_000)?;
    let order_line = stat_table_scan("l1", &order_line_schema, 300_000)?;
    let order = stat_table_scan("oorder", &order_schema, 30_000)?;

    let cross = LogicalPlan::Join(Join::try_new(
        Arc::new(supplier),
        Arc::new(order_line),
        vec![],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let plan = LogicalPlan::Join(Join::try_new(
        Arc::new(cross),
        Arc::new(order),
        vec![
            (
                Expr::Column(Column::new(Some("l1"), "ol_o_id")),
                Expr::Column(Column::new(Some("oorder"), "o_id")),
            ),
            (
                Expr::Column(Column::new(Some("l1"), "ol_w_id")),
                Expr::Column(Column::new(Some("oorder"), "o_w_id")),
            ),
            (
                Expr::Column(Column::new(Some("l1"), "ol_d_id")),
                Expr::Column(Column::new(Some("oorder"), "o_d_id")),
            ),
        ],
        Some(
            Expr::Column(Column::new(Some("oorder"), "o_entry_d"))
                .lt(Expr::Column(Column::new(Some("l1"), "ol_delivery_d"))),
        ),
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let original_schema = Arc::clone(plan.schema());

    let transformed = cross_join_rule().rewrite(
        plan.clone(),
        &datafusion::optimizer::OptimizerContext::new(),
    )?;
    assert!(
        transformed.transformed,
        "cross join with later selective predicates should be reassociated; plan was:\n{plan}"
    );
    assert_eq!(
        transformed.data.schema(),
        &original_schema,
        "reassociation must preserve output schema order"
    );

    let LogicalPlan::Join(outer) = &transformed.data else {
        panic!("expected outer join after reassociation")
    };
    assert!(
        outer.on.is_empty(),
        "supplier should remain cross-joined after the selective B/C join"
    );
    assert!(
        outer.filter.is_none(),
        "all parent predicates in this shape should move to the B/C join"
    );
    assert!(plan_is_table_scan(&outer.left, "supplier"));

    let LogicalPlan::Join(inner) = outer.right.as_ref() else {
        panic!("expected order_line/oorder inner join under the outer cross join")
    };
    assert_eq!(inner.on.len(), 3);
    assert!(inner.filter.is_some());
    assert!(plan_is_table_scan(&inner.left, "l1"));
    assert!(plan_is_table_scan(&inner.right, "oorder"));

    Ok(())
}

#[test]
fn cross_join_reassociation_keeps_a_c_predicates_on_outer_join() -> Result<()> {
    let supplier_schema = Arc::new(Schema::new(vec![Field::new(
        "su_suppkey",
        DataType::Int64,
        false,
    )]));
    let order_line_schema = Arc::new(Schema::new(vec![Field::new(
        "ol_i_id",
        DataType::Int64,
        false,
    )]));
    let stock_schema = Arc::new(Schema::new(vec![
        Field::new("s_i_id", DataType::Int64, false),
        Field::new("s_suppkey", DataType::Int64, false),
    ]));

    let supplier = stat_table_scan("supplier", &supplier_schema, 10_000)?;
    let order_line = stat_table_scan("l1", &order_line_schema, 300_000)?;
    let stock = stat_table_scan("stock", &stock_schema, 100_000)?;

    let cross = LogicalPlan::Join(Join::try_new(
        Arc::new(supplier),
        Arc::new(order_line),
        vec![],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let plan = LogicalPlan::Join(Join::try_new(
        Arc::new(cross),
        Arc::new(stock),
        vec![
            (
                Expr::Column(Column::new(Some("l1"), "ol_i_id")),
                Expr::Column(Column::new(Some("stock"), "s_i_id")),
            ),
            (
                Expr::Column(Column::new(Some("supplier"), "su_suppkey")),
                Expr::Column(Column::new(Some("stock"), "s_suppkey")),
            ),
        ],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    let transformed = cross_join_rule().rewrite(
        plan.clone(),
        &datafusion::optimizer::OptimizerContext::new(),
    )?;
    assert!(
        transformed.transformed,
        "B/C predicates should move inward while A/C predicates stay outside; plan was:\n{plan}"
    );

    let LogicalPlan::Join(outer) = &transformed.data else {
        panic!("expected outer join after reassociation")
    };
    assert_eq!(outer.on.len(), 1);
    assert!(expr_is_column_named(&outer.on[0].0, "su_suppkey"));

    let LogicalPlan::Join(inner) = outer.right.as_ref() else {
        panic!("expected l1/stock inner join under the outer supplier join")
    };
    assert_eq!(inner.on.len(), 1);
    assert!(expr_is_column_named(&inner.on[0].0, "ol_i_id"));

    Ok(())
}

#[test]
fn cross_join_reassociation_requires_b_c_equi_key() -> Result<()> {
    let supplier_schema = Arc::new(Schema::new(vec![Field::new(
        "su_suppkey",
        DataType::Int64,
        false,
    )]));
    let order_line_schema = Arc::new(Schema::new(vec![Field::new(
        "ol_i_id",
        DataType::Int64,
        false,
    )]));
    let stock_schema = Arc::new(Schema::new(vec![Field::new(
        "s_suppkey",
        DataType::Int64,
        false,
    )]));

    let supplier = stat_table_scan("supplier", &supplier_schema, 10_000)?;
    let order_line = stat_table_scan("l1", &order_line_schema, 300_000)?;
    let stock = stat_table_scan("stock", &stock_schema, 100_000)?;

    let cross = LogicalPlan::Join(Join::try_new(
        Arc::new(supplier),
        Arc::new(order_line),
        vec![],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let plan = LogicalPlan::Join(Join::try_new(
        Arc::new(cross),
        Arc::new(stock),
        vec![(
            Expr::Column(Column::new(Some("supplier"), "su_suppkey")),
            Expr::Column(Column::new(Some("stock"), "s_suppkey")),
        )],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    let transformed = cross_join_rule().rewrite(
        plan.clone(),
        &datafusion::optimizer::OptimizerContext::new(),
    )?;
    assert!(
        !transformed.transformed,
        "rule must not reassociate without a B/C equi-key to move inward; plan was:\n{plan}"
    );

    Ok(())
}

#[test]
fn cross_join_reassociation_skips_non_cayenne_subtrees() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let middle_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, false)]));
    let right_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, false)]));

    let left = stat_table_scan("a", &left_schema, 10_000)?;
    let middle = stat_table_scan("b", &middle_schema, 300_000)?;
    let right = stat_table_scan("c", &right_schema, 30_000)?;
    let cross = LogicalPlan::Join(Join::try_new(
        Arc::new(left),
        Arc::new(middle),
        vec![],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let plan = LogicalPlan::Join(Join::try_new(
        Arc::new(cross),
        Arc::new(right),
        vec![(
            Expr::Column(Column::new(Some("b"), "b")),
            Expr::Column(Column::new(Some("c"), "c")),
        )],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    let rule = CayenneReassociateCrossJoin::new_with_table_source_predicate(|_| false);
    let transformed = rule.rewrite(
        plan.clone(),
        &datafusion::optimizer::OptimizerContext::new(),
    )?;
    assert!(
        !transformed.transformed,
        "rule must stay scoped to Cayenne-backed matched subtrees; plan was:\n{plan}"
    );

    Ok(())
}

#[test]
fn cross_join_reassociation_skips_when_only_untouched_side_is_cayenne() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let middle_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, false)]));
    let right_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, false)]));

    let left = stat_table_scan("a", &left_schema, 10_000)?;
    let middle = stat_table_scan("b", &middle_schema, 300_000)?;
    let right = stat_table_scan("c", &right_schema, 30_000)?;
    let cross = LogicalPlan::Join(Join::try_new(
        Arc::new(left),
        Arc::new(middle),
        vec![],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);
    let plan = LogicalPlan::Join(Join::try_new(
        Arc::new(cross),
        Arc::new(right),
        vec![(
            Expr::Column(Column::new(Some("b"), "b")),
            Expr::Column(Column::new(Some("c"), "c")),
        )],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    let rule = CayenneReassociateCrossJoin::new_with_table_provider_predicate(|provider| {
        provider.schema().field_with_name("a").is_ok()
    });
    let transformed = rule.rewrite(
        plan.clone(),
        &datafusion::optimizer::OptimizerContext::new(),
    )?;
    assert!(
        !transformed.transformed,
        "rule must not reassociate a non-Cayenne B/C branch just because the untouched A side is Cayenne; plan was:\n{plan}"
    );

    Ok(())
}

fn plan_is_table_scan(plan: &LogicalPlan, table_name: &str) -> bool {
    matches!(plan, LogicalPlan::TableScan(scan) if scan.table_name.table() == table_name)
}

fn expr_is_column_named(expr: &Expr, column_name: &str) -> bool {
    matches!(expr, Expr::Column(column) if column.name == column_name)
}

#[tokio::test]
async fn inner_join_with_dim_filter_propagates_via_subquery() -> Result<()> {
    // Representative large fact/dimension join shape:
    //   FROM supplier, nation
    //   WHERE s_nationkey = n_nationkey AND n_name = 'CHINA'
    //
    // After PushDownFilter, `n_name = 'CHINA'` lives in a Filter directly
    // above the nation TableScan. The rule should then wrap supplier with
    // `Filter(s_nationkey IN (SELECT n_nationkey FROM nation
    //                          WHERE n_name = 'CHINA'))`.
    let ctx = make_ctx()?;
    let plan = ctx
        .sql(
            "SELECT s_suppkey FROM supplier, nation \
                 WHERE s_nationkey = n_nationkey AND n_name = 'CHINA'",
        )
        .await?
        .into_optimized_plan()?;

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (transformed_plan, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;

    // Depending on `DataFusion`'s planner the join's `left`/`right` may be
    // either order. We don't care which side gets the InSubquery, only
    // that exactly one of them does, and that it carries the marker.
    let propagated = find_propagated_side(&transformed_plan);
    assert!(
        changed,
        "rule should fire on inner join with dim-side non-key filter; plan was:\n{plan}"
    );
    assert!(
        propagated.is_some(),
        "rule fired but produced no propagated-filter marker; plan was:\n{transformed_plan}"
    );

    // Cycle prevention: running the rule a second time on the
    // already-transformed plan must be a no-op.
    let (second_plan, changed2) = apply_rule_to_all_joins(&r, transformed_plan.clone(), &cfg)?;
    assert!(
        !changed2,
        "second pass must not re-propagate (cycle guard); plan was:\n{second_plan}"
    );

    Ok(())
}

#[tokio::test]
async fn stats_less_provider_propagation_is_skipped() -> Result<()> {
    let ctx = SessionContext::new();
    let nation_schema = Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, true),
    ]));
    let supplier_schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_nationkey", DataType::Int64, false),
    ]));

    let nation_batch = RecordBatch::try_new(
        Arc::clone(&nation_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("CHINA"), Some("FRANCE"), None])) as ArrayRef,
        ],
    )?;
    let supplier_batch = RecordBatch::try_new(
        Arc::clone(&supplier_schema),
        vec![
            Arc::new(Int64Array::from(vec![10, 11, 12, 13])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2, 1, 4])) as ArrayRef,
        ],
    )?;

    ctx.register_table(
        "nation",
        Arc::new(NoStatsTable::try_new(
            nation_schema,
            vec![vec![nation_batch]],
        )?),
    )?;
    ctx.register_table(
        "supplier",
        Arc::new(NoStatsTable::try_new(
            supplier_schema,
            vec![vec![supplier_batch]],
        )?),
    )?;

    let plan = ctx
        .sql(
            "SELECT s_suppkey FROM supplier JOIN nation \
                 ON s_nationkey = n_nationkey \
                 WHERE n_name = 'CHINA' ORDER BY s_suppkey",
        )
        .await?
        .into_optimized_plan()?;

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
    assert!(
        !changed,
        "rule should not propagate without cardinality evidence; plan was:\n{plan}"
    );

    Ok(())
}

#[tokio::test]
async fn left_semi_join_with_dim_filter_propagates_via_subquery() -> Result<()> {
    // The `IN (subquery)` shape that `decorrelate_predicate_subquery`
    // rewrites into a `LeftSemi` join. The propagation rule must still
    // fire on the resulting semi-join so the dim filter reaches the fact
    // side across chained joins.
    let ctx = make_ctx()?;
    let plan = ctx
        .sql(
            "SELECT s_suppkey FROM supplier \
                 WHERE s_nationkey IN \
                   (SELECT n_nationkey FROM nation WHERE n_name = 'CHINA')",
        )
        .await?
        .into_optimized_plan()?;

    // Sanity-check that decorrelation produced a semi-join shape; if it
    // didn't, this test is testing the wrong thing.
    let mut semi_seen = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::Join(j) = node
            && matches!(j.join_type, JoinType::LeftSemi | JoinType::RightSemi)
        {
            semi_seen = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    assert!(
        semi_seen,
        "expected decorrelation to produce a semi-join; plan was:\n{plan}"
    );

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (transformed_plan, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
    assert!(
        changed,
        "rule should fire on semi-join with dim-side non-key filter; plan was:\n{plan}"
    );
    assert!(
        find_propagated_side(&transformed_plan).is_some(),
        "rule fired but produced no propagated-filter marker; plan was:\n{transformed_plan}"
    );
    Ok(())
}

#[tokio::test]
async fn left_outer_join_is_unchanged_even_when_preserved_side_has_filter() -> Result<()> {
    // `supplier LEFT JOIN nation ON s_nationkey = n_nationkey WHERE
    // s_name = 'X'`. The LEFT side (supplier) has a non-key filter; it is
    // the preserved side. This could be semantically safe to propagate to
    // the lookup side, but it adds an extra semi-join shape and was too
    // easy to over-apply in HTAP workloads.
    //
    // Note: `eliminate_outer_join` will rewrite the LEFT JOIN to an INNER
    // JOIN only if the WHERE clause forces the right side to be non-null
    // — using a filter on the LEFT side instead preserves the outer
    // semantics, which is what we want for this test.
    let ctx = make_ctx()?;
    let plan = ctx
        .sql(
            "SELECT s_suppkey FROM supplier LEFT JOIN nation \
                 ON s_nationkey = n_nationkey WHERE s_suppkey > 5",
        )
        .await?
        .into_optimized_plan()?;

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
    assert!(
        !changed,
        "LEFT OUTER joins must stay unchanged by the rule; plan was:\n{plan}"
    );
    Ok(())
}

#[tokio::test]
async fn left_outer_join_blocks_right_to_left_propagation() -> Result<()> {
    // Filter on the RIGHT (lookup) side of a LEFT OUTER must NOT cause
    // propagation onto the LEFT (preserved) side: doing so would drop
    // left rows the outer join should emit as `(left, NULL...)`.
    let ctx = make_ctx()?;
    let plan = ctx
        .sql(
            "SELECT s_suppkey FROM supplier LEFT JOIN nation \
                 ON s_nationkey = n_nationkey \
                 WHERE n_name = 'CHINA' OR n_name IS NULL",
        )
        .await?
        .into_optimized_plan()?;

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
    assert!(
        !changed,
        "RIGHT→LEFT propagation must not fire on LEFT OUTER; plan was:\n{plan}"
    );
    Ok(())
}

#[tokio::test]
async fn rule_does_not_re_fire_on_post_decorrelation_left_semi() -> Result<()> {
    // Regression test for the cycle-detection bug across optimizer
    // iterations: after Pass 1 wraps the receiving side with an
    // `InSubquery`, `decorrelate_predicate_subquery` rewrites that into a
    // `LeftSemi` join with the marker `SubqueryAlias` as its right child.
    // If the rule's cycle detection only sees `InSubquery` markers (and
    // not the structural `LeftSemi`-with-marker shape), Pass 2 sees no
    // marker on the receiving side and re-propagates, producing nested
    // LeftSemi joins on every subsequent optimizer pass.
    //
    // The fix detects the post-decorrelation shape and records the
    // already-propagated target so the rule's cycle guard short-circuits
    // on subsequent passes.
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinConstraint;
    use datafusion_expr::{LogicalPlanBuilder, builder::table_scan, lit};

    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, true),
    ]));
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_nationkey", DataType::Int64, false),
    ]));

    // Build the dim subquery: `Filter(n_name='CHINA') → TableScan(nation)`
    // wrapped in the propagated-filter alias the rule would have produced.
    let nation_scan = table_scan(Some("nation"), &dim_schema, None)?.build()?;
    let nation_filter = LogicalPlan::Filter(Filter::try_new(
        Expr::Column(Column::new(Some("nation"), "n_name")).eq(lit("CHINA")),
        Arc::new(nation_scan),
    )?);
    let nation_projection = LogicalPlan::Projection(Projection::try_new(
        vec![Expr::Column(Column::new(Some("nation"), "n_nationkey"))],
        Arc::new(nation_filter),
    )?);
    let dim_subquery_alias = format!("{PROPAGATED_FILTER_ALIAS_PREFIX}1");
    let dim_subquery = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(nation_projection),
        TableReference::bare(dim_subquery_alias),
    )?);

    // Build supplier scan (the receiving fact side).
    let supplier_scan = table_scan(Some("supplier"), &fact_schema, None)?.build()?;

    // Compose the post-decorrelation shape: `LeftSemi(supplier, dim_subquery)`
    // on `s_nationkey = n_nationkey`.
    let semi_join_input = LogicalPlanBuilder::from(supplier_scan)
        .join_with_expr_keys(
            dim_subquery,
            JoinType::LeftSemi,
            (
                vec![Expr::Column(Column::new(Some("supplier"), "s_nationkey"))],
                vec![Expr::Column(Column::new(
                    Some(format!("{PROPAGATED_FILTER_ALIAS_PREFIX}1")),
                    "n_nationkey",
                ))],
            ),
            None,
        )?
        .build()?;

    // Now build an outer `Inner Join` between the *original* nation_filtered
    // and this `LeftSemi` subtree on the same equi-key — the exact shape an
    // optimizer pass would see after the rule already fired + decorrelated.
    let dim_filter_again_scan = table_scan(Some("nation_outer"), &dim_schema, None)?.build()?;
    let dim_filter_again = LogicalPlan::Filter(Filter::try_new(
        Expr::Column(Column::new(Some("nation_outer"), "n_name")).eq(lit("CHINA")),
        Arc::new(dim_filter_again_scan),
    )?);

    let outer_join = LogicalPlan::Join(Join::try_new(
        Arc::new(dim_filter_again),
        Arc::new(semi_join_input),
        vec![(
            Expr::Column(Column::new(Some("nation_outer"), "n_nationkey")),
            Expr::Column(Column::new(Some("supplier"), "s_nationkey")),
        )],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, outer_join.clone(), &cfg)?;
    assert!(
        !changed,
        "rule must not re-fire when the receiving side already contains a \
             post-decorrelation LeftSemi propagation marker; plan was:\n{outer_join}"
    );
    Ok(())
}

#[tokio::test]
async fn rule_re_fires_when_receiving_side_has_non_marker_subquery_alias() -> Result<()> {
    // Devil's-advocate edge case: a `LeftSemi` whose right side is a
    // `SubqueryAlias` with a *non-marker* name should NOT block
    // propagation (the marker prefix is the unique signal that this rule
    // already fired). Guards against the cycle guard being too aggressive.
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinConstraint;
    use datafusion_expr::{LogicalPlanBuilder, lit};

    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, true),
    ]));
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_nationkey", DataType::Int64, false),
    ]));

    let nation_scan = stat_table_scan("nation", &dim_schema, 5_000)?;
    let nation_filter = LogicalPlan::Filter(Filter::try_new(
        Expr::Column(Column::new(Some("nation"), "n_name")).eq(lit("CHINA")),
        Arc::new(nation_scan),
    )?);
    let nation_projection = LogicalPlan::Projection(Projection::try_new(
        vec![Expr::Column(Column::new(Some("nation"), "n_nationkey"))],
        Arc::new(nation_filter),
    )?);
    let user_alias = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(nation_projection),
        TableReference::bare("some_user_alias"),
    )?);

    let supplier_scan = stat_table_scan("supplier", &fact_schema, 500_000)?;
    let semi_join_input = LogicalPlanBuilder::from(supplier_scan)
        .join_with_expr_keys(
            user_alias,
            JoinType::LeftSemi,
            (
                vec![Expr::Column(Column::new(Some("supplier"), "s_nationkey"))],
                vec![Expr::Column(Column::new(
                    Some("some_user_alias".to_string()),
                    "n_nationkey",
                ))],
            ),
            None,
        )?
        .build()?;

    let outer_dim_scan = stat_table_scan("nation_outer", &dim_schema, 5_000)?;
    let outer_dim_filter = LogicalPlan::Filter(Filter::try_new(
        Expr::Column(Column::new(Some("nation_outer"), "n_name")).eq(lit("CHINA")),
        Arc::new(outer_dim_scan),
    )?);

    let outer_join = LogicalPlan::Join(Join::try_new(
        Arc::new(outer_dim_filter),
        Arc::new(semi_join_input),
        vec![(
            Expr::Column(Column::new(Some("nation_outer"), "n_nationkey")),
            Expr::Column(Column::new(Some("supplier"), "s_nationkey")),
        )],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, outer_join.clone(), &cfg)?;
    assert!(
        changed,
        "rule should still fire when the receiving LeftSemi's alias is \
             user-supplied (not the propagation marker); plan was:\n{outer_join}"
    );
    Ok(())
}

#[tokio::test]
async fn rule_detects_marker_through_projection_wrapper() -> Result<()> {
    // Subsequent optimizer rules (`MergeProjection`, etc.) may wrap the
    // marker `SubqueryAlias` in a `Projection`. The cycle guard must still
    // detect the marker through this wrapping.
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinConstraint;
    use datafusion_expr::{LogicalPlanBuilder, builder::table_scan, lit};

    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("n_nationkey", DataType::Int64, false),
        Field::new("n_name", DataType::Utf8, true),
    ]));
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("s_suppkey", DataType::Int64, false),
        Field::new("s_nationkey", DataType::Int64, false),
    ]));

    let nation_scan = table_scan(Some("nation"), &dim_schema, None)?.build()?;
    let nation_filter = LogicalPlan::Filter(Filter::try_new(
        Expr::Column(Column::new(Some("nation"), "n_name")).eq(lit("CHINA")),
        Arc::new(nation_scan),
    )?);
    let inner_projection = LogicalPlan::Projection(Projection::try_new(
        vec![Expr::Column(Column::new(Some("nation"), "n_nationkey"))],
        Arc::new(nation_filter),
    )?);
    let marker_alias = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(inner_projection),
        TableReference::bare(format!("{PROPAGATED_FILTER_ALIAS_PREFIX}1")),
    )?);
    let wrapped_marker = LogicalPlan::Projection(Projection::try_new(
        vec![Expr::Column(Column::new(
            Some(format!("{PROPAGATED_FILTER_ALIAS_PREFIX}1")),
            "n_nationkey",
        ))],
        Arc::new(marker_alias),
    )?);

    let supplier_scan = table_scan(Some("supplier"), &fact_schema, None)?.build()?;
    let semi_join_input = LogicalPlanBuilder::from(supplier_scan)
        .join_with_expr_keys(
            wrapped_marker,
            JoinType::LeftSemi,
            (
                vec![Expr::Column(Column::new(Some("supplier"), "s_nationkey"))],
                vec![Expr::Column(Column::new(
                    Some(format!("{PROPAGATED_FILTER_ALIAS_PREFIX}1")),
                    "n_nationkey",
                ))],
            ),
            None,
        )?
        .build()?;

    let outer_dim_scan = table_scan(Some("nation_outer"), &dim_schema, None)?.build()?;
    let outer_dim_filter = LogicalPlan::Filter(Filter::try_new(
        Expr::Column(Column::new(Some("nation_outer"), "n_name")).eq(lit("CHINA")),
        Arc::new(outer_dim_scan),
    )?);

    let outer_join = LogicalPlan::Join(Join::try_new(
        Arc::new(outer_dim_filter),
        Arc::new(semi_join_input),
        vec![(
            Expr::Column(Column::new(Some("nation_outer"), "n_nationkey")),
            Expr::Column(Column::new(Some("supplier"), "s_nationkey")),
        )],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, outer_join.clone(), &cfg)?;
    assert!(
        !changed,
        "cycle guard must detect a marker wrapped in an outer Projection; \
             plan was:\n{outer_join}"
    );
    Ok(())
}

#[tokio::test]
async fn inner_join_without_filter_is_noop() -> Result<()> {
    let ctx = make_ctx()?;
    let plan = ctx
        .sql(
            "SELECT s_suppkey FROM supplier, nation \
                 WHERE s_nationkey = n_nationkey",
        )
        .await?
        .into_optimized_plan()?;

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
    assert!(
        !changed,
        "rule must not fire when neither side has a non-key filter; plan was:\n{plan}"
    );
    Ok(())
}

#[tokio::test]
async fn inner_join_with_expression_fact_key_is_unchanged() -> Result<()> {
    // Common expression-key join shape: a non-trivial expression on
    // the fact side and a pure column on the dim side, with the dim side
    // carrying the selective non-key filter.
    //
    // These expression-key joins were valid to rewrite but too easy to
    // over-apply, so the selective key-domain rule leaves them alone.
    let ctx = make_ctx()?;
    let plan = ctx
        .sql(
            "SELECT c_id FROM customer, nation \
                 WHERE ascii(substr(c_state, 1, 1)) - 65 = n_nationkey \
                   AND n_name = 'CHINA'",
        )
        .await?
        .into_optimized_plan()?;

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
    assert!(
        !changed,
        "rule must not fire on expression-vs-column equi-key; plan was:\n{plan}"
    );
    Ok(())
}

#[tokio::test]
async fn multi_hop_dim_subtree_propagates_through_region_nation() -> Result<()> {
    // The canonical Q5 shape: `region ⋈ nation ⋈ supplier` with a
    // selective filter on `region.r_name`. With the multi-hop dim
    // detector the `region ⋈ nation` subtree counts as dim-like, so the
    // rule can propagate the filtered `n_nationkey` domain to `supplier`
    // in a single pass instead of waiting for the optimizer's fixed
    // point.
    let ctx = make_ctx()?;
    let plan = ctx
        .sql(
            "SELECT s_suppkey FROM supplier, nation, region \
                 WHERE s_nationkey = n_nationkey \
                   AND n_regionkey = r_regionkey \
                   AND r_name = 'ASIA'",
        )
        .await?
        .into_optimized_plan()?;

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (transformed_plan, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
    assert!(
        changed,
        "rule should propagate r_name filter through the multi-hop dim subtree; \
             plan was:\n{plan}"
    );
    assert!(
        find_propagated_side(&transformed_plan).is_some(),
        "rule fired but produced no propagated-filter marker; plan was:\n{transformed_plan}"
    );
    Ok(())
}

#[test]
fn key_preserved_through_summaries_accepts_distinct_all() -> Result<()> {
    // `Distinct::All` deduplicates whole rows but preserves every column's
    // values (it can only remove duplicate rows), so any join key survives.
    use datafusion::logical_expr::Distinct;
    use datafusion_expr::builder::table_scan;

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let scan = table_scan(Some("t"), &schema, None)?.build()?;
    let distinct = LogicalPlan::Distinct(Distinct::All(Arc::new(scan)));

    let key_a = Column::new(Some("t"), "a");
    let key_b = Column::new(Some("t"), "b");

    assert!(key_preserved_through_summaries(&distinct, &key_a));
    assert!(key_preserved_through_summaries(&distinct, &key_b));
    Ok(())
}

#[tokio::test]
async fn aggregate_dim_propagates_when_key_is_in_group_by() -> Result<()> {
    // Pre-aggregated dim: `SELECT n_nationkey, count(*) FROM nation
    // WHERE n_name = 'CHINA' GROUP BY n_nationkey` joined against
    // supplier. The aggregate's GROUP BY includes `n_nationkey`, so the
    // key's domain is preserved through the aggregation and the rule
    // should still propagate to supplier.
    let ctx = make_ctx()?;
    let plan = ctx
        .sql(
            "SELECT s_suppkey FROM supplier, \
                 (SELECT n_nationkey FROM nation WHERE n_name = 'CHINA' \
                  GROUP BY n_nationkey) AS n_agg \
                 WHERE s_nationkey = n_nationkey",
        )
        .await?
        .into_optimized_plan()?;

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (transformed_plan, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
    assert!(
        changed,
        "rule should fire when dim has Aggregate(GROUP BY key); plan was:\n{plan}"
    );
    assert!(
        find_propagated_side(&transformed_plan).is_some(),
        "rule fired but produced no propagated-filter marker; plan was:\n{transformed_plan}"
    );
    Ok(())
}

#[tokio::test]
async fn aggregate_propagates_using_group_key_domain() -> Result<()> {
    // A large outer fact scan can join to an aggregate over a filtered
    // dimension/fact subtree. The aggregate subtree contains a large fact
    // scan, but the propagated `i_id` domain is bounded by `item`, so the
    // ratio gate should still allow the aggregate-domain pruning path.
    let ctx = SessionContext::new();
    let item_schema = Arc::new(Schema::new(vec![
        Field::new("i_id", DataType::Int64, false),
        Field::new("i_data", DataType::Utf8, true),
    ]));
    let order_line_schema = Arc::new(Schema::new(vec![
        Field::new("ol_i_id", DataType::Int64, false),
        Field::new("ol_quantity", DataType::Int64, false),
    ]));
    ctx.register_table(
        "item",
        Arc::new(StatMemTable::try_new(
            Arc::clone(&item_schema),
            vec![vec![]],
            100_000,
        )?),
    )?;
    ctx.register_table(
        "order_line",
        Arc::new(StatMemTable::try_new(
            Arc::clone(&order_line_schema),
            vec![vec![]],
            5_000_000,
        )?),
    )?;

    let plan = ctx
        .sql(
            "SELECT sum(ol_outer.ol_quantity) FROM order_line ol_outer, \
                 (SELECT i_id, avg(ol_inner.ol_quantity) AS a \
                  FROM item, order_line ol_inner \
                  WHERE i_data LIKE '%b' AND ol_inner.ol_i_id = i_id \
                  GROUP BY i_id) t \
                 WHERE ol_outer.ol_i_id = t.i_id AND ol_outer.ol_quantity < t.a",
        )
        .await?
        .into_optimized_plan()?;

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (transformed_plan, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;

    assert!(
        changed,
        "rule should keep q17-shaped aggregate propagation; plan was:\n{plan}"
    );
    assert!(
        find_propagated_side(&transformed_plan).is_some(),
        "rule fired but produced no propagated-filter marker; plan was:\n{transformed_plan}"
    );
    Ok(())
}

#[test]
fn key_preserved_through_summaries_rejects_aggregate_without_key_in_group() -> Result<()> {
    // Sanity-check the helper: an aggregate that does NOT group by `a`
    // must report the key as not preserved.
    use datafusion::logical_expr::Aggregate;
    use datafusion_expr::builder::table_scan;

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let scan = table_scan(Some("t"), &schema, None)?.build()?;
    let agg = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(scan),
        vec![Expr::Column(Column::new(Some("t"), "b"))],
        vec![],
    )?);

    let key_a = Column::new(Some("t"), "a");
    let key_b = Column::new(Some("t"), "b");

    assert!(
        !key_preserved_through_summaries(&agg, &key_a),
        "`a` aggregated away, must not be preserved"
    );
    assert!(
        key_preserved_through_summaries(&agg, &key_b),
        "`b` is in GROUP BY, must be preserved"
    );
    Ok(())
}

#[test]
fn cardinality_gate_uses_key_domain_and_fact_ratio() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let key = Column::new(Some("dim"), "k");

    // Single scan: row count is reported directly.
    let small = stat_table_scan("dim", &schema, 500)?;
    assert_eq!(subtree_upper_bound_rows(&small), Some(500));
    assert_eq!(key_domain_upper_bound_rows(&small, &key), Some(500));

    // Large fact-to-dim ratio → gate is silent.
    let fact = stat_table_scan("fact", &schema, 1_000_000)?;
    assert!(!skip_propagation_by_cardinality(&small, &fact, &key));

    // Comparable sides → gate fires to avoid adding a semi-join that is
    // unlikely to pay for itself.
    let big_dim = stat_table_scan("dim", &schema, 50_000)?;
    let comparable_fact = stat_table_scan("fact", &schema, 200_000)?;
    assert!(skip_propagation_by_cardinality(
        &big_dim,
        &comparable_fact,
        &key
    ));

    // Below the fact threshold → gate fires from the fact side.
    let tiny_fact = stat_table_scan("fact", &schema, 50_000)?;
    assert!(skip_propagation_by_cardinality(&big_dim, &tiny_fact, &key));

    Ok(())
}

#[test]
fn skip_propagation_by_cardinality_blocks_when_stats_absent() -> Result<()> {
    // MemTable doesn't expose row counts via `TableProvider::statistics()`,
    // so there is no clear evidence that the extra subquery will pay off.
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let provider = Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]])?);
    let source = Arc::new(DefaultTableSource::new(provider));
    let scan = LogicalPlanBuilder::scan("t", source, None)?.build()?;
    let key = Column::new(Some("t"), "k");

    assert_eq!(subtree_upper_bound_rows(&scan), None);
    assert!(
        skip_propagation_by_cardinality(&scan, &scan, &key),
        "absent stats must trigger the cardinality gate"
    );
    Ok(())
}

#[test]
fn key_preserved_through_summaries_rejects_same_name_different_relation() -> Result<()> {
    use datafusion::logical_expr::{Aggregate, Distinct, DistinctOn};
    use datafusion_expr::builder::table_scan;

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let scan = table_scan(Some("t2"), &schema, None)?.build()?;
    let t1_key = Column::new(Some("t1"), "a");
    let t2_key = Column::new(Some("t2"), "a");

    let aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(scan.clone()),
        vec![Expr::Column(t2_key.clone())],
        vec![],
    )?);
    assert!(
        !key_preserved_through_summaries(&aggregate, &t1_key),
        "same-name GROUP BY columns from a different relation must not preserve the key"
    );

    let distinct_on = LogicalPlan::Distinct(Distinct::On(DistinctOn::try_new(
        vec![Expr::Column(t2_key.clone())],
        vec![Expr::Column(t2_key)],
        None,
        Arc::new(scan),
    )?));
    assert!(
        !key_preserved_through_summaries(&distinct_on, &t1_key),
        "same-name DISTINCT ON columns from a different relation must not preserve the key"
    );

    Ok(())
}

#[tokio::test]
async fn inner_join_with_key_only_filter_is_noop() -> Result<()> {
    // `n_nationkey = 22` references only the join key — `DataFusion`'s
    // stock `infer_join_predicates` already handles this case, so our
    // rule must NOT fire and create a redundant subquery.
    let ctx = make_ctx()?;
    let plan = ctx
        .sql(
            "SELECT s_suppkey FROM supplier, nation \
                 WHERE s_nationkey = n_nationkey AND n_nationkey = 22",
        )
        .await?
        .into_optimized_plan()?;

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
    assert!(
        !changed,
        "rule must not fire when filter references only the join key; plan was:\n{plan}"
    );
    Ok(())
}

#[tokio::test]
async fn inner_join_with_non_selective_non_key_filter_is_noop() -> Result<()> {
    let ctx = make_ctx()?;
    let plan = ctx
        .sql(
            "SELECT s_suppkey FROM supplier, nation \
                 WHERE s_nationkey = n_nationkey AND n_name IS NOT NULL",
        )
        .await?
        .into_optimized_plan()?;

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, plan.clone(), &cfg)?;
    assert!(
        !changed,
        "rule must not fire for broad non-key predicates like IS NOT NULL; plan was:\n{plan}"
    );
    Ok(())
}

#[test]
fn null_equal_inner_join_is_noop() -> Result<()> {
    use datafusion::logical_expr::JoinConstraint;
    use datafusion_expr::{builder::table_scan, lit};

    let left_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("c", DataType::Utf8, true),
    ]));
    let right_schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));

    let left_scan = table_scan(Some("l"), &left_schema, None)?.build()?;
    let left = LogicalPlan::Filter(Filter::try_new(
        Expr::Column(Column::new(Some("l"), "c")).eq(lit("v")),
        Arc::new(left_scan),
    )?);
    let right = table_scan(Some("r"), &right_schema, None)?.build()?;

    let join = LogicalPlan::Join(Join::try_new(
        Arc::new(left),
        Arc::new(right),
        vec![(
            Expr::Column(Column::new(Some("l"), "a")),
            Expr::Column(Column::new(Some("r"), "x")),
        )],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNull,
        false,
    )?);

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (_, changed) = apply_rule_to_all_joins(&r, join, &cfg)?;

    assert!(
        !changed,
        "rule must not introduce SQL IN filters for null-equal joins"
    );
    Ok(())
}

#[test]
fn composite_join_receives_one_filter_per_non_key_constrained_key() -> Result<()> {
    use datafusion::common::NullEquality;
    use datafusion::logical_expr::JoinConstraint;
    use datafusion_expr::lit;

    let left_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Utf8, true),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("y", DataType::Int64, false),
    ]));

    let left_scan = stat_table_scan("l", &left_schema, 5_000)?;
    let left = LogicalPlan::Filter(Filter::try_new(
        Expr::Column(Column::new(Some("l"), "c")).eq(lit("v")),
        Arc::new(left_scan),
    )?);
    let right = stat_table_scan("r", &right_schema, 500_000)?;

    let join = LogicalPlan::Join(Join::try_new(
        Arc::new(left),
        Arc::new(right),
        vec![
            (
                Expr::Column(Column::new(Some("l"), "a")),
                Expr::Column(Column::new(Some("r"), "x")),
            ),
            (
                Expr::Column(Column::new(Some("l"), "b")),
                Expr::Column(Column::new(Some("r"), "y")),
            ),
        ],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?);

    let r = rule();
    let cfg = datafusion::optimizer::OptimizerContext::new();
    let (transformed_plan, changed) = apply_rule_to_all_joins(&r, join, &cfg)?;

    assert!(
        changed,
        "rule should fire on composite inner join with side-local non-key filter"
    );
    assert_eq!(
        count_propagated_filter_exprs(&transformed_plan),
        2,
        "each matching composite key should get one propagated filter; plan was:\n{transformed_plan}"
    );
    Ok(())
}

#[test]
fn expr_has_propagated_filter_detects_marker_alias() -> Result<()> {
    use datafusion_expr::{LogicalPlanBuilder, builder::table_scan, lit};

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let scan = table_scan(Some("t"), &schema, None)?.build()?;
    let projection = LogicalPlanBuilder::from(scan)
        .project(vec![Expr::Column(Column::new(Some("t"), "a"))])?
        .build()?;

    let alias_name = format!("{PROPAGATED_FILTER_ALIAS_PREFIX}1");
    let aliased = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
        Arc::new(projection),
        TableReference::bare(alias_name),
    )?);

    let in_subquery = Expr::InSubquery(InSubquery::new(
        Box::new(lit(1i64)),
        Subquery {
            subquery: Arc::new(aliased),
            outer_ref_columns: vec![],
            spans: Spans::default(),
        },
        false,
    ));

    assert!(expr_has_propagated_filter(&in_subquery));
    assert!(!expr_has_propagated_filter(&lit(5i64)));
    Ok(())
}

#[test]
fn is_dim_like_subtree_handles_simple_scan() -> Result<()> {
    use datafusion_expr::{LogicalPlanBuilder, builder::table_scan, lit};

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("x", DataType::Utf8, true),
    ]));
    let scan = table_scan(Some("t"), &schema, None)?.build()?;
    assert!(is_dim_like_subtree(&scan));

    let filtered = LogicalPlanBuilder::from(scan)
        .filter(Expr::Column(Column::new(Some("t"), "x")).eq(lit("v")))?
        .build()?;
    assert!(is_dim_like_subtree(&filtered));
    Ok(())
}

#[test]
fn inlist_to_range_rule_rewrites_filter_predicate() -> Result<()> {
    use datafusion::optimizer::OptimizerContext;
    use datafusion_expr::builder::table_scan;
    use datafusion_expr::{LogicalPlanBuilder, lit};

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let scan = table_scan(Some("t"), &schema, None)?.build()?;
    let in_list = Expr::Column(Column::new(Some("t"), "id"))
        .in_list(vec![lit(5_i64), lit(6_i64), lit(7_i64), lit(8_i64)], false);
    let plan = LogicalPlanBuilder::from(scan).filter(in_list)?.build()?;

    let rule = CayenneInListToRangeRewrite::new_with_table_source_predicate(|_| true);
    let cfg = OptimizerContext::new();
    let transformed = rule.rewrite(plan, &cfg)?;
    assert!(
        transformed.transformed,
        "rule should transform a Filter whose predicate is a rewritable InList"
    );
    let LogicalPlan::Filter(filter) = transformed.data else {
        panic!("expected Filter after rewrite")
    };
    assert!(
        matches!(filter.predicate, Expr::Between(_)),
        "predicate should be rewritten to Expr::Between, got: {:?}",
        filter.predicate
    );
    Ok(())
}

#[test]
fn inlist_to_range_rule_leaves_sparse_inlist_untouched() -> Result<()> {
    use datafusion::optimizer::OptimizerContext;
    use datafusion_expr::builder::table_scan;
    use datafusion_expr::{LogicalPlanBuilder, lit};

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let scan = table_scan(Some("t"), &schema, None)?.build()?;
    let in_list = Expr::Column(Column::new(Some("t"), "id"))
        .in_list(vec![lit(1_i64), lit(100_i64), lit(1000_i64)], false);
    let plan = LogicalPlanBuilder::from(scan).filter(in_list)?.build()?;

    let rule = CayenneInListToRangeRewrite::new_with_table_source_predicate(|_| true);
    let cfg = OptimizerContext::new();
    let transformed = rule.rewrite(plan, &cfg)?;
    assert!(
        !transformed.transformed,
        "rule should leave sparse IN-list untouched"
    );
    Ok(())
}

#[test]
fn inlist_to_range_rule_rewrites_nested_inside_and() -> Result<()> {
    use datafusion::optimizer::OptimizerContext;
    use datafusion_expr::builder::table_scan;
    use datafusion_expr::{LogicalPlanBuilder, lit};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Int64, false),
    ]));
    let scan = table_scan(Some("t"), &schema, None)?.build()?;
    let in_list = Expr::Column(Column::new(Some("t"), "id"))
        .in_list(vec![lit(5_i64), lit(6_i64), lit(7_i64), lit(8_i64)], false);
    let combined = in_list.and(Expr::Column(Column::new(Some("t"), "status")).eq(lit(1_i64)));
    let plan = LogicalPlanBuilder::from(scan).filter(combined)?.build()?;

    let rule = CayenneInListToRangeRewrite::new_with_table_source_predicate(|_| true);
    let cfg = OptimizerContext::new();
    let transformed = rule.rewrite(plan, &cfg)?;
    assert!(
        transformed.transformed,
        "rule should rewrite InList even when nested inside AND"
    );
    Ok(())
}

#[test]
fn inlist_to_range_rule_leaves_short_consecutive_inlist_untouched() -> Result<()> {
    use datafusion::optimizer::OptimizerContext;
    use datafusion_expr::builder::table_scan;
    use datafusion_expr::{LogicalPlanBuilder, lit};

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let scan = table_scan(Some("t"), &schema, None)?.build()?;
    let in_list = Expr::Column(Column::new(Some("t"), "id"))
        .in_list(vec![lit(5_i64), lit(6_i64), lit(7_i64)], false);
    let plan = LogicalPlanBuilder::from(scan).filter(in_list)?.build()?;

    let rule = CayenneInListToRangeRewrite::new_with_table_source_predicate(|_| true);
    let cfg = OptimizerContext::new();
    let transformed = rule.rewrite(plan, &cfg)?;
    assert!(
        !transformed.transformed,
        "rule should leave short consecutive IN-list untouched"
    );
    Ok(())
}

#[test]
fn inlist_to_range_rule_leaves_non_cayenne_filter_untouched() -> Result<()> {
    use datafusion::optimizer::OptimizerContext;
    use datafusion_expr::builder::table_scan;
    use datafusion_expr::{LogicalPlanBuilder, lit};

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let scan = table_scan(Some("t"), &schema, None)?.build()?;
    let in_list = Expr::Column(Column::new(Some("t"), "id"))
        .in_list(vec![lit(5_i64), lit(6_i64), lit(7_i64), lit(8_i64)], false);
    let plan = LogicalPlanBuilder::from(scan).filter(in_list)?.build()?;

    let rule = CayenneInListToRangeRewrite::new_with_table_source_predicate(|_| false);
    let cfg = OptimizerContext::new();
    let transformed = rule.rewrite(plan, &cfg)?;
    assert!(
        !transformed.transformed,
        "rule should leave non-Cayenne filter inputs untouched"
    );
    Ok(())
}

#[test]
fn inlist_to_range_rule_leaves_join_filter_untouched() -> Result<()> {
    use datafusion::optimizer::OptimizerContext;
    use datafusion_expr::builder::table_scan;
    use datafusion_expr::{JoinType, LogicalPlanBuilder, lit};

    let left_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let right_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let left = table_scan(Some("c"), &left_schema, None)?.build()?;
    let right = table_scan(Some("p"), &right_schema, None)?.build()?;

    let joined = LogicalPlanBuilder::from(left)
        .join_using(right, JoinType::Inner, vec!["id".into()])?
        .filter(
            Expr::Column(Column::new(Some("p"), "id"))
                .in_list(vec![lit(5_i64), lit(6_i64), lit(7_i64), lit(8_i64)], false),
        )?
        .build()?;

    let rule = CayenneInListToRangeRewrite::new_with_table_source_predicate(|_| true);
    let cfg = OptimizerContext::new();
    let transformed = rule.rewrite(joined, &cfg)?;
    assert!(
        !transformed.transformed,
        "rule should not rewrite join-level filter inputs that span multiple table scans"
    );
    Ok(())
}
