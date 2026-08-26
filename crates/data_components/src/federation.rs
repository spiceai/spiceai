/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Spice-owned `SQLExecutor` wrapper that applies a function deny-list to
//! federation decisions for any `SqlTable`-backed connector (Snowflake, `ClickHouse`, ODBC, …).
//!
//! This wrapper installs a logical optimizer that consults a `FunctionSupport`
//! deny-list and unwraps federated plans when an unsupported function appears,
//! so `DataFusion` evaluates the affected expression locally instead of
//! pushing Spice-only UDFs like `json_get_str` to the remote engine.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::Result as DataFusionResult,
    physical_plan::{PhysicalExpr, SendableRecordBatchStream},
    sql::{TableReference, unparser::dialect::Dialect},
};
use datafusion_federation::{
    FederatedTableProviderAdaptor, FederatedTableSource,
    sql::{LogicalOptimizer, RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource},
};
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;

use crate::function_support::{FunctionSupport, unfederate_plan_with_unsupported_functions};

/// `SQLExecutor` that delegates to a wrapped [`SqlTable`] but installs a logical
/// optimizer to consult a function deny-list.
pub struct DenyFunctionsSqlExecutor<T: 'static, P: 'static> {
    inner: Arc<SqlTable<T, P>>,
    function_support: Option<FunctionSupport>,
}

impl<T: 'static, P: 'static> DenyFunctionsSqlExecutor<T, P> {
    #[must_use]
    pub fn new(inner: Arc<SqlTable<T, P>>, function_support: Option<FunctionSupport>) -> Self {
        Self {
            inner,
            function_support,
        }
    }
}

impl<T, P> std::fmt::Debug for DenyFunctionsSqlExecutor<T, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DenyFunctionsSqlExecutor")
            .field("inner", &self.inner)
            .field("function_support", &self.function_support)
            .finish()
    }
}

#[async_trait]
impl<T: 'static, P: 'static> SQLExecutor for DenyFunctionsSqlExecutor<T, P> {
    fn name(&self) -> &str {
        SQLExecutor::name(self.inner.as_ref())
    }

    fn compute_context(&self) -> Option<String> {
        SQLExecutor::compute_context(self.inner.as_ref())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        SQLExecutor::dialect(self.inner.as_ref())
    }

    fn logical_optimizer(&self) -> Option<LogicalOptimizer> {
        let function_support = self.function_support.clone()?;
        Some(Box::new(move |plan| {
            unfederate_plan_with_unsupported_functions(plan, &function_support)
        }))
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> DataFusionResult<SendableRecordBatchStream> {
        SQLExecutor::execute(self.inner.as_ref(), query, schema, filters)
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        SQLExecutor::table_names(self.inner.as_ref()).await
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        SQLExecutor::get_table_schema(self.inner.as_ref(), table_name).await
    }
}

/// Build a [`FederatedTableProviderAdaptor`] for a [`SqlTable`] that
/// routes plan-pushdown decisions through [`DenyFunctionsSqlExecutor`].
///
/// This mirrors [`SqlTable::create_federated_table_provider`] but installs the
/// wrapper executor so that any plan referencing a function on the supplied
/// `function_support` deny-list (e.g. `json_get_str`) falls back to local
/// `DataFusion` evaluation instead of being unparsed into the remote engine's
/// SQL.
#[must_use]
pub fn create_spice_federated_table_provider<T: 'static, P: 'static>(
    table: Arc<SqlTable<T, P>>,
    schema: SchemaRef,
    table_reference: TableReference,
    function_support: Option<FunctionSupport>,
) -> FederatedTableProviderAdaptor {
    let executor: Arc<dyn SQLExecutor> = Arc::new(DenyFunctionsSqlExecutor::new(
        Arc::clone(&table),
        function_support,
    ));
    let fed_provider = Arc::new(SQLFederationProvider::new(executor));
    let table_source: Arc<dyn FederatedTableSource> = Arc::new(SQLTableSource::new_with_schema(
        fed_provider,
        RemoteTableRef::from(table_reference),
        schema,
    ));
    FederatedTableProviderAdaptor::new_with_provider(table_source, table)
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::error::Error;

    use crate::function_support::{FunctionRestriction, FunctionSupport};
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{
        ColumnarValue, Expr, Extension, JoinType, LogicalPlan, LogicalPlanBuilder, ScalarUDF,
        TableSource, Volatility, builder::LogicalTableSource, create_udf, expr::ScalarFunction,
    };
    use datafusion::prelude::{col, lit};
    use datafusion::sql::unparser::Unparser;
    use datafusion_federation::sql::SQLExecutor;
    use datafusion_federation::{FederatedPlanNode, sql::SQLFederationPlanner};
    use datafusion_table_providers::sql::db_connection_pool::{
        DbConnectionPool, JoinPushDown, dbconnection::DbConnection,
    };

    use super::*;

    struct MockConn {}

    impl DbConnection<(), &'static dyn ToString> for MockConn {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    struct MockPool {}

    #[async_trait]
    impl DbConnectionPool<(), &'static dyn ToString> for MockPool {
        async fn connect(
            &self,
        ) -> Result<Box<dyn DbConnection<(), &'static dyn ToString>>, Box<dyn Error + Send + Sync>>
        {
            Ok(Box::new(MockConn {}))
        }

        fn join_push_down(&self) -> JoinPushDown {
            JoinPushDown::Disallow
        }
    }

    fn test_sql_table() -> Arc<SqlTable<(), &'static dyn ToString>> {
        let pool: Arc<dyn DbConnectionPool<(), &'static dyn ToString> + Send + Sync> =
            Arc::new(MockPool {});
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, true),
        ]));
        Arc::new(SqlTable::new_with_schema(
            "test",
            &pool,
            schema,
            TableReference::bare("t"),
            None,
        ))
    }

    fn stub_udf(name: &str) -> Arc<ScalarUDF> {
        Arc::new(create_udf(
            name,
            vec![DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
        ))
    }

    fn table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
        Arc::new(LogicalTableSource::new(Arc::new(Schema::new(fields))))
    }

    fn scan_with_projection(udf_name: &str) -> LogicalPlan {
        let source = table_source(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, true),
        ]);
        LogicalPlanBuilder::scan("t", source, None)
            .expect("scan")
            .project(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                stub_udf(udf_name),
                vec![col("val")],
            ))])
            .expect("project")
            .build()
            .expect("build")
    }

    fn deny_support(names: &[&str]) -> FunctionSupport {
        FunctionSupport::new(
            Some(FunctionRestriction::Deny(
                names.iter().map(|s| (*s).to_string()).collect(),
            )),
            None,
            None,
        )
    }

    fn test_executor() -> impl SQLExecutor + 'static {
        DenyFunctionsSqlExecutor::new(test_sql_table(), None)
    }

    fn federated_plan(plan: LogicalPlan) -> LogicalPlan {
        let executor: Arc<dyn SQLExecutor> = Arc::new(test_executor());
        let planner = Arc::new(SQLFederationPlanner::new(executor));
        LogicalPlan::Extension(Extension {
            node: Arc::new(FederatedPlanNode::new(plan, planner)),
        })
    }

    fn assert_federated_plan_contains(actual: &LogicalPlan, expected: &LogicalPlan) {
        let LogicalPlan::Extension(extension) = actual else {
            panic!("expected federated extension plan");
        };
        let federated = extension
            .node
            .as_any()
            .downcast_ref::<FederatedPlanNode>()
            .expect("federated plan node");
        assert_eq!(federated.plan(), expected);
    }

    #[test]
    fn logical_optimizer_absent_when_no_deny_list() {
        let executor = DenyFunctionsSqlExecutor::new(test_sql_table(), None);
        assert!(
            executor.logical_optimizer().is_none(),
            "no deny-list should leave federation decisions unchanged"
        );
    }

    #[test]
    fn logical_optimizer_unfederates_denied_function() {
        let executor =
            DenyFunctionsSqlExecutor::new(test_sql_table(), Some(deny_support(&["json_get_str"])));
        let plan = scan_with_projection("json_get_str");
        let mut optimizer = executor
            .logical_optimizer()
            .expect("deny-list should install optimizer");
        let rewritten = optimizer(federated_plan(plan.clone())).expect("optimize plan");
        assert!(
            rewritten == plan,
            "deny-listed function in projection must block federation"
        );
    }

    #[test]
    fn logical_optimizer_keeps_non_denied_function_federated() {
        let executor =
            DenyFunctionsSqlExecutor::new(test_sql_table(), Some(deny_support(&["json_get_str"])));
        let plan = scan_with_projection("upper");
        let federated = federated_plan(plan.clone());
        let mut optimizer = executor
            .logical_optimizer()
            .expect("deny-list should install optimizer");
        let rewritten = optimizer(federated).expect("optimize plan");
        assert_federated_plan_contains(&rewritten, &plan);
    }

    #[test]
    fn create_spice_federated_table_provider_wires_deny_list() {
        // Regression test for #10703: building the federated adaptor must route
        // logical optimization through the deny-list wrapper.
        let table = test_sql_table();
        let schema = table.schema();
        let adaptor = create_spice_federated_table_provider(
            Arc::clone(&table),
            Arc::clone(&schema),
            TableReference::bare("t"),
            Some(deny_support(&["json_get_str"])),
        );

        // The adaptor's TableProvider fallback must be the underlying SqlTable
        // so non-federation contexts still scan correctly.
        assert!(adaptor.table_provider.is_some(), "fallback provider wired");

        // Federation source schema must match the SqlTable schema verbatim.
        assert_eq!(adaptor.source.schema().as_ref(), schema.as_ref());
    }

    /// Unparse a plan the way a federated connector would, and demand it
    /// succeed. See `federated_sql_result` for what these guards are for.
    fn federated_sql(plan: &LogicalPlan) -> String {
        federated_sql_result(plan).expect("plan should unparse")
    }

    /// The seam every guard below unparses through, fallible so a guard can pin
    /// a refusal as well as a rendering.
    ///
    /// The upstream fixes these guard live in the `spiceai/datafusion` fork on
    /// `spiceai-54`, so nothing here fails if a later pin bump drops them. The
    /// fork's branch is re-cut per `DataFusion` major and takes its own tests with
    /// it; these stay. Extend them whenever a pin bump carries another unparser
    /// fix — #13081 tracks the three the `edd8861e` → `b5cb7bb3` bump left
    /// unguarded, the two below arrived with `b5cb7bb3` → `8e881090`, and the
    /// captured-correlation refusal with `859621d6` → `c89b3320`.
    ///
    /// This unparses through the federation executor, which supplies no dialect
    /// here, so the SQL is the default dialect's rather than any one connector's.
    /// The plan shapes, not the spelling, are what these assert.
    fn federated_sql_result(plan: &LogicalPlan) -> DataFusionResult<String> {
        Unparser::new(test_executor().dialect().as_ref())
            .plan_to_sql(plan)
            .map(|statement| statement.to_string())
    }

    /// Assert `first` is rendered before `second`, which pins the clause order
    /// without pinning the formatting around it.
    fn assert_precedes(sql: &str, first: &str, second: &str) {
        let (Some(at_first), Some(at_second)) = (sql.find(first), sql.find(second)) else {
            panic!("expected both `{first}` and `{second}` in: {sql}");
        };
        assert!(
            at_first < at_second,
            "expected `{first}` before `{second}` in: {sql}"
        );
    }

    /// Regression test for #12406: a `fetch` the optimizer pushes into a join
    /// input bounds that input, not the join's output. Dropping it asks the
    /// remote engine for the whole table and evaluates the join over it, so the
    /// query can return more rows than the plan it came from.
    ///
    /// The scan carries a filter as well as the fetch, which is the shape the
    /// issue reports and the one the join-input transform rebuilds.
    #[test]
    fn a_fetch_pushed_into_a_join_input_survives_unparsing() {
        let left = LogicalPlanBuilder::scan_with_filters_fetch(
            "left_table",
            table_source(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, true),
            ]),
            None,
            vec![col("left_table.id").eq(lit("a"))],
            Some(5),
        )
        .expect("scan left");
        let right = LogicalPlanBuilder::scan(
            "right_table",
            table_source(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("age", DataType::Int32, true),
            ]),
            None,
        )
        .expect("scan right")
        .build()
        .expect("build right");
        let plan = left
            .join_on(
                right,
                JoinType::Inner,
                [col("left_table.id").eq(col("right_table.id"))],
            )
            .expect("join")
            .build()
            .expect("build join");

        // The fetch bounds the input, so it is rendered inside that input's own
        // scope rather than trailing the join.
        assert_precedes(&federated_sql(&plan), "LIMIT 5", "INNER JOIN");
    }

    /// Regression test for #12591: SQL evaluates `WHERE` before `LIMIT`, so a
    /// `Filter` above a `Limit` rendered as one `SELECT` carrying both means the
    /// opposite of the plan — it can keep rows the plan excludes, and more rows
    /// than the plan can produce.
    #[test]
    fn a_filter_above_a_limit_keeps_the_limit_scoped() {
        let plan = LogicalPlanBuilder::scan(
            "t",
            table_source(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, true),
            ]),
            None,
        )
        .expect("scan")
        .limit(0, Some(5))
        .expect("limit")
        .filter(col("t.id").eq(lit("a")))
        .expect("filter")
        .build()
        .expect("build");

        // The limit has to be taken first, so it is rendered in a scope the
        // filter sits outside of.
        assert_precedes(&federated_sql(&plan), "LIMIT 5", "WHERE");
    }

    /// Two `Int32` columns, matching the schema the upstream `EXISTS` bound
    /// cases use so these plans are the same shapes.
    fn exists_fetch_fields() -> Vec<Field> {
        vec![
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Int32, false),
        ]
    }

    fn exists_scan(name: &str) -> LogicalPlanBuilder {
        LogicalPlanBuilder::scan(name, table_source(exists_fetch_fields()), None)
            .expect("scan should build")
    }

    /// The same correlation still pushes down when there is no bound to scope,
    /// so the refusal stays gated on the bound rather than on the correlation's
    /// shape.
    ///
    /// Only sound where the unbounded rendering is correct in its own right. It
    /// is for a correlation naming several build inputs — every qualifier binds
    /// to the relation it came from — and it is not for the probe-qualified
    /// self-join, whose unbounded form loses the correlation to shadowing.
    fn assert_unbounded_exists_pushdown(plan: &LogicalPlan) {
        let sql = federated_sql_result(plan)
            .expect("an unbounded build side has no bound to scope, so it still unparses");
        assert!(
            sql.contains("EXISTS") && !sql.contains("LIMIT"),
            "expected an unbounded EXISTS pushdown, got: {sql}"
        );
    }

    /// `LeftSemi Join: t1.c = t2.c AND t1.d = t3.d` over a `t2 INNER JOIN t3`
    /// build side, bounded only when asked — the correlation names both of the
    /// build side's inputs.
    fn a_correlation_naming_two_build_inputs(fetch: Option<usize>) -> LogicalPlan {
        let build = exists_scan("t2")
            .join_on(
                exists_scan("t3").build().expect("build t3"),
                JoinType::Inner,
                [col("t2.c").eq(col("t3.c"))],
            )
            .expect("join build inputs");
        // Applied only when asked, so the unbounded plan carries no `Limit` node
        // at all — the shape a plan with no bound actually has, and the one
        // upstream's own fixture builds. The unparser reads `limit(0, None)` as
        // no bound either, so this is fidelity rather than a change of outcome.
        let build = match fetch {
            Some(fetch) => build.limit(0, Some(fetch)).expect("limit build side"),
            None => build,
        }
        .build()
        .expect("build build side");

        exists_scan("t1")
            .project(vec![col("t1.d")])
            .expect("project")
            .join_on(
                build,
                JoinType::LeftSemi,
                [col("t1.c").eq(col("t2.c")), col("t1.d").eq(col("t3.d"))],
            )
            .expect("semi join")
            .build()
            .expect("build plan")
    }

    /// `LeftSemi Join: t.c = t.c` where the build side is the same relation as
    /// the probe, bounded only when asked — the correlation's only qualifier is
    /// one the probe side also answers to.
    fn a_correlation_qualified_by_the_probe(fetch: Option<usize>) -> LogicalPlan {
        let build = LogicalPlanBuilder::scan_with_filters_fetch(
            "t",
            table_source(exists_fetch_fields()),
            None,
            vec![],
            fetch,
        )
        .expect("scan build side")
        .build()
        .expect("build build side");

        exists_scan("t")
            .project(vec![col("t.d")])
            .expect("project")
            .join_on(build, JoinType::LeftSemi, [col("t.c").eq(col("t.c"))])
            .expect("semi join")
            .build()
            .expect("build plan")
    }

    /// Regression test for #13277: a row bound on an `EXISTS`-style build side
    /// has to be moved into a scope of its own, and the scope can carry only one
    /// relation name. When the correlation names both of the build side's inputs,
    /// no name keeps every reference bound to the relation it came from.
    ///
    /// Leaving the bound beside the correlation is not a safe fallback: that SQL
    /// binds — `t1` to the outer query, `t2`/`t3` to the subquery's own inputs —
    /// so the remote engine runs it and answers from rows outside the bound. A
    /// semi join then reports a match on a row the plan never read. Refusing
    /// costs the pushdown instead of returning wrong rows.
    #[test]
    fn a_bounded_exists_refuses_a_correlation_naming_two_build_inputs() {
        let err = federated_sql_result(&a_correlation_naming_two_build_inputs(Some(5)))
            .expect_err("a correlation naming two build-side inputs must be refused");
        assert!(
            err.to_string().contains(
                "not supported when the correlation names more than one of the build side's inputs"
            ),
            "expected the refusal to name the unscopable correlation, got: {err}"
        );

        assert_unbounded_exists_pushdown(&a_correlation_naming_two_build_inputs(None));
    }

    /// The refusal every capture below is reported through. Asserted by one
    /// helper so a reword upstream is a one-place edit here, and matched on the
    /// whole sentence so a message that merely mentions `EXISTS` cannot pass for
    /// it.
    fn assert_captured_correlation_refused(plan: &LogicalPlan, context: &str) {
        let err = federated_sql_result(plan).expect_err(context);
        assert!(
            err.to_string().contains(
                "Unparsing an EXISTS-style join is not supported when a FROM the emitted SQL \
                 introduces would capture the correlation"
            ),
            "{context}, got: {err}"
        );
    }

    /// Regression test for #13277 and #12840: the correlation's only qualifier is
    /// one the subquery's own `FROM` also answers to, so the reference binds
    /// inside the body instead of to the query it was written against.
    ///
    /// `"t"."c" = "t"."c"` is then an inner tautology: `EXISTS` degenerates to
    /// "this relation has a row", so the semi join this builds keeps every probe
    /// row — including rows that match nothing — while an anti join over the same
    /// SQL drops every row instead. That is valid SQL, so the remote engine runs
    /// it and answers from the wrong rows rather than failing.
    ///
    /// **The bound is not what decides this.** SQL's name scoping captures the
    /// reference on its own, so both the bounded and unbounded plans have to be
    /// refused, and asserting both is the point of this test: the refusal used to
    /// live in `exists_scope_name`, which the caller consults only when a row
    /// bound has to be moved into a scope of its own, so the unbounded plan kept
    /// emitting the shadowed SQL. Refusing costs the pushdown — and, because
    /// `datafusion-federation` wraps the plan before unparsing it, costs the
    /// query — instead of returning wrong rows.
    ///
    /// Refusing is not repairing. Emitting these correctly needs the correlated
    /// qualifiers rewritten onto the scope a derived table introduces, which is
    /// what #12840 still tracks.
    #[test]
    fn an_exists_refuses_a_correlation_the_probe_qualifier_captures_at_any_bound() {
        assert_captured_correlation_refused(
            &a_correlation_qualified_by_the_probe(Some(5)),
            "a bounded correlation qualified by the probe side must be refused",
        );
        assert_captured_correlation_refused(
            &a_correlation_qualified_by_the_probe(None),
            "an unbounded correlation qualified by the probe side must be refused too, \
             because the capture is decided by name scoping rather than by the bound",
        );
    }

    /// The probe side for the unqualified-correlation guards: `p`, projected so
    /// both of its outputs carry a bare name. That projection is what leaves the
    /// join key unqualified.
    fn an_unqualified_correlation_semi_join(build_columns: &[&str]) -> LogicalPlan {
        let probe = exists_scan("p")
            .project(vec![col("p.c").alias("c"), col("p.d").alias("d")])
            .expect("project probe")
            .build()
            .expect("build probe");
        let build = LogicalPlanBuilder::scan(
            "b",
            table_source(
                build_columns
                    .iter()
                    .map(|name| Field::new(*name, DataType::Int32, false))
                    .collect(),
            ),
            None,
        )
        .expect("scan build")
        .build()
        .expect("build build side");

        LogicalPlanBuilder::from(probe)
            .join(
                build,
                JoinType::LeftSemi,
                (vec!["c"], vec![build_columns[0]]),
                None,
            )
            .expect("semi join")
            .build()
            .expect("build plan")
    }

    /// Regression test for #12840: a capture needs no shared relation name. An
    /// unqualified reference names no relation to disagree with, so it binds to
    /// whichever relation the innermost scope exposes the *column* on.
    ///
    /// `p` and `b` share no name here, but the probe projects its key to a bare
    /// `c` and `b` has a column called `c`, so the body's own `FROM "b"` answers
    /// to the reference and both halves of `("c" = "c")` bind to `b.c` — the same
    /// inner tautology the self-join reaches, arrived at without either side
    /// naming the other.
    #[test]
    fn an_exists_refuses_an_unqualified_correlation_the_body_exposes() {
        assert_captured_correlation_refused(
            &an_unqualified_correlation_semi_join(&["c", "d"]),
            "an unqualified correlation the build side exposes a column for must be refused",
        );
    }

    /// The keep direction for the same mechanism, which is what stops the guard
    /// above from being satisfied by refusing everything: an unqualified
    /// reference to a name the body does *not* expose binds outward, so it keeps
    /// its pushdown.
    ///
    /// Identical to the test above except for `b`'s column names. The name has to
    /// be absent from the *relation* rather than merely unprojected, because `b`
    /// is emitted bare and `FROM "b"` exposes every column it has whatever the
    /// plan projects — hence a different relation rather than the same one
    /// projected differently.
    #[test]
    fn an_exists_keeps_an_unqualified_correlation_the_body_lacks() {
        let sql = federated_sql(&an_unqualified_correlation_semi_join(&["e", "f"]));
        assert!(
            sql.contains("EXISTS"),
            "a correlation no relation in the body exposes must keep its pushdown, got: {sql}"
        );
    }

    /// The keep direction for the qualifier mechanism, and the reason it is here
    /// rather than left to upstream: this is the shape the federated TPC-H and
    /// TPC-DS benchmarks push down. A refusal reaches those as a query *failure*
    /// rather than a fallback, because `datafusion-federation` wraps the plan in
    /// a `FederatedPlanNode` before it tries to unparse it.
    ///
    /// TPC-H Q22 is `NOT EXISTS (SELECT * FROM orders WHERE o_custkey =
    /// c_custkey)`: the correlated reference is qualified by `customer`, the
    /// body's only relation is `orders`, and no column name is shared. Nothing
    /// about that is captured, so it has to keep rendering.
    #[test]
    fn an_exists_keeps_a_correlation_no_relation_in_the_body_answers_to() {
        let build = LogicalPlanBuilder::scan(
            "orders",
            table_source(vec![
                Field::new("o_orderkey", DataType::Int32, false),
                Field::new("o_custkey", DataType::Int32, false),
            ]),
            None,
        )
        .expect("scan orders")
        .build()
        .expect("build orders");

        let plan = LogicalPlanBuilder::scan(
            "customer",
            table_source(vec![
                Field::new("c_custkey", DataType::Int32, false),
                Field::new("c_phone", DataType::Utf8, false),
            ]),
            None,
        )
        .expect("scan customer")
        .project(vec![col("customer.c_phone")])
        .expect("project")
        .join_on(
            build,
            JoinType::LeftAnti,
            [col("customer.c_custkey").eq(col("orders.o_custkey"))],
        )
        .expect("anti join")
        .build()
        .expect("build plan");

        let sql = federated_sql(&plan);
        assert!(
            sql.contains("EXISTS"),
            "a correlation the body neither answers to nor exposes a column for must keep \
             its pushdown, got: {sql}"
        );
    }
}
