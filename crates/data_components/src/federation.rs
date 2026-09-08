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
    use datafusion::arrow::datatypes::{DataType, Field, IntervalMonthDayNano, Schema, TimeUnit};
    use datafusion::common::Column;
    use datafusion::datasource::TableProvider;
    use datafusion::functions::expr_fn::{date_part, date_trunc};
    use datafusion::functions_aggregate::expr_fn::count;
    use datafusion::logical_expr::{
        ColumnarValue, Expr, Extension, JoinType, LogicalPlan, LogicalPlanBuilder, ScalarUDF,
        TableSource, Volatility, builder::LogicalTableSource, cast, create_udf,
        expr::ScalarFunction,
    };
    use datafusion::prelude::{col, lit};
    use datafusion::scalar::ScalarValue;
    use datafusion::sql::unparser::Unparser;
    use datafusion::sql::unparser::dialect::{
        BigQueryDialect, CustomDialect, CustomDialectBuilder, DefaultDialect, DuckDBDialect,
        MySqlDialect, PostgreSqlDialect, SqliteDialect,
    };
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
    /// Each guard below is a canary for one unparser correctness fix that only a
    /// Spice patch to the `spiceai/datafusion` fork carries: without the fix the
    /// plan renders as SQL that means something other than the plan, so a
    /// federated query returns wrong rows or fails to bind. Nothing else in this
    /// repository notices if such a fix is lost, so every one of them wants a
    /// guard here — #13081 is the earlier instance of this gap, and the fixes it
    /// named are guarded below.
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
        scan_named(name, exists_fetch_fields())
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

    /// The keep direction, mirroring [`assert_captured_correlation_refused`]: the
    /// correlation is not captured, so the join still renders as a pushed-down
    /// `EXISTS`. Named for what it pins rather than asserted inline, so the two
    /// directions read as one pair.
    fn assert_exists_pushdown_kept(plan: &LogicalPlan, context: &str) {
        let sql = federated_sql(plan);
        assert!(sql.contains("EXISTS"), "{context}, got: {sql}");
    }

    /// Scan `name` with the given schema. [`exists_scan`] is this with
    /// [`exists_fetch_fields`]; the shapes whose relations need a schema of
    /// their own call it directly.
    fn scan_named(name: &str, fields: Vec<Field>) -> LogicalPlanBuilder {
        LogicalPlanBuilder::scan(name, table_source(fields), None).expect("scan should build")
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
        let build = scan_named(
            "b",
            build_columns
                .iter()
                .map(|name| Field::new(*name, DataType::Int32, false))
                .collect(),
        )
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
        assert_exists_pushdown_kept(
            &an_unqualified_correlation_semi_join(&["e", "f"]),
            "a correlation no relation in the body exposes must keep its pushdown",
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
        let build = scan_named(
            "orders",
            vec![
                Field::new("o_orderkey", DataType::Int32, false),
                Field::new("o_custkey", DataType::Int32, false),
            ],
        )
        .build()
        .expect("build orders");

        let plan = scan_named(
            "customer",
            vec![
                Field::new("c_custkey", DataType::Int32, false),
                Field::new("c_phone", DataType::Utf8, false),
            ],
        )
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

        assert_exists_pushdown_kept(
            &plan,
            "a correlation the body neither answers to nor exposes a column for must keep \
             its pushdown",
        );
    }

    /// The build relation in this self-join answers to the outer reference's
    /// qualifier. Emitting it as a correlated `EXISTS` would therefore bind the
    /// reference to the inner relation and silently return wrong rows. The
    /// capture guard from `DataFusion` fork PR #207 must refuse the shape even
    /// without a row bound.
    #[test]
    fn an_unbounded_exists_refuses_a_correlation_shadowed_by_its_build_relation() {
        assert_captured_correlation_refused(
            &a_correlation_qualified_by_the_probe(None),
            "a build relation that shadows the correlation must be refused",
        );
    }

    /// The unparser dialects this workspace hands to the unparser, plus a
    /// `CustomDialect` standing in for the family its connectors build — Snowflake,
    /// Oracle, Spark and ODBC each construct one with `CustomDialectBuilder`, and
    /// upstream's `SnowflakeDialect` is not used anywhere here.
    ///
    /// Across this set the renderings differ in identifier quoting and in whether
    /// `NULLS LAST` is emitted, which is why the guards below match bare relation
    /// names and nesting depth instead of a rendered string. They do *not* differ in
    /// the derived-table alias, the empty select list, or fully qualified columns:
    /// the scope-introducing paths alias unconditionally, and no dialect in this
    /// workspace enables `full_qualified_col`.
    ///
    /// The three fixes are each decided in dialect-free code, so this sweep guards
    /// the rendering rather than the decision. It is still the dimension this
    /// boundary adds: upstream pins these fixes against its own default dialect, so
    /// asserting that spelling here would restate an upstream assertion and pass by
    /// construction.
    fn federation_dialects() -> Vec<(&'static str, Arc<dyn Dialect>)> {
        vec![
            ("default", Arc::new(DefaultDialect {})),
            ("postgres", Arc::new(PostgreSqlDialect {})),
            ("mysql", Arc::new(MySqlDialect {})),
            ("sqlite", Arc::new(SqliteDialect {})),
            ("duckdb", Arc::new(DuckDBDialect::new())),
            ("bigquery", Arc::new(BigQueryDialect::new())),
            ("connector-custom", Arc::new(connector_style_dialect())),
        ]
    }

    /// A `CustomDialect` shaped like the ones this crate's connectors build: a quote
    /// style, and nothing that moves a clause.
    fn connector_style_dialect() -> CustomDialect {
        CustomDialectBuilder::new()
            .with_identifier_quote_style('"')
            .build()
    }

    fn unparse_with(dialect_name: &str, dialect: &dyn Dialect, plan: &LogicalPlan) -> String {
        Unparser::new(dialect)
            .plan_to_sql(plan)
            .unwrap_or_else(|error| {
                panic!("{dialect_name} dialect should unparse the plan: {error}")
            })
            .to_string()
    }

    /// Byte offset of the first `needle`, or a failure naming what was looked for.
    fn first_offset_of(sql: &str, needle: &str) -> usize {
        let Some(at) = sql.find(needle) else {
            panic!("expected `{needle}` in: {sql}");
        };
        at
    }

    /// Byte offset of the last `needle`.
    fn last_offset_of(sql: &str, needle: &str) -> usize {
        let Some(at) = sql.rfind(needle) else {
            panic!("expected `{needle}` in: {sql}");
        };
        at
    }

    /// Parenthesis nesting depth at `at`, so a guard can assert *where* a clause
    /// landed rather than how a dialect spelled the identifiers around it. Depth 0
    /// is the statement itself; deeper means inside a derived table or subquery.
    fn paren_depth_at(sql: &str, at: usize) -> usize {
        sql[..at].bytes().fold(0usize, |depth, byte| match byte {
            b'(' => depth + 1,
            b')' => depth.saturating_sub(1),
            _ => depth,
        })
    }

    /// A `Sort` sandwiched between two `Projection`s, which is the shape the hoist
    /// applies to.
    /// The same shape with the inner projection supplied, so a guard can give it an
    /// alias for the sort key to reference. The hoist replaces that projection's
    /// expressions, which is what drops the alias.
    fn sorted_between_projections_over(inner: Vec<Expr>, sort_key: Expr) -> LogicalPlan {
        LogicalPlanBuilder::scan(
            "person",
            table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("age", DataType::Int32, true),
            ]),
            None,
        )
        .expect("scan person")
        .project(inner)
        .expect("inner projection")
        .sort(vec![sort_key.sort(false, false)])
        .expect("sort")
        .project(vec![col("person.id")])
        .expect("outer projection")
        .build()
        .expect("build")
    }

    /// Regression test for the sort-key hoist carried by fork PR #191: a `Sort`
    /// between two `Projection`s is hoisted so the statement itself carries the
    /// ORDER BY. Unpatched, the hoist is gated on the sort key *being* one of the
    /// inner projection's outputs, so a key computed from one bails out and the
    /// ORDER BY is emitted inside a derived table.
    ///
    /// SQL does not require an enclosing query to preserve a derived table's
    /// ordering, so a buried ORDER BY lets the remote engine return the rows in any
    /// order — silently, with no error. The bare-column arm sorts the same shape by a
    /// plain column, which always worked, so a later pin cannot keep one arm and lose
    /// the other. Each arm also asserts the key it sorted by still reaches the
    /// rendered ORDER BY: hoisting replaces the inner projection's expressions, and
    /// the fix has to substitute references it drops rather than emit a different key.
    #[test]
    fn a_computed_sort_key_keeps_order_by_at_the_top_level() {
        for (key_kind, inner, sort_key, rendered_key, forbidden) in [
            (
                "bare-column",
                vec![col("person.id"), col("person.age")],
                col("person.age"),
                "age",
                None,
            ),
            (
                "computed",
                vec![col("person.id"), col("person.age")],
                col("person.age") + lit(1),
                "+ 1",
                None,
            ),
            // The alias arm: fork PR #191 fixed two things, and the two above only
            // reach the gate. Hoisting drops the inner projection's `doubled`, so the
            // key has to be substituted through to `(age * 2) + 1`; emitting a bare
            // `doubled` renders SQL the remote engine cannot bind.
            (
                "dropped-alias",
                vec![
                    col("person.id"),
                    (col("person.age") * lit(2)).alias("doubled"),
                ],
                col("doubled") + lit(1),
                "* 2",
                Some("doubled"),
            ),
        ] {
            let plan = sorted_between_projections_over(inner, sort_key);
            for (dialect_name, dialect) in federation_dialects() {
                let sql = unparse_with(dialect_name, dialect.as_ref(), &plan);
                let ordering_at = first_offset_of(&sql, "ORDER BY");
                assert_eq!(
                    paren_depth_at(&sql, ordering_at),
                    0,
                    "{dialect_name}/{key_kind}: ORDER BY landed inside a derived table, which the \
                     remote engine is free to ignore, so the rows can come back in any order: {sql}"
                );
                assert!(
                    sql[ordering_at..].contains(rendered_key),
                    "{dialect_name}/{key_kind}: the ORDER BY no longer sorts by the key the plan \
                     asked for, so the rows come back in a different order: {sql}"
                );
                if let Some(forbidden) = forbidden {
                    assert!(
                        !sql[ordering_at..].contains(forbidden),
                        "{dialect_name}/{key_kind}: the ORDER BY still names `{forbidden}`, an alias \
                         the hoist dropped, so the remote engine cannot bind the statement: {sql}"
                    );
                }
            }
        }
    }

    /// Regression test for the stacked-aggregate fix carried by fork PR #192: a
    /// `SELECT` expresses one grouping, so a second `Aggregate` underneath one
    /// already folded into the select list needs a scope of its own. Unpatched, that
    /// scope is skipped and the inner GROUP BY never reaches the emitted SQL.
    ///
    /// The optimizer builds exactly this shape for `count(DISTINCT c)` — an outer
    /// `count` over an inner grouping by `c` — and a federating consumer unparses the
    /// optimized plan. Losing the inner GROUP BY either fails to bind against the
    /// remote engine, because the inner alias is not a column of the base table, or
    /// binds where such a column happens to exist and counts every row instead of the
    /// distinct ones.
    #[test]
    fn a_stacked_aggregate_keeps_its_inner_group_by() {
        let plan = LogicalPlanBuilder::scan(
            "hits",
            table_source(vec![Field::new("user_id", DataType::Int32, false)]),
            None,
        )
        .expect("scan hits")
        .aggregate(
            vec![col("hits.user_id").alias("alias1")],
            Vec::<Expr>::new(),
        )
        .expect("inner aggregate")
        .aggregate(Vec::<Expr>::new(), vec![count(col("alias1"))])
        .expect("outer aggregate")
        .build()
        .expect("build");

        for (dialect_name, dialect) in federation_dialects() {
            let sql = unparse_with(dialect_name, dialect.as_ref(), &plan);
            assert!(
                sql.contains("GROUP BY"),
                "{dialect_name}: the inner grouping vanished, so this counts every row rather than \
                 the distinct ones, over an alias the base table does not have: {sql}"
            );
            let grouping_at = first_offset_of(&sql, "GROUP BY");
            assert!(
                paren_depth_at(&sql, grouping_at) >= 1,
                "{dialect_name}: the inner grouping has to be a scope of its own, since one SELECT \
                 expresses one grouping: {sql}"
            );
            assert!(
                sql[grouping_at..].contains("user_id"),
                "{dialect_name}: the surviving GROUP BY has to group by the column the inner \
                 aggregate grouped by: {sql}"
            );
            assert!(
                paren_depth_at(&sql, first_offset_of(&sql, "count(")) == 0,
                "{dialect_name}: the outer aggregate has to stay in the enclosing SELECT, over the \
                 grouped scope: {sql}"
            );
        }
    }

    /// The `Projection` over `Aggregate` shape a grouped dashboard card plans to:
    /// group by a truncated timestamp, then project a *wrapped* form of that same
    /// grouping expression. The projection reads the aggregate's own output columns,
    /// whose names come from the schema rather than being spelled here, so this shape
    /// survives a rename of how `DataFusion` names an unaliased group expression.
    fn projection_wrapping_a_grouping_expression() -> LogicalPlan {
        let grouped = LogicalPlanBuilder::scan(
            "advances",
            table_source(vec![Field::new(
                "funded_ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )]),
            None,
        )
        .expect("scan advances")
        .aggregate(
            vec![date_trunc(lit("week"), col("advances.funded_ts"))],
            vec![count(lit(1u8))],
        )
        .expect("aggregate")
        .build()
        .expect("build aggregate");

        let mut outputs = grouped.schema().columns().into_iter();
        let group_output = outputs
            .next()
            .expect("the group expression's output column");
        let count_output = outputs.next().expect("the aggregate's output column");

        LogicalPlanBuilder::from(grouped)
            .project(vec![
                cast(
                    cast(Expr::Column(group_output), DataType::Date32),
                    DataType::Utf8,
                )
                .alias("week_start"),
                Expr::Column(count_output).alias("advances_funded"),
            ])
            .expect("projection over the aggregate")
            .build()
            .expect("build projection")
    }

    /// Regression test for the projection-over-aggregate fix: a `SELECT` list that
    /// *wraps* a grouping expression needs the `Aggregate` in a scope of its own for
    /// any dialect that resolves `GROUP BY` against whole select items only.
    ///
    /// `GoogleSQL` is such a dialect. Flattening the two nodes into one `SELECT`
    /// leaves the grouping expression bare in `GROUP BY` and wrapped in the select
    /// list, and `BigQuery` rejects the statement outright with "SELECT list
    /// expression references column `funded_ts` which is neither grouped nor
    /// aggregated" — the whole statement fails, not one row of it.
    ///
    /// The two cheaper renderings are both wrong rather than merely different:
    /// `GROUP BY <output alias>` and `GROUP BY <ordinal>` group by the *wrapped*
    /// value, so a wrapper that is not injective over the grouping expression merges
    /// groups and sums their aggregates — fewer rows than the plan asked for, with no
    /// error. Only a derived table reproduces the plan's grouping for every wrapper,
    /// which is why this guard asserts the scope and not just that the statement
    /// parses.
    #[test]
    fn a_projection_wrapping_a_grouping_expression_keeps_the_aggregate_scoped() {
        let plan = projection_wrapping_a_grouping_expression();

        for (dialect_name, dialect) in federation_dialects() {
            let sql = unparse_with(dialect_name, dialect.as_ref(), &plan);
            let grouping_at = first_offset_of(&sql, "GROUP BY");

            if dialect.group_by_matches_select_subexpressions() {
                // The dialect binds the wrapped select item against the grouping
                // expression it contains, so one SELECT is a faithful rendering.
                continue;
            }

            assert!(
                paren_depth_at(&sql, grouping_at) >= 1,
                "{dialect_name}: this dialect matches GROUP BY against whole select items, so the \
                 grouping has to be a scope of its own — flattened, the select list references \
                 columns the statement never grouped: {sql}"
            );
            // Asserted on the rendered grouping call rather than on the base column:
            // a dialect may sanitise the derived output's alias out of the schema
            // name, which spells the base column inside it.
            let outer_select = &sql[..first_offset_of(&sql, "FROM")];
            assert!(
                !outer_select.contains("TIMESTAMP_TRUNC") && !outer_select.contains("date_trunc("),
                "{dialect_name}: the outer select list still re-derives the grouping expression \
                 instead of reading the grouped scope's output, which is the reference this \
                 dialect cannot bind: {sql}"
            );
        }
    }

    /// The semi-join shape every bound guard below shares: a probe side, a build side
    /// rendered as a correlated `EXISTS`, and an output projection. Only the build side
    /// differs between them, so each guard can assert what its own bound changes rather
    /// than restating the whole rendering.
    fn semi_join_over_build(build: LogicalPlan) -> LogicalPlan {
        let probe = LogicalPlanBuilder::scan(
            "probe",
            table_source(vec![
                Field::new("c", DataType::Utf8, false),
                Field::new("d", DataType::Utf8, true),
            ]),
            None,
        )
        .expect("scan probe")
        .build()
        .expect("build probe");

        LogicalPlanBuilder::from(probe)
            .join_on(
                build,
                JoinType::LeftSemi,
                [col("probe.c").eq(col("build.c"))],
            )
            .expect("join")
            .project(vec![col("probe.d")])
            .expect("output projection")
            .build()
            .expect("build join")
    }

    /// A build side bounded by `fetch`, and the same one without it.
    fn semi_join_with_build_side_fetch(fetch: Option<usize>) -> LogicalPlan {
        semi_join_over_build(
            LogicalPlanBuilder::scan_with_filters_fetch(
                "build",
                table_source(vec![Field::new("c", DataType::Utf8, false)]),
                None,
                Vec::<Expr>::new(),
                fetch,
            )
            .expect("scan build")
            .build()
            .expect("build build"),
        )
    }

    /// A build side bounded by a skip and nothing else, which is a `Limit` node with
    /// `fetch: None` — the shape a bound check that only looks for a row count misses.
    fn semi_join_with_build_side_offset(skip: usize) -> LogicalPlan {
        semi_join_over_build(
            LogicalPlanBuilder::scan(
                "build",
                table_source(vec![Field::new("c", DataType::Utf8, false)]),
                None,
            )
            .expect("scan build")
            .limit(skip, None)
            .expect("offset the build side")
            .build()
            .expect("build build"),
        )
    }

    /// Regression test for the bounded-`EXISTS` scoping carried by fork PR #201: a
    /// semi, anti or mark join unparses its build side as a correlated `EXISTS`, and
    /// unpatched, a row bound on that side is emitted beside the correlation
    /// predicate. SQL applies the bound after the `WHERE`, so it chooses among the
    /// rows the correlation has already matched instead of choosing which rows the
    /// correlation can see, and the subquery searches the whole relation.
    ///
    /// That is a wrong-rows defect rather than a too-many-rows one: a semi or mark
    /// join reports a match on a row the plan never read, and an anti join is the
    /// mirror image and drops a row it should have returned. The bound therefore has
    /// to be rendered in a derived table the correlation sits outside of.
    #[test]
    fn a_bounded_exists_build_side_is_scoped_outside_the_correlation() {
        let plan = semi_join_with_build_side_fetch(Some(5));

        for (dialect_name, dialect) in federation_dialects() {
            let sql = unparse_with(dialect_name, dialect.as_ref(), &plan);

            // The last mention of the probe relation is the correlation; the first is
            // the outer query's own FROM. Matching the bare name keeps this
            // independent of how each dialect quotes it.
            let bound = first_offset_of(&sql, "LIMIT");
            assert!(
                bound < last_offset_of(&sql, "probe"),
                "{dialect_name}: the bound is applied after the correlation, so it selects among \
                 the rows already matched and the subquery searches the whole relation: {sql}"
            );

            // Two levels below the EXISTS, not one: the bound sitting directly in the
            // EXISTS body is the defect above, and that is still deeper than the
            // EXISTS itself.
            let exists = paren_depth_at(&sql, first_offset_of(&sql, "EXISTS"));
            assert!(
                paren_depth_at(&sql, bound) >= exists + 2,
                "{dialect_name}: the bound has to sit in a derived table of its own inside the \
                 EXISTS body, not directly in that body beside the correlation: {sql}"
            );
        }
    }

    /// The companion to [`a_bounded_exists_build_side_is_scoped_outside_the_correlation`]:
    /// the scoping is gated on the build side actually carrying a bound, so an
    /// unbounded one keeps the plain correlated form and its pushdown. Without this,
    /// a change that scoped every build side would satisfy the guard above while
    /// costing every unbounded semi join its federation.
    #[test]
    fn an_unbounded_exists_build_side_is_left_unscoped() {
        let plan = semi_join_with_build_side_fetch(None);

        for (dialect_name, dialect) in federation_dialects() {
            let sql = unparse_with(dialect_name, dialect.as_ref(), &plan);
            assert!(
                !sql.contains("LIMIT"),
                "{dialect_name}: an unbounded build side must not acquire a bound: {sql}"
            );

            // Carrying no bound is not the invariant — scoping every build side would
            // satisfy that too, since a scope with nothing to bound emits no LIMIT
            // either. What has to hold is that the build relation is still named
            // directly in the EXISTS body's own FROM.
            let exists = paren_depth_at(&sql, first_offset_of(&sql, "EXISTS"));
            assert_eq!(
                paren_depth_at(&sql, first_offset_of(&sql, "build")),
                exists + 1,
                "{dialect_name}: an unbounded build side has to stay in the EXISTS body's own FROM, \
                 not move behind a derived table that costs the join its pushdown: {sql}"
            );
        }
    }

    /// An **offset-only** build side, which is the same wrong-rows defect as the bounded
    /// one and is reached by a different field: `fetch` is `None`, so a scoping decision
    /// that keys on a row count alone leaves it beside the correlation. The skip then
    /// applies after the `WHERE`, discarding rows the correlation matched instead of
    /// choosing which rows it could see — so a semi or mark join reports no match on a
    /// row that has one, and an anti join returns a row it should have dropped.
    ///
    /// It is a separate guard rather than another arm of the bounded one because the
    /// fixture cannot produce it: `scan_with_filters_fetch` only carries `fetch`, so a
    /// skip has to come from a `Limit` node above the scan.
    #[test]
    fn an_offset_only_exists_build_side_is_scoped_outside_the_correlation() {
        let plan = semi_join_with_build_side_offset(3);

        for (dialect_name, dialect) in federation_dialects() {
            let sql = unparse_with(dialect_name, dialect.as_ref(), &plan);

            let bound = first_offset_of(&sql, "OFFSET");
            assert!(
                bound < last_offset_of(&sql, "probe"),
                "{dialect_name}: the skip is applied after the correlation, so it discards rows the \
                 correlation matched rather than choosing which rows it can see: {sql}"
            );

            let exists = paren_depth_at(&sql, first_offset_of(&sql, "EXISTS"));
            assert!(
                paren_depth_at(&sql, bound) >= exists + 2,
                "{dialect_name}: the skip has to sit in a derived table of its own inside the EXISTS \
                 body, not directly in that body beside the correlation: {sql}"
            );
        }
    }

    /// The **grouped** outer aggregate, which is the shape the optimizer builds for
    /// `a, count(DISTINCT b) GROUP BY a`. Fork PR #192 fixed this alongside the ungrouped
    /// form, and it needs its own guard because it has a failure the ungrouped one cannot
    /// have: once the inner aggregate becomes a derived table, the outer `GROUP BY` and
    /// projection have to be requalified to that derived alias. Emitting them against the
    /// base relation names a table that is out of scope outside the derived table, and the
    /// statement fails to bind at the remote engine — while a guard that only checks the
    /// inner grouping survived still passes.
    #[test]
    fn a_grouped_stacked_aggregate_binds_its_outer_clauses_through_the_derived_scope() {
        let plan = LogicalPlanBuilder::scan(
            "hits",
            table_source(vec![
                Field::new("user_id", DataType::Int32, false),
                Field::new("region", DataType::Utf8, false),
            ]),
            None,
        )
        .expect("scan hits")
        .aggregate(
            vec![col("hits.region"), col("hits.user_id")],
            Vec::<Expr>::new(),
        )
        .expect("inner aggregate")
        .aggregate(vec![col("hits.region")], vec![count(col("hits.user_id"))])
        .expect("outer aggregate")
        .build()
        .expect("build");

        for (dialect_name, dialect) in federation_dialects() {
            let sql = unparse_with(dialect_name, dialect.as_ref(), &plan);

            // Two groupings, in two scopes: the inner one belongs to the derived table
            // and the outer one to the enclosing SELECT, because one SELECT expresses
            // one grouping.
            let inner_grouping = first_offset_of(&sql, "GROUP BY");
            let outer_grouping = last_offset_of(&sql, "GROUP BY");
            assert!(
                inner_grouping < outer_grouping,
                "{dialect_name}: only one grouping survived, so this is no longer the stacked shape \
                 and either the distinct-ness or the outer grouping has been lost: {sql}"
            );
            assert!(
                paren_depth_at(&sql, inner_grouping) >= 1,
                "{dialect_name}: the inner grouping has to be a scope of its own: {sql}"
            );
            assert_eq!(
                paren_depth_at(&sql, outer_grouping),
                0,
                "{dialect_name}: the outer grouping belongs to the enclosing SELECT, not inside the \
                 derived table: {sql}"
            );

            // The requalification. `hits` is reachable only inside the derived table, so
            // naming it in either outer clause is a statement the remote engine refuses.
            assert!(
                !sql[outer_grouping..].contains("hits"),
                "{dialect_name}: the outer GROUP BY names the base relation, which is out of scope \
                 outside the derived table, so the statement cannot bind: {sql}"
            );
            let select_list = &sql[..first_offset_of(&sql, "FROM")];
            assert!(
                !select_list.contains("hits"),
                "{dialect_name}: the outer projection names the base relation rather than the derived \
                 alias, so the statement cannot bind: {sql}"
            );
            assert!(
                select_list.contains("count("),
                "{dialect_name}: the outer aggregate has to stay in the enclosing SELECT, over the \
                 grouped scope: {sql}"
            );
            // Not satisfied by an outer `GROUP BY 1`: it has to group by the column the
            // query grouped by, reached through the derived alias.
            assert!(
                sql[outer_grouping..].contains("region"),
                "{dialect_name}: the outer grouping no longer groups by the column the query asked \
                 for: {sql}"
            );
        }
    }

    /// A relation with two integer columns, for the derived-projection guards below.
    fn two_column_source() -> Arc<dyn TableSource> {
        table_source(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ])
    }

    /// A reference to a `Projection` output by the logical name the projection's
    /// schema reports, which is what an enclosing scope holds when the projection
    /// never named the output itself.
    fn output_named(name: &str) -> Expr {
        Expr::Column(Column::new_unqualified(name))
    }

    /// A volatile scalar function, which is the one output the flattened-`SELECT`
    /// repair deliberately declines to inline.
    fn volatile_udf(name: &str) -> Arc<ScalarUDF> {
        Arc::new(create_udf(
            name,
            vec![],
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(|_args: &[ColumnarValue]| {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.5))))
            }),
        ))
    }

    /// Every enclosing shape that reaches a derived table through the projection
    /// whose outputs it exposes, paired with the relation the enclosing scope names
    /// the derived table by where it has one.
    ///
    /// These are the nodes the naming walk carries an output name out through. A
    /// `Sort` on its own is not among them: a `Sort` between two `Projection`s is
    /// hoisted to the statement's own ORDER BY, so no derived table is built —
    /// `a_computed_sort_key_keeps_order_by_at_the_top_level` is the guard on that.
    /// Bounding it is what forces the scope, which is why the sorted arm carries a
    /// limit. Each arm is a distinct class the walk handles, so a pin that keeps one
    /// arm and drops another fails here rather than in a federated query.
    fn derived_scope_shapes(
        inner: &Expr,
        output_name: &str,
    ) -> Vec<(&'static str, LogicalPlan, Option<&'static str>)> {
        let scanned = || LogicalPlanBuilder::scan("t", two_column_source(), None).expect("scan t");
        let projected = || {
            scanned()
                .project(vec![inner.clone()])
                .expect("inner projection")
        };
        let read_out = |builder: LogicalPlanBuilder| {
            builder
                .project(vec![output_named(output_name)])
                .expect("outer projection")
                .build()
                .expect("build")
        };

        vec![
            (
                "filtered",
                read_out(
                    projected()
                        .filter(output_named(output_name).gt(lit(0)))
                        .expect("filter on the projection output"),
                ),
                None,
            ),
            (
                "limited",
                read_out(projected().limit(0, Some(5)).expect("limit")),
                None,
            ),
            (
                // The inner ORDER BY sorts by the output, so this arm fails unless the
                // sorted node is rebuilt around the *named* projection: an ORDER BY
                // naming the unnamed output binds no better than the outer reference.
                "sorted",
                read_out(
                    projected()
                        .sort(vec![output_named(output_name).sort(true, false)])
                        .expect("sort by the projection output")
                        .limit(0, Some(3))
                        .expect("limit"),
                ),
                None,
            ),
            (
                "distinct",
                read_out(projected().distinct().expect("distinct")),
                None,
            ),
            (
                // A `DISTINCT ON` emits its own SELECT list, so its outputs are the ones
                // the enclosing scope binds — naming only the projection beneath it
                // misses them.
                "distinct-on",
                read_out(
                    scanned()
                        .distinct_on(vec![col("t.a")], vec![inner.clone()], None)
                        .expect("distinct on"),
                ),
                None,
            ),
            (
                // With its own ORDER BY, which is the shape that cannot be rebuilt by
                // round-tripping the node's reported expressions — those do not carry a
                // `DISTINCT ON`'s sort expressions.
                "distinct-on-sorted",
                read_out(
                    scanned()
                        .distinct_on(
                            vec![col("t.a")],
                            vec![inner.clone()],
                            Some(vec![col("t.a").sort(true, false)]),
                        )
                        .expect("distinct on, sorted"),
                ),
                None,
            ),
            (
                // Reached through a relation alias, so the enclosing reference is
                // qualified — and the alias carries no column list, so it names the
                // relation without naming any of its columns. An alias that *does* carry
                // one already names every output and must be left alone; that half is
                // reachable only from parsed SQL, so the fork's own round-trip tests are
                // what guard it. The scan here carries no projection, which is the half of
                // the alias class the naming repairs — with one pushed down the output
                // cannot be named at all, which
                // `a_projected_scan_under_an_alias_does_not_name_the_output_its_scope_references`
                // pins.
                "aliased",
                projected()
                    .alias("x")
                    .expect("alias")
                    .project(vec![Expr::Column(Column::new(Some("x"), output_name))])
                    .expect("outer projection")
                    .build()
                    .expect("build"),
                Some("x"),
            ),
        ]
    }

    /// The identifier the enclosing scope references — the outer `SELECT` list's
    /// single output, spelled and quoted the way this dialect spells it.
    ///
    /// Read out of the statement rather than written into the guard because a dialect
    /// may rewrite the identifier: `BigQuery` renders `t.a + t.b` as `t_46a + t_46b`.
    /// What has to hold is that the derived table exposes *whatever* name the
    /// enclosing scope ended up using, so the guard has to ask the statement which
    /// name that is.
    fn outer_reference(sql: &str) -> &str {
        let Some(list) = sql.strip_prefix("SELECT ") else {
            panic!("expected a statement starting with a SELECT list: {sql}");
        };
        let Some(end) = list.find(" FROM ") else {
            panic!("expected a FROM clause after the SELECT list: {sql}");
        };
        &list[..end]
    }

    /// The derived table's own `SELECT` list — what it exposes to the scope that
    /// encloses it.
    ///
    /// Narrower than "everything after the derived table starts", because `AS`
    /// introduces relation aliases as well as column aliases (`FROM t AS s`, `) AS s`)
    /// and those are emitted whether or not the outputs are named. A guard that reads
    /// the wider region cannot tell a named output from an unnamed one.
    fn derived_select_list(sql: &str) -> &str {
        let opens = "FROM (SELECT ";
        let list = &sql[first_offset_of(sql, opens) + opens.len()..];
        let Some(end) = list.find(" FROM ") else {
            panic!("expected the derived table's own FROM clause in: {sql}");
        };
        &list[..end]
    }

    /// Everything the enclosing `SELECT` list draws from — the derived table and the
    /// alias it is attached to.
    ///
    /// Deliberately wider than `derived_select_list`: the repair for the remaining
    /// half of #12751 names the *relation's* columns on the alias it attaches
    /// (`) AS s ("t.a + t.b")`), which lands after the derived table closes and so
    /// falls outside the derived `SELECT` list entirely. A guard that reads only that
    /// list cannot see that repair land. Searching for a specific column is what makes
    /// the wider region safe here — the reason `derived_select_list` is narrow is that
    /// a bare `AS` matches the relation aliases too.
    fn from_clause(sql: &str) -> &str {
        let Some(start) = sql.find(" FROM ") else {
            panic!("expected a FROM clause after the SELECT list: {sql}");
        };
        &sql[start..]
    }

    /// The column half of a reference the enclosing scope qualifies by `relation`.
    ///
    /// Spelled as the three ways a dialect writes the relation name rather than by
    /// splitting on the last `.`, because the identifier itself contains dots — and,
    /// for a literal output, quote characters of its own.
    fn column_of<'a>(reference: &'a str, relation: Option<&str>) -> &'a str {
        let Some(relation) = relation else {
            return reference;
        };
        for prefix in [
            format!("{relation}."),
            format!("\"{relation}\"."),
            format!("`{relation}`."),
        ] {
            if let Some(column) = reference.strip_prefix(prefix.as_str()) {
                return column;
            }
        }
        panic!("expected `{reference}` to be qualified by `{relation}`");
    }

    /// Regression test for #12751, fixed upstream by fork PR #206: a `Projection`
    /// whose output it never named becomes a derived table when the enclosing
    /// `SELECT` list is already taken, and the enclosing scope refers to that output
    /// by its *logical* name. Nothing named the derived table's columns, so the name
    /// the outer scope used matched nothing the derived table exposed — the engine
    /// named the column itself (`?column?` on `PostgreSQL`) and the emitted statement
    /// carried a reference no engine can bind:
    ///
    /// ```sql
    /// SELECT "t.a + t.b" FROM (SELECT (t.a + t.b) FROM t) WHERE ("t.a + t.b" > 0)
    /// --     ^^^^^^^^^^^ names nothing the derived table exposes
    /// ```
    ///
    /// A federated pushdown emits exactly this to the remote engine, so the failure
    /// is the whole query, not a fallback: an unbindable identifier is a hard error
    /// from the remote engine rather than a plan the runtime can run locally instead.
    ///
    /// The matrix is every output kind that reaches a derived table unnamed — a
    /// computed expression, a literal, a literal whose logical name carries each
    /// dialect's own quote character, and a volatile call — across every enclosing
    /// shape the naming walk handles (`derived_scope_shapes`) — every shape it can
    /// repair, which is not every shape #12751 reports: a projected scan under a
    /// relation alias still cannot be named, and the test below this one pins that.
    /// The volatile output is
    /// the one the flattened-`SELECT` repair (#12599) cannot help: inlining a
    /// volatile expression evaluates it a second time in a clause that can observe a
    /// different value than the `SELECT` list did, so that repair declines it and
    /// leaves the reference unbindable. Naming the output binds the reference *and*
    /// keeps the single evaluation.
    #[test]
    fn a_derived_projection_names_the_output_its_scope_references() {
        let volatile =
            Expr::ScalarFunction(ScalarFunction::new_udf(volatile_udf("random"), vec![]));
        for (output_kind, inner, output_name) in [
            ("computed", col("t.a") + col("t.b"), "t.a + t.b"),
            ("literal", lit(1), "Int32(1)"),
            // A logical name carrying both quote characters this crate's dialects use,
            // so the alias has to be escaped rather than merely emitted.
            ("quoted-literal", lit("a\"b`c"), "Utf8(\"a\"b`c\")"),
            ("volatile", volatile, "random()"),
        ] {
            for (scope_kind, plan, relation) in derived_scope_shapes(&inner, output_name) {
                for (dialect_name, dialect) in federation_dialects() {
                    let arm = format!("{dialect_name}/{output_kind}/{scope_kind}");
                    let sql = unparse_with(dialect_name, dialect.as_ref(), &plan);

                    // The shape the issue is about only exists once the projection is a
                    // derived table. Without this the guard would pass on a statement
                    // that flattened into one SELECT and never had the problem.
                    let derived_at = first_offset_of(&sql, "FROM (SELECT ");

                    let reference = outer_reference(&sql);
                    assert_eq!(
                        paren_depth_at(&sql, first_offset_of(&sql, reference)),
                        0,
                        "{arm}: the enclosing scope's reference is not in the enclosing scope, so \
                         this guard is not looking at the shape it is for: {sql}"
                    );

                    // The invariant: the derived table has to expose the name the
                    // enclosing scope uses. `AS <column>` cannot be satisfied by the
                    // derived table's own alias — that names the relation, not a column,
                    // and it is a different identifier (`derived_projection`, `x`).
                    let column = column_of(reference, relation);
                    let binding = format!("AS {column}");
                    let Some(binding_at) = sql.find(&binding) else {
                        panic!(
                            "{arm}: the derived table does not name its output {column}, which is \
                             the identifier the enclosing scope references, so the remote engine \
                             cannot bind the statement: {sql}"
                        );
                    };
                    assert!(
                        binding_at > derived_at && paren_depth_at(&sql, binding_at) >= 1,
                        "{arm}: {column} is named outside the derived table, so it still does not \
                         name one of the derived table's columns: {sql}"
                    );
                }
            }
        }
    }

    /// The single-evaluation half of the volatile arm above, asserted separately
    /// because it is a different failure: inlining the producing expression at the
    /// point of use would also make the statement bind, while answering the query
    /// with rows the `SELECT` list never saw.
    ///
    /// Naming the output is only a correct repair while the output is still
    /// evaluated once, so this pins the property the naming fix has to preserve
    /// rather than one it introduces: what it refuses is a repair that binds the
    /// reference by inlining the expression instead of naming it.
    #[test]
    fn a_derived_volatile_output_is_evaluated_once() {
        let volatile =
            Expr::ScalarFunction(ScalarFunction::new_udf(volatile_udf("random"), vec![]));
        for (scope_kind, plan, _) in derived_scope_shapes(&volatile, "random()") {
            for (dialect_name, dialect) in federation_dialects() {
                let sql = unparse_with(dialect_name, dialect.as_ref(), &plan);
                // The call renders as `random()`; a reference to its output renders as
                // a quoted identifier, so counting the unquoted call counts evaluations.
                let quoted =
                    sql.matches("\"random()\"").count() + sql.matches("`random()`").count();
                // Saturating so a dialect that renders the call some other way reports 0
                // evaluations against the SQL rather than overflowing the subtraction.
                let evaluations = sql.matches("random()").count().saturating_sub(quoted);
                assert_eq!(
                    evaluations, 1,
                    "{dialect_name}/{scope_kind}: the volatile call is evaluated {evaluations} \
                     times, so the predicate can observe a different value than the SELECT list \
                     did and the query answers with rows the SELECT list never saw: {sql}"
                );
            }
        }
    }

    /// The half of #12751 that fork PR #206 does not fix, pinned so the repair is
    /// noticed rather than quietly leaving this shape unguarded.
    ///
    /// A scan projection pushed down under a relation alias is requalified onto the
    /// alias before the derived table is built, so the only name the derived table can
    /// report for the output — `s.a + s.b` — is not the one the enclosing scope holds
    /// for it, `t.a + t.b`:
    ///
    /// ```sql
    /// SELECT s."t.a + t.b" FROM (SELECT (s.a + s.b) AS "s.a + s.b" FROM t AS s) AS s
    /// ```
    ///
    /// Naming the output cannot close that gap, which is why the fix above does not
    /// try: both names are right for their own scope, and the repair is for the
    /// enclosing scope to name the relation's columns on the alias it attaches. The
    /// fork pins the same rendering in `test_subquery_alias_over_pushed_down_scan_
    /// still_unbindable`; this is the repo-side canary for it.
    ///
    /// Projection pushdown is an ordinary optimized shape, so this is a live defect on
    /// the federated pushdown path, and it is what keeps #12751 open. This test fails
    /// once the gap is repaired: move the shape into `derived_scope_shapes` and close
    /// #12751, rather than re-pinning the rendering below.
    #[test]
    fn a_projected_scan_under_an_alias_does_not_name_the_output_its_scope_references() {
        let plan = LogicalPlanBuilder::scan("t", two_column_source(), Some(vec![0, 1]))
            .expect("scan t with a pushed-down projection")
            .project(vec![col("t.a") + col("t.b")])
            .expect("inner projection")
            .alias("s")
            .expect("alias")
            .project(vec![Expr::Column(Column::new(Some("s"), "t.a + t.b"))])
            .expect("outer projection")
            .build()
            .expect("build");
        for (dialect_name, dialect) in federation_dialects() {
            let sql = unparse_with(dialect_name, dialect.as_ref(), &plan);
            let list = derived_select_list(&sql);

            // The derived table does name an output — this is the requalified name, not
            // the absence of naming that preceded the fix — so the pin below is about
            // *which* name it reports, not about whether it reports one. Read from the
            // derived SELECT list alone: `AS` also introduces the relation aliases
            // (`FROM t AS s`, `) AS s`), which are present either way, so a search over
            // the whole statement would hold on the pre-#206 rendering too and this
            // assertion would distinguish nothing.
            assert!(
                list.contains(" AS "),
                "{dialect_name}: the derived table names no output at all, which is the \
                 pre-#206 behaviour rather than the gap this pins: {sql}"
            );

            // `column_of` panics unless the enclosing reference is qualified by `s`,
            // which is what makes the requalification the subject of this guard.
            //
            // Asked of the whole `FROM` clause rather than of the derived `SELECT`
            // list, because the two ways a pin bump can close this gap land in
            // different places: naming the derived output puts the name inside the
            // list, while naming the relation's columns on the alias — the repair the
            // comment above says this shape actually needs — puts it after the derived
            // table closes. Reading only the list would leave this sentinel green
            // through exactly the fix it exists to catch. Today the enclosing scope is
            // the one place the name appears at all.
            let column = column_of(outer_reference(&sql), Some("s"));
            assert!(
                !from_clause(&sql).contains(column),
                "{dialect_name}: the derived table now exposes {column}, so this shape binds and \
                 the remaining half of #12751 is repaired — move it into \
                 `derived_scope_shapes` and close the issue instead of re-pinning this: {sql}"
            );
        }
    }

    /// The identifier an `AS` at `at` introduces, read back out of the rendered SQL.
    ///
    /// Reading the name rather than matching one is what makes the guard below
    /// dialect-independent: `BigQuery` escapes a `.` in an output name to its
    /// character code, so the derived table it builds names the same output
    /// `t_46a + t_46b`. Asserting the logical spelling would fail on a dialect that
    /// did the right thing.
    fn alias_introduced_at(sql: &str, at: usize) -> Option<&str> {
        let rest = sql.get(at + " AS ".len()..)?;
        let close = match rest.chars().next()? {
            '"' => '"',
            '`' => '`',
            '[' => ']',
            _ => {
                let end = rest
                    .find(|c: char| !c.is_alphanumeric() && c != '_')
                    .unwrap_or(rest.len());
                return (end > 0).then(|| &rest[..end]);
            }
        };
        let end = rest[1..].find(close)? + 1;
        Some(&rest[1..end])
    }

    /// Regression test for the derived-output naming carried by fork PR #206: a
    /// projection that becomes a derived table has to name the outputs it does not
    /// name itself, because the enclosing scope refers to them by their logical name.
    ///
    /// Left unnamed, the engine names the column instead — `?column?` on
    /// `PostgreSQL` — while the statement around it still says `"t.a + t.b"`. The
    /// remote engine cannot bind that, so the pushdown fails outright
    /// ([#12751](https://github.com/spiceai/spiceai/issues/12751)).
    ///
    /// Both shapes that put a projection in a scope of its own are covered: a filter
    /// above it, and a row limit above it. Each carries the reference from the
    /// enclosing scope, which is what makes the missing name observable.
    #[test]
    fn a_derived_tables_unnamed_outputs_are_named() {
        let computed = "t.a + t.b";
        let shapes: Vec<(&str, LogicalPlan)> = vec![
            (
                "filter-above",
                derived_computed_projection()
                    .filter(col(computed).gt(lit(1)))
                    .expect("filter above the projection")
                    .project(vec![col(computed)])
                    .expect("outer projection")
                    .build()
                    .expect("build"),
            ),
            (
                "limit-above",
                derived_computed_projection()
                    .limit(0, Some(5))
                    .expect("limit above the projection")
                    .project(vec![col(computed)])
                    .expect("outer projection")
                    .build()
                    .expect("build"),
            ),
        ];

        for (shape, plan) in shapes {
            for (dialect_name, dialect) in federation_dialects() {
                let sql = unparse_with(dialect_name, dialect.as_ref(), &plan);
                let alias = sql
                    .match_indices(" AS ")
                    .find(|(at, _)| paren_depth_at(&sql, *at) >= 1)
                    .and_then(|(at, _)| alias_introduced_at(&sql, at));
                let Some(alias) = alias else {
                    panic!(
                        "{dialect_name}/{shape}: the derived table does not name its computed \
                         output, so the enclosing reference to it binds to a name the engine \
                         invented instead: {sql}"
                    );
                };
                assert!(
                    sql.match_indices(alias)
                        .any(|(at, _)| paren_depth_at(&sql, at) == 0),
                    "{dialect_name}/{shape}: the enclosing scope refers to something other than \
                     the name `{alias}` the derived table gives its output, so the statement \
                     cannot bind: {sql}"
                );
            }
        }
    }

    /// A projection of `a + b` over `t`, which `DataFusion` names `t.a + t.b` and SQL
    /// names nothing. The caller stacks whatever puts it in a scope of its own.
    fn derived_computed_projection() -> LogicalPlanBuilder {
        LogicalPlanBuilder::scan(
            "t",
            table_source(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
            ]),
            None,
        )
        .expect("scan t")
        .project(vec![col("t.a") + col("t.b")])
        .expect("computed projection")
    }

    /// Regression test for the empty-projection fallback: a `Projection` with no
    /// output expressions must not unparse to an empty `SELECT` list.
    ///
    /// The shape arises on its own — `count(*)` planned over a view or subquery whose
    /// columns are all pruned leaves `Projection: <empty>` over the scan. Rendered
    /// literally that is `SELECT FROM t`, which `DuckDB` and others reject outright
    /// with a parser error, so the federated query fails rather than returning
    /// anything. The fork emits `SELECT 1` for dialects that refuse an empty list.
    ///
    /// This is deliberately not swept over every dialect. The fallback is keyed on
    /// `Dialect::supports_empty_select_list`, and a dialect that declares it accepts
    /// an empty list is entitled to keep one — `PostgreSqlDialect` does, and renders
    /// `SELECT FROM "t"`. The two below are the ones that refuse it, so they are the
    /// ones the fallback has to reach.
    #[test]
    fn an_empty_projection_does_not_unparse_to_an_empty_select_list() {
        let plan = LogicalPlanBuilder::scan(
            "t",
            table_source(vec![Field::new("a", DataType::Int32, false)]),
            None,
        )
        .expect("scan t")
        .project(Vec::<Expr>::new())
        .expect("empty projection")
        .build()
        .expect("build");

        let refuse_an_empty_list: Vec<(&str, Arc<dyn Dialect>)> = vec![
            ("default", Arc::new(DefaultDialect {})),
            ("duckdb", Arc::new(DuckDBDialect::new())),
        ];
        for (dialect_name, dialect) in refuse_an_empty_list {
            let sql = unparse_with(dialect_name, dialect.as_ref(), &plan);
            let select_list = &sql[..first_offset_of(&sql, "FROM")];
            assert_ne!(
                select_list.trim(),
                "SELECT",
                "{dialect_name}: an empty select list is not a statement this engine will parse: {sql}"
            );
        }
    }

    /// A projection casting `ts` to a timestamp carrying `tz`, which is the shape
    /// that reaches the dialect's `AT TIME ZONE` rendering.
    fn timestamp_cast_to_zone(tz: &str) -> LogicalPlan {
        LogicalPlanBuilder::scan(
            "t",
            table_source(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )]),
            None,
        )
        .expect("scan t")
        .project(vec![cast(
            col("t.ts"),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into())),
        )])
        .expect("timestamp cast")
        .build()
        .expect("build")
    }

    /// Regression test for the two `AT TIME ZONE` fixes the fork carries: #160 makes
    /// the unparser emit the timezone at all, and #195 keeps `DuckDB` from receiving a
    /// fixed *zero* offset it cannot resolve.
    ///
    /// Before #160 the timezone was dropped from the SQL entirely, so the remote
    /// engine evaluated the expression in its own session timezone and returned a
    /// different instant — silently, for every federated query touching a
    /// `timestamptz`. #195 is the other direction: Arrow carries a fixed UTC offset
    /// rather than an IANA name (Iceberg maps every `timestamptz` column to
    /// `+00:00`), `DuckDB` resolves `AT TIME ZONE` through an ICU zone-name lookup,
    /// and the result was a permanent `Unknown TimeZone '+00:00'`
    /// ([#12528](https://github.com/spiceai/spiceai/issues/12528)).
    ///
    /// A *non-zero* offset is deliberately still emitted verbatim, and still rejected
    /// by `DuckDB`: there is no safe total mapping from an offset to a zone name, and
    /// a clear error beats a possibly-wrong instant. The named-zone arm holds
    /// `DuckDB` to the faithful rendering so a later fix cannot suppress the zone
    /// wholesale.
    ///
    /// The sweep covers the dialects whose timestamp type carries a zone at all.
    /// `MySqlDialect` renders the cast as `DATETIME`, which has no zone to carry, so
    /// dropping it there is the dialect being right rather than the patch being lost.
    #[test]
    fn a_timezone_survives_unparsing_except_where_the_engine_cannot_resolve_it() {
        let named = "America/New_York";
        let zone_aware: Vec<(&str, Arc<dyn Dialect>)> = vec![
            ("default", Arc::new(DefaultDialect {})),
            ("postgres", Arc::new(PostgreSqlDialect {})),
        ];
        for (dialect_name, dialect) in zone_aware {
            let sql = unparse_with(
                dialect_name,
                dialect.as_ref(),
                &timestamp_cast_to_zone(named),
            );
            assert!(
                sql.contains(named),
                "{dialect_name}: the timezone was dropped, so the remote engine evaluates this in \
                 its own session timezone and returns a different instant: {sql}"
            );
        }

        let duckdb = DuckDBDialect::new();
        let utc_offset = unparse_with("duckdb", &duckdb, &timestamp_cast_to_zone("+00:00"));
        assert!(
            !utc_offset.contains("AT TIME ZONE"),
            "duckdb: a fixed zero offset reaches DuckDB's ICU zone-name lookup, which knows only \
             named zones, and fails permanently with `Unknown TimeZone '+00:00'`: {utc_offset}"
        );
        let named_zone = unparse_with("duckdb", &duckdb, &timestamp_cast_to_zone(named));
        assert!(
            named_zone.contains("AT TIME ZONE") && named_zone.contains(named),
            "duckdb: a named zone is resolvable and has to keep the faithful rendering, or the \
             instant changes: {named_zone}"
        );
    }

    /// Regression test for the `BigQuery` type spelling carried by fork PR #147:
    /// `BigQuery` names the 64-bit float type `FLOAT64` and rejects `DOUBLE`.
    ///
    /// A cast is the common way this reaches the wire — a federated predicate
    /// comparing a column to a float literal unparses through it — and the failure is
    /// the whole query, not a wrong row.
    #[test]
    fn bigquery_names_the_float_type_the_way_bigquery_does() {
        let plan = LogicalPlanBuilder::scan(
            "t",
            table_source(vec![Field::new("n", DataType::Int64, false)]),
            None,
        )
        .expect("scan t")
        .project(vec![cast(col("t.n"), DataType::Float64)])
        .expect("float cast")
        .build()
        .expect("build");

        let sql = unparse_with("bigquery", &BigQueryDialect::new(), &plan);
        assert!(
            sql.contains("FLOAT64"),
            "bigquery: BigQuery has no `DOUBLE` type, so this statement is rejected: {sql}"
        );
        assert!(
            !sql.contains("DOUBLE"),
            "bigquery: BigQuery has no `DOUBLE` type, so this statement is rejected: {sql}"
        );
    }

    /// A scan of `t` carrying one nanosecond timestamp column, which the `BigQuery`
    /// guards below filter or project over. `tz` is the column's Arrow timezone.
    fn timestamp_scan(tz: Option<&str>) -> LogicalPlanBuilder {
        LogicalPlanBuilder::scan(
            "t",
            table_source(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, tz.map(Into::into)),
                true,
            )]),
            None,
        )
        .expect("scan t")
    }

    /// Regression test for the `BigQuery` timestamp literal format carried by fork PR
    /// #144: the offset is attached directly to the time, with no space between them.
    ///
    /// `BigQuery` rejects the spaced form outright — `invalid timestamp: '2016-08-06
    /// 20:05:00 +00:00'` — so a federated predicate comparing a timestamp column to a
    /// literal takes the whole query down. That half holds for any zone.
    ///
    /// The offset itself is load-bearing only away from UTC. `BigQuery` reads a
    /// zone-less literal as UTC, so losing `+00:00` changes no instant, while losing
    /// `-04:00` moves the literal by four hours and the predicate silently selects a
    /// different range of rows. Both arms are here: UTC is the case fork PR #144
    /// reported, and the named zone is the one where dropping the offset is a wrong
    /// answer rather than a formatting change.
    ///
    /// The assertion is deliberately independent of which dialect layer produces the
    /// format: a `BigQueryDialect` override and the `Dialect` trait default are both
    /// able to supply it, so the guard pins the rendering `BigQuery` receives and a
    /// re-cut that changes either layer still has to keep it.
    #[test]
    fn bigquery_attaches_a_timestamp_offset_to_the_time() {
        for (tz, instant, offset, losing_the_offset) in [
            (
                "UTC",
                "20:05:00",
                "+00:00",
                "BigQuery reads a zone-less literal as UTC, so this arm pins the \
                 rendering rather than the instant",
            ),
            (
                "America/New_York",
                "16:05:00",
                "-04:00",
                "BigQuery reads a zone-less literal as UTC, so the literal moves by four \
                 hours and the predicate selects a different range of rows",
            ),
        ] {
            let plan = timestamp_scan(Some(tz))
                .filter(col("t.ts").gt(lit(ScalarValue::TimestampNanosecond(
                    Some(1_470_513_900_000_000_000),
                    Some(tz.into()),
                ))))
                .expect("filter")
                .project(vec![col("t.ts")])
                .expect("project")
                .build()
                .expect("build");

            let sql = unparse_with("bigquery", &BigQueryDialect::new(), &plan);
            assert!(
                sql.contains(&format!("{instant}{offset}")),
                "bigquery ({tz}): the offset has to reach BigQuery attached to the time \
                 — {losing_the_offset}: {sql}"
            );
            assert!(
                !sql.contains(&format!("{instant} {offset}")),
                "bigquery ({tz}): a space before the offset is rejected as an invalid \
                 timestamp, so the whole federated query fails: {sql}"
            );
        }
    }

    /// Regression test for the two `BigQueryDialect` overrides carried by fork PR #146.
    ///
    /// `date_field_extract_style` defaults to `DatePart`, which renders
    /// `date_part('YEAR', …)`, and `BigQuery` answers `Function not found: date_part`.
    /// `interval_style` defaults to `PostgresVerbose`, which renders `INTERVAL '3 MONS'`,
    /// and `BigQuery` answers `Syntax error: Unexpected ")"`. Between them they took out
    /// TPC-H Q4, Q7, Q8, Q9 and Q20.
    ///
    /// Each half also renders its plan through `DefaultDialect`, which still carries both
    /// defaults. Those arms are what keep the `BigQuery` assertions honest: if a re-cut
    /// changes the defaults, these plan shapes stop reaching the overrides, and without
    /// the contrast the guard would keep passing while checking nothing.
    #[test]
    fn bigquery_extracts_date_fields_and_spells_intervals_the_standard_way() {
        let extract = timestamp_scan(None)
            .project(vec![date_part(lit("YEAR"), col("t.ts"))])
            .expect("date_part projection")
            .build()
            .expect("build");

        let sql = unparse_with("bigquery", &BigQueryDialect::new(), &extract);
        assert!(
            sql.contains("EXTRACT(YEAR FROM"),
            "bigquery: BigQuery has no `date_part` function, so this statement is \
             rejected: {sql}"
        );
        assert!(
            !sql.contains("date_part"),
            "bigquery: BigQuery has no `date_part` function, so this statement is \
             rejected: {sql}"
        );
        let default_extract = unparse_with("default", &DefaultDialect {}, &extract);
        assert!(
            default_extract.contains("date_part"),
            "the default dialect no longer renders `date_part`, so this plan no longer \
             reaches the extract-style override and the BigQuery arm above proves \
             nothing: {default_extract}"
        );

        let interval = timestamp_scan(None)
            .project(vec![
                col("t.ts")
                    + lit(ScalarValue::IntervalMonthDayNano(Some(
                        IntervalMonthDayNano::new(3, 0, 0),
                    ))),
            ])
            .expect("interval projection")
            .build()
            .expect("build");

        let sql = unparse_with("bigquery", &BigQueryDialect::new(), &interval);
        assert!(
            sql.contains("INTERVAL '3' MONTH"),
            "bigquery: BigQuery parses only SQL-standard intervals, so this statement is \
             rejected: {sql}"
        );
        assert!(
            !sql.contains("MONS"),
            "bigquery: `MONS` is PostgreSQL's verbose interval spelling, which BigQuery \
             does not parse: {sql}"
        );
        let default_interval = unparse_with("default", &DefaultDialect {}, &interval);
        assert!(
            default_interval.contains("MONS"),
            "the default dialect no longer renders a verbose interval, so this plan no \
             longer reaches the interval-style override and the BigQuery arm above proves \
             nothing: {default_interval}"
        );
    }

    /// Regression test for `supports_column_alias_in_table_alias` carried by fork PR
    /// #148: a derived table's column aliases are inlined into its own projection rather
    /// than listed on the table alias.
    ///
    /// `BigQuery` does not parse a column alias list on a table alias — `Expected ")" but
    /// got "("` — so the whole federated query fails. Inlining has to put the name
    /// somewhere, so the guard also holds the alias to the derived table's projection: an
    /// inlining that dropped the name would leave the outer query with no `key` to bind.
    ///
    /// The `PostgreSQL` arm renders the same plan *with* the alias list. That is the
    /// dialect being right, and it is also the proof that this plan shape still produces
    /// a column alias list at all — without it the `BigQuery` assertions would pass on a
    /// plan that never had one to inline.
    #[test]
    fn bigquery_inlines_a_derived_tables_column_aliases() {
        let plan = LogicalPlanBuilder::scan(
            "orders",
            table_source(vec![Field::new("o_orderkey", DataType::Int64, false)]),
            None,
        )
        .expect("scan orders")
        .project(vec![col("orders.o_orderkey")])
        .expect("inner projection")
        .project(vec![col("orders.o_orderkey").alias("key")])
        .expect("renaming projection")
        .alias("c")
        .expect("subquery alias")
        .project(vec![col("c.key")])
        .expect("outer projection")
        .build()
        .expect("build");

        let sql = unparse_with("bigquery", &BigQueryDialect::new(), &plan);
        assert!(
            sql.trim_end().ends_with("AS `c`"),
            "bigquery: BigQuery does not parse a column alias list on a table alias, so \
             this statement is rejected: {sql}"
        );
        assert!(
            sql.contains("AS `key`"),
            "bigquery: the column alias has to move into the derived table's projection, \
             or the outer query has no `key` column to bind: {sql}"
        );

        let postgres = unparse_with("postgres", &PostgreSqlDialect {}, &plan);
        assert!(
            postgres.contains("(key)"),
            "this plan no longer unparses to a column alias list on any dialect, so there \
             is nothing for the BigQuery arms above to have inlined: {postgres}"
        );
    }

    /// Regression test for the `date_trunc` rewrite carried by fork PR #169: `BigQuery`
    /// spells it `TIMESTAMP_TRUNC(value, PART)` — the value first, and the part a bare
    /// keyword rather than a quoted string.
    ///
    /// The `DefaultDialect` arm holds the same plan to `date_trunc`, so a re-cut that
    /// changes the default cannot leave this guard passing without checking anything.
    #[test]
    fn bigquery_truncates_a_timestamp_the_way_bigquery_does() {
        for (granularity, part, consequence) in [
            (
                "month",
                "MONTH",
                "BigQuery has no `date_trunc` function, so the statement is rejected",
            ),
            (
                "week",
                "ISOWEEK",
                "DataFusion truncates a week to Monday and BigQuery's bare `WEEK` is \
                 Sunday-based, so any other part starts the week on the wrong day — one \
                 day out for a Monday-to-Saturday timestamp, six for a Sunday one — and \
                 returns wrong rows with no error",
            ),
        ] {
            let plan = timestamp_scan(Some("UTC"))
                .project(vec![date_trunc(lit(granularity), col("t.ts"))])
                .expect("date_trunc projection")
                .build()
                .expect("build");

            let sql = unparse_with("bigquery", &BigQueryDialect::new(), &plan);
            assert!(
                sql.contains(&format!("TIMESTAMP_TRUNC(`t`.`ts`, {part})")),
                "bigquery: `date_trunc('{granularity}', …)` has to reach BigQuery as \
                 `TIMESTAMP_TRUNC` with the value first and `{part}` as a bare keyword — \
                 {consequence}: {sql}"
            );
            assert!(
                !sql.contains("date_trunc"),
                "bigquery: BigQuery has no `date_trunc` function, so this statement is \
                 rejected: {sql}"
            );

            let default_sql = unparse_with("default", &DefaultDialect {}, &plan);
            assert!(
                default_sql.contains("date_trunc"),
                "the default dialect no longer renders `date_trunc`, so this plan no \
                 longer reaches the BigQuery rewrite and the arm above proves nothing: \
                 {default_sql}"
            );
        }
    }
}
