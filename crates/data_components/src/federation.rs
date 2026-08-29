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
    /// The upstream fixes these guard live in the `spiceai/datafusion` fork on
    /// `spiceai-54`, so nothing here fails if a later pin bump drops them. The
    /// fork's branch is re-cut per `DataFusion` major and takes its own tests with
    /// it; these stay. Extend them whenever a pin bump carries another unparser
    /// fix — #13081 tracks the three the `edd8861e` → `b5cb7bb3` bump left
    /// unguarded, and the two below arrived with `b5cb7bb3` → `8e881090`.
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

    /// Regression test for #13277: the sibling shape, where the correlation's
    /// only qualifier is one the probe side also answers to. Naming the scope
    /// anything else rebinds those references to the probe, turning the
    /// correlation into a comparison of the outer row with itself.
    ///
    /// Both readings are wrong — the subquery's own `FROM` already shadows the
    /// outer relation, so the correlation is lost whatever the bound does — and
    /// that unscoped output binds and runs. The `EXISTS` then reduces to "this
    /// table has a row", so the semi join this builds keeps every probe row,
    /// including the rows that match nothing (an anti join over the same SQL
    /// drops every row instead). Emitting these correctly needs the
    /// correlation's qualifiers rewritten to the derived scope, tracked by
    /// #12840.
    #[test]
    fn a_bounded_exists_refuses_a_correlation_qualified_by_the_probe() {
        let err = federated_sql_result(&a_correlation_qualified_by_the_probe(Some(5)))
            .expect_err("a correlation qualified by the probe side must be refused");
        assert!(
            err.to_string().contains(
                "not supported when the correlation's only qualifier is one the probe side also answers to"
            ),
            "expected the refusal to name the probe-qualified correlation, got: {err}"
        );

        // The unbounded sibling is deliberately left unpinned. Unlike the
        // multi-relation shape, its rendering is *itself* wrong — the inner
        // `FROM` shadows the outer relation, so the `EXISTS` reduces to "this
        // table has a row" with or without a bound — so asserting that it still
        // unparses would pin a defect and stand in the way of #12840's rewrite,
        // which should be free to refuse this shape at any bound.
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
    /// ORDER BY. The hoist used to be gated on the sort key *being* one of the inner
    /// projection's outputs, so a key computed from one bailed out and the ORDER BY
    /// was emitted inside a derived table.
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
    /// already folded into the select list needs a scope of its own. It used to be
    /// skipped instead, and its GROUP BY never reached the emitted SQL.
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
    /// a row bound on that side used to be emitted beside the correlation predicate.
    /// SQL applies the bound after the `WHERE`, so it chose among the rows the
    /// correlation had already matched instead of choosing which rows the correlation
    /// could see, and the subquery searched the whole relation.
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
    /// literal takes the whole query down rather than returning a wrong row. Both
    /// assertions pin the same boundary, so a rendering that keeps the offset but
    /// re-introduces the space fails, and so does one that drops the offset entirely
    /// and leaves `BigQuery` to read the literal in its own time zone.
    ///
    /// The pin satisfies this through the `Dialect` trait default, not a
    /// `BigQueryDialect` override — the override PR #144 added is no longer on the
    /// branch. That is why this guard asserts the rendering `BigQuery` receives rather
    /// than the presence of an override: either layer may supply it, and a re-cut that
    /// changes either one still has to keep the format.
    #[test]
    fn bigquery_attaches_a_timestamp_offset_to_the_time() {
        let plan = timestamp_scan(Some("UTC"))
            .filter(col("t.ts").gt(lit(ScalarValue::TimestampNanosecond(
                Some(1_470_513_900_000_000_000),
                Some("UTC".into()),
            ))))
            .expect("filter")
            .project(vec![col("t.ts")])
            .expect("project")
            .build()
            .expect("build");

        let sql = unparse_with("bigquery", &BigQueryDialect::new(), &plan);
        assert!(
            sql.contains("20:05:00+00:00"),
            "bigquery: the offset has to reach BigQuery attached to the time, or the \
             literal is read in BigQuery's own time zone and the predicate selects a \
             different range of rows: {sql}"
        );
        assert!(
            !sql.contains("20:05:00 +00:00"),
            "bigquery: a space before the offset is rejected as an invalid timestamp, so \
             the whole federated query fails: {sql}"
        );
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
                 Sunday-based, so any other part shifts every truncated week by a day and \
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
