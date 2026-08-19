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
    use datafusion::functions_aggregate::expr_fn::count;
    use datafusion::logical_expr::{
        ColumnarValue, Expr, Extension, JoinType, LogicalPlan, LogicalPlanBuilder, ScalarUDF,
        TableSource, Volatility, builder::LogicalTableSource, create_udf, expr::ScalarFunction,
    };
    use datafusion::prelude::{col, lit};
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

    /// The upstream fixes these guard live in the `spiceai/datafusion` fork on
    /// `spiceai-54`, so nothing here fails if a later pin bump drops them. The
    /// fork's branch is re-cut per `DataFusion` major and takes its own tests with
    /// it; these stay. Extend them whenever a pin bump carries another unparser
    /// fix.
    ///
    /// This unparses through the federation executor, whose dialect a connector
    /// supplies; the one built here supplies none, which resolves to the default
    /// dialect. [`federation_dialects`] sweeps the rest.
    /// The plan shapes, not the spelling, are what these assert.
    fn federated_sql(plan: &LogicalPlan) -> String {
        unparse_with(
            "federation executor",
            test_executor().dialect().as_ref(),
            plan,
        )
    }

    /// Assert `first` is rendered before `second`, which pins the clause order
    /// without pinning the formatting around it.
    fn assert_precedes(sql: &str, first: &str, second: &str) {
        assert!(
            first_offset_of(sql, first) < first_offset_of(sql, second),
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
    fn sorted_between_projections(sort_key: Expr) -> LogicalPlan {
        LogicalPlanBuilder::scan(
            "person",
            table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("age", DataType::Int32, true),
            ]),
            None,
        )
        .expect("scan person")
        .project(vec![col("person.id"), col("person.age")])
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
        for (key_kind, sort_key, rendered_key) in [
            ("bare-column", col("person.age"), "age"),
            ("computed", col("person.age") + lit(1), "+ 1"),
        ] {
            let plan = sorted_between_projections(sort_key);
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

    /// A semi join whose build side carries `fetch`, and the same join without it.
    /// Only the bound differs, so a guard can assert what the bound changes rather
    /// than restating the whole rendering.
    fn semi_join_with_build_side_fetch(fetch: Option<usize>) -> LogicalPlan {
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
        let build = LogicalPlanBuilder::scan_with_filters_fetch(
            "build",
            table_source(vec![Field::new("c", DataType::Utf8, false)]),
            None,
            Vec::<Expr>::new(),
            fetch,
        )
        .expect("scan build")
        .build()
        .expect("build build");

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
}
