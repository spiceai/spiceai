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
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{
        ColumnarValue, Expr, Extension, JoinType, LogicalPlan, LogicalPlanBuilder, ScalarUDF,
        TableSource, Volatility, builder::LogicalTableSource, cast, create_udf,
        expr::ScalarFunction,
    };
    use datafusion::prelude::{col, lit};
    use datafusion::sql::unparser::Unparser;
    use datafusion::sql::unparser::dialect::{
        BigQueryDialect, DefaultDialect, DuckDBDialect, PostgreSqlDialect,
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
    /// fix — #13081 tracks the three this bump left unguarded.
    ///
    /// This unparses through the federation executor, which supplies no dialect
    /// here, so the SQL is the default dialect's rather than any one connector's.
    /// The plan shapes, not the spelling, are what these assert.
    fn federated_sql(plan: &LogicalPlan) -> String {
        Unparser::new(test_executor().dialect().as_ref())
            .plan_to_sql(plan)
            .expect("plan should unparse")
            .to_string()
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
}
