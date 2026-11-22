/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Keep expressions whose semantics differ from SQL local to Spice.

use std::sync::Arc;

use async_trait::async_trait;
use clickhouse_rs::ClientHandle;
use data_components::function_support::{FunctionSupport, contains_unsupported_functions};
use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    common::{
        DFSchema, NullEquality, Result, ScalarValue, Statistics, TableReference,
        tree_node::{TreeNode, TreeNodeRecursion},
    },
    datasource::TableProvider,
    logical_expr::{
        Expr, ExprSchemable, JoinType, LogicalPlan, Operator, WindowFunctionDefinition,
    },
    optimizer::OptimizerRule,
    physical_plan::{PhysicalExpr, SendableRecordBatchStream, metrics::MetricsSet},
    sql::unparser::dialect::Dialect,
};
use datafusion_federation::{
    FederatedQueryType, FederatedTableProviderAdaptor,
    sql::{
        AstAnalyzer, LogicalOptimizer, RemotePlanNode, RemoteTableRef, SQLExecutor,
        SQLFederationProvider, SQLFilterPushDown, SQLTableSource,
    },
};
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
use runtime_udfs_api::FunctionSupportBuilder;

use crate::dialect::ClickHouseDialect;

type ClickhouseSqlTable = SqlTable<ClientHandle, &'static dyn Sync>;

pub(super) fn create_federated_provider(
    table: ClickhouseSqlTable,
    table_reference: TableReference,
) -> FederatedTableProviderAdaptor {
    let table = Arc::new(
        table
            .with_function_support(Some(function_support()))
            .with_dialect(Arc::new(ClickHouseDialect::new())),
    );
    let executor = Arc::new(ClickhouseExecutor {
        table: Arc::clone(&table),
    });
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let source = Arc::new(SQLTableSource::new_with_schema(
        provider,
        RemoteTableRef::from(table_reference),
        table.schema(),
    ));
    FederatedTableProviderAdaptor::new_with_provider(source, table)
}

/// How an expression check treats the subquery of an `= ANY`/`ALL`.
#[derive(Clone, Copy)]
enum SetComparisonSubquery {
    Check,
    /// The plan walk that runs this check visits the subquery itself.
    Walked,
}

fn function_support() -> FunctionSupport {
    function_support_for(SetComparisonSubquery::Check)
}

fn function_support_for(subquery: SetComparisonSubquery) -> FunctionSupport {
    FunctionSupportBuilder::new()
        // ClickHouse uses byte offsets and ASCII case conversion, and has no
        // `btrim`; its date-part functions also differ in names and field
        // conventions.
        .deny_also(
            [
                "upper",
                "lower",
                "initcap",
                "translate",
                "ascii",
                "substr",
                "substring",
                "overlay",
                "left",
                "right",
                "lpad",
                "rpad",
                "reverse",
                "btrim",
                "length",
                "character_length",
                "char_length",
                "date_part",
                "date_trunc",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expression(Arc::new(move |expr: &Expr, schema: Option<&DFSchema>| {
            supports_expression_for(expr, schema, subquery)
        }))
        .build()
}

#[cfg(test)]
fn supports_expression(expr: &Expr, schema: Option<&DFSchema>) -> bool {
    supports_expression_for(expr, schema, SetComparisonSubquery::Check)
}

fn supports_expression_for(
    expr: &Expr,
    schema: Option<&DFSchema>,
    subquery: SetComparisonSubquery,
) -> bool {
    let scope = schema.unwrap_or_else(|| DFSchema::empty_ref());
    // A local parent can still federate its input projection. Check every
    // computed output, including folded literals and CASE expressions, against
    // the native driver's decimal decoding limit.
    if !matches!(expr, Expr::Column(_)) {
        let Ok(data_type) = expr.get_type(scope) else {
            return false;
        };
        if matches!(
            data_type,
            DataType::Decimal32(precision, _)
                | DataType::Decimal64(precision, _)
                | DataType::Decimal128(precision, _)
                | DataType::Decimal256(precision, _) if precision > 18
        ) {
            return false;
        }
    }
    match expr {
        // ClickHouse 24.8 does not accept SQL truth-value tests. Its IN list
        // semantics ignore a NULL item rather than yielding UNKNOWN.
        Expr::IsTrue(_)
        | Expr::IsNotTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsNotFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotUnknown(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::TryCast(_) => false,
        // `ClickHouse` reads `\` as an escape character inside a quoted name,
        // and the unparser writes a name with only its quote doubled.
        Expr::Column(column) => {
            !column.name.contains('\\')
                && column
                    .relation
                    .as_ref()
                    .is_none_or(|relation| !relation.to_string().contains('\\'))
        }
        Expr::Lambda(lambda) => !lambda.params.iter().any(|param| param.contains('\\')),
        Expr::LambdaVariable(variable) => !variable.name.contains('\\'),
        Expr::ScalarFunction(call) if matches!(call.func.name(), "get_field" | "named_struct") => {
            !call.args.iter().any(
                |arg| matches!(arg, Expr::Literal(value, _) if value.to_string().contains('\\')),
            )
        }
        // The unparser writes the escape character as it is.
        Expr::Like(like) => match like.escape_char {
            None => pattern_reads_the_same(&like.pattern),
            Some('\\') => false,
            Some(_) => true,
        },
        Expr::ScalarFunction(call) if matches!(call.func.name(), "like" | "ilike") => {
            call.args.get(1).is_some_and(pattern_reads_the_same)
        }
        // `ClickHouse` trims each byte of a character set, which can split a
        // multi-byte character. No byte of a multi-byte character is ASCII, so
        // a set of ASCII characters trims the same characters.
        Expr::ScalarFunction(call) if matches!(call.func.name(), "ltrim" | "rtrim") => {
            match call.args.as_slice() {
                [_] => true,
                [_, Expr::Literal(set, _)] => set
                    .try_as_str()
                    .flatten()
                    .is_some_and(|set| !set.is_empty() && set.is_ascii()),
                _ => false,
            }
        }
        // A filter can be pushed into the table scan, whose SQL holds the
        // subquery as it is, so the subquery gets the name and expression
        // checks of a plan. That walk also visits nested subqueries, so their
        // own set comparisons are not checked again.
        Expr::SetComparison(set_comparison) => match subquery {
            SetComparisonSubquery::Check => {
                let plan = &set_comparison.subquery.subquery;
                !plan_names_a_backslash(plan)
                    && contains_unsupported_functions(
                        plan,
                        &function_support_for(SetComparisonSubquery::Walked),
                    )
                    .is_ok_and(|unsupported| !unsupported)
            }
            SetComparisonSubquery::Walked => true,
        },
        Expr::InList(list) => {
            non_decimal(&list.expr, scope)
                && list.list.iter().all(|item| {
                    non_decimal(item, scope) && item.nullable(scope).is_ok_and(|nullable| !nullable)
                })
        }
        Expr::Between(between) => [&between.expr, &between.low, &between.high]
            .iter()
            .all(|arg| non_decimal(arg, scope)),
        // Native decimal SUM widens to a precision the driver cannot decode;
        // decimal AVG returns a rounded Float64, and the standard deviation and
        // variance answer `nan` for NULL. Windows have the same result types as
        // their corresponding ordinary aggregate.
        Expr::AggregateFunction(aggregate) => {
            aggregate_args_supported(aggregate.func.name(), &aggregate.params.args, scope)
        }
        Expr::WindowFunction(window) => match &window.fun {
            WindowFunctionDefinition::AggregateUDF(aggregate) => {
                aggregate_args_supported(aggregate.name(), &window.params.args, scope)
            }
            WindowFunctionDefinition::WindowUDF(_) => true,
        },
        // Decimal arithmetic and casts need DataFusion's precision, scale and
        // overflow behavior, even when only a later aggregate stays local.
        // A SQL CAST target has no Nullable wrapper in this dialect, so a
        // nullable operand must remain local to preserve NULLs.
        // The unparser writes the zone of `AT TIME ZONE` as it is.
        Expr::Cast(cast) => {
            !cast.field.data_type().is_decimal()
                && !matches!(cast.field.data_type(), DataType::Timestamp(_, Some(zone)) if zone.contains('\\'))
                && non_decimal(&cast.expr, scope)
                && cast.expr.nullable(scope).is_ok_and(|nullable| !nullable)
        }
        Expr::BinaryExpr(binary) => {
            [&binary.left, &binary.right]
                .iter()
                .all(|arg| non_decimal(arg, scope))
                && (binary.op != Operator::Modulo || modulo_divisor_is_renderable(&binary.right))
        }
        _ => true,
    }
}

/// `ClickHouse` 24.8 gives `%` by a negative constant the divisor's sign:
/// `a % -2` answers `1` for `a = -33`, where SQL answers `-1`. A column divisor,
/// whatever its sign, answers correctly.
fn modulo_divisor_is_renderable(divisor: &Expr) -> bool {
    match divisor {
        Expr::Literal(value, _) => !value.is_null() && !is_negative(value),
        divisor => !divisor.column_refs().is_empty(),
    }
}

/// A value that cannot be compared with zero counts as negative.
fn is_negative(value: &ScalarValue) -> bool {
    ScalarValue::new_zero(&value.data_type())
        .ok()
        .and_then(|zero| value.partial_cmp(&zero))
        .is_none_or(std::cmp::Ordering::is_lt)
}

fn non_decimal(expr: &Expr, scope: &DFSchema) -> bool {
    expr.get_type(scope)
        .is_ok_and(|data_type| !data_type.is_decimal())
}

fn aggregate_args_supported(name: &str, args: &[Expr], scope: &DFSchema) -> bool {
    // ClickHouse answers `nan` where DataFusion answers NULL: for the sample
    // standard deviation and variance of one row, and for all four over no rows
    // of a non-`Nullable` column.
    if matches!(name, "stddev" | "stddev_pop" | "var" | "var_pop") {
        return false;
    }
    if !name.eq_ignore_ascii_case("sum") && !name.eq_ignore_ascii_case("avg") {
        return true;
    }
    args.iter().all(|arg| {
        arg.get_type(scope)
            .is_ok_and(|data_type| !data_type.is_decimal())
    })
}

/// Whether `ClickHouse` reads this `LIKE` pattern as `DataFusion` does: string
/// literals, joined with `||`, in each of which every `\` comes before `%`, `_`
/// or `\`. `ClickHouse` reads any other `\` as itself and rejects a pattern
/// that ends in `\`.
fn pattern_reads_the_same(pattern: &Expr) -> bool {
    match pattern {
        Expr::Literal(value, _) => value.try_as_str().is_some_and(|text| {
            let mut chars = text.unwrap_or_default().chars();
            while let Some(c) = chars.next() {
                if c == '\\' && !matches!(chars.next(), Some('%' | '_' | '\\')) {
                    return false;
                }
            }
            true
        }),
        Expr::Cast(cast) => pattern_reads_the_same(&cast.expr),
        Expr::BinaryExpr(binary) if binary.op == Operator::StringConcat => {
            pattern_reads_the_same(&binary.left) && pattern_reads_the_same(&binary.right)
        }
        _ => false,
    }
}

/// Whether a name the unparser writes for a node of this plan or of a subquery
/// in it holds `\`, which `ClickHouse` reads as an escape character inside a
/// quoted name. A node's output schema holds its column aliases and subquery
/// qualifiers.
fn plan_names_a_backslash(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let walked = plan.apply_with_subqueries(|node| {
        found = node.schema().iter().any(|(qualifier, field)| {
            field.name().contains('\\')
                || qualifier.is_some_and(|qualifier| qualifier.to_string().contains('\\'))
        });
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    walked.is_err() || found
}

struct ClickhouseExecutor {
    table: Arc<ClickhouseSqlTable>,
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl SQLExecutor for ClickhouseExecutor {
    fn name(&self) -> &str {
        self.table.name()
    }
    fn compute_context(&self) -> Option<String> {
        self.table.compute_context()
    }
    fn dialect(&self) -> Arc<dyn Dialect> {
        self.table.dialect()
    }
    fn can_execute_plan(&self, plan: &LogicalPlan) -> bool {
        // The walk visits each `= ANY`/`ALL` subquery itself, so the expression
        // check does not check it again.
        let supported = !contains_unsupported_functions(
            plan,
            &function_support_for(SetComparisonSubquery::Walked),
        )
        .unwrap_or(true);
        supported && !plan.exists(|node| {
            Ok(matches!(node, LogicalPlan::Join(join) if
                !matches!(join.join_type, JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full)
                || join.filter.is_some()
                || join.null_equality == NullEquality::NullEqualsNull))
        }).unwrap_or(true) && !plan_names_a_backslash(plan)
    }
    fn pre_federation_optimizer_rules(&self) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
        self.table.pre_federation_optimizer_rules()
    }
    fn logical_optimizer(&self) -> Option<LogicalOptimizer> {
        self.table.logical_optimizer()
    }
    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        self.table.ast_analyzer()
    }
    fn supports_filters_pushdown(&self, filters: &[&dyn PhysicalExpr]) -> Vec<SQLFilterPushDown> {
        SQLExecutor::supports_filters_pushdown(self.table.as_ref(), filters)
    }
    fn supports_sort_pushdown(&self) -> bool {
        self.table.supports_sort_pushdown()
    }
    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> Result<SendableRecordBatchStream> {
        self.table.execute(query, schema, filters)
    }
    async fn statistics(&self, plan: &LogicalPlan) -> Result<Statistics> {
        SQLExecutor::statistics(self.table.as_ref(), plan).await
    }
    async fn explain_plan(
        &self,
        query: &str,
        query_type: FederatedQueryType,
    ) -> Result<Option<RemotePlanNode>> {
        self.table.explain_plan(query, query_type).await
    }
    async fn table_names(&self) -> Result<Vec<String>> {
        self.table.table_names().await
    }
    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        self.table.get_table_schema(table_name).await
    }
    fn metrics(&self) -> Option<MetricsSet> {
        self.table.metrics()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use clickhouse_rs::Options;
    use datafusion::arrow::datatypes::{Field, Fields, Schema, TimeUnit};
    use datafusion::common::{Column, Spans};
    use datafusion::error::DataFusionError;
    use datafusion::functions::core::expr_fn::{get_field, named_struct};
    use datafusion::logical_expr::expr::{Lambda, LambdaVariable, SetComparison, SetQuantifier};
    use datafusion::logical_expr::{
        Cast, ColumnarValue, Like, LogicalPlanBuilder, Subquery, TableProviderFilterPushDown,
        Volatility, binary_expr, col, create_udf, exists, lit, table_scan,
    };
    use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;

    use super::*;
    use crate::pool::ClickhouseConnectionPool;

    fn executor(schema: Schema) -> Result<ClickhouseExecutor> {
        let options = Options::from_str("tcp://127.0.0.1:9000")
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let pool: Arc<dyn DbConnectionPool<ClientHandle, &'static dyn Sync> + Send + Sync> =
            Arc::new(ClickhouseConnectionPool::new(
                options,
                "default".into(),
                String::new(),
            ));
        #[expect(
            clippy::disallowed_methods,
            reason = "installs the ClickHouse expression policy with `with_function_support` below"
        )]
        let table = SqlTable::new_with_schema("clickhouse", &pool, schema, "t", None)
            .with_function_support(Some(function_support()))
            .with_dialect(Arc::new(ClickHouseDialect::new()));
        Ok(ClickhouseExecutor {
            table: Arc::new(table),
        })
    }

    fn like(pattern: Expr, escape_char: Option<char>) -> Expr {
        Expr::Like(Like::new(
            false,
            Box::new(col("s")),
            Box::new(pattern),
            escape_char,
            false,
        ))
    }

    /// A two-argument string function with this name, such as Spark's `like`.
    fn call(name: &str, pattern: &str) -> Expr {
        create_udf(
            name,
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Boolean,
            Volatility::Immutable,
            Arc::new(|_: &[ColumnarValue]| -> Result<ColumnarValue> {
                datafusion::common::not_impl_err!("test only")
            }),
        )
        .call(vec![col("s"), lit(pattern)])
    }

    fn at_zone(zone: &str) -> Expr {
        Expr::Cast(Cast::new(
            Box::new(lit(ScalarValue::TimestampMicrosecond(Some(0), None))),
            DataType::Timestamp(TimeUnit::Microsecond, Some(zone.into())),
        ))
    }

    #[test]
    fn an_expression_clickhouse_would_misread_stays_local() -> Result<()> {
        let fields = Fields::from(vec![
            Field::new("ab", DataType::Utf8, true),
            Field::new("a\\b", DataType::Utf8, true),
        ]);
        let scope = DFSchema::try_from(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("st", DataType::Struct(fields), true),
        ]))?;
        let supports = |expr: &Expr| supports_expression(expr, Some(&scope));
        let concat =
            |left: &str, right: &str| binary_expr(lit(left), Operator::StringConcat, lit(right));
        assert!(!supports(&col("a\\b")));
        assert!(!supports(&Expr::Column(Column::new(Some("q\\"), "s"))));
        assert!(!supports(&Expr::Lambda(Lambda::new(
            vec!["v\\".into()],
            lit(1)
        ))));
        assert!(!supports(&Expr::LambdaVariable(LambdaVariable::new(
            "v\\".into(),
            None
        ))));
        assert!(!supports(&get_field(col("st"), "a\\b")));
        assert!(!supports(&named_struct(vec![lit("a\\b"), lit(1)])));
        assert!(!supports(&like(lit("a!%"), Some('\\'))));
        assert!(!supports(&like(lit("C:\\new%"), None)));
        assert!(!supports(&like(lit("trail\\"), None)));
        assert!(!supports(&like(col("s"), None)));
        assert!(!supports(&like(concat("a", "\\b"), None)));
        assert!(!supports(&call("like", "C:\\new%")));
        assert!(!supports(&call("ilike", "trail\\")));
        assert!(!supports(&at_zone("a\\b")));

        assert!(supports(&col("ab")));
        assert!(supports(&Expr::Column(Column::new(Some("q"), "s"))));
        assert!(supports(&Expr::Lambda(Lambda::new(
            vec!["v".into()],
            lit(1)
        ))));
        assert!(supports(&Expr::LambdaVariable(LambdaVariable::new(
            "v".into(),
            None
        ))));
        assert!(supports(&get_field(col("st"), "ab")));
        assert!(supports(&named_struct(vec![lit("ab"), lit(1)])));
        assert!(supports(&like(lit("a\\%\\_\\\\%"), None)));
        assert!(supports(&like(lit("a!%"), Some('!'))));
        assert!(supports(&like(concat("%", "a\\%"), None)));
        assert!(supports(&call("like", "a\\%%")));
        assert!(supports(&call("ilike", "%a%")));
        assert!(supports(&like(
            Expr::Cast(Cast::new(Box::new(lit("a%")), DataType::Utf8View)),
            None
        )));
        assert!(supports(&at_zone("UTC")));
        Ok(())
    }

    #[test]
    fn a_plan_that_names_a_backslash_stays_local() -> Result<()> {
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, true)]);
        let executor = executor(schema.clone())?;
        let scan = table_scan(Some("t"), &schema, None)?.build()?;

        let aliased = LogicalPlanBuilder::from(scan.clone())
            .project(vec![col("s").alias("x\\")])?
            .project(vec![lit(1).alias("y")])?
            .build()?;
        assert!(!executor.can_execute_plan(&aliased));
        let in_subquery = LogicalPlanBuilder::from(scan.clone())
            .filter(exists(Arc::new(aliased)))?
            .build()?;
        assert!(plan_names_a_backslash(&in_subquery));
        let subquery = LogicalPlanBuilder::from(scan.clone())
            .alias("q\\")?
            .build()?;
        assert!(!executor.can_execute_plan(&subquery));

        let plain = LogicalPlanBuilder::from(scan)
            .project(vec![col("s").alias("x")])?
            .alias("q")?
            .build()?;
        assert!(executor.can_execute_plan(&plain));
        Ok(())
    }

    #[test]
    fn a_string_function_clickhouse_counts_in_bytes_stays_local() -> Result<()> {
        let scope = DFSchema::try_from(Schema::new(vec![Field::new("s", DataType::Utf8, true)]))?;
        let support = function_support();
        let string_call = |name: &str, args: Vec<Expr>| {
            create_udf(
                name,
                vec![DataType::Utf8; args.len()],
                DataType::Utf8,
                Volatility::Immutable,
                Arc::new(|_: &[ColumnarValue]| -> Result<ColumnarValue> {
                    datafusion::common::not_impl_err!("test only")
                }),
            )
            .call(args)
        };
        for name in [
            "initcap",
            "translate",
            "ascii",
            "overlay",
            "left",
            "right",
            "lpad",
            "rpad",
            "reverse",
            "btrim",
        ] {
            assert!(
                !support.supports(&string_call(name, vec![col("s")]), Some(&scope)),
                "{name}"
            );
        }
        for name in ["ltrim", "rtrim"] {
            assert!(
                support.supports(&string_call(name, vec![col("s")]), Some(&scope)),
                "{name}"
            );
            assert!(
                support.supports(&string_call(name, vec![col("s"), lit("0 ")]), Some(&scope)),
                "{name}"
            );
            for set in [lit("é"), lit(""), lit(ScalarValue::Utf8(None)), col("s")] {
                assert!(
                    !support.supports(&string_call(name, vec![col("s"), set]), Some(&scope)),
                    "{name}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn an_any_filter_with_a_refused_subquery_stays_above_the_scan() -> Result<()> {
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, true)]);
        let executor = executor(schema.clone())?;
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        let any = |subquery: LogicalPlan| {
            Expr::SetComparison(SetComparison::new(
                Box::new(lit("a")),
                Subquery {
                    subquery: Arc::new(subquery),
                    outer_ref_columns: vec![],
                    spans: Spans::new(),
                },
                Operator::Eq,
                SetQuantifier::Any,
            ))
        };
        let aliased = LogicalPlanBuilder::from(scan.clone())
            .project(vec![col("s").alias("x\\")])?
            .build()?;
        let refused = LogicalPlanBuilder::from(scan.clone())
            .filter(like(lit("C:\\new%"), None))?
            .project(vec![col("s")])?
            .build()?;
        let plain = LogicalPlanBuilder::from(scan.clone())
            .project(vec![col("s")])?
            .build()?;
        let nested = |subquery: LogicalPlan| {
            LogicalPlanBuilder::from(scan.clone())
                .filter(any(subquery))?
                .project(vec![col("s")])?
                .build()
        };
        let nested_aliased = nested(aliased.clone())?;
        let nested_plain = nested(plain.clone())?;

        let nested_refused = nested(refused.clone())?;

        let filtered = |predicate: Expr| {
            LogicalPlanBuilder::from(scan.clone())
                .filter(predicate)?
                .build()
        };
        assert!(!executor.can_execute_plan(&filtered(like(lit("C:\\new%"), None))?));
        assert!(!executor.can_execute_plan(&filtered(any(nested_refused.clone()))?));
        assert!(executor.can_execute_plan(&filtered(any(nested_plain.clone()))?));

        for subquery in [aliased, refused, nested_aliased, nested_refused] {
            assert_eq!(
                TableProvider::supports_filters_pushdown(
                    executor.table.as_ref(),
                    &[&any(subquery)]
                )?,
                vec![TableProviderFilterPushDown::Unsupported]
            );
        }
        assert!(supports_expression(&any(plain), None));
        assert!(supports_expression(&any(nested_plain), None));
        Ok(())
    }
}
