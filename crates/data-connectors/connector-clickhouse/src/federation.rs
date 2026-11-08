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
use data_components::function_support::FunctionSupport;
use datafusion::{
    arrow::datatypes::{DataType, SchemaRef},
    common::{DFSchema, NullEquality, Result, Statistics, TableReference, tree_node::TreeNode},
    datasource::TableProvider,
    logical_expr::{Expr, ExprSchemable, JoinType, LogicalPlan, WindowFunctionDefinition},
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

type ClickhouseSqlTable = SqlTable<ClientHandle, &'static dyn Sync>;

pub(super) fn create_federated_provider(
    table: ClickhouseSqlTable,
    table_reference: TableReference,
) -> FederatedTableProviderAdaptor {
    let table = Arc::new(table.with_function_support(Some(function_support())));
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

fn function_support() -> FunctionSupport {
    FunctionSupportBuilder::new()
        // ClickHouse uses byte offsets and ASCII case conversion; its date-part
        // functions also differ in names and field conventions.
        .deny_also(
            [
                "upper",
                "lower",
                "substr",
                "substring",
                "length",
                "character_length",
                "char_length",
                "date_part",
                "date_trunc",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expression(Arc::new(supports_expression))
        .build()
}

fn supports_expression(expr: &Expr, schema: Option<&DFSchema>) -> bool {
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
        // decimal AVG returns a rounded Float64. Windows have the same result
        // types as their corresponding ordinary aggregate.
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
        Expr::Cast(cast) => {
            !cast.field.data_type().is_decimal()
                && non_decimal(&cast.expr, scope)
                && cast.expr.nullable(scope).is_ok_and(|nullable| !nullable)
        }
        Expr::BinaryExpr(binary) => [&binary.left, &binary.right]
            .iter()
            .all(|arg| non_decimal(arg, scope)),
        _ => true,
    }
}

fn non_decimal(expr: &Expr, scope: &DFSchema) -> bool {
    expr.get_type(scope)
        .is_ok_and(|data_type| !data_type.is_decimal())
}

fn aggregate_args_supported(name: &str, args: &[Expr], scope: &DFSchema) -> bool {
    if !name.eq_ignore_ascii_case("sum") && !name.eq_ignore_ascii_case("avg") {
        return true;
    }
    args.iter().all(|arg| {
        arg.get_type(scope)
            .is_ok_and(|data_type| !data_type.is_decimal())
    })
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
        self.table.can_execute_plan(plan) && !plan.exists(|node| {
            Ok(matches!(node, LogicalPlan::Join(join) if
                !matches!(join.join_type, JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full)
                || join.filter.is_some()
                || join.null_equality == NullEquality::NullEqualsNull))
        }).unwrap_or(true)
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
