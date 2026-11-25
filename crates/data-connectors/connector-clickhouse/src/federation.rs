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
use data_components::{
    SOURCE_TYPE_METADATA_KEY,
    function_support::{FunctionSupport, contains_unsupported_functions},
};
use datafusion::{
    arrow::datatypes::{DataType, Field, SchemaRef},
    common::{
        DFSchema, NullEquality, Result, ScalarValue, Statistics, TableReference,
        tree_node::TreeNodeRecursion,
    },
    datasource::TableProvider,
    logical_expr::{
        Distinct, Expr, ExprSchemable, JoinType, LogicalPlan, Operator, WindowFrameBound,
        WindowFrameUnits, WindowFunctionDefinition,
        expr::{AggregateFunction, ScalarFunction, WindowFunction},
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
    let table = Arc::new(with_clickhouse_policy(table));
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

/// `table` with the expressions, dialect and sort pushdown that `ClickHouse`
/// evaluates as Spice does. Its own scan, used without federation, cannot check
/// the keys of a sort pushed into it, so a table with a column
/// [`orders_by_stored_value`] sorts in Spice there.
fn with_clickhouse_policy(table: ClickhouseSqlTable) -> ClickhouseSqlTable {
    let sorts_as_spice = !table
        .schema()
        .fields()
        .iter()
        .any(|field| orders_by_stored_value(field));
    table
        .with_function_support(Some(function_support()))
        .with_dialect(Arc::new(ClickHouseDialect::new()))
        .with_allow_physical_sort_pushdown(sorts_as_spice)
}

/// Whether `ClickHouse` orders this column, or a value inside it, by a stored
/// value that its text, as Spice reads it, does not sort by: an `Enum` by its
/// number, a `UUID` by its two halves, and an IP address by its bytes.
pub(crate) fn orders_by_stored_value(field: &Field) -> bool {
    field
        .metadata()
        .get(SOURCE_TYPE_METADATA_KEY)
        .is_some_and(|source_type| {
            ["Enum8(", "Enum16(", "UUID", "IPv4", "IPv6"]
                .iter()
                .any(|name| source_type.contains(name))
        })
}

/// Whether `expr` reads a column [`orders_by_stored_value`], or a column that is
/// not in `scope` at all. A filter checked for the scan's own SQL names its
/// columns with the table, which `scope` may not, so a column is also found by
/// its name; the filters `SqlExec` checks come with no scope.
fn reads_a_stored_value(expr: &Expr, scope: &DFSchema) -> bool {
    expr.column_refs().into_iter().any(|column| {
        let field = scope
            .qualified_field_from_column(column)
            .map(|(_, field)| field)
            .or_else(|_| scope.field_with_unqualified_name(&column.name));
        match field {
            Ok(field) => orders_by_stored_value(field),
            Err(_) => true,
        }
    })
}

/// Whether `expr` compares, sorts or converts a column [`orders_by_stored_value`]
/// rather than only selecting it or testing it for equality. `ClickHouse` would
/// order its stored values, and its functions reject or convert them. A `CASE`
/// that returns such a column stays local too: its result in Spice carries no
/// source type, so a later sort or `min`/`max` on it would not be recognized.
fn uses_a_stored_value(expr: &Expr, scope: &DFSchema) -> bool {
    let reads = |expr: &Expr| reads_a_stored_value(expr, scope);
    let is_min_or_max = |name: &str| matches!(name, "min" | "max");
    let orders = |op: &Operator| {
        matches!(
            op,
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
        )
    };
    match expr {
        Expr::BinaryExpr(binary) => {
            (orders(&binary.op) || binary.op == Operator::StringConcat)
                && (reads(&binary.left) || reads(&binary.right))
        }
        Expr::SetComparison(set_comparison) => {
            orders(&set_comparison.op)
                && (reads(&set_comparison.expr)
                    || set_comparison
                        .subquery
                        .subquery
                        .schema()
                        .fields()
                        .iter()
                        .any(|field| orders_by_stored_value(field)))
        }
        Expr::Case(case) => {
            case.when_then_expr.iter().any(|(_, then)| reads(then))
                || case.else_expr.as_deref().is_some_and(reads)
        }
        Expr::Between(between) => {
            reads(&between.expr) || reads(&between.low) || reads(&between.high)
        }
        Expr::Like(like) => reads(&like.expr) || reads(&like.pattern),
        Expr::ScalarFunction(call) => call.args.iter().any(reads),
        Expr::AggregateFunction(aggregate) => {
            is_min_or_max(aggregate.func.name()) && aggregate.params.args.iter().any(reads)
        }
        Expr::WindowFunction(window) => {
            window.params.order_by.iter().any(|key| reads(&key.expr))
                || matches!(&window.fun, WindowFunctionDefinition::AggregateUDF(aggregate)
                    if is_min_or_max(aggregate.name()))
                    && window.params.args.iter().any(reads)
        }
        _ => false,
    }
}

/// How an expression check treats the subquery of an `= ANY`/`ALL`.
#[derive(Clone, Copy)]
enum SetComparisonSubquery {
    Check,
    /// The plan walk that runs this check visits the subquery itself.
    Walked,
}

/// The scalar functions `ClickHouse` evaluates the same way under the name the
/// unparser writes. Every other function stays in Spice: many `ClickHouse`
/// functions of the same name count bytes (`left`, `lpad`, `reverse`), change
/// only ASCII letters (`upper`, `initcap`), round half to even (`round`), take
/// the natural log (`log`), replace every match (`regexp_replace`) or return
/// NULL for a NULL argument (`concat_ws`, and `greatest`/`least` before
/// `ClickHouse` 24.12), and some names do not exist there. Some of these are
/// refused for some arguments: `concat`, `coalesce` and `get_field` by
/// [`scalar_call_reads_the_same`], `like`/`ilike` and `ltrim`/`rtrim` by
/// [`supports_expression_for`], and every call that reads a column
/// [`orders_by_stored_value`] by [`uses_a_stored_value`].
const CLICKHOUSE_SCALAR_FUNCTIONS: &[&str] = &[
    "abs",
    "acos",
    "acosh",
    "asin",
    "asinh",
    "atan2",
    "cbrt",
    "ceil",
    "coalesce",
    "concat",
    "cos",
    "cosh",
    "degrees",
    "floor",
    "get_field",
    "ilike",
    "like",
    "log10",
    "log2",
    "ltrim",
    "nullif",
    "octet_length",
    "pi",
    "power",
    "radians",
    "repeat",
    "replace",
    "rtrim",
    "sin",
    "sinh",
    "tan",
    "trunc",
];

/// The aggregate functions `ClickHouse` evaluates the same way. `median` is
/// approximate there, and `first_value`/`last_value` skip NULLs.
const CLICKHOUSE_AGGREGATE_FUNCTIONS: &[&str] = &[
    "avg", "bit_and", "bit_or", "bit_xor", "count", "max", "min", "sum",
];

/// The window functions `ClickHouse` evaluates the same way. It rejects the
/// frame the unparser writes for `lag`, `lead`, `ntile`, `percent_rank` and
/// `cume_dist`, and its `first_value`/`last_value`/`nth_value` skip NULLs.
const CLICKHOUSE_WINDOW_FUNCTIONS: &[&str] = &["dense_rank", "rank", "row_number"];

fn function_support() -> FunctionSupport {
    function_support_for(SetComparisonSubquery::Check)
}

fn scalar_call_reads_the_same(call: &ScalarFunction, scope: &DFSchema) -> bool {
    let name = call.func.name();
    if !CLICKHOUSE_SCALAR_FUNCTIONS.contains(&name) {
        return false;
    }
    let argument_types = || {
        call.args
            .iter()
            .map(|arg| arg.get_type(scope))
            .collect::<Result<Vec<_>>>()
    };
    match name {
        "concat" => argument_types().is_ok_and(|types| types.iter().all(concatenates_the_same)),
        // Some functions have another result type on `ClickHouse` (`abs` and
        // `octet_length` return `UInt64`), so only columns and literals count.
        "coalesce" => {
            call.args
                .iter()
                .all(|arg| matches!(arg, Expr::Column(_) | Expr::Literal(..)))
                && argument_types().is_ok_and(|types| share_a_kind(&types))
        }
        // `ClickHouse` reads `m.k` on a `Map` column as a subcolumn name.
        "get_field" => call
            .args
            .first()
            .is_some_and(|arg| matches!(arg.get_type(scope), Ok(DataType::Struct(_)))),
        _ => true,
    }
}

/// `ClickHouse` writes floats, decimals, booleans and date-times as text in
/// other forms (`100` for `100.0`, `1` for `true`, a space before the time).
fn concatenates_the_same(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) || data_type.is_integer()
}

/// `ClickHouse` gives `coalesce` of an integer and a float, or of a string and
/// a number, a `Variant` type that the driver cannot read.
fn share_a_kind(types: &[DataType]) -> bool {
    let kind = |data_type: &DataType| {
        if matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            Some(0)
        } else if data_type.is_signed_integer() {
            Some(1)
        } else if data_type.is_unsigned_integer() {
            Some(2)
        } else if data_type.is_floating() {
            Some(3)
        } else {
            None
        }
    };
    types
        .windows(2)
        .all(|pair| pair[0] == pair[1] || kind(&pair[0]).is_some_and(|k| kind(&pair[1]) == Some(k)))
}

/// `ClickHouse`'s `count` takes at most one argument.
fn aggregate_reads_the_same(name: &str, args: &[Expr]) -> bool {
    CLICKHOUSE_AGGREGATE_FUNCTIONS.contains(&name) && (name != "count" || args.len() <= 1)
}

/// `ClickHouse` has no `GROUPS` frame and rejects the text offset the unparser
/// writes in a `RANGE` frame; with a numeric offset it orders NULL keys
/// differently. In a `ROWS` frame its `rank` and `dense_rank` number each row,
/// where peers share a rank.
fn window_frame_reads_the_same(call: &WindowFunction) -> bool {
    let frame = &call.params.window_frame;
    match frame.units {
        WindowFrameUnits::Groups => false,
        WindowFrameUnits::Range => {
            [&frame.start_bound, &frame.end_bound]
                .iter()
                .all(|bound| match bound {
                    WindowFrameBound::Preceding(offset) | WindowFrameBound::Following(offset) => {
                        offset.is_null()
                    }
                    WindowFrameBound::CurrentRow => true,
                })
        }
        WindowFrameUnits::Rows => !matches!(
            &call.fun,
            WindowFunctionDefinition::WindowUDF(window)
                if matches!(window.name(), "rank" | "dense_rank")
        ),
    }
}

fn function_support_for(subquery: SetComparisonSubquery) -> FunctionSupport {
    FunctionSupportBuilder::new()
        .scalar_call(Arc::new(
            |call: &ScalarFunction, schema: Option<&DFSchema>| {
                scalar_call_reads_the_same(call, schema.unwrap_or_else(|| DFSchema::empty_ref()))
            },
        ))
        .aggregate_call(Arc::new(|call: &AggregateFunction| {
            aggregate_reads_the_same(call.func.name(), &call.params.args)
        }))
        .window_call(Arc::new(|call: &WindowFunction| {
            window_frame_reads_the_same(call)
                && match &call.fun {
                    WindowFunctionDefinition::AggregateUDF(aggregate) => {
                        aggregate_reads_the_same(aggregate.name(), &call.params.args)
                    }
                    WindowFunctionDefinition::WindowUDF(window) => {
                        CLICKHOUSE_WINDOW_FUNCTIONS.contains(&window.name())
                    }
                }
        }))
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
    if uses_a_stored_value(expr, scope) {
        return false;
    }
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
        // semantics ignore a NULL item rather than yielding UNKNOWN. `ROLLUP`
        // and `CUBE` over no rows give no total row on `ClickHouse`, and a key
        // that a grouping set leaves out holds its type's default value (`0`,
        // `''`), not NULL.
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
        | Expr::TryCast(_)
        | Expr::GroupingSet(_) => false,
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
        Expr::ScalarFunction(call) if call.func.name() == "get_field" => !call
            .args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(value, _) if value.to_string().contains('\\'))),
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
                    && !plan_has_a_node_clickhouse_refuses(plan)
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
        // `ClickHouse` reads `count(x)` of a `Nullable` column from its `null`
        // subcolumn, which a window cannot find.
        Expr::WindowFunction(window) => match &window.fun {
            WindowFunctionDefinition::AggregateUDF(aggregate) => {
                aggregate_args_supported(aggregate.name(), &window.params.args, scope)
                    && (aggregate.name() != "count"
                        || window.params.distinct
                        || window
                            .params
                            .args
                            .iter()
                            .all(|arg| arg.nullable(scope).is_ok_and(|nullable| !nullable)))
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
                && (!matches!(
                    cast.field.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                ) || cast
                    .expr
                    .get_type(scope)
                    .is_ok_and(|data_type| concatenates_the_same(&data_type)))
        }
        Expr::BinaryExpr(binary) => {
            [&binary.left, &binary.right]
                .iter()
                .all(|arg| non_decimal(arg, scope))
                && (binary.op != Operator::Modulo || modulo_divisor_is_renderable(&binary.right))
                && (binary.op != Operator::StringConcat
                    || [&binary.left, &binary.right].iter().all(|arg| {
                        arg.get_type(scope)
                            .is_ok_and(|data_type| concatenates_the_same(&data_type))
                    }))
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
    // ClickHouse adds the values of a 64-bit integer `avg` in that type, where
    // the sum can wrap. DataFusion adds them as floats.
    if name.eq_ignore_ascii_case("avg")
        && args
            .iter()
            .any(|arg| matches!(arg.get_type(scope), Ok(DataType::Int64 | DataType::UInt64)))
    {
        return false;
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

/// Whether this plan or a subquery in it has a node `ClickHouse` evaluates
/// differently or does not have:
/// - a join of another kind than inner and outer, with a filter, or matching
///   NULL keys, and an outer join whose side with no match has an `Array`,
///   `Map` or `Tuple` column (see [`has_a_list_map_or_tuple`]);
/// - an `UNNEST`;
/// - a sort by a column [`orders_by_stored_value`], also the sort of a
///   `DISTINCT ON`, and a `UNION` whose output column lost that type because
///   its inputs differ;
/// - a sort or limit directly over a `UNION` (see [`is_a_union`]), which
///   `ClickHouse` applies to the last `SELECT` of the `UNION` only.
fn plan_has_a_node_clickhouse_refuses(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let walked = plan.apply_with_subqueries(|node| {
        found = match node {
            LogicalPlan::Join(join) => {
                !matches!(
                    join.join_type,
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                ) || join.filter.is_some()
                    || join.null_equality == NullEquality::NullEqualsNull
                    || matches!(join.join_type, JoinType::Left | JoinType::Full)
                        && has_a_list_map_or_tuple(&join.right)
                    || matches!(join.join_type, JoinType::Right | JoinType::Full)
                        && has_a_list_map_or_tuple(&join.left)
            }
            LogicalPlan::Unnest(_) => true,
            LogicalPlan::Sort(sort) => {
                is_a_union(&sort.input)
                    || sort
                        .expr
                        .iter()
                        .any(|key| reads_a_stored_value(&key.expr, sort.input.schema()))
            }
            LogicalPlan::Limit(limit) => is_a_union(&limit.input),
            LogicalPlan::Distinct(Distinct::On(distinct_on)) => distinct_on
                .sort_expr
                .iter()
                .flatten()
                .any(|key| reads_a_stored_value(&key.expr, distinct_on.input.schema())),
            LogicalPlan::Union(union) => {
                union
                    .schema
                    .fields()
                    .iter()
                    .enumerate()
                    .any(|(index, field)| {
                        !orders_by_stored_value(field)
                            && union.inputs.iter().any(|input| {
                                input
                                    .schema()
                                    .fields()
                                    .get(index)
                                    .is_some_and(|field| orders_by_stored_value(field))
                            })
                    })
            }
            _ => false,
        };
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    walked.is_err() || found
}

/// Whether `plan` is a `UNION`, also a `UNION` without `ALL`, which is a
/// `DISTINCT` over the union that the unparser writes into the same query. A
/// projection over a union is written as a subquery.
fn is_a_union(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Union(_) => true,
        LogicalPlan::Distinct(Distinct::All(input)) => is_a_union(input),
        _ => false,
    }
}

/// Whether `plan` outputs an `Array`, `Map` or `Tuple` column. `ClickHouse`
/// fills an `Array` or `Map` of the side of an outer join with no match with
/// an empty value, not NULL, and only a server that can make a `Tuple`
/// nullable returns NULL for one.
fn has_a_list_map_or_tuple(plan: &LogicalPlan) -> bool {
    plan.schema().fields().iter().any(|field| {
        matches!(
            field.data_type(),
            DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(..)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::Map(..)
                | DataType::Struct(_)
        )
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
        // The walk visits each `= ANY`/`ALL` subquery itself, so the expression
        // check does not check it again.
        let supported = !contains_unsupported_functions(
            plan,
            &function_support_for(SetComparisonSubquery::Walked),
        )
        .unwrap_or(true);
        supported && !plan_has_a_node_clickhouse_refuses(plan) && !plan_names_a_backslash(plan)
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
    /// A sort pushed into the federated scan is also checked with
    /// [`Self::can_execute_plan`], which keeps a sort by a column
    /// [`orders_by_stored_value`] or over a `UNION` in Spice.
    fn supports_sort_pushdown(&self) -> bool {
        true
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
    use datafusion::functions::core::expr_fn::get_field;
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
            reason = "installs the ClickHouse expression policy with `with_clickhouse_policy` below"
        )]
        let table = SqlTable::new_with_schema("clickhouse", &pool, schema, "t", None);
        Ok(ClickhouseExecutor {
            table: Arc::new(with_clickhouse_policy(table)),
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
    fn a_function_clickhouse_evaluates_differently_stays_local() -> Result<()> {
        use datafusion::functions_aggregate::{
            average::avg_udaf, count::count_udaf, first_last::first_value_udaf,
            median::median_udaf, sum::sum_udaf,
        };
        use datafusion::functions_window::expr_fn::{dense_rank, lag, ntile, rank, row_number};
        use datafusion::logical_expr::WindowFrame;
        use datafusion::logical_expr::expr::WindowFunction;

        let scope = DFSchema::try_from(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("nn", DataType::Int64, true),
            Field::new("un", DataType::UInt64, false),
            Field::new("x", DataType::Float64, false),
        ]))?;
        let support = function_support();
        let scalar = |name: &str| {
            create_udf(
                name,
                vec![DataType::Int64],
                DataType::Int64,
                Volatility::Immutable,
                Arc::new(|_: &[ColumnarValue]| -> Result<ColumnarValue> {
                    datafusion::common::not_impl_err!("test only")
                }),
            )
            .call(vec![col("n")])
        };
        for name in ["abs", "coalesce", "concat", "power", "trunc"] {
            assert!(support.supports(&scalar(name), Some(&scope)), "{name}");
        }
        for name in [
            "round",
            "log",
            "regexp_replace",
            "concat_ws",
            "hex",
            "bin",
            "soundex",
            "from_unixtime",
            "gcd",
            "starts_with",
            "strpos",
            "md5",
            "hour",
            "date_bin",
            "named_struct",
            "greatest",
            "least",
        ] {
            assert!(!support.supports(&scalar(name), Some(&scope)), "{name}");
        }
        for (aggregate, federates) in [
            (count_udaf(), true),
            (sum_udaf(), true),
            (median_udaf(), false),
            (first_value_udaf(), false),
        ] {
            let name = aggregate.name().to_string();
            let call = aggregate.call(vec![col("n")]);
            assert_eq!(support.supports(&call, Some(&scope)), federates, "{name}");
            let window = Expr::WindowFunction(Box::new(WindowFunction::new(
                WindowFunctionDefinition::AggregateUDF(aggregate),
                vec![col("n")],
            )));
            assert_eq!(
                support.supports(&window, Some(&scope)),
                federates,
                "{name} OVER"
            );
        }
        let aggregate_window = |aggregate: Arc<datafusion::logical_expr::AggregateUDF>,
                                arg: &str,
                                frame: WindowFrame| {
            let mut window = WindowFunction::new(
                WindowFunctionDefinition::AggregateUDF(aggregate),
                vec![col(arg)],
            );
            window.params.window_frame = frame;
            Expr::WindowFunction(Box::new(window))
        };
        let framed = |expr: Expr, frame: WindowFrame| match expr {
            Expr::WindowFunction(mut window) => {
                window.params.window_frame = frame;
                Expr::WindowFunction(window)
            }
            expr => expr,
        };
        let unbounded = ScalarValue::UInt64(None);
        let every_row = || {
            WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(unbounded.clone()),
                WindowFrameBound::Following(unbounded.clone()),
            )
        };
        let one_before = |units: WindowFrameUnits, offset: ScalarValue| {
            WindowFrame::new_bounds(
                units,
                WindowFrameBound::Preceding(offset),
                WindowFrameBound::CurrentRow,
            )
        };

        let avg = avg_udaf();
        assert!(!support.supports(&avg.call(vec![col("n")]), Some(&scope)));
        assert!(!support.supports(&avg.call(vec![col("un")]), Some(&scope)));
        assert!(support.supports(&avg.call(vec![col("x")]), Some(&scope)));
        assert!(!support.supports(
            &aggregate_window(avg_udaf(), "n", every_row()),
            Some(&scope)
        ));
        assert!(support.supports(
            &aggregate_window(avg_udaf(), "x", every_row()),
            Some(&scope)
        ));

        let rows = one_before(WindowFrameUnits::Rows, ScalarValue::UInt64(Some(1)));
        let range = one_before(WindowFrameUnits::Range, ScalarValue::Utf8(Some("1".into())));
        let groups = one_before(WindowFrameUnits::Groups, ScalarValue::UInt64(Some(1)));
        assert!(support.supports(&aggregate_window(sum_udaf(), "x", rows), Some(&scope)));
        assert!(!support.supports(&aggregate_window(sum_udaf(), "x", range), Some(&scope)));
        assert!(!support.supports(&aggregate_window(sum_udaf(), "x", groups), Some(&scope)));
        assert!(!support.supports(&framed(rank(), every_row()), Some(&scope)));
        assert!(!support.supports(&framed(dense_rank(), every_row()), Some(&scope)));
        assert!(support.supports(&count_udaf().call(vec![col("nn")]), Some(&scope)));
        let two = vec![col("n"), col("x")];
        assert!(!support.supports(&count_udaf().call(two.clone()), Some(&scope)));
        let window = WindowFunction::new(WindowFunctionDefinition::AggregateUDF(count_udaf()), two);
        assert!(!support.supports(&Expr::WindowFunction(Box::new(window)), Some(&scope)));
        assert!(!support.supports(
            &aggregate_window(count_udaf(), "nn", every_row()),
            Some(&scope)
        ));
        assert!(support.supports(
            &aggregate_window(sum_udaf(), "nn", every_row()),
            Some(&scope)
        ));
        let mut distinct = WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(count_udaf()),
            vec![col("nn")],
        );
        distinct.params.distinct = true;
        assert!(support.supports(&Expr::WindowFunction(Box::new(distinct)), Some(&scope)));
        assert!(support.supports(&framed(row_number(), every_row()), Some(&scope)));
        assert!(support.supports(&row_number(), Some(&scope)));
        let ordered = WindowFrame::new(Some(false));
        assert!(support.supports(&framed(rank(), ordered), Some(&scope)));
        assert!(!support.supports(&lag(col("n"), None, None), Some(&scope)));
        assert!(!support.supports(&ntile(lit(3)), Some(&scope)));
        Ok(())
    }

    #[test]
    fn a_call_on_arguments_clickhouse_reads_differently_stays_local() -> Result<()> {
        let entries = (
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        );
        let scope = DFSchema::try_from(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("i", DataType::Int64, false),
            Field::new("u", DataType::UInt32, false),
            Field::new("v", DataType::Float64, false),
            Field::new("f", DataType::Float32, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("d", DataType::Timestamp(TimeUnit::Second, None), false),
            Field::new(
                "st",
                DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int64, false)])),
                false,
            ),
            Field::new_map("m", "entries", entries.0, entries.1, false, false),
        ]))?;
        let call = |name: &str, args: Vec<Expr>| {
            let udf = create_udf(
                name,
                vec![],
                DataType::Utf8,
                Volatility::Immutable,
                Arc::new(|_: &[ColumnarValue]| -> Result<ColumnarValue> {
                    datafusion::common::not_impl_err!("test only")
                }),
            );
            scalar_call_reads_the_same(&ScalarFunction::new_udf(Arc::new(udf), args), &scope)
        };
        assert!(call("concat", vec![col("s"), col("i"), col("u")]));
        for arg in ["v", "b", "d"] {
            assert!(
                !call("concat", vec![col("s"), col(arg)]),
                "concat with {arg}"
            );
        }
        assert!(call("coalesce", vec![col("i"), lit(0_i64)]));
        assert!(call("coalesce", vec![col("f"), lit(0.5)]));
        assert!(call("coalesce", vec![col("d"), col("d")]));
        assert!(!call("coalesce", vec![col("i"), col("v")]));
        assert!(!call("coalesce", vec![col("i"), col("u")]));
        assert!(!call("coalesce", vec![col("s"), col("i")]));
        assert!(!call("coalesce", vec![col("i") + lit(1_i64), lit(0_i64)]));

        let support = function_support();
        assert!(support.supports(&get_field(col("st"), "a"), Some(&scope)));
        assert!(!support.supports(&get_field(col("m"), "k"), Some(&scope)));

        let string_concat = |left: &str, right: &str| {
            let expr = binary_expr(col(left), Operator::StringConcat, col(right));
            supports_expression(&expr, Some(&scope))
        };
        assert!(string_concat("s", "i"));
        assert!(!string_concat("s", "v"));
        assert!(!string_concat("b", "s"));

        for text in [DataType::Utf8, DataType::Utf8View] {
            let to_text = |value: Expr| Expr::Cast(Cast::new(Box::new(value), text.clone()));
            assert!(
                supports_expression(&to_text(lit(100_i64)), Some(&scope)),
                "{text}"
            );
            assert!(
                !supports_expression(&to_text(lit(100.0)), Some(&scope)),
                "{text}"
            );
        }
        Ok(())
    }

    #[test]
    fn a_join_clickhouse_evaluates_differently_stays_local() -> Result<()> {
        let left = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let right = Schema::new(vec![Field::new("b", DataType::Int64, true)]);
        let executor = executor(left.clone())?;
        let join = |join_type: JoinType,
                    filter: Option<Expr>,
                    null_equality: NullEquality|
         -> Result<LogicalPlan> {
            LogicalPlanBuilder::from(table_scan(Some("l"), &left, None)?.build()?)
                .join_detailed(
                    table_scan(Some("r"), &right, None)?.build()?,
                    join_type,
                    (vec!["a"], vec!["b"]),
                    filter,
                    null_equality,
                )?
                .build()
        };
        let nothing = NullEquality::NullEqualsNothing;
        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
        ] {
            assert!(
                executor.can_execute_plan(&join(join_type, None, nothing)?),
                "{join_type:?}"
            );
        }
        assert!(!executor.can_execute_plan(&join(JoinType::LeftSemi, None, nothing)?));
        let non_equi = col("a").lt(col("b"));
        assert!(!executor.can_execute_plan(&join(JoinType::Inner, Some(non_equi), nothing)?));
        assert!(!executor.can_execute_plan(&join(
            JoinType::Inner,
            None,
            NullEquality::NullEqualsNull
        )?));
        Ok(())
    }

    /// A column with this `ClickHouse` source type.
    fn sourced(name: &str, data_type: DataType, source_type: &str) -> Field {
        Field::new(name, data_type, true).with_metadata(
            [(
                SOURCE_TYPE_METADATA_KEY.to_string(),
                source_type.to_string(),
            )]
            .into(),
        )
    }

    /// `left <op> ANY (SELECT <column> FROM t)` over the columns of
    /// [`a_column_clickhouse_orders_by_stored_value_stays_local`].
    fn any(left: Expr, op: Operator, column: &str) -> Expr {
        let schema = Schema::new(vec![
            sourced("ip", DataType::Utf8, "IPv4"),
            sourced("s", DataType::Utf8, "String"),
        ]);
        let subquery = table_scan(Some("t"), &schema, None)
            .and_then(|scan| scan.project(vec![col(column)]))
            .and_then(LogicalPlanBuilder::build)
            .expect("subquery");
        Expr::SetComparison(SetComparison::new(
            Box::new(left),
            Subquery {
                subquery: Arc::new(subquery),
                outer_ref_columns: vec![],
                spans: Spans::new(),
            },
            op,
            SetQuantifier::Any,
        ))
    }

    #[test]
    fn a_column_clickhouse_orders_by_stored_value_stays_local() -> Result<()> {
        use datafusion::functions::expr_fn::{coalesce, concat};
        use datafusion::functions_aggregate::expr_fn::{count, max, min};
        use datafusion::functions_window::expr_fn::rank;
        use datafusion::logical_expr::{ExprFunctionExt, when};

        for (source_type, orders_by_stored_value) in [
            ("Enum8('b' = 1, 'a' = 2)", true),
            ("Enum16('a' = 1000)", true),
            ("Nullable(UUID)", true),
            ("IPv4", true),
            ("LowCardinality(Nullable(IPv6))", true),
            ("Array(UUID)", true),
            ("Tuple(u UUID, n Int32)", true),
            ("String", false),
            ("FixedString(4)", false),
            ("Map(String, UInt64)", false),
        ] {
            assert_eq!(
                super::orders_by_stored_value(&sourced("c", DataType::Utf8, source_type)),
                orders_by_stored_value,
                "{source_type}"
            );
        }
        assert!(!super::orders_by_stored_value(&Field::new(
            "c",
            DataType::Utf8,
            true
        )));

        let schema = Schema::new(vec![
            sourced("e", DataType::Utf8, "Enum8('b' = 1, 'a' = 2)"),
            sourced("ip", DataType::Utf8, "IPv4"),
            sourced("u", DataType::Utf8, "Nullable(UUID)"),
            sourced("s", DataType::Utf8, "String"),
        ]);
        let scope = DFSchema::try_from(schema.clone())?;
        let supports = |expr: &Expr| supports_expression(expr, Some(&scope));
        let like_u = Expr::Like(Like::new(
            false,
            Box::new(col("u")),
            Box::new(lit("0%")),
            None,
            false,
        ));
        for expr in [
            col("e").gt(lit("a")),
            lit("9.0.0.1").lt_eq(col("ip")),
            col("ip").between(lit("10.0.0.0"), lit("10.0.0.9")),
            max(col("e")),
            min(col("u")),
            coalesce(vec![col("u"), lit("none")]),
            concat(vec![col("e"), lit("!")]),
            like_u,
            rank().order_by(vec![col("ip").sort(true, false)]).build()?,
            when(col("s").eq(lit("x")), col("e")).otherwise(lit("b"))?,
            when(col("s").eq(lit("x")), lit("b")).otherwise(col("e"))?,
            binary_expr(col("ip"), Operator::StringConcat, lit("")),
            Expr::from(WindowFunction::new(
                WindowFunctionDefinition::AggregateUDF(
                    datafusion::functions_aggregate::min_max::max_udaf(),
                ),
                vec![col("e")],
            )),
            any(col("ip"), Operator::Gt, "s"),
            any(col("s"), Operator::Lt, "ip"),
        ] {
            assert!(!supports(&expr), "{expr}");
        }
        for expr in [
            col("e"),
            col("e").eq(lit("a")),
            col("ip").not_eq(lit("9.0.0.1")),
            col("u").in_list(vec![lit("00000000-0000-0000-0000-000000000002")], false),
            col("u").is_null(),
            count(col("e")),
            col("s").gt(lit("a")),
            max(col("s")),
            rank().order_by(vec![col("s").sort(true, false)]).build()?,
            when(col("e").eq(lit("a")), col("s")).otherwise(lit("b"))?,
            any(col("ip"), Operator::Eq, "ip"),
            any(col("s"), Operator::Gt, "s"),
        ] {
            assert!(supports(&expr), "{expr}");
        }
        // The filters `SqlExec` checks come with no scope to resolve `u` in.
        let like_u = Expr::Like(Like::new(
            false,
            Box::new(col("u")),
            Box::new(lit("0%")),
            None,
            false,
        ));
        assert!(!supports_expression(&like_u, None));

        let stored = executor(schema.clone())?;
        // A filter checked for the scan's own SQL names its columns with the table.
        let like = |column: &str| {
            Expr::Like(Like::new(
                false,
                Box::new(Expr::Column(Column::new(Some("t"), column))),
                Box::new(lit("x%")),
                None,
                false,
            ))
        };
        let (like_s, like_u) = (like("s"), like("u"));
        assert_eq!(
            TableProvider::supports_filters_pushdown(stored.table.as_ref(), &[&like_s, &like_u])?,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Unsupported
            ]
        );
        let scan = || table_scan(Some("t"), &schema, None);
        let sorted = |key: &str| scan()?.sort(vec![col(key).sort(true, false)])?.build();
        assert!(!stored.can_execute_plan(&sorted("e")?));
        assert!(!stored.can_execute_plan(&sorted("u")?));
        assert!(stored.can_execute_plan(&sorted("s")?));
        let distinct_on = |keys: &[&str]| {
            scan()?
                .distinct_on(
                    vec![col("s")],
                    vec![col("s"), col("e")],
                    Some(keys.iter().map(|key| col(*key).sort(true, false)).collect()),
                )?
                .build()
        };
        assert!(!stored.can_execute_plan(&distinct_on(&["s", "e"])?));
        assert!(stored.can_execute_plan(&distinct_on(&["s"])?));

        // A `UNION` of `UUID` and `Nullable(UUID)` columns has no source type.
        let other = Schema::new(vec![sourced("u", DataType::Utf8, "UUID")]);
        let union_with = |schema: &Schema| {
            scan()?
                .project(vec![col("u")])?
                .union(table_scan(Some("o"), schema, None)?.build()?)?
                .build()
        };
        assert!(!stored.can_execute_plan(&union_with(&other)?));
        let same = Schema::new(vec![sourced("u", DataType::Utf8, "Nullable(UUID)")]);
        assert!(stored.can_execute_plan(&union_with(&same)?));

        // The federated scan checks a pushed sort with `can_execute_plan`, as
        // above; the table's own scan cannot, so it sorts in Spice.
        assert!(stored.supports_sort_pushdown());
        assert!(!SQLExecutor::supports_sort_pushdown(stored.table.as_ref()));
        let plain = Schema::new(vec![sourced("s", DataType::Utf8, "String")]);
        assert!(SQLExecutor::supports_sort_pushdown(
            executor(plain)?.table.as_ref()
        ));
        Ok(())
    }

    /// `ClickHouse` applies an `ORDER BY` or `LIMIT` after a `UNION` to its last
    /// `SELECT` only; under an alias the unparser writes the `UNION` as a
    /// subquery.
    #[test]
    fn a_sort_or_limit_over_a_union_stays_local() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let executor = executor(schema.clone())?;
        let union = || -> Result<LogicalPlanBuilder> {
            table_scan(Some("l"), &schema, None)?
                .union(table_scan(Some("r"), &schema, None)?.build()?)
        };
        let sorted = |builder: LogicalPlanBuilder| -> Result<LogicalPlan> {
            builder.sort(vec![col("a").sort(true, false)])?.build()
        };
        assert!(executor.can_execute_plan(&union()?.build()?));
        assert!(!executor.can_execute_plan(&sorted(union()?)?));
        assert!(!executor.can_execute_plan(&union()?.limit(0, Some(1))?.build()?));
        // `UNION` without `ALL`.
        assert!(!executor.can_execute_plan(&sorted(union()?.distinct()?)?));
        assert!(!executor.can_execute_plan(&union()?.distinct()?.limit(0, Some(1))?.build()?));
        // A projection or an alias over the union is written as a subquery.
        assert!(executor.can_execute_plan(&sorted(union()?.project(vec![col("a")])?)?));
        assert!(executor.can_execute_plan(&sorted(union()?.alias("q")?)?));
        Ok(())
    }

    /// The federated scan asks `can_execute_plan` before it takes a sort pushed
    /// down to it, so a sort by a column [`orders_by_stored_value`] stays local.
    #[test]
    fn the_federated_scan_declines_a_sort_by_a_stored_value() -> Result<()> {
        use datafusion::arrow::compute::SortOptions;
        use datafusion::physical_expr::{PhysicalSortExpr, expressions::Column as PhysicalColumn};
        use datafusion::physical_plan::{ExecutionPlan, sort_pushdown::SortOrderPushdownResult};
        use datafusion_federation::sql::VirtualExecutionPlan;

        let schema = Schema::new(vec![
            sourced("e", DataType::Utf8, "Enum8('b' = 1, 'a' = 2)"),
            sourced("s", DataType::Utf8, "String"),
        ]);
        let scan = VirtualExecutionPlan::new(
            table_scan(Some("t"), &schema, None)?.build()?,
            Arc::new(executor(schema.clone())?),
            Statistics::new_unknown(&schema),
            None,
        );
        let sort_on = |name: &str, index: usize| {
            vec![PhysicalSortExpr::new(
                Arc::new(PhysicalColumn::new(name, index)),
                SortOptions::default(),
            )]
        };
        assert!(matches!(
            scan.try_pushdown_sort(&sort_on("e", 0))?,
            SortOrderPushdownResult::Unsupported
        ));
        assert!(matches!(
            scan.try_pushdown_sort(&sort_on("s", 1))?,
            SortOrderPushdownResult::Exact { .. }
        ));
        Ok(())
    }

    #[test]
    fn an_outer_join_that_would_fill_an_array_map_or_tuple_stays_local() -> Result<()> {
        let plain = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let with = |name: &str, data_type: DataType| {
            Schema::new(vec![
                Field::new("b", DataType::Int64, true),
                Field::new(name, data_type, true),
            ])
        };
        let item = Arc::new(Field::new("item", DataType::Int64, true));
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Int64, true),
            ])),
            false,
        ));
        for other in [
            with("m", DataType::Map(entries, false)),
            with(
                "tp",
                DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int64, true)])),
            ),
        ] {
            let executor = executor(plain.clone())?;
            let left = table_scan(Some("l"), &plain, None)?
                .join(
                    table_scan(Some("r"), &other, None)?.build()?,
                    JoinType::Left,
                    (vec!["a"], vec!["b"]),
                    None,
                )?
                .build()?;
            assert!(!executor.can_execute_plan(&left), "{}", other.field(1));
        }
        let with_list = with("tags", DataType::List(item));
        let executor = executor(plain.clone())?;
        let join = |left: &Schema, right: &Schema, join_type: JoinType| -> Result<LogicalPlan> {
            let key = |schema: &Schema| schema.field(0).name().clone();
            table_scan(Some("l"), left, None)?
                .join(
                    table_scan(Some("r"), right, None)?.build()?,
                    join_type,
                    (vec![key(left)], vec![key(right)]),
                    None,
                )?
                .build()
        };
        for (join_type, list_on_the_right, list_on_the_left) in [
            (JoinType::Inner, true, true),
            (JoinType::Left, false, true),
            (JoinType::Right, true, false),
            (JoinType::Full, false, false),
        ] {
            assert_eq!(
                executor.can_execute_plan(&join(&plain, &with_list, join_type)?),
                list_on_the_right,
                "{join_type:?}, list on the right"
            );
            assert_eq!(
                executor.can_execute_plan(&join(&with_list, &plain, join_type)?),
                list_on_the_left,
                "{join_type:?}, list on the left"
            );
        }
        Ok(())
    }

    #[test]
    fn a_grouping_set_stays_local() -> Result<()> {
        use datafusion::logical_expr::{cube, grouping_set, rollup};

        let scope = DFSchema::try_from(Schema::new(vec![Field::new("i", DataType::Int64, true)]))?;
        for set in [
            rollup(vec![col("i")]),
            cube(vec![col("i")]),
            grouping_set(vec![vec![col("i")], vec![]]),
        ] {
            assert!(!supports_expression(&set, Some(&scope)), "{set}");
        }
        Ok(())
    }

    #[test]
    fn an_unnest_stays_local() -> Result<()> {
        let schema = Schema::new(vec![Field::new_list(
            "tags",
            Field::new_list_field(DataType::Int64, true),
            true,
        )]);
        let executor = executor(schema.clone())?;
        let scan = table_scan(Some("t"), &schema, None)?.build()?;
        let unnest = LogicalPlanBuilder::from(scan.clone())
            .unnest_column("tags")?
            .build()?;
        assert!(!executor.can_execute_plan(&unnest));
        assert!(executor.can_execute_plan(&scan));

        let outer = Schema::new(vec![Field::new("i", DataType::Int64, true)]);
        let any = Expr::SetComparison(SetComparison::new(
            Box::new(col("i")),
            Subquery {
                subquery: Arc::new(unnest),
                outer_ref_columns: vec![],
                spans: Spans::new(),
            },
            Operator::Eq,
            SetQuantifier::Any,
        ));
        let scope = DFSchema::try_from_qualified_schema("o", &outer)?;
        assert!(!supports_expression(&any, Some(&scope)));
        let filtered = LogicalPlanBuilder::from(table_scan(Some("o"), &outer, None)?.build()?)
            .filter(any)?
            .build()?;
        assert!(!executor.can_execute_plan(&filtered));
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
