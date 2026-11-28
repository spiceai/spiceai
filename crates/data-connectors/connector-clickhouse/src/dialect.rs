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

//! The SQL dialect federated `ClickHouse` queries are written in.

use std::sync::Arc;

use chrono::DateTime;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::Result;
use datafusion::logical_expr::{Expr, SortExpr};
use datafusion::sql::sqlparser::ast::{self, BinaryOperator, WindowFrameBound};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{
    CharacterLengthStyle, DateFieldExtractStyle, DefaultDialect, Dialect, DistinctFromStyle,
    FloatLiteralStyle, IntervalStyle, ScalarFnToSqlHandler,
};

/// [`DefaultDialect`] with `ClickHouse`'s integer division.
pub(crate) struct ClickHouseDialect {
    inner: DefaultDialect,
}

impl ClickHouseDialect {
    pub(crate) fn new() -> Self {
        Self {
            inner: DefaultDialect {},
        }
    }
}

/// Every `Dialect` method is forwarded explicitly, and the lint keeps it that way.
///
/// An unlisted method falls back to the *trait default* rather than
/// [`DefaultDialect`]'s rendering, with no error anywhere. Denying
/// `missing_trait_methods` turns that into a compile failure instead.
#[deny(clippy::missing_trait_methods)]
impl Dialect for ClickHouseDialect {
    fn with_custom_scalar_overrides(self, handlers: Vec<(&str, ScalarFnToSqlHandler)>) -> Self {
        Self {
            inner: self.inner.with_custom_scalar_overrides(handlers),
        }
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        self.inner
            .scalar_function_to_sql_overrides(unparser, func_name, args)
    }

    fn aggregate_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
        distinct: bool,
        filter: Option<&Expr>,
        order_by: &[SortExpr],
    ) -> Result<Option<ast::Expr>> {
        self.inner.aggregate_function_to_sql_overrides(
            unparser, func_name, args, distinct, filter, order_by,
        )
    }

    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        self.inner.identifier_quote_style(identifier)
    }

    fn quoted_identifier_escapes(&self) -> &'static [(char, &'static str)] {
        self.inner.quoted_identifier_escapes()
    }

    fn use_array_keyword_for_array_literals(&self) -> bool {
        self.inner.use_array_keyword_for_array_literals()
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        self.inner.supports_nulls_first_in_sort()
    }

    fn use_timestamp_for_date64(&self) -> bool {
        self.inner.use_timestamp_for_date64()
    }

    fn interval_style(&self) -> IntervalStyle {
        self.inner.interval_style()
    }

    fn float64_ast_dtype(&self) -> ast::DataType {
        self.inner.float64_ast_dtype()
    }

    fn float_literal_style(&self) -> FloatLiteralStyle {
        self.inner.float_literal_style()
    }

    fn utf8_cast_dtype(&self) -> ast::DataType {
        self.inner.utf8_cast_dtype()
    }

    fn large_utf8_cast_dtype(&self) -> ast::DataType {
        self.inner.large_utf8_cast_dtype()
    }

    fn date_field_extract_style(&self) -> DateFieldExtractStyle {
        self.inner.date_field_extract_style()
    }

    fn distinct_from_style(&self) -> DistinctFromStyle {
        self.inner.distinct_from_style()
    }

    fn character_length_style(&self) -> CharacterLengthStyle {
        self.inner.character_length_style()
    }

    fn int64_cast_dtype(&self) -> ast::DataType {
        self.inner.int64_cast_dtype()
    }

    fn int8_cast_dtype(&self) -> ast::DataType {
        self.inner.int8_cast_dtype()
    }

    fn int32_cast_dtype(&self) -> ast::DataType {
        self.inner.int32_cast_dtype()
    }

    fn timestamp_cast_dtype(&self, time_unit: &TimeUnit, tz: &Option<Arc<str>>) -> ast::DataType {
        self.inner.timestamp_cast_dtype(time_unit, tz)
    }

    fn timestamp_literal_cast_dtype(
        &self,
        time_unit: &TimeUnit,
        tz: &Option<Arc<str>>,
    ) -> ast::DataType {
        self.inner.timestamp_literal_cast_dtype(time_unit, tz)
    }

    fn decimal_type_to_sql(&self, precision: u64, scale: i64) -> Option<ast::DataType> {
        self.inner.decimal_type_to_sql(precision, scale)
    }

    fn date_difference_to_sql(&self, lhs: ast::Expr, rhs: ast::Expr) -> Option<ast::Expr> {
        self.inner.date_difference_to_sql(lhs, rhs)
    }

    fn date_to_integer_to_sql(&self, date: ast::Expr) -> Option<ast::Expr> {
        self.inner.date_to_integer_to_sql(date)
    }

    fn string_to_timestamp_to_sql(
        &self,
        value: ast::Expr,
        tz: Option<&Arc<str>>,
    ) -> Option<ast::Expr> {
        self.inner.string_to_timestamp_to_sql(value, tz)
    }

    fn string_to_date_to_sql(&self, value: ast::Expr) -> Option<ast::Expr> {
        self.inner.string_to_date_to_sql(value)
    }

    fn supports_recursive_cte(&self) -> bool {
        self.inner.supports_recursive_cte()
    }

    fn supports_distinct_recursive_cte(&self) -> bool {
        self.inner.supports_distinct_recursive_cte()
    }

    /// `ClickHouse`'s `/` always divides as `Float64`, so `7 / 2` answers `3.5`.
    /// `intDiv` truncates toward zero like `DataFusion`'s integer `/`, and fails
    /// on a zero divisor and on the minimum value divided by `-1`.
    fn integer_division_to_sql(&self, lhs: ast::Expr, rhs: ast::Expr) -> Option<ast::Expr> {
        Some(ast::Expr::Function(ast::Function {
            name: ast::ObjectName::from(vec![ast::Ident::new("intDiv")]),
            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(lhs)),
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(rhs)),
                ],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: ast::FunctionArguments::None,
            uses_odbc_syntax: false,
        }))
    }

    fn string_concat_to_sql(&self, lhs: ast::Expr, rhs: ast::Expr) -> Option<ast::Expr> {
        self.inner.string_concat_to_sql(lhs, rhs)
    }

    fn requires_explicit_comparison_coercion(&self) -> bool {
        self.inner.requires_explicit_comparison_coercion()
    }

    fn timestamp_literal_max_subsecond_digits(&self) -> Option<usize> {
        self.inner.timestamp_literal_max_subsecond_digits()
    }

    fn timestamp_at_time_zone_to_sql(&self, input: ast::Expr, tz: &str) -> Option<ast::Expr> {
        self.inner.timestamp_at_time_zone_to_sql(input, tz)
    }

    fn date32_cast_dtype(&self) -> ast::DataType {
        self.inner.date32_cast_dtype()
    }

    fn supports_column_alias_in_table_alias(&self) -> bool {
        self.inner.supports_column_alias_in_table_alias()
    }

    fn derived_table_evaluates_volatile_outputs_once(&self) -> bool {
        self.inner.derived_table_evaluates_volatile_outputs_once()
    }

    fn identifier_max_length(&self) -> Option<usize> {
        self.inner.identifier_max_length()
    }

    fn requires_derived_table_alias(&self) -> bool {
        self.inner.requires_derived_table_alias()
    }

    fn division_operator(&self) -> BinaryOperator {
        self.inner.division_operator()
    }

    fn higher_order_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        self.inner
            .higher_order_function_to_sql_overrides(unparser, func_name, args)
    }

    fn window_func_support_window_frame(
        &self,
        func_name: &str,
        start_bound: &WindowFrameBound,
        end_bound: &WindowFrameBound,
    ) -> bool {
        self.inner
            .window_func_support_window_frame(func_name, start_bound, end_bound)
    }

    fn union_distinct_set_quantifier(&self) -> ast::SetQuantifier {
        self.inner.union_distinct_set_quantifier()
    }

    fn full_qualified_col(&self) -> bool {
        self.inner.full_qualified_col()
    }

    fn unnest_as_table_factor(&self) -> bool {
        self.inner.unnest_as_table_factor()
    }

    fn unnest_as_lateral_flatten(&self) -> bool {
        self.inner.unnest_as_lateral_flatten()
    }

    fn col_alias_overrides(&self, alias: &str) -> Result<Option<String>> {
        self.inner.col_alias_overrides(alias)
    }

    fn supports_qualify(&self) -> bool {
        self.inner.supports_qualify()
    }

    fn timestamp_with_tz_to_string(&self, dt: DateTime<Tz>, unit: TimeUnit) -> String {
        self.inner.timestamp_with_tz_to_string(dt, unit)
    }

    fn supports_empty_select_list(&self) -> bool {
        self.inner.supports_empty_select_list()
    }

    /// `ClickHouse` reads `\` as an escape character in every string literal,
    /// so each one is written twice. The SQL printer doubles each `'`.
    fn string_literal_to_sql(&self, s: &str) -> Option<ast::Expr> {
        Some(ast::Expr::value(ast::Value::SingleQuotedString(
            s.replace('\\', "\\\\"),
        )))
    }

    fn group_by_matches_select_subexpressions(&self) -> bool {
        self.inner.group_by_matches_select_subexpressions()
    }

    fn range_window_default_nulls_first(&self, asc: bool) -> Option<bool> {
        self.inner.range_window_default_nulls_first(asc)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Expr, col};
    use datafusion::sql::sqlparser::ast::{self, BinaryOperator, SetExpr, Statement, Value};
    use datafusion::sql::sqlparser::dialect::ClickHouseDialect as ClickHouseParser;
    use datafusion::sql::sqlparser::parser::Parser;
    use datafusion::sql::unparser::Unparser;

    use super::ClickHouseDialect;

    const VALUES: &[&str] = &[
        "a\\'b",
        "a\\\\'b",
        "trail\\",
        "C:\\new",
        "x'y",
        "it''s",
        "\\N",
        "\\x41",
        "nul\0byte",
        "日本\\語",
    ];

    /// The `WHERE` clause of `SELECT 1 FROM t WHERE <condition>` as a
    /// backslash-escaping engine reads it.
    fn read_where(condition: &str) -> ast::Expr {
        let sql = format!("SELECT 1 FROM t WHERE {condition}");
        let statements =
            Parser::parse_sql(&ClickHouseParser {}, &sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("{sql}: not one query");
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("{sql}: not a select");
        };
        let mut selection = select.selection.clone().expect("a WHERE clause");
        while let ast::Expr::Nested(inner) = selection {
            selection = *inner;
        }
        selection
    }

    fn string_value(expr: &ast::Expr) -> &str {
        match expr {
            ast::Expr::Value(value) => match &value.value {
                Value::SingleQuotedString(s) => s,
                other => panic!("not a string literal: {other:?}"),
            },
            other => panic!("not a literal: {other:?}"),
        }
    }

    #[test]
    fn clickhouse_reads_back_each_string_value() {
        let dialect = ClickHouseDialect::new();
        let unparser = Unparser::new(&dialect);
        for value in VALUES {
            for scalar in [
                ScalarValue::Utf8(Some((*value).to_string())),
                ScalarValue::LargeUtf8(Some((*value).to_string())),
                ScalarValue::Utf8View(Some((*value).to_string())),
                ScalarValue::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(ScalarValue::Utf8(Some((*value).to_string()))),
                ),
            ] {
                let condition = unparser
                    .expr_to_sql(&col("s").eq(Expr::Literal(scalar, None)))
                    .expect("unparse")
                    .to_string();
                let ast::Expr::BinaryOp { op, right, .. } = read_where(&condition) else {
                    panic!("{condition}: not one comparison");
                };
                assert_eq!(op, BinaryOperator::Eq, "{condition}");
                assert_eq!(string_value(&right), *value, "{condition}");
            }
        }
    }

    #[test]
    fn clickhouse_reads_back_each_in_list_value() {
        let dialect = ClickHouseDialect::new();
        let list = VALUES
            .iter()
            .map(|value| Expr::Literal(ScalarValue::Utf8(Some((*value).to_string())), None))
            .collect();
        let condition = Unparser::new(&dialect)
            .expr_to_sql(&col("s").in_list(list, false))
            .expect("unparse")
            .to_string();
        let ast::Expr::InList { list, .. } = read_where(&condition) else {
            panic!("{condition}: not one IN list");
        };
        let read: Vec<&str> = list.iter().map(string_value).collect();
        assert_eq!(read, VALUES, "{condition}");
    }
}
