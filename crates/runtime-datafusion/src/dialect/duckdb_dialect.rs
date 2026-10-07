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

use std::sync::Arc;

use chrono::DateTime;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::Result;
use datafusion::logical_expr::{Expr, SortExpr};
use datafusion::sql::sqlparser::ast::{self, BinaryOperator, WindowFrameBound};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{
    CharacterLengthStyle, DateFieldExtractStyle, Dialect, DistinctFromStyle, DuckDBDialect,
    IntervalStyle, ScalarFnToSqlHandler,
};

use super::duckdb::ordered_aggregate_to_sql;

/// [`DuckDBDialect`] plus the argument-list `ORDER BY` that
/// [`ordered_aggregate_to_sql`] renders.
///
/// `DuckDBDialect` takes scalar renderings but has no seam for an aggregate one, so
/// this wraps it, forwarding every other [`Dialect`] method to the inner dialect.
/// **Every** method is forwarded explicitly: inheriting a trait default here would
/// silently unparse `DuckDB` SQL as if it were the generic dialect, changing quoting,
/// casts and interval rendering with no error anywhere.
pub(crate) struct SpiceDuckDBDialect {
    inner: DuckDBDialect,
}

impl SpiceDuckDBDialect {
    pub(crate) fn new(inner: DuckDBDialect) -> Self {
        Self { inner }
    }
}

/// Every `Dialect` method is forwarded explicitly, and the lint keeps it that way.
///
/// An unlisted method falls back to the *trait default*, which is the generic
/// rendering rather than `DuckDB`'s — so a method added upstream would silently
/// revert `DuckDB` to generic SQL, with no error anywhere. Denying
/// `missing_trait_methods` turns that into a compile failure instead.
#[deny(clippy::missing_trait_methods)]
impl Dialect for SpiceDuckDBDialect {
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
        if let Some(rendered) =
            ordered_aggregate_to_sql(unparser, func_name, args, distinct, filter, order_by)?
        {
            return Ok(Some(rendered));
        }
        self.inner.aggregate_function_to_sql_overrides(
            unparser, func_name, args, distinct, filter, order_by,
        )
    }

    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        self.inner.identifier_quote_style(identifier)
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

    fn integer_division_to_sql(&self, lhs: ast::Expr, rhs: ast::Expr) -> Option<ast::Expr> {
        self.inner.integer_division_to_sql(lhs, rhs)
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

    fn string_literal_to_sql(&self, s: &str) -> Option<ast::Expr> {
        self.inner.string_literal_to_sql(s)
    }

    fn group_by_matches_select_subexpressions(&self) -> bool {
        self.inner.group_by_matches_select_subexpressions()
    }

    fn range_window_default_nulls_first(&self, asc: bool) -> Option<bool> {
        self.inner.range_window_default_nulls_first(asc)
    }
}
