/*
Copyright 2026 The Spice.ai OSS Authors

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

//! String literals for engines that read `\` as an escape character inside a
//! quoted string: Spark SQL, Databricks, Snowflake and `BigQuery`.

use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::datatypes::TimeUnit;
use chrono::DateTime;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, SortExpr};
use datafusion::sql::sqlparser::ast::{self, BinaryOperator, WindowFrameBound};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{
    CharacterLengthStyle, DateFieldExtractStyle, Dialect, DistinctFromStyle, FloatLiteralStyle,
    IntervalStyle, ScalarFnToSqlHandler,
};

/// How a string literal writes a `'` that is part of its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteEscape {
    /// As `''`, which the SQL printer writes for every `'`. Snowflake reads
    /// `''` as one quote.
    Doubled,
    /// As `\u0027`. Spark SQL reads `''` as two adjacent literals, so
    /// `'a''b'` is `ab`, and `BigQuery` reads `'''` as the start of a
    /// triple-quoted string. A `$` is also written as `\u0024`, because Spark
    /// replaces each `${...}` in the SQL text before it reads a literal, and a
    /// line break as `\n` or `\r`, which a `BigQuery` single-quoted literal
    /// cannot hold.
    Unicode,
}

/// A single-quoted string literal that an engine reading `\` as an escape
/// character reads back as exactly `s`.
#[must_use]
pub fn backslash_escaped_string_literal(s: &str, quote: QuoteEscape) -> ast::Expr {
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => escaped.push_str(r"\\"),
            '\'' if quote == QuoteEscape::Unicode => escaped.push_str(r"\u0027"),
            '$' if quote == QuoteEscape::Unicode => escaped.push_str(r"\u0024"),
            '\n' if quote == QuoteEscape::Unicode => escaped.push_str(r"\n"),
            '\r' if quote == QuoteEscape::Unicode => escaped.push_str(r"\r"),
            c => escaped.push(c),
        }
    }
    ast::Expr::value(ast::Value::SingleQuotedString(escaped))
}

/// Refuses SQL text for Spark SQL or Databricks that holds `${`.
///
/// These engines replace each `${...}` anywhere in the SQL text before they
/// parse it, quoted identifiers and time zone names included, and an
/// identifier has no escape for `$`. A string literal written with
/// [`QuoteEscape::Unicode`] never holds `${`.
///
/// # Errors
///
/// [`DataFusionError::NotImplemented`] when `sql` holds `${`.
pub fn refuse_variable_reference(sql: &str) -> Result<()> {
    if sql.contains("${") {
        return Err(DataFusionError::NotImplemented(
            "SQL that holds ${ cannot be sent to Spark or Databricks, because they substitute variables written as ${...} in SQL text".to_string(),
        ));
    }
    Ok(())
}

/// `inner`, with every string literal written by
/// [`backslash_escaped_string_literal`].
#[derive(Debug)]
pub struct BackslashEscapedStrings<D> {
    inner: D,
    quote: QuoteEscape,
}

impl<D: Dialect> BackslashEscapedStrings<D> {
    #[must_use]
    pub fn new(inner: D, quote: QuoteEscape) -> Self {
        Self { inner, quote }
    }
}

impl<D: Dialect> Dialect for BackslashEscapedStrings<D> {
    fn string_literal_to_sql(&self, s: &str) -> Option<ast::Expr> {
        Some(backslash_escaped_string_literal(s, self.quote))
    }

    fn with_custom_scalar_overrides(self, handlers: Vec<(&str, ScalarFnToSqlHandler)>) -> Self {
        Self {
            inner: self.inner.with_custom_scalar_overrides(handlers),
            quote: self.quote,
        }
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

    fn timestamp_literal_max_subsecond_digits(&self) -> Option<usize> {
        self.inner.timestamp_literal_max_subsecond_digits()
    }

    fn string_to_timestamp_to_sql(
        &self,
        value: ast::Expr,
        tz: Option<&Arc<str>>,
    ) -> Option<ast::Expr> {
        self.inner.string_to_timestamp_to_sql(value, tz)
    }

    fn supports_recursive_cte(&self) -> bool {
        self.inner.supports_recursive_cte()
    }

    fn supports_distinct_recursive_cte(&self) -> bool {
        self.inner.supports_distinct_recursive_cte()
    }

    fn string_to_date_to_sql(&self, value: ast::Expr) -> Option<ast::Expr> {
        self.inner.string_to_date_to_sql(value)
    }

    fn requires_explicit_comparison_coercion(&self) -> bool {
        self.inner.requires_explicit_comparison_coercion()
    }

    fn date_to_integer_to_sql(&self, date: ast::Expr) -> Option<ast::Expr> {
        self.inner.date_to_integer_to_sql(date)
    }

    fn date_difference_to_sql(&self, lhs: ast::Expr, rhs: ast::Expr) -> Option<ast::Expr> {
        self.inner.date_difference_to_sql(lhs, rhs)
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

    fn requires_derived_table_alias(&self) -> bool {
        self.inner.requires_derived_table_alias()
    }

    fn derived_table_evaluates_volatile_outputs_once(&self) -> bool {
        self.inner.derived_table_evaluates_volatile_outputs_once()
    }

    fn identifier_max_length(&self) -> Option<usize> {
        self.inner.identifier_max_length()
    }

    fn division_operator(&self) -> BinaryOperator {
        self.inner.division_operator()
    }

    fn integer_division_to_sql(&self, lhs: ast::Expr, rhs: ast::Expr) -> Option<ast::Expr> {
        self.inner.integer_division_to_sql(lhs, rhs)
    }

    fn string_concat_to_sql(&self, lhs: ast::Expr, rhs: ast::Expr) -> Option<ast::Expr> {
        self.inner.string_concat_to_sql(lhs, rhs)
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

    fn group_by_matches_select_subexpressions(&self) -> bool {
        self.inner.group_by_matches_select_subexpressions()
    }

    fn range_window_default_nulls_first(&self, asc: bool) -> Option<bool> {
        self.inner.range_window_default_nulls_first(asc)
    }
}

/// Readers that model how each engine reads a string literal back, for tests
/// that assert a rendered literal holds exactly its value and nothing after it.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::{Column, DataFusionError, Result};
    use datafusion::logical_expr::builder::LogicalTableSource;
    use datafusion::logical_expr::expr::Like;
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, cast};
    use datafusion::prelude::{Expr, col, lit};
    use datafusion::sql::sqlparser::dialect::{
        BigQueryDialect, DatabricksDialect, Dialect as ParserDialect,
    };
    use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

    /// Quotes and backslashes in the orders a string literal grammar treats
    /// differently: a lone and a doubled quote, a lone backslash, a backslash
    /// before a quote, an escaped backslash before a quote, a backslash before
    /// a letter, a name with a quote, and a value that ends the literal early
    /// wherever `\'` is read as an escaped quote; then a Spark variable
    /// reference and a line break.
    pub const VALUES: [&str; 10] = [
        "'",
        "''",
        r"\",
        r"\'",
        r"\\'",
        r"a\nb",
        "O'Brien",
        r"x\') OR 1=1 --",
        "${x}",
        "a\r\nb",
    ];

    /// Each of [`VALUES`] written with [`super::QuoteEscape::Unicode`].
    pub const UNICODE_QUOTE_LITERALS: [&str; 10] = [
        r"'\u0027'",
        r"'\u0027\u0027'",
        r"'\\'",
        r"'\\\u0027'",
        r"'\\\\\u0027'",
        r"'a\\nb'",
        r"'O\u0027Brien'",
        r"'x\\\u0027) OR 1=1 --'",
        r"'\u0024{x}'",
        r"'a\r\nb'",
    ];

    /// Each of [`VALUES`] written with [`super::QuoteEscape::Doubled`].
    pub const DOUBLED_QUOTE_LITERALS: [&str; 10] = [
        "''''",
        "''''''",
        r"'\\'",
        r"'\\'''",
        r"'\\\\'''",
        r"'a\\nb'",
        "'O''Brien'",
        r"'x\\'') OR 1=1 --'",
        "'${x}'",
        "'a\r\nb'",
    ];

    /// Asserts that `unparse` writes column `c` compared with each of
    /// [`VALUES`] as `<column> = <literal>`, and a `LIKE` that escapes with `\`
    /// as `<column> LIKE 'a' ESCAPE <literal of \>`, each bare or in one pair of
    /// parentheses, where `literals` holds each value's literal; and that `read`
    /// reads each literal back as its value.
    ///
    /// # Panics
    ///
    /// When any of those does not hold.
    pub fn assert_string_literals(
        unparse: impl Fn(&Expr) -> String,
        column: &str,
        literals: [&str; 10],
        read: impl Fn(&str) -> String,
    ) {
        let assert_written = |expr: &Expr, expected: &str| {
            let sql = unparse(expr);
            assert!(
                sql == expected || sql == format!("({expected})"),
                "{expr} written as {sql}, expected {expected}"
            );
        };
        for (value, literal) in VALUES.into_iter().zip(literals) {
            assert_written(&col("c").eq(lit(value)), &format!("{column} = {literal}"));
            assert_eq!(read(literal), value, "{value:?} written as {literal}");
        }
        let like = Expr::Like(Like::new(
            false,
            Box::new(col("c")),
            Box::new(lit("a")),
            Some('\\'),
            false,
        ));
        assert_written(&like, &format!("{column} LIKE 'a' ESCAPE {}", literals[2]));
    }

    /// The value of `sql` read as exactly one string literal by the sqlparser
    /// tokenizer for `dialect`, with escapes applied.
    ///
    /// # Panics
    ///
    /// When `sql` is not exactly one string literal.
    #[must_use]
    pub fn read_with_tokenizer(dialect: &dyn ParserDialect, sql: &str) -> String {
        let tokens = Tokenizer::new(dialect, sql)
            .tokenize()
            .unwrap_or_else(|e| panic!("{sql} does not tokenize: {e}"));
        match tokens.as_slice() {
            [Token::SingleQuotedString(value)] => value.clone(),
            other => panic!("{sql} is not one string literal: {other:?}"),
        }
    }

    /// The value of `sql` read as exactly one Spark SQL string literal: the
    /// lexer rule `'\'' ( ~('\''|'\\') | ('\\' .) )* '\''`, then the body
    /// decoded as Spark 3.5's `SparkParserUtils.unescapeSQLString` decodes it.
    ///
    /// # Panics
    ///
    /// When `sql` is not exactly one Spark SQL string literal, or holds `${`,
    /// which Spark's variable substitution replaces before it reads the SQL.
    #[must_use]
    pub fn read_spark(sql: &str) -> String {
        assert!(!sql.contains("${"), "Spark substitutes variables in {sql}");
        decode_spark(&literal_body(&SparkLiteral, sql))
    }

    /// The values of every string literal in `sql`, as the sqlparser tokenizer
    /// for `dialect` reads them with escapes applied.
    ///
    /// # Panics
    ///
    /// When `sql` does not tokenize.
    #[must_use]
    pub fn string_literals_with_tokenizer(dialect: &dyn ParserDialect, sql: &str) -> Vec<String> {
        Tokenizer::new(dialect, sql)
            .tokenize()
            .unwrap_or_else(|e| panic!("{sql} does not tokenize: {e}"))
            .into_iter()
            .filter_map(|token| match token {
                Token::SingleQuotedString(value) => Some(value),
                _ => None,
            })
            .collect()
    }

    /// The values of every string literal in `sql`, as Spark SQL reads them.
    ///
    /// # Panics
    ///
    /// When `sql` does not tokenize, holds `${`, or a literal holds `''`.
    #[must_use]
    pub fn spark_string_literals(sql: &str) -> Vec<String> {
        assert!(!sql.contains("${"), "Spark substitutes variables in {sql}");
        Tokenizer::new(&SparkLiteral, sql)
            .with_unescape(false)
            .tokenize()
            .unwrap_or_else(|e| panic!("{sql} does not tokenize: {e}"))
            .into_iter()
            .filter_map(|token| match token {
                Token::SingleQuotedString(body) => {
                    assert!(!body.contains("''"), "{sql} holds `''`");
                    Some(decode_spark(&body))
                }
                _ => None,
            })
            .collect()
    }

    /// A Spark SQL literal body decoded as Spark 3.5's
    /// `SparkParserUtils.unescapeSQLString` decodes it.
    fn decode_spark(body: &str) -> String {
        let code_points = CodePoints {
            hex: &[('u', 4), ('U', 8)],
            octal_lead: '0'..='1',
        };
        decode(body, &code_points, |escape, out| match escape {
            '0' => out.push('\0'),
            'b' => out.push('\u{8}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            'Z' => out.push('\u{1a}'),
            '%' | '_' => {
                out.push('\\');
                out.push(escape);
            }
            other => out.push(other),
        })
    }

    /// The value of `sql` read as exactly one `BigQuery` single-quoted string
    /// literal, with the escape sequences of the `GoogleSQL` lexical reference.
    ///
    /// # Panics
    ///
    /// When `sql` is not exactly one `BigQuery` string literal, or holds a line
    /// break or an escape `BigQuery` rejects.
    #[must_use]
    pub fn read_bigquery(sql: &str) -> String {
        let body = literal_body(&BigQueryDialect {}, sql);
        assert!(
            !body.contains(['\n', '\r']),
            "BigQuery rejects a line break in {sql}"
        );
        let code_points = CodePoints {
            hex: &[('u', 4), ('U', 8), ('x', 2), ('X', 2)],
            octal_lead: '0'..='3',
        };
        decode(&body, &code_points, |escape, out| {
            out.push(match escape {
                'a' => '\u{7}',
                'b' => '\u{8}',
                'f' => '\u{c}',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'v' => '\u{b}',
                '\\' | '?' | '"' | '\'' | '`' => escape,
                other => panic!("BigQuery rejects the escape \\{other} in {body}"),
            });
        })
    }

    /// The raw body of the one single-quoted literal `sql` holds, read with
    /// `\` escaping the next character. A `''` in the body is refused: Spark
    /// reads it as two adjacent literals and `BigQuery` rejects it.
    fn literal_body(dialect: &dyn ParserDialect, sql: &str) -> String {
        let tokens = Tokenizer::new(dialect, sql)
            .with_unescape(false)
            .tokenize()
            .unwrap_or_else(|e| panic!("{sql} does not tokenize: {e}"));
        let [Token::SingleQuotedString(body)] = tokens.as_slice() else {
            panic!("{sql} is not one string literal: {tokens:?}");
        };
        assert!(!body.contains("''"), "{sql} holds `''`");
        body.clone()
    }

    #[derive(Debug)]
    struct SparkLiteral;

    impl ParserDialect for SparkLiteral {
        fn is_identifier_start(&self, ch: char) -> bool {
            DatabricksDialect {}.is_identifier_start(ch)
        }

        fn is_identifier_part(&self, ch: char) -> bool {
            DatabricksDialect {}.is_identifier_part(ch)
        }

        fn supports_string_literal_backslash_escape(&self) -> bool {
            true
        }
    }

    /// The escapes that spell a code point: a letter and that many hex digits,
    /// or three octal digits whose first is in `octal_lead`.
    struct CodePoints {
        hex: &'static [(char, usize)],
        octal_lead: std::ops::RangeInclusive<char>,
    }

    /// Decodes `body`: a backslash before a code point in `code_points` is that
    /// character, and `escape` writes what any other backslash escapes. A
    /// trailing lone backslash is kept.
    fn decode(body: &str, code_points: &CodePoints, escape: impl Fn(char, &mut String)) -> String {
        let chars: Vec<char> = body.chars().collect();
        let number = |from: usize, len: usize, radix: u32| -> Option<char> {
            let digits = chars.get(from..from + len)?;
            if !digits.iter().all(|c| c.is_digit(radix)) {
                return None;
            }
            let value = digits
                .iter()
                .filter_map(|c| c.to_digit(radix))
                .fold(0, |n, d| n * radix + d);
            char::from_u32(value)
        };
        let mut out = String::new();
        let mut i = 0;
        while let Some(&c) = chars.get(i) {
            let Some(&next) = chars.get(i + 1).filter(|_| c == '\\') else {
                out.push(c);
                i += 1;
                continue;
            };
            let code_point = if code_points.octal_lead.contains(&next) {
                number(i + 1, 3, 8).map(|c| (c, 4))
            } else {
                code_points
                    .hex
                    .iter()
                    .find(|(letter, _)| *letter == next)
                    .and_then(|&(_, len)| number(i + 2, len, 16).map(|c| (c, len + 2)))
            };
            if let Some((decoded, width)) = code_point {
                out.push(decoded);
                i += width;
            } else {
                escape(next, &mut out);
                i += 2;
            }
        }
        out
    }

    fn cast_to_zone_holding_a_variable_reference() -> Expr {
        cast(
            col("ts"),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("${x}".into())),
        )
    }

    /// Filters over `t(c STRING, ts TIMESTAMP)` that Spark SQL and Databricks
    /// would substitute a variable in: on a column whose name holds `${`, and
    /// on a cast to a time zone whose name holds `${`; then a filter whose only
    /// `${` is in a string literal value.
    #[must_use]
    pub fn variable_reference_filters() -> ([Expr; 2], Expr) {
        (
            [
                Expr::Column(Column::from_name("${x}")).is_not_null(),
                cast_to_zone_holding_a_variable_reference().is_not_null(),
            ],
            col("c").eq(lit("${x}")),
        )
    }

    /// Plans over `t(c STRING, ts TIMESTAMP)` that Spark SQL and Databricks
    /// would substitute a variable in: a column alias that holds `${`, and a
    /// cast to a time zone whose name holds `${`; then a plan whose only `${`
    /// is in a string literal value.
    ///
    /// # Panics
    ///
    /// When a plan does not build.
    #[must_use]
    pub fn variable_reference_plans() -> ([LogicalPlan; 2], LogicalPlan) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Utf8, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let scan = || {
            LogicalPlanBuilder::scan(
                "t",
                Arc::new(LogicalTableSource::new(Arc::clone(&schema))),
                None,
            )
        };
        let build = |builder: Result<LogicalPlanBuilder>| {
            builder
                .and_then(LogicalPlanBuilder::build)
                .unwrap_or_else(|e| panic!("plan does not build: {e}"))
        };
        (
            [
                build(scan().and_then(|scan| scan.project(vec![col("c").alias("${x}")]))),
                build(scan().and_then(|scan| {
                    scan.filter(cast_to_zone_holding_a_variable_reference().is_not_null())
                })),
            ],
            build(scan().and_then(|scan| scan.filter(col("c").eq(lit("${x}"))))),
        )
    }

    /// Asserts that `sent` is refused with `NotImplemented`, or is SQL that
    /// holds no `${`.
    ///
    /// # Panics
    ///
    /// When `sent` is SQL that holds `${`, or another error.
    pub fn assert_no_variable_reference_sent(sent: Result<String>) {
        match sent {
            Err(DataFusionError::NotImplemented(_)) => {}
            Ok(sql) => assert!(!sql.contains("${"), "{sql} holds a variable reference"),
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::SnowflakeDialect;

    use super::test_support::{
        DOUBLED_QUOTE_LITERALS, UNICODE_QUOTE_LITERALS, VALUES, read_bigquery, read_spark,
        read_with_tokenizer,
    };
    use super::*;

    #[test]
    fn spark_and_bigquery_read_every_value_back() {
        for (value, literal) in VALUES.into_iter().zip(UNICODE_QUOTE_LITERALS) {
            let sql = backslash_escaped_string_literal(value, QuoteEscape::Unicode).to_string();
            assert_eq!(sql, literal, "{value:?}");
            assert_eq!(read_spark(&sql), value, "{value:?} written as {sql}");
            assert_eq!(read_bigquery(&sql), value, "{value:?} written as {sql}");
        }
    }

    #[test]
    fn snowflake_reads_every_value_back() {
        for (value, literal) in VALUES.into_iter().zip(DOUBLED_QUOTE_LITERALS) {
            let sql = backslash_escaped_string_literal(value, QuoteEscape::Doubled).to_string();
            assert_eq!(sql, literal, "{value:?}");
            assert_eq!(read_with_tokenizer(&SnowflakeDialect, &sql), value);
        }
    }
}
