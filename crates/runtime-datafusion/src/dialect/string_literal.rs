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

//! String literals that survive `sqlparser`'s quote escaping.
//!
//! `sqlparser` renders `Value::SingleQuotedString` by doubling each `'`, but it
//! leaves a quote alone when the character before it is a backslash (it assumes
//! `\'` is already escaped) and when the next character is another quote (it
//! assumes `''` is already escaped). Neither assumption holds for a *value*: the
//! string `a\'b` renders as `'a\'b'`, which `DuckDB` and `SQLite` read as the
//! string `a\` followed by stray SQL (a parser error), and the string `a''b`
//! renders as `'a''b'`, which reads back as `a'b` — a filter on the wrong value.
//!
//! Neither engine treats backslash as an escape in a standard string literal, so
//! the only quoting needed is doubling. When a value contains one of the two
//! shapes the renderer mishandles, it is rendered as a concatenation of
//! quote-free pieces and lone-quote literals instead, e.g.
//! `('a\' || '''' || 'b')`. A lone `'` renders as `''''` because it has nothing
//! before or after it to trip either skip.

use datafusion::sql::sqlparser::ast::{self, BinaryOperator, Value};

/// Render `s` as a string literal that reads back as exactly `s`, or `None` when
/// the default single-quoted rendering already does.
#[must_use]
pub fn quote_safe_string_literal(s: &str) -> Option<ast::Expr> {
    if !s.contains("\\'") && !s.contains("''") {
        return None;
    }

    let mut pieces: Vec<ast::Expr> = Vec::new();
    for (i, part) in s.split('\'').enumerate() {
        if i > 0 {
            pieces.push(ast::Expr::value(Value::SingleQuotedString("'".to_string())));
        }
        if !part.is_empty() {
            pieces.push(ast::Expr::value(Value::SingleQuotedString(
                part.to_string(),
            )));
        }
    }

    let mut iter = pieces.into_iter();
    let first = iter.next()?;
    let concatenated = iter.fold(first, |left, right| ast::Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::StringConcat,
        right: Box::new(right),
    });
    Some(ast::Expr::Nested(Box::new(concatenated)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered(s: &str) -> String {
        quote_safe_string_literal(s).map_or_else(
            || ast::Expr::value(Value::SingleQuotedString(s.to_string())).to_string(),
            |expr| expr.to_string(),
        )
    }

    /// Read a rendering back the way `DuckDB` and `SQLite` do: standard string
    /// literals with `''` as the only escape, joined by `||`.
    fn read_back(sql: &str) -> String {
        let sql = sql.trim();
        let sql = sql
            .strip_prefix('(')
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or(sql);
        let mut out = String::new();
        let mut chars = sql.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '\'' => loop {
                    match chars.next() {
                        Some('\'') if chars.peek() == Some(&'\'') => {
                            chars.next();
                            out.push('\'');
                        }
                        Some('\'') => break,
                        Some(other) => out.push(other),
                        None => panic!("unterminated string literal in {sql}"),
                    }
                },
                ' ' | '|' => {}
                other => panic!("unexpected `{other}` outside a literal in {sql}"),
            }
        }
        out
    }

    #[test]
    fn values_round_trip_through_standard_string_literals() {
        for value in [
            "plain",
            r"a\b",
            r"C:\path\to\file",
            "a'b",
            r"a\'b",
            r"\'",
            r"a\''b",
            "a''b",
            "''",
            "'",
            "'leading",
            "trailing'",
            r"trailing\",
            r"\\'",
            "",
        ] {
            let sql = rendered(value);
            assert_eq!(read_back(&sql), value, "{value:?} rendered as {sql}");
        }
    }

    #[test]
    fn backslash_then_quote_is_doubled() {
        assert_eq!(rendered(r"a\'b"), r"('a\' || '''' || 'b')");
    }

    #[test]
    fn plain_and_single_quote_values_keep_the_default_rendering() {
        assert!(quote_safe_string_literal(r"a\b").is_none());
        assert!(quote_safe_string_literal("a'b").is_none());
        assert_eq!(rendered("a'b"), "'a''b'");
    }
}
