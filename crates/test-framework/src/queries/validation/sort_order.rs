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

//! Verify that a result set honors its own query's top-level `ORDER BY`.
//!
//! This is a **self-check on one engine's output**, not a comparison between two
//! engines: it needs no oracle, so it applies on every lane, including the ones
//! that compare against a single reference engine.
//!
//! It exists because multiset content equality — the comparison
//! [`super::compare_query_result_batches`] performs under
//! [`super::RowOrder::Multiset`] — canonically sorts both sides before comparing
//! and therefore says nothing about the order an engine actually returned. A
//! query whose sort is wrong and whose rows are otherwise right compares equal.
//! Checking each side against its own `ORDER BY` closes that gap without
//! reintroducing the reason multiset equality is used in the first place:
//! an `ORDER BY` on a non-unique key leaves the relative order of tied rows
//! engine-dependent, and tied rows never violate the check below.
//!
//! Two deliberate limits, both of which report themselves rather than passing
//! silently (see [`SortCheck::Skipped`]):
//!
//! - **`NULL` placement is not checked.** Engines disagree on where `NULL`s sort
//!   absent an explicit `NULLS FIRST`/`NULLS LAST` — `DataFusion` and `PostgreSQL`
//!   put them last for `ASC`, `SQLite` puts them first — so once a key column
//!   holds a `NULL` on either side of a row pair, that pair is left unchecked.
//!   Later key columns are not consulted for it either: SQL never reaches them
//!   once an earlier column separates two rows.
//! - **Only keys that map onto output columns are checked.** An `ORDER BY` over
//!   an expression absent from the projection resolves to
//!   [`SortKeyResolution::Unresolved`].

use std::cmp::Ordering;

use anyhow::Result;
use arrow::array::{Array, RecordBatch, make_comparator};
use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use datafusion::sql::sqlparser::ast::{
    Expr, OrderBy, OrderByKind, Query as SqlQuery, SelectItem, SetExpr, Statement, Value,
};
use datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect, PostgreSqlDialect};
use datafusion::sql::sqlparser::parser::Parser;

use super::array_value_to_string;

/// One resolved `ORDER BY` term, mapped onto a result column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKeyColumn {
    /// Position in the result schema.
    pub index: usize,
    /// Result column name, for failure messages.
    pub name: String,
    /// `true` for `DESC`.
    pub descending: bool,
}

/// Outcome of mapping a query's top-level `ORDER BY` onto its result columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortKeyResolution {
    /// No top-level `ORDER BY`: the engine may return rows in any order.
    Unordered,
    /// The `ORDER BY` maps onto these result columns, in significance order.
    Resolved(Vec<SortKeyColumn>),
    /// A top-level `ORDER BY` is present but did not map onto result columns.
    /// Row order goes unchecked; `reason` says why so the hole stays countable.
    Unresolved { reason: String },
}

/// A row that breaks the query's `ORDER BY`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortOrderViolation {
    /// Result column whose values are out of order.
    pub column: String,
    /// 1-based position of the row that sorts before its predecessor.
    pub row_number: usize,
    /// Key value on the preceding row.
    pub previous: String,
    /// Key value on the offending row.
    pub current: String,
}

/// Result of checking one engine's rows against its own `ORDER BY`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortCheck {
    /// Rows are ordered per the resolved key.
    Ordered,
    /// A row sorts before its predecessor.
    Violation(SortOrderViolation),
    /// The check did not run. Not a pass — a hole, named so it can be counted.
    Skipped { reason: String },
}

/// Parse `sql` and map its top-level `ORDER BY` onto the columns of `schema`.
///
/// `schema` is the result schema of the query, so a term is resolved by output
/// position: an ordinal (`ORDER BY 2`), a name or alias carried into the result
/// (`ORDER BY revenue`), or an expression that matches a projection item
/// textually (`ORDER BY sum(l_quantity)`).
#[must_use]
pub fn resolve_sort_key(sql: &str, schema: &SchemaRef) -> SortKeyResolution {
    let Some(statement) = parse_one_statement(sql) else {
        return SortKeyResolution::Unresolved {
            reason: "SQL did not parse as a single statement".to_string(),
        };
    };
    let Statement::Query(query) = statement else {
        return SortKeyResolution::Unordered;
    };
    let Some(order_by) = query.order_by.as_ref() else {
        return SortKeyResolution::Unordered;
    };
    resolve_order_by(order_by, &query, schema)
}

/// Try each dialect the corpus is written in; the first that yields exactly one
/// statement wins. A query no dialect parses resolves as `Unresolved`, never as
/// ordered.
fn parse_one_statement(sql: &str) -> Option<Statement> {
    let dialects: [&dyn Dialect; 2] = [&PostgreSqlDialect {}, &GenericDialect {}];
    for dialect in dialects {
        if let Ok(mut statements) = Parser::parse_sql(dialect, sql)
            && statements.len() == 1
        {
            return Some(statements.remove(0));
        }
    }
    None
}

fn resolve_order_by(order_by: &OrderBy, query: &SqlQuery, schema: &SchemaRef) -> SortKeyResolution {
    let OrderByKind::Expressions(terms) = &order_by.kind else {
        return SortKeyResolution::Unresolved {
            reason: "ORDER BY ALL is not mapped to result columns".to_string(),
        };
    };
    if terms.is_empty() {
        return SortKeyResolution::Unordered;
    }

    let projection = positional_projection(leftmost_projection(&query.body), schema);
    let mut key = Vec::with_capacity(terms.len());
    for term in terms {
        let Some(index) = resolve_term(&term.expr, projection, schema) else {
            return SortKeyResolution::Unresolved {
                reason: format!(
                    "ORDER BY term '{}' does not map to a result column",
                    term.expr
                ),
            };
        };
        key.push(SortKeyColumn {
            index,
            name: schema.field(index).name().clone(),
            descending: term.options.asc == Some(false),
        });
    }
    SortKeyResolution::Resolved(key)
}

/// Projection of the leftmost `SELECT` in a set expression. A top-level
/// `ORDER BY` over a `UNION` applies to the whole result, whose column names
/// come from the first branch.
fn leftmost_projection(body: &SetExpr) -> Option<&[SelectItem]> {
    match body {
        SetExpr::Select(select) => Some(&select.projection),
        SetExpr::Query(query) => leftmost_projection(&query.body),
        SetExpr::SetOperation { left, .. } => leftmost_projection(left),
        _ => None,
    }
}

/// A projection is only usable for resolving by *position* when every item
/// contributes exactly one output column in order. A wildcard expands to an
/// unknown number of columns, so its neighbours' positions no longer line up
/// with the result — resolving through it would name the wrong column and could
/// report a violation on a correctly sorted result.
fn positional_projection<'a>(
    projection: Option<&'a [SelectItem]>,
    schema: &SchemaRef,
) -> Option<&'a [SelectItem]> {
    let items = projection?;
    if items.len() != schema.fields().len() {
        return None;
    }
    items
        .iter()
        .all(|item| {
            matches!(
                item,
                SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. }
            )
        })
        .then_some(items)
}

fn resolve_term(
    expr: &Expr,
    projection: Option<&[SelectItem]>,
    schema: &SchemaRef,
) -> Option<usize> {
    // `ORDER BY 2` — a 1-based output ordinal.
    if let Expr::Value(value) = expr
        && let Value::Number(digits, _) = &value.value
        && let Ok(ordinal) = digits.parse::<usize>()
        && ordinal >= 1
        && ordinal <= schema.fields().len()
    {
        return Some(ordinal - 1);
    }

    // `ORDER BY revenue` / `ORDER BY o.o_orderdate` — a name that survives into
    // the result schema, or a projection alias.
    if let Some(name) = simple_name(expr) {
        if let Some(index) = field_index(schema, name) {
            return Some(index);
        }
        if let Some(index) = projection.and_then(|items| alias_index(items, name)) {
            return Some(index);
        }
    }

    // `ORDER BY sum(l_quantity)` — an expression repeated from the projection.
    projection.and_then(|items| expression_index(items, expr))
}

/// Trailing identifier of a bare or qualified column reference.
fn simple_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.as_str()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.as_str()),
        _ => None,
    }
}

fn field_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name().eq_ignore_ascii_case(name))
}

fn alias_index(items: &[SelectItem], name: &str) -> Option<usize> {
    items.iter().position(|item| match item {
        SelectItem::ExprWithAlias { alias, .. } => alias.value.eq_ignore_ascii_case(name),
        _ => false,
    })
}

/// Position of a projection item whose expression renders identically to `expr`.
/// Rendering both through the parser's own `Display` normalizes the source
/// text's whitespace and casing of keywords.
fn expression_index(items: &[SelectItem], expr: &Expr) -> Option<usize> {
    let wanted = expr.to_string().to_ascii_lowercase();
    items.iter().position(|item| {
        let (SelectItem::UnnamedExpr(projected)
        | SelectItem::ExprWithAlias {
            expr: projected, ..
        }) = item
        else {
            return false;
        };
        projected.to_string().to_ascii_lowercase() == wanted
    })
}

/// Render one cell for a failure message. Only reached on a violation, so a
/// column whose type has no string form degrades the message, never the verdict.
fn value_string(array: &dyn Array, index: usize) -> String {
    match array_value_to_string(array, index) {
        Ok(Some(value)) => value,
        Ok(None) => "NULL".to_string(),
        Err(e) => format!("<unrenderable {}: {e}>", array.data_type()),
    }
}

/// Verify `batches` are ordered by `key`.
///
/// Ties are legal: an `ORDER BY` on a non-unique key leaves the relative order
/// of equal rows engine-dependent, so only a row that sorts strictly *before*
/// its predecessor is a violation. Row pairs where either side of a key column
/// is `NULL` are treated as tied — see the module docs.
///
/// # Errors
/// Returns an error if the batches cannot be concatenated.
pub fn verify_sorted(batches: &[RecordBatch], key: &[SortKeyColumn]) -> Result<SortCheck> {
    if key.is_empty() {
        return Ok(SortCheck::Skipped {
            reason: "empty sort key".to_string(),
        });
    }
    let Some(schema) = batches.first().map(RecordBatch::schema) else {
        return Ok(SortCheck::Ordered);
    };
    let batch = arrow::compute::concat_batches(&schema, batches)?;
    if batch.num_rows() < 2 {
        return Ok(SortCheck::Ordered);
    }

    // One comparator per key column, built once and reused across rows.
    let mut comparators = Vec::with_capacity(key.len());
    for column in key {
        let Some(array) = batch.columns().get(column.index) else {
            return Ok(SortCheck::Skipped {
                reason: format!(
                    "sort key column '{}' at index {} is outside the result schema",
                    column.name, column.index
                ),
            });
        };
        let options = SortOptions {
            descending: column.descending,
            nulls_first: false,
        };
        match make_comparator(array.as_ref(), array.as_ref(), options) {
            Ok(compare) => comparators.push((column, array, compare)),
            Err(e) => {
                return Ok(SortCheck::Skipped {
                    reason: format!(
                        "sort key column '{}' has a type that cannot be compared ({}): {e}",
                        column.name,
                        array.data_type()
                    ),
                });
            }
        }
    }

    for row in 1..batch.num_rows() {
        for (column, array, compare) in &comparators {
            // A `NULL` on either side of this column decides the pair's order in a
            // way the engine chose (`NULLS FIRST`/`NULLS LAST`), so stop rather
            // than falling through to the next column: under SQL the later
            // columns are never consulted once an earlier one separates two rows,
            // and reading them anyway reports a violation on a correctly ordered
            // result. TPC-DS q71 sorts on a `SUM` that is `NULL` for nine rows,
            // and comparing the tiebreaker across that boundary flags it.
            if array.is_null(row - 1) || array.is_null(row) {
                break;
            }
            match compare(row - 1, row) {
                Ordering::Less => break,
                Ordering::Equal => {}
                Ordering::Greater => {
                    return Ok(SortCheck::Violation(SortOrderViolation {
                        column: column.name.clone(),
                        row_number: row + 1,
                        previous: value_string(array.as_ref(), row - 1),
                        current: value_string(array.as_ref(), row),
                    }));
                }
            }
        }
    }

    Ok(SortCheck::Ordered)
}

/// Whether `sql` carries a top-level `ORDER BY`, i.e. whether the order of its
/// result rows is constrained at all.
///
/// Parser-backed, so an `ORDER BY` inside a subquery, a CTE or a window frame —
/// none of which constrain the outer result — does not count. A substring search
/// for `ORDER BY` counts all three.
#[must_use]
pub fn has_top_level_order_by(sql: &str) -> bool {
    match parse_one_statement(sql) {
        Some(Statement::Query(query)) => match query.order_by.as_ref().map(|o| &o.kind) {
            Some(OrderByKind::Expressions(terms)) => !terms.is_empty(),
            Some(OrderByKind::All(_)) => true,
            None => false,
        },
        _ => false,
    }
}

/// Resolve `sql`'s `ORDER BY` against `batches` and verify they honor it.
///
/// # Errors
/// Returns an error if the batches cannot be concatenated.
pub fn check_sort_order(sql: &str, batches: &[RecordBatch]) -> Result<SortCheck> {
    let Some(schema) = batches.first().map(RecordBatch::schema) else {
        return Ok(SortCheck::Ordered);
    };
    match resolve_sort_key(sql, &schema) {
        SortKeyResolution::Unordered => Ok(SortCheck::Ordered),
        SortKeyResolution::Unresolved { reason } => Ok(SortCheck::Skipped { reason }),
        SortKeyResolution::Resolved(key) => verify_sorted(batches, &key),
    }
}
