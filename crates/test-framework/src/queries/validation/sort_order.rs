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
//! # What goes unverified, and why it is reported
//!
//! Anything this module cannot check surfaces as [`SortCheck::Skipped`] or
//! [`SortCheck::PartiallyOrdered`] rather than as a silent success, because a
//! coverage hole that reads as a pass is the failure mode this whole check
//! exists to remove. Callers must keep that distinction: see
//! [`super::compare_query_result_batches_with_sort_check`], which returns the
//! reasons rather than folding them into `Pass`.
//!
//! - **`NULL` placement, unless the query states it.** Engines disagree on where
//!   `NULL`s sort absent an explicit `NULLS FIRST`/`NULLS LAST` — `DataFusion`
//!   and `PostgreSQL` put them last for `ASC`, `SQLite` puts them first. So when
//!   exactly one side of a row pair is `NULL` and the query did not say, that
//!   pair's relative order is not judged. When the query *does* say, the stated
//!   placement is enforced like any other key.
//!
//!   Two rows that are **both** `NULL` in a key column are tied under every
//!   convention, so the check continues to the next key column for them, exactly
//!   as SQL requires.
//!
//!   Leaving a pair unjudged would still hide an inversion straddling a `NULL`
//!   (`[2, NULL, 1]` is illegal under either convention), so each key column's
//!   non-`NULL` values are additionally checked as a subsequence — within the run
//!   of rows tied on the columns before it, which is the only span that column
//!   orders. `ORDER BY cnt, state` may therefore still step `state` backwards the
//!   moment `cnt` changes, while an inversion inside one `cnt` group is caught.
//! - **Terms that do not map onto an output column.** `ORDER BY` over an
//!   expression absent from the projection cannot be located in the result. The
//!   mappable leading terms are still checked and the rest is named — dropping a
//!   whole key because its second term is a `CASE` would leave the first term
//!   unverified for no reason.

use std::cmp::Ordering;

use anyhow::Result;
use arrow::array::{Array, ArrayRef, RecordBatch, make_comparator};
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
    /// `Some` when the query states `NULLS FIRST`/`NULLS LAST`, which makes the
    /// placement part of the requested order; `None` leaves it to the engine.
    pub nulls_first: Option<bool>,
}

/// Outcome of mapping a query's top-level `ORDER BY` onto its result columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortKeyResolution {
    /// No top-level `ORDER BY`: the engine may return rows in any order.
    Unordered,
    /// The leading `key` terms map onto result columns, in significance order.
    /// `unresolved_suffix` names the first term that did not map, if any; the
    /// terms from there on go unchecked.
    Resolved {
        key: Vec<SortKeyColumn>,
        unresolved_suffix: Option<String>,
    },
    /// Not even the first term mapped onto a result column.
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
    /// Every `ORDER BY` term was verified.
    Ordered,
    /// The terms that could be located were verified; `unchecked` names what
    /// could not be. Not a clean pass — a partial one, reported as such.
    PartiallyOrdered { unchecked: String },
    /// Nothing was verified. Not a pass — a hole, named so it can be counted.
    Skipped { reason: String },
    /// A row sorts before its predecessor.
    Violation(SortOrderViolation),
}

impl SortCheck {
    /// The coverage hole this outcome represents, if any.
    #[must_use]
    pub fn unchecked_reason(&self) -> Option<&str> {
        match self {
            Self::PartiallyOrdered { unchecked } => Some(unchecked.as_str()),
            Self::Skipped { reason } => Some(reason.as_str()),
            Self::Ordered | Self::Violation(_) => None,
        }
    }
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
    resolve_statement_sort_key(&statement, schema)
}

/// Same as [`resolve_sort_key`] for a statement already parsed, so a caller
/// checking both sides of a comparison parses the SQL once rather than per side.
#[must_use]
pub fn resolve_statement_sort_key(statement: &Statement, schema: &SchemaRef) -> SortKeyResolution {
    let Statement::Query(query) = statement else {
        return SortKeyResolution::Unordered;
    };
    let Some(order_by) = query.order_by.as_ref() else {
        return SortKeyResolution::Unordered;
    };
    resolve_order_by(order_by, query, schema)
}

/// Try each dialect the corpus is written in; the first that yields exactly one
/// statement wins. A query no dialect parses resolves as `Unresolved`, never as
/// ordered.
#[must_use]
pub fn parse_one_statement(sql: &str) -> Option<Statement> {
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
    let mut key: Vec<SortKeyColumn> = Vec::with_capacity(terms.len());
    let mut unresolved_suffix: Option<String> = None;
    for term in terms {
        // Stop at the first term that cannot be located, keeping the prefix: a
        // violation on the prefix is a real violation, and the terms after it are
        // only ever consulted once the prefix ties.
        let Some(index) = resolve_term(&term.expr, projection, schema) else {
            unresolved_suffix = Some(match key.last() {
                Some(last) => format!(
                    "verified through '{}'; ORDER BY term '{}' does not map to a result column, so it and any term after it are unchecked",
                    last.name, term.expr
                ),
                None => format!(
                    "nothing about the row order was verified: the first ORDER BY term '{}' does not map to a result column",
                    term.expr
                ),
            });
            break;
        };
        key.push(SortKeyColumn {
            index,
            name: schema.field(index).name().clone(),
            descending: term.options.asc == Some(false),
            nulls_first: term.options.nulls_first,
        });
    }

    match (key.is_empty(), unresolved_suffix) {
        (true, Some(reason)) => SortKeyResolution::Unresolved { reason },
        (true, None) => SortKeyResolution::Unordered,
        (false, unresolved_suffix) => SortKeyResolution::Resolved {
            key,
            unresolved_suffix,
        },
    }
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
    // the result schema, or a projection alias. Only when the name picks out one
    // column: a join can project two columns sharing a name, and taking the first
    // would silently check the wrong one. An ambiguous name falls through to the
    // projection, where `o.o_orderdate` still matches its own item exactly.
    if let Some(name) = simple_name(expr) {
        if let [only] = field_indices(schema, name).as_slice() {
            return Some(*only);
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

/// Every result column carrying `name`. More than one means the name alone does
/// not identify a column, so the caller must resolve it some other way rather
/// than guessing at the first.
fn field_indices(schema: &SchemaRef, name: &str) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.name().eq_ignore_ascii_case(name))
        .map(|(index, _)| index)
        .collect()
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
    let wanted = expr.to_string();
    items.iter().position(|item| {
        let (SelectItem::UnnamedExpr(projected)
        | SelectItem::ExprWithAlias {
            expr: projected, ..
        }) = item
        else {
            return false;
        };
        projected.to_string().eq_ignore_ascii_case(&wanted)
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

/// One key column's array plus the comparators used to judge it.
struct KeyComparator<'a> {
    column: &'a SortKeyColumn,
    array: &'a ArrayRef,
    /// Honors the query's `NULLS FIRST`/`NULLS LAST` when it stated one.
    compare: arrow::array::DynComparator,
}

fn violation(
    column: &SortKeyColumn,
    array: &dyn Array,
    previous_row: usize,
    row: usize,
) -> SortCheck {
    SortCheck::Violation(SortOrderViolation {
        column: column.name.clone(),
        row_number: row + 1,
        previous: value_string(array, previous_row),
        current: value_string(array, row),
    })
}

/// Verify `batches` are ordered by `key`.
///
/// Ties are legal: an `ORDER BY` on a non-unique key leaves the relative order
/// of equal rows engine-dependent, so only a row that sorts strictly *before*
/// its predecessor is a violation. See the module docs for how `NULL`s are
/// treated.
///
/// # Errors
/// Returns an error if the batches cannot be concatenated.
pub fn verify_sorted(batches: &[RecordBatch], key: &[SortKeyColumn]) -> Result<SortCheck> {
    let Some(schema) = batches.first().map(RecordBatch::schema) else {
        return Ok(SortCheck::Ordered);
    };
    let batch = arrow::compute::concat_batches(&schema, batches)?;
    verify_sorted_batch(&batch, key)
}

/// [`verify_sorted`] over an already-concatenated batch, so a caller that has
/// one does not copy the result set again.
///
/// # Errors
/// Returns an error if a key column's comparator cannot be built.
pub fn verify_sorted_batch(batch: &RecordBatch, key: &[SortKeyColumn]) -> Result<SortCheck> {
    if key.is_empty() {
        return Ok(SortCheck::Skipped {
            reason: "empty sort key".to_string(),
        });
    }
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
            // Only consulted for a pair the query pinned with NULLS FIRST/LAST;
            // otherwise such pairs are skipped before the comparator is called.
            nulls_first: column.nulls_first.unwrap_or(false),
        };
        match make_comparator(array.as_ref(), array.as_ref(), options) {
            Ok(compare) => comparators.push(KeyComparator {
                column,
                array,
                compare,
            }),
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

    if let Some(found) = check_adjacent_rows(batch.num_rows(), &comparators) {
        return Ok(found);
    }
    if let Some(found) = check_non_null_subsequences(batch.num_rows(), &comparators) {
        return Ok(found);
    }
    Ok(SortCheck::Ordered)
}

/// The ordinary walk: each row against the one before it, most significant key
/// column first.
fn check_adjacent_rows(rows: usize, comparators: &[KeyComparator]) -> Option<SortCheck> {
    for row in 1..rows {
        for KeyComparator {
            column,
            array,
            compare,
        } in comparators
        {
            let previous_null = array.is_null(row - 1);
            let current_null = array.is_null(row);
            // Both NULL is a tie under every convention, so SQL requires the next
            // key column to decide — keep going rather than accepting the pair.
            if previous_null && current_null {
                continue;
            }
            // Exactly one NULL, and the query did not pin the placement: this
            // column decides the pair in an engine-specific way, so neither judge
            // it nor consult the later columns SQL would never reach.
            if (previous_null || current_null) && column.nulls_first.is_none() {
                break;
            }
            match compare(row - 1, row) {
                Ordering::Less => break,
                Ordering::Equal => {}
                Ordering::Greater => return Some(violation(column, array.as_ref(), row - 1, row)),
            }
        }
    }
    None
}

/// Adjacent pairs alone cannot see an inversion that straddles an unjudged
/// `NULL`: `[2, NULL, 1]` skips both pairs while being illegal under either
/// placement convention. Comparing a key column's non-`NULL` values against the
/// previous non-`NULL` value catches that without taking a position on where
/// `NULL`s belong.
///
/// Every key column needs this, not just the leading one, because a later column
/// is still constrained *within* a run of rows tied on the columns before it:
/// `ORDER BY k1, k2` over `[(1, 2), (1, NULL), (1, 1)]` is illegal, and both of
/// its adjacent pairs are unjudged. So each column is checked inside its own tie
/// group, which is what keeps `ORDER BY cnt, state` free to step `state`
/// backwards the moment `cnt` changes.
fn check_non_null_subsequences(rows: usize, comparators: &[KeyComparator]) -> Option<SortCheck> {
    for (depth, key) in comparators.iter().enumerate() {
        let KeyComparator {
            column,
            array,
            compare,
        } = key;
        if !(0..rows).any(|row| array.is_null(row)) {
            // No NULLs, so no pair went unjudged and the adjacent walk covered it.
            continue;
        }
        let preceding = &comparators[..depth];
        let mut previous: Option<usize> = None;
        for row in 0..rows {
            if row > 0 && !tied_on(preceding, row - 1, row) {
                // An earlier key separated these rows (or left them unjudged), so
                // this column's ordering starts over.
                previous = None;
            }
            if array.is_null(row) {
                continue;
            }
            if let Some(previous_row) = previous
                && compare(previous_row, row) == Ordering::Greater
            {
                return Some(violation(column, array.as_ref(), previous_row, row));
            }
            previous = Some(row);
        }
    }
    None
}

/// Whether two adjacent rows are *known* equal on every one of `keys`.
///
/// Treating two `NULL`s as unequal here would reset the group on every row of a
/// `NULL` run and hide inversions inside it: `ORDER BY k1, k2` over
/// `[(NULL, 2), (NULL, NULL), (NULL, 1)]` is illegal under `NULLS FIRST` and
/// `NULLS LAST` alike, and every one of its pairs ties on `k1`.
fn tied_on(keys: &[KeyComparator], a: usize, b: usize) -> bool {
    keys.iter().all(|KeyComparator { array, compare, .. }| {
        match (array.is_null(a), array.is_null(b)) {
            // Equal under every placement convention — the same reasoning the
            // adjacent walk uses to keep checking later columns.
            (true, true) => true,
            (false, false) => compare(a, b) == Ordering::Equal,
            // Exactly one NULL: placement decides this pair, which is not a
            // known tie, so the group ends here.
            _ => false,
        }
    })
}

/// Whether `sql` carries a top-level `ORDER BY`, i.e. whether the order of its
/// result rows is constrained at all.
///
/// Parser-backed, so an `ORDER BY` inside a subquery, a CTE or a window frame —
/// none of which constrain the outer result — does not count. A substring search
/// for `ORDER BY` counts all three.
#[must_use]
pub fn has_top_level_order_by(sql: &str) -> bool {
    parse_one_statement(sql).is_some_and(|statement| statement_has_top_level_order_by(&statement))
}

fn statement_has_top_level_order_by(statement: &Statement) -> bool {
    let Statement::Query(query) = statement else {
        return false;
    };
    match query.order_by.as_ref().map(|o| &o.kind) {
        Some(OrderByKind::Expressions(terms)) => !terms.is_empty(),
        Some(OrderByKind::All(_)) => true,
        None => false,
    }
}

/// Whether `sql` carries a top-level `LIMIT`/`OFFSET`/`FETCH`, i.e. whether the
/// *set* of returned rows depends on the order.
///
/// Parser-backed for the same reason as [`has_top_level_order_by`]: a `LIMIT`
/// inside a subquery does not make the outer result order-dependent, and a
/// substring search cannot tell the two apart.
#[must_use]
pub fn has_top_level_limit(sql: &str) -> bool {
    parse_one_statement(sql).is_some_and(|statement| statement_has_top_level_limit(&statement))
}

fn statement_has_top_level_limit(statement: &Statement) -> bool {
    let Statement::Query(query) = statement else {
        return false;
    };
    query.limit_clause.is_some() || query.fetch.is_some()
}

/// Resolve `sql`'s `ORDER BY` against `batches` and verify they honor it.
///
/// # Errors
/// Returns an error if the batches cannot be concatenated.
pub fn check_sort_order(sql: &str, batches: &[RecordBatch]) -> Result<SortCheck> {
    let Some(schema) = batches.first().map(RecordBatch::schema) else {
        return Ok(SortCheck::Ordered);
    };
    let batch = arrow::compute::concat_batches(&schema, batches)?;
    match resolve_sort_key(sql, &schema) {
        SortKeyResolution::Unordered => Ok(SortCheck::Ordered),
        SortKeyResolution::Unresolved { reason } => Ok(SortCheck::Skipped { reason }),
        SortKeyResolution::Resolved {
            key,
            unresolved_suffix,
        } => Ok(finish_sort_check(
            verify_sorted_batch(&batch, &key)?,
            unresolved_suffix,
        )),
    }
}

/// Fold a partially resolved key's unchecked suffix into an otherwise clean
/// result, so the hole travels with the outcome instead of being dropped.
fn finish_sort_check(check: SortCheck, unresolved_suffix: Option<String>) -> SortCheck {
    match (check, unresolved_suffix) {
        (SortCheck::Ordered, Some(unchecked)) => SortCheck::PartiallyOrdered { unchecked },
        (check, _) => check,
    }
}

/// [`check_sort_order`] for a caller that already parsed the SQL and
/// concatenated the rows, so neither is repeated per side of a comparison.
///
/// # Errors
/// Returns an error if a key column's comparator cannot be built.
pub fn check_sort_order_parsed(
    statement: Option<&Statement>,
    batch: &RecordBatch,
) -> Result<SortCheck> {
    let Some(statement) = statement else {
        return Ok(SortCheck::Skipped {
            reason: "SQL did not parse as a single statement".to_string(),
        });
    };
    let schema = batch.schema();
    match resolve_statement_sort_key(statement, &schema) {
        SortKeyResolution::Unordered => Ok(SortCheck::Ordered),
        SortKeyResolution::Unresolved { reason } => Ok(SortCheck::Skipped { reason }),
        SortKeyResolution::Resolved {
            key,
            unresolved_suffix,
        } => Ok(finish_sort_check(
            verify_sorted_batch(batch, &key)?,
            unresolved_suffix,
        )),
    }
}
