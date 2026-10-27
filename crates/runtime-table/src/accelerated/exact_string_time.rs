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

//! Pins the parts of a delete predicate that parse a string column as a
//! timestamp to the exact strings `DataFusion` selects.
//!
//! An accelerator's `delete_from` renders its filters into the engine's own SQL
//! without consulting the pushdown gates, and no SQL engine parses an ISO 8601
//! string the way `DataFusion` does: `DuckDB` drops the UTC offset and the
//! nanoseconds, `PostgreSQL` drops the offset, and `SQLite` reads a string
//! without an offset in the host's time zone. A retention delete over
//! `CAST(ts AS Timestamp) < cutoff` would then remove live rows or keep expired
//! ones.
//!
//! Each such comparison is instead evaluated by `DataFusion` over the stored
//! rows — the gates keep the cast out of the scan — and replaced with
//! `ts IN (<the strings it selected>)`. Two rows holding the same string name the
//! same instant, so the replacement selects exactly the rows the comparison
//! does, and string equality means the same thing in every engine.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{
        Column,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    error::Result,
    logical_expr::{BinaryExpr, Cast, Operator, TryCast},
    prelude::Expr,
    scalar::ScalarValue,
};
use spice_table::resolve_keys_matching_predicate;

/// The most strings one `IN` list carries. A larger selection is split across
/// several deletes where that selects the same rows (see [`pin_string_time_comparisons`]).
pub(crate) const MAX_PINNED_VALUES_PER_DELETE: usize = 10_000;

/// Rewrites `expr` so that every comparison parsing a string column as a
/// timestamp is replaced with the strings `DataFusion` selects for it out of
/// `table`'s current rows.
///
/// Returns the predicates to delete with, in order; the union of the rows they
/// select is the rows `expr` selects. That is one predicate unless a single
/// comparison selected more than [`MAX_PINNED_VALUES_PER_DELETE`] strings and
/// sits under nothing but `AND`/`OR`: the predicate is then monotone in it, so
/// each chunk of the strings can be deleted on its own.
///
/// A comparison that also references another column cannot be pinned to one
/// column's strings; it is left as written.
///
/// # Errors
///
/// Returns an error if `DataFusion` cannot evaluate a comparison over `table`
/// (for instance a string that does not parse as a timestamp).
pub(crate) async fn pin_string_time_comparisons(
    table: &Arc<dyn TableProvider>,
    session: &dyn Session,
    expr: Expr,
) -> Result<Vec<Expr>> {
    let schema = table.schema();
    let mut atoms = Vec::new();
    collect_atoms(&expr, &schema, true, &mut atoms);
    if atoms.is_empty() {
        return Ok(vec![expr]);
    }

    let mut pinned = Vec::with_capacity(atoms.len());
    for atom in &atoms {
        let Some(at_null) = value_at_null(&atom.expr, &atom.column, &schema)? else {
            tracing::warn!(
                "[retention] '{}' does not fold to a constant for a NULL '{}'; it is left as written",
                atom.expr,
                atom.column.name
            );
            continue;
        };
        let rows = resolve_keys_matching_predicate(
            table,
            session,
            vec![atom.expr.clone()],
            std::slice::from_ref(&atom.column.name),
        )
        .await?;
        let mut seen = HashSet::new();
        let mut values = Vec::new();
        if let Some(array) = rows.column_by_name(&atom.column.name) {
            for row in 0..array.len() {
                if array.is_null(row) {
                    continue;
                }
                let value = ScalarValue::try_from_array(array.as_ref(), row)?;
                if seen.insert(value.clone()) {
                    values.push(value);
                }
            }
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        pinned.push((atom, at_null, values));
    }

    let chunked = match pinned.as_slice() {
        [(atom, _, values)] if atom.positive && values.len() > MAX_PINNED_VALUES_PER_DELETE => true,
        _ => false,
    };
    if chunked {
        let (atom, at_null, values) = &pinned[0];
        let mut predicates = Vec::new();
        for (index, chunk) in values.chunks(MAX_PINNED_VALUES_PER_DELETE).enumerate() {
            // A NULL row is the chunks' common business; it rides on the first.
            let at_null = if index == 0 { *at_null } else { AtNull::False };
            let replacement = membership(&atom.column, chunk, at_null);
            predicates.push(replace(expr.clone(), &atom.expr, &replacement)?);
        }
        return Ok(predicates);
    }

    let mut expr = expr;
    for (atom, at_null, values) in &pinned {
        let replacement = membership(&atom.column, values, *at_null);
        expr = replace(expr, &atom.expr, &replacement)?;
    }
    Ok(vec![expr])
}

/// A boolean subexpression to pin: everything below the `AND`/`OR`/`NOT` that
/// combine it, referencing one string column it parses as a timestamp.
#[derive(Debug)]
struct Atom {
    expr: Expr,
    column: Column,
    /// Whether the path from the root to it crosses only `AND` and `OR`.
    positive: bool,
}

fn collect_atoms(expr: &Expr, schema: &SchemaRef, positive: bool, atoms: &mut Vec<Atom>) {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And | Operator::Or,
            right,
        }) => {
            collect_atoms(left, schema, positive, atoms);
            collect_atoms(right, schema, positive, atoms);
        }
        Expr::Not(inner) => collect_atoms(inner, schema, false, atoms),
        _ => {
            let Some(column) = string_column_cast_to_timestamp(expr, schema) else {
                return;
            };
            let columns = expr.column_refs();
            if columns.len() != 1 {
                tracing::warn!(
                    "[retention] '{expr}' parses '{}' as a timestamp but references other columns too; it is left as written",
                    column.name
                );
                return;
            }
            if atoms.iter().any(|atom| &atom.expr == expr) {
                if !positive {
                    for atom in atoms.iter_mut().filter(|atom| &atom.expr == expr) {
                        atom.positive = false;
                    }
                }
                return;
            }
            atoms.push(Atom {
                expr: expr.clone(),
                column,
                positive,
            });
        }
    }
}

/// The string column a `CAST`/`TRY_CAST` to a timestamp inside `expr` reads,
/// if there is one.
fn string_column_cast_to_timestamp(expr: &Expr, schema: &SchemaRef) -> Option<Column> {
    let mut found = None;
    let _ = expr.apply(|node| {
        let (Expr::Cast(Cast {
            expr: operand,
            field,
        })
        | Expr::TryCast(TryCast {
            expr: operand,
            field,
        })) = node
        else {
            return Ok(TreeNodeRecursion::Continue);
        };
        if !matches!(field.data_type(), DataType::Timestamp(_, _)) {
            return Ok(TreeNodeRecursion::Continue);
        }
        if let Expr::Column(column) = operand.as_ref()
            && schema
                .field_with_name(&column.name)
                .is_ok_and(|field| is_string(field.data_type()))
        {
            found = Some(column.clone());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn is_string(data_type: &DataType) -> bool {
    data_type.is_string()
        || matches!(data_type, DataType::Dictionary(_, value) if value.is_string())
}

/// What a comparison answers for a row whose column is NULL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AtNull {
    True,
    False,
    Null,
}

/// What `atom` answers for a row whose `column` is NULL, or `None` if it does
/// not fold to a constant.
fn value_at_null(atom: &Expr, column: &Column, schema: &SchemaRef) -> Result<Option<AtNull>> {
    let data_type = schema.field_with_name(&column.name)?.data_type().clone();
    let null = ScalarValue::try_from(&data_type)?;
    let substituted = atom
        .clone()
        .transform(|node| match node {
            Expr::Column(ref c) if c.name == column.name => {
                Ok(Transformed::yes(Expr::Literal(null.clone(), None)))
            }
            other => Ok(Transformed::no(other)),
        })?
        .data;
    match util::expr::simplify_expr(substituted, schema)? {
        Expr::Literal(ScalarValue::Boolean(Some(true)), _) => Ok(Some(AtNull::True)),
        Expr::Literal(ScalarValue::Boolean(Some(false)), _) => Ok(Some(AtNull::False)),
        Expr::Literal(value, _) if value.is_null() => Ok(Some(AtNull::Null)),
        _ => Ok(None),
    }
}

/// `column IN (values)`, answering `at_null` for a NULL `column` so the
/// replacement agrees with the comparison on NULL rows too — including under a
/// `NOT`, where NULL and false part ways.
fn membership(column: &Column, values: &[ScalarValue], at_null: AtNull) -> Expr {
    let column = Expr::Column(column.clone());
    let list = (!values.is_empty()).then(|| {
        column.clone().in_list(
            values
                .iter()
                .map(|value| Expr::Literal(value.clone(), None))
                .collect(),
            false,
        )
    });
    match (list, at_null) {
        // `IN` already answers NULL for a NULL operand.
        (Some(list), AtNull::Null) => list,
        (Some(list), AtNull::True) => column.is_null().or(list),
        (Some(list), AtNull::False) => column.is_not_null().and(list),
        (None, AtNull::True) => column.is_null(),
        (None, AtNull::False) => Expr::Literal(ScalarValue::Boolean(Some(false)), None),
        (None, AtNull::Null) => column
            .is_null()
            .and(Expr::Literal(ScalarValue::Boolean(None), None)),
    }
}

fn replace(expr: Expr, target: &Expr, replacement: &Expr) -> Result<Expr> {
    Ok(expr
        .transform_down(|node| {
            if &node == target {
                Ok(Transformed::new(
                    replacement.clone(),
                    true,
                    TreeNodeRecursion::Jump,
                ))
            } else {
                Ok(Transformed::no(node))
            }
        })?
        .data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{RecordBatch, StringArray};
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionContext, cast, col, lit};

    const CUTOFF: i64 = 1_620_025_200_000_000_000; // 2021-05-03T07:00:00Z

    fn table(values: &[Option<&str>]) -> Arc<dyn TableProvider> {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(values.to_vec()))],
        )
        .expect("batch");
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("table"))
    }

    fn before_cutoff() -> Expr {
        cast(col("ts"), DataType::Timestamp(TimeUnit::Nanosecond, None))
            .lt(lit(ScalarValue::TimestampNanosecond(Some(CUTOFF), None)))
    }

    async fn selected(table: &Arc<dyn TableProvider>, predicates: &[Expr]) -> Vec<String> {
        let ctx = SessionContext::new();
        let mut out = Vec::new();
        for predicate in predicates {
            let rows = resolve_keys_matching_predicate(
                table,
                &ctx.state(),
                vec![predicate.clone()],
                &["ts".to_string()],
            )
            .await
            .expect("select");
            let array = rows
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .cloned();
            if let Some(array) = array {
                out.extend(array.iter().map(|v| v.unwrap_or("NULL").to_string()));
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// The offsets and nanoseconds `DuckDB` and `PostgreSQL` drop decide the
    /// pinned set: each string is selected by the instant it names.
    #[tokio::test]
    async fn pins_each_spelling_to_the_instant_it_names() {
        let rows = [
            Some("2021-05-02T23:30:00-07:00"),      // 06:30Z: expired
            Some("2021-05-03T06:59:59.999999999Z"), // one ns before: expired
            Some("2021-05-03T07:00:00.000000001Z"), // one ns after: live
            Some("2021-05-03T08:00:00+08:00"),      // 00:00Z: expired
            Some("2021-05-03 01:00:00-07:00"),      // 08:00Z: live
            Some("2021-05-03T06:00:00"),            // no offset, UTC: expired
            None,
        ];
        let table = table(&rows);
        let ctx = SessionContext::new();
        let predicates = pin_string_time_comparisons(&table, &ctx.state(), before_cutoff())
            .await
            .expect("pin");
        assert_eq!(predicates.len(), 1);
        assert!(
            !predicates[0].to_string().contains("CAST"),
            "{}",
            predicates[0]
        );
        assert_eq!(
            selected(&table, &predicates).await,
            selected(&table, &[before_cutoff()]).await
        );
        assert_eq!(
            selected(&table, &predicates).await,
            vec![
                "2021-05-02T23:30:00-07:00",
                "2021-05-03T06:00:00",
                "2021-05-03T06:59:59.999999999Z",
                "2021-05-03T08:00:00+08:00",
            ]
        );
    }

    /// NULL rows agree with the comparison under `NOT` and `IS NOT TRUE`, and a
    /// text conjunct beside the comparison is kept.
    #[tokio::test]
    async fn agrees_with_the_comparison_on_null_rows_and_beside_other_conjuncts() {
        let rows = [
            Some("2021-05-02T23:30:00-07:00"),
            Some("2021-05-03T08:00:00Z"),
            None,
        ];
        let table = table(&rows);
        let ctx = SessionContext::new();
        for expr in [
            Expr::Not(Box::new(before_cutoff())),
            before_cutoff().is_not_true(),
            before_cutoff().or(col("ts").is_null()),
            col("ts").gt_eq(lit("2021-05-01")).and(before_cutoff()),
        ] {
            let predicates = pin_string_time_comparisons(&table, &ctx.state(), expr.clone())
                .await
                .expect("pin");
            assert_eq!(
                selected(&table, &predicates).await,
                selected(&table, &[expr.clone()]).await,
                "{expr} -> {predicates:?}"
            );
        }
    }

    /// A selection past the per-delete limit is split, and the chunks together
    /// select what the comparison does.
    #[tokio::test]
    async fn splits_a_large_selection_across_deletes() {
        let owned: Vec<String> = (0..MAX_PINNED_VALUES_PER_DELETE + 5)
            .map(|i| format!("2021-05-01T00:00:{:02}.{:09}Z", i % 60, i))
            .collect();
        let mut rows: Vec<Option<&str>> = owned.iter().map(|s| Some(s.as_str())).collect();
        rows.push(Some("2021-05-04T00:00:00Z"));
        let table = table(&rows);
        let ctx = SessionContext::new();
        let predicates = pin_string_time_comparisons(&table, &ctx.state(), before_cutoff())
            .await
            .expect("pin");
        assert_eq!(predicates.len(), 2);
        assert_eq!(
            selected(&table, &predicates).await.len(),
            MAX_PINNED_VALUES_PER_DELETE + 5
        );
    }

    /// Nothing to pin: the predicate comes back as it was.
    #[tokio::test]
    async fn leaves_a_predicate_without_a_string_cast_alone() {
        let table = table(&[Some("x")]);
        let ctx = SessionContext::new();
        let expr = col("ts").eq(lit("x"));
        let predicates = pin_string_time_comparisons(&table, &ctx.state(), expr.clone())
            .await
            .expect("pin");
        assert_eq!(predicates, vec![expr]);
    }
}
