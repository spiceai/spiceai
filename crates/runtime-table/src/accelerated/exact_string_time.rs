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
/// Only `AND` and `OR` are looked through, and only above a subexpression over
/// more than one column. Under them a delete keeps a row
/// exactly when the predicate is TRUE, and both are monotone in each operand,
/// so a replacement that is TRUE on exactly the rows the comparison is TRUE on
/// selects the same rows, whether the comparison was FALSE or NULL on the rest.
/// Anything else — `NOT`, `IS NOT TRUE`, `NULLIF`, `CASE` — tells FALSE from
/// NULL, so the whole subexpression containing it is pinned instead.
///
/// Returns the predicates to delete with; the union of the rows they select is
/// the rows `expr` selects. That is one predicate unless a single comparison
/// selected more than [`MAX_PINNED_VALUES_PER_DELETE`] strings: the predicate
/// is then `(c AND a) OR b` for that comparison `c` (Kleene logic is a
/// distributive lattice), so each chunk of the strings can be deleted on its own.
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
    collect_atoms(&expr, &schema, &mut atoms);
    if atoms.is_empty() {
        return Ok(vec![expr]);
    }

    let mut pinned = Vec::with_capacity(atoms.len());
    for atom in atoms {
        let rows = resolve_keys_matching_predicate(
            table,
            session,
            vec![atom.expr.clone()],
            std::slice::from_ref(&atom.column.name),
        )
        .await?;
        let mut seen = HashSet::new();
        let mut values = Vec::new();
        let mut selected_a_null = false;
        if let Some(array) = rows.column_by_name(&atom.column.name) {
            for row in 0..array.len() {
                if array.is_null(row) {
                    selected_a_null = true;
                    continue;
                }
                let value = ScalarValue::try_from_array(array.as_ref(), row)?;
                if seen.insert(value.clone()) {
                    values.push(value);
                }
            }
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        // Whether a NULL string satisfies it is a property of the expression; the
        // stored rows answer only when it does not fold.
        let selects_null =
            true_at_null(&atom.expr, &atom.column, &schema)?.unwrap_or(selected_a_null);
        pinned.push(Pinned {
            atom,
            values,
            selects_null,
        });
    }

    // A volatile remainder (`OR random() < 0.01`) would get one more chance per
    // chunk, so such a predicate is issued once, however long its list.
    if let [only] = pinned.as_slice()
        && only.values.len() > MAX_PINNED_VALUES_PER_DELETE
        && !expr.is_volatile()
    {
        let mut predicates = Vec::new();
        for (index, chunk) in only.values.chunks(MAX_PINNED_VALUES_PER_DELETE).enumerate() {
            // A NULL row is the chunks' common business; it rides on the first.
            let replacement = membership(&only.atom.column, chunk, index == 0 && only.selects_null);
            predicates.push(replace(expr.clone(), &[(&only.atom.expr, replacement)]));
        }
        return Ok(predicates);
    }

    let replacements: Vec<(&Expr, Expr)> = pinned
        .iter()
        .map(|p| {
            (
                &p.atom.expr,
                membership(&p.atom.column, &p.values, p.selects_null),
            )
        })
        .collect();
    Ok(vec![replace(expr, &replacements)])
}

/// A boolean subexpression to pin: the largest one below the `AND`/`OR` that
/// combine it that references only the string column it parses as a timestamp.
#[derive(Debug)]
struct Atom {
    expr: Expr,
    column: Column,
}

struct Pinned {
    atom: Atom,
    values: Vec<ScalarValue>,
    selects_null: bool,
}

fn collect_atoms(expr: &Expr, schema: &SchemaRef, atoms: &mut Vec<Atom>) {
    let Some(column) = string_column_cast_to_timestamp(expr, schema) else {
        return;
    };
    // The largest subexpression over that one column is pinned whole: one scan,
    // and the comparison is evaluated together with the text bound and
    // nanosecond tie-break the converter writes around it. A volatile one
    // (`random()`) is not: equal strings need not get equal results.
    if expr.column_refs().len() == 1 && !expr.is_volatile() {
        if !atoms.iter().any(|atom| &atom.expr == expr) {
            atoms.push(Atom {
                expr: expr.clone(),
                column,
            });
        }
        return;
    }
    if let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::And | Operator::Or,
        right,
    }) = expr
    {
        collect_atoms(left, schema, atoms);
        collect_atoms(right, schema, atoms);
        return;
    }
    tracing::warn!(
        "[retention] '{expr}' parses '{}' as a timestamp but references other columns or is volatile; it is left as written",
        column.name
    );
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

/// Whether `atom` is TRUE for a row whose `column` is NULL, or `None` if it
/// does not fold to a constant.
fn true_at_null(atom: &Expr, column: &Column, schema: &SchemaRef) -> Result<Option<bool>> {
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
        Expr::Literal(ScalarValue::Boolean(value), _) => Ok(Some(value == Some(true))),
        Expr::Literal(value, _) if value.is_null() => Ok(Some(false)),
        _ => Ok(None),
    }
}

/// `column IN (values)`, plus the NULL rows when the comparison selects them.
/// It is TRUE on exactly the rows the comparison is TRUE on.
fn membership(column: &Column, values: &[ScalarValue], selects_null: bool) -> Expr {
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
    match (list, selects_null) {
        (Some(list), false) => list,
        (Some(list), true) => column.is_null().or(list),
        (None, true) => column.is_null(),
        (None, false) => Expr::Literal(ScalarValue::Boolean(Some(false)), None),
    }
}

/// Replaces each target with its replacement, looking through `AND`/`OR`
/// only, exactly as [`collect_atoms`] did: a target also inside a subexpression
/// that was left as written (`NULLIF(c, flag)`) is left there too, where FALSE
/// and NULL part ways.
fn replace(expr: Expr, replacements: &[(&Expr, Expr)]) -> Expr {
    if let Some((_, replacement)) = replacements.iter().find(|(target, _)| **target == expr) {
        return replacement.clone();
    }
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: op @ (Operator::And | Operator::Or),
            right,
        }) => Expr::BinaryExpr(BinaryExpr::new(
            Box::new(replace(*left, replacements)),
            op,
            Box::new(replace(*right, replacements)),
        )),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{RecordBatch, StringArray};
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use datafusion::datasource::MemTable;
    use datafusion::functions::expr_fn::{nullif, random};
    use datafusion::prelude::{SessionContext, cast, col, lit, try_cast};

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

    /// The rewrite selects what the predicate does — NULL rows included — under
    /// `NOT`, `IS NOT TRUE` and `NOT NULLIF(..)`, which tell FALSE from NULL, and
    /// beside a text conjunct.
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
            // Never TRUE inside, so pinning the inside alone would select every row.
            Expr::Not(Box::new(nullif(before_cutoff(), lit(true)))),
            // The same comparison bare and under `NOT`.
            before_cutoff().or(Expr::Not(Box::new(before_cutoff())).and(col("ts").is_null())),
            try_cast(col("ts"), DataType::Timestamp(TimeUnit::Nanosecond, None))
                .lt(lit(ScalarValue::TimestampNanosecond(Some(CUTOFF), None))),
        ] {
            let predicates = pin_string_time_comparisons(&table, &ctx.state(), expr.clone())
                .await
                .expect("pin");
            assert_eq!(
                selected(&table, &predicates).await,
                selected(&table, std::slice::from_ref(&expr)).await,
                "{expr} -> {predicates:?}"
            );
        }
    }

    /// A comparison repeated inside a subexpression over another column is left
    /// there: with no string selected it pins to FALSE, which `NULLIF` would turn
    /// from NULL into a value and select a row the predicate keeps.
    #[tokio::test]
    async fn leaves_a_repeat_inside_a_multi_column_subexpression_alone() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(arrow::array::BooleanArray::from(vec![Some(true)])),
            ],
        )
        .expect("batch");
        let table: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("table"));
        let expr = before_cutoff().or(nullif(before_cutoff(), col("flag")).is_not_null());
        let ctx = SessionContext::new();
        let predicates = pin_string_time_comparisons(&table, &ctx.state(), expr.clone())
            .await
            .expect("pin");
        assert_eq!(predicates.len(), 1);
        assert!(
            predicates[0].to_string().contains("NULLIF(CAST(ts"),
            "{}",
            predicates[0]
        );
        assert_eq!(
            selected(&table, &predicates).await,
            selected(&table, std::slice::from_ref(&expr)).await
        );
    }

    /// A volatile conjunct is kept as written next to the pinned comparison:
    /// pinning `c AND random() < 0.01` whole would delete every row sharing a
    /// sampled row's timestamp.
    #[tokio::test]
    async fn keeps_a_volatile_conjunct_outside_the_pinned_comparison() {
        let table = table(&[Some("2021-05-01"), Some("2021-05-01")]);
        let expr = before_cutoff().and(random().lt(lit(0.01)));
        let ctx = SessionContext::new();
        let predicates = pin_string_time_comparisons(&table, &ctx.state(), expr)
            .await
            .expect("pin");
        assert_eq!(predicates.len(), 1);
        let rendered = predicates[0].to_string();
        assert!(rendered.contains("random()"), "{rendered}");
        assert!(!rendered.contains("CAST(ts"), "{rendered}");
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

    /// A volatile remainder is evaluated once: a long selection is not split
    /// when chunks would each re-roll `random()`.
    #[tokio::test]
    async fn issues_a_volatile_predicate_as_one_delete() {
        let owned: Vec<String> = (0..MAX_PINNED_VALUES_PER_DELETE + 5)
            .map(|i| format!("2021-05-01T00:00:{:02}.{:09}Z", i % 60, i))
            .collect();
        let rows: Vec<Option<&str>> = owned.iter().map(|s| Some(s.as_str())).collect();
        let table = table(&rows);
        let ctx = SessionContext::new();
        let expr = before_cutoff().or(random().lt(lit(0.01)));
        let predicates = pin_string_time_comparisons(&table, &ctx.state(), expr)
            .await
            .expect("pin");
        assert_eq!(predicates.len(), 1);
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
