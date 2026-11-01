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

//! Inverse of a retention delete predicate, for scans that must see the same
//! rows the accelerator keeps after retention.

use datafusion::common::ToDFSchema;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::Cast;
use datafusion::logical_expr::{Expr, ExprSchemable};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_expr::create_physical_expr;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};

/// Rows retention keeps: it deletes only when `delete_pred` is TRUE.
///
/// SQL three-valued logic: `NOT (pred) OR pred IS NULL`, i.e. `pred IS NOT TRUE`.
#[must_use]
pub fn keep_expr_for_retention_delete(delete_pred: Expr) -> Expr {
    delete_pred.is_not_true()
}

/// Whether `keep` can be planned as a physical filter against `schema`.
///
/// # Errors
///
/// Returns an error if `keep` cannot be coerced or compiled against `schema`.
pub fn validate_keep_expr(keep: &Expr, schema: &SchemaRef) -> Result<(), DataFusionError> {
    let df_schema = Arc::clone(schema).to_dfschema()?;
    let ctx = SessionContext::new();
    create_physical_expr(
        keep,
        &df_schema,
        ctx.state().execution_props(),
        &PhysicalPlanningContext::default(),
    )?;
    Ok(())
}

/// A cast that type coercion added to a retention predicate and that can fail
/// on individual rows at execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FallibleImplicitCast {
    /// The expression being cast, as written in the predicate (usually a column).
    pub expr: String,
    pub from: DataType,
    pub to: DataType,
}

impl std::fmt::Display for FallibleImplicitCast {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "`{}` is {} but is compared with a {} value, so it is implicitly cast to {}, and that cast fails at execution on any row whose value does not convert",
            self.expr, self.from, self.to, self.to
        )
    }
}

/// The first cast that coercing `delete_pred` against `schema` adds from a
/// string-typed, row-dependent expression to a non-string type.
///
/// Planning a predicate such as `status > 5` with a `Utf8` `status` succeeds:
/// coercion rewrites it to `CAST(status AS Int64) > 5`, which compiles to a
/// physical filter and then errors on the first row whose `status` is not a
/// number. [`validate_keep_expr`] cannot see that, because the cast is valid
/// for the type; it is only invalid for the data. Casts the predicate spells
/// out (`CAST`, `TRY_CAST`) are the author's choice and are not reported, and
/// casts of constants are folded or rejected at planning, so neither is
/// counted.
///
/// # Errors
///
/// Returns an error if `delete_pred` cannot be coerced against `schema`.
pub fn fallible_implicit_string_cast(
    delete_pred: &Expr,
    schema: &SchemaRef,
) -> Result<Option<FallibleImplicitCast>, DataFusionError> {
    let df_schema = Arc::clone(schema).to_dfschema_ref()?;
    let simplifier =
        ExprSimplifier::new(SimplifyContext::builder().with_schema(Arc::clone(&df_schema)).build());
    let coerced = simplifier.coerce(delete_pred.clone(), &df_schema)?;

    // Coercion keeps the shape of the predicate and only wraps operands in
    // `Expr::Cast` nodes, so walking `delete_pred` and `coerced` side by side
    // tells each cast's provenance: a cast in `coerced` that lines up with a
    // cast of the same target type in `delete_pred` was written by the author;
    // any other cast was inserted by coercion.
    implicit_string_cast(delete_pred, &coerced, df_schema.as_ref())
}

fn implicit_string_cast(
    original: &Expr,
    coerced: &Expr,
    schema: &datafusion::common::DFSchema,
) -> Result<Option<FallibleImplicitCast>, DataFusionError> {
    if let Expr::Cast(Cast { expr: inner, field }) = coerced {
        if let Expr::Cast(Cast {
            expr: original_inner,
            field: original_field,
        }) = original
            && original_field.data_type() == field.data_type()
        {
            return implicit_string_cast(original_inner, inner, schema);
        }
        let to = field.data_type();
        if !inner.column_refs().is_empty() && !is_string(to) {
            let from = inner.get_type(schema)?;
            if is_string(&from) {
                return Ok(Some(FallibleImplicitCast {
                    expr: inner.to_string(),
                    from,
                    to: to.clone(),
                }));
            }
        }
        return implicit_string_cast(original, inner, schema);
    }

    let original_children = children(original)?;
    let coerced_children = children(coerced)?;
    if original_children.len() == coerced_children.len() {
        for (o, c) in original_children.into_iter().zip(coerced_children) {
            if let Some(found) = implicit_string_cast(o, c, schema)? {
                return Ok(Some(found));
            }
        }
        return Ok(None);
    }
    // Coercion reshaped this node, so provenance is lost below it. Pair the
    // casts by target type and cast-stripped operand, consuming each written
    // cast at most once.
    fallback_by_operand(original, coerced, schema)
}

fn children(expr: &Expr) -> Result<Vec<&Expr>, DataFusionError> {
    let mut out = Vec::new();
    expr.apply_children(|child| {
        out.push(child);
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(out)
}

fn fallback_by_operand(
    original: &Expr,
    coerced: &Expr,
    schema: &datafusion::common::DFSchema,
) -> Result<Option<FallibleImplicitCast>, DataFusionError> {
    let mut explicit: Vec<(DataType, Expr)> = Vec::new();
    original.apply(|expr| {
        if let Expr::Cast(Cast { expr: inner, field }) = expr {
            explicit.push((field.data_type().clone(), inner.as_ref().clone()));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut found = None;
    coerced.apply(|expr| {
        let Expr::Cast(Cast { expr: inner, field }) = expr else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let data_type = field.data_type();
        let stripped = strip_casts(inner)?;
        if let Some(pos) = explicit.iter().position(|(to, operand)| {
            to == data_type && strip_casts(operand).is_ok_and(|o| o == stripped)
        }) {
            explicit.swap_remove(pos);
            return Ok(TreeNodeRecursion::Continue);
        }
        if inner.column_refs().is_empty() || is_string(data_type) {
            return Ok(TreeNodeRecursion::Continue);
        }
        let from = inner.get_type(schema)?;
        if is_string(&from) {
            found = Some(FallibleImplicitCast {
                expr: inner.to_string(),
                from,
                to: data_type.clone(),
            });
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

/// `expr` with every `Expr::Cast` replaced by its operand.
fn strip_casts(expr: &Expr) -> Result<Expr, DataFusionError> {
    expr.clone()
        .transform(|e| match e {
            Expr::Cast(Cast { expr, .. }) => Ok(Transformed::yes(*expr)),
            other => Ok(Transformed::no(other)),
        })
        .map(|t| t.data)
}

fn is_string(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, value) => is_string(value),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use datafusion::prelude::{SessionContext, col, lit};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("deleted", DataType::Boolean, true),
        ]))
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(BooleanArray::from(vec![Some(false), Some(true), None])),
            ],
        )
        .expect("batch")
    }

    async fn ids_matching(keep: Expr) -> Vec<i64> {
        let ctx = SessionContext::new();
        let table = Arc::new(MemTable::try_new(schema(), vec![vec![batch()]]).expect("memtable"));
        ctx.register_table("t", table).expect("register");
        let df = ctx
            .table("t")
            .await
            .expect("table")
            .filter(keep)
            .expect("filter");
        let batches = df.collect().await.expect("collect");
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id")
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
    }

    #[tokio::test]
    async fn keep_expr_matches_sql_three_valued_logic() {
        let keep = keep_expr_for_retention_delete(col("deleted").eq(lit(true)));
        let ids = ids_matching(keep).await;
        assert_eq!(
            ids,
            vec![1, 3],
            "deleted=true is removed; false and NULL are kept"
        );
    }

    #[test]
    fn validate_keep_expr_accepts_source_column() {
        let keep = keep_expr_for_retention_delete(col("deleted").eq(lit(true)));
        validate_keep_expr(&keep, &schema()).expect("deleted is on the schema");
    }

    #[test]
    fn validate_keep_expr_rejects_unknown_column() {
        let keep = keep_expr_for_retention_delete(col("missing").eq(lit(true)));
        validate_keep_expr(&keep, &schema()).expect_err("missing is not on the schema");
    }

    fn string_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn implicit_string_to_number_cast_is_reported() {
        let cast = fallible_implicit_string_cast(&col("status").gt(lit(5_i64)), &string_schema())
            .expect("coerces")
            .expect("status > 5 casts status to a number");
        assert_eq!(cast.expr, "status");
        assert_eq!(cast.from, DataType::Utf8);
        assert_eq!(cast.to, DataType::Int64);
        // Coercion-only failure: the inverse still plans as a physical filter.
        validate_keep_expr(
            &keep_expr_for_retention_delete(col("status").gt(lit(5_i64))),
            &string_schema(),
        )
        .expect("the cast plans; it only fails per row at execution");
    }

    #[test]
    fn explicit_cast_elsewhere_does_not_hide_an_implicit_one() {
        use datafusion::prelude::cast;
        let pred = col("status").gt(lit(5_i64)).or(cast(
            cast(col("status"), DataType::Boolean),
            DataType::Int64,
        )
        .eq(lit(1_i64)));
        let found = fallible_implicit_string_cast(&pred, &string_schema())
            .expect("coerces")
            .expect("`status > 5` still casts status to a number");
        assert_eq!(found.expr, "status");
        assert_eq!(found.to, DataType::Int64);
    }

    #[test]
    fn same_type_and_explicit_casts_are_not_reported() {
        for pred in [
            col("status").eq(lit("archived")),
            col("status").gt(lit("5")),
            col("id").gt(lit(5_i32)),
            datafusion::prelude::try_cast(col("status"), DataType::Int64).gt(lit(5_i64)),
            datafusion::prelude::cast(col("status"), DataType::Int64).gt(lit(5_i64)),
            // Coercion casts the `NULL` branch inside the explicit cast; the
            // explicit cast must still be recognized as the author's.
            datafusion::prelude::cast(
                datafusion::logical_expr::when(col("id").eq(lit(1_i64)), col("status"))
                    .otherwise(lit(datafusion::scalar::ScalarValue::Null))
                    .expect("case"),
                DataType::Int64,
            )
            .gt(lit(5_i64)),
        ] {
            assert_eq!(
                fallible_implicit_string_cast(&pred, &string_schema()).expect("coerces"),
                None,
                "{pred}"
            );
        }
    }
}
