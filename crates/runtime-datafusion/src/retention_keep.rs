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
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_expr::create_physical_expr;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

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
}
