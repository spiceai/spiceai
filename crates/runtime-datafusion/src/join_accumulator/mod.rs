/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{collections::HashSet, sync::Arc};

use arrow::{
    array::{Array, RecordBatch},
    datatypes::SchemaRef,
};
use datafusion::error::Result as DataFusionResult;
use datafusion::{
    physical_plan::{
        PhysicalExpr,
        expressions::{InListExpr, Literal},
        joins::{CollectLeftAccumulator, ColumnBounds},
    },
    scalar::ScalarValue,
};

const MAXIMUM_INLIST_MEMORY_BYTES: usize = 128 * 1024 * 1024; // 128Mb - approx 128 million i32 keys per partition calculated
// bounds are calculated per-partition, so total memory usage for bounds calculation is potentially num_partitions * MAXIMUM_INLIST_MEMORY_BYTES
// similarly, because rows are distributed across partitions the rows per partition is total_rows / num_partitions

/// A simple implementation of a CollectLeftAccumulator that collects exact values for dynamic filtering.
/// Performs no approximation or range merging, simply storing all values seen.
///
/// Tradeoff: potentially higher memory usage on the build-side of the join, but more precise filtering on the probe-side.
/// If `JoinSelection` has correctly re-ordered the plan so the larger scan is on the probe-side, this can be beneficial.
pub struct ExactLeftAccumulator {
    arrays: Vec<Arc<dyn Array>>,
    expr: Arc<dyn PhysicalExpr>,
}

impl CollectLeftAccumulator for ExactLeftAccumulator {
    fn try_new(expr: Arc<dyn PhysicalExpr>, _schema: &SchemaRef) -> DataFusionResult<Self> {
        Ok(Self {
            arrays: Vec::new(),
            expr,
        })
    }

    fn update_batch(&mut self, batch: &RecordBatch) -> DataFusionResult<()> {
        // eagerly evaluate the expression and store the resulting array
        // this avoids storing the entire record batch in memory, only storing the evaluated column
        let array = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;
        self.arrays.push(array);
        Ok(())
    }

    fn evaluate(self) -> DataFusionResult<Arc<dyn ColumnBounds>> {
        Ok(Arc::new(ExactColumnBounds {
            arrays: self.arrays,
        }))
    }
}

#[derive(Debug)]
pub struct ExactColumnBounds {
    arrays: Vec<Arc<dyn Array>>,
}

impl ColumnBounds for ExactColumnBounds {
    /// Converts the collected arrays into an InListExpr for use in dynamic filtering.
    /// This builds an IN expression with all collected values.
    fn physical_expr(
        &self,
        left_expr: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let total_memory_size = self
            .arrays
            .iter()
            .map(|array| array.get_array_memory_size())
            .sum::<usize>();

        if total_memory_size > MAXIMUM_INLIST_MEMORY_BYTES {
            tracing::debug!(
                "ExactLeftAccumulator exceeded maximum in-list memory size ({} bytes > {} bytes).",
                total_memory_size,
                MAXIMUM_INLIST_MEMORY_BYTES
            );

            return Ok(Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))); // Fallback to a no-op filter (always true) - the default dynamic filter behaviour
        }

        let unique_values: HashSet<_> = self
            .arrays
            .iter()
            .flat_map(|array| {
                (0..array.len()).map(move |i| ScalarValue::try_from_array(array.as_ref(), i))
            })
            .collect::<DataFusionResult<Vec<ScalarValue>>>()?
            .into_iter()
            .collect();

        let expr_values = unique_values
            .into_iter()
            .map(|sv| Arc::new(Literal::new(sv)) as Arc<dyn PhysicalExpr>)
            .collect::<Vec<_>>();

        let in_expr = Arc::new(InListExpr::new(
            left_expr,
            expr_values,
            false, // not negated (IN, not NOT IN)
            None,  // no static filter optimization
        ));

        Ok(in_expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::expressions::col;

    fn create_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a: ArrayRef = Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()));
        RecordBatch::try_new(Arc::new(schema), vec![a]).expect("Should create record batch")
    }

    #[test]
    fn test_exact_left_accumulator() {
        // Test the ExactLeftAccumulator implementation. Define a sample PhysicalExpr with a projection for a column to be scanned into a dynamic filter
        // In this scenario, we pass through a record batch with 10 values. We then build the column bounds, and verify the returned PhysicalExpr is an InListExpr with the expected values.
        let batch = create_test_batch();
        let schema = batch.schema();

        let left_expr = col("a", &schema).expect("Should create column expr");

        let mut accumulator =
            ExactLeftAccumulator::try_new(Arc::clone(&left_expr), &batch.schema())
                .expect("Should create accumulator");

        accumulator
            .update_batch(&batch)
            .expect("Should update batches");

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let in_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        // Validate the expression is an InListExpr with the expected values
        let in_list_expr = in_expr.as_any().downcast_ref::<InListExpr>();
        let in_list_expr = in_list_expr.expect("Should downcast to InListExpr");
        let expected_values: Vec<ScalarValue> =
            (0..10).map(|i| ScalarValue::Int32(Some(i))).collect();
        let actual_values: Vec<ScalarValue> = in_list_expr
            .list()
            .iter()
            .map(|expr| {
                let literal = expr
                    .as_any()
                    .downcast_ref::<Literal>()
                    .expect("Should be a literal");
                literal.value().clone()
            })
            .collect();
        assert_eq!(expected_values, actual_values);
    }
}
