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

const MAXIMUM_INLIST_MEMORY_BYTES_PER_PARTITION: usize = 128 * 1024 * 1024; // 128Mb - can store approximately 128 million i32 keys per partition calculated
// bounds are calculated per-partition, so total memory usage for bounds calculation is potentially num_partitions * MAXIMUM_INLIST_MEMORY_BYTES_PER_PARTITION
// similarly, because rows are distributed across partitions the rows per partition is total_rows / num_partitions

/// A simple implementation of a `CollectLeftAccumulator` that collects exact values for dynamic filtering.
/// Performs no approximation or range merging, simply storing all values seen.
///
/// Tradeoff: potentially higher memory usage on the build-side of the join, but more precise filtering on the probe-side.
/// If `JoinSelection` has correctly re-ordered the plan so the larger scan is on the probe-side, this can be beneficial.
pub struct ExactLeftAccumulator {
    arrays: Vec<Arc<dyn Array>>,
    expr: Arc<dyn PhysicalExpr>,
}

impl CollectLeftAccumulator for ExactLeftAccumulator {
    fn name(&self) -> &'static str {
        "ExactLeftAccumulator"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "ExactLeftAccumulator"
    }

    fn try_new(expr: Arc<dyn PhysicalExpr>, _schema: &SchemaRef) -> DataFusionResult<Self> {
        tracing::debug!("Trying to build ExactLeftAccumulator.");
        Ok(Self {
            arrays: Vec::new(),
            expr,
        })
    }

    fn update_batch(&mut self, batch: &RecordBatch) -> DataFusionResult<()> {
        if batch.num_rows() == 0 {
            tracing::debug!("ExactLeftAccumulator received empty batch, skipping.");
            return Ok(());
        }

        tracing::debug!(
            "ExactLeftAccumulator updating batch with {} rows",
            batch.num_rows()
        );

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
    /// Converts the collected arrays into an `InListExpr` for use in dynamic filtering.
    /// This builds an IN expression with all collected values.
    fn physical_expr(
        &self,
        left_expr: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let total_memory_size = self
            .arrays
            .iter()
            .map(arrow::array::Array::get_array_memory_size)
            .sum::<usize>();

        if total_memory_size > MAXIMUM_INLIST_MEMORY_BYTES_PER_PARTITION {
            tracing::debug!(
                "ExactLeftAccumulator exceeded maximum in-list memory size ({} bytes > {} bytes).",
                total_memory_size,
                MAXIMUM_INLIST_MEMORY_BYTES_PER_PARTITION
            );

            return Ok(Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))); // Fallback to a no-op filter (always true) - the default dynamic filter behaviour
        }

        let unique_values = self
            .arrays
            .iter()
            .flat_map(|array| {
                (0..array.len()).map(move |i| ScalarValue::try_from_array(array.as_ref(), i))
            })
            .collect::<DataFusionResult<HashSet<ScalarValue>>>()?;

        if unique_values.is_empty() {
            // No values collected - return a no-op filter (always true)
            tracing::debug!("ExactLeftAccumulator collected no values, returning no-op filter.");
            return Ok(Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))));
        }

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

        tracing::debug!(
            "ExactLeftAccumulator created InListExpr with {} values ({} bytes).",
            in_expr.list().len(),
            total_memory_size,
        );

        Ok(in_expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, UInt64Array};
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
        let mut actual_values: Vec<ScalarValue> = in_list_expr
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
        actual_values.sort_by(|a, b| a.partial_cmp(b).expect("Should be comparable"));
        assert_eq!(expected_values, actual_values);
    }

    #[test]
    fn test_exact_left_accumulator_empty_batch() {
        // Test that updating with an empty batch does not cause errors and results in an always-true filter
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let empty_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        )
        .expect("Should create empty record batch");

        let left_expr = col("a", &empty_batch.schema()).expect("Should create column expr");

        let mut accumulator =
            ExactLeftAccumulator::try_new(Arc::clone(&left_expr), &empty_batch.schema())
                .expect("Should create accumulator");

        accumulator
            .update_batch(&empty_batch)
            .expect("Should update with empty batch");

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        // Validate the expression is a Literal true (no-op filter)
        let literal_expr = physical_expr.as_any().downcast_ref::<Literal>();
        let literal_expr = literal_expr.expect("Should downcast to Literal");
        let expected_value = ScalarValue::Boolean(Some(true));
        assert_eq!(literal_expr.value(), &expected_value);
    }

    #[test]
    fn test_exact_left_accumulator_exceeds_memory() {
        // Test that when the accumulated arrays exceed the maximum in-list memory size, we fallback to a no-op filter
        let schema = Schema::new(vec![Field::new("a", DataType::UInt64, false)]);
        let large_array: ArrayRef = Arc::new(UInt64Array::from(
            (0..(MAXIMUM_INLIST_MEMORY_BYTES_PER_PARTITION + 1) as u64).collect::<Vec<u64>>(),
        ));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![large_array])
            .expect("Should create large record batch");

        let left_expr = col("a", &batch.schema()).expect("Should create column expr");

        let mut accumulator =
            ExactLeftAccumulator::try_new(Arc::clone(&left_expr), &batch.schema())
                .expect("Should create accumulator");

        accumulator
            .update_batch(&batch)
            .expect("Should update with large batch");

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        // Validate the expression is a Literal true (no-op filter)
        let literal_expr = physical_expr.as_any().downcast_ref::<Literal>();
        let literal_expr = literal_expr.expect("Should downcast to Literal");
        let expected_value = ScalarValue::Boolean(Some(true));
        assert_eq!(literal_expr.value(), &expected_value);
    }

    #[test]
    fn test_exact_left_accumulator_duplicate_values() {
        // Test that duplicate values are correctly handled and only unique values are included in the InListExpr
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 2, 3, 3, 3]));
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![a]).expect("Should create record batch");

        let left_expr = col("a", &batch.schema()).expect("Should create column expr");

        let mut accumulator =
            ExactLeftAccumulator::try_new(Arc::clone(&left_expr), &batch.schema())
                .expect("Should create accumulator");

        accumulator
            .update_batch(&batch)
            .expect("Should update with batch");

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let in_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        // Validate the expression is an InListExpr with the expected unique values
        let in_list_expr = in_expr.as_any().downcast_ref::<InListExpr>();
        let in_list_expr = in_list_expr.expect("Should downcast to InListExpr");
        let expected_values: Vec<ScalarValue> = vec![1, 2, 3]
            .into_iter()
            .map(|i| ScalarValue::Int32(Some(i)))
            .collect();
        let mut actual_values: Vec<ScalarValue> = in_list_expr
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
        actual_values.sort_by(|a, b| a.partial_cmp(b).expect("Should be comparable"));

        assert_eq!(expected_values, actual_values);
    }

    #[test]
    fn test_exact_left_accumulator_multiple_batches() {
        // Test that multiple batches can be accumulated correctly
        let batch1 = {
            let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
            RecordBatch::try_new(Arc::new(schema), vec![a]).expect("Should create record batch")
        };

        let batch2 = {
            let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            let a: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));
            RecordBatch::try_new(Arc::new(schema), vec![a]).expect("Should create record batch")
        };

        let left_expr = col("a", &batch1.schema()).expect("Should create column expr");

        let mut accumulator =
            ExactLeftAccumulator::try_new(Arc::clone(&left_expr), &batch1.schema())
                .expect("Should create accumulator");

        accumulator
            .update_batch(&batch1)
            .expect("Should update with batch 1");
        accumulator
            .update_batch(&batch2)
            .expect("Should update with batch 2");
        accumulator
            .update_batch(&batch1)
            .expect("Should update with batch 1 a second time");

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let in_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        // Validate the expression is an InListExpr with the expected values
        let in_list_expr = in_expr.as_any().downcast_ref::<InListExpr>();
        let in_list_expr = in_list_expr.expect("Should downcast to InListExpr");
        let expected_values: Vec<ScalarValue> =
            (1..=6).map(|i| ScalarValue::Int32(Some(i))).collect();
        let mut actual_values: Vec<ScalarValue> = in_list_expr
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
        actual_values.sort_by(|a, b| a.partial_cmp(b).expect("Should be comparable"));
        assert_eq!(expected_values, actual_values);
    }
}
