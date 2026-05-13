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

use std::{cmp::Ordering, collections::HashSet, sync::Arc};

use arrow::{
    array::{Array, RecordBatch},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::error::Result as DataFusionResult;
use datafusion::{
    logical_expr::Operator,
    physical_plan::{
        PhysicalExpr,
        expressions::{BinaryExpr, InListExpr, Literal},
        joins::{CollectLeftAccumulator, ColumnBounds},
    },
    scalar::ScalarValue,
};

const MAXIMUM_INLIST_MEMORY_BYTES_PER_PARTITION: usize = 128 * 1024 * 1024; // 128Mb - can store approximately 32 million i32 keys per partition
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
    total_memory_size: usize,
    max_inlist_memory_size: usize,
    range_bounds: RangeBounds,
    exact_values_exceeded_memory_limit: bool,
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
        Ok(Self::new_with_memory_limit(
            expr,
            MAXIMUM_INLIST_MEMORY_BYTES_PER_PARTITION,
        ))
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

        if self.exact_values_exceeded_memory_limit {
            self.range_bounds.update(array.as_ref())?;
            return Ok(());
        }

        let total_memory_size = self
            .total_memory_size
            .saturating_add(array.get_array_memory_size());

        if total_memory_size > self.max_inlist_memory_size {
            tracing::debug!(
                "ExactLeftAccumulator exceeded maximum in-list memory size ({} bytes > {} bytes); using range fallback.",
                total_memory_size,
                self.max_inlist_memory_size
            );
            self.range_bounds = self.range_bounds_from_collected_arrays(array.as_ref())?;
            self.arrays.clear();
            self.total_memory_size = total_memory_size;
            self.exact_values_exceeded_memory_limit = true;
            return Ok(());
        }

        self.total_memory_size = total_memory_size;
        self.arrays.push(array);
        Ok(())
    }

    fn evaluate(self) -> DataFusionResult<Arc<dyn ColumnBounds>> {
        Ok(Arc::new(ExactColumnBounds {
            arrays: self.arrays,
            total_memory_size: self.total_memory_size,
            range_bounds: self.range_bounds,
            use_range_fallback: self.exact_values_exceeded_memory_limit,
        }))
    }
}

impl ExactLeftAccumulator {
    /// Creates an accumulator with a custom per-partition in-list memory limit.
    #[must_use]
    pub fn new_with_memory_limit(
        expr: Arc<dyn PhysicalExpr>,
        max_inlist_memory_size: usize,
    ) -> Self {
        tracing::debug!("Trying to build ExactLeftAccumulator.");
        Self {
            arrays: Vec::new(),
            expr,
            total_memory_size: 0,
            max_inlist_memory_size,
            range_bounds: RangeBounds::default(),
            exact_values_exceeded_memory_limit: false,
        }
    }

    fn range_bounds_from_collected_arrays(
        &self,
        array: &dyn Array,
    ) -> DataFusionResult<RangeBounds> {
        let mut range_bounds = RangeBounds::default();
        for collected_array in &self.arrays {
            range_bounds.update(collected_array.as_ref())?;
        }
        range_bounds.update(array)?;
        Ok(range_bounds)
    }
}

#[derive(Debug)]
pub struct ExactColumnBounds {
    arrays: Vec<Arc<dyn Array>>,
    total_memory_size: usize,
    range_bounds: RangeBounds,
    use_range_fallback: bool,
}

impl ColumnBounds for ExactColumnBounds {
    /// Converts the collected arrays into an `InListExpr` for use in dynamic filtering.
    /// This builds an IN expression with all collected values.
    fn physical_expr(
        &self,
        left_expr: Arc<dyn PhysicalExpr>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        if self.use_range_fallback {
            return Ok(self.range_bounds.physical_expr(left_expr));
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
            return Ok(literal_true());
        }

        let expr_values = unique_values
            .into_iter()
            .map(|sv| Arc::new(Literal::new(sv)) as Arc<dyn PhysicalExpr>)
            .collect::<Vec<_>>();

        // Build a schema compatible with `left_expr` so InListExpr::try_new can validate data types.
        // If `left_expr` is a Column referencing index N, we need at least N+1 fields.
        // Literals carry their own type, so only the field at the column's index matters.
        let data_type = expr_values
            .first()
            .and_then(|e| {
                let s = Schema::new(vec![Field::new(
                    "_",
                    arrow::datatypes::DataType::Null,
                    true,
                )]);
                e.data_type(&s).ok()
            })
            .unwrap_or(arrow::datatypes::DataType::Null);

        let col_index = left_expr
            .as_any()
            .downcast_ref::<datafusion::physical_plan::expressions::Column>()
            .map_or(0, datafusion::physical_expr::expressions::Column::index);

        let mut fields: Vec<Field> = (0..col_index)
            .map(|i| Field::new(format!("_pad{i}"), arrow::datatypes::DataType::Null, true))
            .collect();
        fields.push(Field::new("_col", data_type, true));
        let dummy_schema = Schema::new(fields);

        let in_expr = Arc::new(InListExpr::try_new(
            left_expr,
            expr_values,
            false, // not negated (IN, not NOT IN)
            &dummy_schema,
        )?);

        tracing::debug!(
            "ExactLeftAccumulator created InListExpr with {} values ({} bytes).",
            in_expr.list().len(),
            self.total_memory_size,
        );

        Ok(in_expr)
    }
}

#[derive(Debug)]
struct RangeBounds {
    min_value: Option<ScalarValue>,
    max_value: Option<ScalarValue>,
    contains_null: bool,
    supports_range_filter: bool,
}

impl Default for RangeBounds {
    fn default() -> Self {
        Self {
            min_value: None,
            max_value: None,
            contains_null: false,
            supports_range_filter: true,
        }
    }
}

impl RangeBounds {
    fn update(&mut self, array: &dyn Array) -> DataFusionResult<()> {
        if !self.supports_range_filter {
            self.contains_null |= array.null_count() > 0;
            return Ok(());
        }

        for row_index in 0..array.len() {
            let value = ScalarValue::try_from_array(array, row_index)?;

            if value.is_null() {
                self.contains_null = true;
                continue;
            }

            if !supports_range_comparison(&value) {
                self.supports_range_filter = false;
                return Ok(());
            }

            self.update_value(value);

            if !self.supports_range_filter {
                return Ok(());
            }
        }

        Ok(())
    }

    fn update_value(&mut self, value: ScalarValue) {
        match &self.min_value {
            Some(min_value) => match value.partial_cmp(min_value) {
                Some(Ordering::Less) => self.min_value = Some(value.clone()),
                Some(_) => {}
                None => {
                    self.supports_range_filter = false;
                    return;
                }
            },
            None => self.min_value = Some(value.clone()),
        }

        match &self.max_value {
            Some(max_value) => match value.partial_cmp(max_value) {
                Some(Ordering::Greater) => self.max_value = Some(value),
                Some(_) => {}
                None => self.supports_range_filter = false,
            },
            None => self.max_value = Some(value),
        }
    }

    fn physical_expr(&self, left_expr: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        let (Some(min_value), Some(max_value)) = (&self.min_value, &self.max_value) else {
            tracing::debug!(
                "ExactLeftAccumulator range fallback has no non-null values, returning no-op filter."
            );
            return literal_true();
        };

        if self.contains_null || !self.supports_range_filter {
            tracing::debug!(
                contains_null = self.contains_null,
                supports_range_filter = self.supports_range_filter,
                "ExactLeftAccumulator could not create range fallback, returning no-op filter."
            );
            return literal_true();
        }

        let lower_bound = Arc::new(BinaryExpr::new(
            Arc::clone(&left_expr),
            Operator::GtEq,
            Arc::new(Literal::new(min_value.clone())),
        ));
        let upper_bound = Arc::new(BinaryExpr::new(
            left_expr,
            Operator::LtEq,
            Arc::new(Literal::new(max_value.clone())),
        ));

        tracing::debug!(
            "ExactLeftAccumulator created range fallback from {min_value} to {max_value}."
        );

        Arc::new(BinaryExpr::new(lower_bound, Operator::And, upper_bound))
    }
}

fn supports_range_comparison(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Float16(Some(value)) => !value.is_nan(),
        ScalarValue::Float32(Some(value)) => !value.is_nan(),
        ScalarValue::Float64(Some(value)) => !value.is_nan(),
        _ => matches!(
            value.data_type(),
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
        ),
    }
}

fn literal_true() -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::expressions::col;

    fn create_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a: ArrayRef = Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()));
        RecordBatch::try_new(Arc::new(schema), vec![a]).expect("Should create record batch")
    }

    fn create_uint64_batch(values: Vec<u64>) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("a", DataType::UInt64, false)]);
        let a: ArrayRef = Arc::new(UInt64Array::from(values));
        RecordBatch::try_new(Arc::new(schema), vec![a]).expect("Should create record batch")
    }

    fn create_nullable_uint64_batch(values: Vec<Option<u64>>) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("a", DataType::UInt64, true)]);
        let a: ArrayRef = Arc::new(UInt64Array::from(values));
        RecordBatch::try_new(Arc::new(schema), vec![a]).expect("Should create record batch")
    }

    fn assert_literal_true(physical_expr: &Arc<dyn PhysicalExpr>) {
        let literal_expr = physical_expr
            .as_any()
            .downcast_ref::<Literal>()
            .expect("Should downcast to Literal");
        let expected_value = ScalarValue::Boolean(Some(true));
        assert_eq!(literal_expr.value(), &expected_value);
    }

    fn evaluate_boolean_expression(
        physical_expr: &Arc<dyn PhysicalExpr>,
        batch: &RecordBatch,
    ) -> Vec<Option<bool>> {
        let result = physical_expr
            .evaluate(batch)
            .expect("Should evaluate expression")
            .into_array(batch.num_rows())
            .expect("Should produce boolean array");
        let bool_result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("Should downcast to BooleanArray");

        (0..bool_result.len())
            .map(|row_index| {
                if bool_result.is_null(row_index) {
                    None
                } else {
                    Some(bool_result.value(row_index))
                }
            })
            .collect()
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

        assert_literal_true(&physical_expr);
    }

    #[test]
    fn test_exact_left_accumulator_uses_exact_values_at_memory_limit() {
        let batch = create_uint64_batch(vec![1, 3, 5]);
        let max_memory_size = batch.column(0).get_array_memory_size();

        let left_expr = col("a", &batch.schema()).expect("Should create column expr");

        let mut accumulator =
            ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&left_expr), max_memory_size);

        accumulator
            .update_batch(&batch)
            .expect("Should update with batch");
        assert_eq!(1, accumulator.arrays.len());
        assert!(!accumulator.exact_values_exceeded_memory_limit);

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        physical_expr
            .as_any()
            .downcast_ref::<InListExpr>()
            .expect("Should downcast to InListExpr");
    }

    #[test]
    fn test_exact_left_accumulator_exceeds_memory() {
        // Test that when accumulated arrays exceed the in-list memory limit, we fallback to a range filter.
        let batch = create_uint64_batch(vec![1, 3, 5]);

        let left_expr = col("a", &batch.schema()).expect("Should create column expr");

        let mut accumulator =
            ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&left_expr), 1);

        accumulator
            .update_batch(&batch)
            .expect("Should update with batch");
        assert!(accumulator.arrays.is_empty());
        assert!(accumulator.exact_values_exceeded_memory_limit);

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        // Validate the expression is a range filter from 1 through 5, not a no-op filter.
        assert!(physical_expr.as_any().downcast_ref::<Literal>().is_none());

        let probe_schema = Schema::new(vec![Field::new("a", DataType::UInt64, false)]);
        let probe_array: ArrayRef = Arc::new(UInt64Array::from(vec![0, 1, 3, 5, 6]));
        let probe_batch = RecordBatch::try_new(Arc::new(probe_schema), vec![probe_array])
            .expect("Should create probe record batch");
        let actual_values = evaluate_boolean_expression(&physical_expr, &probe_batch);

        assert_eq!(
            vec![Some(false), Some(true), Some(true), Some(true), Some(false)],
            actual_values
        );
    }

    #[test]
    fn test_exact_left_accumulator_defers_range_bounds_until_memory_limit_exceeded() {
        let first_batch = create_uint64_batch(vec![10, 20]);
        let second_batch = create_uint64_batch(vec![1, 30]);
        let max_memory_size = first_batch.column(0).get_array_memory_size();

        let left_expr = col("a", &first_batch.schema()).expect("Should create column expr");
        let mut accumulator =
            ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&left_expr), max_memory_size);

        accumulator
            .update_batch(&first_batch)
            .expect("Should update first batch");
        assert_eq!(1, accumulator.arrays.len());
        assert!(accumulator.range_bounds.min_value.is_none());
        assert!(accumulator.range_bounds.max_value.is_none());
        assert!(!accumulator.exact_values_exceeded_memory_limit);

        accumulator
            .update_batch(&second_batch)
            .expect("Should update second batch");
        assert!(accumulator.arrays.is_empty());
        assert!(accumulator.exact_values_exceeded_memory_limit);

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        let probe_batch = create_uint64_batch(vec![0, 1, 15, 30, 31]);
        let actual_values = evaluate_boolean_expression(&physical_expr, &probe_batch);

        assert_eq!(
            vec![Some(false), Some(true), Some(true), Some(true), Some(false)],
            actual_values
        );
    }

    #[test]
    fn test_exact_left_accumulator_range_fallback_updates_after_limit_exceeded() {
        let first_batch = create_uint64_batch(vec![10, 20]);
        let second_batch = create_uint64_batch(vec![1, 30]);

        let left_expr = col("a", &first_batch.schema()).expect("Should create column expr");
        let mut accumulator =
            ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&left_expr), 1);

        accumulator
            .update_batch(&first_batch)
            .expect("Should update first batch");
        accumulator
            .update_batch(&second_batch)
            .expect("Should update second batch");
        assert!(accumulator.arrays.is_empty());
        assert!(accumulator.exact_values_exceeded_memory_limit);

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        let probe_batch = create_uint64_batch(vec![0, 1, 15, 30, 31]);
        let actual_values = evaluate_boolean_expression(&physical_expr, &probe_batch);

        assert_eq!(
            vec![Some(false), Some(true), Some(true), Some(true), Some(false)],
            actual_values
        );
    }

    #[test]
    fn test_exact_left_accumulator_range_fallback_with_nulls_returns_noop() {
        let batch = create_nullable_uint64_batch(vec![Some(1), None, Some(3)]);

        let left_expr = col("a", &batch.schema()).expect("Should create column expr");

        let mut accumulator =
            ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&left_expr), 1);

        accumulator
            .update_batch(&batch)
            .expect("Should update with batch");

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        assert_literal_true(&physical_expr);
    }

    #[test]
    fn test_exact_left_accumulator_range_fallback_with_only_nulls_returns_noop() {
        let batch = create_nullable_uint64_batch(vec![None, None]);

        let left_expr = col("a", &batch.schema()).expect("Should create column expr");
        let mut accumulator =
            ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&left_expr), 0);

        accumulator
            .update_batch(&batch)
            .expect("Should update with batch");

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        assert_literal_true(&physical_expr);
    }

    #[test]
    fn test_exact_left_accumulator_range_fallback_with_unsupported_type_returns_noop() {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![a]).expect("Should create record batch");

        let left_expr = col("a", &batch.schema()).expect("Should create column expr");
        let mut accumulator =
            ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&left_expr), 0);

        accumulator
            .update_batch(&batch)
            .expect("Should update with batch");

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        assert_literal_true(&physical_expr);
    }

    #[test]
    fn test_exact_left_accumulator_range_fallback_with_nan_returns_noop() {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.0, f64::NAN, 3.0]));
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![a]).expect("Should create record batch");

        let left_expr = col("a", &batch.schema()).expect("Should create column expr");
        let mut accumulator =
            ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&left_expr), 0);

        accumulator
            .update_batch(&batch)
            .expect("Should update with batch");

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        assert_literal_true(&physical_expr);
    }

    #[test]
    fn test_exact_left_accumulator_range_fallback_with_strings() {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let a: ArrayRef = Arc::new(StringArray::from(vec!["delta", "bravo", "charlie"]));
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![a]).expect("Should create record batch");

        let left_expr = col("a", &batch.schema()).expect("Should create column expr");
        let mut accumulator =
            ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&left_expr), 0);

        accumulator
            .update_batch(&batch)
            .expect("Should update with batch");

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        let probe_schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let probe_array: ArrayRef = Arc::new(StringArray::from(vec![
            "alpha", "bravo", "charlie", "delta", "zulu",
        ]));
        let probe_batch = RecordBatch::try_new(Arc::new(probe_schema), vec![probe_array])
            .expect("Should create probe record batch");
        let actual_values = evaluate_boolean_expression(&physical_expr, &probe_batch);

        assert_eq!(
            vec![Some(false), Some(true), Some(true), Some(true), Some(false)],
            actual_values
        );
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
