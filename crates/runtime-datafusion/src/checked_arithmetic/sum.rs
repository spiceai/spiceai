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

//! `SUM` that errors when an `Int64` / `UInt64` total is not representable.
//!
//! The built-in accumulator uses wrapping `arrow::compute::sum` and
//! `add_wrapping`. A wrapping SIMD total cannot be checked after the fact:
//! `[i64::MAX, i64::MAX, 2]` wraps twice and lands on zero. This accumulator
//! widens to `i128` / `u128` and converts back.

use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use arrow::datatypes::{DataType, FieldRef, Int64Type, UInt64Type};
use datafusion::common::{Result, ScalarValue, exec_err, not_impl_err};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, GroupsAccumulator, ReversedUDAF,
    SetMonotonicity, Signature, StatisticsArgs,
};

use super::is_checked_sum_return_type;

/// Registry `sum` that checks `Int64` / `UInt64` totals and otherwise delegates.
#[derive(Debug)]
pub struct CheckedSum {
    inner: Arc<AggregateUDF>,
}

impl CheckedSum {
    #[must_use]
    pub fn new() -> Self {
        Self { inner: sum_udaf() }
    }
}

impl Default for CheckedSum {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for CheckedSum {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for CheckedSum {}

impl std::hash::Hash for CheckedSum {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl AggregateUDFImpl for CheckedSum {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return self.inner.accumulator(args);
        }
        match args.return_field.data_type() {
            DataType::Int64 => Ok(Box::new(CheckedInt64Sum::new())),
            DataType::UInt64 => Ok(Box::new(CheckedUInt64Sum::new())),
            _ => self.inner.accumulator(args),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        if !args.is_distinct && is_checked_sum_return_type(args.return_field.data_type()) {
            return false;
        }
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if !args.is_distinct && is_checked_sum_return_type(args.return_field.data_type()) {
            return not_impl_err!(
                "checked integer SUM does not provide a GroupsAccumulator; use the row accumulator"
            );
        }
        self.inner.create_groups_accumulator(args)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return self.inner.create_sliding_accumulator(args);
        }
        match args.return_field.data_type() {
            DataType::Int64 => Ok(Box::new(CheckedInt64SlidingSum::new())),
            DataType::UInt64 => Ok(Box::new(CheckedUInt64SlidingSum::new())),
            _ => self.inner.create_sliding_accumulator(args),
        }
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_udf()
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.inner().set_monotonicity(data_type)
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        if is_checked_sum_return_type(statistics_args.return_type) {
            return None;
        }
        self.inner.value_from_stats(statistics_args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner.default_value(data_type)
    }
}

fn int64_overflow() -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(
        "Overflow in SUM of BIGINT: the total is not representable as an Int64".to_string(),
    )
}

fn uint64_overflow() -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(
        "Overflow in SUM of BIGINT: the total is not representable as a UInt64".to_string(),
    )
}

fn first_array(values: &[ArrayRef]) -> Result<&ArrayRef> {
    values.first().ok_or_else(|| {
        datafusion::error::DataFusionError::Internal("SUM expected one input column".to_string())
    })
}

fn int64_values(array: &ArrayRef) -> Result<&PrimitiveArray<Int64Type>> {
    if let Some(values) = array.as_primitive_opt::<Int64Type>() {
        Ok(values)
    } else {
        exec_err!(
            "checked SUM of BIGINT expected Int64 values, got {}",
            array.data_type()
        )
    }
}

fn uint64_values(array: &ArrayRef) -> Result<&PrimitiveArray<UInt64Type>> {
    if let Some(values) = array.as_primitive_opt::<UInt64Type>() {
        Ok(values)
    } else {
        exec_err!(
            "checked SUM of BIGINT expected UInt64 values, got {}",
            array.data_type()
        )
    }
}

fn non_null_count(array: &dyn Array) -> Result<u64> {
    u64::try_from(array.len() - array.null_count()).map_err(|_| {
        datafusion::error::DataFusionError::Internal(
            "SUM batch is larger than u64::MAX".to_string(),
        )
    })
}

#[derive(Debug, Default)]
struct CheckedInt64Sum {
    sum: Option<i128>,
}

impl CheckedInt64Sum {
    fn new() -> Self {
        Self { sum: None }
    }

    fn add_array(&mut self, array: &ArrayRef) -> Result<()> {
        let values = int64_values(array)?;
        if values.null_count() == values.len() {
            return Ok(());
        }
        let mut acc = self.sum.unwrap_or(0);
        for value in values.iter().flatten() {
            acc = acc
                .checked_add(i128::from(value))
                .ok_or_else(int64_overflow)?;
        }
        self.sum = Some(acc);
        Ok(())
    }
}

impl Accumulator for CheckedInt64Sum {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.add_array(first_array(values)?)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.add_array(first_array(states)?)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self.sum {
            None => Ok(ScalarValue::Int64(None)),
            Some(sum) => {
                let value = i64::try_from(sum).map_err(|_| int64_overflow())?;
                Ok(ScalarValue::Int64(Some(value)))
            }
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

#[derive(Debug, Default)]
struct CheckedUInt64Sum {
    sum: Option<u128>,
}

impl CheckedUInt64Sum {
    fn new() -> Self {
        Self { sum: None }
    }

    fn add_array(&mut self, array: &ArrayRef) -> Result<()> {
        let values = uint64_values(array)?;
        if values.null_count() == values.len() {
            return Ok(());
        }
        let mut acc = self.sum.unwrap_or(0);
        for value in values.iter().flatten() {
            acc = acc
                .checked_add(u128::from(value))
                .ok_or_else(uint64_overflow)?;
        }
        self.sum = Some(acc);
        Ok(())
    }
}

impl Accumulator for CheckedUInt64Sum {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.add_array(first_array(values)?)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.add_array(first_array(states)?)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match self.sum {
            None => Ok(ScalarValue::UInt64(None)),
            Some(sum) => {
                let value = u64::try_from(sum).map_err(|_| uint64_overflow())?;
                Ok(ScalarValue::UInt64(Some(value)))
            }
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

#[derive(Debug, Default)]
struct CheckedInt64SlidingSum {
    sum: i128,
    count: u64,
}

impl CheckedInt64SlidingSum {
    fn new() -> Self {
        Self { sum: 0, count: 0 }
    }

    fn add_array(&mut self, array: &ArrayRef) -> Result<()> {
        let values = int64_values(array)?;
        self.count = self.count.saturating_add(non_null_count(values)?);
        for value in values.iter().flatten() {
            self.sum = self
                .sum
                .checked_add(i128::from(value))
                .ok_or_else(int64_overflow)?;
        }
        Ok(())
    }
}

impl Accumulator for CheckedInt64SlidingSum {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?, self.count.into()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.add_array(first_array(values)?)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Partial state is `(sum, count)`. Add the sums; take the counts
        // from the second column so we do not double-count non-nulls.
        let values = int64_values(first_array(states)?)?;
        for value in values.iter().flatten() {
            self.sum = self
                .sum
                .checked_add(i128::from(value))
                .ok_or_else(int64_overflow)?;
        }
        if let Some(extra) = states
            .get(1)
            .and_then(|counts| arrow::compute::sum(counts.as_primitive::<UInt64Type>()))
        {
            self.count = self.count.saturating_add(extra);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0 {
            return Ok(ScalarValue::Int64(None));
        }
        let value = i64::try_from(self.sum).map_err(|_| int64_overflow())?;
        Ok(ScalarValue::Int64(Some(value)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = int64_values(first_array(values)?)?;
        for value in values.iter().flatten() {
            self.sum = self
                .sum
                .checked_sub(i128::from(value))
                .ok_or_else(int64_overflow)?;
        }
        self.count = self.count.saturating_sub(non_null_count(values)?);
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Debug, Default)]
struct CheckedUInt64SlidingSum {
    sum: u128,
    count: u64,
}

impl CheckedUInt64SlidingSum {
    fn new() -> Self {
        Self { sum: 0, count: 0 }
    }

    fn add_array(&mut self, array: &ArrayRef) -> Result<()> {
        let values = uint64_values(array)?;
        self.count = self.count.saturating_add(non_null_count(values)?);
        for value in values.iter().flatten() {
            self.sum = self
                .sum
                .checked_add(u128::from(value))
                .ok_or_else(uint64_overflow)?;
        }
        Ok(())
    }
}

impl Accumulator for CheckedUInt64SlidingSum {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?, self.count.into()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.add_array(first_array(values)?)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let values = uint64_values(first_array(states)?)?;
        for value in values.iter().flatten() {
            self.sum = self
                .sum
                .checked_add(u128::from(value))
                .ok_or_else(uint64_overflow)?;
        }
        if let Some(extra) = states
            .get(1)
            .and_then(|counts| arrow::compute::sum(counts.as_primitive::<UInt64Type>()))
        {
            self.count = self.count.saturating_add(extra);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.count == 0 {
            return Ok(ScalarValue::UInt64(None));
        }
        let value = u64::try_from(self.sum).map_err(|_| uint64_overflow())?;
        Ok(ScalarValue::UInt64(Some(value)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = uint64_values(first_array(values)?)?;
        for value in values.iter().flatten() {
            self.sum = self
                .sum
                .checked_sub(u128::from(value))
                .ok_or_else(uint64_overflow)?;
        }
        self.count = self.count.saturating_sub(non_null_count(values)?);
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}
