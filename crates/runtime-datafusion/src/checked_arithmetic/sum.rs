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
//! widens to `i128` / `u128`. Partial state is `Decimal128(38, 0)` so a
//! partition whose running total does not fit in `Int64` can still cancel at
//! merge; [`Accumulator::evaluate`] is the only narrowing point.
//!
//! `SUM(DISTINCT)` is covered too: `DataFusion`'s distinct accumulator wraps
//! unique values with `add_wrapping`, so `{i64::MAX, 1}` becomes `i64::MIN`.
//! Distinct state stays a list of unique keys; evaluate is the narrowing point.

use std::collections::{HashMap, HashSet};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DataType, Decimal128Type, Field, FieldRef, Int64Type, UInt64Type,
};
use datafusion::common::{Result, ScalarValue, exec_err, not_impl_err};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, GroupsAccumulator, ReversedUDAF,
    SetMonotonicity, Signature, StatisticsArgs,
};

use super::is_checked_sum_return_type;

/// Wide enough for any pair of `i64`/`u64` totals that can still cancel to a
/// representable `SUM` result. `DECIMAL128_MAX_PRECISION` is 38.
const CHECKED_SUM_STATE_PRECISION: u8 = DECIMAL128_MAX_PRECISION;
const CHECKED_SUM_STATE_SCALE: i8 = 0;

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
        match args.return_field.data_type() {
            DataType::Int64 if args.is_distinct => Ok(Box::new(CheckedInt64DistinctSum::new())),
            DataType::Int64 => Ok(Box::new(CheckedInt64Sum::new())),
            DataType::UInt64 if args.is_distinct => Ok(Box::new(CheckedUInt64DistinctSum::new())),
            DataType::UInt64 => Ok(Box::new(CheckedUInt64Sum::new())),
            _ => self.inner.accumulator(args),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if is_checked_sum_return_type(args.return_type()) {
            if args.is_distinct {
                return Ok(vec![
                    Field::new_list(
                        format_state_name(args.name, "sum distinct"),
                        Field::new_list_field(args.return_type().clone(), true),
                        false,
                    )
                    .into(),
                ]);
            }
            return Ok(vec![
                Field::new(
                    format_state_name(args.name, "sum"),
                    checked_sum_state_type(),
                    true,
                )
                .into(),
            ]);
        }
        self.inner.state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        if is_checked_sum_return_type(args.return_field.data_type()) {
            return false;
        }
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if is_checked_sum_return_type(args.return_field.data_type()) {
            return not_impl_err!(
                "checked integer SUM does not provide a GroupsAccumulator; use the row accumulator"
            );
        }
        self.inner.create_groups_accumulator(args)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        match args.return_field.data_type() {
            DataType::Int64 if args.is_distinct => {
                Ok(Box::new(CheckedInt64SlidingDistinctSum::new()))
            }
            DataType::Int64 => Ok(Box::new(CheckedInt64SlidingSum::new())),
            DataType::UInt64 if args.is_distinct => {
                Ok(Box::new(CheckedUInt64SlidingDistinctSum::new()))
            }
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

fn checked_sum_state_type() -> DataType {
    DataType::Decimal128(CHECKED_SUM_STATE_PRECISION, CHECKED_SUM_STATE_SCALE)
}

fn widened_state(sum: Option<i128>) -> ScalarValue {
    ScalarValue::Decimal128(sum, CHECKED_SUM_STATE_PRECISION, CHECKED_SUM_STATE_SCALE)
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

fn decimal128_values(array: &ArrayRef) -> Result<&PrimitiveArray<Decimal128Type>> {
    if let Some(values) = array.as_primitive_opt::<Decimal128Type>() {
        Ok(values)
    } else {
        exec_err!(
            "checked SUM expected Decimal128(38, 0) partial state, got {}",
            array.data_type()
        )
    }
}

fn fold_decimal128_i128(mut acc: i128, array: &ArrayRef) -> Result<i128> {
    for value in decimal128_values(array)?.iter().flatten() {
        acc = acc.checked_add(value).ok_or_else(int64_overflow)?;
    }
    Ok(acc)
}

fn merge_optional_i128(sum: &mut Option<i128>, array: &ArrayRef) -> Result<()> {
    let values = decimal128_values(array)?;
    if values.null_count() == values.len() {
        return Ok(());
    }
    *sum = Some(fold_decimal128_i128(sum.unwrap_or(0), array)?);
    Ok(())
}

fn fold_decimal128_u128(mut acc: u128, array: &ArrayRef) -> Result<u128> {
    for value in decimal128_values(array)?.iter().flatten() {
        let value = u128::try_from(value).map_err(|_| uint64_overflow())?;
        acc = acc.checked_add(value).ok_or_else(uint64_overflow)?;
    }
    Ok(acc)
}

fn merge_optional_u128(sum: &mut Option<u128>, array: &ArrayRef) -> Result<()> {
    let values = decimal128_values(array)?;
    if values.null_count() == values.len() {
        return Ok(());
    }
    *sum = Some(fold_decimal128_u128(sum.unwrap_or(0), array)?);
    Ok(())
}

fn u128_to_widened_state(sum: Option<u128>) -> Result<ScalarValue> {
    let widened = match sum {
        None => None,
        Some(sum) => Some(i128::try_from(sum).map_err(|_| uint64_overflow())?),
    };
    Ok(widened_state(widened))
}

fn non_null_count(array: &dyn Array) -> Result<u64> {
    u64::try_from(array.len() - array.null_count()).map_err(|_| {
        datafusion::error::DataFusionError::Internal(
            "SUM batch is larger than u64::MAX".to_string(),
        )
    })
}

fn list_state(values: &[ScalarValue], data_type: &DataType) -> Vec<ScalarValue> {
    vec![ScalarValue::List(ScalarValue::new_list_nullable(
        values, data_type,
    ))]
}

fn int64_list_state(values: &HashSet<i64>) -> Vec<ScalarValue> {
    let keys: Vec<ScalarValue> = values
        .iter()
        .copied()
        .map(Some)
        .map(ScalarValue::Int64)
        .collect();
    list_state(&keys, &DataType::Int64)
}

fn uint64_list_state(values: &HashSet<u64>) -> Vec<ScalarValue> {
    let keys: Vec<ScalarValue> = values
        .iter()
        .copied()
        .map(Some)
        .map(ScalarValue::UInt64)
        .collect();
    list_state(&keys, &DataType::UInt64)
}

fn distinct_list_array(states: &[ArrayRef]) -> Result<&arrow::array::ListArray> {
    let array = first_array(states)?;
    array.as_list_opt::<i32>().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!(
            "checked SUM(DISTINCT) expected List state, got {}",
            array.data_type()
        ))
    })
}

fn merge_int64_distinct(values: &mut HashSet<i64>, states: &[ArrayRef]) -> Result<()> {
    for maybe_inner in distinct_list_array(states)?.iter().flatten() {
        values.extend(int64_values(&maybe_inner)?.iter().flatten());
    }
    Ok(())
}

fn merge_uint64_distinct(values: &mut HashSet<u64>, states: &[ArrayRef]) -> Result<()> {
    for maybe_inner in distinct_list_array(states)?.iter().flatten() {
        values.extend(uint64_values(&maybe_inner)?.iter().flatten());
    }
    Ok(())
}

fn fold_distinct_i64(values: &HashSet<i64>) -> Result<ScalarValue> {
    if values.is_empty() {
        return Ok(ScalarValue::Int64(None));
    }
    let mut acc = 0_i128;
    for value in values {
        acc = acc
            .checked_add(i128::from(*value))
            .ok_or_else(int64_overflow)?;
    }
    let value = i64::try_from(acc).map_err(|_| int64_overflow())?;
    Ok(ScalarValue::Int64(Some(value)))
}

fn fold_distinct_u64(values: &HashSet<u64>) -> Result<ScalarValue> {
    if values.is_empty() {
        return Ok(ScalarValue::UInt64(None));
    }
    let mut acc = 0_u128;
    for value in values {
        acc = acc
            .checked_add(u128::from(*value))
            .ok_or_else(uint64_overflow)?;
    }
    let value = u64::try_from(acc).map_err(|_| uint64_overflow())?;
    Ok(ScalarValue::UInt64(Some(value)))
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
        Ok(vec![widened_state(self.sum)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.add_array(first_array(values)?)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        merge_optional_i128(&mut self.sum, first_array(states)?)
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
        Ok(vec![u128_to_widened_state(self.sum)?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.add_array(first_array(values)?)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        merge_optional_u128(&mut self.sum, first_array(states)?)
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
        Ok(vec![widened_state(Some(self.sum)), self.count.into()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.add_array(first_array(values)?)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Partial state is `(sum, count)`. Add the widened sums; take the
        // counts from the second column so we do not double-count non-nulls.
        self.sum = fold_decimal128_i128(self.sum, first_array(states)?)?;
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
        Ok(vec![
            u128_to_widened_state(Some(self.sum))?,
            self.count.into(),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.add_array(first_array(values)?)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.sum = fold_decimal128_u128(self.sum, first_array(states)?)?;
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

#[derive(Debug, Default)]
struct CheckedInt64DistinctSum {
    values: HashSet<i64>,
}

impl CheckedInt64DistinctSum {
    fn new() -> Self {
        Self {
            values: HashSet::new(),
        }
    }
}

impl Accumulator for CheckedInt64DistinctSum {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(int64_list_state(&self.values))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.values
            .extend(int64_values(first_array(values)?)?.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        merge_int64_distinct(&mut self.values, states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        fold_distinct_i64(&self.values)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.capacity() * size_of::<i64>()
    }
}

#[derive(Debug, Default)]
struct CheckedUInt64DistinctSum {
    values: HashSet<u64>,
}

impl CheckedUInt64DistinctSum {
    fn new() -> Self {
        Self {
            values: HashSet::new(),
        }
    }
}

impl Accumulator for CheckedUInt64DistinctSum {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(uint64_list_state(&self.values))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.values
            .extend(uint64_values(first_array(values)?)?.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        merge_uint64_distinct(&mut self.values, states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        fold_distinct_u64(&self.values)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.capacity() * size_of::<u64>()
    }
}

#[derive(Debug, Default)]
struct CheckedInt64SlidingDistinctSum {
    counts: HashMap<i64, usize>,
    sum: i128,
}

impl CheckedInt64SlidingDistinctSum {
    fn new() -> Self {
        Self {
            counts: HashMap::new(),
            sum: 0,
        }
    }

    fn update_value(&mut self, value: i64) -> Result<()> {
        match self.counts.entry(value) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                self.sum = self
                    .sum
                    .checked_add(i128::from(value))
                    .ok_or_else(int64_overflow)?;
                entry.insert(1);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                *entry.get_mut() += 1;
            }
        }
        Ok(())
    }

    fn retract_value(&mut self, value: i64) -> Result<()> {
        let std::collections::hash_map::Entry::Occupied(mut entry) = self.counts.entry(value)
        else {
            return Ok(());
        };
        let remaining = entry.get().saturating_sub(1);
        if remaining == 0 {
            self.sum = self
                .sum
                .checked_sub(i128::from(value))
                .ok_or_else(int64_overflow)?;
            entry.remove();
        } else {
            *entry.get_mut() = remaining;
        }
        Ok(())
    }

    fn apply_valid_values<F>(&mut self, array: &PrimitiveArray<Int64Type>, mut op: F) -> Result<()>
    where
        F: FnMut(&mut Self, i64) -> Result<()>,
    {
        if array.null_count() == 0 {
            for &value in array.values() {
                op(self, value)?;
            }
        } else {
            for (idx, &value) in array.values().iter().enumerate() {
                if array.is_valid(idx) {
                    op(self, value)?;
                }
            }
        }
        Ok(())
    }
}

impl Accumulator for CheckedInt64SlidingDistinctSum {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let keys: Vec<ScalarValue> = self
            .counts
            .keys()
            .copied()
            .map(Some)
            .map(ScalarValue::Int64)
            .collect();
        Ok(list_state(&keys, &DataType::Int64))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.apply_valid_values(int64_values(first_array(values)?)?, Self::update_value)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let mut keys = HashSet::new();
        merge_int64_distinct(&mut keys, states)?;
        for value in keys {
            self.update_value(value)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.counts.is_empty() {
            return Ok(ScalarValue::Int64(None));
        }
        let value = i64::try_from(self.sum).map_err(|_| int64_overflow())?;
        Ok(ScalarValue::Int64(Some(value)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.counts.capacity() * size_of::<(i64, usize)>()
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.apply_valid_values(int64_values(first_array(values)?)?, Self::retract_value)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Debug, Default)]
struct CheckedUInt64SlidingDistinctSum {
    counts: HashMap<u64, usize>,
    sum: u128,
}

impl CheckedUInt64SlidingDistinctSum {
    fn new() -> Self {
        Self {
            counts: HashMap::new(),
            sum: 0,
        }
    }

    fn update_value(&mut self, value: u64) -> Result<()> {
        match self.counts.entry(value) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                self.sum = self
                    .sum
                    .checked_add(u128::from(value))
                    .ok_or_else(uint64_overflow)?;
                entry.insert(1);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                *entry.get_mut() += 1;
            }
        }
        Ok(())
    }

    fn retract_value(&mut self, value: u64) -> Result<()> {
        let std::collections::hash_map::Entry::Occupied(mut entry) = self.counts.entry(value)
        else {
            return Ok(());
        };
        let remaining = entry.get().saturating_sub(1);
        if remaining == 0 {
            self.sum = self
                .sum
                .checked_sub(u128::from(value))
                .ok_or_else(uint64_overflow)?;
            entry.remove();
        } else {
            *entry.get_mut() = remaining;
        }
        Ok(())
    }

    fn apply_valid_values<F>(&mut self, array: &PrimitiveArray<UInt64Type>, mut op: F) -> Result<()>
    where
        F: FnMut(&mut Self, u64) -> Result<()>,
    {
        if array.null_count() == 0 {
            for &value in array.values() {
                op(self, value)?;
            }
        } else {
            for (idx, &value) in array.values().iter().enumerate() {
                if array.is_valid(idx) {
                    op(self, value)?;
                }
            }
        }
        Ok(())
    }
}

impl Accumulator for CheckedUInt64SlidingDistinctSum {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let keys: Vec<ScalarValue> = self
            .counts
            .keys()
            .copied()
            .map(Some)
            .map(ScalarValue::UInt64)
            .collect();
        Ok(list_state(&keys, &DataType::UInt64))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.apply_valid_values(uint64_values(first_array(values)?)?, Self::update_value)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let mut keys = HashSet::new();
        merge_uint64_distinct(&mut keys, states)?;
        for value in keys {
            self.update_value(value)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.counts.is_empty() {
            return Ok(ScalarValue::UInt64(None));
        }
        let value = u64::try_from(self.sum).map_err(|_| uint64_overflow())?;
        Ok(ScalarValue::UInt64(Some(value)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.counts.capacity() * size_of::<(u64, usize)>()
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.apply_valid_values(uint64_values(first_array(values)?)?, Self::retract_value)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod state_tests {
    use super::*;
    use arrow::array::{Int64Array, UInt64Array};

    fn int64_col(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn uint64_col(values: &[Option<u64>]) -> ArrayRef {
        Arc::new(UInt64Array::from(values.to_vec()))
    }

    #[test]
    fn int64_partial_state_is_decimal128_and_evaluate_narrows() {
        let mut acc = CheckedInt64Sum::new();
        acc.update_batch(&[int64_col(&[Some(i64::MAX), Some(i64::MAX)])])
            .expect("update MAX+MAX");
        assert_eq!(
            acc.state().expect("widened state"),
            vec![ScalarValue::Decimal128(
                Some(
                    i128::from(i64::MAX)
                        .checked_add(i128::from(i64::MAX))
                        .expect("2*MAX fits i128")
                ),
                CHECKED_SUM_STATE_PRECISION,
                CHECKED_SUM_STATE_SCALE,
            )]
        );
        let error = acc
            .evaluate()
            .expect_err("MAX+MAX must overflow at evaluate");
        assert!(
            error.to_string().to_ascii_lowercase().contains("overflow"),
            "evaluate overflow, got {error}"
        );
    }

    #[test]
    fn int64_merge_cancels_partials_that_overflow_i64() {
        let mut left = CheckedInt64Sum::new();
        left.update_batch(&[int64_col(&[Some(i64::MAX), Some(i64::MAX)])])
            .expect("left MAX+MAX");
        let mut right = CheckedInt64Sum::new();
        right
            .update_batch(&[int64_col(&[Some(-i64::MAX)])])
            .expect("right -MAX");

        let mut merged = CheckedInt64Sum::new();
        let left_state = left.state().expect("left state");
        let left_array = left_state[0].to_array().expect("left state array");
        merged.merge_batch(&[left_array]).expect("merge left");
        let right_state = right.state().expect("right state");
        let right_array = right_state[0].to_array().expect("right state array");
        merged.merge_batch(&[right_array]).expect("merge right");
        assert_eq!(
            merged.evaluate().expect("cancelled total"),
            ScalarValue::Int64(Some(i64::MAX))
        );
    }

    #[test]
    fn int64_distinct_duplicates_are_unique_and_max_plus_one_overflows() {
        let mut acc = CheckedInt64DistinctSum::new();
        acc.update_batch(&[int64_col(&[Some(i64::MAX), Some(i64::MAX)])])
            .expect("two copies of MAX");
        assert_eq!(
            acc.evaluate().expect("DISTINCT of one value"),
            ScalarValue::Int64(Some(i64::MAX))
        );

        let mut overflow = CheckedInt64DistinctSum::new();
        overflow
            .update_batch(&[int64_col(&[Some(i64::MAX), Some(1)])])
            .expect("MAX and 1");
        let error = overflow
            .evaluate()
            .expect_err("DISTINCT MAX+1 must overflow");
        assert!(
            error.to_string().to_ascii_lowercase().contains("overflow"),
            "distinct overflow, got {error}"
        );
    }

    #[test]
    fn int64_distinct_merge_unions_keys_then_overflows() {
        let mut left = CheckedInt64DistinctSum::new();
        left.update_batch(&[int64_col(&[Some(i64::MAX)])])
            .expect("left MAX");
        let mut right = CheckedInt64DistinctSum::new();
        right
            .update_batch(&[int64_col(&[Some(1)])])
            .expect("right 1");

        let mut merged = CheckedInt64DistinctSum::new();
        let left_state = left.state().expect("left state");
        let left_array = left_state[0].to_array().expect("left state array");
        merged.merge_batch(&[left_array]).expect("merge left");
        let right_state = right.state().expect("right state");
        let right_array = right_state[0].to_array().expect("right state array");
        merged.merge_batch(&[right_array]).expect("merge right");
        let error = merged
            .evaluate()
            .expect_err("merged DISTINCT MAX+1 must overflow");
        assert!(
            error.to_string().to_ascii_lowercase().contains("overflow"),
            "merged distinct overflow, got {error}"
        );
    }

    #[test]
    fn int64_sliding_distinct_retract_batch_keeps_duplicates_then_nulls() {
        let mut acc = CheckedInt64SlidingDistinctSum::new();
        assert!(
            acc.supports_retract_batch(),
            "bounded DISTINCT windows require retract_batch"
        );

        acc.update_batch(&[int64_col(&[Some(5)])])
            .expect("enter first 5");
        assert_eq!(
            acc.evaluate().expect("frame [5]"),
            ScalarValue::Int64(Some(5))
        );

        acc.update_batch(&[int64_col(&[Some(5)])])
            .expect("enter second 5");
        assert_eq!(
            acc.evaluate().expect("frame [5, 5]"),
            ScalarValue::Int64(Some(5))
        );

        acc.retract_batch(&[int64_col(&[Some(5)])])
            .expect("leave first 5");
        acc.update_batch(&[int64_col(&[Some(7)])])
            .expect("enter first 7");
        assert_eq!(
            acc.evaluate().expect("frame [5, 7]"),
            ScalarValue::Int64(Some(12))
        );

        acc.retract_batch(&[int64_col(&[Some(5)])])
            .expect("leave last 5");
        acc.update_batch(&[int64_col(&[Some(7)])])
            .expect("enter second 7");
        assert_eq!(
            acc.evaluate().expect("frame [7, 7]"),
            ScalarValue::Int64(Some(7))
        );

        acc.retract_batch(&[int64_col(&[Some(7)])])
            .expect("leave first 7");
        acc.update_batch(&[int64_col(&[None])])
            .expect("enter first NULL");
        assert_eq!(
            acc.evaluate().expect("frame [7, NULL]"),
            ScalarValue::Int64(Some(7))
        );

        acc.retract_batch(&[int64_col(&[Some(7)])])
            .expect("leave last 7");
        acc.update_batch(&[int64_col(&[None])])
            .expect("enter second NULL");
        assert_eq!(
            acc.evaluate().expect("NULL-only frame"),
            ScalarValue::Int64(None)
        );
    }

    #[test]
    fn uint64_sliding_distinct_retract_batch_keeps_duplicates_then_nulls() {
        let mut acc = CheckedUInt64SlidingDistinctSum::new();
        assert!(
            acc.supports_retract_batch(),
            "bounded DISTINCT windows require retract_batch"
        );

        acc.update_batch(&[uint64_col(&[Some(5)])])
            .expect("enter first 5");
        acc.update_batch(&[uint64_col(&[Some(5)])])
            .expect("enter second 5");
        acc.retract_batch(&[uint64_col(&[Some(5)])])
            .expect("leave first 5");
        acc.update_batch(&[uint64_col(&[Some(7)])])
            .expect("enter first 7");
        assert_eq!(
            acc.evaluate().expect("frame [5, 7]"),
            ScalarValue::UInt64(Some(12))
        );

        acc.retract_batch(&[uint64_col(&[Some(5)])])
            .expect("leave last 5");
        acc.update_batch(&[uint64_col(&[Some(7)])])
            .expect("enter second 7");
        acc.retract_batch(&[uint64_col(&[Some(7)])])
            .expect("leave first 7");
        acc.update_batch(&[uint64_col(&[None])])
            .expect("enter first NULL");
        assert_eq!(
            acc.evaluate().expect("frame [7, NULL]"),
            ScalarValue::UInt64(Some(7))
        );

        acc.retract_batch(&[uint64_col(&[Some(7)])])
            .expect("leave last 7");
        acc.update_batch(&[uint64_col(&[None])])
            .expect("enter second NULL");
        assert_eq!(
            acc.evaluate().expect("NULL-only frame"),
            ScalarValue::UInt64(None)
        );
    }
}
