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
//! A batch is totalled in `u64` lanes over the halves of each value (see
//! [`exact_sum_i64`]), which is exact and vectorizes, and the batch total is
//! then added to the running total with a check. `GROUP BY` gets
//! [`CheckedSumGroups`], which keeps one `i128` total per group and the same
//! partial state, so the hash aggregate does not fall back to adapting the
//! row accumulator group by group (and, once it skips partial aggregation,
//! row by row).
//!
//! `SUM(DISTINCT)` is covered too: `DataFusion`'s distinct accumulator wraps
//! unique values with `add_wrapping`, so `{i64::MAX, 1}` becomes `i64::MIN`.
//! Distinct state stays a list of unique keys; evaluate is the narrowing point.

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    ArrowPrimitiveType, DECIMAL128_MAX_PRECISION, DataType, Decimal128Type, Field, FieldRef,
    Int64Type, UInt64Type,
};
use datafusion::common::{Result, ScalarValue, exec_err, internal_err, not_impl_err};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, Expr, GroupsAccumulator,
    Operator, ReversedUDAF, SetMonotonicity, Signature, StatisticsArgs,
};
use datafusion::physical_expr::NullState;

use super::is_checked_sum_return_type;
use super::sum_plus_literal::sum_plus_literal;

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
            // `SUM(DISTINCT)` keeps the row accumulator, as the built-in does.
            return !args.is_distinct;
        }
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        match args.return_field.data_type() {
            DataType::Int64 if !args.is_distinct => {
                Ok(Box::new(CheckedSumGroups::<Int64Type>::new()))
            }
            DataType::UInt64 if !args.is_distinct => {
                Ok(Box::new(CheckedSumGroups::<UInt64Type>::new()))
            }
            data_type if is_checked_sum_return_type(data_type) => not_impl_err!(
                "checked integer SUM(DISTINCT) does not provide a GroupsAccumulator; use the row accumulator"
            ),
            _ => self.inner.create_groups_accumulator(args),
        }
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

    /// `DataFusion` offers `SUM(arg + literal)` here when several `SUM`s share
    /// `arg`, to rewrite to `SUM(arg) + literal * COUNT(arg)`. A signed
    /// integer `arg` gets the exact rewrite in `sum_plus_literal`.
    ///
    /// Every other type is left as written rather than handed to the
    /// built-in's rewrite: that one adds and multiplies with wrapping kernels,
    /// widens a decimal `SUM`'s result type to precision 38, and does not plan
    /// for an interval (it multiplies two intervals).
    fn simplify_expr_op_literal(
        &self,
        agg_function: &AggregateFunction,
        arg: &Expr,
        op: Operator,
        lit: &Expr,
        _arg_is_left: bool,
    ) -> Result<Option<Expr>> {
        // `+` only, as the built-in, so the operand order does not matter.
        if op != Operator::Plus {
            return Ok(None);
        }
        let Expr::Literal(value, _) = lit else {
            return Ok(None);
        };
        let k = match value {
            ScalarValue::Int8(k) => k.map(i64::from),
            ScalarValue::Int16(k) => k.map(i64::from),
            ScalarValue::Int32(k) => k.map(i64::from),
            ScalarValue::Int64(k) => *k,
            _ => None,
        };
        Ok(k.map(|k| sum_plus_literal(agg_function, arg, k)))
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

pub(super) fn int64_overflow() -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(
        "Overflow in SUM of BIGINT: the total is not representable as an Int64".to_string(),
    )
}

fn uint64_overflow() -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(
        "Overflow in SUM of BIGINT: the total is not representable as a UInt64".to_string(),
    )
}

pub(super) fn checked_sum_state_type() -> DataType {
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

/// Values per lane pass in [`exact_sum_i64`] and [`exact_sum_u64`]. Each value
/// adds less than 2^32 to a `u64` lane, so no lane can overflow in a chunk of
/// 2^31 values.
const EXACT_SUM_CHUNK: usize = 1 << 31;

/// Mask of the low 32 bits of a `u64`.
const LOW_HALF: u64 = 0xFFFF_FFFF;

/// Exact total of `values`.
///
/// Each value is read as its two's-complement bits and split into 32-bit
/// halves. The halves, and the count of negative values, are summed in `u64`
/// lanes, which cannot overflow within a chunk and which the compiler can
/// vectorize; the lanes are recombined in `i128`, subtracting 2^64 for each
/// negative value. The result never overflows: a slice holds fewer than 2^64
/// values, each of magnitude at most 2^63.
fn exact_sum_i64(values: &[i64]) -> i128 {
    exact_sum_i64_in_chunks(values, EXACT_SUM_CHUNK)
}

/// [`exact_sum_i64`] with lane passes of `chunk_len` values, which must be
/// between 1 and [`EXACT_SUM_CHUNK`].
fn exact_sum_i64_in_chunks(values: &[i64], chunk_len: usize) -> i128 {
    let mut total = 0_i128;
    for chunk in values.chunks(chunk_len) {
        let (mut high, mut low, mut negative) = (0_u64, 0_u64, 0_u64);
        for &value in chunk {
            let bits = value.cast_unsigned();
            high += bits >> 32;
            low += bits & LOW_HALF;
            negative += bits >> 63;
        }
        total += (i128::from(high) << 32) + i128::from(low) - (i128::from(negative) << 64);
    }
    total
}

/// Exact total of `values`, summed in two `u64` lanes over the 32-bit halves
/// of each value. See [`exact_sum_i64`].
fn exact_sum_u64(values: &[u64]) -> u128 {
    exact_sum_u64_in_chunks(values, EXACT_SUM_CHUNK)
}

/// [`exact_sum_u64`] with lane passes of `chunk_len` values, which must be
/// between 1 and [`EXACT_SUM_CHUNK`].
fn exact_sum_u64_in_chunks(values: &[u64], chunk_len: usize) -> u128 {
    let mut total = 0_u128;
    for chunk in values.chunks(chunk_len) {
        let (mut high, mut low) = (0_u64, 0_u64);
        for &value in chunk {
            high += value >> 32;
            low += value & LOW_HALF;
        }
        total += (u128::from(high) << 32) + u128::from(low);
    }
    total
}

/// The `Int64` / `UInt64` input of a [`CheckedSumGroups`]. A value widens to
/// its `i128` amount with `Into`, and a total narrows back to the `SUM` result
/// with `TryFrom`, which fails when the total is not representable.
trait CheckedSumInput: ArrowPrimitiveType<Native: Into<i128> + TryFrom<i128>> + Send + Sized {
    fn values_of(array: &ArrayRef) -> Result<&PrimitiveArray<Self>>;

    /// Whether a partial total from another partition is a total this input
    /// type can produce. The row accumulator rejects the same values on merge.
    fn partial_in_range(partial: i128) -> bool;

    fn overflow() -> datafusion::error::DataFusionError;
}

impl CheckedSumInput for Int64Type {
    fn values_of(array: &ArrayRef) -> Result<&PrimitiveArray<Self>> {
        int64_values(array)
    }

    fn partial_in_range(_partial: i128) -> bool {
        true
    }

    fn overflow() -> datafusion::error::DataFusionError {
        int64_overflow()
    }
}

impl CheckedSumInput for UInt64Type {
    fn values_of(array: &ArrayRef) -> Result<&PrimitiveArray<Self>> {
        uint64_values(array)
    }

    fn partial_in_range(partial: i128) -> bool {
        partial >= 0
    }

    fn overflow() -> datafusion::error::DataFusionError {
        uint64_overflow()
    }
}

/// Checked `SUM(BIGINT)` for `GROUP BY`: one `i128` total per group.
///
/// Adding one `Int64` or `UInt64` value cannot overflow an `i128` total
/// before 2^64 values reach one group; every add is still checked, and
/// [`GroupsAccumulator::evaluate`] is the only narrowing point, as in
/// [`CheckedInt64Sum`]. The partial state is the row accumulators':
/// `Decimal128(38, 0)`, null for a group that has seen no value. As for the
/// built-in `SUM`, [`NullState`] tracks which groups have seen a value.
#[derive(Debug)]
struct CheckedSumGroups<T: CheckedSumInput> {
    /// One total per group; 0 for a group that has seen no value.
    sums: Vec<i128>,
    null_state: NullState,
    input: PhantomData<T>,
}

impl<T: CheckedSumInput> CheckedSumGroups<T> {
    fn new() -> Self {
        Self {
            sums: Vec::new(),
            null_state: NullState::new(),
            input: PhantomData,
        }
    }

    /// Adds each non-null row of `values` that `opt_filter` keeps to its
    /// group. `widen` gives the amount a value adds, or `None` when the value
    /// is out of range for `T`.
    fn accumulate<V: ArrowPrimitiveType + Send>(
        &mut self,
        values: &PrimitiveArray<V>,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        widen: impl Fn(V::Native) -> Option<i128> + Send + Sync,
    ) -> Result<()> {
        ensure_rows(values.len(), group_indices.len(), "group indices")?;
        if let Some(filter) = opt_filter {
            ensure_rows(values.len(), filter.len(), "filter values")?;
        }
        self.sums.resize(total_num_groups, 0);
        let sums = &mut self.sums;
        let mut overflow = false;
        let mut bad_group = None;
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group, value| {
                let Some(sum) = sums.get_mut(group) else {
                    bad_group = Some(group);
                    return;
                };
                if let Some(total) = widen(value).and_then(|wide| sum.checked_add(wide)) {
                    *sum = total;
                } else {
                    overflow = true;
                }
            },
        );
        if let Some(group) = bad_group {
            return internal_err!(
                "checked SUM got group index {group} for {total_num_groups} groups"
            );
        }
        if overflow {
            return Err(T::overflow());
        }
        Ok(())
    }
}

fn ensure_rows(expected: usize, actual: usize, what: &str) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        internal_err!("checked SUM got {actual} {what} for {expected} rows")
    }
}

fn decimal_state(sums: Vec<i128>, nulls: Option<NullBuffer>) -> Result<ArrayRef> {
    let state = PrimitiveArray::<Decimal128Type>::try_new(sums.into(), nulls)?
        .with_precision_and_scale(CHECKED_SUM_STATE_PRECISION, CHECKED_SUM_STATE_SCALE)?;
    Ok(Arc::new(state))
}

impl<T: CheckedSumInput> GroupsAccumulator for CheckedSumGroups<T> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let values = T::values_of(first_array(values)?)?;
        self.accumulate(
            values,
            group_indices,
            opt_filter,
            total_num_groups,
            |value| Some(value.into()),
        )
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        let partials = decimal128_values(first_array(values)?)?;
        self.accumulate(partials, group_indices, None, total_num_groups, |partial| {
            T::partial_in_range(partial).then_some(partial)
        })
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let sums = emit_to.take_needed(&mut self.sums);
        let nulls = self.null_state.build(emit_to);
        // A group that has seen no value holds 0, so every total narrows.
        let totals = sums
            .into_iter()
            .map(|sum| T::Native::try_from(sum).map_err(|_| T::overflow()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(PrimitiveArray::<T>::try_new(
            totals.into(),
            nulls,
        )?))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);
        let nulls = self.null_state.build(emit_to);
        Ok(vec![decimal_state(sums, nulls)?])
    }

    /// Each row becomes its own partial total: the widened value, or null
    /// when the value is null or the filter drops the row.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = T::values_of(first_array(values)?)?;
        let sums: Vec<i128> = values.values().iter().map(|&value| value.into()).collect();
        let nulls = match opt_filter {
            None => values.nulls().cloned(),
            Some(filter) => {
                ensure_rows(values.len(), filter.len(), "filter values")?;
                // A null filter value drops the row, as `false` does.
                let kept = filter.nulls().map_or_else(
                    || filter.values().clone(),
                    |filter_nulls| filter_nulls.inner() & filter.values(),
                );
                NullBuffer::union(values.nulls(), Some(&NullBuffer::new(kept)))
            }
        };
        Ok(vec![decimal_state(sums, nulls)?])
    }

    fn size(&self) -> usize {
        self.sums.capacity() * size_of::<i128>() + self.null_state.size()
    }
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
        let batch = if values.null_count() == 0 {
            exact_sum_i64(values.values())
        } else {
            values.iter().flatten().map(i128::from).sum()
        };
        let acc = self.sum.unwrap_or(0);
        self.sum = Some(acc.checked_add(batch).ok_or_else(int64_overflow)?);
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
        // Narrow only here. `SingleDistinctToGroupBy` must not evaluate a
        // per-group total that is not the query result; see
        // `CheckedSingleDistinctToGroupBy`.
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
        let batch = if values.null_count() == 0 {
            exact_sum_u64(values.values())
        } else {
            values.iter().flatten().map(u128::from).sum()
        };
        let acc = self.sum.unwrap_or(0);
        self.sum = Some(acc.checked_add(batch).ok_or_else(uint64_overflow)?);
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

    /// `xorshift64*`: seeded inputs without a dependency.
    fn next_random(state: &mut u64) -> u64 {
        *state ^= *state >> 12;
        *state ^= *state << 25;
        *state ^= *state >> 27;
        state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    #[test]
    fn exact_sums_match_an_i128_fold_on_extremes_and_seeded_values() {
        const SEED: u64 = 0x5EED_0000_0000_0042;
        let signed_extremes = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
        let unsigned_extremes = [
            0,
            1,
            u64::from(u32::MAX),
            u64::from(u32::MAX) + 1,
            u64::MAX - 1,
            u64::MAX,
        ];
        let mut state = SEED;
        let mut signed_cases: Vec<Vec<i64>> = vec![vec![i64::MIN; 8_192], vec![i64::MAX; 8_192]];
        let mut unsigned_cases: Vec<Vec<u64>> = vec![vec![u64::MAX; 8_192]];
        for len in [0_usize, 1, 2, 3, 7, 64, 1_000, 8_192] {
            signed_cases.push(
                (0..len)
                    .map(|i| {
                        let bits = next_random(&mut state);
                        if bits.is_multiple_of(4) {
                            signed_extremes[i % signed_extremes.len()]
                        } else {
                            bits.cast_signed()
                        }
                    })
                    .collect(),
            );
            unsigned_cases.push(
                (0..len)
                    .map(|i| {
                        let bits = next_random(&mut state);
                        if bits.is_multiple_of(4) {
                            unsigned_extremes[i % unsigned_extremes.len()]
                        } else {
                            bits
                        }
                    })
                    .collect(),
            );
        }
        for (case, values) in signed_cases.iter().enumerate() {
            let fold: i128 = values.iter().map(|&value| i128::from(value)).sum();
            assert_eq!(
                exact_sum_i64(values),
                fold,
                "seed {SEED:#x}, signed case {case} ({} values)",
                values.len()
            );
            for chunk_len in [1, 2, 3, 1_000] {
                assert_eq!(
                    exact_sum_i64_in_chunks(values, chunk_len),
                    fold,
                    "seed {SEED:#x}, signed case {case}, chunks of {chunk_len}"
                );
            }
        }
        for (case, values) in unsigned_cases.iter().enumerate() {
            let fold: u128 = values.iter().map(|&value| u128::from(value)).sum();
            assert_eq!(
                exact_sum_u64(values),
                fold,
                "seed {SEED:#x}, unsigned case {case} ({} values)",
                values.len()
            );
            for chunk_len in [1, 2, 3, 1_000] {
                assert_eq!(
                    exact_sum_u64_in_chunks(values, chunk_len),
                    fold,
                    "seed {SEED:#x}, unsigned case {case}, chunks of {chunk_len}"
                );
            }
        }
    }

    fn int64_groups(array: &ArrayRef) -> Vec<Option<i64>> {
        array.as_primitive::<Int64Type>().iter().collect()
    }

    fn state_rows(states: &[ArrayRef]) -> Vec<Option<i128>> {
        assert_eq!(states.len(), 1, "checked SUM has one state column");
        assert_eq!(
            states[0].data_type(),
            &checked_sum_state_type(),
            "checked SUM state type"
        );
        states[0].as_primitive::<Decimal128Type>().iter().collect()
    }

    #[test]
    fn int64_groups_skip_null_and_filtered_rows_and_null_groups_with_no_value() {
        let mut acc = CheckedSumGroups::<Int64Type>::new();
        let filter =
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(false), None]);
        acc.update_batch(
            &[int64_col(&[Some(5), None, Some(-2), Some(7), Some(100)])],
            &[0, 1, 0, 2, 2],
            Some(&filter),
            4,
        )
        .expect("update with a filter");
        assert_eq!(
            int64_groups(&acc.evaluate(EmitTo::All).expect("evaluate")),
            vec![Some(3), None, None, None]
        );
    }

    #[test]
    fn int64_groups_keep_a_wide_state_that_cancels_on_merge_or_overflows_at_evaluate() {
        let mut partial = CheckedSumGroups::<Int64Type>::new();
        partial
            .update_batch(
                &[int64_col(&[Some(i64::MAX), Some(i64::MAX), Some(1)])],
                &[0, 0, 1],
                None,
                2,
            )
            .expect("partial MAX+MAX");
        let state = partial.state(EmitTo::All).expect("partial state");
        assert_eq!(
            state_rows(&state),
            vec![Some(i128::from(i64::MAX) * 2), Some(1)]
        );

        let mut cancel = CheckedSumGroups::<Int64Type>::new();
        cancel
            .merge_batch(&state, &[0, 1], 2)
            .expect("merge the partial");
        cancel
            .update_batch(&[int64_col(&[Some(-i64::MAX)])], &[0], None, 2)
            .expect("add -MAX");
        assert_eq!(
            int64_groups(&cancel.evaluate(EmitTo::All).expect("MAX+MAX-MAX fits")),
            vec![Some(i64::MAX), Some(1)]
        );

        let mut overflow = CheckedSumGroups::<Int64Type>::new();
        overflow
            .merge_batch(&state, &[0, 1], 2)
            .expect("merge the partial");
        let error = overflow
            .evaluate(EmitTo::All)
            .expect_err("MAX+MAX must overflow at evaluate");
        assert_eq!(error.to_string(), int64_overflow().to_string());
    }

    #[test]
    fn int64_groups_emit_first_keeps_the_remaining_groups_in_order() {
        let mut acc = CheckedSumGroups::<Int64Type>::new();
        acc.update_batch(
            &[int64_col(&[Some(1), Some(2), Some(3), None])],
            &[0, 1, 2, 3],
            None,
            4,
        )
        .expect("four groups");
        assert_eq!(
            int64_groups(&acc.evaluate(EmitTo::First(2)).expect("first two groups")),
            vec![Some(1), Some(2)]
        );
        acc.update_batch(&[int64_col(&[Some(10), Some(20)])], &[0, 1], None, 2)
            .expect("indices shifted down by two");
        assert_eq!(
            int64_groups(&acc.evaluate(EmitTo::All).expect("remaining groups")),
            vec![Some(13), Some(20)]
        );
    }

    #[test]
    fn groups_convert_to_state_widens_each_row_and_round_trips_through_merge() {
        let acc = CheckedSumGroups::<Int64Type>::new();
        let filter = BooleanArray::from(vec![Some(true), Some(true), None, Some(true)]);
        let state = acc
            .convert_to_state(
                &[int64_col(&[Some(i64::MAX), None, Some(-4), Some(9)])],
                Some(&filter),
            )
            .expect("convert filtered rows");
        assert_eq!(
            state_rows(&state),
            vec![Some(i128::from(i64::MAX)), None, None, Some(9)]
        );
        assert_eq!(
            state_rows(
                &acc.convert_to_state(&[int64_col(&[Some(-1), None])], None)
                    .expect("convert unfiltered rows")
            ),
            vec![Some(-1), None]
        );

        let mut merged = CheckedSumGroups::<Int64Type>::new();
        merged
            .merge_batch(&state, &[0, 0, 0, 1], 2)
            .expect("merge converted rows");
        assert_eq!(
            int64_groups(&merged.evaluate(EmitTo::All).expect("evaluate merged rows")),
            vec![Some(i64::MAX), Some(9)]
        );
    }

    #[test]
    fn uint64_groups_overflow_past_max_and_reject_a_negative_partial() {
        let mut acc = CheckedSumGroups::<UInt64Type>::new();
        acc.update_batch(
            &[uint64_col(&[Some(u64::MAX), Some(1), Some(5)])],
            &[0, 0, 1],
            None,
            2,
        )
        .expect("UInt64 update");
        let state = acc.state(EmitTo::All).expect("UInt64 state");
        assert_eq!(
            state_rows(&state),
            vec![Some(i128::from(u64::MAX) + 1), Some(5)]
        );

        let mut overflow = CheckedSumGroups::<UInt64Type>::new();
        overflow
            .merge_batch(&state, &[0, 1], 2)
            .expect("merge UInt64 state");
        let error = overflow
            .evaluate(EmitTo::All)
            .expect_err("MAX+1 must overflow at evaluate");
        assert_eq!(error.to_string(), uint64_overflow().to_string());

        let negative = decimal_state(vec![-1], None).expect("decimal state");
        let mut rejected = CheckedSumGroups::<UInt64Type>::new();
        let error = rejected
            .merge_batch(&[negative], &[0], 1)
            .expect_err("a negative partial is out of range for UInt64");
        assert_eq!(error.to_string(), uint64_overflow().to_string());
    }

    #[test]
    fn groups_refuse_a_batch_whose_group_indices_do_not_cover_its_rows() {
        let mut acc = CheckedSumGroups::<Int64Type>::new();
        let error = acc
            .update_batch(&[int64_col(&[Some(1), Some(2)])], &[0], None, 1)
            .expect_err("two rows with one group index");
        assert!(
            matches!(
                &error,
                datafusion::error::DataFusionError::Internal(message)
                    if message.starts_with("checked SUM got 1 group indices for 2 rows")
            ),
            "got {error}"
        );
    }
}
