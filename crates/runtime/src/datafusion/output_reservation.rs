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

//! Charges the output of scalar functions whose output size is set by an
//! argument (`repeat('a', n)`, `range(0, n)`, …) to the query memory pool.
//!
//! Operators reserve what they buffer, but a scalar function allocates its
//! result outside the pool, so one call can allocate gigabytes under any
//! `runtime.query.memory_limit`. Each wrapped function first reserves a lower
//! bound of its output size, computed from its arguments, for as long as it
//! runs, and fails with the pool's resources-exhausted error instead of
//! allocating when the pool cannot grant it.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, AsArray, Int32Array, Int64Array, LargeStringArray, OffsetSizeTrait, StringArray,
    StringViewArray,
};
use arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Date32Type, FieldRef, Int32Type, Int64Type,
    IntervalMonthDayNano, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{ExprSchema, Result, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::preimage::PreimageResult;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, StructFieldMapping,
};

/// A lower bound, in bytes, of the output of one call with these arguments
/// over `rows` rows.
type OutputBytes = fn(args: &[ColumnarValue], rows: usize) -> u128;

/// Rewrites arguments into an equivalent form before the call.
type PrepareArgs = fn(args: &mut [ColumnarValue]);

fn output_bytes_for(name: &str) -> Option<OutputBytes> {
    match name {
        "repeat" => Some(repeat_bytes),
        "lpad" | "rpad" => Some(pad_bytes),
        "array_repeat" => Some(array_repeat_bytes),
        "array_resize" => Some(array_resize_bytes),
        "range" => Some(range_bytes::<false>),
        "generate_series" => Some(range_bytes::<true>),
        _ => None,
    }
}

fn prepare_args_for(name: &str) -> Option<PrepareArgs> {
    match name {
        "lpad" | "rpad" => Some(clamp_negative_pad_lengths),
        _ => None,
    }
}

/// Re-registers every function in `state` that [`output_bytes_for`] covers so
/// its output is reserved from `pool` before it runs.
pub(crate) fn reserve_argument_sized_outputs(state: &mut SessionState, pool: &Arc<dyn MemoryPool>) {
    let wrapped: Vec<_> = state
        .scalar_functions()
        .values()
        .filter(|udf| !udf.inner().is::<ReservedOutput>())
        .filter_map(|udf| {
            output_bytes_for(udf.name()).map(|output_bytes| ReservedOutput {
                inner: Arc::clone(udf.inner()),
                pool: Arc::clone(pool),
                output_bytes,
                prepare_args: prepare_args_for(udf.name()),
            })
        })
        .collect();
    for udf in wrapped {
        // `register_udf` only fails for a function whose name is invalid,
        // which the function being replaced already passed.
        let _ = state.register_udf(Arc::new(ScalarUDF::new_from_impl(udf)));
    }
}

#[derive(Debug)]
struct ReservedOutput {
    inner: Arc<dyn ScalarUDFImpl>,
    pool: Arc<dyn MemoryPool>,
    output_bytes: OutputBytes,
    prepare_args: Option<PrepareArgs>,
}

impl PartialEq for ReservedOutput {
    fn eq(&self, other: &Self) -> bool {
        self.inner.as_ref().dyn_eq(other.inner.as_ref() as &dyn Any)
            && Arc::ptr_eq(&self.pool, &other.pool)
    }
}

impl Eq for ReservedOutput {}

impl Hash for ReservedOutput {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.dyn_hash(state);
    }
}

#[warn(clippy::missing_trait_methods)]
impl ScalarUDFImpl for ReservedOutput {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn display_name(&self, args: &[Expr]) -> Result<String> {
        #[expect(deprecated)]
        self.inner.display_name(args)
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        self.inner.schema_name(args)
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        #[expect(deprecated)]
        self.inner.is_nullable(args, schema)
    }

    fn is_strict(&self) -> bool {
        self.inner.is_strict()
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let Some(prepare_args) = self.prepare_args {
            prepare_args(&mut args.args);
        }
        let rows = if args
            .args
            .iter()
            .any(|arg| matches!(arg, ColumnarValue::Array(_)))
        {
            args.number_rows
        } else {
            1
        };
        let bytes = usize::try_from((self.output_bytes)(&args.args, rows)).unwrap_or(usize::MAX);
        let reservation = MemoryConsumer::new(self.inner.name()).register(&self.pool);
        reservation.try_grow(bytes)?;
        self.inner.invoke_with_args(args)
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        self.inner.with_updated_config(config).map(|udf| {
            ScalarUDF::new_from_impl(Self {
                inner: Arc::clone(udf.inner()),
                pool: Arc::clone(&self.pool),
                output_bytes: self.output_bytes,
                prepare_args: self.prepare_args,
            })
        })
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        self.inner.simplify(args, info)
    }

    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        self.inner.preimage(args, lit_expr, info)
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.inner.conditional_arguments(args)
    }

    fn short_circuits(&self) -> bool {
        self.inner.short_circuits()
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> Result<Interval> {
        self.inner.evaluate_bounds(input)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.inner.propagate_constraints(interval, inputs)
    }

    fn struct_field_mapping(
        &self,
        literal_args: &[Option<ScalarValue>],
    ) -> Option<StructFieldMapping> {
        self.inner.struct_field_mapping(literal_args)
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> Result<SortProperties> {
        self.inner.output_ordering(inputs)
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.inner.preserves_lex_ordering(inputs)
    }

    fn strictly_order_preserving(&self, inputs: &[ExprProperties]) -> Result<bool> {
        self.inner.strictly_order_preserving(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        self.inner.placement(args)
    }
}

/// An integer argument, read row by row; a scalar applies to every row.
enum Ints<'a> {
    Scalar(Option<i64>),
    I32(&'a Int32Array),
    I64(&'a Int64Array),
}

impl<'a> Ints<'a> {
    fn new(arg: &'a ColumnarValue) -> Option<Self> {
        match arg {
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => Some(Self::Scalar(*value)),
            ColumnarValue::Scalar(ScalarValue::Int32(value)) => {
                Some(Self::Scalar(value.map(i64::from)))
            }
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Int64 => Some(Self::I64(array.as_primitive::<Int64Type>())),
                DataType::Int32 => Some(Self::I32(array.as_primitive::<Int32Type>())),
                _ => None,
            },
            ColumnarValue::Scalar(_) => None,
        }
    }

    fn get(&self, row: usize) -> Option<i64> {
        match self {
            Self::Scalar(value) => *value,
            Self::I32(array) => array.is_valid(row).then(|| i64::from(array.value(row))),
            Self::I64(array) => array.is_valid(row).then(|| array.value(row)),
        }
    }

    /// The value at `row` as a count of output items; null or negative is 0.
    fn count(&self, row: usize) -> u128 {
        self.get(row)
            .and_then(|value| u128::try_from(value).ok())
            .unwrap_or(0)
    }
}

/// The byte length of a string argument, read row by row.
enum StrLens<'a> {
    Scalar(Option<usize>),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> StrLens<'a> {
    fn new(arg: &'a ColumnarValue) -> Option<Self> {
        match arg {
            ColumnarValue::Scalar(
                ScalarValue::Utf8(value)
                | ScalarValue::LargeUtf8(value)
                | ScalarValue::Utf8View(value),
            ) => Some(Self::Scalar(value.as_ref().map(String::len))),
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Utf8 => Some(Self::Utf8(array.as_string::<i32>())),
                DataType::LargeUtf8 => Some(Self::LargeUtf8(array.as_string::<i64>())),
                DataType::Utf8View => Some(Self::Utf8View(array.as_string_view())),
                _ => None,
            },
            ColumnarValue::Scalar(_) => None,
        }
    }

    fn get(&self, row: usize) -> u128 {
        let len = match self {
            Self::Scalar(len) => *len,
            Self::Utf8(array) => array.is_valid(row).then(|| array.value(row).len()),
            Self::LargeUtf8(array) => array.is_valid(row).then(|| array.value(row).len()),
            Self::Utf8View(array) => array.is_valid(row).then(|| array.value(row).len()),
        };
        len.map_or(0, |len| len as u128)
    }
}

fn sum_rows(rows: usize, per_row: impl Fn(usize) -> u128) -> u128 {
    (0..rows).fold(0, |total: u128, row| total.saturating_add(per_row(row)))
}

/// The bytes one value of `data_type` takes, or 1 for a variable-width type.
fn value_width(data_type: &DataType) -> u128 {
    data_type.primitive_width().map_or(1, |width| width as u128)
}

/// `repeat(string, n)`: the string's bytes `n` times.
fn repeat_bytes(args: &[ColumnarValue], rows: usize) -> u128 {
    let [string, count] = args else { return 0 };
    let (Some(string), Some(count)) = (StrLens::new(string), Ints::new(count)) else {
        return 0;
    };
    sum_rows(rows, |row| string.get(row).saturating_mul(count.count(row)))
}

/// `lpad`/`rpad(string, n[, fill])`: the functions allocate `n` bytes for each
/// row before they pad or truncate, also when an empty fill only truncates.
fn pad_bytes(args: &[ColumnarValue], rows: usize) -> u128 {
    let ([_, length] | [_, length, _]) = args else {
        return 0;
    };
    let Some(length) = Ints::new(length) else {
        return 0;
    };
    sum_rows(rows, |row| length.count(row))
}

/// `lpad`/`rpad` pad a negative length to an empty string, but size their
/// output buffer from the length cast to `usize`, so negative lengths are
/// passed as 0.
fn clamp_negative_pad_lengths(args: &mut [ColumnarValue]) {
    let Some(ColumnarValue::Array(lengths)) = args.get(1) else {
        return;
    };
    let Some(lengths) = lengths.as_primitive_opt::<Int64Type>() else {
        return;
    };
    if arrow::compute::min(lengths).is_some_and(|min| min < 0) {
        let clamped: Int64Array = lengths.unary(|length| length.max(0));
        args[1] = ColumnarValue::Array(Arc::new(clamped));
    }
}

/// `array_repeat(element, n)`: `n` copies of the element, plus the 8-byte take
/// index the function builds for each value it copies. A list element is
/// copied value by value, so it needs one index per value of the list.
fn array_repeat_bytes(args: &[ColumnarValue], rows: usize) -> u128 {
    let [element, count] = args else { return 0 };
    let Some(count) = Ints::new(count) else {
        return 0;
    };
    let copy_bytes = |element: &dyn Array, row: usize| {
        let indices = match element.data_type() {
            DataType::List(_) => list_range(element.as_list::<i32>().value_offsets(), row).len(),
            DataType::LargeList(_) => {
                list_range(element.as_list::<i64>().value_offsets(), row).len()
            }
            _ => 1,
        };
        value_bytes(element, row).saturating_add((indices as u128).saturating_mul(8))
    };
    match element {
        ColumnarValue::Array(element) => sum_rows(rows, |row| {
            count
                .count(row)
                .saturating_mul(copy_bytes(element.as_ref(), row))
        }),
        ColumnarValue::Scalar(scalar) => {
            let Ok(element) = scalar.to_array() else {
                return 0;
            };
            let copy_bytes = copy_bytes(element.as_ref(), 0);
            sum_rows(rows, |row| count.count(row).saturating_mul(copy_bytes))
        }
    }
}

/// A lower bound of the bytes the value at `row` takes in an array of its
/// type: lists, maps and structs count their nested values.
fn value_bytes(array: &dyn Array, row: usize) -> u128 {
    if let Some(width) = array.data_type().primitive_width() {
        return width as u128;
    }
    let nested = |values: &dyn Array, range: std::ops::Range<usize>| {
        if let Some(width) = values.data_type().primitive_width() {
            return (range.len() as u128).saturating_mul(width as u128);
        }
        range.fold(0, |total: u128, value| {
            total.saturating_add(value_bytes(values, value))
        })
    };
    match array.data_type() {
        DataType::Utf8 => 4 + array.as_string::<i32>().value(row).len() as u128,
        DataType::LargeUtf8 => 8 + array.as_string::<i64>().value(row).len() as u128,
        DataType::Utf8View => 16 + array.as_string_view().value(row).len() as u128,
        DataType::Binary => 4 + array.as_binary::<i32>().value(row).len() as u128,
        DataType::LargeBinary => 8 + array.as_binary::<i64>().value(row).len() as u128,
        DataType::BinaryView => 16 + array.as_binary_view().value(row).len() as u128,
        DataType::FixedSizeBinary(width) => u128::try_from(*width).unwrap_or(0),
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            4 + nested(
                list.values().as_ref(),
                list_range(list.value_offsets(), row),
            )
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            8 + nested(
                list.values().as_ref(),
                list_range(list.value_offsets(), row),
            )
        }
        DataType::Map(..) => {
            let map = array.as_map();
            4 + nested(map.entries(), list_range(map.value_offsets(), row))
        }
        DataType::FixedSizeList(..) => {
            let list = array.as_fixed_size_list();
            let start = list.value_offset(row).as_usize();
            let len = list.value_length().as_usize();
            nested(list.values().as_ref(), start..start + len)
        }
        DataType::Struct(_) => array.as_struct().columns().iter().fold(0, |total, column| {
            total.saturating_add(value_bytes(column.as_ref(), row))
        }),
        DataType::Dictionary(key, _) => value_width(key),
        _ => 0,
    }
}

fn list_range<O: OffsetSizeTrait>(offsets: &[O], row: usize) -> std::ops::Range<usize> {
    offsets[row].as_usize()..offsets[row + 1].as_usize()
}

/// `array_resize(array, n[, value])`: `n` elements per row.
fn array_resize_bytes(args: &[ColumnarValue], rows: usize) -> u128 {
    let ([array, size] | [array, size, _]) = args else {
        return 0;
    };
    let Some(size) = Ints::new(size) else {
        return 0;
    };
    let width = match array.data_type() {
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            value_width(field.data_type())
        }
        _ => 1,
    };
    sum_rows(rows, |row| size.count(row).saturating_mul(width))
}

const NANOS_PER_DAY: i128 = 86_400_000_000_000;

/// `range`/`generate_series(start, stop[, step])` over integers, dates or
/// timestamps: one value per step.
fn range_bytes<const INCLUSIVE: bool>(args: &[ColumnarValue], rows: usize) -> u128 {
    match args {
        [stop] => Ints::new(stop).map_or(0, |stop| {
            sum_rows(rows, |row| {
                stop.get(row)
                    .map_or(0, |stop| int_steps::<INCLUSIVE>(0, stop, 1))
                    .saturating_mul(8)
            })
        }),
        [start, stop] => integer_range_bytes::<INCLUSIVE>(start, stop, None, rows),
        [start, stop, stride] => match stride {
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(stride)) => {
                temporal_range_bytes::<INCLUSIVE>(start, stop, *stride, rows)
            }
            _ => integer_range_bytes::<INCLUSIVE>(start, stop, Some(stride), rows),
        },
        _ => 0,
    }
}

fn integer_range_bytes<const INCLUSIVE: bool>(
    start: &ColumnarValue,
    stop: &ColumnarValue,
    stride: Option<&ColumnarValue>,
    rows: usize,
) -> u128 {
    let (Some(start), Some(stop)) = (Ints::new(start), Ints::new(stop)) else {
        return 0;
    };
    let stride = match stride {
        Some(stride) => match Ints::new(stride) {
            Some(stride) => Some(stride),
            None => return 0,
        },
        None => None,
    };
    sum_rows(rows, |row| {
        let (Some(start), Some(stop)) = (start.get(row), stop.get(row)) else {
            return 0;
        };
        let stride = match &stride {
            Some(stride) => match stride.get(row) {
                Some(stride) => stride,
                None => return 0,
            },
            None => 1,
        };
        int_steps::<INCLUSIVE>(start, stop, stride).saturating_mul(8)
    })
}

/// The number of values `range` (exclusive) or `generate_series` (inclusive)
/// yields from `start` to `stop` by `step`.
fn int_steps<const INCLUSIVE: bool>(start: i64, stop: i64, stride: i64) -> u128 {
    let distance = i128::from(stop) - i128::from(start);
    let stride = i128::from(stride);
    if stride == 0 || (distance != 0 && distance.signum() != stride.signum()) {
        return 0;
    }
    let (distance, stride) = (distance.unsigned_abs(), stride.unsigned_abs());
    if INCLUSIVE {
        distance / stride + 1
    } else {
        distance.div_ceil(stride)
    }
}

/// Date and timestamp ranges, with the step taken at its longest (31-day
/// months, 25-hour days) so the count is never overstated.
fn temporal_range_bytes<const INCLUSIVE: bool>(
    start: &ColumnarValue,
    stop: &ColumnarValue,
    stride: Option<IntervalMonthDayNano>,
    rows: usize,
) -> u128 {
    let Some(stride) = stride else { return 0 };
    let components = [
        i128::from(stride.months) * 31 * NANOS_PER_DAY,
        i128::from(stride.days) * 25 * NANOS_PER_DAY / 24,
        i128::from(stride.nanoseconds),
    ];
    let stride_nanos = if components.iter().all(|c| *c >= 0) || components.iter().all(|c| *c <= 0) {
        components.iter().sum::<i128>()
    } else {
        return 0;
    };
    if stride_nanos == 0 {
        return 0;
    }
    let (Some(start), Some(stop)) = (Temporal::new(start), Temporal::new(stop)) else {
        return 0;
    };
    let width = value_width(&start.data_type);
    sum_rows(rows, |row| {
        let (Some(start), Some(stop)) = (start.nanos(row), stop.nanos(row)) else {
            return 0;
        };
        let distance = stop - start;
        if distance != 0 && distance.signum() != stride_nanos.signum() {
            return 0;
        }
        let steps = distance.unsigned_abs() / stride_nanos.unsigned_abs();
        let steps = if INCLUSIVE { steps + 1 } else { steps };
        steps.saturating_mul(width)
    })
}

/// A date or timestamp argument as nanoseconds since the epoch, row by row.
struct Temporal<'a> {
    data_type: DataType,
    nanos_per_unit: i128,
    values: Box<dyn Fn(usize) -> Option<i64> + 'a>,
}

impl<'a> Temporal<'a> {
    fn new(arg: &'a ColumnarValue) -> Option<Self> {
        let data_type = arg.data_type();
        let nanos_per_unit = match &data_type {
            DataType::Date32 => NANOS_PER_DAY,
            DataType::Timestamp(TimeUnit::Second, _) => 1_000_000_000,
            DataType::Timestamp(TimeUnit::Millisecond, _) => 1_000_000,
            DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000,
            DataType::Timestamp(TimeUnit::Nanosecond, _) => 1,
            _ => return None,
        };
        let values: Box<dyn Fn(usize) -> Option<i64> + 'a> = match arg {
            ColumnarValue::Scalar(scalar) => {
                let value = match scalar {
                    ScalarValue::Date32(value) => value.map(i64::from),
                    ScalarValue::TimestampSecond(value, _)
                    | ScalarValue::TimestampMillisecond(value, _)
                    | ScalarValue::TimestampMicrosecond(value, _)
                    | ScalarValue::TimestampNanosecond(value, _) => *value,
                    _ => return None,
                };
                Box::new(move |_| value)
            }
            ColumnarValue::Array(array) => match &data_type {
                DataType::Date32 => rows_of::<Date32Type>(array.as_ref()),
                DataType::Timestamp(TimeUnit::Second, _) => {
                    rows_of::<TimestampSecondType>(array.as_ref())
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    rows_of::<TimestampMillisecondType>(array.as_ref())
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    rows_of::<TimestampMicrosecondType>(array.as_ref())
                }
                _ => rows_of::<TimestampNanosecondType>(array.as_ref()),
            },
        };
        Some(Self {
            data_type,
            nanos_per_unit,
            values,
        })
    }

    fn nanos(&self, row: usize) -> Option<i128> {
        (self.values)(row).map(|value| i128::from(value) * self.nanos_per_unit)
    }
}

fn rows_of<'a, T>(array: &'a dyn Array) -> Box<dyn Fn(usize) -> Option<i64> + 'a>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i64>,
{
    let array = array.as_primitive::<T>();
    Box::new(move |row| array.is_valid(row).then(|| array.value(row).into()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::dataaccelerator::AcceleratorEngineRegistry;
    use arrow::array::{ArrayRef, AsArray, Int64Array, ListArray, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Int64Type};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::common::ScalarValue;
    use datafusion::error::DataFusionError;
    use datafusion::logical_expr::ColumnarValue;

    use super::{array_repeat_bytes, clamp_negative_pad_lengths, pad_bytes};

    use crate::datafusion::builder::DataFusionBuilder;
    use crate::status;

    const MEMORY_LIMIT: u64 = 16 * 1024 * 1024;

    fn session() -> crate::datafusion::DataFusion {
        DataFusionBuilder::new(
            status::RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::default()),
            tokio::runtime::Handle::current(),
        )
        .memory_limit(Some(MEMORY_LIMIT))
        .build()
    }

    /// Each query asks one function for about 64 MiB, four times the query
    /// memory limit. `v - v` keeps the arguments column-dependent so the call
    /// runs during execution; the last query has only literal arguments.
    #[tokio::test]
    async fn argument_sized_function_output_is_held_to_the_query_memory_limit() {
        let df = session();
        let one_row = "FROM (SELECT value AS v FROM generate_series(1, 1)) t";
        let cases = [
            (
                "repeat",
                format!("SELECT length(repeat('a', 67108864 + v - v)) {one_row}"),
            ),
            (
                "lpad",
                format!("SELECT length(lpad('a', 67108864 + v - v)) {one_row}"),
            ),
            (
                "rpad",
                format!("SELECT length(rpad('a', 67108864 + v - v, 'b')) {one_row}"),
            ),
            (
                "lpad",
                format!("SELECT length(lpad('a', 67108864 + v - v, '')) {one_row}"),
            ),
            (
                "array_repeat",
                format!("SELECT cardinality(array_repeat(v, 8388608 + v - v)) {one_row}"),
            ),
            (
                "array_repeat",
                format!(
                    "SELECT cardinality(array_repeat(range(v - v, 1024), 4096 + v - v)) {one_row}"
                ),
            ),
            (
                "array_repeat",
                format!(
                    "SELECT cardinality(array_repeat(CAST(repeat('a', 1024 + v - v) AS BYTEA), \
                     65536 + v - v)) {one_row}"
                ),
            ),
            (
                "array_resize",
                format!("SELECT cardinality(array_resize([v], 8388608 + v - v, v)) {one_row}"),
            ),
            (
                "range",
                format!("SELECT cardinality(range(v - v, 8388608)) {one_row}"),
            ),
            (
                "generate_series",
                format!("SELECT cardinality(generate_series(v, 8388608)) {one_row}"),
            ),
            (
                "generate_series",
                format!(
                    "SELECT cardinality(generate_series(to_timestamp(1704067200 + v - v), \
                     TIMESTAMP '2024-01-01T02:30:00', INTERVAL '1 millisecond')) {one_row}"
                ),
            ),
            ("repeat", "SELECT length(repeat('a', 67108864))".to_string()),
        ];
        for (function, sql) in cases {
            let error = df
                .ctx
                .sql(&sql)
                .await
                .expect("plan the query")
                .collect()
                .await
                .expect_err(&format!("{sql} must exceed the query memory limit"));
            let message = error.to_string();
            assert!(
                matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
                "{sql}: expected resources exhausted, got {message}"
            );
            assert!(
                message.contains(&format!("for {function}")),
                "{sql}: the error must name {function}, got {message}"
            );
        }
    }

    async fn pad_lengths(
        df: &crate::datafusion::DataFusion,
        sql: &str,
    ) -> Result<Vec<i32>, DataFusionError> {
        let batches = df.ctx.sql(sql).await?.collect().await?;
        Ok(batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int32Type>()
                    .iter()
                    .map(|length| length.expect("a length"))
                    .collect::<Vec<_>>()
            })
            .collect())
    }

    /// An empty fill only truncates, but the functions still allocate the
    /// requested length, so it is charged like any other fill.
    #[tokio::test]
    async fn pad_with_an_empty_fill_is_charged_its_requested_length() {
        let df = session();
        let mib = 1024 * 1024;
        let one_row = "FROM (SELECT value AS v FROM generate_series(1, 1)) t";
        for function in ["lpad", "rpad"] {
            let under = format!(
                "SELECT length({function}('abc', {} + v - v, '')) {one_row}",
                MEMORY_LIMIT - mib
            );
            assert_eq!(
                pad_lengths(&df, &under).await.expect("run under the limit"),
                vec![3]
            );
            let over = format!(
                "SELECT length({function}('abc', {} + v - v, '')) {one_row}",
                MEMORY_LIMIT + mib
            );
            let error = pad_lengths(&df, &over)
                .await
                .expect_err("an empty fill over the limit must fail");
            assert!(
                matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)),
                "{over}: expected resources exhausted, got {error}"
            );
        }
    }

    #[tokio::test]
    async fn pad_with_negative_lengths_returns_empty_strings() {
        let df = session();
        let lengths = pad_lengths(
            &df,
            "SELECT length(lpad('abc', v - 3)) + length(rpad('abc', v - 3, 'xy')) \
             FROM (SELECT value AS v FROM generate_series(1, 5)) t ORDER BY v",
        )
        .await
        .expect("run the query");
        assert_eq!(lengths, vec![0, 0, 0, 2, 4]);
    }

    #[test]
    fn pad_sizing_counts_non_negative_lengths_for_every_fill() {
        let lengths = ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            Some(4),
            Some(-7),
            None,
            Some(2),
        ])));
        let string = ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string())));
        let empty = ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::new())));
        let fill = ColumnarValue::Scalar(ScalarValue::Utf8(Some("xy".to_string())));
        assert_eq!(pad_bytes(&[string.clone(), lengths.clone()], 4), 6);
        assert_eq!(pad_bytes(&[string.clone(), lengths.clone(), empty], 4), 6);
        assert_eq!(pad_bytes(&[string.clone(), lengths.clone(), fill], 4), 6);

        let mut args = [string, lengths];
        clamp_negative_pad_lengths(&mut args);
        let ColumnarValue::Array(clamped) = &args[1] else {
            panic!("lengths stay an array");
        };
        assert_eq!(
            clamped.as_primitive::<Int64Type>(),
            &Int64Array::from(vec![Some(4), Some(0), None, Some(2)])
        );
    }

    #[test]
    fn array_repeat_sizing_counts_element_values_and_take_indices() {
        let count = ColumnarValue::Scalar(ScalarValue::Int64(Some(10)));
        let list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
        ])]);
        let strukt = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["xy"])) as ArrayRef,
            ),
        ]);
        let cases = [
            (ScalarValue::Int64(Some(1)), 10 * (8 + 8)),
            (ScalarValue::Utf8(Some("abc".to_string())), 10 * (4 + 3 + 8)),
            (ScalarValue::Boolean(Some(true)), 10 * 8),
            (ScalarValue::Null, 10 * 8),
            (ScalarValue::Binary(Some(vec![0; 5])), 10 * (4 + 5 + 8)),
            (ScalarValue::List(Arc::new(list)), 10 * (4 + 3 * 8 + 3 * 8)),
            (ScalarValue::Struct(Arc::new(strukt)), 10 * (8 + 4 + 2 + 8)),
        ];
        let (expected, charged): (Vec<_>, Vec<_>) = cases
            .into_iter()
            .map(|(element, bytes)| {
                let data_type = element.data_type();
                let charged =
                    array_repeat_bytes(&[ColumnarValue::Scalar(element), count.clone()], 1);
                ((data_type.clone(), bytes), (data_type, charged))
            })
            .unzip();
        assert_eq!(charged, expected);

        let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(3)]),
        ]);
        let counts = Int64Array::from(vec![Some(2), Some(5), Some(3)]);
        assert_eq!(
            array_repeat_bytes(
                &[
                    ColumnarValue::Array(Arc::new(lists)),
                    ColumnarValue::Array(Arc::new(counts)),
                ],
                3
            ),
            2 * (4 + 2 * 8 + 2 * 8) + 5 * 4 + 3 * (4 + 8 + 8)
        );
    }

    #[tokio::test]
    async fn argument_sized_functions_within_the_limit_return_their_results() {
        let df = session();
        let batches = df
            .ctx
            .sql(
                "SELECT repeat('ab', 3 + v - v) AS r, lpad('x', 4 + v - v, 'yz') AS l, \
                 rpad('x', 4 + v - v) AS p, array_repeat(v, 2) AS a, array_repeat([v, v], 2) AS b, \
                 array_resize([v], 3, 0) AS z, \
                 range(v - v, 3) AS g, generate_series(v, 3) AS s \
                 FROM (SELECT value AS v FROM generate_series(1, 2)) t ORDER BY v",
            )
            .await
            .expect("plan the query")
            .collect()
            .await
            .expect("run the query");
        insta::assert_snapshot!(
            "argument_sized_functions_within_the_limit",
            pretty_format_batches(&batches).expect("format the results")
        );
    }
}
