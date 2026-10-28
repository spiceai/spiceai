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
    Array, AsArray, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, FieldRef, Int32Type, Int64Type, IntervalMonthDayNano,
    TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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

/// `lpad`/`rpad(string, n[, fill])`: `n` characters, at least `n` bytes. With
/// an empty fill the string is only truncated, so nothing is counted.
fn pad_bytes(args: &[ColumnarValue], rows: usize) -> u128 {
    let (length, fill) = match args {
        [_, length] => (length, None),
        [_, length, fill] => (length, StrLens::new(fill)),
        _ => return 0,
    };
    let Some(length) = Ints::new(length) else {
        return 0;
    };
    sum_rows(rows, |row| match &fill {
        Some(fill) if fill.get(row) == 0 => 0,
        _ => length.count(row),
    })
}

/// `array_repeat(element, n)`: `n` copies of the element.
fn array_repeat_bytes(args: &[ColumnarValue], rows: usize) -> u128 {
    let [element, count] = args else { return 0 };
    let Some(count) = Ints::new(count) else {
        return 0;
    };
    let strings = StrLens::new(element);
    let width = value_width(&element.data_type());
    sum_rows(rows, |row| {
        let element_bytes = strings
            .as_ref()
            .map_or(width, |strings| strings.get(row).max(1));
        count.count(row).saturating_mul(element_bytes)
    })
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
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::error::DataFusionError;

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
                "array_repeat",
                format!("SELECT cardinality(array_repeat(v, 8388608 + v - v)) {one_row}"),
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

    #[tokio::test]
    async fn argument_sized_functions_within_the_limit_return_their_results() {
        let df = session();
        let batches = df
            .ctx
            .sql(
                "SELECT repeat('ab', 3 + v - v) AS r, lpad('x', 4 + v - v, 'yz') AS l, \
                 rpad('x', 4 + v - v) AS p, array_repeat(v, 2) AS a, array_resize([v], 3, 0) AS z, \
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
