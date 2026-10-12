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

//! Time-series aggregates for OTLP metrics (#15060).
//!
//! - `histogram_quantile(q, explicit_bounds, bucket_counts)` adds bucket counts element by
//!   element across rows, then interpolates within the target bucket like Prometheus.
//! - `increase(value, time)` is the growth of a cumulative counter across its samples,
//!   treating any decrease as a counter reset.
//! - `rate(value, time)` is `increase` divided by the seconds between the first and last
//!   sample, with `time` in Unix nanoseconds or a timestamp.
//!
//! Unlike `PromQL`, `increase` and `rate` do not extrapolate to the edges of a time window:
//! they use only the samples in the group.

use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array, ListArray};
use arrow::compute::{CastOptions, cast, cast_with_options};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type, TimeUnit, UInt64Type};
use datafusion::common::{Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};

pub const HISTOGRAM_QUANTILE_UDAF_NAME: &str = "histogram_quantile";
pub const INCREASE_UDAF_NAME: &str = "increase";
pub const RATE_UDAF_NAME: &str = "rate";

runtime_udfs_api::register_spice_function!(
    HISTOGRAM_QUANTILE_SPICE_FUNCTION,
    HISTOGRAM_QUANTILE_UDAF_NAME
);
runtime_udfs_api::register_spice_function!(INCREASE_SPICE_FUNCTION, INCREASE_UDAF_NAME);
runtime_udfs_api::register_spice_function!(RATE_SPICE_FUNCTION, RATE_UDAF_NAME);

fn f64_list_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Float64, true)))
}

fn u64_list_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::UInt64, true)))
}

fn is_integer_list(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            f.data_type().is_integer() || f.data_type().is_null()
        }
        DataType::Null => true,
        _ => false,
    }
}

/// Each row of a list column as exact `u64` counts; a null list is `None`. A negative count
/// is an error rather than a silent null.
fn u64_lists(array: &ArrayRef) -> Result<Vec<Option<Vec<u64>>>> {
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let list = cast_with_options(array, &u64_list_type(), &options)?;
    let list = list.as_list::<i32>();
    Ok((0..list.len())
        .map(|i| {
            list.is_valid(i).then(|| {
                let values = list.value(i);
                values
                    .as_primitive::<UInt64Type>()
                    .iter()
                    .map(|v| v.unwrap_or(0))
                    .collect()
            })
        })
        .collect())
}

fn is_numeric_list(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            f.data_type().is_numeric() || f.data_type().is_null()
        }
        DataType::Null => true,
        _ => false,
    }
}

/// Each row of a list column as `f64` values; a null list is `None`.
fn f64_lists(array: &ArrayRef) -> Result<Vec<Option<Vec<f64>>>> {
    let list = cast(array, &f64_list_type())?;
    let list = list.as_list::<i32>();
    Ok((0..list.len())
        .map(|i| {
            list.is_valid(i).then(|| {
                let values = list.value(i);
                values
                    .as_primitive::<Float64Type>()
                    .iter()
                    .map(|v| v.unwrap_or(0.0))
                    .collect()
            })
        })
        .collect())
}

fn f64_values(array: &ArrayRef) -> Result<Float64Array> {
    Ok(cast(array, &DataType::Float64)?
        .as_primitive::<Float64Type>()
        .clone())
}

/// Times as Unix nanoseconds. Timestamps of any unit are converted to nanoseconds; integers
/// are taken to be nanoseconds already.
fn nanos_values(array: &ArrayRef) -> Result<Int64Array> {
    let nanos = match array.data_type() {
        DataType::Timestamp(_, tz) => cast(
            &cast(
                array,
                &DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
            )?,
            &DataType::Int64,
        )?,
        _ => cast(array, &DataType::Int64)?,
    };
    Ok(nanos.as_primitive::<Int64Type>().clone())
}

/// Prometheus `histogram_quantile` over cumulative bucket counts whose upper bounds are
/// `bounds` followed by +Inf.
#[must_use]
pub fn bucket_quantile(q: f64, bounds: &[f64], counts: &[u64]) -> f64 {
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    // OTLP has one more count than bounds: the last bucket is (last bound, +Inf].
    if bounds.is_empty() || counts.len() != bounds.len() + 1 {
        return f64::NAN;
    }
    // Cumulative counts are kept exact in `u128` so the bucket choice cannot be skewed by
    // float rounding of large `u64` counts.
    let mut cumulative: Vec<u128> = Vec::with_capacity(counts.len());
    let mut total: u128 = 0;
    for &count in counts {
        total += u128::from(count);
        cumulative.push(total);
    }
    if total == 0 {
        return f64::NAN;
    }
    let rank = q * count_as_f64(total);
    // Counts are integers, so `c >= rank` is `c >= ceil(rank)`. At q = 1 the rank is exactly
    // the total, which f64 may not represent.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "rank is in [0, total], and the result is clamped to total"
    )]
    let rank_int = if q >= 1.0 {
        total
    } else {
        (rank.ceil() as u128).min(total)
    };
    let bucket = cumulative
        .iter()
        .position(|&c| c >= rank_int)
        .unwrap_or(cumulative.len() - 1);
    if bucket == bounds.len() {
        // The +Inf bucket: return the highest finite bound.
        return bounds[bounds.len() - 1];
    }
    if bucket == 0 && bounds[0] <= 0.0 {
        return bounds[0];
    }
    let (start, below) = if bucket == 0 {
        (0.0, 0)
    } else {
        (bounds[bucket - 1], cumulative[bucket - 1])
    };
    let end = bounds[bucket];
    let in_bucket = cumulative[bucket] - below;
    if in_bucket == 0 {
        return end;
    }
    // At q = 1 the rank is the exact top of the bucket; f64 may not hold `total - below`.
    let offset = if q >= 1.0 {
        count_as_f64(in_bucket)
    } else {
        rank - count_as_f64(below)
    };
    start + (end - start) * (offset / count_as_f64(in_bucket)).clamp(0.0, 1.0)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "interpolation is done in f64; only the bucket choice must be exact"
)]
fn count_as_f64(count: u128) -> f64 {
    count as f64
}

/// `histogram_quantile(q, explicit_bounds, bucket_counts)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HistogramQuantile {
    signature: Signature,
}

impl Default for HistogramQuantile {
    fn default() -> Self {
        Self::new()
    }
}

impl HistogramQuantile {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for HistogramQuantile {
    fn name(&self) -> &str {
        HISTOGRAM_QUANTILE_UDAF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [q, bounds, counts] = arg_types else {
            return plan_err!(
                "{HISTOGRAM_QUANTILE_UDAF_NAME} takes 3 arguments: (quantile, explicit_bounds, bucket_counts)"
            );
        };
        if !(q.is_numeric() || q.is_null()) {
            return plan_err!("{HISTOGRAM_QUANTILE_UDAF_NAME}: quantile must be numeric, got {q}");
        }
        if !is_numeric_list(bounds) {
            return plan_err!(
                "{HISTOGRAM_QUANTILE_UDAF_NAME}: explicit_bounds must be a list of numbers, got {bounds}"
            );
        }
        // Counts are summed exactly as integers; OTLP bucket counts are `uint64`.
        if !is_integer_list(counts) {
            return plan_err!(
                "{HISTOGRAM_QUANTILE_UDAF_NAME}: bucket_counts must be a list of integers, got {counts}"
            );
        }
        Ok(vec![DataType::Float64, f64_list_type(), u64_list_type()])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::<HistogramQuantileAccumulator>::default())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(format_state_name(args.name, "q"), DataType::Float64, true).into(),
            Field::new(
                format_state_name(args.name, "bounds"),
                f64_list_type(),
                true,
            )
            .into(),
            Field::new(
                format_state_name(args.name, "counts"),
                u64_list_type(),
                true,
            )
            .into(),
        ])
    }
}

#[derive(Debug, Default)]
struct HistogramQuantileAccumulator {
    q: Option<f64>,
    bounds: Option<Vec<f64>>,
    counts: Vec<u64>,
}

impl HistogramQuantileAccumulator {
    fn add(
        &mut self,
        q: Option<f64>,
        bounds: Option<Vec<f64>>,
        counts: Option<Vec<u64>>,
    ) -> Result<()> {
        if self.q.is_none() {
            self.q = q;
        }
        let (Some(bounds), Some(counts)) = (bounds, counts) else {
            return Ok(());
        };
        match &self.bounds {
            None => {
                self.bounds = Some(bounds);
                self.counts = counts;
            }
            Some(existing) => {
                if *existing != bounds || self.counts.len() != counts.len() {
                    return exec_err!(
                        "{HISTOGRAM_QUANTILE_UDAF_NAME}: every row in a group must have the same explicit_bounds; group by them, or filter to one bucket layout"
                    );
                }
                for (total, count) in self.counts.iter_mut().zip(counts) {
                    let Some(sum) = total.checked_add(count) else {
                        return exec_err!(
                            "{HISTOGRAM_QUANTILE_UDAF_NAME}: a summed bucket count overflows UInt64"
                        );
                    };
                    *total = sum;
                }
            }
        }
        Ok(())
    }

    fn add_arrays(&mut self, q: &ArrayRef, bounds: &ArrayRef, counts: &ArrayRef) -> Result<()> {
        let q = f64_values(q)?;
        let bounds = f64_lists(bounds)?;
        let counts = u64_lists(counts)?;
        for ((q, bounds), counts) in q.iter().zip(bounds).zip(counts) {
            self.add(q, bounds, counts)?;
        }
        Ok(())
    }
}

fn list_scalar(values: Option<&[f64]>) -> ScalarValue {
    match values {
        Some(values) => {
            let array: ArrayRef =
                Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([Some(
                    values.iter().map(|v| Some(*v)).collect::<Vec<_>>(),
                )]));
            ScalarValue::List(Arc::new(array.as_list::<i32>().clone()))
        }
        None => ScalarValue::try_from(&f64_list_type()).unwrap_or(ScalarValue::Null),
    }
}

fn u64_list_scalar(values: Option<&[u64]>) -> ScalarValue {
    match values {
        Some(values) => {
            let array: ArrayRef =
                Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>([Some(
                    values.iter().map(|v| Some(*v)).collect::<Vec<_>>(),
                )]));
            ScalarValue::List(Arc::new(array.as_list::<i32>().clone()))
        }
        None => ScalarValue::try_from(&u64_list_type()).unwrap_or(ScalarValue::Null),
    }
}

impl Accumulator for HistogramQuantileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [q, bounds, counts] = values else {
            return exec_err!("{HISTOGRAM_QUANTILE_UDAF_NAME} expects 3 arguments");
        };
        self.add_arrays(q, bounds, counts)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let has_buckets = self.bounds.is_some();
        Ok(vec![
            ScalarValue::Float64(self.q),
            list_scalar(self.bounds.as_deref()),
            u64_list_scalar(has_buckets.then_some(self.counts.as_slice())),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let (Some(q), Some(bounds)) = (self.q, self.bounds.as_ref()) else {
            return Ok(ScalarValue::Float64(None));
        };
        Ok(ScalarValue::Float64(Some(bucket_quantile(
            q,
            bounds,
            &self.counts,
        ))))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.bounds.as_ref().map_or(0, |b| b.capacity() * 8)
            + self.counts.capacity() * 8
    }
}

/// Counter growth over `(time, value)` samples, sorted here. A decrease is a counter reset:
/// the counter restarted from zero, so the new value is all growth. `None` with fewer than two
/// samples.
#[must_use]
pub fn counter_increase(samples: &mut [(i64, f64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    samples.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.total_cmp(&b.1)));
    let mut increase = 0.0;
    for pair in samples.windows(2) {
        let (prev, next) = (pair[0].1, pair[1].1);
        increase += if next >= prev { next - prev } else { next };
    }
    Some(increase)
}

/// [`counter_increase`] for integer counters, summed exactly in `i128` so increments past
/// 2^53 are not lost to `f64` rounding. Every `i64` or `u64` delta fits in `i128`, and the sum
/// saturates only after more samples than memory can hold.
#[must_use]
pub fn integer_counter_increase<T: Copy + Ord + Into<i128>>(
    samples: &mut [(i64, T)],
) -> Option<i128> {
    if samples.len() < 2 {
        return None;
    }
    samples.sort_unstable();
    let mut increase: i128 = 0;
    for pair in samples.windows(2) {
        let (prev, next): (i128, i128) = (pair[0].1.into(), pair[1].1.into());
        increase = increase.saturating_add(if next >= prev { next - prev } else { next });
    }
    Some(increase)
}

/// How `increase`/`rate` hold counter values: integer counters keep exact integer arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CounterKind {
    Float,
    Signed,
    Unsigned,
}

impl CounterKind {
    fn of(value: &DataType) -> Self {
        if value.is_signed_integer() {
            Self::Signed
        } else if value.is_unsigned_integer() {
            Self::Unsigned
        } else {
            Self::Float
        }
    }

    fn value_type(self) -> DataType {
        match self {
            Self::Float => DataType::Float64,
            Self::Signed => DataType::Int64,
            Self::Unsigned => DataType::UInt64,
        }
    }

    fn list_type(self) -> DataType {
        DataType::List(Arc::new(Field::new_list_field(self.value_type(), true)))
    }
}

/// `increase` (`per_second = false`) or `rate` (`per_second = true`).
///
/// Integer counters are coerced to `Int64` or `UInt64` (not `Float64`) and their deltas are
/// summed exactly, so `increase` returns the same integer type and `rate` converts only the
/// final increase to `Float64`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CounterGrowth {
    signature: Signature,
    per_second: bool,
}

impl CounterGrowth {
    #[must_use]
    pub fn increase() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            per_second: false,
        }
    }

    #[must_use]
    pub fn rate() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            per_second: true,
        }
    }
}

fn i64_list_type() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)))
}

impl AggregateUDFImpl for CounterGrowth {
    fn name(&self) -> &str {
        if self.per_second {
            RATE_UDAF_NAME
        } else {
            INCREASE_UDAF_NAME
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let name = self.name();
        let [value, time] = arg_types else {
            return plan_err!("{name} takes 2 arguments: (value, time)");
        };
        if !(value.is_numeric() || value.is_null()) {
            return plan_err!("{name}: value must be numeric, got {value}");
        }
        let time = match time {
            DataType::Timestamp(_, _) => time.clone(),
            t if t.is_integer() || t.is_null() => DataType::Int64,
            t => {
                return plan_err!(
                    "{name}: time must be a timestamp or an integer of Unix nanoseconds, got {t}"
                );
            }
        };
        Ok(vec![CounterKind::of(value).value_type(), time])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if self.per_second {
            return Ok(DataType::Float64);
        }
        Ok(arg_types.first().map_or(DataType::Float64, |value| {
            CounterKind::of(value).value_type()
        }))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let kind = acc_args
            .expr_fields
            .first()
            .map_or(CounterKind::Float, |f| CounterKind::of(f.data_type()));
        Ok(Box::new(CounterGrowthAccumulator {
            per_second: self.per_second,
            samples: CounterSamples::new(kind),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let kind = args
            .input_fields
            .first()
            .map_or(CounterKind::Float, |f| CounterKind::of(f.data_type()));
        Ok(vec![
            Field::new(
                format_state_name(args.name, "values"),
                kind.list_type(),
                true,
            )
            .into(),
            Field::new(format_state_name(args.name, "times"), i64_list_type(), true).into(),
        ])
    }
}

/// `(time, value)` samples in the counter's own type.
#[derive(Debug)]
enum CounterSamples {
    Float(Vec<(i64, f64)>),
    Signed(Vec<(i64, i64)>),
    Unsigned(Vec<(i64, u64)>),
}

impl CounterSamples {
    fn new(kind: CounterKind) -> Self {
        match kind {
            CounterKind::Float => Self::Float(Vec::new()),
            CounterKind::Signed => Self::Signed(Vec::new()),
            CounterKind::Unsigned => Self::Unsigned(Vec::new()),
        }
    }

    fn kind(&self) -> CounterKind {
        match self {
            Self::Float(_) => CounterKind::Float,
            Self::Signed(_) => CounterKind::Signed,
            Self::Unsigned(_) => CounterKind::Unsigned,
        }
    }

    fn capacity(&self) -> usize {
        match self {
            Self::Float(s) => s.capacity(),
            Self::Signed(s) => s.capacity(),
            Self::Unsigned(s) => s.capacity(),
        }
    }

    fn times(&self) -> Vec<Option<i64>> {
        match self {
            Self::Float(s) => s.iter().map(|s| Some(s.0)).collect(),
            Self::Signed(s) => s.iter().map(|s| Some(s.0)).collect(),
            Self::Unsigned(s) => s.iter().map(|s| Some(s.0)).collect(),
        }
    }

    /// Times of the earliest and latest sample; only valid once sorted by `evaluate`.
    fn time_span(&self) -> Option<(i64, i64)> {
        fn span<T>(s: &[(i64, T)]) -> Option<(i64, i64)> {
            Some((s.first()?.0, s.last()?.0))
        }
        match self {
            Self::Float(s) => span(s),
            Self::Signed(s) => span(s),
            Self::Unsigned(s) => span(s),
        }
    }

    /// Appends each `(value, time)` pair where both are non-null; `values` is already cast to
    /// this kind's value type. A `NaN` float sample is skipped.
    fn push_array(&mut self, values: &dyn Array, times: &[Option<i64>]) {
        match self {
            Self::Float(s) => {
                for (v, t) in values.as_primitive::<Float64Type>().iter().zip(times) {
                    if let (Some(v), Some(t)) = (v, *t)
                        && !v.is_nan()
                    {
                        s.push((t, v));
                    }
                }
            }
            Self::Signed(s) => {
                for (v, t) in values.as_primitive::<Int64Type>().iter().zip(times) {
                    if let (Some(v), Some(t)) = (v, *t) {
                        s.push((t, v));
                    }
                }
            }
            Self::Unsigned(s) => {
                for (v, t) in values.as_primitive::<UInt64Type>().iter().zip(times) {
                    if let (Some(v), Some(t)) = (v, *t) {
                        s.push((t, v));
                    }
                }
            }
        }
    }

    fn values_scalar(&self) -> ScalarValue {
        let array: ArrayRef = match self {
            Self::Float(s) => {
                Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>([Some(
                    s.iter().map(|s| Some(s.1)).collect::<Vec<_>>(),
                )]))
            }
            Self::Signed(s) => Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
                s.iter().map(|s| Some(s.1)).collect::<Vec<_>>(),
            )])),
            Self::Unsigned(s) => {
                Arc::new(ListArray::from_iter_primitive::<UInt64Type, _, _>([Some(
                    s.iter().map(|s| Some(s.1)).collect::<Vec<_>>(),
                )]))
            }
        };
        ScalarValue::List(Arc::new(array.as_list::<i32>().clone()))
    }
}

#[derive(Debug)]
struct CounterGrowthAccumulator {
    per_second: bool,
    samples: CounterSamples,
}

impl Accumulator for CounterGrowthAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [value, time] = values else {
            return exec_err!("increase/rate expect 2 arguments");
        };
        let value = cast(value, &self.samples.kind().value_type())?;
        let time: Vec<Option<i64>> = nanos_values(time)?.iter().collect();
        self.samples.push_array(value.as_ref(), &time);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [values, times] = states else {
            return exec_err!("increase/rate expect 2 state fields");
        };
        let values = cast(values, &self.samples.kind().list_type())?;
        let values = values.as_list::<i32>();
        let times = cast(times, &i64_list_type())?;
        let times = times.as_list::<i32>();
        for i in 0..values.len() {
            if values.is_null(i) || times.is_null(i) {
                continue;
            }
            let row_times = times.value(i);
            let row_times: Vec<Option<i64>> =
                row_times.as_primitive::<Int64Type>().iter().collect();
            self.samples
                .push_array(values.value(i).as_ref(), &row_times);
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let times: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
            self.samples.times(),
        )]));
        Ok(vec![
            self.samples.values_scalar(),
            ScalarValue::List(Arc::new(times.as_list::<i32>().clone())),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let name = if self.per_second {
            RATE_UDAF_NAME
        } else {
            INCREASE_UDAF_NAME
        };
        // `increase` in the counter's own type; `rate` only needs it as `f64`.
        let (increase, increase_f64) = match &mut self.samples {
            CounterSamples::Float(s) => {
                let increase = counter_increase(s);
                (ScalarValue::Float64(increase), increase)
            }
            CounterSamples::Signed(s) => {
                let increase = integer_counter_increase(s);
                let exact = match increase.map(i64::try_from).transpose() {
                    Ok(exact) => exact,
                    Err(_) => return exec_err!("{name}: the counter increase overflows Int64"),
                };
                (ScalarValue::Int64(exact), increase.map(i128_to_f64))
            }
            CounterSamples::Unsigned(s) => {
                let increase = integer_counter_increase(s);
                let exact = match increase.map(u64::try_from).transpose() {
                    Ok(exact) => exact,
                    Err(_) => return exec_err!("{name}: the counter increase overflows UInt64"),
                };
                (ScalarValue::UInt64(exact), increase.map(i128_to_f64))
            }
        };
        if !self.per_second {
            return Ok(increase);
        }
        let (Some(increase), Some((first, last))) = (increase_f64, self.samples.time_span()) else {
            return Ok(ScalarValue::Float64(None));
        };
        // Sorted by the increase above.
        let span_nanos = last.saturating_sub(first);
        if span_nanos <= 0 {
            return Ok(ScalarValue::Float64(None));
        }
        #[expect(
            clippy::cast_precision_loss,
            reason = "a time span in seconds does not need nanosecond precision"
        )]
        let seconds = span_nanos as f64 / 1e9;
        Ok(ScalarValue::Float64(Some(increase / seconds)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.samples.capacity() * size_of_val(&(0i64, 0f64))
    }
}

/// An exact integer increase as `f64`, rounded only once at the end for `rate`.
#[expect(
    clippy::cast_precision_loss,
    reason = "a per-second rate is a float; the increase itself was summed exactly"
)]
fn i128_to_f64(value: i128) -> f64 {
    value as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::logical_expr::AggregateUDF;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udaf(AggregateUDF::from(HistogramQuantile::new()));
        ctx.register_udaf(AggregateUDF::from(CounterGrowth::increase()));
        ctx.register_udaf(AggregateUDF::from(CounterGrowth::rate()));
        ctx
    }

    async fn query_f64(ctx: &SessionContext, sql: &str) -> Vec<Option<f64>> {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        let mut out = Vec::new();
        for batch in &batches {
            let col = cast(batch.column(batch.num_columns() - 1), &DataType::Float64).expect("f64");
            out.extend(col.as_primitive::<Float64Type>().iter());
        }
        assert!(
            !out.is_empty(),
            "{}",
            pretty_format_batches(&batches).expect("fmt")
        );
        out
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    /// Expected values follow Prometheus `bucketQuantile` on the same buckets.
    #[test]
    fn bucket_quantile_matches_prometheus() {
        // Buckets le=1:10, le=2:20 (cumulative 30), le=4:10 (cum 40), +Inf:0 (cum 40).
        let bounds = [1.0, 2.0, 4.0];
        let counts = [10, 20, 10, 0];
        // rank 20 falls in (1,2]: 1 + 1 * (20-10)/20 = 1.5
        assert!(close(bucket_quantile(0.5, &bounds, &counts), 1.5));
        // rank 4 in the first bucket, interpolated from 0: 4 * 1/10 = 0.4
        assert!(close(bucket_quantile(0.1, &bounds, &counts), 0.4));
        // rank 38 in (2,4]: 2 + 2 * (38-30)/10 = 3.6
        assert!(close(bucket_quantile(0.95, &bounds, &counts), 3.6));
        assert!(close(bucket_quantile(1.0, &bounds, &counts), 4.0));
        // Ranks in the +Inf bucket return the highest finite bound.
        assert!(close(bucket_quantile(0.99, &bounds, &[0, 0, 1, 9]), 4.0));
        assert!(bucket_quantile(-0.1, &bounds, &counts).is_infinite());
        assert!(bucket_quantile(1.1, &bounds, &counts).is_sign_positive());
        assert!(bucket_quantile(f64::NAN, &bounds, &counts).is_nan());
        assert!(bucket_quantile(0.5, &bounds, &[0; 4]).is_nan());
        assert!(bucket_quantile(0.5, &[], &[1]).is_nan());
        // A non-positive lowest bound is returned as is.
        assert!(close(bucket_quantile(0.1, &[-1.0, 1.0], &[5, 5, 0]), -1.0));
        // Large counts are summed exactly: the +1 past 2^53 moves q = 1 into the second bucket.
        assert!(close(
            bucket_quantile(1.0, &[1.0, 2.0], &[9_007_199_254_740_992, 1, 0]),
            2.0
        ));
    }

    #[test]
    fn counter_increase_handles_resets_and_order() {
        let mut samples = vec![(3, 2.0), (1, 5.0), (2, 9.0)];
        // Sorted: 5, 9, 2 -> 4 + reset(2) = 6
        assert_eq!(counter_increase(&mut samples), Some(6.0));
        assert_eq!(counter_increase(&mut [(1, 1.0)]), None);
        assert_eq!(counter_increase(&mut [(1, 1.0), (2, 1.0)]), Some(0.0));
    }

    /// 2^53: the first integer above it, 2^53 + 1, has no `f64` representation.
    const TWO_POW_53: i64 = 9_007_199_254_740_992;

    #[test]
    fn integer_counter_increase_is_exact_past_2_pow_53() {
        // As f64 both samples round to 2^53 and the +1 would be lost.
        let mut signed = vec![(2, TWO_POW_53 + 1), (1, TWO_POW_53)];
        assert_eq!(integer_counter_increase(&mut signed), Some(1));
        // Reset from 2^53 + 3 to 2^53 + 1: the new value is all growth.
        let mut reset = vec![(1, TWO_POW_53), (2, TWO_POW_53 + 3), (3, TWO_POW_53 + 1)];
        assert_eq!(
            integer_counter_increase(&mut reset),
            Some(3 + i128::from(TWO_POW_53) + 1)
        );
        let mut unsigned = vec![(1, u64::MAX - 1), (2, u64::MAX)];
        assert_eq!(integer_counter_increase(&mut unsigned), Some(1));
        assert_eq!(integer_counter_increase(&mut [(1, 1_i64)]), None);
    }

    async fn query_scalar(ctx: &SessionContext, sql: &str) -> ScalarValue {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("run");
        let batch = batches.first().expect("a batch");
        ScalarValue::try_from_array(batch.column(batch.num_columns() - 1), 0).expect("scalar")
    }

    #[tokio::test]
    async fn integer_increase_and_rate_are_exact_past_2_pow_53() {
        let ctx = ctx();
        let rows = format!(
            "(VALUES (0, {a}), (1000000000, {b})) AS t(ts, v)",
            a = TWO_POW_53,
            b = TWO_POW_53 + 1
        );
        assert_eq!(
            query_scalar(&ctx, &format!("SELECT increase(v, ts) FROM {rows}")).await,
            ScalarValue::Int64(Some(1))
        );
        assert_eq!(
            query_scalar(
                &ctx,
                &format!("SELECT increase(arrow_cast(v, 'UInt64'), ts) FROM {rows}")
            )
            .await,
            ScalarValue::UInt64(Some(1))
        );
        assert_eq!(
            query_scalar(&ctx, &format!("SELECT rate(v, ts) FROM {rows}")).await,
            ScalarValue::Float64(Some(1.0))
        );
        assert_eq!(
            query_scalar(
                &ctx,
                "SELECT increase(v, ts) FROM (VALUES (1, 1)) AS t(ts, v)"
            )
            .await,
            ScalarValue::Int64(None)
        );
    }

    #[test]
    fn integer_counter_state_merges_exactly() {
        let signed = || CounterGrowthAccumulator {
            per_second: false,
            samples: CounterSamples::new(CounterKind::Signed),
        };
        let mut parts = [signed(), signed()];
        for (acc, (t, v)) in parts
            .iter_mut()
            .zip([(1_i64, TWO_POW_53), (2, TWO_POW_53 + 1)])
        {
            let values: ArrayRef = Arc::new(Int64Array::from(vec![v]));
            let times: ArrayRef = Arc::new(Int64Array::from(vec![t]));
            acc.update_batch(&[values, times]).expect("update");
        }
        let mut merged = signed();
        for acc in &mut parts {
            let state: Vec<ArrayRef> = acc
                .state()
                .expect("state")
                .into_iter()
                .map(|s| s.to_array().expect("array"))
                .collect();
            merged.merge_batch(&state).expect("merge");
        }
        assert_eq!(
            merged.evaluate().expect("evaluate"),
            ScalarValue::Int64(Some(1))
        );
    }

    #[tokio::test]
    async fn histogram_quantile_sums_buckets_across_rows() {
        let ctx = ctx();
        let got = query_f64(
            &ctx,
            "SELECT histogram_quantile(0.5, b, c) FROM (VALUES
                (make_array(1.0, 2.0, 4.0), make_array(5, 10, 5, 0)),
                (make_array(1.0, 2.0, 4.0), make_array(5, 10, 5, 0))
            ) AS t(b, c)",
        )
        .await;
        assert!(close(got[0].expect("value"), 1.5));
    }

    #[tokio::test]
    async fn histogram_quantile_groups_and_unsigned_counts() {
        let ctx = ctx();
        let got = query_f64(
            &ctx,
            "SELECT g, histogram_quantile(0.5, b, arrow_cast(c, 'List(UInt64)')) FROM (VALUES
                ('a', make_array(1.0, 2.0), make_array(10, 0, 0)),
                ('b', make_array(1.0, 2.0), make_array(0, 10, 0))
            ) AS t(g, b, c) GROUP BY g ORDER BY g",
        )
        .await;
        assert!(close(got[0].expect("a"), 0.5));
        assert!(close(got[1].expect("b"), 1.5));
    }

    #[tokio::test]
    async fn histogram_quantile_rejects_mixed_bucket_layouts() {
        let ctx = ctx();
        let err = ctx
            .sql(
                "SELECT histogram_quantile(0.5, b, c) FROM (VALUES
                    (make_array(1.0, 2.0), make_array(1, 1, 0)),
                    (make_array(1.0, 3.0), make_array(1, 1, 0))
                ) AS t(b, c)",
            )
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("mixed bounds");
        assert!(err.to_string().contains("same explicit_bounds"), "{err}");
    }

    #[tokio::test]
    async fn increase_and_rate_over_unix_nanos_with_a_reset() {
        let ctx = ctx();
        let rows = "(VALUES (0, 10.0), (1000000000, 15.0), (2000000000, 3.0), (4000000000, 7.0)) AS t(ts, v)";
        let increase = query_f64(&ctx, &format!("SELECT increase(v, ts) FROM {rows}")).await;
        // 5 + reset(3) + 4 = 12
        assert!(close(increase[0].expect("increase"), 12.0));
        let rate = query_f64(&ctx, &format!("SELECT rate(v, ts) FROM {rows}")).await;
        // 12 over 4 seconds
        assert!(close(rate[0].expect("rate"), 3.0));
    }

    #[tokio::test]
    async fn rate_accepts_timestamps_and_unsigned_nanos() {
        let ctx = ctx();
        let ts = query_f64(
            &ctx,
            "SELECT rate(v, ts) FROM (VALUES
                (TIMESTAMP '2026-01-01 00:00:00', 0.0),
                (TIMESTAMP '2026-01-01 00:00:10', 20.0)
            ) AS t(ts, v)",
        )
        .await;
        assert!(close(ts[0].expect("rate"), 2.0));
        let unsigned = query_f64(
            &ctx,
            "SELECT increase(v, arrow_cast(ts, 'UInt64')) FROM (VALUES (1, 1.0), (2, 4.0)) AS t(ts, v)",
        )
        .await;
        assert!(close(unsigned[0].expect("increase"), 3.0));
    }

    #[tokio::test]
    async fn single_sample_groups_are_null() {
        let ctx = ctx();
        let got = query_f64(
            &ctx,
            "SELECT rate(v, ts) FROM (VALUES (1, 1.0)) AS t(ts, v)",
        )
        .await;
        assert_eq!(got, vec![None]);
    }
}
