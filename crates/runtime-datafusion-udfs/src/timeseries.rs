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

//! Time-series aggregates for OTLP metrics (#15060).
//!
//! - `histogram_quantile(q, explicit_bounds, bucket_counts)` adds bucket counts element by
//!   element across rows, then interpolates within the target bucket like Prometheus.
//! - `increase(value, time)` is the growth of a cumulative counter across its samples,
//!   treating any decrease as a counter reset.
//! - `rate(value, time)` is `increase` divided by the seconds between the first and last
//!   sample, with `time` in Unix nanoseconds or a timestamp.
//!
//! Unlike PromQL, `increase` and `rate` do not extrapolate to the edges of a time window:
//! they use only the samples in the group.

use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array, ListArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type, TimeUnit};
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
pub fn bucket_quantile(q: f64, bounds: &[f64], counts: &[f64]) -> f64 {
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
    let mut cumulative = Vec::with_capacity(counts.len());
    let mut total = 0.0;
    for count in counts {
        total += count;
        cumulative.push(total);
    }
    if total <= 0.0 {
        return f64::NAN;
    }
    let rank = q * total;
    let bucket = cumulative
        .iter()
        .position(|&c| c >= rank)
        .unwrap_or(cumulative.len() - 1);
    if bucket == bounds.len() {
        // The +Inf bucket: return the highest finite bound.
        return bounds[bounds.len() - 1];
    }
    if bucket == 0 && bounds[0] <= 0.0 {
        return bounds[0];
    }
    let (start, below) = if bucket == 0 {
        (0.0, 0.0)
    } else {
        (bounds[bucket - 1], cumulative[bucket - 1])
    };
    let end = bounds[bucket];
    let in_bucket = cumulative[bucket] - below;
    if in_bucket <= 0.0 {
        return end;
    }
    start + (end - start) * ((rank - below) / in_bucket)
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
        for (name, t) in [("explicit_bounds", bounds), ("bucket_counts", counts)] {
            if !is_numeric_list(t) {
                return plan_err!(
                    "{HISTOGRAM_QUANTILE_UDAF_NAME}: {name} must be a list of numbers, got {t}"
                );
            }
        }
        Ok(vec![DataType::Float64, f64_list_type(), f64_list_type()])
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
                f64_list_type(),
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
    counts: Vec<f64>,
}

impl HistogramQuantileAccumulator {
    fn add(
        &mut self,
        q: Option<f64>,
        bounds: Option<Vec<f64>>,
        counts: Option<Vec<f64>>,
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
                    *total += count;
                }
            }
        }
        Ok(())
    }

    fn add_arrays(&mut self, q: &ArrayRef, bounds: &ArrayRef, counts: &ArrayRef) -> Result<()> {
        let q = f64_values(q)?;
        let bounds = f64_lists(bounds)?;
        let counts = f64_lists(counts)?;
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
            list_scalar(has_buckets.then_some(self.counts.as_slice())),
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

/// `increase` (`per_second = false`) or `rate` (`per_second = true`).
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
        Ok(vec![DataType::Float64, time])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CounterGrowthAccumulator {
            per_second: self.per_second,
            samples: Vec::new(),
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "values"),
                f64_list_type(),
                true,
            )
            .into(),
            Field::new(format_state_name(args.name, "times"), i64_list_type(), true).into(),
        ])
    }
}

#[derive(Debug)]
struct CounterGrowthAccumulator {
    per_second: bool,
    samples: Vec<(i64, f64)>,
}

impl Accumulator for CounterGrowthAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [value, time] = values else {
            return exec_err!("increase/rate expect 2 arguments");
        };
        let value = f64_values(value)?;
        let time = nanos_values(time)?;
        for (v, t) in value.iter().zip(time.iter()) {
            if let (Some(v), Some(t)) = (v, t)
                && !v.is_nan()
            {
                self.samples.push((t, v));
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [values, times] = states else {
            return exec_err!("increase/rate expect 2 state fields");
        };
        let values = f64_lists(values)?;
        let times = cast(times, &i64_list_type())?;
        let times = times.as_list::<i32>();
        for (i, values) in values.into_iter().enumerate() {
            let Some(values) = values else { continue };
            if times.is_null(i) {
                continue;
            }
            let row = times.value(i);
            let row = row.as_primitive::<Int64Type>();
            for (t, v) in row.iter().zip(values) {
                if let Some(t) = t {
                    self.samples.push((t, v));
                }
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<f64> = self.samples.iter().map(|s| s.1).collect();
        let times: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([Some(
            self.samples.iter().map(|s| Some(s.0)).collect::<Vec<_>>(),
        )]));
        Ok(vec![
            list_scalar(Some(&values)),
            ScalarValue::List(Arc::new(times.as_list::<i32>().clone())),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let Some(increase) = counter_increase(&mut self.samples) else {
            return Ok(ScalarValue::Float64(None));
        };
        if !self.per_second {
            return Ok(ScalarValue::Float64(Some(increase)));
        }
        // Sorted by `counter_increase`.
        let first = self.samples[0].0;
        let last = self.samples[self.samples.len() - 1].0;
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
        let counts = [10.0, 20.0, 10.0, 0.0];
        // rank 20 falls in (1,2]: 1 + 1 * (20-10)/20 = 1.5
        assert!(close(bucket_quantile(0.5, &bounds, &counts), 1.5));
        // rank 4 in the first bucket, interpolated from 0: 4 * 1/10 = 0.4
        assert!(close(bucket_quantile(0.1, &bounds, &counts), 0.4));
        // rank 38 in (2,4]: 2 + 2 * (38-30)/10 = 3.6
        assert!(close(bucket_quantile(0.95, &bounds, &counts), 3.6));
        assert!(close(bucket_quantile(1.0, &bounds, &counts), 4.0));
        // Ranks in the +Inf bucket return the highest finite bound.
        assert!(close(
            bucket_quantile(0.99, &bounds, &[0.0, 0.0, 1.0, 9.0]),
            4.0
        ));
        assert!(bucket_quantile(-0.1, &bounds, &counts).is_infinite());
        assert!(bucket_quantile(1.1, &bounds, &counts).is_sign_positive());
        assert!(bucket_quantile(f64::NAN, &bounds, &counts).is_nan());
        assert!(bucket_quantile(0.5, &bounds, &[0.0; 4]).is_nan());
        assert!(bucket_quantile(0.5, &[], &[1.0]).is_nan());
        // A non-positive lowest bound is returned as is.
        assert!(close(
            bucket_quantile(0.1, &[-1.0, 1.0], &[5.0, 5.0, 0.0]),
            -1.0
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
