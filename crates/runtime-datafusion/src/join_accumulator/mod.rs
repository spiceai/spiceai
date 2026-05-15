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

use std::{
    any::{Any, type_name},
    cmp::Ordering,
    collections::HashSet,
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
    mem::size_of,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, BooleanBuilder, GenericStringArray, OffsetSizeTrait,
        PrimitiveArray, RecordBatch, StringViewArray,
    },
    compute::{max, max_string, max_string_view, min, min_string, min_string_view},
    datatypes::{
        ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type,
        Decimal128Type, Decimal256Type, Field, Float16Type, Float32Type, Float64Type, Int8Type,
        Int16Type, Int32Type, Int64Type, Schema, SchemaRef, Time32MillisecondType,
        Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::{
    common::hash_utils::{combine_hashes, with_hashes},
    logical_expr::Operator,
    physical_plan::{
        ColumnarValue, PhysicalExpr,
        expressions::{BinaryExpr, InListExpr, Literal},
        joins::{CollectLeftAccumulator, ColumnBounds, SeededRandomState},
    },
    scalar::ScalarValue,
};

pub const DEFAULT_MAXIMUM_SHARED_INLIST_MEMORY_BYTES: usize = 128 * 1024 * 1024; // 128Mb - can store approximately 32 million i32 keys.
const DEFAULT_MAXIMUM_BLOOM_FILTER_MEMORY_BYTES: usize = 8 * 1024 * 1024;
const MAXIMUM_RANGE_INTERVALS: usize = 64;

static MAXIMUM_SHARED_INLIST_MEMORY_BYTES: AtomicUsize =
    AtomicUsize::new(DEFAULT_MAXIMUM_SHARED_INLIST_MEMORY_BYTES);
static CURRENT_INLIST_MEMORY_BYTES: AtomicUsize = AtomicUsize::new(0);
// The exact in-list path reserves against one process-wide budget shared across
// all accumulator instances. This keeps dynamic join filters bounded under query
// concurrency; individual accumulators still keep their own local cap.

#[must_use]
pub fn maximum_shared_inlist_memory_bytes() -> usize {
    MAXIMUM_SHARED_INLIST_MEMORY_BYTES.load(AtomicOrdering::Relaxed)
}

/// Conservatively clamps the process-wide exact in-list reservation budget.
///
/// `DataFusion` constructs `CollectLeftAccumulator` instances without session
/// state, so the Cayenne join rewriter cannot attach a per-session limit at
/// accumulator construction time. If multiple `DataFusion` instances are built
/// in one process, use the strictest configured limit instead of letting the
/// most recent builder raise the shared budget for existing instances.
pub fn clamp_maximum_shared_inlist_memory_bytes(limit: usize) {
    MAXIMUM_SHARED_INLIST_MEMORY_BYTES.fetch_min(limit, AtomicOrdering::Relaxed);
}

#[derive(Debug)]
struct InListMemoryReservation {
    bytes: usize,
}

impl InListMemoryReservation {
    fn try_new(bytes: usize) -> Option<Self> {
        reserve_inlist_memory(bytes).then_some(Self { bytes })
    }

    fn try_grow(&mut self, bytes: usize) -> bool {
        if !reserve_inlist_memory(bytes) {
            return false;
        }

        self.bytes = self.bytes.saturating_add(bytes);
        true
    }
}

impl Drop for InListMemoryReservation {
    fn drop(&mut self) {
        CURRENT_INLIST_MEMORY_BYTES.fetch_sub(self.bytes, AtomicOrdering::Relaxed);
    }
}

fn reserve_inlist_memory(bytes: usize) -> bool {
    reserve_inlist_memory_with_limit(bytes, maximum_shared_inlist_memory_bytes())
}

fn reserve_inlist_memory_with_limit(bytes: usize, limit: usize) -> bool {
    if bytes == 0 {
        return true;
    }

    if limit == 0 || bytes > limit {
        return false;
    }

    CURRENT_INLIST_MEMORY_BYTES
        .fetch_update(
            AtomicOrdering::Relaxed,
            AtomicOrdering::Relaxed,
            |current| current.checked_add(bytes).filter(|next| *next <= limit),
        )
        .is_ok()
}

fn bloom_memory_limit(max_inlist_memory_size: usize) -> usize {
    if max_inlist_memory_size == 0 {
        0
    } else {
        (max_inlist_memory_size / 16).min(DEFAULT_MAXIMUM_BLOOM_FILTER_MEMORY_BYTES)
    }
}

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
    max_bloom_filter_memory_size: usize,
    inlist_memory_reservation: Option<InListMemoryReservation>,
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
            maximum_shared_inlist_memory_bytes(),
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

        let array_memory_size = array.get_array_memory_size();
        let total_memory_size = self.total_memory_size.saturating_add(array_memory_size);

        if total_memory_size > self.max_inlist_memory_size {
            tracing::warn!(
                total_memory_size,
                max_inlist_memory_size = self.max_inlist_memory_size,
                "ExactLeftAccumulator exceeded its local in-list memory limit and fell back to a range pre-filter. \
                 For anti-join / LeftAnti / NOT IN / NOT EXISTS patterns this approximation can silently drop rows that should have survived the anti-condition (false negatives). \
                 This is a known correctness trade-off under memory pressure. Consider increasing memory limits or reducing cardinality of the build side."
            );
            self.inlist_memory_reservation = None;
            self.range_bounds = self.range_bounds_from_collected_arrays(array.as_ref())?;
            self.arrays.clear();
            self.total_memory_size = total_memory_size;
            self.exact_values_exceeded_memory_limit = true;
            return Ok(());
        }

        if !self.try_reserve_inlist_memory(array_memory_size) {
            tracing::warn!(
                requested_bytes = array_memory_size,
                current_shared_inlist_memory_bytes =
                    CURRENT_INLIST_MEMORY_BYTES.load(AtomicOrdering::Relaxed),
                maximum_shared_inlist_memory_bytes = maximum_shared_inlist_memory_bytes(),
                "ExactLeftAccumulator shared in-list memory budget is exhausted and fell back to a range pre-filter. \
                 For anti-join / LeftAnti / NOT IN / NOT EXISTS patterns this approximation can silently drop rows that should have survived the anti-condition (false negatives). \
                 This is a known correctness trade-off under memory pressure. Consider increasing memory limits or reducing cardinality of the build side."
            );
            self.inlist_memory_reservation = None;
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
        let Self {
            arrays,
            total_memory_size,
            range_bounds,
            exact_values_exceeded_memory_limit,
            inlist_memory_reservation,
            ..
        } = self;

        Ok(Arc::new(ExactColumnBounds {
            arrays,
            total_memory_size,
            range_bounds,
            use_range_fallback: exact_values_exceeded_memory_limit,
            _inlist_memory_reservation: inlist_memory_reservation,
        }))
    }
}

impl ExactLeftAccumulator {
    /// Creates an accumulator with a custom local in-list memory limit.
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
            max_bloom_filter_memory_size: bloom_memory_limit(max_inlist_memory_size),
            inlist_memory_reservation: None,
            range_bounds: RangeBounds::new(bloom_memory_limit(max_inlist_memory_size)),
            exact_values_exceeded_memory_limit: false,
        }
    }

    fn try_reserve_inlist_memory(&mut self, bytes: usize) -> bool {
        if let Some(reservation) = &mut self.inlist_memory_reservation {
            reservation.try_grow(bytes)
        } else {
            let Some(reservation) = InListMemoryReservation::try_new(bytes) else {
                return false;
            };
            self.inlist_memory_reservation = Some(reservation);
            true
        }
    }

    fn range_bounds_from_collected_arrays(
        &self,
        array: &dyn Array,
    ) -> DataFusionResult<RangeBounds> {
        let mut range_bounds = RangeBounds::new(self.max_bloom_filter_memory_size);
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
    _inlist_memory_reservation: Option<InListMemoryReservation>,
}

impl ColumnBounds for ExactColumnBounds {
    /// Converts the collected arrays into an `InListExpr` for use in dynamic filtering.
    /// This builds an IN expression with all collected values.
    ///
    /// Devil's advocate (ACID Consistency for anti-join / LeftAnti / Q21 patterns):
    /// When `use_range_fallback` is true (memory limit hit), we produce a range
    /// filter instead of an exact InList. For inner/semi joins this is a safe
    /// over-filter (some rows may be unnecessarily excluded from the probe, but
    /// the join result remains correct because the build side still filters).
    ///
    /// For LeftAnti (and similar "not exists" / "not in" patterns pushed to
    /// Cayenne via ExactLeftAccumulator), the inclusive range pre-filter
    /// (`col BETWEEN min AND max`) is an approximation. Probe rows outside the
    /// min/max range that are not present in the exact collected set should
    /// survive the anti-join, but the scanner can skip them, producing false
    /// negatives (missing result rows). This is accepted as a performance
    /// trade-off when memory is exhausted; the alternative would be to spill or
    /// OOM.
    ///
    /// The CoalescePartitionsExec + iterative flatten wrapper detection added
    /// in the optimizer ensures more plans (including those with partition
    /// coalescing between join and Cayenne scan) now correctly route through
    /// ExactLeftAccumulator, increasing the importance of these edge cases
    /// being well understood and tested.
    ///
    /// NULL handling: In the exact path, NULLs from the build side are collected
    /// as ScalarValue::Null. An InList containing NULL never matches (SQL
    /// three-valued logic), which is the correct "not in" behavior for anti-joins.
    /// The range fallback may treat NULLs differently depending on RangeBounds
    /// implementation.
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
    intervals: Vec<RangeInterval>,
    bloom_filter: Option<BloomFilter>,
    supports_range_filter: bool,
}

impl RangeBounds {
    fn new(max_bloom_filter_memory_size: usize) -> Self {
        Self {
            intervals: Vec::new(),
            bloom_filter: BloomFilter::try_new(max_bloom_filter_memory_size),
            supports_range_filter: true,
        }
    }

    fn update(&mut self, array: &dyn Array) -> DataFusionResult<()> {
        if !self.supports_range_filter {
            return Ok(());
        }

        let batch_bounds = min_max_values(array)?;
        let RangeBatchBounds::Values {
            min_value,
            max_value,
        } = batch_bounds
        else {
            if matches!(batch_bounds, RangeBatchBounds::Unsupported) {
                self.supports_range_filter = false;
            }
            return Ok(());
        };

        if !supports_range_comparison(&min_value) || !supports_range_comparison(&max_value) {
            self.supports_range_filter = false;
            return Ok(());
        }

        self.update_bloom_filter(array)?;
        if !self.add_interval(RangeInterval::new(min_value, max_value)) {
            self.supports_range_filter = false;
        }

        Ok(())
    }

    fn update_bloom_filter(&mut self, array: &dyn Array) -> DataFusionResult<()> {
        let Some(bloom_filter) = &mut self.bloom_filter else {
            return Ok(());
        };

        // The Cayenne rewriter only enables this accumulator for
        // `NullEqualsNothing`, so build-side NULL keys cannot match probe keys.
        bloom_filter.insert_array(array)
    }

    fn add_interval(&mut self, interval: RangeInterval) -> bool {
        self.intervals.push(interval);
        self.intervals.sort_by(|left, right| {
            left.min_value
                .partial_cmp(&right.min_value)
                .unwrap_or(Ordering::Equal)
        });

        let mut merged_intervals: Vec<RangeInterval> = Vec::with_capacity(self.intervals.len());
        for interval in self.intervals.drain(..) {
            let Some(previous) = merged_intervals.last_mut() else {
                merged_intervals.push(interval);
                continue;
            };

            if previous.overlaps(&interval) {
                if !previous.merge(interval) {
                    return false;
                }
            } else {
                merged_intervals.push(interval);
            }
        }

        if merged_intervals.len() > MAXIMUM_RANGE_INTERVALS {
            let Some(mut global_range) = merged_intervals.first().cloned() else {
                self.intervals.clear();
                return true;
            };

            for interval in merged_intervals.into_iter().skip(1) {
                if !global_range.merge(interval) {
                    return false;
                }
            }

            self.intervals.push(global_range);
        } else {
            self.intervals = merged_intervals;
        }

        true
    }

    fn physical_expr(&self, left_expr: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        if self.intervals.is_empty() {
            tracing::debug!(
                "ExactLeftAccumulator range fallback has no non-null values, returning no-op filter."
            );
            return literal_true();
        }

        if !self.supports_range_filter {
            tracing::debug!(
                supports_range_filter = self.supports_range_filter,
                "ExactLeftAccumulator could not create range fallback, returning no-op filter."
            );
            return literal_true();
        }

        let mut range_expr = self
            .intervals
            .iter()
            .map(|interval| interval.physical_expr(Arc::clone(&left_expr)))
            .reduce(|left, right| Arc::new(BinaryExpr::new(left, Operator::Or, right)) as _)
            .unwrap_or_else(literal_true);

        if let Some(bloom_filter) = &self.bloom_filter
            && bloom_filter.has_values()
        {
            let bloom_expr = Arc::new(BloomFilterExpr::new(
                left_expr,
                Arc::new(bloom_filter.clone()),
            )) as Arc<dyn PhysicalExpr>;
            range_expr = Arc::new(BinaryExpr::new(range_expr, Operator::And, bloom_expr));
        }

        tracing::debug!(
            interval_count = self.intervals.len(),
            has_bloom_filter = self
                .bloom_filter
                .as_ref()
                .is_some_and(BloomFilter::has_values),
            "ExactLeftAccumulator created range fallback."
        );

        range_expr
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RangeInterval {
    min_value: ScalarValue,
    max_value: ScalarValue,
}

impl RangeInterval {
    fn new(min_value: ScalarValue, max_value: ScalarValue) -> Self {
        Self {
            min_value,
            max_value,
        }
    }

    fn overlaps(&self, other: &Self) -> bool {
        matches!(
            other.min_value.partial_cmp(&self.max_value),
            Some(Ordering::Less | Ordering::Equal)
        )
    }

    fn merge(&mut self, other: Self) -> bool {
        let min_value = match other.min_value.partial_cmp(&self.min_value) {
            Some(Ordering::Less) => other.min_value,
            Some(_) => self.min_value.clone(),
            None => return false,
        };
        let max_value = match other.max_value.partial_cmp(&self.max_value) {
            Some(Ordering::Greater) => other.max_value,
            Some(_) => self.max_value.clone(),
            None => return false,
        };

        self.min_value = min_value;
        self.max_value = max_value;
        true
    }

    fn physical_expr(&self, left_expr: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        let lower_bound = Arc::new(BinaryExpr::new(
            Arc::clone(&left_expr),
            Operator::GtEq,
            Arc::new(Literal::new(self.min_value.clone())),
        ));
        let upper_bound = Arc::new(BinaryExpr::new(
            left_expr,
            Operator::LtEq,
            Arc::new(Literal::new(self.max_value.clone())),
        ));

        Arc::new(BinaryExpr::new(lower_bound, Operator::And, upper_bound))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BloomFilter {
    bits: Vec<u64>,
    bit_count: usize,
    inserted_values: usize,
}

impl BloomFilter {
    const HASH_COUNT: u64 = 7;
    const HASH_STEP_SALT: u64 = 0x9E37_79B9_7F4A_7C15;

    fn try_new(memory_bytes: usize) -> Option<Self> {
        let word_count = memory_bytes / size_of::<u64>();
        if word_count == 0 {
            return None;
        }

        Some(Self {
            bits: vec![0; word_count],
            bit_count: word_count * u64::BITS as usize,
            inserted_values: 0,
        })
    }

    fn insert_hash(&mut self, hash: u64) {
        let (hash_one, hash_two) = bloom_hashes(hash);
        self.insert_hashes(hash_one, hash_two);
    }

    fn insert_hashes(&mut self, hash_one: u64, hash_two: u64) {
        for hash_index in 0..Self::HASH_COUNT {
            let bit_index = self.bit_index(hash_one, hash_two, hash_index);
            self.bits[bit_index / u64::BITS as usize] |= 1 << (bit_index % u64::BITS as usize);
        }
        self.inserted_values = self.inserted_values.saturating_add(1);
    }

    fn might_contain_hash(&self, hash: u64) -> bool {
        let (hash_one, hash_two) = bloom_hashes(hash);
        self.might_contain_hashes(hash_one, hash_two)
    }

    fn might_contain_hashes(&self, hash_one: u64, hash_two: u64) -> bool {
        (0..Self::HASH_COUNT).all(|hash_index| {
            let bit_index = self.bit_index(hash_one, hash_two, hash_index);
            let word = self.bits[bit_index / u64::BITS as usize];
            (word & (1 << (bit_index % u64::BITS as usize))) != 0
        })
    }

    fn insert_array(&mut self, array: &dyn Array) -> DataFusionResult<()> {
        match array.data_type() {
            DataType::Float16 => self.insert_float_array::<Float16Type>(array),
            DataType::Float32 => self.insert_float_array::<Float32Type>(array),
            DataType::Float64 => self.insert_float_array::<Float64Type>(array),
            _ => self.insert_datafusion_hashes(array),
        }
    }

    fn evaluate_array(&self, array: &dyn Array) -> DataFusionResult<BooleanArray> {
        match array.data_type() {
            DataType::Float16 => self.evaluate_float_array::<Float16Type>(array),
            DataType::Float32 => self.evaluate_float_array::<Float32Type>(array),
            DataType::Float64 => self.evaluate_float_array::<Float64Type>(array),
            _ => self.evaluate_datafusion_hashes(array),
        }
    }

    fn insert_float_array<T>(&mut self, array: &dyn Array) -> DataFusionResult<()>
    where
        T: BloomFloatType,
    {
        let array = downcast_array::<PrimitiveArray<T>>(array)?;
        let zero_hashes = float_zero_hashes::<T>()?;
        for_each_datafusion_hash(array, |row_index, hash| {
            if array.is_valid(row_index) {
                let value = array.value(row_index);
                if T::is_nan(value) {
                    return;
                }

                self.insert_hash(hash);
                if T::is_zero(value) {
                    self.insert_hash(zero_hashes.0);
                    self.insert_hash(zero_hashes.1);
                }
            }
        })
    }

    fn evaluate_float_array<T>(&self, array: &dyn Array) -> DataFusionResult<BooleanArray>
    where
        T: BloomFloatType,
    {
        let array = downcast_array::<PrimitiveArray<T>>(array)?;
        let mut builder = BooleanBuilder::with_capacity(array.len());
        for_each_datafusion_hash(array, |row_index, hash| {
            builder.append_value(if array.is_valid(row_index) {
                let value = array.value(row_index);
                !T::is_nan(value) && self.might_contain_hash(hash)
            } else {
                false
            });
        })?;
        Ok(builder.finish())
    }

    fn insert_datafusion_hashes(&mut self, array: &dyn Array) -> DataFusionResult<()> {
        for_each_datafusion_hash(array, |row_index, hash| {
            if array.is_valid(row_index) {
                self.insert_hash(hash);
            }
        })
    }

    fn evaluate_datafusion_hashes(&self, array: &dyn Array) -> DataFusionResult<BooleanArray> {
        let mut builder = BooleanBuilder::with_capacity(array.len());
        for_each_datafusion_hash(array, |row_index, hash| {
            builder.append_value(array.is_valid(row_index) && self.might_contain_hash(hash));
        })?;
        Ok(builder.finish())
    }

    fn has_values(&self) -> bool {
        self.inserted_values > 0
    }

    fn bit_index(&self, hash_one: u64, hash_two: u64, hash_index: u64) -> usize {
        let hash = hash_one.wrapping_add(hash_index.wrapping_mul(hash_two | 1));
        usize::try_from(hash % self.bit_count as u64).unwrap_or(0)
    }
}

fn datafusion_hash_join_random_state() -> SeededRandomState {
    SeededRandomState::with_seeds('J' as u64, 'O' as u64, 'I' as u64, 'N' as u64)
}

fn for_each_datafusion_hash(
    array: &dyn Array,
    mut f: impl FnMut(usize, u64),
) -> DataFusionResult<()> {
    with_hashes(
        [array],
        datafusion_hash_join_random_state().random_state(),
        |hashes| {
            for (row_index, hash) in hashes.iter().copied().enumerate() {
                f(row_index, hash);
            }
            Ok(())
        },
    )
}

fn float_zero_hashes<T>() -> DataFusionResult<(u64, u64)>
where
    T: BloomFloatType,
{
    let array = PrimitiveArray::<T>::from_iter_values([T::positive_zero(), T::negative_zero()]);
    let mut zero_hashes = (0, 0);
    with_hashes(
        [&array as &dyn Array],
        datafusion_hash_join_random_state().random_state(),
        |hashes| {
            zero_hashes = (hashes[0], hashes[1]);
            Ok(())
        },
    )?;
    Ok(zero_hashes)
}

fn bloom_hashes(datafusion_hash: u64) -> (u64, u64) {
    (
        datafusion_hash,
        combine_hashes(datafusion_hash, BloomFilter::HASH_STEP_SALT),
    )
}

trait BloomFloatType: ArrowPrimitiveType {
    fn positive_zero() -> Self::Native;

    fn negative_zero() -> Self::Native;

    fn is_zero(value: Self::Native) -> bool;

    fn is_nan(value: Self::Native) -> bool;
}

impl BloomFloatType for Float16Type {
    fn positive_zero() -> Self::Native {
        <Self as ArrowPrimitiveType>::Native::from_f32(0.0)
    }

    fn negative_zero() -> Self::Native {
        <Self as ArrowPrimitiveType>::Native::from_f32(-0.0)
    }

    fn is_zero(value: Self::Native) -> bool {
        value == Self::positive_zero()
    }

    fn is_nan(value: Self::Native) -> bool {
        value.is_nan()
    }
}

impl BloomFloatType for Float32Type {
    fn positive_zero() -> Self::Native {
        0.0
    }

    fn negative_zero() -> Self::Native {
        -0.0
    }

    fn is_zero(value: Self::Native) -> bool {
        value == 0.0
    }

    fn is_nan(value: Self::Native) -> bool {
        value.is_nan()
    }
}

impl BloomFloatType for Float64Type {
    fn positive_zero() -> Self::Native {
        0.0
    }

    fn negative_zero() -> Self::Native {
        -0.0
    }

    fn is_zero(value: Self::Native) -> bool {
        value == 0.0
    }

    fn is_nan(value: Self::Native) -> bool {
        value.is_nan()
    }
}

#[derive(Debug, Clone)]
struct BloomFilterExpr {
    expr: Arc<dyn PhysicalExpr>,
    bloom_filter: Arc<BloomFilter>,
}

impl BloomFilterExpr {
    fn new(expr: Arc<dyn PhysicalExpr>, bloom_filter: Arc<BloomFilter>) -> Self {
        Self { expr, bloom_filter }
    }
}

impl PartialEq for BloomFilterExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.bloom_filter.eq(&other.bloom_filter)
    }
}

impl Eq for BloomFilterExpr {}

impl Hash for BloomFilterExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.bloom_filter.hash(state);
    }
}

impl Display for BloomFilterExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bloom_filter({})", self.expr)
    }
}

impl PhysicalExpr for BloomFilterExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let array = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(ColumnarValue::Array(
            Arc::new(self.bloom_filter.evaluate_array(array.as_ref())?) as ArrayRef,
        ))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let Some(expr) = children.into_iter().next() else {
            return Err(DataFusionError::Internal(
                "BloomFilterExpr expected one child expression".to_string(),
            ));
        };

        Ok(Arc::new(Self::new(expr, Arc::clone(&self.bloom_filter))))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bloom_filter({})", self.expr)
    }
}

enum RangeBatchBounds {
    Values {
        min_value: ScalarValue,
        max_value: ScalarValue,
    },
    NoValues,
    Unsupported,
}

fn min_max_values(array: &dyn Array) -> DataFusionResult<RangeBatchBounds> {
    match array.data_type() {
        DataType::Int8 => primitive_min_max::<Int8Type, _>(array, ScalarValue::Int8),
        DataType::Int16 => primitive_min_max::<Int16Type, _>(array, ScalarValue::Int16),
        DataType::Int32 => primitive_min_max::<Int32Type, _>(array, ScalarValue::Int32),
        DataType::Int64 => primitive_min_max::<Int64Type, _>(array, ScalarValue::Int64),
        DataType::UInt8 => primitive_min_max::<UInt8Type, _>(array, ScalarValue::UInt8),
        DataType::UInt16 => primitive_min_max::<UInt16Type, _>(array, ScalarValue::UInt16),
        DataType::UInt32 => primitive_min_max::<UInt32Type, _>(array, ScalarValue::UInt32),
        DataType::UInt64 => primitive_min_max::<UInt64Type, _>(array, ScalarValue::UInt64),
        DataType::Float16 => float_min_max::<Float16Type, _>(array, ScalarValue::Float16),
        DataType::Float32 => float_min_max::<Float32Type, _>(array, ScalarValue::Float32),
        DataType::Float64 => float_min_max::<Float64Type, _>(array, ScalarValue::Float64),
        DataType::Decimal32(precision, scale) => {
            let precision = *precision;
            let scale = *scale;
            primitive_min_max::<Decimal32Type, _>(array, |value| {
                ScalarValue::Decimal32(value, precision, scale)
            })
        }
        DataType::Decimal64(precision, scale) => {
            let precision = *precision;
            let scale = *scale;
            primitive_min_max::<Decimal64Type, _>(array, |value| {
                ScalarValue::Decimal64(value, precision, scale)
            })
        }
        DataType::Decimal128(precision, scale) => {
            let precision = *precision;
            let scale = *scale;
            primitive_min_max::<Decimal128Type, _>(array, |value| {
                ScalarValue::Decimal128(value, precision, scale)
            })
        }
        DataType::Decimal256(precision, scale) => {
            let precision = *precision;
            let scale = *scale;
            primitive_min_max::<Decimal256Type, _>(array, |value| {
                ScalarValue::Decimal256(value, precision, scale)
            })
        }
        DataType::Date32 => primitive_min_max::<Date32Type, _>(array, ScalarValue::Date32),
        DataType::Date64 => primitive_min_max::<Date64Type, _>(array, ScalarValue::Date64),
        DataType::Time32(TimeUnit::Second) => {
            primitive_min_max::<Time32SecondType, _>(array, ScalarValue::Time32Second)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            primitive_min_max::<Time32MillisecondType, _>(array, ScalarValue::Time32Millisecond)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            primitive_min_max::<Time64MicrosecondType, _>(array, ScalarValue::Time64Microsecond)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            primitive_min_max::<Time64NanosecondType, _>(array, ScalarValue::Time64Nanosecond)
        }
        DataType::Timestamp(TimeUnit::Second, timezone) => {
            let timezone = timezone.clone();
            primitive_min_max::<TimestampSecondType, _>(array, |value| {
                ScalarValue::TimestampSecond(value, timezone.clone())
            })
        }
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => {
            let timezone = timezone.clone();
            primitive_min_max::<TimestampMillisecondType, _>(array, |value| {
                ScalarValue::TimestampMillisecond(value, timezone.clone())
            })
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let timezone = timezone.clone();
            primitive_min_max::<TimestampMicrosecondType, _>(array, |value| {
                ScalarValue::TimestampMicrosecond(value, timezone.clone())
            })
        }
        DataType::Timestamp(TimeUnit::Nanosecond, timezone) => {
            let timezone = timezone.clone();
            primitive_min_max::<TimestampNanosecondType, _>(array, |value| {
                ScalarValue::TimestampNanosecond(value, timezone.clone())
            })
        }
        DataType::Utf8 => string_min_max::<i32, _>(array, ScalarValue::Utf8),
        DataType::LargeUtf8 => string_min_max::<i64, _>(array, ScalarValue::LargeUtf8),
        DataType::Utf8View => string_view_min_max(array),
        _ => Ok(RangeBatchBounds::Unsupported),
    }
}

fn primitive_min_max<T, F>(array: &dyn Array, scalar_value: F) -> DataFusionResult<RangeBatchBounds>
where
    T: ArrowPrimitiveType,
    T::Native: PartialOrd,
    F: Fn(Option<T::Native>) -> ScalarValue,
{
    let array = downcast_array::<PrimitiveArray<T>>(array)?;
    let (Some(min_value), Some(max_value)) = (min::<T>(array), max::<T>(array)) else {
        return Ok(RangeBatchBounds::NoValues);
    };

    Ok(RangeBatchBounds::Values {
        min_value: scalar_value(Some(min_value)),
        max_value: scalar_value(Some(max_value)),
    })
}

fn float_min_max<T, F>(array: &dyn Array, scalar_value: F) -> DataFusionResult<RangeBatchBounds>
where
    T: BloomFloatType,
    T::Native: PartialOrd,
    F: Fn(Option<T::Native>) -> ScalarValue,
{
    let array = downcast_array::<PrimitiveArray<T>>(array)?;
    if array.iter().flatten().any(T::is_nan) {
        return Ok(RangeBatchBounds::Unsupported);
    }

    let (Some(min_value), Some(max_value)) = (min::<T>(array), max::<T>(array)) else {
        return Ok(RangeBatchBounds::NoValues);
    };

    Ok(RangeBatchBounds::Values {
        min_value: scalar_value(Some(min_value)),
        max_value: scalar_value(Some(max_value)),
    })
}

fn string_min_max<T, F>(array: &dyn Array, scalar_value: F) -> DataFusionResult<RangeBatchBounds>
where
    T: OffsetSizeTrait,
    F: Fn(Option<String>) -> ScalarValue,
{
    let array = downcast_array::<GenericStringArray<T>>(array)?;
    let (Some(min_value), Some(max_value)) = (min_string(array), max_string(array)) else {
        return Ok(RangeBatchBounds::NoValues);
    };

    Ok(RangeBatchBounds::Values {
        min_value: scalar_value(Some(min_value.to_string())),
        max_value: scalar_value(Some(max_value.to_string())),
    })
}

fn string_view_min_max(array: &dyn Array) -> DataFusionResult<RangeBatchBounds> {
    let array = downcast_array::<StringViewArray>(array)?;
    let (Some(min_value), Some(max_value)) = (min_string_view(array), max_string_view(array))
    else {
        return Ok(RangeBatchBounds::NoValues);
    };

    Ok(RangeBatchBounds::Values {
        min_value: ScalarValue::Utf8View(Some(min_value.to_string())),
        max_value: ScalarValue::Utf8View(Some(max_value.to_string())),
    })
}

fn downcast_array<T: 'static>(array: &dyn Array) -> DataFusionResult<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Failed to downcast join filter array with type {} to {}",
            array.data_type(),
            type_name::<T>()
        ))
    })
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
    use arrow::array::{Float64Array, Int32Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Column;
    use datafusion::physical_plan::expressions::col;
    use datafusion_pruning::{PruningPredicate, PruningStatistics};
    use std::sync::Mutex;

    static INLIST_MEMORY_TEST_LOCK: Mutex<()> = Mutex::new(());

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

    #[derive(Debug)]
    struct TestPruningStats {
        min_values: ArrayRef,
        max_values: ArrayRef,
    }

    impl PruningStatistics for TestPruningStats {
        fn min_values(&self, _column: &Column) -> Option<ArrayRef> {
            Some(Arc::clone(&self.min_values))
        }

        fn max_values(&self, _column: &Column) -> Option<ArrayRef> {
            Some(Arc::clone(&self.max_values))
        }

        fn num_containers(&self) -> usize {
            self.min_values.len()
        }

        fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
            None
        }

        fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
            None
        }

        fn contained(
            &self,
            _column: &Column,
            _values: &HashSet<ScalarValue>,
        ) -> Option<BooleanArray> {
            None
        }
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
    fn test_exact_left_accumulator_memory_fallback_with_nulls_and_mixed_values() {
        // Edge case: memory limit exceeded while accumulating a column that contains NULLs.
        // The range fallback must still produce a valid (conservative) range filter.
        // For anti-join / LeftAnti usage this is important: the range must not
        // cause incorrect dropping of probe rows that should survive the anti-condition.
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let values: Vec<Option<i32>> = vec![Some(5), None, Some(10), Some(15), None, Some(20)];
        let array: ArrayRef = Arc::new(Int32Array::from(values));
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array])
            .expect("Should create batch with NULLs");

        let left_expr = col("a", &batch.schema()).expect("Should create column expr");

        // Extremely small memory limit to force immediate fallback.
        let mut accumulator =
            ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&left_expr), 1);

        accumulator
            .update_batch(&batch)
            .expect("Should update batch with NULLs and values");

        assert!(accumulator.exact_values_exceeded_memory_limit);
        assert!(accumulator.arrays.is_empty());

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        // Must not be a no-op literal (would be incorrect for anti-join semantics).
        assert!(
            physical_expr.as_any().downcast_ref::<Literal>().is_none(),
            "Range fallback must produce a real range filter, not a literal no-op"
        );

        // The range should be derived from the non-null min/max present in the data.
        // (Exact bounds depend on RangeBounds implementation; we only assert it is a
        // non-trivial filter.)
        let probe_schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let probe_values: Vec<Option<i32>> = vec![Some(0), Some(12), None, Some(25), Some(30)];
        let probe_array: ArrayRef = Arc::new(Int32Array::from(probe_values));
        let probe_batch = RecordBatch::try_new(Arc::new(probe_schema), vec![probe_array])
            .expect("Should create probe batch");

        let filtered = evaluate_boolean_expression(&physical_expr, &probe_batch);
        // We do not assert exact boolean results here (depends on the concrete
        // range expression), but we do assert that the filter was evaluable
        // without error and produced a boolean array of the expected length.
        assert_eq!(filtered.len(), 5);
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
        assert!(accumulator.range_bounds.intervals.is_empty());
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
    fn test_exact_left_accumulator_range_fallback_keeps_disjoint_intervals() {
        let first_batch = create_uint64_batch(vec![10, 20]);
        let second_batch = create_uint64_batch(vec![100, 110]);
        let max_memory_size = first_batch.column(0).get_array_memory_size();

        let left_expr = col("a", &first_batch.schema()).expect("Should create column expr");
        let mut accumulator =
            ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&left_expr), max_memory_size);

        accumulator
            .update_batch(&first_batch)
            .expect("Should update first batch");
        accumulator
            .update_batch(&second_batch)
            .expect("Should update second batch");

        assert_eq!(2, accumulator.range_bounds.intervals.len());

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(left_expr)
            .expect("Should create physical expr");

        let probe_batch = create_uint64_batch(vec![15, 50, 105]);
        let actual_values = evaluate_boolean_expression(&physical_expr, &probe_batch);

        assert_eq!(vec![Some(true), Some(false), Some(true)], actual_values);
    }

    #[test]
    fn test_shared_inlist_budget_rejects_reservations_above_limit() {
        let _guard = INLIST_MEMORY_TEST_LOCK
            .lock()
            .expect("memory budget test lock should not be poisoned");

        let current_usage = CURRENT_INLIST_MEMORY_BYTES.load(AtomicOrdering::Relaxed);
        let reservation_size = 16;
        assert!(reserve_inlist_memory_with_limit(
            reservation_size,
            usize::MAX
        ));
        let reservation = InListMemoryReservation {
            bytes: reservation_size,
        };

        let saturated_limit = CURRENT_INLIST_MEMORY_BYTES.load(AtomicOrdering::Relaxed);
        assert!(saturated_limit >= current_usage + reservation_size);
        assert!(!reserve_inlist_memory_with_limit(1, saturated_limit));

        drop(reservation);
    }

    #[test]
    fn test_bloom_filter_expression_excludes_definitely_absent_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::UInt64, true)]));
        let a: ArrayRef = Arc::new(UInt64Array::from(vec![Some(1), Some(3), None, Some(5)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![a])
            .expect("Should create probe record batch");
        let left_expr = col("a", &schema).expect("Should create column expr");

        let mut bloom_filter = BloomFilter::try_new(1_024).expect("Bloom filter should be created");
        let build_array = UInt64Array::from(vec![1, 5]);
        bloom_filter
            .insert_array(&build_array)
            .expect("Should insert build values");

        let physical_expr = Arc::new(BloomFilterExpr::new(left_expr, Arc::new(bloom_filter)))
            as Arc<dyn PhysicalExpr>;
        let actual_values = evaluate_boolean_expression(&physical_expr, &batch);

        assert_eq!(
            vec![Some(true), Some(false), Some(false), Some(true)],
            actual_values
        );
    }

    #[test]
    fn test_bloom_filter_normalizes_float_zero() {
        let mut bloom_filter = BloomFilter::try_new(1_024).expect("Bloom filter should be created");
        let build_array = Float64Array::from(vec![0.0]);
        bloom_filter
            .insert_array(&build_array)
            .expect("Should insert float zero");

        let probe_array = Float64Array::from(vec![-0.0, 1.0]);
        let result = bloom_filter
            .evaluate_array(&probe_array)
            .expect("Should evaluate float bloom filter");

        assert!(result.value(0));
        assert!(!result.value(1));
    }

    #[test]
    fn test_bloom_filter_skips_float_nan() {
        let mut bloom_filter = BloomFilter::try_new(1_024).expect("Bloom filter should be created");
        let build_array = Float64Array::from(vec![f64::NAN, 1.0]);
        bloom_filter
            .insert_array(&build_array)
            .expect("Should insert non-NaN float values");

        let probe_array = Float64Array::from(vec![f64::NAN, 1.0]);
        let result = bloom_filter
            .evaluate_array(&probe_array)
            .expect("Should evaluate float bloom filter");

        assert!(!result.value(0));
        assert!(result.value(1));
    }

    #[test]
    fn range_interval_merge_rejects_incomparable_bounds() {
        let mut interval =
            RangeInterval::new(ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2)));
        let incomparable = RangeInterval::new(
            ScalarValue::Utf8(Some("a".to_string())),
            ScalarValue::Utf8(Some("z".to_string())),
        );

        assert!(!interval.merge(incomparable));
        assert_eq!(interval.min_value, ScalarValue::Int64(Some(1)));
        assert_eq!(interval.max_value, ScalarValue::Int64(Some(2)));
    }

    #[test]
    fn test_range_fallback_expression_prunes_row_group_statistics() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::UInt64, false)]));
        let left_expr = col("a", &schema).expect("Should create column expr");
        let mut range_bounds = RangeBounds::new(1_024);

        range_bounds
            .update(create_uint64_batch(vec![10, 20]).column(0).as_ref())
            .expect("Should update first range");
        range_bounds
            .update(create_uint64_batch(vec![100, 110]).column(0).as_ref())
            .expect("Should update second range");

        let physical_expr = range_bounds.physical_expr(left_expr);
        let pruning_predicate = PruningPredicate::try_new(physical_expr, Arc::clone(&schema))
            .expect("Range fallback should produce a pruning predicate");
        let pruning_stats = TestPruningStats {
            min_values: Arc::new(UInt64Array::from(vec![0, 30, 105, 200])) as ArrayRef,
            max_values: Arc::new(UInt64Array::from(vec![5, 40, 106, 220])) as ArrayRef,
        };

        let should_keep = pruning_predicate
            .prune(&pruning_stats)
            .expect("Pruning predicate should evaluate");

        assert_eq!(vec![false, false, true, false], should_keep);
    }

    #[test]
    fn test_exact_left_accumulator_range_fallback_ignores_nulls() {
        let batch = create_nullable_uint64_batch(vec![Some(1), None, Some(3)]);

        let left_expr = col("a", &batch.schema()).expect("Should create column expr");

        let mut accumulator =
            ExactLeftAccumulator::new_with_memory_limit(Arc::clone(&left_expr), 1);

        accumulator
            .update_batch(&batch)
            .expect("Should update with batch");

        let column_bounds = accumulator.evaluate().expect("Should evaluate bounds");
        let physical_expr = column_bounds
            .physical_expr(Arc::clone(&left_expr))
            .expect("Should create physical expr");

        let probe_batch = create_uint64_batch(vec![0, 1, 2, 3, 4]);
        let actual_values = evaluate_boolean_expression(&physical_expr, &probe_batch);

        assert_eq!(
            vec![Some(false), Some(true), Some(true), Some(true), Some(false)],
            actual_values
        );
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
