/*
Copyright 2025-2026 The Spice.ai OSS Authors
Licensed under the Apache License, Version 2.0 (the "License");
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Per-column write-time statistics accumulation for Cayenne.
//!
//! [`ColumnStatsAccumulator`] folds per-column min/max/null-count and NDV
//! sketches across the `RecordBatch`es of a write, producing a serialized Vortex
//! `FileStatistics` blob for metastore persistence. [`RowCountUpdate`] describes
//! how a stats persist adjusts the table's live `num_rows`.

use arrow::array::{
    BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::kernels::aggregate;
use arrow::datatypes::{
    Date32Type, Date64Type, Decimal128Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, TimeUnit};
use datafusion_common::ScalarValue;
use vortex::arrow::FromArrowType;

/// Joint accumulator state held under a single mutex so `update()` and
/// `merge_from()` only pay one acquire per batch. `seeded[i]` is `true`
/// once column i has been assigned its first batch — the first batch is
/// assigned directly (not merged) because `StatsSet::default()` is
/// `merge_unordered`'s identity-less "unknown" and merging into it drops
/// the new stats.
#[derive(Debug, Default)]
struct ColumnStatsState {
    columns: Vec<vortex::array::stats::StatsSet>,
    seeded: Vec<bool>,
    /// Per-column NDV (distinct-count) `HyperLogLog` sketch, `Some` for the
    /// NDV-tracked columns (integers, strings, temporal — see
    /// [`ColumnStatsAccumulator::supports_ndv`]). Parallel to `columns` for O(1)
    /// access on the write hot path. See [`crate::hll`].
    ndv: Vec<Option<crate::hll::HyperLogLog>>,
}

/// How a stats persist updates the live `num_rows` count, keeping it tracking
/// `SELECT COUNT(*)` rather than the sum of every insert ever made.
#[derive(Debug, Clone, Copy)]
pub(crate) enum RowCountUpdate {
    /// Add a signed net delta for this commit (`inserted - superseded - deleted`).
    /// Used by the normal write/CDC-upsert path.
    Delta(i64),
    /// Replace with an authoritative live count. Used by compaction and overwrite
    /// rewrites, which materialize exactly the live rows and so bound any drift
    /// the incremental deltas might accumulate.
    Set(i64),
    /// Leave the count unchanged — rows moved, not added (e.g. the inline-data
    /// checkpoint flush, whose rows were already counted on insert).
    Unchanged,
}

/// Accumulates per-column statistics across multiple `RecordBatch`es during a write.
///
/// Builds Vortex [`StatsSet`] objects per column (min, max, null count) and tracks
/// the total row count. After the write completes, call
/// [`to_file_statistics_blob_with_row_count`] to produce a serialized Vortex
/// `FileStatistics` blob for metastore persistence.
///
/// Thread-safe: guarded by `Mutex` when shared across stream tasks.
///
/// [`StatsSet`]: vortex::array::stats::StatsSet
#[derive(Debug)]
pub(crate) struct ColumnStatsAccumulator {
    state: std::sync::Mutex<ColumnStatsState>,
    /// Column dtypes (Vortex types, derived from Arrow schema)
    dtypes: Vec<vortex::dtype::DType>,
    /// Total accumulated row count across all batches
    row_count: std::sync::atomic::AtomicI64,
    /// Arrow schema for serialization
    schema: arrow_schema::Schema,
}

impl ColumnStatsAccumulator {
    /// Create a new accumulator for the given schema.
    pub(crate) fn new(schema: &arrow_schema::Schema) -> Self {
        let num_cols = schema.fields().len();
        let dtypes: Vec<vortex::dtype::DType> = schema
            .fields()
            .iter()
            .map(|f| {
                vortex::dtype::DType::from_arrow((
                    f.data_type(),
                    if f.is_nullable() {
                        vortex::dtype::Nullability::Nullable
                    } else {
                        vortex::dtype::Nullability::NonNullable
                    },
                ))
            })
            .collect();
        // NDV sketches only for NDV-tracked columns (integers, strings, temporal);
        // other columns get `None` so the write path skips them.
        let ndv: Vec<Option<crate::hll::HyperLogLog>> = schema
            .fields()
            .iter()
            .map(|f| Self::supports_ndv(f.data_type()).then(crate::hll::HyperLogLog::new))
            .collect();
        Self {
            state: std::sync::Mutex::new(ColumnStatsState {
                columns: vec![vortex::array::stats::StatsSet::default(); num_cols],
                seeded: vec![false; num_cols],
                ndv,
            }),
            dtypes,
            row_count: std::sync::atomic::AtomicI64::new(0),
            schema: schema.clone(),
        }
    }

    /// Whether to maintain an NDV sketch for `dt`. Covers the types whose
    /// distinct count is useful for join/group-by sizing and can diverge sharply
    /// from a min/max range:
    /// - integers — join-key candidates (e.g. `*_custkey`, `*_orderkey`) under
    ///   sparse CDC keys;
    /// - strings — group-by / join keys (e.g. `n_name`, `c_state`) where min/max
    ///   carries no cardinality signal;
    /// - temporal (dates, times, timestamps) — low-cardinality group keys (e.g.
    ///   `o_entry_d`, `ol_delivery_d`) whose distinct count is far smaller than
    ///   their min/max range.
    ///
    /// Deliberately excluded: floats, decimals, and booleans — these are
    /// measures, not keys, so an NDV signal would not feed join/group-by sizing.
    /// The set mirrors the integer/string/temporal arms folded in
    /// [`add_column_to_hll`](Self::add_column_to_hll); keep the two in sync.
    ///
    /// The sketch hashes all of these uniformly (`add_hash`), yielding `Inexact`
    /// NDV — exactly what the optimizer's cardinality gate accepts.
    fn supports_ndv(dt: &DataType) -> bool {
        matches!(
            dt,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
        )
    }

    /// Fold every non-null value of an NDV-tracked Arrow column into `hll`.
    /// Iterates the typed array directly (no `ScalarValue` boxing) to keep the
    /// write hot path cheap. Integer and date values sign-extend to `i128` so all
    /// widths share one hash path; strings hash their raw UTF-8 bytes. Types not
    /// listed here (i.e. those for which [`supports_ndv`] returns `false`) are
    /// silently ignored.
    ///
    /// [`supports_ndv`]: Self::supports_ndv
    fn add_column_to_hll(col: &dyn arrow::array::Array, hll: &mut crate::hll::HyperLogLog) {
        macro_rules! fold_int {
            ($array_ty:ty) => {{
                if let Some(a) = col.as_any().downcast_ref::<$array_ty>() {
                    for v in a.iter().flatten() {
                        hll.add_i128(i128::from(v));
                    }
                    return;
                }
            }};
        }
        macro_rules! fold_bytes {
            ($array_ty:ty) => {{
                if let Some(a) = col.as_any().downcast_ref::<$array_ty>() {
                    for v in a.iter().flatten() {
                        hll.add_bytes(v.as_bytes());
                    }
                    return;
                }
            }};
        }
        fold_int!(Int8Array);
        fold_int!(Int16Array);
        fold_int!(Int32Array);
        fold_int!(Int64Array);
        fold_int!(UInt8Array);
        fold_int!(UInt16Array);
        fold_int!(UInt32Array);
        fold_int!(UInt64Array);
        // Temporal types are integer-backed (Date32 = days; Date64/Timestamp =
        // ms/us/ns since epoch; Time32/Time64 = since midnight) — hash them
        // through the same i128 path as integers. The array type is independent
        // of any timezone, so a single downcast per unit covers tz-aware columns.
        fold_int!(Date32Array);
        fold_int!(Date64Array);
        fold_int!(TimestampSecondArray);
        fold_int!(TimestampMillisecondArray);
        fold_int!(TimestampMicrosecondArray);
        fold_int!(TimestampNanosecondArray);
        fold_int!(Time32SecondArray);
        fold_int!(Time32MillisecondArray);
        fold_int!(Time64MicrosecondArray);
        fold_int!(Time64NanosecondArray);
        fold_bytes!(StringArray);
        fold_bytes!(LargeStringArray);
        fold_bytes!(StringViewArray);
    }

    /// Update accumulated stats from a `RecordBatch`.
    pub(crate) fn update(&self, batch: &RecordBatch) {
        let Ok(mut state) = self.state.lock() else {
            tracing::warn!("ColumnStatsAccumulator: mutex poisoned in update(), skipping");
            return;
        };

        let num_rows = batch.num_rows();
        // Use saturating addition at i64::MAX so overflow on extremely long-lived
        // accumulators surfaces as a clamped row count rather than wrapping to
        // a negative value that would get persisted as a bogus `num_rows`.
        let delta = i64::try_from(num_rows).unwrap_or(i64::MAX);
        let _ = self.row_count.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |current| Some(current.saturating_add(delta)),
        );

        for (i, col) in batch.columns().iter().enumerate() {
            if i >= state.columns.len() || i >= self.dtypes.len() || i >= state.seeded.len() {
                continue;
            }

            // Build a StatsSet for this batch's column
            let batch_stats =
                crate::stats::column_stats_to_stats_set(&Self::compute_column_stats(col));

            // For the first batch, seed directly. `StatsSet::default()` is
            // treated by Vortex as "unknown" — and `merge_unordered(unknown,
            // known) == unknown`, which would otherwise silently drop the
            // first batch's stats. On subsequent batches, merge using the
            // commutative unordered merge so statistics stay correct
            // regardless of the order batches arrive in.
            if state.seeded[i] {
                let existing = std::mem::take(&mut state.columns[i]);
                state.columns[i] = existing.merge_unordered(&batch_stats, &self.dtypes[i]);
            } else {
                state.columns[i] = batch_stats;
                state.seeded[i] = true;
            }

            // Maintain the per-column NDV sketch for NDV-tracked columns
            // (integers, strings, temporal).
            if let Some(Some(hll)) = state.ndv.get_mut(i) {
                Self::add_column_to_hll(col.as_ref(), hll);
            }
        }
    }

    /// Compute `DataFusion` `ColumnStatistics` from a single Arrow column.
    pub(crate) fn compute_column_stats(
        col: &dyn arrow::array::Array,
    ) -> datafusion_common::ColumnStatistics {
        use datafusion_common::stats::Precision;

        let null_count = Precision::Exact(col.null_count());

        if col.is_empty() || col.null_count() == col.len() {
            return datafusion_common::ColumnStatistics {
                null_count,
                min_value: Precision::Absent,
                max_value: Precision::Absent,
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            };
        }

        let (batch_min, batch_max) =
            Self::fast_column_min_max(col).unwrap_or_else(|| Self::scalar_column_min_max(col));

        datafusion_common::ColumnStatistics {
            null_count,
            min_value: batch_min.map_or(Precision::Absent, Precision::Exact),
            max_value: batch_max.map_or(Precision::Absent, Precision::Exact),
            // The metadata-only `SUM`/`AVG` fold sources its column sum from the
            // per-file Vortex footer (`Stat::Sum`, surfaced by
            // `VortexFormat::infer_stats` and persisted into the per-file
            // `snapshot_file_statistics` blob), NOT from this table-level
            // accumulator. Computing a sum here would only add a per-write pass
            // for the table-level `TableStatistics` (used for join sizing), which
            // does not consume `sum_value` — so we leave it absent.
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        }
    }

    fn scalar_column_min_max(
        col: &dyn arrow::array::Array,
    ) -> (Option<ScalarValue>, Option<ScalarValue>) {
        // O(n) linear scan to find min/max using `ScalarValue` comparison.
        // NaN values are skipped entirely so stats remain deterministic.
        let mut batch_min: Option<datafusion_common::ScalarValue> = None;
        let mut batch_max: Option<datafusion_common::ScalarValue> = None;

        for row_idx in 0..col.len() {
            if col.is_null(row_idx) {
                continue;
            }
            let Ok(value) = datafusion_common::ScalarValue::try_from_array(col, row_idx) else {
                continue;
            };

            // Skip NaN: partial_cmp(NaN, x) always returns None
            if value.partial_cmp(&value) != Some(std::cmp::Ordering::Equal) {
                continue;
            }

            batch_min = Some(match batch_min {
                None => value.clone(),
                Some(existing) => {
                    if value.partial_cmp(&existing) == Some(std::cmp::Ordering::Less) {
                        value.clone()
                    } else {
                        existing
                    }
                }
            });
            batch_max = Some(match batch_max {
                None => value,
                Some(existing) => {
                    if value.partial_cmp(&existing) == Some(std::cmp::Ordering::Greater) {
                        value
                    } else {
                        existing
                    }
                }
            });
        }

        (batch_min, batch_max)
    }

    fn fast_column_min_max(
        col: &dyn arrow::array::Array,
    ) -> Option<(Option<ScalarValue>, Option<ScalarValue>)> {
        macro_rules! primitive_min_max {
            ($array_ty:ty, $arrow_ty:ty, |$value:ident| $scalar:expr) => {{
                let array = col.as_any().downcast_ref::<$array_ty>()?;
                let min_value = aggregate::min::<$arrow_ty>(array).map(|$value| $scalar);
                let max_value = aggregate::max::<$arrow_ty>(array).map(|$value| $scalar);
                Some((min_value, max_value))
            }};
        }

        macro_rules! byte_min_max {
            ($array_ty:ty, $min_fn:ident, $max_fn:ident, |$value:ident| $scalar:expr) => {{
                let array = col.as_any().downcast_ref::<$array_ty>()?;
                let min_value = aggregate::$min_fn(array).map(|$value| $scalar);
                let max_value = aggregate::$max_fn(array).map(|$value| $scalar);
                Some((min_value, max_value))
            }};
        }

        match col.data_type() {
            DataType::Boolean => {
                let array = col.as_any().downcast_ref::<BooleanArray>()?;
                Some((
                    aggregate::min_boolean(array).map(|value| ScalarValue::Boolean(Some(value))),
                    aggregate::max_boolean(array).map(|value| ScalarValue::Boolean(Some(value))),
                ))
            }
            DataType::Int8 => primitive_min_max!(Int8Array, Int8Type, |value| {
                ScalarValue::Int8(Some(value))
            }),
            DataType::Int16 => primitive_min_max!(Int16Array, Int16Type, |value| {
                ScalarValue::Int16(Some(value))
            }),
            DataType::Int32 => primitive_min_max!(Int32Array, Int32Type, |value| {
                ScalarValue::Int32(Some(value))
            }),
            DataType::Int64 => primitive_min_max!(Int64Array, Int64Type, |value| {
                ScalarValue::Int64(Some(value))
            }),
            DataType::UInt8 => primitive_min_max!(UInt8Array, UInt8Type, |value| {
                ScalarValue::UInt8(Some(value))
            }),
            DataType::UInt16 => primitive_min_max!(UInt16Array, UInt16Type, |value| {
                ScalarValue::UInt16(Some(value))
            }),
            DataType::UInt32 => primitive_min_max!(UInt32Array, UInt32Type, |value| {
                ScalarValue::UInt32(Some(value))
            }),
            DataType::UInt64 => primitive_min_max!(UInt64Array, UInt64Type, |value| {
                ScalarValue::UInt64(Some(value))
            }),
            DataType::Float32 => {
                let array = col.as_any().downcast_ref::<Float32Array>()?;
                let (min_value, max_value) = Self::float32_min_max(array);
                Some((
                    min_value.map(|value| ScalarValue::Float32(Some(value))),
                    max_value.map(|value| ScalarValue::Float32(Some(value))),
                ))
            }
            DataType::Float64 => {
                let array = col.as_any().downcast_ref::<Float64Array>()?;
                let (min_value, max_value) = Self::float64_min_max(array);
                Some((
                    min_value.map(|value| ScalarValue::Float64(Some(value))),
                    max_value.map(|value| ScalarValue::Float64(Some(value))),
                ))
            }
            DataType::Decimal128(precision, scale) => {
                primitive_min_max!(Decimal128Array, Decimal128Type, |value| {
                    ScalarValue::Decimal128(Some(value), *precision, *scale)
                })
            }
            DataType::Utf8 => byte_min_max!(StringArray, min_string, max_string, |value| {
                ScalarValue::Utf8(Some(value.to_string()))
            }),
            DataType::LargeUtf8 => {
                byte_min_max!(LargeStringArray, min_string, max_string, |value| {
                    ScalarValue::LargeUtf8(Some(value.to_string()))
                })
            }
            DataType::Utf8View => {
                byte_min_max!(StringViewArray, min_string_view, max_string_view, |value| {
                    ScalarValue::Utf8View(Some(value.to_string()))
                })
            }
            DataType::Binary => byte_min_max!(BinaryArray, min_binary, max_binary, |value| {
                ScalarValue::Binary(Some(value.to_vec()))
            }),
            DataType::LargeBinary => {
                byte_min_max!(LargeBinaryArray, min_binary, max_binary, |value| {
                    ScalarValue::LargeBinary(Some(value.to_vec()))
                })
            }
            DataType::BinaryView => {
                byte_min_max!(BinaryViewArray, min_binary_view, max_binary_view, |value| {
                    ScalarValue::BinaryView(Some(value.to_vec()))
                })
            }
            DataType::FixedSizeBinary(size) => byte_min_max!(
                FixedSizeBinaryArray,
                min_fixed_size_binary,
                max_fixed_size_binary,
                |value| { ScalarValue::FixedSizeBinary(*size, Some(value.to_vec())) }
            ),
            DataType::Date32 => primitive_min_max!(Date32Array, Date32Type, |value| {
                ScalarValue::Date32(Some(value))
            }),
            DataType::Date64 => primitive_min_max!(Date64Array, Date64Type, |value| {
                ScalarValue::Date64(Some(value))
            }),
            DataType::Time32(TimeUnit::Second) => {
                primitive_min_max!(Time32SecondArray, Time32SecondType, |value| {
                    ScalarValue::Time32Second(Some(value))
                })
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                primitive_min_max!(Time32MillisecondArray, Time32MillisecondType, |value| {
                    ScalarValue::Time32Millisecond(Some(value))
                })
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                primitive_min_max!(Time64MicrosecondArray, Time64MicrosecondType, |value| {
                    ScalarValue::Time64Microsecond(Some(value))
                })
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                primitive_min_max!(Time64NanosecondArray, Time64NanosecondType, |value| {
                    ScalarValue::Time64Nanosecond(Some(value))
                })
            }
            DataType::Timestamp(TimeUnit::Second, tz) => {
                primitive_min_max!(TimestampSecondArray, TimestampSecondType, |value| {
                    ScalarValue::TimestampSecond(Some(value), tz.clone())
                })
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => primitive_min_max!(
                TimestampMillisecondArray,
                TimestampMillisecondType,
                |value| { ScalarValue::TimestampMillisecond(Some(value), tz.clone()) }
            ),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => primitive_min_max!(
                TimestampMicrosecondArray,
                TimestampMicrosecondType,
                |value| { ScalarValue::TimestampMicrosecond(Some(value), tz.clone()) }
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                primitive_min_max!(TimestampNanosecondArray, TimestampNanosecondType, |value| {
                    ScalarValue::TimestampNanosecond(Some(value), tz.clone())
                })
            }
            _ => None,
        }
    }

    fn float32_min_max(array: &Float32Array) -> (Option<f32>, Option<f32>) {
        let mut min_value: Option<f32> = None;
        let mut max_value: Option<f32> = None;

        for value in array.iter().flatten() {
            if value.is_nan() {
                continue;
            }
            min_value = Some(match min_value {
                Some(current) if current <= value => current,
                _ => value,
            });
            max_value = Some(match max_value {
                Some(current) if current >= value => current,
                _ => value,
            });
        }

        (min_value, max_value)
    }

    fn float64_min_max(array: &Float64Array) -> (Option<f64>, Option<f64>) {
        let mut min_value: Option<f64> = None;
        let mut max_value: Option<f64> = None;

        for value in array.iter().flatten() {
            if value.is_nan() {
                continue;
            }
            min_value = Some(match min_value {
                Some(current) if current <= value => current,
                _ => value,
            });
            max_value = Some(match max_value {
                Some(current) if current >= value => current,
                _ => value,
            });
        }

        (min_value, max_value)
    }

    /// Get the total accumulated row count.
    pub(crate) fn row_count(&self) -> i64 {
        self.row_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn merge_from(&self, other: &Self) {
        let other_row_count = other.row_count();
        if other_row_count == 0 {
            return;
        }

        let (other_columns, other_seeded, other_ndv) = {
            let Ok(other_state) = other.state.lock() else {
                tracing::warn!("ColumnStatsAccumulator: mutex poisoned in merge_from(), skipping");
                return;
            };
            (
                other_state.columns.clone(),
                other_state.seeded.clone(),
                other_state.ndv.clone(),
            )
        };

        let Ok(mut state) = self.state.lock() else {
            tracing::warn!("ColumnStatsAccumulator: mutex poisoned in merge_from(), skipping");
            return;
        };

        let _ = self.row_count.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |current| Some(current.saturating_add(other_row_count)),
        );

        for (idx, other_stats) in other_columns.into_iter().enumerate() {
            if idx >= state.columns.len()
                || idx >= state.seeded.len()
                || idx >= self.dtypes.len()
                || !other_seeded.get(idx).copied().unwrap_or(false)
            {
                continue;
            }

            if state.seeded[idx] {
                let existing = std::mem::take(&mut state.columns[idx]);
                state.columns[idx] = existing.merge_unordered(&other_stats, &self.dtypes[idx]);
            } else {
                state.columns[idx] = other_stats;
                state.seeded[idx] = true;
            }
        }

        // Merge per-column NDV sketches (register-wise max).
        for (idx, other_hll) in other_ndv.into_iter().enumerate() {
            let (Some(other_hll), Some(slot)) = (other_hll, state.ndv.get_mut(idx)) else {
                continue;
            };
            match slot {
                Some(hll) => hll.merge(&other_hll),
                None => *slot = Some(other_hll),
            }
        }
    }

    /// Snapshot the accumulated per-column NDV sketches as an [`NdvSketches`]
    /// container (column index -> sketch), for serialization/merge on persist.
    pub(crate) fn to_ndv_sketches(&self) -> crate::hll::NdvSketches {
        let mut sketches = crate::hll::NdvSketches::new();
        let Ok(state) = self.state.lock() else {
            tracing::warn!(
                "ColumnStatsAccumulator: mutex poisoned in to_ndv_sketches(), returning empty"
            );
            return sketches;
        };
        for (idx, slot) in state.ndv.iter().enumerate() {
            if let Some(hll) = slot
                && let Ok(col_idx) = u32::try_from(idx)
            {
                *sketches.entry(col_idx) = hll.clone();
            }
        }
        sketches
    }

    pub(crate) fn to_file_statistics_blob_with_row_count(&self) -> Option<(Vec<u8>, i64)> {
        let row_count = self.row_count();
        if row_count == 0 {
            return None;
        }
        let Ok(state) = self.state.lock() else {
            tracing::warn!(
                "ColumnStatsAccumulator: mutex poisoned in to_file_statistics_blob(), returning None"
            );
            return None;
        };

        let file_stats = crate::stats::build_file_statistics(state.columns.clone(), &self.schema);
        match crate::stats::serialize_file_statistics(&file_stats) {
            Ok(bytes) => Some((bytes, row_count)),
            Err(e) => {
                tracing::warn!("Failed to serialize file statistics: {e}");
                None
            }
        }
    }

    pub(crate) fn merged_file_statistics_blob(&self, existing_blob: &[u8]) -> Option<Vec<u8>> {
        let Ok(state) = self.state.lock() else {
            tracing::warn!(
                "ColumnStatsAccumulator: mutex poisoned in merged_file_statistics_blob(), returning None"
            );
            return None;
        };

        crate::stats::merge_serialized_stats(
            existing_blob,
            &state.columns,
            &self.dtypes,
            &self.schema,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Date32Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, TimeUnit};

    use super::*;

    #[test]
    fn ndv_sketches_cover_integer_string_and_temporal_columns() {
        // Schema mixes NDV-tracked types (int, string, date, timestamp) with an
        // excluded type (float) to confirm the gate and the per-column fold.
        // Timestamp is the column class behind TPC `o_entry_d` / `ol_delivery_d`.
        let schema = arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("d", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("amount", DataType::Float64, true),
        ]);
        let acc = ColumnStatsAccumulator::new(&schema);

        // 100 distinct ids, 4 distinct names (each repeated 25x), 10 distinct
        // dates, 7 distinct timestamps, and floats (which must not get a sketch).
        let ids: Int64Array = (0..100i64).map(Some).collect();
        let names: StringArray = (0..100).map(|i| Some(format!("name-{}", i % 4))).collect();
        let dates: Date32Array = (0..100).map(|i| Some(i % 10)).collect();
        let timestamps: TimestampMicrosecondArray = (0..100).map(|i| Some(i % 7)).collect();
        let amounts: Float64Array = (0..100).map(|i| Some(f64::from(i))).collect();

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(ids),
                Arc::new(names),
                Arc::new(dates),
                Arc::new(timestamps),
                Arc::new(amounts),
            ],
        )
        .expect("batch");
        acc.update(&batch);

        let sketches = acc.to_ndv_sketches();
        // id: ~100 distinct.
        let id_ndv = sketches.estimate(0).expect("id sketch present");
        assert!((90..=110).contains(&id_ndv), "id NDV {id_ndv} not ~100");
        // name: 4 distinct, despite 100 rows.
        let name_ndv = sketches.estimate(1).expect("name sketch present");
        assert!((3..=5).contains(&name_ndv), "name NDV {name_ndv} not ~4");
        // date: 10 distinct.
        let date_ndv = sketches.estimate(2).expect("date sketch present");
        assert!((9..=11).contains(&date_ndv), "date NDV {date_ndv} not ~10");
        // timestamp: 7 distinct.
        let ts_ndv = sketches.estimate(3).expect("timestamp sketch present");
        assert!((6..=8).contains(&ts_ndv), "timestamp NDV {ts_ndv} not ~7");
        // float (amount): excluded — no sketch.
        assert_eq!(
            sketches.estimate(4),
            None,
            "float column must not get an NDV sketch"
        );
    }
}
