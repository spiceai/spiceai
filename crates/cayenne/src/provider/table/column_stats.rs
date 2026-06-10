//! Column statistics accumulation, cached table statistics, and stats persistence.
//!
//! [`ColumnStatsAccumulator`] gathers per-column min/max/null + integer-NDV
//! sketches batch-by-batch on the write path; [`CachedTableStatistics`] is the
//! optimizer-facing cache behind the `table_statistics` `RwLock`.
//! Persistence (`persist_table_stats` and the overwrite/rewrite resets) is
//! serialized by `table_statistics_persistence_lock` so concurrent maintenance
//! tasks cannot merge from the same cached base and lose each other's deltas.
//!
//! Mechanically split out of `provider/table.rs`; code is moved verbatim.

use crate::catalog::MetadataCatalog;
use vortex::dtype::arrow::FromArrowType;

use super::{
    Arc, BinaryArray, BinaryViewArray, BooleanArray, CayenneTableProvider, ColumnStatistics,
    DataType, Date32Array, Date32Type, Date64Array, Date64Type, Decimal128Array, Decimal128Type,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int8Type, Int16Array, Int16Type,
    Int32Array, Int32Type, Int64Array, Int64Type, LargeBinaryArray, LargeStringArray,
    ObjectStoreExt, Ordering, RecordBatch, ScalarValue, Statistics, StringArray, StringViewArray,
    TABLE_STATISTICS_FULL_COLUMN_SYNC_LIMIT, TableMetadata, TableStatistics,
    Time32MillisecondArray, Time32MillisecondType, Time32SecondArray, Time32SecondType,
    Time64MicrosecondArray, Time64MicrosecondType, Time64NanosecondArray, Time64NanosecondType,
    TimeUnit, TimestampMicrosecondArray, TimestampMicrosecondType, TimestampMillisecondArray,
    TimestampMillisecondType, TimestampNanosecondArray, TimestampNanosecondType,
    TimestampSecondArray, TimestampSecondType, UInt8Array, UInt8Type, UInt16Array, UInt16Type,
    UInt32Array, UInt32Type, UInt64Array, UInt64Type, aggregate,
};

/// Joint accumulator state held under a single mutex so `update()` and
/// `merge_from()` only pay one acquire per batch. `seeded[i]` is `true`
/// once column i has been assigned its first batch — the first batch is
/// assigned directly (not merged) because `StatsSet::default()` is
/// `merge_unordered`'s identity-less "unknown" and merging into it drops
/// the new stats.
#[derive(Debug, Default)]
pub(super) struct ColumnStatsState {
    pub(super) columns: Vec<vortex::array::stats::StatsSet>,
    pub(super) seeded: Vec<bool>,
    /// Per-column NDV (distinct-count) `HyperLogLog` sketch, `Some` only for
    /// integer columns (join-key candidates). Parallel to `columns` for O(1)
    /// access on the write hot path. See [`crate::hll`].
    pub(super) ndv: Vec<Option<crate::hll::HyperLogLog>>,
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
/// [`Self::to_file_statistics_blob_with_row_count`] to produce a serialized
/// Vortex `FileStatistics` blob for metastore persistence.
///
/// Thread-safe: guarded by `Mutex` when shared across stream tasks.
///
/// [`StatsSet`]: vortex::array::stats::StatsSet
#[derive(Debug)]
pub(crate) struct ColumnStatsAccumulator {
    pub(super) state: std::sync::Mutex<ColumnStatsState>,
    /// Column dtypes (Vortex types, derived from Arrow schema)
    pub(super) dtypes: Vec<vortex::dtype::DType>,
    /// Total accumulated row count across all batches
    pub(super) row_count: std::sync::atomic::AtomicI64,
    /// Arrow schema for serialization
    pub(super) schema: arrow_schema::Schema,
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
        // NDV sketches only for integer columns (join-key candidates); other
        // columns get `None` so the write path skips them.
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

    /// Whether to maintain an NDV sketch for `dt`. Restricted to integer types:
    /// these are the join-key candidates (e.g. `*_custkey`, `*_orderkey`) whose
    /// distinct count can diverge sharply from their min/max range under sparse
    /// CDC keys. Mirrors the consumer-side `supports_ndv` in the cluster reporter.
    pub(super) fn supports_ndv(dt: &DataType) -> bool {
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
        )
    }

    /// Fold every non-null value of an integer Arrow column into `hll`. Iterates
    /// the typed array directly (no `ScalarValue` boxing) to keep the write hot
    /// path cheap, sign-extending to `i128` so all widths share one hash path.
    pub(super) fn add_int_column_to_hll(
        col: &dyn arrow::array::Array,
        hll: &mut crate::hll::HyperLogLog,
    ) {
        macro_rules! fold {
            ($array_ty:ty) => {{
                if let Some(a) = col.as_any().downcast_ref::<$array_ty>() {
                    for v in a.iter().flatten() {
                        hll.add_i128(i128::from(v));
                    }
                    return;
                }
            }};
        }
        fold!(Int8Array);
        fold!(Int16Array);
        fold!(Int32Array);
        fold!(Int64Array);
        fold!(UInt8Array);
        fold!(UInt16Array);
        fold!(UInt32Array);
        fold!(UInt64Array);
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

            // Maintain the per-column NDV sketch for integer columns.
            if let Some(Some(hll)) = state.ndv.get_mut(i) {
                Self::add_int_column_to_hll(col.as_ref(), hll);
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
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        }
    }

    pub(super) fn scalar_column_min_max(
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

    pub(super) fn fast_column_min_max(
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

    pub(super) fn float32_min_max(array: &Float32Array) -> (Option<f32>, Option<f32>) {
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

    pub(super) fn float64_min_max(array: &Float64Array) -> (Option<f64>, Option<f64>) {
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
    pub(super) fn to_ndv_sketches(&self) -> crate::hll::NdvSketches {
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

    pub(super) fn merged_file_statistics_blob(&self, existing_blob: &[u8]) -> Option<Vec<u8>> {
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

#[derive(Debug, Clone, Default)]
pub(super) struct CachedTableStatistics {
    pub(super) optimizer: Option<Statistics>,
    /// Pre-converted `to_inexact` view of `optimizer`. Populated at every
    /// cache write so the overlay-active scan path skips a per-call
    /// per-column transform. See `cached_table_statistics_wide` bench.
    /// `None` falls back to computing on-the-fly from `optimizer`.
    pub(super) optimizer_inexact: Option<Statistics>,
    /// Raw blob last read from (or written to) the catalog.
    /// Allows `persist_table_stats_locked` to attempt an in-memory merge
    /// and avoid a catalog GET on the common steady-state path.
    pub(super) raw: Option<TableStatistics>,
}

impl CayenneTableProvider {
    pub(super) async fn load_table_statistics(
        catalog: &Arc<dyn MetadataCatalog>,
        table_metadata: &TableMetadata,
    ) -> Option<Statistics> {
        let stats = match catalog.get_table_statistics(&table_metadata.table_id).await {
            Ok(stats) => stats?,
            Err(e) => {
                tracing::warn!(
                    "Failed to load table stats for {}: {e}",
                    table_metadata.table_name
                );
                return None;
            }
        };

        Self::table_statistics_to_df(&table_metadata.schema, &stats).or_else(|| {
            tracing::warn!(
                "Failed to deserialize table stats for {}",
                table_metadata.table_name
            );
            None
        })
    }

    pub(super) fn table_statistics_to_df(
        schema: &arrow_schema::Schema,
        stats: &TableStatistics,
    ) -> Option<Statistics> {
        let file_stats = crate::stats::deserialize_file_statistics(&stats.statistics_blob, schema)
            .map_err(|e| {
                tracing::warn!("Failed to deserialize serialized table statistics: {e}");
                e
            })
            .ok()?;

        let mut df_stats = crate::stats::file_statistics_to_df(&file_stats, stats.num_rows);

        // Overlay per-column NDV estimates from the HyperLogLog sketches as
        // `distinct_count`. The cluster reporter uses this to encode an
        // effective max (`min(true_max, min + ndv)`) that survives `UnionExec`,
        // letting distributed JoinSelection size joins on sparse integer keys.
        if let Some(blob) = stats.ndv_sketches.as_deref()
            && let Some(sketches) = crate::hll::NdvSketches::deserialize(blob)
        {
            for (idx, col) in df_stats.column_statistics.iter_mut().enumerate() {
                if let Ok(col_idx) = u32::try_from(idx)
                    && let Some(ndv) = sketches.estimate(col_idx)
                    && let Ok(ndv) = usize::try_from(ndv)
                {
                    col.distinct_count = datafusion_common::stats::Precision::Inexact(ndv);
                }
            }
        }

        Some(df_stats)
    }

    /// The incrementally-maintained metastore statistics aggregate (live
    /// `num_rows` + per-column min/max + integer NDV via `distinct_count`), for
    /// distributed-join sizing by the cluster executor-statistics reporter.
    ///
    /// Unlike [`TableProvider::statistics`], this is **not** gated off while
    /// position-based deletions are pending — under CDC the aggregate is exactly
    /// what the coordinator needs, and it is always-fresh (kept warm at
    /// construction and on every write commit) and O(1) to read.
    #[must_use]
    pub fn optimizer_table_statistics(&self) -> Option<Statistics> {
        self.cached_table_statistics_for_optimizer()
    }

    pub(super) fn cached_table_statistics_for_optimizer(&self) -> Option<Statistics> {
        // Inline/RAM-tier rows are already reflected in the persisted `num_rows`
        // via `live_rows_delta` (passed to `schedule_post_write_maintenance` from
        // `try_inline_or_restream` and the staged-append path). We must NOT add
        // `inlined_row_count` on top — doing so double-counts the inline rows
        // because both the persisted delta AND the in-memory counter capture them.
        let has_pending_visibility_changes =
            self.has_pending_deletions() || self.inlined_row_count.load(Ordering::Relaxed) > 0;

        let cache = self.table_statistics.read();
        let cached_ref: Option<&Statistics> = if has_pending_visibility_changes {
            cache.optimizer_inexact.as_ref()
        } else {
            cache.optimizer.as_ref()
        };

        if let Some(source) = cached_ref {
            // Wide-table fast path: build the top-level summary directly from a
            // borrowed reference instead of cloning the full column_statistics
            // vector only to discard it. See `cached_table_statistics_wide` bench.
            if source.column_statistics.len() > TABLE_STATISTICS_FULL_COLUMN_SYNC_LIMIT {
                tracing::trace!(
                    table = self.table_metadata.table_name.as_str(),
                    column_count = source.column_statistics.len(),
                    full_column_sync_limit = TABLE_STATISTICS_FULL_COLUMN_SYNC_LIMIT,
                    "Returning top-level table statistics only for wide table"
                );
                return Some(Self::top_level_statistics_only(source, false));
            }
            return Some(source.clone());
        }

        // Cache-miss visibility-overlay path: cache.optimizer_inexact is None,
        // so transform optimizer on-the-fly. Rare — test seed only.
        if has_pending_visibility_changes {
            let Some(optimizer) = cache.optimizer.clone() else {
                drop(cache);
                // No persisted stats at all (pre-first-maintenance window on a
                // new CDC table). `inlined_row_count` is the only cardinality
                // signal available — hand it to the planner as Inexact so a join
                // against an actively-inlining table is sized rather than treated
                // as statistics-less. Safe: no persisted `num_rows` exists yet,
                // so there is nothing to double-count against.
                let inlined = self.inlined_row_count.load(Ordering::Relaxed).max(0);
                let inlined = usize::try_from(inlined).unwrap_or(usize::MAX);
                return (inlined > 0).then(|| Statistics {
                    num_rows: datafusion_common::stats::Precision::Inexact(inlined),
                    total_byte_size: datafusion_common::stats::Precision::Absent,
                    column_statistics: Vec::new(),
                });
            };
            drop(cache);
            let inexact = Self::statistics_to_inexact(optimizer);
            if inexact.column_statistics.len() > TABLE_STATISTICS_FULL_COLUMN_SYNC_LIMIT {
                return Some(Self::top_level_statistics_only(&inexact, false));
            }
            return Some(inexact);
        }

        None
    }

    pub(super) fn top_level_statistics_only(stats: &Statistics, inexact: bool) -> Statistics {
        let num_rows = if inexact {
            stats.num_rows.to_inexact()
        } else {
            stats.num_rows
        };
        let total_byte_size = if inexact {
            stats.total_byte_size.to_inexact()
        } else {
            stats.total_byte_size
        };

        Statistics {
            num_rows,
            total_byte_size,
            column_statistics: Vec::new(),
        }
    }

    pub(super) fn statistics_to_inexact(stats: Statistics) -> Statistics {
        Statistics {
            num_rows: stats.num_rows.to_inexact(),
            total_byte_size: stats.total_byte_size.to_inexact(),
            column_statistics: stats
                .column_statistics
                .into_iter()
                .map(Self::column_statistics_to_inexact)
                .collect(),
        }
    }

    pub(super) fn column_statistics_to_inexact(stats: ColumnStatistics) -> ColumnStatistics {
        ColumnStatistics {
            null_count: stats.null_count.to_inexact(),
            max_value: stats.max_value.to_inexact(),
            min_value: stats.min_value.to_inexact(),
            sum_value: stats.sum_value.to_inexact(),
            distinct_count: stats.distinct_count.to_inexact(),
            byte_size: stats.byte_size.to_inexact(),
        }
    }

    pub(super) fn clear_cached_table_statistics_unlocked(&self) {
        let mut cache = self.table_statistics.write();
        cache.optimizer = None;
        cache.optimizer_inexact = None;
        cache.raw = None;
    }

    pub(super) fn clear_scan_file_statistics_cache(&self) {
        self.scan_file_statistics.clear();
    }
}

impl CayenneTableProvider {
    /// Persist table-level statistics by merging the current write's accumulator
    /// (min/max/null + NDV sketches) into the existing metastore aggregate and
    /// applying `num_rows_update` to the live row count.
    ///
    /// Best-effort: logs a warning and continues if stats persistence fails,
    /// since stats are an optimization and not critical for correctness.
    pub(crate) async fn persist_table_stats(
        &self,
        accumulator: &ColumnStatsAccumulator,
        num_rows_update: RowCountUpdate,
    ) {
        let _stats_persistence_guard = self.table_statistics_persistence_lock.lock().await;
        self.persist_table_stats_locked(accumulator, num_rows_update, false)
            .await;
    }

    /// Replace the aggregate entirely with the overwrite's accumulator and reset
    /// the live count to the rewritten row count (the prior data is gone, so the
    /// old min/max/NDV must not survive — see [`RowCountUpdate`]).
    pub(crate) async fn reset_table_stats_after_overwrite(
        &self,
        accumulator: &ColumnStatsAccumulator,
    ) {
        let _stats_persistence_guard = self.table_statistics_persistence_lock.lock().await;
        self.clear_cached_table_statistics_unlocked();
        let new_rows = accumulator.row_count();
        self.persist_table_stats_locked(accumulator, RowCountUpdate::Set(new_rows), true)
            .await;
    }

    /// Replace the aggregate with a full live-row rewrite (compaction).
    ///
    /// Compaction materializes exactly the live rows, so its accumulator's
    /// min/max + NDV are the authoritative *live* aggregate. Replacing (rather
    /// than merging) resets any superset drift accumulated incrementally — e.g.
    /// min/max widened by since-deleted rows, or an NDV sketch inflated by
    /// superseded keys — back to the live set, and `Set`s the live count.
    pub(crate) async fn replace_table_stats_after_rewrite(
        &self,
        accumulator: &ColumnStatsAccumulator,
    ) {
        let _stats_persistence_guard = self.table_statistics_persistence_lock.lock().await;
        let new_rows = accumulator.row_count();
        self.persist_table_stats_locked(accumulator, RowCountUpdate::Set(new_rows), true)
            .await;
    }

    /// Persist merged/replaced stats.
    ///
    /// `replace_aggregate` true ignores any existing aggregate (overwrite); false
    /// merges this write into it. `num_rows_update` sets the live count relative
    /// to the previous aggregate (`Delta`), to an authoritative value (`Set`), or
    /// leaves it (`Unchanged`).
    pub(super) async fn persist_table_stats_locked(
        &self,
        accumulator: &ColumnStatsAccumulator,
        num_rows_update: RowCountUpdate,
        replace_aggregate: bool,
    ) {
        let Some((new_blob, _new_rows)) = accumulator.to_file_statistics_blob_with_row_count()
        else {
            return;
        };
        let new_ndv = accumulator.to_ndv_sketches();

        // Prefer an in-memory cached raw blob (populated by previous persist or load)
        // to avoid a catalog round-trip on every write. Only hit the catalog when
        // the cache is cold. Skipped entirely when replacing the aggregate.
        let existing_stats = if replace_aggregate {
            None
        } else {
            let cached_raw = {
                let guard = self.table_statistics.read();
                guard.raw.clone()
            };
            if let Some(raw) = cached_raw {
                Some(raw)
            } else {
                match self
                    .catalog
                    .get_table_statistics(&self.table_metadata.table_id)
                    .await
                {
                    Ok(stats) => stats,
                    Err(e) => {
                        tracing::warn!(
                            "Failed to load existing table stats for {} before merge: {e}",
                            self.table_metadata.table_name
                        );
                        None
                    }
                }
            }
        };

        // Merge min/max/null blob and NDV sketches into the existing aggregate.
        let prev_num_rows = existing_stats.as_ref().map_or(0, |e| e.num_rows);
        let statistics_blob = match &existing_stats {
            Some(existing) => accumulator
                .merged_file_statistics_blob(&existing.statistics_blob)
                .unwrap_or_else(|| {
                    tracing::warn!(
                        "Failed to merge table stats for {}; replacing with current write",
                        self.table_metadata.table_name
                    );
                    new_blob.clone()
                }),
            None => new_blob,
        };
        let mut merged_ndv = new_ndv;
        if let Some(existing_ndv) = existing_stats
            .as_ref()
            .and_then(|e| e.ndv_sketches.as_deref())
        {
            merged_ndv.merge_serialized(existing_ndv);
        }
        let ndv_sketches = merged_ndv.serialize();

        // Apply the live-row-count update relative to the previous aggregate.
        let num_rows = match num_rows_update {
            RowCountUpdate::Delta(delta) => prev_num_rows.saturating_add(delta).max(0),
            RowCountUpdate::Set(n) => n.max(0),
            RowCountUpdate::Unchanged => prev_num_rows,
        };

        let stats = TableStatistics {
            table_id: self.table_metadata.table_id.clone(),
            statistics_blob,
            num_rows,
            ndv_sketches,
        };

        if let Err(e) = self.catalog.upsert_table_statistics(&stats).await {
            tracing::warn!(
                "Failed to persist table stats for {}: {e}",
                self.table_metadata.table_name
            );
            return;
        }

        let df_stats = Self::table_statistics_to_df(&self.table_metadata.schema, &stats);
        let df_stats_inexact = df_stats
            .as_ref()
            .map(|s| Self::statistics_to_inexact(s.clone()));
        let mut cache = self.table_statistics.write();
        cache.optimizer = df_stats;
        cache.optimizer_inexact = df_stats_inexact;
        // Keep the raw blob for the next persist to avoid a catalog read.
        cache.raw = Some(stats);
    }
}
