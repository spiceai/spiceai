/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Conversion utilities between `DataFusion` and Vortex statistics.
//!
//! Provides serialization/deserialization of Vortex [`FileStatistics`]
//! (flatbuffer blobs persisted in the metastore) and conversion between
//! `DataFusion` [`ColumnStatistics`] and Vortex [`StatsSet`].
//!
//! Only three per-column statistics round-trip: **min**, **max**, and
//! **null count** (each carrying exact/inexact precision); `sum_value`,
//! `distinct_count`, and `byte_size` are always `Precision::Absent` on the
//! `DataFusion` side. Scalar conversion covers booleans, all integer widths,
//! `Float32`/`Float64`, UTF-8 strings, dates, and timestamps; other scalar
//! types (e.g. decimals, binary) are skipped rather than converted lossily.
//! Cross-write aggregation goes through `merge_serialized_stats`, which
//! merges per-column stats commutatively (min/max widen, null counts add).

use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::Schema;
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
use vortex::VortexSessionDefault;
use vortex::array::stats::StatsSet;
use vortex::dtype::arrow::FromArrowType;
use vortex::dtype::{DType, Nullability};
use vortex::error::VortexResult;
use vortex::expr::stats::{Precision as VortexPrecision, Stat};
use vortex::file::FileStatistics;
use vortex::flatbuffers::WriteFlatBufferExt;
use vortex::scalar::Scalar;

/// Convert a `DataFusion` [`ScalarValue`] to a Vortex [`vortex::scalar::ScalarValue`].
///
/// Returns `None` for null values or unsupported types.
fn df_scalar_to_vortex(sv: &ScalarValue) -> Option<vortex::scalar::ScalarValue> {
    let vortex_scalar: Scalar = match sv {
        ScalarValue::Boolean(Some(v)) => (*v).into(),
        ScalarValue::Int8(Some(v)) => (*v).into(),
        ScalarValue::Int16(Some(v)) => (*v).into(),
        ScalarValue::Int32(Some(v)) => (*v).into(),
        ScalarValue::Int64(Some(v)) => (*v).into(),
        ScalarValue::UInt8(Some(v)) => (*v).into(),
        ScalarValue::UInt16(Some(v)) => (*v).into(),
        ScalarValue::UInt32(Some(v)) => (*v).into(),
        ScalarValue::UInt64(Some(v)) => (*v).into(),
        ScalarValue::Float32(Some(v)) => (*v).into(),
        ScalarValue::Float64(Some(v)) => (*v).into(),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => v.as_str().into(),
        ScalarValue::Date32(Some(v)) => {
            let dtype = DType::from_arrow((&sv.data_type(), Nullability::Nullable));
            Scalar::try_new(dtype, Some(vortex::scalar::ScalarValue::from(*v))).ok()?
        }
        ScalarValue::Date64(Some(v))
        | ScalarValue::TimestampSecond(Some(v), _)
        | ScalarValue::TimestampMillisecond(Some(v), _)
        | ScalarValue::TimestampMicrosecond(Some(v), _)
        | ScalarValue::TimestampNanosecond(Some(v), _) => {
            let dtype = DType::from_arrow((&sv.data_type(), Nullability::Nullable));
            Scalar::try_new(dtype, Some(vortex::scalar::ScalarValue::from(*v))).ok()?
        }
        _ => return None,
    };
    vortex_scalar.into_value()
}

/// Convert a Vortex stat scalar value to a `DataFusion` [`ScalarValue`].
///
/// Uses the Vortex [`Scalar`] type to perform the conversion via the `DType`.
fn vortex_stat_to_df(
    sv: &vortex::scalar::ScalarValue,
    stat: Stat,
    col_dtype: &DType,
) -> Option<ScalarValue> {
    let stat_dtype = stat.dtype(col_dtype)?;
    let scalar = Scalar::try_new(stat_dtype, Some(sv.clone())).ok()?;
    scalar_to_df(&scalar)
}

/// Convert a Vortex [`Scalar`] to a `DataFusion` [`ScalarValue`].
fn scalar_to_df(scalar: &Scalar) -> Option<ScalarValue> {
    match scalar.dtype() {
        DType::Bool(_) => {
            let v: bool = scalar.try_into().ok()?;
            Some(ScalarValue::Boolean(Some(v)))
        }
        DType::Primitive(ptype, _) => {
            use vortex::dtype::PType;
            match ptype {
                PType::I8 => Some(ScalarValue::Int8(Some(scalar.try_into().ok()?))),
                PType::I16 => Some(ScalarValue::Int16(Some(scalar.try_into().ok()?))),
                PType::I32 => Some(ScalarValue::Int32(Some(scalar.try_into().ok()?))),
                PType::I64 => Some(ScalarValue::Int64(Some(scalar.try_into().ok()?))),
                PType::U8 => Some(ScalarValue::UInt8(Some(scalar.try_into().ok()?))),
                PType::U16 => Some(ScalarValue::UInt16(Some(scalar.try_into().ok()?))),
                PType::U32 => Some(ScalarValue::UInt32(Some(scalar.try_into().ok()?))),
                PType::U64 => Some(ScalarValue::UInt64(Some(scalar.try_into().ok()?))),
                PType::F16 => None, // DataFusion Float16 support is limited
                PType::F32 => Some(ScalarValue::Float32(Some(scalar.try_into().ok()?))),
                PType::F64 => Some(ScalarValue::Float64(Some(scalar.try_into().ok()?))),
            }
        }
        DType::Utf8(_) => {
            let v: String = scalar.try_into().ok()?;
            Some(ScalarValue::Utf8(Some(v)))
        }
        DType::Extension(_) => {
            // Temporal types (Date/Time/Timestamp) are represented as Vortex
            // extension types. Round-trip through Arrow so DataFusion
            // `ScalarValue` gets the correct logical type (preserving time
            // unit / time zone).
            let datum = Arc::<dyn arrow::array::Datum>::try_from(scalar).ok()?;
            let (array, _is_scalar) = datum.get();
            ScalarValue::try_from_array(array, 0).ok()
        }
        _ => None,
    }
}

/// Convert a Vortex [`VortexPrecision`] to a `DataFusion` [`Precision`].
fn vortex_precision_to_df<T: Debug + Clone + PartialEq + Eq + PartialOrd>(
    p: VortexPrecision<T>,
) -> Precision<T> {
    match p {
        VortexPrecision::Exact(v) => Precision::Exact(v),
        VortexPrecision::Inexact(v) => Precision::Inexact(v),
        VortexPrecision::Absent => Precision::Absent,
    }
}

/// Build a Vortex [`StatsSet`] from a `DataFusion` [`ColumnStatistics`].
///
/// Converts min/max/`null_count` from `DataFusion` precision types to Vortex stats.
pub(crate) fn column_stats_to_stats_set(cs: &ColumnStatistics) -> StatsSet {
    let mut stats = StatsSet::default();

    if let Some(sv) = cs.min_value.get_value()
        && let Some(vortex_sv) = df_scalar_to_vortex(sv)
    {
        let precision = if cs.min_value.is_exact().is_some() {
            VortexPrecision::Exact(vortex_sv)
        } else {
            VortexPrecision::Inexact(vortex_sv)
        };
        stats.set(Stat::Min, precision);
    }

    if let Some(sv) = cs.max_value.get_value()
        && let Some(vortex_sv) = df_scalar_to_vortex(sv)
    {
        let precision = if cs.max_value.is_exact().is_some() {
            VortexPrecision::Exact(vortex_sv)
        } else {
            VortexPrecision::Inexact(vortex_sv)
        };
        stats.set(Stat::Max, precision);
    }

    if let Some(count) = cs.null_count.get_value() {
        // `usize -> u64` is lossless on all currently supported targets
        // (cayenne requires \u2265 64-bit pointers per project policy), but use
        // `try_from` so that a future >64-bit pointer width either succeeds
        // exactly or skips persisting the stat rather than silently
        // truncating to a wrong value.
        let Ok(count_u64) = u64::try_from(*count) else {
            tracing::warn!(
                "column_stats_to_stats_set: null_count {} exceeds u64::MAX; skipping stat",
                count,
            );
            return stats;
        };
        let vortex_sv = vortex::scalar::ScalarValue::from(count_u64);
        if cs.null_count.is_exact().is_some() {
            stats.set(Stat::NullCount, VortexPrecision::Exact(vortex_sv));
        } else {
            stats.set(Stat::NullCount, VortexPrecision::Inexact(vortex_sv));
        }
    }

    stats
}

/// Convert a Vortex [`StatsSet`] and column [`DType`] to `DataFusion` [`ColumnStatistics`].
pub(crate) fn stats_set_to_column_stats(stats: &StatsSet, dtype: &DType) -> ColumnStatistics {
    let min_value = vortex_precision_to_df(
        stats
            .get(Stat::Min)
            .and_then(|v| vortex_stat_to_df(&v, Stat::Min, dtype)),
    );

    let max_value = vortex_precision_to_df(
        stats
            .get(Stat::Max)
            .and_then(|v| vortex_stat_to_df(&v, Stat::Max, dtype)),
    );

    let null_count = vortex_precision_to_df(
        stats
            .get_as::<u64>(Stat::NullCount, &vortex::dtype::PType::U64.into())
            .and_then(|count| usize::try_from(count).ok()),
    );

    ColumnStatistics {
        null_count,
        max_value,
        min_value,
        sum_value: Precision::Absent,
        distinct_count: Precision::Absent,
        byte_size: Precision::Absent,
    }
}

/// Convert a Vortex [`FileStatistics`] to `DataFusion` [`Statistics`].
///
/// Maps per-column Vortex stats to `DataFusion` column statistics and uses the
/// caller-provided `num_rows` as the total row count.
///
/// `num_rows` must be the exact total row count for the file represented by
/// `file_stats`. Negative values (which can occur if an upstream writer failed
/// to track row counts correctly) are reported as `Precision::Absent` rather
/// than silently wrapped into a bogus `usize`.
#[must_use]
pub fn file_statistics_to_df(file_stats: &FileStatistics, num_rows: i64) -> Statistics {
    let column_statistics: Vec<ColumnStatistics> = file_stats
        .into_iter()
        .map(|(stats, dtype)| stats_set_to_column_stats(stats, dtype))
        .collect();

    let num_rows = usize::try_from(num_rows).map_or(Precision::Absent, Precision::Exact);

    Statistics {
        num_rows,
        total_byte_size: Precision::Absent,
        column_statistics,
    }
}

/// Serialize `DataFusion` scan statistics to a persisted Vortex blob.
///
/// Returns `None` when any column cannot be converted or serialization fails.
pub(crate) fn statistics_to_persisted_blob(stats: &Statistics, schema: &Schema) -> Option<Vec<u8>> {
    if stats.column_statistics.len() != schema.fields().len() {
        return None;
    }
    let column_stats: Vec<StatsSet> = stats
        .column_statistics
        .iter()
        .map(column_stats_to_stats_set)
        .collect();
    let file_stats = build_file_statistics(column_stats, schema);
    serialize_file_statistics(&file_stats).ok()
}

/// Restore `DataFusion` scan statistics from a persisted Vortex blob.
pub(crate) fn statistics_from_persisted_blob(
    blob: &[u8],
    schema: &Schema,
    num_rows: i64,
) -> Option<Arc<Statistics>> {
    let file_stats = deserialize_file_statistics(blob, schema).ok()?;
    Some(Arc::new(file_statistics_to_df(&file_stats, num_rows)))
}

/// Serialize a Vortex [`FileStatistics`] to bytes.
pub(crate) fn serialize_file_statistics(stats: &FileStatistics) -> VortexResult<Vec<u8>> {
    let fb = stats.write_flatbuffer_bytes()?;
    Ok(fb.as_slice().to_vec())
}

/// Deserialize a Vortex [`FileStatistics`] from bytes.
///
/// The `schema` is used to derive Vortex [`DType`]s for proper scalar deserialization.
///
/// # Errors
///
/// Returns an error if the flatbuffer bytes are malformed or do not match the
/// expected schema.
pub fn deserialize_file_statistics(bytes: &[u8], schema: &Schema) -> VortexResult<FileStatistics> {
    let struct_dtype = vortex_struct_dtype_from_schema(schema);
    let fb_stats = flatbuffers::root::<vortex::flatbuffers::footer::FileStatistics>(bytes)?;
    FileStatistics::from_flatbuffer(
        &fb_stats,
        &struct_dtype,
        &vortex_session::VortexSession::default(),
    )
}

/// Convert an Arrow [`Schema`] to a Vortex struct [`DType`].
pub(crate) fn vortex_struct_dtype_from_schema(schema: &Schema) -> DType {
    DType::from_arrow(schema)
}

/// Build a [`FileStatistics`] from per-column [`StatsSet`] entries and the table schema.
pub(crate) fn build_file_statistics(
    column_stats: Vec<StatsSet>,
    schema: &Schema,
) -> FileStatistics {
    let struct_dtype = vortex_struct_dtype_from_schema(schema);
    FileStatistics::new_with_dtype(Arc::from(column_stats.into_boxed_slice()), &struct_dtype)
}

/// Merge an existing serialized [`FileStatistics`] blob with new per-column
/// [`StatsSet`]s and return the merged, serialized blob.
///
/// Uses Vortex's commutative `merge_unordered` per column so the caller does
/// not need to worry about ordering. If the existing blob cannot be
/// deserialized, or if the column counts do not match, `None` is returned so
/// callers can fall back to writing the new stats alone.
///
/// This preserves data correctness across multi-write sequences: once a row's
/// min/max/null-count has been incorporated it stays incorporated, and the
/// merged `num_rows` reflects the full table when `existing_num_rows` is
/// passed in correctly by the caller.
pub(crate) fn merge_serialized_stats(
    existing_blob: &[u8],
    new_column_stats: &[StatsSet],
    dtypes: &[DType],
    schema: &Schema,
) -> Option<Vec<u8>> {
    if new_column_stats.len() != dtypes.len() {
        tracing::warn!(
            "merge_serialized_stats: new_column_stats len {} != dtypes len {}",
            new_column_stats.len(),
            dtypes.len(),
        );
        return None;
    }

    let existing = match deserialize_file_statistics(existing_blob, schema) {
        Ok(fs) => fs,
        Err(e) => {
            tracing::warn!(
                "merge_serialized_stats: failed to deserialize existing stats blob: {e}"
            );
            return None;
        }
    };

    let existing_sets: Vec<StatsSet> = existing.into_iter().map(|(set, _)| set.clone()).collect();

    if existing_sets.len() != new_column_stats.len() {
        tracing::warn!(
            "merge_serialized_stats: existing column count {} != new count {}; \
             skipping merge (schema may have changed)",
            existing_sets.len(),
            new_column_stats.len(),
        );
        return None;
    }

    let merged: Vec<StatsSet> = existing_sets
        .into_iter()
        .zip(new_column_stats.iter())
        .zip(dtypes.iter())
        .map(|((existing, new), dtype)| existing.merge_unordered(new, dtype))
        .collect();

    let file_stats = build_file_statistics(merged, schema);
    match serialize_file_statistics(&file_stats) {
        Ok(bytes) => Some(bytes),
        Err(e) => {
            tracing::warn!("merge_serialized_stats: failed to serialize merged stats: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{ColumnStatistics, ScalarValue, stats::Precision as DfPrecision};
    use std::sync::Arc;

    #[test]
    fn utf8_min_max_roundtrip_through_file_statistics() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let cs = ColumnStatistics {
            null_count: DfPrecision::Exact(0),
            min_value: DfPrecision::Exact(ScalarValue::Utf8(Some("apple".into()))),
            max_value: DfPrecision::Exact(ScalarValue::Utf8(Some("cherry".into()))),
            sum_value: DfPrecision::Absent,
            distinct_count: DfPrecision::Absent,
            byte_size: DfPrecision::Absent,
        };
        let set = column_stats_to_stats_set(&cs);
        // Pre-serialize sanity: StatsSet has Utf8 min/max.
        assert!(
            matches!(
                set.get(Stat::Min),
                VortexPrecision::Exact(_) | VortexPrecision::Inexact(_)
            ),
            "min present in StatsSet"
        );
        assert!(
            matches!(
                set.get(Stat::Max),
                VortexPrecision::Exact(_) | VortexPrecision::Inexact(_)
            ),
            "max present in StatsSet"
        );

        let file_stats = build_file_statistics(vec![set], &schema);
        let bytes = serialize_file_statistics(&file_stats).expect("serialize ok");
        let rt = deserialize_file_statistics(&bytes, &schema).expect("deserialize ok");

        let df = file_statistics_to_df(&rt, 3);
        let col = &df.column_statistics[0];
        assert_eq!(col.null_count, DfPrecision::Exact(0));
        assert_eq!(
            col.min_value,
            DfPrecision::Exact(ScalarValue::Utf8(Some("apple".into())))
        );
        assert_eq!(
            col.max_value,
            DfPrecision::Exact(ScalarValue::Utf8(Some("cherry".into())))
        );
    }

    #[test]
    fn serialized_statistics_merge_preserves_cross_write_min_max() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let first_stats = ColumnStatistics {
            null_count: DfPrecision::Exact(1),
            min_value: DfPrecision::Exact(ScalarValue::Int64(Some(10))),
            max_value: DfPrecision::Exact(ScalarValue::Int64(Some(20))),
            sum_value: DfPrecision::Absent,
            distinct_count: DfPrecision::Absent,
            byte_size: DfPrecision::Absent,
        };
        let second_stats = ColumnStatistics {
            null_count: DfPrecision::Exact(2),
            min_value: DfPrecision::Exact(ScalarValue::Int64(Some(1))),
            max_value: DfPrecision::Exact(ScalarValue::Int64(Some(30))),
            sum_value: DfPrecision::Absent,
            distinct_count: DfPrecision::Absent,
            byte_size: DfPrecision::Absent,
        };
        let first_set = column_stats_to_stats_set(&first_stats);
        let second_set = column_stats_to_stats_set(&second_stats);
        let first_file_stats = build_file_statistics(vec![first_set], &schema);
        let first_blob = serialize_file_statistics(&first_file_stats).expect("serialize ok");
        let dtypes = vec![DType::from_arrow((
            schema.field(0).data_type(),
            Nullability::Nullable,
        ))];

        let merged_blob = merge_serialized_stats(&first_blob, &[second_set], &dtypes, &schema)
            .expect("statistics should merge");
        let merged_stats =
            deserialize_file_statistics(&merged_blob, &schema).expect("deserialize ok");
        let df = file_statistics_to_df(&merged_stats, 6);
        let col = &df.column_statistics[0];

        assert_eq!(df.num_rows, DfPrecision::Exact(6));
        assert_eq!(col.null_count, DfPrecision::Exact(3));
        assert_eq!(
            col.min_value,
            DfPrecision::Exact(ScalarValue::Int64(Some(1)))
        );
        assert_eq!(
            col.max_value,
            DfPrecision::Exact(ScalarValue::Int64(Some(30)))
        );
    }
}
