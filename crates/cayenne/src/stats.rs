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
//! Provides serialization/deserialization of Vortex [`FileStatistics`] and
//! conversion between `DataFusion` [`ColumnStatistics`] and Vortex [`StatsSet`].

use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::i256 as ArrowI256;
use arrow_schema::{DataType, Schema};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
use vortex::VortexSessionDefault;
use vortex::array::stats::StatsSet;
use vortex::arrow::{FromArrowType, ToArrowDatum};
use vortex::buffer::ByteBuffer;
use vortex::dtype::{DType, DecimalDType, Nullability, i256 as VortexI256};
use vortex::error::VortexResult;
use vortex::expr::stats::{Precision as VortexPrecision, Stat};
use vortex::file::FileStatistics;
use vortex::flatbuffers::WriteFlatBufferExt;
use vortex::scalar::{DecimalValue, Scalar};

/// Convert a `DataFusion` [`ScalarValue`] to a Vortex [`vortex::scalar::ScalarValue`].
///
/// Returns `None` for null values or unsupported types. Dictionary scalars are
/// unwrapped to the inner value: Vortex has no dictionary dtype, and the bound
/// is the dictionary's value.
fn df_scalar_to_vortex(sv: &ScalarValue) -> Option<vortex::scalar::ScalarValue> {
    if let ScalarValue::Dictionary(_, inner) = sv {
        return df_scalar_to_vortex(inner);
    }
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
        ScalarValue::Float16(Some(v)) => (*v).into(),
        ScalarValue::Float32(Some(v)) => (*v).into(),
        ScalarValue::Float64(Some(v)) => (*v).into(),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => v.as_str().into(),
        // Carry the column's DECLARED precision/scale through rather than
        // deriving one from the value: the reverse conversion rebuilds the
        // DataFusion width from the Arrow schema, and a width that disagrees
        // with the column's would be rejected downstream as a different type.
        ScalarValue::Decimal32(Some(v), precision, scale) => Scalar::decimal(
            DecimalValue::I32(*v),
            DecimalDType::new(*precision, *scale),
            Nullability::Nullable,
        ),
        ScalarValue::Decimal64(Some(v), precision, scale) => Scalar::decimal(
            DecimalValue::I64(*v),
            DecimalDType::new(*precision, *scale),
            Nullability::Nullable,
        ),
        ScalarValue::Decimal128(Some(v), precision, scale) => Scalar::decimal(
            DecimalValue::I128(*v),
            DecimalDType::new(*precision, *scale),
            Nullability::Nullable,
        ),
        ScalarValue::Decimal256(Some(v), precision, scale) => Scalar::decimal(
            DecimalValue::I256(VortexI256::from_le_bytes(v.to_le_bytes())),
            DecimalDType::new(*precision, *scale),
            Nullability::Nullable,
        ),
        // Vortex has one binary dtype, so the offset width is not preserved
        // in the blob. Restore tags the bound from the Arrow schema.
        ScalarValue::Binary(Some(v))
        | ScalarValue::LargeBinary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::FixedSizeBinary(_, Some(v)) => {
            Scalar::binary(ByteBuffer::from(v.clone()), Nullability::Nullable)
        }
        ScalarValue::Date32(Some(v))
        | ScalarValue::Time32Second(Some(v))
        | ScalarValue::Time32Millisecond(Some(v)) => {
            let dtype = DType::from_arrow((&sv.data_type(), Nullability::Nullable));
            Scalar::try_new(dtype, Some(vortex::scalar::ScalarValue::from(*v))).ok()?
        }
        ScalarValue::Date64(Some(v))
        | ScalarValue::Time64Microsecond(Some(v))
        | ScalarValue::Time64Nanosecond(Some(v))
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
/// Min/max are then tagged with `column_type` so a bound collapsed by Vortex
/// (one Utf8, one Binary, one Decimal) comes back as the column's Arrow type.
fn vortex_stat_to_df(
    sv: &vortex::scalar::ScalarValue,
    stat: Stat,
    col_dtype: &DType,
    column_type: Option<&DataType>,
) -> Option<ScalarValue> {
    let stat_dtype = stat.dtype(col_dtype)?;
    let scalar = Scalar::try_new(stat_dtype, Some(sv.clone())).ok()?;
    let df = scalar_to_df(&scalar)?;
    match (stat, column_type) {
        (Stat::Min | Stat::Max, Some(arrow_type)) => retag_bound_to_column(df, arrow_type),
        _ => Some(df),
    }
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
                PType::F16 => Some(ScalarValue::Float16(Some(scalar.try_into().ok()?))),
                PType::F32 => Some(ScalarValue::Float32(Some(scalar.try_into().ok()?))),
                PType::F64 => Some(ScalarValue::Float64(Some(scalar.try_into().ok()?))),
            }
        }
        DType::Utf8(_) => {
            let v: String = scalar.try_into().ok()?;
            Some(ScalarValue::Utf8(Some(v)))
        }
        DType::Decimal(decimal_type, _) => {
            // Prefer Decimal128: that is what Spice maps NUMERIC into, and
            // picking the narrowest width that fits the precision would turn a
            // `Decimal128(10, 2)` bound into `Decimal32(10, 2)`. Values that
            // do not fit in i128 (a `Decimal256` column) keep the 256-bit
            // width. Restore then retags from the Arrow schema so an explicit
            // `Decimal32`/`Decimal64`/`Decimal256` column gets its own variant.
            let value = scalar.as_decimal().decimal_value()?;
            let precision = decimal_type.precision();
            let scale = decimal_type.scale();
            // Vortex `SUM` widens precision by 10, capped at 76. A small
            // `Decimal128(38, 2)` sum is still an i128, but precision 48 is
            // not a valid `Decimal128`. Keep values that fit in 38 digits as
            // `Decimal128`; anything wider is `Decimal256`.
            if precision <= 38 {
                if let Some(v128) = value.cast::<i128>() {
                    Some(ScalarValue::Decimal128(Some(v128), precision, scale))
                } else {
                    let v256 = value.cast::<VortexI256>()?;
                    Some(ScalarValue::Decimal256(
                        Some(ArrowI256::from_le_bytes(v256.to_le_bytes())),
                        precision,
                        scale,
                    ))
                }
            } else {
                let v256 = value.cast::<VortexI256>()?;
                Some(ScalarValue::Decimal256(
                    Some(ArrowI256::from_le_bytes(v256.to_le_bytes())),
                    precision,
                    scale,
                ))
            }
        }
        DType::Binary(_) => {
            let bytes = scalar.as_binary().value().cloned()?;
            Some(ScalarValue::Binary(Some(Vec::<u8>::from(
                bytes.into_inner(),
            ))))
        }
        DType::Extension(_) => {
            // Temporal types (Date/Time/Timestamp) are represented as Vortex
            // extension types. Round-trip through Arrow so DataFusion
            // `ScalarValue` gets the correct logical type (preserving time
            // unit / time zone).
            let datum = scalar.to_arrow_datum().ok()?;
            let (array, _is_scalar) = datum.get();
            ScalarValue::try_from_array(array, 0).ok()
        }
        _ => None,
    }
}

/// Tag a restored min/max with the column's Arrow type.
///
/// Vortex collapses several Arrow families to one dtype (Utf8, Binary,
/// Decimal). A bound whose variant disagrees with the column is discarded by
/// pruning and clustering, so restore copies the column's tag onto the same
/// payload. Returns `None` when the value cannot be represented as that type
/// (a `Decimal256` bound that does not fit `Decimal32`).
fn retag_bound_to_column(sv: ScalarValue, column_type: &DataType) -> Option<ScalarValue> {
    if sv.data_type() == *column_type {
        return Some(sv);
    }
    match column_type {
        DataType::Dictionary(key, value) => {
            let inner = retag_bound_to_column(sv, value)?;
            Some(ScalarValue::Dictionary(key.clone(), Box::new(inner)))
        }
        DataType::Utf8 => match sv {
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
                Some(ScalarValue::Utf8(v))
            }
            other => Some(other),
        },
        DataType::LargeUtf8 => match sv {
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
                Some(ScalarValue::LargeUtf8(v))
            }
            other => Some(other),
        },
        DataType::Utf8View => match sv {
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
                Some(ScalarValue::Utf8View(v))
            }
            other => Some(other),
        },
        DataType::Binary => match sv {
            ScalarValue::Binary(v)
            | ScalarValue::LargeBinary(v)
            | ScalarValue::BinaryView(v)
            | ScalarValue::FixedSizeBinary(_, v) => Some(ScalarValue::Binary(v)),
            other => Some(other),
        },
        DataType::LargeBinary => match sv {
            ScalarValue::Binary(v)
            | ScalarValue::LargeBinary(v)
            | ScalarValue::BinaryView(v)
            | ScalarValue::FixedSizeBinary(_, v) => Some(ScalarValue::LargeBinary(v)),
            other => Some(other),
        },
        DataType::BinaryView => match sv {
            ScalarValue::Binary(v)
            | ScalarValue::LargeBinary(v)
            | ScalarValue::BinaryView(v)
            | ScalarValue::FixedSizeBinary(_, v) => Some(ScalarValue::BinaryView(v)),
            other => Some(other),
        },
        DataType::FixedSizeBinary(n) => match sv {
            ScalarValue::Binary(v)
            | ScalarValue::LargeBinary(v)
            | ScalarValue::BinaryView(v)
            | ScalarValue::FixedSizeBinary(_, v) => Some(ScalarValue::FixedSizeBinary(*n, v)),
            other => Some(other),
        },
        DataType::Decimal32(precision, scale) => {
            decimal_bound_as_width(&sv, DecimalWidth::ThirtyTwo, *precision, *scale)
        }
        DataType::Decimal64(precision, scale) => {
            decimal_bound_as_width(&sv, DecimalWidth::SixtyFour, *precision, *scale)
        }
        DataType::Decimal128(precision, scale) => {
            decimal_bound_as_width(&sv, DecimalWidth::OneTwentyEight, *precision, *scale)
        }
        DataType::Decimal256(precision, scale) => {
            decimal_bound_as_width(&sv, DecimalWidth::TwoFiftySix, *precision, *scale)
        }
        _ => Some(sv),
    }
}

#[derive(Clone, Copy)]
enum DecimalWidth {
    ThirtyTwo,
    SixtyFour,
    OneTwentyEight,
    TwoFiftySix,
}

fn decimal_bound_as_i256(sv: &ScalarValue) -> Option<ArrowI256> {
    match sv {
        ScalarValue::Decimal32(Some(v), _, _) => Some(ArrowI256::from_i128(i128::from(*v))),
        ScalarValue::Decimal64(Some(v), _, _) => Some(ArrowI256::from_i128(i128::from(*v))),
        ScalarValue::Decimal128(Some(v), _, _) => Some(ArrowI256::from_i128(*v)),
        ScalarValue::Decimal256(Some(v), _, _) => Some(*v),
        _ => None,
    }
}

fn decimal_scale(sv: &ScalarValue) -> Option<i8> {
    match sv {
        ScalarValue::Decimal32(_, _, scale)
        | ScalarValue::Decimal64(_, _, scale)
        | ScalarValue::Decimal128(_, _, scale)
        | ScalarValue::Decimal256(_, _, scale) => Some(*scale),
        _ => None,
    }
}

/// Multiply (or refuse to divide) an unscaled decimal so it represents the
/// same value at `to_scale`. Schema evolution never shrinks scale, and a
/// shrink would drop fractional digits, so that direction returns `None`.
fn rescale_unscaled_decimal(value: ArrowI256, from_scale: i8, to_scale: i8) -> Option<ArrowI256> {
    if from_scale == to_scale {
        return Some(value);
    }
    let delta = i32::from(to_scale) - i32::from(from_scale);
    if delta < 0 {
        return None;
    }
    let times = u32::try_from(delta).ok()?;
    let ten = ArrowI256::from_i128(10);
    let mut scaled = value;
    for _ in 0..times {
        scaled = scaled.checked_mul(ten)?;
    }
    Some(scaled)
}

fn decimal_bound_as_width(
    sv: &ScalarValue,
    width: DecimalWidth,
    precision: u8,
    scale: i8,
) -> Option<ScalarValue> {
    let value = rescale_unscaled_decimal(decimal_bound_as_i256(sv)?, decimal_scale(sv)?, scale)?;
    match width {
        DecimalWidth::ThirtyTwo => Some(ScalarValue::Decimal32(
            Some(i32::try_from(value.to_i128()?).ok()?),
            precision,
            scale,
        )),
        DecimalWidth::SixtyFour => Some(ScalarValue::Decimal64(
            Some(i64::try_from(value.to_i128()?).ok()?),
            precision,
            scale,
        )),
        DecimalWidth::OneTwentyEight => Some(ScalarValue::Decimal128(
            Some(value.to_i128()?),
            precision,
            scale,
        )),
        DecimalWidth::TwoFiftySix => Some(ScalarValue::Decimal256(Some(value), precision, scale)),
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

    // Persist the column sum so whole-table `SUM`/`AVG` can be answered from
    // metadata. The value is already widened to the sum dtype upstream by
    // `infer_stats` in the Vortex persistent format (via `Stat::Sum.dtype`:
    // signed -> Int64, unsigned -> UInt64, float -> Float64), matching
    // `Sum::return_dtype` so it round-trips through `stats_set_to_column_stats`.
    // Cross-batch/cross-file combination is handled additively by Vortex's
    // `StatsSet::merge_unordered` (`merge_sum`).
    if let Some(sv) = cs.sum_value.get_value()
        && let Some(vortex_sv) = df_scalar_to_vortex(sv)
    {
        let precision = if cs.sum_value.is_exact().is_some() {
            VortexPrecision::Exact(vortex_sv)
        } else {
            VortexPrecision::Inexact(vortex_sv)
        };
        stats.set(Stat::Sum, precision);
    }

    // Persist the column's uncompressed byte size so a scan served from this blob
    // reports the same size the Vortex footer reports. `JoinSelection` compares
    // `total_byte_size` before anything else, so a column that loses its size here
    // changes which side of a join is built (spiceai/spiceai#13829).
    if let Some(size) = cs.byte_size.get_value() {
        // `usize -> u64` is lossless on every supported target; `try_from` keeps a
        // hypothetical wider pointer from truncating to a wrong size, in which case
        // the stat is skipped rather than persisted wrong.
        match u64::try_from(*size) {
            Ok(size_u64) => {
                let vortex_sv = vortex::scalar::ScalarValue::from(size_u64);
                if cs.byte_size.is_exact().is_some() {
                    stats.set(
                        Stat::UncompressedSizeInBytes,
                        VortexPrecision::Exact(vortex_sv),
                    );
                } else {
                    stats.set(
                        Stat::UncompressedSizeInBytes,
                        VortexPrecision::Inexact(vortex_sv),
                    );
                }
            }
            Err(_) => {
                tracing::warn!(
                    "column_stats_to_stats_set: byte_size {} exceeds u64::MAX; skipping stat",
                    size,
                );
            }
        }
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
///
/// `column_type` is the Arrow type of the column these stats describe. Min/max
/// are tagged with it after the Vortex round-trip, which otherwise collapses
/// the utf8, binary, and decimal families.
pub(crate) fn stats_set_to_column_stats(
    stats: &StatsSet,
    dtype: &DType,
    column_type: Option<&DataType>,
) -> ColumnStatistics {
    let min_value = vortex_precision_to_df(
        stats
            .get(Stat::Min)
            .and_then(|v| vortex_stat_to_df(&v, Stat::Min, dtype, column_type)),
    );

    let max_value = vortex_precision_to_df(
        stats
            .get(Stat::Max)
            .and_then(|v| vortex_stat_to_df(&v, Stat::Max, dtype, column_type)),
    );

    let null_count = vortex_precision_to_df(
        stats
            .get_as::<u64>(Stat::NullCount, &vortex::dtype::PType::U64.into())
            .and_then(|count| usize::try_from(count).ok()),
    );

    // `Stat::Sum` is stored in the sum's widened dtype (signed -> I64,
    // unsigned -> U64, float -> F64; see `Sum::return_dtype`), and
    // `vortex_stat_to_df` reconstructs that via `Stat::Sum.dtype(dtype)`. This
    // lets the metadata-only `SUM`/`AVG` fold (`crate::stats_aggregate`) answer
    // whole-table sums without a scan.
    let sum_value = vortex_precision_to_df(
        stats
            .get(Stat::Sum)
            .and_then(|v| vortex_stat_to_df(&v, Stat::Sum, dtype, None)),
    );

    // The uncompressed size the footer reported when this blob was written. A blob
    // written before this stat was persisted has none, which `file_statistics_to_df`
    // reads as a stale blob.
    let byte_size = vortex_precision_to_df(
        stats
            .get_as::<u64>(
                Stat::UncompressedSizeInBytes,
                &vortex::dtype::PType::U64.into(),
            )
            .and_then(|size| usize::try_from(size).ok()),
    );

    ColumnStatistics {
        null_count,
        max_value,
        min_value,
        sum_value,
        distinct_count: Precision::Absent,
        byte_size,
    }
}

/// Convert a Vortex [`FileStatistics`] to `DataFusion` [`Statistics`].
///
/// Maps per-column Vortex stats to `DataFusion` column statistics and uses the
/// caller-provided `num_rows` as the total row count. `schema` tags restored
/// min/max with the column's Arrow type, so Vortex's single `Utf8`, `Binary`,
/// and `Decimal` dtypes come back as `Utf8View` / `Decimal32` / … rather than a
/// collapsed sibling that pruning would discard.
///
/// `num_rows` must be the exact total row count for the file represented by
/// `file_stats`. Negative values (which can occur if an upstream writer failed
/// to track row counts correctly) are reported as `Precision::Absent` rather
/// than silently wrapped into a bogus `usize`.
#[must_use]
pub fn file_statistics_to_df(
    file_stats: &FileStatistics,
    schema: &Schema,
    num_rows: i64,
) -> Statistics {
    let column_statistics: Vec<ColumnStatistics> = file_stats
        .into_iter()
        .enumerate()
        .map(|(idx, (stats, dtype))| {
            let arrow_type = schema.fields().get(idx).map(|field| field.data_type());
            stats_set_to_column_stats(stats, dtype, arrow_type)
        })
        .collect();

    let num_rows = usize::try_from(num_rows).map_or(Precision::Absent, Precision::Exact);

    // Sum the per-column sizes the way the footer path does
    // (`VortexFormat::infer_stats`, whose Vortex `Precision::zip` absorbs `Absent`
    // identically), so the same file reports the same `total_byte_size` whichever
    // source served it. `Precision::add` is absorbing: one column without a size
    // makes the whole total `Absent` and keeps it there. That is deliberate — a
    // blob written before the size was persisted has none on any column, and a
    // total summed from only the columns that happen to carry one would report a
    // file as a fraction of its real size rather than as unknown.
    let total_byte_size = column_statistics
        .iter()
        .fold(Precision::Exact(0_usize), |acc, col| {
            acc.add(&col.byte_size)
        });

    Statistics {
        num_rows,
        total_byte_size,
        column_statistics,
    }
}

/// Serialize `DataFusion` scan statistics to a persisted Vortex blob.
///
/// Returns `None` when any column cannot be converted or serialization fails.
///
/// Public so a test can seed a `cayenne_snapshot_file_statistics` row with a blob
/// of its choosing — in particular one carrying no per-column byte sizes, which is
/// what rows written before those sizes were persisted look like. It is the
/// serializing counterpart of the already-public `deserialize_file_statistics` and
/// `file_statistics_to_df`.
#[must_use]
pub fn statistics_to_persisted_blob(stats: &Statistics, schema: &Schema) -> Option<Vec<u8>> {
    if stats.column_statistics.len() != schema.fields().len() {
        return None;
    }
    let column_stats: Vec<StatsSet> = stats
        .column_statistics
        .iter()
        .zip(schema.fields())
        .map(|(cs, field)| {
            column_stats_to_stats_set(&align_column_stats_to_schema(cs, field.data_type()))
        })
        .collect();
    let file_stats = build_file_statistics(column_stats, schema);
    serialize_file_statistics(&file_stats).ok()
}

/// Copy min/max onto `column_type`, rescaling decimal unscaled values when
/// the bound's scale is narrower. A bound that cannot be represented as
/// `column_type` is dropped rather than persisted at the wrong scale.
fn align_column_stats_to_schema(cs: &ColumnStatistics, column_type: &DataType) -> ColumnStatistics {
    let align = |bound: &Precision<ScalarValue>| -> Precision<ScalarValue> {
        let Some(value) = bound.get_value() else {
            return bound.clone();
        };
        match retag_bound_to_column(value.clone(), column_type) {
            Some(aligned) if bound.is_exact().is_some() => Precision::Exact(aligned),
            Some(aligned) => Precision::Inexact(aligned),
            None => Precision::Absent,
        }
    };
    ColumnStatistics {
        min_value: align(&cs.min_value),
        max_value: align(&cs.max_value),
        ..cs.clone()
    }
}

/// Restore `DataFusion` scan statistics from a persisted Vortex blob.
pub(crate) fn statistics_from_persisted_blob(
    blob: &[u8],
    schema: &Schema,
    num_rows: i64,
) -> Option<Arc<Statistics>> {
    let file_stats = deserialize_file_statistics(blob, schema).ok()?;
    Some(Arc::new(file_statistics_to_df(
        &file_stats,
        schema,
        num_rows,
    )))
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
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit, i256};
    use datafusion_common::{
        ColumnStatistics, ScalarValue, Statistics, stats::Precision as DfPrecision,
    };
    use std::sync::Arc;

    fn restored_column(
        data_type: DataType,
        min: ScalarValue,
        max: ScalarValue,
    ) -> ColumnStatistics {
        assert!(
            df_scalar_to_vortex(&min).is_some(),
            "min {min:?} must convert to a Vortex scalar value"
        );
        assert!(
            df_scalar_to_vortex(&max).is_some(),
            "max {max:?} must convert to a Vortex scalar value"
        );
        let schema = Schema::new(vec![Field::new("c", data_type, true)]);
        let stats = Statistics {
            num_rows: DfPrecision::Exact(2),
            total_byte_size: DfPrecision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: DfPrecision::Exact(0),
                min_value: DfPrecision::Exact(min),
                max_value: DfPrecision::Exact(max),
                sum_value: DfPrecision::Absent,
                distinct_count: DfPrecision::Absent,
                byte_size: DfPrecision::Absent,
            }],
        };
        let blob = statistics_to_persisted_blob(&stats, &schema).expect("blob serializes");
        let restored = statistics_from_persisted_blob(&blob, &schema, 2).expect("blob restores");
        restored.column_statistics[0].clone()
    }

    /// The size a scan reports must survive the blob, or the same file answers
    /// one size from its footer and another from the blob written off that very
    /// footer — which is what decides a join's build side (#13829).
    #[test]
    fn per_column_byte_size_survives_the_persisted_blob() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let stats = Statistics {
            num_rows: DfPrecision::Exact(4),
            total_byte_size: DfPrecision::Exact(96),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: DfPrecision::Exact(0),
                    min_value: DfPrecision::Absent,
                    max_value: DfPrecision::Absent,
                    sum_value: DfPrecision::Absent,
                    distinct_count: DfPrecision::Absent,
                    byte_size: DfPrecision::Exact(32),
                },
                ColumnStatistics {
                    null_count: DfPrecision::Exact(1),
                    min_value: DfPrecision::Absent,
                    max_value: DfPrecision::Absent,
                    sum_value: DfPrecision::Absent,
                    distinct_count: DfPrecision::Absent,
                    byte_size: DfPrecision::Exact(64),
                },
            ],
        };

        let blob = statistics_to_persisted_blob(&stats, &schema).expect("blob serializes");
        let restored = statistics_from_persisted_blob(&blob, &schema, 4).expect("blob restores");

        assert_eq!(
            restored.column_statistics[0].byte_size,
            DfPrecision::Exact(32),
            "fixed-width column keeps its byte size"
        );
        assert_eq!(
            restored.column_statistics[1].byte_size,
            DfPrecision::Exact(64),
            "variable-width column keeps its byte size"
        );
        // Summed the way `VortexFormat::infer_stats` sums them, so the two
        // sources agree on the total and not merely on the parts.
        assert_eq!(
            restored.total_byte_size,
            DfPrecision::Exact(96),
            "total is the sum of the per-column sizes"
        );
    }

    /// A blob written before byte sizes were persisted has none, and a total
    /// summed from only the columns that happen to carry one would be wrong
    /// rather than missing. `Absent` is also the signal
    /// `collect_scan_file_statistics` re-infers such a blob from the footer on.
    #[test]
    fn a_missing_column_byte_size_leaves_the_total_absent() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let sized = ColumnStatistics {
            null_count: DfPrecision::Exact(0),
            min_value: DfPrecision::Absent,
            max_value: DfPrecision::Absent,
            sum_value: DfPrecision::Absent,
            distinct_count: DfPrecision::Absent,
            byte_size: DfPrecision::Exact(32),
        };
        let unsized_column = ColumnStatistics {
            byte_size: DfPrecision::Absent,
            ..sized.clone()
        };

        // A legacy blob: no column carries a size.
        let legacy = Statistics {
            num_rows: DfPrecision::Exact(4),
            total_byte_size: DfPrecision::Absent,
            column_statistics: vec![unsized_column.clone(), unsized_column.clone()],
        };
        let blob = statistics_to_persisted_blob(&legacy, &schema).expect("blob serializes");
        let restored = statistics_from_persisted_blob(&blob, &schema, 4).expect("blob restores");
        assert_eq!(
            restored.total_byte_size,
            DfPrecision::Absent,
            "a blob with no sizes reports no total"
        );

        // The partial case is the dangerous one: summing 32 here would report a
        // table a third of its real size and skew every join that reads it.
        let partial = Statistics {
            num_rows: DfPrecision::Exact(4),
            total_byte_size: DfPrecision::Absent,
            column_statistics: vec![sized, unsized_column],
        };
        let blob = statistics_to_persisted_blob(&partial, &schema).expect("blob serializes");
        let restored = statistics_from_persisted_blob(&blob, &schema, 4).expect("blob restores");
        assert_eq!(
            restored.total_byte_size,
            DfPrecision::Absent,
            "one column without a size makes the total absent, not a partial sum"
        );
    }

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

        let df = file_statistics_to_df(&rt, &schema, 3);
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
        let df = file_statistics_to_df(&merged_stats, &schema, 6);
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

    /// A `Decimal128` bound must survive the Vortex round-trip with its
    /// precision, scale, and 128-bit width intact.
    ///
    /// Before this arm existed, `df_scalar_to_vortex` fell through to
    /// `_ => return None` and the min/max never reached the stats blob at all.
    /// Restore must not pick the narrowest width that fits the precision: a
    /// `Decimal128(10, 2)` bound coming back as `Decimal32(10, 2)` is discarded
    /// as a type mismatch.
    #[test]
    fn decimal128_bounds_survive_the_vortex_round_trip() {
        let min = ScalarValue::Decimal128(Some(12_345), 10, 2);
        let max = ScalarValue::Decimal128(Some(67_890), 10, 2);
        let col = restored_column(DataType::Decimal128(10, 2), min.clone(), max.clone());
        assert_eq!(
            col.min_value,
            DfPrecision::Exact(min),
            "a Decimal128(10, 2) column must not come back as a narrower decimal width"
        );
        assert_eq!(col.max_value, DfPrecision::Exact(max));
    }

    #[test]
    fn decimal32_and_decimal64_bounds_keep_their_width() {
        let d32_min = ScalarValue::Decimal32(Some(101), 5, 2);
        let d32_max = ScalarValue::Decimal32(Some(909), 5, 2);
        let col = restored_column(DataType::Decimal32(5, 2), d32_min.clone(), d32_max.clone());
        assert_eq!(col.min_value, DfPrecision::Exact(d32_min));
        assert_eq!(col.max_value, DfPrecision::Exact(d32_max));

        let d64_min = ScalarValue::Decimal64(Some(101), 10, 2);
        let d64_max = ScalarValue::Decimal64(Some(909), 10, 2);
        let col = restored_column(DataType::Decimal64(10, 2), d64_min.clone(), d64_max.clone());
        assert_eq!(col.min_value, DfPrecision::Exact(d64_min));
        assert_eq!(col.max_value, DfPrecision::Exact(d64_max));
    }

    #[test]
    fn decimal256_bounds_survive_the_vortex_round_trip() {
        let min = ScalarValue::Decimal256(Some(i256::from_i128(101)), 40, 2);
        let max = ScalarValue::Decimal256(Some(i256::from_i128(909)), 40, 2);
        let col = restored_column(DataType::Decimal256(40, 2), min.clone(), max.clone());
        assert_eq!(col.min_value, DfPrecision::Exact(min));
        assert_eq!(col.max_value, DfPrecision::Exact(max));
    }

    /// A bound computed at scale 2 and persisted against a scale-4 schema must
    /// be rewritten so 123.45 stays 123.45, not 1.2345.
    #[test]
    fn persisting_decimal_bounds_against_a_wider_scale_rescales() {
        let schema = Schema::new(vec![Field::new("c", DataType::Decimal128(14, 4), true)]);
        let stats = Statistics {
            num_rows: DfPrecision::Exact(2),
            total_byte_size: DfPrecision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: DfPrecision::Exact(0),
                min_value: DfPrecision::Exact(ScalarValue::Decimal128(Some(12_345), 10, 2)),
                max_value: DfPrecision::Exact(ScalarValue::Decimal128(Some(67_890), 10, 2)),
                sum_value: DfPrecision::Absent,
                distinct_count: DfPrecision::Absent,
                byte_size: DfPrecision::Absent,
            }],
        };
        let blob = statistics_to_persisted_blob(&stats, &schema).expect("blob serializes");
        let restored = statistics_from_persisted_blob(&blob, &schema, 2).expect("blob restores");
        assert_eq!(
            restored.column_statistics[0].min_value,
            DfPrecision::Exact(ScalarValue::Decimal128(Some(1_234_500), 14, 4)),
            "123.45 at scale 2 must become 123.4500 at scale 4"
        );
        assert_eq!(
            restored.column_statistics[0].max_value,
            DfPrecision::Exact(ScalarValue::Decimal128(Some(6_789_000), 14, 4))
        );
    }

    #[test]
    fn a_decimal_sum_wider_than_decimal128_restores_as_decimal256() {
        let scalar = Scalar::decimal(
            DecimalValue::I128(90),
            DecimalDType::new(48, 2),
            Nullability::Nullable,
        );
        assert_eq!(
            scalar_to_df(&scalar),
            Some(ScalarValue::Decimal256(Some(i256::from_i128(90)), 48, 2)),
            "Vortex SUM precision 48 is not a valid Decimal128"
        );
    }

    /// A `Binary` bound must survive the round-trip byte-for-byte, tagged with
    /// the column's offset width. Vortex has a single binary dtype; restore
    /// copies the tag from the Arrow schema.
    #[test]
    fn binary_bounds_survive_the_vortex_round_trip() {
        let bytes = vec![0xDE_u8, 0xAD, 0xBE, 0xEF];
        // Vortex has no FixedSizeBinary column dtype, so it cannot live in a
        // stats blob schema — but the write-path min is still converted.
        assert!(
            df_scalar_to_vortex(&ScalarValue::FixedSizeBinary(4, Some(bytes.clone()))).is_some(),
            "FixedSizeBinary min must convert even though Cayenne rejects the column type"
        );
        for (data_type, min, max) in [
            (
                DataType::Binary,
                ScalarValue::Binary(Some(bytes.clone())),
                ScalarValue::Binary(Some(bytes.clone())),
            ),
            (
                DataType::LargeBinary,
                ScalarValue::LargeBinary(Some(bytes.clone())),
                ScalarValue::LargeBinary(Some(bytes.clone())),
            ),
            (
                DataType::BinaryView,
                ScalarValue::BinaryView(Some(bytes.clone())),
                ScalarValue::BinaryView(Some(bytes)),
            ),
        ] {
            let col = restored_column(data_type, min.clone(), max.clone());
            assert_eq!(col.min_value, DfPrecision::Exact(min));
            assert_eq!(col.max_value, DfPrecision::Exact(max));
        }
    }

    #[test]
    fn utf8view_bounds_come_back_tagged_utf8view() {
        let min = ScalarValue::Utf8View(Some("apple".into()));
        let max = ScalarValue::Utf8View(Some("cherry".into()));
        let col = restored_column(DataType::Utf8View, min.clone(), max.clone());
        assert_eq!(col.min_value, DfPrecision::Exact(min));
        assert_eq!(col.max_value, DfPrecision::Exact(max));
    }

    #[test]
    fn time32_and_time64_bounds_survive_the_vortex_round_trip() {
        let cases = [
            (
                DataType::Time32(TimeUnit::Second),
                ScalarValue::Time32Second(Some(1)),
                ScalarValue::Time32Second(Some(9)),
            ),
            (
                DataType::Time32(TimeUnit::Millisecond),
                ScalarValue::Time32Millisecond(Some(1)),
                ScalarValue::Time32Millisecond(Some(9)),
            ),
            (
                DataType::Time64(TimeUnit::Microsecond),
                ScalarValue::Time64Microsecond(Some(1)),
                ScalarValue::Time64Microsecond(Some(9)),
            ),
            (
                DataType::Time64(TimeUnit::Nanosecond),
                ScalarValue::Time64Nanosecond(Some(1)),
                ScalarValue::Time64Nanosecond(Some(9)),
            ),
        ];
        for (data_type, min, max) in cases {
            let col = restored_column(data_type, min.clone(), max.clone());
            assert_eq!(col.min_value, DfPrecision::Exact(min), "time min");
            assert_eq!(col.max_value, DfPrecision::Exact(max), "time max");
        }
    }

    #[test]
    fn dictionary_utf8_bounds_unwrap_and_restore_as_dictionary() {
        let min = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Utf8(Some("aa".into()))),
        );
        let max = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Utf8(Some("zz".into()))),
        );
        let col = restored_column(
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            min.clone(),
            max.clone(),
        );
        assert_eq!(col.min_value, DfPrecision::Exact(min));
        assert_eq!(col.max_value, DfPrecision::Exact(max));
    }

    /// The write path produces `Decimal128` mins via `compute_column_stats`;
    /// those must persist, not only hand-built `ScalarValue`s.
    #[test]
    fn decimal128_array_min_survives_the_blob() {
        use crate::provider::column_stats::ColumnStatsAccumulator;
        use arrow::array::Decimal128Array;

        let col = Decimal128Array::from(vec![Some(12_345), Some(67_890)])
            .with_precision_and_scale(10, 2)
            .expect("decimal array");
        let cs = ColumnStatsAccumulator::compute_column_stats(&col);
        let schema = Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            true,
        )]);
        let stats = Statistics {
            num_rows: DfPrecision::Exact(2),
            total_byte_size: DfPrecision::Absent,
            column_statistics: vec![cs],
        };
        let blob = statistics_to_persisted_blob(&stats, &schema).expect("blob serializes");
        let restored = statistics_from_persisted_blob(&blob, &schema, 2).expect("blob restores");
        assert_eq!(
            restored.column_statistics[0].min_value,
            DfPrecision::Exact(ScalarValue::Decimal128(Some(12_345), 10, 2))
        );
        assert_eq!(
            restored.column_statistics[0].max_value,
            DfPrecision::Exact(ScalarValue::Decimal128(Some(67_890), 10, 2))
        );
    }

    /// The write path produces `Time32` mins via `compute_column_stats`; those
    /// must persist, not only hand-built `ScalarValue`s.
    #[test]
    fn time32_array_min_survives_the_blob() {
        use crate::provider::column_stats::ColumnStatsAccumulator;
        use arrow::array::Time32SecondArray;

        let col = Time32SecondArray::from(vec![Some(1), Some(9)]);
        let cs = ColumnStatsAccumulator::compute_column_stats(&col);
        let schema = Schema::new(vec![Field::new(
            "t",
            DataType::Time32(TimeUnit::Second),
            true,
        )]);
        let stats = Statistics {
            num_rows: DfPrecision::Exact(2),
            total_byte_size: DfPrecision::Absent,
            column_statistics: vec![cs],
        };
        let blob = statistics_to_persisted_blob(&stats, &schema).expect("blob serializes");
        let restored = statistics_from_persisted_blob(&blob, &schema, 2).expect("blob restores");
        assert_eq!(
            restored.column_statistics[0].min_value,
            DfPrecision::Exact(ScalarValue::Time32Second(Some(1)))
        );
        assert_eq!(
            restored.column_statistics[0].max_value,
            DfPrecision::Exact(ScalarValue::Time32Second(Some(9)))
        );
    }

    #[test]
    fn sum_roundtrips_through_file_statistics() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let cs = ColumnStatistics {
            null_count: DfPrecision::Exact(0),
            min_value: DfPrecision::Exact(ScalarValue::Int64(Some(1))),
            max_value: DfPrecision::Exact(ScalarValue::Int64(Some(3))),
            sum_value: DfPrecision::Exact(ScalarValue::Int64(Some(6))),
            distinct_count: DfPrecision::Absent,
            byte_size: DfPrecision::Absent,
        };
        let set = column_stats_to_stats_set(&cs);
        assert!(
            matches!(set.get(Stat::Sum), VortexPrecision::Exact(_)),
            "sum present in StatsSet"
        );

        let file_stats = build_file_statistics(vec![set], &schema);
        let bytes = serialize_file_statistics(&file_stats).expect("serialize ok");
        let rt = deserialize_file_statistics(&bytes, &schema).expect("deserialize ok");

        let df = file_statistics_to_df(&rt, &schema, 3);
        assert_eq!(
            df.column_statistics[0].sum_value,
            DfPrecision::Exact(ScalarValue::Int64(Some(6))),
            "exact sum must survive the metastore blob roundtrip"
        );
    }

    #[test]
    fn serialized_statistics_merge_adds_sum_across_writes() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let mk_sum = |sum: i64| ColumnStatistics {
            null_count: DfPrecision::Exact(0),
            min_value: DfPrecision::Absent,
            max_value: DfPrecision::Absent,
            sum_value: DfPrecision::Exact(ScalarValue::Int64(Some(sum))),
            distinct_count: DfPrecision::Absent,
            byte_size: DfPrecision::Absent,
        };
        let first_set = column_stats_to_stats_set(&mk_sum(60));
        let second_set = column_stats_to_stats_set(&mk_sum(30));
        let first_blob =
            serialize_file_statistics(&build_file_statistics(vec![first_set], &schema))
                .expect("serialize ok");
        let dtypes = vec![DType::from_arrow((
            schema.field(0).data_type(),
            Nullability::Nullable,
        ))];

        let merged_blob = merge_serialized_stats(&first_blob, &[second_set], &dtypes, &schema)
            .expect("statistics should merge");
        let merged = deserialize_file_statistics(&merged_blob, &schema).expect("deserialize ok");

        let df = file_statistics_to_df(&merged, &schema, 9);
        assert_eq!(
            df.column_statistics[0].sum_value,
            DfPrecision::Exact(ScalarValue::Int64(Some(90))),
            "sums must add additively across writes/files (Vortex merge_sum)"
        );
    }
}
