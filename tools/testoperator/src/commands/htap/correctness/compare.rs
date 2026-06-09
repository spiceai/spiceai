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

//! Engine-agnostic numeric comparison shared by the HTAP correctness gates
//! (`analytical` and `row_count`).
//!
//! Postgres (the source of truth) and Cayenne emit the same logical values with
//! different physical Arrow encodings. These helpers compare *values* — not
//! their string renderings — with a type-aware tolerance:
//!   * integer / decimal columns (row counts, money sums): **exact**, zero
//!     tolerance — a count or a cent that drifts at all is a real defect;
//!   * floating-point columns: a small relative epsilon — the only place real
//!     rounding occurs in this pipeline (FP/encoding error here is < 0.001%).
//!
//! Cells are compared after casting to `f64`. That is exact for integers and
//! decimals whose magnitude stays below 2^53 (~9.0e15), which holds for every
//! CH-benCH aggregate at the scale factors we run (the largest row counts and
//! money sums are ~1e9–1e13). This bound is documented here so a future,
//! enormous scale factor doesn't silently lose integer exactness unnoticed.

use arrow::array::{Array, Float64Array, RecordBatch};
use arrow::datatypes::DataType;

/// Relative tolerance for floating-point columns (0.1%). Comfortably above the
/// real FP/encoding error (< 0.001%) yet far tighter than the legacy 5% gate,
/// so a genuine sub-5% value drift is now caught instead of passing silently.
pub const FLOAT_REL_TOLERANCE: f64 = 0.001;

/// Outcome of comparing the numeric columns of two row-aligned record batches.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NumericDelta {
    /// Largest relative delta observed across all compared numeric cells.
    /// Always reported (even within tolerance) so sub-threshold drift is
    /// visible rather than hidden behind a binary pass/fail.
    pub max_rel_delta: f64,
    /// `true` if any integer/decimal cell differed at all, or any float cell
    /// exceeded [`FLOAT_REL_TOLERANCE`].
    pub exceeded: bool,
    /// Column / row / values of the worst *offending* cell, for the failure
    /// message. `None` when nothing exceeded tolerance.
    pub worst: Option<String>,
}

/// Whether a column's values are compared numerically by [`numeric_delta`]
/// (integers, decimals, floats). Public so the fingerprint gate can decide
/// which columns get a `MIN`/`MAX` aggregate.
#[must_use]
pub fn is_numeric(dt: &DataType) -> bool {
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
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn is_float(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

/// Per-column float-ness of a batch's schema, for the `actual_source_floats`
/// argument of [`numeric_delta`].
///
/// The analytical gate casts Spice's output to the *source* (Postgres) schema
/// before comparison, which turns an `avg()` that Spice computed as `Float64`
/// into the `Decimal128` the PG arrow connector returns for `NUMERIC`. Captured
/// from the pre-alignment actual batch, this lets [`numeric_delta`] keep the
/// relative float tolerance for those approximate columns instead of demoting
/// them to the exact integer/decimal path. The fingerprint gate runs identical
/// SQL on both engines (no alignment), so it passes its engine-native batch and
/// this is simply the actual types.
#[must_use]
pub fn float_columns(batch: &RecordBatch) -> Vec<bool> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| is_float(f.data_type()))
        .collect()
}

/// Cast both columns to `Float64` for value comparison. Returns `None` if either
/// cast (or the subsequent downcast) fails — the caller fails safe rather than
/// silently skipping the column.
fn cast_pair_to_f64(e_col: &dyn Array, a_col: &dyn Array) -> Option<(Float64Array, Float64Array)> {
    let e = arrow::compute::cast(e_col, &DataType::Float64).ok()?;
    let a = arrow::compute::cast(a_col, &DataType::Float64).ok()?;
    let e = e.as_any().downcast_ref::<Float64Array>()?.clone();
    let a = a.as_any().downcast_ref::<Float64Array>()?.clone();
    Some((e, a))
}

/// Compare the numeric columns of `expected` and `actual` position-by-position.
///
/// The two batches are assumed row-aligned (same row order, same column order):
/// the analytical gate guarantees this by schema-aligning and lexsorting both
/// sides; the fingerprint gate by issuing the identical aggregate SQL to both
/// engines. Columns whose values genuinely diverge will also misalign under the
/// shared sort, which only inflates the delta — a divergence still surfaces.
///
/// Non-numeric columns are intentionally ignored here: the string comparator
/// (analytical) and per-column non-null counts (fingerprint) cover those, and
/// cross-engine text collation / timestamp precision make their MIN/MAX
/// unreliable to compare directly.
///
/// `actual_source_floats[i]` flags columns the actual engine produced as
/// floating point *before* any schema alignment (see [`float_columns`]). A
/// column is compared with the relative float tolerance when either side's
/// compared type is float *or* its pre-alignment actual type was — so an
/// `avg()` that Spice computed in `Float64` but the gate cast to the source's
/// `Decimal128` keeps its tolerance, while money sums and counts (decimal /
/// integer on both sides, and never float pre-alignment) stay exact.
#[must_use]
pub fn numeric_delta(
    expected: &RecordBatch,
    actual: &RecordBatch,
    actual_source_floats: &[bool],
) -> NumericDelta {
    let mut out = NumericDelta::default();
    let mut worst_rel = 0.0_f64;
    let n_rows = expected.num_rows().min(actual.num_rows());

    // Zip the two column slices (truncating to the shorter) alongside the
    // expected schema's fields for the column name used in the failure message.
    let e_schema = expected.schema();
    for (c, ((e_col, a_col), field)) in expected
        .columns()
        .iter()
        .zip(actual.columns().iter())
        .zip(e_schema.fields().iter())
        .enumerate()
    {
        if !is_numeric(e_col.data_type()) || !is_numeric(a_col.data_type()) {
            continue;
        }
        let float_col = is_float(e_col.data_type())
            || is_float(a_col.data_type())
            || actual_source_floats.get(c).copied().unwrap_or(false);
        let col_name = field.name();

        // Both columns are numeric, so casting to f64 should always succeed.
        // If it somehow doesn't, fail safe: in the fingerprint gate this is the
        // *only* comparator, so silently skipping the column could let a real
        // numeric divergence pass.
        let Some((e_arr, a_arr)) = cast_pair_to_f64(e_col, a_col) else {
            out.exceeded = true;
            if out.worst.is_none() {
                out.worst = Some(format!(
                    "{col_name}: numeric column could not be cast to f64 for comparison"
                ));
            }
            continue;
        };

        for r in 0..n_rows {
            // Null-pattern differences are caught by the string comparator;
            // here we only compare two present numeric values.
            if e_arr.is_null(r) || a_arr.is_null(r) {
                continue;
            }
            let ev = e_arr.value(r);
            let av = a_arr.value(r);
            let diff = (ev - av).abs();
            // Floor the denominator: 1.0 for exact (integer/decimal) columns so
            // the displayed rel% is sensible; tiny for floats so genuine drift
            // from a near-zero expected value still registers.
            let rel = diff / ev.abs().max(if float_col { 1e-12 } else { 1.0 });

            if rel > out.max_rel_delta {
                out.max_rel_delta = rel;
            }

            let cell_exceeded = if float_col {
                rel > FLOAT_REL_TOLERANCE
            } else {
                diff > 0.0
            };
            if cell_exceeded {
                out.exceeded = true;
                if out.worst.is_none() || rel > worst_rel {
                    worst_rel = rel;
                    out.worst = Some(format!(
                        "{col_name}[row {r}]: expected {ev}, actual {av} (rel {:.6}%)",
                        rel * 100.0
                    ));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let arrays: Vec<ArrayRef> = cols.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("valid batch")
    }

    fn int_col(name: &str, v: Vec<i64>) -> (&str, ArrayRef) {
        (name, Arc::new(Int64Array::from(v)) as ArrayRef)
    }

    fn float_col(name: &str, v: Vec<f64>) -> (&str, ArrayRef) {
        (name, Arc::new(Float64Array::from(v)) as ArrayRef)
    }

    #[test]
    fn identical_integers_have_no_drift() {
        let e = batch(vec![int_col("count_order", vec![10, 20, 30])]);
        let a = batch(vec![int_col("count_order", vec![10, 20, 30])]);
        let d = numeric_delta(&e, &a, &float_columns(&a));
        assert!(!d.exceeded);
        assert!(d.max_rel_delta.abs() < f64::EPSILON);
        assert!(d.worst.is_none());
    }

    #[test]
    fn integer_off_by_one_exceeds_exact_tolerance() {
        // A single-row count off by one — the wrong-upsert / stale-update class.
        let e = batch(vec![int_col("c", vec![1_000_000])]);
        let a = batch(vec![int_col("c", vec![999_999])]);
        let d = numeric_delta(&e, &a, &float_columns(&a));
        assert!(d.exceeded, "any integer diff must exceed (exact tolerance)");
        assert!(d.worst.is_some());
    }

    #[test]
    fn float_within_tolerance_passes_but_reports_delta() {
        // 0.05% drift: under the 0.1% float tolerance, but must still be visible.
        let e = batch(vec![float_col("avg_amount", vec![1000.0])]);
        let a = batch(vec![float_col("avg_amount", vec![1000.5])]);
        let d = numeric_delta(&e, &a, &float_columns(&a));
        assert!(!d.exceeded, "0.05% < 0.1% tolerance");
        assert!(d.max_rel_delta > 0.0 && d.max_rel_delta < FLOAT_REL_TOLERANCE);
    }

    #[test]
    fn float_beyond_tolerance_exceeds() {
        // 1% drift — passed the old 5% gate silently, must now fail.
        let e = batch(vec![float_col("revenue", vec![1000.0])]);
        let a = batch(vec![float_col("revenue", vec![1010.0])]);
        let d = numeric_delta(&e, &a, &float_columns(&a));
        assert!(d.exceeded);
        assert!(d.max_rel_delta > FLOAT_REL_TOLERANCE);
    }

    #[test]
    fn decimal_exact_mismatch_exceeds() {
        let e = batch(vec![(
            "money",
            Arc::new(
                Decimal128Array::from(vec![123_456_i128, 999_i128])
                    .with_data_type(DataType::Decimal128(38, 2)),
            ) as ArrayRef,
        )]);
        let a = batch(vec![(
            "money",
            Arc::new(
                Decimal128Array::from(vec![123_457_i128, 999_i128])
                    .with_data_type(DataType::Decimal128(38, 2)),
            ) as ArrayRef,
        )]);
        let d = numeric_delta(&e, &a, &float_columns(&a));
        assert!(d.exceeded, "a one-cent decimal diff must fail (exact)");
    }

    #[test]
    fn non_numeric_columns_are_ignored() {
        let e = batch(vec![(
            "name",
            Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
        )]);
        let a = batch(vec![(
            "name",
            Arc::new(StringArray::from(vec!["alice", "carol"])) as ArrayRef,
        )]);
        let d = numeric_delta(&e, &a, &float_columns(&a));
        assert!(
            !d.exceeded,
            "text divergence is the string comparator's job"
        );
    }

    #[test]
    fn nulls_are_skipped() {
        let e = batch(vec![(
            "c",
            Arc::new(Int64Array::from(vec![Some(5), None])) as ArrayRef,
        )]);
        let a = batch(vec![(
            "c",
            Arc::new(Int64Array::from(vec![Some(5), None])) as ArrayRef,
        )]);
        let d = numeric_delta(&e, &a, &float_columns(&a));
        assert!(!d.exceeded);
    }

    fn decimal_col(name: &str, raw: Vec<i128>, precision: u8, scale: i8) -> (&str, ArrayRef) {
        (
            name,
            Arc::new(
                Decimal128Array::from(raw).with_data_type(DataType::Decimal128(precision, scale)),
            ) as ArrayRef,
        )
    }

    #[test]
    fn aligned_avg_keeps_float_tolerance_when_actual_was_float() {
        // Reproduces the analytical gate after alignment: Spice computed avg()
        // as Float64, but the gate cast it to the source's Decimal128(NUMERIC),
        // so both compared columns are now decimal. A tiny rounding drift
        // (1000.0000001 vs 1000.0000002, rel 1e-10) must NOT fail when the
        // pre-alignment actual type was float...
        let e = batch(vec![decimal_col("avg_qty", vec![10_000_000_001_i128], 38, 7)]);
        let a = batch(vec![decimal_col("avg_qty", vec![10_000_000_002_i128], 38, 7)]);

        let approx = numeric_delta(&e, &a, &[true]);
        assert!(
            !approx.exceeded,
            "sub-tolerance avg drift must pass when actual was float pre-alignment: {:?}",
            approx.worst
        );
        assert!(approx.max_rel_delta > 0.0 && approx.max_rel_delta < FLOAT_REL_TOLERANCE);

        // ...but the same decimal cells stay exact when neither side was float
        // (a money sum / count), so a one-unit drift is still a defect.
        let exact = numeric_delta(&e, &a, &[false]);
        assert!(
            exact.exceeded,
            "decimal money/count must stay exact regardless of magnitude"
        );
    }
}
