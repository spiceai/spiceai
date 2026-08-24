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
//! The source engine (Postgres or `MySQL` — the source of truth) and Cayenne emit
//! the same logical values with different physical Arrow encodings. These
//! helpers compare *values* — not their string renderings — with a type-aware
//! tolerance:
//!   * integers and *exact-reproduction* decimals (row counts, money `SUM`/
//!     `MIN`/`MAX`): **exact**, zero tolerance — a count or a cent that drifts at
//!     all is a real defect;
//!   * floating-point columns and `AVG`/division decimals: a small relative
//!     epsilon — the only places real rounding occurs in this pipeline (FP /
//!     encoding / decimal-division error here is < 0.001%).
//!
//! Two decimal cells are decided on their **mantissas**, rescaled to a common
//! scale — never on a `f64` cast, which is not a reliable equality test for them
//! (see `decimal_pair_to_i128`). Everything else is compared after casting to
//! `f64`: exact for integers whose magnitude stays below 2^53 (~9.0e15), which
//! holds for every CH-benCH aggregate at the scale factors we run (the largest
//! row counts are ~1e9–1e13). That bound is documented here so a future,
//! enormous scale factor doesn't silently lose integer exactness unnoticed.
//!
//! The `f64` cast is still taken for every numeric cell, because the reported
//! `rel %` and `max_rel_delta` are computed from it. Only the decimal pass/fail
//! decision is made elsewhere.

use arrow::array::{Array, Decimal256Array, Float64Array, RecordBatch};
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, i256};

/// Relative tolerance for floating-point columns (0.1%). Comfortably above the
/// real FP/encoding error (< 0.001%) yet far tighter than the legacy 5% gate,
/// so a genuine sub-5% value drift is now caught instead of passing silently.
pub const FLOAT_REL_TOLERANCE: f64 = 0.001;

/// Decimal scale of TPC-C money columns (`NUMERIC(_,2)` — cents). Exact
/// aggregates (`SUM`/`MIN`/`MAX`) preserve this scale, so they stay on the exact
/// comparison path; only `AVG`/division inflates a decimal result beyond it.
/// A decimal column whose scale exceeds this is therefore treated as an
/// approximate (rounding-prone) aggregate. See [`approximate_columns`].
pub const MONEY_SCALE: i8 = 2;

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
    /// Row index (0-based) of the worst offending cell, so callers can print
    /// the surrounding rows for context. `None` when nothing exceeded tolerance
    /// (or the divergence was a whole-column cast failure with no single row).
    pub worst_row: Option<usize>,
    /// Column index of the worst offending cell, matching `worst_row`. `None`
    /// under the same conditions.
    pub worst_col: Option<usize>,
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

/// Decimal scale of a numeric type, or `None` for non-decimals.
fn decimal_scale(dt: &DataType) -> Option<i8> {
    match dt {
        DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => Some(*s),
        _ => None,
    }
}

/// Whether a column is numeric *and* exact — integers and decimals, never
/// floats. `SUM` over such a column is bit-identical across engines (no
/// order-dependent rounding), so the fingerprint gate can compare it with zero
/// tolerance; a floating `SUM` legitimately drifts and must not be summed.
#[must_use]
pub fn is_exact_numeric(dt: &DataType) -> bool {
    is_numeric(dt) && !is_float(dt)
}

/// Per-column float-ness of a batch's schema, for the `actual_source_floats`
/// argument of [`numeric_delta`].
///
/// The analytical gate casts Spice's output to the *source* engine's schema
/// before comparison, which turns an `avg()` that Spice computed as `Float64`
/// into the `Decimal128` the source arrow connector returns for
/// `NUMERIC`/`DECIMAL`. Captured
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

/// Analytical-gate generalization of [`float_columns`]: flags a column for
/// relative float tolerance when either side is float, or the column is a
/// decimal produced by `AVG`/division. Exact reproductions (`SUM`/`MIN`/`MAX`/
/// `COUNT`) preserve the operand scale (money is [`MONEY_SCALE`] digits in
/// TPC-C) and stay exact; `AVG`/division *inflate* the scale — `DataFusion` and
/// `MySQL` to operand scale + 4, Postgres to ~13 — so their low digits
/// legitimately differ per-engine and must not be compared bit-exactly.
///
/// We detect that inflation directly rather than assuming the two engines
/// *disagree* on the inflated scale: a decimal column is approximate when the
/// scales differ **or** the (common) scale exceeds [`MONEY_SCALE`]. The
/// scales-differ arm alone was Postgres-specific — `MySQL`'s `AVG` scale
/// (operand + 4 = 6 for `NUMERIC(_,2)`) coincides with `DataFusion`'s, so a
/// same-scale `AVG` used to fall into the exact path and a benign last-digit
/// rounding difference tripped `DIVERGE`.
///
/// Must run pre-alignment (alignment casts actual to the source scale, erasing
/// the signal); columns match by position.
#[must_use]
pub fn approximate_columns(expected: &RecordBatch, actual: &RecordBatch) -> Vec<bool> {
    let a_schema = actual.schema();
    let a_fields = a_schema.fields();
    expected
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let Some(a) = a_fields.get(i) else {
                return is_float(e.data_type());
            };
            let (e_dt, a_dt) = (e.data_type(), a.data_type());
            is_float(e_dt)
                || is_float(a_dt)
                || matches!(
                    (decimal_scale(e_dt), decimal_scale(a_dt)),
                    (Some(es), Some(a_s)) if es != a_s || es.max(a_s) > MONEY_SCALE
                )
        })
        .collect()
}

/// Two columns' mantissas rescaled to a common decimal scale, expected then actual.
type RescaledPair = (Vec<Option<i256>>, Vec<Option<i256>>);

/// What [`decimal_pair_to_i256`] could make of a column pair.
enum ExactDecimals {
    /// Not a decimal pair; the `f64` comparison applies.
    NotApplicable,
    /// Mantissas at a common scale, comparable exactly.
    Rescaled(RescaledPair),
    /// A decimal pair that could not be brought to a common scale without
    /// overflowing. Deliberately NOT the same answer as `NotApplicable`: falling
    /// back to `f64` here would report two decimals equal whenever `f64` cannot
    /// tell them apart, which for wide `Decimal256` values is any pair sharing the
    /// leading ~15 digits. A gate that answers "equal" because it ran out of
    /// precision is worse than one that is too strict.
    Unrepresentable,
}

/// Both columns' values as `i256` mantissas at a common scale.
///
/// Exists because comparing two decimals THROUGH `f64` cannot be made reliable:
/// Arrow casts a decimal to `f64` by dividing the mantissa by `10^scale` in
/// floating point, and above roughly scale 18 neither operand is representable,
/// so the quotient stops being correctly rounded. Two sides holding the same
/// decimal but declaring different scales then land on `f64` values that differ
/// in the last place. Comparing the mantissas instead is exact by construction.
///
/// `i256` rather than `i128` so `Decimal256` is handled natively: narrowing it to
/// `Decimal128` first would fail for exactly the wide values whose comparison
/// matters most.
fn decimal_pair_to_i256(e_col: &dyn Array, a_col: &dyn Array) -> ExactDecimals {
    let (Some(e_scale), Some(a_scale)) = (
        decimal_scale(e_col.data_type()),
        decimal_scale(a_col.data_type()),
    ) else {
        return ExactDecimals::NotApplicable;
    };
    // Rescale both sides UP to the wider scale: scaling down would discard the
    // digits a genuine divergence might live in (asserted by
    // `a_low_digit_difference_at_a_wider_scale_still_diverges`).
    let common = e_scale.max(a_scale);
    // `safe: false` so an overflowing rescale ERRORS. The default nulls it out
    // instead, and a null reads as "skip this row" below — the same silent pass
    // this function exists to prevent.
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let widen = |col: &dyn Array| -> Option<Vec<Option<i256>>> {
        let decimal =
            arrow::compute::cast_with_options(col, &DataType::Decimal256(76, common), &options)
                .ok()?;
        let decimal = decimal.as_any().downcast_ref::<Decimal256Array>()?;
        Some(
            (0..decimal.len())
                .map(|r| (!decimal.is_null(r)).then(|| decimal.value(r)))
                .collect(),
        )
    };
    match (widen(e_col), widen(a_col)) {
        (Some(e), Some(a)) => ExactDecimals::Rescaled((e, a)),
        _ => ExactDecimals::Unrepresentable,
    }
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
/// `approximate[i]` flags columns to compare with relative float tolerance
/// instead of exactly: the fingerprint gate passes [`float_columns`], the
/// analytical gate [`approximate_columns`]. Sums and counts stay exact.
#[must_use]
pub fn numeric_delta(
    expected: &RecordBatch,
    actual: &RecordBatch,
    approximate: &[bool],
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
            || approximate.get(c).copied().unwrap_or(false);
        let col_name = field.name();

        // Both columns are numeric, so casting to f64 should always succeed.
        // If it somehow doesn't, fail safe: in the fingerprint gate this is the
        // *only* comparator, so silently skipping the column could let a real
        // numeric divergence pass.
        // Exact decimals are decided on their mantissas, never on the `f64` cast
        // below (see `decimal_pair_to_i256`). The cast is still taken, because the
        // reported `rel %` and `max_rel_delta` are computed from it -- only the
        // pass/fail decision moves.
        let exact_decimals = if float_col {
            ExactDecimals::NotApplicable
        } else {
            decimal_pair_to_i256(e_col, a_col)
        };
        if matches!(exact_decimals, ExactDecimals::Unrepresentable) {
            // Fail the column rather than guess. See `ExactDecimals::Unrepresentable`.
            out.exceeded = true;
            if out.worst.is_none() {
                out.worst = Some(format!(
                    "{col_name}: decimal values could not be brought to a common scale for an exact comparison"
                ));
            }
            continue;
        }

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

            let cell_exceeded = match (&exact_decimals, float_col) {
                // Same mantissa at a common scale is the same number, whatever the
                // two `f64` casts made of it.
                (ExactDecimals::Rescaled((e_dec, a_dec)), _) => {
                    e_dec.get(r).copied().flatten() != a_dec.get(r).copied().flatten()
                }
                (_, true) => rel > FLOAT_REL_TOLERANCE,
                (_, false) => diff > 0.0,
            };
            if cell_exceeded {
                out.exceeded = true;
                if out.worst.is_none() || rel > worst_rel {
                    worst_rel = rel;
                    out.worst = Some(format!(
                        "{col_name}[row {r}]: expected {ev}, actual {av} (rel {:.6}%)",
                        rel * 100.0
                    ));
                    out.worst_row = Some(r);
                    out.worst_col = Some(c);
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
        // The offending cell's coordinates are surfaced so the gate can print
        // the surrounding rows for context.
        assert_eq!(d.worst_row, Some(0));
        assert_eq!(d.worst_col, Some(0));
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

    /// Two decimals holding the SAME number at different scales must compare
    /// equal, even though casting each to `f64` does not produce the same value.
    ///
    /// Regression: the fingerprint gate compared exact columns via `f64` and
    /// demanded bit equality, so a `NUMERIC` the source reported at one scale and
    /// Spice at another failed with a `rel 0.000000%` delta -- the comparator
    /// printing evidence that the difference was its own cast. It broke SF1000
    /// `postgres-cayenne` for days, and only passed when every aggregate in the
    /// fingerprint happened to round identically on both sides.
    #[test]
    fn the_same_decimal_at_two_scales_is_not_a_divergence() {
        // 9290582224.69 at scale 20 against the same number at money scale. Scale 20
        // is where Arrow's decimal-to-f64 cast starts to disagree with itself: both
        // the mantissa and the 10^20 divisor stop being exactly representable, so the
        // quotient is no longer correctly rounded. It reproduces the SF1000 gate's
        // `sum_d_ytd` rejection to the digit -- 9290582224.689999 against
        // 9290582224.69.
        //
        // At scales 2 through 18 the two conversions ROUND TO THE SAME `f64` -- not
        // because .69 is exactly representable in binary (it is not), but because
        // both operands of the division are, so each side lands on the same nearest
        // double. That is why this failed intermittently rather than always.
        let expected = batch_of(
            "sum_d_ytd",
            Arc::new(
                // The money-scale mantissa, restated at scale 20.
                Decimal128Array::from(vec![929_058_222_469_i128 * 10_i128.pow(18)])
                    .with_precision_and_scale(38, 20)
                    .expect("scale 20"),
            ),
        );
        let actual = batch_of(
            "sum_d_ytd",
            Arc::new(
                Decimal128Array::from(vec![929_058_222_469_i128])
                    .with_precision_and_scale(38, 2)
                    .expect("scale 2"),
            ),
        );

        // Guard the premise: if the two casts ever agree, this test would pass for
        // the wrong reason and stop covering the bug.
        let (e_f64, a_f64) =
            cast_pair_to_f64(expected.column(0).as_ref(), actual.column(0).as_ref())
                .expect("both cast to f64");
        #[expect(
            clippy::float_cmp,
            reason = "bit-exact f64 inequality IS the premise being guarded"
        )]
        {
            assert_ne!(
                e_f64.value(0),
                a_f64.value(0),
                "premise: the f64 casts must disagree, or this test proves nothing"
            );
        }

        let delta = numeric_delta(&expected, &actual, &float_columns(&actual));
        assert!(
            !delta.exceeded,
            "the same number at two scales must not diverge, got {:?}",
            delta.worst
        );
    }

    /// The exact path must stay exact: a decimal difference of one unit in the
    /// last place is a REAL divergence and must still fail, or the fix above
    /// would have bought a passing gate by blinding it.
    #[test]
    fn a_one_ulp_decimal_difference_still_diverges() {
        let expected = batch_of(
            "sum_w_ytd",
            Arc::new(
                Decimal128Array::from(vec![5_000_000_i128])
                    .with_precision_and_scale(38, 2)
                    .expect("scale 2"),
            ),
        );
        let actual = batch_of(
            "sum_w_ytd",
            Arc::new(
                Decimal128Array::from(vec![5_000_001_i128])
                    .with_precision_and_scale(38, 2)
                    .expect("scale 2"),
            ),
        );

        let delta = numeric_delta(&expected, &actual, &float_columns(&actual));
        assert!(
            delta.exceeded,
            "a genuine one-cent difference must still be caught"
        );
    }

    /// A difference living ONLY in the wider scale's low digits must still
    /// diverge, which is what pins the rescale direction.
    ///
    /// The equality test above passes under either direction, so on its own it
    /// would let an implementation that rescaled DOWN to the narrower scale
    /// through -- and that one truncates the digits a real divergence hides in,
    /// silently answering "equal". Here 50000.00 against 50000.0001 differs
    /// nowhere else: rescaled up to scale 4 the mantissas are 500000000 against
    /// 500000001 and it is caught, rescaled down to scale 2 both become 5000000
    /// and it is missed.
    #[test]
    fn a_low_digit_difference_at_a_wider_scale_still_diverges() {
        let expected = batch_of(
            "sum_w_ytd",
            Arc::new(
                Decimal128Array::from(vec![5_000_000_i128])
                    .with_precision_and_scale(38, 2)
                    .expect("scale 2"),
            ),
        );
        let actual = batch_of(
            "sum_w_ytd",
            Arc::new(
                // 50000.0001 -- equal to the expected value in every digit the
                // narrower scale can represent.
                Decimal128Array::from(vec![500_000_001_i128])
                    .with_precision_and_scale(38, 4)
                    .expect("scale 4"),
            ),
        );

        let delta = numeric_delta(&expected, &actual, &float_columns(&actual));
        assert!(
            delta.exceeded,
            "a difference below the narrower scale must not be rounded away"
        );
    }

    /// Two `Decimal256` values `f64` cannot tell apart must NOT be reported equal.
    ///
    /// `is_numeric` accepts `Decimal256`, and `10^40` against `10^40 + 1` collides
    /// under `f64` (they share every representable digit). Before the tri-state, a
    /// pair that failed exact conversion was indistinguishable from a non-decimal
    /// pair, so the comparison fell back to `f64` and answered "equal" — a gate
    /// silently passing a real divergence, which is worse than the over-strictness
    /// this whole change set set out to fix.
    #[test]
    fn wide_decimals_that_f64_cannot_distinguish_are_not_called_equal() {
        let ten_pow_40 = i256::from_i128(10_i128.pow(38)) * i256::from_i128(100);
        let expected = batch_of(
            "sum_wide",
            Arc::new(
                Decimal256Array::from(vec![ten_pow_40])
                    .with_precision_and_scale(76, 0)
                    .expect("wide decimal"),
            ),
        );
        let actual = batch_of(
            "sum_wide",
            Arc::new(
                Decimal256Array::from(vec![ten_pow_40 + i256::from_i128(1)])
                    .with_precision_and_scale(76, 0)
                    .expect("wide decimal"),
            ),
        );

        // Guard the premise: if `f64` could tell these apart, the test would pass
        // without exercising the exact path at all.
        let (e_f64, a_f64) =
            cast_pair_to_f64(expected.column(0).as_ref(), actual.column(0).as_ref())
                .expect("both cast to f64");
        #[expect(
            clippy::float_cmp,
            reason = "bit-exact f64 equality IS the collision being guarded"
        )]
        {
            assert_eq!(
                e_f64.value(0),
                a_f64.value(0),
                "premise: f64 must collide here, or this test proves nothing"
            );
        }

        let delta = numeric_delta(&expected, &actual, &float_columns(&actual));
        assert!(
            delta.exceeded,
            "a difference f64 cannot see must still be caught"
        );
    }

    /// A decimal pair that cannot be brought to a common scale must FAIL the
    /// column, not fall through to `f64`.
    ///
    /// Covers the `Unrepresentable` arm and the `safe: false` cast together, which
    /// nothing else does: the wide-decimal test above uses equal scales, so no
    /// rescale happens and the arm is never reached. Here a 76-digit mantissa at
    /// scale 0 is asked to widen to scale 2, which needs 78 digits and overflows
    /// `Decimal256`. With `safe: true` the cast would null the value instead of
    /// erroring, and a null reads as "skip this row" — a silent pass.
    #[test]
    fn decimals_too_wide_to_bring_to_a_common_scale_fail_the_column() {
        // 10^75, i.e. 76 significant digits: multiplying by 100 to reach scale 2
        // exceeds what Decimal256 can hold.
        let huge = (0..75).fold(i256::from_i128(1), |acc, _| acc * i256::from_i128(10));
        let expected = batch_of(
            "sum_wide",
            Arc::new(
                Decimal256Array::from(vec![huge])
                    .with_precision_and_scale(76, 0)
                    .expect("scale 0"),
            ),
        );
        let actual = batch_of(
            "sum_wide",
            Arc::new(
                Decimal256Array::from(vec![huge])
                    .with_precision_and_scale(76, 2)
                    .expect("scale 2"),
            ),
        );

        let delta = numeric_delta(&expected, &actual, &float_columns(&actual));
        assert!(
            delta.exceeded,
            "an uncomparable decimal pair must fail rather than be guessed at"
        );
        assert!(
            delta
                .worst
                .as_deref()
                .is_some_and(|w| w.contains("common scale")),
            "the diagnostic must say why it could not be compared, got {:?}",
            delta.worst
        );
    }

    fn batch_of(name: &str, col: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            name,
            col.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![col]).expect("batch")
    }

    #[test]
    fn approximate_columns_flags_scale_mismatched_decimals() {
        // avg(): both decimal but different scale (Postgres ~13, DataFusion 6) ->
        // approximate. sum() (money, scale 2 both) and count (integer) stay
        // exact; a genuinely float column is always approximate.
        let expected = batch(vec![
            decimal_col("avg_amount", vec![1_i128], 38, 13),
            decimal_col("sum_amount", vec![1_i128], 38, 2),
            int_col("cnt", vec![1]),
            float_col("ratio", vec![1.0]),
        ]);
        let actual = batch(vec![
            decimal_col("avg_amount", vec![1_i128], 38, 6),
            decimal_col("sum_amount", vec![1_i128], 38, 2),
            int_col("cnt", vec![1]),
            float_col("ratio", vec![1.0]),
        ]);
        assert_eq!(
            approximate_columns(&expected, &actual),
            vec![true, false, false, true]
        );
    }

    #[test]
    fn approximate_columns_flags_same_scale_inflated_avg() {
        // MySQL regression (chbench_q1 `avg_amount`): both the source and
        // DataFusion produce AVG(NUMERIC(_,2)) at scale 6 (operand + 4), so the
        // scales *match*. The old "scales differ" heuristic left the column on
        // the exact path, and a 1-ULP rounding difference (959.717385 vs
        // 959.717384) tripped DIVERGE. Scale 6 > MONEY_SCALE (2) must now flag it
        // approximate, while the money SUM (scale 2) and count stay exact.
        let expected = batch(vec![
            decimal_col("avg_amount", vec![959_717_385_i128], 38, 6),
            decimal_col("sum_amount", vec![7_i128], 38, 2),
            int_col("count_order", vec![1]),
        ]);
        let actual = batch(vec![
            decimal_col("avg_amount", vec![959_717_384_i128], 38, 6),
            decimal_col("sum_amount", vec![7_i128], 38, 2),
            int_col("count_order", vec![1]),
        ]);
        assert_eq!(
            approximate_columns(&expected, &actual),
            vec![true, false, false],
            "same-scale inflated AVG must be approximate; SUM/COUNT stay exact"
        );

        // With that classification the 1-ULP avg difference is within the
        // relative float tolerance and must NOT fail the gate.
        let approx = approximate_columns(&expected, &actual);
        let delta = numeric_delta(&expected, &actual, &approx);
        assert!(
            !delta.exceeded,
            "same-scale AVG rounding must pass under relative tolerance: {:?}",
            delta.worst
        );
        assert!(delta.max_rel_delta > 0.0 && delta.max_rel_delta < FLOAT_REL_TOLERANCE);

        // But a genuine one-cent drift in the exact money SUM still fails, so the
        // fix does not weaken the exactness guarantee that matters.
        let bad_sum = batch(vec![
            decimal_col("avg_amount", vec![959_717_385_i128], 38, 6),
            decimal_col("sum_amount", vec![8_i128], 38, 2),
            int_col("count_order", vec![1]),
        ]);
        let delta = numeric_delta(&expected, &bad_sum, &approx);
        assert!(delta.exceeded, "a one-cent SUM drift must still DIVERGE");
    }

    #[test]
    fn aligned_avg_keeps_float_tolerance_when_actual_was_float() {
        // Reproduces the analytical gate after alignment: Spice computed avg()
        // as Float64, but the gate cast it to the source's Decimal128(NUMERIC),
        // so both compared columns are now decimal. A tiny rounding drift
        // (1000.0000001 vs 1000.0000002, rel 1e-10) must NOT fail when the
        // pre-alignment actual type was float...
        let e = batch(vec![decimal_col(
            "avg_qty",
            vec![10_000_000_001_i128],
            38,
            7,
        )]);
        let a = batch(vec![decimal_col(
            "avg_qty",
            vec![10_000_000_002_i128],
            38,
            7,
        )]);

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
