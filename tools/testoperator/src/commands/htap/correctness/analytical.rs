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

//! Analytical-query gate for HTAP benchmarks.
//!
//! Runs after the row-count gate (`correctness::verify_after_drain`) confirms
//! convergence. Executes each CH-benCH analytical query against both the source
//! Postgres and Spice, comparing results with the existing
//! `validate_with_expected_batches` comparator (schema equivalence + 5%
//! numeric tolerance).

use std::sync::Arc;

use super::super::spice::SpiceClients;
use super::compare;
use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use arrow_tools::record_batch::try_cast_to;
use chbench_driver::ChBenchDriver;
use test_framework::anyhow;
use test_framework::queries::validation::{
    QueryValidationFailReason, QueryValidationResult, validate_with_expected_batches,
};
use test_framework::queries::{QueryOverrides, get_chbench_test_queries};

/// Number of rows of context printed on either side of an analytical-gate
/// mismatch, from both the reference (source) and Spice result sets.
const MISMATCH_CONTEXT_ROWS: usize = 10;

/// Analytical queries that are executed and reported but do **not** gate the
/// build, because their result is sensitive to floating-point summation order
/// rather than to engine correctness.
///
/// `chbench_q15` selects rows via `total_revenue = (SELECT MAX(total_revenue)
/// ...)` — a knife-edge equality over `SUM(ol_amount)`, where `ol_amount` is
/// `DOUBLE PRECISION`. Postgres and Cayenne can accumulate that floating sum in
/// a different order, so the equality can admit a different number of rows on
/// each engine even when both sums are numerically correct (the CTE is also
/// re-evaluated for the subquery, so the two sides need not even agree within a
/// single engine). This is a property of the query, not a divergence in the
/// data (<https://github.com/spiceai/spiceai/issues/11212>), so the gate
/// surfaces any difference but never fails on it.
const ADVISORY_QUERIES: &[&str] = &["chbench_q15"];

fn is_advisory(name: &str) -> bool {
    ADVISORY_QUERIES.contains(&name)
}

/// Whether a result should fail the gate: any non-`Pass` outcome, except a
/// value/row [`Outcome::Divergence`] on an advisory query (reported but
/// non-gating). A harness error ([`Outcome::Fail`]) or an execution error
/// ([`Outcome::SourceError`] / [`Outcome::SpiceError`]) gates even on an
/// advisory query, so a real regression on q15 is never silently hidden.
/// `emit` mirrors this same classification.
fn is_gating_failure(result: &AnalyticalQueryResult) -> bool {
    match &result.outcome {
        Outcome::Pass => false,
        Outcome::Divergence(_) if is_advisory(&result.name) => false,
        _ => true,
    }
}

/// Outcome for a single analytical query.
#[derive(Debug)]
pub enum Outcome {
    Pass,
    /// Value/row mismatch vs the source: numeric drift over tolerance, or a
    /// row-set / `NoAnswer` divergence from the validator. Advisory-eligible
    /// (see [`ADVISORY_QUERIES`]) because an FP-summation-order artifact on an
    /// advisory query is a property of the query, not an engine bug.
    Divergence(String),
    /// Harness / internal error (schema-align, sort, validator). Always gates,
    /// even on an advisory query — it is never an FP artifact.
    Fail(String),
    SourceError(String),
    SpiceError(String),
}

impl Outcome {
    fn label(&self) -> &'static str {
        match self {
            Outcome::Pass => "PASS",
            Outcome::Divergence(_) => "DIVERGE",
            Outcome::Fail(_) => "FAIL",
            Outcome::SourceError(_) => "PG_ERROR",
            Outcome::SpiceError(_) => "SPICE_ERROR",
        }
    }

    fn detail(&self) -> Option<&str> {
        match self {
            Outcome::Pass => None,
            Outcome::Divergence(m)
            | Outcome::Fail(m)
            | Outcome::SourceError(m)
            | Outcome::SpiceError(m) => Some(m),
        }
    }
}

#[derive(Debug)]
pub struct AnalyticalQueryResult {
    pub name: String,
    pub outcome: Outcome,
    /// Largest relative numeric delta vs the source for this query, as a
    /// fraction (e.g. `0.012` = 1.2%). `None` when the query errored or
    /// produced no rows to compare. Surfaced so sub-tolerance drift is visible.
    pub max_rel_delta: Option<f64>,
}

#[derive(Debug)]
pub struct AnalyticalReport {
    pub results: Vec<AnalyticalQueryResult>,
}

impl AnalyticalReport {
    pub fn emit(&self) {
        println!("\nAnalytical Query Correctness");
        println!("  {:<14} {:>12} {:>10}", "query", "outcome", "max Δ%");

        let mut passed: u64 = 0;
        let mut failed: u64 = 0;
        let mut advisory: u64 = 0;
        for r in &self.results {
            let delta = r
                .max_rel_delta
                .map_or_else(|| "-".to_string(), |d| format!("{:.4}", d * 100.0));
            println!("  {:<14} {:>12} {:>10}", r.name, r.outcome.label(), delta);
            if let Some(detail) = r.outcome.detail() {
                println!("    └─ {detail}");
            }
            match &r.outcome {
                Outcome::Pass => passed += 1,
                // Only a value/row *divergence* on an advisory query is
                // non-gating; harness/execution errors still gate so a real
                // regression on q15 is not silently hidden.
                Outcome::Divergence(_) if is_advisory(&r.name) => {
                    advisory += 1;
                    println!(
                        "       (advisory — floating-point summation-order artifact, not gated; see https://github.com/spiceai/spiceai/issues/11212)"
                    );
                }
                _ => failed += 1,
            }
        }

        let total = passed + failed + advisory;
        if failed == 0 {
            if advisory == 0 {
                println!("  verdict: PASSED — {passed}/{total} queries match");
            } else {
                println!(
                    "  verdict: PASSED — {passed}/{total} queries match ({advisory} advisory, non-gating)"
                );
            }
        } else {
            println!("  verdict: FAILED — {failed}/{total} queries diverged");
        }
    }

    /// Returns a single joined failure summary, or `None` if every query passed.
    pub fn failure_message(&self) -> Option<String> {
        let problems: Vec<String> = self
            .results
            .iter()
            .filter(|r| is_gating_failure(r))
            .map(|r| match &r.outcome {
                Outcome::Pass => unreachable!(),
                Outcome::Divergence(m) => format!("{} divergence: {m}", r.name),
                Outcome::Fail(m) => format!("{} error: {m}", r.name),
                Outcome::SourceError(m) => format!("{} source error: {m}", r.name),
                Outcome::SpiceError(m) => format!("{} spice error: {m}", r.name),
            })
            .collect();

        if problems.is_empty() {
            None
        } else {
            Some(format!(
                "HTAP analytical-query gate failed:\n  {}",
                problems.join("\n  ")
            ))
        }
    }
}

/// Run every CH-benCH analytical query against both the source and Spice,
/// comparing results.
pub async fn verify_analytical_results(
    driver: Arc<dyn ChBenchDriver>,
    spice: &SpiceClients,
    query_overrides: Option<QueryOverrides>,
) -> anyhow::Result<AnalyticalReport> {
    // All 22 CH-benCH analytical queries are executed and reported. q15 is
    // advisory (run but non-gating — see `ADVISORY_QUERIES`): its
    // `total_revenue = (SELECT MAX(total_revenue) ...)` is a knife-edge equality
    // over a floating SUM of DOUBLE PRECISION `ol_amount`
    // (https://github.com/spiceai/spiceai/issues/11212). The source and Spice
    // can accumulate that sum in a different order, so the predicate can admit a
    // different number of rows even when both sums are numerically correct — a
    // query artifact, not an engine divergence, so it is surfaced but not gated.
    let queries = get_chbench_test_queries(query_overrides);
    println!(
        "\nRunning analytical-query gate over {} queries",
        queries.len()
    );

    let mut results = Vec::with_capacity(queries.len());
    for query in &queries {
        let sql = query.to_sql_with_inlined_params();

        let expected = match driver.query_arrow(sql.as_ref()).await {
            Ok(batches) => batches,
            Err(e) => {
                results.push(AnalyticalQueryResult {
                    name: query.name.to_string(),
                    outcome: Outcome::SourceError(e.to_string()),
                    max_rel_delta: None,
                });
                continue;
            }
        };

        let actual = match spice.query_arrow(sql.as_ref()).await {
            Ok(batches) => batches,
            Err(e) => {
                results.push(AnalyticalQueryResult {
                    name: query.name.to_string(),
                    outcome: Outcome::SpiceError(e.to_string()),
                    max_rel_delta: None,
                });
                continue;
            }
        };

        // Spice and the source connector emit different physical Arrow types
        // for the same logical SQL columns (e.g. Cayenne stores timestamps as
        // Microsecond while PG arrow streams produce Nanosecond; aggregate
        // expressions return Int32 vs Decimal128(38,0)). Cast Spice columns
        // to the source's per-column type so the string-based row comparator
        // sees consistent encodings before comparing values.
        // Columns to compare with relative float tolerance, captured pre-alignment
        // (alignment casts actual to the source schema). Covers Spice floats and
        // avg()/division emitted as different-scale decimals; sums/counts stay exact.
        let approximate_cols = match (expected.first(), actual.first()) {
            (Some(e0), Some(a0)) => compare::approximate_columns(e0, a0),
            _ => Vec::new(),
        };

        let actual = match align_to_expected_schema(&actual, &expected) {
            Ok(batches) => batches,
            Err(e) => {
                results.push(AnalyticalQueryResult {
                    name: query.name.to_string(),
                    outcome: Outcome::Fail(format!("schema align error: {e}")),
                    max_rel_delta: None,
                });
                continue;
            }
        };

        // Several benchmark queries have non-deterministic row order. Sort both sides by every column so comparison is set-based.
        let (expected_sorted, actual_sorted) = match sort_for_comparison(&expected, &actual) {
            Ok(pair) => pair,
            Err(e) => {
                results.push(AnalyticalQueryResult {
                    name: query.name.to_string(),
                    outcome: Outcome::Fail(format!("sort error: {e}")),
                    max_rel_delta: None,
                });
                continue;
            }
        };

        let (outcome, max_rel_delta) = if total_rows(&expected_sorted) == 0
            && total_rows(&actual_sorted) == 0
        {
            (Outcome::Pass, None)
        } else {
            match validate_with_expected_batches(
                query.name.as_ref(),
                &actual_sorted,
                &expected_sorted,
            ) {
                Ok(QueryValidationResult::Pass) => {
                    // Structure, schema and row set agree within the string
                    // comparator's tolerance. Now apply the tight, type-aware
                    // numeric check (exact for integer/decimal, 0.1% for
                    // float — including avg() that alignment cast to decimal)
                    // and surface the magnitude either way.
                    match (expected_sorted.first(), actual_sorted.first()) {
                        (Some(e0), Some(a0)) => {
                            let delta = compare::numeric_delta(e0, a0, &approximate_cols);
                            if delta.exceeded {
                                if let Some(row) = delta.worst_row {
                                    let column = delta
                                        .worst_col
                                        .and_then(|c| e0.schema().fields().get(c).cloned())
                                        .map(|f| f.name().clone());
                                    print_mismatch_context(
                                        query.name.as_ref(),
                                        e0,
                                        a0,
                                        row,
                                        column.as_deref(),
                                    );
                                }
                                (
                                    Outcome::Divergence(format!(
                                        "numeric drift exceeds tolerance — {}",
                                        delta.worst.as_deref().unwrap_or("(unknown cell)")
                                    )),
                                    Some(delta.max_rel_delta),
                                )
                            } else {
                                (Outcome::Pass, Some(delta.max_rel_delta))
                            }
                        }
                        _ => (Outcome::Pass, None),
                    }
                }
                Ok(QueryValidationResult::Fail(reason)) => {
                    // Print the surrounding rows from both sides so a
                    // divergence can be inspected in context rather than as a
                    // lone cell. A `DataMismatch` carries the exact 1-based row
                    // and column of the first disagreement; a `RowCountMismatch`
                    // has no single cell, so center the window on the boundary
                    // where the shorter (lex-sorted) side ends — the first row
                    // index present on only one side. Other reasons (schema, no
                    // answer) have no meaningful row to center on.
                    if let (Some(e0), Some(a0)) = (expected_sorted.first(), actual_sorted.first()) {
                        match &reason {
                            QueryValidationFailReason::DataMismatch {
                                row_number, column, ..
                            } => print_mismatch_context(
                                query.name.as_ref(),
                                e0,
                                a0,
                                row_number.saturating_sub(1),
                                Some(column),
                            ),
                            QueryValidationFailReason::RowCountMismatch { .. } => {
                                print_mismatch_context(
                                    query.name.as_ref(),
                                    e0,
                                    a0,
                                    e0.num_rows().min(a0.num_rows()),
                                    None,
                                );
                            }
                            _ => {}
                        }
                    }
                    (
                        Outcome::Divergence(format!(
                            "{reason:?} (source rows={}, spice rows={})",
                            total_rows(&expected_sorted),
                            total_rows(&actual_sorted),
                        )),
                        None,
                    )
                }
                Err(e) => (Outcome::Fail(e.to_string()), None),
            }
        };

        results.push(AnalyticalQueryResult {
            name: query.name.to_string(),
            outcome,
            max_rel_delta,
        });
    }

    Ok(AnalyticalReport { results })
}

/// Build a target schema by taking each field from `actual` and replacing its
/// data type with the same-position field type from `expected`, then cast
/// `actual`'s batches to that schema. Names and nullability are preserved from
/// `actual` so the string-based row comparator (which already tolerates name /
/// nullable differences) keeps its existing tolerance.
///
/// If column counts differ or no column needs casting, `actual` is returned
/// unchanged.
fn align_to_expected_schema(
    actual: &[RecordBatch],
    expected: &[RecordBatch],
) -> anyhow::Result<Vec<RecordBatch>> {
    let Some(expected_schema) = expected.first().map(RecordBatch::schema) else {
        return Ok(actual.to_vec());
    };
    let Some(actual_schema) = actual.first().map(RecordBatch::schema) else {
        return Ok(actual.to_vec());
    };
    if expected_schema.fields().len() != actual_schema.fields().len() {
        return Ok(actual.to_vec());
    }

    let mut needs_cast = false;
    let mut target_fields = Vec::with_capacity(actual_schema.fields().len());
    for (af, ef) in actual_schema
        .fields()
        .iter()
        .zip(expected_schema.fields().iter())
    {
        if af.data_type() == ef.data_type() {
            target_fields.push(af.as_ref().clone());
        } else {
            needs_cast = true;
            target_fields.push(Field::new(
                af.name(),
                ef.data_type().clone(),
                af.is_nullable(),
            ));
        }
    }

    if !needs_cast {
        return Ok(actual.to_vec());
    }

    let target_schema = Arc::new(Schema::new(target_fields));
    actual
        .iter()
        .map(|batch| try_cast_to(batch.clone(), Arc::clone(&target_schema)).map_err(Into::into))
        .collect()
}

/// Concatenate each side into a single batch and lex-sort both by every projected column.
fn sort_for_comparison(
    expected: &[RecordBatch],
    actual: &[RecordBatch],
) -> anyhow::Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    Ok((sort_all_columns(expected)?, sort_all_columns(actual)?))
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn sort_all_columns(batches: &[RecordBatch]) -> anyhow::Result<Vec<RecordBatch>> {
    let Some(schema) = batches.first().map(RecordBatch::schema) else {
        return Ok(vec![]);
    };
    let combined = concat_batches(&schema, batches)?;
    if combined.num_rows() < 2 {
        return Ok(vec![combined]);
    }

    let sort_columns: Vec<SortColumn> = combined
        .columns()
        .iter()
        .map(|c| SortColumn {
            values: Arc::clone(c),
            options: None,
        })
        .collect();
    let indices = lexsort_to_indices(&sort_columns, None)?;
    let new_columns = combined
        .columns()
        .iter()
        .map(|c| take(c, &indices, None))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(vec![RecordBatch::try_new(schema, new_columns)?])
}

/// Print up to [`MISMATCH_CONTEXT_ROWS`] rows on either side of `mismatch_row`
/// (0-based, into the shared lex-sorted row order) from both the reference
/// (source) and Spice result sets, so a divergence can be inspected in context
/// rather than from a single offending cell. `column` names the diverging
/// column when known.
///
/// Each side is rendered as its own table via Arrow's `pretty_format_batches`,
/// with a prepended absolute `row` index column so the mismatch row (called out
/// in the header) can be located and the two tables lined up. The window is
/// clamped to the rows each side actually has; the shorter side of a row-count
/// divergence simply prints fewer rows.
fn print_mismatch_context(
    query_name: &str,
    expected: &RecordBatch,
    actual: &RecordBatch,
    mismatch_row: usize,
    column: Option<&str>,
) {
    let total = expected.num_rows().max(actual.num_rows());
    if total == 0 {
        return;
    }
    let last = total - 1;
    let lo = mismatch_row.saturating_sub(MISMATCH_CONTEXT_ROWS);
    let hi = mismatch_row.saturating_add(MISMATCH_CONTEXT_ROWS).min(last);

    let column_note = column.map_or_else(String::new, |c| format!(", diverging column '{c}'"));
    println!(
        "    ── {query_name} mismatch context: rows {lo}..={hi} of {total} (0-based, lex-sorted), mismatch at row {mismatch_row}{column_note} ──"
    );

    for (label, batch) in [
        ("reference (source of truth, Postgres)", expected),
        ("spice (Spice)", actual),
    ] {
        println!("    {label}:");
        print_windowed_table(batch, lo, hi);
    }
}

/// Slice `batch` to the inclusive absolute row range `[lo, hi]` (clamped to the
/// rows this side actually has), prepend an absolute `row` index column, and
/// print it as a table via Arrow's `pretty_format_batches`, indented under the
/// caller's label.
fn print_windowed_table(batch: &RecordBatch, lo: usize, hi: usize) {
    let n = batch.num_rows();
    if lo >= n {
        println!("      <no rows in window>");
        return;
    }
    let len = hi.min(n - 1) - lo + 1;
    let sliced = batch.slice(lo, len);

    // Prepend the absolute row index so the mismatch row can be identified in
    // the rendered table and the two sides lined up row-for-row.
    let row_index =
        Int64Array::from_iter_values((lo..lo + len).map(|r| i64::try_from(r).unwrap_or(i64::MAX)));
    let mut fields = Vec::with_capacity(sliced.num_columns() + 1);
    fields.push(Field::new("row", DataType::Int64, false));
    fields.extend(sliced.schema().fields().iter().map(|f| f.as_ref().clone()));
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(sliced.num_columns() + 1);
    columns.push(Arc::new(row_index) as ArrayRef);
    columns.extend(sliced.columns().iter().map(Arc::clone));

    let windowed = match RecordBatch::try_new(Arc::new(Schema::new(fields)), columns) {
        Ok(b) => b,
        Err(e) => {
            println!("      <failed to build context window: {e}>");
            return;
        }
    };

    match pretty_format_batches(std::slice::from_ref(&windowed)) {
        Ok(table) => {
            for line in table.to_string().lines() {
                println!("      {line}");
            }
        }
        Err(e) => println!("      <failed to render context window: {e}>"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    fn result(name: &str, outcome: Outcome) -> AnalyticalQueryResult {
        AnalyticalQueryResult {
            name: name.to_string(),
            outcome,
            max_rel_delta: None,
        }
    }

    #[test]
    fn only_q15_is_advisory() {
        assert!(is_advisory("chbench_q15"));
        assert!(!is_advisory("chbench_q1"));
        assert!(!is_advisory("chbench_q14"));
    }

    #[test]
    fn advisory_divergence_does_not_gate() {
        // q15's row/value divergence is reported but must not fail the gate.
        let report = AnalyticalReport {
            results: vec![result(
                "chbench_q15",
                Outcome::Divergence("NoAnswer (source rows=1, spice rows=0)".to_string()),
            )],
        };
        assert!(report.failure_message().is_none());
    }

    #[test]
    fn non_advisory_divergence_gates() {
        let report = AnalyticalReport {
            results: vec![result(
                "chbench_q1",
                Outcome::Divergence("drift".to_string()),
            )],
        };
        let msg = report
            .failure_message()
            .expect("a non-advisory divergence must gate the build");
        assert!(msg.contains("chbench_q1"));
    }

    #[test]
    fn advisory_execution_error_still_gates() {
        // Only a value/row divergence is non-gating; an execution error on an
        // advisory query is a real regression and must still fail the gate.
        let report = AnalyticalReport {
            results: vec![result(
                "chbench_q15",
                Outcome::SpiceError("query failed".to_string()),
            )],
        };
        let msg = report
            .failure_message()
            .expect("an execution error on an advisory query must still gate");
        assert!(msg.contains("chbench_q15"));
    }

    #[test]
    fn advisory_harness_error_still_gates() {
        // A harness/internal error (schema-align, sort, validator) is encoded as
        // Outcome::Fail, not Divergence, and must gate even for q15 — it is never
        // an FP-summation-order artifact.
        let report = AnalyticalReport {
            results: vec![result(
                "chbench_q15",
                Outcome::Fail("schema align error: column count mismatch".to_string()),
            )],
        };
        let msg = report
            .failure_message()
            .expect("a harness error on an advisory query must still gate");
        assert!(msg.contains("chbench_q15"));
    }

    fn ctx_batch() -> RecordBatch {
        let ids: ArrayRef = Arc::new(Int64Array::from((0..5).collect::<Vec<_>>()));
        let cities: ArrayRef = Arc::new(StringArray::from(vec![
            Some("Berlin"),
            None,
            Some("Munich"),
            Some("Hamburg"),
            Some("Cologne"),
        ]));
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("city", DataType::Utf8, true),
            ])),
            vec![ids, cities],
        )
        .expect("valid batch")
    }

    #[test]
    fn mismatch_context_clamps_window_to_bounds() {
        // Must not panic when the mismatch sits at row 0 (lo underflow) or beyond
        // the end; the window is clamped to the rows present.
        let batch = ctx_batch();
        print_mismatch_context("chbench_q10", &batch, &batch, 0, Some("city"));
        print_mismatch_context("chbench_q10", &batch, &batch, 999, Some("city"));
        // Empty batches print nothing rather than underflowing `total - 1`.
        let empty = RecordBatch::new_empty(batch.schema());
        print_mismatch_context("chbench_q10", &empty, &empty, 0, None);
    }

    #[test]
    fn mismatch_context_handles_row_count_divergence() {
        // The RowCountMismatch path centers on min(expected, actual) rows, which
        // sits at (or past) the shorter side's end — must not panic and must
        // clamp each side to its own length.
        let full = ctx_batch(); // 5 rows
        let short = full.slice(0, 2); // 2 rows
        let boundary = full.num_rows().min(short.num_rows()); // 2
        print_mismatch_context("chbench_q10", &full, &short, boundary, None);
        print_mismatch_context("chbench_q10", &short, &full, boundary, None);
    }

    #[test]
    fn windowed_table_handles_unequal_side_lengths() {
        // The shorter side of a row-count divergence must not panic: a window
        // past its end prints "<no rows in window>", a partial window is clamped.
        let full = ctx_batch(); // 5 rows
        let short = full.slice(0, 2); // 2 rows
        print_windowed_table(&full, 0, 4);
        print_windowed_table(&short, 0, 4); // hi clamped to row 1
        print_windowed_table(&short, 3, 4); // lo past end → no rows
    }
}
