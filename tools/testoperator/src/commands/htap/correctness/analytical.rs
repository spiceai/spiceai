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
use arrow::array::RecordBatch;
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::{Field, Schema};
use arrow_tools::record_batch::try_cast_to;
use chbench_driver::ChBenchDriver;
use test_framework::anyhow;
use test_framework::queries::validation::{QueryValidationResult, validate_with_expected_batches};
use test_framework::queries::{QueryOverrides, get_chbench_test_queries};

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
/// value/row *divergence* on an advisory query (reported but non-gating —
/// execution errors on advisory queries still gate). Shared by `emit` and
/// `failure_message` so the two stay consistent.
fn is_gating_failure(result: &AnalyticalQueryResult) -> bool {
    match &result.outcome {
        Outcome::Pass => false,
        Outcome::Fail(_) if is_advisory(&result.name) => false,
        _ => true,
    }
}

/// Outcome for a single analytical query.
#[derive(Debug)]
pub enum Outcome {
    Pass,
    Fail(String),
    SourceError(String),
    SpiceError(String),
}

impl Outcome {
    fn label(&self) -> &'static str {
        match self {
            Outcome::Pass => "PASS",
            Outcome::Fail(_) => "FAIL",
            Outcome::SourceError(_) => "PG_ERROR",
            Outcome::SpiceError(_) => "SPICE_ERROR",
        }
    }

    fn detail(&self) -> Option<&str> {
        match self {
            Outcome::Pass => None,
            Outcome::Fail(m) | Outcome::SourceError(m) | Outcome::SpiceError(m) => Some(m),
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
                // non-gating; an execution error (PG or Spice) still gates so a
                // real regression on q15 is not silently hidden.
                Outcome::Fail(_) if is_advisory(&r.name) => {
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
                Outcome::Fail(m) => format!("{} mismatch: {m}", r.name),
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
        // Remember which columns Spice produced as floating point *before*
        // alignment casts them to the source schema (Float64 avg() → Decimal128
        // NUMERIC), so the numeric check below keeps the relative float
        // tolerance for those approximate columns instead of demoting them to
        // the exact integer/decimal path.
        let actual_source_floats = actual
            .first()
            .map(compare::float_columns)
            .unwrap_or_default();

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

        let (outcome, max_rel_delta) =
            if total_rows(&expected_sorted) == 0 && total_rows(&actual_sorted) == 0 {
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
                                let delta = compare::numeric_delta(e0, a0, &actual_source_floats);
                                if delta.exceeded {
                                    (
                                        Outcome::Fail(format!(
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
                    Ok(QueryValidationResult::Fail(reason)) => (
                        Outcome::Fail(format!(
                            "{reason:?} (source rows={}, spice rows={})",
                            total_rows(&expected_sorted),
                            total_rows(&actual_sorted),
                        )),
                        None,
                    ),
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

#[cfg(test)]
mod tests {
    use super::*;

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
            results: vec![result("chbench_q15", Outcome::Fail("NoAnswer".to_string()))],
        };
        assert!(report.failure_message().is_none());
    }

    #[test]
    fn non_advisory_divergence_gates() {
        let report = AnalyticalReport {
            results: vec![result("chbench_q1", Outcome::Fail("drift".to_string()))],
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
        assert!(report.failure_message().is_some());
    }
}
