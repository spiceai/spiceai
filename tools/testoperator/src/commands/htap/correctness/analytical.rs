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
use std::time::Instant;

use futures::StreamExt;

use super::super::spice::SpiceClients;
use super::compare;
use arrow::array::RecordBatch;
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::{Field, Schema};
use arrow_tools::record_batch::try_cast_to;
use chbench_driver::ChBenchDriver;
use test_framework::anyhow;
use test_framework::queries::validation::{QueryValidationResult, validate_with_expected_batches};
use test_framework::queries::{Query, QueryOverrides, get_chbench_test_queries};

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
///
/// Queries are streamed through [`StreamExt::buffered`] with a `concurrency`
/// window (controllable, at least one and never more than the query count), so
/// several queries run at once; each in-flight query runs the source and Spice
/// sides *in parallel* (see [`evaluate_query`]). `buffered` yields results in
/// submission order, so the returned report preserves the original query order
/// regardless of completion order.
pub async fn verify_analytical_results(
    driver: Arc<dyn ChBenchDriver>,
    spice: &SpiceClients,
    query_overrides: Option<QueryOverrides>,
    concurrency: usize,
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
    let n = queries.len();
    // At least one in-flight query, and never more than the query count.
    let workers = concurrency.clamp(1, n.max(1));
    println!(
        "\nRunning analytical-query gate over {n} queries ({workers} concurrent, source+Spice in parallel per query)"
    );

    // Stream the queries through a `buffered(workers)` window: up to `workers`
    // `evaluate_query` futures run concurrently and results are yielded in
    // submission order, so `collect` reconstructs the report in query order with
    // no per-index bookkeeping. The futures are polled in place (not spawned), so
    // they borrow `driver` and `spice` for the duration of this call — no
    // `'static`/`Send` bound and no cloning of the clients.
    let driver = &driver;
    let overall = Instant::now();
    let results: Vec<AnalyticalQueryResult> = futures::stream::iter(queries.iter())
        .map(|query| async move {
            // Logged when the query is admitted into the `buffered` window (only
            // `workers` are in flight at once), so CI output shows exactly which
            // queries started before a cancellation/timeout and which are still
            // waiting for a slot.
            println!("  [{}] dispatching (source + Spice)", query.name);
            let started = Instant::now();
            let result = evaluate_query(query, driver, spice).await;
            println!(
                "  [{}] done in {:.1}s — {}",
                query.name,
                started.elapsed().as_secs_f64(),
                result.outcome.label()
            );
            result
        })
        .buffered(workers)
        .collect()
        .await;
    println!(
        "Analytical-query gate finished all {n} queries in {:.1}s",
        overall.elapsed().as_secs_f64()
    );

    Ok(AnalyticalReport { results })
}

/// Evaluate a single analytical query: run it against the source and Spice **in
/// parallel**, then align, sort and compare the two result sets. Returns the
/// per-query [`AnalyticalQueryResult`]; the advisory/gating classification
/// happens later in [`AnalyticalReport`].
async fn evaluate_query(
    query: &Query,
    driver: &Arc<dyn ChBenchDriver>,
    spice: &SpiceClients,
) -> AnalyticalQueryResult {
    let name = query.name.to_string();
    let sql = query.to_sql_with_inlined_params();

    // Run both engines concurrently — the two are independent and each pulls its
    // own pooled connection, so there is no reason to serialize them. Each side
    // is timed and its completion logged, so a hang (e.g. the CI job is canceled
    // mid-gate with no report emitted) can be attributed to the source or the
    // Spice side of a named query rather than the whole gate. If both error, the
    // source error is reported (matching the previous sequential precedence,
    // which checked the source first).
    let (expected_res, actual_res) = tokio::join!(
        async {
            let t = Instant::now();
            let r = driver.query_arrow(sql.as_ref()).await;
            println!(
                "  [{name}] source {} in {:.1}s",
                if r.is_ok() { "ok" } else { "ERROR" },
                t.elapsed().as_secs_f64()
            );
            r
        },
        async {
            let t = Instant::now();
            let r = spice.query_arrow(sql.as_ref()).await;
            println!(
                "  [{name}] Spice {} in {:.1}s",
                if r.is_ok() { "ok" } else { "ERROR" },
                t.elapsed().as_secs_f64()
            );
            r
        }
    );

    let expected = match expected_res {
        Ok(batches) => batches,
        Err(e) => {
            return AnalyticalQueryResult {
                name,
                outcome: Outcome::SourceError(e.to_string()),
                max_rel_delta: None,
            };
        }
    };

    let actual = match actual_res {
        Ok(batches) => batches,
        Err(e) => {
            return AnalyticalQueryResult {
                name,
                outcome: Outcome::SpiceError(e.to_string()),
                max_rel_delta: None,
            };
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
            return AnalyticalQueryResult {
                name,
                outcome: Outcome::Fail(format!("schema align error: {e}")),
                max_rel_delta: None,
            };
        }
    };

    // Several benchmark queries have non-deterministic row order. Sort both sides by every column so comparison is set-based.
    let (expected_sorted, actual_sorted) = match sort_for_comparison(&expected, &actual) {
        Ok(pair) => pair,
        Err(e) => {
            return AnalyticalQueryResult {
                name,
                outcome: Outcome::Fail(format!("sort error: {e}")),
                max_rel_delta: None,
            };
        }
    };

    let (outcome, max_rel_delta) = if total_rows(&expected_sorted) == 0
        && total_rows(&actual_sorted) == 0
    {
        (Outcome::Pass, None)
    } else {
        match validate_with_expected_batches(query.name.as_ref(), &actual_sorted, &expected_sorted)
        {
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
            Ok(QueryValidationResult::Fail(reason)) => (
                Outcome::Divergence(format!(
                    "{reason:?} (source rows={}, spice rows={})",
                    total_rows(&expected_sorted),
                    total_rows(&actual_sorted),
                )),
                None,
            ),
            Err(e) => (Outcome::Fail(e.to_string()), None),
        }
    };

    AnalyticalQueryResult {
        name,
        outcome,
        max_rel_delta,
    }
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
}
