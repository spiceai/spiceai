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

use arrow::array::RecordBatch;
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::{Field, Schema};
use arrow_tools::record_batch::try_cast_to;
use chbench_driver::ChBenchDriver;
use futures::TryStreamExt;
use test_framework::anyhow;
use test_framework::queries::validation::{QueryValidationResult, validate_with_expected_batches};
use test_framework::queries::{QueryOverrides, get_chbench_test_queries};

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
}

#[derive(Debug)]
pub struct AnalyticalReport {
    pub results: Vec<AnalyticalQueryResult>,
}

impl AnalyticalReport {
    pub fn emit(&self) {
        println!("\nAnalytical Query Correctness");
        println!("  {:<14} {:>12}", "query", "outcome");

        let mut passed: u64 = 0;
        let mut failed: u64 = 0;
        for r in &self.results {
            println!("  {:<14} {:>12}", r.name, r.outcome.label());
            if let Some(detail) = r.outcome.detail() {
                println!("    └─ {detail}");
            }
            if matches!(r.outcome, Outcome::Pass) {
                passed += 1;
            } else {
                failed += 1;
            }
        }

        let total = passed + failed;
        if failed == 0 {
            println!("  verdict: PASSED — {passed}/{total} queries match");
        } else {
            println!("  verdict: FAILED — {failed}/{total} queries diverged");
        }
    }

    /// Returns a single joined failure summary, or `None` if every query passed.
    pub fn failure_message(&self) -> Option<String> {
        let problems: Vec<String> = self
            .results
            .iter()
            .filter(|r| !matches!(r.outcome, Outcome::Pass))
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
    spice_client: &spiceai::Client,
    query_overrides: Option<QueryOverrides>,
) -> anyhow::Result<AnalyticalReport> {
    let queries = get_chbench_test_queries(query_overrides);
    // q15 compares `total_revenue = (SELECT MAX(total_revenue) ...)` over a SUM
    // of DOUBLE PRECISION values might return 0 rows: https://github.com/spiceai/spiceai/issues/11212
    let queries: Vec<_> = queries
        .into_iter()
        .filter(|q| q.name.as_ref() != "chbench_q15")
        .collect();
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
                });
                continue;
            }
        };

        let actual = match run_spice_query(spice_client, sql.as_ref()).await {
            Ok(batches) => batches,
            Err(e) => {
                results.push(AnalyticalQueryResult {
                    name: query.name.to_string(),
                    outcome: Outcome::SpiceError(e.to_string()),
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
        let actual = match align_to_expected_schema(&actual, &expected) {
            Ok(batches) => batches,
            Err(e) => {
                results.push(AnalyticalQueryResult {
                    name: query.name.to_string(),
                    outcome: Outcome::Fail(format!("schema align error: {e}")),
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
                });
                continue;
            }
        };

        let outcome = if total_rows(&expected_sorted) == 0 && total_rows(&actual_sorted) == 0 {
            Outcome::Pass
        } else {
            match validate_with_expected_batches(
                query.name.as_ref(),
                &actual_sorted,
                &expected_sorted,
            ) {
                Ok(QueryValidationResult::Pass) => Outcome::Pass,
                Ok(QueryValidationResult::Fail(reason)) => Outcome::Fail(format!(
                    "{reason:?} (source rows={}, spice rows={})",
                    total_rows(&expected_sorted),
                    total_rows(&actual_sorted),
                )),
                Err(e) => Outcome::Fail(e.to_string()),
            }
        };

        results.push(AnalyticalQueryResult {
            name: query.name.to_string(),
            outcome,
        });
    }

    Ok(AnalyticalReport { results })
}

async fn run_spice_query(client: &spiceai::Client, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    let stream = client.sql(sql).await?;
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    Ok(batches)
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
