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

//! Full-text-search correctness gate for the `chbench-fts` query set
//! (`chbench_fts1`/`fts2`/`fts3` — see `crates/test-framework/src/queries/chbench_fts/`).
//!
//! The analytical gate (`correctness::analytical`) can't cover these: it
//! compares a query's result against the *source* engine, and Postgres/`MySQL`
//! have no `text_search` UDTF. Instead each query is checked against a
//! deterministic expectation:
//!   * `fts1`/`fts2` search `nation`/`region` — static seed data untouched by
//!     the OLTP workload — so the matching keys are fixed and asserted
//!     directly.
//!   * `fts3` searches `customer.c_data`, which the TPC-C payment transaction
//!     mutates by prepending a whitespace-tokenized prefix to bad-credit
//!     customers (`tools/chbench-driver/src/txn/payment.rs`). The exact set of
//!     customers that must match is therefore recomputed from the source's
//!     current `c_data` and compared against Spice's search result as a set —
//!     this is what actually exercises the CDC-fed full-text index rather than
//!     letting a missing/stale index pass silently.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use chbench_driver::ChBenchDriver;
use test_framework::anyhow;

use super::super::spice::SpiceClients;

/// Fixed TPC-H `nation` rows matching the full-text term `'united'` over
/// `n_name` (see `csv_gen::NATIONS`) — `nation` is static seed data (25 rows)
/// that the OLTP workload never touches, so these keys are asserted directly
/// rather than recomputed from the source.
const EXPECTED_NATION_HITS: &[(i64, &str)] = &[(23, "UNITED KINGDOM"), (24, "UNITED STATES")];

/// Fixed TPC-H `region` row matching the full-text term `'america'` over
/// `r_name` (see `csv_gen::REGIONS`).
const EXPECTED_REGION_HITS: &[(i64, &str)] = &[(1, "AMERICA")];

/// Outcome of a single `chbench-fts` query's correctness check.
#[derive(Debug)]
pub enum FtsOutcome {
    Pass,
    Fail(String),
}

#[derive(Debug)]
pub struct FtsQueryResult {
    pub name: String,
    pub outcome: FtsOutcome,
}

#[derive(Debug)]
pub struct FtsReport {
    pub results: Vec<FtsQueryResult>,
}

impl FtsReport {
    pub fn emit(&self) {
        println!("\nFull-Text-Search Correctness");
        println!("  {:<14} {:>8}", "query", "outcome");

        let mut failed: u64 = 0;
        for r in &self.results {
            let label = match &r.outcome {
                FtsOutcome::Pass => "PASS",
                FtsOutcome::Fail(_) => "FAIL",
            };
            println!("  {:<14} {:>8}", r.name, label);
            if let FtsOutcome::Fail(detail) = &r.outcome {
                failed += 1;
                println!("    └─ {detail}");
            }
        }

        let total = self.results.len();
        if failed == 0 {
            println!("  verdict: PASSED — {total}/{total} queries match");
        } else {
            println!("  verdict: FAILED — {failed}/{total} queries diverged");
        }
    }

    /// Returns a single joined failure summary, or `None` if every query passed.
    pub fn failure_message(&self) -> Option<String> {
        let problems: Vec<String> = self
            .results
            .iter()
            .filter_map(|r| match &r.outcome {
                FtsOutcome::Pass => None,
                FtsOutcome::Fail(m) => Some(format!("{}: {m}", r.name)),
            })
            .collect();

        if problems.is_empty() {
            None
        } else {
            Some(format!(
                "HTAP full-text-search gate failed:\n  {}",
                problems.join("\n  ")
            ))
        }
    }
}

/// Run the three `chbench-fts` full-text queries against Spice and check each
/// against a deterministic expectation. See the module docs for why this is a
/// fixed-expectation check rather than a source/Spice comparison.
pub async fn verify_fts_results(
    driver: &Arc<dyn ChBenchDriver>,
    spice: &SpiceClients,
) -> anyhow::Result<FtsReport> {
    let results = vec![
        verify_static_hits(
            spice,
            "chbench_fts1",
            "nation",
            "n_nationkey",
            "n_name",
            "united",
            EXPECTED_NATION_HITS,
        )
        .await,
        verify_static_hits(
            spice,
            "chbench_fts2",
            "region",
            "r_regionkey",
            "r_name",
            "america",
            EXPECTED_REGION_HITS,
        )
        .await,
        verify_customer_hits(driver, spice).await,
    ];

    Ok(FtsReport { results })
}

/// Check a full-text search over a static table (`nation`/`region`) against a
/// fixed set of expected `(key, name)` hits. No `LIMIT`/`ORDER BY` — the gate
/// needs the full match set, not the top-N the benchmark query displays.
async fn verify_static_hits(
    spice: &SpiceClients,
    query_name: &str,
    table: &str,
    key_col: &str,
    name_col: &str,
    term: &str,
    expected: &[(i64, &str)],
) -> FtsQueryResult {
    let name = query_name.to_string();
    let sql =
        format!("SELECT {key_col}, {name_col} FROM text_search({table}, '{term}', {name_col})");

    let batches = match spice.query_arrow(&sql).await {
        Ok(batches) => batches,
        Err(e) => {
            return FtsQueryResult {
                name,
                outcome: FtsOutcome::Fail(format!("spice query failed: {e}")),
            };
        }
    };

    let mut actual = match extract_key_name_pairs(&batches) {
        Ok(pairs) => pairs,
        Err(e) => {
            return FtsQueryResult {
                name,
                outcome: FtsOutcome::Fail(e.to_string()),
            };
        }
    };
    actual.sort();

    let mut expected: Vec<(i64, String)> = expected
        .iter()
        .map(|(k, n)| (*k, (*n).to_string()))
        .collect();
    expected.sort();

    if actual == expected {
        FtsQueryResult {
            name,
            outcome: FtsOutcome::Pass,
        }
    } else {
        FtsQueryResult {
            name,
            outcome: FtsOutcome::Fail(format!(
                "expected hits {expected:?}, spice full-text search over {table}.{name_col} \
                 returned {actual:?} — a missing expected hit means the full-text index is \
                 stale or was never populated"
            )),
        }
    }
}

/// Check the `customer.c_data` full-text search against the set of customers
/// whose *current source* `c_data` actually contains the search term as a
/// whitespace-delimited token — an exact reproduction of how the payment
/// transaction mutates bad-credit customers, so this is a like-for-like
/// recomputation of the expected match set, not an approximation of it.
async fn verify_customer_hits(
    driver: &Arc<dyn ChBenchDriver>,
    spice: &SpiceClients,
) -> FtsQueryResult {
    const TERM: &str = "5";

    let name = "chbench_fts3".to_string();

    // Only bad-credit ("BC") customers ever have the payment prefix prepended
    // (tools/chbench-driver/src/txn/payment.rs); everyone else's c_data is
    // untouched random alnum seed data (chbench_driver::rand::rand_chars —
    // no whitespace in its character set), which can never contain an
    // isolated whitespace-bounded token and so can never spuriously match.
    let source_rows = match driver
        .query_arrow("SELECT c_w_id, c_d_id, c_id, c_data FROM customer WHERE c_credit = 'BC'")
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            return FtsQueryResult {
                name,
                outcome: FtsOutcome::Fail(format!("source query failed: {e}")),
            };
        }
    };

    let expected = match customer_keys_matching_token(&source_rows, TERM) {
        Ok(keys) => keys,
        Err(e) => {
            return FtsQueryResult {
                name,
                outcome: FtsOutcome::Fail(e.to_string()),
            };
        }
    };

    // No LIMIT/ORDER BY, unlike the benchmark's chbench_fts3.sql — the
    // correctness gate needs the full match set to compare as a set.
    let actual_rows = match spice
        .query_arrow(&format!(
            "SELECT c_w_id, c_d_id, c_id FROM text_search(customer, '{TERM}', c_data)"
        ))
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            return FtsQueryResult {
                name,
                outcome: FtsOutcome::Fail(format!("spice query failed: {e}")),
            };
        }
    };

    let actual = match extract_customer_keys(&actual_rows) {
        Ok(keys) => keys,
        Err(e) => {
            return FtsQueryResult {
                name,
                outcome: FtsOutcome::Fail(e.to_string()),
            };
        }
    };

    if actual == expected {
        FtsQueryResult {
            name,
            outcome: FtsOutcome::Pass,
        }
    } else {
        let missing: Vec<_> = expected.difference(&actual).take(10).collect();
        let extra: Vec<_> = actual.difference(&expected).take(10).collect();
        FtsQueryResult {
            name,
            outcome: FtsOutcome::Fail(format!(
                "source has {} customer(s) whose c_data contains the token '{TERM}', spice \
                 full-text search returned {} — missing (source has, spice doesn't; up to 10 \
                 shown): {missing:?}; extra (spice has, source doesn't; up to 10 shown): \
                 {extra:?}. A non-empty 'missing' set means the CDC-maintained full-text index \
                 over customer.c_data is stale.",
                expected.len(),
                actual.len(),
            )),
        }
    }
}

/// A customer's TPC-C primary key: `(c_w_id, c_d_id, c_id)`.
type CustomerKey = (i64, i64, i64);

/// Rows whose `c_data` (4th column) contains `token` as an exact
/// whitespace-delimited word — matching the space-padded integer fields the
/// payment transaction prepends, not a substring match.
fn customer_keys_matching_token(
    batches: &[RecordBatch],
    token: &str,
) -> anyhow::Result<BTreeSet<CustomerKey>> {
    let mut keys = BTreeSet::new();
    for batch in batches {
        anyhow::ensure!(
            batch.num_columns() >= 4,
            "expected columns (c_w_id, c_d_id, c_id, c_data), got {} columns",
            batch.num_columns()
        );
        let w = to_i64_array(batch.column(0))?;
        let d = to_i64_array(batch.column(1))?;
        let c = to_i64_array(batch.column(2))?;
        let data = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("c_data column is not Utf8"))?;

        for row in 0..batch.num_rows() {
            if data.is_valid(row) && data.value(row).split_whitespace().any(|t| t == token) {
                keys.insert((w.value(row), d.value(row), c.value(row)));
            }
        }
    }
    Ok(keys)
}

/// Rows as `(c_w_id, c_d_id, c_id)` keys — the first three columns.
fn extract_customer_keys(batches: &[RecordBatch]) -> anyhow::Result<BTreeSet<CustomerKey>> {
    let mut keys = BTreeSet::new();
    for batch in batches {
        anyhow::ensure!(
            batch.num_columns() >= 3,
            "expected columns (c_w_id, c_d_id, c_id), got {} columns",
            batch.num_columns()
        );
        let w = to_i64_array(batch.column(0))?;
        let d = to_i64_array(batch.column(1))?;
        let c = to_i64_array(batch.column(2))?;
        for row in 0..batch.num_rows() {
            keys.insert((w.value(row), d.value(row), c.value(row)));
        }
    }
    Ok(keys)
}

/// Rows as `(key, name)` pairs — the first two columns.
fn extract_key_name_pairs(batches: &[RecordBatch]) -> anyhow::Result<Vec<(i64, String)>> {
    let mut pairs = Vec::new();
    for batch in batches {
        anyhow::ensure!(
            batch.num_columns() >= 2,
            "expected columns (key, name), got {} columns",
            batch.num_columns()
        );
        let keys = to_i64_array(batch.column(0))?;
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("name column is not Utf8"))?;
        for row in 0..batch.num_rows() {
            pairs.push((keys.value(row), names.value(row).to_string()));
        }
    }
    Ok(pairs)
}

/// Cast an integer-typed array to `Int64` for uniform key comparison.
fn to_i64_array(array: &dyn Array) -> anyhow::Result<Int64Array> {
    let casted = arrow::compute::cast(array, &DataType::Int64)?;
    casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("expected an integer column"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{Field, Schema};

    fn customer_batch(rows: &[(i32, i32, i32, &str)]) -> RecordBatch {
        let w: ArrayRef = Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.0)));
        let d: ArrayRef = Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.1)));
        let c: ArrayRef = Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.2)));
        let data: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3)));
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("c_w_id", DataType::Int32, false),
                Field::new("c_d_id", DataType::Int32, false),
                Field::new("c_id", DataType::Int32, false),
                Field::new("c_data", DataType::Utf8, false),
            ])),
            vec![w, d, c, data],
        )
        .expect("valid batch")
    }

    #[test]
    fn matches_exact_whitespace_token_only() {
        let batch = customer_batch(&[
            // Payment-mutated: "5" appears as its own token (c_d_id field).
            (
                1,
                5,
                10,
                "|   10    5    1  2    1 $   5.00 1700000000abc123",
            ),
            // No isolated "5" token: "45" and "$5.00" (embedded, not bare "5").
            (2, 6, 11, "abc45def"),
            (
                3,
                7,
                12,
                "|   10    6    1  2    1 $   5.00 1700000000xyz789",
            ),
        ]);

        let matched = customer_keys_matching_token(&[batch], "5").expect("matches");
        assert_eq!(matched, BTreeSet::from([(1, 5, 10)]));
    }

    #[test]
    fn random_seed_data_never_spuriously_matches() {
        // Random seed data (chbench_driver::rand::rand_chars) has no whitespace,
        // so a digit "5" anywhere inside it can never form an isolated token.
        let batch = customer_batch(&[(9, 9, 99, "aZ5bQ9012345abcXYZ")]);
        let matched = customer_keys_matching_token(&[batch], "5").expect("matches");
        assert!(matched.is_empty());
    }

    #[test]
    fn extract_customer_keys_reads_first_three_columns() {
        let batch = customer_batch(&[(1, 2, 3, "anything"), (4, 5, 6, "anything")]);
        let keys = extract_customer_keys(&[batch]).expect("extracts keys");
        assert_eq!(keys, BTreeSet::from([(1, 2, 3), (4, 5, 6)]));
    }

    #[test]
    fn extract_key_name_pairs_reads_first_two_columns() {
        let keys: ArrayRef = Arc::new(Int32Array::from(vec![23, 24]));
        let names: ArrayRef = Arc::new(StringArray::from(vec!["UNITED KINGDOM", "UNITED STATES"]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n_nationkey", DataType::Int32, false),
                Field::new("n_name", DataType::Utf8, false),
            ])),
            vec![keys, names],
        )
        .expect("valid batch");

        let pairs = extract_key_name_pairs(&[batch]).expect("extracts pairs");
        assert_eq!(
            pairs,
            vec![
                (23, "UNITED KINGDOM".to_string()),
                (24, "UNITED STATES".to_string())
            ]
        );
    }
}
