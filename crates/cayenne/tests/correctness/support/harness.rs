// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Result-correctness **harness**: execute SQL on engines, then compare the
//! actual returned batches with the shipped validation path.
//!
//! Callers (integration tests) must not re-implement comparison or eyeball
//! logs. The only check is
//! [`test_framework::queries::validation::compare_query_result_batches_with_sort_check`]
//! via [`super::compare_results`], which compares content *and* verifies each
//! side honors the query's own `ORDER BY`.

use arrow::array::RecordBatch;
use test_framework::queries::Query;

use super::{CayenneHarness, ParityOutcome, compare_results};

/// Execute `sql` on Cayenne and return the collected result batches.
pub async fn execute_cayenne(
    cayenne: &CayenneHarness,
    sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    cayenne.query(sql).await
}

/// Compare two already-executed result sets with the shipped equality path.
///
/// This is the sole content oracle for the correctness suite.
#[must_use]
pub fn compare_actual_results(
    query: &Query,
    left: &[RecordBatch],
    right: &[RecordBatch],
) -> ParityOutcome {
    compare_results(query, left, right)
}

/// Run `sql` on Cayenne and on a reference batch producer, then compare.
pub async fn execute_and_compare_cayenne_to_batches<F>(
    query: &Query,
    cayenne: &CayenneHarness,
    cayenne_sql: &str,
    reference_label: &'static str,
    mut reference: F,
) -> ParityOutcome
where
    F: FnMut() -> Result<Vec<RecordBatch>, String>,
{
    let left = match execute_cayenne(cayenne, cayenne_sql).await {
        Ok(b) => b,
        Err(e) => {
            return ParityOutcome::EngineError {
                side: "cayenne",
                detail: e,
            };
        }
    };
    let right = match reference() {
        Ok(b) => b,
        Err(e) => {
            return ParityOutcome::EngineError {
                side: reference_label,
                detail: e,
            };
        }
    };
    compare_actual_results(query, &left, &right)
}

/// Assert that every `ParityOutcome` is Pass or justified Excluded; panic with
/// detail otherwise. Integration tests call this so a human never “grades” logs.
pub fn assert_all_pass_or_excluded(results: &[(String, ParityOutcome)], context: &str) {
    let fails: Vec<_> = results
        .iter()
        .filter(|(_, o)| !o.is_pass_or_excluded())
        .collect();
    assert!(
        fails.is_empty(),
        "{context}: harness found {} unexplained failure(s): {fails:#?}",
        fails.len()
    );
}

/// Cross-check that several Cayenne load-mode result sets are pairwise equal
/// under the shipped compare path (actual SQL results, not transitive reasoning).
pub fn assert_modes_agree_on_actual_results(
    query: &Query,
    mode_batches: &[(&str, &[RecordBatch])],
) -> ParityOutcome {
    if mode_batches.len() < 2 {
        return ParityOutcome::Pass;
    }
    let (ref_mode, ref_batches) = mode_batches[0];
    for (mode, batches) in mode_batches.iter().skip(1) {
        let outcome = compare_actual_results(query, ref_batches, batches);
        if !matches!(outcome, ParityOutcome::Pass) {
            return ParityOutcome::Fail {
                detail: format!(
                    "load modes disagree on actual results for {}: {ref_mode} vs {mode}: {outcome:?}",
                    query.name
                ),
            };
        }
    }
    ParityOutcome::Pass
}
