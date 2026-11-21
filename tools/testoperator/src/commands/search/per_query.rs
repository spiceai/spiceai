/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Per-query retrieval metrics of a `testoperator run search` run, recorded as OTEL gauges at
//! the primary rank cutoff, or with `--per-query-all-k` at every cutoff up to it. Two runs over
//! the same query set pair up by query id, which is the input a paired significance test between
//! two runs needs.

use std::ops::RangeInclusive;

use test_framework::{anyhow, opentelemetry::KeyValue, spicetest::search::RetrievalEvaluation};

/// Rank cutoff of the run-level metrics row and of the per-query gauges. Matches MTEB.
const PRIMARY_K: usize = 10;

/// [`PRIMARY_K`], or the largest evaluated cutoff when no query returned that many results.
/// `None` when no judged query returned any result.
pub(crate) fn primary_k(evaluation: &RetrievalEvaluation) -> Option<usize> {
    match evaluation.max_k() {
        0 => None,
        max_k => Some(max_k.min(PRIMARY_K)),
    }
}

/// The cutoffs to record per-query metrics at: `primary_k` only, or with `all_k` every cutoff
/// from 1 to `primary_k`.
pub(crate) fn cutoffs(primary_k: usize, all_k: bool) -> RangeInclusive<usize> {
    if all_k {
        1..=primary_k
    } else {
        primary_k..=primary_k
    }
}

/// Records each judged query's metrics at each of `cutoffs`, with `query_id` and `k` attributes.
/// The dataset and spicepod attributes come from the telemetry resource, as for the run-level
/// metrics.
///
/// A run adds 4 points per judged query per cutoff, so the default of one cutoff keeps the
/// volume low.
pub(crate) fn record_metrics(
    evaluation: &RetrievalEvaluation,
    cutoffs: RangeInclusive<usize>,
) -> anyhow::Result<()> {
    for k in cutoffs {
        let k_attr = KeyValue::new("k", i64::try_from(k)?);
        for (query_id, metrics) in evaluation.at_k(k) {
            let attributes = [
                KeyValue::new("query_id", query_id.to_string()),
                k_attr.clone(),
            ];
            crate::metrics::SEARCH_QUERY_NDCG.record(metrics.ndcg, &attributes);
            crate::metrics::SEARCH_QUERY_RECALL.record(metrics.recall, &attributes);
            crate::metrics::SEARCH_QUERY_RECIPROCAL_RANK.record(metrics.mrr, &attributes);
            crate::metrics::SEARCH_QUERY_PRECISION.record(metrics.precision, &attributes);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{cutoffs, primary_k};
    use std::collections::HashMap;
    use test_framework::spicetest::search::RetrievalEvaluation;

    /// One judged query with `result_count` search results.
    fn evaluation(result_count: u32) -> RetrievalEvaluation {
        let qrels = HashMap::from([("q1".to_string(), HashMap::from([("d1".to_string(), 1)]))]);
        let results: HashMap<String, HashMap<String, f64>> = HashMap::from([(
            "q1".to_string(),
            (1..=result_count)
                .map(|i| (format!("d{i}"), 1.0 / f64::from(i)))
                .collect(),
        )]);
        RetrievalEvaluation::up_to_longest_result_list(&qrels, &results)
            .expect("metrics are calculable for a non-empty qrels")
    }

    #[test]
    fn primary_k_is_ten_or_the_largest_evaluated_cutoff() {
        assert_eq!(primary_k(&evaluation(12)), Some(10));
        assert_eq!(primary_k(&evaluation(10)), Some(10));
        assert_eq!(primary_k(&evaluation(3)), Some(3));
        assert_eq!(primary_k(&evaluation(0)), None);
    }

    #[test]
    fn cutoffs_are_the_primary_k_or_every_cutoff_up_to_it() {
        assert_eq!(cutoffs(10, false).collect::<Vec<_>>(), vec![10]);
        assert_eq!(
            cutoffs(10, true).collect::<Vec<_>>(),
            (1..=10).collect::<Vec<_>>()
        );
        assert_eq!(cutoffs(3, true).collect::<Vec<_>>(), vec![1, 2, 3]);
    }
}
