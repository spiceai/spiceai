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

use std::collections::{BTreeMap, HashMap};

/// Retrieval-quality metrics computed over a full search run, each evaluated at the same rank
/// cutoff `k` (see [`calculate_retrieval_metrics`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetrievalMetrics {
    /// Normalized Discounted Cumulative Gain@k.
    pub ndcg: f64,
    /// Recall@k: fraction of all relevant documents found within the top k results.
    pub recall: f64,
    /// Mean Reciprocal Rank@k: average of `1 / rank_of_first_relevant_result` within the top k.
    pub mrr: f64,
    /// Precision@k: fraction of the top k results that are relevant.
    pub precision: f64,
}

/// Calculates NDCG@k, Recall@k, MRR@k, and Precision@k in a single pass over `qrels`, following
/// the MTEB `RetrievalEvaluator` methodology for NDCG and the standard TREC/`pytrec_eval`
/// binary-relevance convention (relevance grade > 0 counts as relevant) for the other three.
///
/// # Arguments
/// * `qrels` - Query relevance judgments mapping `query_id` -> (`doc_id` -> `relevance_score`)
/// * `results` - Search results mapping `query_id` -> (`doc_id` -> `similarity_score`)
/// * `k` - Rank cutoff shared by all four metrics
///
/// Every judged query in `qrels` contributes to each average. A query the search returned
/// nothing for scores 0.0 rather than being dropped — dropping it would raise the mean
/// instead of lowering it.
///
/// # Errors
/// Returns an error when `qrels` is empty: an average over zero queries is undefined.
///
/// # Reference
/// MTEB `RetrievalEvaluator`: <https://github.com/embeddings-benchmark/mteb/blob/03347ebfe4809056e0fd2894fcae69dcdd2ed964/mteb/evaluation/evaluators/RetrievalEvaluator.py#L500>
pub(crate) fn calculate_retrieval_metrics<S: ::std::hash::BuildHasher>(
    qrels: &HashMap<String, HashMap<String, i32, S>, S>,
    results: &HashMap<String, HashMap<String, f64, S>, S>,
    k: usize,
) -> anyhow::Result<RetrievalMetrics> {
    anyhow::ensure!(
        !qrels.is_empty(),
        "Cannot calculate retrieval metrics: no query relevance judgments were provided"
    );

    let mut ndcg_values = Vec::with_capacity(qrels.len());
    let mut recall_values = Vec::with_capacity(qrels.len());
    let mut mrr_values = Vec::with_capacity(qrels.len());
    let mut precision_values = Vec::with_capacity(qrels.len());

    for (query_id, relevance) in qrels {
        // An unanswered query ranks nothing, which scores 0.0 for all four metrics
        // through the same code path as a ranking that retrieved nothing relevant.
        let ranked_relevance = if let Some(ranked_results) = results.get(query_id) {
            score_sorted_relevance(ranked_results, relevance)
        } else {
            println!("No search results found for test query {query_id}");
            Vec::new()
        };

        ndcg_values.push(ndcg_at_k(&ranked_relevance, relevance, k));
        recall_values.push(recall_at_k(&ranked_relevance, relevance, k));
        mrr_values.push(mrr_at_k(&ranked_relevance, k));
        precision_values.push(precision_at_k(&ranked_relevance, k));
    }

    Ok(RetrievalMetrics {
        ndcg: average(&ndcg_values),
        recall: average(&recall_values),
        mrr: average(&mrr_values),
        precision: average(&precision_values),
    })
}

/// One query's precomputed rank-ordered relevance grades (see [`score_sorted_relevance`]),
/// paired with its full relevance judgments so per-k metrics can be recomputed cheaply.
struct QueryRankedRelevance<'a, S> {
    ranked_relevance: Vec<f64>,
    relevance: &'a HashMap<String, i32, S>,
}

/// Computes [`RetrievalMetrics`] at every rank cutoff `k` in `1..=n`, where `n` is the largest
/// number of results returned for any evaluated query. Because the top-`n` results for a query are
/// already retrieved, evaluating every `k` is pure post-processing over the ranked relevance list —
/// no additional search is required — so the full metric-vs-`k` curve comes for free.
///
/// Each query's ranked relevance is computed once (via [`score_sorted_relevance`]) and reused across
/// all `k`, and the same per-metric helpers as [`calculate_retrieval_metrics`] are applied, so the
/// value at any `k` here equals `calculate_retrieval_metrics(qrels, results, k)` by construction. A
/// query returning fewer than `k` results contributes its saturated (`take(k)`) list, exactly as the
/// single-cutoff path already does when a query returns fewer than `k` documents.
///
/// The result is keyed by `k` (ascending); an empty map means no query had any results to evaluate.
#[must_use]
pub(crate) fn calculate_retrieval_metrics_at_all_k<S: ::std::hash::BuildHasher>(
    qrels: &HashMap<String, HashMap<String, i32, S>, S>,
    results: &HashMap<String, HashMap<String, f64, S>, S>,
) -> BTreeMap<usize, RetrievalMetrics> {
    // Precompute each evaluated query's ranked relevance once, plus the max list length (`n`).
    let mut per_query: Vec<QueryRankedRelevance<'_, S>> = Vec::new();
    let mut max_k = 0;

    for (query_id, relevance) in qrels {
        let Some(ranked_results) = results.get(query_id) else {
            println!("No search results found for test query {query_id}");
            continue;
        };

        let ranked_relevance = score_sorted_relevance(ranked_results, relevance);
        max_k = max_k.max(ranked_relevance.len());
        per_query.push(QueryRankedRelevance {
            ranked_relevance,
            relevance,
        });
    }

    let mut metrics_by_k = BTreeMap::new();
    for k in 1..=max_k {
        let mut ndcg_values = Vec::with_capacity(per_query.len());
        let mut recall_values = Vec::with_capacity(per_query.len());
        let mut mrr_values = Vec::with_capacity(per_query.len());
        let mut precision_values = Vec::with_capacity(per_query.len());

        for query in &per_query {
            ndcg_values.push(ndcg_at_k(&query.ranked_relevance, query.relevance, k));
            recall_values.push(recall_at_k(&query.ranked_relevance, query.relevance, k));
            mrr_values.push(mrr_at_k(&query.ranked_relevance, k));
            precision_values.push(precision_at_k(&query.ranked_relevance, k));
        }

        metrics_by_k.insert(
            k,
            RetrievalMetrics {
                ndcg: average(&ndcg_values),
                recall: average(&recall_values),
                mrr: average(&mrr_values),
                precision: average(&precision_values),
            },
        );
    }

    metrics_by_k
}

/// Orders a query's search results by score (descending) and returns the relevance grade of each
/// ranked document. `ranked_results` is a `HashMap`, so its iteration order has no relation to
/// score; every rank-based metric needs results in score order first, or the ranking quality it
/// measures is lost.
fn score_sorted_relevance<S: ::std::hash::BuildHasher>(
    ranked_results: &HashMap<String, f64, S>,
    relevance: &HashMap<String, i32, S>,
) -> Vec<f64> {
    let mut scored_docs: Vec<(&String, &f64)> = ranked_results.iter().collect();
    scored_docs.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored_docs
        .into_iter()
        .map(|(doc_id, _)| f64::from(*relevance.get(doc_id).unwrap_or(&0)))
        .collect()
}

/// Mean of `values`. Never called with an empty slice: `calculate_retrieval_metrics`
/// rejects empty `qrels` and pushes exactly one value per judged query.
#[expect(clippy::cast_precision_loss)]
fn average(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

#[expect(clippy::cast_precision_loss)]
fn dcg_at_k(relevance_scores: &[f64], k: usize) -> f64 {
    relevance_scores
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, &rel)| rel / (i as f64 + 2f64).log2())
        .sum()
}

/// Ideal DCG@k: every document judged for this query, ranked by relevance grade. Taken from
/// `relevance` rather than from what search returned, so a result set that misses relevant
/// documents scores below 1.0 — deriving it from the retrieved documents makes any ranking
/// of them look perfect, however much it left behind.
fn ideal_dcg_at_k<S: ::std::hash::BuildHasher>(
    relevance: &HashMap<String, i32, S>,
    k: usize,
) -> f64 {
    let mut grades: Vec<f64> = relevance.values().map(|&grade| f64::from(grade)).collect();
    grades.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    dcg_at_k(&grades, k)
}

fn ndcg_at_k<S: ::std::hash::BuildHasher>(
    ranked_relevance: &[f64],
    relevance: &HashMap<String, i32, S>,
    k: usize,
) -> f64 {
    let idcg = ideal_dcg_at_k(relevance, k);
    if idcg == 0.0 {
        return 0.0;
    }
    dcg_at_k(ranked_relevance, k) / idcg
}

/// Fraction of all relevant documents (per `relevance`, not just those retrieved) that appear
/// within the top k ranked results.
#[expect(clippy::cast_precision_loss)]
fn recall_at_k<S: ::std::hash::BuildHasher>(
    ranked_relevance: &[f64],
    relevance: &HashMap<String, i32, S>,
    k: usize,
) -> f64 {
    let total_relevant = relevance.values().filter(|&&grade| grade > 0).count();
    if total_relevant == 0 {
        return 0.0;
    }
    let retrieved_relevant = ranked_relevance
        .iter()
        .take(k)
        .filter(|&&rel| rel > 0.0)
        .count();
    retrieved_relevant as f64 / total_relevant as f64
}

/// Fraction of the top k ranked results that are relevant.
#[expect(clippy::cast_precision_loss)]
fn precision_at_k(ranked_relevance: &[f64], k: usize) -> f64 {
    let retrieved_relevant = ranked_relevance
        .iter()
        .take(k)
        .filter(|&&rel| rel > 0.0)
        .count();
    retrieved_relevant as f64 / k as f64
}

/// Reciprocal rank (1-indexed) of the first relevant result within the top k, or 0.0 if none.
#[expect(clippy::cast_precision_loss)]
fn mrr_at_k(ranked_relevance: &[f64], k: usize) -> f64 {
    ranked_relevance
        .iter()
        .take(k)
        .position(|&rel| rel > 0.0)
        .map_or(0.0, |rank| 1.0 / (rank as f64 + 1.0))
}

#[cfg(test)]
mod tests {
    use super::{calculate_retrieval_metrics, calculate_retrieval_metrics_at_all_k};
    use std::collections::HashMap;

    type Qrels = HashMap<String, HashMap<String, i32>>;
    type ScoredResults = HashMap<String, HashMap<String, f64>>;

    /// Six distinct relevance grades, all "relevant" (grade > 0), ranked by search score in
    /// exactly the same order as their relevance grade. Any ordering other than
    /// score-descending would strictly reduce DCG below IDCG, and would also move the
    /// top-graded (first-relevant) doc out of rank 0, changing MRR.
    fn perfectly_ranked_query() -> (Qrels, ScoredResults) {
        let qrels = HashMap::from([(
            "q1".to_string(),
            HashMap::from([
                ("doc0".to_string(), 6),
                ("doc1".to_string(), 5),
                ("doc2".to_string(), 4),
                ("doc3".to_string(), 3),
                ("doc4".to_string(), 2),
                ("doc5".to_string(), 1),
            ]),
        )]);

        let results = HashMap::from([(
            "q1".to_string(),
            HashMap::from([
                ("doc0".to_string(), 0.6),
                ("doc1".to_string(), 0.5),
                ("doc2".to_string(), 0.4),
                ("doc3".to_string(), 0.3),
                ("doc4".to_string(), 0.2),
                ("doc5".to_string(), 0.1),
            ]),
        )]);

        (qrels, results)
    }

    #[test]
    fn ndcg_sorts_results_by_score_before_scoring() {
        let (qrels, results) = perfectly_ranked_query();

        let metrics = calculate_retrieval_metrics(&qrels, &results, 6)
            .expect("metrics are calculable for a non-empty qrels");
        assert!(
            (metrics.ndcg - 1.0).abs() < 1e-9,
            "expected a perfect NDCG@6 of 1.0 when search scores exactly match relevance order, got {}",
            metrics.ndcg
        );
    }

    #[test]
    fn recall_precision_and_mrr_on_perfectly_ranked_results() {
        let (qrels, results) = perfectly_ranked_query();

        // All 6 relevant docs are retrieved within the top 6, and the top-ranked
        // result is relevant.
        let metrics = calculate_retrieval_metrics(&qrels, &results, 6)
            .expect("metrics are calculable for a non-empty qrels");
        assert!(
            (metrics.recall - 1.0).abs() < 1e-9,
            "recall = {}",
            metrics.recall
        );
        assert!(
            (metrics.precision - 1.0).abs() < 1e-9,
            "precision = {}",
            metrics.precision
        );
        assert!((metrics.mrr - 1.0).abs() < 1e-9, "mrr = {}", metrics.mrr);
    }

    #[test]
    fn recall_precision_and_mrr_respect_k_cutoff() {
        let (qrels, results) = perfectly_ranked_query();

        // Only the top 3 of 6 relevant docs are within the cutoff.
        let metrics = calculate_retrieval_metrics(&qrels, &results, 3)
            .expect("metrics are calculable for a non-empty qrels");
        assert!(
            (metrics.recall - 0.5).abs() < 1e-9,
            "recall@3 = {}",
            metrics.recall
        );
        assert!(
            (metrics.precision - 1.0).abs() < 1e-9,
            "precision@3 = {}",
            metrics.precision
        );
        assert!((metrics.mrr - 1.0).abs() < 1e-9, "mrr@3 = {}", metrics.mrr);
    }

    #[test]
    fn mrr_finds_first_relevant_result_past_top_rank() {
        let qrels = HashMap::from([("q1".to_string(), HashMap::from([("doc2".to_string(), 1)]))]);

        // doc2 is the only relevant document, ranked 3rd by score.
        let results = HashMap::from([(
            "q1".to_string(),
            HashMap::from([
                ("doc0".to_string(), 0.9),
                ("doc1".to_string(), 0.8),
                ("doc2".to_string(), 0.7),
            ]),
        )]);

        let metrics = calculate_retrieval_metrics(&qrels, &results, 3)
            .expect("metrics are calculable for a non-empty qrels");
        assert!(
            (metrics.mrr - (1.0 / 3.0)).abs() < 1e-9,
            "expected MRR@3 = 1/3 for a relevant doc at rank 3, got {}",
            metrics.mrr
        );
        assert!(
            (metrics.recall - 1.0).abs() < 1e-9,
            "recall@3 = {}",
            metrics.recall
        );
        assert!(
            (metrics.precision - (1.0 / 3.0)).abs() < 1e-9,
            "precision@3 = {}",
            metrics.precision
        );
    }

    #[test]
    fn all_k_covers_every_cutoff_and_agrees_with_single_k() {
        let (qrels, results) = perfectly_ranked_query();

        let all_k = calculate_retrieval_metrics_at_all_k(&qrels, &results);

        // Six results were returned, so metrics exist for every cutoff 1..=6 and no more.
        assert_eq!(
            all_k.keys().copied().collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6]
        );

        // Every cutoff must match the dedicated single-k computation exactly.
        for (&k, metrics) in &all_k {
            assert_eq!(
                *metrics,
                calculate_retrieval_metrics(&qrels, &results, k)
                    .expect("metrics are calculable for a non-empty qrels"),
                "all-k metrics disagree with single-k metrics at k={k}"
            );
        }

        // On a perfectly ranked query precision@k is 1.0 at every k, and recall@k is k/6.
        for (&k, metrics) in &all_k {
            assert!(
                (metrics.precision - 1.0).abs() < 1e-9,
                "precision@{k} = {}",
                metrics.precision
            );
            #[expect(clippy::cast_precision_loss)]
            let expected_recall = k as f64 / 6.0;
            assert!(
                (metrics.recall - expected_recall).abs() < 1e-9,
                "recall@{k} = {}, expected {expected_recall}",
                metrics.recall
            );
        }
    }

    #[test]
    fn all_k_spans_the_longest_result_list_across_queries() {
        // q1 returns 3 results, q2 returns 1; the longest list drives the max cutoff.
        let qrels = HashMap::from([
            (
                "q1".to_string(),
                HashMap::from([("doc0".to_string(), 1), ("doc2".to_string(), 1)]),
            ),
            ("q2".to_string(), HashMap::from([("docA".to_string(), 1)])),
        ]);
        let results = HashMap::from([
            (
                "q1".to_string(),
                HashMap::from([
                    ("doc0".to_string(), 0.9),
                    ("doc1".to_string(), 0.8),
                    ("doc2".to_string(), 0.7),
                ]),
            ),
            ("q2".to_string(), HashMap::from([("docA".to_string(), 0.5)])),
        ]);

        let all_k = calculate_retrieval_metrics_at_all_k(&qrels, &results);
        assert_eq!(all_k.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);

        // Beyond q2's single result the `take(k)` list saturates, so its per-query metrics stay
        // fixed while q1's continue to evolve — agreement with single-k proves the averaging.
        for (&k, metrics) in &all_k {
            assert_eq!(
                *metrics,
                calculate_retrieval_metrics(&qrels, &results, k)
                    .expect("metrics are calculable for a non-empty qrels"),
                "all-k metrics disagree with single-k metrics at k={k}"
            );
        }
    }

    /// The ideal DCG must rank every *judged* document, not just the retrieved ones: search
    /// returns one of two equally-relevant documents, so NDCG@10 has to fall below 1.0.
    /// Deriving the ideal from the retrieved set scores this a perfect 1.0.
    #[test]
    fn ndcg_penalizes_relevant_documents_the_search_missed() {
        let qrels: Qrels = HashMap::from([(
            "q1".to_string(),
            HashMap::from([("doc0".to_string(), 1), ("doc1".to_string(), 1)]),
        )]);
        let results: ScoredResults =
            HashMap::from([("q1".to_string(), HashMap::from([("doc0".to_string(), 0.9)]))]);

        let metrics = calculate_retrieval_metrics(&qrels, &results, 10)
            .expect("metrics are calculable for a non-empty qrels");

        // DCG@10 = 1/log2(2) = 1.0; ideal ranks both judged docs:
        // 1/log2(2) + 1/log2(3).
        let expected = 1.0 / (1.0 + 1.0 / 3.0f64.log2());
        assert!(
            (metrics.ndcg - expected).abs() < 1e-9,
            "expected NDCG@10 of {expected} when one of two relevant docs is retrieved, got {}",
            metrics.ndcg
        );
        assert!(
            (metrics.recall - 0.5).abs() < 1e-9,
            "recall@10 = {}",
            metrics.recall
        );
    }

    /// A judged query the search answered with nothing scores 0.0 and stays in the average.
    /// Dropping it divides by the answered queries only, which raises every metric.
    #[test]
    fn an_unanswered_query_scores_zero_and_stays_in_the_average() {
        let qrels: Qrels = HashMap::from([
            ("q1".to_string(), HashMap::from([("doc0".to_string(), 1)])),
            ("q2".to_string(), HashMap::from([("doc1".to_string(), 1)])),
        ]);
        // Only q1 is answered, and perfectly.
        let results: ScoredResults =
            HashMap::from([("q1".to_string(), HashMap::from([("doc0".to_string(), 0.9)]))]);

        let metrics = calculate_retrieval_metrics(&qrels, &results, 10)
            .expect("metrics are calculable for a non-empty qrels");

        // q1 scores a perfect 1.0 and q2 a 0.0, so each average is halved.
        for (name, value) in [
            ("ndcg", metrics.ndcg),
            ("recall", metrics.recall),
            ("mrr", metrics.mrr),
        ] {
            assert!(
                (value - 0.5).abs() < 1e-9,
                "expected {name}@10 of 0.5 when one of two judged queries is unanswered, got {value}"
            );
        }
        // Precision@10 divides by k, so q1's single relevant hit is 0.1, averaged with 0.0.
        assert!(
            (metrics.precision - 0.05).abs() < 1e-9,
            "expected precision@10 of 0.05, got {}",
            metrics.precision
        );
    }

    /// An empty `qrels` used to divide by zero and publish `NaN` into the benchmark metrics.
    #[test]
    fn empty_qrels_is_an_error_rather_than_nan() {
        let qrels: Qrels = HashMap::new();
        let results: ScoredResults = HashMap::new();

        assert!(
            calculate_retrieval_metrics(&qrels, &results, 10).is_err(),
            "expected an error when no query relevance judgments are provided"
        );
    }
}
