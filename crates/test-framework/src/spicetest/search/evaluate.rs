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
use std::fmt;

use anyhow::Context;
use serde::{Deserialize, Serialize};

/// Id of a judged query, from the `query-id` column of the relevance judgments.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct QueryId(String);

impl QueryId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for QueryId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for QueryId {
    fn from(id: &str) -> Self {
        Self(id.to_string())
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Retrieval-quality metrics at one rank cutoff `k`, for one query or as the mean over all
/// judged queries (see [`RetrievalEvaluation`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetrievalMetrics {
    /// Normalized Discounted Cumulative Gain@k.
    pub ndcg: f64,
    /// Recall@k: fraction of all relevant documents found within the top k results.
    pub recall: f64,
    /// Reciprocal Rank@k: `1 / rank_of_first_relevant_result` within the top k. Its mean over
    /// queries is MRR@k.
    pub mrr: f64,
    /// Precision@k: fraction of the top k results that are relevant.
    pub precision: f64,
}

impl RetrievalMetrics {
    /// Mean of each metric over `metrics`, or `None` when `metrics` is empty.
    fn mean<'a>(metrics: impl IntoIterator<Item = &'a Self>) -> Option<Self> {
        let mut count = 0_u32;
        let mut sum = Self {
            ndcg: 0.0,
            recall: 0.0,
            mrr: 0.0,
            precision: 0.0,
        };
        for m in metrics {
            count += 1;
            sum.ndcg += m.ndcg;
            sum.recall += m.recall;
            sum.mrr += m.mrr;
            sum.precision += m.precision;
        }
        if count == 0 {
            return None;
        }
        let n = f64::from(count);
        Some(Self {
            ndcg: sum.ndcg / n,
            recall: sum.recall / n,
            mrr: sum.mrr / n,
            precision: sum.precision / n,
        })
    }
}

/// One judged query's metrics at every rank cutoff `1..=max_k` of its [`RetrievalEvaluation`].
#[derive(Debug, Clone, PartialEq)]
pub struct QueryRetrievalMetrics {
    result_count: usize,
    /// The metrics at cutoff `k` are at index `k - 1`.
    metrics: Vec<RetrievalMetrics>,
}

impl QueryRetrievalMetrics {
    /// Number of results the search returned for this query. Zero when it returned none.
    #[must_use]
    pub fn result_count(&self) -> usize {
        self.result_count
    }

    /// The metrics at rank cutoff `k`, or `None` when `k` is outside `1..=max_k`.
    #[must_use]
    pub fn at_k(&self, k: usize) -> Option<&RetrievalMetrics> {
        k.checked_sub(1).and_then(|index| self.metrics.get(index))
    }
}

/// Per-query retrieval metrics for every judged query in `qrels`, at every rank cutoff `k` in
/// `1..=max_k`. Every query has a value at every cutoff, so two runs over the same query set pair
/// up query by query. The run-level metrics are the means of these values ([`Self::mean_at_k`]).
///
/// NDCG follows the MTEB `RetrievalEvaluator` methodology. Recall, MRR, and precision follow the
/// standard TREC/`pytrec_eval` binary-relevance convention (relevance grade > 0 is relevant).
/// Queries that have search results but no relevance judgments are not evaluated.
///
/// # Reference
/// MTEB `RetrievalEvaluator`: <https://github.com/embeddings-benchmark/mteb/blob/03347ebfe4809056e0fd2894fcae69dcdd2ed964/mteb/evaluation/evaluators/RetrievalEvaluator.py#L500>
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(try_from = "EvaluationRepr<QueryId>")]
pub struct RetrievalEvaluation {
    max_k: usize,
    /// Never empty: construction rejects an empty `qrels`.
    queries: BTreeMap<QueryId, QueryRetrievalMetrics>,
}

impl RetrievalEvaluation {
    /// Evaluates every judged query at each rank cutoff `1..=max_k`.
    ///
    /// # Arguments
    /// * `qrels` - Query relevance judgments mapping `query_id` -> (`doc_id` -> `relevance_score`)
    /// * `results` - Search results mapping `query_id` -> (`doc_id` -> `similarity_score`)
    ///
    /// A judged query the search returned nothing for scores 0.0 at every cutoff rather than being
    /// dropped: dropping it would raise the mean instead of lowering it.
    ///
    /// # Errors
    /// Returns an error when `qrels` is empty: an average over zero queries is undefined.
    pub(crate) fn new<S: ::std::hash::BuildHasher>(
        qrels: &HashMap<String, HashMap<String, i32, S>, S>,
        results: &HashMap<String, HashMap<String, f64, S>, S>,
        max_k: usize,
    ) -> anyhow::Result<Self> {
        Ok(Self::from_ranked(ranked_relevance(qrels, results)?, max_k))
    }

    /// [`Self::new`] up to the largest number of results returned for any judged query. Because
    /// those results are already retrieved, the full metric-vs-`k` curve needs no extra search.
    /// A query that returned fewer than `k` results keeps its saturated (`take(k)`) list.
    ///
    /// # Errors
    /// Returns an error when `qrels` is empty.
    pub(crate) fn up_to_longest_result_list<S: ::std::hash::BuildHasher>(
        qrels: &HashMap<String, HashMap<String, i32, S>, S>,
        results: &HashMap<String, HashMap<String, f64, S>, S>,
    ) -> anyhow::Result<Self> {
        let ranked = ranked_relevance(qrels, results)?;
        let max_k = ranked
            .values()
            .map(|query| query.ranked_relevance.len())
            .max()
            .unwrap_or(0);
        Ok(Self::from_ranked(ranked, max_k))
    }

    fn from_ranked<S: ::std::hash::BuildHasher>(
        ranked: BTreeMap<QueryId, QueryRankedRelevance<'_, S>>,
        max_k: usize,
    ) -> Self {
        let queries = ranked
            .into_iter()
            .map(|(query_id, query)| {
                let metrics = (1..=max_k)
                    .map(|k| RetrievalMetrics {
                        ndcg: ndcg_at_k(&query.ranked_relevance, query.relevance, k),
                        recall: recall_at_k(&query.ranked_relevance, query.relevance, k),
                        mrr: mrr_at_k(&query.ranked_relevance, k),
                        precision: precision_at_k(&query.ranked_relevance, k),
                    })
                    .collect();
                let metrics = QueryRetrievalMetrics {
                    result_count: query.ranked_relevance.len(),
                    metrics,
                };
                (query_id, metrics)
            })
            .collect();
        Self { max_k, queries }
    }

    /// Largest evaluated rank cutoff. Zero when no judged query returned any result.
    #[must_use]
    pub fn max_k(&self) -> usize {
        self.max_k
    }

    /// Every judged query's metrics, ordered by query id.
    #[must_use]
    pub fn queries(&self) -> &BTreeMap<QueryId, QueryRetrievalMetrics> {
        &self.queries
    }

    /// Every judged query's metrics at rank cutoff `k`. Empty when `k` is outside `1..=max_k`.
    pub fn at_k(&self, k: usize) -> impl Iterator<Item = (&QueryId, &RetrievalMetrics)> {
        self.queries
            .iter()
            .filter_map(move |(query_id, query)| query.at_k(k).map(|m| (query_id, m)))
    }

    /// Mean over all judged queries at rank cutoff `k`, or `None` when `k` is outside `1..=max_k`.
    #[must_use]
    pub fn mean_at_k(&self, k: usize) -> Option<RetrievalMetrics> {
        RetrievalMetrics::mean(self.at_k(k).map(|(_, m)| m))
    }

    /// Mean over all judged queries at every rank cutoff, keyed by `k` (ascending).
    #[must_use]
    pub fn mean_at_all_k(&self) -> BTreeMap<usize, RetrievalMetrics> {
        (1..=self.max_k)
            .filter_map(|k| self.mean_at_k(k).map(|m| (k, m)))
            .collect()
    }
}

// Serialized form of a `RetrievalEvaluation`: one entry per query, each with one entry per `k`,
// so a reader does not need to know the in-memory layout.
#[derive(Serialize, Deserialize)]
struct EvaluationRepr<Q> {
    max_k: usize,
    queries: Vec<QueryRepr<Q>>,
}

#[derive(Serialize, Deserialize)]
struct QueryRepr<Q> {
    query_id: Q,
    result_count: usize,
    metrics_at_k: Vec<MetricsAtKRepr>,
}

#[derive(Serialize, Deserialize)]
struct MetricsAtKRepr {
    k: usize,
    ndcg: f64,
    recall: f64,
    /// One query's `mrr`: its mean over queries is MRR.
    reciprocal_rank: f64,
    precision: f64,
}

impl Serialize for RetrievalEvaluation {
    fn serialize<Ser: serde::Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        let queries = self
            .queries
            .iter()
            .map(|(query_id, query)| QueryRepr {
                query_id,
                result_count: query.result_count,
                metrics_at_k: query
                    .metrics
                    .iter()
                    .zip(1..)
                    .map(|(m, k)| MetricsAtKRepr {
                        k,
                        ndcg: m.ndcg,
                        recall: m.recall,
                        reciprocal_rank: m.mrr,
                        precision: m.precision,
                    })
                    .collect(),
            })
            .collect();
        EvaluationRepr {
            max_k: self.max_k,
            queries,
        }
        .serialize(serializer)
    }
}

impl TryFrom<EvaluationRepr<QueryId>> for RetrievalEvaluation {
    type Error = String;

    /// Rejects input that breaks the invariants the constructors guarantee: at least one query,
    /// unique query ids, and metrics at exactly `k = 1..=max_k` for every query.
    fn try_from(repr: EvaluationRepr<QueryId>) -> Result<Self, Self::Error> {
        if repr.queries.is_empty() {
            return Err("a retrieval evaluation must have at least one query".to_string());
        }
        let mut queries = BTreeMap::new();
        for query in repr.queries {
            let ks: Vec<usize> = query.metrics_at_k.iter().map(|m| m.k).collect();
            if !ks.iter().copied().eq(1..=repr.max_k) {
                return Err(format!(
                    "query {} must have metrics at k = 1..={} in order, got {ks:?}",
                    query.query_id, repr.max_k
                ));
            }
            let metrics = query
                .metrics_at_k
                .into_iter()
                .map(|m| RetrievalMetrics {
                    ndcg: m.ndcg,
                    recall: m.recall,
                    mrr: m.reciprocal_rank,
                    precision: m.precision,
                })
                .collect();
            let duplicate = queries
                .insert(
                    query.query_id.clone(),
                    QueryRetrievalMetrics {
                        result_count: query.result_count,
                        metrics,
                    },
                )
                .is_some();
            if duplicate {
                return Err(format!("query {} appears more than once", query.query_id));
            }
        }
        Ok(Self {
            max_k: repr.max_k,
            queries,
        })
    }
}

/// Mean NDCG@k, Recall@k, MRR@k, and Precision@k over every judged query in `qrels` (see
/// [`RetrievalEvaluation`]).
///
/// # Errors
/// Returns an error when `qrels` is empty or `k` is zero.
pub(crate) fn calculate_retrieval_metrics<S: ::std::hash::BuildHasher>(
    qrels: &HashMap<String, HashMap<String, i32, S>, S>,
    results: &HashMap<String, HashMap<String, f64, S>, S>,
    k: usize,
) -> anyhow::Result<RetrievalMetrics> {
    RetrievalEvaluation::new(qrels, results, k)?
        .mean_at_k(k)
        .context("Cannot calculate retrieval metrics: the rank cutoff k must be at least 1")
}

/// One judged query's rank-ordered relevance grades (see [`score_sorted_relevance`]), paired with
/// its full relevance judgments so per-k metrics can be computed cheaply.
struct QueryRankedRelevance<'a, S> {
    ranked_relevance: Vec<f64>,
    relevance: &'a HashMap<String, i32, S>,
}

/// Ranks the results of every judged query in `qrels`. A judged query without results gets an
/// empty ranking, which scores 0.0 through the same code path as a ranking with nothing relevant.
fn ranked_relevance<'a, S: ::std::hash::BuildHasher>(
    qrels: &'a HashMap<String, HashMap<String, i32, S>, S>,
    results: &HashMap<String, HashMap<String, f64, S>, S>,
) -> anyhow::Result<BTreeMap<QueryId, QueryRankedRelevance<'a, S>>> {
    anyhow::ensure!(
        !qrels.is_empty(),
        "Cannot calculate retrieval metrics: no query relevance judgments were provided"
    );

    Ok(qrels
        .iter()
        .map(|(query_id, relevance)| {
            let ranked_relevance = if let Some(ranked_results) = results.get(query_id) {
                score_sorted_relevance(ranked_results, relevance)
            } else {
                println!("No search results found for test query {query_id}");
                Vec::new()
            };
            (
                QueryId::from(query_id.as_str()),
                QueryRankedRelevance {
                    ranked_relevance,
                    relevance,
                },
            )
        })
        .collect())
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

#[expect(clippy::cast_precision_loss)]
fn dcg_at_k(relevance_scores: &[f64], k: usize) -> f64 {
    relevance_scores
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, &rel)| rel / (i as f64 + 2f64).log2())
        .sum::<f64>()
        // An empty f64 sum is -0.0; adding 0.0 makes it 0.0, so per-query output never shows -0.0.
        + 0.0
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
    use super::{QueryId, RetrievalEvaluation, RetrievalMetrics, calculate_retrieval_metrics};
    use std::collections::{BTreeMap, HashMap};

    type Qrels = HashMap<String, HashMap<String, i32>>;
    type ScoredResults = HashMap<String, HashMap<String, f64>>;

    fn calculate_retrieval_metrics_at_all_k(
        qrels: &Qrels,
        results: &ScoredResults,
    ) -> BTreeMap<usize, RetrievalMetrics> {
        RetrievalEvaluation::up_to_longest_result_list(qrels, results)
            .expect("metrics are calculable for a non-empty qrels")
            .mean_at_all_k()
    }

    fn assert_close(actual: f64, expected: f64, what: &str) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "{what}: expected {expected}, got {actual}"
        );
    }

    /// q1 ranks its only relevant doc 2nd of 3, q2 ranks its two relevant docs 1st and 3rd of 3,
    /// and q3 is judged but has no search results.
    fn three_queries_one_unanswered() -> (Qrels, ScoredResults) {
        let qrels = HashMap::from([
            ("q1".to_string(), HashMap::from([("a1".to_string(), 1)])),
            (
                "q2".to_string(),
                HashMap::from([("b0".to_string(), 2), ("b2".to_string(), 1)]),
            ),
            ("q3".to_string(), HashMap::from([("c0".to_string(), 1)])),
        ]);
        let results = HashMap::from([
            (
                "q1".to_string(),
                HashMap::from([
                    ("a0".to_string(), 0.9),
                    ("a1".to_string(), 0.8),
                    ("a2".to_string(), 0.7),
                ]),
            ),
            (
                "q2".to_string(),
                HashMap::from([
                    ("b0".to_string(), 0.9),
                    ("b1".to_string(), 0.8),
                    ("b2".to_string(), 0.7),
                ]),
            ),
        ]);
        (qrels, results)
    }

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

    /// The all-k path must score an unanswered judged query as 0.0, the same as the single-k
    /// path. It used to skip the query, so the mean covered only the answered queries.
    #[test]
    fn all_k_keeps_an_unanswered_query_in_the_average() {
        let qrels: Qrels = HashMap::from([
            ("q1".to_string(), HashMap::from([("doc0".to_string(), 1)])),
            ("q2".to_string(), HashMap::from([("doc1".to_string(), 1)])),
        ]);
        let results: ScoredResults =
            HashMap::from([("q1".to_string(), HashMap::from([("doc0".to_string(), 0.9)]))]);

        let all_k = calculate_retrieval_metrics_at_all_k(&qrels, &results);
        let single_k = calculate_retrieval_metrics(&qrels, &results, 1)
            .expect("metrics are calculable for a non-empty qrels");
        assert_eq!(all_k.get(&1), Some(&single_k));
        assert_close(single_k.ndcg, 0.5, "ndcg@1");
    }

    #[test]
    fn per_query_metrics_cover_every_judged_query_at_every_k() {
        let (qrels, results) = three_queries_one_unanswered();
        let evaluation = RetrievalEvaluation::up_to_longest_result_list(&qrels, &results)
            .expect("metrics are calculable for a non-empty qrels");

        assert_eq!(evaluation.max_k(), 3);
        let ids: Vec<&str> = evaluation.queries().keys().map(QueryId::as_str).collect();
        assert_eq!(ids, vec!["q1", "q2", "q3"]);

        let q1 = &evaluation.queries()[&QueryId::from("q1")];
        assert_eq!(q1.result_count(), 3);
        let q1_at_1 = q1.at_k(1).expect("k=1 is evaluated");
        assert_close(q1_at_1.mrr, 0.0, "q1 rr@1");
        assert_close(q1_at_1.recall, 0.0, "q1 recall@1");
        let q1_at_3 = q1.at_k(3).expect("k=3 is evaluated");
        assert_close(q1_at_3.mrr, 0.5, "q1 rr@3");
        assert_close(q1_at_3.recall, 1.0, "q1 recall@3");
        assert_close(q1_at_3.precision, 1.0 / 3.0, "q1 precision@3");
        // DCG@3 = 1/log2(3); the ideal ranks the one relevant doc first, so IDCG@3 = 1.
        assert_close(q1_at_3.ndcg, 1.0 / 3.0f64.log2(), "q1 ndcg@3");
        assert!(q1.at_k(0).is_none() && q1.at_k(4).is_none());

        let q2_at_3 = evaluation.queries()[&QueryId::from("q2")]
            .at_k(3)
            .expect("k=3 is evaluated");
        assert_close(q2_at_3.mrr, 1.0, "q2 rr@3");
        assert_close(q2_at_3.recall, 1.0, "q2 recall@3");
        assert_close(q2_at_3.precision, 2.0 / 3.0, "q2 precision@3");
        // DCG@3 = 2/log2(2) + 1/log2(4); the ideal puts grade 1 at rank 2: 2 + 1/log2(3).
        assert_close(q2_at_3.ndcg, 2.5 / (2.0 + 1.0 / 3.0f64.log2()), "q2 ndcg@3");

        // The unanswered query is kept, with 0.0 at every cutoff.
        let q3 = &evaluation.queries()[&QueryId::from("q3")];
        assert_eq!(q3.result_count(), 0);
        for k in 1..=3 {
            let zero = RetrievalMetrics {
                ndcg: 0.0,
                recall: 0.0,
                mrr: 0.0,
                precision: 0.0,
            };
            assert_eq!(q3.at_k(k), Some(&zero), "q3 at k={k}");
            // `-0.0 == 0.0`, so check the sign too: a written file must show `0.0`.
            assert!(
                q3.at_k(k).is_some_and(|m| m.ndcg.is_sign_positive()),
                "q3 ndcg@{k} must be +0.0"
            );
        }
        assert_eq!(evaluation.at_k(2).count(), 3);
        assert_eq!(evaluation.at_k(4).count(), 0);
    }

    /// The run-level value at each k is the plain mean of the per-query values at that k.
    #[test]
    fn mean_at_k_is_the_mean_of_the_per_query_metrics() {
        let (qrels, results) = three_queries_one_unanswered();
        let evaluation = RetrievalEvaluation::up_to_longest_result_list(&qrels, &results)
            .expect("metrics are calculable for a non-empty qrels");
        let means = evaluation.mean_at_all_k();
        assert_eq!(means.keys().copied().collect::<Vec<_>>(), vec![1, 2, 3]);

        for (&k, mean) in &means {
            let per_query: Vec<&RetrievalMetrics> = evaluation.at_k(k).map(|(_, m)| m).collect();
            assert_eq!(per_query.len(), 3, "every judged query counts at k={k}");
            #[expect(clippy::cast_precision_loss)]
            let n = per_query.len() as f64;
            let sum =
                |f: fn(&RetrievalMetrics) -> f64| per_query.iter().copied().map(f).sum::<f64>();
            assert_close(mean.ndcg, sum(|m| m.ndcg) / n, &format!("ndcg@{k}"));
            assert_close(mean.recall, sum(|m| m.recall) / n, &format!("recall@{k}"));
            assert_close(mean.mrr, sum(|m| m.mrr) / n, &format!("mrr@{k}"));
            assert_close(
                mean.precision,
                sum(|m| m.precision) / n,
                &format!("precision@{k}"),
            );
            assert_eq!(
                Some(*mean),
                calculate_retrieval_metrics(&qrels, &results, k).ok(),
                "all-k and single-k disagree at k={k}"
            );
        }
        // MRR@3 = (1/2 + 1 + 0) / 3.
        assert_close(means[&3].mrr, 0.5, "mrr@3");
    }

    #[test]
    fn evaluation_round_trips_through_json() {
        let (qrels, results) = three_queries_one_unanswered();
        let evaluation = RetrievalEvaluation::up_to_longest_result_list(&qrels, &results)
            .expect("metrics are calculable for a non-empty qrels");

        let json = serde_json::to_value(&evaluation).expect("serializes");
        assert_eq!(json["max_k"], 3);
        assert_eq!(json["queries"][2]["query_id"], "q3");
        assert_eq!(json["queries"][2]["result_count"], 0);
        assert_eq!(json["queries"][0]["metrics_at_k"][2]["k"], 3);
        assert_eq!(
            json["queries"][0]["metrics_at_k"][2]["reciprocal_rank"],
            0.5
        );

        let parsed: RetrievalEvaluation = serde_json::from_value(json).expect("deserializes");
        assert_eq!(parsed, evaluation);
    }

    #[test]
    fn deserializing_rejects_a_broken_evaluation() {
        let metrics_at = |k: usize| {
            serde_json::json!({
                "k": k, "ndcg": 0.0, "recall": 0.0, "reciprocal_rank": 0.0, "precision": 0.0,
            })
        };
        let query = |id: &str, ks: &[usize]| {
            serde_json::json!({
                "query_id": id,
                "result_count": 0,
                "metrics_at_k": ks.iter().map(|&k| metrics_at(k)).collect::<Vec<_>>(),
            })
        };
        for (what, json) in [
            ("no queries", serde_json::json!({"max_k": 1, "queries": []})),
            (
                "a missing k",
                serde_json::json!({"max_k": 2, "queries": [query("q1", &[1])]}),
            ),
            (
                "a duplicate query",
                serde_json::json!({"max_k": 1, "queries": [query("q1", &[1]), query("q1", &[1])]}),
            ),
        ] {
            assert!(
                serde_json::from_value::<RetrievalEvaluation>(json).is_err(),
                "expected an error for {what}"
            );
        }
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
        assert!(RetrievalEvaluation::up_to_longest_result_list(&qrels, &results).is_err());
    }
}
