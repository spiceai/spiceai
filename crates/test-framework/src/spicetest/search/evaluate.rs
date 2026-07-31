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

use std::collections::HashMap;

/// Calculates the average Normalized Discounted Cumulative Gain (NDCG@k) across all search queries.
///
/// NDCG@k measures the quality of ranking by considering both relevance and position,
/// with higher-ranked relevant documents contributing more to the score. This implementation
/// follows the MTEB (Massive Text Embedding Benchmark) methodology.
///
/// # Arguments
/// * `qrels` - Query relevance judgments mapping `query_id` -> (`doc_id` -> `relevance_score`)
/// * `results` - Search results mapping `query_id` -> (`doc_id` -> `similarity_score`)
/// * `k` - Number of top results to consider for NDCG calculation
///
/// # Returns
/// Average NDCG@k score across all queries in `qrels` (0.0 to 1.0, where 1.0 is perfect
/// ranking). A judged query with no search results scores 0.0 and stays in the average.
///
/// # Errors
/// Returns an error when `qrels` is empty — an average over zero queries is undefined.
///
/// # Reference
/// MTEB `RetrievalEvaluator`: <https://github.com/embeddings-benchmark/mteb/blob/03347ebfe4809056e0fd2894fcae69dcdd2ed964/mteb/evaluation/evaluators/RetrievalEvaluator.py#L500>
#[expect(clippy::cast_precision_loss)]
pub(crate) fn calculate_ndcg<S: ::std::hash::BuildHasher>(
    qrels: &HashMap<String, HashMap<String, i32, S>, S>,
    results: &HashMap<String, HashMap<String, f64, S>, S>,
    k: usize,
) -> anyhow::Result<f64> {
    anyhow::ensure!(
        !qrels.is_empty(),
        "Cannot calculate NDCG: no query relevance judgments were provided"
    );

    let mut ndcg_sum = 0.0;
    for (query_id, relevance) in qrels {
        let Some(ranked_results) = results.get(query_id) else {
            // Scores 0.0: the query still counts toward the average below;
            // skipping it would inflate the overall score.
            println!("No search results found for test query {query_id}");
            continue;
        };

        // `ranked_results` is a HashMap, so its iteration order has no relation to
        // score; NDCG is position-weighted, so results must be sorted by score
        // (descending) before computing gain, or the ranking quality it measures is lost.
        let mut scored_docs: Vec<(&String, &f64)> = ranked_results.iter().collect();
        scored_docs.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
        let relevance_scores: Vec<f64> = scored_docs
            .into_iter()
            .map(|(doc_id, _)| f64::from(*relevance.get(doc_id).unwrap_or(&0)))
            .collect();

        let idcg = ideal_dcg_at_k(relevance, k);
        if idcg > 0.0 {
            ndcg_sum += dcg_at_k(&relevance_scores, k) / idcg;
        }
    }

    Ok(ndcg_sum / qrels.len() as f64)
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

/// The ideal DCG ranks all judged documents for the query by relevance, independent of
/// what the search returned — a result set that misses relevant documents must score
/// below 1.0.
fn ideal_dcg_at_k<S: ::std::hash::BuildHasher>(
    relevance: &HashMap<String, i32, S>,
    k: usize,
) -> f64 {
    let mut relevance_scores: Vec<f64> = relevance
        .values()
        .map(|&score| f64::from(score))
        .collect();
    relevance_scores.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    dcg_at_k(&relevance_scores, k)
}

#[cfg(test)]
mod tests {
    use super::calculate_ndcg;
    use std::collections::HashMap;

    #[test]
    fn ndcg_sorts_results_by_score_before_scoring() {
        // Six distinct relevance grades so any ordering other than
        // score-descending strictly reduces DCG below IDCG.
        let qrels: HashMap<String, HashMap<String, i32>> = HashMap::from([(
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

        // Search scores rank the docs in exactly the same order as their
        // relevance grade, so a correctly score-sorted NDCG@6 is exactly 1.0.
        let results: HashMap<String, HashMap<String, f64>> = HashMap::from([(
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

        let ndcg =
            calculate_ndcg(&qrels, &results, 6).expect("NDCG should be calculable for one query");
        assert!(
            (ndcg - 1.0).abs() < 1e-9,
            "expected a perfect NDCG@6 of 1.0 when search scores exactly match relevance order, got {ndcg}"
        );
    }

    #[test]
    fn ndcg_penalizes_missing_relevant_docs() {
        // Two relevant docs judged, but the search returns only one of them.
        let qrels: HashMap<String, HashMap<String, i32>> = HashMap::from([(
            "q1".to_string(),
            HashMap::from([("doc0".to_string(), 1), ("doc1".to_string(), 1)]),
        )]);
        let results: HashMap<String, HashMap<String, f64>> = HashMap::from([(
            "q1".to_string(),
            HashMap::from([("doc0".to_string(), 0.9)]),
        )]);

        let ndcg = calculate_ndcg(&qrels, &results, 10)
            .expect("NDCG should be calculable for one query");
        // DCG = 1/log2(2) = 1.0; IDCG = 1/log2(2) + 1/log2(3) ≈ 1.6309.
        let expected = 1.0 / (1.0 + 1.0 / 3.0f64.log2());
        assert!(
            (ndcg - expected).abs() < 1e-9,
            "expected NDCG@10 of {expected} when one of two relevant docs is retrieved, got {ndcg}"
        );
    }

    #[test]
    fn ndcg_scores_queries_without_results_as_zero() {
        // Two judged queries; only q1 has search results (a perfect hit).
        let qrels: HashMap<String, HashMap<String, i32>> = HashMap::from([
            (
                "q1".to_string(),
                HashMap::from([("doc0".to_string(), 1)]),
            ),
            (
                "q2".to_string(),
                HashMap::from([("doc1".to_string(), 1)]),
            ),
        ]);
        let results: HashMap<String, HashMap<String, f64>> = HashMap::from([(
            "q1".to_string(),
            HashMap::from([("doc0".to_string(), 0.9)]),
        )]);

        let ndcg = calculate_ndcg(&qrels, &results, 10)
            .expect("NDCG should be calculable for two queries");
        assert!(
            (ndcg - 0.5).abs() < 1e-9,
            "expected NDCG@10 of 0.5 when one of two judged queries has no results, got {ndcg}"
        );
    }

    #[test]
    fn ndcg_errors_on_empty_qrels() {
        let qrels: HashMap<String, HashMap<String, i32>> = HashMap::new();
        let results: HashMap<String, HashMap<String, f64>> = HashMap::new();

        assert!(
            calculate_ndcg(&qrels, &results, 10).is_err(),
            "expected an error when no query relevance judgments are provided"
        );
    }
}
