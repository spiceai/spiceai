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
/// Average NDCG@k score across all queries (0.0 to 1.0, where 1.0 is perfect ranking)
///
/// # Reference
/// MTEB `RetrievalEvaluator`: <https://github.com/embeddings-benchmark/mteb/blob/03347ebfe4809056e0fd2894fcae69dcdd2ed964/mteb/evaluation/evaluators/RetrievalEvaluator.py#L500>
#[expect(clippy::cast_precision_loss)]
#[must_use]
pub(crate) fn calculate_ndcg<S: ::std::hash::BuildHasher>(
    qrels: &HashMap<String, HashMap<String, i32, S>, S>,
    results: &HashMap<String, HashMap<String, f64, S>, S>,
    k: usize,
) -> f64 {
    let mut ndcg_at_k_values = Vec::new();

    for (query_id, relevance) in qrels {
        if let Some(ranked_results) = results.get(query_id) {
            // `ranked_results` is a HashMap, so its iteration order has no relation to
            // score; NDCG is position-weighted, so results must be sorted by score
            // (descending) before computing gain, or the ranking quality it measures is lost.
            let mut scored_docs: Vec<(&String, &f64)> = ranked_results.iter().collect();
            scored_docs.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
            let relevance_scores: Vec<f64> = scored_docs
                .into_iter()
                .map(|(doc_id, _)| f64::from(*relevance.get(doc_id).unwrap_or(&0)))
                .collect();
            ndcg_at_k_values.push(ndcg_at_k(&relevance_scores, k));
        } else {
            println!("No search results found for test query {query_id}");
        }
    }
    let len = ndcg_at_k_values.len();
    ndcg_at_k_values.into_iter().sum::<f64>() / len as f64
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

fn idcg_at_k(relevance_scores: &[f64], k: usize) -> f64 {
    let mut sorted_relevance_scores = relevance_scores.to_owned();
    sorted_relevance_scores.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    dcg_at_k(&sorted_relevance_scores, k)
}

fn ndcg_at_k(relevance_scores: &[f64], k: usize) -> f64 {
    let dcg = dcg_at_k(relevance_scores, k);
    let idcg = idcg_at_k(relevance_scores, k);
    if idcg == 0.0 {
        return 0.0;
    }
    dcg / idcg
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

        let ndcg = calculate_ndcg(&qrels, &results, 6);
        assert!(
            (ndcg - 1.0).abs() < 1e-9,
            "expected a perfect NDCG@6 of 1.0 when search scores exactly match relevance order, got {ndcg}"
        );
    }
}
