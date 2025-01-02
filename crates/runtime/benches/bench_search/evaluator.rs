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

/// Evaluate the search results using NDCG@10 metric.
/// Reference: `https://github.com/embeddings-benchmark/mteb/blob/03347ebfe4809056e0fd2894fcae69dcdd2ed964/mteb/evaluation/evaluators/RetrievalEvaluator.py#L500`
#[allow(clippy::cast_precision_loss)]
pub(crate) fn evaluate(
    qrels: &HashMap<String, HashMap<String, i32>>,
    results: &HashMap<String, HashMap<String, f64>>,
) -> f64 {
    // Similar to MTEB report NDCG@10 as the main metric
    const K: usize = 10;
    let mut ndcg_at_k_values = Vec::new();

    for (query_id, relevance) in qrels {
        if let Some(ranked_results) = results.get(query_id) {
            let relevance_scores: Vec<f64> = ranked_results
                .iter()
                .map(|(doc_id, _score)| f64::from(*relevance.get(doc_id).unwrap_or(&0)))
                .collect();
            ndcg_at_k_values.push(ndcg_at_k(&relevance_scores, K));
        } else {
            tracing::warn!("No results found for query {query_id}");
        }
    }
    let len = ndcg_at_k_values.len();
    ndcg_at_k_values.into_iter().sum::<f64>() / len as f64
}

#[allow(clippy::cast_precision_loss)]
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
