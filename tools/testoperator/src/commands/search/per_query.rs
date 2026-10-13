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

//! Per-query retrieval metrics of a `testoperator run search` run: OTEL gauges at the primary
//! rank cutoff, and an optional JSON file with every cutoff. Two runs over the same query set pair
//! up by query id, which is the input a paired significance test between two runs needs.

use std::{borrow::Cow, collections::BTreeMap, io::BufWriter, path::Path};

use serde::{Deserialize, Serialize};
use test_framework::{
    anyhow, opentelemetry::KeyValue, opentelemetry_sdk::Resource,
    spicetest::search::RetrievalEvaluation,
};

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

/// Records each judged query's metrics at cutoff `k`, with `query_id` and `k` attributes. The
/// dataset and spicepod attributes come from the telemetry resource, as for the run-level metrics.
///
/// Only one cutoff is recorded, so a run adds 4 points per judged query. All cutoffs would
/// multiply that by `max_k`; they are in the `--per-query-output` file instead.
pub(crate) fn record_metrics(evaluation: &RetrievalEvaluation, k: usize) -> anyhow::Result<()> {
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
    Ok(())
}

/// Contents of the `--per-query-output` file.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PerQueryReport<'a> {
    /// The run's telemetry resource attributes, such as `benchmark_dataset`, `name`, and
    /// `spiced_version`, so a file identifies the run it came from.
    attributes: BTreeMap<String, String>,
    /// Rank cutoff of the run-level metrics and of the per-query gauges.
    primary_k: usize,
    evaluation: Cow<'a, RetrievalEvaluation>,
}

impl<'a> PerQueryReport<'a> {
    pub(crate) fn new(
        resource: &Resource,
        primary_k: usize,
        evaluation: &'a RetrievalEvaluation,
    ) -> Self {
        let attributes = resource
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        Self {
            attributes,
            primary_k,
            evaluation: Cow::Borrowed(evaluation),
        }
    }

    pub(crate) fn write(&self, path: &Path) -> anyhow::Result<()> {
        let file = std::fs::File::create(path).map_err(|e| {
            anyhow::anyhow!(
                "Failed to create per-query output file {}: {e}",
                path.display()
            )
        })?;
        serde_json::to_writer(BufWriter::new(file), self)?;
        println!("Wrote per-query retrieval metrics to {}", path.display());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{PerQueryReport, primary_k};
    use test_framework::{
        opentelemetry::KeyValue, opentelemetry_sdk::Resource,
        spicetest::search::RetrievalEvaluation,
    };

    fn evaluation(max_k: usize) -> RetrievalEvaluation {
        let metrics_at_k: Vec<_> = (1..=max_k)
            .map(|k| {
                serde_json::json!({
                    "k": k, "ndcg": 0.5, "recall": 1.0, "reciprocal_rank": 0.5, "precision": 0.1,
                })
            })
            .collect();
        serde_json::from_value(serde_json::json!({
            "max_k": max_k,
            "queries": [
                {"query_id": "q1", "result_count": max_k, "metrics_at_k": metrics_at_k},
            ],
        }))
        .expect("valid evaluation")
    }

    #[test]
    fn primary_k_is_ten_or_the_largest_evaluated_cutoff() {
        assert_eq!(primary_k(&evaluation(12)), Some(10));
        assert_eq!(primary_k(&evaluation(10)), Some(10));
        assert_eq!(primary_k(&evaluation(3)), Some(3));
        assert_eq!(primary_k(&evaluation(0)), None);
    }

    #[test]
    fn report_round_trips_with_run_attributes() {
        let resource = Resource::builder_empty()
            .with_attributes([
                KeyValue::new("benchmark_dataset", "scifact_retrieval"),
                KeyValue::new("name", "hybrid"),
            ])
            .build();
        let evaluation = evaluation(10);
        let report = PerQueryReport::new(&resource, 10, &evaluation);

        let json = serde_json::to_value(&report).expect("serializes");
        assert_eq!(json["attributes"]["benchmark_dataset"], "scifact_retrieval");
        assert_eq!(json["primary_k"], 10);
        assert_eq!(json["evaluation"]["queries"][0]["query_id"], "q1");
        assert_eq!(
            json["evaluation"]["queries"][0]["metrics_at_k"][9]["reciprocal_rank"],
            0.5
        );

        let parsed: PerQueryReport<'_> = serde_json::from_value(json).expect("deserializes");
        assert_eq!(parsed.evaluation.as_ref(), &evaluation);
        assert_eq!(parsed.attributes["name"], "hybrid");
    }
}
