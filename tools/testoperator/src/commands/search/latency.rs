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

//! Latency percentiles of a `testoperator run search` run, for all queries and for each query type
//! (the optional `test_queries.query_type` column).

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use test_framework::{
    anyhow,
    opentelemetry::KeyValue,
    spicetest::search::{LatencyPercentile, LatencySummary, SearchResult},
};

/// The `query_type` attribute of the percentiles over every query.
const ALL_QUERIES: &str = "all";

pub(crate) struct LatencyReport {
    pub(crate) overall: LatencySummary,
    by_query_type: BTreeMap<String, LatencySummary>,
}

impl LatencyReport {
    pub(crate) fn new(
        results: &BTreeMap<String, SearchResult>,
        query_types: &HashMap<String, String>,
    ) -> Self {
        let mut overall = Vec::new();
        let mut by_query_type: BTreeMap<String, Vec<Duration>> = BTreeMap::new();
        for (query_id, result) in results {
            overall.extend_from_slice(&result.durations);
            if let Some(query_type) = query_types.get(query_id) {
                by_query_type
                    .entry(query_type.clone())
                    .or_default()
                    .extend_from_slice(&result.durations);
            }
        }

        Self {
            overall: LatencySummary::new(overall),
            by_query_type: by_query_type
                .into_iter()
                .map(|(query_type, samples)| (query_type, LatencySummary::new(samples)))
                .collect(),
        }
    }

    fn groups(&self) -> impl Iterator<Item = (&str, &LatencySummary)> {
        std::iter::once((ALL_QUERIES, &self.overall)).chain(
            self.by_query_type
                .iter()
                .map(|(query_type, summary)| (query_type.as_str(), summary)),
        )
    }

    /// Prints one row per group. `-` marks a percentile with too few samples beyond it.
    pub(crate) fn print(&self) {
        println!("Search latency (ms):");
        print!("{:<20}  {:>9}", "query_type", "samples");
        for percentile in LatencyPercentile::ALL {
            print!("  {:>9}", percentile.label());
        }
        println!();

        for (query_type, summary) in self.groups() {
            print!("{query_type:<20}  {:>9}", summary.samples());
            for percentile in LatencyPercentile::ALL {
                match summary.percentile_ms(percentile) {
                    Some(ms) => print!("  {ms:>9.3}"),
                    None => print!("  {:>9}", "-"),
                }
            }
            println!();
        }
    }

    pub(crate) fn record_metrics(&self) -> anyhow::Result<()> {
        for (query_type, summary) in self.groups() {
            let query_type_attr = KeyValue::new("query_type", query_type.to_string());
            crate::metrics::SEARCH_LATENCY_SAMPLES.record(
                u64::try_from(summary.samples())?,
                std::slice::from_ref(&query_type_attr),
            );
            for percentile in LatencyPercentile::ALL {
                if let Some(ms) = summary.percentile_ms(percentile) {
                    crate::metrics::SEARCH_LATENCY.record(
                        ms,
                        &[
                            query_type_attr.clone(),
                            KeyValue::new("percentile", percentile.label()),
                        ],
                    );
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::LatencyReport;
    use std::{
        collections::{BTreeMap, HashMap},
        time::Duration,
    };
    use test_framework::spicetest::search::{SearchApiResponse, SearchResponse, SearchResult};

    fn result(millis: &[u64]) -> SearchResult {
        SearchResult {
            response: SearchResponse::SearchApi(SearchApiResponse {
                results: vec![],
                duration_ms: None,
            }),
            score: 0.0,
            durations: millis.iter().copied().map(Duration::from_millis).collect(),
        }
    }

    #[test]
    fn groups_samples_by_query_type() {
        let results = BTreeMap::from([
            ("q1".to_string(), result(&[1, 2])),
            ("q2".to_string(), result(&[3])),
            ("q3".to_string(), result(&[4, 5, 6])),
        ]);
        let query_types = HashMap::from([
            ("q1".to_string(), "union".to_string()),
            ("q3".to_string(), "union".to_string()),
            ("q2".to_string(), "phrase".to_string()),
        ]);

        let report = LatencyReport::new(&results, &query_types);
        let samples: Vec<(&str, usize)> = report
            .groups()
            .map(|(query_type, summary)| (query_type, summary.samples()))
            .collect();
        assert_eq!(samples, vec![("all", 6), ("phrase", 1), ("union", 5)]);
    }

    #[test]
    fn untyped_queries_only_count_toward_all() {
        let results = BTreeMap::from([("q1".to_string(), result(&[1, 2, 3]))]);
        let report = LatencyReport::new(&results, &HashMap::new());
        let groups: Vec<&str> = report.groups().map(|(query_type, _)| query_type).collect();
        assert_eq!(groups, vec!["all"]);
        assert_eq!(report.overall.samples(), 3);
    }
}
