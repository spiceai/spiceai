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

use std::{
    collections::{BTreeMap, HashMap},
    time::SystemTime,
};

use crate::{
    metrics::{
        Builder, BuilderTarget, ExtendedMetrics, MetricCollector, QueryMetric, QueryStatus,
        StatisticsCollector, system_time_to_unix_epoch_ms,
    },
    spicetest::search::evaluate::calculate_ndcg,
};
use anyhow::{Context, Result};
use arrow::{
    array::Float64Builder,
    datatypes::{DataType, Field},
};
use tokio::task::JoinHandle;

use super::{SpiceTest, TestCompleted, TestNotStarted, TestState};

mod evaluate;
mod worker;
pub use worker::SearchResult;
pub use worker::{SearchConfig, SearchRequest};
use worker::{VectorSearchWorker, VectorSearchWorkerResult};

#[derive(Default)]
pub struct NotStarted {
    parallel_count: usize,
    config: SearchConfig,
}

impl NotStarted {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_parallel_count(mut self, parallel_count: usize) -> Self {
        self.parallel_count = parallel_count;
        self
    }

    #[must_use]
    pub fn with_config(mut self, config: SearchConfig) -> Self {
        self.config = config;
        self
    }
}

type VectorSearchWorkers = Vec<JoinHandle<Result<VectorSearchWorkerResult>>>;

pub struct Running {
    vector_workers: VectorSearchWorkers,
}

pub struct Completed {
    end_time: SystemTime,
    search_results: BTreeMap<String, worker::SearchResult>,
}

impl TestState for NotStarted {}
impl TestState for Running {}
impl TestState for Completed {}
impl TestNotStarted for NotStarted {}
impl TestCompleted for Completed {
    fn end_time(&self) -> SystemTime {
        self.end_time
    }
}

impl SpiceTest<NotStarted> {
    pub fn start(self) -> Result<SpiceTest<Running>> {
        let http_client = self
            .spiced_instance
            .as_ref()
            .context("Spiced instance should be present")?
            .http_client()?;

        // Split the requests among workers based on the parallel_count
        let requests = self.state.config.into_requests();
        let chunk_size = if self.state.parallel_count > 0 {
            requests.len().div_ceil(self.state.parallel_count)
        } else {
            requests.len()
        };

        let workers = requests
            .chunks(chunk_size)
            .enumerate()
            .map(|(worker_id, chunk)| {
                let worker_config = SearchConfig::new().add_requests(chunk.iter().cloned());
                VectorSearchWorker::new(worker_id, http_client.clone(), worker_config).start()
            })
            .collect();

        Ok(SpiceTest {
            name: self.name,
            spiced_instance: self.spiced_instance,
            start_time: self.start_time,
            use_progress_bars: self.use_progress_bars,
            api_key: self.api_key,
            explain_plan_snapshot: self.explain_plan_snapshot,
            results_snapshot_predicate: self.results_snapshot_predicate,
            state: Running {
                vector_workers: workers,
            },
        })
    }
}

impl SpiceTest<Running> {
    pub async fn wait(self) -> Result<SpiceTest<Completed>> {
        let mut search_results = BTreeMap::new();

        for worker in self.state.vector_workers {
            let worker_result = worker
                .await
                .context("Error waiting for vector search worker")??;

            // Combine all worker results
            search_results.extend(worker_result.search_results);
        }

        Ok(SpiceTest {
            name: self.name,
            spiced_instance: self.spiced_instance,
            start_time: self.start_time,
            use_progress_bars: self.use_progress_bars,
            api_key: self.api_key,
            explain_plan_snapshot: self.explain_plan_snapshot,
            results_snapshot_predicate: self.results_snapshot_predicate,
            state: Completed {
                end_time: SystemTime::now(),
                search_results,
            },
        })
    }
}

impl SpiceTest<Completed> {
    #[must_use]
    pub fn get_search_results(&self) -> &BTreeMap<String, worker::SearchResult> {
        &self.state.search_results
    }

    pub fn get_p95_response_time_metric(&self) -> Result<f64> {
        let durations = self
            .state
            .search_results
            .values()
            .map(|result| result.duration) // Convert to milliseconds
            .collect::<Vec<_>>();

        #[expect(clippy::cast_precision_loss)]
        let p95 = durations.percentile(95.0)?.as_millis() as f64;
        Ok(p95)
    }

    pub fn get_rps_metric(&self) -> Result<f64> {
        let total_duration = self.state.end_time.duration_since(self.start_time)?;

        #[expect(clippy::cast_precision_loss)]
        let total_requests = self.state.search_results.len() as f64;
        if total_duration.as_secs() == 0 {
            return Ok(total_requests);
        }
        Ok(total_requests / total_duration.as_secs_f64())
    }

    /// Calculate overall search score metric based on the search results and query relevance data.
    /// The `transform` function is used to convert the search results into a format suitable for
    /// evaluation
    pub fn calculate_search_score_metric<S, F>(
        &self,
        qrels: &HashMap<String, HashMap<String, i32, S>, S>,
        transform: F,
    ) -> Result<f64>
    where
        S: ::std::hash::BuildHasher,
        F: Fn(&BTreeMap<String, SearchResult>) -> HashMap<String, HashMap<String, f64, S>, S>,
    {
        let transformed_results = transform(&self.state.search_results);
        // Similar to MTEB, use NDCG@10 as the main metric for search score
        Ok(calculate_ndcg(qrels, &transformed_results, 10))
    }
}

impl MetricCollector<SearchScoreMetric, SearchRunMetric> for SpiceTest<Completed> {
    fn start_time(&self) -> SystemTime {
        self.start_time
    }

    fn end_time(&self) -> SystemTime {
        self.state.end_time
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn spiced_version(&self) -> Result<&str> {
        let spiced_instance = self.spiced_instance.as_ref().ok_or(
            anyhow::anyhow!(
                "Spiced instance is not available. SpiceTest must be started before metrics can be collected."
            ))?;

        Ok(spiced_instance.version())
    }

    fn metrics(&self) -> Result<Vec<QueryMetric<SearchScoreMetric>>> {
        self.state
            .search_results
            .iter()
            .map(|(id, result)| {
                QueryMetric::new_from_durations(
                    id.as_str().into(),
                    &vec![result.duration],
                    QueryStatus::Passed,
                    system_time_to_unix_epoch_ms(self.start_time)?,
                    system_time_to_unix_epoch_ms(self.state.end_time)?,
                )
                .map(|metric| metric.with_extended_metrics(SearchScoreMetric::new(result.score)))
            })
            .collect()
    }
}

pub struct SearchScoreMetric {
    pub score: f64,
}
impl ExtendedMetrics for SearchScoreMetric {
    fn fields() -> Vec<Field> {
        vec![Field::new("score", DataType::Float64, false)]
    }

    fn builders() -> BTreeMap<String, Builder> {
        let mut builders = BTreeMap::new();
        builders.insert("score".to_string(), Builder::Float64(Float64Builder::new()));
        builders
    }

    fn build(&self) -> Result<Vec<BuilderTarget>> {
        Ok(vec![BuilderTarget::Float64((
            "score".to_string(),
            self.score,
        ))])
    }
}
impl SearchScoreMetric {
    #[must_use]
    pub fn new(score: f64) -> Self {
        Self { score }
    }
}

pub struct SearchRunMetric {
    pub rps: f64,
    pub p95_latency_ms: f64,
    pub score: f64,
}
impl ExtendedMetrics for SearchRunMetric {
    fn fields() -> Vec<Field> {
        vec![
            Field::new("rps", DataType::Float64, false),
            Field::new("p95_latency_ms", DataType::Float64, false),
            Field::new("score", DataType::Float64, false),
        ]
    }

    fn builders() -> BTreeMap<String, Builder> {
        let mut builders = BTreeMap::new();
        builders.insert("rps".to_string(), Builder::Float64(Float64Builder::new()));
        builders.insert(
            "p95_latency_ms".to_string(),
            Builder::Float64(Float64Builder::new()),
        );
        builders.insert("score".to_string(), Builder::Float64(Float64Builder::new()));
        builders
    }

    fn build(&self) -> Result<Vec<BuilderTarget>> {
        Ok(vec![
            BuilderTarget::Float64(("rps".to_string(), self.rps)),
            BuilderTarget::Float64(("p95_latency_ms".to_string(), self.p95_latency_ms)),
            BuilderTarget::Float64(("score".to_string(), self.score)),
        ])
    }
}
impl SearchRunMetric {
    #[must_use]
    pub fn new(rps: f64, p95_latency_ms: f64, score: f64) -> Self {
        Self {
            rps,
            p95_latency_ms,
            score,
        }
    }
}
