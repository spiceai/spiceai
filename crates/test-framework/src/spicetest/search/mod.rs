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
    time::{Duration, Instant, SystemTime},
};

use crate::{
    metrics::{
        Builder, BuilderTarget, ExtendedMetrics, MetricCollector, QueryMetric, QueryStatus,
        StatisticsCollector, system_time_to_unix_epoch_ms,
    },
    spicetest::search::evaluate::calculate_retrieval_metrics,
};
use anyhow::{Context, Result};
use arrow::{
    array::{Float64Builder, UInt64Builder},
    datatypes::{DataType, Field},
};
use tokio::task::JoinHandle;

use super::{SpiceTest, TestCompleted, TestNotStarted, TestState};

mod evaluate;
mod latency;
mod worker;
pub use evaluate::RetrievalMetrics;
pub use latency::{LatencyPercentile, LatencySummary, MIN_SAMPLES_BEYOND_PERCENTILE};
pub use worker::{
    CorpusId, SearchApiResponse, SearchConfig, SearchMethod, SearchPasses, SearchRequest,
    SearchResponse, SearchResult, SqlPipeline, SqlSearchHit, SqlSearchResponse,
};
use worker::{VectorSearchWorker, VectorSearchWorkerResult};

pub use evaluate::{QueryId, QueryRetrievalMetrics, RetrievalEvaluation};

#[derive(Default)]
pub struct NotStarted {
    parallel_count: usize,
    config: SearchConfig,
    passes: SearchPasses,
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

    #[must_use]
    pub fn with_passes(mut self, passes: SearchPasses) -> Self {
        self.passes = passes;
        self
    }
}

type VectorSearchWorkers = Vec<JoinHandle<Result<VectorSearchWorkerResult>>>;

pub struct Running {
    vector_workers: VectorSearchWorkers,
}

pub struct Completed {
    end_time: SystemTime,
    /// From the first worker's first measured pass to the last worker's end. Warmup is excluded.
    measured_duration: Duration,
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
        let method = self.state.config.method().clone();
        let passes = self.state.passes;
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
                let worker_config = SearchConfig::new()
                    .with_method(method.clone())
                    .add_requests(chunk.iter().cloned());
                VectorSearchWorker::new(worker_id, http_client.clone(), worker_config, passes)
                    .start()
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
            validate_row_count: self.validate_row_count,
            state: Running {
                vector_workers: workers,
            },
        })
    }
}

impl SpiceTest<Running> {
    pub async fn wait(self) -> Result<SpiceTest<Completed>> {
        let mut search_results = BTreeMap::new();
        let mut measured_window: Option<(Instant, Instant)> = None;

        for worker in self.state.vector_workers {
            let worker_result = worker
                .await
                .context("Error waiting for vector search worker")??;

            measured_window = Some(match measured_window {
                None => (worker_result.measured_started_at, worker_result.finished_at),
                Some((start, end)) => (
                    start.min(worker_result.measured_started_at),
                    end.max(worker_result.finished_at),
                ),
            });

            // Combine all worker results
            search_results.extend(worker_result.search_results);
        }
        let measured_duration = measured_window.map_or(Duration::ZERO, |(start, end)| {
            end.saturating_duration_since(start)
        });

        Ok(SpiceTest {
            name: self.name,
            spiced_instance: self.spiced_instance,
            start_time: self.start_time,
            use_progress_bars: self.use_progress_bars,
            api_key: self.api_key,
            explain_plan_snapshot: self.explain_plan_snapshot,
            results_snapshot_predicate: self.results_snapshot_predicate,
            validate_row_count: self.validate_row_count,
            state: Completed {
                end_time: SystemTime::now(),
                measured_duration,
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

    /// The latency of every measured request, across all queries and passes.
    #[must_use]
    pub fn latency_samples(&self) -> Vec<Duration> {
        self.state
            .search_results
            .values()
            .flat_map(|result| result.durations.iter().copied())
            .collect()
    }

    pub fn get_p95_response_time_metric(&self) -> Result<f64> {
        let durations = self.latency_samples();

        #[expect(clippy::cast_precision_loss)]
        let p95 = durations.percentile(95.0)?.as_millis() as f64;
        Ok(p95)
    }

    /// Measured requests per second. Warmup passes are not counted.
    pub fn get_rps_metric(&self) -> Result<f64> {
        #[expect(clippy::cast_precision_loss)]
        let total_requests = self
            .state
            .search_results
            .values()
            .map(|result| result.durations.len())
            .sum::<usize>() as f64;
        let seconds = self.state.measured_duration.as_secs_f64();
        if seconds <= 0.0 {
            return Ok(total_requests);
        }
        Ok(total_requests / seconds)
    }

    /// Calculate retrieval-quality metrics (NDCG, Recall, MRR, Precision, all at the same rank
    /// cutoff) based on the search results and query relevance data. The `transform` function is
    /// used to convert the search results into a format suitable for evaluation.
    pub fn calculate_search_score_metrics<S, F>(
        &self,
        qrels: &HashMap<String, HashMap<String, i32, S>, S>,
        transform: F,
    ) -> Result<RetrievalMetrics>
    where
        S: ::std::hash::BuildHasher,
        F: Fn(&BTreeMap<String, SearchResult>) -> HashMap<String, HashMap<String, f64, S>, S>,
    {
        let transformed_results = transform(&self.state.search_results);
        // Matches MTEB's methodology of evaluating retrieval quality at rank cutoff 10.
        calculate_retrieval_metrics(qrels, &transformed_results, 10)
    }

    /// Evaluate retrieval quality (NDCG, Recall, MRR, Precision) for every judged query at every
    /// rank cutoff `k` in `1..=n`, where `n` is the largest number of results returned for any
    /// query. Computing the full metric-vs-`k` curve requires no additional search — it is
    /// post-processing over the already-ranked results. The `transform` function converts the
    /// search results into a format suitable for evaluation.
    pub fn evaluate_search_results<S, F>(
        &self,
        qrels: &HashMap<String, HashMap<String, i32, S>, S>,
        transform: F,
    ) -> Result<RetrievalEvaluation>
    where
        S: ::std::hash::BuildHasher,
        F: Fn(&BTreeMap<String, SearchResult>) -> HashMap<String, HashMap<String, f64, S>, S>,
    {
        let transformed_results = transform(&self.state.search_results);
        RetrievalEvaluation::up_to_longest_result_list(qrels, &transformed_results)
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
                    &result.durations,
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

/// The run-level row of a search test. The percentiles are `None` when the run has too few samples
/// (see [`LatencySummary::percentile_ms`]), and the retrieval metrics are `None` when the run has no
/// relevance judgments (a latency-only run).
pub struct SearchRunMetric {
    pub rps: f64,
    pub p95_latency_ms: f64,
    pub latency_samples: u64,
    pub p50_latency_ms: Option<f64>,
    pub p99_latency_ms: Option<f64>,
    pub p999_latency_ms: Option<f64>,
    pub retrieval: Option<RetrievalMetrics>,
}

const FLOAT64_RUN_FIELDS: [&str; 2] = ["rps", "p95_latency_ms"];
const NULLABLE_FLOAT64_RUN_FIELDS: [&str; 7] = [
    "p50_latency_ms",
    "p99_latency_ms",
    "p999_latency_ms",
    // NDCG@10 (kept as `score` for telemetry backward-compatibility).
    "score",
    "recall",
    "mrr",
    "precision",
];

impl ExtendedMetrics for SearchRunMetric {
    fn fields() -> Vec<Field> {
        FLOAT64_RUN_FIELDS
            .iter()
            .map(|name| Field::new(*name, DataType::Float64, false))
            .chain(std::iter::once(Field::new(
                "latency_samples",
                DataType::UInt64,
                false,
            )))
            .chain(
                NULLABLE_FLOAT64_RUN_FIELDS
                    .iter()
                    .map(|name| Field::new(*name, DataType::Float64, true)),
            )
            .collect()
    }

    fn builders() -> BTreeMap<String, Builder> {
        let mut builders: BTreeMap<String, Builder> = FLOAT64_RUN_FIELDS
            .iter()
            .chain(NULLABLE_FLOAT64_RUN_FIELDS.iter())
            .map(|name| ((*name).to_string(), Builder::Float64(Float64Builder::new())))
            .collect();
        builders.insert(
            "latency_samples".to_string(),
            Builder::UInt64(UInt64Builder::new()),
        );
        builders
    }

    fn build(&self) -> Result<Vec<BuilderTarget>> {
        let retrieval = self.retrieval.as_ref();
        Ok(vec![
            BuilderTarget::Float64(("rps".to_string(), self.rps)),
            BuilderTarget::Float64(("p95_latency_ms".to_string(), self.p95_latency_ms)),
            BuilderTarget::UInt64(("latency_samples".to_string(), self.latency_samples)),
            BuilderTarget::NullableFloat64(("p50_latency_ms".to_string(), self.p50_latency_ms)),
            BuilderTarget::NullableFloat64(("p99_latency_ms".to_string(), self.p99_latency_ms)),
            BuilderTarget::NullableFloat64(("p999_latency_ms".to_string(), self.p999_latency_ms)),
            BuilderTarget::NullableFloat64(("score".to_string(), retrieval.map(|r| r.ndcg))),
            BuilderTarget::NullableFloat64(("recall".to_string(), retrieval.map(|r| r.recall))),
            BuilderTarget::NullableFloat64(("mrr".to_string(), retrieval.map(|r| r.mrr))),
            BuilderTarget::NullableFloat64((
                "precision".to_string(),
                retrieval.map(|r| r.precision),
            )),
        ])
    }
}

impl SearchRunMetric {
    #[must_use]
    pub fn new(
        rps: f64,
        p95_latency_ms: f64,
        latency: &LatencySummary,
        retrieval: Option<RetrievalMetrics>,
    ) -> Self {
        Self {
            rps,
            p95_latency_ms,
            latency_samples: u64::try_from(latency.samples()).unwrap_or(u64::MAX),
            p50_latency_ms: latency.percentile_ms(LatencyPercentile::P50),
            p99_latency_ms: latency.percentile_ms(LatencyPercentile::P99),
            p999_latency_ms: latency.percentile_ms(LatencyPercentile::P999),
            retrieval,
        }
    }
}
