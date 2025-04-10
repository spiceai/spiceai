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

use std::{collections::BTreeMap, time::SystemTime};

use crate::metrics::{
    Builder, BuilderTarget, ExtendedMetrics, MetricCollector, NoExtendedMetrics, QueryMetric,
    QueryStatus, system_time_to_unix_epoch_ms,
};
use anyhow::{Context, Result};
use arrow::{
    array::Float64Builder,
    datatypes::{DataType, Field},
};
use tokio::task::JoinHandle;

use super::{SpiceTest, TestCompleted, TestNotStarted, TestState};

mod worker;
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
    results: Vec<VectorSearchWorkerResult>,
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

        let workers = (0..self.state.parallel_count)
            .map(|_| {
                VectorSearchWorker::new(http_client.clone(), self.state.config.clone()).start()
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
        let mut results = vec![];
        for worker in self.state.vector_workers {
            // TODO: combine results from multiple workers?
            results.push(
                worker
                    .await
                    .context("Error waiting for vector search worker")??,
            );
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
                results,
            },
        })
    }
}

impl MetricCollector<SearchScoreMetric, NoExtendedMetrics> for SpiceTest<Completed> {
    fn start_time(&self) -> SystemTime {
        self.start_time
    }

    fn end_time(&self) -> SystemTime {
        self.state.end_time
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn metrics(&self) -> Result<Vec<QueryMetric<SearchScoreMetric>>> {
        self.state
            .results
            .first()
            .context("No results found")?
            .search_results
            .iter()
            .map(|(id, result)| {
                QueryMetric::new_from_durations(
                    id,
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
