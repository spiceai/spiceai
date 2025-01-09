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
    collections::BTreeMap,
    time::{Duration, Instant, SystemTime},
};

use crate::{
    metrics::{MetricCollector, NoExtendedMetrics, QueryMetric, QueryMetrics},
    spiced::SpicedInstance,
};
use anyhow::Result;
use futures::future::join_all;
use tokio::task::JoinHandle;
use worker::{ThroughputQueryWorker, ThroughputQueryWorkerResult};

mod worker;

#[derive(Debug, Clone, Copy)]
pub enum EndCondition {
    Duration(Duration),
    QuerySetCompleted(usize),
}

impl EndCondition {
    #[must_use]
    pub fn is_met(&self, start: &Instant, query_set_count: usize) -> bool {
        match self {
            EndCondition::Duration(duration) => start.elapsed() >= *duration,
            EndCondition::QuerySetCompleted(count) => query_set_count >= *count,
        }
    }
}

pub struct NotStarted {
    query_set: Vec<(&'static str, &'static str)>,
    end_condition: EndCondition,
}

pub type QueryWorkers = Vec<JoinHandle<Result<ThroughputQueryWorkerResult>>>;

pub struct Running {
    start_time: Instant,
    query_workers: QueryWorkers,
}
pub struct Completed {
    query_durations: BTreeMap<String, Vec<Duration>>,
    test_duration: Duration,
    end_time: SystemTime,
}

pub trait TestState {}

impl TestState for NotStarted {}
impl TestState for Running {}
impl TestState for Completed {}

/// A throughput test is a test that runs a set of queries in a loop until a condition is met
/// The test queries can also be run in parallel, each with the same end condition.
pub struct ThroughputTest<S: TestState> {
    name: String,
    spiced_instance: SpicedInstance,
    query_count: usize,
    parallel_count: usize,
    start_time: SystemTime,

    state: S,
}

impl ThroughputTest<NotStarted> {
    #[must_use]
    pub fn new(name: String, spiced_instance: SpicedInstance) -> Self {
        Self {
            name,
            spiced_instance,
            query_count: 0,
            parallel_count: 1,
            start_time: SystemTime::now(),
            state: NotStarted {
                query_set: vec![],
                end_condition: EndCondition::QuerySetCompleted(1),
            },
        }
    }

    #[must_use]
    pub fn with_parallel_count(mut self, parallel_count: usize) -> Self {
        self.parallel_count = parallel_count;
        self
    }

    #[must_use]
    pub fn with_query_set(mut self, query_set: Vec<(&'static str, &'static str)>) -> Self {
        self.query_count = query_set.len();
        self.state.query_set = query_set;
        self
    }

    #[must_use]
    pub fn with_end_condition(mut self, end_condition: EndCondition) -> Self {
        self.state.end_condition = end_condition;
        self
    }

    pub async fn start(self) -> Result<ThroughputTest<Running>> {
        if self.state.query_set.is_empty() {
            return Err(anyhow::anyhow!("Query set is empty"));
        }

        if self.parallel_count == 0 {
            return Err(anyhow::anyhow!("Parallel count must be greater than 0"));
        }

        let flight_client = self.spiced_instance.flight_client().await?;
        let query_workers = (0..self.parallel_count)
            .map(|id| {
                ThroughputQueryWorker::new(
                    id,
                    self.state.query_set.clone(),
                    self.state.end_condition,
                    flight_client.clone(),
                )
            })
            .map(ThroughputQueryWorker::start)
            .collect();

        Ok(ThroughputTest {
            name: self.name,
            spiced_instance: self.spiced_instance,
            query_count: self.query_count,
            parallel_count: self.parallel_count,
            start_time: self.start_time,
            state: Running {
                start_time: Instant::now(),
                query_workers,
            },
        })
    }
}

impl ThroughputTest<Running> {
    pub async fn wait(self) -> Result<ThroughputTest<Completed>> {
        let mut query_durations = BTreeMap::new();
        for query_duration in join_all(self.state.query_workers).await {
            let worker_result = query_duration??;
            if worker_result.connection_failed {
                return Err(anyhow::anyhow!(
                    "Test failed - a connection failed during the test"
                ));
            }

            for (query, duration) in worker_result.query_durations {
                query_durations
                    .entry(query)
                    .or_insert_with(Vec::new)
                    .extend(duration);
            }
        }
        Ok(ThroughputTest {
            name: self.name,
            spiced_instance: self.spiced_instance,
            query_count: self.query_count,
            parallel_count: self.parallel_count,
            start_time: self.start_time,
            state: Completed {
                query_durations,
                test_duration: self.state.start_time.elapsed(),
                end_time: SystemTime::now(),
            },
        })
    }
}

impl ThroughputTest<Completed> {
    #[must_use]
    pub fn get_query_durations(&self) -> &BTreeMap<String, Vec<Duration>> {
        &self.state.query_durations
    }

    #[must_use]
    pub fn get_cumulative_query_duration(&self) -> Duration {
        self.state.query_durations.values().flatten().copied().sum()
    }

    #[must_use]
    pub fn get_test_duration(&self) -> Duration {
        self.state.test_duration
    }

    pub fn get_throughput_metric(&self, scale: f64) -> Result<f64> {
        // metric = (Parallel Query Count * Test Suite Query Count * 3600) / Cs * Scale
        let lhs = self.parallel_count * self.query_count * 3600;
        let rhs = self.get_cumulative_query_duration().as_secs_f64() * scale;
        // u32 is safe because lhs is unlikely to be greater than u32::MAX unless some extreme parameters are used (more than 1000 parallel and query count)
        Ok(f64::from(u32::try_from(lhs)?) / rhs)
    }

    /// Once the test has completed, return ownership of the spiced instance
    #[must_use]
    pub fn end(self) -> SpicedInstance {
        self.spiced_instance
    }
}

impl std::fmt::Display for ThroughputTest<Completed> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ThroughputTest: {} - Cumulative query duration: {} seconds, Test duration: {} seconds",
            self.name,
            self.get_cumulative_query_duration().as_secs_f32(),
            self.get_test_duration().as_secs_f32()
        )
    }
}

impl MetricCollector<NoExtendedMetrics> for ThroughputTest<Completed> {
    fn collect(&self) -> Result<QueryMetrics<NoExtendedMetrics>> {
        let query_metrics = self
            .get_query_durations()
            .iter()
            .map(|(query, durations)| QueryMetric::new_from_durations(query, durations))
            .collect::<Result<Vec<_>>>()?;

        Ok(QueryMetrics {
            run_id: uuid::Uuid::new_v4(),
            run_name: self.name.clone(),
            commit_sha: "TODO".to_string(),
            branch_name: "TODO".to_string(),
            test_type: crate::TestType::Throughput,
            started_at: usize::try_from(
                self.start_time
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs(),
            )?,
            finished_at: usize::try_from(
                self.state
                    .end_time
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs(),
            )?,
            metrics: query_metrics,
        })
    }
}
