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
    time::{Duration, Instant},
};

use crate::spiced::SpicedInstance;
use anyhow::Result;
use futures::future::join_all;
use tokio::task::JoinHandle;
use worker::ThroughputQueryWorker;

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

pub type QueryWorkers = Vec<JoinHandle<Result<BTreeMap<String, Vec<Duration>>>>>;

pub struct Running {
    start_time: Instant,
    query_workers: QueryWorkers,
}
pub struct Completed {
    query_durations: BTreeMap<String, Vec<Duration>>,
    test_duration: Duration,
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
            let query_duration = query_duration??;
            for (query, duration) in query_duration {
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
            state: Completed {
                query_durations,
                test_duration: self.state.start_time.elapsed(),
            },
        })
    }
}

impl ThroughputTest<Completed> {
    pub fn get_statistically_sorted_durations(&self) -> Result<BTreeMap<String, Vec<Duration>>> {
        let mut statistically_sorted_durations = BTreeMap::new();
        for (query, durations) in &self.state.query_durations {
            let mut sorted_durations = durations.clone();
            sorted_durations.sort();

            // calculate the inter-quartile range to remove statistical outliers
            // safety: sorted_durations.len() cannot be negative, and is unlikely to be larger than u32::MAX
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            {
                let first_quartile_index =
                    (f64::from(u32::try_from(sorted_durations.len())?) * 0.25).floor();
                let third_quartile_index =
                    (f64::from(u32::try_from(sorted_durations.len())?) * 0.75).ceil();
                let first_quartile_secs =
                    sorted_durations[first_quartile_index as usize].as_secs_f64();
                let third_quartile_secs =
                    sorted_durations[third_quartile_index as usize].as_secs_f64();

                let iqr = third_quartile_secs - first_quartile_secs;
                let lower_bound = first_quartile_secs - 1.5 * iqr;
                let upper_bound = third_quartile_secs + 1.5 * iqr;

                sorted_durations.retain(|duration| {
                    let duration_secs = duration.as_secs_f64();
                    duration_secs >= lower_bound && duration_secs <= upper_bound
                });
            }

            statistically_sorted_durations.insert(query.clone(), sorted_durations);
        }
        Ok(statistically_sorted_durations)
    }

    pub fn get_duration_percentile(&self, percentile: f64) -> Result<BTreeMap<String, Duration>> {
        let mut slowest_durations = BTreeMap::new();
        for (query, durations) in &self.get_statistically_sorted_durations()? {
            // safety: sorted_durations.len() cannot be negative, and is unlikely to be larger than u32::MAX
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let index = ((percentile * f64::from(u32::try_from(durations.len())?)).ceil() as usize)
                .saturating_sub(1)
                .min(durations.len() - 1);
            slowest_durations.insert(query.clone(), durations[index]);
        }
        Ok(slowest_durations)
    }

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
