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

use std::time::{Duration, Instant};

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
    parallel_count: usize,
    query_set: Vec<(&'static str, &'static str)>,
    end_condition: EndCondition,
}
pub struct Running {
    start_time: Instant,
    query_workers: Vec<JoinHandle<Result<Duration>>>,
}
pub struct Completed {
    cumulative_query_duration: Duration,
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

    state: S,
}

impl ThroughputTest<NotStarted> {
    #[must_use]
    pub fn new(name: String, spiced_instance: SpicedInstance) -> Self {
        Self {
            name,
            spiced_instance,
            state: NotStarted {
                parallel_count: 1,
                query_set: vec![],
                end_condition: EndCondition::QuerySetCompleted(1),
            },
        }
    }

    #[must_use]
    pub fn with_parallel_count(mut self, parallel_count: usize) -> Self {
        self.state.parallel_count = parallel_count;
        self
    }

    #[must_use]
    pub fn with_query_set(mut self, query_set: Vec<(&'static str, &'static str)>) -> Self {
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

        if self.state.parallel_count == 0 {
            return Err(anyhow::anyhow!("Parallel count must be greater than 0"));
        }

        let flight_client = self.spiced_instance.flight_client().await?;
        let query_workers = (1..self.state.parallel_count)
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
            state: Running {
                start_time: Instant::now(),
                query_workers,
            },
        })
    }
}

impl ThroughputTest<Running> {
    pub async fn wait(self) -> Result<ThroughputTest<Completed>> {
        let mut cumulative_query_duration = Duration::ZERO;
        for query_duration in join_all(self.state.query_workers).await {
            cumulative_query_duration += query_duration??;
        }
        Ok(ThroughputTest {
            name: self.name,
            spiced_instance: self.spiced_instance,
            state: Completed {
                cumulative_query_duration,
                test_duration: self.state.start_time.elapsed(),
            },
        })
    }
}

impl ThroughputTest<Completed> {
    #[must_use]
    pub fn get_cumulative_query_duration(&self) -> Duration {
        self.state.cumulative_query_duration
    }

    #[must_use]
    pub fn get_test_duration(&self) -> Duration {
        self.state.test_duration
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
