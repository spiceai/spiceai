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
    metrics::{
        DatasetMetrics, MetricCollector, NoExtendedMetrics, QueryMetric, QueryStatus,
        ThroughputMetrics, system_time_to_unix_epoch_ms,
    },
    queries::Query,
};
use anyhow::Result;
use futures::future::join_all;
use indicatif::{MultiProgress, ProgressBar};
use tokio::task::JoinHandle;

use super::{SpiceTest, TestCompleted, TestNotStarted, TestState};
mod worker;
pub(crate) use worker::{SpiceTestQueryWorker, SpiceTestQueryWorkerResult};

#[derive(Debug, Clone, Copy)]
pub enum EndCondition {
    Duration(Duration),
    QuerySetCompleted(usize),
}

impl Default for EndCondition {
    fn default() -> Self {
        EndCondition::QuerySetCompleted(1)
    }
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

#[derive(Default)]
pub struct NotStarted {
    query_set: Vec<Query>,
    end_condition: EndCondition,
    query_count: usize,
    parallel_count: usize,
    validate: bool,
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
    pub fn with_query_set(mut self, query_set: Vec<Query>) -> Self {
        self.query_count = query_set.len();
        self.query_set = query_set;
        self
    }

    #[must_use]
    pub fn with_end_condition(mut self, end_condition: EndCondition) -> Self {
        self.end_condition = end_condition;
        self
    }

    #[must_use]
    pub fn with_validate(mut self, validate: bool) -> Self {
        self.validate = validate;
        self
    }
}

pub type SpiceTestQueryWorkers = Vec<JoinHandle<Result<SpiceTestQueryWorkerResult>>>;

pub struct Running {
    start_time: Instant,
    query_workers: SpiceTestQueryWorkers,
    progress_bar: Option<MultiProgress>,
    query_count: usize,
    parallel_count: usize,
    end_condition: EndCondition,
}
pub struct Completed {
    pub(crate) query_durations: BTreeMap<String, Vec<Duration>>,
    pub(crate) query_iteration_durations: BTreeMap<String, (SystemTime, SystemTime)>,
    pub(crate) row_counts: BTreeMap<String, Vec<usize>>,
    pub(crate) query_statuses: BTreeMap<String, QueryStatus>,
    pub(crate) test_duration: Duration,
    pub(crate) end_time: SystemTime,
    pub(crate) query_count: usize,
    pub(crate) parallel_count: usize,
    pub(crate) end_condition: EndCondition,
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
    fn get_new_progress_bar(&self) -> ProgressBar {
        match self.state.end_condition {
            EndCondition::Duration(duration) => {
                // refresh the progress bar every 10 seconds, or every 1/1000th of the duration
                // for an 8 hour test, this would be every ~28 seconds
                ProgressBar::new((duration.as_secs() / self.state.query_set.len() as u64) * 2)
                // this isn't 100% representative of the progress bar count, but it's close enough (2s per query)
            }
            EndCondition::QuerySetCompleted(count) => {
                ProgressBar::new((self.state.query_set.len() * count) as u64)
            }
        }
    }

    pub async fn start(self) -> Result<SpiceTest<Running>> {
        if self.state.query_set.is_empty() {
            return Err(anyhow::anyhow!("Query set is empty"));
        }

        if self.state.parallel_count == 0 {
            return Err(anyhow::anyhow!("Parallel count must be greater than 0"));
        }

        let multi = if self.use_progress_bars {
            Some(MultiProgress::new())
        } else {
            None
        };

        let flight_client = self
            .get_spiced()?
            .flight_client(self.api_key.clone())
            .await?;
        let query_workers = (0..self.state.parallel_count)
            .map(|id| {
                let worker = SpiceTestQueryWorker::new(
                    id,
                    self.state.query_set.clone(),
                    self.state.end_condition,
                    flight_client.clone(),
                    self.name.clone(),
                )
                .with_explain_plan_snapshot(self.explain_plan_snapshot)
                .with_results_snapshot(self.results_snapshot_predicate)
                .with_validate(self.state.validate);

                if let Some(multi) = &multi {
                    worker.with_progress_bar(multi.add(self.get_new_progress_bar()))
                } else {
                    worker
                }
            })
            .map(SpiceTestQueryWorker::start)
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
                start_time: Instant::now(),
                query_workers,
                progress_bar: multi,
                query_count: self.state.query_count,
                parallel_count: self.state.parallel_count,
                end_condition: self.state.end_condition,
            },
        })
    }
}

impl SpiceTest<Running> {
    pub async fn wait(self) -> Result<SpiceTest<Completed>> {
        let mut query_durations = BTreeMap::new();
        let mut query_iteration_durations = BTreeMap::new();
        let mut row_counts = BTreeMap::new();
        let mut query_statuses = BTreeMap::new();
        for worker_result in join_all(self.state.query_workers).await {
            let worker_result = worker_result??;
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

            for (query, iteration_durations) in worker_result.query_iteration_durations {
                query_iteration_durations
                    .entry(query)
                    .or_insert_with(|| iteration_durations);
            }

            for (query, query_row_counts) in worker_result.row_counts {
                row_counts
                    .entry(query)
                    .or_insert_with(Vec::new)
                    .extend(query_row_counts);
            }

            for (query, worker_status) in worker_result.query_statuses {
                query_statuses
                    .entry(query)
                    .and_modify(|existing_status| {
                        // If the worker reports failure, update the status to Failed
                        if worker_status == QueryStatus::Failed {
                            *existing_status = QueryStatus::Failed;
                        }
                    })
                    .or_insert(worker_status);
            }
        }

        if let Some(multi) = self.state.progress_bar {
            multi.clear()?;
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
                query_durations,
                query_iteration_durations,
                row_counts,
                query_statuses,
                test_duration: self.state.start_time.elapsed(),
                end_time: SystemTime::now(),
                query_count: self.state.query_count,
                parallel_count: self.state.parallel_count,
                end_condition: self.state.end_condition,
            },
        })
    }
}

impl SpiceTest<Completed> {
    #[must_use]
    pub fn get_query_durations(&self) -> &BTreeMap<String, Vec<Duration>> {
        &self.state.query_durations
    }

    /// Returns the start and end time of for all consecutive query iterations of the same query in a set.
    ///
    /// Only valid when the `EndCondition` is `QuerySetCompleted`.
    #[must_use]
    pub fn get_query_iteration_durations(&self) -> &BTreeMap<String, (SystemTime, SystemTime)> {
        &self.state.query_iteration_durations
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
        let lhs = self.state.parallel_count * self.state.query_count * 3600;

        // u32 is safe because lhs is unlikely to be greater than u32::MAX unless some extreme parameters are used (more than 1000 parallel and query count)
        let lhs = f64::from(u32::try_from(lhs)?);

        // because we perform query sets one after the other, we're not 100% like the original TPCH QpH metric because it expects to only run once
        // apply a modifier based on the query set count
        // e.g. query set count of 5, means our calculated QpH needs to be times 5 because we ran 5 query sets
        // this adjusts for a longer test duration as a result of running multiple query sets, which would otherwise reduce the QpH
        let end_condition_modifier = match self.state.end_condition {
            EndCondition::Duration(_) => {
                return Err(anyhow::anyhow!(
                    "Throughput metric calculation for duration-based tests is not supported. Use a QuerySetCompleted test instead."
                ));
            }
            EndCondition::QuerySetCompleted(count) => f64::from(u32::try_from(count)?),
        };

        Ok((lhs / self.state.test_duration.as_secs_f64()) * scale * end_condition_modifier)
    }

    /// Validates that row counts are consistent across queries
    /// Each query should return the same number of rows
    pub fn validate_returned_row_counts(&self) -> Result<BTreeMap<String, usize>> {
        // validate that row counts are consistent across queries - each query should return the same number of rows
        let mut returned_row_counts = BTreeMap::new();
        for (query, counts) in &self.state.row_counts {
            let first = counts
                .first()
                .ok_or_else(|| anyhow::anyhow!("No row counts found for query {}", query))?;
            if !counts.iter().all(|count| count == first) {
                return Err(anyhow::anyhow!(
                    "Row counts for query {} are inconsistent",
                    query
                ));
            }

            returned_row_counts.insert(query.clone(), *first);
        }

        Ok(returned_row_counts)
    }

    /// Returns true if every query status is passed. Returns false if any query status is failed.
    #[must_use]
    pub fn succeeded(&self) -> bool {
        self.state
            .query_statuses
            .values()
            .all(|status| status == &QueryStatus::Passed)
    }
}

impl std::fmt::Display for SpiceTest<Completed> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SpiceTest: {} - Cumulative query duration: {} seconds, Test duration: {} seconds",
            self.name,
            self.get_cumulative_query_duration().as_secs_f32(),
            self.get_test_duration().as_secs_f32()
        )
    }
}

impl MetricCollector<DatasetMetrics, NoExtendedMetrics> for SpiceTest<Completed> {
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

    fn metrics(&self) -> Result<Vec<QueryMetric<DatasetMetrics>>> {
        self.get_query_durations()
            .iter()
            .map(|(query, durations)| {
                let query_iteration_durations = self.state.query_iteration_durations.get(query);
                let query_start_time =
                    query_iteration_durations.map_or(self.start_time, |(start, _)| *start);
                let query_end_time =
                    query_iteration_durations.map_or(self.state.end_time, |(_, end)| *end);
                let query_status = self
                    .state
                    .query_statuses
                    .get(query)
                    .unwrap_or(&QueryStatus::Failed)
                    .to_owned();

                let metric = QueryMetric::new_from_durations(
                    query,
                    durations,
                    query_status,
                    system_time_to_unix_epoch_ms(query_start_time)?,
                    system_time_to_unix_epoch_ms(query_end_time)?,
                );

                metric.map(|metric| {
                    metric.with_extended_metrics(DatasetMetrics::new(self.name.clone()))
                })
            })
            .collect::<Result<Vec<_>>>()
    }
}

impl MetricCollector<NoExtendedMetrics, ThroughputMetrics> for SpiceTest<Completed> {
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

    fn metrics(&self) -> Result<Vec<QueryMetric<NoExtendedMetrics>>> {
        self.get_query_durations()
            .iter()
            .map(|(query, durations)| {
                let query_iteration_durations = self.state.query_iteration_durations.get(query);
                let query_start_time =
                    query_iteration_durations.map_or(self.start_time, |(start, _)| *start);
                let query_end_time =
                    query_iteration_durations.map_or(self.state.end_time, |(_, end)| *end);
                let query_status = self
                    .state
                    .query_statuses
                    .get(query)
                    .unwrap_or(&QueryStatus::Failed)
                    .to_owned();

                QueryMetric::new_from_durations(
                    query,
                    durations,
                    query_status,
                    system_time_to_unix_epoch_ms(query_start_time)?,
                    system_time_to_unix_epoch_ms(query_end_time)?,
                )
            })
            .collect::<Result<Vec<_>>>()
    }
}
