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
    path::PathBuf,
    time::{Duration, Instant, SystemTime},
};

use crate::{
    metrics::QueryStatus,
    queries::{QueryOverrides, QuerySet},
};
use anyhow::{Context, Result};
use futures::future::join_all;
use indicatif::{MultiProgress, ProgressBar};
use tokio::task::JoinHandle;

use super::{
    datasets::{self, EndCondition, SpiceTestQueryWorker, SpiceTestQueryWorkers},
    SpiceTest, TestNotStarted, TestState,
};

mod worker;
use worker::{AppendConfig, AppendWorker};

mod sources;
use sources::FileAppendableSource;

#[derive(Default)]
pub struct NotStarted {
    query_set: QuerySet,
    queries: Vec<(&'static str, &'static str)>,
    query_count: usize,
    parallel_count: usize,
    end_duration: Duration,
    tempdir_path: Option<PathBuf>,
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
    pub fn with_query_set(
        mut self,
        query_set: QuerySet,
        overrides: Option<QueryOverrides>,
    ) -> Self {
        self.queries = query_set.get_queries(overrides);
        self.query_count = self.queries.len();
        self.query_set = query_set;
        self
    }

    #[must_use]
    pub fn with_end_duration(mut self, end_duration: Duration) -> Self {
        self.end_duration = end_duration;
        self
    }

    #[must_use]
    pub fn with_tempdir_path(mut self, tempdir_path: PathBuf) -> Self {
        self.tempdir_path = Some(tempdir_path);
        self
    }

    pub fn get_tempdir_path(&self) -> Result<&PathBuf> {
        self.tempdir_path
            .as_ref()
            .context("Start request should be present")
    }
}

pub struct AppendStarted {
    queries: Vec<(&'static str, &'static str)>,
    append_worker: JoinHandle<Result<()>>,
    query_count: usize,
    parallel_count: usize,
    end_duration: Duration,
}

pub struct Running {
    start_time: Instant,
    end_duration: Duration,
    query_workers: SpiceTestQueryWorkers,
    append_worker: JoinHandle<Result<()>>,
    progress_bar: Option<MultiProgress>,
    query_count: usize,
    parallel_count: usize,
}

impl TestState for NotStarted {}
impl TestState for AppendStarted {}
impl TestState for Running {}
impl TestNotStarted for NotStarted {}
impl TestNotStarted for AppendStarted {}

impl SpiceTest<NotStarted> {
    pub async fn start_appending(self) -> Result<SpiceTest<AppendStarted>> {
        if self.state.queries.is_empty() {
            return Err(anyhow::anyhow!("Query set is empty"));
        }

        if self.state.parallel_count == 0 {
            return Err(anyhow::anyhow!("Parallel count must be greater than 0"));
        }

        let append_config = AppendConfig::new(
            self.state.end_duration,
            self.state.query_set,
            self.state.get_tempdir_path()?.clone(),
        );
        let append_source = FileAppendableSource::new(&append_config);

        let append_worker = AppendWorker::new(append_config, Box::new(append_source))
            .start()
            .await?;

        Ok(SpiceTest {
            name: self.name,
            spiced_instance: self.spiced_instance,
            start_time: self.start_time,
            use_progress_bars: self.use_progress_bars,
            api_key: self.api_key,
            explain_plan_snapshot: self.explain_plan_snapshot,
            results_snapshot_predicate: self.results_snapshot_predicate,
            state: AppendStarted {
                queries: self.state.queries.clone(),
                append_worker,
                query_count: self.state.query_count,
                parallel_count: self.state.parallel_count,
                end_duration: self.state.end_duration,
            },
        })
    }
}

impl SpiceTest<AppendStarted> {
    fn get_new_progress_bar(&self) -> ProgressBar {
        ProgressBar::new(self.state.end_duration.as_secs())
    }

    pub async fn start_test(self) -> Result<SpiceTest<Running>> {
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
                    self.state.queries.clone(),
                    EndCondition::Duration(self.state.end_duration),
                    flight_client.clone(),
                    self.name.clone(),
                )
                .with_explain_plan_snapshot(self.explain_plan_snapshot)
                .with_results_snapshot(self.results_snapshot_predicate);

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
                end_duration: self.state.end_duration,
                append_worker: self.state.append_worker,
            },
        })
    }
}

impl SpiceTest<Running> {
    pub async fn wait(self) -> Result<SpiceTest<datasets::Completed>> {
        let mut query_durations = BTreeMap::new();
        let mut query_iteration_durations = BTreeMap::new();
        let mut row_counts = BTreeMap::new();
        let mut query_statuses = BTreeMap::new();
        match self.state.append_worker.await {
            Err(e) => {
                self.state.query_workers.iter().for_each(|worker| {
                    worker.abort();
                });

                return Err(anyhow::anyhow!("Append worker failed: {}", e));
            }
            Ok(Err(e)) => {
                self.state.query_workers.iter().for_each(|worker| {
                    worker.abort();
                });

                return Err(anyhow::anyhow!("Append worker failed: {}", e));
            }
            _ => {}
        }

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
            state: datasets::Completed {
                query_durations,
                query_iteration_durations,
                row_counts,
                query_statuses,
                test_duration: self.state.start_time.elapsed(),
                end_time: SystemTime::now(),
                parallel_count: self.state.parallel_count,
                end_condition: EndCondition::Duration(self.state.end_duration),
                query_count: self.state.query_count,
            },
        })
    }
}
