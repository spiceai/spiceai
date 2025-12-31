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
    MetricCollector, QueryMetric, QueryStatus, StatisticsCollector, system_time_to_unix_epoch_ms,
};
use anyhow::{Context, Result};

use super::{SpiceTest, TestCompleted, TestNotStarted, TestState};
mod metrics;
pub use metrics::{TextToSqlMetric, TextToSqlRunMetric};
mod worker;
pub use worker::{TextToSqlConfig, TextToSqlRequest, TextToSqlResult};
use worker::{TextToSqlWorker, TextToSqlWorkerResult};

#[derive(Default)]
pub struct NotStarted {
    config: TextToSqlConfig,
}

impl NotStarted {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_config(mut self, config: TextToSqlConfig) -> Self {
        self.config = config;
        self
    }
}

type TextToSqlWorkers = Vec<tokio::task::JoinHandle<Result<TextToSqlWorkerResult>>>;

pub struct Running {
    workers: TextToSqlWorkers,
}

pub struct Completed {
    end_time: SystemTime,
    results: BTreeMap<String, worker::TextToSqlResult>,
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
    pub async fn start(self) -> Result<SpiceTest<Running>> {
        let spiced_instance = self
            .spiced_instance
            .as_ref()
            .context("Spiced instance should be present")?;
        let spice_client = spiced_instance
            .spice_client(self.api_key.clone(), true)
            .await
            .context("Failed to create Spice client")?;
        let http_client = spiced_instance.http_client()?;

        Ok(SpiceTest {
            name: self.name,
            spiced_instance: self.spiced_instance,
            start_time: self.start_time,
            use_progress_bars: self.use_progress_bars,
            api_key: self.api_key,
            explain_plan_snapshot: self.explain_plan_snapshot,
            results_snapshot_predicate: self.results_snapshot_predicate,
            state: Running {
                workers: vec![
                    TextToSqlWorker::new(http_client, spice_client, self.state.config).start(),
                ],
            },
        })
    }
}

impl SpiceTest<Running> {
    pub async fn wait(self) -> Result<SpiceTest<Completed>> {
        let mut results = BTreeMap::new();

        for worker in self.state.workers {
            let worker_result = worker
                .await
                .context("Error waiting for text-to-sql worker")??;

            results.extend(worker_result.results);
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

impl SpiceTest<Completed> {
    #[must_use]
    pub fn get_results(&self) -> &BTreeMap<String, worker::TextToSqlResult> {
        &self.state.results
    }

    pub fn get_run_metrics(&self) -> Result<TextToSqlRunMetric> {
        Ok(TextToSqlRunMetric::new(
            self.get_p95_response_time_metric()?,
            self.get_median_response_time_metric()?,
            self.get_average_attempts_metric(),
            self.get_exact_match_count(),
            self.get_error_rate(),
        ))
    }

    fn get_exact_match_count(&self) -> f64 {
        let count = self
            .state
            .results
            .values()
            .filter(|result| result.generated_sql.trim() == result.expected_sql.trim())
            .count();

        #[expect(clippy::cast_precision_loss)]
        let rate = count as f64 / self.state.results.len() as f64;
        rate
    }

    fn get_error_rate(&self) -> f64 {
        let errors: f64 = self
            .state
            .results
            .values()
            .map(|result| f64::from(result.is_error))
            .sum();

        #[expect(clippy::cast_precision_loss)]
        let rate = errors / self.state.results.len() as f64;
        rate
    }

    fn get_p95_response_time_metric(&self) -> Result<f64> {
        let durations = self
            .state
            .results
            .values()
            .map(|result| result.duration)
            .collect::<Vec<_>>();

        #[expect(clippy::cast_precision_loss)]
        let p95 = durations.percentile(95.0)?.as_millis() as f64;
        Ok(p95)
    }

    fn get_median_response_time_metric(&self) -> Result<f64> {
        let durations = self
            .state
            .results
            .values()
            .map(|result| result.duration)
            .collect::<Vec<_>>();

        #[expect(clippy::cast_precision_loss)]
        let median = durations.median()?.as_millis() as f64;
        Ok(median)
    }

    fn get_average_attempts_metric(&self) -> f64 {
        let total_attempts: usize = self
            .state
            .results
            .values()
            .map(|result| result.number_of_attempts)
            .sum();

        #[expect(clippy::cast_precision_loss)]
        let avg = total_attempts as f64 / self.state.results.len() as f64;
        avg
    }
}

impl MetricCollector<TextToSqlMetric, TextToSqlRunMetric> for SpiceTest<Completed> {
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

    fn metrics(&self) -> Result<Vec<QueryMetric<TextToSqlMetric>>> {
        self.state
            .results
            .iter()
            .map(|(id, result)| {
                QueryMetric::new_from_durations(
                    id.as_str().into(),
                    &vec![result.duration],
                    QueryStatus::Passed,
                    system_time_to_unix_epoch_ms(self.start_time)?,
                    system_time_to_unix_epoch_ms(self.state.end_time)?,
                )
                .map(|metric| {
                    metric.with_extended_metrics(TextToSqlMetric::new(
                        result.generated_sql.clone(),
                        result.expected_sql.clone(),
                        result.number_of_attempts,
                        result.sample_data_enabled,
                        result.return_sql,
                        result.is_error,
                    ))
                })
            })
            .collect()
    }
}
