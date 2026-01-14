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
    time::{Duration, SystemTime},
};

use crate::metrics::{MetricCollector, QueryMetric, QueryStatus, system_time_to_unix_epoch_ms};
use anyhow::{Context, Result};

use super::{SpiceTest, TestCompleted, TestNotStarted, TestState};
mod metrics;
pub use metrics::{TextToSqlMetric, TextToSqlRunMetric};
mod worker;
pub use worker::{TextToSqlConfig, TextToSqlRequest};
use worker::{TextToSqlWorker, TextToSqlWorkerResult};
mod task_history;

mod parse;

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
    results: BTreeMap<String, metrics::TextToSqlMetric>,
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
        let http_base_url = spiced_instance.http_base_url().to_string();

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
                    TextToSqlWorker::new(
                        http_client,
                        http_base_url,
                        spice_client,
                        self.state.config,
                    )
                    .start(),
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
    pub fn get_results(&self) -> &BTreeMap<String, metrics::TextToSqlMetric> {
        &self.state.results
    }

    pub fn get_run_metrics(&self) -> Result<TextToSqlRunMetric> {
        #[expect(clippy::cast_precision_loss)]
        Ok(TextToSqlRunMetric::new(
            1000.0 * self.percentile(|result| result.latency_ms, 95.0),
            1000.0 * self.percentile(|result| result.latency_ms, 50.0),
            self.mean(|result| result.generated_sql.trim() == result.expected_sql.trim()),
            self.mean(|result| result.is_error),
            self.mean(|result| result.sql_query_count as f64),
            self.mean(|result| result.llm_input_tokens as f64),
            self.mean(|result| result.llm_output_tokens as f64),
            self.mean(|result| result.exact_logical_plan_match as f64),
            self.mean(|result| result.correct_tables),
            self.mean(|result| result.correct_table_projections),
            self.mean(|result| result.correct_output_schema),
        ))
    }

    fn aggregate<F, T, A>(&self, mut extractor: F, aggregator: A) -> f64
    where
        F: FnMut(&TextToSqlMetric) -> T,
        T: Into<f64>,
        A: FnOnce(Vec<f64>) -> f64,
    {
        let values: Vec<f64> = self
            .state
            .results
            .values()
            .map(|x| extractor(x).into())
            .collect();

        aggregator(values)
    }
    fn mean<F, T>(&self, extractor: F) -> f64
    where
        F: FnMut(&TextToSqlMetric) -> T,
        T: Into<f64>,
    {
        self.aggregate(extractor, |values| {
            let summ: f64 = values.iter().sum();

            #[expect(clippy::cast_precision_loss)]
            let rate = summ / self.state.results.len() as f64;
            rate
        })
    }
    fn percentile<F, T>(&self, extractor: F, percentile: f64) -> f64
    where
        F: FnMut(&TextToSqlMetric) -> T,
        T: Into<f64>,
    {
        self.aggregate(extractor, move |mut values| {
            if values.is_empty() {
                return 0.0;
            }

            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            #[expect(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            let index = ((values.len() - 1) as f64 * percentile / 100.0).round() as usize;
            values[index]
        })
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
                #[expect(clippy::cast_possible_truncation)]
                #[expect(clippy::cast_sign_loss)]
                Ok(QueryMetric::new_from_durations(
                    id.as_str().into(),
                    &vec![Duration::from_millis(result.latency_ms as u64)],
                    QueryStatus::Passed,
                    system_time_to_unix_epoch_ms(self.start_time)?,
                    system_time_to_unix_epoch_ms(self.state.end_time)?,
                )?
                .with_extended_metrics(result.clone()))
            })
            .collect()
    }
}
