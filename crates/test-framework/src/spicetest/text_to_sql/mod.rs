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

use crate::metrics::{MetricCollector, QueryMetric, QueryStatus, system_time_to_unix_epoch_ms};
use anyhow::{Context, Result};

use super::{SpiceTest, TestCompleted, TestNotStarted, TestState};
mod metrics;
pub use metrics::{TextToSqlMetric, TextToSqlRunMetric};
mod worker;
pub use worker::{TextToSqlConfig, TextToSqlRequest, TextToSqlResult};
use worker::{TextToSqlWorker, TextToSqlWorkerResult};
mod task_history;

#[derive(Default)]
pub struct NotStarted {
    config: TextToSqlConfig,
    parallel_count: usize,
}

impl NotStarted {
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: TextToSqlConfig::default(),
            parallel_count: 1,
        }
    }

    #[must_use]
    pub fn with_config(mut self, config: TextToSqlConfig) -> Self {
        self.config = config;
        self
    }

    #[must_use]
    pub fn with_parallel_count(mut self, parallel_count: usize) -> Self {
        self.parallel_count = parallel_count;
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

        if self.state.parallel_count == 0 {
            return Err(anyhow::anyhow!("Parallel count must be greater than 0"));
        }

        let requests = self.state.config.requests;

        // Use a smaller buffer to limit memory usage - workers pull concurrently
        let buffer_size = (self.state.parallel_count * 2).max(1);
        let (tx, rx) = async_channel::bounded::<TextToSqlRequest>(buffer_size);

        // Add tasks to channel.
        tokio::spawn(async move {
            for request in requests {
                if let Err(e) = tx.send(request).await {
                    eprintln!("Failed to send request to workers: {e}");
                    break;
                }
            }
        });

        // Create workers, each pulling from the shared channel
        let mut workers = Vec::with_capacity(self.state.parallel_count);
        for id in 0..self.state.parallel_count {
            let spice_client = spiced_instance
                .spice_client(self.api_key.clone(), true)
                .await
                .context("Failed to create Spice client")?;
            let http_client = spiced_instance.http_client()?;
            let http_base_url = spiced_instance.http_base_url().to_string();

            workers.push(
                TextToSqlWorker::new(id, http_client, http_base_url, spice_client, rx.clone())
                    .start(),
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
            state: Running { workers },
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
            self.get_p95_response_time_metric(),
            self.get_median_response_time_metric(),
            self.get_exact_match_count(),
            self.get_error_rate(),
            self.get_mean_sql_query_count(),
            self.get_mean_llm_input_tokens(),
            self.get_mean_llm_output_tokens(),
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

    fn aggregate<F, T, A>(&self, mut extractor: F, aggregator: A) -> f64
    where
        F: FnMut(&TextToSqlResult) -> T,
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
        F: FnMut(&TextToSqlResult) -> T,
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
        F: FnMut(&TextToSqlResult) -> T,
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

    fn get_error_rate(&self) -> f64 {
        self.mean(|result| result.is_error)
    }

    fn get_p95_response_time_metric(&self) -> f64 {
        1000.0 * self.percentile(|result| result.duration.as_secs_f64(), 95.0)
    }

    fn get_median_response_time_metric(&self) -> f64 {
        1000.0 * self.percentile(|result| result.duration.as_secs_f64(), 50.0)
    }

    #[expect(clippy::cast_precision_loss)]
    fn get_mean_sql_query_count(&self) -> f64 {
        self.mean(|result| result.query_count as f64)
    }

    #[expect(clippy::cast_precision_loss)]
    fn get_mean_llm_input_tokens(&self) -> f64 {
        self.mean(|result| result.llm_input_tokens as f64)
    }

    #[expect(clippy::cast_precision_loss)]
    fn get_mean_llm_output_tokens(&self) -> f64 {
        self.mean(|result| result.llm_output_tokens as f64)
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
                #[expect(clippy::cast_precision_loss)]
                let latency_ms = result.duration.as_millis() as f64;
                QueryMetric::new_from_durations(
                    id.as_str().into(),
                    &vec![result.duration],
                    QueryStatus::Passed,
                    system_time_to_unix_epoch_ms(self.start_time)?,
                    system_time_to_unix_epoch_ms(self.state.end_time)?,
                )
                .map(|metric| {
                    metric.with_extended_metrics(TextToSqlMetric::new(
                        result.question.clone(),
                        result.generated_sql.clone(),
                        result.expected_sql.clone(),
                        result.query_count,
                        result.sample_data_enabled,
                        result.return_sql,
                        result.is_error,
                        latency_ms,
                        result.sql_duration_ms,
                        result.llm_duration_ms,
                        result.llm_count,
                        result.llm_input_tokens,
                        result.llm_output_tokens,
                    ))
                })
            })
            .collect()
    }
}
