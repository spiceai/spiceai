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

use super::HttpConfig;
use crate::metrics::{MetricCollector, NoExtendedMetrics, QueryMetric};
use crate::spicetest::{SpiceTest, TestCompleted, TestNotStarted, TestState};
use crate::utils::get_random_element;
use anyhow::Result;
use indicatif::ProgressBar;
use reqwest::Client;
use std::time::{Duration, Instant, SystemTime};
use tokio::time::timeout;

use std::sync::Arc;
use tokio::task::JoinHandle;

use super::component::HttpComponent;

pub type ConsistencyJobHandle = JoinHandle<Result<ConsistencyResult>>;

#[derive(Default)]
pub struct ConsistencyResult {
    /// The duration of requests, per bucket.
    pub durations: Vec<Vec<Duration>>,
    pub error_count: usize,
}

#[derive(Clone)]
pub struct ConsistencyConfig {
    pub http: HttpConfig,

    /// The number of buckets to divide the test duration into.
    pub buckets: usize,
}
impl ConsistencyConfig {
    #[must_use]
    pub fn new(
        duration: Duration,
        concurrency: usize,
        payloads: Vec<Arc<str>>,
        component: HttpComponent,
        warmup: Duration,
        buckets: usize,
        disable_progress_bars: bool,
    ) -> Self {
        Self {
            http: HttpConfig {
                duration,
                concurrency,
                payloads,
                component,
                warmup,
                disable_progress_bars,
            },
            buckets,
        }
    }
}

pub struct NotStarted {
    config: ConsistencyConfig,
}

impl NotStarted {
    #[must_use]
    pub fn new(config: ConsistencyConfig) -> Self {
        Self { config }
    }
}

pub struct Running {
    config: ConsistencyConfig,
    worker_handles: Vec<ConsistencyJobHandle>,
}

pub struct Completed {
    result: ConsistencyResult,
    end_time: SystemTime,
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
        let client = self.spiced_instance.http_client()?;

        let ConsistencyConfig {
            http:
                HttpConfig {
                    duration,
                    concurrency,
                    payloads,
                    component,
                    warmup,
                    disable_progress_bars,
                },
            buckets,
        } = self.state.config.clone();

        let worker_handles = (0..concurrency)
            .map(|id| {
                let worker = ConsistencyWorker::new(
                    id,
                    duration,
                    warmup,
                    buckets,
                    client.clone(),
                    component.clone(),
                    payloads.clone(),
                )
                .with_show_progress(!disable_progress_bars && id == 0);

                worker.start()
            })
            .collect::<Vec<_>>();

        Ok(SpiceTest {
            name: self.name,
            start_time: self.start_time,
            spiced_instance: self.spiced_instance,
            use_progress_bars: self.use_progress_bars,
            state: Running {
                worker_handles,
                config: self.state.config,
            },
        })
    }
}

impl SpiceTest<Running> {
    pub async fn wait(self) -> Result<SpiceTest<Completed>> {
        let mut error_count = 0;

        let mut durations: Vec<Vec<Duration>> = vec![vec![]; self.state.config.buckets];

        for worker_handle in self.state.worker_handles {
            match worker_handle.await? {
                Ok(worker_result) => {
                    for (i, minute) in worker_result.durations.iter().enumerate() {
                        durations[i].extend(minute);
                    }
                    error_count += worker_result.error_count;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Worker failed: {e:?}"));
                }
            }
        }

        Ok(SpiceTest {
            name: self.name,
            start_time: self.start_time,
            spiced_instance: self.spiced_instance,
            use_progress_bars: self.use_progress_bars,
            state: Completed {
                result: ConsistencyResult {
                    durations,
                    error_count,
                },
                end_time: SystemTime::now(),
            },
        })
    }
}

impl SpiceTest<Completed> {
    #[must_use]
    pub fn get_durations(&self) -> &Vec<Vec<Duration>> {
        &self.state.result.durations
    }
}

impl MetricCollector<NoExtendedMetrics, NoExtendedMetrics> for SpiceTest<Completed> {
    fn start_time(&self) -> SystemTime {
        self.start_time
    }

    fn end_time(&self) -> SystemTime {
        self.state.end_time()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn metrics(&self) -> Result<Vec<QueryMetric<NoExtendedMetrics>>> {
        self.state
            .result
            .durations
            .iter()
            .enumerate()
            .map(|(i, durations)| {
                QueryMetric::new_from_durations(format!("{i}").as_str(), durations)
            })
            .collect()
    }
}

pub(crate) struct ConsistencyWorker {
    id: usize,
    duration: Duration,
    warmup: Duration,
    buckets: usize,
    client: Client,

    show_progress: bool,

    /// The component to test against.
    component: HttpComponent,

    payload: Vec<Arc<str>>,
}

impl ConsistencyWorker {
    pub fn new(
        id: usize,
        duration: Duration,
        warmup: Duration,
        buckets: usize,
        client: Client,
        component: HttpComponent,
        payload: Vec<Arc<str>>,
    ) -> Self {
        Self {
            id,
            client,
            duration,
            buckets,
            warmup,
            component,
            payload,
            show_progress: false,
        }
    }

    pub fn with_show_progress(mut self, show_progress: bool) -> Self {
        self.show_progress = show_progress;
        self
    }

    pub fn start(self) -> ConsistencyJobHandle {
        tokio::spawn(async move {
            let warmup_start = Instant::now();
            if !self.warmup.is_zero() && self.show_progress {
                println!("Warming up...");
            }
            while warmup_start.elapsed() < self.warmup {
                self.send_request().await?;
            }

            let mut durations: Vec<Vec<Duration>> = vec![vec![]; self.buckets];
            let bucket_duration = self.duration.as_secs() / self.buckets as u64;
            let mut error_count = 0;
            let start = Instant::now();

            let mut bar = if self.show_progress {
                println!("Commencing testing...");
                ProgressBar::new(self.duration.as_secs())
            } else {
                ProgressBar::hidden()
            };

            while start.elapsed() < self.duration {
                bar = bar.with_position(start.elapsed().as_secs());
                bar.tick();
                let start_request = Instant::now();
                let remaining = self
                    .duration
                    .checked_sub(start.elapsed())
                    .unwrap_or(Duration::from_secs(0));

                match timeout(remaining, self.send_request()).await {
                    Ok(Ok(request_duration)) => {
                        let idx = usize::try_from(
                            start_request
                                .duration_since(start)
                                .as_secs()
                                .div_euclid(bucket_duration),
                        )?;
                        durations[idx].push(request_duration);
                    }
                    Ok(Err(e)) => {
                        eprintln!("Worker {} - Request failed: {}", self.id, e);
                        error_count += 1;
                        continue;
                    }
                    Err(_) => {
                        eprintln!("Worker {} - Request timed out.", self.id);
                        continue;
                    }
                }
            }

            bar.finish();
            Ok(ConsistencyResult {
                durations,
                error_count,
            })
        })
    }
    async fn send_request(&self) -> Result<Duration> {
        let Some(p) = get_random_element(&self.payload) else {
            eprintln!("Worker {} - No payload found. Exiting...", self.id);
            return Err(anyhow::anyhow!("No payload found"));
        };
        self.component
            .send_request(&self.client, &Arc::clone(p))
            .await
    }
}
