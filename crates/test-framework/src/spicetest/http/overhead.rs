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

use super::component::HttpComponent;
use super::HttpConfig;
use crate::metrics::{MetricCollector, NoExtendedMetrics, QueryMetric};
use crate::spicetest::{SpiceTest, TestCompleted, TestNotStarted, TestState};
use crate::utils::get_random_element;
use anyhow::Result;
use futures::future::try_join_all;
use reqwest::Client;
use std::time::{Duration, Instant, SystemTime};

use std::sync::Arc;
use tokio::task::JoinHandle;

pub type OverheadJobHandle = JoinHandle<Result<OverheadResult>>;

#[derive(Default)]
pub struct OverheadResult {
    pub durations: Vec<Duration>,
    pub error_count: usize,
}

pub struct NotStarted {
    config: HttpConfig,
    baseline: BaselineConfig,
}

pub struct BaselineConfig {
    component: HttpComponent,
    client: Client,
    payloads: Vec<Arc<str>>,
}

impl BaselineConfig {
    #[must_use]
    pub fn new(component: HttpComponent, client: Client, payloads: Vec<Arc<str>>) -> Self {
        Self {
            component,
            client,
            payloads,
        }
    }
}

impl NotStarted {
    #[must_use]
    pub fn new(config: HttpConfig, baseline: BaselineConfig) -> Self {
        Self { config, baseline }
    }
}

pub struct Running {
    /// Workers sending traffic to the baseline HTTP component
    baseline_handles: Vec<OverheadJobHandle>,

    /// Workers sending traffic to the Spice HTTP component
    spice_handles: Vec<OverheadJobHandle>,
}

pub struct Completed {
    baseline_results: OverheadResult,
    spice_results: OverheadResult,
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
    // On start, we create:
    //  - N workers to send requests to the baseline component
    //  - N (separate) workers to send requests the spice component.
    pub fn start(self) -> Result<SpiceTest<Running>> {
        let spiced_client = self.spiced_instance.http_client()?;

        let baseline_handles = (0..self.state.config.concurrency)
            .map(|id| {
                let worker = OverHeadWorker::new(
                    id,
                    self.state.config.duration,
                    self.state.baseline.payloads.clone(),
                    self.state.baseline.component.clone(),
                    self.state.baseline.client.clone(),
                );
                worker.start()
            })
            .collect::<Vec<_>>();

        let spice_handles = (0..self.state.config.concurrency)
            .map(|id| {
                let worker = OverHeadWorker::new(
                    id,
                    self.state.config.duration,
                    self.state.config.payloads.clone(),
                    self.state.config.component.clone(),
                    spiced_client.clone(),
                );
                worker.start()
            })
            .collect::<Vec<_>>();

        Ok(SpiceTest {
            name: self.name,
            start_time: self.start_time,
            spiced_instance: self.spiced_instance,
            use_progress_bars: self.use_progress_bars,
            state: Running {
                baseline_handles,
                spice_handles,
            },
        })
    }
}

impl SpiceTest<Running> {
    pub async fn wait(self) -> Result<SpiceTest<Completed>> {
        let baseline_results = try_join_all(self.state.baseline_handles)
            .await?
            .into_iter()
            .collect::<Result<Vec<OverheadResult>>>()?
            .into_iter()
            .fold(OverheadResult::default(), |mut a, b| {
                a.durations.extend(b.durations);
                a.error_count += b.error_count;
                a
            });

        let spice_results = try_join_all(self.state.spice_handles)
            .await?
            .into_iter()
            .collect::<Result<Vec<OverheadResult>>>()?
            .into_iter()
            .fold(OverheadResult::default(), |mut a, b| {
                a.durations.extend(b.durations);
                a.error_count += b.error_count;
                a
            });

        Ok(SpiceTest {
            name: self.name,
            start_time: self.start_time,
            spiced_instance: self.spiced_instance,
            use_progress_bars: self.use_progress_bars,
            state: Completed {
                baseline_results,
                spice_results,
                end_time: SystemTime::now(),
            },
        })
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
        let baseline =
            QueryMetric::new_from_durations("baseline", &self.state.baseline_results.durations)?;
        let spice = QueryMetric::new_from_durations("spice", &self.state.spice_results.durations)?;
        Ok(vec![baseline, spice])
    }
}

/// A worker that sends requests to the component, either the baseline or the Spice component.
struct OverHeadWorker {
    id: usize,
    duration: Duration,
    payloads: Vec<Arc<str>>,
    component: HttpComponent,
    client: Client,
}

impl OverHeadWorker {
    pub fn new(
        id: usize,
        duration: Duration,
        payloads: Vec<Arc<str>>,
        component: HttpComponent,
        client: Client,
    ) -> Self {
        Self {
            id,
            duration,
            payloads,
            component,
            client,
        }
    }

    pub fn start(self) -> OverheadJobHandle {
        tokio::spawn(async move {
            let mut durations: Vec<Duration> = vec![];
            let mut error_count = 0;
            let start = Instant::now();

            while start.elapsed() < self.duration {
                let Some(p) = get_random_element(&self.payloads) else {
                    eprintln!("Worker {} - No payload found. Exiting...", self.id);
                    return Ok(OverheadResult::default());
                };
                match self
                    .component
                    .send_request(&self.client, &Arc::clone(p))
                    .await
                {
                    Ok(request_duration) => {
                        durations.push(request_duration);
                    }
                    Err(e) => {
                        eprintln!("Worker {} - Request failed: {}", self.id, e);
                        error_count += 1;
                        continue;
                    }
                }
            }

            Ok(OverheadResult {
                durations,
                error_count,
            })
        })
    }
}
