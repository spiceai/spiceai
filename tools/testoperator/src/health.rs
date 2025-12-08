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

use crate::metrics;

use test_framework::{
    anyhow::{self, Context},
    constants::{HEALTH_ENDPOINT, HTTP_BASE_URL, READY_ENDPOINT},
    opentelemetry::KeyValue,
    tokio_util::sync::CancellationToken,
};

const ENDPOINTS: [&str; 2] = [HEALTH_ENDPOINT, READY_ENDPOINT];
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
const LATENCY_THRESHOLD: Duration = Duration::from_millis(50);

#[derive(Debug, Default, Clone)]
pub(crate) struct EndpointStats {
    pub(crate) failure_count: u64,
    pub(crate) max_latency: Duration,
    pub(crate) last_error: Option<String>,
}

impl EndpointStats {
    fn record_sample(&mut self, latency: Duration, failure: Option<String>) {
        if latency > self.max_latency {
            self.max_latency = latency;
        }

        if let Some(reason) = failure {
            self.failure_count = self.failure_count.saturating_add(1);
            self.last_error = Some(reason);
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct HealthCheckReport {
    endpoints: BTreeMap<&'static str, EndpointStats>,
}

impl HealthCheckReport {
    pub(crate) fn failure_message(&self) -> Option<String> {
        let mut parts = Vec::new();

        for (endpoint, stats) in &self.endpoints {
            if stats.failure_count == 0 {
                continue;
            }

            let max_latency_ms = stats.max_latency.as_secs_f64() * 1000.0;
            let reason = stats
                .last_error
                .as_deref()
                .unwrap_or("latency threshold exceeded");
            parts.push(format!(
                "{endpoint} failed {count} time(s); max latency {max_latency_ms:.2} ms; last error: {reason}",
                count = stats.failure_count
            ));
        }

        if parts.is_empty() {
            None
        } else {
            Some(format!(
                "Health checks detected issues: {}",
                parts.join(" | ")
            ))
        }
    }
}

pub(crate) struct HealthMonitor {
    cancel_token: CancellationToken,
    task: Option<tokio::task::JoinHandle<HealthCheckReport>>,
}

impl HealthMonitor {
    pub(crate) fn spawn() -> anyhow::Result<Self> {
        let cancel_token = CancellationToken::new();
        let task_token = cancel_token.clone();

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .context("Failed to create health monitor HTTP client")?;

        let task = tokio::spawn(async move {
            let mut stats: BTreeMap<&'static str, EndpointStats> = ENDPOINTS
                .into_iter()
                .map(|ep| (ep, EndpointStats::default()))
                .collect();

            loop {
                for endpoint in ENDPOINTS {
                    if task_token.is_cancelled() {
                        return HealthCheckReport { endpoints: stats };
                    }

                    let url = format!("{HTTP_BASE_URL}{endpoint}");
                    let start = Instant::now();
                    let response = client.get(&url).send().await;
                    let latency = start.elapsed();
                    let latency_ms = latency.as_secs_f64() * 1_000.0;

                    let failure_reason = match response {
                        Ok(response) => {
                            if !response.status().is_success() {
                                Some(format!("status {}", response.status()))
                            } else if latency > LATENCY_THRESHOLD {
                                Some(format!(
                                    "latency {}ms exceeded {}ms budget",
                                    latency.as_secs_f64() * 1_000.0,
                                    LATENCY_THRESHOLD.as_secs_f64() * 1_000.0
                                ))
                            } else {
                                None
                            }
                        }
                        Err(error) => Some(error.to_string()),
                    };

                    metrics::HEALTH_LATENCY.record(
                        latency_ms,
                        &[
                            KeyValue::new("endpoint", endpoint),
                            KeyValue::new(
                                "status",
                                if failure_reason.is_some() {
                                    "failure"
                                } else {
                                    "success"
                                },
                            ),
                        ],
                    );

                    if let Some(entry) = stats.get_mut(endpoint) {
                        entry.record_sample(latency, failure_reason);
                    }
                }

                tokio::select! {
                    () = task_token.cancelled() => {
                        return HealthCheckReport { endpoints: stats };
                    }
                    () = tokio::time::sleep(SAMPLE_INTERVAL) => {}
                }
            }
        });

        Ok(Self {
            cancel_token,
            task: Some(task),
        })
    }

    pub(crate) async fn stop(mut self) -> anyhow::Result<HealthCheckReport> {
        self.cancel_token.cancel();
        let Some(task) = self.task.take() else {
            return Ok(HealthCheckReport::default());
        };

        match task.await {
            Ok(report) => Ok(report),
            Err(err) => {
                Err(anyhow::anyhow!(err)
                    .context("Health monitor task did not complete successfully"))
            }
        }
    }
}

impl Drop for HealthMonitor {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
