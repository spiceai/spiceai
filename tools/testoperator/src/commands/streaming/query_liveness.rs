/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Query liveness monitoring for streaming benchmarks.
//!
//! Periodically executes count queries against each dataset table and tracks
//! latency and success rate during ingestion.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::TryStreamExt;
use test_framework::anyhow::Result;
use test_framework::spiced::SpicedInstance;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::datasets::DatasetType;
use super::traits::StreamingDataset;

/// Statistics for query liveness checks on a single dataset.
#[derive(Debug, Clone, Default)]
pub struct QueryLivenessStats {
    /// Total number of queries executed.
    pub total_queries: u64,
    /// Number of successful queries.
    pub successful_queries: u64,
    /// Number of failed queries.
    pub failed_queries: u64,
    /// Total latency across all successful queries.
    pub total_latency: Duration,
    /// Maximum latency observed.
    pub max_latency: Duration,
    /// Minimum latency observed (None if no successful queries).
    pub min_latency: Option<Duration>,
    /// All individual latencies for percentile calculation.
    latencies: Vec<Duration>,
}

impl QueryLivenessStats {
    /// Calculate average latency for successful queries.
    #[must_use]
    #[expect(clippy::cast_precision_loss)]
    pub fn avg_latency(&self) -> Duration {
        if self.successful_queries == 0 {
            Duration::ZERO
        } else {
            // Use div_duration_f64 to avoid potential truncation issues
            Duration::from_secs_f64(
                self.total_latency.as_secs_f64() / self.successful_queries as f64,
            )
        }
    }

    /// Calculate success rate as a percentage (0.0 - 100.0).
    #[must_use]
    #[expect(clippy::cast_precision_loss)]
    pub fn success_rate(&self) -> f64 {
        if self.total_queries == 0 {
            100.0
        } else {
            (self.successful_queries as f64 / self.total_queries as f64) * 100.0
        }
    }

    /// Calculate a percentile latency (0-100).
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    pub fn percentile(&self, p: f64) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }

        let mut sorted = self.latencies.clone();
        sorted.sort();

        let index = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[index.min(sorted.len() - 1)]
    }

    /// Get p90 latency.
    #[must_use]
    pub fn p90(&self) -> Duration {
        self.percentile(90.0)
    }

    /// Get p95 latency.
    #[must_use]
    pub fn p95(&self) -> Duration {
        self.percentile(95.0)
    }

    /// Get p99 latency.
    #[must_use]
    pub fn p99(&self) -> Duration {
        self.percentile(99.0)
    }

    fn record_success(&mut self, latency: Duration) {
        self.total_queries += 1;
        self.successful_queries += 1;
        self.total_latency += latency;
        self.latencies.push(latency);
        if latency > self.max_latency {
            self.max_latency = latency;
        }
        self.min_latency = Some(match self.min_latency {
            Some(min) if latency < min => latency,
            Some(min) => min,
            None => latency,
        });
    }

    fn record_failure(&mut self) {
        self.total_queries += 1;
        self.failed_queries += 1;
    }
}

/// Report from query liveness monitoring.
#[derive(Debug)]
pub struct QueryLivenessReport {
    /// Statistics per dataset.
    pub stats: HashMap<DatasetType, QueryLivenessStats>,
    /// Total monitoring duration.
    pub duration: Duration,
}

impl QueryLivenessReport {
    /// Get aggregate stats across all datasets.
    pub fn aggregate_stats(&self) -> QueryLivenessStats {
        let mut aggregate = QueryLivenessStats::default();
        for stats in self.stats.values() {
            aggregate.total_queries += stats.total_queries;
            aggregate.successful_queries += stats.successful_queries;
            aggregate.failed_queries += stats.failed_queries;
            aggregate.total_latency += stats.total_latency;
            aggregate.latencies.extend(stats.latencies.iter().copied());
            if stats.max_latency > aggregate.max_latency {
                aggregate.max_latency = stats.max_latency;
            }
            aggregate.min_latency = match (aggregate.min_latency, stats.min_latency) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            };
        }
        aggregate
    }

    /// Print a summary of the query liveness report.
    pub fn print_summary(&self) {
        println!("\n{}", "=".repeat(100));
        println!("Query Liveness Report");
        println!("{}", "=".repeat(100));
        println!("Monitoring Duration: {:?}", self.duration);
        println!("{}", "-".repeat(100));
        println!(
            "{:<12} | {:>7} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8}",
            "Dataset", "Queries", "Success%", "Avg(ms)", "p90(ms)", "p95(ms)", "p99(ms)", "Max(ms)"
        );
        println!("{}", "-".repeat(100));

        for (dataset, stats) in &self.stats {
            println!(
                "{:<12} | {:>7} | {:>7.1}% | {:>8.1} | {:>8.1} | {:>8.1} | {:>8.1} | {:>8.1}",
                dataset.table_name(),
                stats.total_queries,
                stats.success_rate(),
                stats.avg_latency().as_secs_f64() * 1000.0,
                stats.p90().as_secs_f64() * 1000.0,
                stats.p95().as_secs_f64() * 1000.0,
                stats.p99().as_secs_f64() * 1000.0,
                stats.max_latency.as_secs_f64() * 1000.0
            );
        }

        let aggregate = self.aggregate_stats();
        println!("{}", "-".repeat(100));
        println!(
            "{:<12} | {:>7} | {:>7.1}% | {:>8.1} | {:>8.1} | {:>8.1} | {:>8.1} | {:>8.1}",
            "TOTAL",
            aggregate.total_queries,
            aggregate.success_rate(),
            aggregate.avg_latency().as_secs_f64() * 1000.0,
            aggregate.p90().as_secs_f64() * 1000.0,
            aggregate.p95().as_secs_f64() * 1000.0,
            aggregate.p99().as_secs_f64() * 1000.0,
            aggregate.max_latency.as_secs_f64() * 1000.0
        );
        println!("{}", "=".repeat(100));
    }
}

/// Monitor that periodically executes liveness queries against datasets.
pub struct QueryLivenessMonitor {
    cancel_token: CancellationToken,
    stats: Arc<Mutex<HashMap<DatasetType, QueryLivenessStats>>>,
    task: Option<tokio::task::JoinHandle<()>>,
    start_time: Instant,
}

impl QueryLivenessMonitor {
    /// Spawn a new query liveness monitor.
    ///
    /// The monitor will periodically execute the liveness query for each dataset
    /// and track latency and success rate.
    pub async fn spawn(
        spiced: &SpicedInstance,
        datasets: &[Box<dyn StreamingDataset>],
        poll_interval: Duration,
    ) -> Result<Self> {
        let cancel_token = CancellationToken::new();
        let stats: Arc<Mutex<HashMap<DatasetType, QueryLivenessStats>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Initialize stats for each dataset
        {
            let mut s = stats.lock().await;
            for dataset in datasets {
                s.insert(dataset.dataset_type(), QueryLivenessStats::default());
            }
        }

        // Collect queries to execute
        let queries: Vec<(DatasetType, String)> = datasets
            .iter()
            .map(|d| (d.dataset_type(), d.liveness_query()))
            .collect();

        let stats_clone = Arc::clone(&stats);
        let cancel_clone = cancel_token.clone();

        // Create the client before spawning the task
        let client = spiced.spice_client(None, false).await?;

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = cancel_clone.cancelled() => {
                        break;
                    }
                    () = tokio::time::sleep(poll_interval) => {
                        // Execute all liveness queries
                        for (dataset_type, query) in &queries {
                            let start = Instant::now();
                            let result = client.sql(query).await;

                            let mut stats = stats_clone.lock().await;
                            if let Some(dataset_stats) = stats.get_mut(dataset_type) {
                                match result {
                                    Ok(stream) => {
                                        // Try to consume the stream
                                        match stream.try_collect::<Vec<_>>().await {
                                            Ok(_) => {
                                                dataset_stats.record_success(start.elapsed());
                                            }
                                            Err(_) => {
                                                dataset_stats.record_failure();
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        dataset_stats.record_failure();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            cancel_token,
            stats,
            task: Some(task),
            start_time: Instant::now(),
        })
    }

    /// Stop the monitor and return the final report.
    pub async fn stop(mut self) -> Result<QueryLivenessReport> {
        self.cancel_token.cancel();

        if let Some(task) = self.task.take() {
            let _ = task.await;
        }

        let stats = self.stats.lock().await.clone();
        let duration = self.start_time.elapsed();

        Ok(QueryLivenessReport { stats, duration })
    }
}
