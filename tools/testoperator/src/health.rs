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
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;

use crate::metrics;

use test_framework::{
    anyhow::{self, Context},
    constants::{HEALTH_ENDPOINT, HTTP_BASE_URL, READY_ENDPOINT},
    opentelemetry::KeyValue,
    tokio_util::sync::CancellationToken,
};

const ENDPOINTS: [&str; 2] = [HEALTH_ENDPOINT, READY_ENDPOINT];
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

// Use a large latency threshold for health endpoints as latency can spike when the CPU is fully utilized during
// intensive benchmark runs. This reduces noise from false positives. See <https://github.com/spiceai/spiceai/issues/7766>
//
// A sample past this budget counts as a health-check failure and is logged as a WARNING.
const LATENCY_THRESHOLD_MS: u64 = 125;
const LATENCY_THRESHOLD: Duration = Duration::from_millis(LATENCY_THRESHOLD_MS);

/// 4x the latency budget — logged as an ERROR. Kubernetes fails a probe that
/// overruns its `timeoutSeconds` (1s by default) and restarts the container after
/// enough consecutive failures, so this fires while there is still headroom.
const ERROR_LATENCY: Duration = Duration::from_millis(LATENCY_THRESHOLD_MS * 4);

/// Well above [`ERROR_LATENCY`]: a tighter timeout truncates every slow sample to
/// the same value, hiding the latency tail.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Why a probe sample counted as a failure. Kept distinct because they point at
/// different faults: a timeout means the HTTP server accepted the connection but
/// could not answer, a refusal means it is not accepting connections at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureKind {
    Timeout,
    Refused,
    Transport,
    Status,
    Latency,
}

/// One completed probe of a single endpoint.
struct ProbeSample {
    latency: Duration,
    /// `None` when the endpoint answered 2xx within [`LATENCY_THRESHOLD`].
    failure: Option<(FailureKind, String)>,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct EndpointStats {
    /// Latency of every sample, for percentiles. Unsorted; callers sort a copy.
    latencies_ms: Vec<f64>,
    pub(crate) failure_count: u64,
    /// Samples slower than [`LATENCY_THRESHOLD`] but within [`ERROR_LATENCY`].
    warn_count: u64,
    /// Samples slower than [`ERROR_LATENCY`].
    error_count: u64,
    /// Samples that hit [`PROBE_TIMEOUT`] — the analog of a kubelet
    /// `context deadline exceeded` probe event.
    timeout_count: u64,
    /// Samples that could not connect — the analog of `connection refused`.
    refused_count: u64,
    /// Samples that answered with a non-2xx status.
    status_count: u64,
    pub(crate) max_latency: Duration,
    pub(crate) last_error: Option<String>,
}

impl EndpointStats {
    fn record_sample(&mut self, sample: &ProbeSample) {
        let latency = sample.latency;
        self.latencies_ms.push(latency.as_secs_f64() * 1_000.0);

        if latency > self.max_latency {
            self.max_latency = latency;
        }

        if latency > ERROR_LATENCY {
            self.error_count = self.error_count.saturating_add(1);
        } else if latency > LATENCY_THRESHOLD {
            self.warn_count = self.warn_count.saturating_add(1);
        }

        if let Some((kind, reason)) = &sample.failure {
            self.failure_count = self.failure_count.saturating_add(1);
            self.last_error = Some(reason.clone());

            match kind {
                FailureKind::Timeout => {
                    self.timeout_count = self.timeout_count.saturating_add(1);
                }
                FailureKind::Refused => {
                    self.refused_count = self.refused_count.saturating_add(1);
                }
                FailureKind::Status => {
                    self.status_count = self.status_count.saturating_add(1);
                }
                FailureKind::Transport | FailureKind::Latency => {}
            }
        }
    }

    /// Every sample over [`LATENCY_THRESHOLD`], including the error-level ones —
    /// [`Self::warn_count`] alone covers only the band up to [`ERROR_LATENCY`].
    fn over_budget_count(&self) -> u64 {
        self.warn_count.saturating_add(self.error_count)
    }
}

#[derive(Debug, Default)]
pub(crate) struct HealthCheckReport {
    pub endpoints: BTreeMap<&'static str, EndpointStats>,
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

    /// Prints the probe latency distribution, one row per endpoint.
    ///
    /// `phase` labels the window the report covers (e.g. "under load").
    ///
    /// These are the endpoints a Kubernetes liveness/readiness probe hits, served
    /// by a Tokio runtime kept separate from query execution so they stay
    /// responsive under load. When that isolation breaks the probes time out and
    /// Kubernetes restarts the container, so the tail (p99.9/max) and the failure
    /// counts matter more than the median.
    pub(crate) fn print_latency_summary(&self, phase: &str) {
        if self.endpoints.is_empty() {
            return;
        }

        let warn_ms = LATENCY_THRESHOLD.as_millis();
        let error_ms = ERROR_LATENCY.as_millis();

        println!("\n=== Liveness / Readiness Probes ({phase}) ===");
        println!(
            "  {:<12} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8} {:>8} {:>9} {:>8}",
            "endpoint",
            "samples",
            "p50",
            "p90",
            "p99",
            "p99.9",
            "max",
            format!(">{warn_ms}ms"),
            format!(">{error_ms}ms"),
            "timeouts",
            "refused"
        );

        for (endpoint, stats) in &self.endpoints {
            // Latency cells carry their unit, so format each before padding it: a
            // `{:>10.1}ms` width applies to the number alone and skews the columns
            // once a value reaches four digits. An endpoint with no samples shows
            // dashes rather than a `0.0ms` that would read as a measurement.
            let cells: Vec<String> = if stats.latencies_ms.is_empty() {
                vec!["-".to_string(); 5]
            } else {
                let sorted = crate::stats::sorted_ms(&stats.latencies_ms);
                let mut cells: Vec<String> = [0.50, 0.90, 0.99, 0.999]
                    .into_iter()
                    .map(|q| format!("{:.1}ms", crate::stats::percentile(&sorted, q)))
                    .collect();
                // Read max from the tracked field rather than deriving it a second
                // way, so it can't disagree with the max in the lines below.
                cells.push(format!(
                    "{:.1}ms",
                    stats.max_latency.as_secs_f64() * 1_000.0
                ));
                cells
            };

            println!(
                "  {endpoint:<12} {:>8} {:>10} {:>10} {:>10} {:>10} {:>10} {:>8} {:>8} {:>9} {:>8}",
                stats.latencies_ms.len(),
                cells[0],
                cells[1],
                cells[2],
                cells[3],
                cells[4],
                stats.over_budget_count(),
                stats.error_count,
                stats.timeout_count,
                stats.refused_count,
            );
        }

        // Name each failure mode in the same terms a Kubernetes probe event reports
        // it, so a run that reproduces the symptom says so outright.
        for (endpoint, stats) in &self.endpoints {
            let max_ms = stats.max_latency.as_secs_f64() * 1_000.0;

            if stats.timeout_count > 0 {
                println!(
                    "  ERROR: {endpoint} never responded on {} sample(s) — the same failure a Kubernetes probe reports as 'context deadline exceeded'",
                    stats.timeout_count
                );
            }
            if stats.refused_count > 0 {
                println!(
                    "  ERROR: {endpoint} refused the connection on {} sample(s) — the same failure a Kubernetes probe reports as 'connection refused'",
                    stats.refused_count
                );
            }
            if stats.status_count > 0 {
                println!(
                    "  ERROR: {endpoint} answered with a non-2xx status on {} sample(s) — a Kubernetes probe counts these as failed",
                    stats.status_count
                );
            }
            if stats.error_count > 0 {
                println!(
                    "  ERROR: {endpoint} exceeded {error_ms}ms on {} sample(s) (max {max_ms:.0}ms) — the HTTP server stalls under load",
                    stats.error_count
                );
            } else if stats.warn_count > 0 {
                println!(
                    "  WARNING: {endpoint} exceeded {warn_ms}ms on {} sample(s) (max {max_ms:.0}ms)",
                    stats.warn_count
                );
            }
        }
        println!();
    }
}

/// Probes one endpoint once, recording the latency metric and logging any
/// threshold breach. Logs here, not under the stats lock, to keep I/O out of the
/// critical section.
async fn probe(client: &reqwest::Client, endpoint: &'static str) -> ProbeSample {
    let url = format!("{HTTP_BASE_URL}{endpoint}");
    let start = Instant::now();
    let response = client.get(&url).send().await;
    let latency = start.elapsed();
    let latency_ms = latency.as_secs_f64() * 1_000.0;

    let failure = match response {
        Ok(response) => {
            if response.status().is_success() {
                if latency > LATENCY_THRESHOLD {
                    Some((
                        FailureKind::Latency,
                        format!(
                            "latency {latency_ms:.1}ms exceeded {}ms budget",
                            LATENCY_THRESHOLD.as_millis()
                        ),
                    ))
                } else {
                    None
                }
            } else {
                Some((FailureKind::Status, format!("status {}", response.status())))
            }
        }
        Err(error) => {
            let kind = if error.is_timeout() {
                FailureKind::Timeout
            } else if error.is_connect() {
                FailureKind::Refused
            } else {
                FailureKind::Transport
            };
            Some((kind, error.to_string()))
        }
    };

    metrics::HEALTH_LATENCY.record(
        latency_ms,
        &[
            KeyValue::new("endpoint", endpoint),
            KeyValue::new(
                "status",
                if failure.is_some() {
                    "failure"
                } else {
                    "success"
                },
            ),
        ],
    );

    // Log the breach as it is observed, so a stall is visible in the run log at the
    // moment it happens rather than only in the end-of-run summary.
    if let Some(line) = breach_log_line(endpoint, latency, failure.as_ref()) {
        eprintln!("{line}");
    }

    ProbeSample { latency, failure }
}

/// One log line per breaching sample: `ERROR` for a failed request or one past
/// [`ERROR_LATENCY`], `WARNING` for a response that overran [`LATENCY_THRESHOLD`].
/// `None` when the sample is within budget.
///
/// Compares `Duration`s, as [`EndpointStats::record_sample`] does, so a logged
/// severity always agrees with the counter it increments.
fn breach_log_line(
    endpoint: &str,
    latency: Duration,
    failure: Option<&(FailureKind, String)>,
) -> Option<String> {
    let latency_ms = latency.as_secs_f64() * 1_000.0;
    let error_ms = ERROR_LATENCY.as_millis();
    let warn_ms = LATENCY_THRESHOLD.as_millis();

    match failure {
        // An unanswered request is a failed probe however fast it failed — a
        // refused connection returns in microseconds.
        Some((FailureKind::Timeout | FailureKind::Refused | FailureKind::Transport, reason)) => {
            Some(format!(
                "ERROR: probe {endpoint} failed after {latency_ms:.1}ms: {reason}"
            ))
        }
        Some((FailureKind::Status, reason)) => Some(format!(
            "ERROR: probe {endpoint} returned {reason} after {latency_ms:.1}ms"
        )),
        _ => {
            if latency > ERROR_LATENCY {
                Some(format!(
                    "ERROR: probe {endpoint} took {latency_ms:.1}ms (> {error_ms}ms budget)"
                ))
            } else if latency > LATENCY_THRESHOLD {
                Some(format!(
                    "WARNING: probe {endpoint} took {latency_ms:.1}ms (> {warn_ms}ms budget)"
                ))
            } else {
                None
            }
        }
    }
}

/// Samples one endpoint on a fixed cadence until cancelled.
///
/// One task per endpoint, rather than one task probing both: a hung endpoint holds
/// its own sampler for up to [`PROBE_TIMEOUT`], and sharing a task would stall the
/// other endpoint's cadence with it — biasing its percentiles and undercounting its
/// breaches for a fault that isn't its own.
async fn sample_endpoint(
    client: reqwest::Client,
    endpoint: &'static str,
    stats: Arc<Mutex<BTreeMap<&'static str, EndpointStats>>>,
    token: CancellationToken,
) {
    // A fixed interval keeps the cadence independent of probe latency; `Delay`
    // avoids the catch-up burst a slow probe would otherwise trigger.
    let mut ticker = tokio::time::interval(SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            () = token.cancelled() => return,
            _ = ticker.tick() => {}
        }

        // Abandon an in-flight probe on cancellation rather than waiting out the
        // timeout, so shutdown is prompt even against a hung endpoint.
        let sample = tokio::select! {
            () = token.cancelled() => return,
            sample = probe(&client, endpoint) => sample,
        };

        if let Some(entry) = stats.lock().get_mut(endpoint) {
            entry.record_sample(&sample);
        }
    }
}

pub(crate) struct HealthMonitor {
    cancel_token: CancellationToken,
    /// Shared with the sampling tasks so a report can be taken mid-run, without
    /// stopping the monitor.
    stats: Arc<Mutex<BTreeMap<&'static str, EndpointStats>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl HealthMonitor {
    pub(crate) fn spawn() -> anyhow::Result<Self> {
        let cancel_token = CancellationToken::new();

        let client = reqwest::Client::builder()
            .timeout(PROBE_TIMEOUT)
            .build()
            .context("Failed to create health monitor HTTP client")?;

        let stats: Arc<Mutex<BTreeMap<&'static str, EndpointStats>>> = Arc::new(Mutex::new(
            ENDPOINTS
                .into_iter()
                .map(|ep| (ep, EndpointStats::default()))
                .collect(),
        ));

        let tasks = ENDPOINTS
            .into_iter()
            .map(|endpoint| {
                tokio::spawn(sample_endpoint(
                    client.clone(),
                    endpoint,
                    Arc::clone(&stats),
                    cancel_token.clone(),
                ))
            })
            .collect();

        Ok(Self {
            cancel_token,
            stats,
            tasks,
        })
    }

    /// Report of everything sampled so far, leaving the monitor running, so a
    /// caller can scope a report to one phase of a run.
    pub(crate) fn snapshot(&self) -> HealthCheckReport {
        HealthCheckReport {
            endpoints: self.stats.lock().clone(),
        }
    }

    pub(crate) async fn stop(mut self) -> anyhow::Result<HealthCheckReport> {
        self.cancel_token.cancel();

        for task in std::mem::take(&mut self.tasks) {
            task.await
                .map_err(|err| anyhow::anyhow!(err))
                .context("Health monitor task did not complete successfully")?;
        }

        // Every sampler has joined, so the stats are uniquely owned — take them
        // rather than cloning a map that is about to be dropped.
        Ok(HealthCheckReport {
            endpoints: std::mem::take(&mut *self.stats.lock()),
        })
    }
}

impl Drop for HealthMonitor {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        for task in std::mem::take(&mut self.tasks) {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EndpointStats, FailureKind, HealthCheckReport, ProbeSample, breach_log_line};
    use std::time::Duration;

    fn healthy(latency: Duration) -> ProbeSample {
        ProbeSample {
            latency,
            failure: None,
        }
    }

    fn slow(latency: Duration) -> ProbeSample {
        ProbeSample {
            latency,
            failure: Some((FailureKind::Latency, "latency budget".to_string())),
        }
    }

    fn failed(latency: Duration, kind: FailureKind) -> ProbeSample {
        ProbeSample {
            latency,
            failure: Some((kind, "probe failed".to_string())),
        }
    }

    #[test]
    fn latency_buckets_split_at_the_budget_and_its_4x_error_threshold() {
        let mut stats = EndpointStats::default();

        // Fast, then either side of the 125ms budget and the 500ms error threshold.
        // Everything past the budget also counts as a failure, so those arrive as `slow`.
        stats.record_sample(&healthy(Duration::from_millis(1)));
        stats.record_sample(&healthy(Duration::from_millis(125)));
        stats.record_sample(&slow(Duration::from_millis(126)));
        stats.record_sample(&slow(Duration::from_millis(500)));
        stats.record_sample(&slow(Duration::from_millis(501)));

        // Exactly at a threshold is not a breach of it: 125ms is not > 125ms.
        assert_eq!(stats.warn_count, 2, "126ms and 500ms are warn-level");
        assert_eq!(stats.error_count, 1, "only 501ms is error-level");
        assert_eq!(stats.failure_count, 3, "every over-budget sample counts");
        // The reported over-budget total must include the error-level sample,
        // otherwise it under-counts against its own ">125ms" label.
        assert_eq!(stats.over_budget_count(), 3);
        assert_eq!(stats.max_latency, Duration::from_millis(501));
        assert_eq!(stats.latencies_ms.len(), 5);
    }

    #[test]
    fn failure_kinds_are_counted_separately() {
        let mut stats = EndpointStats::default();

        stats.record_sample(&failed(Duration::from_secs(3), FailureKind::Timeout));
        stats.record_sample(&failed(Duration::from_micros(200), FailureKind::Refused));
        stats.record_sample(&failed(Duration::from_millis(5), FailureKind::Status));
        stats.record_sample(&failed(Duration::from_millis(5), FailureKind::Transport));

        assert_eq!(stats.timeout_count, 1);
        assert_eq!(stats.refused_count, 1);
        assert_eq!(stats.status_count, 1);
        assert_eq!(stats.failure_count, 4, "every kind counts as a failure");
        // A refused connection fails in microseconds, so it is not a latency breach.
        assert_eq!(stats.error_count, 1, "only the 3s timeout is error-level");
        assert_eq!(stats.warn_count, 0, "5ms failures are not latency breaches");
    }

    #[test]
    fn every_breaching_sample_produces_a_log_line() {
        let timeout = (FailureKind::Timeout, "operation timed out".to_string());
        let refused = (FailureKind::Refused, "connection refused".to_string());

        let ms = Duration::from_millis;

        assert_eq!(
            breach_log_line("/health", ms(4), None),
            None,
            "within budget"
        );
        assert_eq!(
            breach_log_line("/health", ms(125), None),
            None,
            "exactly at the budget is not a breach of it"
        );

        let warning = breach_log_line("/health", ms(126), None).expect("over the 125ms budget");
        assert!(
            warning.starts_with("WARNING: probe /health took 126.0ms"),
            "{warning}"
        );

        let error = breach_log_line("/health", ms(1_402), None).expect("over the 500ms threshold");
        assert!(
            error.starts_with("ERROR: probe /health took 1402.0ms"),
            "{error}"
        );

        let timed_out = breach_log_line("/health", ms(3_001), Some(&timeout)).expect("failed");

        assert!(timed_out.contains("operation timed out"), "{timed_out}");

        // Fast enough to clear every latency threshold, but still a failed probe.
        let refused = breach_log_line("/v1/ready", Duration::from_micros(200), Some(&refused))
            .expect("failed");
        assert!(
            refused.starts_with("ERROR: probe /v1/ready failed"),
            "{refused}"
        );
    }

    /// An endpoint with no samples must render alongside populated ones without
    /// panicking, and every row must carry the same column count.
    #[test]
    fn latency_summary_renders_healthy_slow_and_empty_endpoints() {
        let mut populated = EndpointStats::default();
        for ms in [1, 2, 2, 3, 4] {
            populated.record_sample(&healthy(Duration::from_millis(ms)));
        }
        for ms in [130, 260, 600, 1_400] {
            populated.record_sample(&slow(Duration::from_millis(ms)));
        }

        let mut report = HealthCheckReport::default();
        report.endpoints.insert("/health", populated);
        report
            .endpoints
            .insert("/v1/ready", EndpointStats::default());

        report.print_latency_summary("test");
    }

    /// A probe that never returns is cut off by `PROBE_TIMEOUT` and must be counted
    /// as a timeout and logged, not silently dropped.
    #[test]
    fn a_probe_that_never_returns_is_recorded_as_a_timeout() {
        let mut stats = EndpointStats::default();
        let timeout = (FailureKind::Timeout, "operation timed out".to_string());

        // 20 consecutive timeouts: a minute of hung endpoint at a 3s timeout.
        for _ in 0..20 {
            stats.record_sample(&failed(super::PROBE_TIMEOUT, FailureKind::Timeout));
        }
        // Interleaved healthy traffic, at the 10Hz cadence the stall suppressed.
        for _ in 0..5_400 {
            stats.record_sample(&healthy(Duration::from_millis(1)));
        }

        assert_eq!(stats.timeout_count, 20);
        assert_eq!(stats.error_count, 20, "a 3s timeout is error-level");
        assert_eq!(stats.max_latency, super::PROBE_TIMEOUT);
        // Each hung probe is logged as it happens, which is what makes the outage
        // visible: a stall suppresses its own sample rate (one sample per 3s
        // timeout), so it stays a small fraction of samples and p99 reads healthy.
        // Read the timeout count and the per-sample ERROR lines, not the percentiles.
        let sorted = crate::stats::sorted_ms(&stats.latencies_ms);
        assert!(
            crate::stats::percentile(&sorted, 0.99) < 2.0,
            "p99 cannot see a stall that suppressed its own sample rate"
        );

        assert!(
            breach_log_line("/health", super::PROBE_TIMEOUT, Some(&timeout))
                .is_some_and(|line| line.starts_with("ERROR:")),
            "each hung probe is logged as it happens"
        );
    }

    #[test]
    fn healthy_samples_record_no_failures() {
        let mut stats = EndpointStats::default();
        for _ in 0..10 {
            stats.record_sample(&healthy(Duration::from_millis(2)));
        }

        assert_eq!(stats.failure_count, 0);
        assert_eq!(stats.warn_count, 0);
        assert_eq!(stats.error_count, 0);
        assert!(stats.last_error.is_none());
        assert_eq!(stats.latencies_ms.len(), 10);
    }
}
