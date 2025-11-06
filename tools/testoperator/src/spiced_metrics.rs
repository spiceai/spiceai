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

use std::collections::HashMap;
use std::time::Duration;
use test_framework::{
    anyhow::{self, Context},
    constants::METRICS_URL,
    tokio_util::sync::CancellationToken,
};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// Represents a single metric sample with its name, labels, and value
#[derive(Debug, Clone)]
pub struct MetricSample {
    pub name: String,
    #[allow(dead_code)]
    pub labels: HashMap<String, String>,
    pub value: f64,
    pub metric_type: MetricType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Summary,
    Untyped,
}

/// Aggregated metrics collected during a test run
#[derive(Debug, Default)]
pub struct SpicedMetrics {
    /// All samples collected, keyed by metric name
    pub samples: HashMap<String, Vec<MetricSample>>,
}

impl SpicedMetrics {
    /// Get the final (latest) value for a counter metric
    pub fn get_counter_value(&self, name: &str) -> Option<f64> {
        self.samples
            .get(name)?
            .iter()
            .filter(|s| s.metric_type == MetricType::Counter)
            .next_back()
            .map(|s| s.value)
    }

    /// Get the max value observed for a gauge metric
    pub fn get_gauge_max(&self, name: &str) -> Option<f64> {
        self.samples
            .get(name)?
            .iter()
            .filter(|s| s.metric_type == MetricType::Gauge)
            .map(|s| s.value)
            .filter(|v| !v.is_nan()) // Filter out NaN values to avoid incorrect comparisons
            .max_by(|a, b| {
                // partial_cmp returns Some for all non-NaN f64 values, and NaN values are filtered out above
                match a.partial_cmp(b) {
                    Some(ordering) => ordering,
                    None => unreachable!("partial_cmp should succeed for non-NaN f64 values"),
                }
            })
    }

    /// Get the average value for a gauge metric
    #[allow(dead_code, clippy::cast_precision_loss)]
    pub fn get_gauge_avg(&self, name: &str) -> Option<f64> {
        let samples: Vec<f64> = self
            .samples
            .get(name)?
            .iter()
            .filter(|s| s.metric_type == MetricType::Gauge)
            .map(|s| s.value)
            .filter(|v| !v.is_nan()) // Filter out NaN values to avoid corrupting the average
            .collect();

        if samples.is_empty() {
            return None;
        }

        Some(samples.iter().sum::<f64>() / samples.len() as f64)
    }

    /// Get all samples for a specific metric name
    #[allow(dead_code)]
    pub fn get_samples(&self, name: &str) -> Option<&Vec<MetricSample>> {
        self.samples.get(name)
    }

    /// Get all unique metric names
    #[allow(dead_code)]
    pub fn metric_names(&self) -> Vec<&String> {
        self.samples.keys().collect()
    }
}

/// Background scraper that periodically fetches metrics from spiced
pub struct MetricsScraper {
    cancel_token: CancellationToken,
    task: Option<tokio::task::JoinHandle<SpicedMetrics>>,
}

impl MetricsScraper {
    /// Start a background scraper task
    pub fn spawn() -> anyhow::Result<Self> {
        let cancel_token = CancellationToken::new();
        let task_token = cancel_token.clone();

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .context("Failed to create metrics scraper HTTP client")?;

        let task = tokio::spawn(async move {
            let mut all_samples: HashMap<String, Vec<MetricSample>> = HashMap::new();

            loop {
                tokio::select! {
                    () = task_token.cancelled() => {
                        return SpicedMetrics { samples: all_samples };
                    }
                    () = tokio::time::sleep(SAMPLE_INTERVAL) => {
                        match Self::scrape_metrics(&client).await {
                            Ok(samples) => {
                                for sample in samples {
                                    all_samples
                                        .entry(sample.name.clone())
                                        .or_default()
                                        .push(sample);
                                }
                            }
                            #[cfg(debug_assertions)]
                            Err(e) => {
                                // Log transient scraping errors to aid troubleshooting
                                // Using eprintln for debug output to stderr
                                eprintln!("Debug: Failed to scrape metrics: {e}");
                            }
                            #[cfg(not(debug_assertions))]
                            Err(_) => {
                                // Silently ignore in release builds
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            cancel_token,
            task: Some(task),
        })
    }

    /// Stop the scraper and return collected metrics
    pub async fn stop(mut self) -> anyhow::Result<SpicedMetrics> {
        self.cancel_token.cancel();
        let Some(task) = self.task.take() else {
            return Ok(SpicedMetrics::default());
        };

        match task.await {
            Ok(metrics) => Ok(metrics),
            Err(err) => {
                Err(anyhow::anyhow!(err)
                    .context("Metrics scraper task did not complete successfully"))
            }
        }
    }

    /// Scrape metrics from the spiced /metrics endpoint
    async fn scrape_metrics(client: &reqwest::Client) -> anyhow::Result<Vec<MetricSample>> {
        let response = client
            .get(METRICS_URL)
            .send()
            .await
            .context("Failed to fetch metrics endpoint")?;

        if !response.status().is_success() {
            anyhow::bail!("Metrics endpoint returned status: {}", response.status());
        }

        let text = response
            .text()
            .await
            .context("Failed to read metrics response")?;
        Ok(Self::parse_prometheus_text(&text))
    }

    /// Parse Prometheus text format into metric samples
    /// This is a simple parser that handles basic Prometheus format
    fn parse_prometheus_text(text: &str) -> Vec<MetricSample> {
        let mut samples = Vec::new();
        let mut current_type: Option<(String, MetricType)> = None;

        for line in text.lines() {
            let line = line.trim();

            // Skip empty lines and comments (except TYPE and HELP)
            if line.is_empty() {
                continue;
            }

            if line.starts_with("# TYPE ") {
                // Parse: # TYPE metric_name counter|gauge|histogram|summary
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 4 {
                    let metric_name = parts[2].to_string();
                    let metric_type = match parts[3] {
                        "counter" => MetricType::Counter,
                        "gauge" => MetricType::Gauge,
                        "histogram" => MetricType::Histogram,
                        "summary" => MetricType::Summary,
                        _ => MetricType::Untyped,
                    };
                    current_type = Some((metric_name, metric_type));
                }
                continue;
            }

            if line.starts_with('#') {
                // Skip other comment lines (HELP, etc.)
                continue;
            }

            // Parse metric line: metric_name{label1="value1",label2="value2"} value
            // or: metric_name value
            if let Some((name, labels_and_value)) = Self::split_metric_line(line) {
                let (labels, value) = Self::parse_labels_and_value(labels_and_value);

                // Determine metric type - check if current_type matches this metric
                let metric_type = current_type
                    .as_ref()
                    .and_then(|(type_name, mtype)| {
                        name.starts_with(type_name).then(|| mtype.clone())
                    })
                    .unwrap_or(MetricType::Untyped);

                samples.push(MetricSample {
                    name: name.to_string(),
                    labels,
                    value,
                    metric_type,
                });
            }
        }

        samples
    }

    /// Split a metric line into name and the rest (labels + value)
    fn split_metric_line(line: &str) -> Option<(&str, &str)> {
        if let Some(brace_pos) = line.find('{') {
            // Has labels: metric_name{...} value
            Some((&line[..brace_pos], &line[brace_pos..]))
        } else if let Some(space_pos) = line.find(' ') {
            // No labels: metric_name value
            Some((&line[..space_pos], &line[space_pos..]))
        } else {
            None
        }
    }

    /// Parse labels and value from the remainder of a metric line
    fn parse_labels_and_value(remainder: &str) -> (HashMap<String, String>, f64) {
        let remainder = remainder.trim();

        if let Some(close_brace) = remainder.find('}') {
            // Has labels: {label1="value1",label2="value2"} value
            let labels_str = &remainder[1..close_brace];
            let value_str = remainder[close_brace + 1..].trim();

            let labels = Self::parse_labels(labels_str);
            let value = match value_str.parse::<f64>() {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Warning: Failed to parse metric value '{value_str}': {e}");
                    0.0
                }
            };

            (labels, value)
        } else if let Ok(value) = remainder.parse::<f64>() {
            // No labels, just value
            (HashMap::new(), value)
        } else {
            // Failed to parse
            eprintln!("Warning: Failed to parse metric line: '{remainder}'");
            (HashMap::new(), 0.0)
        }
    }

    /// Parse label string: label1="value1",label2="value2"
    fn parse_labels(labels_str: &str) -> HashMap<String, String> {
        let mut labels = HashMap::new();

        for part in labels_str.split(',') {
            let part = part.trim();
            if let Some((key, value)) = part.split_once('=') {
                let key = key.trim().to_string();
                // Remove surrounding quotes from value
                let value = value.trim().trim_matches('"').to_string();
                labels.insert(key, value);
            }
        }

        labels
    }
}

impl Drop for MetricsScraper {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_metric() {
        let text = "
# TYPE http_requests_total counter
http_requests_total 12345
";
        let samples = MetricsScraper::parse_prometheus_text(text);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "http_requests_total");
        assert!((samples[0].value - 12345.0).abs() < f64::EPSILON);
        assert_eq!(samples[0].metric_type, MetricType::Counter);
    }

    #[test]
    fn test_parse_metric_with_labels() {
        let text = "
# TYPE http_requests_total counter
http_requests_total{method=\"GET\",status=\"200\"} 1234
";
        let samples = MetricsScraper::parse_prometheus_text(text);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "http_requests_total");
        assert!((samples[0].value - 1234.0).abs() < f64::EPSILON);
        assert_eq!(samples[0].labels.get("method"), Some(&"GET".to_string()));
        assert_eq!(samples[0].labels.get("status"), Some(&"200".to_string()));
    }
}
