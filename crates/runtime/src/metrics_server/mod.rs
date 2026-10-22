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

pub mod cluster;

use bytes::Bytes;
use cluster::ClusterMetricsCollector;
use governor::{
    RateLimiter,
    clock::{Clock, DefaultClock},
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
};
use http::{HeaderValue, Request, Response, StatusCode};
use http_body_util::Full;
use hyper::{
    body::Incoming,
    header::{CONTENT_TYPE, RETRY_AFTER},
};
use hyper_util::{rt::TokioIo, server::conn::auto::Builder as AutoBuilder};
use opentelemetry_prometheus::PrometheusExporter;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_sdk::error::OTelSdkError;
use prometheus::{
    Encoder, TextEncoder,
    proto::{Bucket, Histogram, LabelPair, Metric, MetricFamily, MetricType},
};
use runtime_tls::TlsConfig;
use snafu::prelude::*;
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::net::ToSocketAddrs;
use std::time::Duration;
use std::{fmt::Debug, sync::Arc};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

type DirectRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

const PERCENTILES: [f64; 4] = [50.0, 90.0, 95.0, 99.0];

/// Query parameter for requesting cluster-wide metrics.
const SCOPE_PARAM: &str = "scope";
const SCOPE_CLUSTER: &str = "cluster";

fn retry_after_seconds(wait_time: Duration) -> u64 {
    wait_time
        .as_secs()
        .saturating_add(u64::from(wait_time.subsec_nanos() > 0))
        .max(1)
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to bind metrics server to address: {source}"))]
    UnableToBindServerToPort { source: std::io::Error },

    #[snafu(display("Failed to start the metrics HTTP server: {source}"))]
    UnableToStartHttpServer { source: std::io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Builds the Prometheus reader that `/metrics` is served from.
///
/// # Errors
///
/// Returns an error when the exporter cannot register its collector with
/// `registry`.
pub fn prometheus_reader(
    registry: prometheus::Registry,
) -> Result<PrometheusExporter, OTelSdkError> {
    opentelemetry_prometheus::exporter()
        .with_registry(registry)
        .without_scope_info()
        .without_units()
        .without_counter_suffixes()
        .without_target_info()
        .build()
}

pub(crate) async fn start<A>(
    bind_address: Option<A>,
    prometheus_registry: Option<prometheus::Registry>,
    tls_config: Option<Arc<TlsConfig>>,
    cluster_collector: Option<Arc<ClusterMetricsCollector>>,
    rate_limiter: Option<Arc<DirectRateLimiter>>,
) -> Result<()>
where
    A: ToSocketAddrs + Debug + Clone + Copy,
{
    let (Some(bind_address), Some(prometheus_registry)) = (bind_address, prometheus_registry)
    else {
        return Ok(());
    };

    let listener = std::net::TcpListener::bind(bind_address)
        .and_then(|listener| {
            listener.set_nonblocking(true)?;
            Ok(listener)
        })
        .context(UnableToBindServerToPortSnafu)?;
    let listener = TcpListener::from_std(listener).context(UnableToBindServerToPortSnafu)?;
    tracing::info!("Spice Runtime Metrics listening on {:?}", bind_address);

    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                tracing::debug!(
                    "Error accepting connection to serve Prometheus metrics request: {e}"
                );
                continue;
            }
        };

        match tls_config {
            Some(ref config) => {
                let acceptor = TlsAcceptor::from(Arc::clone(&config.http_server_config));
                process_tls_tcp_stream(
                    stream,
                    acceptor.clone(),
                    prometheus_registry.clone(),
                    cluster_collector.clone(),
                    rate_limiter.clone(),
                );
            }
            None => {
                process_tcp_stream(
                    stream,
                    prometheus_registry.clone(),
                    cluster_collector.clone(),
                    rate_limiter.clone(),
                );
            }
        }
    }
}

fn process_tls_tcp_stream(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    prometheus_registry: prometheus::Registry,
    cluster_collector: Option<Arc<ClusterMetricsCollector>>,
    rate_limiter: Option<Arc<DirectRateLimiter>>,
) {
    tokio::spawn(async move {
        let stream = acceptor.accept(stream).await;
        match stream {
            Ok(stream) => {
                serve_connection(stream, prometheus_registry, cluster_collector, rate_limiter)
                    .await;
            }
            Err(e) => {
                tracing::debug!("Error accepting TLS connection: {e}");
            }
        }
    });
}

fn process_tcp_stream(
    stream: TcpStream,
    prometheus_registry: prometheus::Registry,
    cluster_collector: Option<Arc<ClusterMetricsCollector>>,
    rate_limiter: Option<Arc<DirectRateLimiter>>,
) {
    tokio::spawn(serve_connection(
        stream,
        prometheus_registry,
        cluster_collector,
        rate_limiter,
    ));
}

async fn serve_connection<S>(
    stream: S,
    prometheus_registry: prometheus::Registry,
    cluster_collector: Option<Arc<ClusterMetricsCollector>>,
    rate_limiter: Option<Arc<DirectRateLimiter>>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service = hyper::service::service_fn(move |req: Request<Incoming>| {
        let prometheus_registry = prometheus_registry.clone();
        let cluster_collector = cluster_collector.clone();
        let rate_limiter = rate_limiter.clone();
        async move {
            Ok::<_, hyper::Error>(
                handle_http_request(
                    &prometheus_registry,
                    cluster_collector.as_deref(),
                    rate_limiter.as_deref(),
                    &req,
                )
                .await,
            )
        }
    });

    if let Err(err) = AutoBuilder::new(hyper_util::rt::TokioExecutor::new())
        .serve_connection(TokioIo::new(stream), service)
        .await
    {
        tracing::debug!(error = ?err, "Error serving Prometheus metrics connection.");
    }
}

/// Parses query string into key-value pairs.
fn parse_query_string(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            if key.is_empty() {
                return None;
            }
            let value = parts.next().unwrap_or("");
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

async fn handle_http_request<B>(
    prometheus_registry: &prometheus::Registry,
    cluster_collector: Option<&ClusterMetricsCollector>,
    rate_limiter: Option<&DirectRateLimiter>,
    req: &Request<B>,
) -> Response<Full<Bytes>> {
    let path = req.uri().path();
    if path == "/health" {
        let mut response = Response::new(Full::from("OK"));
        response
            .headers_mut()
            .append(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        return response;
    }

    if path != "/metrics" {
        let mut resp = Response::new(Full::from("Not Found\n"));
        *resp.status_mut() = StatusCode::NOT_FOUND;
        resp.headers_mut()
            .append(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        return resp;
    }

    if let Some(Err(wait_time)) = rate_limiter.map(DirectRateLimiter::check) {
        let retry_after =
            retry_after_seconds(wait_time.wait_time_from(DefaultClock::default().now()));
        let mut resp = Response::new(Full::from(format!(
            "Too many requests. Retry after {retry_after} seconds.\n"
        )));
        *resp.status_mut() = StatusCode::TOO_MANY_REQUESTS;
        let headers = resp.headers_mut();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        if let Ok(val) = HeaderValue::from_str(&retry_after.to_string()) {
            headers.insert(RETRY_AFTER, val);
        }
        return resp;
    }

    // Check for ?scope=cluster query parameter
    let query_params = req
        .uri()
        .query()
        .map(parse_query_string)
        .unwrap_or_default();

    let is_cluster_scope = query_params
        .get(SCOPE_PARAM)
        .is_some_and(|v| v == SCOPE_CLUSTER);

    let body: Bytes = if is_cluster_scope {
        // Cluster-wide metrics requested
        match cluster_collector {
            Some(collector) => match collector.collect().await {
                Ok(otlp_metrics) => {
                    let prometheus_text = otlp_to_prometheus_text(&otlp_metrics);
                    prometheus_text.into()
                }
                Err(e) => {
                    tracing::warn!("Failed to collect cluster metrics: {e}");
                    format!("# Error collecting cluster metrics: {e}\n").into()
                }
            },
            None => "# Cluster metrics not available (not running in cluster mode)\n".into(),
        }
    } else {
        encode_registry(prometheus_registry)
    };

    let mut response = Response::new(Full::from(body));
    response
        .headers_mut()
        .append(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    response
}

/// Converts OTLP metrics to Prometheus text exposition format.
///
/// This function handles the conversion of OTLP protobuf metrics (collected from
/// cluster nodes) into the Prometheus text format for scraping.
#[expect(clippy::cast_precision_loss)]
fn otlp_to_prometheus_text(request: &ExportMetricsServiceRequest) -> String {
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;

    let mut output = String::new();

    for resource_metrics in &request.resource_metrics {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                let name = sanitize_metric_name(&metric.name);
                let description = &metric.description;

                match &metric.data {
                    Some(Data::Gauge(gauge)) => {
                        write_help_type(&mut output, &name, description, "gauge");
                        for dp in &gauge.data_points {
                            let labels = format_attributes(&dp.attributes);
                            let value = extract_number_value(dp.value.as_ref());
                            write_metric_line(
                                &mut output,
                                &name,
                                &labels,
                                value,
                                dp.time_unix_nano,
                            );
                        }
                    }
                    Some(Data::Sum(sum)) => {
                        let type_str = if sum.is_monotonic { "counter" } else { "gauge" };
                        write_help_type(&mut output, &name, description, type_str);
                        for dp in &sum.data_points {
                            let labels = format_attributes(&dp.attributes);
                            let value = extract_number_value(dp.value.as_ref());
                            write_metric_line(
                                &mut output,
                                &name,
                                &labels,
                                value,
                                dp.time_unix_nano,
                            );
                        }
                    }
                    Some(Data::Histogram(histogram)) => {
                        write_help_type(&mut output, &name, description, "histogram");
                        for dp in &histogram.data_points {
                            let base_labels = format_attributes(&dp.attributes);
                            let timestamp = dp.time_unix_nano;

                            // Write bucket lines
                            let mut cumulative_count = 0u64;
                            for (i, &bound) in dp.explicit_bounds.iter().enumerate() {
                                if let Some(&count) = dp.bucket_counts.get(i) {
                                    cumulative_count += count;
                                    let bucket_labels = if base_labels.is_empty() {
                                        format!("le=\"{bound}\"")
                                    } else {
                                        format!("{base_labels},le=\"{bound}\"")
                                    };
                                    write_metric_line(
                                        &mut output,
                                        &format!("{name}_bucket"),
                                        &bucket_labels,
                                        cumulative_count as f64,
                                        timestamp,
                                    );
                                }
                            }

                            // +Inf bucket
                            if let Some(&last_count) = dp.bucket_counts.last() {
                                cumulative_count += last_count;
                            }
                            let inf_labels = if base_labels.is_empty() {
                                "le=\"+Inf\"".to_string()
                            } else {
                                format!("{base_labels},le=\"+Inf\"")
                            };
                            write_metric_line(
                                &mut output,
                                &format!("{name}_bucket"),
                                &inf_labels,
                                cumulative_count as f64,
                                timestamp,
                            );

                            // _sum and _count
                            if let Some(sum) = dp.sum {
                                write_metric_line(
                                    &mut output,
                                    &format!("{name}_sum"),
                                    &base_labels,
                                    sum,
                                    timestamp,
                                );
                            }
                            write_metric_line(
                                &mut output,
                                &format!("{name}_count"),
                                &base_labels,
                                dp.count as f64,
                                timestamp,
                            );
                        }
                    }
                    Some(Data::Summary(summary)) => {
                        write_help_type(&mut output, &name, description, "summary");
                        for dp in &summary.data_points {
                            let base_labels = format_attributes(&dp.attributes);
                            let timestamp = dp.time_unix_nano;

                            // Write quantile lines
                            for quantile in &dp.quantile_values {
                                let q_labels = if base_labels.is_empty() {
                                    format!("quantile=\"{}\"", quantile.quantile)
                                } else {
                                    format!("{base_labels},quantile=\"{}\"", quantile.quantile)
                                };
                                write_metric_line(
                                    &mut output,
                                    &name,
                                    &q_labels,
                                    quantile.value,
                                    timestamp,
                                );
                            }

                            // _sum and _count
                            write_metric_line(
                                &mut output,
                                &format!("{name}_sum"),
                                &base_labels,
                                dp.sum,
                                timestamp,
                            );
                            write_metric_line(
                                &mut output,
                                &format!("{name}_count"),
                                &base_labels,
                                dp.count as f64,
                                timestamp,
                            );
                        }
                    }
                    Some(Data::ExponentialHistogram(_)) | None => {
                        // Prometheus doesn't natively support exponential histograms
                        // Skip for now
                    }
                }
            }
        }
    }

    output
}

/// Writes HELP and TYPE lines for a metric.
fn write_help_type(output: &mut String, name: &str, description: &str, type_str: &str) {
    if !description.is_empty() {
        let _ = writeln!(output, "# HELP {name} {description}");
    }
    let _ = writeln!(output, "# TYPE {name} {type_str}");
}

/// Writes a single metric line in Prometheus format.
fn write_metric_line(
    output: &mut String,
    name: &str,
    labels: &str,
    value: f64,
    timestamp_nanos: u64,
) {
    if labels.is_empty() {
        if timestamp_nanos > 0 {
            let timestamp_ms = timestamp_nanos / 1_000_000;
            let _ = writeln!(output, "{name} {value} {timestamp_ms}");
        } else {
            let _ = writeln!(output, "{name} {value}");
        }
    } else if timestamp_nanos > 0 {
        let timestamp_ms = timestamp_nanos / 1_000_000;
        let _ = writeln!(output, "{name}{{{labels}}} {value} {timestamp_ms}");
    } else {
        let _ = writeln!(output, "{name}{{{labels}}} {value}");
    }
}

/// Sanitizes a metric name to be Prometheus-compatible.
fn sanitize_metric_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Formats OTLP attributes as Prometheus label string.
fn format_attributes(attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue]) -> String {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;

    attributes
        .iter()
        .filter_map(|kv| {
            let key = sanitize_label_name(&kv.key);
            let value = kv.value.as_ref().and_then(|v| v.value.as_ref())?;
            let value_str = match value {
                Value::StringValue(s) => escape_label_value(s),
                Value::BoolValue(b) => b.to_string(),
                Value::IntValue(i) => i.to_string(),
                Value::DoubleValue(d) => d.to_string(),
                _ => return None,
            };
            Some(format!("{key}=\"{value_str}\""))
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Sanitizes a label name to be Prometheus-compatible.
fn sanitize_label_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    // Label names must not start with a digit
    if sanitized.starts_with(|c: char| c.is_ascii_digit()) {
        format!("_{sanitized}")
    } else {
        sanitized
    }
}

/// Escapes special characters in label values.
fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Extracts a numeric value from OTLP number data point.
#[expect(clippy::cast_precision_loss)]
fn extract_number_value(
    value: Option<&opentelemetry_proto::tonic::metrics::v1::number_data_point::Value>,
) -> f64 {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;

    match value {
        Some(Value::AsDouble(d)) => *d,
        Some(Value::AsInt(i)) => *i as f64,
        None => 0.0,
    }
}

#[expect(clippy::cast_precision_loss)]
fn calculate_percentile(
    cumulative_counts: &[u64],
    bounds: &[f64],
    total_count: u64,
    percentile: f64,
) -> f64 {
    if total_count == 0 || !(0.0..=100.0).contains(&percentile) {
        return f64::NAN;
    }

    let target = (percentile / 100.0) * total_count as f64;
    let mut prev_count = 0;
    let mut prev_bound = bounds.first().copied().unwrap_or(0.0);

    for (i, &count) in cumulative_counts.iter().enumerate() {
        if count as f64 >= target {
            let lower = prev_count as f64;
            let upper = count as f64;
            let lower_bound = prev_bound;
            let upper_bound = bounds[i];

            // Linear interpolation
            if upper > lower {
                let fraction = (target - lower) / (upper - lower);
                return lower_bound + fraction * (upper_bound - lower_bound);
            }
            return lower_bound;
        }
        prev_count = count;
        prev_bound = bounds[i];
    }

    *bounds.last().unwrap_or(&f64::INFINITY)
}

/// Encodes `registry` as Prometheus text, including one derived `*_summary`
/// family per histogram family.
fn encode_registry(prometheus_registry: &prometheus::Registry) -> Bytes {
    let encoder = TextEncoder::new();
    let mut metric_families = prometheus_registry.gather();
    metric_families.extend(derived_histogram_summaries(&metric_families));

    let mut result = Vec::new();
    match encoder.encode(&metric_families, &mut result) {
        Ok(()) => result.into(),
        Err(e) => {
            tracing::error!("Error encoding Prometheus metrics: {e}");
            "Error encoding Prometheus metrics".into()
        }
    }
}

/// One `*_summary` family per histogram family, so the text encoder emits a
/// single `# HELP`/`# TYPE` block. The Go `expfmt` parser (promtool, Telegraf)
/// rejects a second HELP for the same name and drops the whole scrape.
fn derived_histogram_summaries(metric_families: &[MetricFamily]) -> Vec<MetricFamily> {
    metric_families
        .iter()
        .filter(|family| family.get_field_type() == MetricType::HISTOGRAM)
        .map(histogram_family_to_summary)
        .collect()
}

fn histogram_family_to_summary(family: &MetricFamily) -> MetricFamily {
    let mut summary = MetricFamily::new();
    summary.set_name(format!("{}_summary", family.name()));
    summary.set_help(format!("Summary derived from histogram {}", family.name()));
    summary.set_field_type(MetricType::SUMMARY);

    for metric in family.get_metric() {
        summary.mut_metric().push(histogram_to_summary_metric(
            metric.get_label(),
            metric.get_histogram(),
        ));
    }

    summary
}

fn histogram_to_summary_metric(histogram_labels: &[LabelPair], h: &Histogram) -> Metric {
    let mut summary_metric = Metric::new();
    summary_metric.set_label(histogram_labels.into());

    let total_count = h.get_sample_count();
    let total_sum = h.get_sample_sum();
    let (first_lower, last_upper) = occupied_bucket_bounds(h);

    let cumulative_counts: Vec<u64> = h
        .get_bucket()
        .iter()
        .map(Bucket::cumulative_count)
        .collect();
    let bounds: Vec<f64> = h.get_bucket().iter().map(Bucket::upper_bound).collect();

    let mut summary_proto = prometheus::proto::Summary::new();

    for &p in &PERCENTILES {
        let interpolated = calculate_percentile(&cumulative_counts, &bounds, total_count, p);
        let value = clamp_derived_quantile(
            interpolated,
            total_count,
            total_sum,
            first_lower,
            last_upper,
        );
        let mut quantile = prometheus::proto::Quantile::new();
        quantile.set_quantile(p / 100.0);
        quantile.set_value(value);
        summary_proto.quantile.push(quantile);
    }

    summary_proto.set_sample_count(total_count);
    summary_proto.set_sample_sum(total_sum);

    summary_metric.set_summary(summary_proto);
    summary_metric
}

/// Lower bound of the first occupied bucket and upper bound of the last.
fn occupied_bucket_bounds(h: &Histogram) -> (f64, f64) {
    let mut previous_upper = f64::NEG_INFINITY;
    let mut previous_count = 0_u64;
    let mut first_lower = f64::NEG_INFINITY;
    let mut last_upper = f64::INFINITY;
    let mut saw_observation = false;

    for bucket in h.get_bucket() {
        let count = bucket.cumulative_count();
        let upper = bucket.upper_bound();
        if count > previous_count {
            if !saw_observation {
                first_lower = previous_upper;
                saw_observation = true;
            }
            last_upper = upper;
        }
        previous_upper = upper;
        previous_count = count;
    }

    (first_lower, last_upper)
}

/// Bucket interpolation can report a quantile no observation could have, given
/// count, sum, and the occupied bucket. A single observation *is* the
/// distribution: every quantile is that sample.
#[expect(clippy::cast_precision_loss)]
fn clamp_derived_quantile(
    value: f64,
    sample_count: u64,
    sample_sum: f64,
    first_lower: f64,
    last_upper: f64,
) -> f64 {
    if sample_count == 0 || !value.is_finite() {
        return value;
    }
    if sample_count == 1 {
        return sample_sum;
    }

    let others = (sample_count - 1) as f64;
    let min_from_sum = if last_upper.is_finite() {
        sample_sum - others * last_upper
    } else {
        f64::NEG_INFINITY
    };
    let max_from_sum = if first_lower.is_finite() {
        sample_sum - others * first_lower
    } else {
        f64::INFINITY
    };
    let min_bound = first_lower.max(min_from_sum);
    let max_bound = last_upper.min(max_from_sum);
    let lo = min_bound.min(max_bound);
    let hi = min_bound.max(max_bound);
    value.clamp(lo, hi)
}

#[cfg(test)]
mod tests {
    use super::*;
    use governor::Quota;
    use http_body_util::BodyExt;
    use prometheus::{HistogramOpts, HistogramVec};
    use std::collections::HashSet;
    use std::num::NonZeroU32;

    fn assert_float_eq(a: f64, b: f64) {
        assert!(
            (a - b).abs() < f64::EPSILON,
            "{a} is not approximately equal to {b}",
        );
    }

    #[test]
    fn test_calculate_percentile() {
        let cumulative_counts = vec![10, 30, 60, 90, 100];
        let bounds = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let total_count = 100;

        assert_float_eq(
            calculate_percentile(&cumulative_counts, &bounds, total_count, 25.0),
            17.5,
        );
        assert_float_eq(
            calculate_percentile(&cumulative_counts, &bounds, total_count, 50.0),
            26.666_666_666_666_664,
        );
        assert_float_eq(
            calculate_percentile(&cumulative_counts, &bounds, total_count, 75.0),
            35.0,
        );
        assert_float_eq(
            calculate_percentile(&cumulative_counts, &bounds, total_count, 90.0),
            40.0,
        );
        assert_float_eq(
            calculate_percentile(&cumulative_counts, &bounds, total_count, 95.0),
            45.0,
        );
        assert_float_eq(
            calculate_percentile(&cumulative_counts, &bounds, total_count, 99.0),
            49.0,
        );
    }

    /// Cumulative bucket counts for `count` observations that each took `latency_ms`.
    fn cumulative_counts_for(bounds: &[f64], latency_ms: f64, count: u64) -> Vec<u64> {
        bounds
            .iter()
            .map(|&bound| if latency_ms <= bound { count } else { 0 })
            .collect()
    }

    /// A quantile has to fall in the bucket the observations landed in, which it cannot do while a
    /// whole distribution shares one bucket.
    ///
    /// Regression test for #12693.
    #[test]
    fn sub_millisecond_latencies_are_resolved_by_the_duration_buckets() {
        use telemetry::DURATION_MS_HISTOGRAM_BUCKETS as BOUNDS;

        // The mean of a real series whose quantiles came back as 50/90/95/99.
        let latency_ms = 0.2755;
        let total_count = 11_925;
        let counts = cumulative_counts_for(&BOUNDS, latency_ms, total_count);

        for percentile in [50.0, 90.0, 95.0, 99.0] {
            let value = calculate_percentile(&counts, &BOUNDS, total_count, percentile);

            assert!(
                value > 0.25 && value <= 0.5,
                "the {percentile}th percentile of {latency_ms}ms observations should land in the \
                 bucket holding them, got {value}"
            );
        }
    }

    #[test]
    fn test_calculate_percentile_edge_cases() {
        let cumulative_counts = vec![0, 100];
        let bounds = vec![0.0, 100.0];
        let total_count = 100;

        assert_float_eq(
            calculate_percentile(&cumulative_counts, &bounds, total_count, 0.0),
            0.0,
        );
        assert_float_eq(
            calculate_percentile(&cumulative_counts, &bounds, total_count, 100.0),
            100.0,
        );
    }

    #[test]
    fn test_histogram_to_summary() {
        let (family, _metric, _histogram) = create_test_histogram();
        let summary = histogram_family_to_summary(&family);

        assert_eq!(summary.name(), "test_histogram_summary");
        assert_eq!(
            summary.help(),
            "Summary derived from histogram test_histogram"
        );
        assert_eq!(summary.get_field_type(), MetricType::SUMMARY);

        let metric = &summary.get_metric()[0];
        let summary_proto = metric.get_summary();

        assert_eq!(summary_proto.sample_count(), 550);
        assert_float_eq(summary_proto.sample_sum(), 35750.0);

        let quantiles: Vec<(f64, f64)> = summary_proto
            .quantile
            .iter()
            .map(|q| (q.quantile(), q.value()))
            .collect();

        assert_eq!(quantiles.len(), 4);
        assert_float_eq(quantiles[0].0, 0.5);
        assert_float_eq(quantiles[1].0, 0.9);
        assert_float_eq(quantiles[2].0, 0.95);
        assert_float_eq(quantiles[3].0, 0.99);

        assert!(quantiles[0].1 > 69.2 && quantiles[0].1 < 69.3);
        assert!(quantiles[1].1 > 94.4 && quantiles[1].1 < 94.6);
        assert!(quantiles[2].1 > 97.2 && quantiles[2].1 < 97.3);
        assert!(quantiles[3].1 > 99.4 && quantiles[3].1 < 99.5);
    }

    /// The Go `expfmt` rule: a family name may appear on `# HELP` and `# TYPE` at
    /// most once. This is what `promtool check metrics` and Telegraf enforce.
    fn parse_prometheus_text_strict(text: &str) -> Result<HashSet<String>, String> {
        let mut help_names = HashSet::new();
        let mut type_names = HashSet::new();
        for (idx, line) in text.lines().enumerate() {
            let line_no = idx + 1;
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let name = rest
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| format!("line {line_no}: HELP without a metric name"))?;
                if !help_names.insert(name.to_string()) {
                    return Err(format!(
                        "PARSE ERROR: text format parsing error in line {line_no}: second HELP line for metric name \"{name}\""
                    ));
                }
            } else if let Some(rest) = line.strip_prefix("# TYPE ") {
                let name = rest
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| format!("line {line_no}: TYPE without a metric name"))?;
                if !type_names.insert(name.to_string()) {
                    return Err(format!(
                        "PARSE ERROR: text format parsing error in line {line_no}: second TYPE line for metric name \"{name}\""
                    ));
                }
            }
        }
        Ok(help_names.union(&type_names).cloned().collect())
    }

    fn comment_name<'a>(line: &'a str, kind: &str) -> Option<&'a str> {
        let rest = match kind {
            "HELP" => line.strip_prefix("# HELP ")?,
            "TYPE" => line.strip_prefix("# TYPE ")?,
            _ => return None,
        };
        rest.split_whitespace().next()
    }

    fn help_count(text: &str, family: &str) -> usize {
        text.lines()
            .filter(|line| comment_name(line, "HELP") == Some(family))
            .count()
    }

    fn type_count(text: &str, family: &str) -> usize {
        text.lines()
            .filter(|line| comment_name(line, "TYPE") == Some(family))
            .count()
    }

    fn two_refresh_histograms() -> prometheus::Registry {
        let registry = prometheus::Registry::new();
        let histogram = HistogramVec::new(
            HistogramOpts::new(
                "dataset_acceleration_refresh_duration_ms",
                "Duration in milliseconds of a dataset acceleration refresh",
            )
            .buckets(telemetry::DURATION_MS_HISTOGRAM_BUCKETS.to_vec()),
            &["dataset"],
        )
        .expect("refresh duration histogram");
        registry
            .register(Box::new(histogram.clone()))
            .expect("register refresh duration histogram");
        histogram.with_label_values(&["products"]).observe(11.88);
        histogram.with_label_values(&["events"]).observe(217.0);
        registry
    }

    fn histogram_with_single_observation(latency_ms: f64) -> Histogram {
        let mut histogram = Histogram::new();
        let mut cumulative = 0_u64;
        for &upper in &telemetry::DURATION_MS_HISTOGRAM_BUCKETS {
            if latency_ms <= upper {
                cumulative = 1;
            }
            let mut bucket = Bucket::new();
            bucket.set_cumulative_count(cumulative);
            bucket.set_upper_bound(upper);
            histogram.bucket.push(bucket);
        }
        let mut inf = Bucket::new();
        inf.set_cumulative_count(1);
        inf.set_upper_bound(f64::INFINITY);
        histogram.bucket.push(inf);
        histogram.set_sample_count(1);
        histogram.set_sample_sum(latency_ms);
        histogram
    }

    fn labeled_histogram_family(name: &str, series: &[(&str, &str, &Histogram)]) -> MetricFamily {
        let mut family = MetricFamily::new();
        family.set_name(name.to_string());
        family.set_help(format!("Test histogram {name}"));
        family.set_field_type(MetricType::HISTOGRAM);
        for (label_name, label_value, histogram) in series {
            let mut label = LabelPair::new();
            label.set_name((*label_name).to_string());
            label.set_value((*label_value).to_string());
            let mut metric = Metric::new();
            metric.set_label(vec![label].into());
            metric.set_histogram((*histogram).clone());
            family.mut_metric().push(metric);
        }
        family
    }

    /// `TextEncoder` writes HELP/TYPE once per `MetricFamily`. Two families that
    /// share a name are invalid exposition: `expfmt` rejects the scrape.
    #[test]
    fn a_summary_family_per_label_set_is_invalid_exposition() {
        let products = histogram_with_single_observation(11.88);
        let events = histogram_with_single_observation(217.0);
        let mut families = Vec::new();
        for (dataset, histogram) in [("products", &products), ("events", &events)] {
            let family = labeled_histogram_family(
                "dataset_acceleration_refresh_duration_ms",
                &[("dataset", dataset, histogram)],
            );
            families.push(histogram_family_to_summary(&family));
        }

        let mut encoded = Vec::new();
        TextEncoder::new()
            .encode(&families, &mut encoded)
            .expect("encode the per-series families");
        let text = String::from_utf8(encoded).expect("utf8 prometheus text");

        assert_eq!(
            help_count(&text, "dataset_acceleration_refresh_duration_ms_summary"),
            2
        );
        let err = parse_prometheus_text_strict(&text)
            .expect_err("a second HELP for the same family must fail");
        assert!(
            err.contains("second HELP line for metric name \"dataset_acceleration_refresh_duration_ms_summary\""),
            "expfmt-shaped error, got {err}"
        );
    }

    #[test]
    fn derived_summaries_emit_one_help_and_type_per_family() {
        let text = encode_registry(&two_refresh_histograms());
        let text = std::str::from_utf8(&text).expect("utf8 prometheus text");

        parse_prometheus_text_strict(text).expect("strict prometheus exposition");
        assert_eq!(
            help_count(text, "dataset_acceleration_refresh_duration_ms_summary"),
            1
        );
        assert_eq!(
            type_count(text, "dataset_acceleration_refresh_duration_ms_summary"),
            1
        );
        assert!(
            text.contains("dataset_acceleration_refresh_duration_ms_summary{dataset=\"products\""),
            "products series missing from:\n{text}"
        );
        assert!(
            text.contains("dataset_acceleration_refresh_duration_ms_summary{dataset=\"events\""),
            "events series missing from:\n{text}"
        );
    }

    #[test]
    fn grouped_histogram_family_keeps_every_label_set() {
        let products = histogram_with_single_observation(11.88);
        let events = histogram_with_single_observation(217.0);
        let family = labeled_histogram_family(
            "dataset_acceleration_refresh_duration_ms",
            &[
                ("dataset", "products", &products),
                ("dataset", "events", &events),
            ],
        );
        let summary = histogram_family_to_summary(&family);
        assert_eq!(summary.get_metric().len(), 2);
        assert_eq!(
            summary.name(),
            "dataset_acceleration_refresh_duration_ms_summary"
        );
        assert_eq!(summary.get_field_type(), MetricType::SUMMARY);

        let mut encoded = Vec::new();
        TextEncoder::new()
            .encode(std::slice::from_ref(&summary), &mut encoded)
            .expect("encode the grouped family");
        let text = String::from_utf8(encoded).expect("utf8 prometheus text");
        parse_prometheus_text_strict(&text).expect("strict prometheus exposition");
        assert_eq!(
            help_count(&text, "dataset_acceleration_refresh_duration_ms_summary"),
            1
        );
    }

    /// A single refresh of 11.88ms must not report p50=17.5 / p99=24.85, which is
    /// what interpolating inside the 10–25ms duration bucket does.
    #[test]
    fn single_observation_quantiles_match_the_observed_value() {
        for latency_ms in [11.88, 217.0] {
            let histogram = histogram_with_single_observation(latency_ms);
            let family = labeled_histogram_family(
                "dataset_acceleration_refresh_duration_ms",
                &[("dataset", "products", &histogram)],
            );
            let summary = histogram_family_to_summary(&family);
            let quantiles: Vec<f64> = summary.get_metric()[0]
                .get_summary()
                .quantile
                .iter()
                .map(prometheus::proto::Quantile::value)
                .collect();
            assert_eq!(quantiles.len(), 4);
            for (idx, value) in quantiles.iter().enumerate() {
                assert_float_eq(*value, latency_ms);
                assert!(
                    *value <= latency_ms,
                    "quantile[{idx}] {value} exceeds the only observation {latency_ms}"
                );
            }
        }
    }

    /// Unclamped interpolation of the brief's 11.88ms / 217ms samples, so the
    /// clamp is tested against the numbers a scrape actually produced.
    #[test]
    fn duration_bucket_interpolation_exceeds_a_single_observation() {
        let cases = [(11.88, 17.5, 24.85), (217.0, 175.0, 248.5)];
        for (latency_ms, p50, p99) in cases {
            let histogram = histogram_with_single_observation(latency_ms);
            let counts: Vec<u64> = histogram
                .get_bucket()
                .iter()
                .map(Bucket::cumulative_count)
                .collect();
            let bounds: Vec<f64> = histogram
                .get_bucket()
                .iter()
                .map(Bucket::upper_bound)
                .collect();
            let interpolated_p50 = calculate_percentile(&counts, &bounds, 1, 50.0);
            let interpolated_p99 = calculate_percentile(&counts, &bounds, 1, 99.0);
            assert!(
                (interpolated_p50 - p50).abs() < 1e-9,
                "p50: {interpolated_p50} vs {p50}"
            );
            assert!(
                (interpolated_p99 - p99).abs() < 1e-9,
                "p99: {interpolated_p99} vs {p99}"
            );
            assert!(
                (interpolated_p50 - latency_ms).abs() > 1.0
                    || (interpolated_p99 - latency_ms).abs() > 1.0,
                "bucket interpolation of a single {latency_ms}ms sample should miss the observation"
            );

            let (first_lower, last_upper) = occupied_bucket_bounds(&histogram);
            assert_float_eq(
                clamp_derived_quantile(interpolated_p50, 1, latency_ms, first_lower, last_upper),
                latency_ms,
            );
            assert_float_eq(
                clamp_derived_quantile(interpolated_p99, 1, latency_ms, first_lower, last_upper),
                latency_ms,
            );
        }
    }

    #[tokio::test]
    async fn metrics_endpoint_exposition_is_strictly_valid() {
        let registry = two_refresh_histograms();
        let request = Request::builder()
            .uri("/metrics")
            .body(())
            .expect("test request should build");
        let response = handle_http_request(&registry, None, None, &request).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        let text = std::str::from_utf8(&body).expect("utf8 prometheus text");
        parse_prometheus_text_strict(text)
            .expect("GET /metrics must be valid Prometheus text exposition");
    }

    #[expect(clippy::cast_precision_loss)]
    fn create_test_histogram() -> (MetricFamily, Metric, Histogram) {
        let mut family = MetricFamily::new();
        family.set_name("test_histogram".to_string());
        family.set_help("Test histogram".to_string());
        family.set_field_type(MetricType::HISTOGRAM);

        let mut metric = Metric::new();
        let mut histogram = Histogram::new();

        let buckets = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0];
        let mut cumulative_count = 0;
        let mut sample_sum = 0.0;

        for (i, &upper_bound) in buckets.iter().enumerate() {
            let count = (i + 1) * 10;
            cumulative_count += count as u64;
            let mut bucket = Bucket::new();
            bucket.set_cumulative_count(cumulative_count);
            bucket.set_upper_bound(upper_bound);
            histogram.bucket.push(bucket);

            // Calculate sample sum (approximate)
            let lower_bound = if i == 0 { 0.0 } else { buckets[i - 1] };
            sample_sum += f64::midpoint(lower_bound, upper_bound) * count as f64;
        }

        histogram.set_sample_count(cumulative_count);
        histogram.set_sample_sum(sample_sum);

        metric.set_histogram(histogram.clone());
        family.mut_metric().push(metric.clone());

        (family, metric, histogram)
    }

    #[test]
    fn test_sanitize_metric_name() {
        assert_eq!(
            sanitize_metric_name("http_requests_total"),
            "http_requests_total"
        );
        assert_eq!(
            sanitize_metric_name("http.requests.total"),
            "http_requests_total"
        );
        assert_eq!(
            sanitize_metric_name("http-requests-total"),
            "http_requests_total"
        );
        assert_eq!(
            sanitize_metric_name("metric:with:colons"),
            "metric:with:colons"
        );
    }

    #[test]
    fn test_sanitize_label_name() {
        assert_eq!(sanitize_label_name("method"), "method");
        assert_eq!(sanitize_label_name("http.method"), "http_method");
        assert_eq!(sanitize_label_name("123start"), "_123start");
    }

    #[test]
    fn test_escape_label_value() {
        assert_eq!(escape_label_value("simple"), "simple");
        assert_eq!(escape_label_value("with\"quote"), "with\\\"quote");
        assert_eq!(escape_label_value("with\\backslash"), "with\\\\backslash");
        assert_eq!(escape_label_value("with\nnewline"), "with\\nnewline");
    }

    #[test]
    fn test_parse_query_string() {
        let params = parse_query_string("scope=cluster&foo=bar");
        assert_eq!(params.get("scope"), Some(&"cluster".to_string()));
        assert_eq!(params.get("foo"), Some(&"bar".to_string()));

        let params = parse_query_string("scope=cluster");
        assert_eq!(params.get("scope"), Some(&"cluster".to_string()));

        let params = parse_query_string("");
        assert!(params.is_empty());
    }

    #[tokio::test]
    async fn test_handle_http_request_rate_limit_returns_retry_after() {
        let registry = prometheus::Registry::new();
        let rate_limiter = RateLimiter::direct(Quota::per_minute(
            NonZeroU32::new(1).expect("test quota must be non-zero"),
        ));
        let request = Request::builder()
            .uri("/metrics")
            .body(())
            .expect("test request should build");

        let first = handle_http_request(&registry, None, Some(&rate_limiter), &request).await;
        assert_eq!(first.status(), StatusCode::OK);

        let second = handle_http_request(&registry, None, Some(&rate_limiter), &request).await;
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);

        let retry_after = second
            .headers()
            .get(RETRY_AFTER)
            .expect("Retry-After header should be present")
            .to_str()
            .expect("Retry-After header should be ASCII")
            .parse::<u64>()
            .expect("Retry-After header should be an integer");
        assert!(retry_after >= 1);
    }

    #[tokio::test]
    async fn test_handle_http_request_health_bypasses_rate_limit() {
        // Health probes (used by readiness checks) must never be subject to the /metrics
        // bucket — otherwise an exhausted limiter would also fail liveness.
        let registry = prometheus::Registry::new();
        let rate_limiter = RateLimiter::direct(Quota::per_minute(
            NonZeroU32::new(1).expect("test quota must be non-zero"),
        ));
        let metrics_request = Request::builder()
            .uri("/metrics")
            .body(())
            .expect("test request should build");
        let health_request = Request::builder()
            .uri("/health")
            .body(())
            .expect("test request should build");

        // Burn the only token in the bucket.
        let first =
            handle_http_request(&registry, None, Some(&rate_limiter), &metrics_request).await;
        assert_eq!(first.status(), StatusCode::OK);
        let second =
            handle_http_request(&registry, None, Some(&rate_limiter), &metrics_request).await;
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);

        // /health should still succeed repeatedly even with the bucket empty.
        for _ in 0..3 {
            let resp =
                handle_http_request(&registry, None, Some(&rate_limiter), &health_request).await;
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn test_handle_http_request_non_metrics_paths_return_not_found() {
        let registry = prometheus::Registry::new();
        let rate_limiter = RateLimiter::direct(Quota::per_minute(
            NonZeroU32::new(1).expect("test quota must be non-zero"),
        ));

        // Paths other than /health and /metrics must return 404 and must NOT consume the
        // rate-limit bucket — otherwise the limiter could be bypassed by scraping `/`,
        // `/metrics/`, or any other path.
        for bypass_path in ["/", "/metrics/", "/foo"] {
            let request = Request::builder()
                .uri(bypass_path)
                .body(())
                .expect("test request should build");
            let response =
                handle_http_request(&registry, None, Some(&rate_limiter), &request).await;
            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "path {bypass_path} should return 404"
            );
        }

        let metrics_request = Request::builder()
            .uri("/metrics")
            .body(())
            .expect("test request should build");

        let first_metrics =
            handle_http_request(&registry, None, Some(&rate_limiter), &metrics_request).await;
        assert_eq!(first_metrics.status(), StatusCode::OK);

        let second_metrics =
            handle_http_request(&registry, None, Some(&rate_limiter), &metrics_request).await;
        assert_eq!(second_metrics.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn test_otlp_to_prometheus_gauge() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric as OtlpMetric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
            number_data_point::Value as NumberValue,
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![OtlpMetric {
                        name: "test_gauge".to_string(),
                        description: "A test gauge".to_string(),
                        unit: String::new(),
                        metadata: Vec::new(),
                        data: Some(
                            opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint {
                                    attributes: vec![KeyValue {
                                        key: "host".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(Value::StringValue(
                                                "localhost".to_string(),
                                            )),
                                        }),
                                        ..Default::default()
                                    }],
                                    start_time_unix_nano: 0,
                                    time_unix_nano: 0,
                                    value: Some(NumberValue::AsDouble(42.5)),
                                    exemplars: Vec::new(),
                                    flags: 0,
                                }],
                            }),
                        ),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let output = otlp_to_prometheus_text(&request);
        assert!(output.contains("# HELP test_gauge A test gauge"));
        assert!(output.contains("# TYPE test_gauge gauge"));
        assert!(output.contains("test_gauge{host=\"localhost\"} 42.5"));
    }

    #[test]
    fn test_otlp_to_prometheus_counter() {
        use opentelemetry_proto::tonic::metrics::v1::{
            Metric as OtlpMetric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
            number_data_point::Value as NumberValue,
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![OtlpMetric {
                        name: "http_requests_total".to_string(),
                        description: "Total HTTP requests".to_string(),
                        unit: String::new(),
                        metadata: Vec::new(),
                        data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Sum(
                            Sum {
                                data_points: vec![NumberDataPoint {
                                    attributes: Vec::new(),
                                    start_time_unix_nano: 0,
                                    time_unix_nano: 0,
                                    value: Some(NumberValue::AsInt(100)),
                                    exemplars: Vec::new(),
                                    flags: 0,
                                }],
                                aggregation_temporality: 2, // Cumulative
                                is_monotonic: true,
                            },
                        )),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let output = otlp_to_prometheus_text(&request);
        assert!(output.contains("# TYPE http_requests_total counter"));
        assert!(output.contains("http_requests_total 100"));
    }

    #[test]
    fn test_otlp_to_prometheus_empty() {
        let request = ExportMetricsServiceRequest::default();
        let output = otlp_to_prometheus_text(&request);
        assert!(output.is_empty());
    }

    #[test]
    fn test_parse_query_string_empty_value() {
        // Handle `key=` with no value
        let params = parse_query_string("scope=&foo=bar");
        assert_eq!(params.get("scope"), Some(&String::new()));
        assert_eq!(params.get("foo"), Some(&"bar".to_string()));

        // Just `key=`
        let params = parse_query_string("key=");
        assert_eq!(params.get("key"), Some(&String::new()));
    }

    #[test]
    fn test_parse_query_string_multiple_equals() {
        // Handle `key=value=with=equals`
        let params = parse_query_string("filter=status=active");
        // The value should be everything after the first `=`
        assert_eq!(params.get("filter"), Some(&"status=active".to_string()));

        let params = parse_query_string("query=a=b=c&other=value");
        assert_eq!(params.get("query"), Some(&"a=b=c".to_string()));
        assert_eq!(params.get("other"), Some(&"value".to_string()));
    }

    #[test]
    fn test_otlp_to_prometheus_histogram() {
        use opentelemetry_proto::tonic::metrics::v1::{
            Histogram as OtlpHistogram, HistogramDataPoint, Metric as OtlpMetric, ResourceMetrics,
            ScopeMetrics,
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![OtlpMetric {
                        name: "http_request_duration_seconds".to_string(),
                        description: "HTTP request duration".to_string(),
                        unit: String::new(),
                        metadata: Vec::new(),
                        data: Some(
                            opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(
                                OtlpHistogram {
                                    data_points: vec![HistogramDataPoint {
                                        attributes: Vec::new(),
                                        start_time_unix_nano: 0,
                                        time_unix_nano: 0,
                                        count: 100,
                                        sum: Some(50.0),
                                        bucket_counts: vec![10, 30, 40, 15, 5],
                                        explicit_bounds: vec![0.01, 0.05, 0.1, 0.5],
                                        exemplars: Vec::new(),
                                        flags: 0,
                                        min: None,
                                        max: None,
                                    }],
                                    aggregation_temporality: 2,
                                },
                            ),
                        ),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let output = otlp_to_prometheus_text(&request);

        // Check help and type
        assert!(output.contains("# HELP http_request_duration_seconds HTTP request duration"));
        assert!(output.contains("# TYPE http_request_duration_seconds histogram"));

        // Check bucket lines (cumulative counts)
        assert!(output.contains("http_request_duration_seconds_bucket{le=\"0.01\"} 10"));
        assert!(output.contains("http_request_duration_seconds_bucket{le=\"0.05\"} 40")); // 10 + 30
        assert!(output.contains("http_request_duration_seconds_bucket{le=\"0.1\"} 80")); // 10 + 30 + 40
        assert!(output.contains("http_request_duration_seconds_bucket{le=\"0.5\"} 95")); // 10 + 30 + 40 + 15
        assert!(output.contains("http_request_duration_seconds_bucket{le=\"+Inf\"} 100")); // all

        // Check sum and count
        assert!(output.contains("http_request_duration_seconds_sum 50"));
        assert!(output.contains("http_request_duration_seconds_count 100"));
    }
}
