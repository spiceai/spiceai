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

use crate::tls::TlsConfig;
use bytes::Bytes;
use cluster::ClusterMetricsCollector;
use http::{HeaderValue, Request, Response};
use http_body_util::Full;
use hyper::{
    body::{self, Incoming},
    header::CONTENT_TYPE,
    server::conn::http1::Builder,
};
use hyper_util::rt::TokioIo;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prometheus::{
    Encoder, TextEncoder,
    proto::{Bucket, Histogram, LabelPair, Metric, MetricFamily, MetricType},
};
use snafu::prelude::*;
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::net::ToSocketAddrs;
use std::{fmt::Debug, sync::Arc};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

const PERCENTILES: [f64; 4] = [50.0, 90.0, 95.0, 99.0];

/// Query parameter for requesting cluster-wide metrics.
const SCOPE_PARAM: &str = "scope";
const SCOPE_CLUSTER: &str = "cluster";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to bind metrics server to address: {source}"))]
    UnableToBindServerToPort { source: std::io::Error },

    #[snafu(display("Failed to start the metrics HTTP server: {source}"))]
    UnableToStartHttpServer { source: std::io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) async fn start<A>(
    bind_address: Option<A>,
    prometheus_registry: Option<prometheus::Registry>,
    tls_config: Option<Arc<TlsConfig>>,
    cluster_collector: Option<Arc<ClusterMetricsCollector>>,
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
                let acceptor = TlsAcceptor::from(Arc::clone(&config.server_config));
                process_tls_tcp_stream(
                    stream,
                    acceptor.clone(),
                    prometheus_registry.clone(),
                    cluster_collector.clone(),
                );
            }
            None => {
                process_tcp_stream(
                    stream,
                    prometheus_registry.clone(),
                    cluster_collector.clone(),
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
) {
    tokio::spawn(async move {
        let stream = acceptor.accept(stream).await;
        match stream {
            Ok(stream) => {
                serve_connection(stream, prometheus_registry, cluster_collector).await;
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
) {
    tokio::spawn(serve_connection(
        stream,
        prometheus_registry,
        cluster_collector,
    ));
}

async fn serve_connection<S>(
    stream: S,
    prometheus_registry: prometheus::Registry,
    cluster_collector: Option<Arc<ClusterMetricsCollector>>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service = hyper::service::service_fn(move |req: Request<body::Incoming>| {
        let prometheus_registry = prometheus_registry.clone();
        let cluster_collector = cluster_collector.clone();
        async move {
            Ok::<_, hyper::Error>(
                handle_http_request(&prometheus_registry, cluster_collector.as_deref(), &req).await,
            )
        }
    });

    if let Err(err) = Builder::new()
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

async fn handle_http_request(
    prometheus_registry: &prometheus::Registry,
    cluster_collector: Option<&ClusterMetricsCollector>,
    req: &Request<Incoming>,
) -> Response<Full<Bytes>> {
    let mut response = Response::new(if req.uri().path() == "/health" {
        "OK".into()
    } else {
        // Check for ?scope=cluster query parameter
        let query_params = req
            .uri()
            .query()
            .map(parse_query_string)
            .unwrap_or_default();

        let is_cluster_scope = query_params
            .get(SCOPE_PARAM)
            .is_some_and(|v| v == SCOPE_CLUSTER);

        if is_cluster_scope {
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
            // Local metrics only (original behavior)
            let encoder = TextEncoder::new();
            let mut metric_families = prometheus_registry.gather();

            let mut histogram_summaries = Vec::new();
            for family in &metric_families {
                if family.get_field_type() == MetricType::HISTOGRAM {
                    for metric in family.get_metric() {
                        let histogram = metric.get_histogram();
                        let summary =
                            histogram_to_summary(family.name(), metric.get_label(), histogram);
                        histogram_summaries.push(summary);
                    }
                }
            }
            metric_families.extend(histogram_summaries);

            let mut result = Vec::new();
            match encoder.encode(&metric_families, &mut result) {
                Ok(()) => result.into(),
                Err(e) => {
                    tracing::error!("Error encoding Prometheus metrics: {e}");
                    "Error encoding Prometheus metrics".into()
                }
            }
        }
    });
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

fn histogram_to_summary(
    histogram_name: &str,
    histogram_labels: &[LabelPair],
    h: &Histogram,
) -> MetricFamily {
    let mut summary = MetricFamily::new();
    summary.set_name(format!("{histogram_name}_summary"));
    summary.set_help(format!("Summary derived from histogram {histogram_name}",));
    summary.set_field_type(MetricType::SUMMARY);

    let mut summary_metric = Metric::new();
    summary_metric.set_label(histogram_labels.into());

    let total_count = h.get_sample_count();
    let total_sum = h.get_sample_sum();

    let cumulative_counts: Vec<u64> = h
        .get_bucket()
        .iter()
        .map(Bucket::cumulative_count)
        .collect();
    let bounds: Vec<f64> = h.get_bucket().iter().map(Bucket::upper_bound).collect();

    let mut summary_proto = prometheus::proto::Summary::new();

    for &p in &PERCENTILES {
        let value = calculate_percentile(&cumulative_counts, &bounds, total_count, p);
        let mut quantile = prometheus::proto::Quantile::new();
        quantile.set_quantile(p / 100.0);
        quantile.set_value(value);
        summary_proto.quantile.push(quantile);
    }

    summary_proto.set_sample_count(total_count);
    summary_proto.set_sample_sum(total_sum);

    summary_metric.set_summary(summary_proto);
    summary.mut_metric().push(summary_metric);

    summary
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let (family, metric, histogram) = create_test_histogram();
        let summary = histogram_to_summary(family.name(), metric.get_label(), &histogram);

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
