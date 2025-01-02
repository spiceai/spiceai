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

use crate::tls::TlsConfig;
use bytes::Bytes;
use http::{HeaderValue, Request, Response};
use http_body_util::Full;
use hyper::{
    body::{self, Incoming},
    header::CONTENT_TYPE,
    server::conn::http1::Builder,
};
use hyper_util::rt::TokioIo;
use prometheus::{
    proto::{Bucket, Histogram, LabelPair, Metric, MetricFamily, MetricType},
    Encoder, TextEncoder,
};
use snafu::prelude::*;
use std::net::ToSocketAddrs;
use std::{fmt::Debug, sync::Arc};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

const PERCENTILES: [f64; 4] = [50.0, 90.0, 95.0, 99.0];

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to bind to address: {source}"))]
    UnableToBindServerToPort { source: std::io::Error },

    #[snafu(display("Unable to start HTTP server: {source}"))]
    UnableToStartHttpServer { source: std::io::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) async fn start<A>(
    bind_address: Option<A>,
    prometheus_registry: Option<prometheus::Registry>,
    tls_config: Option<Arc<TlsConfig>>,
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
                process_tls_tcp_stream(stream, acceptor.clone(), prometheus_registry.clone());
            }
            None => {
                process_tcp_stream(stream, prometheus_registry.clone());
            }
        }
    }
}

fn process_tls_tcp_stream(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    prometheus_registry: prometheus::Registry,
) {
    tokio::spawn(async move {
        let stream = acceptor.accept(stream).await;
        match stream {
            Ok(stream) => {
                serve_connection(stream, prometheus_registry).await;
            }
            Err(e) => {
                tracing::debug!("Error accepting TLS connection: {e}");
            }
        }
    });
}

fn process_tcp_stream(stream: TcpStream, prometheus_registry: prometheus::Registry) {
    tokio::spawn(serve_connection(stream, prometheus_registry));
}

async fn serve_connection<S>(stream: S, prometheus_registry: prometheus::Registry)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let service = hyper::service::service_fn(move |req: Request<body::Incoming>| {
        let prometheus_registry = prometheus_registry.clone();
        async move { Ok::<_, hyper::Error>(handle_http_request(&prometheus_registry, &req)) }
    });

    if let Err(err) = Builder::new()
        .serve_connection(TokioIo::new(stream), service)
        .await
    {
        tracing::debug!(error = ?err, "Error serving Prometheus metrics connection.");
    }
}

fn handle_http_request(
    prometheus_registry: &prometheus::Registry,
    req: &Request<Incoming>,
) -> Response<Full<Bytes>> {
    let mut response = Response::new(if req.uri().path() == "/health" {
        "OK".into()
    } else {
        let encoder = TextEncoder::new();
        let mut metric_families = prometheus_registry.gather();

        let mut histogram_summaries = Vec::new();
        for family in &metric_families {
            if family.get_field_type() == MetricType::HISTOGRAM {
                for metric in family.get_metric() {
                    let histogram = metric.get_histogram();
                    let summary =
                        histogram_to_summary(family.get_name(), metric.get_label(), histogram);
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
    });
    response
        .headers_mut()
        .append(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    response
}

#[allow(clippy::cast_precision_loss)]
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
        .map(Bucket::get_cumulative_count)
        .collect();
    let bounds: Vec<f64> = h.get_bucket().iter().map(Bucket::get_upper_bound).collect();

    let mut summary_proto = prometheus::proto::Summary::new();

    for &p in &PERCENTILES {
        let value = calculate_percentile(&cumulative_counts, &bounds, total_count, p);
        let mut quantile = prometheus::proto::Quantile::new();
        quantile.set_quantile(p / 100.0);
        quantile.set_value(value);
        summary_proto.mut_quantile().push(quantile);
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
        let summary = histogram_to_summary(family.get_name(), metric.get_label(), &histogram);

        assert_eq!(summary.get_name(), "test_histogram_summary");
        assert_eq!(
            summary.get_help(),
            "Summary derived from histogram test_histogram"
        );
        assert_eq!(summary.get_field_type(), MetricType::SUMMARY);

        let metric = &summary.get_metric()[0];
        let summary_proto = metric.get_summary();

        assert_eq!(summary_proto.get_sample_count(), 550);
        assert_float_eq(summary_proto.get_sample_sum(), 35750.0);

        let quantiles: Vec<(f64, f64)> = summary_proto
            .get_quantile()
            .iter()
            .map(|q| (q.get_quantile(), q.get_value()))
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

    #[allow(clippy::cast_precision_loss)]
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
            histogram.mut_bucket().push(bucket);

            // Calculate sample sum (approximate)
            let lower_bound = if i == 0 { 0.0 } else { buckets[i - 1] };
            sample_sum += (lower_bound + upper_bound) / 2.0 * count as f64;
        }

        histogram.set_sample_count(cumulative_count);
        histogram.set_sample_sum(sample_sum);

        metric.set_histogram(histogram.clone());
        family.mut_metric().push(metric.clone());

        (family, metric, histogram)
    }
}
