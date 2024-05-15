/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use metrics_table::MetricsTable;
use snafu::prelude::*;
use tokio::spawn;

pub mod metrics_table;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error creating record batch: {source}",))]
    UnableToCreateRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Error queriing prometheus metrics: {source}"))]
    FailedToQueryPrometheusMetrics { source: reqwest::Error },

    #[snafu(display("Error getting timestamp: {source}"))]
    UnableToGetTimestamp { source: std::time::SystemTimeError },

    #[snafu(display("Error converting timestamp to seconds"))]
    UnableToConvertTimestampToSeconds {},

    #[snafu(display("Error parsing prometheus metrics: {source}"))]
    UnableToParsePrometheusMetrics { source: std::io::Error },
}

pub struct MetricsRecorder {
    socket_addr: Arc<SocketAddr>,
    table: Arc<MetricsTable>,
}

impl MetricsRecorder {
    pub fn table(&self) -> Arc<MetricsTable> {
        Arc::clone(&self.table)
    }
}

impl MetricsRecorder {
    pub fn new(socket_addr: SocketAddr) -> Self {
        let table = MetricsTable::new();

        Self {
            socket_addr: Arc::new(socket_addr),
            table: Arc::new(table),
        }
    }

    async fn tick(socket_addr: &SocketAddr, table: &Arc<MetricsTable>) -> Result<(), Error> {
        let body = reqwest::get(format!("http://{socket_addr}/metrics"))
            .await
            .context(FailedToQueryPrometheusMetricsSnafu)?
            .text()
            .await
            .context(FailedToQueryPrometheusMetricsSnafu)?;

        let lines = body.lines().map(|s| Ok(s.to_owned()));
        let scrape =
            prometheus_parse::Scrape::parse(lines).context(UnableToParsePrometheusMetricsSnafu)?;

        for sample in scrape.samples {
            let value: f64 = match sample.value {
                prometheus_parse::Value::Counter(v)
                | prometheus_parse::Value::Gauge(v)
                | prometheus_parse::Value::Untyped(v) => v,
                prometheus_parse::Value::Histogram(v) => v.into_iter().map(|v| v.count).sum(),
                prometheus_parse::Value::Summary(v) => v.into_iter().map(|v| v.count).sum(),
            };

            table
                .add_record(
                    sample.timestamp.timestamp(),
                    &sample.metric,
                    value,
                    sample.labels.to_string().as_str(),
                )
                .await?;
        }

        Ok(())
    }

    pub fn start(&self) {
        let table = Arc::clone(&self.table);
        let addr = Arc::clone(&self.socket_addr);

        spawn(async move {
            loop {
                if let Err(err) = MetricsRecorder::tick(&addr, &table).await {
                    tracing::debug!("{err}");
                }
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
    }
}
