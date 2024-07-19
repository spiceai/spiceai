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

use arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_tools::record_batch::{self, try_cast_to};
use chrono::Utc;
use datafusion::sql::TableReference;
use snafu::prelude::*;
use tokio::spawn;
use tokio::sync::RwLock;

use crate::accelerated_table::refresh::Refresh;
use crate::accelerated_table::Retention;
use crate::component::dataset::acceleration::Acceleration;
use crate::component::dataset::TimeFormat;
use crate::datafusion::Error as DataFusionError;
use crate::datafusion::{DataFusion, SPICE_RUNTIME_SCHEMA};
use crate::dataupdate::DataUpdate;
use crate::internal_table::{create_internal_accelerated_table, Error as InternalTableError};
use crate::secrets::Secrets;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error creating record batch: {source}",))]
    UnableToCreateRecordBatch { source: arrow::error::ArrowError },

    #[snafu(display("Error casting record batch: {source}",))]
    UnableToCastRecordBatch { source: record_batch::Error },

    #[snafu(display("Error querying prometheus metrics: {source}"))]
    FailedToQueryPrometheusMetrics { source: reqwest::Error },

    #[snafu(display("Error parsing prometheus metrics: {source}"))]
    UnableToParsePrometheusMetrics { source: std::io::Error },

    #[snafu(display("Error writing to metrics table: {source}"))]
    UnableToWriteToMetricsTable { source: DataFusionError },

    #[snafu(display("Error creating metrics table: {source}"))]
    UnableToCreateMetricsTable { source: InternalTableError },

    #[snafu(display("Error registering metrics table: {source}"))]
    UnableToRegisterToMetricsTable { source: DataFusionError },
}

pub struct MetricsRecorder {
    socket_addr: Arc<SocketAddr>,
    remote_schema: Arc<Option<Arc<Schema>>>,
}

impl MetricsRecorder {
    #[must_use]
    pub fn new(socket_addr: SocketAddr) -> Self {
        Self {
            socket_addr: Arc::new(socket_addr),
            remote_schema: Arc::new(None),
        }
    }

    pub fn set_remote_schema(&mut self, schema: Arc<Option<Arc<Schema>>>) {
        self.remote_schema = schema;
    }

    pub async fn register_metrics_table(datafusion: &Arc<DataFusion>) -> Result<(), Error> {
        let metrics_table_reference = get_metrics_table_reference();

        let retention = Retention::new(
            Some("timestamp".to_string()),
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(1800)), // delete metrics older then 30 minutes
            Some(Duration::from_secs(300)),  // run retention every 5 minutes
            true,
        );

        let table = create_internal_accelerated_table(
            metrics_table_reference.clone(),
            get_metrics_schema(),
            Acceleration::default(),
            Refresh::default(),
            retention,
            Arc::new(RwLock::new(Secrets::default())),
        )
        .await
        .context(UnableToCreateMetricsTableSnafu)?;

        datafusion
            .register_runtime_table(metrics_table_reference, table)
            .context(UnableToRegisterToMetricsTableSnafu)?;

        Ok(())
    }

    async fn tick(
        socket_addr: &SocketAddr,
        instance_name: String,
        datafusion: &Arc<DataFusion>,
        remote_schema: &Arc<Option<Arc<Schema>>>,
    ) -> Result<(), Error> {
        let body = reqwest::get(format!("http://{socket_addr}/metrics"))
            .await
            .context(FailedToQueryPrometheusMetricsSnafu)?
            .text()
            .await
            .context(FailedToQueryPrometheusMetricsSnafu)?;

        let lines = body.lines().map(|s| Ok(s.to_owned()));
        let scrape =
            prometheus_parse::Scrape::parse(lines).context(UnableToParsePrometheusMetricsSnafu)?;

        let sample_size = scrape.samples.len();

        let mut timestamps: Vec<i64> = Vec::with_capacity(sample_size);
        let mut instances: Vec<String> = Vec::with_capacity(sample_size);
        let mut names: Vec<String> = Vec::with_capacity(sample_size);
        let mut values: Vec<f64> = Vec::with_capacity(sample_size);
        let mut properties: Vec<Option<String>> = Vec::with_capacity(sample_size);

        for sample in scrape.samples {
            let value: f64 = match sample.value {
                prometheus_parse::Value::Counter(v)
                | prometheus_parse::Value::Gauge(v)
                | prometheus_parse::Value::Untyped(v) => v,
                prometheus_parse::Value::Histogram(v) => v.into_iter().map(|v| v.count).sum(),
                prometheus_parse::Value::Summary(v) => v.into_iter().map(|v| v.count).sum(),
            };

            let timestamp = sample.timestamp.with_timezone(&Utc);
            if let Some(timestamp_nano) = timestamp.timestamp_nanos_opt() {
                timestamps.push(timestamp_nano);
            } else {
                timestamps.push(timestamp.timestamp_micros() * 1000);
            }
            instances.push(instance_name.clone());
            names.push(sample.metric);
            values.push(value);

            properties.push(if sample.labels.is_empty() {
                None
            } else {
                serde_json::to_string(&*sample.labels).ok()
            });
        }

        let mut schema = get_metrics_schema();
        let mut record_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(
                    TimestampNanosecondArray::from(timestamps).with_timezone(Arc::from("UTC")),
                ),
                Arc::new(StringArray::from(instances)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(properties)),
            ],
        )
        .context(UnableToCreateRecordBatchSnafu)?;

        // If a remote schema is provided, cast the record batch to it
        if let Some(remote_schema) = remote_schema.as_ref() {
            schema = Arc::clone(remote_schema);
            record_batch = try_cast_to(record_batch.clone(), Arc::clone(remote_schema))
                .context(UnableToCastRecordBatchSnafu)?;
        }

        let data_update = DataUpdate {
            schema: Arc::clone(&schema),
            data: vec![record_batch],
            update_type: crate::dataupdate::UpdateType::Append,
        };

        datafusion
            .write_data(get_metrics_table_reference(), data_update)
            .await
            .context(UnableToWriteToMetricsTableSnafu)?;

        Ok(())
    }

    pub fn start(&self, instance_name: String, datafusion: &Arc<DataFusion>) {
        let addr = Arc::clone(&self.socket_addr);
        let df = Arc::clone(datafusion);
        let schema = Arc::clone(&self.remote_schema);

        spawn(async move {
            loop {
                if let Err(err) =
                    MetricsRecorder::tick(&addr, instance_name.clone(), &df, &schema).await
                {
                    tracing::error!("{err}");
                }
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
    }
}

#[must_use]
pub fn get_metrics_schema() -> Arc<Schema> {
    let fields = vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(
                arrow::datatypes::TimeUnit::Nanosecond,
                Some(Arc::from("UTC")),
            ),
            false,
        ),
        Field::new("instance", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("properties", DataType::Utf8, true),
    ];

    Arc::new(Schema::new(fields))
}

#[must_use]
pub fn get_metrics_table_reference() -> TableReference {
    TableReference::partial(SPICE_RUNTIME_SCHEMA, "metrics")
}
