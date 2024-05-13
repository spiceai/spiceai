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

use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, StringArray, TimestampSecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::common::Constraints;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use snafu::prelude::*;
use tokio::spawn;
use tokio::sync::RwLock;

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

pub struct SpiceMetricsRecorder {
    socket_addr: Arc<SocketAddr>,
    table: Arc<MetricsTable>,
}

impl SpiceMetricsRecorder {
    pub fn table(&self) -> Arc<MetricsTable> {
        Arc::clone(&self.table)
    }
}

impl SpiceMetricsRecorder {
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
                if let Err(err) = SpiceMetricsRecorder::tick(&addr, &table).await {
                    tracing::debug!("{err}");
                }
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
    }
}

pub type PartitionData = Arc<RwLock<Vec<RecordBatch>>>;

pub struct MetricsTable {
    pub schema: SchemaRef,
    pub batches: RwLock<Vec<PartitionData>>,
    constraints: Constraints,
}

impl Default for MetricsTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsTable {
    pub fn new() -> Self {
        let fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new("metric", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("labels", DataType::Utf8, false),
        ];

        let schema = Arc::new(Schema::new(fields));

        Self {
            schema,
            batches: RwLock::new(vec![]),
            constraints: Constraints::empty(),
        }
    }

    pub async fn add_record_batch(&self, record_batch: RecordBatch) {
        let mut batches = self.batches.write().await;
        batches.push(Arc::new(RwLock::new(vec![record_batch])));
        drop(batches);
    }

    pub async fn add_record(
        &self,
        timestamp: i64,
        metric: &str,
        value: f64,
        label: &str,
    ) -> Result<(), Error> {
        let mut batches = self.batches.write().await;

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(TimestampSecondArray::from(vec![timestamp])),
                Arc::new(StringArray::from(vec![metric])),
                Arc::new(Float64Array::from(vec![value])),
                Arc::new(StringArray::from(vec![label])),
            ],
        )
        .context(UnableToCreateRecordBatchSnafu)?;

        batches.push(Arc::new(RwLock::new(vec![batch])));

        drop(batches);

        Ok(())
    }
}

#[async_trait]
impl TableProvider for MetricsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut partitions = vec![];
        let batches_guard = &self.batches.read().await;
        for arc_inner_vec in batches_guard.iter() {
            let inner_vec = arc_inner_vec.read().await;
            partitions.push(inner_vec.clone());
        }
        Ok(Arc::new(MemoryExec::try_new(
            &partitions,
            self.schema(),
            projection.cloned(),
        )?))
    }
}
