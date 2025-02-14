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

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::sql::TableReference;
use opentelemetry_sdk::metrics::MetricError;
use snafu::prelude::*;
use tokio::sync::RwLock;

use crate::accelerated_table::refresh::Refresh;
use crate::accelerated_table::Retention;
use crate::component::dataset::acceleration::Acceleration;
use crate::component::dataset::TimeFormat;
use crate::datafusion::Error as DataFusionError;
use crate::datafusion::{DataFusion, SPICE_RUNTIME_SCHEMA};
use crate::dataupdate::{DataUpdate, UpdateType};
use crate::internal_table::{create_internal_accelerated_table, Error as InternalTableError};
use crate::secrets::Secrets;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error creating metrics table: {source}"))]
    UnableToCreateMetricsTable { source: InternalTableError },

    #[snafu(display("Error registering metrics table: {source}"))]
    UnableToRegisterToMetricsTable { source: DataFusionError },
}

pub struct SpiceMetricsExporter {
    datafusion: Arc<DataFusion>,
}

impl SpiceMetricsExporter {
    pub fn new(datafusion: Arc<DataFusion>) -> Self {
        SpiceMetricsExporter { datafusion }
    }
}

#[async_trait]
impl otel_arrow::ArrowExporter for SpiceMetricsExporter {
    async fn export(&self, metrics: RecordBatch) -> Result<(), MetricError> {
        let data_update = DataUpdate {
            schema: metrics.schema(),
            data: vec![metrics],
            update_type: UpdateType::Append,
        };

        self.datafusion
            .write_data(&get_metrics_table_reference(), data_update)
            .await
            .map_err(|e| MetricError::Other(e.to_string()))
    }

    async fn force_flush(&self) -> Result<(), MetricError> {
        Ok(())
    }

    fn shutdown(&self) -> Result<(), MetricError> {
        Ok(())
    }
}

pub async fn register_metrics_table(datafusion: &Arc<DataFusion>) -> Result<(), Error> {
    let metrics_table_reference = get_metrics_table_reference();

    let retention = Retention::new(
        Some("time_unix_nano".to_string()),
        Some(TimeFormat::Timestamptz),
        None,
        None,
        Some(Duration::from_secs(1800)), // delete metrics older then 30 minutes
        Some(Duration::from_secs(300)),  // run retention every 5 minutes
        true,
    );

    let table = create_internal_accelerated_table(
        datafusion.runtime_status(),
        metrics_table_reference.clone(),
        otel_arrow::schema(),
        None,
        Acceleration::default(),
        Refresh::default(),
        retention,
        Arc::new(RwLock::new(Secrets::default())),
    )
    .await
    .context(UnableToCreateMetricsTableSnafu)?;

    datafusion
        .register_table_as_writable_and_with_schema(metrics_table_reference, table)
        .context(UnableToRegisterToMetricsTableSnafu)?;

    Ok(())
}

#[must_use]
pub fn get_metrics_table_reference() -> TableReference {
    TableReference::partial(SPICE_RUNTIME_SCHEMA, "metrics")
}
