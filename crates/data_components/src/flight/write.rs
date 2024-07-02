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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_plan::{
        insert::{DataSink, DataSinkExec},
        metrics::MetricsSet,
        DisplayAs, DisplayFormatType, ExecutionPlan,
    },
    sql::TableReference,
};
use flight_client::FlightClient;
use futures::StreamExt;
use snafu::prelude::*;
use std::{any::Any, fmt, sync::Arc};

use crate::util::transient_error::detect_transient_data_retrieval_error;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to publish data to Flight endpoint: {source}"))]
    UnableToPublishData { source: flight_client::Error },
}

pub struct FlightTableWriter {
    read_provider: Arc<dyn TableProvider>,
    table_reference: TableReference,
    flight_client: FlightClient,
}

impl FlightTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        table_reference: TableReference,
        flight_client: FlightClient,
    ) -> Arc<dyn TableProvider> {
        Arc::new(Self {
            read_provider,
            table_reference,
            flight_client,
        }) as _
    }
}

#[async_trait]
impl TableProvider for FlightTableWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read_provider.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .scan(state, projection, filters, limit)
            .await
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(FlightDataSink::new(
                self.flight_client.clone(),
                self.table_reference.clone(),
                overwrite,
            )),
            self.schema(),
            None,
        )) as _)
    }
}

#[derive(Clone)]
struct FlightDataSink {
    flight_client: FlightClient,
    table_reference: TableReference,
    _overwrite: bool,
}

#[async_trait]
impl DataSink for FlightDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let mut num_rows = 0;
        let mut flight_client = self.flight_client.clone();

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(detect_transient_data_retrieval_error)?;
            num_rows += batch.num_rows() as u64;

            flight_client
                .publish(&format!("{}", self.table_reference), vec![batch])
                .await
                .context(UnableToPublishDataSnafu)
                .map_err(to_external_error)?;
        }

        Ok(num_rows)
    }
}

impl FlightDataSink {
    fn new(flight_client: FlightClient, table_reference: TableReference, overwrite: bool) -> Self {
        Self {
            flight_client,
            table_reference,
            _overwrite: overwrite,
        }
    }
}

impl std::fmt::Debug for FlightDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FlightDataSink")
    }
}

impl DisplayAs for FlightDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "FlightDataSink")
    }
}

fn to_external_error(e: Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
