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

use std::{any::Any, fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_plan::{
        insert::{DataSink, DataSinkExec},
        metrics::MetricsSet,
        DisplayAs, DisplayFormatType, ExecutionPlan,
    },
};
use deltalake::{protocol::SaveMode, DeltaOps, DeltaTable, DeltaTableError};
use futures::StreamExt;
use snafu::prelude::*;

use crate::util::transient_error::detect_transient_data_retrieval_error;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get Delta Lake table state"))]
    FailedToGetDeltaLakeState {},

    #[snafu(display("Failed to get the schema for the Delta Lake table"))]
    FailedToGetDeltaLakeSchema { source: DeltaTableError },

    #[snafu(display("The provided TableProvider is not a DeltaTable"))]
    NotADeltaTable {},
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DeltaTableWriter {
    read_provider: Arc<dyn TableProvider>,
    delta_table: DeltaTable,
}

impl DeltaTableWriter {
    #[allow(clippy::needless_pass_by_value)]
    pub fn create(
        delta_table_read_provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>> {
        let delta_table = delta_table_read_provider
            .as_any()
            .downcast_ref::<DeltaTable>()
            .ok_or(Error::NotADeltaTable {})?;

        Ok(Arc::new(Self {
            read_provider: Arc::clone(&delta_table_read_provider),
            delta_table: delta_table.clone(),
        }) as _)
    }
}

#[async_trait]
impl TableProvider for DeltaTableWriter {
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
            Arc::new(DeltaTableDataSink::new(self.delta_table.clone(), overwrite)),
            self.schema(),
            None,
        )) as _)
    }
}

#[derive(Clone)]
struct DeltaTableDataSink {
    delta_table: DeltaTable,
    save_mode: SaveMode,
}

#[async_trait]
impl DataSink for DeltaTableDataSink {
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
        while let Some(batch) = data.next().await {
            let batch = batch.map_err(detect_transient_data_retrieval_error)?;
            num_rows += batch.num_rows() as u64;
            let _ = DeltaOps(self.delta_table.clone())
                .write([batch])
                .with_save_mode(self.save_mode)
                .await?;
        }

        Ok(num_rows)
    }
}

impl DeltaTableDataSink {
    fn new(delta_table: DeltaTable, overwrite: bool) -> Self {
        Self {
            delta_table,
            save_mode: if overwrite {
                SaveMode::Overwrite
            } else {
                SaveMode::Append
            },
        }
    }
}

impl std::fmt::Debug for DeltaTableDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaTableDataSink save_mode={:?}", self.save_mode)
    }
}

impl DisplayAs for DeltaTableDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaTableDataSink save_mode={:?}", self.save_mode)
    }
}
