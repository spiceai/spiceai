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

use crate::duckdb::DuckDB;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_plan::{
        insert::{DataSink, FileSinkExec},
        metrics::MetricsSet,
        DisplayAs, DisplayFormatType, ExecutionPlan,
    },
};
use futures::StreamExt;

use super::to_datafusion_error;

pub struct DuckDBTableWriter {
    read_provider: Arc<dyn TableProvider>,
    duckdb: Arc<DuckDB>,
}

impl DuckDBTableWriter {
    pub fn create(read_provider: Arc<dyn TableProvider>, duckdb: DuckDB) -> Arc<dyn TableProvider> {
        Arc::new(Self {
            read_provider,
            duckdb: Arc::new(duckdb),
        }) as _
    }
}

#[async_trait]
impl TableProvider for DuckDBTableWriter {
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
        Ok(Arc::new(FileSinkExec::new(
            input,
            Arc::new(DuckDBDataSink::new(Arc::clone(&self.duckdb), overwrite)),
            self.schema(),
            None,
        )) as _)
    }
}

#[derive(Clone)]
struct DuckDBDataSink {
    duckdb: Arc<DuckDB>,
    overwrite: bool,
}

#[async_trait]
impl DataSink for DuckDBDataSink {
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

        let mut db_conn = self.duckdb.connect().await.map_err(to_datafusion_error)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_datafusion_error)?;

        if self.overwrite {
            self.duckdb
                .delete_all_table_data(duckdb_conn, true)
                .map_err(to_datafusion_error)?;
        }

        while let Some(batch) = data.next().await {
            let batch = batch?;
            num_rows += batch.num_rows() as u64;

            self.duckdb
                .insert_batch(duckdb_conn, &batch)
                .map_err(to_datafusion_error)?;
        }

        Ok(num_rows)
    }
}

impl DuckDBDataSink {
    fn new(duckdb: Arc<DuckDB>, overwrite: bool) -> Self {
        Self { duckdb, overwrite }
    }
}

impl std::fmt::Debug for DuckDBDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBDataSink")
    }
}

impl DisplayAs for DuckDBDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBDataSink")
    }
}
