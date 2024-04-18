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

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_plan::{
        insert::{DataSink, FileSinkExec},
        metrics::MetricsSet,
        DisplayAs, DisplayFormatType, ExecutionPlan,
    },
};
use futures::StreamExt;
use snafu::prelude::*;

use crate::delete::{DeletionExec, DeletionSink, DeletionTableProvider};

use super::{to_datafusion_error, Sqlite};

pub struct SqliteTableWriter {
    read_provider: Arc<dyn TableProvider>,
    sqlite: Arc<Sqlite>,
}

impl SqliteTableWriter {
    pub fn create(read_provider: Arc<dyn TableProvider>, sqlite: Sqlite) -> Arc<dyn TableProvider> {
        Arc::new(Self {
            read_provider,
            sqlite: Arc::new(sqlite),
        }) as _
    }
}

#[async_trait]
impl TableProvider for SqliteTableWriter {
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
            Arc::new(SqliteDataSink::new(Arc::clone(&self.sqlite), overwrite)),
            self.schema(),
            None,
        )) as _)
    }
}

#[derive(Clone)]
struct SqliteDataSink {
    sqlite: Arc<Sqlite>,
    overwrite: bool,
}

#[async_trait]
impl DataSink for SqliteDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let mut num_rows: u64 = 0;

        let mut db_conn = self.sqlite.connect().await.map_err(to_datafusion_error)?;
        let sqlite_conn = Sqlite::sqlite_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let data_batches_result = data
            .collect::<Vec<datafusion::common::Result<RecordBatch>>>()
            .await;

        let data_batches: Vec<RecordBatch> = data_batches_result
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        for data_batch in &data_batches {
            num_rows += u64::try_from(data_batch.num_rows()).map_err(|e| {
                DataFusionError::Execution(format!("Unable to convert num_rows() to u64: {e}"))
            })?;
        }

        let overwrite = self.overwrite;
        let sqlite = Arc::clone(&self.sqlite);
        sqlite_conn
            .conn
            .call(move |conn| {
                let transaction = conn.transaction()?;

                if overwrite {
                    sqlite.delete_all_table_data(&transaction)?;
                }

                for batch in data_batches {
                    sqlite.insert_batch(&transaction, batch)?;
                }

                transaction.commit()?;

                Ok(())
            })
            .await
            .context(super::UnableToInsertIntoTableAsyncSnafu)
            .map_err(to_datafusion_error)?;

        Ok(num_rows)
    }
}

impl SqliteDataSink {
    fn new(sqlite: Arc<Sqlite>, overwrite: bool) -> Self {
        Self { sqlite, overwrite }
    }
}

impl std::fmt::Debug for SqliteDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SqliteDataSink")
    }
}

impl DisplayAs for SqliteDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "SqliteDataSink")
    }
}

#[async_trait]
impl DeletionTableProvider for SqliteTableWriter {
    async fn delete_from(
        &self,
        _state: &SessionState,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(
            Arc::new(SqliteDeletionSink::new(self.sqlite.clone(), filters)),
            &self.schema(),
        )))
    }
}

struct SqliteDeletionSink {
    sqlite: Arc<Sqlite>,
    filters: Vec<Expr>,
}

impl SqliteDeletionSink {
    fn new(sqlite: Arc<Sqlite>, filters: &[Expr]) -> Self {
        Self {
            sqlite,
            filters: filters.to_vec(),
        }
    }
}

#[async_trait]
impl DeletionSink for SqliteDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut db_conn = self.sqlite.connect().await?;
        let sqlite_conn = Sqlite::sqlite_conn(&mut db_conn)?;
        let sqlite = Arc::clone(&self.sqlite);
        let sql = crate::util::filters_to_sql(&self.filters)?;
        let count: u64 = sqlite_conn
            .conn
            .call(move |conn| {
                let tx = conn.transaction()?;

                let count = sqlite.delete_from(&tx, &sql)?;

                tx.commit()?;

                Ok(count)
            })
            .await?;

        Ok(count)
    }
}
