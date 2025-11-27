// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::adbc::ADBC;
use crate::sql::db_connection_pool::adbcpool::ADBCPool;
use crate::util::retriable_error::{check_and_mark_retriable_error, to_retriable_data_write_error};
use adbc_core::options::{IngestMode, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Optionable, Statement};
use arrow::array::{RecordBatch, RecordBatchReader};
use arrow_schema::ArrowError;
use async_trait::async_trait;
use datafusion::common::not_impl_err;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::Session,
    datasource::{
        sink::{DataSink, DataSinkExec},
        TableProvider,
    },
    execution::TaskContext,
    logical_expr::{dml::InsertOp, Expr, TableType},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream},
    sql::TableReference,
};
use futures::StreamExt;
use snafu::ResultExt;
use std::{any::Any, fmt, sync::Arc};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

#[derive(Default)]
pub struct ADBCTableWriterBuilder<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    read_provider: Option<Arc<dyn TableProvider>>,
    pool: Option<Arc<ADBCPool<D>>>,
    table_reference: Option<TableReference>,
}

impl<D> ADBCTableWriterBuilder<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    #[must_use]
    pub fn new() -> Self {
        Self {
            read_provider: None,
            pool: None,
            table_reference: None,
        }
    }

    #[must_use]
    pub fn with_read_provider(mut self, read_provider: Arc<dyn TableProvider>) -> Self {
        self.read_provider = Some(read_provider);
        self
    }

    #[must_use]
    pub fn with_pool(mut self, pool: Arc<ADBCPool<D>>) -> Self {
        self.pool = Some(pool);
        self
    }

    #[must_use]
    pub fn with_table_reference(mut self, table_reference: TableReference) -> Self {
        self.table_reference = Some(table_reference);
        self
    }

    pub fn build(self) -> super::Result<ADBCTableWriter<D>> {
        let Some(read_provider) = self.read_provider else {
            return Err(super::Error::MissingReadProvider);
        };

        let Some(pool) = self.pool else {
            return Err(super::Error::MissingPool);
        };

        let Some(table_reference) = self.table_reference else {
            return Err(super::Error::MissingTableReference);
        };

        Ok(ADBCTableWriter {
            read_provider,
            pool,
            table_reference,
        })
    }
}

#[derive(Clone)]
pub struct ADBCTableWriter<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    pub read_provider: Arc<dyn TableProvider>,
    pool: Arc<ADBCPool<D>>,
    table_reference: TableReference,
}

impl<D> std::fmt::Debug for ADBCTableWriter<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ADBCTableWriter")
            .field("table_reference", &self.table_reference)
            .finish()
    }
}

#[async_trait]
impl<D> TableProvider for ADBCTableWriter<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
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
        state: &dyn Session,
        project: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .scan(state, project, filters, limit)
            .await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(AdbcDataSink::new(
                Arc::clone(&self.pool),
                self.table_reference.clone(),
                op,
                self.schema(),
            )),
            None,
        )))
    }
}

#[derive(Clone)]
pub(crate) struct AdbcDataSink<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    pool: Arc<ADBCPool<D>>,
    table_reference: TableReference,
    overwrite: InsertOp,
    schema: SchemaRef,
}

impl<D> std::fmt::Debug for AdbcDataSink<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("AdbcDataSink")
            .field("table_reference", &self.table_reference)
            .field("overwrite", &self.overwrite)
            .field("schema", &self.schema)
            .finish()
    }
}

impl<D> DisplayAs for AdbcDataSink<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "AdbcDataSink table_reference={}, overwrite={:?}, schema={:?}",
            self.table_reference, self.overwrite, self.schema
        )
    }
}

#[async_trait]
impl<D> DataSink for AdbcDataSink<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let pool = Arc::clone(&self.pool);
        let table_ref = self.table_reference.clone();
        let overwrite = self.overwrite;

        let (batch_tx, batch_rx): (Sender<RecordBatch>, Receiver<RecordBatch>) = mpsc::channel(100);

        let schema = data.schema();

        let adbc_write_handle: JoinHandle<datafusion::common::Result<u64>> =
            tokio::task::spawn_blocking(move || {
                bulk_insert(pool, &table_ref, batch_rx, schema, overwrite)
            });

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;

            if let Err(send_error) = batch_tx.send(batch).await {
                match adbc_write_handle.await {
                    Err(join_error) => {
                        return Err(DataFusionError::Execution(format!(
                            "ADBC write task join error: {}, original send error: {}",
                            join_error, send_error
                        )));
                    }
                    Ok(Err(datafusion_error)) => {
                        return Err(datafusion_error);
                    }
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "Unable to send RecordBatch to ADBC writer: {send_error}"
                        )));
                    }
                };
            }
        }

        drop(batch_tx);

        match adbc_write_handle.await {
            Ok(result) => result,
            Err(join_error) => Err(DataFusionError::Execution(format!(
                "Error writing using ADBC: {join_error}"
            ))),
        }
    }
}

impl<D> AdbcDataSink<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    pub(crate) fn new(
        pool: Arc<ADBCPool<D>>,
        table_reference: TableReference,
        overwrite: InsertOp,
        schema: SchemaRef,
    ) -> Self {
        Self {
            pool,
            table_reference,
            overwrite,
            schema,
        }
    }
}

fn bulk_insert<D>(
    pool: Arc<ADBCPool<D>>,
    table_ref: &TableReference,
    batch_rx: Receiver<RecordBatch>,
    schema: SchemaRef,
    overwrite: InsertOp,
) -> datafusion::common::Result<u64>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    let mut db_conn = pool
        .connect_sync()
        .context(super::DbConnectionPoolSnafu)
        .map_err(to_retriable_data_write_error)?;

    let adbc_conn = ADBC::adbc_conn(&mut db_conn).map_err(to_retriable_data_write_error)?;

    let conn_mx = adbc_conn.conn.lock().unwrap();
    let mut conn = conn_mx.borrow_mut();
    let mut stmt = conn
        .new_statement()
        .map_err(to_retriable_data_write_error)?;

    stmt.set_option(
        OptionStatement::TargetTable,
        OptionValue::String(table_ref.to_string()),
    )
    .map_err(to_retriable_data_write_error)?;

    match overwrite {
        InsertOp::Append => stmt
            .set_option(OptionStatement::IngestMode, IngestMode::CreateAppend.into())
            .map_err(to_retriable_data_write_error)?,
        InsertOp::Overwrite => stmt
            .set_option(OptionStatement::IngestMode, IngestMode::Replace.into())
            .map_err(to_retriable_data_write_error)?,
        InsertOp::Replace => not_impl_err!("upsert is not implemented for ADBC data sink")
            .map_err(to_retriable_data_write_error)?,
    }

    stmt.bind_stream(Box::new(RecordBatchReaderFromStream::new(batch_rx, schema)))
        .map_err(to_retriable_data_write_error)?;

    let count = stmt
        .execute_update()
        .map_err(to_retriable_data_write_error)?;
    Ok(count.unwrap_or(-1) as u64)
}

struct RecordBatchReaderFromStream {
    stream: Receiver<RecordBatch>,
    schema: SchemaRef,
}

impl RecordBatchReaderFromStream {
    fn new(stream: Receiver<RecordBatch>, schema: SchemaRef) -> Self {
        Self { stream, schema }
    }
}

impl Iterator for RecordBatchReaderFromStream {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.stream.blocking_recv().map(Ok)
    }
}

impl RecordBatchReader for RecordBatchReaderFromStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
