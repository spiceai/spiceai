use crate::mysql::MySQL;
use crate::util::on_conflict::OnConflict;
use crate::util::retriable_error::check_and_mark_retriable_error;
use crate::util::{constraints, to_datafusion_error};
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{dml::InsertOp, Expr},
    physical_plan::{metrics::MetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan},
};
use futures::StreamExt;
use mysql_async::TxOpts;
use snafu::ResultExt;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MySQLTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
    mysql: Arc<MySQL>,
    on_conflict: Option<OnConflict>,
}

impl MySQLTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        mysql: MySQL,
        on_conflict: Option<OnConflict>,
    ) -> Arc<Self> {
        Arc::new(Self {
            read_provider,
            mysql: Arc::new(mysql),
            on_conflict,
        })
    }

    pub fn mysql(&self) -> Arc<MySQL> {
        Arc::clone(&self.mysql)
    }
}

#[async_trait]
impl TableProvider for MySQLTableWriter {
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
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .scan(state, projection, filters, limit)
            .await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(MySQLDataSink::new(
                Arc::clone(&self.mysql),
                op == InsertOp::Overwrite,
                self.on_conflict.clone(),
                self.schema(),
            )),
            None,
        )))
    }
}

pub struct MySQLDataSink {
    pub mysql: Arc<MySQL>,
    pub overwrite: bool,
    pub on_conflict: Option<OnConflict>,
    schema: SchemaRef,
}

#[async_trait]
impl DataSink for MySQLDataSink {
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
        let mut num_rows = 0u64;

        let mut db_conn = self.mysql.connect().await.map_err(to_datafusion_error)?;
        let mysql_conn = MySQL::mysql_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let mut conn_guard = mysql_conn.conn.lock().await;
        let mut tx = conn_guard
            .start_transaction(TxOpts::default())
            .await
            .context(super::UnableToBeginTransactionSnafu)
            .map_err(to_datafusion_error)?;

        if self.overwrite {
            self.mysql
                .delete_all_table_data(&mut tx)
                .await
                .map_err(to_datafusion_error)?;
        }

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;
            let batch_num_rows = batch.num_rows();

            if batch_num_rows == 0 {
                continue;
            }

            num_rows += batch_num_rows as u64;

            constraints::validate_batch_with_constraints(
                vec![batch.clone()],
                self.mysql.constraints(),
                &crate::util::constraints::UpsertOptions::default(),
            )
            .await
            .context(super::ConstraintViolationSnafu)
            .map_err(to_datafusion_error)?;

            self.mysql
                .insert_batch(&mut tx, batch, self.on_conflict.clone())
                .await
                .map_err(to_datafusion_error)?;
        }

        tx.commit()
            .await
            .context(super::UnableToCommitMySQLTransactionSnafu)
            .map_err(to_datafusion_error)?;

        drop(conn_guard);

        Ok(num_rows)
    }
}

impl MySQLDataSink {
    pub fn new(
        mysql: Arc<MySQL>,
        overwrite: bool,
        on_conflict: Option<OnConflict>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            mysql,
            overwrite,
            on_conflict,
            schema,
        }
    }
}

impl fmt::Debug for MySQLDataSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MySQLDataSink")
    }
}

impl DisplayAs for MySQLDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MySQLDataSink")
    }
}
