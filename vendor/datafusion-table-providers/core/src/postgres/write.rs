use std::{any::Any, fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Constraints, SchemaExt},
    datasource::{
        sink::{DataSink, DataSinkExec},
        TableProvider, TableType,
    },
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{dml::InsertOp, Expr},
    physical_plan::{metrics::MetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan},
};
use futures::StreamExt;
use snafu::prelude::*;

use crate::util::{
    constraints::{self},
    on_conflict::OnConflict,
    retriable_error::check_and_mark_retriable_error,
};

use crate::postgres::Postgres;

use super::to_datafusion_error;

#[derive(Debug, Clone)]
pub struct PostgresTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
    postgres: Arc<Postgres>,
    on_conflict: Option<OnConflict>,
}

impl PostgresTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        postgres: Postgres,
        on_conflict: Option<OnConflict>,
    ) -> Arc<Self> {
        Arc::new(Self {
            read_provider,
            postgres: Arc::new(postgres),
            on_conflict,
        })
    }

    pub fn postgres(&self) -> Arc<Postgres> {
        Arc::clone(&self.postgres)
    }
}

#[async_trait]
impl TableProvider for PostgresTableWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read_provider.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(self.postgres.constraints())
    }

    async fn scan(
        &self,
        state: &dyn Session,
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
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(PostgresDataSink::new(
                Arc::clone(&self.postgres),
                op,
                self.on_conflict.clone(),
                self.schema(),
            )),
            None,
        )) as _)
    }
}

#[derive(Clone)]
struct PostgresDataSink {
    postgres: Arc<Postgres>,
    overwrite: InsertOp,
    on_conflict: Option<OnConflict>,
    schema: SchemaRef,
}

#[async_trait]
impl DataSink for PostgresDataSink {
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
        let mut num_rows = 0;

        let mut db_conn = self.postgres.connect().await.map_err(to_datafusion_error)?;
        let postgres_conn = Postgres::postgres_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let tx = postgres_conn
            .conn
            .transaction()
            .await
            .context(super::UnableToBeginTransactionSnafu)
            .map_err(to_datafusion_error)?;

        if matches!(self.overwrite, InsertOp::Overwrite) {
            self.postgres
                .delete_all_table_data(&tx)
                .await
                .map_err(to_datafusion_error)?;
        }

        let postgres_fields = self
            .postgres
            .schema
            .fields
            .iter()
            .map(|f| {
                Arc::new(Field::new(
                    f.name(),
                    if f.data_type() == &DataType::LargeUtf8 {
                        DataType::Utf8
                    } else {
                        f.data_type().clone()
                    },
                    f.is_nullable(),
                ))
            })
            .collect::<Vec<_>>();

        let postgres_schema = Arc::new(Schema::new(postgres_fields));

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;

            // for the purposes of PostgreSQL, LargeUtf8 is equivalent to Utf8
            // because Postgres physically cannot store anything larger than 1Gb in text (VARCHAR)
            // normalize LargeUtf8 fields to Utf8 for both the incoming batch, and Postgres if it happens to specify any
            let batch_fields = batch
                .schema_ref()
                .fields()
                .iter()
                .map(|f| {
                    Arc::new(Field::new(
                        f.name(),
                        if f.data_type() == &DataType::LargeUtf8 {
                            DataType::Utf8
                        } else {
                            f.data_type().clone()
                        },
                        f.is_nullable(),
                    ))
                })
                .collect::<Vec<_>>();
            let batch_schema = Arc::new(Schema::new(batch_fields));

            if !Arc::clone(&postgres_schema).equivalent_names_and_types(&batch_schema) {
                return Err(to_datafusion_error(super::Error::SchemaValidationError {
                    table_name: self.postgres.table.to_string(),
                }));
            }

            let batch_num_rows = batch.num_rows();

            if batch_num_rows == 0 {
                continue;
            };

            num_rows += batch_num_rows as u64;

            constraints::validate_batch_with_constraints(
                vec![batch.clone()],
                self.postgres.constraints(),
                &crate::util::constraints::UpsertOptions::default(),
            )
            .await
            .context(super::ConstraintViolationSnafu)
            .map_err(to_datafusion_error)?;

            self.postgres
                .insert_batch(&tx, batch, self.on_conflict.clone())
                .await
                .map_err(to_datafusion_error)?;
        }

        tx.commit()
            .await
            .context(super::UnableToCommitPostgresTransactionSnafu)
            .map_err(to_datafusion_error)?;

        Ok(num_rows)
    }
}

impl PostgresDataSink {
    fn new(
        postgres: Arc<Postgres>,
        overwrite: InsertOp,
        on_conflict: Option<OnConflict>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            postgres,
            overwrite,
            on_conflict,
            schema,
        }
    }
}

impl std::fmt::Debug for PostgresDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PostgresDataSink")
    }
}

impl DisplayAs for PostgresDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "PostgresDataSink")
    }
}
