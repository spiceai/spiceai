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

use arrow::{
    array::{ArrayRef, RecordBatch, UInt64Array},
    datatypes::{DataType, Schema, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        insert::{DataSink, FileSinkExec},
        metrics::MetricsSet,
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
    },
};
use futures::StreamExt;
use snafu::prelude::*;
use sql_provider_datafusion::expr::to_sql;

use crate::DeleteTableProvider;

use super::{to_datafusion_error, Postgres};

pub struct PostgresTableWriter {
    read_provider: Arc<dyn TableProvider>,
    postgres: Arc<Postgres>,
}

impl PostgresTableWriter {
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        postgres: Postgres,
    ) -> Arc<dyn TableProvider> {
        Arc::new(Self {
            read_provider,
            postgres: Arc::new(postgres),
        }) as _
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
            Arc::new(PostgresDataSink::new(Arc::clone(&self.postgres), overwrite)),
            self.schema(),
            None,
        )) as _)
    }
}

#[async_trait]
impl DeleteTableProvider for PostgresTableWriter {
    async fn delete_from(
        &self,
        _state: &SessionState,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PostgresDeleteExec::new(
            self.postgres.clone(),
            &self.schema(),
            filters,
        )))
    }
}

struct PostgresDeleteExec {
    postgres: Arc<Postgres>,
    properties: PlanProperties,
    filters: Vec<Expr>,
}

impl PostgresDeleteExec {
    fn new(postgres: Arc<Postgres>, schema: &SchemaRef, filters: &[Expr]) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            postgres,
            properties,
            filters: filters.to_vec(),
        }
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for PostgresDeleteExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresDeleteExec").finish()
    }
}

impl DisplayAs for PostgresDeleteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PostgresDeleteExec")
            }
        }
    }
}

impl ExecutionPlan for PostgresDeleteExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let count_schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

        let _filters = self.filters.clone();

        let mut sqls = vec![];
        for sql in self.filters.clone().into_iter().map(|expr| to_sql(&expr)) {
            if let Ok(sql) = sql {
                sqls.push(sql);
            } else {
                return Err(DataFusionError::SQL(
                    datafusion::sql::sqlparser::parser::ParserError::ParserError(
                        "cant parse sql".to_string(),
                    ),
                    None,
                ));
            }
        }

        let postgres = self.postgres.clone();

        Ok(Box::pin(RecordBatchStreamAdapter::new(count_schema, {
            futures::stream::once(async move {
                let mut db_conn = postgres.connect().await.map_err(to_datafusion_error)?;
                let postgres_conn =
                    Postgres::postgres_conn(&mut db_conn).map_err(to_datafusion_error)?;

                let tx = postgres_conn
                    .conn
                    .transaction()
                    .await
                    .context(super::UnableToBeginTransactionSnafu)
                    .map_err(to_datafusion_error)?;

                let count = postgres
                    .delete_from(&tx, &sqls.join(" AND "))
                    .await
                    .map_err(to_datafusion_error)?;

                tx.commit()
                    .await
                    .context(super::UnableToCommitPostgresTransactionSnafu)
                    .map_err(to_datafusion_error)?;

                let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;

                if let Ok(batch) =
                    RecordBatch::try_from_iter_with_nullable(vec![("count", array, false)])
                {
                    Ok(batch)
                } else {
                    Err(DataFusionError::Execution(
                        "failed to create record batch".to_string(),
                    ))
                }
            })
        })))
    }
}

#[derive(Clone)]
struct PostgresDataSink {
    postgres: Arc<Postgres>,
    overwrite: bool,
}

#[async_trait]
impl DataSink for PostgresDataSink {
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

        let mut db_conn = self.postgres.connect().await.map_err(to_datafusion_error)?;
        let postgres_conn = Postgres::postgres_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let tx = postgres_conn
            .conn
            .transaction()
            .await
            .context(super::UnableToBeginTransactionSnafu)
            .map_err(to_datafusion_error)?;

        if self.overwrite {
            self.postgres
                .delete_all_table_data(&tx)
                .await
                .map_err(to_datafusion_error)?;
        }

        while let Some(batch) = data.next().await {
            let batch = batch?;
            num_rows += batch.num_rows() as u64;

            self.postgres
                .insert_batch(&tx, batch)
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
    fn new(postgres: Arc<Postgres>, overwrite: bool) -> Self {
        Self {
            postgres,
            overwrite,
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
