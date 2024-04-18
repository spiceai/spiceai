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
        insert::{DataSink, FileSinkExec},
        metrics::MetricsSet,
        DisplayAs, DisplayFormatType, ExecutionPlan,
    },
};
use futures::StreamExt;
use snafu::prelude::*;
use sql_provider_datafusion::expr::{self};

use crate::{delete::DeleteExec, delete::DeletionSink, DeleteTableProvider};

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
        Ok(Arc::new(DeleteExec::new(
            Arc::new(PostgresDeletionSink::new(self.postgres.clone(), filters)),
            &self.schema(),
        )))
    }
}

struct PostgresDeletionSink {
    postgres: Arc<Postgres>,
    filters: Vec<Expr>,
}

impl PostgresDeletionSink {
    fn new(postgres: Arc<Postgres>, filters: &[Expr]) -> Self {
        Self {
            postgres,
            filters: filters.to_vec(),
        }
    }
}

#[async_trait]
impl DeletionSink for PostgresDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let sqls = self
            .filters
            .iter()
            .map(expr::to_sql)
            .collect::<expr::Result<Vec<_>>>()
            .context(super::UnableToGenerateSQLSnafu)?;

        let mut db_conn = self.postgres.connect().await?;
        let postgres_conn = Postgres::postgres_conn(&mut db_conn)?;
        let tx = postgres_conn
            .conn
            .transaction()
            .await
            .context(super::UnableToBeginTransactionSnafu)?;
        let count = self.postgres.delete_from(&tx, &sqls.join(" AND ")).await?;
        tx.commit()
            .await
            .context(super::UnableToCommitPostgresTransactionSnafu)?;

        Ok(count)
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
