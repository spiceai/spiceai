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

use crate::{
    adbc::write::{ADBCTableWriterBuilder, AdbcDataSink},
    sql::db_connection_pool::{
        self,
        adbcpool::ADBCPool,
        dbconnection::{adbcconn::AdbcDbConnection, get_schema, DbConnection},
        DbConnectionPool,
    },
};

use adbc_core::{Connection, Database};
use arrow::array::RecordBatch;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion::{
    catalog::Session,
    datasource::{sink::DataSinkExec, TableProvider},
    logical_expr::dml::InsertOp,
    physical_plan::ExecutionPlan,
    sql::TableReference,
};
use r2d2_adbc::AdbcConnectionManager;
use snafu::prelude::*;
use std::sync::Arc;

use self::sql_table::AdbcDBTable;

mod sql_table;
mod write;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("Unable to downcast DbConnection to AdbcDbConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError { source: db_connection_pool::Error },

    #[snafu(display("A read provider is required to create an ADBCTableWriter"))]
    MissingReadProvider,

    #[snafu(display("A pool is required to create an ADBCTableWriter"))]
    MissingPool,

    #[snafu(display("A table reference is required to create an ADBCTableWriter"))]
    MissingTableReference,
}

type Result<T, E = Error> = std::result::Result<T, E>;
type DynAdbcConnectionPool<D> = dyn DbConnectionPool<r2d2::PooledConnection<AdbcConnectionManager<D>>, RecordBatch>
    + Send
    + Sync;

pub struct AdbcTableFactory<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    pool: Arc<ADBCPool<D>>,
}

impl<D> AdbcTableFactory<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    #[must_use]
    pub fn new(pool: Arc<ADBCPool<D>>) -> Self {
        Self { pool }
    }

    pub async fn create_from(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        table_reference: TableReference,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(AdbcDataSink::new(
                Arc::clone(&self.pool),
                table_reference,
                InsertOp::Append,
                schema,
            )),
            None,
        )))
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
        dialect: Option<Arc<dyn Dialect + Send + Sync>>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let conn = Arc::clone(&pool).connect().await?;
        let schema = get_schema(conn, &table_reference).await?;
        let dyn_pool: Arc<DynAdbcConnectionPool<D>> = pool;

        let table_provider = Arc::new(AdbcDBTable::new_with_schema(
            &dyn_pool,
            Arc::clone(&schema),
            table_reference.clone(),
            dialect,
        ));

        Ok(table_provider)
    }

    pub async fn read_write_table_provider(
        &self,
        table_reference: TableReference,
        dialect: Option<Arc<dyn Dialect + Send + Sync>>,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let read_provider = Self::table_provider(self, table_reference.clone(), dialect).await?;

        let table_writer_builder = ADBCTableWriterBuilder::new()
            .with_pool(Arc::clone(&self.pool))
            .with_table_reference(table_reference)
            .with_read_provider(Arc::clone(&read_provider));

        Ok(Arc::new(table_writer_builder.build()?))
    }
}

pub struct ADBC<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    table_name: String,
    pool: Arc<DynAdbcConnectionPool<D>>,
}

impl<D> ADBC<D>
where
    D: Database + Send + 'static,
    D::ConnectionType: Connection + Send + Sync,
{
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn pool(&self) -> Arc<DynAdbcConnectionPool<D>> {
        Arc::clone(&self.pool)
    }

    pub fn adbc_conn(
        db_connection: &mut Box<
            dyn DbConnection<r2d2::PooledConnection<AdbcConnectionManager<D>>, RecordBatch>,
        >,
    ) -> Result<&mut AdbcDbConnection<D>> {
        db_connection
            .as_any_mut()
            .downcast_mut::<AdbcDbConnection<D>>()
            .context(UnableToDowncastDbConnectionSnafu)
    }
}
