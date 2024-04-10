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

use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
};
use async_trait::async_trait;
use bb8_postgres::PostgresConnectionManager;
use datafusion::{
    common::OwnedTableReference,
    datasource::{provider::TableProviderFactory, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::CreateExternalTable,
};
use db_connection_pool::{
    dbconnection::{duckdbconn::DuckDbConnection, DbConnection},
    duckdbpool::DuckDbConnectionPool,
    DbConnectionPool,
};
use duckdb::{vtab::arrow::arrow_recordbatch_to_query_params, DuckdbConnectionManager, ToSql};
use postgres_native_tls::MakeTlsConnector;
use snafu::prelude::*;
use sql_provider_datafusion::SqlTable;
use std::{cmp, sync::Arc};

use crate::Read;

// use self::write::PostgresTableWriter;

pub mod write;

pub type PostgresConnectionPool = dyn DbConnectionPool<
        bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
        &'static (dyn ToSql + Sync),
    > + Send
    + Sync;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError {
        source: db_connection_pool::Error,
    },

    #[snafu(display("Unable to downcast DbConnection to PostgresConnection"))]
    UnableToDowncastDbConnection {},

    UnableToDropDuckDBTable {
        source: duckdb::Error,
    },

    UnableToCreateDuckDBTable {
        source: duckdb::Error,
    },

    UnableToInsertToDuckDBTable {
        source: duckdb::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct PostgresTableFactory {
    pool: Arc<PostgresConnectionPool>,
}

impl PostgresTableFactory {
    #[must_use]
    pub fn new(pool: Arc<PostgresConnectionPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Read for PostgresTableFactory {
    async fn table_provider(
        &self,
        table_reference: OwnedTableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let table_provider = SqlTable::new(&pool, table_reference)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(Arc::new(table_provider))
    }
}

#[async_trait]
impl TableProviderFactory for PostgresTableFactory {
    async fn create(
        &self,
        _state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let name = cmd.name.to_string();
        let mut options = cmd.options.clone();
        let schema: Schema = cmd.schema.as_ref().into();

        let params = Arc::new(Some(options));

        let pool: Arc<DuckDbConnectionPool> = Arc::new(
            DuckDbConnectionPool::new(&name, &mode, &params)
                .context(DbConnectionPoolSnafu)
                .map_err(to_datafusion_error)?,
        );

        let duckdb = DuckDB::new(name.clone(), Arc::new(schema), Arc::clone(&pool));

        let mut db_conn = duckdb.connect().await.map_err(to_datafusion_error)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_datafusion_error)?;
        duckdb
            .create_table(duckdb_conn, false)
            .map_err(to_datafusion_error)?;

        let dyn_pool: Arc<DynDuckDbConnectionPool> = pool;

        let read_provider = Arc::new(
            SqlTable::new(&dyn_pool, OwnedTableReference::bare(name.clone()))
                .await
                .context(DuckDBDataFusionSnafu)
                .map_err(to_datafusion_error)?,
        );

        let read_write_provider: Arc<dyn TableProvider> =
            DuckDBTableWriter::create(read_provider, duckdb);

        Ok(read_write_provider)
    }
}

fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[derive(Clone)]
pub struct PostgresTable {
    table_name: String,
    schema: SchemaRef,
    pool: Arc<DuckDbConnectionPool>,
}

impl PostgresTable {
    #[must_use]
    pub fn new(table_name: String, schema: SchemaRef, pool: Arc<DuckDbConnectionPool>) -> Self {
        Self {
            table_name,
            schema,
            pool,
        }
    }

    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>>,
    > {
        self.pool.connect().await.context(DbConnectionSnafu)
    }

    fn duckdb_conn<'a>(
        db_connection: &'a mut Box<
            dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>,
        >,
    ) -> Result<&'a mut DuckDbConnection> {
        db_connection
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .ok_or_else(|| UnableToDowncastDbConnectionSnafu {}.build())
    }

    fn table_exists(&self, duckdb_conn: &mut DuckDbConnection) -> bool {
        let sql = format!(
            r#"SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables 
          WHERE table_name = '{name}'
        )"#,
            name = self.table_name
        );
        tracing::trace!("{sql}");

        duckdb_conn
            .conn
            .query_row(&sql, [], |row| row.get::<usize, bool>(0))
            .unwrap_or(false)
    }

    const MAX_BATCH_SIZE: usize = 2048;

    fn split_batch(batch: &RecordBatch) -> Vec<RecordBatch> {
        let mut result = vec![];
        (0..=batch.num_rows())
            .step_by(Self::MAX_BATCH_SIZE)
            .for_each(|offset| {
                let length = cmp::min(Self::MAX_BATCH_SIZE, batch.num_rows() - offset);
                result.push(batch.slice(offset, length));
            });
        result
    }

    fn insert_batch(&self, duckdb_conn: &mut DuckDbConnection, batch: &RecordBatch) -> Result<()> {
        let sql = format!(
            r#"INSERT INTO "{name}" SELECT * FROM arrow(?, ?)"#,
            name = self.table_name
        );
        tracing::trace!("{sql}");

        for sliced in Self::split_batch(batch) {
            let arrow_params = arrow_recordbatch_to_query_params(sliced);
            let arrow_params_vec: Vec<&dyn ToSql> = arrow_params
                .iter()
                .map(|p| p as &dyn ToSql)
                .collect::<Vec<_>>();
            let arrow_params_ref: &[&dyn ToSql] = &arrow_params_vec;
            duckdb_conn
                .conn
                .execute(&sql, arrow_params_ref)
                .context(UnableToInsertToDuckDBTableSnafu)?;
        }

        Ok(())
    }

    fn create_table(&self, duckdb_conn: &mut DuckDbConnection, drop_if_exists: bool) -> Result<()> {
        if self.table_exists(duckdb_conn) {
            if drop_if_exists {
                let sql = format!(r#"DROP TABLE "{}""#, self.table_name);
                tracing::trace!("{sql}");
                duckdb_conn
                    .conn
                    .execute(&sql, [])
                    .context(UnableToDropDuckDBTableSnafu)?;
            } else {
                return Ok(());
            }
        }

        let empty_record = RecordBatch::new_empty(Arc::clone(&self.schema));

        let arrow_params = arrow_recordbatch_to_query_params(empty_record);
        let arrow_params_vec: Vec<&dyn ToSql> = arrow_params
            .iter()
            .map(|p| p as &dyn ToSql)
            .collect::<Vec<_>>();
        let arrow_params_ref: &[&dyn ToSql] = &arrow_params_vec;
        let sql = format!(
            r#"CREATE TABLE "{name}" AS SELECT * FROM arrow(?, ?)"#,
            name = self.table_name
        );
        tracing::trace!("{sql}");

        duckdb_conn
            .conn
            .execute(&sql, arrow_params_ref)
            .context(UnableToCreateDuckDBTableSnafu)?;

        Ok(())
    }
}

// #[async_trait]
// impl ReadWrite for DuckDBTableFactory {
//     async fn table_provider(
//         &self,
//         _table_reference: OwnedTableReference,
//     ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
//         todo!()
//     }
// }
