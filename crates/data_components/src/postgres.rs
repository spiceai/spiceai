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

#![allow(clippy::module_name_repetitions)]
use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use bb8_postgres::{tokio_postgres::types::ToSql, PostgresConnectionManager};
use datafusion::{
    common::OwnedTableReference,
    datasource::{provider::TableProviderFactory, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::CreateExternalTable,
};
use db_connection_pool::{
    dbconnection::{postgresconn::PostgresConnection, DbConnection},
    postgrespool::PostgresConnectionPool,
    DbConnectionPool,
};
use postgres_native_tls::MakeTlsConnector;
use snafu::prelude::*;
use sql_provider_datafusion::SqlTable;
use std::sync::Arc;

use crate::Read;

// use self::write::PostgresTableWriter;

pub mod write;

pub type DynPostgresConnectionPool = dyn DbConnectionPool<
        bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
        &'static (dyn ToSql + Sync),
    > + Send
    + Sync;
pub type DynPostgresConnection = dyn DbConnection<
    bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
    &'static (dyn ToSql + Sync),
>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("Unable to create Postgres connection pool: {source}"))]
    UnableToCreatePostgresConnectionPool {
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
    pool: Arc<DynPostgresConnectionPool>,
}

impl PostgresTableFactory {
    #[must_use]
    pub fn new(pool: Arc<DynPostgresConnectionPool>) -> Self {
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

pub struct PostgresTableProviderFactory {}

#[async_trait]
impl TableProviderFactory for PostgresTableProviderFactory {
    async fn create(
        &self,
        _state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let name = cmd.name.to_string();
        let mut options = cmd.options.clone();
        let schema: Schema = cmd.schema.as_ref().into();

        let params = Arc::new(Some(options));

        let pool = Arc::new(
            PostgresConnectionPool::new(params, None)
                .await
                .context(UnableToCreatePostgresConnectionPoolSnafu)
                .map_err(to_datafusion_error)?,
        );

        let postgres = Postgres::new(name.clone(), Arc::new(schema), Arc::clone(&pool));

        let mut db_conn = postgres.connect().await.map_err(to_datafusion_error)?;
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
pub struct Postgres {
    table_name: String,
    schema: SchemaRef,
    pool: Arc<PostgresConnectionPool>,
}

impl Postgres {
    #[must_use]
    pub fn new(table_name: String, schema: SchemaRef, pool: Arc<PostgresConnectionPool>) -> Self {
        Self {
            table_name,
            schema,
            pool,
        }
    }

    async fn connect(&self) -> Result<Box<DynPostgresConnection>> {
        self.pool.connect().await.context(DbConnectionSnafu)
    }

    fn postgres_conn<'a>(
        db_connection: &'a mut Box<DynPostgresConnection>,
    ) -> Result<&'a mut PostgresConnection> {
        db_connection
            .as_any_mut()
            .downcast_mut::<PostgresConnection>()
            .ok_or_else(|| UnableToDowncastDbConnectionSnafu {}.build())
    }

    async fn table_exists(&mut self, postgres_conn: &PostgresConnection) -> bool {
        let sql = format!(
            r#"SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = '{name}'
          )"#,
            name = self.table_name
        );
        tracing::trace!("{sql}");

        let Ok(row) = postgres_conn.conn.query_one(&sql, &[]).await else {
            return false;
        };

        row.get(0)
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
