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
use async_trait::async_trait;
use bb8_postgres::{tokio_postgres::types::ToSql, PostgresConnectionManager};
use datafusion::{common::OwnedTableReference, datasource::TableProvider};
use db_connection_pool::DbConnectionPool;
use postgres_native_tls::MakeTlsConnector;
use snafu::prelude::*;
use sql_provider_datafusion::SqlTable;
use std::sync::Arc;

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

// #[async_trait]
// impl TableProviderFactory for PostgresTableFactory {
//     async fn create(
//         &self,
//         _state: &SessionState,
//         cmd: &CreateExternalTable,
//     ) -> DataFusionResult<Arc<dyn TableProvider>> {
//         let name = cmd.name.to_string();
//         let mut options = cmd.options.clone();
//         let schema: Schema = cmd.schema.as_ref().into();

//         let params = Arc::new(Some(options));

//         let pool: Arc<DuckDbConnectionPool> = Arc::new(
//             DuckDbConnectionPool::new(&name, &mode, &params)
//                 .context(DbConnectionPoolSnafu)
//                 .map_err(to_datafusion_error)?,
//         );

//         let duckdb = DuckDB::new(name.clone(), Arc::new(schema), Arc::clone(&pool));

//         let mut db_conn = duckdb.connect().await.map_err(to_datafusion_error)?;
//         let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_datafusion_error)?;
//         duckdb
//             .create_table(duckdb_conn, false)
//             .map_err(to_datafusion_error)?;

//         let dyn_pool: Arc<DynDuckDbConnectionPool> = pool;

//         let read_provider = Arc::new(
//             SqlTable::new(&dyn_pool, OwnedTableReference::bare(name.clone()))
//                 .await
//                 .context(DuckDBDataFusionSnafu)
//                 .map_err(to_datafusion_error)?,
//         );

//         let read_write_provider: Arc<dyn TableProvider> =
//             DuckDBTableWriter::create(read_provider, duckdb);

//         Ok(read_write_provider)
//     }
// }

// fn to_datafusion_error(error: Error) -> DataFusionError {
//     DataFusionError::External(Box::new(error))
// }

// #[derive(Clone)]
// pub struct Postgres {
//     table_name: String,
//     schema: SchemaRef,
//     pool: Arc<PostgresConnectionPool>,
// }

// impl Postgres {
//     #[must_use]
//     pub fn new(table_name: String, schema: SchemaRef, pool: Arc<PostgresConnectionPool>) -> Self {
//         Self {
//             table_name,
//             schema,
//             pool,
//         }
//     }

//     // async fn connect(
//     //     &self,
//     // ) -> Result<
//     //     Box<
//     //         dyn DbConnection<
//     //             bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
//     //             &'static dyn ToSql,
//     //         >,
//     //     >,
//     // > {
//     //     self.pool.connect().await.context(DbConnectionSnafu)
//     // }
// }

// #[async_trait]
// impl ReadWrite for DuckDBTableFactory {
//     async fn table_provider(
//         &self,
//         _table_reference: OwnedTableReference,
//     ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
//         todo!()
//     }
// }
