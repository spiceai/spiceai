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
use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
};
use arrow_sql_gen::statement::{
    CreateTableBuilder, Error as SqlGenError, IndexBuilder, InsertBuilder,
};
use async_trait::async_trait;
use bb8_postgres::{
    tokio_postgres::{types::ToSql, Transaction},
    PostgresConnectionManager,
};
use datafusion::{
    common::Constraints,
    datasource::{provider::TableProviderFactory, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use db_connection_pool::{
    dbconnection::{postgresconn::PostgresConnection, DbConnection},
    postgrespool::{self, PostgresConnectionPool},
    DbConnectionPool,
};
use postgres_native_tls::MakeTlsConnector;
use snafu::prelude::*;
use sql_provider_datafusion::{
    expr::{self, Engine},
    SqlTable,
};
use std::{collections::HashMap, sync::Arc};

use crate::{
    delete::DeletionTableProviderAdapter,
    util::{
        constraints::{self, get_primary_keys_from_constraints},
        indexes::{self, IndexType},
    },
    Read, ReadWrite,
};

use self::write::PostgresTableWriter;

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
    UnableToCreatePostgresConnectionPool { source: postgrespool::Error },

    #[snafu(display("Unable to downcast DbConnection to PostgresConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("Unable to begin Postgres transaction: {source}"))]
    UnableToBeginTransaction {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to create the Postgres table: {source}"))]
    UnableToCreatePostgresTable {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to create an index for the Postgres table: {source}"))]
    UnableToCreateIndexForPostgresTable {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to commit the Postgres transaction: {source}"))]
    UnableToCommitPostgresTransaction {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display(
        "Unable to construct the DataFusion SQL Table Provider for Postgres: {source}"
    ))]
    UnableToConstructSqlTable {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL { source: expr::Error },

    #[snafu(display("Unable to delete all data from the Postgres table: {source}"))]
    UnableToDeleteAllTableData {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to delete data from the Postgres table: {source}"))]
    UnableToDeleteData {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to insert Arrow batch to Postgres table: {source}"))]
    UnableToInsertArrowBatch {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to create insertion statement for Postgres table: {source}"))]
    UnableToCreateInsertStatement { source: SqlGenError },

    #[snafu(display("The table '{table_name}' doesn't exist in the Postgres server"))]
    TableDoesntExist { table_name: String },

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation { source: constraints::Error },
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
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let dyn_pool: Arc<DynPostgresConnectionPool> = pool;
        let table_provider = Arc::new(
            SqlTable::new("postgres", &dyn_pool, table_reference, None)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}

#[async_trait]
impl ReadWrite for PostgresTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let read_provider = Read::table_provider(self, table_reference.clone()).await?;

        let table_name = table_reference.to_string();
        let postgres = Postgres::new(table_name, Arc::clone(&self.pool), Constraints::empty());

        Ok(PostgresTableWriter::create(read_provider, postgres))
    }
}

pub struct PostgresTableProviderFactory {}

impl PostgresTableProviderFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for PostgresTableProviderFactory {
    fn default() -> Self {
        Self::new()
    }
}

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

        let indexes_option_str = options.remove("indexes");
        let indexes = match indexes_option_str {
            Some(indexes_str) => indexes::indexes_from_option_string(&indexes_str),
            None => HashMap::new(),
        };

        let indexes: Vec<(Vec<&str>, IndexType)> = indexes
            .iter()
            .map(|(key, ty)| (indexes::index_columns(key), *ty))
            .collect();

        let params = Arc::new(options);

        let pool = Arc::new(
            PostgresConnectionPool::new(params, None)
                .await
                .context(UnableToCreatePostgresConnectionPoolSnafu)
                .map_err(to_datafusion_error)?,
        );

        let schema = Arc::new(schema);
        let postgres = Postgres::new(name.clone(), Arc::clone(&pool), cmd.constraints.clone());

        let mut db_conn = pool
            .connect()
            .await
            .context(DbConnectionSnafu)
            .map_err(to_datafusion_error)?;
        let postgres_conn = Postgres::postgres_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let tx = postgres_conn
            .conn
            .transaction()
            .await
            .context(UnableToBeginTransactionSnafu)
            .map_err(to_datafusion_error)?;

        let primary_keys = get_primary_keys_from_constraints(&cmd.constraints, &schema);

        postgres
            .create_table(Arc::clone(&schema), &tx, primary_keys)
            .await
            .map_err(to_datafusion_error)?;

        for index in indexes {
            postgres
                .create_index(&tx, index.0, index.1 == IndexType::Unique)
                .await
                .map_err(to_datafusion_error)?;
        }

        tx.commit()
            .await
            .context(UnableToCommitPostgresTransactionSnafu)
            .map_err(to_datafusion_error)?;

        let dyn_pool: Arc<DynPostgresConnectionPool> = pool;

        let read_provider = Arc::new(SqlTable::new_with_schema(
            "postgres",
            &dyn_pool,
            Arc::clone(&schema),
            TableReference::bare(name.clone()),
            Some(Engine::Postgres),
        ));

        let delete_adapter =
            DeletionTableProviderAdapter::new(PostgresTableWriter::create(read_provider, postgres));
        Ok(Arc::new(delete_adapter))
    }
}

fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[derive(Clone)]
pub struct Postgres {
    table_name: String,
    pool: Arc<PostgresConnectionPool>,
    constraints: Constraints,
}

impl Postgres {
    #[must_use]
    pub fn new(
        table_name: String,
        pool: Arc<PostgresConnectionPool>,
        constraints: Constraints,
    ) -> Self {
        Self {
            table_name,
            pool,
            constraints,
        }
    }

    #[must_use]
    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    async fn connect(&self) -> Result<Box<DynPostgresConnection>> {
        let mut conn = self.pool.connect().await.context(DbConnectionSnafu)?;

        let pg_conn = Self::postgres_conn(&mut conn)?;

        if !self.table_exists(pg_conn).await {
            TableDoesntExistSnafu {
                table_name: self.table_name.clone(),
            }
            .fail()?;
        }

        Ok(conn)
    }

    fn postgres_conn(
        db_connection: &mut Box<DynPostgresConnection>,
    ) -> Result<&mut PostgresConnection> {
        db_connection
            .as_any_mut()
            .downcast_mut::<PostgresConnection>()
            .context(UnableToDowncastDbConnectionSnafu)
    }

    async fn table_exists(&self, postgres_conn: &PostgresConnection) -> bool {
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

    async fn insert_batch(&self, transaction: &Transaction<'_>, batch: RecordBatch) -> Result<()> {
        let insert_table_builder = InsertBuilder::new(&self.table_name, vec![batch]);
        let sql = insert_table_builder
            .build_postgres()
            .context(UnableToCreateInsertStatementSnafu)?;

        transaction
            .execute(&sql, &[])
            .await
            .context(UnableToInsertArrowBatchSnafu)?;

        Ok(())
    }

    async fn delete_all_table_data(&self, transaction: &Transaction<'_>) -> Result<()> {
        transaction
            .execute(
                format!(r#"DELETE FROM "{}""#, self.table_name).as_str(),
                &[],
            )
            .await
            .context(UnableToDeleteAllTableDataSnafu)?;

        Ok(())
    }

    #[allow(clippy::cast_sign_loss)]
    async fn delete_from(&self, transaction: &Transaction<'_>, where_clause: &str) -> Result<u64> {
        let row = transaction
            .query_one(
                format!(
                    r#"WITH deleted AS (DELETE FROM "{}" WHERE {} RETURNING *) SELECT COUNT(*) FROM deleted"#,
                    self.table_name, where_clause
                )
                .as_str(),
                &[],
            )
            .await
            .context(UnableToDeleteDataSnafu)?;

        let deleted: i64 = row.get(0);

        Ok(deleted as u64)
    }

    async fn create_table(
        &self,
        schema: SchemaRef,
        transaction: &Transaction<'_>,
        primary_keys: Vec<String>,
    ) -> Result<()> {
        let create_table_statement =
            CreateTableBuilder::new(schema, &self.table_name).primary_keys(primary_keys);
        let sql = create_table_statement.build_postgres();

        transaction
            .execute(&sql, &[])
            .await
            .context(UnableToCreatePostgresTableSnafu)?;

        Ok(())
    }

    async fn create_index(
        &self,
        transaction: &Transaction<'_>,
        columns: Vec<&str>,
        unique: bool,
    ) -> Result<()> {
        let mut index_builder = IndexBuilder::new(&self.table_name, columns);
        if unique {
            index_builder = index_builder.unique();
        }
        let sql = index_builder.build_postgres();

        transaction
            .execute(&sql, &[])
            .await
            .context(UnableToCreateIndexForPostgresTableSnafu)?;

        Ok(())
    }
}
