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
use crate::mysql::write::MySQLTableWriter;
use crate::sql::arrow_sql_gen::statement::{CreateTableBuilder, IndexBuilder, InsertBuilder};
use crate::sql::db_connection_pool::dbconnection::mysqlconn::MySQLConnection;
use crate::sql::db_connection_pool::dbconnection::DbConnection;
use crate::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use crate::sql::db_connection_pool::{self, mysqlpool, DbConnectionPool};
use crate::sql::sql_provider_datafusion::{self, expr::Engine, SqlTable};
use crate::util::supported_functions::FunctionSupport;
use crate::util::{
    self, column_reference::ColumnReference, constraints::get_primary_keys_from_constraints,
    indexes::IndexType, on_conflict::OnConflict, secrets::to_secret_map, to_datafusion_error,
};
use crate::util::{column_reference, constraints, on_conflict};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::sql::unparser::dialect::MySqlDialect;
use datafusion::{
    catalog::TableProviderFactory, common::Constraints, datasource::TableProvider,
    error::DataFusionError, logical_expr::CreateExternalTable, sql::TableReference,
};
use mysql_async::prelude::{Queryable, ToValue};
use mysql_async::{Metrics, TxOpts};
use sea_query::{Alias, DeleteStatement, MysqlQueryBuilder};
use snafu::prelude::*;
use sql_table::MySQLTable;
use std::collections::HashMap;
use std::sync::Arc;

pub type DynMySQLConnectionPool =
    dyn DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)> + Send + Sync;

pub type DynMySQLConnection = dyn DbConnection<mysql_async::Conn, &'static (dyn ToValue + Sync)>;

#[cfg(feature = "mysql-federation")]
pub mod federation;
pub(crate) mod mysql_window;
pub mod sql_table;
pub mod write;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("Unable to construct SQL table: {source}"))]
    UnableToConstructSQLTable {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("Unable to delete all data from the MySQL table: {source}"))]
    UnableToDeleteAllTableData { source: mysql_async::Error },

    #[snafu(display("Unable to insert Arrow batch to MySQL table: {source}"))]
    UnableToInsertArrowBatch { source: mysql_async::Error },

    #[snafu(display("Unable to downcast DbConnection to MySQLConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("Unable to begin MySQL transaction: {source}"))]
    UnableToBeginTransaction { source: mysql_async::Error },

    #[snafu(display("Unable to create MySQL connection pool: {source}"))]
    UnableToCreateMySQLConnectionPool { source: mysqlpool::Error },

    #[snafu(display("Unable to create the MySQL table: {source}"))]
    UnableToCreateMySQLTable { source: mysql_async::Error },

    #[snafu(display("Unable to create an index for the MySQL table: {source}"))]
    UnableToCreateIndexForMySQLTable { source: mysql_async::Error },

    #[snafu(display("Unable to commit the MySQL transaction: {source}"))]
    UnableToCommitMySQLTransaction { source: mysql_async::Error },

    #[snafu(display("Unable to create insertion statement for MySQL table: {source}"))]
    UnableToCreateInsertStatement {
        source: crate::sql::arrow_sql_gen::statement::Error,
    },

    #[snafu(display("The table '{table_name}' doesn't exist in the MySQL server"))]
    TableDoesntExist { table_name: String },

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation { source: constraints::Error },

    #[snafu(display("Error parsing column reference: {source}"))]
    UnableToParseColumnReference { source: column_reference::Error },

    #[snafu(display("Error parsing on_conflict: {source}"))]
    UnableToParseOnConflict { source: on_conflict::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct MySQLTableFactory {
    pool: Arc<MySQLConnectionPool>,
    function_support: Option<FunctionSupport>,
}

impl MySQLTableFactory {
    #[must_use]
    pub fn new(pool: Arc<MySQLConnectionPool>) -> Self {
        Self {
            pool,
            function_support: None,
        }
    }

    #[must_use]
    pub fn with_function_support(mut self, function_support: FunctionSupport) -> Self {
        self.function_support = Some(function_support);
        self
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let table_provider = Arc::new(
            MySQLTable::new(&pool, table_reference, None, self.function_support.clone())
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        #[cfg(feature = "mysql-federation")]
        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }

    pub async fn read_write_table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let read_provider = Self::table_provider(self, table_reference.clone()).await?;
        let schema = read_provider.schema();

        let table_name = table_reference.to_string();
        let mysql = MySQL::new(
            table_name,
            Arc::clone(&self.pool),
            schema,
            Constraints::default(),
        );

        Ok(MySQLTableWriter::create(read_provider, mysql, None))
    }

    pub fn conn_pool_metrics(&self) -> Arc<Metrics> {
        self.pool.metrics()
    }
}

#[derive(Debug)]
pub struct MySQLTableProviderFactory {}

impl MySQLTableProviderFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for MySQLTableProviderFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProviderFactory for MySQLTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        let name = cmd.name.to_string();
        let mut options = cmd.options.clone();
        let schema: Schema = cmd.schema.as_ref().into();

        let indexes_option_str = options.remove("indexes");
        let unparsed_indexes: HashMap<String, IndexType> = match indexes_option_str {
            Some(indexes_str) => util::hashmap_from_option_string(&indexes_str),
            None => HashMap::new(),
        };

        let unparsed_indexes = unparsed_indexes
            .into_iter()
            .map(|(key, value)| {
                let columns = ColumnReference::try_from(key.as_str())
                    .context(UnableToParseColumnReferenceSnafu)
                    .map_err(util::to_datafusion_error);
                (columns, value)
            })
            .collect::<Vec<(Result<ColumnReference, DataFusionError>, IndexType)>>();

        let mut indexes: Vec<(ColumnReference, IndexType)> = Vec::new();
        for (columns, index_type) in unparsed_indexes {
            let columns = columns?;
            indexes.push((columns, index_type));
        }

        let mut on_conflict: Option<OnConflict> = None;
        if let Some(on_conflict_str) = options.remove("on_conflict") {
            on_conflict = Some(
                OnConflict::try_from(on_conflict_str.as_str())
                    .context(UnableToParseOnConflictSnafu)
                    .map_err(util::to_datafusion_error)?,
            );
        }

        let params = to_secret_map(options);

        let pool = Arc::new(
            MySQLConnectionPool::new(params)
                .await
                .context(UnableToCreateMySQLConnectionPoolSnafu)
                .map_err(to_datafusion_error)?,
        );
        let schema = Arc::new(schema);
        let mysql = MySQL::new(
            name.clone(),
            Arc::clone(&pool),
            Arc::clone(&schema),
            cmd.constraints.clone(),
        );

        let mut db_conn = pool
            .connect()
            .await
            .context(DbConnectionSnafu)
            .map_err(to_datafusion_error)?;

        let mysql_conn = MySQL::mysql_conn(&mut db_conn).map_err(to_datafusion_error)?;
        let mut conn_guard = mysql_conn.conn.lock().await;
        let mut transaction = conn_guard
            .start_transaction(TxOpts::default())
            .await
            .context(UnableToBeginTransactionSnafu)
            .map_err(to_datafusion_error)?;

        let primary_keys = get_primary_keys_from_constraints(&cmd.constraints, &schema);

        mysql
            .create_table(Arc::clone(&schema), &mut transaction, primary_keys)
            .await
            .map_err(to_datafusion_error)?;

        for index in indexes {
            mysql
                .create_index(
                    &mut transaction,
                    index.0.iter().collect(),
                    index.1 == IndexType::Unique,
                )
                .await
                .map_err(to_datafusion_error)?;
        }

        transaction
            .commit()
            .await
            .context(UnableToCommitMySQLTransactionSnafu)
            .map_err(to_datafusion_error)?;

        drop(conn_guard);

        let dyn_pool: Arc<DynMySQLConnectionPool> = pool;

        let read_provider = Arc::new(
            SqlTable::new_with_schema(
                "mysql",
                &dyn_pool,
                Arc::clone(&schema),
                TableReference::bare(name.clone()),
                Some(Engine::MySQL),
            )
            .with_dialect(Arc::new(MySqlDialect {})),
        );

        #[cfg(feature = "mysql-federation")]
        let read_provider = Arc::new(read_provider.create_federated_table_provider()?);
        Ok(MySQLTableWriter::create(read_provider, mysql, on_conflict))
    }
}

#[derive(Debug)]
pub struct MySQL {
    table_name: String,
    pool: Arc<MySQLConnectionPool>,
    schema: SchemaRef,
    constraints: Constraints,
}

impl MySQL {
    #[must_use]
    pub fn new(
        table_name: String,
        pool: Arc<MySQLConnectionPool>,
        schema: SchemaRef,
        constraints: Constraints,
    ) -> Self {
        Self {
            table_name,
            pool,
            schema,
            constraints,
        }
    }

    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    #[must_use]
    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    pub async fn connect(&self) -> Result<Box<DynMySQLConnection>> {
        let mut conn = self.pool.connect().await.context(DbConnectionSnafu)?;

        let mysql_conn = Self::mysql_conn(&mut conn)?;

        if !self.table_exists(mysql_conn).await {
            TableDoesntExistSnafu {
                table_name: self.table_name.clone(),
            }
            .fail()?;
        }

        Ok(conn)
    }

    pub fn mysql_conn(db_connection: &mut Box<DynMySQLConnection>) -> Result<&mut MySQLConnection> {
        let conn = db_connection
            .as_any_mut()
            .downcast_mut::<MySQLConnection>()
            .context(UnableToDowncastDbConnectionSnafu)?;

        Ok(conn)
    }

    async fn table_exists(&self, mysql_connection: &MySQLConnection) -> bool {
        let sql = format!(
            "SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_name = '{name}'
        )",
            name = self.table_name
        );
        tracing::trace!("{sql}");
        let Ok(Some((exists,))) = mysql_connection
            .conn
            .lock()
            .await
            .query_first::<(bool,), _>(&sql)
            .await
        else {
            return false;
        };

        exists
    }

    async fn insert_batch(
        &self,
        transaction: &mut mysql_async::Transaction<'_>,
        batch: RecordBatch,
        on_conflict: Option<OnConflict>,
    ) -> Result<()> {
        let insert_table_builder =
            InsertBuilder::new(&TableReference::bare(self.table_name.clone()), vec![batch]);

        let sea_query_on_conflict =
            on_conflict.map(|oc| oc.build_sea_query_on_conflict(&self.schema));

        let sql = insert_table_builder
            .build_mysql(sea_query_on_conflict)
            .context(UnableToCreateInsertStatementSnafu)?;

        transaction
            .exec_drop(&sql, ())
            .await
            .context(UnableToInsertArrowBatchSnafu)?;

        Ok(())
    }

    async fn delete_all_table_data(
        &self,
        transaction: &mut mysql_async::Transaction<'_>,
    ) -> Result<()> {
        let delete = DeleteStatement::new()
            .from_table(Alias::new(self.table_name.clone()))
            .to_string(MysqlQueryBuilder);
        transaction
            .exec_drop(delete.as_str(), ())
            .await
            .context(UnableToDeleteAllTableDataSnafu)?;

        Ok(())
    }

    async fn create_table(
        &self,
        schema: SchemaRef,
        transaction: &mut mysql_async::Transaction<'_>,
        primary_keys: Vec<String>,
    ) -> Result<()> {
        let create_table_statement =
            CreateTableBuilder::new(schema, &self.table_name).primary_keys(primary_keys);
        let create_stmts = create_table_statement.build_mysql();

        transaction
            .exec_drop(create_stmts, ())
            .await
            .context(UnableToCreateMySQLTableSnafu)
    }

    async fn create_index(
        &self,
        transaction: &mut mysql_async::Transaction<'_>,
        columns: Vec<&str>,
        unique: bool,
    ) -> Result<()> {
        let mut index_builder = IndexBuilder::new(&self.table_name, columns);
        if unique {
            index_builder = index_builder.unique();
        }
        let sql = index_builder.build_mysql();

        transaction
            .exec_drop(sql, ())
            .await
            .context(UnableToCreateIndexForMySQLTableSnafu)
    }
}
