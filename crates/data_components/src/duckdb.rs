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

use crate::{
    delete::DeletionTableProviderAdapter,
    util::{
        constraints,
        indexes::{self, IndexType},
    },
    Read, ReadWrite,
};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use arrow_sql_gen::statement::IndexBuilder;
use async_trait::async_trait;
use datafusion::{
    common::Constraints,
    datasource::{provider::TableProviderFactory, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::CreateExternalTable,
    sql::{sqlparser::{ast::TableFactor, dialect::DuckDbDialect, parser::Parser, tokenizer::Tokenizer}, TableReference},
};
use db_connection_pool::{
    dbconnection::{duckdbconn::DuckDbConnection, get_schema, DbConnection},
    duckdbpool::DuckDbConnectionPool,
    DbConnectionPool, Mode,
};
use duckdb::{
    vtab::arrow::arrow_recordbatch_to_query_params, AccessMode, DuckdbConnectionManager, ToSql,
    Transaction,
};
use snafu::prelude::*;
use sql_provider_datafusion::{expr::Engine, SqlTable};
use std::{cmp, collections::HashMap, sync::Arc, time::SystemTime};

use self::write::DuckDBTableWriter;

pub mod write;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError { source: db_connection_pool::Error },

    #[snafu(display("DuckDBDataFusionError: {source}"))]
    DuckDBDataFusion {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("Unable to downcast DbConnection to DuckDbConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("Unable to drop duckdb table: {source}"))]
    UnableToDropDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to create duckdb table: {source}"))]
    UnableToCreateDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to create index on duckdb table: {source}"))]
    UnableToCreateIndexOnDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to insert into duckdb table: {source}"))]
    UnableToInsertToDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to get appender to duckdb table: {source}"))]
    UnableToGetAppenderToDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to delete data from the duckdb table: {source}"))]
    UnableToDeleteDuckdbData { source: duckdb::Error },

    #[snafu(display("Unable to query data from the duckdb table: {source}"))]
    UnableToQueryData { source: duckdb::Error },

    #[snafu(display("Unable to commit transaction: {source}"))]
    UnableToCommitTransaction { source: duckdb::Error },

    #[snafu(display("Unable to begin duckdb transaction: {source}"))]
    UnableToBeginTransaction { source: duckdb::Error },

    #[snafu(display("Unable to commit the Postgres transaction: {source}"))]
    UnableToCommitDuckDBTransaction { source: duckdb::Error },

    #[snafu(display("Unable to delete all data from the Postgres table: {source}"))]
    UnableToDeleteAllTableData { source: duckdb::Error },

    #[snafu(display("Unable to insert data into the Sqlite table: {source}"))]
    UnableToInsertIntoTableAsync { source: duckdb::Error },

    #[snafu(display("The table '{table_name}' doesn't exist in the DuckDB server"))]
    TableDoesntExist { table_name: String },

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation { source: constraints::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDBTableProviderFactory {
    access_mode: AccessMode,
    db_path_param: String,
}

impl DuckDBTableProviderFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            access_mode: AccessMode::ReadOnly,
            db_path_param: "open".to_string(),
        }
    }

    #[must_use]
    pub fn access_mode(mut self, access_mode: AccessMode) -> Self {
        self.access_mode = access_mode;
        self
    }

    #[must_use]
    pub fn db_path_param(mut self, db_path_param: &str) -> Self {
        self.db_path_param = db_path_param.to_string();
        self
    }
}

impl Default for DuckDBTableProviderFactory {
    fn default() -> Self {
        Self::new()
    }
}

type DynDuckDbConnectionPool = dyn DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>
    + Send
    + Sync;

#[async_trait]
impl TableProviderFactory for DuckDBTableProviderFactory {
    async fn create(
        &self,
        _state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let name = cmd.name.to_string();
        let mut options = cmd.options.clone();
        let mode = options.remove("mode").unwrap_or_default();
        let mode: Mode = mode.as_str().into();

        let indexes_option_str = options.remove("indexes");
        let indexes = match indexes_option_str {
            Some(indexes_str) => indexes::indexes_from_option_string(&indexes_str),
            None => HashMap::new(),
        };

        let indexes: Vec<(Vec<&str>, IndexType)> = indexes
            .iter()
            .map(|(key, ty)| (indexes::index_columns(key), *ty))
            .collect();

        let pool: Arc<DuckDbConnectionPool> = Arc::new(match &mode {
            Mode::File => {
                // open duckdb at given path or create a new one
                let db_path = cmd
                    .options
                    .get(self.db_path_param.as_str())
                    .cloned()
                    .unwrap_or(format!("{name}.db"));

                DuckDbConnectionPool::new_file(&db_path, &self.access_mode)
                    .context(DbConnectionPoolSnafu)
                    .map_err(to_datafusion_error)?
            }
            Mode::Memory => DuckDbConnectionPool::new_memory(&self.access_mode)
                .context(DbConnectionPoolSnafu)
                .map_err(to_datafusion_error)?,
        });

        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().into());
        let duckdb = DuckDB::new(
            name.clone(),
            Arc::clone(&schema),
            Arc::clone(&pool),
            cmd.constraints.clone(),
        );

        let mut db_conn = duckdb.connect().await.map_err(to_datafusion_error)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let tx = duckdb_conn
            .conn
            .transaction()
            .context(UnableToBeginTransactionSnafu)
            .map_err(to_datafusion_error)?;

        duckdb.create_table(&tx).map_err(to_datafusion_error)?;

        for index in indexes {
            duckdb
                .create_index(&tx, index.0, index.1 == IndexType::Unique)
                .map_err(to_datafusion_error)?;
        }

        tx.commit()
            .context(UnableToCommitDuckDBTransactionSnafu)
            .map_err(to_datafusion_error)?;

        let dyn_pool: Arc<DynDuckDbConnectionPool> = pool;

        let read_provider = Arc::new(SqlTable::new_with_schema(
            "duckdb",
            &dyn_pool,
            Arc::clone(&schema),
            TableReference::bare(name.clone()),
            Some(Engine::DuckDB),
        ));

        let read_write_provider = DuckDBTableWriter::create(read_provider, duckdb);

        let deleted_table_provider = DeletionTableProviderAdapter::new(read_write_provider);

        Ok(Arc::new(deleted_table_provider))
    }
}

fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[derive(Clone)]
pub struct DuckDB {
    table_name: String,
    schema: SchemaRef,
    pool: Arc<DuckDbConnectionPool>,
    constraints: Constraints,
}

impl DuckDB {
    #[must_use]
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        pool: Arc<DuckDbConnectionPool>,
        constraints: Constraints,
    ) -> Self {
        Self {
            table_name,
            schema,
            pool,
            constraints,
        }
    }

    #[must_use]
    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    async fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>>,
    > {
        self.pool.connect().await.context(DbConnectionSnafu)
    }

    fn connect_sync(
        &self,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>>,
    > {
        Arc::clone(&self.pool)
            .connect_sync()
            .context(DbConnectionSnafu)
    }

    fn duckdb_conn<'a>(
        db_connection: &'a mut Box<
            dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>,
        >,
    ) -> Result<&'a mut DuckDbConnection> {
        db_connection
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .context(UnableToDowncastDbConnectionSnafu)
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

    fn insert_batch(&self, transaction: &Transaction<'_>, batch: &RecordBatch) -> Result<()> {
        let mut appender = transaction
            .appender(&self.table_name)
            .context(UnableToGetAppenderToDuckDBTableSnafu)?;

        for batch in Self::split_batch(batch) {
            appender
                .append_record_batch(batch.clone())
                .context(UnableToInsertToDuckDBTableSnafu)?;
        }

        Ok(())
    }

    fn delete_all_table_data(&self, transaction: &Transaction<'_>) -> Result<()> {
        transaction
            .execute(format!(r#"DELETE FROM "{}""#, self.table_name).as_str(), [])
            .context(UnableToDeleteAllTableDataSnafu)?;

        Ok(())
    }

    fn delete_from(&self, duckdb_conn: &mut DuckDbConnection, where_clause: &str) -> Result<u64> {
        let tx = duckdb_conn
            .conn
            .transaction()
            .context(UnableToBeginTransactionSnafu)?;

        let count_sql = format!(
            r#"SELECT COUNT(*) FROM "{}" WHERE {}"#,
            self.table_name, where_clause
        );

        let mut count: u64 = tx
            .query_row(&count_sql, [], |row| row.get::<usize, u64>(0))
            .context(UnableToQueryDataSnafu)?;

        let sql = format!(
            r#"DELETE FROM "{}" WHERE {}"#,
            self.table_name, where_clause
        );
        tx.execute(&sql, [])
            .context(UnableToDeleteDuckdbDataSnafu)?;

        count -= tx
            .query_row(&count_sql, [], |row| row.get::<usize, u64>(0))
            .context(UnableToQueryDataSnafu)?;

        tx.commit().context(UnableToCommitTransactionSnafu)?;
        Ok(count)
    }

    fn create_table(&self, transaction: &Transaction<'_>) -> Result<()> {
        let empty_record = RecordBatch::new_empty(Arc::clone(&self.schema));

        let arrow_params = arrow_recordbatch_to_query_params(empty_record);
        let arrow_params_vec: Vec<&dyn ToSql> = arrow_params
            .iter()
            .map(|p| p as &dyn ToSql)
            .collect::<Vec<_>>();
        let arrow_params_ref: &[&dyn ToSql] = &arrow_params_vec;
        let sql = format!(
            r#"CREATE TABLE IF NOT EXISTS "{name}" AS SELECT * FROM arrow(?, ?)"#,
            name = self.table_name
        );
        tracing::trace!("{sql}");

        transaction
            .execute(&sql, arrow_params_ref)
            .context(UnableToCreateDuckDBTableSnafu)?;

        Ok(())
    }

    fn create_index(
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
            .execute(&sql, [])
            .context(UnableToCreateIndexOnDuckDBTableSnafu)?;

        Ok(())
    }
}

pub struct DuckDBTableFactory {
    pool: Arc<DuckDbConnectionPool>,
}

impl DuckDBTableFactory {
    #[must_use]
    pub fn new(pool: Arc<DuckDbConnectionPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Read for DuckDBTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let conn = Arc::clone(&pool).connect().await?;
        let dyn_pool: Arc<DynDuckDbConnectionPool> = pool;

        let schema = get_schema(conn, &table_reference).await?;
        let table_provider = Arc::new(SqlTable::new_with_schema("duckdb", &dyn_pool, schema, table_reference, None));
        let table_provider = Arc::new(table_provider.create_federated_table_provider()?);
        Ok(table_provider)
    }
}

#[async_trait]
impl ReadWrite for DuckDBTableFactory {
    async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let read_provider = Read::table_provider(self, table_reference.clone()).await?;

        let table_name = table_reference.to_string();
        let duckdb = DuckDB::new(
            table_name,
            Arc::clone(&read_provider).schema(),
            Arc::clone(&self.pool),
            Constraints::empty(),
        );

        Ok(DuckDBTableWriter::create(read_provider, duckdb))
    }
}

fn is_table_function(table_reference: &TableReference) -> bool {
    let table_name = match table_reference {
        TableReference::Full { .. } | TableReference::Partial { .. } => return false,
        TableReference::Bare { table } => table,
    };

    let dialect = DuckDbDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, table_name);
    let Ok(tokens) = tokenizer.tokenize() else {
        return false;
    };
    let Ok(tf) = Parser::new(&dialect)
        .with_tokens(tokens)
        .parse_table_factor()
    else {
        return false;
    };

    let TableFactor::Table { args, .. } = tf else {
        return false;
    };

    args.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_table_function() {
        let tests = vec![
            ("table_name", false),
            ("table_name()", true),
            ("table_name(arg1, arg2)", true),
            ("read_parquet", false),
            ("read_parquet()", true),
            ("read_parquet('my_parquet_file.parquet')", true),
            ("read_csv_auto('my_csv_file.csv')", true),
        ];

        for (table_name, expected) in tests {
            let table_reference = TableReference::bare(table_name.to_string());
            assert_eq!(is_table_function(&table_reference), expected);
        }
    }
}
