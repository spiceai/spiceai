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
        self,
        column_reference::{self, ColumnReference},
        constraints,
        indexes::IndexType,
        on_conflict::{self, OnConflict},
    },
    Read, ReadWrite,
};
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::Constraints,
    datasource::{provider::TableProviderFactory, TableProvider},
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionState,
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use db_connection_pool::{
    dbconnection::{
        duckdbconn::{flatten_table_function_name, is_table_function, DuckDbConnection},
        get_schema, DbConnection,
    },
    duckdbpool::DuckDbConnectionPool,
    DbConnectionPool, Mode,
};
use duckdb::{AccessMode, DuckdbConnectionManager, ToSql, Transaction};
use itertools::Itertools;
use snafu::prelude::*;
use sql_provider_datafusion::expr::Engine;
use std::{cmp, collections::HashMap, sync::Arc};

use self::{creator::TableCreator, sql_table::DuckDBTable, write::DuckDBTableWriter};

mod creator;
mod federation;
mod sql_table;
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

    #[snafu(display("Unable to drop index on duckdb table: {source}"))]
    UnableToDropIndexOnDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to rename duckdb table: {source}"))]
    UnableToRenameDuckDBTable { source: duckdb::Error },

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

    #[snafu(display("Unable to rollback transaction: {source}"))]
    UnableToRollbackTransaction { source: duckdb::Error },

    #[snafu(display("Unable to delete all data from the Postgres table: {source}"))]
    UnableToDeleteAllTableData { source: duckdb::Error },

    #[snafu(display("Unable to insert data into the Sqlite table: {source}"))]
    UnableToInsertIntoTableAsync { source: duckdb::Error },

    #[snafu(display("The table '{table_name}' doesn't exist in the DuckDB server"))]
    TableDoesntExist { table_name: String },

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation { source: constraints::Error },

    #[snafu(display("Error parsing column reference: {source}"))]
    UnableToParseColumnReference { source: column_reference::Error },

    #[snafu(display("Error parsing on_conflict: {source}"))]
    UnableToParseOnConflict { source: on_conflict::Error },
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

    #[must_use]
    pub fn duckdb_file_path(&self, name: &str, options: &HashMap<String, String>) -> String {
        options
            .get(&self.db_path_param)
            .cloned()
            .unwrap_or_else(|| format!("{name}.db"))
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
        let unparsed_indexes: HashMap<String, IndexType> = match indexes_option_str {
            Some(indexes_str) => util::hashmap_from_option_string(&indexes_str),
            None => HashMap::new(),
        };

        let unparsed_indexes = unparsed_indexes
            .into_iter()
            .map(|(key, value)| {
                let columns = ColumnReference::try_from(key.as_str())
                    .context(UnableToParseColumnReferenceSnafu)
                    .map_err(to_datafusion_error);
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
                    .map_err(to_datafusion_error)?,
            );
        }

        let pool: Arc<DuckDbConnectionPool> = Arc::new(match &mode {
            Mode::File => {
                // open duckdb at given path or create a new one
                let db_path = self.duckdb_file_path(&name, &cmd.options);

                DuckDbConnectionPool::new_file(&db_path, &self.access_mode)
                    .context(DbConnectionPoolSnafu)
                    .map_err(to_datafusion_error)?
            }
            Mode::Memory => DuckDbConnectionPool::new_memory(&self.access_mode)
                .context(DbConnectionPoolSnafu)
                .map_err(to_datafusion_error)?,
        });

        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().into());

        let duckdb = TableCreator::new(name.clone(), Arc::clone(&schema), Arc::clone(&pool))
            .constraints(cmd.constraints.clone())
            .indexes(indexes)
            .create()
            .map_err(to_datafusion_error)?;

        let dyn_pool: Arc<DynDuckDbConnectionPool> = pool;

        let read_provider = Arc::new(DuckDBTable::new_with_schema(
            "duckdb",
            &dyn_pool,
            Arc::clone(&schema),
            TableReference::bare(name.clone()),
            Some(Engine::DuckDB),
            None,
        ));

        let read_write_provider = DuckDBTableWriter::create(read_provider, duckdb, on_conflict);

        let deleted_table_provider = DeletionTableProviderAdapter::new(read_write_provider);

        Ok(Arc::new(deleted_table_provider))
    }
}

fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

pub struct DuckDB {
    table_name: String,
    pool: Arc<DuckDbConnectionPool>,
    schema: SchemaRef,
    constraints: Constraints,
    table_creator: Option<TableCreator>,
}

impl DuckDB {
    #[must_use]
    pub fn existing_table(
        table_name: String,
        pool: Arc<DuckDbConnectionPool>,
        schema: SchemaRef,
        constraints: Constraints,
    ) -> Self {
        Self {
            table_name,
            pool,
            schema,
            constraints,
            table_creator: None,
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

    pub fn duckdb_conn<'a>(
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

    fn insert_table_into(
        &self,
        tx: &Transaction<'_>,
        table_to_insert_into: &DuckDB,
        on_conflict: Option<&OnConflict>,
    ) -> Result<()> {
        let mut insert_sql = format!(
            r#"INSERT INTO "{}" SELECT * FROM "{}""#,
            table_to_insert_into.table_name, self.table_name
        );

        if let Some(on_conflict) = on_conflict {
            let on_conflict_sql = on_conflict.build_on_conflict_statement(&self.schema);
            insert_sql.push_str(&format!(" {on_conflict_sql}"));
        }
        tracing::debug!("{insert_sql}");

        tx.execute(&insert_sql, [])
            .context(UnableToInsertToDuckDBTableSnafu)?;

        Ok(())
    }

    fn insert_batch_no_constraints(
        &self,
        transaction: &Transaction<'_>,
        batch: &RecordBatch,
    ) -> Result<()> {
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
        let (tbl_ref, cte) = if is_table_function(&table_reference) {
            let tbl_ref_view = create_table_function_view_name(&table_reference);
            (
                tbl_ref_view.clone(),
                Some(HashMap::from_iter(vec![(
                    tbl_ref_view.to_string(),
                    table_reference.table().to_string(),
                )])),
            )
        } else {
            (table_reference.clone(), None)
        };

        let table_provider = Arc::new(DuckDBTable::new_with_schema(
            "duckdb", &dyn_pool, schema, tbl_ref, None, cte,
        ));
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
        let schema = read_provider.schema();

        let table_name = table_reference.to_string();
        let duckdb = DuckDB::existing_table(
            table_name,
            Arc::clone(&self.pool),
            schema,
            Constraints::empty(),
        );

        Ok(DuckDBTableWriter::create(read_provider, duckdb, None))
    }
}

/// For a [`TableReference`] that is a table function, create a name for a view on the original [`TableReference`]
/// ### Example
///
/// ```rust
/// use data_components::duckdb::create_table_function_view_name;
/// use datafusion::datasource::TableReference;
///
/// let table_reference = TableReference::from("catalog.schema.read_parquet('cleaned_sales_data.parquet')");
/// let view_name = create_table_function_view_name(&table_reference);
/// assert_eq!(view_name.to_string(), "catalog.schema.read_parquet_cleaned_sales_dataparquet__view");
/// ```  read_parquetcleaned_sales_dataparquet_view
///
fn create_table_function_view_name(table_reference: &TableReference) -> TableReference {
    let tbl_ref_view = [
        table_reference.catalog(),
        table_reference.schema(),
        Some(&flatten_table_function_name(table_reference)),
    ]
    .iter()
    .flatten()
    .join(".");
    TableReference::from(&tbl_ref_view)
}
