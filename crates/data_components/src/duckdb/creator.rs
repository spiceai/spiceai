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
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use arrow_sql_gen::statement::IndexBuilder;
use datafusion::common::Constraints;
use db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::{vtab::arrow_recordbatch_to_query_params, ToSql, Transaction};
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

use super::DuckDB;
use crate::util::{
    constraints::get_primary_keys_from_constraints,
    indexes::{self, IndexType},
};

/// Responsible for creating a `DuckDB` table along with any constraints and indexes
pub(crate) struct TableCreator {
    table_name: String,
    schema: SchemaRef,
    pool: Arc<DuckDbConnectionPool>,
    constraints: Option<Constraints>,
    indexes: HashMap<String, IndexType>,
    created: bool,
}

impl TableCreator {
    pub fn new(table_name: String, schema: SchemaRef, pool: Arc<DuckDbConnectionPool>) -> Self {
        Self {
            table_name,
            schema,
            pool,
            constraints: None,
            indexes: HashMap::new(),
            created: false,
        }
    }

    pub fn constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = Some(constraints);
        self
    }

    pub fn indexes(mut self, indexes: HashMap<String, IndexType>) -> Self {
        self.indexes = indexes;
        self
    }

    fn indexes_vec(&self) -> Vec<(Vec<&str>, IndexType)> {
        self.indexes
            .iter()
            .map(|(key, ty)| (indexes::index_columns(key), *ty))
            .collect()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn create(mut self) -> super::Result<DuckDB> {
        assert!(!self.created, "Table already created");
        let primary_keys = if let Some(constraints) = &self.constraints {
            get_primary_keys_from_constraints(constraints, &self.schema)
        } else {
            Vec::new()
        };

        let mut db_conn = Arc::clone(&self.pool)
            .connect_sync()
            .context(super::DbConnectionSnafu)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)?;

        let tx = duckdb_conn
            .conn
            .transaction()
            .context(super::UnableToBeginTransactionSnafu)?;

        self.create_table(&tx, primary_keys)?;

        for index in self.indexes_vec() {
            self.create_index(&tx, index.0, index.1 == IndexType::Unique)?;
        }

        tx.commit().context(super::UnableToCommitTransactionSnafu)?;

        let constraints = self.constraints.clone().unwrap_or(Constraints::empty());

        let mut duckdb =
            DuckDB::existing_table(self.table_name.clone(), Arc::clone(&self.pool), constraints);

        self.created = true;

        duckdb.table_creator = Some(self);

        Ok(duckdb)
    }

    /// Creates a copy of the `DuckDB` table with the same schema and constraints
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn create_empty_clone(&self) -> super::Result<DuckDB> {
        assert!(self.created, "Table must be created before cloning");

        let new_table_name = format!(
            "{}_spice_{}",
            self.table_name,
            &Uuid::new_v4().to_string()[..8]
        );
        tracing::debug!(
            "Creating empty table {} from {}",
            new_table_name,
            self.table_name,
        );

        let new_table_creator = TableCreator {
            table_name: new_table_name.clone(),
            schema: Arc::clone(&self.schema),
            pool: Arc::clone(&self.pool),
            constraints: self.constraints.clone(),
            indexes: self.indexes.clone(),
            created: false,
        };

        new_table_creator.create()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn delete_table(self, tx: &Transaction<'_>) -> super::Result<()> {
        assert!(self.created, "Table must be created before deleting");
        for index in self.indexes_vec() {
            self.drop_index(tx, index.0)?;
        }
        self.drop_table(tx)?;

        Ok(())
    }

    /// Consumes the current table and replaces `table_to_replace` with the current table's contents.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn replace_table(
        mut self,
        tx: &Transaction<'_>,
        table_to_replace: &TableCreator,
    ) -> super::Result<()> {
        assert!(
            self.created,
            "Table must be created before replacing another table"
        );

        // Drop indexes and table for the table we want to replace
        for index in table_to_replace.indexes_vec() {
            table_to_replace.drop_index(tx, index.0)?;
        }
        // Drop the old table with the name we want to claim
        table_to_replace.drop_table(tx)?;

        // DuckDB doesn't support renaming tables with existing indexes, so first drop them, rename the table and recreate them.
        for index in self.indexes_vec() {
            self.drop_index(tx, index.0)?;
        }
        // Rename our table to the target table name
        self.rename_table(tx, table_to_replace.table_name.as_str())?;
        // Update our table name to the target table name so the indexes are created correctly
        self.table_name.clone_from(&table_to_replace.table_name);
        // Recreate the indexes, now for our newly renamed table.
        for index in self.indexes_vec() {
            self.create_index(tx, index.0, index.1 == IndexType::Unique)?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, transaction))]
    fn create_table(
        &self,
        transaction: &Transaction<'_>,
        primary_keys: Vec<String>,
    ) -> super::Result<()> {
        let mut sql = self.get_table_create_statement()?;

        if !primary_keys.is_empty() {
            let primary_key_clause = format!(", PRIMARY KEY ({}));", primary_keys.join(", "));
            sql = sql.replace(");", &primary_key_clause);
        }
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToCreateDuckDBTableSnafu)?;

        Ok(())
    }

    /// DuckDB CREATE TABLE statements aren't supported by sea-query - so we create a temporary table
    /// from an Arrow schema and ask DuckDB for the CREATE TABLE statement.
    #[tracing::instrument(level = "debug", skip_all)]
    fn get_table_create_statement(&self) -> super::Result<String> {
        let mut db_conn = Arc::clone(&self.pool)
            .connect_sync()
            .context(super::DbConnectionSnafu)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)?;

        let tx = duckdb_conn
            .conn
            .transaction()
            .context(super::UnableToBeginTransactionSnafu)?;

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
        tracing::debug!("{sql}");

        tx.execute(&sql, arrow_params_ref)
            .context(super::UnableToCreateDuckDBTableSnafu)?;

        let create_stmt = tx
            .query_row(
                &format!(
                    "select sql from duckdb_tables() where table_name = '{}'",
                    self.table_name
                ),
                [],
                |r| r.get::<usize, String>(0),
            )
            .context(super::UnableToQueryDataSnafu)?;

        tx.rollback()
            .context(super::UnableToRollbackTransactionSnafu)?;

        Ok(create_stmt)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn drop_table(&self, transaction: &Transaction<'_>) -> super::Result<()> {
        let sql = format!(r#"DROP TABLE IF EXISTS "{}""#, self.table_name);
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToDropDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, transaction))]
    fn rename_table(
        &self,
        transaction: &Transaction<'_>,
        new_table_name: &str,
    ) -> super::Result<()> {
        let sql = format!(
            r#"ALTER TABLE "{}" RENAME TO "{new_table_name}""#,
            self.table_name
        );
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToRenameDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, transaction))]
    fn create_index(
        &self,
        transaction: &Transaction<'_>,
        columns: Vec<&str>,
        unique: bool,
    ) -> super::Result<()> {
        let mut index_builder = IndexBuilder::new(&self.table_name, columns);
        if unique {
            index_builder = index_builder.unique();
        }
        let sql = index_builder.build_postgres();
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToCreateIndexOnDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, transaction))]
    fn drop_index(&self, transaction: &Transaction<'_>, columns: Vec<&str>) -> super::Result<()> {
        let index_name = IndexBuilder::new(&self.table_name, columns).index_name();

        let sql = format!(r#"DROP INDEX IF EXISTS "{index_name}""#);
        tracing::debug!("{sql}");

        transaction
            .execute(&sql, [])
            .context(super::UnableToDropIndexOnDuckDBTableSnafu)?;

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use arrow::array::RecordBatch;
    use datafusion::{
        execution::{SendableRecordBatchStream, TaskContext},
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
        physical_plan::{insert::DataSink, memory::MemoryStream},
    };
    use db_connection_pool::{
        dbconnection::duckdbconn::DuckDbConnection, duckdbpool::DuckDbConnectionPool,
    };
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::{duckdb::write::DuckDBDataSink, util::constraints::tests::get_constraints};

    use super::*;

    fn get_mem_duckdb() -> Arc<DuckDbConnectionPool> {
        Arc::new(
            DuckDbConnectionPool::new_memory(&duckdb::AccessMode::ReadWrite)
                .expect("to get a memory duckdb connection pool"),
        )
    }

    async fn get_logs_batches() -> Vec<RecordBatch> {
        let parquet_bytes = reqwest::get("https://public-data.spiceai.org/eth.recent_logs.parquet")
            .await
            .expect("to get parquet file")
            .bytes()
            .await
            .expect("to get parquet bytes");

        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes)
            .expect("to get parquet reader builder")
            .build()
            .expect("to build parquet reader");

        parquet_reader
            .collect::<Result<Vec<_>, arrow::error::ArrowError>>()
            .expect("to get records")
    }

    fn get_stream_from_batches(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let schema = batches[0].schema();
        Box::pin(MemoryStream::try_new(batches, schema, None).expect("to get stream"))
    }

    #[tokio::test]
    async fn test_table_creator() {
        let _guard = init_tracing(None);
        let batches = get_logs_batches().await;

        let schema = batches[0].schema();

        for overwrite in &[false, true] {
            let pool = get_mem_duckdb();
            let constraints =
                get_constraints(&["log_index", "transaction_hash"], Arc::clone(&schema));
            let created_table = TableCreator::new(
                "eth.logs".to_string(),
                Arc::clone(&schema),
                Arc::clone(&pool),
            )
            .constraints(constraints)
            .indexes(
                vec![
                    ("block_number".to_string(), IndexType::Enabled),
                    (
                        "(log_index, transaction_hash)".to_string(),
                        IndexType::Unique,
                    ),
                ]
                .into_iter()
                .collect(),
            )
            .create()
            .expect("to create table");

            let arc_created_table = Arc::new(created_table);

            let duckdb_sink = DuckDBDataSink::new(arc_created_table, *overwrite);
            let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);
            let rows_written = data_sink
                .write_all(
                    get_stream_from_batches(batches.clone()),
                    &Arc::new(TaskContext::default()),
                )
                .await
                .expect("to write all");

            let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
            let conn = pool_conn
                .as_any_mut()
                .downcast_mut::<DuckDbConnection>()
                .expect("to downcast to duckdb connection");
            let num_rows = conn
                .get_underlying_conn_mut()
                .query_row(r#"SELECT COUNT(1) FROM "eth.logs""#, [], |r| {
                    r.get::<usize, u64>(0)
                })
                .expect("to get count");

            assert_eq!(num_rows, rows_written);
        }
    }

    pub(crate) fn init_tracing(default_level: Option<&str>) -> DefaultGuard {
        let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
            (_, Some(log)) => EnvFilter::new(log),
            (Some(level), None) => EnvFilter::new(level),
            _ => EnvFilter::new("INFO,data_components=TRACE"),
        };

        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(filter)
            .with_ansi(true)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }
}
