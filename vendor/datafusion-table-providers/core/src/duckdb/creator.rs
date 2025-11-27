use crate::sql::arrow_sql_gen::statement::IndexBuilder;
use crate::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
use crate::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use crate::util::on_conflict::OnConflict;
use arrow::{
    array::{RecordBatch, RecordBatchIterator, RecordBatchReader},
    datatypes::SchemaRef,
    ffi_stream::FFI_ArrowArrayStream,
};
use datafusion::common::utils::quote_identifier;
use datafusion::common::Constraints;
use datafusion::sql::TableReference;
use duckdb::Transaction;
use itertools::Itertools;
use snafu::prelude::*;
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;

use super::DuckDB;
use crate::util::{
    column_reference::ColumnReference, constraints::get_primary_keys_from_constraints,
    indexes::IndexType,
};

/// A newtype for a relation name, to better control the inputs for the `TableDefinition`, `TableCreator`, and `ViewCreator`.
#[derive(Debug, Clone, PartialEq)]
pub struct RelationName(String);

impl Display for RelationName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl RelationName {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl From<TableReference> for RelationName {
    fn from(table_ref: TableReference) -> Self {
        RelationName(table_ref.to_string())
    }
}

/// A table definition, which includes the table name, schema, constraints, and indexes.
/// This is used to store the definition of a table for a dataset, and can be re-used to create one or more tables (like internal data tables).
#[derive(Debug, Clone, PartialEq)]
pub struct TableDefinition {
    name: RelationName,
    schema: SchemaRef,
    constraints: Option<Constraints>,
    indexes: Vec<(ColumnReference, IndexType)>,
}

impl TableDefinition {
    #[must_use]
    pub fn new(name: RelationName, schema: SchemaRef) -> Self {
        Self {
            name,
            schema,
            constraints: None,
            indexes: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = Some(constraints);
        self
    }

    #[must_use]
    pub fn with_indexes(mut self, indexes: Vec<(ColumnReference, IndexType)>) -> Self {
        self.indexes = indexes;
        self
    }

    #[must_use]
    pub fn with_name(self, name: RelationName) -> Self {
        Self {
            name,
            schema: self.schema,
            constraints: self.constraints,
            indexes: self.indexes,
        }
    }

    pub fn name(&self) -> &RelationName {
        &self.name
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// For an internal table, generate a unique name based on the table definition name and the current system time.
    pub fn generate_internal_name(&self) -> super::Result<RelationName> {
        let unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context(super::UnableToGetSystemTimeSnafu)?
            .as_millis();
        Ok(RelationName(format!(
            "__data_{table_name}_{unix_ms}",
            table_name = self.name,
        )))
    }

    pub fn constraints(&self) -> Option<&Constraints> {
        self.constraints.as_ref()
    }

    pub fn indexes(&self) -> &[(ColumnReference, IndexType)] {
        &self.indexes
    }

    /// Returns true if this table definition has a base table matching the exact `RelationName` of the definition
    ///
    /// # Errors
    ///
    /// If the transaction fails to query for whether the table exists.
    pub fn has_table(&self, tx: &Transaction<'_>) -> super::Result<bool> {
        let mut stmt = tx
            .prepare("SELECT 1 FROM duckdb_tables() WHERE table_name = ?")
            .context(super::UnableToQueryDataSnafu)?;
        let mut rows = stmt
            .query([self.name.to_string()])
            .context(super::UnableToQueryDataSnafu)?;

        Ok(rows
            .next()
            .context(super::UnableToQueryDataSnafu)?
            .is_some())
    }

    /// List all internal tables related to this table definition.
    ///
    /// # Errors
    ///
    /// Returns an error if the internal tables cannot be listed.
    pub fn list_internal_tables(
        &self,
        tx: &Transaction<'_>,
    ) -> super::Result<Vec<(RelationName, u64)>> {
        // list all related internal tables, based on the table definition name
        let sql = format!(
            "select table_name from duckdb_tables() where table_name LIKE '__data_{table_name}%'",
            table_name = self.name
        );
        let mut stmt = tx.prepare(&sql).context(super::UnableToQueryDataSnafu)?;
        let mut rows = stmt.query([]).context(super::UnableToQueryDataSnafu)?;

        let mut table_names = Vec::new();
        while let Some(row) = rows.next().context(super::UnableToQueryDataSnafu)? {
            let table_name = row
                .get::<usize, String>(0)
                .context(super::UnableToQueryDataSnafu)?;
            // __data_{table_name}% could be a subset of another table name, so we need to check if the table name starts with the table definition name
            let inner_name = table_name.replace("__data_", "");
            let mut parts = inner_name.split('_');
            let Some(timestamp) = parts.next_back() else {
                continue; // skip invalid table names
            };

            let inner_name = parts.join("_");
            if inner_name != self.name.to_string() {
                continue;
            }

            let timestamp = timestamp
                .parse::<u64>()
                .context(super::UnableToParseSystemTimeSnafu)?;

            table_names.push((table_name, timestamp));
        }

        table_names.sort_by(|a, b| a.1.cmp(&b.1));

        Ok(table_names
            .into_iter()
            .map(|(name, time_created)| (RelationName(name), time_created))
            .collect())
    }
}

/// A table creator, which is used to create, delete, and manage tables based on a `TableDefinition`.
#[derive(Debug, Clone)]
pub struct TableManager {
    table_definition: Arc<TableDefinition>,
    internal_name: Option<RelationName>,
}

impl TableManager {
    pub fn new(table_definition: Arc<TableDefinition>) -> Self {
        Self {
            table_definition,
            internal_name: None,
        }
    }

    /// Set the internal flag for the table creator.
    pub fn with_internal(mut self, is_internal: bool) -> super::Result<Self> {
        if is_internal {
            self.internal_name = Some(self.table_definition.generate_internal_name()?);
        } else {
            self.internal_name = None;
        }

        Ok(self)
    }

    pub fn definition_name(&self) -> &RelationName {
        &self.table_definition.name
    }

    /// Returns the canonical name for this table, which is the internal name if the table is internal, or the table name if it is not.
    pub fn table_name(&self) -> &RelationName {
        self.internal_name
            .as_ref()
            .unwrap_or_else(|| &self.table_definition.name)
    }

    /// Returns the table definition for the table creator.
    pub fn table_definition(&self) -> &Arc<TableDefinition> {
        &self.table_definition
    }

    /// Searches if a table by the name specified in the table definition exists in the database.
    /// Returns None if the table does not exist, or an instance of a `TableCreator` for the base table if it does.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn base_table(&self, tx: &Transaction<'_>) -> super::Result<Option<Self>> {
        let mut stmt = tx
            .prepare("SELECT 1 FROM duckdb_tables() WHERE table_name = ?")
            .context(super::UnableToQueryDataSnafu)?;
        let mut rows = stmt
            .query([self.definition_name().to_string()])
            .context(super::UnableToQueryDataSnafu)?;

        if rows
            .next()
            .context(super::UnableToQueryDataSnafu)?
            .is_some()
        {
            let base_table = self.clone();
            Ok(Some(base_table.with_internal(false)?))
        } else {
            Ok(None)
        }
    }

    pub fn indexes_vec(&self) -> Vec<(Vec<&str>, IndexType)> {
        self.table_definition
            .indexes
            .iter()
            .map(|(key, ty)| (key.iter().collect(), *ty))
            .collect()
    }

    /// Creates the table for this `TableManager`. Does not create indexes - use `TableManager::create_indexes` to apply indexes.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn create_table(
        &self,
        pool: Arc<DuckDbConnectionPool>,
        tx: &Transaction<'_>,
    ) -> super::Result<()> {
        let mut db_conn = pool.connect_sync().context(super::DbConnectionPoolSnafu)?;
        let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)?;

        // create the table with the supplied table name, or a generated internal name
        let mut create_stmt = self.get_table_create_statement(duckdb_conn)?;
        tracing::debug!("{create_stmt}");

        let primary_keys = if let Some(constraints) = &self.table_definition.constraints {
            get_primary_keys_from_constraints(constraints, &self.table_definition.schema)
        } else {
            Vec::new()
        };

        if !primary_keys.is_empty() && !create_stmt.contains("PRIMARY KEY") {
            let primary_key_clause = format!(", PRIMARY KEY ({}));", primary_keys.join(", "));
            create_stmt = create_stmt.replace(");", &primary_key_clause);
        }

        tx.execute(&create_stmt, [])
            .context(super::UnableToCreateDuckDBTableSnafu)?;

        Ok(())
    }

    /// Drops indexes from the table, then drops the table itself.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn delete_table(&self, tx: &Transaction<'_>) -> super::Result<()> {
        // drop indexes first
        self.drop_indexes(tx)?;
        self.drop_table(tx)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn drop_table(&self, tx: &Transaction<'_>) -> super::Result<()> {
        // drop this table
        tx.execute(
            &format!(r#"DROP TABLE IF EXISTS "{}""#, self.table_name()),
            [],
        )
        .context(super::UnableToDropDuckDBTableSnafu)?;

        Ok(())
    }

    /// Inserts data from this table into the target table.
    #[tracing::instrument(level = "debug", skip_all)]
    #[allow(dead_code)]
    pub(crate) fn insert_into(
        &self,
        table: &TableManager,
        tx: &Transaction<'_>,
        on_conflict: Option<&OnConflict>,
    ) -> super::Result<u64> {
        // insert from this table, into the target table
        let mut insert_sql = format!(
            r#"INSERT INTO "{}" SELECT * FROM "{}""#,
            table.table_name(),
            self.table_name()
        );

        if let Some(on_conflict) = on_conflict {
            let on_conflict_sql =
                on_conflict.build_on_conflict_statement(&self.table_definition.schema);
            insert_sql.push_str(&format!(" {on_conflict_sql}"));
        }
        tracing::debug!("{insert_sql}");

        let rows = tx
            .execute(&insert_sql, [])
            .context(super::UnableToInsertToDuckDBTableSnafu)?;

        Ok(rows as u64)
    }

    fn get_index_name(table_name: &RelationName, index: &(Vec<&str>, IndexType)) -> String {
        let index_builder = IndexBuilder::new(&table_name.to_string(), index.0.clone());
        index_builder.index_name()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn create_index(
        &self,
        tx: &Transaction<'_>,
        index: (Vec<&str>, IndexType),
    ) -> super::Result<()> {
        let table_name = self.table_name();

        let unique = index.1 == IndexType::Unique;
        let columns = index.0;
        let mut index_builder = IndexBuilder::new(&table_name.to_string(), columns);
        if unique {
            index_builder = index_builder.unique();
        }
        let sql = index_builder.build_postgres();
        tracing::debug!("Creating index: {sql}");

        tx.execute(&sql, [])
            .context(super::UnableToCreateIndexOnDuckDBTableSnafu)?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub fn create_indexes(&self, tx: &Transaction<'_>) -> super::Result<()> {
        // create indexes on this table
        for index in self.indexes_vec() {
            self.create_index(tx, index)?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    fn drop_index(&self, tx: &Transaction<'_>, index: (Vec<&str>, IndexType)) -> super::Result<()> {
        let table_name = self.table_name();
        let index_name = TableManager::get_index_name(table_name, &index);

        let sql = format!(r#"DROP INDEX IF EXISTS "{index_name}""#);
        tracing::debug!("{sql}");

        tx.execute(&sql, [])
            .context(super::UnableToDropIndexOnDuckDBTableSnafu)?;

        Ok(())
    }

    pub(crate) fn drop_indexes(&self, tx: &Transaction<'_>) -> super::Result<()> {
        // drop indexes on this table
        for index in self.indexes_vec() {
            self.drop_index(tx, index)?;
        }

        Ok(())
    }

    /// DuckDB CREATE TABLE statements aren't supported by sea-query - so we create a temporary table
    /// from an Arrow schema and ask DuckDB for the CREATE TABLE statement.
    #[tracing::instrument(level = "debug", skip_all)]
    fn get_table_create_statement(
        &self,
        duckdb_conn: &mut DuckDbConnection,
    ) -> super::Result<String> {
        let tx = duckdb_conn
            .conn
            .transaction()
            .context(super::UnableToBeginTransactionSnafu)?;
        let table_name = self.table_name();
        let record_batch_reader =
            create_empty_record_batch_reader(Arc::clone(&self.table_definition.schema));
        let stream = FFI_ArrowArrayStream::new(Box::new(record_batch_reader));

        let current_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context(super::UnableToGetSystemTimeSnafu)?
            .as_millis();

        let view_name = format!("__scan_{}_{current_ts}", table_name);
        tx.register_arrow_scan_view(&view_name, &stream)
            .context(super::UnableToRegisterArrowScanViewForTableCreationSnafu)?;

        let sql =
            format!(r#"CREATE TABLE IF NOT EXISTS "{table_name}" AS SELECT * FROM "{view_name}""#,);
        tracing::debug!("{sql}");

        tx.execute(&sql, [])
            .context(super::UnableToCreateDuckDBTableSnafu)?;

        let create_stmt = tx
            .query_row(
                &format!("select sql from duckdb_tables() where table_name = '{table_name}'",),
                [],
                |r| r.get::<usize, String>(0),
            )
            .context(super::UnableToQueryDataSnafu)?;

        // DuckDB doesn't add IF NOT EXISTS to CREATE TABLE statements, so we add it here.
        let create_stmt = create_stmt.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");

        tx.rollback()
            .context(super::UnableToRollbackTransactionSnafu)?;

        Ok(create_stmt)
    }

    /// List all internal tables related to this table manager's table definition.
    /// Excludes itself from the list of tables, if created.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn list_other_internal_tables(
        &self,
        tx: &Transaction<'_>,
    ) -> super::Result<Vec<(Self, u64)>> {
        let tables = self.table_definition.list_internal_tables(tx)?;

        Ok(tables
            .into_iter()
            .filter_map(|(name, time_created)| {
                if let Some(internal_name) = &self.internal_name {
                    if name == *internal_name {
                        return None;
                    }
                }

                let internal_table = TableManager {
                    table_definition: Arc::clone(&self.table_definition),
                    internal_name: Some(name),
                };
                Some((internal_table, time_created))
            })
            .collect())
    }

    /// If this table is an internal table, creates a view with the table definition name targeting this table.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn create_view(&self, tx: &Transaction<'_>) -> super::Result<()> {
        if self.internal_name.is_none() {
            return Ok(());
        }

        tx.execute(
            &format!(
                "CREATE OR REPLACE VIEW {base_table} AS SELECT * FROM {internal_table}",
                base_table = quote_identifier(&self.definition_name().to_string()),
                internal_table = quote_identifier(&self.table_name().to_string())
            ),
            [],
        )
        .context(super::UnableToCreateDuckDBTableSnafu)?;

        Ok(())
    }

    /// Returns the current primary keys in database for this table.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn current_primary_keys(
        &self,
        tx: &Transaction<'_>,
    ) -> super::Result<HashSet<String>> {
        // DuckDB provides convenient queryable 'pragma_table_info' table function
        // Complex table name with schema as part of the name must be quoted as
        // '"<name>"', otherwise it will be parsed to schema and table name
        let sql = format!(
            "SELECT name FROM pragma_table_info('{table_name}') WHERE pk = true",
            table_name = quote_identifier(&self.table_name().to_string())
        );
        tracing::debug!("{sql}");

        let mut stmt = tx
            .prepare(&sql)
            .context(super::UnableToGetPrimaryKeysOnDuckDBTableSnafu)?;

        let primary_keys_iter = stmt
            .query_map([], |row| row.get::<usize, String>(0))
            .context(super::UnableToGetPrimaryKeysOnDuckDBTableSnafu)?;

        let mut primary_keys = HashSet::new();
        for pk in primary_keys_iter {
            primary_keys.insert(pk.context(super::UnableToGetPrimaryKeysOnDuckDBTableSnafu)?);
        }

        Ok(primary_keys)
    }

    /// Returns the current indexes in database for this table.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn current_indexes(&self, tx: &Transaction<'_>) -> super::Result<HashSet<String>> {
        let sql = format!(
            "SELECT index_name FROM duckdb_indexes WHERE table_name = '{table_name}'",
            table_name = &self.table_name().to_string()
        );

        tracing::debug!("{sql}");

        let mut stmt = tx
            .prepare(&sql)
            .context(super::UnableToGetPrimaryKeysOnDuckDBTableSnafu)?;

        let indexes_iter = stmt
            .query_map([], |row| row.get::<usize, String>(0))
            .context(super::UnableToGetPrimaryKeysOnDuckDBTableSnafu)?;

        let mut indexes = HashSet::new();
        for index in indexes_iter {
            indexes.insert(index.context(super::UnableToGetPrimaryKeysOnDuckDBTableSnafu)?);
        }

        Ok(indexes)
    }

    #[cfg(test)]
    pub fn from_table_name(
        table_definition: Arc<TableDefinition>,
        table_name: RelationName,
    ) -> Self {
        Self {
            table_definition,
            internal_name: Some(table_name),
        }
    }

    /// Verifies that the primary keys match between this table creator and another table creator.
    pub fn verify_primary_keys_match(
        &self,
        other_table: &TableManager,
        tx: &Transaction<'_>,
    ) -> super::Result<bool> {
        let expected_pk_keys_str_map =
            if let Some(constraints) = self.table_definition.constraints.as_ref() {
                get_primary_keys_from_constraints(constraints, &self.table_definition.schema)
                    .into_iter()
                    .collect()
            } else {
                HashSet::new()
            };

        let actual_pk_keys_str_map = other_table.current_primary_keys(tx)?;

        tracing::debug!(
            "Expected primary keys: {:?}\nActual primary keys: {:?}",
            expected_pk_keys_str_map,
            actual_pk_keys_str_map
        );

        let missing_in_actual = expected_pk_keys_str_map
            .difference(&actual_pk_keys_str_map)
            .collect::<Vec<_>>();
        let extra_in_actual = actual_pk_keys_str_map
            .difference(&expected_pk_keys_str_map)
            .collect::<Vec<_>>();

        if !missing_in_actual.is_empty() {
            tracing::warn!(
                "Missing primary key(s) detected for the table '{name}': {:?}.",
                missing_in_actual.iter().join(", "),
                name = self.table_name()
            );
        }

        if !extra_in_actual.is_empty() {
            tracing::warn!(
                "The table '{name}' has unexpected primary key(s) not defined in the configuration: {:?}.",
                extra_in_actual.iter().join(", "),
                name = self.table_name()
            );
        }

        Ok(missing_in_actual.is_empty() && extra_in_actual.is_empty())
    }

    /// Verifies that the indexes match between this table creator and another table creator.
    pub fn verify_indexes_match(
        &self,
        other_table: &TableManager,
        tx: &Transaction<'_>,
    ) -> super::Result<bool> {
        let expected_indexes_str_map: HashSet<String> = self
            .indexes_vec()
            .iter()
            .map(|index| TableManager::get_index_name(self.table_name(), index))
            .collect();

        let actual_indexes_str_map = other_table.current_indexes(tx)?;

        // replace table names for each index with nothing, as table names could be internal and have unique timestamps
        let expected_indexes_str_map = expected_indexes_str_map
            .iter()
            .map(|index| index.replace(&self.table_name().to_string(), ""))
            .collect::<HashSet<_>>();

        let actual_indexes_str_map = actual_indexes_str_map
            .iter()
            .map(|index| index.replace(&other_table.table_name().to_string(), ""))
            .collect::<HashSet<_>>();

        tracing::debug!(
            "Expected indexes: {:?}\nActual indexes: {:?}",
            expected_indexes_str_map,
            actual_indexes_str_map
        );

        let missing_in_actual = expected_indexes_str_map
            .difference(&actual_indexes_str_map)
            .collect::<Vec<_>>();
        let extra_in_actual = actual_indexes_str_map
            .difference(&expected_indexes_str_map)
            .collect::<Vec<_>>();

        if !missing_in_actual.is_empty() {
            tracing::warn!(
                "Missing index(es) detected for the table '{name}': {:?}.",
                missing_in_actual.iter().join(", "),
                name = self.table_name()
            );
        }
        if !extra_in_actual.is_empty() {
            tracing::warn!(
                "Unexpected index(es) detected in table '{name}': {}.\nThese indexes are not defined in the configuration.",
                extra_in_actual.iter().join(", "),
                name = self.table_name()
            );
        }

        Ok(missing_in_actual.is_empty() && extra_in_actual.is_empty())
    }

    /// Returns the current schema in database for this table.
    pub fn current_schema(&self, tx: &Transaction<'_>) -> super::Result<SchemaRef> {
        let sql = format!(
            "SELECT * FROM {table_name} LIMIT 0",
            table_name = quote_identifier(&self.table_name().to_string())
        );
        let mut stmt = tx.prepare(&sql).context(super::UnableToQueryDataSnafu)?;
        let result: duckdb::Arrow<'_> = stmt
            .query_arrow([])
            .context(super::UnableToQueryDataSnafu)?;
        Ok(result.get_schema())
    }

    pub fn get_row_count(&self, tx: &Transaction<'_>) -> super::Result<u64> {
        let sql = format!(
            "SELECT COUNT(1) FROM {table_name}",
            table_name = quote_identifier(&self.table_name().to_string())
        );
        let count = tx
            .query_row(&sql, [], |r| r.get::<usize, u64>(0))
            .context(super::UnableToQueryDataSnafu)?;

        Ok(count)
    }
}

fn create_empty_record_batch_reader(schema: SchemaRef) -> impl RecordBatchReader {
    let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));
    let batches = vec![empty_batch];
    RecordBatchIterator::new(batches.into_iter().map(Ok), schema)
}

#[derive(Debug, Clone)]
pub struct ViewCreator {
    name: RelationName,
}

impl ViewCreator {
    #[must_use]
    pub fn from_name(name: RelationName) -> Self {
        Self { name }
    }

    pub fn insert_into(
        &self,
        table: &TableManager,
        tx: &Transaction<'_>,
        on_conflict: Option<&OnConflict>,
    ) -> super::Result<u64> {
        // insert from this view, into the target table
        let mut insert_sql = format!(
            r#"INSERT INTO "{table_name}" SELECT * FROM "{view_name}""#,
            view_name = self.name,
            table_name = table.table_name()
        );

        if let Some(on_conflict) = on_conflict {
            let on_conflict_sql =
                on_conflict.build_on_conflict_statement(&table.table_definition.schema);
            insert_sql.push_str(&format!(" {on_conflict_sql}"));
        }
        tracing::debug!("{insert_sql}");

        let rows = tx
            .execute(&insert_sql, [])
            .context(super::UnableToInsertToDuckDBTableSnafu)?;

        Ok(rows as u64)
    }

    pub fn drop(&self, tx: &Transaction<'_>) -> super::Result<()> {
        // drop this view
        tx.execute(
            &format!(
                r#"DROP VIEW IF EXISTS "{view_name}""#,
                view_name = self.name
            ),
            [],
        )
        .context(super::UnableToDropDuckDBTableSnafu)?;

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::{
        duckdb::make_initial_table,
        sql::db_connection_pool::{
            dbconnection::duckdbconn::DuckDbConnection, duckdbpool::DuckDbConnectionPool,
        },
    };
    use datafusion::{
        arrow::array::RecordBatch,
        common::SchemaExt,
        datasource::sink::DataSink,
        execution::{SendableRecordBatchStream, TaskContext},
        logical_expr::dml::InsertOp,
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
        physical_plan::memory::MemoryStream,
    };
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use crate::{
        duckdb::write::DuckDBDataSink,
        util::constraints::tests::{get_pk_constraints, get_unique_constraints},
    };

    use super::*;

    pub(crate) fn get_mem_duckdb() -> Arc<DuckDbConnectionPool> {
        Arc::new(
            DuckDbConnectionPool::new_memory().expect("to get a memory duckdb connection pool"),
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
            .collect::<Result<Vec<_>, datafusion::arrow::error::ArrowError>>()
            .expect("to get records")
    }

    fn get_stream_from_batches(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        let schema = batches[0].schema();
        Box::pin(MemoryStream::try_new(batches, schema, None).expect("to get stream"))
    }

    pub(crate) fn init_tracing(default_level: Option<&str>) -> DefaultGuard {
        let filter = match default_level {
            Some(level) => EnvFilter::new(level),
            _ => EnvFilter::new("INFO,datafusion_table_providers=TRACE"),
        };

        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(filter)
            .with_ansi(true)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    pub(crate) fn get_basic_table_definition() -> Arc<TableDefinition> {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));

        Arc::new(TableDefinition::new(
            RelationName::new("test_table"),
            schema,
        ))
    }

    #[ignore = "External parquet data source is currently unavailable (403 Forbidden)"]
    #[tokio::test]
    async fn test_table_creator() {
        let _guard = init_tracing(None);
        let batches = get_logs_batches().await;

        let schema = batches[0].schema();

        let table_definition = Arc::new(
            TableDefinition::new(RelationName::new("eth.logs"), Arc::clone(&schema))
                .with_constraints(get_unique_constraints(
                    &["log_index", "transaction_hash"],
                    Arc::clone(&schema),
                ))
                .with_indexes(vec![
                    (
                        ColumnReference::try_from("block_number").expect("valid column ref"),
                        IndexType::Enabled,
                    ),
                    (
                        ColumnReference::try_from("(log_index, transaction_hash)")
                            .expect("valid column ref"),
                        IndexType::Unique,
                    ),
                ]),
        );

        for overwrite in &[InsertOp::Append, InsertOp::Overwrite] {
            let pool = get_mem_duckdb();

            make_initial_table(Arc::clone(&table_definition), &pool)
                .expect("to make initial table");

            let duckdb_sink = DuckDBDataSink::new(
                Arc::clone(&pool),
                Arc::clone(&table_definition),
                *overwrite,
                None,
                table_definition.schema(),
            );
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

            let tx = conn
                .get_underlying_conn_mut()
                .transaction()
                .expect("should begin transaction");
            let table_creator = if matches!(overwrite, InsertOp::Overwrite) {
                let internal_tables: Vec<(RelationName, u64)> = table_definition
                    .list_internal_tables(&tx)
                    .expect("should list internal tables");
                assert_eq!(internal_tables.len(), 1);

                let internal_table = internal_tables.first().expect("to get internal table");
                let internal_table = internal_table.0.clone();

                TableManager::from_table_name(Arc::clone(&table_definition), internal_table.clone())
            } else {
                let table_creator = TableManager::new(Arc::clone(&table_definition))
                    .with_internal(false)
                    .expect("to create table creator");

                let base_table = table_creator.base_table(&tx).expect("to get base table");
                assert!(base_table.is_some());
                table_creator
            };

            let primary_keys = table_creator
                .current_primary_keys(&tx)
                .expect("should get primary keys");

            assert_eq!(primary_keys, HashSet::<String>::new());

            let created_indexes_str_map = table_creator
                .current_indexes(&tx)
                .expect("should get indexes");

            assert_eq!(
                created_indexes_str_map,
                vec![
                    format!(
                        "i_{table_name}_block_number",
                        table_name = table_creator.table_name()
                    ),
                    format!(
                        "i_{table_name}_log_index_transaction_hash",
                        table_name = table_creator.table_name()
                    )
                ]
                .into_iter()
                .collect::<HashSet<_>>(),
                "Indexes must match"
            );

            tx.rollback().expect("should rollback transaction");
        }
    }

    #[ignore = "External parquet data source is currently unavailable (403 Forbidden)"]
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_table_creator_primary_key() {
        let _guard = init_tracing(None);
        let batches = get_logs_batches().await;

        let schema = batches[0].schema();
        let table_definition = Arc::new(
            TableDefinition::new(RelationName::new("eth.logs"), Arc::clone(&schema))
                .with_constraints(get_pk_constraints(
                    &["log_index", "transaction_hash"],
                    Arc::clone(&schema),
                ))
                .with_indexes(
                    vec![(
                        ColumnReference::try_from("block_number").expect("valid column ref"),
                        IndexType::Enabled,
                    )]
                    .into_iter()
                    .collect(),
                ),
        );

        for overwrite in &[InsertOp::Append, InsertOp::Overwrite] {
            let pool = get_mem_duckdb();

            make_initial_table(Arc::clone(&table_definition), &pool)
                .expect("to make initial table");

            let duckdb_sink = DuckDBDataSink::new(
                Arc::clone(&pool),
                Arc::clone(&table_definition),
                *overwrite,
                None,
                table_definition.schema(),
            );
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

            let tx = conn
                .get_underlying_conn_mut()
                .transaction()
                .expect("should begin transaction");

            let table_creator = if matches!(overwrite, InsertOp::Overwrite) {
                let internal_tables: Vec<(RelationName, u64)> = table_definition
                    .list_internal_tables(&tx)
                    .expect("should list internal tables");
                assert_eq!(internal_tables.len(), 1);

                let internal_table = internal_tables.first().expect("to get internal table");
                let internal_table = internal_table.0.clone();

                TableManager::from_table_name(Arc::clone(&table_definition), internal_table.clone())
            } else {
                let table_creator = TableManager::new(Arc::clone(&table_definition))
                    .with_internal(false)
                    .expect("to create table creator");

                let base_table = table_creator.base_table(&tx).expect("to get base table");
                assert!(base_table.is_some());
                table_creator
            };

            let create_stmt = tx
                .query_row(
                    "select sql from duckdb_tables() where table_name = ?",
                    [table_creator.table_name().to_string()],
                    |r| r.get::<usize, String>(0),
                )
                .expect("to get create table statement");

            assert_eq!(
                create_stmt,
                format!(
                    r#"CREATE TABLE "{table_name}"(log_index BIGINT, transaction_hash VARCHAR, transaction_index BIGINT, address VARCHAR, "data" VARCHAR, topics VARCHAR[], block_timestamp BIGINT, block_hash VARCHAR, block_number BIGINT, PRIMARY KEY(log_index, transaction_hash));"#,
                    table_name = table_creator.table_name(),
                )
            );

            let primary_keys = table_creator
                .current_primary_keys(&tx)
                .expect("should get primary keys");

            assert_eq!(
                primary_keys,
                vec!["log_index".to_string(), "transaction_hash".to_string()]
                    .into_iter()
                    .collect::<HashSet<_>>()
            );

            let created_indexes_str_map = table_creator
                .current_indexes(&tx)
                .expect("should get indexes");

            assert_eq!(
                created_indexes_str_map,
                vec![format!(
                    "i_{table_name}_block_number",
                    table_name = table_creator.table_name()
                )]
                .into_iter()
                .collect::<HashSet<_>>(),
                "Indexes must match"
            );

            tx.rollback().expect("should rollback transaction");
        }
    }

    #[tokio::test]
    async fn test_list_related_tables_from_definition() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make 3 internal tables
        for _ in 0..3 {
            TableManager::new(Arc::clone(&table_definition))
                .with_internal(true)
                .expect("to create table creator")
                .create_table(Arc::clone(&pool), &tx)
                .expect("to create table");
        }

        // using the table definition, list the names of the internal tables
        let table_name = table_definition.name.clone();
        let internal_tables = table_definition
            .list_internal_tables(&tx)
            .expect("should list internal tables");

        assert_eq!(internal_tables.len(), 3);

        // validate the first table is the oldest, and the last table is the newest
        let first_table = internal_tables.first().expect("to get first table");
        let last_table = internal_tables.last().expect("to get last table");
        assert!(first_table.1 < last_table.1);

        // validate none of the internal tables are the same, they are not equal to the base table
        let mut seen_tables = vec![];
        for (internal_table, _) in internal_tables {
            let internal_name = internal_table.clone();
            assert_ne!(&internal_name, &table_name);
            assert!(!seen_tables.contains(&internal_name));
            seen_tables.push(internal_name);
        }

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_list_related_tables_from_creator() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make 3 internal tables
        for _ in 0..3 {
            TableManager::new(Arc::clone(&table_definition))
                .with_internal(true)
                .expect("to create table creator")
                .create_table(Arc::clone(&pool), &tx)
                .expect("to create table");
        }

        // instantiate a new table creator, make it, and list the internal tables
        let table_creator = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let internal_tables = table_creator
            .list_other_internal_tables(&tx)
            .expect("should list internal tables");

        assert_eq!(internal_tables.len(), 3);

        // validate none of the internal tables are the same, they are not equal to the base table, and they are not equal to the internal table that listed them
        let mut seen_tables = vec![];
        for (internal_table, _) in &internal_tables {
            let table_name = internal_table.table_name().clone();
            assert_ne!(&table_name, table_creator.definition_name());
            assert_ne!(Some(&table_name), table_creator.internal_name.as_ref());
            assert!(!seen_tables.contains(&table_name));
            seen_tables.push(table_name);
        }

        // drop the internal tables except the last one
        for (internal_table, _) in internal_tables {
            internal_table.delete_table(&tx).expect("to delete table");
        }

        // list the internal tables again
        let internal_tables = table_creator
            .list_other_internal_tables(&tx)
            .expect("should list internal tables");

        assert_eq!(internal_tables.len(), 0);

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_create_view() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make a table
        let table_creator = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        // create a view from the internal table
        table_creator.create_view(&tx).expect("to create view");

        // check if the view exists
        let view_exists = tx
            .query_row(
                "from duckdb_views() select 1 where view_name = ? and not internal",
                [table_creator.definition_name().to_string()],
                |r| r.get::<usize, i32>(0),
            )
            .expect("to get view");

        assert_eq!(view_exists, 1);

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_insert_into_tables() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make a base table
        let base_table = TableManager::new(Arc::clone(&table_definition))
            .with_internal(false)
            .expect("to create table creator");

        base_table
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        // make an internal table
        let internal_table = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        internal_table
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        // insert some rows directly into the base table
        let insert_stmt = format!(
            r#"INSERT INTO "{base_table}" VALUES (1, 'test'), (2, 'test2')"#,
            base_table = base_table.table_name()
        );

        tx.execute(&insert_stmt, [])
            .expect("to insert into base table");

        // insert from the base table into the internal table
        base_table
            .insert_into(&internal_table, &tx, None)
            .expect("to insert into internal table");

        // check if the rows were inserted
        let rows = tx
            .query_row(
                &format!(
                    r#"SELECT COUNT(1) FROM "{internal_table}""#,
                    internal_table = internal_table.table_name()
                ),
                [],
                |r| r.get::<usize, u64>(0),
            )
            .expect("to get count");

        assert_eq!(rows, 2);

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_lists_base_table_from_definition() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make a base table
        let table_creator = TableManager::new(Arc::clone(&table_definition))
            .with_internal(false)
            .expect("to create table creator");

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        // list the base table from another base table
        let internal_table = TableManager::new(Arc::clone(&table_definition))
            .with_internal(false)
            .expect("to create table creator");

        let base_table = internal_table.base_table(&tx).expect("to get base table");

        assert!(base_table.is_some());
        assert_eq!(
            base_table.expect("to be some").table_definition,
            table_creator.table_definition
        );

        // list the base table from an internal table
        let internal_table = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        let base_table = internal_table.base_table(&tx).expect("to get base table");

        assert!(base_table.is_some());
        assert_eq!(
            base_table.expect("to be some").table_definition,
            table_creator.table_definition
        );

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_primary_keys_match() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));

        let table_definition = Arc::new(
            TableDefinition::new(RelationName::new("test_table"), Arc::clone(&schema))
                .with_constraints(get_pk_constraints(&["id"], Arc::clone(&schema))),
        );

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make 2 internal tables which should have the same indexes
        let table_creator = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let table_creator2 = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator2
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let primary_keys_match = table_creator
            .verify_primary_keys_match(&table_creator2, &tx)
            .expect("to verify primary keys match");

        assert!(primary_keys_match);

        // make another table that does not match
        let table_definition = get_basic_table_definition();

        let table_creator3 = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator3
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let primary_keys_match = table_creator
            .verify_primary_keys_match(&table_creator3, &tx)
            .expect("to verify primary keys match");

        assert!(!primary_keys_match);

        // validate that 2 empty tables return true
        let table_creator4 = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator4
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let primary_keys_match = table_creator3
            .verify_primary_keys_match(&table_creator4, &tx)
            .expect("to verify primary keys match");

        assert!(primary_keys_match);

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_indexes_match() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));

        let table_definition = Arc::new(
            TableDefinition::new(RelationName::new("test_table"), Arc::clone(&schema))
                .with_indexes(
                    vec![(
                        ColumnReference::try_from("id").expect("valid column ref"),
                        IndexType::Enabled,
                    )]
                    .into_iter()
                    .collect(),
                ),
        );

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make 2 internal tables which should have the same indexes
        let table_creator = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        table_creator
            .create_indexes(&tx)
            .expect("to create indexes");

        let table_creator2 = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator2
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        table_creator2
            .create_indexes(&tx)
            .expect("to create indexes");

        let indexes_match = table_creator
            .verify_indexes_match(&table_creator2, &tx)
            .expect("to verify indexes match");

        assert!(indexes_match);

        // make another table that does not match
        let table_definition = get_basic_table_definition();

        let table_creator3 = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator3
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        table_creator3
            .create_indexes(&tx)
            .expect("to create indexes");

        let indexes_match = table_creator
            .verify_indexes_match(&table_creator3, &tx)
            .expect("to verify indexes match");

        assert!(!indexes_match);

        // validate that 2 empty tables return true
        let table_creator4 = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator4
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        table_creator4
            .create_indexes(&tx)
            .expect("to create indexes");

        let indexes_match = table_creator3
            .verify_indexes_match(&table_creator4, &tx)
            .expect("to verify indexes match");

        assert!(indexes_match);

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_current_schema() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        let table_creator = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let schema = table_creator
            .current_schema(&tx)
            .expect("to get current schema");

        assert!(schema.equivalent_names_and_types(&table_definition.schema));

        // schemas between different tables are equivalent
        let table_creator2 = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator2
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let schema2 = table_creator2
            .current_schema(&tx)
            .expect("to get current schema");

        assert!(schema.equivalent_names_and_types(&schema2));

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_internal_tables_exclude_subsets_of_other_tables() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();
        let other_definition = Arc::new(TableDefinition::new(
            RelationName::new("test_table_second"),
            Arc::clone(&table_definition.schema),
        ));

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");

        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        // make an internal table for each definition
        let table_creator = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table creator");

        table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        let other_table_creator = TableManager::new(Arc::clone(&other_definition))
            .with_internal(true)
            .expect("to create table creator");

        other_table_creator
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        // each table should not list the other as an internal table
        let first_tables = table_definition
            .list_internal_tables(&tx)
            .expect("should list internal tables");
        let second_tables = other_definition
            .list_internal_tables(&tx)
            .expect("should list internal tables");

        assert_eq!(first_tables.len(), 1);
        assert_eq!(second_tables.len(), 1);

        assert_ne!(
            first_tables.first().expect("should have a table").0,
            second_tables.first().expect("should have a table").0
        );
    }

    #[tokio::test]
    async fn test_explain_analyze_with_index_and_view() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("age", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, false),
        ]));

        let table_definition = Arc::new(
            TableDefinition::new(RelationName::new("test_table"), Arc::clone(&schema))
                .with_indexes(vec![(
                    ColumnReference::try_from("id").expect("valid column ref"),
                    IndexType::Enabled,
                )]),
        );

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        let table_manager = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table manager");

        table_manager
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        table_manager
            .create_indexes(&tx)
            .expect("to create indexes");

        tx.execute(
            &format!(
                r#"INSERT INTO "{table_name}" VALUES ('Alice', 1, 30, 'active'), ('Bob', 2, 25, 'inactive'), ('Charlie', 3, 35, 'active')"#,
                table_name = table_manager.table_name()
            ),
            [],
        )
        .expect("to insert test data");

        table_manager.create_view(&tx).expect("to create view");

        let queries = [
            format!(
                "EXPLAIN ANALYZE SELECT * FROM {} WHERE id = 1",
                table_definition.name()
            ),
            format!(
                "EXPLAIN ANALYZE SELECT name FROM {} WHERE id = 1",
                table_definition.name()
            ),
        ];

        for (idx, query) in queries.iter().enumerate() {
            let mut stmt = tx.prepare(query).expect("to prepare statement");
            let mut rows = stmt.query([]).expect("to execute query");

            let mut explain_output = Vec::new();
            while let Some(row) = rows.next().expect("to get next row") {
                let line: String = row.get(1).expect("to get explain line");
                explain_output.push(line);
            }

            let explain_result = explain_output.join("\n");

            insta::with_settings!({
                filters => vec![
                    (r"Total Time: \d+\.\d+s", "Total Time: replaced"),
                    (r"\(\d+\.\d+s\)", "(0.00s)"),
                    (r"│__data_test_table_\d+│\n│\s+\d+\s+│", "│__data_test_table_redacted│\n│     redacted     │"),
                ],
            }, {
                insta::assert_snapshot!(format!("explain_analyze_{idx}"), explain_result);
            });
        }

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_explain_analyze_with_multiple_indexes_and_view() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("age", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, false),
        ]));

        let table_definition = Arc::new(
            TableDefinition::new(RelationName::new("test_table"), Arc::clone(&schema))
                .with_indexes(vec![
                    (
                        ColumnReference::try_from("id").expect("valid column ref"),
                        IndexType::Enabled,
                    ),
                    (
                        ColumnReference::try_from("age").expect("valid column ref"),
                        IndexType::Enabled,
                    ),
                    (
                        ColumnReference::try_from("status").expect("valid column ref"),
                        IndexType::Enabled,
                    ),
                ]),
        );

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        let table_manager = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table manager");

        table_manager
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        table_manager
            .create_indexes(&tx)
            .expect("to create indexes");

        tx.execute(
            &format!(
                r#"INSERT INTO "{table_name}" VALUES 
                ('Alice', 1, 30, 'active'), 
                ('Bob', 2, 25, 'inactive'), 
                ('Charlie', 3, 35, 'active'),
                ('David', 4, 30, 'pending'),
                ('Eve', 5, 40, 'active')"#,
                table_name = table_manager.table_name()
            ),
            [],
        )
        .expect("to insert test data");

        table_manager.create_view(&tx).expect("to create view");

        let queries = [
            // Test index on id column
            format!(
                "EXPLAIN ANALYZE SELECT * FROM {} WHERE id = 1",
                table_definition.name()
            ),
            format!(
                "EXPLAIN ANALYZE SELECT name FROM {} WHERE id = 1",
                table_definition.name()
            ),
            format!(
                "EXPLAIN ANALYZE SELECT name, status FROM {} WHERE id = 1",
                table_definition.name()
            ),
            // Test index on age column
            format!(
                "EXPLAIN ANALYZE SELECT * FROM {} WHERE age = 30",
                table_definition.name()
            ),
            format!(
                "EXPLAIN ANALYZE SELECT name FROM {} WHERE age = 30",
                table_definition.name()
            ),
            format!(
                "EXPLAIN ANALYZE SELECT id, name FROM {} WHERE age = 30",
                table_definition.name()
            ),
            // Test index on status column
            format!(
                "EXPLAIN ANALYZE SELECT * FROM {} WHERE status = 'active'",
                table_definition.name()
            ),
            format!(
                "EXPLAIN ANALYZE SELECT name FROM {} WHERE status = 'active'",
                table_definition.name()
            ),
            format!(
                "EXPLAIN ANALYZE SELECT id, age FROM {} WHERE status = 'active'",
                table_definition.name()
            ),
        ];

        for (idx, query) in queries.iter().enumerate() {
            let mut stmt = tx.prepare(query).expect("to prepare statement");
            let mut rows = stmt.query([]).expect("to execute query");

            let mut explain_output = Vec::new();
            while let Some(row) = rows.next().expect("to get next row") {
                let line: String = row.get(1).expect("to get explain line");
                explain_output.push(line);
            }

            let explain_result = explain_output.join("\n");

            insta::with_settings!({
                filters => vec![
                    (r"Total Time: \d+\.\d+s", "Total Time: replaced"),
                    (r"\(\d+\.\d+s\)", "(0.00s)"),
                    (r"│__data_test_table_\d+│\n│\s+\d+\s+│", "│__data_test_table_redacted│\n│     redacted     │"),
                ],
            }, {
                insta::assert_snapshot!(format!("explain_analyze_multiple_indexes_{idx}"), explain_result);
            });
        }

        tx.rollback().expect("should rollback transaction");
    }

    #[tokio::test]
    async fn test_explain_analyze_with_primary_key_and_index_and_view() {
        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("age", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("status", arrow::datatypes::DataType::Utf8, false),
        ]));

        let table_definition = Arc::new(
            TableDefinition::new(RelationName::new("test_table"), Arc::clone(&schema))
                .with_indexes(vec![(
                    ColumnReference::try_from("id").expect("valid column ref"),
                    IndexType::Enabled,
                )])
                .with_constraints(get_pk_constraints(&["name"], Arc::clone(&schema))),
        );

        let mut pool_conn = Arc::clone(&pool).connect_sync().expect("to get connection");
        let conn = pool_conn
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .expect("to downcast to duckdb connection");
        let tx = conn
            .get_underlying_conn_mut()
            .transaction()
            .expect("should begin transaction");

        let table_manager = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table manager");

        table_manager
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        table_manager
            .create_indexes(&tx)
            .expect("to create indexes");

        tx.execute(
            &format!(
                r#"INSERT INTO "{table_name}" VALUES 
                ('Alice', 1, 30, 'active'), 
                ('Bob', 2, 25, 'inactive'), 
                ('Charlie', 3, 35, 'active'),
                ('David', 4, 30, 'pending'),
                ('Eve', 5, 40, 'active')"#,
                table_name = table_manager.table_name()
            ),
            [],
        )
        .expect("to insert test data");

        table_manager.create_view(&tx).expect("to create view");

        let queries = [
            // Test index on id column
            format!(
                "EXPLAIN ANALYZE SELECT * FROM {} WHERE id = 1",
                table_definition.name()
            ),
            format!(
                "EXPLAIN ANALYZE SELECT name FROM {} WHERE id = 1",
                table_definition.name()
            ),
            format!(
                "EXPLAIN ANALYZE SELECT name, status FROM {} WHERE id = 1",
                table_definition.name()
            ),
            // Test primary key
            format!(
                "EXPLAIN ANALYZE SELECT * FROM {} WHERE name = 'Alice'",
                table_definition.name()
            ),
            format!(
                "EXPLAIN ANALYZE SELECT id, status FROM {} WHERE name = 'Bob'",
                table_definition.name()
            ),
        ];

        for (idx, query) in queries.iter().enumerate() {
            let mut stmt = tx.prepare(query).expect("to prepare statement");
            let mut rows = stmt.query([]).expect("to execute query");

            let mut explain_output = Vec::new();
            while let Some(row) = rows.next().expect("to get next row") {
                let line: String = row.get(1).expect("to get explain line");
                explain_output.push(line);
            }

            let explain_result = explain_output.join("\n");

            insta::with_settings!({
                filters => vec![
                    (r"Total Time: \d+\.\d+s", "Total Time: replaced"),
                    (r"\(\d+\.\d+s\)", "(0.00s)"),
                    (r"│__data_test_table_\d+│\n│\s+\d+\s+│", "│__data_test_table_redacted│\n│     redacted     │"),
                ],
            }, {
                insta::assert_snapshot!(format!("explain_analyze_primary_key_and_index_{idx}"), explain_result);
            });
        }

        tx.rollback().expect("should rollback transaction");
    }
}
