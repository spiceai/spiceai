use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field};
use async_stream::stream;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::sql::sqlparser::ast::TableFactor;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::{dialect::DuckDbDialect, tokenizer::Tokenizer};
use datafusion::sql::TableReference;
use duckdb::vtab::to_duckdb_type_id;
use duckdb::ToSql;
use duckdb::{Connection, DuckdbConnectionManager};
use dyn_clone::DynClone;
use rand::distr::{Alphanumeric, SampleString};
use snafu::{prelude::*, ResultExt};
use tokio::sync::mpsc::Sender;

use crate::sql::db_connection_pool::runtime::run_sync_with_tokio;
use crate::util::schema::SchemaValidator;
use crate::UnsupportedTypeAction;

use super::DbConnection;
use super::Result;
use super::SyncDbConnection;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDB connection failed.\n{source}\nFor details, refer to the DuckDB manual: https://duckdb.org/docs/"))]
    DuckDBConnectionError { source: duckdb::Error },

    #[snafu(display("Query execution failed.\n{source}\nFor details, refer to the DuckDB manual: https://duckdb.org/docs/"))]
    DuckDBQueryError { source: duckdb::Error },

    #[snafu(display(
        "An unexpected error occurred.\n{message}\nVerify the configuration and try again."
    ))]
    ChannelError { message: String },

    #[snafu(display(
        "Unable to attach DuckDB database {path}.\n{source}\nEnsure the DuckDB file path is valid."
    ))]
    UnableToAttachDatabase {
        path: Arc<str>,
        source: std::io::Error,
    },
}

pub trait DuckDBSyncParameter: ToSql + Sync + Send + DynClone {
    fn as_input_parameter(&self) -> &dyn ToSql;
}

impl<T: ToSql + Sync + Send + DynClone> DuckDBSyncParameter for T {
    fn as_input_parameter(&self) -> &dyn ToSql {
        self
    }
}
dyn_clone::clone_trait_object!(DuckDBSyncParameter);
pub type DuckDBParameter = Box<dyn DuckDBSyncParameter>;

#[derive(Debug)]
pub struct DuckDBAttachments {
    attachments: HashSet<Arc<str>>,
    random_id: String,
    main_db: String,
}

impl DuckDBAttachments {
    /// Creates a new instance of a `DuckDBAttachments`, which instructs DuckDB connections to attach other DuckDB databases for queries.
    #[must_use]
    pub fn new(main_db: &str, attachments: &[Arc<str>]) -> Self {
        let random_id = Alphanumeric.sample_string(&mut rand::rng(), 8);
        let attachments: HashSet<Arc<str>> = attachments.iter().cloned().collect();
        Self {
            attachments,
            random_id,
            main_db: main_db.to_string(),
        }
    }

    /// Returns the search path for the given database and attachments.
    /// The given database needs to be included separately, as search path by default do not include the main database.
    /// The `attachments` parameter represents full attachment names, e.g., ["attachment_zCVN0zYJ_0", ...]
    #[must_use]
    fn get_search_path<'a>(id: &str, attachments: impl IntoIterator<Item = &'a str>) -> Arc<str> {
        let mut path = String::from(id);

        for attachment in attachments {
            path.push(',');
            path.push_str(attachment);
        }

        Arc::from(path)
    }

    /// Sets the search path for the given connection.
    ///
    /// The `attachments` parameter represents full attachment names, e.g., ["attachment_zCVN0zYJ_0", ...]
    /// # Errors
    ///
    /// Returns an error if the search path cannot be set or the connection fails.
    /// Returns search path if successful.
    pub fn set_search_path<'a>(
        &self,
        conn: &Connection,
        attachments: impl IntoIterator<Item = &'a str>,
    ) -> Result<Arc<str>> {
        let search_path = Self::get_search_path(&self.main_db, attachments);

        tracing::trace!("Setting search_path to {search_path}");

        conn.execute(&format!("SET search_path ='{}'", search_path), [])
            .context(DuckDBConnectionSnafu)?;
        Ok(search_path)
    }

    /// Resets the search path for the given connection to default.
    ///
    /// # Errors
    ///
    /// Returns an error if the search path cannot be set or the connection fails.
    pub fn reset_search_path(&self, conn: &Connection) -> Result<()> {
        conn.execute("RESET search_path", [])
            .context(DuckDBConnectionSnafu)?;
        Ok(())
    }

    /// Attaches the databases to the given connection and sets the search path for the newly attached databases.
    /// If connection already contains attachments, it will skip the attachments override (including search_path).
    ///
    /// # Errors
    ///
    /// Returns an error if a specific attachment is missing, cannot be attached, search path cannot be set or the connection fails.
    /// Returns search path if successful.
    pub fn attach(&self, conn: &Connection) -> Result<Arc<str>> {
        // Check if attachments already exist; skip attachments override in this case as it requires changing the search_path
        let mut stmt = conn
            .prepare("PRAGMA database_list;")
            .context(DuckDBConnectionSnafu)?;
        let mut rows = stmt.query([]).context(DuckDBConnectionSnafu)?;

        let mut existing_attachments = std::collections::HashMap::new();
        while let Some(row) = rows.next()? {
            let db_name: String = row.get(1)?;
            let db_path: Option<String> = row.get(2)?;
            if db_name.starts_with("attachment_") {
                // attachment always has a path so it is safe to use unwrap_or_default
                existing_attachments.insert(db_path.unwrap_or_default(), db_name);
            }
        }

        // Check if the connection already contains the desired attachments
        if !existing_attachments.is_empty() {
            tracing::trace!(
                "Attachments {:?} creation skipped as connection contains existing attachments: {existing_attachments:?}",
                self.attachments
            );
            for db in &self.attachments {
                if !existing_attachments.contains_key(db.as_ref()) {
                    // Only partial attachments may be available in some cases (for example, right after pool creation).
                    // This does not always indicate an error, so we log it at debug level.
                    tracing::debug!("{db} not found among existing attachments");
                }
            }
            // The connection can have attachments but not the search_path, so we must set it based on the existing attachment names
            return self.set_search_path(conn, existing_attachments.values().map(|s| s.as_str()));
        }

        let mut created_attachments = Vec::new();

        for (i, db) in self.attachments.iter().enumerate() {
            // check the db file exists
            std::fs::metadata(db.as_ref()).context(UnableToAttachDatabaseSnafu {
                path: Arc::clone(db),
            })?;
            let attachment_name = Self::get_attachment_name(&self.random_id, i);
            let sql = format!("ATTACH IF NOT EXISTS '{db}' AS {attachment_name} (READ_ONLY)");
            tracing::trace!("Attaching {db} using: {sql}");
            conn.execute(&sql, []).context(DuckDBConnectionSnafu)?;
            created_attachments.push(attachment_name);
        }

        self.set_search_path(conn, created_attachments.iter().map(|s| s.as_str()))
    }

    /// Detaches the databases from the given connection and resets the search path to default.
    ///
    /// # Errors
    ///
    /// Returns an error if an attachment cannot be detached, search path cannot be set or the connection fails.
    pub fn detach(&self, conn: &Connection) -> Result<()> {
        for (i, _) in self.attachments.iter().enumerate() {
            conn.execute(
                &format!(
                    "DETACH DATABASE IF EXISTS {}",
                    Self::get_attachment_name(&self.random_id, i)
                ),
                [],
            )
            .context(DuckDBConnectionSnafu)?;
        }

        self.reset_search_path(conn)?;
        Ok(())
    }

    #[must_use]
    fn get_attachment_name(random_id: &str, index: usize) -> String {
        format!("attachment_{random_id}_{index}")
    }
}

pub struct DuckDbConnection {
    pub conn: r2d2::PooledConnection<DuckdbConnectionManager>,
    attachments: Option<Arc<DuckDBAttachments>>,
    unsupported_type_action: UnsupportedTypeAction,
    connection_setup_queries: Vec<Arc<str>>,
}

impl SchemaValidator for DuckDbConnection {
    type Error = super::Error;

    fn is_data_type_supported(data_type: &DataType) -> bool {
        match data_type {
            DataType::List(inner_field)
            | DataType::FixedSizeList(inner_field, _)
            | DataType::LargeList(inner_field) => {
                match inner_field.data_type() {
                    dt if dt.is_primitive() => true,
                    DataType::Utf8
                    | DataType::Binary
                    | DataType::Utf8View
                    | DataType::BinaryView
                    | DataType::Boolean => true,
                    _ => false, // nested lists don't support anything else yet
                }
            }
            DataType::Struct(inner_fields) => inner_fields
                .iter()
                .all(|field| Self::is_data_type_supported(field.data_type())),
            _ => true,
        }
    }

    fn is_field_supported(field: &Arc<Field>) -> bool {
        let duckdb_type_id = to_duckdb_type_id(field.data_type());
        Self::is_data_type_supported(field.data_type()) && duckdb_type_id.is_ok()
    }

    fn unsupported_type_error(data_type: &DataType, field_name: &str) -> Self::Error {
        super::Error::UnsupportedDataType {
            data_type: data_type.to_string(),
            field_name: field_name.to_string(),
        }
    }
}

impl DuckDbConnection {
    pub fn get_underlying_conn_mut(
        &mut self,
    ) -> &mut r2d2::PooledConnection<DuckdbConnectionManager> {
        &mut self.conn
    }

    #[must_use]
    pub fn with_unsupported_type_action(
        mut self,
        unsupported_type_action: UnsupportedTypeAction,
    ) -> Self {
        self.unsupported_type_action = unsupported_type_action;
        self
    }

    #[must_use]
    pub fn with_attachments(mut self, attachments: Option<Arc<DuckDBAttachments>>) -> Self {
        self.attachments = attachments;
        self
    }

    #[must_use]
    pub fn with_connection_setup_queries(mut self, queries: Vec<Arc<str>>) -> Self {
        self.connection_setup_queries = queries;
        self
    }

    /// Passthrough if Option is Some for `DuckDBAttachments::attach`
    ///
    /// # Errors
    ///
    /// See `DuckDBAttachments::attach` for more information.
    pub fn attach(conn: &Connection, attachments: &Option<Arc<DuckDBAttachments>>) -> Result<()> {
        if let Some(attachments) = attachments {
            attachments.attach(conn)?;
        }
        Ok(())
    }

    /// Passthrough if Option is Some for `DuckDBAttachments::detach`
    ///
    /// # Errors
    ///
    /// See `DuckDBAttachments::detach` for more information.
    pub fn detach(conn: &Connection, attachments: &Option<Arc<DuckDBAttachments>>) -> Result<()> {
        if let Some(attachments) = attachments {
            attachments.detach(conn)?;
        }
        Ok(())
    }

    fn apply_connection_setup_queries(&self, conn: &Connection) -> Result<()> {
        for query in self.connection_setup_queries.iter() {
            conn.execute(query, []).context(DuckDBConnectionSnafu)?;
        }
        Ok(())
    }
}

impl DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>
    for DuckDbConnection
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_sync(
        &self,
    ) -> Option<
        &dyn SyncDbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>,
    > {
        Some(self)
    }
}

impl SyncDbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>
    for DuckDbConnection
{
    fn new(conn: r2d2::PooledConnection<DuckdbConnectionManager>) -> Self {
        DuckDbConnection {
            conn,
            attachments: None,
            unsupported_type_action: UnsupportedTypeAction::default(),
            connection_setup_queries: Vec::new(),
        }
    }

    fn tables(&self, schema: &str) -> Result<Vec<String>, super::Error> {
        let sql = "SELECT table_name FROM information_schema.tables \
                  WHERE table_schema = ? AND table_type = 'BASE TABLE'";

        let mut stmt = self
            .conn
            .prepare(sql)
            .boxed()
            .context(super::UnableToGetTablesSnafu)?;
        let mut rows = stmt
            .query([schema])
            .boxed()
            .context(super::UnableToGetTablesSnafu)?;
        let mut tables = vec![];

        while let Some(row) = rows.next().boxed().context(super::UnableToGetTablesSnafu)? {
            tables.push(row.get(0).boxed().context(super::UnableToGetTablesSnafu)?);
        }

        Ok(tables)
    }

    fn schemas(&self) -> Result<Vec<String>, super::Error> {
        let sql = "SELECT DISTINCT schema_name FROM information_schema.schemata \
                  WHERE schema_name NOT IN ('information_schema', 'pg_catalog')";

        let mut stmt = self
            .conn
            .prepare(sql)
            .boxed()
            .context(super::UnableToGetSchemasSnafu)?;
        let mut rows = stmt
            .query([])
            .boxed()
            .context(super::UnableToGetSchemasSnafu)?;
        let mut schemas = vec![];

        while let Some(row) = rows
            .next()
            .boxed()
            .context(super::UnableToGetSchemasSnafu)?
        {
            schemas.push(row.get(0).boxed().context(super::UnableToGetSchemasSnafu)?);
        }

        Ok(schemas)
    }

    fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef, super::Error> {
        let table_str = if is_table_function(table_reference) {
            table_reference.to_string()
        } else {
            table_reference.to_quoted_string()
        };
        let mut stmt = self
            .conn
            .prepare(&format!("SELECT * FROM {table_str} LIMIT 0"))
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let result: duckdb::Arrow<'_> = stmt
            .query_arrow([])
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        Self::handle_unsupported_schema(&result.get_schema(), self.unsupported_type_action)
    }

    fn query_arrow(
        &self,
        sql: &str,
        params: &[DuckDBParameter],
        _projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream> {
        let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel::<RecordBatch>(4);

        let conn = self.conn.try_clone()?;
        Self::attach(&conn, &self.attachments)?;
        self.apply_connection_setup_queries(&conn)?;

        let fetch_schema_sql =
            format!("WITH fetch_schema AS ({sql}) SELECT * FROM fetch_schema LIMIT 0");
        let mut stmt = conn
            .prepare(&fetch_schema_sql)
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let result: duckdb::Arrow<'_> = stmt
            .query_arrow([])
            .boxed()
            .context(super::UnableToGetSchemaSnafu)?;

        let schema = result.get_schema();

        let params = params.iter().map(dyn_clone::clone).collect::<Vec<_>>();

        let sql = sql.to_string();

        let cloned_schema = schema.clone();

        let create_stream = || -> Result<SendableRecordBatchStream> {
            let join_handle = tokio::task::spawn_blocking(move || {
                let mut stmt = conn.prepare(&sql).context(DuckDBQuerySnafu)?;
                let params: &[&dyn ToSql] = &params
                    .iter()
                    .map(|f| f.as_input_parameter())
                    .collect::<Vec<_>>();
                let result: duckdb::ArrowStream<'_> = stmt
                    .stream_arrow(params, cloned_schema)
                    .context(DuckDBQuerySnafu)?;
                for i in result {
                    blocking_channel_send(&batch_tx, i)?;
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            });

            let output_stream = stream! {
                while let Some(batch) = batch_rx.recv().await {
                    yield Ok(batch);
                }

                match join_handle.await {
                    Ok(Err(task_error)) => {
                        yield Err(DataFusionError::Execution(format!(
                            "Failed to execute DuckDB query: {task_error}"
                        )))
                    },
                    Err(join_error) => {
                        yield Err(DataFusionError::Execution(format!(
                            "Failed to execute DuckDB query: {join_error}"
                        )))
                    },
                    _ => {}
                }
            };

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                output_stream,
            )))
        };

        run_sync_with_tokio(create_stream)
    }

    fn execute(&self, sql: &str, params: &[DuckDBParameter]) -> Result<u64> {
        let params: &[&dyn ToSql] = &params
            .iter()
            .map(|f| f.as_input_parameter())
            .collect::<Vec<_>>();

        let rows_modified = self.conn.execute(sql, params).context(DuckDBQuerySnafu)?;
        Ok(rows_modified as u64)
    }
}

fn blocking_channel_send<T>(channel: &Sender<T>, item: T) -> Result<()> {
    match channel.blocking_send(item) {
        Ok(()) => Ok(()),
        Err(e) => Err(Error::ChannelError {
            message: format!("{e}"),
        }
        .into()),
    }
}

#[must_use]
pub fn flatten_table_function_name(table_reference: &TableReference) -> String {
    let table_name = table_reference.table();
    let filtered_name: String = table_name
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '(')
        .collect();
    let result = filtered_name.replace('(', "_");

    format!("{result}__view")
}

#[must_use]
pub fn is_table_function(table_reference: &TableReference) -> bool {
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
    use arrow_schema::{DataType, Field, Fields, SchemaBuilder};
    use tempfile::tempdir;

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

    #[test]
    fn test_field_is_unsupported() {
        // A list with a struct is not supported
        let field = Field::new(
            "list_struct",
            DataType::List(Arc::new(Field::new(
                "struct",
                DataType::Struct(vec![Field::new("field", DataType::Int64, false)].into()),
                false,
            ))),
            false,
        );

        assert!(
            !DuckDbConnection::is_data_type_supported(field.data_type()),
            "list with struct should be unsupported"
        );
    }

    #[test]
    fn test_fields_are_supported() {
        // test that the usual field types are supported, string, numbers, etc
        let fields = vec![
            Field::new("string", DataType::Utf8, false),
            Field::new("int", DataType::Int64, false),
            Field::new("float", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("binary", DataType::Binary, false),
        ];

        for field in fields {
            assert!(
                DuckDbConnection::is_data_type_supported(field.data_type()),
                "field should be supported"
            );
        }
    }

    #[test]
    fn test_schema_rebuild_with_supported_fields() {
        let fields = vec![
            Field::new("string", DataType::Utf8, false),
            Field::new("int", DataType::Int64, false),
            Field::new("float", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("binary", DataType::Binary, false),
        ];

        let schema = Arc::new(SchemaBuilder::from(Fields::from(fields)).finish());

        let rebuilt_schema =
            DuckDbConnection::handle_unsupported_schema(&schema, UnsupportedTypeAction::Error)
                .expect("should rebuild schema successfully");

        assert_eq!(schema, rebuilt_schema);
    }

    #[test]
    fn test_schema_rebuild_with_unsupported_fields() {
        let fields = vec![
            Field::new("string", DataType::Utf8, false),
            Field::new("int", DataType::Int64, false),
            Field::new("float", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("binary", DataType::Binary, false),
            Field::new(
                "list_struct",
                DataType::List(Arc::new(Field::new(
                    "struct",
                    DataType::Struct(vec![Field::new("field", DataType::Int64, false)].into()),
                    false,
                ))),
                false,
            ),
            Field::new("another_bool", DataType::Boolean, false),
            Field::new(
                "another_list_struct",
                DataType::List(Arc::new(Field::new(
                    "struct",
                    DataType::Struct(vec![Field::new("field", DataType::Int64, false)].into()),
                    false,
                ))),
                false,
            ),
            Field::new("another_float", DataType::Float32, false),
        ];

        let rebuilt_fields = vec![
            Field::new("string", DataType::Utf8, false),
            Field::new("int", DataType::Int64, false),
            Field::new("float", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("binary", DataType::Binary, false),
            // this also tests that ordering is preserved when rebuilding the schema with removed fields
            Field::new("another_bool", DataType::Boolean, false),
            Field::new("another_float", DataType::Float32, false),
        ];

        let schema = Arc::new(SchemaBuilder::from(Fields::from(fields)).finish());
        let expected_rebuilt_schema =
            Arc::new(SchemaBuilder::from(Fields::from(rebuilt_fields)).finish());

        assert!(
            DuckDbConnection::handle_unsupported_schema(&schema, UnsupportedTypeAction::Error)
                .is_err()
        );

        let rebuilt_schema =
            DuckDbConnection::handle_unsupported_schema(&schema, UnsupportedTypeAction::Warn)
                .expect("should rebuild schema successfully");

        assert_eq!(rebuilt_schema, expected_rebuilt_schema);

        let rebuilt_schema =
            DuckDbConnection::handle_unsupported_schema(&schema, UnsupportedTypeAction::Ignore)
                .expect("should rebuild schema successfully");

        assert_eq!(rebuilt_schema, expected_rebuilt_schema);
    }

    #[test]
    fn test_duckdb_attachments_deduplication() {
        let db1 = Arc::from("db1.duckdb");
        let db2 = Arc::from("db2.duckdb");
        let db3 = Arc::from("db3.duckdb");

        // Create attachments with duplicates
        let attachments = vec![
            Arc::clone(&db1),
            Arc::clone(&db2),
            Arc::clone(&db1), // duplicate of db1
            Arc::clone(&db3),
            Arc::clone(&db2), // duplicate of db2
        ];

        let duckdb_attachments = DuckDBAttachments::new("main_db", &attachments);

        // Verify that duplicates are removed
        assert_eq!(duckdb_attachments.attachments.len(), 3);
        assert!(duckdb_attachments.attachments.contains(&db1));
        assert!(duckdb_attachments.attachments.contains(&db2));
        assert!(duckdb_attachments.attachments.contains(&db3));
    }

    #[test]
    fn test_duckdb_attachments_search_path() -> Result<()> {
        let temp_dir = tempdir()?;
        let db1: Arc<str> = temp_dir
            .path()
            .join("db1.duckdb")
            .to_str()
            .expect("to convert path to str")
            .into();
        let db2: Arc<str> = temp_dir
            .path()
            .join("db2.duckdb")
            .to_str()
            .expect("to convert path to str")
            .into();
        let db3: Arc<str> = temp_dir
            .path()
            .join("db3.duckdb")
            .to_str()
            .expect("to convert path to str")
            .into();

        for db in [&db1, &db2, &db3] {
            let conn1 = Connection::open(db.as_ref())?;
            conn1.execute("CREATE TABLE test1 (id INTEGER, name VARCHAR)", [])?;
        }

        // Create attachments with duplicates
        let attachments = vec![
            Arc::clone(&db1),
            Arc::clone(&db2),
            Arc::clone(&db1), // duplicate of db1
            Arc::clone(&db3),
            Arc::clone(&db2), // duplicate of db2
        ];

        let duckdb_attachments = DuckDBAttachments::new("main", &attachments);

        let conn = Connection::open_in_memory()?;

        let search_path = duckdb_attachments.attach(&conn)?;

        // Verify that the search path contains the main database and unique attachments

        assert!(search_path.starts_with("main"));
        assert!(search_path.contains("attachment_"));
        assert_eq!(search_path.split(',').count(), 4); // main + 3 unique attachments

        Ok(())
    }

    #[test]
    fn test_duckdb_attachments_empty() -> Result<()> {
        let duckdb_attachments = DuckDBAttachments::new("main", &[]);

        // Verify empty attachments
        assert!(duckdb_attachments.attachments.is_empty());

        // Verify search path only contains main database

        let conn = Connection::open_in_memory()?;

        let search_path = duckdb_attachments.attach(&conn)?;
        assert_eq!(search_path, "main".into());

        Ok(())
    }

    #[test]
    fn test_duckdb_attachments_with_real_files() -> Result<()> {
        // Create a temporary directory for our test files
        let temp_dir = tempdir()?;
        let db1_path = temp_dir.path().join("db1.duckdb");
        let db2_path = temp_dir.path().join("db2.duckdb");

        // Create two test databases with some data
        {
            let conn1 = Connection::open(&db1_path)?;
            conn1.execute("CREATE TABLE test1 (id INTEGER, name VARCHAR)", [])?;
            conn1.execute("INSERT INTO test1 VALUES (1, 'test1_1')", [])?;

            let conn2 = Connection::open(&db2_path)?;
            conn2.execute("CREATE TABLE test2 (id INTEGER, name VARCHAR)", [])?;
            conn2.execute("INSERT INTO test2 VALUES (2, 'test2_1')", [])?;
        }

        // Create attachments with duplicates
        let attachments = vec![
            Arc::from(db1_path.to_str().unwrap()),
            Arc::from(db2_path.to_str().unwrap()),
            Arc::from(db1_path.to_str().unwrap()), // duplicate of db1
        ];

        // Create a new in-memory DuckDB connection
        let conn = Connection::open_in_memory()?;

        // Create DuckDBAttachments and attach the databases
        let duckdb_attachments = DuckDBAttachments::new("main", &attachments);
        duckdb_attachments.attach(&conn)?;

        // Verify we can query data from both databases
        let result1: (i64, String) = conn
            .query_row("SELECT * FROM test1 LIMIT 1", [], |row| {
                Ok((
                    row.get::<_, i64>(0).expect("to get i64"),
                    row.get::<_, String>(1).expect("to get string"),
                ))
            })
            .expect("to get result");
        let result2: (i64, String) = conn
            .query_row("SELECT * FROM test2 LIMIT 1", [], |row| {
                Ok((
                    row.get::<_, i64>(0).expect("to get i64"),
                    row.get::<_, String>(1).expect("to get string"),
                ))
            })
            .expect("to get result");

        assert_eq!(result1, (1, "test1_1".to_string()));
        assert_eq!(result2, (2, "test2_1".to_string()));

        // Verify the search path
        let search_path: String = conn
            .query_row("SELECT current_setting('search_path');", [], |row| {
                Ok(row.get::<_, String>(0).expect("to get string"))
            })
            .expect("to get search path");
        assert!(search_path.contains("main"));
        assert!(search_path.contains("attachment_"));

        // Clean up
        duckdb_attachments.detach(&conn)?;
        Ok(())
    }

    #[test]
    fn test_duckdb_attach_multiple_times() -> Result<()> {
        // Create a temporary directory for our test files
        let temp_dir = tempdir()?;
        let db1_path = temp_dir.path().join("db1.duckdb");
        let db2_path = temp_dir.path().join("db2.duckdb");

        // Create two test databases with some data
        {
            let conn1 = Connection::open(&db1_path)?;
            conn1.execute("CREATE TABLE test1 (id INTEGER, name VARCHAR)", [])?;
            conn1.execute("INSERT INTO test1 VALUES (1, 'test1_1')", [])?;

            let conn2 = Connection::open(&db2_path)?;
            conn2.execute("CREATE TABLE test2 (id INTEGER, name VARCHAR)", [])?;
            conn2.execute("INSERT INTO test2 VALUES (2, 'test2_1')", [])?;
        }

        let attachments = vec![
            Arc::from(db1_path.to_str().expect("to convert path top str")),
            Arc::from(db2_path.to_str().expect("to convert path top str")),
        ];

        let conn = Connection::open_in_memory()?;

        // Simulate attaching to the same connection multiple times
        DuckDBAttachments::new("main", &attachments).attach(&conn)?;
        DuckDBAttachments::new("main", &attachments).attach(&conn)?;
        DuckDBAttachments::new("main", &attachments).attach(&conn)?;

        let join_result: (i64, String, i64, String) = conn
            .query_row(
                "SELECT t1.id, t1.name, t2.id, t2.name FROM test1 t1, test2 t2",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0).expect("to get i64"),
                        row.get::<_, String>(1).expect("to get string"),
                        row.get::<_, i64>(2).expect("to get i64"),
                        row.get::<_, String>(3).expect("to get string"),
                    ))
                },
            )
            .expect("to get join result");

        assert_eq!(
            join_result,
            (1, "test1_1".to_string(), 2, "test2_1".to_string())
        );

        Ok(())
    }
}
