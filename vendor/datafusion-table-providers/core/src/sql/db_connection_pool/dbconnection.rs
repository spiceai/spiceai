use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream, sql::TableReference,
};
use snafu::prelude::*;

#[cfg(feature = "adbc")]
pub mod adbcconn;
#[cfg(feature = "clickhouse")]
pub mod clickhouseconn;
#[cfg(feature = "duckdb")]
pub mod duckdbconn;
#[cfg(feature = "mysql")]
pub mod mysqlconn;
#[cfg(feature = "odbc")]
pub mod odbcconn;
#[cfg(feature = "postgres")]
pub mod postgresconn;
#[cfg(feature = "sqlite")]
pub mod sqliteconn;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = GenericError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to downcast connection.\nReport a bug to request support: https://github.com/datafusion-contrib/datafusion-table-providers/issues"))]
    UnableToDowncastConnection {},

    #[snafu(display("{source}"))]
    UnableToGetSchema { source: GenericError },

    #[snafu(display("The field '{field_name}' has an unsupported data type: {data_type}."))]
    UnsupportedDataType {
        data_type: String,
        field_name: String,
    },

    #[snafu(display("Failed to execute query.\n{source}"))]
    UnableToQueryArrow { source: GenericError },

    #[snafu(display(
        "Table '{table_name}' not found. Ensure the table name is correctly spelled."
    ))]
    UndefinedTable {
        table_name: String,
        source: GenericError,
    },

    #[snafu(display("Unable to get schemas: {source}"))]
    UnableToGetSchemas { source: GenericError },

    #[snafu(display("Unable to get tables: {source}"))]
    UnableToGetTables { source: GenericError },
}

pub trait SyncDbConnection<T, P>: DbConnection<T, P> {
    fn new(conn: T) -> Self
    where
        Self: Sized;

    fn tables(&self, schema: &str) -> Result<Vec<String>, Error>;

    fn schemas(&self) -> Result<Vec<String>, Error>;

    /// Get the schema for a table reference.
    ///
    /// # Arguments
    ///
    /// * `table_reference` - The table reference.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be retrieved.
    fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef, Error>;

    /// Query the database with the given SQL statement and parameters, returning a `Result` of `SendableRecordBatchStream`.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statement.
    /// * `params` - The parameters for the SQL statement.
    /// * `projected_schema` - The Projected schema for the query.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    fn query_arrow(
        &self,
        sql: &str,
        params: &[P],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream>;

    /// Execute the given SQL statement with parameters, returning the number of affected rows.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statement.
    /// * `params` - The parameters for the SQL statement.
    ///
    /// # Errors
    ///
    /// Returns an error if the execution fails.
    fn execute(&self, sql: &str, params: &[P]) -> Result<u64>;
}

#[async_trait::async_trait]
pub trait AsyncDbConnection<T, P>: DbConnection<T, P> + Sync {
    fn new(conn: T) -> Self
    where
        Self: Sized;

    async fn tables(&self, schema: &str) -> Result<Vec<String>, Error>;

    async fn schemas(&self) -> Result<Vec<String>, Error>;

    /// Get the schema for a table reference.
    ///
    /// # Arguments
    ///
    /// * `table_reference` - The table reference.
    async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef, Error>;

    /// Query the database with the given SQL statement and parameters, returning a `Result` of `SendableRecordBatchStream`.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statement.
    /// * `params` - The parameters for the SQL statement.
    /// * `projected_schema` - The Projected schema for the query.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    async fn query_arrow(
        &self,
        sql: &str,
        params: &[P],
        projected_schema: Option<SchemaRef>,
    ) -> Result<SendableRecordBatchStream>;

    /// Execute the given SQL statement with parameters, returning the number of affected rows.
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL statement.
    /// * `params` - The parameters for the SQL statement.
    async fn execute(&self, sql: &str, params: &[P]) -> Result<u64>;
}

pub trait DbConnection<T, P>: Send {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;

    fn as_sync(&self) -> Option<&dyn SyncDbConnection<T, P>> {
        None
    }
    fn as_async(&self) -> Option<&dyn AsyncDbConnection<T, P>> {
        None
    }
}

pub async fn get_tables<T, P>(
    conn: Box<dyn DbConnection<T, P>>,
    schema: &str,
) -> Result<Vec<String>, Error> {
    let schema = if let Some(conn) = conn.as_sync() {
        conn.tables(schema)?
    } else if let Some(conn) = conn.as_async() {
        conn.tables(schema).await?
    } else {
        return Err(Error::UnableToDowncastConnection {});
    };
    Ok(schema)
}

/// Get the schemas for the database.
///
/// # Errors
///
/// Returns an error if the schemas cannot be retrieved.
pub async fn get_schemas<T, P>(conn: Box<dyn DbConnection<T, P>>) -> Result<Vec<String>, Error> {
    let schema = if let Some(conn) = conn.as_sync() {
        conn.schemas()?
    } else if let Some(conn) = conn.as_async() {
        conn.schemas().await?
    } else {
        return Err(Error::UnableToDowncastConnection {});
    };
    Ok(schema)
}

/// Get the schema for a table reference.
///
/// # Arguments
///
/// * `conn` - The database connection.
/// * `table_reference` - The table reference.
///
/// # Errors
///
/// Returns an error if the schema cannot be retrieved.
pub async fn get_schema<T, P>(
    conn: Box<dyn DbConnection<T, P>>,
    table_reference: &datafusion::sql::TableReference,
) -> Result<Arc<datafusion::arrow::datatypes::Schema>, Error> {
    let schema = if let Some(conn) = conn.as_sync() {
        conn.get_schema(table_reference)?
    } else if let Some(conn) = conn.as_async() {
        conn.get_schema(table_reference).await?
    } else {
        return Err(Error::UnableToDowncastConnection {});
    };
    Ok(schema)
}

/// Query the database with the given SQL statement and parameters, returning a `Result` of `SendableRecordBatchStream`.
///
/// # Arguments
///
/// * `conn` - The database connection.
/// * `sql` - The SQL statement.
///
/// # Errors
///
/// Returns an error if the query fails.
pub async fn query_arrow<T, P>(
    conn: Box<dyn DbConnection<T, P>>,
    sql: String,
    projected_schema: Option<SchemaRef>,
) -> Result<SendableRecordBatchStream, Error> {
    if let Some(conn) = conn.as_sync() {
        conn.query_arrow(&sql, &[], projected_schema)
            .context(UnableToQueryArrowSnafu {})
    } else if let Some(conn) = conn.as_async() {
        conn.query_arrow(&sql, &[], projected_schema)
            .await
            .context(UnableToQueryArrowSnafu {})
    } else {
        Err(Error::UnableToDowncastConnection {})
    }
}
