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

use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream, sql::TableReference,
};
use snafu::prelude::*;

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
#[cfg(feature = "snowflake")]
pub mod snowflakeconn;
#[cfg(feature = "sqlite")]
pub mod sqliteconn;

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = GenericError> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to downcast connection"))]
    UnableToDowncastConnection {},

    #[snafu(display("Unable to get schema: {source}"))]
    UnableToGetSchema { source: GenericError },

    #[snafu(display("Unable to query arrow: {source}"))]
    UnableToQueryArrow { source: GenericError },

    #[snafu(display("Table {table_name} not found. Ensure the table name is correctly spelled."))]
    UndefinedTable {
        table_name: String,
        source: GenericError,
    },
}

pub trait SyncDbConnection<T, P>: DbConnection<T, P> {
    fn new(conn: T) -> Self
    where
        Self: Sized;

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
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    fn query_arrow(&self, sql: &str, params: &[P]) -> Result<SendableRecordBatchStream>;

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
    async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef, Error>;
    async fn query_arrow(&self, sql: &str, params: &[P]) -> Result<SendableRecordBatchStream>;
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
) -> Result<Arc<arrow::datatypes::Schema>, Error> {
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
) -> Result<SendableRecordBatchStream, Error> {
    if let Some(conn) = conn.as_sync() {
        conn.query_arrow(&sql, &[])
            .context(UnableToQueryArrowSnafu {})
    } else if let Some(conn) = conn.as_async() {
        conn.query_arrow(&sql, &[])
            .await
            .context(UnableToQueryArrowSnafu {})
    } else {
        return Err(Error::UnableToDowncastConnection {});
    }
}
