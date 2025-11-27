pub mod connection;
pub mod connection_pool;
pub mod table;
pub mod utils;

use crate::mongodb::connection_pool::MongoDBConnectionPool;
use crate::mongodb::table::MongoDBTable;
use arrow_schema::ArrowError;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use snafu::prelude::*;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid MongoDB URI: {source}"))]
    InvalidUri { source: mongodb::error::Error },

    #[snafu(display("Invalid value for parameter {parameter_name}. Ensure the value is valid for parameter {parameter_name}"))]
    InvalidParameter { parameter_name: String },

    #[snafu(display("TLS root certificate path is invalid: {path}"))]
    InvalidRootCertPath { path: String },

    #[snafu(display("Failed to connect to MongoDB: {source}"))]
    ConnectionFailed { source: mongodb::error::Error },

    #[snafu(display("Unable to get tables: {source}"))]
    UnableToGetTables {
        source: Box<dyn std::error::Error + std::marker::Send + Sync>,
    },

    #[snafu(display("Unable to get schema: {source}"))]
    UnableToGetSchema {
        source: Box<dyn std::error::Error + std::marker::Send + Sync>,
    },

    #[snafu(display("Unable to get schemas: {source}"))]
    UnableToGetSchemas {
        source: Box<dyn std::error::Error + std::marker::Send + Sync>,
    },

    #[snafu(display("Failed to execute MongoDB query: {source}"))]
    QueryError {
        source: Box<dyn std::error::Error + std::marker::Send + Sync>,
    },

    #[snafu(display("Failed to convert MongoDB documents to Arrow: {source}"))]
    ConversionError {
        source: Box<dyn std::error::Error + std::marker::Send + Sync>,
    },

    #[snafu(display("Invalid decimal parameters: {source}"))]
    InvalidDecimalError { source: ArrowError },

    #[snafu(display("Authentication failed. Verify username and password."))]
    InvalidUsernameOrPassword,

    #[snafu(display("Invalid document access: {message}"))]
    InvalidDocumentAccess { message: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct MongoDBTableFactory {
    pool: Arc<MongoDBConnectionPool>,
}

impl MongoDBTableFactory {
    #[must_use]
    pub fn new(pool: Arc<MongoDBConnectionPool>) -> Self {
        Self { pool }
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let table_provider = Arc::new(
            MongoDBTable::new(&pool, table_reference)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }
}
