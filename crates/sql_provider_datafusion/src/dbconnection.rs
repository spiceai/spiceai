use ::duckdb_rs::arrow::array::RecordBatch;
use datafusion::{arrow::datatypes::SchemaRef, sql::TableReference};
use snafu::prelude::*;

pub mod duckdb;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb_rs::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub trait DbConnection<T: r2d2::ManageConnection> {
    fn new(conn: r2d2::PooledConnection<T>) -> Self
    where
        Self: Sized;
    fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef>;
    fn query_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>>;
    fn execute(&self, sql: &str) -> Result<usize>;
}
