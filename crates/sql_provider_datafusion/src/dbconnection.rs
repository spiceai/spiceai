use ::duckdb::arrow::array::RecordBatch;
use datafusion::{arrow::datatypes::SchemaRef, sql::TableReference};
use snafu::prelude::*;

pub mod duckdbconn;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub trait DbConnection<T: r2d2::ManageConnection> {
    fn new(conn: r2d2::PooledConnection<T>) -> Self
    where
        Self: Sized;
    fn get_schema(&mut self, table_reference: &TableReference) -> Result<SchemaRef>;
    fn query_arrow(&mut self, sql: &str) -> Result<Vec<RecordBatch>>;
    fn execute(&mut self, sql: &str) -> Result<u64>;
}
