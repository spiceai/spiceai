use ::duckdb::arrow::array::RecordBatch;
use datafusion::{arrow::datatypes::SchemaRef, sql::TableReference};

pub mod duckdbconn;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub trait DbConnection<T: r2d2::ManageConnection, P> {
    fn new(conn: r2d2::PooledConnection<T>) -> Self
    where
        Self: Sized;
    fn get_schema(&mut self, table_reference: &TableReference) -> Result<SchemaRef>;
    fn query_arrow(&mut self, sql: &str, params: &[P]) -> Result<Vec<RecordBatch>>;
    fn execute(&mut self, sql: &str, params: &[P]) -> Result<u64>;
}
