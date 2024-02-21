use std::any::Any;

use datafusion::{
    arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream, sql::TableReference,
};

pub mod duckdbconn;
pub mod postgresconn;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = Error> = std::result::Result<T, E>;

pub trait DbConnection<T: r2d2::ManageConnection, P> {
    fn new(conn: r2d2::PooledConnection<T>) -> Self
    where
        Self: Sized;
    fn get_schema(&mut self, table_reference: &TableReference) -> Result<SchemaRef>;
    fn query_arrow(&mut self, sql: &str, params: &[P]) -> Result<SendableRecordBatchStream>;
    fn execute(&mut self, sql: &str, params: &[P]) -> Result<u64>;

    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}
