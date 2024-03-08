use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream, sql::TableReference,
};

pub mod duckdbconn;
pub mod postgresconn;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = Error> = std::result::Result<T, E>;

pub trait SyncDbConnection<T, P>: DbConnection<T, P> {
    fn new(conn: T) -> Self
    where
        Self: Sized;
    fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef>;
    fn query_arrow(&self, sql: &str, params: &[P]) -> Result<SendableRecordBatchStream>;
    fn execute(&self, sql: &str, params: &[P]) -> Result<u64>;
}

#[async_trait::async_trait]
pub trait AsyncDbConnection<T, P>: DbConnection<T, P> + Sync {
    fn new(conn: T) -> Self
    where
        Self: Sized;
    async fn get_schema(&self, table_reference: &TableReference) -> Result<SchemaRef>;
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

pub async fn get_schema<T, P>(
    conn: Box<dyn DbConnection<T, P>>,
    table_reference: &datafusion::sql::TableReference<'_>,
) -> Result<Arc<arrow::datatypes::Schema>, Error> {
    let schema = if let Some(conn) = conn.as_sync() {
        conn.get_schema(table_reference)?
    } else if let Some(conn) = conn.as_async() {
        conn.get_schema(table_reference).await?
    } else {
        return Err(Error::from("Unable to downcast connection"));
    };
    Ok(schema)
}

pub async fn query_arrow<T, P>(
    conn: Box<dyn DbConnection<T, P>>,
    sql: String,
) -> Result<SendableRecordBatchStream> {
    if let Some(conn) = conn.as_sync() {
        conn.query_arrow(&sql, &[])
    } else if let Some(conn) = conn.as_async() {
        conn.query_arrow(&sql, &[]).await
    } else {
        Err(Error::from("Unable to downcast connection"))
    }
}
