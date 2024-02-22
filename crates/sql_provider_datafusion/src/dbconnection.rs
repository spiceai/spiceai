use std::sync::Arc;
use std::{any::Any, pin::Pin, task::Poll};

use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::{arrow::datatypes::SchemaRef, execution::RecordBatchStream, sql::TableReference};
use futures::StreamExt;
use futures::{stream::BoxStream, Stream};

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

pub struct FlightStream<'a> {
    inner: BoxStream<
        'a,
        std::result::Result<arrow::array::RecordBatch, datafusion::error::DataFusionError>,
    >,
    schema: SchemaRef,
}

impl<'a> FlightStream<'a> {
    pub fn new(
        inner: BoxStream<
            'a,
            std::result::Result<arrow::array::RecordBatch, datafusion::error::DataFusionError>,
        >,
        schema: SchemaRef,
    ) -> Self {
        Self { inner, schema }
    }
}

impl Stream for FlightStream<'_> {
    type Item = std::result::Result<arrow::array::RecordBatch, datafusion::error::DataFusionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<datafusion::error::Result<arrow::array::RecordBatch>>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl RecordBatchStream for FlightStream<'_> {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
