use arrow::record_batch::RecordBatch;
use futures_core::stream::BoxStream;

pub mod debug;

pub struct DataUpdate {
    pub log_sequence_number: u64,
    pub data: Vec<RecordBatch>,
}

pub trait DataSource {
    fn get_data<'a>(&self) -> BoxStream<'a, DataUpdate>;
}
