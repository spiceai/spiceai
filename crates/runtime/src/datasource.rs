use arrow::record_batch::RecordBatch;
use futures_core::Stream;
use snafu::prelude::*;

pub mod debug;
// mod spicefirecache;

#[derive(Debug, Snafu)]
pub enum Error {
    DataSource {},
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DataUpdate {
    pub log_sequence_number: u64,
    pub data: Vec<RecordBatch>,
}

pub trait DataSource {
    fn get_data(&self) -> impl Stream<Item = DataUpdate> + Send;
}
