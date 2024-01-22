use arrow::record_batch::RecordBatch;
use snafu::prelude::*;
use std::future::Future;

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
    fn get_data(&self) -> impl Future<Output = Result<DataUpdate>> + Send;
}
