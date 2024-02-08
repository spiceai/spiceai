use arrow::record_batch::RecordBatch;
use snafu::prelude::*;

pub mod tract;

#[derive(Debug, Snafu)]
pub enum Error {
    TractError { source: tract_core::anyhow::Error },

    ArrowError { source: arrow::error::ArrowError },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait Runnable: Send + Sync {
    fn run(&self, input: Vec<RecordBatch>) -> Result<RecordBatch>;
}

pub trait ModelRuntime {
    fn load(&self) -> Result<Box<dyn Runnable>>;
}