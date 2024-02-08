use arrow::record_batch::RecordBatch;
use snafu::prelude::*;
pub mod tract;

#[derive(Debug, Snafu)]
pub enum Error {
    TractError { source: tract_core::anyhow::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait Runnable {
    fn run(&self) -> Result<RecordBatch>;
}

pub trait ModelRuntime {
    fn load(&self) -> Result<Box<dyn Runnable>>;
}
