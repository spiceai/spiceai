use arrow::record_batch::RecordBatch;
use std::result::Result;

pub mod tract;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub trait Runnable: Send + Sync {
    fn run(&self, input: Vec<RecordBatch>, loopback_size: usize) -> Result<RecordBatch, Error>;
}

pub trait ModelRuntime {
    fn load(&self) -> Result<Box<dyn Runnable>, Error>;
}
