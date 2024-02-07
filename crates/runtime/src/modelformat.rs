pub mod onnx;
use arrow::record_batch::RecordBatch;
use std::pin::Pin;
use std::sync::Arc;

use std::{collections::HashMap, future::Future};

use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create model format"))]
    UnableToCreateModelFormat { source: Box<dyn std::error::Error> },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait ModelFormat {
    fn new(
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = Result<Self>>>>
    where
        Self: Sized;

    fn load_model(&self, model_name: &str) -> Result<Box<dyn Runnable>>;
}

pub trait Runnable {
    fn run(&self, inputs: Vec<RecordBatch>) -> Result<Vec<RecordBatch>>;
}
