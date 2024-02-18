use arrow::record_batch::RecordBatch;
use std::result::Result;

pub mod tract;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A `Runnable` is a loaded model in a particular `ModelFormat`.
///
/// Implementing `run` is required. It takes a `Vec<RecordBatch>` and returns a arrow `RecordBatch`
pub trait Runnable: Send + Sync {
    // Run inference with the input and loaded model
    fn run(&self, input: Vec<RecordBatch>, loopback_size: usize) -> Result<RecordBatch, Error>;
}


/// A `ModelRuntime` loads a model into it supported `ModelFormat`.
/// Currently only `Tract` + `Onnx` is supported
///
/// Implementing `load` is required, which returns a `Runnable` in a particular `ModelFormat`.
pub trait ModelRuntime {
    // Load the model into the runtime and return a runnable
    // TODO: add format parameter when more formats are supported
    fn load(&self) -> Result<Box<dyn Runnable>, Error>;
}
