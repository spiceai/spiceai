use arrow::record_batch::RecordBatch;

pub mod tract;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub trait Runnable: Send + Sync {
    fn run(
        &self,
        input: Vec<RecordBatch>,
        loopback_size: usize,
    ) -> std::result::Result<RecordBatch, Error>;
}

pub trait ModelRuntime {
    fn load(&self) -> std::result::Result<Box<dyn Runnable>, Error>;
}
