use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone)]
pub enum UpdateType {
    Append,
    Overwrite,
}

#[derive(Debug, Clone)]
pub struct DataUpdate {
    pub data: Vec<RecordBatch>,
    /// The type of update to perform.
    /// If UpdateType::Append, the runtime will append the data to the existing dataset.
    /// If UpdateType::Overwrite, the runtime will overwrite the existing data with the new data.
    pub update_type: UpdateType,
}
