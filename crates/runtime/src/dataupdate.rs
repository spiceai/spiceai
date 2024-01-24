use arrow::record_batch::RecordBatch;

pub enum UpdateType {
    Append,
    Overwrite,
}

pub struct DataUpdate {
    /// The unique identifier associated with this DataUpdate.
    /// If the runtime sees two DataUpdates with the same log_sequence_number,
    /// it will remove the data associated with the first DataUpdate, and replace it with the second.
    ///
    /// A None value will disable the de-duplication logic.
    pub log_sequence_number: Option<u64>,
    pub data: Vec<RecordBatch>,
    /// The type of update to perform.
    /// If UpdateType::Append, the runtime will append the data to the existing dataset.
    /// If UpdateType::Overwrite, the runtime will overwrite the existing data with the new data.
    pub update_type: UpdateType,
}
