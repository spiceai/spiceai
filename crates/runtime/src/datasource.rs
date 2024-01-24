use arrow::record_batch::RecordBatch;
use futures_core::stream::BoxStream;

use crate::auth::Auth;

pub mod debug;

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

/// A DataSource knows how to retrieve data for a given dataset.
///
/// Implementing `get_all_data` is required, but `stream_data_updates` & `supports_data_streaming` is optional.
/// If `stream_data_updates` is not supported for a dataset, the runtime will fall back to polling `get_all_data` and returning a
/// DataUpdate that is constructed like:
///
/// ```rust
/// DataUpdate {
///    log_sequence_number: None,
///    data: get_all_data(dataset),
///    update_type: UpdateType::Overwrite,
/// }
/// ```
pub trait DataSource {
    /// Create a new DataSource with the given Auth.
    fn new<T: Auth>(auth: T) -> Self;
    /// Returns true if the given dataset supports streaming by this DataSource.
    fn supports_data_streaming(&self, dataset: &str) -> bool {
        false
    }
    /// Returns a stream of DataUpdates for the given dataset.
    fn stream_data_updates<'a>(&self, dataset: &str) -> BoxStream<'a, DataUpdate> {
        panic!("stream_data_updates not implemented for {}", dataset)
    }
    /// Returns all data for the given dataset.
    fn get_all_data(&self, dataset: &str) -> Vec<RecordBatch>;
}
