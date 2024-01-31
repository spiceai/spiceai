use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_stream::stream;
use futures_core::stream::BoxStream;
use std::future::Future;

use crate::auth::AuthProvider;
use crate::dataupdate::{DataUpdate, UpdateType};

pub mod debug;
pub mod dremio;
pub mod flight;
pub mod spiceai;

#[derive(Debug, Snafu)]
pub enum Error {
    UnableToCreateDataSource {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A `DataSource` knows how to retrieve data for a given dataset.
///
/// Implementing `get_all_data` is required, but `stream_data_updates` & `supports_data_streaming` is optional.
/// If `stream_data_updates` is not supported for a dataset, the runtime will fall back to polling `get_all_data` and returning a
/// `DataUpdate` that is constructed like:
///
/// ```rust
/// DataUpdate {
///    log_sequence_number: None,
///    data: get_all_data(dataset),
///    update_type: UpdateType::Overwrite,
/// }
/// ```
pub trait DataSource: Send + Sync {
    /// Create a new `DataSource` with the given `AuthProvider`.
    fn new(
        auth_provider: Box<dyn AuthProvider>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = Result<Self>>>>
    where
        Self: Sized;

    /// Returns true if the given dataset supports streaming by this `DataSource`.
    fn supports_data_streaming(&self, _dataset: &Dataset) -> bool {
        false
    }

    /// Returns a stream of `DataUpdates` for the given dataset.
    fn stream_data_updates<'a>(&self, dataset: &Dataset) -> BoxStream<'a, DataUpdate> {
        panic!("stream_data_updates not implemented for {}", dataset.name)
    }

    /// Returns all data for the given dataset.
    fn get_all_data(
        &self,
        dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<RecordBatch>> + Send>>;
}

impl dyn DataSource + '_ {
    pub fn get_data<'a>(&'a self, dataset: &'a Dataset) -> BoxStream<'_, DataUpdate> {
        if self.supports_data_streaming(dataset) {
            return self.stream_data_updates(dataset);
        }

        // If a refresh_interval is defined, refresh the data on that interval.
        if let Some(refresh_interval) = dataset.refresh_interval() {
            return Box::pin(stream! {
                loop {
                    tokio::time::sleep(refresh_interval).await;
                    yield DataUpdate {
                        log_sequence_number: None,
                        data: self.get_all_data(dataset).await,
                        update_type: UpdateType::Overwrite,
                    };
                }
            });
        }

        // Otherwise, just return the data once.
        Box::pin(stream! {
            yield DataUpdate {
                log_sequence_number: None,
                data: self.get_all_data(dataset).await,
                update_type: UpdateType::Overwrite,
            };
        })
    }
}
