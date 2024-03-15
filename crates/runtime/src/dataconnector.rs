use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use futures::stream;
use object_store::ObjectStore;
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::RefreshMode;
use spicepod::component::dataset::Dataset;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use url::Url;

use arrow::record_batch::RecordBatch;
use async_stream::stream;
use futures_core::stream::BoxStream;
use secrets::Secret;
use std::future::Future;

use crate::datapublisher::DataPublisher;
use crate::dataupdate::{DataUpdate, UpdateType};

pub mod databricks;
pub mod debug;
pub mod dremio;
pub mod flight;
pub mod flightsql;
pub mod postgres;
pub mod s3;
pub mod spiceai;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create data connector: {source}"))]
    UnableToCreateDataConnector {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to get table provider: {source}"))]
    UnableToGetTableProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type AnyErrorResult = std::result::Result<(), Box<dyn std::error::Error>>;

/// A `DataConnector` knows how to retrieve and modify data for a given dataset.
///
/// Implementing `get_all_data` is required, but `stream_data_updates` & `supports_data_streaming` is optional.
/// If `stream_data_updates` is not supported for a dataset, the runtime will fall back to polling `get_all_data` and returning a
/// `DataUpdate` that is constructed like:
///
/// ```rust
/// DataUpdate {
///    data: get_all_data(dataset),
///    update_type: UpdateType::Overwrite,
/// }
/// ```
#[async_trait]
pub trait DataConnector: Send + Sync {
    /// Create a new `DataConnector` with the given `Secret`.
    fn new(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = Result<Self>> + Send>>
    where
        Self: Sized;

    /// Returns true if the given dataset supports streaming by this `DataConnector`.
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

    fn get_data_publisher(&self) -> Option<Box<dyn DataPublisher>> {
        None
    }

    fn has_table_provider(&self) -> bool {
        false
    }

    fn has_object_store(&self) -> bool {
        false
    }

    fn get_object_store(
        &self,
        dataset: &Dataset,
    ) -> std::result::Result<(Url, Arc<dyn ObjectStore + 'static>), Error> {
        panic!("get_object_store not implemented for {}", dataset.name)
    }

    async fn get_table_provider(
        &self,
        dataset: &Dataset,
    ) -> Result<Arc<dyn TableProvider + 'static>> {
        panic!("get_table_provider not implemented for {}", dataset.name)
    }
}

impl dyn DataConnector + '_ {
    pub fn get_data<'a>(&'a self, dataset: &'a Dataset) -> BoxStream<'_, DataUpdate> {
        let refresh_mode = dataset
            .acceleration
            .as_ref()
            .map_or(RefreshMode::Full, |acc| {
                acc.refresh_mode
                    .as_ref()
                    .map_or(RefreshMode::Full, Clone::clone)
            });

        if refresh_mode == RefreshMode::Append && self.supports_data_streaming(dataset) {
            return self.stream_data_updates(dataset);
        }

        // If a refresh_interval is defined, refresh the data on that interval.
        if let Some(refresh_interval) = dataset.refresh_interval() {
            tracing::trace!("stream::interval");
            return Box::pin(stream! {
                loop {
                    tracing::info!("Refreshing data for {}", dataset.name);
                    yield DataUpdate {
                        data: self.get_all_data(dataset).await,
                        update_type: UpdateType::Overwrite,
                    };
                    tokio::time::sleep(refresh_interval).await;
                }
            });
        }

        tracing::trace!("stream::once");
        // Otherwise, just return the data once.
        Box::pin(stream::once(async move {
            DataUpdate {
                data: self.get_all_data(dataset).await,
                update_type: UpdateType::Overwrite,
            }
        }))
    }
}
