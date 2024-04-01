/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use futures::stream;
use lazy_static::lazy_static;
use object_store::ObjectStore;
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::RefreshMode;
use spicepod::component::dataset::Dataset;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

use arrow::record_batch::RecordBatch;
use async_stream::stream;
use futures_core::stream::BoxStream;
use secrets::Secret;
use std::future::Future;

use crate::datapublisher::DataPublisher;
use crate::dataupdate::{DataUpdate, UpdateType};
use crate::status;
use crate::timing::TimeMeasurement;

pub mod databricks;
pub mod dremio;
pub mod flight;
pub mod flightsql;
pub mod mysql;
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

type NewDataConnectorResult = Result<Box<dyn DataConnector>>;

type NewDataConnectorFn = dyn Fn(
        Option<Secret>,
        Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>>
    + Send;

lazy_static! {
    static ref DATA_CONNECTOR_FACTORY_REGISTRY: Mutex<HashMap<String, Box<NewDataConnectorFn>>> =
        Mutex::new(HashMap::new());
}

pub async fn register_connector_factory(
    name: &str,
    connector_factory: impl Fn(
            Option<Secret>,
            Arc<Option<HashMap<String, String>>>,
        ) -> Pin<Box<dyn Future<Output = Result<Box<dyn DataConnector>>> + Send>>
        + Send
        + 'static,
) {
    let mut registry = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;

    registry.insert(name.to_string(), Box::new(connector_factory));
}

/// Create a new `DataConnector` by name.
///
/// # Returns
///
/// `None` if the connector for `name` is not registered, otherwise a `Result` containing the result of calling the constructor to create a `DataConnector`.
#[allow(clippy::implicit_hasher)]
pub async fn create_new_connector(
    name: &str,
    secret: Option<Secret>,
    params: Arc<Option<HashMap<String, String>>>,
) -> Option<Result<Box<dyn DataConnector>>> {
    let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;

    let connector_factory = guard.get(name);

    match connector_factory {
        Some(factory) => Some(factory(secret, params).await),
        None => None,
    }
}

pub async fn register_all() {
    tokio::join!(
        register_connector_factory("databricks", databricks::Databricks::create),
        register_connector_factory("dremio", dremio::Dremio::create),
        register_connector_factory("flightsql", flightsql::FlightSQL::create),
        register_connector_factory("postgres", postgres::Postgres::create),
        register_connector_factory("mysql", mysql::MySQL::create),
        register_connector_factory("s3", s3::S3::create),
        register_connector_factory("spiceai", spiceai::SpiceAI::create),
    );
}

pub trait DataConnectorFactory {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>>;
}

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
                    status::update_dataset(dataset.name.clone(), status::ComponentStatus::Refreshing);
                    let timer = TimeMeasurement::new("load_dataset_duration_ms", vec![("dataset", dataset.name.clone())]);
                    let new_data = self.get_all_data(dataset).await;
                    status::update_dataset(dataset.name.clone(), status::ComponentStatus::Ready);
                    yield DataUpdate {
                        data: new_data,
                        update_type: UpdateType::Overwrite,
                    };
                    drop(timer);
                    tokio::time::sleep(refresh_interval).await;
                }
            });
        }

        tracing::trace!("stream::once");
        // Otherwise, just return the data once.
        Box::pin(stream::once(async move {
            let timer = TimeMeasurement::new(
                "load_dataset_duration_ms",
                vec![("dataset", dataset.name.clone())],
            );
            status::update_dataset(dataset.name.clone(), status::ComponentStatus::Refreshing);
            let data = self.get_all_data(dataset).await;
            status::update_dataset(dataset.name.clone(), status::ComponentStatus::Ready);
            drop(timer);

            DataUpdate {
                data,
                update_type: UpdateType::Overwrite,
            }
        }))
    }
}
