use async_trait::async_trait;
use data_components::{Read, Stream, Write};
use datafusion::common::OwnedTableReference;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use futures::{stream, StreamExt};
use lazy_static::lazy_static;
use object_store::ObjectStore;
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::RefreshMode;
use spicepod::component::dataset::Dataset;
use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

use async_stream::stream;
use futures_core::stream::BoxStream;
use secrets::Secret;
use std::future::Future;

use crate::dataupdate::{DataUpdate, UpdateType};
use crate::timing::TimeMeasurement;
use data_components::databricks::Databricks;

pub mod databricks;
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

    #[snafu(display("Unable to scan table provider: {source}"))]
    UnableToScanTableProvider {
        source: datafusion::error::DataFusionError,
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
        register_connector_factory("databricks", Databricks::create),
        register_connector_factory("dremio", dremio::Dremio::create),
        register_connector_factory("flightsql", flightsql::FlightSQL::create),
        register_connector_factory("postgres", postgres::Postgres::create),
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
    fn as_any(&self) -> &dyn Any;

    fn read_provider(&self) -> &dyn Read;

    fn write_provider(&self) -> Option<&dyn Write> {
        None
    }

    fn stream_provider(&self) -> Option<&dyn Stream> {
        None
    }

    fn has_object_store(&self) -> bool {
        false
    }

    fn get_object_store(&self, dataset: &Dataset) -> Result<(Url, Arc<dyn ObjectStore + 'static>)> {
        panic!("get_object_store not implemented for {}", dataset.name)
    }
}

// Gets all data from a table provider and returns it as a vector of RecordBatches.
pub async fn get_all_data(
    table_provider: &dyn TableProvider,
) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    let ctx = SessionContext::new();
    let plan = table_provider
        .scan(&ctx.state(), None, &[], None)
        .await
        .context(UnableToScanTableProviderSnafu {})?;

    let mut batches = vec![];
    for i in 0..plan.output_partitioning().partition_count() {
        let mut partition = plan
            .execute(i, ctx.task_ctx())
            .context(UnableToScanTableProviderSnafu {})?;
        while let Some(batch) = partition.next().await {
            batches.push(batch.context(UnableToScanTableProviderSnafu {})?);
        }
    }

    Ok(batches)
}

impl dyn DataConnector + '_ {
    pub async fn get_data<'a>(&'a self, dataset: &'a Dataset) -> BoxStream<'_, Result<DataUpdate>> {
        // let refresh_mode = dataset
        //     .acceleration
        //     .as_ref()
        //     .map_or(RefreshMode::Full, |acc| {
        //         acc.refresh_mode
        //             .as_ref()
        //             .map_or(RefreshMode::Full, Clone::clone)
        //     });

        // TODOREARCH
        // if refresh_mode == RefreshMode::Append {
        //     if let Some(stream_provider) = self.stream_provider() {
        //         return self.stream_data_updates(dataset);
        //     }
        // }

        let read_provider = self.read_provider();

        let table_provider = match read_provider
            .table_provider(OwnedTableReference::from(dataset.name.clone()))
            .await
        {
            Ok(provider) => provider,
            Err(e) => {
                tracing::error!("Unable to get read data for for {}", dataset.name);
                tracing::debug!("Error getting table provider for {}: {e:?}", dataset.name);
                return Box::pin(stream::empty());
            }
        };

        // If a refresh_interval is defined, refresh the data on that interval.
        if let Some(refresh_interval) = dataset.refresh_interval() {
            tracing::trace!("stream::interval");
            return Box::pin(stream! {
                loop {
                    tracing::info!("Refreshing data for {}", dataset.name);
                    let timer = TimeMeasurement::new("load_dataset_duration_ms", vec![("dataset", dataset.name.clone())]);
                    let all_data = match get_all_data(table_provider.as_ref()).await {
                        Ok(data) => data,
                        Err(e) => {
                            tracing::error!("Error refreshing data for {}: {e}", dataset.name);
                            yield Err(e);
                            continue;
                        }
                    };
                    yield Ok(DataUpdate {
                        data: all_data,
                        update_type: UpdateType::Overwrite,
                    });
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
            let all_data = match get_all_data(table_provider.as_ref()).await {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!("Error refreshing data for {}: {e}", dataset.name);
                    return Err(e);
                }
            };
            drop(timer);

            Ok(DataUpdate {
                data: all_data,
                update_type: UpdateType::Overwrite,
            })
        }))
    }
}
