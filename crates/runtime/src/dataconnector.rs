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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::OwnedTableReference;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder};
use lazy_static::lazy_static;
use object_store::ObjectStore;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

use secrets::Secret;
use std::future::Future;

#[cfg(feature = "databricks")]
pub mod databricks;
#[cfg(feature = "dremio")]
pub mod dremio;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "flightsql")]
pub mod flightsql;
pub mod localhost;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod s3;
pub mod spiceai;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to scan table provider: {source}"))]
    UnableToScanTableProvider {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to construct logical plan builder: {source}"))]
    UnableToConstructLogicalPlanBuilder {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to build logical plan: {source}"))]
    UnableToBuildLogicalPlan {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to register table provider: {source}"))]
    UnableToRegisterTableProvider {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to create data frame: {source}"))]
    UnableToCreateDataFrame {
        source: datafusion::error::DataFusionError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type AnyErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

type NewDataConnectorResult = AnyErrorResult<Arc<dyn DataConnector>>;

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
        ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>>
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
) -> Option<AnyErrorResult<Arc<dyn DataConnector>>> {
    let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;

    let connector_factory = guard.get(name);

    match connector_factory {
        Some(factory) => Some(factory(secret, params).await),
        None => None,
    }
}

pub async fn register_all() {
    register_connector_factory("localhost", localhost::LocalhostConnector::create).await;
    #[cfg(feature = "databricks")]
    register_connector_factory("databricks", databricks::Databricks::create).await;
    #[cfg(feature = "dremio")]
    register_connector_factory("dremio", dremio::Dremio::create).await;
    #[cfg(feature = "flightsql")]
    register_connector_factory("flightsql", flightsql::FlightSQL::create).await;
    register_connector_factory("s3", s3::S3::create).await;
    register_connector_factory("spiceai", spiceai::SpiceAI::create).await;
    #[cfg(feature = "mysql")]
    register_connector_factory("mysql", mysql::MySQL::create).await;
    #[cfg(feature = "postgres")]
    register_connector_factory("postgres", postgres::Postgres::create).await;
    #[cfg(feature = "duckdb")]
    register_connector_factory("duckdb", duckdb::DuckDB::create).await;
}

pub trait DataConnectorFactory {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>>;
}

/// A `DataConnector` knows how to retrieve and optionally write or stream data.
#[async_trait]
pub trait DataConnector: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    async fn read_provider(&self, dataset: &Dataset) -> AnyErrorResult<Arc<dyn TableProvider>>;

    async fn read_write_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<AnyErrorResult<Arc<dyn TableProvider>>> {
        None
    }

    async fn stream_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<AnyErrorResult<Arc<dyn TableProvider>>> {
        None
    }

    fn get_object_store(
        &self,
        _dataset: &Dataset,
    ) -> Option<AnyErrorResult<(Url, Arc<dyn ObjectStore + 'static>)>> {
        None
    }
}

// Gets all data from a table provider and returns it as a vector of RecordBatches.
pub async fn get_all_data(
    ctx: &mut SessionContext,
    table_name: OwnedTableReference,
    table_provider: Arc<dyn TableProvider>,
    sql: Option<String>,
    filters: Vec<Expr>,
) -> Result<(SchemaRef, Vec<arrow::record_batch::RecordBatch>)> {
    // TODO: handle filters in following PR
    _ = filters;

    let df = match sql {
        None => {
            let table_source = Arc::new(DefaultTableSource::new(Arc::clone(&table_provider)));
            let logical_plan = LogicalPlanBuilder::scan(table_name.clone(), table_source, None)
                .context(UnableToConstructLogicalPlanBuilderSnafu {})?
                .build()
                .context(UnableToBuildLogicalPlanSnafu {})?;

            DataFrame::new(ctx.state(), logical_plan)
        }
        Some(sql) => ctx
            .sql(&sql)
            .await
            .context(UnableToCreateDataFrameSnafu {})?,
    };

    let batches = df.collect().await.context(UnableToScanTableProviderSnafu)?;

    Ok((table_provider.schema(), batches))
}
