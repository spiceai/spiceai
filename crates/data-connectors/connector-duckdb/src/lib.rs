/*
Copyright 2026 The Spice.ai OSS Authors

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

//! `DuckDB` data connector for Spice.ai runtime.
//!
//! This crate provides the `DuckDB` connector implementation, allowing
//! Spice.ai to connect to `DuckDB` databases as data sources.
//!
//! This connector is extracted from the runtime crate to enable faster
//! incremental builds - changes to this connector only require rebuilding
//! this crate, not the entire runtime.

use async_trait::async_trait;
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::UnsupportedTypeAction;
use datafusion_table_providers::duckdb::DuckDBTableFactory;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::is_table_function;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use runtime::component::dataset::Dataset;
use runtime::dataconnector::{
    AnyErrorResult, ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError,
    DataConnectorFactory, DataConnectorResult,
};
use runtime::datafusion::dialect::new_duckdb_dialect;
use runtime::parameters::ParameterSpec;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: open. Specify a DuckDB file with the `open` parameter"
    ))]
    MissingDuckDBFile,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `DuckDB` data connector.
pub struct DuckDB {
    duckdb_factory: DuckDBTableFactory,
}

impl std::fmt::Debug for DuckDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDB").finish_non_exhaustive()
    }
}

impl DuckDB {
    /// Creates an in-memory `DuckDB` table factory.
    ///
    /// # Errors
    ///
    /// Returns an error if the in-memory `DuckDB` connection cannot be established.
    pub fn create_in_memory(params: &ConnectorParams) -> AnyErrorResult<DuckDBTableFactory> {
        let pool = Arc::new(
            DuckDbConnectionPool::new_memory()
                .map_err(|source| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "duckdb".to_string(),
                    connector_component: params.component.clone(),
                    source,
                })?
                .with_unsupported_type_action(
                    params
                        .unsupported_type_action
                        .unwrap_or(UnsupportedTypeAction::Error),
                ),
        );

        Ok(DuckDBTableFactory::new(pool).with_dialect(new_duckdb_dialect()))
    }

    /// Creates a file-based `DuckDB` table factory.
    ///
    /// # Errors
    ///
    /// Returns an error if the file-based `DuckDB` connection cannot be established.
    pub fn create_file(path: &str, params: &ConnectorParams) -> AnyErrorResult<DuckDBTableFactory> {
        let pool = Arc::new(
            DuckDbConnectionPool::new_file(path, &AccessMode::ReadOnly)
                .map_err(|source| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "duckdb".to_string(),
                    connector_component: params.component.clone(),
                    source,
                })?
                .with_unsupported_type_action(
                    params
                        .unsupported_type_action
                        .unwrap_or(UnsupportedTypeAction::Error),
                ),
        );

        Ok(DuckDBTableFactory::new(pool).with_dialect(new_duckdb_dialect()))
    }
}

/// Factory for creating `DuckDB` connector instances.
#[derive(Default, Copy, Clone)]
pub struct DuckDBFactory {}

impl DuckDBFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[ParameterSpec::component("open")];

impl DataConnectorFactory for DuckDBFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = runtime::dataconnector::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let duckdb_factory =
                if let Some(db_path) = params.parameters.clone().get("open").expose().ok() {
                    DuckDB::create_file(db_path, &params)?
                } else {
                    DuckDB::create_in_memory(&params)?
                };

            Ok(Arc::new(DuckDB { duckdb_factory }) as Arc<dyn DataConnector>)
        })
    }

    fn supports_unsupported_type_action(&self) -> bool {
        true
    }

    fn prefix(&self) -> &'static str {
        "duckdb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "duckdb";

/// Returns a new instance of the `DuckDB` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    DuckDBFactory::new_arc()
}

#[derive(Debug, Snafu)]
enum ReadProviderError {
    #[snafu(display("Unable to get read provider for {dataconnector}: {source}"))]
    UnableToGetReadProvider {
        dataconnector: &'static str,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl From<ReadProviderError> for DataConnectorError {
    fn from(err: ReadProviderError) -> Self {
        match err {
            ReadProviderError::UnableToGetReadProvider {
                dataconnector,
                connector_component,
                source,
            } => DataConnectorError::UnableToGetReadProvider {
                dataconnector: dataconnector.to_string(),
                connector_component,
                source,
            },
        }
    }
}

#[async_trait]
impl DataConnector for DuckDB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let path: TableReference = dataset.path().into();

        if !(is_table_function(&path) || dataset.params.contains_key("duckdb_open")) {
            return Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "duckdb".to_string(),
                source: Box::new(Error::MissingDuckDBFile {}),
                connector_component: ConnectorComponent::from(dataset),
            });
        }

        Ok(Read::table_provider(&self.duckdb_factory, path)
            .await
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "duckdb",
                connector_component: ConnectorComponent::from(dataset),
            })?)
    }
}
