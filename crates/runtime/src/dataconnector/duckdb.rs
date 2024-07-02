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

use crate::component::dataset::Dataset;
use crate::secrets::Secret;
use async_trait::async_trait;
use data_components::duckdb::DuckDBTableFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use db_connection_pool::dbconnection::duckdbconn::is_table_function;
use db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{AnyErrorResult, DataConnector, DataConnectorError, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create DuckDB connection pool: {source}"))]
    UnableToCreateDuckDBConnectionPool { source: db_connection_pool::Error },

    #[snafu(display("Missing required parameter: open"))]
    MissingDuckDBFile,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDB {
    file_factory: Option<Arc<DuckDBTableFactory>>,
    memory_factory: DuckDBTableFactory,
}

impl DuckDB {
    /// Returns true if the [`Dataset`] should be loaded in the in-memory [`DuckDbConnectionPool`].
    pub(crate) fn use_memory(dataset: &Dataset) -> bool {
        let path: TableReference = dataset.path().into();
        let has_db_file = dataset.params.contains_key("open");

        !has_db_file && is_table_function(&path)
    }

    pub(crate) fn create_in_memory() -> AnyErrorResult<DuckDBTableFactory> {
        let pool = Arc::new(
            DuckDbConnectionPool::new_memory(&AccessMode::Automatic).map_err(|source| {
                DataConnectorError::UnableToConnectInternal {
                    dataconnector: "duckdb".to_string(),
                    source,
                }
            })?,
        );

        Ok(DuckDBTableFactory::new(pool))
    }

    pub(crate) fn create_file(path: &str) -> AnyErrorResult<DuckDBTableFactory> {
        let pool = Arc::new(
            DuckDbConnectionPool::new_file(path, &AccessMode::ReadOnly).map_err(|source| {
                DataConnectorError::UnableToConnectInternal {
                    dataconnector: "duckdb".to_string(),
                    source,
                }
            })?,
        );

        Ok(DuckDBTableFactory::new(pool))
    }
}

impl DataConnectorFactory for DuckDB {
    fn create(
        _secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let file_factory = if let Some(db_path) = params.get("open") {
                let file_factory = Self::create_file(db_path)?;
                Some(Arc::new(file_factory))
            } else {
                None
            };

            let memory_factory = Self::create_in_memory().map_err(|e| {
                DataConnectorError::UnableToConnectInternal {
                    dataconnector: "duckdb".to_string(),
                    source: e,
                }
            })?;

            Ok(Arc::new(Self {
                file_factory,
                memory_factory,
            }) as Arc<dyn DataConnector>)
        })
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
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let factory = if Self::use_memory(dataset) {
            &self.memory_factory
        } else {
            &self
                .file_factory
                .clone()
                .ok_or(DataConnectorError::InternalWithSource {
                    dataconnector: "duckdb".to_string(),
                    source: Box::new(Error::MissingDuckDBFile {}),
                })?
        };

        Ok(Read::table_provider(factory, dataset.path().into())
            .await
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "duckdb",
            })?)
    }
}
