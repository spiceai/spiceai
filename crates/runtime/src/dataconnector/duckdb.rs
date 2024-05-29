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
use async_trait::async_trait;
use data_components::duckdb::DuckDBTableFactory;
use data_components::{Read, ReadWrite};
use datafusion::datasource::TableProvider;
use db_connection_pool::duckdbpool::DuckDbConnectionPool;
use duckdb::AccessMode;
use secrets::Secret;
use snafu::prelude::*;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorError, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create DuckDB connection pool: {source}"))]
    UnableToCreateDuckDBConnectionPool { source: db_connection_pool::Error },

    #[snafu(display("Missing required parameter: open"))]
    MissingDuckDBFile,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDB {
    duckdb_factory: DuckDBTableFactory,
}

impl DataConnectorFactory for DuckDB {
    fn create(
        _secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let mut from_duckdb_function = false;
            if let Some(from_duckdb_function_str) = params.get("from_duckdb_function") {
                match from_duckdb_function_str.to_lowercase().as_str() {
                    "true" => from_duckdb_function = true,
                    "false" => from_duckdb_function = false,
                    invalid_value => {
                        return Err(DataConnectorError::InvalidConfiguration {
                            dataconnector: "duckdb".to_string(),
                            message: format!("Invalid value for parameter from_duckdb_function {invalid_value}, use either true or false"),
                            source: "".into(),
                        }
                        .into());
                    }
                }
            }

            let access_mode = params
                .get("access_mode")
                .cloned()
                .unwrap_or("automatic".to_string());

            let access_mode = match access_mode.as_str() {
                "read_only" => &AccessMode::ReadOnly,
                "read_write" => &AccessMode::ReadWrite,
                "automatic" => &AccessMode::Automatic,
                invalid_value => {
                    return Err(DataConnectorError::InvalidConfiguration {
                        dataconnector: "duckdb".to_string(),
                        message: format!("Invalid value for parameter access_mode {invalid_value}, use read_only, read_writ or automatic"),
                        source: "".into(),
                    }
                    .into());
                }
            };

            let pool = match params.get("open") {
                Some(open) => {
                    let db_path = open.clone();
                    Arc::new(
                        DuckDbConnectionPool::new_file(&db_path, access_mode).map_err(|e| {
                            DataConnectorError::UnableToConnectInternal {
                                dataconnector: "duckdb".to_string(),
                                source: e,
                            }
                        })?,
                    )
                }
                None => Arc::new(DuckDbConnectionPool::new_memory(access_mode).map_err(|e| {
                    DataConnectorError::UnableToConnectInternal {
                        dataconnector: "duckdb".to_string(),
                        source: e,
                    }
                })?),
            };

            let duckdb_factory = DuckDBTableFactory::new(pool, from_duckdb_function);

            Ok(Arc::new(Self { duckdb_factory }) as Arc<dyn DataConnector>)
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
        Ok(
            Read::table_provider(&self.duckdb_factory, dataset.path().into())
                .await
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "duckdb",
                })?,
        )
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        let read_write_result =
            ReadWrite::table_provider(&self.duckdb_factory, dataset.path().into())
                .await
                .context(super::UnableToGetReadWriteProviderSnafu {
                    dataconnector: "duckdb",
                });

        Some(read_write_result)
    }
}
