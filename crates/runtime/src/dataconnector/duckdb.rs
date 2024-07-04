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
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::duckdb::DuckDBTableFactory;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::is_table_function;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::Error as DbConnectionPoolError;
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
    UnableToCreateDuckDBConnectionPool { source: DbConnectionPoolError },

    #[snafu(display("Missing required parameter: open"))]
    MissingDuckDBFile,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDB {
    duckdb_factory: DuckDBTableFactory,
}

impl DuckDB {
    pub(crate) fn create_in_memory() -> AnyErrorResult<DuckDBTableFactory> {
        let pool = Arc::new(DuckDbConnectionPool::new_memory().map_err(|source| {
            DataConnectorError::UnableToConnectInternal {
                dataconnector: "duckdb".to_string(),
                source,
            }
        })?);

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
            let duckdb_factory = if let Some(db_path) = params.get("open") {
                Self::create_file(db_path)?
            } else {
                Self::create_in_memory()?
            };

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
        let path: TableReference = dataset.path().into();

        if !(is_table_function(&path) || dataset.params.contains_key("open")) {
            return Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "duckdb".to_string(),
                source: Box::new(Error::MissingDuckDBFile {}),
            });
        }

        Ok(Read::table_provider(&self.duckdb_factory, path)
            .await
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "duckdb",
            })?)
    }
}
