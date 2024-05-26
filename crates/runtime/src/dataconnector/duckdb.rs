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
use data_components::Read;
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
            // data connector requires valid "open" parameter
            let db_path: String =
                params
                    .get("open")
                    .cloned()
                    .ok_or(DataConnectorError::InvalidConfiguration {
                        dataconnector: "duckdb".to_string(),
                        message: "Missing required open parameter.".to_string(),
                        source: "Missing open".into(),
                    })?;

            // TODO: wire to dataset.mode once readwrite implemented for duckdb
            let pool = Arc::new(
                DuckDbConnectionPool::new_file(&db_path, &AccessMode::ReadOnly).map_err(|e| {
                    DataConnectorError::UnableToConnectInternal {
                        dataconnector: "duckdb".to_string(),
                        source: e,
                    }
                })?,
            );

            let duckdb_factory = DuckDBTableFactory::new(pool);

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
}
