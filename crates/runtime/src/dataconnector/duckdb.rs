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
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion_table_providers::duckdb::DuckDBTableFactory;
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::is_table_function;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::Error as DbConnectionPoolError;
use datafusion_table_providers::InvalidTypeAction;
use duckdb::AccessMode;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{
    AnyErrorResult, DataConnector, DataConnectorError, DataConnectorFactory, DataConnectorParams,
    ParameterSpec,
};

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
    pub(crate) fn create_in_memory(
        invalid_type_action: InvalidTypeAction,
    ) -> AnyErrorResult<DuckDBTableFactory> {
        let pool = Arc::new(
            DuckDbConnectionPool::new_memory()
                .map_err(|source| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "duckdb".to_string(),
                    source,
                })?
                .with_invalid_type_action(invalid_type_action),
        );

        Ok(DuckDBTableFactory::new(pool))
    }

    pub(crate) fn create_file(
        path: &str,
        invalid_type_action: InvalidTypeAction,
    ) -> AnyErrorResult<DuckDBTableFactory> {
        let pool = Arc::new(
            DuckDbConnectionPool::new_file(path, &AccessMode::ReadOnly)
                .map_err(|source| DataConnectorError::UnableToConnectInternal {
                    dataconnector: "duckdb".to_string(),
                    source,
                })?
                .with_invalid_type_action(invalid_type_action),
        );

        Ok(DuckDBTableFactory::new(pool))
    }
}

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

const PARAMETERS: &[ParameterSpec] = &[ParameterSpec::connector("open")];

impl DataConnectorFactory for DuckDBFactory {
    fn create(
        &self,
        params: DataConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let invalid_type_action = params
                .invalid_type_action
                .unwrap_or(InvalidTypeAction::Error);

            let duckdb_factory = if let Some(db_path) = params.parameters.get("open").expose().ok()
            {
                DuckDB::create_file(db_path, invalid_type_action)?
            } else {
                DuckDB::create_in_memory(invalid_type_action)?
            };

            Ok(Arc::new(DuckDB { duckdb_factory }) as Arc<dyn DataConnector>)
        })
    }

    fn supports_invalid_type_action(&self) -> bool {
        true
    }

    fn prefix(&self) -> &'static str {
        "duckdb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
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

        if !(is_table_function(&path) || dataset.params.contains_key("duckdb_open")) {
            return Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "duckdb".to_string(),
                source: Box::new(Error::MissingDuckDBFile {}),
            });
        }

        Ok(
            Read::table_provider(&self.duckdb_factory, path, dataset.schema())
                .await
                .context(super::UnableToGetReadProviderSnafu {
                    dataconnector: "duckdb",
                })?,
        )
    }
}
