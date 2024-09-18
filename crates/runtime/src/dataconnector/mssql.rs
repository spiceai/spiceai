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
use data_components::mssql::{
    self, SqlServerConnectionPool, SqlServerTableProvider, TiberiusConnectionManager,
};
use datafusion::datasource::TableProvider;
use snafu::{ResultExt, Snafu};
use std::pin::Pin;
use std::sync::Arc;
use std::{any::Any, future::Future};

use super::{
    DataConnector, DataConnectorFactory, DataConnectorResult, ParameterSpec, Parameters,
    UnableToGetReadProviderSnafu,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: {parameter}"))]
    MissingParameter { parameter: String },

    #[snafu(display("Unable to create MS SQL Server connection pool: {source}"))]
    UnableToCreateConnectionPool { source: mssql::Error },
}

const PARAMETERS: &[ParameterSpec] = &[ParameterSpec::connector("connection_string")
    .secret()
    .required()];

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct SqlServer {
    conn: Arc<SqlServerConnectionPool>,
}

impl SqlServer {
    async fn new(params: &Parameters) -> Result<Self> {
        let conn_string = params
            .get("connection_string")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;

        let conn = TiberiusConnectionManager::create_connection_pool(conn_string)
            .await
            .context(UnableToCreateConnectionPoolSnafu)?;

        return Ok(Self {
            conn: Arc::new(conn),
        });
    }
}

#[derive(Default, Copy, Clone)]
pub struct SqlServerFactory {}

impl SqlServerFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for SqlServerFactory {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(
            async move { Ok(Arc::new(SqlServer::new(&params).await?) as Arc<dyn DataConnector>) },
        )
    }

    fn prefix(&self) -> &'static str {
        "mssql"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for SqlServer {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let provider = SqlServerTableProvider::new(Arc::clone(&self.conn), &dataset.path().into())
            .await
            .boxed()
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "mssql",
            });

        Ok(Arc::new(provider.unwrap()))
    }
}
