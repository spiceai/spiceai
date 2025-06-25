/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use data_components::oracle::OracleTableProvider;
use data_components::oracle::connection::{OracleConnectionParamsBuilder, OracleConnectionPool};
use datafusion::datasource::TableProvider;
use snafu::{ResultExt, Snafu};
use std::pin::Pin;
use std::sync::Arc;
use std::{any::Any, future::Future};

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, DataConnectorResult,
    ParameterSpec, Parameters, UnableToGetReadProviderSnafu,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: '{parameter}'. Specify a value.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/oracle"
    ))]
    MissingParameter { parameter: String },

    #[snafu(display(
        "Failed to connect to the Oracle Server.\nVerify your connection configuration, and try again.\n{source}"
    ))]
    UnableToCreateConnectionPool {
        source: data_components::oracle::Error,
    },

    #[snafu(display(
        "Invalid value provided for the 'port' parameter: {port}.\nSpecify a valid port, and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/oracle"
    ))]
    FailedToParsePort { port: String },
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("username").secret(),
    ParameterSpec::component("password").secret(),
    ParameterSpec::component("host"),
    ParameterSpec::component("port"),
    ParameterSpec::component("service_name"),
];

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Oracle {
    conn: Arc<OracleConnectionPool>,
}

impl Oracle {
    async fn new(params: &Parameters) -> Result<Self> {
        let mut conn_params = OracleConnectionParamsBuilder::new(
            params
                .get("host")
                .expose()
                .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?,
            params
                .get("username")
                .expose()
                .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?,
            params
                .get("password")
                .expose()
                .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?,
        );

        if let Some(port_str) = params.get("port").expose().ok() {
            let port = port_str.parse::<u16>().map_err(|_| {
                FailedToParsePortSnafu {
                    port: port_str.to_string(),
                }
                .build()
            })?;
            conn_params.port(port);
        }

        if let Some(service_name) = params.get("service_name").expose().ok() {
            conn_params.service_name(service_name);
        }

        let conn = data_components::oracle::connection::connect(&conn_params.build())
            .await
            .context(UnableToCreateConnectionPoolSnafu)?;

        Ok(Self {
            conn: Arc::new(conn),
        })
    }
}

#[derive(Default, Copy, Clone)]
pub struct OracleFactory {}

impl OracleFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for OracleFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Arc::new(Oracle::new(&params.parameters).await?) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "oracle"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Oracle {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let provider = OracleTableProvider::new(Arc::clone(&self.conn), &dataset.path().into())
            .await
            .boxed()
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "oracle",
                connector_component: ConnectorComponent::from(dataset),
            })?;

        Ok(Arc::new(provider))
    }
}
