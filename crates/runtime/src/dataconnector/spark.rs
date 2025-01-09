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

use async_trait::async_trait;

use crate::component::dataset::Dataset;
use data_components::spark_connect::SparkConnect;
use data_components::Read;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec, Parameters,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required Spark Remote, not available as secret or plaintext parameter"
    ))]
    MissingSparkRemote,

    #[snafu(display("Endpoint {endpoint} is invalid: {source}"))]
    InvalidEndpoint {
        endpoint: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{source}"))]
    UnableToConstructSparkConnect {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Spark {
    read_provider: Arc<dyn Read>,
}

impl Spark {
    async fn new(params: Parameters) -> Result<Self> {
        let conn = params.get("remote").expose().ok();
        let Some(conn) = conn else {
            return MissingSparkRemoteSnafu.fail();
        };
        SparkConnect::validate_connection_string(conn)
            .context(InvalidEndpointSnafu { endpoint: conn })?;
        let spark = SparkConnect::from_connection(conn)
            .await
            .context(UnableToConstructSparkConnectSnafu)?;
        Ok(Self {
            read_provider: Arc::new(spark.clone()),
        })
    }
}

#[derive(Default, Copy, Clone)]
pub struct SparkFactory {}

impl SparkFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[ParameterSpec::connector("remote").secret().required()];

impl DataConnectorFactory for SparkFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            match Spark::new(params.parameters).await {
                Ok(spark_connector) => Ok(Arc::new(spark_connector) as Arc<dyn DataConnector>),
                Err(e) => match e {
                    Error::MissingSparkRemote
                    | Error::InvalidEndpoint {
                        endpoint: _,
                        source: _,
                    } => Err(DataConnectorError::InvalidConfiguration {
                        dataconnector: "spark".to_string(),
                        connector_component: params.component.clone(),
                        message: e.to_string(),
                        source: e.into(),
                    }
                    .into()),
                    Error::UnableToConstructSparkConnect { source } => {
                        Err(DataConnectorError::UnableToConnectInternal {
                            dataconnector: "spark".to_string(),
                            connector_component: params.component.clone(),
                            source,
                        }
                        .into())
                    }
                },
            }
        })
    }

    fn prefix(&self) -> &'static str {
        "spark"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Spark {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let table_reference = TableReference::from(dataset.path());
        Ok(self
            .read_provider
            .table_provider(table_reference, dataset.schema())
            .await
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "spark",
                connector_component: ConnectorComponent::from(dataset),
            })?)
    }
}
