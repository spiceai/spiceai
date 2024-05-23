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

use async_trait::async_trait;

use data_components::spark_connect::SparkConnect;
use data_components::{Read, ReadWrite};
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorError, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required Spark Remote, not available as secret or plaintext parameter"
    ))]
    MissingSparkRemote,

    #[snafu(display("Spark Remote configured twice, both as secret or plaintext parameter"))]
    DuplicatedSparkRemote,

    #[snafu(display("Endpoint {endpoint} is invalid: {source}"))]
    InvalidEndpoint {
        endpoint: String,
        source: ns_lookup::Error,
    },

    #[snafu(display("{source}"))]
    UnableToConstructSparkConnect {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Spark {
    read_provider: Arc<dyn Read>,
    read_write_provider: Arc<dyn ReadWrite>,
}

impl Spark {
    async fn new(
        secret: Arc<Option<Secret>>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Self> {
        let plain_text_connection = params.as_ref().as_ref().and_then(|x| x.get("spark_remote"));
        let secret_connection = secret.as_ref().as_ref().and_then(|x| x.get("spark_remote"));
        let conn = match (plain_text_connection, secret_connection) {
            (Some(_), Some(_)) => DuplicatedSparkRemoteSnafu.fail(),
            (_, Some(conn)) => Ok(conn),
            (Some(conn), _) => Ok(conn.as_str()),
            _ => MissingSparkRemoteSnafu.fail(),
        }?;
        let spark = SparkConnect::from_connection(conn)
            .await
            .context(UnableToConstructSparkConnectSnafu)?;
        Ok(Self {
            read_provider: Arc::new(spark.clone()),
            read_write_provider: Arc::new(spark),
        })
    }
}

impl DataConnectorFactory for Spark {
    fn create(
        secret: Option<Secret>,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            match Spark::new(Arc::new(secret), params).await {
                Ok(spark_connector) => Ok(Arc::new(spark_connector) as Arc<dyn DataConnector>),
                Err(e) => match e {
                    Error::UnableToConstructSparkConnect { source } => {
                        Err(DataConnectorError::UnableToConnectInternal {
                            dataconnector: "spark".to_string(),
                            source,
                        }
                        .into())
                    }
                    _ => Err(DataConnectorError::InvalidConfiguration {
                        dataconnector: "spark".to_string(),
                        message: e.to_string(),
                        source: e.into(),
                    }
                    .into()),
                },
            }
        })
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
            .table_provider(table_reference)
            .await
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "spark",
            })?)
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        let table_reference = TableReference::from(dataset.path());
        let read_write_result = self
            .read_write_provider
            .table_provider(table_reference)
            .await
            .context(super::UnableToGetReadWriteProviderSnafu {
                dataconnector: "spark",
            });

        Some(read_write_result)
    }
}
