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

use super::DataConnector;
use super::DataConnectorFactory;
use super::ParameterSpec;
use super::Parameters;
use crate::component::dataset::Dataset;
use crate::dataconnector::DataConnectorError;
use async_trait::async_trait;
use data_components::flight::FlightFactory;
use data_components::Read;
use data_components::ReadWrite;
use datafusion::datasource::TableProvider;
use datafusion::sql::unparser::dialect::DefaultDialect;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion::sql::unparser::dialect::IntervalStyle;
use flight_client::FlightClient;
use ns_lookup::verify_endpoint_connection;
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: {parameter}"))]
    MissingParameter { parameter: String },

    #[snafu(display(r#"Unable to connect to endpoint "{endpoint}": {source}"#))]
    UnableToVerifyEndpointConnection {
        source: ns_lookup::Error,
        endpoint: String,
    },

    #[snafu(display("Unable to create flight client: {source}"))]
    UnableToCreateFlightClient { source: flight_client::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Dremio {
    flight_factory: FlightFactory,
}

pub struct DremioDialect {}

impl Dialect for DremioDialect {
    fn use_timestamp_for_date64(&self) -> bool {
        true
    }

    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::SQLStandard
    }

    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        DefaultDialect {}.identifier_quote_style(identifier)
    }
}

#[derive(Default, Copy, Clone)]
pub struct DremioFactory {}

impl DremioFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("username").secret(),
    ParameterSpec::connector("password").secret(),
    ParameterSpec::connector("endpoint"),
];

impl DataConnectorFactory for DremioFactory {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let endpoint: String = params
                .get("endpoint")
                .expose()
                .ok_or_else(|p| Error::MissingParameter {
                    parameter: p.to_string(),
                })?
                .to_string();

            verify_endpoint_connection(&endpoint)
                .await
                .with_context(|_| UnableToVerifyEndpointConnectionSnafu {
                    endpoint: endpoint.clone(),
                })?;

            let flight_client = FlightClient::new(
                endpoint.as_str(),
                params.get("username").expose().ok().unwrap_or_default(),
                params.get("password").expose().ok().unwrap_or_default(),
            )
            .await
            .context(UnableToCreateFlightClientSnafu)?;
            let flight_factory =
                FlightFactory::new("dremio", flight_client, Arc::new(DremioDialect {}));
            Ok(Arc::new(Dremio { flight_factory }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "dremio"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Dremio {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        match Read::table_provider(
            &self.flight_factory,
            dataset.path().into(),
            dataset.schema(),
        )
        .await
        {
            Ok(provider) => Ok(provider),
            Err(e) => {
                if let Some(data_components::flight::Error::UnableToGetSchema {
                    source: _,
                    table,
                }) = e.downcast_ref::<data_components::flight::Error>()
                {
                    tracing::debug!("{e}");
                    return Err(DataConnectorError::UnableToGetSchema {
                        dataconnector: "dremio".to_string(),
                        dataset_name: dataset.name.to_string(),
                        table_name: table.clone(),
                    });
                }

                return Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "dremio".to_string(),
                    source: e,
                });
            }
        }
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        let read_write_result = ReadWrite::table_provider(
            &self.flight_factory,
            dataset.path().into(),
            dataset.schema(),
        )
        .await
        .context(super::UnableToGetReadWriteProviderSnafu {
            dataconnector: "dremio",
        });

        Some(read_write_result)
    }
}
