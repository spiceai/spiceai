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

use super::{DataConnector, DataConnectorFactory};
use crate::component::dataset::Dataset;
use arrow_flight::sql::client::FlightSqlServiceClient;
use async_trait::async_trait;
use data_components::flightsql::FlightSQLFactory;
use data_components::Read;
use datafusion::datasource::TableProvider;
use flight_client::tls::new_tls_flight_channel;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::{future::Future, sync::Arc};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: endpoint"))]
    MissingEndpointParameter,

    #[snafu(display("Unable to construct TLS flight client: {source}"))]
    UnableToConstructTlsChannel { source: flight_client::tls::Error },

    #[snafu(display("Unable to perform FlightSQL handshake: {source}"))]
    UnableToPerformHandshake { source: arrow::error::ArrowError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct FlightSQL {
    pub flightsql_factory: FlightSQLFactory,
}

impl DataConnectorFactory for FlightSQL {
    fn create(
        params: HashMap<String, SecretString>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let endpoint: String = params
                .get("endpoint")
                .map(ExposeSecret::expose_secret)
                .cloned()
                .context(MissingEndpointParameterSnafu)?;
            let flight_channel = new_tls_flight_channel(&endpoint)
                .await
                .context(UnableToConstructTlsChannelSnafu)?;

            let mut client = FlightSqlServiceClient::new(flight_channel);
            let username = params.get("username").map(|s| s.expose_secret().as_str());
            let password = params.get("password").map(|s| s.expose_secret().as_str());
            if let (Some(username), Some(password)) = (username, password) {
                client
                    .handshake(username, password)
                    .await
                    .context(UnableToPerformHandshakeSnafu)?;
            };
            let flightsql_factory = FlightSQLFactory::new(client, endpoint);
            Ok(Arc::new(Self { flightsql_factory }) as Arc<dyn DataConnector>)
        })
    }
}

#[async_trait]
impl DataConnector for FlightSQL {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn prefix(&self) -> &'static str {
        "flightsql"
    }

    fn autoload_secrets(&self) -> &'static [&'static str] {
        &["username", "password"]
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Read::table_provider(
            &self.flightsql_factory,
            dataset.path().into(),
            dataset.schema(),
        )
        .await
        .context(super::UnableToGetReadProviderSnafu {
            dataconnector: "flightsql",
        })?)
    }
}
