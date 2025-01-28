/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this Https except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use crate::component::dataset::Dataset;
use crate::dataconnector::listing::LISTING_TABLE_PARAMETERS;

use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use url::Url;

use super::{
    listing::{self, ListingTableConnector},
    DataConnector, DataConnectorError, DataConnectorFactory, DataConnectorResult, ParameterSpec,
    Parameters,
};
use super::{ConnectorComponent, ConnectorParams};

pub struct Https {
    params: Parameters,
}

impl std::fmt::Display for Https {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "https")
    }
}

#[derive(Default, Copy, Clone)]
pub struct HttpsFactory {}

impl HttpsFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut all_parameters = Vec::new();
    all_parameters.extend_from_slice(&[
        ParameterSpec::connector("username").secret(),
        ParameterSpec::connector("password").secret(),
        ParameterSpec::connector("port").description("The port to connect to."),
        ParameterSpec::runtime("client_timeout")
            .description("The timeout setting for HTTP(S) client."),
    ]);
    all_parameters.extend_from_slice(LISTING_TABLE_PARAMETERS);
    all_parameters
});

impl DataConnectorFactory for HttpsFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Arc::new(Https {
                params: params.parameters,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "http"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
    }
}

impl ListingTableConnector for Https {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let mut u = Url::parse(&dataset.from).boxed().map_err(|e| {
            DataConnectorError::InvalidConfiguration {
                dataconnector: "https".to_string(),
                message: "The specified URL in the dataset 'from' is not valid. Ensure the URL is valid and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/https".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            }
        })?;

        if let Some(p) = self.params.get("port").expose().ok() {
            let n = match p.parse::<u16>() {
                Ok(n) => n,
                Err(e) => {
                    return Err(DataConnectorError::InvalidConfiguration {
                        dataconnector: "https".to_string(),
                        message: "The specified `https_port` parameter was invalid. Specify a valid port number and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/https#parameters".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        source: Box::new(e),
                    });
                }
            };
            let _ = u.set_port(Some(n));
        };

        if let Some(p) = self.params.get("password").expose().ok() {
            if u.set_password(Some(p)).is_err() {
                return Err(
                    DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                        dataconnector: "https".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                    },
                );
            };
        }

        if let Some(p) = self.params.get("username").expose().ok() {
            if u.set_username(p).is_err() {
                return Err(
                    DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                        dataconnector: "https".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                    },
                );
            };
        }

        u.set_fragment(Some(&listing::build_fragments(
            &self.params,
            vec!["client_timeout"],
        )));

        Ok(u)
    }
}
