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
use async_trait::async_trait;
use data_components::imap::ImapTableProvider;
use datafusion::datasource::TableProvider;
use imap::{Client, ImapConnection};
use regex::Regex;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use snafu::prelude::*;
use std::{
    any::Any,
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, LazyLock},
};
use url::Url;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    InvalidConfigurationSnafu, ParameterSpec, Parameters,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("A password parameter is required, but was not provided"))]
    PasswordRequired,
    #[snafu(display("A username parameter is required, but was not provided"))]
    UsernameRequired,
    #[snafu(display("A host parameter is required, but was not provided"))]
    HostRequired,
    #[snafu(display("The specified port parameter is not a valid number"))]
    InvalidPort,
    #[snafu(display("An IMAP error occurred: {source}"))]
    ImapError { source: imap::Error },
    #[snafu(display("The specified 'from' address is not a valid email address: {from}"))]
    InvalidFrom { from: String },
}

pub struct Imap {
    params: Parameters,
    host: Arc<str>,
    port: u16,
}

#[derive(Default, Copy, Clone)]
pub struct ImapFactory {}

impl ImapFactory {
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
    ParameterSpec::connector("host"),
    ParameterSpec::connector("mailbox"),
    ParameterSpec::connector("port").default("993"),
];

// Regex that matches an email address in a simple way
// Email-ish - because it could match things that are not valid email addresses
static EMAILISH_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[\w._%+-]+@[\w-]+\.([\w-]+\.?){1,}$").expect("Should create emailish regex")
});

static PRESET_HOST_CONNECTIONS: LazyLock<HashMap<&str, &str>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    map.insert("gmail.com", "imap.gmail.com");
    map.insert("outlook.com", "outlook.office365.com");
    map
});

impl DataConnectorFactory for ImapFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        mut params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let host = match &params.component {
                ConnectorComponent::Dataset(dataset) => {
                    // let email = dataset.from.matches(&EMAILISH_REGEX).collect::<Vec<&str>>();
                    if let Some(captures) =
                        EMAILISH_REGEX.captures(&dataset.from.replace("imap:", ""))
                    {
                        let Some(email) = captures.get(0) else {
                            unreachable!(
                                "If there is a capture, capture group 0 will always exist"
                            );
                        };

                        if params.parameters.get("username").expose().ok().is_none() {
                            params
                                .parameters
                                .insert("username".to_string(), email.as_str().to_string().into());
                        }

                        let segments = email.as_str().split('@').collect::<Vec<&str>>();
                        let Some(host) = segments.get(1) else {
                            unreachable!("If there is a capture, there should be a split at @");
                        };

                        let host_param = params.parameters.get("host").expose().ok();

                        if host_param.is_none()
                            && (host.is_empty() || !PRESET_HOST_CONNECTIONS.contains_key(host))
                        {
                            return Err(DataConnectorError::InvalidConfigurationSourceOnly {
                                dataconnector: "imap".to_string(),
                                connector_component: params.component.clone(),
                                source: Error::HostRequired.into(),
                            }
                            .into());
                        }

                        if let Some(host_param) = host_param {
                            host_param
                        } else {
                            let Some(preset_host) = PRESET_HOST_CONNECTIONS.get(host) else {
                                return Err(DataConnectorError::InvalidConfigurationSourceOnly {
                                    dataconnector: "imap".to_string(),
                                    connector_component: params.component.clone(),
                                    source: Error::HostRequired.into(),
                                }
                                .into());
                            };
                            *preset_host
                        }
                    } else {
                        return Err(DataConnectorError::InvalidConfigurationSourceOnly {
                            dataconnector: "imap".to_string(),
                            connector_component: params.component.clone(),
                            source: Error::InvalidFrom {
                                from: dataset.from.to_string(),
                            }
                            .into(),
                        }
                        .into());
                    }
                }
                ConnectorComponent::Catalog(_) => {
                    return Err(DataConnectorError::InvalidConnectorType {
                        dataconnector: "imap".to_string(),
                        connector_component: params.component.clone(),
                    }
                    .into());
                }
            };

            if params.parameters.get("password").expose().ok().is_none() {
                return Err(DataConnectorError::InvalidConfigurationSourceOnly {
                    dataconnector: "imap".to_string(),
                    connector_component: params.component.clone(),
                    source: Error::PasswordRequired.into(),
                }
                .into());
            }

            let port = if let Some(port) = params.parameters.get("port").expose().ok() {
                match port.parse::<u16>() {
                    Ok(port) => port,
                    Err(_) => {
                        return Err(DataConnectorError::InvalidConfigurationSourceOnly {
                            dataconnector: "imap".to_string(),
                            connector_component: params.component.clone(),
                            source: Error::InvalidPort.into(),
                        }
                        .into());
                    }
                }
            } else {
                993
            };

            let host = host.into();
            Ok(Arc::new(Imap {
                params: params.parameters,
                host,
                port,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "imap"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Imap {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let client = imap::ClientBuilder::new(Arc::clone(&self.host), self.port)
            .connect()
            .map_err(|source| DataConnectorError::UnableToGetReadProvider {
                dataconnector: "imap".to_string(),
                connector_component: ConnectorComponent::Dataset(dataset.clone().into()),
                source: Error::ImapError { source }.into(),
            })?;

        let Some(password) = self.params.get("password").expose().ok() else {
            return Err(DataConnectorError::InvalidConfigurationSourceOnly {
                dataconnector: "imap".to_string(),
                connector_component: ConnectorComponent::Dataset(dataset.clone().into()),
                source: Error::PasswordRequired.into(),
            });
        };

        let Some(username) = self.params.get("username").expose().ok() else {
            return Err(DataConnectorError::InvalidConfigurationSourceOnly {
                dataconnector: "imap".to_string(),
                connector_component: ConnectorComponent::Dataset(dataset.clone().into()),
                source: Error::UsernameRequired.into(),
            });
        };

        let session = client.login(username, password).map_err(|source| {
            DataConnectorError::UnableToGetReadProvider {
                dataconnector: "imap".to_string(),
                connector_component: ConnectorComponent::Dataset(dataset.clone().into()),
                source: Error::ImapError { source: source.0 }.into(),
            }
        })?;

        Ok(Arc::new(ImapTableProvider::new(session)))
    }
}
