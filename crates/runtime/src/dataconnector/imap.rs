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
use data_components::imap::{
    session::{ImapAuthMode, ImapAuthModeParameter, ImapSSLMode, ImapSession},
    ImapTableProvider,
};
use datafusion::datasource::TableProvider;
use regex::Regex;
use secrecy::SecretString;
use snafu::prelude::*;
use std::{
    any::Any,
    collections::HashMap,
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::{Arc, LazyLock},
};

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec,
};

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("username")
        .secret()
        .description("The username to use for the IMAP connection"),
    ParameterSpec::connector("password")
        .secret()
        .description("The password to use for the IMAP connection"),
    ParameterSpec::connector("access_token")
        .secret()
        .description("The OAuth access token to use for the IMAP connection"),
    ParameterSpec::connector("host").description("The IMAP server host to connect to"),
    ParameterSpec::connector("mailbox")
        .default("INBOX")
        .description("The name of the IMAP mailbox to connect to"),
    ParameterSpec::connector("port")
        .default("993")
        .description("The port to connect to on the IMAP server"),
    ParameterSpec::connector("ssl_mode")
        .default("auto")
        .description("The IMAP SSL mode to use"),
];

// Regex that matches an email address in a simple way
// Email-ish - because it could match things that are not valid email addresses
static EMAILISH_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[\w._%+-]+@[\w-]+\.([\w-]+\.?){1,}$")
        .unwrap_or_else(|_| unreachable!("The regex is a valid regex, so it should compile"))
});

static PRESET_HOST_CONNECTIONS: LazyLock<HashMap<&str, &str>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    map.insert("gmail.com", "imap.gmail.com");
    map.insert("outlook.com", "outlook.office365.com");
    map
});

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("A password parameter is required, but was not provided"))]
    PasswordRequired,
    #[snafu(display("An access token parameter is required, but was not provided"))]
    PasswordOrAccessTokenRequired,
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
    #[snafu(display("A password and access token were provided. Only one can be specified."))]
    PasswordAndAccessTokenError,
}

pub struct Imap {
    session: ImapSession,
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

    /// Parse the email address and subsequently the host from the 'from' field of the dataset
    fn parse_host(
        params: &mut ConnectorParams,
    ) -> Result<Arc<str>, Box<dyn std::error::Error + Send + Sync>> {
        match &params.component {
            ConnectorComponent::Dataset(dataset) => {
                if let Some(captures) = EMAILISH_REGEX.captures(&dataset.from.replace("imap:", ""))
                {
                    let Some(email) = captures.get(0) else {
                        unreachable!("If there is a capture, capture group 0 will always exist");
                    };

                    // With our new email capture, do a couple things:
                    // 1. Set the username to the email, if no username is provided
                    // 2. Figure out the domain portion of the email, and check if it has pre-set host information
                    // 3. If it doesn't have pre-set host information, and there is no host parameter, error out
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
                        Ok(host_param.into())
                    } else {
                        let Some(preset_host) = PRESET_HOST_CONNECTIONS.get(host) else {
                            return Err(DataConnectorError::InvalidConfigurationSourceOnly {
                                dataconnector: "imap".to_string(),
                                connector_component: params.component.clone(),
                                source: Error::HostRequired.into(),
                            }
                            .into());
                        };
                        Ok((*preset_host).into())
                    }
                } else {
                    Err(DataConnectorError::InvalidConfigurationSourceOnly {
                        dataconnector: "imap".to_string(),
                        connector_component: params.component.clone(),
                        source: Error::InvalidFrom {
                            from: dataset.from.to_string(),
                        }
                        .into(),
                    }
                    .into())
                }
            }
            ConnectorComponent::Catalog(_) => Err(DataConnectorError::InvalidConnectorType {
                dataconnector: "imap".to_string(),
                connector_component: params.component.clone(),
            }
            .into()),
        }
    }

    fn parse_authentication(
        connector_component: &ConnectorComponent,
        username: &SecretString,
        password: Option<&SecretString>,
        access_token: Option<&SecretString>,
    ) -> Result<ImapAuthMode, Box<dyn std::error::Error + Send + Sync>> {
        match (password, access_token) {
            (Some(_), Some(_)) => Err(DataConnectorError::InvalidConfigurationSourceOnly {
                dataconnector: "imap".to_string(),
                connector_component: connector_component.clone(),
                source: Error::PasswordAndAccessTokenError.into(),
            }
            .into()),
            (Some(password), None) => {
                Ok(ImapAuthModeParameter::Plain.build(username.clone(), password.clone()))
            }
            (None, Some(access_token)) => {
                Ok(ImapAuthModeParameter::OAuth.build(username.clone(), access_token.clone()))
            }
            (None, None) => Err(DataConnectorError::InvalidConfigurationSourceOnly {
                dataconnector: "imap".to_string(),
                connector_component: connector_component.clone(),
                source: Error::PasswordOrAccessTokenRequired.into(),
            }
            .into()),
        }
    }
}

impl DataConnectorFactory for ImapFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        mut params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let host = Self::parse_host(&mut params)?;

            let Some(username) = params.parameters.get("username").ok() else {
                return Err(DataConnectorError::InvalidConfigurationSourceOnly {
                    dataconnector: "imap".to_string(),
                    connector_component: params.component.clone(),
                    source: Error::PasswordRequired.into(),
                }
                .into());
            };

            let password_parameter = params.parameters.get("password").ok();
            let access_token_parameter = params.parameters.get("access_token").ok();

            let authentication = Self::parse_authentication(
                &params.component,
                username,
                password_parameter,
                access_token_parameter,
            )?;

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

            let mailbox = params
                .parameters
                .get("mailbox")
                .expose()
                .ok()
                .unwrap_or("INBOX");

            let mailbox = mailbox.into();

            let ssl_mode = ImapSSLMode::from_str(
                params
                    .parameters
                    .get("ssl_mode")
                    .expose()
                    .ok()
                    .unwrap_or("auto"),
            )
            .map_err(|e| DataConnectorError::InvalidConfigurationSourceOnly {
                dataconnector: "imap".to_string(),
                connector_component: params.component.clone(),
                source: e.into(),
            })?;

            Ok(Arc::new(Imap {
                session: ImapSession::new(authentication, host, port, mailbox)
                    .with_ssl_mode(ssl_mode),
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
        Ok(Arc::new(ImapTableProvider::new(
            self.session.clone(),
            dataset.is_accelerated(),
        )))
    }
}
