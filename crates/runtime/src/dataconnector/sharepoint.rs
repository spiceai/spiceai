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
use data_components::sharepoint::client::SharepointClient;
use datafusion::datasource::TableProvider;
use graph_rs_sdk::{
    identity::{
        AuthorizationCodeCredential, ConfidentialClientApplication, PublicClientApplication,
    },
    GraphClient,
};
use snafu::{ResultExt, Snafu};
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use url::Url;

use super::{
    DataConnector, DataConnectorFactory, DataConnectorResult, ParameterSpec, Parameters,
    UnableToGetReadProviderSnafu,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: {parameter}"))]
    MissingParameter { parameter: String },

    #[snafu(display("Invalid Sharepoint parameters: {source}"))]
    InvalidParameters {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Missing redirect url parameter: {url}"))]
    InvalidRedirectUrlError { url: String },

    #[snafu(display("Could not construct authorization url for sharepoint OAuth2: {source}"))]
    AuthorizationUrlError {
        source: graph_rs_sdk::error::AuthorizationFailure,
    },

    /// Error when user hasn't clicked authorisation URL yet. If this error occurs, can still be valid on retry.
    #[snafu(display("No authorisation code yet available to access sharepoint data"))]
    AuthorizationCodeError {},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Sharepoint {
    client: Arc<GraphClient>,
}

impl Sharepoint {
    fn new(params: &Parameters) -> Result<Self> {
        let client_id = params
            .get("client_id")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;
        let tenant_id = params
            .get("tenant_id")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;

        let client_secret = params.get("client_secret").expose().ok();
        let authorization_code = params.get("authorization_code").expose().ok();

        let graph_client = match (client_secret, authorization_code) {
            (Some(client_secret), None) => GraphClient::from(
                &ConfidentialClientApplication::builder(client_id)
                    .with_client_secret(client_secret)
                    .with_tenant(tenant_id)
                    .with_scope([".default"])
                    .build(),
            ),
            (None, Some(authorization_code)) => {
                let redirect_url = Url::parse("http://localhost:8091")
                    .boxed()
                    .context(InvalidParametersSnafu)?;
                GraphClient::from(&PublicClientApplication::from(
                    AuthorizationCodeCredential::new_with_redirect_uri(
                        tenant_id,
                        client_id,
                        "",
                        authorization_code,
                        redirect_url,
                    ),
                ))
            }
            (None, None) => {
                return Err(Error::InvalidParameters {
                    source: "either 'client_secret' or 'authorization_code' must be provided"
                        .into(),
                })
            }
            (Some(_), Some(_)) => {
                return Err(Error::InvalidParameters {
                    source: "both 'client_secret' and 'authorization_code' cannot be provided"
                        .into(),
                })
            }
        };

        Ok(Sharepoint {
            client: Arc::new(graph_client),
        })
    }

    pub async fn health(&self) -> std::result::Result<String, String> {
        let u = self
            .client
            .me()
            .get_user()
            .send()
            .await
            .map_err(|e| format!("{e:#?}").to_string())?;

        let resp = u
            .json::<serde_json::Value>()
            .await
            .map_err(|e| e.to_string())?;
        Ok(resp.to_string())
    }
}

#[derive(Default, Copy, Clone)]
pub struct SharepointFactory {}

impl SharepointFactory {
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
    ParameterSpec::connector("client_id").secret().required(),
    ParameterSpec::connector("authorization_code").secret(),
    ParameterSpec::connector("tenant_id").secret().required(),
    ParameterSpec::connector("client_secret").secret(),
];

impl DataConnectorFactory for SharepointFactory {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let s = Sharepoint::new(&params)?;
            println!("Sharepoint health: {:#?}", s.health().await);
            Ok(Arc::new(s) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "sharepoint"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Sharepoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let client = SharepointClient::new(Arc::clone(&self.client), &dataset.from, true)
            .boxed()
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "sharepoint",
            })?;

        Ok(Arc::new(client))
    }

    async fn metadata_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        if !dataset.has_metadata_table {
            return None;
        }

        match SharepointClient::new(Arc::clone(&self.client), &dataset.from, false)
            .boxed()
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "sharepoint",
            }) {
            Err(e) => Some(Err(e)),
            Ok(client) => Some(Ok(Arc::new(client))),
        }
    }
}
