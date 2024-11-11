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
use data_components::sharepoint::{client::SharepointClient, table::SharepointTableProvider};
use datafusion::datasource::TableProvider;
use document_parse::DocumentParser;
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
    DataConnector, DataConnectorFactory, DataConnectorParams, DataConnectorResult, ParameterSpec,
    Parameters, UnableToGetReadProviderSnafu,
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
        let auth_code = params.get("auth_code").expose().ok();

        let graph_client = match (client_secret, auth_code) {
            (Some(client_secret), None) => GraphClient::from(
                &ConfidentialClientApplication::builder(client_id)
                    .with_client_secret(client_secret)
                    .with_tenant(tenant_id)
                    .with_scope([".default"])
                    .build(),
            ),
            (Some(_) | None, Some(auth_code)) => {
                tracing::warn!("Both `params.client_secret` and `params.auth_code` are provided. Using `params.auth_code`.");
                // Must match the redirect URL used in `spice login sharepoint...`.
                let redirect_url = Url::parse("http://localhost:8091")
                    .boxed()
                    .context(InvalidParametersSnafu)?;
                GraphClient::from(&PublicClientApplication::from(
                    AuthorizationCodeCredential::new_with_redirect_uri(
                        tenant_id,
                        client_id,
                        "",
                        auth_code,
                        redirect_url,
                    ),
                ))
            }
            (None, None) => {
                return Err(Error::InvalidParameters {
                    source: "either 'client_secret' or 'auth_code' must be provided".into(),
                })
            }
        };

        Ok(Sharepoint {
            client: Arc::new(graph_client),
        })
    }

    async fn get_formatter(&self, dataset: &Dataset) -> Option<Arc<dyn DocumentParser>> {
        let file_format = dataset.params.get("file_format")?;

        document_parse::get_parser_factory(file_format)
            .await
            .map(|factory| {
                // TODO: add opts.
                factory.default()
            })
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
    ParameterSpec::connector("auth_code").secret(),
    ParameterSpec::connector("tenant_id").secret().required(),
    ParameterSpec::connector("client_secret").secret(),
    ParameterSpec::runtime("file_format"),
];

impl DataConnectorFactory for SharepointFactory {
    fn create(
        &self,
        params: DataConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Arc::new(Sharepoint::new(&params.parameters)?) as Arc<dyn DataConnector>)
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
        let client = SharepointClient::new(Arc::clone(&self.client), &dataset.from)
            .await
            .boxed()
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "sharepoint",
            })?;
        Ok(Arc::new(SharepointTableProvider::new(
            client,
            true,
            self.get_formatter(dataset).await,
        )))
    }

    async fn metadata_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        if !dataset.has_metadata_table {
            return None;
        }

        match SharepointClient::new(Arc::clone(&self.client), &dataset.from)
            .await
            .boxed()
            .context(UnableToGetReadProviderSnafu {
                dataconnector: "sharepoint",
            }) {
            Ok(client) => Some(Ok(Arc::new(SharepointTableProvider::new(
                client,
                false,
                self.get_formatter(dataset).await,
            )))),
            Err(e) => return Some(Err(e)),
        }
    }
}
