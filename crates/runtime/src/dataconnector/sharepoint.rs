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
    error::AuthorizationFailure,
    identity::{AuthorizationCodeCredential, ConfidentialClientApplication},
    GraphClient,
};
use snafu::{ResultExt, Snafu};
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use url::Url;

use super::{DataConnector, DataConnectorFactory, DataConnectorResult, ParameterSpec, Parameters};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: {parameter}"))]
    MissingParameter { parameter: String },

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

/// Create a [`GraphClient`] from a client authorization code (i.e. OAuth 2.0 authorization code flow).
/// `<https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-auth-code-flow>`
#[must_use]
pub fn auth_code_grant_secret(
    authorization_code: &str,
    client_id: &str,
    client_secret: &str,
    scope: Vec<String>,
    redirect_uri: Url,
) -> GraphClient {
    GraphClient::from(
        &ConfidentialClientApplication::builder(client_id)
            .with_auth_code(authorization_code)
            .with_client_secret(client_secret)
            .with_scope(scope)
            .with_redirect_uri(redirect_uri)
            .build(),
    )
}

/// [`Url`] for users to sign in to the application. Returns state + authorization URL.
pub fn authorization_sign_in_url(
    client_id: &str,
    state: &str,
    tenant_id: &str,
    redirect_uri: Url,
    scope: Vec<String>,
) -> std::result::Result<Url, AuthorizationFailure> {
    AuthorizationCodeCredential::authorization_url_builder(client_id)
        .with_redirect_uri(redirect_uri)
        .with_scope(scope)
        .with_tenant(tenant_id)
        .with_state(state)
        .url()
}

static NEEDED_SCOPE: [&str; 2] = ["User.Read", "Files.Read"];
static REDIRECT_URL: &str = "http://localhost:8090/redirect/sharepoint";

impl Sharepoint {
    fn new(params: &Parameters) -> Result<Self> {
        let client_id = params
            .get("client_id")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;
        let client_secret = params
            .get("client_secret")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;
        let tenant_id = params
            .get("tenant_id")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;
        let redirect_url_str = params
            .get("redirect_url")
            .expose()
            .ok()
            .unwrap_or(REDIRECT_URL);
        let redirect_url = Url::parse(redirect_url_str).map_err(|_| {
            InvalidRedirectUrlSnafu {
                url: redirect_url_str.to_string(),
            }
            .build()
        })?;

        let graph_client =
            if let Some(authorization_code) = params.get("authorization_code").expose().ok() {
                Ok(auth_code_grant_secret(
                    authorization_code,
                    client_id,
                    client_secret,
                    NEEDED_SCOPE.map(ToString::to_string).to_vec(),
                    redirect_url,
                ))
            } else {
                let authorization_url = authorization_sign_in_url(
                    client_id,
                    "",
                    tenant_id,
                    redirect_url,
                    NEEDED_SCOPE.map(ToString::to_string).to_vec(),
                )
                .context(AuthorizationUrlSnafu)?;
                println!("Please sign in to the following URL: {authorization_url}");

                Err(AuthorizationCodeSnafu {}.build())?
            }?;

        Ok(Sharepoint {
            client: Arc::new(graph_client),
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
    ParameterSpec::runtime("client_id")
        .secret()
        .description("")
        .required(),
    ParameterSpec::runtime("client_secret")
        .secret()
        .description("")
        .required(),
    ParameterSpec::runtime("tenant_id")
        .secret()
        .description("")
        .required(),
    ParameterSpec::runtime("redirect_url").description(""),
    ParameterSpec::runtime("authorization_code")
        .secret()
        .description(""),
];

impl DataConnectorFactory for SharepointFactory {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(Sharepoint::new(&params)?) as Arc<dyn DataConnector>) })
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
        _dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Arc::new(SharepointClient::new(Arc::clone(&self.client))))
    }

    async fn metadata_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        if !dataset.has_metadata_table {
            return None;
        }
        Some(Ok(Arc::new(SharepointClient::new(Arc::clone(
            &self.client,
        )))))
    }
}
