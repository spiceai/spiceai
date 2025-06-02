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

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, ParameterSpec, Parameters,
    listing::{self, ListingTableConnector},
    parameters::{
        self, Validator,
        aws::{AuthValidator, RegionValidator, S3EndpointValidator},
    },
};

use crate::{component::dataset::Dataset, dataconnector::listing::LISTING_TABLE_PARAMETERS};

use snafu::prelude::*;
use std::any::Any;
use std::clone::Clone;
use std::future::Future;
use std::pin::Pin;
use std::string::String;
use std::sync::{Arc, LazyLock};
use url::Url;

static PREFIX: &str = "s3";

static VALIDATORS: LazyLock<
    Vec<Box<dyn Validator<Error = parameters::aws::Error> + Send + Sync + 'static>>,
> = LazyLock::new(|| {
    vec![
        Box::new(S3EndpointValidator),
        Box::new(RegionValidator),
        Box::new(AuthValidator),
    ]
});

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "S3 auth method 'key' requires an AWS access secret.\nSpecify an access secret with the `s3_secret` parameter.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"
    ))]
    NoAccessSecret,

    #[snafu(display(
        "S3 auth method 'key' requires an AWS access key.\nSpecify an access key with the `s3_key` parameter.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"
    ))]
    NoAccessKey,

    #[snafu(display(
        "Unsupported S3 auth method '{method}'.\nUse 'public', 'iam_role', or 'key' for `s3_auth` parameter.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"
    ))]
    UnsupportedAuthenticationMethod { method: String },

    #[snafu(display(
        "The '{parameter}' parameter requires `s3_auth` set to '{auth}'.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"
    ))]
    InvalidAuthParameterCombination { parameter: String, auth: String },

    #[snafu(display(
        "The `s3_endpoint` parameter must be a HTTP/S URL, but '{endpoint}' was provided.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#params"
    ))]
    InvalidEndpoint { endpoint: String },

    #[snafu(display(
        "The `s3_region` parameter must be a valid AWS region code, but '{region}' was provided.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#params"
    ))]
    InvalidRegion { region: String },

    #[snafu(display(
        "The `s3_region` parameter requires a lowercase AWS region code, but '{region}' was provided.\nSpice will automatically convert the region code to lowercase.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#params"
    ))]
    InvalidRegionCorrected { region: String },

    #[snafu(display(
        "IAM role authentication failed.\nAre you sure you're running in an environment with an IAM role?\n{source}\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"
    ))]
    InvalidIAMRoleAuthentication {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "The '{endpoint}' is a HTTP URL, but `allow_http` is not enabled. Set the parameter `allow_http: true` and retry.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/abfs#params"
    ))]
    InsecureEndpointWithoutAllowHTTP { endpoint: String },
}

#[derive(Debug)]
pub struct S3 {
    pub(crate) params: Parameters,
}

#[derive(Default, Copy, Clone)]
pub struct S3Factory {}

impl S3Factory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

pub(crate) static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut all_parameters = Vec::new();
    all_parameters.extend_from_slice(&[
            ParameterSpec::component("region").secret(),
            ParameterSpec::component("endpoint").secret(),
            ParameterSpec::component("key").secret(),
            ParameterSpec::component("secret").secret(),
            ParameterSpec::component("session_token").secret(),
            ParameterSpec::component("auth")
                .description("Configures the authentication method for S3. Supported methods are: public (i.e. no auth), iam_role, key.")
                .secret(),
            ParameterSpec::runtime("client_timeout")
                .description("The timeout setting for S3 client."),
            ParameterSpec::runtime("allow_http")
                .description("Allow HTTP protocol for S3 endpoint.")
        ]);
    all_parameters.extend_from_slice(LISTING_TABLE_PARAMETERS);
    all_parameters
});

impl DataConnectorFactory for S3Factory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        mut params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        if let Some(endpoint) = params.parameters.get("endpoint").expose().ok() {
            if endpoint.ends_with('/') {
                tracing::warn!("Trimming trailing '/' from S3 endpoint {endpoint}");
                params.parameters.insert(
                    "endpoint".to_string(),
                    endpoint.trim_end_matches('/').to_string().into(),
                );
            }
        }

        Box::pin(async move {
            for validator in VALIDATORS.iter() {
                validator.validate(&mut params).await?;
            }

            let s3 = S3 {
                params: params.parameters,
            };
            Ok(Arc::new(s3) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        PREFIX
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
    }
}

impl std::fmt::Display for S3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{PREFIX}")
    }
}

impl ListingTableConnector for S3 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_object_store_url(
        &self,
        dataset: &Dataset,
        url: Option<&str>,
    ) -> DataConnectorResult<Url> {
        let url = url.unwrap_or(dataset.from.as_str());
        let mut s3_url =
            Url::parse(url)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!("The specified URL is not valid: {url}.\nEnsure the URL is valid and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/{PREFIX}#from"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;

        s3_url.set_fragment(Some(&listing::build_fragments(
            &self.params,
            vec![
                "region",
                "endpoint",
                "key",
                "secret",
                "client_timeout",
                "allow_http",
                "auth",
                "session_token",
            ],
        )));

        Ok(s3_url)
    }

    fn handle_object_store_error(
        &self,
        dataset: &Dataset,
        error: object_store::Error,
    ) -> DataConnectorError {
        match error {
            object_store::Error::Generic { source, .. } => {
                if self.params.get("auth").expose().ok() == Some("iam_role") {
                    let err = Error::InvalidIAMRoleAuthentication { source };

                    DataConnectorError::InvalidConfiguration {
                        dataconnector: format!("{self}"),
                        message: format!("{err}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source: err.into(),
                    }
                } else {
                    DataConnectorError::UnableToConnectInternal {
                        dataconnector: format!("{self}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source,
                    }
                }
            }
            error => DataConnectorError::UnableToConnectInternal {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
                source: error.into(),
            },
        }
    }
}
