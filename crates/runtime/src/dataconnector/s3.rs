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
    listing::{self, ListingTableConnector},
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, ParameterSpec, Parameters,
};

use crate::parameters::ParamLookup;
use crate::{component::dataset::Dataset, dataconnector::listing::LISTING_TABLE_PARAMETERS};

use snafu::prelude::*;
use std::any::Any;
use std::clone::Clone;
use std::future::Future;
use std::pin::Pin;
use std::string::String;
use std::sync::{Arc, LazyLock};
use url::Url;

// https://docs.aws.amazon.com/general/latest/gr/rande.html
pub const AWS_REGIONS: [&str; 32] = [
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "af-south-1",
    "ap-east-1",
    "ap-south-1",
    "ap-south-2",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ap-southeast-5",
    "ca-central-1",
    "ca-west-1",
    "eu-central-1",
    "eu-central-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-south-1",
    "eu-south-2",
    "eu-north-1",
    "sa-east-1",
    "il-central-1",
    "me-south-1",
    "me-central-1",
    "us-gov-east-1",
    "us-gov-west-1",
];

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("S3 auth method 'key' requires an AWS access secret.\nSpecify an access secret with the `s3_secret` parameter.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"))]
    NoAccessSecret,

    #[snafu(display("S3 auth method 'key' requires an AWS access key.\nSpecify an access key with the `s3_key` parameter.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"))]
    NoAccessKey,

    #[snafu(display("Unsupported S3 auth method '{method}'.\nUse 'public', 'iam_role', or 'key' for `s3_auth` parameter.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"))]
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

    #[snafu(display("IAM role authentication failed.\nAre you sure you're running in an environment with an IAM role?\n{source}\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#auth"))]
    InvalidIAMRoleAuthentication {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("The '{endpoint}' is a HTTP URL, but `allow_http` is not enabled. Set the parameter `allow_http: true` and retry.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/abfs#params"))]
    InsecureEndpointWithoutAllowHTTP { endpoint: String },
}

pub struct S3 {
    params: Parameters,
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
static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut all_parameters = Vec::new();
    all_parameters.extend_from_slice(&[
            ParameterSpec::connector("region").secret(),
            ParameterSpec::connector("endpoint").secret(),
            ParameterSpec::connector("key").secret(),
            ParameterSpec::connector("secret").secret(),
            ParameterSpec::connector("auth")
                .description("Configures the authentication method for S3. Supported methods are: public (i.e. no auth), iam_role, key.")
                .secret(),
            ParameterSpec::runtime("client_timeout")
                .description("The timeout setting for S3 client."),
            ParameterSpec::runtime("allow_http")
                .description("Allow HTTP protocol for S3 endpoint."),
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
            if let Some(endpoint) = params.parameters.get("endpoint").expose().ok() {
                if !(endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
                    return Err(Box::new(Error::InvalidEndpoint {
                        endpoint: endpoint.to_string(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }

                if endpoint.starts_with("http://")
                    && params.parameters.get("allow_http").expose().ok() != Some("true")
                {
                    return Err(Box::new(Error::InsecureEndpointWithoutAllowHTTP {
                        endpoint: endpoint.to_string(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
            }

            if let Some(region) = params.parameters.get("region").expose().ok() {
                if AWS_REGIONS.contains(&region.to_lowercase().as_str())
                    && !AWS_REGIONS.contains(&region)
                {
                    tracing::warn!(
                        "{}",
                        Error::InvalidRegionCorrected {
                            region: region.to_string()
                        }
                    );
                    params
                        .parameters
                        .insert("region".to_string(), region.to_lowercase().into());
                } else if !AWS_REGIONS.contains(&region) {
                    tracing::warn!(
                        "{}",
                        Error::InvalidRegion {
                            region: region.to_string(),
                        }
                    );
                }
            }

            match params.parameters.get("auth").expose().ok() {
                None | Some("public" | "iam_role") => {
                    if matches!(params.parameters.get("key"), ParamLookup::Present(_)) {
                        // The 's3_key' parameter cannot be set unless the `s3_auth` parameter is set to 'key'.
                        return Err(Box::new(Error::InvalidAuthParameterCombination {
                            parameter: "s3_key".to_string(),
                            auth: "key".to_string(),
                        })
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                    if matches!(params.parameters.get("secret"), ParamLookup::Present(_)) {
                        // The 's3_secret' parameter cannot be set unless the `s3_auth` parameter is set to 'key'.
                        return Err(Box::new(Error::InvalidAuthParameterCombination {
                            parameter: "s3_secret".to_string(),
                            auth: "key".to_string(),
                        })
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                }
                Some("key") => {
                    if matches!(params.parameters.get("key"), ParamLookup::Absent(_)) {
                        return Err(Box::new(Error::NoAccessKey)
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                    if matches!(params.parameters.get("secret"), ParamLookup::Absent(_)) {
                        return Err(Box::new(Error::NoAccessSecret)
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                }
                Some(auth) => {
                    return Err(Box::new(Error::UnsupportedAuthenticationMethod {
                        method: auth.to_string(),
                    })
                        as Box<dyn std::error::Error + Send + Sync>);
                }
            };

            let s3 = S3 {
                params: params.parameters,
            };
            Ok(Arc::new(s3) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "s3"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
    }
}

impl std::fmt::Display for S3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s3")
    }
}

impl ListingTableConnector for S3 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let mut s3_url =
            Url::parse(&dataset.from)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!("The specified URL is not valid: {}.\nEnsure the URL is valid and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/s3#from", dataset.from),
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
