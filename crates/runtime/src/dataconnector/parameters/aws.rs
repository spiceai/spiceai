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

use crate::parameters::{ParamLookup, Parameters};
use aws_config::ConfigLoader;
#[cfg(feature = "dynamodb")]
use aws_sdk_credential_bridge::{
    initiate_config_auth_iam_env, initiate_config_auth_iam_metadata, initiate_config_auth_key,
    initiate_config_default_auth,
};
use snafu::prelude::*;
use tonic::async_trait;

use super::{ConnectorParams, Validator};

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
    #[snafu(display("Invalid endpoint URL '{endpoint}'. Provide a valid HTTP or HTTPS URL."))]
    InvalidEndpoint { endpoint: String },

    #[snafu(display(
        "Insecure HTTP endpoint '{endpoint}' requires 'allow_http: true' in the dataset parameters."
    ))]
    InsecureEndpointWithoutAllowHTTP { endpoint: String },

    #[snafu(display(
        "Invalid AWS region '{region}'. Specify a valid AWS region (e.g., 'us-east-1')."
    ))]
    InvalidRegion { region: String },

    #[snafu(display("Invalid AWS region corrected to '{region}'."))]
    InvalidRegionCorrected { region: String },

    #[snafu(display(
        "Invalid auth parameter combination: {parameter} requires auth 'key', not {auth}"
    ))]
    InvalidAuthParameterCombination { parameter: String, auth: String },

    #[snafu(display("No AWS region specified; defaulting to '{region}'."))]
    NoRegionSpecified { region: String },

    #[snafu(display("No auth method specified; defaulting to '{auth_name}'."))]
    NoAuthSpecified { auth_name: String },

    #[snafu(display("Missing required AWS access key. Set the 'aws_access_key_id' parameter."))]
    NoAccessKey,

    #[snafu(display(
        "Missing required AWS secret access key. Set the 'aws_secret_access_key' parameter."
    ))]
    NoAccessSecret,

    #[snafu(display(
        "Unsupported authentication method '{method}'. Supported methods: 'key', 'iam_role', 'public'."
    ))]
    UnsupportedAuthenticationMethod { method: String },

    #[snafu(display("Invalid {key}: {method}. Valid values are 'auto', 'metadata' and 'env'"))]
    InvalidAuth { key: String, method: String },

    #[snafu(display("Invalid {key}: {iam_source}. Valid values are 'auto', 'metadata' and 'env'"))]
    InvalidIamRoleSource { key: String, iam_source: String },
}

pub(crate) struct S3EndpointValidator;

#[async_trait]
impl Validator for S3EndpointValidator {
    type Error = Error;

    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Error> {
        if let Some(endpoint) = params.parameters.get("endpoint").expose().ok() {
            let endpoint: String = endpoint.to_string();
            if endpoint.ends_with('/') {
                tracing::warn!("Trimming trailing '/' from S3 endpoint {endpoint}");
                params.parameters.insert(
                    "endpoint".to_string(),
                    endpoint.trim_end_matches('/').to_string().into(),
                );
            }
            if !(endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
                return Err(Error::InvalidEndpoint { endpoint });
            }
            if endpoint.starts_with("http://")
                && params.parameters.get("allow_http").expose().ok() != Some("true")
            {
                return Err(Error::InsecureEndpointWithoutAllowHTTP { endpoint });
            }
        }
        Ok(())
    }
}

pub(crate) struct RegionValidator;

#[async_trait]
impl Validator for RegionValidator {
    type Error = Error;

    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Error> {
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
                        region: region.to_string()
                    }
                );
            }
        }
        Ok(())
    }
}

pub(crate) struct AuthValidator;

#[async_trait]
impl Validator for AuthValidator {
    type Error = Error;

    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Error> {
        match params.parameters.get("auth").expose().ok() {
            None | Some("public" | "iam_role") => {
                for param in ["key", "secret", "session_token"] {
                    if matches!(params.parameters.get(param), ParamLookup::Present(_)) {
                        return Err(Error::InvalidAuthParameterCombination {
                            parameter: param.to_string(),
                            auth: "key".to_string(),
                        });
                    }
                }
            }
            Some("key") => {
                for (param, error) in [
                    ("key", Error::NoAccessKey),
                    ("secret", Error::NoAccessSecret),
                ] {
                    if matches!(params.parameters.get(param), ParamLookup::Absent(_)) {
                        return Err(error);
                    }
                }
            }
            Some(auth) => {
                return Err(Error::UnsupportedAuthenticationMethod {
                    method: auth.to_string(),
                });
            }
        }
        Ok(())
    }
}

/// Initiate a [`ConfigLoader`] with AWS credentials as we'd expect them to be defined in [`Parameters`] (for a given `provider_name`).
///
/// Return [`ConfigLoader`] to allow further customisation.
pub async fn initiate_config_with_credentials(
    provider_name: &'static str,
    region_name: &'static str,
    key_name: &'static str,
    secret_name: &'static str,
    token_name: &'static str,
    params: &Parameters,
) -> Result<ConfigLoader, Error> {
    let region = params
        .get(region_name)
        .expose()
        .ok_or_else(|_| Error::NoRegionSpecified {
            region: region_name.to_string(),
        })?
        .to_string();

    let access_key_id = params.get(key_name).expose().ok().map(ToString::to_string);
    let secret_access_key = params
        .get(secret_name)
        .expose()
        .ok()
        .map(ToString::to_string);
    let session_token = params
        .get(token_name)
        .expose()
        .ok()
        .map(ToString::to_string);

    // Delegate to the common implementation in aws-sdk-credential-bridge
    Ok(aws_sdk_credential_bridge::initiate_config_with_credentials(
        provider_name,
        region,
        access_key_id,
        secret_access_key,
        session_token,
    )
    .await)
}

/// Initiate a [`ConfigLoader`] with AWS credentials using an explicit authentication method from [`Parameters`].
///
/// Supports two authentication methods:
/// - `iam_role`: IAM role-based authentication with configurable source (`auto`, `metadata`, `env`)
/// - `key`: Explicit access key credentials
///
/// Return [`ConfigLoader`] to allow further customisation.
#[cfg(feature = "dynamodb")]
#[expect(clippy::too_many_arguments)]
pub async fn initiate_config_with_auth_method(
    provider_name: &'static str,
    auth_name: &'static str,
    iam_role_source_name: &'static str,
    region_name: &'static str,
    key_name: &'static str,
    secret_name: &'static str,
    token_name: &'static str,
    params: &Parameters,
) -> Result<ConfigLoader, Error> {
    let region = params
        .get(region_name)
        .expose()
        .ok_or_else(|_| Error::NoRegionSpecified {
            region: region_name.to_string(),
        })?
        .to_string();

    let auth = params
        .get(auth_name)
        .expose()
        .ok_or_else(|_| Error::NoAuthSpecified {
            auth_name: auth_name.to_string(),
        })?
        .to_string();

    Ok(match auth.as_str() {
        "iam_role" => {
            let iam_role_source = params.get(iam_role_source_name).expose().ok();

            match iam_role_source {
                Some("metadata") => initiate_config_auth_iam_metadata(region),
                Some("env") => initiate_config_auth_iam_env(region),
                Some("auto") | None => initiate_config_default_auth(region).await,
                Some(other) => {
                    return Err(Error::InvalidIamRoleSource {
                        key: iam_role_source_name.to_string(),
                        iam_source: other.to_string(),
                    });
                }
            }
        }
        "key" => {
            let access_key_id = params
                .get(key_name)
                .expose()
                .ok_or_else(|_| Error::NoAccessKey)?
                .to_string();
            let secret_access_key = params
                .get(secret_name)
                .expose()
                .ok_or_else(|_| Error::NoAccessSecret)?
                .to_string();
            let session_token = params
                .get(token_name)
                .expose()
                .ok()
                .map(ToString::to_string);

            initiate_config_auth_key(
                provider_name,
                region,
                access_key_id,
                secret_access_key,
                session_token,
            )
        }
        _ => {
            return Err(Error::InvalidAuth {
                key: auth_name.to_string(),
                method: auth,
            });
        }
    })
}
