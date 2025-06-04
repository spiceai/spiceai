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

use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::Credentials;
use snafu::prelude::*;
use tonic::async_trait;

use crate::parameters::{ParamLookup, Parameters};

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
    #[snafu(display("Invalid endpoint: {endpoint}"))]
    InvalidEndpoint { endpoint: String },

    #[snafu(display("Insecure endpoint without allow_http: {endpoint}"))]
    InsecureEndpointWithoutAllowHTTP { endpoint: String },

    #[snafu(display("Invalid region: {region}"))]
    InvalidRegion { region: String },

    #[snafu(display("Invalid region corrected: {region}"))]
    InvalidRegionCorrected { region: String },

    #[snafu(display(
        "Invalid auth parameter combination: {parameter} requires auth 'key', not {auth}"
    ))]
    InvalidAuthParameterCombination { parameter: String, auth: String },

    #[snafu(display("No region specified using {region}"))]
    NoRegionSpecified { region: String },

    #[snafu(display("Missing access key"))]
    NoAccessKey,

    #[snafu(display("Missing access secret"))]
    NoAccessSecret,

    #[snafu(display("Unsupported authentication method: {method}"))]
    UnsupportedAuthenticationMethod { method: String },
}

pub(crate) struct S3EndpointValidator;

#[async_trait]
impl Validator for S3EndpointValidator {
    type Error = Error;

    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Error> {
        if let Some(endpoint) = params.parameters.get("endpoint").expose().ok() {
            let endpoint = endpoint.to_string();
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

pub async fn load_config(
    provider_name: &'static str,
    region_name: &'static str,
    key_name: &'static str,
    secret_name: &'static str,
    token_name: &'static str,
    params: &Parameters,
) -> Result<SdkConfig, Error> {
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

    Ok(match (access_key_id, secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => {
            let credentials = Credentials::new(
                access_key_id,
                secret_access_key,
                session_token,
                None,
                provider_name,
            );

            aws_config::defaults(BehaviorVersion::v2025_01_17())
                .region(Region::new(region))
                .credentials_provider(credentials)
                .load()
                .await
        }
        _ => {
            // This will automatically load AWS credentials from the environment, via IAM roles if configured.
            aws_config::defaults(BehaviorVersion::v2025_01_17())
                .region(Region::new(region))
                .load()
                .await
        }
    })
}
