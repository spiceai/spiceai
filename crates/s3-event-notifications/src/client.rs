/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Build the SQS client a consumer receives notifications with.
//!
//! The client is built from the same AWS configuration loaders as the rest of
//! the runtime, so the SDK's standard environment (including a service-specific
//! endpoint in `AWS_ENDPOINT_URL_SQS`) applies to it.

use snafu::Snafu;

/// The credentials an SQS client signs requests with.
///
/// A consumer resolves these from its own configuration: an S3 dataset or
/// snapshot location selects them with the same rules as its object store.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum SqsCredentials {
    /// An access key pair, with an optional session token.
    Static {
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
    },
    /// The IAM credential chain restricted to one source: `metadata` or `env`.
    RestrictedIam { source: String },
    /// The default AWS credential chain.
    DefaultChain,
}

impl std::fmt::Debug for SqsCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Static { .. } => f.write_str("Static { .. }"),
            Self::RestrictedIam { source } => f
                .debug_struct("RestrictedIam")
                .field("source", source)
                .finish(),
            Self::DefaultChain => f.write_str("DefaultChain"),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum ClientError {
    #[snafu(display("{source}"))]
    LoadConfig {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("no AWS credentials were resolved for SQS"))]
    NoCredentials,
}

/// Build an SQS client for `region` that signs with `credentials`.
///
/// # Errors
///
/// Returns [`ClientError`] when the AWS configuration for the selected
/// credential source cannot be loaded, or the default chain resolves no
/// credentials.
pub async fn build_sqs_client(
    credentials: &SqsCredentials,
    region: &str,
) -> Result<aws_sdk_sqs::Client, ClientError> {
    match credentials {
        SqsCredentials::Static {
            access_key,
            secret_key,
            session_token,
        } => {
            let credentials = aws_credential_types::Credentials::new(
                access_key,
                secret_key,
                session_token.clone(),
                None,
                "spice-sqs",
            );
            let sdk_config = aws_sdk_credential_bridge::default_aws_config()
                .region(aws_config::Region::new(region.to_string()))
                .credentials_provider(credentials)
                .load()
                .await;
            Ok(aws_sdk_sqs::Client::new(&sdk_config))
        }
        SqsCredentials::RestrictedIam { source } => {
            let sdk_config = aws_sdk_credential_bridge::build_restricted_sdk_config(
                source,
                Some(region.to_string()),
            )
            .await
            .map_err(|error| ClientError::LoadConfig {
                source: Box::new(error),
            })?;
            Ok(aws_sdk_sqs::Client::new(&sdk_config))
        }
        SqsCredentials::DefaultChain => {
            let sdk_config =
                aws_sdk_credential_bridge::get_or_init_sdk_config_with_region(Some(region))
                    .await
                    .map_err(|error| ClientError::LoadConfig {
                        source: Box::new(error),
                    })?
                    .ok_or(ClientError::NoCredentials)?;
            let sqs_config = aws_sdk_sqs::config::Builder::from(sdk_config.as_ref())
                .region(aws_config::Region::new(region.to_string()))
                .build();
            Ok(aws_sdk_sqs::Client::from_conf(sqs_config))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_does_not_print_static_keys() {
        let credentials = SqsCredentials::Static {
            access_key: "AKIAEXAMPLE".to_string(),
            secret_key: "super-secret".to_string(),
            session_token: Some("token".to_string()),
        };
        let debug = format!("{credentials:?}");
        assert!(
            !debug.contains("AKIAEXAMPLE") && !debug.contains("super-secret"),
            "SqsCredentials must not print key material, got: {debug}"
        );
    }
}
