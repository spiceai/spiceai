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

#[cfg(feature = "bedrock")]
use std::{collections::HashMap, num::NonZeroU32};

#[cfg(feature = "bedrock")]
use governor::Quota;
#[cfg(feature = "bedrock")]
use llms::bedrock::BedrockClient;
#[cfg(feature = "bedrock")]
use runtime_rate_control::RateControllerBuilder;
#[cfg(feature = "bedrock")]
use secrecy::{ExposeSecret, SecretString};
#[cfg(feature = "bedrock")]
use snafu::ResultExt;

// Maximum number of concurrently running requests.
// The overall request rate is controlled by the rate_limiter.
#[cfg(feature = "bedrock")]
const DEFAULT_MAX_CONCURRENT_INVOCATIONS: usize = 40;

/// Extract a secret from a hashmap of secrets, if it exists.
#[cfg(feature = "bedrock")]
macro_rules! extract_secret {
    ($params:expr, $key:expr) => {
        $params.get($key).map(secrecy::ExposeSecret::expose_secret)
    };
}

/// Why a Bedrock client could not be built: no AWS credentials, with the message that says how to
/// give some, or another failure.
#[cfg(feature = "bedrock")]
#[derive(Debug)]
pub(super) enum BedrockClientError {
    NoCredentials(String),
    Other(Box<dyn std::error::Error + Send + Sync>),
}

#[cfg(feature = "bedrock")]
impl From<Box<dyn std::error::Error + Send + Sync>> for BedrockClientError {
    fn from(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::Other(error)
    }
}

/// `error` as a model's loading error. No credentials is not `FailedToLoadModel`, which presents
/// the cause as a bug to report.
#[cfg(feature = "bedrock")]
pub(super) fn bedrock_client_error(error: BedrockClientError) -> llms::chat::Error {
    match error {
        BedrockClientError::NoCredentials(message) => llms::chat::Error::FailedToAuthenticate {
            source: message.into(),
        },
        BedrockClientError::Other(source) => llms::chat::Error::FailedToLoadModel { source },
    }
}

/// The message for a Bedrock model or embedding with no AWS credentials: `reason` lists each
/// source tried and what it said.
#[cfg(feature = "bedrock")]
pub(super) fn bedrock_no_credential_message(component: &str, reason: &str) -> String {
    format!(
        "Failed to authenticate {component}: {reason}. On EKS, use IRSA or Pod Identity; in GitHub Actions, set `AWS_ROLE_ARN` and `permissions: id-token: write`; locally, sign in with the AWS CLI or set `aws_access_key_id`. See: https://spiceai.org/docs/components/models/bedrock"
    )
}

/// A Bedrock client for `component`, such as `model 'claude' (bedrock)`, as its `params`
/// configure it. The AWS credentials are found once here: the load log names where they came
/// from, and when there are none, the error says what was tried before any request.
#[cfg(feature = "bedrock")]
pub(super) async fn create_bedrock_client(
    component: &str,
    params: &HashMap<String, SecretString>,
    credential_provider_name: &'static str,
) -> Result<BedrockClient, BedrockClientError> {
    // Extract credentials from params
    let region = extract_secret!(params, "aws_region")
        .map(std::string::ToString::to_string)
        .unwrap_or_else(|| "us-east-1".to_string());

    let access_key_id =
        extract_secret!(params, "aws_access_key_id").map(std::string::ToString::to_string);

    let secret_access_key =
        extract_secret!(params, "aws_secret_access_key").map(std::string::ToString::to_string);

    let session_token =
        extract_secret!(params, "aws_session_token").map(std::string::ToString::to_string);

    let iam_role_source = extract_secret!(params, "aws_iam_role_source");
    let static_keys = access_key_id.is_some() && secret_access_key.is_some();
    let static_source = if static_keys {
        "the access key in `aws_access_key_id`"
    } else {
        match iam_role_source {
            Some("metadata") => "instance or container metadata (`aws_iam_role_source: metadata`)",
            Some("env") => "environment keys (`aws_iam_role_source: env`)",
            _ => "the configured AWS credentials",
        }
    };

    // Use common credential initialization from aws-sdk-credential-bridge
    let mut config_builder = aws_sdk_credential_bridge::initiate_config_with_credentials(
        credential_provider_name,
        region,
        access_key_id,
        secret_access_key,
        session_token,
        iam_role_source,
    )
    .await;

    // Set profile if provided (additional config not handled by initiate_config_with_credentials).
    // The credential chain, when it is the one used, reads it too.
    if let Some(profile) = extract_secret!(params, "aws_profile") {
        config_builder = config_builder.profile_name(profile);
    }

    let mut rate_limit_builder = RateControllerBuilder::default();
    let rpm = if let Some(rpm) = params
        .get("requests_per_min_limit")
        .map(|rpm| rpm.expose_secret().parse::<NonZeroU32>().boxed())
        .transpose()?
    {
        rpm
    } else {
        let Some(rpm) = NonZeroU32::new(1_500) else {
            unreachable!("Default RPM should always be non-zero");
        };

        rpm
    };

    rate_limit_builder = rate_limit_builder.add_quota(Quota::per_minute(rpm));

    let max_concurrent_requests = if let Some(invocations) = params
        .get("max_concurrent_invocations")
        .map(|inv| inv.expose_secret().parse::<usize>().boxed())
        .transpose()?
    {
        invocations
    } else {
        DEFAULT_MAX_CONCURRENT_INVOCATIONS
    };

    rate_limit_builder = rate_limit_builder.with_max_concurrent_requests(max_concurrent_requests);

    let config = config_builder.load().await;
    if let Some(provider) = config.credentials_provider() {
        match aws_sdk_credential_bridge::named_chain::credential_source(&provider, static_source)
            .await
        {
            Ok(source) => {
                tracing::info!("Authenticated {component} with AWS credentials from {source}.");
            }
            Err(reason) => {
                return Err(BedrockClientError::NoCredentials(
                    bedrock_no_credential_message(component, &reason),
                ));
            }
        }
    }
    Ok(BedrockClient::new(&config, rate_limit_builder.build()))
}
