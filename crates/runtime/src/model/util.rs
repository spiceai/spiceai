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

use std::collections::HashMap;

use llms::bedrock::{BedrockClient, rate_limit::BedrockRateLimitConfigBuilder};
use secrecy::{ExposeSecret, SecretString};
use snafu::ResultExt;

/// Extract a secret from a hashmap of secrets, if it exists.
macro_rules! extract_secret {
    ($params:expr, $key:expr) => {
        $params.get($key).map(secrecy::ExposeSecret::expose_secret)
    };
}

pub(super) async fn create_bedrock_client(
    params: &HashMap<String, SecretString>,
    credential_provider_name: &'static str,
) -> Result<BedrockClient, Box<dyn std::error::Error + Send + Sync>> {
    // Build AWS config
    let mut config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest());

    // Set region if provided
    if let Some(region) = extract_secret!(params, "aws_region") {
        config_builder = config_builder.region(aws_config::Region::new(region.to_owned()));
    }

    // Set profile if provided
    if let Some(profile) = extract_secret!(params, "aws_profile") {
        config_builder = config_builder.profile_name(profile);
    }

    // Set access key and secret key if provided
    if let (Some(access_key), Some(secret_key)) = (
        extract_secret!(params, "aws_access_key_id"),
        extract_secret!(params, "aws_secret_access_key"),
    ) {
        let session_token = extract_secret!(params, "aws_session_token");

        let credentials = aws_credential_types::Credentials::new(
            access_key,
            secret_key,
            session_token.map(std::string::ToString::to_string),
            None,
            credential_provider_name,
        );

        config_builder = config_builder.credentials_provider(credentials);
    }

    let mut rate_limit_builder = BedrockRateLimitConfigBuilder::new();
    if let Some(rpm) = params
        .get("requests_per_min_limit")
        .map(|rpm| rpm.expose_secret().parse::<u32>().boxed())
        .transpose()?
    {
        let _ = rate_limit_builder.requests_per_minute(rpm);
    }

    if let Some(invocations) = params
        .get("max_concurrent_invocations")
        .map(|inv| inv.expose_secret().parse::<usize>().boxed())
        .transpose()?
    {
        let _ = rate_limit_builder.max_concurrent_invocations(invocations);
    }

    let config = config_builder.load().await;
    Ok(BedrockClient::new(&config, rate_limit_builder.build()))
}
