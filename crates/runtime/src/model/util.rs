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

use std::{collections::HashMap, num::NonZeroU32};

use governor::Quota;
use llms::bedrock::BedrockClient;
use runtime_rate_control::RateControllerBuilder;
use secrecy::{ExposeSecret, SecretString};
use snafu::ResultExt;

// Maximum number of concurrently running requests.
// The overall request rate is controlled by the rate_limiter.
const DEFAULT_MAX_CONCURRENT_INVOCATIONS: usize = 40;

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
    Ok(BedrockClient::new(&config, rate_limit_builder.build()))
}
