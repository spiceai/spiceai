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

use std::collections::HashMap;

use async_trait::async_trait;

use super::{Secret, SecretStore};

use aws_sdk_secretsmanager::error::SdkError;

use aws_config::{self, BehaviorVersion};
use aws_sdk_secretsmanager::{self};

use snafu::Snafu;

const SPICE_SECRET_PREFIX: &str = "spice_secret_";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("AWS identify verification failed, check configuration with `aws configure list` and `aws sts get-caller-identity`."))]
    InvalidAwsCredentials {},
}

#[allow(clippy::module_name_repetitions)]
#[derive(Copy, Clone)]
pub struct AwsSecretsManager {}

impl Default for AwsSecretsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AwsSecretsManager {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    /// Initializes AWS configuration and verifies AWS credentials.
    ///
    /// # Errors
    ///
    /// This function will return an error:
    /// - If the AWS configuration cannot be loaded.
    /// - If the call to STS `get_caller_identity` fails, which might be due to invalid or expired AWS credentials.
    pub async fn init(self) -> Result<(), Box<dyn std::error::Error>> {
        let config = aws_config::defaults(BehaviorVersion::v2023_11_09())
            .load()
            .await;

        let sts_client = aws_sdk_sts::Client::new(&config);

        if sts_client.get_caller_identity().send().await.is_err() {
            return Err(Box::new(Error::InvalidAwsCredentials {}));
        }

        Ok(())
    }
}

#[async_trait]
impl SecretStore for AwsSecretsManager {
    #[must_use]
    async fn get_secret(&self, secret_name: &str) -> Option<Secret> {
        let secret_name = format!("{SPICE_SECRET_PREFIX}{secret_name}");

        tracing::trace!("Getting secret {} from AWS Secrets Manager", secret_name);

        let config = aws_config::defaults(BehaviorVersion::v2023_11_09())
            .load()
            .await;

        let asm = aws_sdk_secretsmanager::Client::new(&config);

        let secret_value = match asm.get_secret_value().secret_id(&secret_name).send().await {
            Ok(secret) => secret,
            Err(SdkError::ServiceError(e)) => {
                // It is expected that not all parameters are present in secrets.
                // Warn only if it is a different error
                if !e.err().is_resource_not_found_exception() {
                    tracing::warn!(
                        "Failed to get secret {} from AWS Secrets Manager: {}",
                        secret_name,
                        e.err()
                    );
                }
                return None;
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to get secret {} from AWS Secrets Manager: {}",
                    secret_name,
                    err
                );
                return None;
            }
        };

        if let Some(secret_str) = secret_value.secret_string() {
            match parse_json_to_hashmap(secret_str) {
                Ok(data) => return Some(Secret::new(data)),
                Err(err) => {
                    tracing::warn!("Failed to parse secret {} content: {}", secret_name, err);
                    return None;
                }
            }
        }

        None
    }
}
/// Parses a JSON string into a `HashMap<String, String>`.
///
/// # Errors
///
/// This function will return an error if:
/// - The input string cannot be parsed as a valid JSON.
/// - The parsed JSON is not an object. This function expects the top-level JSON structure to be a JSON object (`{}`).
pub fn parse_json_to_hashmap(
    json_str: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let parsed: serde_json::Value = serde_json::from_str(json_str)?;
    let object = parsed.as_object().ok_or("String is not a JSON object")?;

    let mut data = HashMap::new();
    for (key, value) in object {
        if let Some(value_str) = value.as_str() {
            data.insert(key.clone(), value_str.to_string());
        }
    }

    Ok(data)
}
