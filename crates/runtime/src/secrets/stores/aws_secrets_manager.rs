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

use async_trait::async_trait;
use aws_sdk_sts::operation::get_caller_identity::GetCallerIdentityError;
use secrecy::SecretString;

use crate::secrets::SecretStore;

use aws_sdk_secretsmanager::{error::SdkError, operation::get_secret_value::GetSecretValueError};

use aws_config::{self, BehaviorVersion};
use aws_sdk_secretsmanager::{self};

use snafu::{OptionExt, ResultExt, Snafu};

const SPICE_KEY_PREFIX: &str = "spice_";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("AWS identity verification failed, check configuration with `aws configure list` and `aws sts get-caller-identity`: {}", source))]
    UnableToVerifyAwsIdentity {
        source: SdkError<GetCallerIdentityError>,
    },

    #[snafu(display("Unable to parse AWS secret as JSON: {source}"))]
    UnableToParseJson { source: serde_json::Error },

    #[snafu(display("Invalid AWS secret value: JSON object is expected"))]
    InvalidJsonFormat {},

    #[snafu(display("Unable to get AWS secret: {source}"))]
    UnableToGetSecret {
        source: SdkError<GetSecretValueError>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct AwsSecretsManager {
    secret_name: String,
}

impl AwsSecretsManager {
    #[must_use]
    pub fn new(secret_name: String) -> Self {
        Self { secret_name }
    }

    /// Initializes AWS configuration and verifies AWS credentials.
    ///
    /// # Errors
    ///
    /// This function will return an error:
    /// - If the AWS configuration cannot be loaded.
    /// - If the call to STS `get_caller_identity` fails, which might be due to invalid or expired AWS credentials.
    pub async fn init(&self) -> Result<()> {
        let config = aws_config::defaults(BehaviorVersion::v2024_03_28())
            .load()
            .await;

        let sts_client = aws_sdk_sts::Client::new(&config);

        sts_client
            .get_caller_identity()
            .send()
            .await
            .context(UnableToVerifyAwsIdentitySnafu)?;

        Ok(())
    }
}

#[async_trait]
impl SecretStore for AwsSecretsManager {
    #[must_use]
    async fn get_secret(&self, key: &str) -> crate::secrets::AnyErrorResult<Option<SecretString>> {
        tracing::trace!(
            "Getting secret {} from AWS Secrets Manager",
            self.secret_name
        );

        let config = aws_config::defaults(BehaviorVersion::v2024_03_28())
            .load()
            .await;

        let asm = aws_sdk_secretsmanager::Client::new(&config);

        let secret_value = match asm
            .get_secret_value()
            .secret_id(&self.secret_name)
            .send()
            .await
        {
            Ok(secret) => secret,
            Err(SdkError::ServiceError(e)) => {
                // It is expected that not all parameters are present in secrets.
                // Pass through only if it is a different error
                if !e.err().is_resource_not_found_exception() {
                    return Err(Box::new(Error::UnableToGetSecret {
                        source: SdkError::ServiceError(e),
                    }));
                }
                return Ok(None);
            }
            Err(err) => {
                return Err(Box::new(Error::UnableToGetSecret { source: err }));
            }
        };

        let prefixed_key = format!("{SPICE_KEY_PREFIX}{key}");
        if let Some(secret_str) = secret_value.secret_string() {
            let data = parse_json_to_hashmap(secret_str)?;
            if let Some(value) = data.get(&prefixed_key) {
                return Ok(Some(SecretString::new(value.clone())));
            }
            return Ok(data.get(key).cloned().map(SecretString::new));
        }

        Ok(None)
    }
}

/// Parses a JSON string into a `HashMap<String, String>`.
///
/// # Errors
///
/// This function will return an error if:
/// - The input string cannot be parsed as a valid JSON object.
pub fn parse_json_to_hashmap(json_str: &str) -> Result<HashMap<String, String>> {
    let parsed: serde_json::Value =
        serde_json::from_str(json_str).context(UnableToParseJsonSnafu)?;
    let root = parsed.as_object().context(InvalidJsonFormatSnafu)?;

    let mut data = HashMap::new();
    for (key, value) in root {
        if let Some(value_str) = value.as_str() {
            data.insert(key.clone(), value_str.to_string());
        }
    }

    Ok(data)
}
