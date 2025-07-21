/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::sync::Arc;

use async_trait::async_trait;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::provider::ProvideCredentials;
use iceberg::io::{AwsCredential, AwsCredentialLoad, CustomAwsCredentialLoader};
use reqwest::Client;
use snafu::prelude::*;

use crate::{FailedToGetCredentialsSnafu, Result};

#[derive(Debug)]
pub struct S3CredentialProvider {
    credentials: aws_credential_types::provider::SharedCredentialsProvider,
}

impl S3CredentialProvider {
    #[must_use]
    pub fn new(credentials: aws_credential_types::provider::SharedCredentialsProvider) -> Self {
        Self { credentials }
    }

    /// Loads credentials from the environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials cannot be loaded from the environment.
    pub async fn from_env() -> Result<(Self, SdkConfig)> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

        let credentials = config
            .credentials_provider()
            .context(FailedToGetCredentialsSnafu)?;

        Ok((Self { credentials }, config))
    }

    /// Loads credentials from a given SDK configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials cannot be loaded from the provided config.
    pub fn from_config(config: &SdkConfig) -> Result<Self> {
        let credentials = config
            .credentials_provider()
            .context(FailedToGetCredentialsSnafu)?;

        Ok(Self { credentials })
    }

    #[must_use]
    pub fn into_custom_loader(self) -> CustomAwsCredentialLoader {
        CustomAwsCredentialLoader::new(Arc::new(self))
    }
}

#[async_trait]
impl AwsCredentialLoad for S3CredentialProvider {
    async fn load_credential(&self, _client: Client) -> anyhow::Result<Option<AwsCredential>> {
        let creds = self.credentials.provide_credentials().await?;
        Ok(Some(AwsCredential {
            access_key_id: creds.access_key_id().to_string(),
            secret_access_key: creds.secret_access_key().to_string(),
            session_token: creds.session_token().map(ToString::to_string),
            expires_in: creds.expiry().map(Into::into),
        }))
    }
}
