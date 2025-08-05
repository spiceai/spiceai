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
use aws_credential_types::{Credentials, provider::ProvideCredentials};
use object_store::{CredentialProvider, aws::AwsCredential};

#[derive(Debug)]
pub struct S3CredentialProvider {
    credentials_provider: aws_credential_types::provider::SharedCredentialsProvider,
    credentials: Option<Credentials>,
}

impl S3CredentialProvider {
    #[must_use]
    pub fn new(credentials: aws_credential_types::provider::SharedCredentialsProvider) -> Self {
        Self {
            credentials_provider: credentials,
            credentials: None,
        }
    }

    #[must_use]
    pub fn try_new(
        credentials: aws_credential_types::provider::SharedCredentialsProvider,
    ) -> object_store::Result<Self> {
        Ok(Self::new(credentials).load_credentials()?)
    }

    /// Loads credentials from the environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials cannot be loaded from the environment.
    pub async fn from_env() -> object_store::Result<(Self, SdkConfig)> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

        Ok((Self::from_config(&config)?, config))
    }

    /// Creates a new `S3CredentialProvider` from the given SDK configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials provider cannot be obtained from the SDK configuration.
    pub fn from_config(sdk_config: &SdkConfig) -> object_store::Result<Self> {
        let credentials_provider =
            sdk_config
                .credentials_provider()
                .ok_or_else(|| object_store::Error::Generic {
                    store: "S3",
                    source: "Failed to get S3 credentials from the AWS SDK".into(),
                })?;

        Ok(Self {
            credentials_provider: credentials_provider.clone(),
            credentials: None,
        }
        .load_credentials()?)
    }

    fn load_credentials(self) -> object_store::Result<Self> {
        if self.credentials.is_some() {
            return Ok(self);
        }

        let handle =
            tokio::runtime::Handle::try_current().map_err(|e| object_store::Error::Generic {
                store: "S3",
                source: Box::new(e),
            })?;

        let creds = tokio::task::block_in_place(|| {
            handle.block_on(self.credentials_provider.provide_credentials())
        })
        .map_err(|e| object_store::Error::Generic {
            store: "S3",
            source: Box::new(e),
        })?;

        Ok(Self {
            credentials_provider: self.credentials_provider,
            credentials: Some(creds),
        })
    }
}

#[async_trait]
impl CredentialProvider for S3CredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let creds = if let Some(creds) = self.credentials.as_ref() {
            creds.clone()
        } else {
            self.credentials_provider
                .provide_credentials()
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "S3",
                    source: Box::new(e),
                })?
        };

        Ok(Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(ToString::to_string),
        }))
    }
}
