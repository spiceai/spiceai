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

use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use aws_config::SdkConfig;

use aws_credential_types::Credentials;
use aws_runtime::auth::sigv4::SigV4AuthScheme;
use aws_sdk_s3::{
    Client,
    config::{
        ConfigBag, IdentityCache, ResolveCachedIdentity, SharedIdentityCache,
        auth::{DefaultAuthSchemeResolver, ResolveAuthScheme},
        endpoint::{DefaultResolver, ResolveEndpoint},
    },
};
use aws_smithy_runtime::client::retries::strategy::StandardRetryStrategy;
use aws_smithy_runtime_api::client::{
    auth::{AuthSchemeId, SharedAuthScheme},
    identity::SharedIdentityResolver,
    runtime_components::{RuntimeComponents, RuntimeComponentsBuilder},
};
use iceberg_storage_opendal::{AwsCredential as IcebergAwsCredential, CustomAwsCredentialLoader};
use object_store::{CredentialProvider, aws::AwsCredential as ObjectStoreAwsCredential};
use reqsign_core::{Context, Error as ReqsignError, ProvideCredential, Result as ReqsignResult};
use snafu::prelude::*;

use crate::{Error, FailedToBuildAWSRuntimeComponentsSnafu, Result};

#[derive(Debug)]
pub struct S3CredentialProvider {
    runtime: RuntimeComponents,
    cache: SharedIdentityCache,
    identity_resolver: SharedIdentityResolver,
}

impl S3CredentialProvider {
    /// Loads credentials from the environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials cannot be loaded from the environment.
    pub async fn from_env() -> Result<(Self, SdkConfig)> {
        Self::from_env_with_region(None).await
    }

    /// Loads credentials from the environment with an explicit region when available.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials cannot be loaded from the environment.
    pub async fn from_env_with_region(region: Option<&str>) -> Result<(Self, SdkConfig)> {
        let config = super::default_aws_config_for_region(region).load().await;

        Ok((Self::from_config(&config)?, config))
    }

    /// Loads credentials from a given SDK configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials cannot be loaded from the provided config.
    pub fn from_config(sdk_config: &SdkConfig) -> Result<Self> {
        let credentials_provider = sdk_config
            .credentials_provider()
            .ok_or_else(|| Error::FailedToGetCredentialsProviderFromConfig)?;
        Ok(Self {
            cache: IdentityCache::lazy().build(),
            runtime: Self::build_aws_runtime_components(sdk_config, &Client::new(sdk_config))?,
            identity_resolver: SharedIdentityResolver::new(credentials_provider),
        })
    }

    #[must_use]
    pub fn into_custom_loader(self) -> CustomAwsCredentialLoader {
        CustomAwsCredentialLoader::new(Arc::new(self))
    }

    fn build_aws_runtime_components(
        sdk_config: &SdkConfig,
        client: &Client,
    ) -> Result<RuntimeComponents> {
        RuntimeComponentsBuilder::new("S3CredentialProvider")
            .with_auth_scheme_option_resolver(Some(
                DefaultAuthSchemeResolver::default().into_shared_resolver(),
            ))
            .with_endpoint_resolver(Some(DefaultResolver::new().into_shared_resolver()))
            .with_auth_scheme(SharedAuthScheme::new(SigV4AuthScheme::new()))
            .with_identity_cache(Some(IdentityCache::lazy().build()))
            .with_identity_resolver(
                AuthSchemeId::new("SpiceS3CredentialProvider"),
                SharedIdentityResolver::new(
                    sdk_config
                        .credentials_provider()
                        .ok_or_else(|| Error::FailedToGetCredentialsProviderFromConfig)?,
                ),
            )
            .with_retry_strategy(Some(StandardRetryStrategy::new()))
            .with_time_source(client.config().time_source())
            .with_sleep_impl(client.config().sleep_impl())
            .build()
            .context(FailedToBuildAWSRuntimeComponentsSnafu)
    }
}

impl ProvideCredential for S3CredentialProvider {
    type Credential = IcebergAwsCredential;

    async fn provide_credential(&self, _ctx: &Context) -> ReqsignResult<Option<Self::Credential>> {
        // `resolve_cached_identity` will first check the cache for valid, unexpired credentials, and fetch new credentials if needed.
        // The identity resolver and runtime components are required parameters for this function, which is why they're fields of this struct.
        let wrapped_credentials = self
            .cache
            .resolve_cached_identity(
                self.identity_resolver.clone(),
                &self.runtime,
                &ConfigBag::base(),
            )
            .await
            .map_err(|err| {
                tracing::error!(
                    error = %err,
                    "Failed to resolve AWS credentials from identity cache"
                );
                ReqsignError::credential_invalid(format!(
                    "Failed to find valid credentials from the AWS credential provider chain for the S3 connection: {err}. Ensure that valid AWS credentials are provided in the environment. Details: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html#credproviders-default-credentials-provider-chain"
                ))
            })?;

        let credentials = wrapped_credentials.data::<Credentials>().ok_or_else(|| {
            tracing::error!("Resolved identity does not contain AWS credentials");
            ReqsignError::credential_invalid("Failed to find valid credentials from the AWS credential provider chain for the S3 connection. The resolved identity does not contain credential data. Ensure that valid AWS credentials are provided in the environment. Details: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html#credproviders-default-credentials-provider-chain")
        })?;

        Ok(Some(IcebergAwsCredential {
            access_key_id: credentials.access_key_id().to_string(),
            secret_access_key: credentials.secret_access_key().to_string(),
            session_token: credentials.session_token().map(ToString::to_string),
            expires_in: credential_expiry(credentials)?,
        }))
    }
}

fn credential_expiry(
    credentials: &Credentials,
) -> ReqsignResult<Option<reqsign_core::time::Timestamp>> {
    let Some(expiry) = credentials.expiry() else {
        return Ok(None);
    };

    let duration = expiry
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|err| {
            ReqsignError::credential_invalid(
                "Resolved AWS credential expiry is before the Unix epoch",
            )
            .with_source(err)
        })?;
    let millis = i64::try_from(duration.as_millis()).map_err(|err| {
        ReqsignError::credential_invalid(
            "Resolved AWS credential expiry exceeds the supported timestamp range",
        )
        .with_source(err)
    })?;

    reqsign_core::time::Timestamp::from_millisecond(millis)
        .map(Some)
        .map_err(|err| {
            ReqsignError::credential_invalid(
                "Resolved AWS credential expiry is outside the supported timestamp range",
            )
            .with_source(err)
        })
}

#[async_trait]
impl CredentialProvider for S3CredentialProvider {
    type Credential = ObjectStoreAwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        // `resolve_cached_identity` will first check the cache for valid, unexpired credentials, and fetch new credentials if needed.
        // The identity resolver and runtime components are required parameters for this function, which is why they're fields of this struct.
        let wrapped_credentials = self
            .cache
            .resolve_cached_identity(
                self.identity_resolver.clone(),
                &self.runtime,
                &ConfigBag::base(),
            )
            .await
            .map_err(|err| {
                tracing::error!(
                    error = %err,
                    "Failed to resolve AWS credentials from identity cache"
                );
                object_store::Error::Generic {
                    store: "S3",
                    source: format!("Failed to find valid credentials from the AWS credential provider chain for the S3 connection: {err}. Ensure that valid AWS credentials are provided in the environment. Details: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html#credproviders-default-credentials-provider-chain").into(),
                }
            })?;

        let credentials = wrapped_credentials.data::<Credentials>().ok_or_else(|| {
            tracing::error!("Resolved identity does not contain AWS credentials");
            object_store::Error::Generic {
                store: "S3",
                source: "Failed to find valid credentials from the AWS credential provider chain for the S3 connection. The resolved identity does not contain credential data. Ensure that valid AWS credentials are provided in the environment. Details: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html#credproviders-default-credentials-provider-chain".into(),
            }
        })?;

        Ok(Arc::new(ObjectStoreAwsCredential {
            key_id: credentials.access_key_id().to_string(),
            secret_key: credentials.secret_access_key().to_string(),
            token: credentials.session_token().map(ToString::to_string),
        }))
    }
}
