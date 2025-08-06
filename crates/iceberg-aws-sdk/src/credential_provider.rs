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
use iceberg::io::{AwsCredential, AwsCredentialLoad, CustomAwsCredentialLoader};

use crate::{Error, Result};

#[derive(Debug)]
pub struct S3CredentialProvider {
    runtime: RuntimeComponents,
    cache: SharedIdentityCache,
    identity_resolver: SharedIdentityResolver,
}

impl S3CredentialProvider {
    /// Attempts to create a new `S3CredentialProvider` using the provided SDK configuration.
    ///
    /// # Errors
    /// Returns an error if a credentials provider cannot be obtained from the SDK configuration,
    /// or if the AWS runtime components are not built correctly.
    pub fn try_new(config: &SdkConfig) -> Result<Self> {
        let client = aws_sdk_s3::Client::new(config);
        let runtime = Self::build_aws_runtime_components(config, &client)
            .map_err(|e| Error::InternalError { source: e.into() })?;
        let credentials_provider =
            config
                .credentials_provider()
                .ok_or_else(|| Error::InternalError {
                    source: "Failed to get credentials provider".into(),
                })?;
        Ok(Self {
            cache: IdentityCache::lazy().build(),
            runtime,
            identity_resolver: SharedIdentityResolver::new(credentials_provider),
        })
    }

    /// Loads credentials from the environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials cannot be loaded from the environment.
    pub async fn from_env() -> Result<(Self, SdkConfig)> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

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
            .ok_or_else(|| Error::FailedToGetCredentials)?;
        Ok(Self {
            cache: IdentityCache::lazy().build(),
            runtime: Self::build_aws_runtime_components(sdk_config, &Client::new(sdk_config))
                .map_err(|e| Error::InternalError { source: e.into() })?,
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
        let mut runtime_components = RuntimeComponentsBuilder::new("ServiceRuntimePlugin");
        runtime_components.set_auth_scheme_option_resolver(::std::option::Option::Some({
            DefaultAuthSchemeResolver::default().into_shared_resolver()
        }));
        runtime_components.set_endpoint_resolver(::std::option::Option::Some({
            DefaultResolver::new().into_shared_resolver()
        }));
        runtime_components.push_auth_scheme(SharedAuthScheme::new(SigV4AuthScheme::new()));

        runtime_components
            .with_identity_cache(Some(IdentityCache::lazy().build()))
            .with_identity_resolver(
                AuthSchemeId::new("hello"),
                SharedIdentityResolver::new(sdk_config.credentials_provider().ok_or_else(
                    || Error::InternalError {
                        source: "No credentials provider found in SdkConfig".into(),
                    },
                )?),
            )
            .with_retry_strategy(Some(StandardRetryStrategy::new()))
            .with_time_source(client.config().time_source())
            .with_sleep_impl(client.config().sleep_impl())
            .build()
            .map_err(|e| Error::InternalError { source: e.into() })
    }
}

#[async_trait]
impl AwsCredentialLoad for S3CredentialProvider {
    async fn load_credential(
        &self,
        _client: reqwest::Client,
    ) -> anyhow::Result<Option<AwsCredential>> {
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
            .map_err(|_| Error::FailedToGetCredentials)?;

        let credentials = wrapped_credentials
            .data::<Credentials>()
            .ok_or_else(|| Error::FailedToGetCredentials)?;

        Ok(Some(AwsCredential {
            access_key_id: credentials.access_key_id().to_string(),
            secret_access_key: credentials.secret_access_key().to_string(),
            session_token: credentials.session_token().map(ToString::to_string),
            expires_in: credentials.expiry().map(Into::into),
        }))
    }
}
