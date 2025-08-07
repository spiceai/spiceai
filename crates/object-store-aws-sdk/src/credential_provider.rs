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

use crate::{Error, FailedToBuildAWSRuntimeComponentsSnafu, Result};
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
    auth::AuthSchemeId,
    runtime_components::{RuntimeComponents, RuntimeComponentsBuilder},
};
use aws_smithy_runtime_api::client::{auth::SharedAuthScheme, identity::SharedIdentityResolver};
use object_store::{CredentialProvider, aws::AwsCredential};
use snafu::ResultExt;

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
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

        Ok((Self::from_config(&config)?, config))
    }

    /// Creates a new `S3CredentialProvider` from the given SDK configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials provider cannot be obtained from the SDK configuration.
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

    fn build_aws_runtime_components(
        sdk_config: &SdkConfig,
        client: &Client,
    ) -> Result<RuntimeComponents> {
        let mut runtime_components = RuntimeComponentsBuilder::new("S3CredentialProvider");
        runtime_components.set_auth_scheme_option_resolver(Some(
            DefaultAuthSchemeResolver::default().into_shared_resolver(),
        ));
        runtime_components
            .set_endpoint_resolver(Some(DefaultResolver::new().into_shared_resolver()));
        runtime_components.push_auth_scheme(SharedAuthScheme::new(SigV4AuthScheme::new()));

        runtime_components
            .with_identity_cache(Some(IdentityCache::lazy().build()))
            .with_identity_resolver(
                AuthSchemeId::new("SpiceObjectStoreS3CredentialsProvider"),
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

#[async_trait]
impl CredentialProvider for S3CredentialProvider {
    type Credential = AwsCredential;

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
            .map_err(|_| object_store::Error::Generic {
                store: "S3",
                source: "Failed to find valid credentials from the AWS credential provider chain for the Iceberg S3 connection. Ensure that valid AWS credentials are provided in the environment. Details: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html#credproviders-default-credentials-provider-chain".into(),
            })?;

        let credentials = wrapped_credentials.data::<Credentials>().ok_or_else(|| {
            object_store::Error::Generic {
                store: "S3",
                source: "Failed to find valid credentials from the AWS credential provider chain for the Iceberg S3 connection. Ensure that valid AWS credentials are provided in the environment. Details: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html#credproviders-default-credentials-provider-chain".into(),
            }
        })?;

        Ok(Arc::new(AwsCredential {
            key_id: credentials.access_key_id().to_string(),
            secret_key: credentials.secret_access_key().to_string(),
            token: credentials.session_token().map(ToString::to_string),
        }))
    }
}
