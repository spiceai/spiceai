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
    auth::AuthSchemeId,
    runtime_components::{RuntimeComponents, RuntimeComponentsBuilder},
};
use aws_smithy_runtime_api::client::{auth::SharedAuthScheme, identity::SharedIdentityResolver};
use object_store::{CredentialProvider, aws::AwsCredential};

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
    pub fn try_new(config: &SdkConfig) -> object_store::Result<Self> {
        let client = aws_sdk_s3::Client::new(config);
        let runtime = Self::build_aws_runtime_components(config, &client).map_err(|e| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(e),
            }
        })?;
        let credentials_provider =
            config
                .credentials_provider()
                .ok_or_else(|| object_store::Error::Generic {
                    store: "S3",
                    source: "No credentials provider found in SdkConfig".into(),
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
                    source: "No credentials provider found in SdkConfig".into(),
                })?;
        Ok(Self {
            cache: IdentityCache::lazy().build(),
            runtime: Self::build_aws_runtime_components(sdk_config, &Client::new(sdk_config))
                .map_err(|e| object_store::Error::Generic {
                    store: "S3",
                    source: Box::new(e),
                })?,
            identity_resolver: SharedIdentityResolver::new(credentials_provider),
        })
    }

    fn build_aws_runtime_components(
        sdk_config: &SdkConfig,
        client: &Client,
    ) -> object_store::Result<RuntimeComponents> {
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
                    || object_store::Error::Generic {
                        store: "S3",
                        source: "No credentials provider found in SdkConfig".into(),
                    },
                )?),
            )
            .with_retry_strategy(Some(StandardRetryStrategy::new()))
            .with_time_source(client.config().time_source())
            .with_sleep_impl(client.config().sleep_impl())
            .build()
            .map_err(|e| object_store::Error::Generic {
                store: "S3",
                source: Box::new(e),
            })
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
            .map_err(|e| object_store::Error::Generic {
                store: "S3",
                source: e,
            })?;

        let credentials = wrapped_credentials.data::<Credentials>().ok_or_else(|| {
            object_store::Error::Generic {
                store: "S3",
                source: "No credentials found in the resolved identity".into(),
            }
        })?;

        Ok(Arc::new(AwsCredential {
            key_id: credentials.access_key_id().to_string(),
            secret_key: credentials.secret_access_key().to_string(),
            token: credentials.session_token().map(ToString::to_string),
        }))
    }
}
