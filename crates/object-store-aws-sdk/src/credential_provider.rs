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
        RuntimeComponentsBuilder::new("S3CredentialProvider")
            .with_auth_scheme_option_resolver(Some(
                DefaultAuthSchemeResolver::default().into_shared_resolver(),
            ))
            .with_endpoint_resolver(Some(DefaultResolver::new().into_shared_resolver()))
            .with_auth_scheme(SharedAuthScheme::new(SigV4AuthScheme::new()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_cognitoidentity as cognito_identity;
    use aws_sdk_cognitoidentityprovider as cognito_idp;
    use aws_sdk_cognitoidentityprovider::types::AuthFlowType;
    use std::io::Write;
    use tempfile::NamedTempFile;

    async fn setup(
        file: &mut NamedTempFile,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let client_id = std::env::var("AWS_S3_CLIENT_ID").expect("AWS_S3_CLIENT_ID must be set");
        let identity_pool_id =
            std::env::var("AWS_S3_IDENTITY_POOL_ID").expect("AWS_S3_IDENTITY_POOL_ID must be set");
        let username = std::env::var("AWS_S3_USERNAME").expect("AWS_S3_USERNAME must be set");
        let password = std::env::var("AWS_S3_PASSWORD").expect("AWS_S3_PASSWORD must be set");
        let cognito_idp_uri =
            std::env::var("AWS_COGNITO_IDP_URI").expect("AWS_COGNITO_IDP_URI must be set");

        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

        let cognito_idp_client = cognito_idp::Client::new(&config);
        let cognito_identity_client = cognito_identity::Client::new(&config);

        let auth_response = cognito_idp_client
            .initiate_auth()
            .auth_flow(AuthFlowType::UserPasswordAuth)
            .client_id(client_id)
            .auth_parameters("USERNAME", username)
            .auth_parameters("PASSWORD", password)
            .send()
            .await?;

        let id_token = auth_response
            .authentication_result()
            .as_ref()
            .and_then(|result| result.id_token())
            .ok_or("Failed to get ID token")?;

        let identity_id_response = cognito_identity_client
            .get_id()
            .identity_pool_id(identity_pool_id)
            .logins(&cognito_idp_uri, id_token)
            .send()
            .await?;

        let identity_id = identity_id_response
            .identity_id()
            .ok_or("Failed to get identity ID")?;

        let open_id_token_response = cognito_identity_client
            .get_open_id_token()
            .identity_id(identity_id)
            .logins(cognito_idp_uri, id_token)
            .send()
            .await?;

        let token = open_id_token_response
            .token()
            .ok_or("Failed to get OpenID token")?;

        writeln!(file, "{token}")?;

        unsafe {
            std::env::set_var(
                "AWS_WEB_IDENTITY_TOKEN_FILE",
                file.path()
                    .to_str()
                    .ok_or("Failed to convert path to string")?,
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn s3_credential_provider_caches_calls() {
        let mut tempfile = NamedTempFile::new().expect("To create temp file");

        setup(&mut tempfile).await.expect("To setup properly");

        let (credential_provider, _) = S3CredentialProvider::from_env()
            .await
            .expect("To Create S3CredentialProvider");

        let first_credentials = credential_provider
            .get_credential()
            .await
            .expect("To Get Credentials");

        let second_credentials = credential_provider
            .get_credential()
            .await
            .expect("To Get Credentials");

        assert_eq!(first_credentials.key_id, second_credentials.key_id);
        assert_eq!(first_credentials.secret_key, second_credentials.secret_key);
        assert_eq!(first_credentials.token, second_credentials.token);
    }
}
