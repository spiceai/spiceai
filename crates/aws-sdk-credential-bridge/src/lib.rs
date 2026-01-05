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

mod credential_provider;
use std::{sync::Arc, time::Duration};

use aws_config::Region;
use aws_config::ecs::EcsCredentialsProvider;
use aws_config::imds::credentials::ImdsCredentialsProvider;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::provider_config::ProviderConfig;
use aws_config::web_identity_token::WebIdentityTokenCredentialsProvider;
use aws_config::{BehaviorVersion, SdkConfig};
use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_s3::{config::ProvideCredentials, error::ConnectorError};
use aws_smithy_runtime_api::client::runtime_components::BuildError;
pub use credential_provider::S3CredentialProvider;
use object_store::{ObjectStore, aws::AmazonS3Builder, client::SpawnedReqwestConnector};
use tokio::{runtime::Handle, sync::OnceCell, time::sleep};
use url::Url;
use util::fibonacci_backoff::FibonacciBackoffBuilder;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display(
        "An unexpected error occurred when initializing the AWS SDK for retrieval of AWS credentials for an Iceberg S3 dataset: {source}."
    ))]
    FailedToBuildAWSRuntimeComponents { source: BuildError },

    #[snafu(display(
        "Failed to find valid credentials from the AWS credential provider chain for the S3 connection. Ensure that valid AWS credentials are provided in the environment. Details: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html#credproviders-default-credentials-provider-chain"
    ))]
    FailedToGetCredentialsProviderFromConfig,

    #[snafu(display("Not an S3 URL: {url}"))]
    NotAnS3Url { url: String },

    #[snafu(display("Not able to parse bucket name from s3 url: {url}"))]
    ParseBucketName { url: String },

    #[snafu(transparent)]
    ObjectStore { source: object_store::Error },
}

#[derive(Debug, snafu::Snafu)]
pub enum LoadError {
    #[snafu(display(
        "Failed to resolve AWS credentials from the default provider chain: {source}. \
         Details: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html#credproviders-default-credentials-provider-chain"
    ))]
    CredentialResolve { source: CredentialsError },

    #[snafu(display("{message}"))]
    Other { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Returns a default AWS SDK configuration with the latest behavior version.
///
/// This is a convenience function to ensure all AWS SDK configuration uses
/// the same behavior version consistently across the codebase.
#[must_use]
pub fn default_aws_config() -> aws_config::ConfigLoader {
    aws_config::defaults(BehaviorVersion::v2025_08_07())
}

static SDK_CONFIG: OnceCell<Option<Arc<SdkConfig>>> = OnceCell::const_new();

/// Returns the global SDK configuration, initializing it if necessary.
///
/// This function retries with Fibonacci backoff until credentials can be resolved successfully.
/// If no credentials provider is configured, the function returns `Ok(None)` without retrying.
///
/// # Errors
///
/// Returns a [`LoadError`] if credential initialization continues to fail due to unrecoverable
/// issues when communicating with the AWS credential provider.
pub async fn get_or_init_sdk_config() -> std::result::Result<Option<Arc<SdkConfig>>, LoadError> {
    if let Some(cached) = SDK_CONFIG.get() {
        return Ok(cached.clone());
    }

    let value = SDK_CONFIG
        .get_or_try_init(initialize_sdk_config_with_retry)
        .await?;

    Ok(value.clone())
}

/// Retrieves the cached SDK configuration if it has already been initialized.
pub fn get_sdk_config() -> Option<Arc<SdkConfig>> {
    SDK_CONFIG
        .get()
        .and_then(|value| value.as_ref().map(Arc::clone))
}

async fn initialize_sdk_config_with_retry() -> std::result::Result<Option<Arc<SdkConfig>>, LoadError>
{
    retry_with_backoff(load_sdk_config_from_env).await
}

async fn retry_with_backoff<F, Fut, T>(mut attempt: F) -> std::result::Result<Option<T>, LoadError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<Option<T>, LoadError>>,
{
    let mut backoff = FibonacciBackoffBuilder::new().build();

    loop {
        match attempt().await {
            Ok(result @ Some(_)) => return Ok(result),
            Ok(None) => return Ok(None),
            Err(err) => {
                let delay = backoff
                    .next_duration()
                    .unwrap_or_else(|| Duration::from_secs(1));
                tracing::warn!(
                    "Failed to initialize AWS SDK credentials (retrying in {delay:?}): {err}"
                );
                sleep(delay).await;
            }
        }
    }
}

async fn load_sdk_config_from_env() -> std::result::Result<Option<Arc<SdkConfig>>, LoadError> {
    let sdk_config = default_aws_config().load().await;

    if let Some(creds_provider) = sdk_config.credentials_provider() {
        match creds_provider.provide_credentials().await {
            Ok(_) => Ok(Some(Arc::new(sdk_config))),
            Err(err @ CredentialsError::CredentialsNotLoaded(_)) => {
                tracing::debug!(
                    "AWS credential provider initialized without credentials: {err}. \
                     Proceeding without authentication."
                );
                Ok(None)
            }
            Err(err) => {
                if let CredentialsError::ProviderError(_) = &err {
                    use core::error::Error as StdError;
                    if let Some(mut current) = err.source() {
                        loop {
                            if current.is::<ConnectorError>() {
                                // Retry for `ConnectorError`s
                                return Err(LoadError::CredentialResolve { source: err });
                            }
                            current = match current.source() {
                                Some(src) => src,
                                None => break,
                            };
                        }
                    }

                    Ok(None)
                } else {
                    tracing::warn!(
                        "Non-retryable AWS credentials error, proceeding without authentication: {err}"
                    );
                    Ok(None)
                }
            }
        }
    } else {
        tracing::debug!(
            "No AWS credential provider detected in the default configuration. \
             Assuming unauthenticated access."
        );
        Ok(None)
    }
}

/// Creates an `ObjectStore` from an S3 URL
///
/// # Errors
///
/// Returns an error if:
/// - Unable to parse bucket name from URL
/// - Unable to build S3 client with provided configuration
/// - Unable to get credentials from environment
pub async fn from_s3_url(url: &url::Url, region: Option<String>) -> Result<Box<dyn ObjectStore>> {
    if url.scheme() != "s3" {
        return Err(Error::NotAnS3Url {
            url: url.to_string(),
        });
    }

    let bucket_name = get_bucket_name(url)?;
    let mut builder = AmazonS3Builder::from_env()
        .with_bucket_name(bucket_name)
        .with_http_connector(SpawnedReqwestConnector::new(Handle::current()));
    let (credential_provider, config) = S3CredentialProvider::from_env().await?;

    if let Some(region) = region.or(config.region().map(ToString::to_string)) {
        builder = builder.with_region(region);
    }

    builder = builder.with_credentials(Arc::new(credential_provider));

    Ok(Box::new(builder.build()?))
}

/// Creates an `ObjectStore` from an S3 URL
///
/// # Errors
///
/// Returns an error if:
/// - Unable to parse bucket name from URL
/// - Unable to build S3 client with provided configuration
/// - Unable to get credentials from environment
pub fn from_s3_url_and_config(
    url: &url::Url,
    region: Option<String>,
    sdk_config: &SdkConfig,
    io_runtime: Handle,
) -> Result<Box<dyn ObjectStore>> {
    if url.scheme() != "s3" {
        return Err(Error::NotAnS3Url {
            url: url.to_string(),
        });
    }

    let bucket_name = get_bucket_name(url)?;
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket_name);
    let credential_provider = S3CredentialProvider::from_config(sdk_config)?;

    builder = builder.with_http_connector(SpawnedReqwestConnector::new(io_runtime));

    if let Some(region) = region.or(sdk_config.region().map(ToString::to_string)) {
        builder = builder.with_region(region);
    }

    builder = builder.with_credentials(Arc::new(credential_provider));

    Ok(Box::new(builder.build()?))
}

/// Extracts the bucket name from an S3 URL
///
/// # Errors
///
/// Returns an error if the URL does not contain a valid bucket name
pub fn get_bucket_name(url: &Url) -> Result<&str> {
    url.host_str().ok_or_else(|| Error::ParseBucketName {
        url: url.to_string(),
    })
}

/// Configuration for S3 credential handling
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CredentialConfig {
    /// Whether to load credentials from AWS SDK environment (IAM roles, environment variables, etc.)
    pub load_from_environment: bool,
    /// Whether to skip request signature (for public/anonymous access)
    pub skip_signature: bool,
}

/// Determines the S3 credential configuration based on provided parameters.
///
/// # Parameters
/// - `key`: Optional access key ID
/// - `secret`: Optional secret access key
/// - `auth_method`: Optional authentication method ("public", "key", "`iam_role`")
///
/// # Returns
/// A `S3CredentialConfig` indicating how credentials should be loaded.
///
/// # Errors
/// Returns an error if the authentication method is not recognized.
///
/// # Logic
/// - If both `key` and `secret` are provided: Use explicit credentials (no environment loading, no skip signature)
/// - If `auth_method` is "public": Skip signature, no environment loading
/// - If `auth_method` is "key": Requires explicit key/secret (enforced by caller)
/// - If `auth_method` is "`iam_role`" or None: Load from environment
pub fn determine_s3_credential_config(
    key: Option<&str>,
    secret: Option<&str>,
    auth_method: Option<&str>,
) -> std::result::Result<S3CredentialConfig, String> {
    // If explicit credentials are provided, use them directly
    if key.is_some() && secret.is_some() {
        return Ok(S3CredentialConfig {
            load_from_environment: false,
            skip_signature: false,
        });
    }

    // Otherwise, determine based on auth method
    match auth_method {
        Some("public") => Ok(S3CredentialConfig {
            load_from_environment: false,
            skip_signature: true,
        }),
        Some("key") => Ok(S3CredentialConfig {
            load_from_environment: false,
            skip_signature: false,
        }),
        Some("iam_role") | None => Ok(S3CredentialConfig {
            load_from_environment: true,
            skip_signature: false,
        }),
        Some(method) => Err(format!(
            "Unsupported S3 authentication method: '{method}'. Supported methods are: 'public', 'key', 'iam_role'"
        )),
    }
}

/// Checks if explicit AWS credentials are provided in the parameters.
///
/// # Parameters
/// - `params`: Parameter map to check
/// - `key_param`: Name of the access key parameter
/// - `secret_param`: Name of the secret key parameter
///
/// # Returns
/// `true` if both key and secret parameters are present, `false` otherwise.
#[must_use]
pub fn has_explicit_credentials<V, S: std::hash::BuildHasher>(
    params: &std::collections::HashMap<String, V, S>,
    key_param: &str,
    secret_param: &str,
) -> bool {
    params.contains_key(key_param) && params.contains_key(secret_param)
}

/// Determines whether to use AWS SDK credentials based on parameters.
///
/// Returns `Some(Arc<SdkConfig>)` if SDK credentials should be used, `None` otherwise.
/// This checks if explicit credentials are NOT provided and returns the cached SDK config.
///
/// # Parameters
/// - `params`: Parameter map to check for explicit credentials
/// - `key_param`: Name of the access key parameter
/// - `secret_param`: Name of the secret key parameter
#[must_use]
pub fn should_use_sdk_credentials<V, S: std::hash::BuildHasher>(
    params: &std::collections::HashMap<String, V, S>,
    key_param: &str,
    secret_param: &str,
) -> Option<Arc<SdkConfig>> {
    if has_explicit_credentials(params, key_param, secret_param) {
        None
    } else {
        get_sdk_config()
    }
}

/// Initiates an AWS SDK configuration with the provided credentials.
///
/// This is a convenience function for creating AWS SDK configurations with explicit credentials
/// or falling back to IAM role authentication.
///
/// # Parameters
/// - `provider_name`: Name of the credential provider (for logging/debugging)
/// - `region`: AWS region
/// - `access_key_id`: Optional access key ID
/// - `secret_access_key`: Optional secret access key  
/// - `session_token`: Optional session token
///
/// # Returns
/// A `ConfigLoader` that can be further customized before loading.
pub async fn initiate_config_with_credentials(
    provider_name: &'static str,
    region: String,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
) -> aws_config::ConfigLoader {
    use aws_config::Region;
    use aws_credential_types::Credentials;

    if let (Some(access_key_id), Some(secret_access_key)) = (access_key_id, secret_access_key) {
        let credentials = Credentials::new(
            access_key_id,
            secret_access_key,
            session_token,
            None,
            provider_name,
        );

        default_aws_config()
            .region(Region::new(region))
            .credentials_provider(credentials)
    } else {
        // Initialize AWS SDK credentials for IAM role authentication.
        // This will automatically load credentials from the environment or IAM roles.
        if let Err(err) = get_or_init_sdk_config().await {
            tracing::warn!("Unable to initialize AWS credentials for {provider_name}: {err}");
        }
        default_aws_config().region(Region::new(region))
    }
}

/// Initiates an AWS SDK configuration that only uses IAM role authentication.
///
/// This bypasses environment variables (`AWS_ACCESS_KEY_ID`, etc.) and profile credentials,
/// only using:
/// - Web Identity Token (EKS/IRSA)
/// - ECS Container Credentials
/// - EC2 Instance Metadata (IMDS)
///
/// # Parameters
/// - `region`: AWS region
///
/// # Returns
/// A `ConfigLoader` that can be further customized before loading.
#[must_use]
pub fn initiate_config_with_iam_role_only(region: String) -> aws_config::ConfigLoader {
    let provider_config = ProviderConfig::default().with_region(Some(Region::new(region.clone())));

    let web_identity_provider = WebIdentityTokenCredentialsProvider::builder()
        .configure(&provider_config)
        .build();
    let ecs_provider = EcsCredentialsProvider::builder()
        .configure(&provider_config)
        .build();
    let imds_provider = ImdsCredentialsProvider::builder()
        .configure(&provider_config)
        .build();

    let iam_only_chain =
        CredentialsProviderChain::first_try("WebIdentityToken", web_identity_provider)
            .or_else("EcsContainer", ecs_provider)
            .or_else("Ec2InstanceMetadata", imds_provider);

    default_aws_config()
        .region(Region::new(region))
        .credentials_provider(iam_only_chain)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::time::Duration;
    use url::Url;

    #[tokio::test(start_paused = true)]
    async fn retry_with_backoff_retries_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));

        let attempts_clone = Arc::clone(&attempts);
        let handle = tokio::spawn(async move {
            retry_with_backoff(|| {
                let attempts = Arc::clone(&attempts_clone);
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst);
                    if current < 2 {
                        Err(LoadError::Other {
                            message: "simulated failure".to_string(),
                        })
                    } else {
                        Ok(Some(()))
                    }
                }
            })
            .await
        });

        // Allow the first attempt to run.
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        // Advance time to trigger the second retry.
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        // Advance time again so the third attempt can succeed.
        tokio::time::advance(Duration::from_secs(1)).await;
        let outcome = handle
            .await
            .expect("task panicked")
            .expect("retry loop failed");
        assert_eq!(outcome, Some(()));
        assert!(attempts.load(Ordering::SeqCst) >= 3);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_with_backoff_returns_none_without_retry() {
        let attempts = Arc::new(AtomicUsize::new(0));

        let result = retry_with_backoff(|| {
            let attempts = Arc::clone(&attempts);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok::<Option<()>, LoadError>(None)
            }
        })
        .await
        .expect("retry loop failed");

        assert!(result.is_none());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_get_bucket_name_valid() {
        let url = Url::parse("s3://my-bucket/path/to/file").expect("Failed to parse URL");
        assert_eq!(
            get_bucket_name(&url).expect("Failed to get bucket name"),
            "my-bucket"
        );
    }

    #[test]
    fn test_get_bucket_name_invalid() {
        let url = Url::parse("s3:///path/to/file").expect("Failed to parse URL");
        get_bucket_name(&url).expect_err("Should fail to get bucket name");
    }

    // Tests for determine_s3_credential_config
    #[test]
    fn test_determine_s3_credential_config_with_explicit_credentials() {
        let config = determine_s3_credential_config(
            Some("AKIAIOSFODNN7EXAMPLE"),
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            None,
        )
        .expect("Should succeed with explicit credentials");

        assert!(!config.load_from_environment);
        assert!(!config.skip_signature);
    }

    #[test]
    fn test_determine_s3_credential_config_public_auth() {
        let config = determine_s3_credential_config(None, None, Some("public"))
            .expect("Should succeed with public auth");

        assert!(!config.load_from_environment);
        assert!(config.skip_signature);
    }

    #[test]
    fn test_determine_s3_credential_config_iam_role() {
        let config = determine_s3_credential_config(None, None, Some("iam_role"))
            .expect("Should succeed with iam_role");

        assert!(config.load_from_environment);
        assert!(!config.skip_signature);
    }

    #[test]
    fn test_determine_s3_credential_config_default_iam_role() {
        let config =
            determine_s3_credential_config(None, None, None).expect("Should default to iam_role");

        assert!(config.load_from_environment);
        assert!(!config.skip_signature);
    }

    #[test]
    fn test_determine_s3_credential_config_key_auth() {
        let config = determine_s3_credential_config(None, None, Some("key"))
            .expect("Should succeed with key auth");

        assert!(!config.load_from_environment);
        assert!(!config.skip_signature);
    }

    #[test]
    fn test_determine_s3_credential_config_explicit_overrides_auth() {
        // Even with "public" auth, explicit credentials should take precedence
        let config = determine_s3_credential_config(
            Some("AKIAIOSFODNN7EXAMPLE"),
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            Some("public"),
        )
        .expect("Explicit credentials should override auth method");

        assert!(!config.load_from_environment);
        assert!(!config.skip_signature);
    }

    #[test]
    fn test_determine_s3_credential_config_invalid_auth() {
        let result = determine_s3_credential_config(None, None, Some("invalid_method"));

        assert!(result.is_err());
        assert!(
            result
                .expect_err("Should error")
                .contains("Unsupported S3 authentication method")
        );
    }

    // Tests for has_explicit_credentials
    #[test]
    fn test_has_explicit_credentials_both_present() {
        let mut params = std::collections::HashMap::new();
        params.insert("aws_access_key_id".to_string(), "key");
        params.insert("aws_secret_access_key".to_string(), "secret");

        assert!(has_explicit_credentials(
            &params,
            "aws_access_key_id",
            "aws_secret_access_key"
        ));
    }

    #[test]
    fn test_has_explicit_credentials_only_key() {
        let mut params = std::collections::HashMap::new();
        params.insert("aws_access_key_id".to_string(), "key");

        assert!(!has_explicit_credentials(
            &params,
            "aws_access_key_id",
            "aws_secret_access_key"
        ));
    }

    #[test]
    fn test_has_explicit_credentials_only_secret() {
        let mut params = std::collections::HashMap::new();
        params.insert("aws_secret_access_key".to_string(), "secret");

        assert!(!has_explicit_credentials(
            &params,
            "aws_access_key_id",
            "aws_secret_access_key"
        ));
    }

    #[test]
    fn test_has_explicit_credentials_neither() {
        let params: std::collections::HashMap<String, &str> = std::collections::HashMap::new();

        assert!(!has_explicit_credentials(
            &params,
            "aws_access_key_id",
            "aws_secret_access_key"
        ));
    }

    // Tests for should_use_sdk_credentials
    #[test]
    fn test_should_use_sdk_credentials_with_explicit() {
        let mut params = std::collections::HashMap::new();
        params.insert("key".to_string(), "value");
        params.insert("secret".to_string(), "value");

        let result = should_use_sdk_credentials(&params, "key", "secret");
        assert!(
            result.is_none(),
            "Should not use SDK credentials when explicit credentials are provided"
        );
    }

    #[test]
    fn test_should_use_sdk_credentials_without_explicit() {
        let params: std::collections::HashMap<String, &str> = std::collections::HashMap::new();

        let result = should_use_sdk_credentials(&params, "key", "secret");
        // Result depends on whether SDK config is initialized, so we just check it doesn't panic
        // In a real scenario without SDK config, this would return None
        let _ = result;
    }

    #[tokio::test]
    async fn test_initiate_config_with_explicit_credentials() {
        let config_loader = initiate_config_with_credentials(
            "test-provider",
            "us-east-1".to_string(),
            Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            None,
        )
        .await;

        let config = config_loader.load().await;
        assert!(config.credentials_provider().is_some());
        assert_eq!(
            config.region().map(std::convert::AsRef::as_ref),
            Some("us-east-1")
        );
    }

    #[tokio::test]
    async fn test_initiate_config_without_explicit_credentials() {
        let config_loader = initiate_config_with_credentials(
            "test-provider",
            "eu-west-1".to_string(),
            None,
            None,
            None,
        )
        .await;

        let config = config_loader.load().await;
        assert_eq!(
            config.region().map(std::convert::AsRef::as_ref),
            Some("eu-west-1")
        );
    }
}
