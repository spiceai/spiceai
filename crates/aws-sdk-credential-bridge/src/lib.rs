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
pub mod object_store_builder;

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use aws_config::Region;
use aws_config::ecs::EcsCredentialsProvider;
use aws_config::environment::EnvironmentVariableCredentialsProvider;
use aws_config::imds::credentials::ImdsCredentialsProvider;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_config::provider_config::ProviderConfig;
use aws_config::web_identity_token::WebIdentityTokenCredentialsProvider;
use aws_config::{AppName, BehaviorVersion, SdkConfig};
use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_s3::{config::ProvideCredentials, error::ConnectorError};
use aws_smithy_runtime_api::client::runtime_components::BuildError;
pub use credential_provider::S3CredentialProvider;
use object_store::{ObjectStore, aws::AmazonS3Builder, client::SpawnedReqwestConnector};
use tokio::{runtime::Handle, sync::OnceCell, time::sleep};
use url::Url;
use util::fibonacci_backoff::FibonacciBackoffBuilder;

/// The APN user-agent string for Spice.
///
/// This is set on all AWS SDK configurations to identify Spice as an AWS Partner Network (APN)
/// application in the user-agent header of AWS API requests.
///
/// The `AppName::new` call is infallible for this input: the name contains only alphanumeric
/// characters plus `.` and `-`, all of which are permitted. The name is truncated to 50
/// characters to stay within the AWS SDK's recommended limit.
static APN_APP_NAME: LazyLock<AppName> = LazyLock::new(|| {
    let version = env!("CARGO_PKG_VERSION");
    let mut name = format!("Spice-{version}");
    name.truncate(50);
    match AppName::new(name) {
        Ok(name) => name,
        Err(_) => unreachable!("Spice version string should always be a valid AppName"),
    }
});

/// Returns the APN [`AppName`] for Spice, suitable for use in AWS SDK configurations.
#[must_use]
pub fn apn_app_name() -> &'static AppName {
    &APN_APP_NAME
}

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

/// Returns a default AWS SDK configuration with the latest behavior version and
/// the Spice APN user-agent.
///
/// This is a convenience function to ensure all AWS SDK configuration uses
/// the same behavior version and APN identification consistently across the codebase.
#[must_use]
pub fn default_aws_config() -> aws_config::ConfigLoader {
    aws_config::defaults(BehaviorVersion::v2026_01_12()).app_name(APN_APP_NAME.clone())
}

#[must_use]
fn default_aws_config_for_region(region: Option<&str>) -> aws_config::ConfigLoader {
    match region {
        Some(region) => default_aws_config().region(Region::new(region.to_string())),
        None => default_aws_config(),
    }
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
    get_or_init_sdk_config_with_region(None).await
}

/// Returns the global SDK configuration, initializing it with an explicit region if necessary.
///
/// The cached SDK config is used to share the resolved credentials provider across the process.
/// Callers should pass an explicit region when they already know it when building AWS clients or
/// object stores. If no explicit region is provided, downstream code may fall back to the cached
/// config's region when one is available.
///
/// # Errors
///
/// Returns a [`LoadError`] if credential initialization continues to fail due to unrecoverable
/// issues when communicating with the AWS credential provider.
pub async fn get_or_init_sdk_config_with_region(
    region: Option<&str>,
) -> std::result::Result<Option<Arc<SdkConfig>>, LoadError> {
    get_or_init_sdk_config_with_region_for_cell(
        &SDK_CONFIG,
        region,
        initialize_sdk_config_with_retry,
    )
    .await
}

async fn get_or_init_sdk_config_with_region_for_cell<F, Fut>(
    sdk_config_cell: &OnceCell<Option<Arc<SdkConfig>>>,
    region: Option<&str>,
    initialize: F,
) -> std::result::Result<Option<Arc<SdkConfig>>, LoadError>
where
    F: FnOnce(Option<String>) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<Option<Arc<SdkConfig>>, LoadError>>,
{
    if let Some(cached) = sdk_config_cell.get() {
        return Ok(cached.clone());
    }

    let region = region.map(ToString::to_string);
    let value = sdk_config_cell
        .get_or_try_init(|| initialize(region))
        .await?;

    Ok(value.clone())
}

/// Retrieves the cached SDK configuration if it has already been initialized.
pub fn get_sdk_config() -> Option<Arc<SdkConfig>> {
    SDK_CONFIG
        .get()
        .and_then(|value| value.as_ref().map(Arc::clone))
}

async fn initialize_sdk_config_with_retry(
    region: Option<String>,
) -> std::result::Result<Option<Arc<SdkConfig>>, LoadError> {
    retry_with_backoff(|| load_sdk_config_from_env(region.clone())).await
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

async fn load_sdk_config_from_env(
    region: Option<String>,
) -> std::result::Result<Option<Arc<SdkConfig>>, LoadError> {
    let sdk_config = default_aws_config_for_region(region.as_deref())
        .load()
        .await;

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
                    // Proceeding without authentication anyway: log at debug. The bridge
                    // doesn't know whether the caller actually needs AWS credentials, so
                    // callers with that context (e.g. the S3 connector when `region` is
                    // explicitly configured) should warn themselves based on whether
                    // `get_sdk_config()` returns `None` after this call.
                    tracing::debug!(
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
    let (credential_provider, config) =
        S3CredentialProvider::from_env_with_region(region.as_deref()).await?;

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

    if let Some(region) = region.or_else(|| sdk_config.region().map(ToString::to_string)) {
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
    /// Optional IAM role source restriction: "auto", "metadata", or "env".
    /// Only meaningful when `load_from_environment` is true.
    pub iam_role_source: Option<String>,
}

/// Determines the S3 credential configuration based on provided parameters.
///
/// # Parameters
/// - `key`: Optional access key ID
/// - `secret`: Optional secret access key
/// - `auth_method`: Optional authentication method ("public", "key", "`iam_role`")
/// - `iam_role_source`: Optional IAM role credential source restriction ("auto", "metadata", "env").
///   Only applied when `auth_method` is "`iam_role`" or `None`. Returns an error for unsupported values.
///
/// # Returns
/// A `S3CredentialConfig` indicating how credentials should be loaded.
///
/// # Errors
/// Returns an error if the authentication method or `iam_role_source` is not recognized.
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
    iam_role_source: Option<&str>,
) -> std::result::Result<S3CredentialConfig, String> {
    // If explicit credentials are provided, use them directly
    if key.is_some() && secret.is_some() {
        return Ok(S3CredentialConfig {
            load_from_environment: false,
            skip_signature: false,
            iam_role_source: None,
        });
    }

    // Otherwise, determine based on auth method
    match auth_method {
        Some("public") => Ok(S3CredentialConfig {
            load_from_environment: false,
            skip_signature: true,
            iam_role_source: None,
        }),
        Some("key") => Ok(S3CredentialConfig {
            load_from_environment: false,
            skip_signature: false,
            iam_role_source: None,
        }),
        Some("iam_role") | None => {
            let validated_iam_role_source = match iam_role_source {
                None => None,
                Some("auto" | "metadata" | "env") => iam_role_source.map(ToString::to_string),
                Some(other) => {
                    return Err(format!(
                        "Unsupported iam_role_source: '{other}'. Supported values are: 'auto', 'metadata', 'env'"
                    ));
                }
            };
            Ok(S3CredentialConfig {
                load_from_environment: true,
                skip_signature: false,
                iam_role_source: validated_iam_role_source,
            })
        }
        Some(method) => Err(format!(
            "Unsupported S3 authentication method: '{method}'. Supported methods are: 'public', 'key', 'iam_role'"
        )),
    }
}

/// Builds an [`SdkConfig`] for a restricted IAM role source.
///
/// This is used by the object store registry (synchronous context) when `iam_role_source`
/// is `metadata` or `env`, requiring a credential chain that differs from the global cached config.
///
/// # Parameters
/// - `iam_role_source`: The IAM role credential source ("metadata" or "env")
/// - `region`: Optional AWS region
///
/// # Returns
/// An `SdkConfig` with the restricted credential chain.
///
/// # Errors
/// Returns an error if the config cannot be loaded.
pub async fn build_restricted_sdk_config(
    iam_role_source: &str,
    region: Option<String>,
) -> std::result::Result<SdkConfig, LoadError> {
    let region_str = region.unwrap_or_else(|| {
        // Derive region from environment variables before falling back to us-east-1.
        std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| {
                tracing::warn!("No AWS region specified and AWS_REGION/AWS_DEFAULT_REGION not set; defaulting to us-east-1");
                "us-east-1".to_string()
            })
    });
    let config_loader = match iam_role_source {
        "metadata" => initiate_config_auth_iam_metadata(region_str),
        "env" => initiate_config_auth_iam_env(region_str),
        other => {
            tracing::warn!(
                iam_role_source = other,
                "Unknown iam_role_source value in build_restricted_sdk_config; defaulting to metadata credentials"
            );
            initiate_config_auth_iam_metadata(region_str)
        }
    };
    Ok(config_loader.load().await)
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
///
/// When `iam_role_source` is provided:
/// - `"auto"` or `None`: Uses the default AWS credential chain (env vars, shared config, web identity, ECS, IMDS).
/// - `"metadata"`: Restricts to metadata-based sources only (Web Identity/IRSA, ECS, IMDS).
/// - `"env"`: Restricts to environment variable credentials only.
pub async fn initiate_config_with_credentials(
    provider_name: &'static str,
    region: String,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    iam_role_source: Option<&str>,
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
        match iam_role_source {
            Some("metadata") => initiate_config_auth_iam_metadata(region),
            Some("env") => initiate_config_auth_iam_env(region),
            _ => {
                // Initialize AWS SDK credentials using the default credential chain.
                if let Err(err) = get_or_init_sdk_config_with_region(Some(region.as_str())).await {
                    tracing::warn!(
                        "Unable to initialize AWS credentials for {provider_name}: {err}"
                    );
                }
                default_aws_config_for_region(Some(region.as_str()))
            }
        }
    }
}

/// Initiates an AWS SDK configuration using the default credential provider chain.
///
/// This uses the standard AWS credential resolution order:
/// - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
/// - Shared config (`~/.aws/config`, `~/.aws/credentials`)
/// - Web Identity Token (EKS/IRSA)
/// - ECS Container Credentials
/// - EC2 Instance Metadata (IMDS)
///
/// # Parameters
/// - `region`: AWS region
///
/// # Returns
/// A [`ConfigLoader`] that can be further customized before loading.
#[must_use]
pub async fn initiate_config_default_auth(region: String) -> aws_config::ConfigLoader {
    if let Err(err) = get_or_init_sdk_config_with_region(Some(region.as_str())).await {
        tracing::warn!("Unable to initialize AWS credentials: {err}");
    }
    default_aws_config_for_region(Some(region.as_str()))
}

/// Initiates an AWS SDK configuration using only IAM role authentication.
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
/// A [`ConfigLoader`] that can be further customized before loading.
#[must_use]
pub fn initiate_config_auth_iam_metadata(region: String) -> aws_config::ConfigLoader {
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

/// Initiates an AWS SDK configuration using only environment variable credentials.
///
/// This exclusively uses credentials from environment variables:
/// - `AWS_ACCESS_KEY_ID`
/// - `AWS_SECRET_ACCESS_KEY`
/// - `AWS_SESSION_TOKEN` (optional)
///
/// # Parameters
/// - `region`: AWS region
///
/// # Returns
/// A [`ConfigLoader`] that can be further customized before loading.
#[must_use]
pub fn initiate_config_auth_iam_env(region: String) -> aws_config::ConfigLoader {
    let env_provider = EnvironmentVariableCredentialsProvider::new();

    default_aws_config()
        .region(Region::new(region))
        .credentials_provider(env_provider)
}

/// Initiates an AWS SDK configuration using explicit access key credentials.
///
/// This uses the provided credentials directly, bypassing all other credential sources.
///
/// # Parameters
/// - `provider_name`: Name of the credential provider
/// - `region`: AWS region
/// - `access_key_id`: AWS access key ID
/// - `secret_access_key`: AWS secret access key
/// - `session_token`: Optional AWS session token (for temporary credentials)
///
/// # Returns
/// A [`ConfigLoader`] that can be further customized before loading.
#[must_use]
pub fn initiate_config_auth_key(
    provider_name: &'static str,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
) -> aws_config::ConfigLoader {
    use aws_credential_types::Credentials;

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
            None,
        )
        .expect("Should succeed with explicit credentials");

        assert!(!config.load_from_environment);
        assert!(!config.skip_signature);
    }

    #[test]
    fn test_determine_s3_credential_config_public_auth() {
        let config = determine_s3_credential_config(None, None, Some("public"), None)
            .expect("Should succeed with public auth");

        assert!(!config.load_from_environment);
        assert!(config.skip_signature);
    }

    #[test]
    fn test_determine_s3_credential_config_iam_role() {
        let config = determine_s3_credential_config(None, None, Some("iam_role"), None)
            .expect("Should succeed with iam_role");

        assert!(config.load_from_environment);
        assert!(!config.skip_signature);
    }

    #[test]
    fn test_determine_s3_credential_config_default_iam_role() {
        let config = determine_s3_credential_config(None, None, None, None)
            .expect("Should default to iam_role");

        assert!(config.load_from_environment);
        assert!(!config.skip_signature);
    }

    #[test]
    fn test_determine_s3_credential_config_key_auth() {
        let config = determine_s3_credential_config(None, None, Some("key"), None)
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
            None,
        )
        .expect("Explicit credentials should override auth method");

        assert!(!config.load_from_environment);
        assert!(!config.skip_signature);
    }

    #[test]
    fn test_determine_s3_credential_config_invalid_auth() {
        let result = determine_s3_credential_config(None, None, Some("invalid_method"), None);

        assert!(result.is_err());
        assert!(
            result
                .expect_err("Should error")
                .contains("Unsupported S3 authentication method")
        );
    }

    #[test]
    fn test_determine_s3_credential_config_iam_role_with_metadata_source() {
        let config = determine_s3_credential_config(None, None, Some("iam_role"), Some("metadata"))
            .expect("Should succeed with iam_role and metadata source");

        assert!(config.load_from_environment);
        assert!(!config.skip_signature);
        assert_eq!(config.iam_role_source.as_deref(), Some("metadata"));
    }

    #[test]
    fn test_determine_s3_credential_config_iam_role_with_env_source() {
        let config = determine_s3_credential_config(None, None, Some("iam_role"), Some("env"))
            .expect("Should succeed with iam_role and env source");

        assert!(config.load_from_environment);
        assert!(!config.skip_signature);
        assert_eq!(config.iam_role_source.as_deref(), Some("env"));
    }

    #[test]
    fn test_determine_s3_credential_config_iam_role_with_auto_source() {
        let config = determine_s3_credential_config(None, None, Some("iam_role"), Some("auto"))
            .expect("Should succeed with iam_role and auto source");

        assert!(config.load_from_environment);
        assert!(!config.skip_signature);
        assert_eq!(config.iam_role_source.as_deref(), Some("auto"));
    }

    #[test]
    fn test_determine_s3_credential_config_explicit_creds_ignore_iam_role_source() {
        let config = determine_s3_credential_config(
            Some("AKIAIOSFODNN7EXAMPLE"),
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            Some("iam_role"),
            Some("metadata"),
        )
        .expect("Explicit credentials should ignore iam_role_source");

        assert!(!config.load_from_environment);
        assert!(!config.skip_signature);
        assert!(config.iam_role_source.is_none());
    }

    #[test]
    fn test_determine_s3_credential_config_invalid_iam_role_source() {
        let result =
            determine_s3_credential_config(None, None, Some("iam_role"), Some("invalid_source"));

        assert!(result.is_err());
        assert!(
            result
                .expect_err("Should error")
                .contains("Unsupported iam_role_source")
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
            None,
        )
        .await;

        let config = config_loader.load().await;
        assert_eq!(
            config.region().map(std::convert::AsRef::as_ref),
            Some("eu-west-1")
        );
    }

    #[test]
    fn test_apn_app_name_is_valid() {
        let name: &AppName = &APN_APP_NAME;
        let name_str: &str = name.as_ref();
        assert!(
            name_str.starts_with("Spice-"),
            "APN app name should start with 'Spice-', got: {name_str}"
        );
        assert!(
            name_str.len() <= 50,
            "APN app name must be at most 50 characters, got {} ({name_str})",
            name_str.len()
        );
    }

    #[tokio::test]
    async fn test_default_aws_config_includes_app_name() {
        let config = default_aws_config().load().await;
        let app_name = config.app_name();
        assert!(app_name.is_some(), "default_aws_config should set app_name");
        let name_str: &str = app_name.expect("already asserted").as_ref();
        assert!(
            name_str.starts_with("Spice-"),
            "APN app name should start with 'Spice-', got: {name_str}"
        );
    }

    #[tokio::test]
    async fn test_default_aws_config_for_region_sets_region() {
        let config = default_aws_config_for_region(Some("ap-south-1"))
            .load()
            .await;

        assert_eq!(
            config.region().map(std::convert::AsRef::as_ref),
            Some("ap-south-1")
        );
    }

    #[tokio::test]
    async fn test_get_or_init_sdk_config_with_region_uses_first_region() {
        async fn initialize_test_sdk_config(
            region: Option<String>,
        ) -> std::result::Result<Option<Arc<SdkConfig>>, LoadError> {
            Ok(Some(Arc::new(
                default_aws_config_for_region(region.as_deref())
                    .load()
                    .await,
            )))
        }

        let sdk_config_cell = OnceCell::const_new();

        let first = get_or_init_sdk_config_with_region_for_cell(
            &sdk_config_cell,
            Some("ap-south-1"),
            initialize_test_sdk_config,
        )
        .await
        .expect("first initialization should succeed")
        .expect("test initializer should return a config");

        let second = get_or_init_sdk_config_with_region_for_cell(
            &sdk_config_cell,
            Some("us-east-1"),
            initialize_test_sdk_config,
        )
        .await
        .expect("second initialization should succeed")
        .expect("test initializer should return a config");

        assert_eq!(
            first.region().map(std::convert::AsRef::as_ref),
            Some("ap-south-1")
        );
        assert_eq!(
            second.region().map(std::convert::AsRef::as_ref),
            Some("ap-south-1")
        );
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn test_from_s3_url_and_config_uses_sdk_config_region_when_region_is_absent() {
        let url = Url::parse("s3://my-bucket/path/to/data").expect("valid url");
        let sdk_config = default_aws_config_for_region(Some("ap-south-1"))
            .load()
            .await;

        let store = from_s3_url_and_config(&url, None, &sdk_config, Handle::current());

        assert!(
            store.is_ok(),
            "object store should build when the SDK config provides the region"
        );
    }
}
