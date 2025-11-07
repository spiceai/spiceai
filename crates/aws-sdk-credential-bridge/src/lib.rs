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

    #[snafu(display(
        "Failed to find valid credentials from the AWS credential provider chain for the Iceberg S3 connection. {source} Ensure that valid AWS credentials are provided in the environment. Details: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html#credproviders-default-credentials-provider-chain"
    ))]
    FailedToResolveIcebergCredentials {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

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
    let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;

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
        assert!(get_bucket_name(&url).is_err());
    }
}
