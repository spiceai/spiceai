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
use std::sync::{Arc, LazyLock, OnceLock};
use tokio::runtime::Handle;

use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_s3::config::ProvideCredentials;
use aws_smithy_runtime_api::client::runtime_components::BuildError;
pub use credential_provider::S3CredentialProvider;
use object_store::{ObjectStore, aws::AmazonS3Builder, client::SpawnedReqwestConnector};
use url::Url;

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

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A cache that stores a value after it has been successfully produced once.
///
/// Unlike a standard `OnceLock`, this cache will retry initialization if the loader returns
/// `None`, which makes it suitable for scenarios where a transient failure (such as a DNS outage)
/// should not permanently poison the cache.
pub(crate) struct RetryOnceCell<T> {
    cell: OnceLock<T>,
}

impl<T> RetryOnceCell<T> {
    pub(crate) const fn new() -> Self {
        Self {
            cell: OnceLock::new(),
        }
    }

    pub(crate) async fn get_or_try_init_cloned<F, Fut>(&self, loader: F) -> Option<T>
    where
        T: Send + Sync + Clone,
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Option<T>>,
    {
        if let Some(value) = self.cell.get() {
            return Some(value.clone());
        }

        if let Some(value) = loader().await {
            match self.cell.set(value) {
                Ok(()) => (),
                Err(value) => drop(value),
            }
        }

        self.cell.get().cloned()
    }

    pub(crate) fn get_cloned(&self) -> Option<T>
    where
        T: Send + Sync + Clone,
    {
        self.cell.get().cloned()
    }
}

static SDK_CONFIG_CACHE: LazyLock<RetryOnceCell<Arc<SdkConfig>>> =
    LazyLock::new(RetryOnceCell::new);

/// Initializes the global SDK configuration if it can provide credentials.
pub async fn initialize_sdk_config() -> Option<Arc<SdkConfig>> {
    SDK_CONFIG_CACHE
        .get_or_try_init_cloned(load_sdk_config_from_env)
        .await
}

/// Gets the initialized SDK configuration if available.
pub fn get_sdk_config() -> Option<Arc<SdkConfig>> {
    SDK_CONFIG_CACHE.get_cloned()
}

async fn load_sdk_config_from_env() -> Option<Arc<SdkConfig>> {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;

    if let Some(creds_provider) = sdk_config.credentials_provider() {
        match creds_provider.provide_credentials().await {
            Ok(_) => Some(Arc::new(sdk_config)),
            Err(err) => {
                tracing::warn!(
                    "Failed to resolve AWS credentials from the default provider chain: {err}. \
                     Spice will retry credential initialization. \
                     Details: https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html#credproviders-default-credentials-provider-chain"
                );
                None
            }
        }
    } else {
        tracing::debug!(
            "No AWS credential provider detected in the default configuration. \
             Assuming unauthenticated access."
        );
        None
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
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::time::sleep;
    use url::Url;

    #[tokio::test]
    async fn retry_once_cell_retries_after_failure() {
        let cache: RetryOnceCell<u32> = RetryOnceCell::new();
        let attempts = Arc::new(AtomicUsize::new(0));

        let first = cache
            .get_or_try_init_cloned({
                let attempts = Arc::clone(&attempts);
                move || {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        None
                    }
                }
            })
            .await;
        assert!(first.is_none());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        let second = cache
            .get_or_try_init_cloned({
                let attempts = Arc::clone(&attempts);
                move || {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Some(42)
                    }
                }
            })
            .await;
        assert_eq!(second, Some(42));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        let third = cache
            .get_or_try_init_cloned(|| async { unreachable!() })
            .await;
        assert_eq!(third, Some(42));
    }

    #[tokio::test]
    async fn retry_once_cell_allows_concurrent_initialization() {
        let cache: Arc<RetryOnceCell<u32>> = Arc::new(RetryOnceCell::new());
        let attempts = Arc::new(AtomicUsize::new(0));

        let loader = || {
            let attempts = Arc::clone(&attempts);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                sleep(Duration::from_millis(10)).await;
                Some(7)
            }
        };

        let cache_clone = Arc::clone(&cache);
        let loader_clone = loader;

        let (a, b) = tokio::join!(
            cache.get_or_try_init_cloned(loader),
            cache_clone.get_or_try_init_cloned(loader_clone)
        );

        let attempt_count = attempts.load(Ordering::SeqCst);
        assert!((1..=2).contains(&attempt_count));
        assert_eq!(a, Some(7));
        assert_eq!(b, Some(7));
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
