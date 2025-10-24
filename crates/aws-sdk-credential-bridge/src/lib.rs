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
use std::sync::Arc;
use tokio::{runtime::Handle, sync::OnceCell};

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

static SDK_CONFIG: OnceCell<Option<SdkConfig>> = OnceCell::const_new();

/// Initializes the global SDK configuration if it can provide credentials.
pub async fn initialize_sdk_config() -> &'static Option<SdkConfig> {
    SDK_CONFIG
        .get_or_init(|| async {
            let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;

            if let Some(creds_provider) = sdk_config.credentials_provider() {
                if creds_provider.provide_credentials().await.is_ok() {
                    Some(sdk_config)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .await
}

/// Gets the initialized SDK configuration if available.
pub fn get_sdk_config() -> Option<&'static SdkConfig> {
    SDK_CONFIG.get().and_then(|opt| opt.as_ref())
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
    use url::Url;

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
