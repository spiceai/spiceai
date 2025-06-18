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

use object_store::{ObjectStore, aws::AmazonS3Builder};

mod credential_provider;
pub use credential_provider::S3CredentialProvider;
use url::Url;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Not an S3 URL: {url}"))]
    NotAnS3Url { url: String },

    #[snafu(display("Not able to parse bucket name from s3 url: {url}"))]
    ParseBucketName { url: String },

    #[snafu(transparent)]
    ObjectStore { source: object_store::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Creates an `ObjectStore` from an S3 URL
///
/// # Errors
///
/// Returns an error if:
/// - Unable to parse bucket name from URL
/// - Unable to build S3 client with provided configuration
/// - Unable to get credentials from environment
pub async fn from_s3_url(url: &url::Url) -> Result<Box<dyn ObjectStore>> {
    if url.scheme() != "s3" {
        return Err(Error::NotAnS3Url {
            url: url.to_string(),
        });
    }

    let bucket_name = get_bucket_name(url)?;
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket_name);
    let (credential_provider, config) = S3CredentialProvider::from_env().await?;

    if let Some(region) = config.region() {
        builder = builder.with_region(region.to_string());
    }

    builder = builder.with_credentials(Arc::new(credential_provider));

    Ok(Box::new(builder.build()?))
}

fn get_bucket_name(url: &Url) -> Result<&str> {
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
