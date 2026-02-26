/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Consolidated S3 `ObjectStore` builder with credential bridge integration.
//!
//! This module provides a builder pattern for creating S3-backed `ObjectStore` instances
//! with proper AWS credential handling. It consolidates the common S3 configuration logic
//! used across the codebase (snapshots, scheduler registry, etc.).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use object_store::aws::AmazonS3Builder;
use object_store::client::SpawnedReqwestConnector;
use object_store::{ClientOptions, ObjectStore};
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use tokio::runtime::Handle;
use url::Url;

use crate::{S3CredentialProvider, get_bucket_name, get_or_init_sdk_config};

/// Error type for S3 object store building operations.
#[derive(Debug, Snafu)]
pub enum S3ObjectStoreBuilderError {
    #[snafu(display("Missing bucket name in S3 URL: {url}"))]
    MissingBucket { url: String },

    #[snafu(display("Unable to parse client_timeout '{value}': {source}"))]
    ClientTimeoutParse {
        value: String,
        source: fundu::ParseError,
    },

    #[snafu(display("Failed to load S3 credentials from environment: {source}"))]
    CredentialLoad { source: crate::Error },

    #[snafu(display("Failed to build S3 object store: {source}"))]
    ObjectStoreBuild { source: object_store::Error },
}

pub type Result<T, E = S3ObjectStoreBuilderError> = std::result::Result<T, E>;

/// Builder for creating S3 `ObjectStore` instances with proper credential handling.
///
/// This builder consolidates common S3 configuration patterns used across:
/// - Acceleration snapshots
/// - Scheduler registry state storage
/// - Other S3-backed storage needs
///
/// # Example
///
/// ```ignore
/// use aws_sdk_credential_bridge::object_store_builder::S3ObjectStoreBuilder;
/// use tokio::runtime::Handle;
///
/// let store = S3ObjectStoreBuilder::from_url(&url, Handle::current())
///     .with_region("us-east-1")
///     .with_client_timeout(Duration::from_secs(30))
///     .build()
///     .await?;
/// ```
#[derive(Debug)]
pub struct S3ObjectStoreBuilder {
    bucket_name: String,
    io_runtime: Handle,
    region: Option<String>,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
    client_timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    allow_http: Option<bool>,
    pool_max_idle_per_host: Option<usize>,
    pool_idle_timeout: Option<Duration>,
}

impl S3ObjectStoreBuilder {
    /// Creates a new builder from an S3 URL.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL doesn't contain a valid bucket name.
    pub fn from_url(url: &Url, io_runtime: Handle) -> Result<Self> {
        let bucket_name = get_bucket_name(url)
            .map_err(|_| S3ObjectStoreBuilderError::MissingBucket {
                url: url.to_string(),
            })?
            .to_string();

        Ok(Self {
            bucket_name,
            io_runtime,
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            client_timeout: None,
            connect_timeout: None,
            allow_http: None,
            pool_max_idle_per_host: None,
            pool_idle_timeout: None,
        })
    }

    /// Creates a new builder from an S3 URL, parsing parameters from the URL fragment.
    ///
    /// Supported fragment parameters:
    /// - `region` or `s3_region`: AWS region
    /// - `endpoint` or `s3_endpoint`: Custom S3 endpoint
    /// - `key` or `s3_key`: Access key ID
    /// - `secret` or `s3_secret`: Secret access key
    /// - `session_token` or `s3_session_token`: Session token
    /// - `client_timeout`: Request timeout (parsed by fundu)
    /// - `allow_http`: Whether to allow HTTP connections
    ///
    /// # Errors
    ///
    /// Returns an error if the URL doesn't contain a valid bucket name or
    /// if the `client_timeout` parameter cannot be parsed.
    pub fn from_url_with_params(url: &Url, io_runtime: Handle) -> Result<Self> {
        let mut builder = Self::from_url(url, io_runtime)?;

        let params: HashMap<String, String> =
            url::form_urlencoded::parse(url.fragment().unwrap_or_default().as_bytes())
                .into_owned()
                .collect();

        builder = builder.with_params(&params)?;

        Ok(builder)
    }

    /// Applies configuration from a parameter map.
    ///
    /// Supports both prefixed (`s3_region`) and unprefixed (`region`) parameter names.
    ///
    /// # Errors
    ///
    /// Returns an error if the `client_timeout` parameter cannot be parsed.
    pub fn with_params(mut self, params: &HashMap<String, String>) -> Result<Self> {
        if let Some(region) = params.get("region").or_else(|| params.get("s3_region")) {
            self.region = Some(region.clone());
        }
        if let Some(endpoint) = params.get("endpoint").or_else(|| params.get("s3_endpoint")) {
            self.endpoint = Some(endpoint.clone());
        }
        if let Some(key) = params.get("key").or_else(|| params.get("s3_key")) {
            self.access_key_id = Some(key.clone());
        }
        if let Some(secret) = params.get("secret").or_else(|| params.get("s3_secret")) {
            self.secret_access_key = Some(secret.clone());
        }
        if let Some(token) = params
            .get("session_token")
            .or_else(|| params.get("s3_session_token"))
        {
            self.session_token = Some(token.clone());
        }
        if let Some(timeout) = params.get("client_timeout") {
            self.client_timeout = Some(
                fundu::parse_duration(timeout)
                    .context(ClientTimeoutParseSnafu { value: timeout })?,
            );
        }
        if let Some(allow_http) = params.get("allow_http")
            && let Ok(value) = allow_http.parse::<bool>()
        {
            self.allow_http = Some(value);
        }
        Ok(self)
    }

    /// Applies configuration from a secret parameter map.
    ///
    /// This is similar to [`with_params`](Self::with_params) but accepts `SecretString` values,
    /// which is useful when working with the `runtime-parameters` crate's `Parameters::to_secret_map()`.
    ///
    /// Supported parameter names (unprefixed):
    /// - `region`: AWS region
    /// - `endpoint`: Custom S3 endpoint
    /// - `key`: Access key ID
    /// - `secret`: Secret access key
    /// - `session_token`: Session token
    /// - `client_timeout`: Request timeout (parsed by fundu)
    /// - `allow_http`: Whether to allow HTTP connections
    ///
    /// # Errors
    ///
    /// Returns an error if the `client_timeout` parameter cannot be parsed.
    pub fn with_secret_params(mut self, params: &HashMap<String, SecretString>) -> Result<Self> {
        if let Some(region) = params.get("region") {
            self.region = Some(region.expose_secret().to_string());
        }
        if let Some(endpoint) = params.get("endpoint") {
            self.endpoint = Some(endpoint.expose_secret().to_string());
        }
        if let Some(key) = params.get("key") {
            self.access_key_id = Some(key.expose_secret().to_string());
        }
        if let Some(secret) = params.get("secret") {
            self.secret_access_key = Some(secret.expose_secret().to_string());
        }
        if let Some(token) = params.get("session_token") {
            self.session_token = Some(token.expose_secret().to_string());
        }
        if let Some(timeout) = params.get("client_timeout") {
            let timeout_str = timeout.expose_secret();
            self.client_timeout = Some(
                fundu::parse_duration(timeout_str)
                    .context(ClientTimeoutParseSnafu { value: timeout_str })?,
            );
        }
        if let Some(allow_http) = params.get("allow_http")
            && let Ok(value) = allow_http.expose_secret().parse::<bool>()
        {
            self.allow_http = Some(value);
        }
        Ok(self)
    }

    /// Sets the AWS region.
    #[must_use]
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Sets a custom S3 endpoint.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Sets explicit AWS credentials.
    #[must_use]
    pub fn with_credentials(
        mut self,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        self.access_key_id = Some(access_key_id.into());
        self.secret_access_key = Some(secret_access_key.into());
        self
    }

    /// Sets a session token for temporary credentials.
    #[must_use]
    pub fn with_session_token(mut self, token: impl Into<String>) -> Self {
        self.session_token = Some(token.into());
        self
    }

    /// Sets the client request timeout.
    #[must_use]
    pub fn with_client_timeout(mut self, timeout: Duration) -> Self {
        self.client_timeout = Some(timeout);
        self
    }

    /// Sets the connection phase timeout.
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Sets whether to allow HTTP connections (vs HTTPS only).
    #[must_use]
    pub fn with_allow_http(mut self, allow: bool) -> Self {
        self.allow_http = Some(allow);
        self
    }

    /// Sets the maximum number of idle connections per host.
    ///
    /// Higher values can improve throughput for repeated requests to the same endpoint.
    /// Default is typically 32.
    #[must_use]
    pub fn with_pool_max_idle_per_host(mut self, max: usize) -> Self {
        self.pool_max_idle_per_host = Some(max);
        self
    }

    /// Sets how long idle connections should be kept alive.
    ///
    /// Longer timeouts can reduce connection establishment overhead for repeated requests.
    #[must_use]
    pub fn with_pool_idle_timeout(mut self, timeout: Duration) -> Self {
        self.pool_idle_timeout = Some(timeout);
        self
    }

    /// Builds the S3 `ObjectStore`.
    ///
    /// If no explicit credentials are provided, this will attempt to load
    /// credentials from the AWS SDK environment (IAM roles, environment variables, etc.).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Credential loading from the environment fails
    /// - The object store cannot be built
    pub async fn build(self) -> Result<Arc<dyn ObjectStore>> {
        let mut s3_builder = AmazonS3Builder::from_env()
            .with_bucket_name(&self.bucket_name)
            .with_http_connector(SpawnedReqwestConnector::new(self.io_runtime));

        let mut client_options = ClientOptions::default();

        // Apply region
        if let Some(region) = &self.region {
            s3_builder = s3_builder.with_region(region);
        }

        // Apply endpoint
        if let Some(endpoint) = &self.endpoint {
            s3_builder = s3_builder.with_endpoint(endpoint);
            // Automatically allow HTTP if endpoint uses HTTP
            if endpoint.starts_with("http://") {
                client_options = client_options.with_allow_http(true);
            }
        }

        // Apply client timeout
        if let Some(timeout) = self.client_timeout {
            client_options = client_options.with_timeout(timeout);
        }

        // Apply connect timeout
        if let Some(timeout) = self.connect_timeout {
            client_options = client_options.with_connect_timeout(timeout);
        }

        // Apply connection pool settings for better throughput
        if let Some(max_idle) = self.pool_max_idle_per_host {
            client_options = client_options.with_pool_max_idle_per_host(max_idle);
        }
        if let Some(idle_timeout) = self.pool_idle_timeout {
            client_options = client_options.with_pool_idle_timeout(idle_timeout);
        }

        // Apply allow_http setting (explicit setting overrides endpoint-based detection)
        if let Some(allow_http) = self.allow_http {
            client_options = client_options.with_allow_http(allow_http);
        }

        s3_builder = s3_builder.with_client_options(client_options);

        // Handle credentials
        let has_explicit_credentials =
            self.access_key_id.is_some() && self.secret_access_key.is_some();

        if has_explicit_credentials {
            if let Some(key) = &self.access_key_id {
                s3_builder = s3_builder.with_access_key_id(key);
            }
            if let Some(secret) = &self.secret_access_key {
                s3_builder = s3_builder.with_secret_access_key(secret);
            }
            if let Some(token) = &self.session_token {
                s3_builder = s3_builder.with_token(token);
            }
        } else {
            // Load credentials from AWS SDK environment
            s3_builder = apply_sdk_credentials(s3_builder).await?;
        }

        let store = s3_builder.build().context(ObjectStoreBuildSnafu)?;

        Ok(Arc::new(store))
    }
}

/// Applies AWS SDK credentials to an S3 builder if available.
///
/// This function initializes the AWS SDK configuration and applies the credential
/// provider to the builder. If no credentials are available, the builder is
/// returned unchanged (which may result in anonymous access for public buckets).
async fn apply_sdk_credentials(mut builder: AmazonS3Builder) -> Result<AmazonS3Builder> {
    tracing::trace!("Loading S3 credentials from environment");
    match get_or_init_sdk_config().await {
        Ok(Some(sdk_config)) => {
            if sdk_config.credentials_provider().is_some() {
                tracing::trace!("Using S3 credentials provider from SDK config");
                builder = builder.with_credentials(Arc::new(
                    S3CredentialProvider::from_config(sdk_config.as_ref())
                        .context(CredentialLoadSnafu)?,
                ));
            } else {
                tracing::trace!("No S3 credentials provider found in SDK config");
            }
        }
        Ok(None) => {
            tracing::trace!("No AWS SDK credentials available");
        }
        Err(err) => {
            tracing::warn!("Unable to initialize AWS credentials: {err}");
        }
    }
    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_builder_from_url() {
        let url = Url::parse("s3://my-bucket/path/to/data").expect("valid url");
        let builder =
            S3ObjectStoreBuilder::from_url(&url, Handle::current()).expect("should create builder");

        assert_eq!(builder.bucket_name, "my-bucket");
    }

    #[tokio::test]
    async fn test_builder_from_url_with_params() {
        let url =
            Url::parse("s3://my-bucket/path#region=us-west-2&client_timeout=30s&allow_http=true")
                .expect("valid url");
        let builder = S3ObjectStoreBuilder::from_url_with_params(&url, Handle::current())
            .expect("should create builder");

        assert_eq!(builder.bucket_name, "my-bucket");
        assert_eq!(builder.region, Some("us-west-2".to_string()));
        assert_eq!(builder.client_timeout, Some(Duration::from_secs(30)));
        assert_eq!(builder.allow_http, Some(true));
    }

    #[tokio::test]
    async fn test_builder_with_s3_prefixed_params() {
        let url =
            Url::parse("s3://my-bucket/path#s3_region=eu-west-1&s3_endpoint=http://localhost:9000")
                .expect("valid url");
        let builder = S3ObjectStoreBuilder::from_url_with_params(&url, Handle::current())
            .expect("should create builder");

        assert_eq!(builder.region, Some("eu-west-1".to_string()));
        assert_eq!(builder.endpoint, Some("http://localhost:9000".to_string()));
    }

    #[tokio::test]
    async fn test_builder_missing_bucket() {
        let url = Url::parse("s3:///path/to/data").expect("valid url");
        let result = S3ObjectStoreBuilder::from_url(&url, Handle::current());
        let _ = result.expect_err("should fail with missing bucket");
    }

    #[tokio::test]
    async fn test_builder_fluent_api() {
        let url = Url::parse("s3://my-bucket/path").expect("valid url");
        let builder = S3ObjectStoreBuilder::from_url(&url, Handle::current())
            .expect("should create builder")
            .with_region("us-east-1")
            .with_endpoint("http://localhost:9000")
            .with_credentials("AKID", "SECRET")
            .with_session_token("TOKEN")
            .with_client_timeout(Duration::from_secs(60))
            .with_allow_http(true);

        assert_eq!(builder.region, Some("us-east-1".to_string()));
        assert_eq!(builder.endpoint, Some("http://localhost:9000".to_string()));
        assert_eq!(builder.access_key_id, Some("AKID".to_string()));
        assert_eq!(builder.secret_access_key, Some("SECRET".to_string()));
        assert_eq!(builder.session_token, Some("TOKEN".to_string()));
        assert_eq!(builder.client_timeout, Some(Duration::from_secs(60)));
        assert_eq!(builder.allow_http, Some(true));
    }
}
