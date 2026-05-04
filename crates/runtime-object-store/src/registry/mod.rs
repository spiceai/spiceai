/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::{Arc, Mutex},
};

use aws_sdk_credential_bridge::S3CredentialProvider;
use datafusion::{
    error::DataFusionError,
    execution::{
        object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
};
use object_store::{
    ClientOptions, ObjectStore, aws::AmazonS3Builder, client::SpawnedReqwestConnector,
    http::HttpBuilder,
};
use tokio::runtime::Handle;
use url::{Url, form_urlencoded::parse};

#[cfg(feature = "ftp")]
use crate::store::ftp::FTPObjectStore;
#[cfg(feature = "nfs")]
use crate::store::nfs::NFSObjectStore;
#[cfg(feature = "sftp")]
use crate::store::sftp::SFTPObjectStore;
#[cfg(feature = "smb")]
use crate::store::smb::SMBObjectStore;

/// S3 URL addressing style.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum S3UrlStyle {
    /// Virtual-hosted style: `<bucket>.endpoint`.
    VirtualHosted,
    /// Path style: `endpoint/<bucket>`.
    Path,
}

impl S3UrlStyle {
    /// Returns `true` if this is virtual-hosted style.
    fn is_virtual_hosted(self) -> bool {
        matches!(self, Self::VirtualHosted)
    }
}

impl std::fmt::Display for S3UrlStyle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VirtualHosted => f.write_str("vhost"),
            Self::Path => f.write_str("path"),
        }
    }
}

/// Internal state for S3 URL style auto-detection.
#[derive(Debug, Default)]
struct S3StyleState {
    /// Cache of definitively detected S3 URL styles per endpoint. Only positive
    /// DNS results ([`S3UrlStyle::VirtualHosted`]) are cached because they are
    /// conclusive. [`S3UrlStyle::Path`] results (from DNS lookup failure) are
    /// *not* cached because the failure may be transient (e.g. temporary
    /// resolver outage) — a later probe may succeed and correctly detect vhost.
    cache: HashMap<String, S3UrlStyle>,

    /// Endpoints for which auto-detection has already been logged, so repeated
    /// probes for the same endpoint don't produce duplicate log lines.
    logged: HashSet<String>,
}

#[derive(Debug)]
pub struct SpiceObjectStoreRegistry {
    inner: DefaultObjectStoreRegistry,
    io_runtime: Handle,
    /// State for S3 URL style auto-detection caching and logging.
    s3_style_state: Mutex<S3StyleState>,
}

impl SpiceObjectStoreRegistry {
    #[must_use]
    pub fn new(io_runtime: Handle) -> Self {
        Self {
            inner: DefaultObjectStoreRegistry::new(),
            io_runtime,
            s3_style_state: Mutex::new(S3StyleState::default()),
        }
    }

    /// Parse `url_style` parameter. Returns `Some(VirtualHosted)` for vhost, `Some(Path)` for path,
    /// `None` when not explicitly set (auto-detect).
    fn parse_s3_url_style(
        params: &HashMap<String, String>,
    ) -> datafusion::error::Result<Option<S3UrlStyle>> {
        match params.get("url_style").map(String::as_str) {
            Some("path") => Ok(Some(S3UrlStyle::Path)),
            Some("vhost") => Ok(Some(S3UrlStyle::VirtualHosted)),
            None => Ok(None),
            Some(value) => Err(DataFusionError::Configuration(format!(
                "{value} is not a valid value for url_style"
            ))),
        }
    }

    fn endpoint_for_s3_url_style(
        endpoint: &str,
        bucket_name: &str,
        url_style: S3UrlStyle,
    ) -> datafusion::error::Result<String> {
        if url_style == S3UrlStyle::Path {
            return Ok(endpoint.to_string());
        }

        let mut endpoint_url = Url::parse(endpoint).map_err(|e| {
            DataFusionError::Configuration(format!(
                "Unable to parse endpoint '{endpoint}' as URL: {e}"
            ))
        })?;

        let Some(host) = endpoint_url.host_str() else {
            return Err(DataFusionError::Configuration(format!(
                "No host found in endpoint URL: {endpoint}"
            )));
        };

        let virtual_hosted = format!("{bucket_name}.{host}");
        endpoint_url.set_host(Some(&virtual_hosted)).map_err(|e| {
            DataFusionError::Configuration(format!(
                "Unable to set virtual-hosted endpoint host to {virtual_hosted}: {e}"
            ))
        })?;

        Ok(endpoint_url.to_string().trim_end_matches('/').to_string())
    }

    /// Returns `true` if the endpoint host is an IP address.
    fn endpoint_is_ip(endpoint: &str) -> bool {
        Url::parse(endpoint)
            .ok()
            .and_then(|u| u.host_str().map(|h| h.parse::<IpAddr>().is_ok()))
            .unwrap_or(false)
    }

    fn prepare_s3_object_store(
        &self,
        url: &Url,
    ) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let Some(bucket_name) = url.host_str() else {
            return Err(DataFusionError::Configuration(
                "No bucket name provided".to_string(),
            ));
        };

        let params: HashMap<String, String> = parse(url.fragment().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();

        let explicit_url_style = Self::parse_s3_url_style(&params)?;
        let endpoint = params.get("endpoint");

        let url_style = match explicit_url_style {
            Some(v) => v,
            None => {
                // Auto-detect: IP endpoints must use path-style.
                if endpoint.is_some_and(|e| Self::endpoint_is_ip(e)) {
                    tracing::debug!("s3_url_style not set; using path style for IP endpoint");
                    S3UrlStyle::Path
                } else if let Some(ep) = endpoint {
                    // Non-IP custom endpoint — cached result or DNS probe.
                    self.resolve_s3_url_style(bucket_name, ep)
                } else {
                    // No custom endpoint (standard AWS) — vhost.
                    S3UrlStyle::VirtualHosted
                }
            }
        };

        self.build_s3_object_store(bucket_name, &params, url_style)
    }

    /// Resolve the S3 URL style for a non-IP custom endpoint, using the cache
    /// when available. Uses a double-checked pattern so the lock is *not* held
    /// during the (potentially slow) DNS probe.
    ///
    /// Only definitive results ([`S3UrlStyle::VirtualHosted`], from a successful
    /// DNS resolution) are cached. [`S3UrlStyle::Path`] results are not cached
    /// because a DNS failure may be transient — a later probe could succeed.
    fn resolve_s3_url_style(&self, bucket_name: &str, endpoint: &str) -> S3UrlStyle {
        // Fast path: return cached result without doing any I/O.
        {
            let state = self
                .s3_style_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(&cached) = state.cache.get(endpoint) {
                return cached;
            }
        }
        // Lock released — perform DNS probe.

        let detected = match Self::detect_s3_url_style(bucket_name, endpoint) {
            Ok(style) => style,
            Err(e) => {
                tracing::warn!("s3_url_style detection failed ({e:#}), defaulting to vhost");
                return S3UrlStyle::VirtualHosted;
            }
        };

        // Re-acquire lock for double-check and state update.
        let mut state = self
            .s3_style_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Another thread may have completed detection while we were probing.
        if let Some(&cached) = state.cache.get(endpoint) {
            return cached;
        }

        // Only cache definitive (VirtualHosted) results. Path results come from
        // DNS lookup failure, which may be a transient resolver/network issue.
        if detected == S3UrlStyle::VirtualHosted {
            state.cache.insert(endpoint.to_string(), detected);
        }

        if state.logged.insert(endpoint.to_string()) {
            tracing::info!("Auto-detected s3_url_style={detected} for endpoint '{endpoint}'");
        }

        detected
    }

    /// Detect whether vhost or path style should be used by performing a DNS
    /// lookup on `<bucket>.<endpoint_host>`. If the name resolves the endpoint
    /// supports virtual-hosted style; NXDOMAIN means path style is required.
    fn detect_s3_url_style(
        bucket_name: &str,
        endpoint: &str,
    ) -> datafusion::error::Result<S3UrlStyle> {
        use std::net::ToSocketAddrs;

        let endpoint_url = Url::parse(endpoint).map_err(|e| {
            DataFusionError::Configuration(format!(
                "Unable to parse endpoint '{endpoint}' as URL: {e}"
            ))
        })?;

        let Some(host) = endpoint_url.host_str() else {
            return Err(DataFusionError::Configuration(format!(
                "No host found in endpoint URL: {endpoint}"
            )));
        };

        let port = endpoint_url.port().unwrap_or(match endpoint_url.scheme() {
            "https" => 443,
            _ => 80,
        });

        let vhost_host = format!("{bucket_name}.{host}:{port}");

        tracing::debug!(
            "s3_url_style not set for endpoint '{endpoint}'; resolving '{vhost_host}' to detect URL style..."
        );

        if vhost_host.to_socket_addrs().is_ok() {
            Ok(S3UrlStyle::VirtualHosted)
        } else {
            Ok(S3UrlStyle::Path)
        }
    }

    fn build_s3_object_store(
        &self,
        bucket_name: &str,
        params: &HashMap<String, String>,
        url_style: S3UrlStyle,
    ) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let mut s3_builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .with_http_connector(SpawnedReqwestConnector::new(self.io_runtime.clone()))
            .with_allow_http(true);
        let mut client_options = ClientOptions::default();

        s3_builder = s3_builder.with_virtual_hosted_style_request(url_style.is_virtual_hosted());

        if let Some(region) = params.get("region") {
            s3_builder = s3_builder.with_region(region);
        }
        if let Some(endpoint) = params.get("endpoint") {
            let endpoint = Self::endpoint_for_s3_url_style(endpoint, bucket_name, url_style)?;
            s3_builder = s3_builder.with_endpoint(endpoint);
        }
        if let Some(timeout) = params.get("client_timeout") {
            client_options =
                client_options.with_timeout(fundu::parse_duration(timeout).map_err(|_| {
                    DataFusionError::Configuration(format!("Unable to parse timeout: {timeout}",))
                })?);
        }
        if let Some(allow_http) = params.get("allow_http") {
            let as_bool = allow_http.parse::<bool>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{allow_http} is not a valid boolean for allow_http"
                ))
            })?;
            client_options = client_options.with_allow_http(as_bool);
        }

        // Determine credential configuration using common utility
        let credential_config = aws_sdk_credential_bridge::determine_s3_credential_config(
            params.get("key").map(String::as_str),
            params.get("secret").map(String::as_str),
            params.get("auth").map(String::as_str),
            params.get("iam_role_source").map(String::as_str),
        )
        .map_err(DataFusionError::Configuration)?;

        // Apply explicit credentials if provided
        if !credential_config.load_from_environment
            && !credential_config.skip_signature
            && let (Some(key), Some(secret)) = (params.get("key"), params.get("secret"))
        {
            s3_builder = s3_builder.with_access_key_id(key);
            s3_builder = s3_builder.with_secret_access_key(secret);
            if let Some(token) = params.get("session_token") {
                s3_builder = s3_builder.with_token(token);
            }
        }

        // Configure skip signature for public access
        if credential_config.skip_signature {
            s3_builder = s3_builder.with_skip_signature(true);
        }

        s3_builder = s3_builder.with_client_options(client_options);

        // Load credentials from AWS SDK environment if needed
        if credential_config.load_from_environment {
            tracing::trace!("Loading S3 credentials from environment");

            let use_restricted_source = credential_config
                .iam_role_source
                .as_deref()
                .is_some_and(|s| s == "metadata" || s == "env");

            if use_restricted_source {
                // For restricted IAM role sources (metadata/env), build a fresh config
                // with the restricted credential chain instead of using the global cache.
                let iam_source = credential_config
                    .iam_role_source
                    .as_deref()
                    .unwrap_or("auto");
                let region = params.get("region").cloned();
                let restricted_config = self.io_runtime.block_on(
                    aws_sdk_credential_bridge::build_restricted_sdk_config(iam_source, region),
                );
                match restricted_config {
                    Ok(sdk_config) => {
                        if sdk_config.credentials_provider().is_some() {
                            tracing::trace!(
                                "Using S3 credentials provider with restricted IAM role source: {iam_source}"
                            );
                            s3_builder = s3_builder.with_credentials(Arc::new(
                                S3CredentialProvider::from_config(&sdk_config).map_err(|e| {
                                    object_store::Error::Generic {
                                        store: "S3",
                                        source: e.into(),
                                    }
                                })?,
                            ));
                        } else {
                            tracing::trace!(
                                "No S3 credentials provider found from restricted IAM source, assuming public access"
                            );
                            s3_builder = s3_builder.with_skip_signature(true);
                        }
                    }
                    Err(err) => {
                        tracing::warn!("Failed to build restricted AWS SDK config for S3: {err}");
                        s3_builder = s3_builder.with_skip_signature(true);
                    }
                }
            } else if let Some(sdk_config) = aws_sdk_credential_bridge::get_sdk_config() {
                if sdk_config.credentials_provider().is_some() {
                    tracing::trace!("Using S3 credentials provider from SDK config");
                    s3_builder = s3_builder.with_credentials(Arc::new(
                        S3CredentialProvider::from_config(sdk_config.as_ref()).map_err(|e| {
                            object_store::Error::Generic {
                                store: "S3",
                                source: e.into(),
                            }
                        })?,
                    ));
                } else {
                    tracing::trace!(
                        "No S3 credentials provider found from AWS SDK, assuming public access"
                    );
                    s3_builder = s3_builder.with_skip_signature(true);
                }
            } else {
                tracing::trace!(
                    "No AWS SDK credentials provider available, assuming public access"
                );
                s3_builder = s3_builder.with_skip_signature(true);
            }
        }

        Ok(Arc::new(s3_builder.build()?))
    }

    fn prepare_https_object_store(
        &self,
        url: &Url,
    ) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let base_url = if url.scheme() == "https" {
            format!("https://{}/", url.authority())
        } else {
            format!("http://{}/", url.authority())
        };

        let mut client_options = ClientOptions::new().with_allow_http(true);
        let params: HashMap<String, String> = parse(url.fragment().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
        if let Some(timeout) = params.get("client_timeout") {
            client_options =
                client_options.with_timeout(fundu::parse_duration(timeout).map_err(|_| {
                    DataFusionError::Configuration(format!("Unable to parse timeout: {timeout}",))
                })?);
        }

        let builder = HttpBuilder::new()
            .with_url(base_url)
            .with_http_connector(SpawnedReqwestConnector::new(self.io_runtime.clone()))
            .with_client_options(client_options);

        Ok(Arc::new(builder.build()?))
    }

    #[cfg(feature = "ftp")]
    fn prepare_ftp_object_store(url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let Some(host) = url.host() else {
            return Err(DataFusionError::Configuration(
                "No host provided for FTP".to_string(),
            ));
        };
        let params: HashMap<String, String> = parse(url.fragment().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();

        let port = params
            .get("port")
            .map_or("21".to_string(), ToOwned::to_owned);
        let user = params.get("user").map(ToOwned::to_owned).ok_or_else(|| {
            DataFusionError::Configuration("No user provided for FTP".to_string())
        })?;
        let password = params.get("pass").map(ToOwned::to_owned).ok_or_else(|| {
            DataFusionError::Configuration("No password provided for FTP".to_string())
        })?;

        let client_timeout = params
            .get("client_timeout")
            .map(|timeout| fundu::parse_duration(timeout))
            .transpose()
            .map_err(|_| {
                DataFusionError::Configuration(format!(
                    "Unable to parse timeout: {}",
                    params["client_timeout"]
                ))
            })?;

        Ok(Arc::new(FTPObjectStore::new(
            user,
            password,
            host.to_string(),
            port,
            client_timeout,
        )) as Arc<dyn ObjectStore>)
    }

    #[cfg(feature = "sftp")]
    fn prepare_sftp_object_store(url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let Some(host) = url.host() else {
            return Err(DataFusionError::Configuration(
                "No host provided for SFTP".to_string(),
            ));
        };
        let params: HashMap<String, String> = parse(url.fragment().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();

        let port = params
            .get("port")
            .map_or("22".to_string(), ToOwned::to_owned);
        let user = params.get("user").map(ToOwned::to_owned).ok_or_else(|| {
            DataFusionError::Configuration("No user provided for SFTP".to_string())
        })?;
        let password = params.get("pass").map(ToOwned::to_owned).ok_or_else(|| {
            DataFusionError::Configuration("No password provided for SFTP".to_string())
        })?;
        let client_timeout = params
            .get("client_timeout")
            .map(|timeout| fundu::parse_duration(timeout))
            .transpose()
            .map_err(|_| {
                DataFusionError::Configuration(format!(
                    "Unable to parse timeout: {}",
                    params["client_timeout"]
                ))
            })?;

        Ok(Arc::new(SFTPObjectStore::new(
            user,
            password,
            host.to_string(),
            port,
            client_timeout,
        )) as Arc<dyn ObjectStore>)
    }

    #[cfg(feature = "smb")]
    fn prepare_smb_object_store(url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let Some(host) = url.host() else {
            return Err(DataFusionError::Configuration(
                "No host provided for SMB".to_string(),
            ));
        };

        // Extract share name from the first path segment
        let path = url.path();
        let share = path
            .trim_start_matches('/')
            .split('/')
            .next()
            .ok_or_else(|| {
                DataFusionError::Configuration("No share name provided for SMB".to_string())
            })?
            .to_string();

        let params: HashMap<String, String> = parse(url.fragment().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();

        let user = params.get("user").map(ToOwned::to_owned).ok_or_else(|| {
            DataFusionError::Configuration("No user provided for SMB".to_string())
        })?;
        let password = params.get("pass").map(ToOwned::to_owned).ok_or_else(|| {
            DataFusionError::Configuration("No password provided for SMB".to_string())
        })?;
        let port = params
            .get("port")
            .map(|p| {
                p.parse::<u16>()
                    .map_err(|_| DataFusionError::Configuration(format!("Invalid SMB port: {p}")))
            })
            .transpose()?;
        let client_timeout = params
            .get("client_timeout")
            .map(|timeout| fundu::parse_duration(timeout))
            .transpose()
            .map_err(|_| {
                DataFusionError::Configuration(format!(
                    "Unable to parse timeout: {}",
                    params["client_timeout"]
                ))
            })?;

        Ok(Arc::new(SMBObjectStore::new(
            host.to_string(),
            port,
            share,
            user,
            password,
            client_timeout,
        )) as Arc<dyn ObjectStore>)
    }

    #[cfg(feature = "nfs")]
    fn prepare_nfs_object_store(url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let Some(host) = url.host() else {
            return Err(DataFusionError::Configuration(
                "No host provided for NFS".to_string(),
            ));
        };

        // The path is the export path
        let export_path = url.path().to_string();
        if export_path.is_empty() || export_path == "/" {
            return Err(DataFusionError::Configuration(
                "No export path provided for NFS".to_string(),
            ));
        }

        let params: HashMap<String, String> = parse(url.fragment().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();

        let client_timeout = params
            .get("client_timeout")
            .map(|timeout| fundu::parse_duration(timeout))
            .transpose()
            .map_err(|_| {
                DataFusionError::Configuration(format!(
                    "Unable to parse timeout: {}",
                    params["client_timeout"]
                ))
            })?;

        Ok(Arc::new(NFSObjectStore::new(
            host.to_string(),
            export_path,
            client_timeout,
        )) as Arc<dyn ObjectStore>)
    }

    // Splitting up this function wouldn't make much sense as it's all used to create the ObjectStore
    fn prepare_azure_object_store(
        &self,
        url: &Url,
    ) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let mut url = url.clone();

        // Rewrite the URL Scheme
        url.set_scheme("abfss").map_err(|()| {
            DataFusionError::Configuration(format!(
                "Unable to set scheme to abfss for URL: {url:?}"
            ))
        })?;

        let params: HashMap<String, String> = parse(url.fragment().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
        url.set_fragment(None);

        crate::builder::build_azure_object_store(&url, &params, self.io_runtime.clone())
    }

    fn prepare_gcs_object_store(
        &self,
        url: &Url,
    ) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let Some(bucket_name) = url.host_str() else {
            return Err(DataFusionError::Configuration(
                "No bucket name provided".to_string(),
            ));
        };

        let params: HashMap<String, String> = parse(url.fragment().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();

        crate::builder::build_gcs_object_store(bucket_name, &params, self.io_runtime.clone())
    }

    fn get_feature_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        if url.as_str().starts_with("https://") || url.as_str().starts_with("http://") {
            return self.prepare_https_object_store(url);
        }
        if url.as_str().starts_with("s3://") {
            return self.prepare_s3_object_store(url);
        }

        if url.as_str().starts_with("abfs://") || url.as_str().starts_with("abfss://") {
            return self.prepare_azure_object_store(url);
        }

        if url.as_str().starts_with("gs://") || url.as_str().starts_with("gcs://") {
            return self.prepare_gcs_object_store(url);
        }

        #[cfg(feature = "ftp")]
        if url.as_str().starts_with("ftp://") {
            return Self::prepare_ftp_object_store(url);
        }

        #[cfg(feature = "sftp")]
        if url.as_str().starts_with("sftp://") {
            return Self::prepare_sftp_object_store(url);
        }

        #[cfg(feature = "smb")]
        if url.as_str().starts_with("smb://") {
            return Self::prepare_smb_object_store(url);
        }

        #[cfg(feature = "nfs")]
        if url.as_str().starts_with("nfs://") {
            return Self::prepare_nfs_object_store(url);
        }

        Err(DataFusionError::Execution(format!(
            "No object store available for: {url:?}"
        )))
    }
}

impl ObjectStoreRegistry for SpiceObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(url, store)
    }

    fn get_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        self.inner.get_store(url).or_else(|_| {
            let store = self.get_feature_store(url)?;
            self.inner.register_store(url, Arc::clone(&store));
            Ok(store)
        })
    }
}

// This method uses unwrap_or_default, however it should never fail on the initialization. See
// RuntimeEnv::default()
#[must_use]
pub fn default_runtime_env(io_runtime: Handle) -> Arc<RuntimeEnv> {
    match RuntimeEnvBuilder::default()
        .with_object_store_registry(Arc::new(SpiceObjectStoreRegistry::new(io_runtime)))
        .build_arc()
    {
        Ok(runtime_env) => runtime_env,
        Err(e) => {
            unreachable!("Tests ensure this should never fail: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_default_runtime_env() {
        let _ = default_runtime_env(Handle::current());
    }

    #[test]
    fn test_parse_s3_url_style_not_set_returns_none() {
        let params = HashMap::new();
        assert_eq!(
            SpiceObjectStoreRegistry::parse_s3_url_style(&params).ok(),
            Some(None)
        );
    }

    #[test]
    fn test_parse_s3_url_style_path() {
        let params = HashMap::from([("url_style".to_string(), "path".to_string())]);
        assert_eq!(
            SpiceObjectStoreRegistry::parse_s3_url_style(&params).ok(),
            Some(Some(S3UrlStyle::Path))
        );
    }

    #[test]
    fn test_parse_s3_url_style_vhost() {
        let params = HashMap::from([("url_style".to_string(), "vhost".to_string())]);
        assert_eq!(
            SpiceObjectStoreRegistry::parse_s3_url_style(&params).ok(),
            Some(Some(S3UrlStyle::VirtualHosted))
        );
    }

    #[test]
    fn test_parse_s3_url_style_invalid_value() {
        let params = HashMap::from([("url_style".to_string(), "invalid".to_string())]);
        let _ = SpiceObjectStoreRegistry::parse_s3_url_style(&params)
            .expect_err("invalid url_style should error");
    }

    #[test]
    fn test_endpoint_for_s3_url_style_path_keeps_endpoint() {
        let endpoint = SpiceObjectStoreRegistry::endpoint_for_s3_url_style(
            "https://t3.storage.dev",
            "spiceai-public-datasets",
            S3UrlStyle::Path,
        )
        .expect("path-style endpoint should parse");

        assert_eq!(endpoint, "https://t3.storage.dev");
    }

    #[test]
    fn test_endpoint_for_s3_url_style_vhost_adds_bucket_prefix() {
        let endpoint = SpiceObjectStoreRegistry::endpoint_for_s3_url_style(
            "https://t3.storage.dev",
            "spiceai-public-datasets",
            S3UrlStyle::VirtualHosted,
        )
        .expect("virtual-hosted endpoint should parse");

        assert_eq!(endpoint, "https://spiceai-public-datasets.t3.storage.dev");
    }

    #[test]
    fn test_endpoint_for_s3_url_style_vhost_always_prepends_bucket() {
        // Even when bucket name matches the start of the host, always prepend.
        let endpoint = SpiceObjectStoreRegistry::endpoint_for_s3_url_style(
            "https://spiceai-public-datasets.t3.storage.dev",
            "spiceai-public-datasets",
            S3UrlStyle::VirtualHosted,
        )
        .expect("virtual-hosted endpoint should parse");

        assert_eq!(
            endpoint,
            "https://spiceai-public-datasets.spiceai-public-datasets.t3.storage.dev"
        );
    }

    #[test]
    fn test_endpoint_for_s3_url_style_vhost_with_port_preserves_port() {
        let endpoint = SpiceObjectStoreRegistry::endpoint_for_s3_url_style(
            "http://minio:9000",
            "bucket",
            S3UrlStyle::VirtualHosted,
        )
        .expect("virtual-hosted endpoint with port should parse");

        assert_eq!(endpoint, "http://bucket.minio:9000");
    }

    #[test]
    fn test_endpoint_for_s3_url_style_vhost_bucket_matches_host_prefix() {
        // Bucket "t3" with endpoint "t3.storage.dev" — must still prepend.
        let endpoint = SpiceObjectStoreRegistry::endpoint_for_s3_url_style(
            "https://t3.storage.dev",
            "t3",
            S3UrlStyle::VirtualHosted,
        )
        .expect("virtual-hosted endpoint should parse");

        assert_eq!(endpoint, "https://t3.t3.storage.dev");
    }

    #[test]
    fn test_endpoint_is_ip() {
        assert!(SpiceObjectStoreRegistry::endpoint_is_ip(
            "http://192.168.1.100:9000"
        ));
        assert!(SpiceObjectStoreRegistry::endpoint_is_ip(
            "http://127.0.0.1:9000"
        ));
        assert!(!SpiceObjectStoreRegistry::endpoint_is_ip(
            "https://t3.storage.dev"
        ));
        assert!(!SpiceObjectStoreRegistry::endpoint_is_ip(
            "http://minio:9000"
        ));
    }
}
