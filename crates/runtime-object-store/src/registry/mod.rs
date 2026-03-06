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

use std::{collections::HashMap, net::IpAddr, sync::Arc};

use aws_sdk_credential_bridge::S3CredentialProvider;
use datafusion::{
    error::DataFusionError,
    execution::{
        object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry},
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
};
use object_store::{
    ClientOptions, ObjectStore, RetryConfig, aws::AmazonS3Builder, azure::MicrosoftAzureBuilder,
    client::SpawnedReqwestConnector, gcp::GoogleCloudStorageBuilder, http::HttpBuilder,
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

#[derive(Debug)]
pub struct SpiceObjectStoreRegistry {
    inner: DefaultObjectStoreRegistry,
    io_runtime: Handle,
}

impl SpiceObjectStoreRegistry {
    #[must_use]
    pub fn new(io_runtime: Handle) -> Self {
        Self {
            inner: DefaultObjectStoreRegistry::new(),
            io_runtime,
        }
    }

    /// Parse `url_style` parameter. Returns `Some(true)` for vhost, `Some(false)` for path,
    /// `None` when not explicitly set (auto-detect).
    fn parse_s3_url_style(
        params: &HashMap<String, String>,
    ) -> datafusion::error::Result<Option<bool>> {
        match params.get("url_style").map(String::as_str) {
            Some("path") => Ok(Some(false)),
            Some("vhost") => Ok(Some(true)),
            None => Ok(None),
            Some(value) => Err(DataFusionError::Configuration(format!(
                "{value} is not a valid value for url_style"
            ))),
        }
    }

    fn endpoint_for_s3_url_style(
        endpoint: &str,
        bucket_name: &str,
        virtual_hosted_style_request: bool,
    ) -> datafusion::error::Result<String> {
        if !virtual_hosted_style_request {
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

        let virtual_hosted = match explicit_url_style {
            Some(v) => v,
            None => {
                // Auto-detect: IP endpoints must use path-style.
                if endpoint.is_some_and(|e| Self::endpoint_is_ip(e)) {
                    tracing::info!("s3_url_style not set; using path style for IP endpoint");
                    false
                } else if let Some(ep) = endpoint {
                    // Non-IP custom endpoint — DNS probe to detect style.
                    match Self::detect_s3_url_style(bucket_name, ep) {
                        Ok(detected) => detected,
                        Err(e) => {
                            tracing::warn!(
                                "s3_url_style detection failed ({e:#}), defaulting to vhost"
                            );
                            true
                        }
                    }
                } else {
                    // No custom endpoint (standard AWS) — vhost.
                    true
                }
            }
        };

        self.build_s3_object_store(bucket_name, &params, virtual_hosted)
    }

    /// Detect whether vhost or path style should be used by performing a DNS
    /// lookup on `<bucket>.<endpoint_host>`. If the name resolves the endpoint
    /// supports virtual-hosted style; NXDOMAIN means path style is required.
    fn detect_s3_url_style(bucket_name: &str, endpoint: &str) -> datafusion::error::Result<bool> {
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

        tracing::info!(
            "s3_url_style not set for endpoint '{endpoint}'; resolving '{vhost_host}' to detect URL style..."
        );

        use std::net::ToSocketAddrs;
        match vhost_host.to_socket_addrs() {
            Ok(_) => {
                tracing::info!(
                    "s3_url_style auto-detected: vhost (DNS resolved for '{vhost_host}')"
                );
                Ok(true)
            }
            Err(_) => {
                tracing::info!(
                    "s3_url_style auto-detected: path (DNS lookup failed for '{vhost_host}')"
                );
                Ok(false)
            }
        }
    }

    fn build_s3_object_store(
        &self,
        bucket_name: &str,
        params: &HashMap<String, String>,
        virtual_hosted_style_request: bool,
    ) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        let mut s3_builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket_name)
            .with_http_connector(SpawnedReqwestConnector::new(self.io_runtime.clone()))
            .with_allow_http(true);
        let mut client_options = ClientOptions::default();

        s3_builder = s3_builder.with_virtual_hosted_style_request(virtual_hosted_style_request);

        if let Some(region) = params.get("region") {
            s3_builder = s3_builder.with_region(region);
        }
        if let Some(endpoint) = params.get("endpoint") {
            let endpoint = Self::endpoint_for_s3_url_style(
                endpoint,
                bucket_name,
                virtual_hosted_style_request,
            )?;
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
            if let Some(sdk_config) = aws_sdk_credential_bridge::get_sdk_config() {
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

        let mut builder = MicrosoftAzureBuilder::from_env()
            .with_http_connector(SpawnedReqwestConnector::new(self.io_runtime.clone()));

        if let Some(sas) = params.get("sas_string") {
            url.set_query(Some(sas));
        }

        if let Some(use_emulator) = params.get("use_emulator") {
            let as_bool = use_emulator.parse::<bool>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{use_emulator} is not a valid boolean for use_emulator"
                ))
            })?;
            builder = builder.with_use_emulator(as_bool);
            if !as_bool {
                builder = builder.with_url(url.to_string());
            }
        } else {
            builder = builder.with_url(url.to_string());
        }

        builder = builder.with_url(url.to_string());

        if let Some(account) = params.get("account") {
            builder = builder.with_account(account);
        }

        if let Some(container_name) = params.get("container_name") {
            builder = builder.with_container_name(container_name);
        }

        if let Some(access_key) = params.get("access_key") {
            builder = builder.with_access_key(access_key);
        }
        if let Some(bearer_token) = params.get("bearer_token") {
            builder = builder.with_bearer_token_authorization(bearer_token);
        }
        if let Some(client_id) = params.get("client_id") {
            builder = builder.with_client_id(client_id);
        }
        if let Some(client_secret) = params.get("client_secret") {
            builder = builder.with_client_secret(client_secret);
        }
        if let Some(tenant_id) = params.get("tenant_id") {
            builder = builder.with_tenant_id(tenant_id);
        }
        if let Some(endpoint) = params.get("endpoint") {
            builder = builder.with_endpoint(endpoint.clone());
        }

        if let Some(use_fabric_endpoint) = params.get("use_fabric_endpoint") {
            let as_bool = use_fabric_endpoint.parse::<bool>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{use_fabric_endpoint} is not a valid boolean for use_fabric_endpoint"
                ))
            })?;
            builder = builder.with_use_fabric_endpoint(as_bool);
        }
        if let Some(allow_http) = params.get("allow_http") {
            let as_bool = allow_http.parse::<bool>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{allow_http} is not a valid boolean for allow_http"
                ))
            })?;
            builder = builder.with_allow_http(as_bool);
        }
        if let Some(authority_host) = params.get("authority_host") {
            builder = builder.with_authority_host(authority_host);
        }

        // Retry and backoff configuration
        let mut retry_config = RetryConfig::default();

        if let Some(retry_timeout) = params.get("retry_timeout") {
            let as_duration = fundu::parse_duration(retry_timeout).map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{retry_timeout} is not a valid duration for retry_timeout"
                ))
            })?;
            retry_config.retry_timeout = as_duration;
        }
        if let Some(max_retries) = params.get("max_retries") {
            let as_usize = max_retries.parse::<usize>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{max_retries} is not a valid usize for max_retries"
                ))
            })?;
            retry_config.max_retries = as_usize;
        }
        if let Some(backoff_initial_duration) = params.get("backoff_initial_duration") {
            let as_duration = fundu::parse_duration(backoff_initial_duration).map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{backoff_initial_duration} is not a valid duration for backoff_initial_duration"
                ))
            })?;
            retry_config.backoff.init_backoff = as_duration;
        }
        if let Some(backoff_max_duration) = params.get("backoff_max_duration") {
            let as_duration = fundu::parse_duration(backoff_max_duration).map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{backoff_max_duration} is not a valid duration for backoff_max_duration"
                ))
            })?;
            retry_config.backoff.max_backoff = as_duration;
        }
        if let Some(backoff_base) = params.get("backoff_base") {
            let as_f64 = backoff_base.parse::<f64>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{backoff_base} is not a valid f64 for backoff_base"
                ))
            })?;
            retry_config.backoff.base = as_f64;
        }
        builder = builder.with_retry(retry_config);

        if let Some(proxy_url) = params.get("proxy_url") {
            builder = builder.with_proxy_url(proxy_url);
        }
        if let Some(proxy_ca_certificate) = params.get("proxy_ca_certificate") {
            builder = builder.with_proxy_ca_certificate(proxy_ca_certificate);
        }
        if let Some(proxy_excludes) = params.get("proxy_excludes") {
            builder = builder.with_proxy_excludes(proxy_excludes);
        }
        if let Some(msi_endpoint) = params.get("msi_endpoint") {
            builder = builder.with_msi_endpoint(msi_endpoint);
        }
        if let Some(federated_token_file) = params.get("federated_token_file") {
            builder = builder.with_federated_token_file(federated_token_file);
        }

        if let Some(use_cli) = params.get("use_cli") {
            let as_bool = use_cli.parse::<bool>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{use_cli} is not a valid boolean for use_cli"
                ))
            })?;
            builder = builder.with_use_azure_cli(as_bool);
        }

        if let Some(skip_signature) = params.get("skip_signature") {
            let as_bool = skip_signature.parse::<bool>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{skip_signature} is not a valid boolean for skip_signature"
                ))
            })?;
            builder = builder.with_skip_signature(as_bool);
        }

        if let Some(disable_tagging) = params.get("disable_tagging") {
            let as_bool = disable_tagging.parse::<bool>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{disable_tagging} is not a valid boolean for disable_tagging"
                ))
            })?;
            builder = builder.with_disable_tagging(as_bool);
        }

        let azure_store = Arc::new(
            builder
                .build()
                .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))?,
        );

        Ok(azure_store as Arc<dyn ObjectStore>)
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

        // Check skip_signature first - if true, use new() instead of from_env() to avoid
        // automatic credential loading attempts
        let skip_signature = match params.get("skip_signature") {
            Some(value) => value.parse::<bool>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{value} is not a valid boolean for skip_signature"
                ))
            })?,
            None => false,
        };

        let mut builder = if skip_signature {
            GoogleCloudStorageBuilder::new()
                .with_skip_signature(true)
                .with_bucket_name(bucket_name)
                .with_http_connector(SpawnedReqwestConnector::new(self.io_runtime.clone()))
        } else {
            GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket_name)
                .with_http_connector(SpawnedReqwestConnector::new(self.io_runtime.clone()))
        };

        let mut client_options = ClientOptions::default();

        // Service account authentication (only if not skip_signature)
        if !skip_signature {
            // Prefer explicit service_account_path, but also accept legacy aliases:
            // - service_account (canonicalized from google_service_account by Parameters::canonicalize_gcs_fragments)
            // - google_service_account (for direct compatibility if not canonicalized)
            if let Some(service_account_path) = params
                .get("service_account_path")
                .or_else(|| params.get("service_account"))
                .or_else(|| params.get("google_service_account"))
            {
                builder = builder.with_service_account_path(service_account_path);
            }
            if let Some(service_account_key) = params.get("service_account_key") {
                builder = builder.with_service_account_key(service_account_key);
            }

            // Application default credentials - use GOOGLE_APPLICATION_CREDENTIALS env var path
            // with_application_credentials takes a path to the credentials file
            if let Some(application_default_credentials) =
                params.get("application_default_credentials")
            {
                let as_bool = application_default_credentials.parse::<bool>().map_err(|_| {
                    DataFusionError::Configuration(format!(
                        "{application_default_credentials} is not a valid boolean for application_default_credentials"
                    ))
                })?;
                if as_bool {
                    // Use GOOGLE_APPLICATION_CREDENTIALS environment variable if set
                    if let Ok(creds_path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
                        builder = builder.with_application_credentials(creds_path);
                    }
                    // If not set, the builder will attempt to use default credentials automatically
                }
            }
        }

        // Client options
        if let Some(timeout) = params.get("client_timeout") {
            client_options =
                client_options.with_timeout(fundu::parse_duration(timeout).map_err(|_| {
                    DataFusionError::Configuration(format!("Unable to parse timeout: {timeout}"))
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

        // Retry and backoff configuration
        let mut retry_config = RetryConfig::default();

        if let Some(retry_timeout) = params.get("retry_timeout") {
            let as_duration = fundu::parse_duration(retry_timeout).map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{retry_timeout} is not a valid duration for retry_timeout"
                ))
            })?;
            retry_config.retry_timeout = as_duration;
        }
        if let Some(max_retries) = params.get("max_retries") {
            let as_usize = max_retries.parse::<usize>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{max_retries} is not a valid usize for max_retries"
                ))
            })?;
            retry_config.max_retries = as_usize;
        }
        if let Some(backoff_initial_duration) = params.get("backoff_initial_duration") {
            let as_duration = fundu::parse_duration(backoff_initial_duration).map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{backoff_initial_duration} is not a valid duration for backoff_initial_duration"
                ))
            })?;
            retry_config.backoff.init_backoff = as_duration;
        }
        if let Some(backoff_max_duration) = params.get("backoff_max_duration") {
            let as_duration = fundu::parse_duration(backoff_max_duration).map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{backoff_max_duration} is not a valid duration for backoff_max_duration"
                ))
            })?;
            retry_config.backoff.max_backoff = as_duration;
        }
        if let Some(backoff_base) = params.get("backoff_base") {
            let as_f64 = backoff_base.parse::<f64>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{backoff_base} is not a valid f64 for backoff_base"
                ))
            })?;
            retry_config.backoff.base = as_f64;
        }
        builder = builder.with_retry(retry_config);

        builder = builder.with_client_options(client_options);

        let gcs_store = Arc::new(
            builder
                .build()
                .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))?,
        );

        Ok(gcs_store as Arc<dyn ObjectStore>)
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
            Some(Some(false))
        );
    }

    #[test]
    fn test_parse_s3_url_style_vhost() {
        let params = HashMap::from([("url_style".to_string(), "vhost".to_string())]);
        assert_eq!(
            SpiceObjectStoreRegistry::parse_s3_url_style(&params).ok(),
            Some(Some(true))
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
            false,
        )
        .expect("path-style endpoint should parse");

        assert_eq!(endpoint, "https://t3.storage.dev");
    }

    #[test]
    fn test_endpoint_for_s3_url_style_vhost_adds_bucket_prefix() {
        let endpoint = SpiceObjectStoreRegistry::endpoint_for_s3_url_style(
            "https://t3.storage.dev",
            "spiceai-public-datasets",
            true,
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
            true,
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
            true,
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
            true,
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
