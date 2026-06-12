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
    fmt::Display,
    path::PathBuf,
    process::{Child, Command},
    sync::Arc,
    time::Duration,
};

use anyhow::{Result, anyhow};
use flight_client::{Credentials, FlightClient};
use secrecy::SecretString;
use spiceai::{Client as SpiceClient, ClientBuilder};
use spicepod::spec::SpicepodDefinition;
use sysinfo::Pid;
use tempfile::TempDir;

use crate::{
    constants::{FLIGHT_URL, HEALTH_ENDPOINT, HTTP_BASE_URL, READY_ENDPOINT},
    process::Process,
    utils::wait_until_true,
};

#[derive(Debug, Clone)]
pub struct SpicedVersion(String);
impl SpicedVersion {
    #[must_use]
    pub fn new(version: String) -> Self {
        Self(version)
    }
}

impl Display for SpicedVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub enum SpicedInstance {
    /// Connect to an existing local spiced instance at default ports
    Existing,
    /// Connect to an external spiced instance at custom URLs
    External {
        flight_url: String,
        http_base_url: String,
        api_key: Option<String>,
    },
    Owned {
        child: Child,
        tempdir: TempDir,
        version: SpicedVersion,
    },
}

pub struct StartRequest {
    spiced_path: PathBuf,
    spicepod: SpicepodDefinition,
    tempdir: TempDir,
    data_dir: Option<PathBuf>,
    additional_args: Vec<String>,
    prepared: bool,
}

impl StartRequest {
    pub fn new(spiced_path: PathBuf, spicepod: SpicepodDefinition) -> Result<Self> {
        Ok(Self {
            spiced_path,
            spicepod,
            tempdir: TempDir::new()?,
            prepared: false,
            data_dir: None,
            additional_args: Vec::new(),
        })
    }

    #[must_use]
    pub fn with_data_dir(mut self, data_dir: PathBuf) -> Self {
        self.data_dir = Some(data_dir);
        self
    }

    #[must_use]
    pub fn with_additional_args(mut self, args: Vec<String>) -> Self {
        self.additional_args = args;
        self
    }

    #[must_use]
    pub fn get_tempdir_path(&self) -> PathBuf {
        self.tempdir.path().to_path_buf()
    }

    pub fn prepare(&mut self) -> Result<()> {
        // Serialize spicepod to `spicepod.yaml` in the tempdir
        let spicepod_yaml = yaml::to_string(&self.spicepod)?;
        let spicepod_yaml_path = self.tempdir.path().join("spicepod.yaml");
        std::fs::write(spicepod_yaml_path, spicepod_yaml)?;

        // Create a symlink to the data directory if one is set
        if let Some(data_dir) = &self.data_dir {
            // resolve the data directory path to an absolute path
            let data_dir = data_dir.canonicalize()?;

            let data_dir_symlink = self.tempdir.path().join("data");
            #[cfg(not(target_os = "windows"))]
            {
                std::os::unix::fs::symlink(data_dir, data_dir_symlink)?;
            }
            #[cfg(target_os = "windows")]
            {
                std::os::windows::fs::symlink_dir(data_dir, data_dir_symlink)?;
            }
        }

        self.prepared = true;

        Ok(())
    }
}

impl SpicedInstance {
    #[must_use]
    pub fn empty() -> Self {
        Self::Existing
    }

    /// Create an instance that connects to an external spiced at the given Flight URL.
    ///
    /// The HTTP base URL is derived from the Flight URL by replacing the port with 8090,
    /// or can be explicitly provided.
    #[must_use]
    pub fn external(flight_url: impl Into<String>) -> Self {
        let flight_url = flight_url.into();

        let http_base_url = derive_http_base_url(&flight_url);

        Self::External {
            flight_url,
            http_base_url,
            api_key: None,
        }
    }

    /// Create an instance with explicit Flight and HTTP URLs.
    #[must_use]
    pub fn external_with_http(
        flight_url: impl Into<String>,
        http_base_url: impl Into<String>,
    ) -> Self {
        Self::External {
            flight_url: flight_url.into(),
            http_base_url: http_base_url.into(),
            api_key: None,
        }
    }

    /// Set the API key for an external instance.
    #[must_use]
    pub fn with_api_key(mut self, api_key: Option<String>) -> Self {
        if let Self::External {
            api_key: ref mut key,
            ..
        } = self
        {
            *key = api_key;
        }
        self
    }

    /// Start a spiced instance
    ///
    /// # Errors
    ///
    /// - If spiced is already running
    /// - If the spiced instance fails to start
    /// - If the spicepod definition fails to serialize
    pub async fn start(mut start_request: StartRequest) -> Result<Self> {
        // Check if spiced is already running
        let spiced_path_str = start_request.spiced_path.to_string_lossy().to_string();
        if spiced_path_str.starts_with("http://") || spiced_path_str.starts_with("https://") {
            return Ok(Self::external(spiced_path_str));
        }

        let client = reqwest::Client::new();
        let health_url = format!("{HTTP_BASE_URL}{HEALTH_ENDPOINT}");
        let response = client.get(&health_url).send().await;
        if response.is_ok() {
            anyhow::bail!("Spiced instance is already running");
        }

        if !start_request.prepared {
            start_request.prepare()?;
        }

        let tempdir = start_request.tempdir;

        // Get spiced version
        let version_cmd = Command::new(start_request.spiced_path.clone())
            .arg("--version")
            .output()?;

        if !version_cmd.status.success() {
            anyhow::bail!(
                "Failed to get spiced version: {}",
                String::from_utf8_lossy(&version_cmd.stderr)
            );
        }

        let version = String::from_utf8_lossy(&version_cmd.stdout).to_string();
        // take just the v1.0.0 part of the version
        let version = match (version.contains('-'), version.contains('+')) {
            (true, _) => version.split('-').next().unwrap_or(&version).to_string(),
            (false, true) => version.split('+').next().unwrap_or(&version).to_string(),
            (false, false) => version,
        };

        // Start the spiced instance
        let mut cmd = Command::new(start_request.spiced_path);
        cmd.current_dir(tempdir.path());
        cmd.arg("--telemetry-enabled=false");

        // Optionally expose the Prometheus `/metrics` endpoint on the spawned
        // spiced. Off by default so concurrent spiced instances in the test
        // suite don't contend a fixed port; opt in by exporting
        // `SPICED_METRICS_ADDR` (e.g. `0.0.0.0:9090`) for profiling/benchmark
        // runs that scrape per-phase metrics. Without it the spawned spiced runs
        // with a no-op meter provider and `/metrics` is unavailable — the blind
        // spot that hid the CDC write-phase breakdown on the co-located
        // (testoperator-spawns-spiced) benchmark topology.
        if let Ok(metrics_addr) = std::env::var("SPICED_METRICS_ADDR")
            && !metrics_addr.is_empty()
        {
            cmd.arg("--metrics").arg(metrics_addr);
        }

        // Add any additional arguments
        for arg in start_request.additional_args {
            cmd.arg(arg);
        }

        let child = cmd.spawn()?;

        Ok(Self::Owned {
            child,
            tempdir,
            version: SpicedVersion::new(version),
        })
    }

    #[must_use]
    pub fn version(&self) -> &str {
        let Self::Owned { version, .. } = self else {
            return "unknown";
        };

        version.0.as_str()
    }

    pub fn get_tempdir_path(&self) -> Result<PathBuf> {
        let Self::Owned { tempdir, .. } = self else {
            anyhow::bail!("SpicedInstance is not owned, no tempdir available");
        };

        Ok(tempdir.path().to_path_buf())
    }

    /// Get a spice client for the spiced instance
    ///
    /// # Errors
    ///
    /// - If the spice client fails to be created
    pub async fn spice_client(
        &self,
        api_key: Option<String>,
        disable_caching: bool,
    ) -> Result<SpiceClient> {
        let mut spice_client = ClientBuilder::new();

        // Caller-supplied key wins; otherwise fall back to whatever was stashed
        // on the External variant (e.g. by the system-adapter setup response).
        let effective_key = api_key.or_else(|| match self {
            Self::External {
                api_key: Some(key), ..
            } => Some(key.clone()),
            _ => None,
        });
        if let Some(key) = effective_key {
            spice_client = spice_client.api_key(key.as_str());
        }

        if disable_caching {
            spice_client = spice_client.cache_control("no-cache");
        }

        let flight_url = match self {
            Self::External { flight_url, .. } => flight_url.as_str(),
            Self::Existing | Self::Owned { .. } => FLIGHT_URL,
        };

        let spice_client = spice_client
            .flight_url(flight_url)
            .user_agent("spice-test-framework/1.0")
            .build()
            .await
            .map_err(|e| anyhow!("{e}"))?;

        Ok(spice_client)
    }

    /// Build a low-level Flight client for this instance.
    ///
    /// Used for Flight SQL metadata calls the higher-level [`spice_client`] does
    /// not surface — notably `GetSchema`, which returns a dataset's Arrow schema
    /// without scanning any rows.
    ///
    /// [`spice_client`]: Self::spice_client
    ///
    /// # Errors
    ///
    /// - If the Flight client cannot connect to the instance.
    pub async fn flight_client(&self, api_key: Option<String>) -> Result<FlightClient> {
        // Caller-supplied key wins; otherwise fall back to whatever was stashed
        // on the External variant (matching `spice_client`).
        let effective_key = api_key.or_else(|| match self {
            Self::External {
                api_key: Some(key), ..
            } => Some(key.clone()),
            _ => None,
        });

        let credentials = match effective_key {
            Some(key) => Credentials::new("", SecretString::new(key.into())),
            None => Credentials::anonymous(),
        };

        let flight_url = match self {
            Self::External { flight_url, .. } => flight_url.as_str(),
            Self::Existing | Self::Owned { .. } => FLIGHT_URL,
        };

        FlightClient::try_new(Arc::from(flight_url), credentials, None, None)
            .await
            .map_err(|e| anyhow!("{e}"))
    }

    /// Get an http client for the spiced instance
    ///
    /// # Errors
    ///
    /// - If the http client fails to be created
    pub fn http_client(&self) -> Result<reqwest::Client> {
        let mut builder = reqwest::Client::builder().user_agent("spice-test-framework/1.0");

        if let Self::External {
            api_key: Some(key), ..
        } = self
        {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                "X-API-Key",
                reqwest::header::HeaderValue::from_str(key)
                    .map_err(|e| anyhow!("Invalid API key header value: {e}"))?,
            );
            builder = builder.default_headers(headers);
        }

        Ok(builder.build()?)
    }

    /// Get the HTTP base URL for this instance
    #[must_use]
    pub fn http_base_url(&self) -> &str {
        match self {
            Self::External { http_base_url, .. } => http_base_url.as_str(),
            Self::Existing | Self::Owned { .. } => HTTP_BASE_URL,
        }
    }

    /// Wait for the spiced instance to be ready
    ///
    /// # Errors
    ///
    /// - If the spiced instance fails to be ready within the timeout
    pub async fn wait_for_ready(&mut self, timeout: Duration) -> Result<()> {
        // Wait for the spiced instance to be ready by polling the `/v1/ready` endpoint
        let client = self.http_client()?;
        let http_base = self.http_base_url().to_string();
        let ready_url = format!("{http_base}{READY_ENDPOINT}");
        if !wait_until_true(timeout, || async {
            let response = client.get(&ready_url).send().await;
            match response {
                Ok(response) => response.status().is_success(),
                Err(_) => false,
            }
        })
        .await
        {
            anyhow::bail!("Spiced instance not ready within {timeout:?}");
        }

        // Give Flight server a moment to finish starting up after HTTP is ready
        // Flight starts asynchronously and may not be available immediately
        tokio::time::sleep(Duration::from_millis(500)).await;

        Ok(())
    }

    pub async fn is_ready(&self) -> bool {
        let Ok(client) = self.http_client() else {
            return false;
        };
        let ready_url = format!("{}{READY_ENDPOINT}", self.http_base_url());
        let response = client.get(&ready_url).send().await;
        match response {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Stop the spiced instance
    ///
    /// # Errors
    ///
    /// - If the spiced instance fails to exit
    pub fn stop(&mut self) -> Result<()> {
        let Self::Owned { child, .. } = self else {
            return Ok(());
        };

        #[cfg(not(target_os = "windows"))]
        {
            // Send a SIGTERM to the spiced instance and wait for it to exit
            let Ok(pid_i32) = child.id().try_into() else {
                anyhow::bail!("Failed to convert pid to i32");
            };
            nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid_i32),
                nix::sys::signal::Signal::SIGTERM,
            )?;
            child.wait()?;
        }

        #[cfg(target_os = "windows")]
        {
            // On Windows, we can use the built-in process termination
            child.kill()?;
            child.wait()?;
        }

        Ok(())
    }

    /// Returns a `Process` handle when this instance owns a local spiced subprocess.
    #[must_use]
    pub fn process(&self) -> Option<Process> {
        let Self::Owned { child, .. } = self else {
            return None;
        };

        Some(Process::new(Pid::from_u32(child.id())))
    }
}

fn derive_http_base_url(flight_url: &str) -> String {
    if flight_url.contains("flight.spiceai.io") {
        return "https://data.spiceai.io".to_string();
    }

    let http_flight_url = flight_url
        .strip_prefix("grpc://")
        .map(|rest| format!("http://{rest}"))
        .or_else(|| {
            flight_url
                .strip_prefix("grpc+tls://")
                .map(|rest| format!("https://{rest}"))
        });
    let parse_target = http_flight_url.as_deref().unwrap_or(flight_url);
    let Ok(mut url) = reqwest::Url::parse(parse_target) else {
        return format!("{flight_url}:8090");
    };

    if url.set_port(Some(8090)).is_err() {
        return format!("{flight_url}:8090");
    }
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
    url.as_str().trim_end_matches('/').to_string()
}

impl Drop for SpicedInstance {
    fn drop(&mut self) {
        let Self::Owned { child, .. } = self else {
            return;
        };

        match child.kill() {
            Ok(()) => (),
            Err(e) => eprintln!("Failed to kill spiced instance: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_derives_http_base_url_without_flight_port() {
        let instance = SpicedInstance::external("https://example.com");

        assert_eq!(instance.http_base_url(), "https://example.com:8090");
    }

    #[test]
    fn external_derives_http_base_url_from_flight_port() {
        let instance = SpicedInstance::external("http://localhost:50051");

        assert_eq!(instance.http_base_url(), "http://localhost:8090");
    }

    #[test]
    fn external_maps_grpc_flight_scheme_to_http() {
        let instance = SpicedInstance::external("grpc://localhost:50051");

        assert_eq!(instance.http_base_url(), "http://localhost:8090");
    }

    #[test]
    fn external_maps_grpc_tls_flight_scheme_to_https() {
        let instance = SpicedInstance::external("grpc+tls://localhost:50051");

        assert_eq!(instance.http_base_url(), "https://localhost:8090");
    }

    #[test]
    fn external_derives_http_base_url_for_ipv6() {
        let instance = SpicedInstance::external("http://[::1]:50051");

        assert_eq!(instance.http_base_url(), "http://[::1]:8090");
    }
}
