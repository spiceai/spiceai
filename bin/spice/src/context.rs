/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Runtime context for managing Spice runtime installation and configuration.

use crate::error::{
    CreateDirectorySnafu, HomeDirectoryNotFoundSnafu, Result, RuntimeExecutionSnafu,
    RuntimeNotInstalledSnafu, RuntimeVersionSnafu,
};
use snafu::ResultExt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

/// Constants for Spice paths and filenames
const DOT_SPICE: &str = ".spice";
const SPICED_FILENAME: &str = "spiced";
const SPICEPODS_DIR: &str = "spicepods";

/// Runtime context holding paths and configuration for CLI operations.
#[derive(Debug, Clone)]
pub struct RuntimeContext {
    /// Path to ~/.spice directory
    spice_runtime_dir: PathBuf,

    /// Path to ~/.spice/bin directory
    spice_bin_dir: PathBuf,

    /// Current working directory (app directory)
    app_dir: PathBuf,

    /// Path to spicepods directory in app (used by install/init commands)
    pods_dir: PathBuf,

    /// HTTP endpoint for runtime API
    http_endpoint: String,

    /// API key for authentication
    api_key: Option<String>,

    /// Whether to use cloud mode
    is_cloud: bool,

    /// User agent string for HTTP requests
    user_agent: String,

    /// Extra headers for HTTP requests
    extra_headers: HashMap<String, String>,

    /// HTTP client with default timeout
    http_client: reqwest::Client,
}

impl RuntimeContext {
    /// Create a new runtime context with default settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the home directory cannot be determined.
    pub fn new() -> Result<Self> {
        let home_dir = dirs::home_dir().ok_or_else(|| HomeDirectoryNotFoundSnafu.build())?;
        let spice_runtime_dir = home_dir.join(DOT_SPICE);
        let spice_bin_dir = spice_runtime_dir.join("bin");

        let app_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let pods_dir = app_dir.join(SPICEPODS_DIR);

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();

        Ok(Self {
            spice_runtime_dir,
            spice_bin_dir,
            app_dir,
            pods_dir,
            http_endpoint: "http://127.0.0.1:8090".to_string(),
            api_key: None,
            is_cloud: false,
            user_agent: Self::default_user_agent(),
            extra_headers: HashMap::new(),
            http_client,
        })
    }

    /// Create a runtime context from CLI arguments.
    pub fn with_args(
        http_endpoint: Option<String>,
        api_key: Option<String>,
        is_cloud: bool,
    ) -> Result<Self> {
        let mut ctx = Self::new()?;

        if let Some(endpoint) = http_endpoint {
            ctx.http_endpoint = endpoint;
        }

        if is_cloud {
            ctx.http_endpoint = "https://data.spiceai.io".to_string();
            ctx.is_cloud = true;
        }

        ctx.api_key = api_key;

        // Load API key from .env if not provided
        if ctx.api_key.is_none() {
            ctx.api_key = ctx.load_api_key_from_env();
        }

        Ok(ctx)
    }

    /// Generate the default user agent string.
    fn default_user_agent() -> String {
        format!(
            "spice/{} ({}; {})",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    }

    /// Load API key from .env or .env.local file.
    fn load_api_key_from_env(&self) -> Option<String> {
        // Try .env.local first, then .env
        let env_files = [".env.local", ".env"];

        for env_file in &env_files {
            let path = self.app_dir.join(env_file);
            if path.exists()
                && let Ok(env_map) = dotenvy::from_path_iter(&path)
            {
                for item in env_map.flatten() {
                    if item.0 == "SPICE_SPICEAI_API_KEY" || item.0 == "SPICE_API_KEY" {
                        return Some(item.1);
                    }
                }
            }
        }

        // Also check environment variables
        std::env::var("SPICE_API_KEY")
            .or_else(|_| std::env::var("SPICE_SPICEAI_API_KEY"))
            .ok()
    }

    /// Get the Spice runtime directory (~/.spice).
    #[must_use]
    pub fn spice_runtime_dir(&self) -> &PathBuf {
        &self.spice_runtime_dir
    }

    /// Get the Spice bin directory (~/.spice/bin).
    #[must_use]
    pub fn spice_bin_dir(&self) -> &PathBuf {
        &self.spice_bin_dir
    }

    /// Get the current app directory.
    #[must_use]
    pub fn app_dir(&self) -> &PathBuf {
        &self.app_dir
    }

    /// Get the spicepods directory.
    #[must_use]
    pub fn pods_dir(&self) -> &PathBuf {
        &self.pods_dir
    }

    /// Add extra headers to HTTP requests.
    pub fn add_headers(&mut self, headers: HashMap<String, String>) {
        self.extra_headers.extend(headers);
    }

    /// Get the HTTP endpoint.
    #[must_use]
    pub fn http_endpoint(&self) -> &str {
        &self.http_endpoint
    }

    /// Get the API key if set.
    #[must_use]
    pub fn api_key(&self) -> Option<&str> {
        self.api_key.as_deref()
    }

    /// Check if cloud mode is enabled.
    #[must_use]
    pub fn is_cloud(&self) -> bool {
        self.is_cloud
    }

    /// Get the HTTP client.
    #[must_use]
    pub fn http_client(&self) -> &reqwest::Client {
        &self.http_client
    }

    /// Get the user agent string.
    #[must_use]
    pub fn user_agent(&self) -> &str {
        &self.user_agent
    }

    /// Get the path to the spiced binary.
    #[must_use]
    pub fn spiced_path(&self) -> PathBuf {
        self.spice_bin_dir.join(SPICED_FILENAME)
    }

    /// Check if the runtime is installed.
    #[must_use]
    pub fn is_runtime_installed(&self) -> bool {
        self.spiced_path().exists()
    }

    /// Get the installed runtime version.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime is not installed or version cannot be determined.
    pub fn runtime_version(&self) -> Result<String> {
        if !self.is_runtime_installed() {
            return Err(RuntimeNotInstalledSnafu.build());
        }

        let output = Command::new(self.spiced_path())
            .arg("--version")
            .output()
            .context(RuntimeExecutionSnafu)?;

        if !output.status.success() {
            return Err(RuntimeVersionSnafu {
                message: String::from_utf8_lossy(&output.stderr).to_string(),
            }
            .build());
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// Create a command to run spiced with the given arguments.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime is not installed.
    pub fn get_run_cmd(&self, args: &[String]) -> Result<Command> {
        if !self.is_runtime_installed() {
            return Err(RuntimeNotInstalledSnafu.build());
        }

        let mut cmd = Command::new(self.spiced_path());
        cmd.arg("--pods-watcher-enabled");
        cmd.args(args);

        // Add HTTP endpoint
        cmd.arg("--http");
        cmd.arg(self.http_socket_address());

        // Add API key if present
        if let Some(api_key) = &self.api_key {
            cmd.arg("--api-key");
            cmd.arg(api_key);
        }

        // Add user agent
        cmd.arg("--user-agent");
        cmd.arg(&self.user_agent);

        Ok(cmd)
    }

    /// Get the HTTP socket address (without http:// prefix).
    #[must_use]
    pub fn http_socket_address(&self) -> String {
        self.http_endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .to_string()
    }

    /// Prepare the installation directory, creating it if necessary.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created.
    pub fn prepare_install_dir(&self) -> Result<()> {
        std::fs::create_dir_all(&self.spice_bin_dir).context(CreateDirectorySnafu {
            path: self.spice_bin_dir.clone(),
        })?;

        // Set permissions to 0755 (rwxr-xr-x)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = std::fs::Permissions::from_mode(0o755);
            std::fs::set_permissions(&self.spice_bin_dir, permissions).context(
                CreateDirectorySnafu {
                    path: self.spice_bin_dir.clone(),
                },
            )?;
        }

        Ok(())
    }

    /// Get headers for HTTP requests including API key and user agent.
    #[must_use]
    pub fn get_headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::new();

        if let Some(api_key) = &self.api_key {
            headers.insert("X-API-Key".to_string(), api_key.clone());
        }

        headers.insert("User-Agent".to_string(), self.user_agent.clone());

        for (key, value) in &self.extra_headers {
            headers.insert(key.clone(), value.clone());
        }

        headers
    }

    /// Make an HTTP GET request to the runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn get(&self, path: &str) -> Result<reqwest::Response> {
        let url = format!("{}{}", self.http_endpoint, path);
        let mut request = self.http_client.get(&url);

        for (key, value) in self.get_headers() {
            request = request.header(&key, &value);
        }

        request
            .send()
            .await
            .context(crate::error::ConnectionFailedSnafu { endpoint: url })
    }

    /// Make an HTTP POST request to the runtime with an optional body.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn post(&self, path: &str, body: Option<String>) -> Result<reqwest::Response> {
        let url = format!("{}{}", self.http_endpoint, path);
        let mut request = self.http_client.post(&url);

        for (key, value) in self.get_headers() {
            request = request.header(&key, &value);
        }

        if let Some(body) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(body);
        }

        request
            .send()
            .await
            .context(crate::error::ConnectionFailedSnafu { endpoint: url })
    }

    /// Make an HTTP POST request to the runtime with a JSON body.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn post_json<T: serde::Serialize>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<reqwest::Response> {
        let url = format!("{}{}", self.http_endpoint, path);
        let mut request = self.http_client.post(&url);

        for (key, value) in self.get_headers() {
            request = request.header(&key, &value);
        }

        request = request.json(body);

        request
            .send()
            .await
            .context(crate::error::ConnectionFailedSnafu { endpoint: url })
    }
}
