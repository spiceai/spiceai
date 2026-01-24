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

    /// TLS root certificate file path
    tls_root_certificate_file: Option<String>,
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
            tls_root_certificate_file: None,
        })
    }

    /// Create a runtime context from CLI arguments.
    pub fn with_args(
        http_endpoint: Option<String>,
        api_key: Option<String>,
        is_cloud: bool,
        tls_root_certificate_file: Option<String>,
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
        ctx.tls_root_certificate_file = tls_root_certificate_file;

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
    /// # Arguments
    /// * `args` - Additional arguments to pass to spiced
    /// * `http_endpoint_override` - Optional HTTP endpoint override for binding (from run command)
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime is not installed.
    pub fn get_run_cmd(
        &self,
        args: &[String],
        http_endpoint_override: Option<&str>,
    ) -> Result<Command> {
        if !self.is_runtime_installed() {
            return Err(RuntimeNotInstalledSnafu.build());
        }

        let mut cmd = Command::new(self.spiced_path());
        cmd.arg("--pods-watcher-enabled");
        cmd.args(args);

        // Add HTTP endpoint (use override if provided, otherwise use context default)
        cmd.arg("--http");
        let http_addr = http_endpoint_override.map_or_else(
            || self.http_socket_address(),
            |ep| {
                ep.trim_start_matches("http://")
                    .trim_start_matches("https://")
                    .to_string()
            },
        );
        cmd.arg(http_addr);

        // Add API key if present
        if let Some(api_key) = &self.api_key {
            cmd.arg("--api-key");
            cmd.arg(api_key);
        }

        // Add TLS root certificate file if present
        if let Some(tls_cert) = &self.tls_root_certificate_file {
            cmd.arg("--tls-root-certificate-file");
            cmd.arg(tls_cert);
        }

        // Add user agent
        cmd.arg("--user-agent");
        cmd.arg(&self.user_agent);

        // Set default captured output for task history (for spice trace)
        cmd.arg("--set-runtime");
        cmd.arg("task_history.captured_output=truncated");

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Helper to create a `RuntimeContext` with a mocked spiced binary for testing.
    fn create_test_context() -> RuntimeContext {
        RuntimeContext {
            spice_runtime_dir: PathBuf::from("/test/.spice"),
            spice_bin_dir: PathBuf::from("/test/.spice/bin"),
            app_dir: PathBuf::from("/test/app"),
            pods_dir: PathBuf::from("/test/app/spicepods"),
            http_endpoint: "http://127.0.0.1:8090".to_string(),
            api_key: None,
            is_cloud: false,
            user_agent: "spice/test (test; test)".to_string(),
            extra_headers: HashMap::new(),
            http_client: reqwest::Client::new(),
            tls_root_certificate_file: None,
        }
    }

    /// Create a test context with a mocked spiced binary in an isolated temp directory.
    /// Returns the context and the `TempDir` (which must be kept alive for the test).
    fn create_test_context_with_runtime() -> (RuntimeContext, TempDir) {
        let temp_dir = TempDir::new().expect("create temp dir");
        let bin_dir = temp_dir.path().join("bin");
        std::fs::create_dir_all(&bin_dir).expect("create bin dir");
        let spiced_path = bin_dir.join(SPICED_FILENAME);
        std::fs::write(&spiced_path, "mock").expect("create mock spiced");

        let ctx = RuntimeContext {
            spice_runtime_dir: temp_dir.path().to_path_buf(),
            spice_bin_dir: bin_dir,
            app_dir: PathBuf::from("/test/app"),
            pods_dir: PathBuf::from("/test/app/spicepods"),
            http_endpoint: "http://127.0.0.1:8090".to_string(),
            api_key: None,
            is_cloud: false,
            user_agent: "spice/test (test; test)".to_string(),
            extra_headers: HashMap::new(),
            http_client: reqwest::Client::new(),
            tls_root_certificate_file: None,
        };

        (ctx, temp_dir)
    }

    /// Convert Command args to a Vec<String> for testing.
    /// This extracts the arguments that would be passed to spiced.
    fn get_cmd_args(cmd: &Command) -> Vec<String> {
        cmd.get_args()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect()
    }

    #[test]
    fn test_get_run_cmd_includes_pods_watcher_enabled() {
        let (ctx, _temp_dir) = create_test_context_with_runtime();

        let cmd = ctx
            .get_run_cmd(&[], None)
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);

        assert!(
            args.contains(&"--pods-watcher-enabled".to_string()),
            "Should include --pods-watcher-enabled, got: {args:?}"
        );
    }

    #[test]
    fn test_get_run_cmd_includes_http_endpoint() {
        let (ctx, _temp_dir) = create_test_context_with_runtime();

        let cmd = ctx
            .get_run_cmd(&[], None)
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);

        assert!(
            args.contains(&"--http".to_string()),
            "Should include --http flag, got: {args:?}"
        );
        assert!(
            args.contains(&"127.0.0.1:8090".to_string()),
            "Should include HTTP socket address, got: {args:?}"
        );
    }

    #[test]
    fn test_get_run_cmd_uses_http_endpoint_override() {
        let (ctx, _temp_dir) = create_test_context_with_runtime();

        let cmd = ctx
            .get_run_cmd(&[], Some("http://0.0.0.0:9999"))
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);

        assert!(
            args.contains(&"--http".to_string()),
            "Should include --http flag, got: {args:?}"
        );
        assert!(
            args.contains(&"0.0.0.0:9999".to_string()),
            "Should use override endpoint, got: {args:?}"
        );
        assert!(
            !args.contains(&"127.0.0.1:8090".to_string()),
            "Should NOT include default endpoint when override is set, got: {args:?}"
        );
    }

    #[test]
    fn test_get_run_cmd_http_override_strips_prefix() {
        let (ctx, _temp_dir) = create_test_context_with_runtime();

        // Test with http:// prefix
        let cmd = ctx
            .get_run_cmd(&[], Some("http://192.168.1.1:8080"))
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);
        assert!(
            args.contains(&"192.168.1.1:8080".to_string()),
            "Should strip http:// prefix, got: {args:?}"
        );

        // Test with https:// prefix
        let cmd = ctx
            .get_run_cmd(&[], Some("https://secure.example.com:443"))
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);
        assert!(
            args.contains(&"secure.example.com:443".to_string()),
            "Should strip https:// prefix, got: {args:?}"
        );
    }

    #[test]
    fn test_get_run_cmd_includes_api_key_when_set() {
        let (mut ctx, _temp_dir) = create_test_context_with_runtime();
        ctx.api_key = Some("test-api-key-12345".to_string());

        let cmd = ctx
            .get_run_cmd(&[], None)
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);

        assert!(
            args.contains(&"--api-key".to_string()),
            "Should include --api-key flag, got: {args:?}"
        );
        assert!(
            args.contains(&"test-api-key-12345".to_string()),
            "Should include the API key value, got: {args:?}"
        );
    }

    #[test]
    fn test_get_run_cmd_excludes_api_key_when_not_set() {
        let (ctx, _temp_dir) = create_test_context_with_runtime();

        let cmd = ctx
            .get_run_cmd(&[], None)
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);

        assert!(
            !args.contains(&"--api-key".to_string()),
            "Should NOT include --api-key flag when not set, got: {args:?}"
        );
    }

    #[test]
    fn test_get_run_cmd_includes_tls_certificate_when_set() {
        let (mut ctx, _temp_dir) = create_test_context_with_runtime();
        ctx.tls_root_certificate_file = Some("/path/to/cert.pem".to_string());

        let cmd = ctx
            .get_run_cmd(&[], None)
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);

        assert!(
            args.contains(&"--tls-root-certificate-file".to_string()),
            "Should include --tls-root-certificate-file flag, got: {args:?}"
        );
        assert!(
            args.contains(&"/path/to/cert.pem".to_string()),
            "Should include the TLS certificate path, got: {args:?}"
        );
    }

    #[test]
    fn test_get_run_cmd_excludes_tls_certificate_when_not_set() {
        let (ctx, _temp_dir) = create_test_context_with_runtime();

        let cmd = ctx
            .get_run_cmd(&[], None)
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);

        assert!(
            !args.contains(&"--tls-root-certificate-file".to_string()),
            "Should NOT include --tls-root-certificate-file flag when not set, got: {args:?}"
        );
    }

    #[test]
    fn test_get_run_cmd_includes_user_agent() {
        let (mut ctx, _temp_dir) = create_test_context_with_runtime();
        ctx.user_agent = "spice/1.0.0 (macos; arm64)".to_string();

        let cmd = ctx
            .get_run_cmd(&[], None)
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);

        assert!(
            args.contains(&"--user-agent".to_string()),
            "Should include --user-agent flag, got: {args:?}"
        );
        assert!(
            args.contains(&"spice/1.0.0 (macos; arm64)".to_string()),
            "Should include the user agent value, got: {args:?}"
        );
    }

    #[test]
    fn test_get_run_cmd_includes_captured_output_setting() {
        let (ctx, _temp_dir) = create_test_context_with_runtime();

        let cmd = ctx
            .get_run_cmd(&[], None)
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);

        assert!(
            args.contains(&"--set-runtime".to_string()),
            "Should include --set-runtime flag, got: {args:?}"
        );
        assert!(
            args.contains(&"task_history.captured_output=truncated".to_string()),
            "Should include task_history.captured_output=truncated, got: {args:?}"
        );
    }

    #[test]
    fn test_get_run_cmd_passes_through_extra_args() {
        let (ctx, _temp_dir) = create_test_context_with_runtime();

        let extra_args = vec![
            "-v".to_string(),
            "--flight".to_string(),
            "0.0.0.0:50051".to_string(),
        ];
        let cmd = ctx
            .get_run_cmd(&extra_args, None)
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);

        assert!(
            args.contains(&"-v".to_string()),
            "Should include -v flag from extra args, got: {args:?}"
        );
        assert!(
            args.contains(&"--flight".to_string()),
            "Should include --flight from extra args, got: {args:?}"
        );
        assert!(
            args.contains(&"0.0.0.0:50051".to_string()),
            "Should include flight endpoint from extra args, got: {args:?}"
        );
    }

    #[test]
    fn test_get_run_cmd_full_argument_order() {
        let (mut ctx, _temp_dir) = create_test_context_with_runtime();
        ctx.api_key = Some("my-api-key".to_string());
        ctx.tls_root_certificate_file = Some("/cert.pem".to_string());
        ctx.user_agent = "test-agent".to_string();
        ctx.http_endpoint = "http://localhost:9090".to_string();

        let extra_args = vec!["-vv".to_string()];
        let cmd = ctx
            .get_run_cmd(&extra_args, None)
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);

        // Verify all expected arguments are present
        let expected = [
            "--pods-watcher-enabled",
            "-vv",
            "--http",
            "localhost:9090",
            "--api-key",
            "my-api-key",
            "--tls-root-certificate-file",
            "/cert.pem",
            "--user-agent",
            "test-agent",
            "--set-runtime",
            "task_history.captured_output=truncated",
        ];

        for expected_arg in expected {
            assert!(
                args.contains(&expected_arg.to_string()),
                "Should include '{expected_arg}', got: {args:?}"
            );
        }
    }

    #[test]
    fn test_get_run_cmd_fails_when_runtime_not_installed() {
        let ctx = create_test_context();
        // spice_bin_dir points to /test/.spice/bin which doesn't exist
        let result = ctx.get_run_cmd(&[], None);

        assert!(result.is_err(), "Should fail when runtime not installed");
    }

    #[test]
    fn test_http_socket_address_strips_http_prefix() {
        let mut ctx = create_test_context();
        ctx.http_endpoint = "http://127.0.0.1:8090".to_string();
        assert_eq!(ctx.http_socket_address(), "127.0.0.1:8090");
    }

    #[test]
    fn test_http_socket_address_strips_https_prefix() {
        let mut ctx = create_test_context();
        ctx.http_endpoint = "https://secure.example.com:443".to_string();
        assert_eq!(ctx.http_socket_address(), "secure.example.com:443");
    }

    #[test]
    fn test_http_socket_address_no_prefix() {
        let mut ctx = create_test_context();
        ctx.http_endpoint = "127.0.0.1:8090".to_string();
        assert_eq!(ctx.http_socket_address(), "127.0.0.1:8090");
    }

    #[test]
    fn test_with_args_sets_http_endpoint() {
        let ctx =
            RuntimeContext::with_args(Some("http://custom:9999".to_string()), None, false, None)
                .expect("with_args should succeed");

        assert_eq!(ctx.http_endpoint(), "http://custom:9999");
    }

    #[test]
    fn test_with_args_sets_api_key() {
        let ctx = RuntimeContext::with_args(None, Some("test-key".to_string()), false, None)
            .expect("with_args should succeed");

        assert_eq!(ctx.api_key(), Some("test-key"));
    }

    #[test]
    fn test_with_args_sets_cloud_mode() {
        let ctx =
            RuntimeContext::with_args(None, None, true, None).expect("with_args should succeed");

        assert!(ctx.is_cloud());
        assert_eq!(ctx.http_endpoint(), "https://data.spiceai.io");
    }

    #[test]
    fn test_with_args_sets_tls_certificate() {
        let ctx =
            RuntimeContext::with_args(None, None, false, Some("/path/to/cert.pem".to_string()))
                .expect("with_args should succeed");

        assert_eq!(
            ctx.tls_root_certificate_file,
            Some("/path/to/cert.pem".to_string())
        );
    }

    #[test]
    fn test_default_user_agent_format() {
        let user_agent = RuntimeContext::default_user_agent();
        assert!(
            user_agent.starts_with("spice/"),
            "User agent should start with spice/, got: {user_agent}"
        );
        assert!(
            user_agent.contains('('),
            "User agent should contain OS/arch info, got: {user_agent}"
        );
    }

    #[test]
    fn test_get_headers_includes_user_agent() {
        let ctx = create_test_context();
        let headers = ctx.get_headers();

        assert!(
            headers.contains_key("User-Agent"),
            "Headers should include User-Agent"
        );
    }

    #[test]
    fn test_get_headers_includes_api_key_when_set() {
        let mut ctx = create_test_context();
        ctx.api_key = Some("my-api-key".to_string());
        let headers = ctx.get_headers();

        assert_eq!(
            headers.get("X-API-Key"),
            Some(&"my-api-key".to_string()),
            "Headers should include X-API-Key"
        );
    }

    #[test]
    fn test_get_headers_excludes_api_key_when_not_set() {
        let ctx = create_test_context();
        let headers = ctx.get_headers();

        assert!(
            !headers.contains_key("X-API-Key"),
            "Headers should NOT include X-API-Key when not set"
        );
    }

    #[test]
    fn test_add_headers() {
        let mut ctx = create_test_context();
        let mut extra = HashMap::new();
        extra.insert("X-Custom-Header".to_string(), "custom-value".to_string());
        ctx.add_headers(extra);

        let headers = ctx.get_headers();
        assert_eq!(
            headers.get("X-Custom-Header"),
            Some(&"custom-value".to_string())
        );
    }

    // ========================================================================
    // Local vs Remote (Cloud) Mode Tests
    // ========================================================================

    #[test]
    fn test_local_mode_default_endpoint() {
        // Local mode should use default localhost endpoint
        let ctx = RuntimeContext::new().expect("new should succeed");

        assert!(!ctx.is_cloud());
        assert_eq!(ctx.http_endpoint(), "http://127.0.0.1:8090");
    }

    #[test]
    fn test_local_mode_custom_endpoint() {
        // Local mode with custom endpoint
        let ctx = RuntimeContext::with_args(
            Some("http://192.168.1.100:8090".to_string()),
            None,
            false,
            None,
        )
        .expect("with_args should succeed");

        assert!(!ctx.is_cloud());
        assert_eq!(ctx.http_endpoint(), "http://192.168.1.100:8090");
    }

    #[test]
    fn test_cloud_mode_overrides_endpoint() {
        // Cloud mode should override any custom endpoint with cloud URL
        let ctx = RuntimeContext::with_args(
            Some("http://custom:9999".to_string()), // This should be ignored
            None,
            true, // Cloud mode enabled
            None,
        )
        .expect("with_args should succeed");

        assert!(ctx.is_cloud());
        assert_eq!(ctx.http_endpoint(), "https://data.spiceai.io");
    }

    #[test]
    fn test_cloud_mode_with_api_key() {
        // Cloud mode with API key
        let ctx =
            RuntimeContext::with_args(None, Some("cloud-api-key-12345".to_string()), true, None)
                .expect("with_args should succeed");

        assert!(ctx.is_cloud());
        assert_eq!(ctx.http_endpoint(), "https://data.spiceai.io");
        assert_eq!(ctx.api_key(), Some("cloud-api-key-12345"));
    }

    #[test]
    fn test_local_mode_with_api_key() {
        // Local mode can also have an API key (for local runtime auth)
        let ctx = RuntimeContext::with_args(
            Some("http://localhost:8090".to_string()),
            Some("local-api-key".to_string()),
            false,
            None,
        )
        .expect("with_args should succeed");

        assert!(!ctx.is_cloud());
        assert_eq!(ctx.http_endpoint(), "http://localhost:8090");
        assert_eq!(ctx.api_key(), Some("local-api-key"));
    }

    #[test]
    fn test_cloud_mode_uses_https() {
        let ctx =
            RuntimeContext::with_args(None, None, true, None).expect("with_args should succeed");

        assert!(
            ctx.http_endpoint().starts_with("https://"),
            "Cloud mode should use HTTPS, got: {}",
            ctx.http_endpoint()
        );
    }

    #[test]
    fn test_local_mode_socket_address() {
        let ctx =
            RuntimeContext::with_args(None, None, false, None).expect("with_args should succeed");

        // Local mode socket address should not have scheme prefix
        assert_eq!(ctx.http_socket_address(), "127.0.0.1:8090");
    }

    #[test]
    fn test_cloud_mode_socket_address() {
        let ctx =
            RuntimeContext::with_args(None, None, true, None).expect("with_args should succeed");

        // Cloud mode socket address should strip https://
        assert_eq!(ctx.http_socket_address(), "data.spiceai.io");
    }

    #[test]
    fn test_mode_reflected_in_headers() {
        // Both local and cloud modes should include user agent
        let local_ctx =
            RuntimeContext::with_args(None, None, false, None).expect("with_args should succeed");
        let cloud_ctx =
            RuntimeContext::with_args(None, None, true, None).expect("with_args should succeed");

        let local_headers = local_ctx.get_headers();
        let cloud_headers = cloud_ctx.get_headers();

        assert!(
            local_headers.contains_key("User-Agent"),
            "Local mode should include User-Agent"
        );
        assert!(
            cloud_headers.contains_key("User-Agent"),
            "Cloud mode should include User-Agent"
        );
    }
}
