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
    CreateDirectorySnafu, HomeDirectoryNotFoundSnafu, HttpClientBuildSnafu, Result,
    RuntimeExecutionSnafu, RuntimeNotInstalledSnafu, RuntimeVersionSnafu,
    WindowsNativeRuntimeUnsupportedSnafu,
};
use snafu::ResultExt;
use spice_cloud_client::endpoints::data_endpoint as spice_cloud_data_endpoint;
use spice_cloud_client::redirect::same_origin_redirect_policy;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

/// The runtime HTTP endpoint the CLI talks to when `--http-endpoint` is not given.
///
/// The default lives here rather than on the flag so that not passing the flag is
/// distinguishable from passing this exact value — see [`RuntimeContext::http_endpoint_chosen`].
pub const DEFAULT_HTTP_ENDPOINT: &str = "http://127.0.0.1:8090";

/// Constants for Spice paths and filenames
const DOT_SPICE: &str = ".spice";
const SPICED_FILENAME: &str = "spiced";
const SPICEPODS_DIR: &str = "spicepods";
const WSL_ENV_KEYS: [&str; 2] = ["WSL_DISTRO_NAME", "WSL_INTEROP"];

/// How long a request waits for the connection itself to be established.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The deadline for the control-plane calls — health, dataset listings, the model list,
/// registry downloads. Their duration is a function of the network, so a whole-request
/// deadline is the right shape and a short one is a useful failure signal.
const CONTROL_PLANE_DEADLINE: Deadline = Deadline::Total(Duration::from_secs(30));

/// The deadline for requests whose duration is set by model inference — a chat completion,
/// a text-to-SQL translation, a search that has to embed the query.
///
/// These have no useful upper bound: a long answer, a tool-calling chain, or a large local
/// model on modest hardware all legitimately take minutes, and none of that is a failure.
/// What is a failure is the endpoint going quiet, so the deadline measures silence. It is
/// generous because the first byte can trail the request by a whole prompt evaluation.
pub(crate) const INFERENCE_DEADLINE: Deadline = Deadline::Silence(Duration::from_mins(5));

/// What an HTTP client's deadline measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Deadline {
    /// The whole request, from connecting until the response body has finished. Fires on a
    /// healthy response that is simply long.
    Total(Duration),
    /// The gap between reads, reset by each one — so it fires only when nothing arrives,
    /// whether that is before the response head or between two chunks of the body.
    ///
    /// Note that this counts *bytes*, so on a stream the server keeps alive it never fires;
    /// a caller that needs the gap between meaningful events has to measure that itself.
    Silence(Duration),
}

impl Deadline {
    /// How long this deadline allows, whatever it is measuring.
    pub(crate) const fn duration(self) -> Duration {
        match self {
            Self::Total(duration) | Self::Silence(duration) => duration,
        }
    }
}

/// The user agent a test context reports, so a server under test can tell it apart.
#[cfg(test)]
const TEST_USER_AGENT: &str = "spice/test (test; test)";

/// Build an HTTP client for the CLI.
///
/// Both of the context's clients are built here, so a setting that has to hold for both —
/// the user agent, the connect timeout, and the same-origin redirect policy — belongs on this
/// builder rather than at a call site. Every `/v1/*` call the CLI makes goes through one of
/// these clients, the context helpers and the per-command sites that build their own request
/// from `ctx.http_client()` alike, so the redirect policy is set once here rather than per
/// call site. (`commands::login` deliberately builds its own; it carries a credential in the
/// request body and so refuses cross-origin redirects outright.)
///
/// # Errors
///
/// Returns an error if the client cannot be built. A default client is not substituted,
/// because it would carry neither the deadline nor the same-origin redirect policy, and these
/// clients send the API key in a header.
fn build_http_client(user_agent: String, deadline: Deadline) -> Result<reqwest::Client> {
    let builder = reqwest::Client::builder()
        .user_agent(user_agent)
        .connect_timeout(CONNECT_TIMEOUT)
        .redirect(same_origin_redirect_policy());

    let builder = match deadline {
        Deadline::Total(duration) => builder.timeout(duration),
        Deadline::Silence(duration) => builder.read_timeout(duration),
    };

    builder.build().context(HttpClientBuildSnafu)
}

/// Treat a blank credential as absent.
///
/// A credential that is empty or whitespace-only cannot authenticate anything, so
/// carrying it as `Some` only means an empty header goes out on the wire and any
/// fallback that keys off `is_none` is suppressed. The value is otherwise kept
/// verbatim -- only the all-blank case is discarded.
fn normalize_credential(value: Option<String>) -> Option<String> {
    value.filter(|key| !key.trim().is_empty())
}

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

    /// Whether `http_endpoint` came from `--http-endpoint`, rather than the built-in default
    /// or the cloud region. `spice sql` needs the provenance and not the value: it moves only
    /// the Flight endpoint, so an HTTP endpoint nobody pointed at that runtime is not it.
    http_endpoint_chosen: bool,

    /// API key for authentication
    api_key: Option<String>,

    /// Cloud region (e.g. "us-east-1"). When set, cloud mode is active.
    cloud_region: Option<String>,

    /// User agent string for HTTP requests
    user_agent: String,

    /// Extra headers for HTTP requests
    extra_headers: HashMap<String, String>,

    /// HTTP client for the control-plane calls, under a whole-request deadline
    http_client: reqwest::Client,

    /// HTTP client for requests whose duration is set by model inference, under a
    /// silence deadline
    inference_http_client: reqwest::Client,

    /// The deadline `inference_http_client` carries. Held so a caller that has to measure
    /// progress itself — a streamed response, where the transport's own silence deadline is
    /// reset by keep-alives — bounds it by the same value rather than a second constant.
    inference_deadline: Deadline,

    /// TLS root certificate file path
    tls_root_certificate_file: Option<String>,
}

impl RuntimeContext {
    /// Create a new runtime context with default settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the home directory cannot be determined, or if the HTTP client
    /// cannot be built — the latter is not defaulted past, because a default client would
    /// not carry the same-origin redirect policy.
    pub fn new() -> Result<Self> {
        let home_dir = dirs::home_dir().ok_or_else(|| HomeDirectoryNotFoundSnafu.build())?;
        let spice_runtime_dir = home_dir.join(DOT_SPICE);
        let spice_bin_dir = spice_runtime_dir.join("bin");

        let app_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let pods_dir = app_dir.join(SPICEPODS_DIR);

        let http_client = build_http_client(Self::default_user_agent(), CONTROL_PLANE_DEADLINE)?;
        let inference_http_client =
            build_http_client(Self::default_user_agent(), INFERENCE_DEADLINE)?;

        Ok(Self {
            spice_runtime_dir,
            spice_bin_dir,
            app_dir,
            pods_dir,
            http_endpoint: DEFAULT_HTTP_ENDPOINT.to_string(),
            http_endpoint_chosen: false,
            api_key: None,
            cloud_region: None,
            user_agent: Self::default_user_agent(),
            extra_headers: HashMap::new(),
            http_client,
            inference_http_client,
            inference_deadline: INFERENCE_DEADLINE,
            tls_root_certificate_file: None,
        })
    }

    /// Create a context pointed at `http_endpoint` whose two HTTP clients carry the given
    /// deadlines.
    ///
    /// The production deadlines are tens of seconds apart, so which client a call site
    /// reached for is only observable after a request that runs that long. Shrinking both
    /// makes the same difference observable in milliseconds.
    #[cfg(test)]
    pub(crate) fn with_deadlines_for_test(
        http_endpoint: &str,
        control_plane: Deadline,
        inference: Deadline,
    ) -> Self {
        Self {
            spice_runtime_dir: PathBuf::from("/test/.spice"),
            spice_bin_dir: PathBuf::from("/test/.spice/bin"),
            app_dir: PathBuf::from("/test/app"),
            pods_dir: PathBuf::from("/test/app/spicepods"),
            http_endpoint: http_endpoint.to_string(),
            http_endpoint_chosen: true,
            api_key: None,
            cloud_region: None,
            user_agent: TEST_USER_AGENT.to_string(),
            extra_headers: HashMap::new(),
            http_client: build_http_client(TEST_USER_AGENT.to_string(), control_plane)
                .expect("the test control-plane client should build"),
            inference_http_client: build_http_client(TEST_USER_AGENT.to_string(), inference)
                .expect("the test inference client should build"),
            inference_deadline: inference,
            tls_root_certificate_file: None,
        }
    }

    /// Create a context whose runtime binary is `<spice_bin_dir>/spiced`.
    ///
    /// This is how a test drives the parts of the CLI that *run* the runtime —
    /// the launcher's working directory, exit status, and signal handling —
    /// against a stub executable, without an installed Spice runtime and
    /// without mutating this process's environment.
    #[cfg(test)]
    pub(crate) fn with_bin_dir_for_test(spice_bin_dir: PathBuf) -> Self {
        Self {
            spice_runtime_dir: spice_bin_dir.clone(),
            spice_bin_dir,
            ..Self::with_deadlines_for_test(
                DEFAULT_HTTP_ENDPOINT,
                CONTROL_PLANE_DEADLINE,
                INFERENCE_DEADLINE,
            )
        }
    }

    /// Create a runtime context from CLI arguments.
    ///
    /// `cloud` is `None` when cloud mode is not enabled, or `Some("region")` with
    /// the Cloud runtime endpoint region.
    pub fn with_args(
        http_endpoint: Option<String>,
        api_key: Option<String>,
        cloud: Option<&str>,
        tls_root_certificate_file: Option<String>,
    ) -> Result<Self> {
        let mut ctx = Self::new()?;

        if let Some(endpoint) = http_endpoint {
            ctx.http_endpoint = endpoint;
            ctx.http_endpoint_chosen = true;
        }

        if let Some(region) = cloud {
            // The region replaces whatever `--http-endpoint` asked for, so the endpoint in use
            // is derived from the region rather than chosen. It is derived alongside the Cloud
            // Flight endpoint, which is what makes the pair trustworthy.
            ctx.http_endpoint = spice_cloud_data_endpoint(region);
            ctx.http_endpoint_chosen = false;
            ctx.cloud_region = Some(region.to_string());
        }

        ctx.api_key = normalize_credential(api_key);
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

    /// Load API key from the environment or a .env / .env.local file.
    ///
    /// `SPICE_API_KEY` is checked before the files because `--api-key` is declared
    /// with `env = "SPICE_API_KEY"`: clap resolves that variable itself whenever the
    /// flag is omitted, so consulting it first here is what makes a blank
    /// `--api-key` resolve to the same key that omitting the flag would.
    fn load_api_key_from_env(&self) -> Option<String> {
        self.resolve_api_key(|key| std::env::var(key).ok())
    }

    /// `load_api_key_from_env` with the environment lookup injected, so the precedence
    /// between the process environment and the app's .env files is testable without
    /// mutating this process's environment.
    fn resolve_api_key<F>(&self, mut get_env: F) -> Option<String>
    where
        F: FnMut(&str) -> Option<String>,
    {
        if let Some(api_key) = normalize_credential(get_env("SPICE_API_KEY")) {
            return Some(api_key);
        }

        if let Some(api_key) = self.load_api_key_from_env_files() {
            return Some(api_key);
        }

        normalize_credential(get_env("SPICE_SPICEAI_API_KEY"))
    }

    /// Load API key from the app's .env.local or .env file.
    ///
    /// The first matching entry wins, exactly as before -- .env.local outranks .env,
    /// and within a file the earlier line wins. A blank value is authoritative but is
    /// never a credential: `spice login` writes `SPICE_SPICEAI_API_KEY=` for an app
    /// that has no key, so a blank resolves to `None` rather than falling through to
    /// an older key in a lower-precedence file.
    fn load_api_key_from_env_files(&self) -> Option<String> {
        // Try .env.local first, then .env
        let env_files = [".env.local", ".env"];

        for env_file in &env_files {
            let path = self.app_dir.join(env_file);
            if path.exists()
                && let Ok(env_map) = dotenv::from_path_iter(&path)
            {
                for item in env_map.flatten() {
                    if item.0 == "SPICE_SPICEAI_API_KEY" || item.0 == "SPICE_API_KEY" {
                        return normalize_credential(Some(item.1));
                    }
                }
            }
        }

        None
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

    /// Whether the HTTP endpoint came from `--http-endpoint`, rather than the built-in default
    /// or the cloud region.
    #[must_use]
    pub fn http_endpoint_chosen(&self) -> bool {
        self.http_endpoint_chosen
    }

    /// Get the API key if set.
    #[must_use]
    pub fn api_key(&self) -> Option<&str> {
        self.api_key.as_deref()
    }

    /// Check if cloud mode is enabled.
    #[must_use]
    pub fn is_cloud(&self) -> bool {
        self.cloud_region.is_some()
    }

    /// Get the TLS root certificate file if one was specified.
    #[must_use]
    pub fn tls_root_certificate_file(&self) -> Option<&str> {
        self.tls_root_certificate_file.as_deref()
    }

    /// Get the cloud region if one was specified.
    #[must_use]
    pub fn cloud_region(&self) -> Option<&str> {
        self.cloud_region.as_deref()
    }

    /// Get the HTTP client for the control-plane calls.
    #[must_use]
    pub fn http_client(&self) -> &reqwest::Client {
        &self.http_client
    }

    /// Get the HTTP client for requests whose duration is set by model inference.
    ///
    /// Use this for anything that reaches a model — a chat completion, a text-to-SQL
    /// translation, a search that embeds its query. [`RuntimeContext::http_client`] caps the
    /// whole request, which cuts off a long answer that is still arriving.
    #[must_use]
    pub fn inference_http_client(&self) -> &reqwest::Client {
        &self.inference_http_client
    }

    /// The deadline [`RuntimeContext::inference_http_client`] carries.
    ///
    /// A streamed response needs this: the client's deadline is reset by every byte, and the
    /// runtime keeps an SSE stream alive with a comment every 30 seconds, so the caller has to
    /// measure the gap between meaningful events against this value itself.
    pub(crate) const fn inference_deadline(&self) -> Deadline {
        self.inference_deadline
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

    /// Check if the runtime is installed, in this user's install directory or
    /// (under `sudo`) the invoking user's — see [`Self::resolve_spiced_path`].
    #[must_use]
    pub fn is_runtime_installed(&self) -> bool {
        self.resolve_spiced_path().is_some()
    }

    /// Locate the `spiced` binary to use, tolerating `sudo`.
    ///
    /// `sudo` resets `HOME` to `/root` on most distributions, so
    /// [`Self::spiced_path`] — which is derived from `HOME` — points at
    /// `/root/.spice/bin/spiced` under `sudo` and misses the runtime the
    /// invoking user actually installed. That matters because
    /// `sudo spice connect service install` is the documented way to install the
    /// service: without this, every such run concludes the runtime is missing
    /// and downloads the latest *release*, which on a machine tracking `trunk`
    /// silently pairs a dev CLI with a released runtime.
    ///
    /// Preference order:
    /// 1. `$HOME/.spice/bin/spiced` — the ordinary case, and a genuine root
    ///    login's own install.
    /// 2. `~<$SUDO_USER>/.spice/bin/spiced` — what the operator installed
    ///    before elevating.
    ///
    /// Returns `None` when neither exists.
    #[must_use]
    pub fn resolve_spiced_path(&self) -> Option<PathBuf> {
        let own = self.spiced_path();
        if own.exists() {
            return Some(own);
        }
        let candidate = sudo_invoker_home()?
            .join(DOT_SPICE)
            .join("bin")
            .join(SPICED_FILENAME);
        candidate.exists().then_some(candidate)
    }

    fn is_wsl_environment<F>(mut get_env: F) -> bool
    where
        F: FnMut(&str) -> Option<String>,
    {
        WSL_ENV_KEYS
            .iter()
            .any(|key| get_env(key).is_some_and(|value| !value.trim().is_empty()))
    }

    fn local_runtime_supported_on_platform<F>(is_windows: bool, get_env: F) -> bool
    where
        F: FnMut(&str) -> Option<String>,
    {
        !is_windows || Self::is_wsl_environment(get_env)
    }

    /// Ensure the local Spice runtime can be managed from this process.
    ///
    /// # Errors
    ///
    /// Returns an error when the Windows CLI is running natively instead of under WSL.
    pub fn ensure_local_runtime_supported(&self) -> Result<()> {
        if !Self::local_runtime_supported_on_platform(cfg!(windows), |key| std::env::var(key).ok())
        {
            return Err(WindowsNativeRuntimeUnsupportedSnafu.build());
        }

        Ok(())
    }

    /// Get the installed runtime version.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime is not installed or version cannot be determined.
    pub fn runtime_version(&self) -> Result<String> {
        let Some(spiced) = self.resolve_spiced_path() else {
            return Err(RuntimeNotInstalledSnafu.build());
        };

        let output = Command::new(spiced)
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
        let Some(spiced) = self.resolve_spiced_path() else {
            return Err(RuntimeNotInstalledSnafu.build());
        };

        let mut cmd = Command::new(spiced);
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

        // Add API key if present. Use the environment so the key is not exposed in process args.
        if let Some(api_key) = &self.api_key {
            cmd.env("SPICE_API_KEY", api_key);
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

/// The home directory of the user who invoked `sudo`, or `None` when not
/// running under `sudo` (or the user cannot be resolved).
///
/// Only consulted as a fallback by [`RuntimeContext::resolve_spiced_path`].
#[cfg(unix)]
fn sudo_invoker_home() -> Option<PathBuf> {
    let user = std::env::var("SUDO_USER").ok()?;
    // `sudo -u root` sets SUDO_USER=root, whose home is the `$HOME` branch we
    // already tried — nothing new to look at.
    if user.is_empty() || user == "root" {
        return None;
    }
    passwd_home(&user).map(PathBuf::from)
}

#[cfg(not(unix))]
fn sudo_invoker_home() -> Option<PathBuf> {
    None
}

/// Absolute paths `getent` ships at, in the order they are tried.
///
/// Resolving it through `PATH` would be a privilege-escalation hole: this runs
/// under `sudo` on the documented `spice connect service install` path, so a `PATH`
/// entry the invoking user controls would have this process execute their binary
/// as root. Only these known locations are accepted, and a host with `getent`
/// somewhere else falls through to reading `/etc/passwd`.
#[cfg(unix)]
const GETENT_PATHS: &[&str] = &["/usr/bin/getent", "/bin/getent"];

/// Absolute path to `dscl`, which is how macOS answers this question. Not
/// resolved through `PATH`, for the reason given on [`GETENT_PATHS`].
#[cfg(target_os = "macos")]
const DSCL_PATH: &str = "/usr/bin/dscl";

/// A user's home directory from the passwd database.
///
/// Asks `getent` first so NSS sources (LDAP, SSSD, systemd-homed) resolve, then
/// `dscl`, then falls back to parsing `/etc/passwd` for the minimal images that
/// ship neither. macOS needs `dscl`: it has no `getent`, and its `/etc/passwd`
/// holds only system accounts, so every ordinary user resolves through
/// Directory Services or not at all. Guessing `/home/<user>` is deliberately
/// not a fallback: a wrong path would silently look for a runtime that was
/// never there.
#[cfg(unix)]
fn passwd_home(user: &str) -> Option<String> {
    for getent in GETENT_PATHS {
        if !std::path::Path::new(getent).is_file() {
            continue;
        }
        if let Ok(output) = Command::new(*getent).arg("passwd").arg(user).output()
            && output.status.success()
            && let Some(home) = passwd_entry_home(&String::from_utf8_lossy(&output.stdout), user)
        {
            return Some(home);
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Some(home) = directory_services_home(user) {
            return Some(home);
        }
    }

    let contents = std::fs::read_to_string("/etc/passwd").ok()?;
    passwd_entry_home(&contents, user)
}

/// A user's home directory from macOS Directory Services.
#[cfg(target_os = "macos")]
fn directory_services_home(user: &str) -> Option<String> {
    if !std::path::Path::new(DSCL_PATH).is_file() {
        return None;
    }
    let output = Command::new(DSCL_PATH)
        .args([".", "-read"])
        .arg(format!("/Users/{user}"))
        .arg("NFSHomeDirectory")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    dscl_home(&String::from_utf8_lossy(&output.stdout))
}

/// Extract the home directory from `dscl . -read /Users/<user> NFSHomeDirectory`.
///
/// The answer is one `NFSHomeDirectory: <path>` line, except that `dscl` wraps
/// a value containing a space onto the line below the key instead.
#[cfg(target_os = "macos")]
fn dscl_home(output: &str) -> Option<String> {
    output
        .split_once("NFSHomeDirectory:")?
        .1
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string)
}

/// Extract `user`'s home (field 6) from passwd-format `contents`.
#[cfg(unix)]
fn passwd_entry_home(contents: &str, user: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let mut fields = line.split(':');
        if fields.next() != Some(user) {
            return None;
        }
        // name:passwd:uid:gid:gecos:home:shell — home is index 5 of the rest.
        let home = fields.nth(4)?;
        (!home.is_empty()).then(|| home.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::{TcpListener, TcpStream};

    use crate::test_support::SlowServer;
    use tempfile::TempDir;

    /// A response that takes about twice the deadline under test to arrive, while never being
    /// quiet for longer than a tenth of it.
    ///
    /// The gap is a small fraction of the deadline on purpose: the server sleeps on the wall
    /// clock, so a loaded CI runner that descheduled it for a moment must not be able to make
    /// a healthy stream look stalled.
    fn slow_but_never_quiet(deadline: Duration) -> SlowServer {
        SlowServer::dribbling(
            std::iter::repeat_n("data: token\n\n".to_string(), 20).collect(),
            deadline / 10,
        )
    }

    /// Read a response body in full, so a body-phase deadline is exercised rather than just
    /// the wait for the response head.
    async fn read_body(client: &reqwest::Client, url: &str) -> reqwest::Result<String> {
        client.get(url).send().await?.text().await
    }

    #[tokio::test]
    async fn a_total_deadline_cuts_off_a_response_that_is_still_arriving() {
        let deadline = Duration::from_secs(1);
        let server = slow_but_never_quiet(deadline);
        let client = build_http_client("spice/test".to_string(), Deadline::Total(deadline))
            .expect("the test client should build");

        let error = read_body(&client, server.url())
            .await
            .expect_err("a total deadline should fire on a response that outlasts it");

        assert!(
            error.is_timeout(),
            "expected a timeout, got: {error} ({error:?})"
        );
    }

    #[tokio::test]
    async fn a_silence_deadline_lets_a_slow_response_finish() {
        let deadline = Duration::from_secs(1);
        let server = slow_but_never_quiet(deadline);
        let client = build_http_client("spice/test".to_string(), Deadline::Silence(deadline))
            .expect("the test client should build");

        let body = read_body(&client, server.url())
            .await
            .expect("a response that keeps arriving should be read in full");

        assert_eq!(
            body.matches("data: token").count(),
            20,
            "every chunk should have been read: {body:?}"
        );
    }

    #[tokio::test]
    async fn a_silence_deadline_still_fires_when_the_body_goes_quiet() {
        let deadline = Duration::from_millis(300);
        let server = SlowServer::stalling_after_head();
        let client = build_http_client("spice/test".to_string(), Deadline::Silence(deadline))
            .expect("the test client should build");

        let started = std::time::Instant::now();
        let error = read_body(&client, server.url())
            .await
            .expect_err("a silence deadline should fire on a body that never arrives");

        assert!(
            error.is_timeout(),
            "expected a timeout, got: {error} ({error:?})"
        );
        assert!(
            started.elapsed() < deadline * 20,
            "the deadline should have fired promptly, took {:?}",
            started.elapsed()
        );
    }

    /// The wait for the response head is the one the connect timeout can no longer end: the
    /// connection is established, so an endpoint that accepts and then blocks while it sets a
    /// model up is holding an already-connected request. `read_timeout` has to cover it, and
    /// reqwest's own documentation only promises per-read behaviour — so assert it.
    #[tokio::test]
    async fn a_silence_deadline_fires_before_the_response_head_arrives() {
        let deadline = Duration::from_millis(300);
        let server = SlowServer::stalling_before_head();
        let client = build_http_client("spice/test".to_string(), Deadline::Silence(deadline))
            .expect("the test client should build");

        let started = std::time::Instant::now();
        let error = client
            .get(server.url())
            .send()
            .await
            .expect_err("a silence deadline should fire while waiting for the response head");

        assert!(
            error.is_timeout(),
            "expected a timeout, got: {error} ({error:?})"
        );
        assert!(
            started.elapsed() < deadline * 20,
            "the deadline should have fired promptly, took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn the_inference_deadline_measures_silence_and_outlasts_the_control_plane_one() {
        let Deadline::Silence(inference) = INFERENCE_DEADLINE else {
            panic!(
                "inference requests must not carry a whole-request deadline: {INFERENCE_DEADLINE:?}"
            );
        };
        let Deadline::Total(control_plane) = CONTROL_PLANE_DEADLINE else {
            panic!("control-plane requests should stay under a whole-request deadline");
        };

        // The silence deadline also bounds the wait for the response head, which for a
        // model is a whole prompt evaluation — so it has to be the more generous of the two.
        assert!(
            inference > control_plane,
            "a {inference:?} silence deadline is tighter than the {control_plane:?} deadline it replaces"
        );
    }

    /// Helper to create a `RuntimeContext` with a mocked spiced binary for testing.
    fn create_test_context() -> RuntimeContext {
        RuntimeContext {
            spice_runtime_dir: PathBuf::from("/test/.spice"),
            spice_bin_dir: PathBuf::from("/test/.spice/bin"),
            app_dir: PathBuf::from("/test/app"),
            pods_dir: PathBuf::from("/test/app/spicepods"),
            http_endpoint: "http://127.0.0.1:8090".to_string(),
            http_endpoint_chosen: false,
            api_key: None,
            cloud_region: None,
            user_agent: "spice/test (test; test)".to_string(),
            extra_headers: HashMap::new(),
            http_client: reqwest::Client::new(),
            inference_http_client: reqwest::Client::new(),
            inference_deadline: INFERENCE_DEADLINE,
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
            http_endpoint_chosen: false,
            api_key: None,
            cloud_region: None,
            user_agent: "spice/test (test; test)".to_string(),
            extra_headers: HashMap::new(),
            http_client: reqwest::Client::new(),
            inference_http_client: reqwest::Client::new(),
            inference_deadline: INFERENCE_DEADLINE,
            tls_root_certificate_file: None,
        };

        (ctx, temp_dir)
    }

    #[test]
    fn passwd_entry_home_reads_the_home_field() {
        let passwd = "root:x:0:0:root:/root:/bin/bash\n\
                      owner:x:1000:1000:Owner,,,:/home/owner:/usr/bin/zsh\n\
                      svc:x:998:998::/var/lib/svc:/usr/sbin/nologin\n";
        assert_eq!(
            passwd_entry_home(passwd, "owner").as_deref(),
            Some("/home/owner")
        );
        assert_eq!(passwd_entry_home(passwd, "root").as_deref(), Some("/root"));
        assert_eq!(
            passwd_entry_home(passwd, "svc").as_deref(),
            Some("/var/lib/svc"),
            "an empty GECOS field must not shift the home field"
        );
        assert_eq!(passwd_entry_home(passwd, "nobody"), None);
    }

    #[test]
    fn passwd_entry_home_ignores_prefix_matches_and_empty_homes() {
        // `own` must not match the `owner` line — a prefix is not the user.
        let passwd = "owner:x:1000:1000::/home/owner:/bin/sh\n";
        assert_eq!(passwd_entry_home(passwd, "own"), None);

        // A user with no home directory has nothing to look in.
        let no_home = "ghost:x:1:1:::/bin/false\n";
        assert_eq!(passwd_entry_home(no_home, "ghost"), None);
    }

    /// `passwd_home` runs `getent` while this process may be root under `sudo`,
    /// so it must never be resolved through `PATH` — an entry the invoking user
    /// controls would be executed as root.
    #[cfg(unix)]
    #[test]
    fn getent_is_only_ever_run_from_an_absolute_path() {
        assert!(!GETENT_PATHS.is_empty());
        for candidate in GETENT_PATHS {
            let path = std::path::Path::new(candidate);
            assert!(
                path.is_absolute(),
                "{candidate} must be absolute, or PATH decides which binary runs as root"
            );
            assert_eq!(
                path.file_name().and_then(std::ffi::OsStr::to_str),
                Some("getent"),
                "{candidate} must name getent itself"
            );
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn dscl_home_reads_the_value_however_dscl_wrapped_it() {
        assert_eq!(
            dscl_home("NFSHomeDirectory: /Users/ada\n"),
            Some("/Users/ada".to_string())
        );
        // `dscl` puts a value containing a space on its own line.
        assert_eq!(
            dscl_home("NFSHomeDirectory:\n /Users/ada lovelace\n"),
            Some("/Users/ada lovelace".to_string())
        );
        assert_eq!(dscl_home("<dscl_cmd> DS Error: -14136\n"), None);
        assert_eq!(dscl_home("NFSHomeDirectory:\n"), None);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn passwd_home_resolves_a_user_macos_keeps_out_of_etc_passwd() {
        // Every ordinary macOS account lives in Directory Services only, so
        // without the `dscl` step `sudo spice connect service install` cannot find the
        // runtime the invoking user installed.
        let Ok(user) = std::env::var("USER") else {
            return;
        };
        let passwd = std::fs::read_to_string("/etc/passwd").unwrap_or_default();
        if user.is_empty() || passwd.contains(&format!("\n{user}:")) {
            // An account `/etc/passwd` already answers for proves nothing here.
            return;
        }
        let Some(home) = passwd_home(&user) else {
            panic!("no home resolved for {user}, who is not in /etc/passwd");
        };
        assert!(PathBuf::from(&home).is_dir(), "{home}");
    }

    /// `sudo` rewrites `HOME`, so a runtime installed under the invoking user's
    /// home must still be found — otherwise `sudo spice connect service install`
    /// concludes the runtime is missing and downloads a release over the
    /// operator's build.
    #[test]
    fn resolve_spiced_path_prefers_the_contexts_own_install() {
        let (ctx, _temp) = create_test_context_with_runtime();
        assert_eq!(
            ctx.resolve_spiced_path(),
            Some(ctx.spiced_path()),
            "an install in this context's own bin dir wins outright"
        );
        assert!(ctx.is_runtime_installed());
    }

    #[test]
    fn resolve_spiced_path_is_none_when_nothing_is_installed() {
        // A context whose bin dir holds no runtime, and (in CI) no SUDO_USER
        // install to fall back to.
        let temp = TempDir::new().expect("create temp dir");
        let ctx = RuntimeContext {
            spice_runtime_dir: temp.path().to_path_buf(),
            spice_bin_dir: temp.path().join("bin"),
            app_dir: PathBuf::from("/test/app"),
            pods_dir: PathBuf::from("/test/app/spicepods"),
            http_endpoint: "http://127.0.0.1:8090".to_string(),
            http_endpoint_chosen: false,
            api_key: None,
            cloud_region: None,
            user_agent: "spice/test (test; test)".to_string(),
            extra_headers: HashMap::new(),
            http_client: reqwest::Client::new(),
            inference_http_client: reqwest::Client::new(),
            inference_deadline: INFERENCE_DEADLINE,
            tls_root_certificate_file: None,
        };
        assert!(!ctx.spiced_path().exists());
        // Without SUDO_USER there is no second place to look. (When the suite
        // itself runs under sudo the fallback may legitimately find one, so
        // assert the invariant rather than a bare `None`.)
        match ctx.resolve_spiced_path() {
            None => {}
            Some(found) => assert_ne!(
                found,
                ctx.spiced_path(),
                "a resolved path must come from the sudo fallback, not the empty bin dir"
            ),
        }
    }

    /// Convert Command args to a Vec<String> for testing.
    /// This extracts the arguments that would be passed to spiced.
    fn get_cmd_args(cmd: &Command) -> Vec<String> {
        cmd.get_args()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect()
    }

    fn get_cmd_env(cmd: &Command, key: &str) -> Option<String> {
        let key = std::ffi::OsStr::new(key);
        cmd.get_envs()
            .find(|(env_key, _)| *env_key == key)
            .and_then(|(_, env_value)| env_value.map(|value| value.to_string_lossy().to_string()))
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
    fn test_get_run_cmd_sets_api_key_env_when_set() {
        let (mut ctx, _temp_dir) = create_test_context_with_runtime();
        ctx.api_key = Some("test-api-key-12345".to_string());

        let cmd = ctx
            .get_run_cmd(&[], None)
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);

        assert!(
            !args.contains(&"--api-key".to_string())
                && !args.contains(&"test-api-key-12345".to_string()),
            "Should not expose API key in process args, got: {args:?}"
        );
        assert_eq!(
            get_cmd_env(&cmd, "SPICE_API_KEY").as_deref(),
            Some("test-api-key-12345")
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
        assert_eq!(get_cmd_env(&cmd, "SPICE_API_KEY"), None);
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
        assert_eq!(
            get_cmd_env(&cmd, "SPICE_API_KEY").as_deref(),
            Some("my-api-key")
        );
    }

    #[test]
    fn test_get_run_cmd_fails_when_runtime_not_installed() {
        let ctx = create_test_context();
        // spice_bin_dir points to /test/.spice/bin which doesn't exist
        let result = ctx.get_run_cmd(&[], None);

        assert!(result.is_err(), "Should fail when runtime not installed");
    }

    #[test]
    fn test_local_runtime_supported_on_non_windows() {
        assert!(RuntimeContext::local_runtime_supported_on_platform(
            false,
            |_| None
        ));
    }

    #[test]
    fn test_local_runtime_supported_in_wsl_on_windows() {
        assert!(RuntimeContext::local_runtime_supported_on_platform(
            true,
            |key| {
                if key == "WSL_DISTRO_NAME" {
                    Some("Ubuntu".to_string())
                } else {
                    None
                }
            }
        ));

        assert!(RuntimeContext::local_runtime_supported_on_platform(
            true,
            |key| {
                if key == "WSL_INTEROP" {
                    Some("/run/WSL/123_interop".to_string())
                } else {
                    None
                }
            }
        ));
    }

    #[test]
    fn test_local_runtime_not_supported_on_native_windows() {
        assert!(!RuntimeContext::local_runtime_supported_on_platform(
            true,
            |_| None
        ));
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
            RuntimeContext::with_args(Some("http://custom:9999".to_string()), None, None, None)
                .expect("with_args should succeed");

        assert_eq!(ctx.http_endpoint(), "http://custom:9999");
    }

    /// #11005: `spice sql` moves only the Flight endpoint, so it needs to tell an HTTP endpoint
    /// somebody chose from the default one nobody pointed anywhere.
    #[test]
    fn test_with_args_records_whether_the_http_endpoint_was_chosen() {
        let chosen =
            RuntimeContext::with_args(Some("http://custom:9999".to_string()), None, None, None)
                .expect("with_args should succeed");

        assert!(chosen.http_endpoint_chosen());

        let omitted =
            RuntimeContext::with_args(None, None, None, None).expect("with_args should succeed");

        assert!(!omitted.http_endpoint_chosen());
        assert_eq!(omitted.http_endpoint(), DEFAULT_HTTP_ENDPOINT);
    }

    /// A cloud region replaces the flag's value, so the endpoint in use was derived rather than
    /// chosen — and it is derived alongside the Cloud Flight endpoint, which is what makes the
    /// pair trustworthy without either being chosen.
    #[test]
    fn test_with_args_treats_a_cloud_endpoint_as_derived() {
        let ctx = RuntimeContext::with_args(
            Some("http://custom:9999".to_string()),
            None,
            Some("us-east-1"),
            None,
        )
        .expect("with_args should succeed");

        assert!(!ctx.http_endpoint_chosen());
        assert_ne!(ctx.http_endpoint(), "http://custom:9999");
    }

    #[test]
    fn test_with_args_sets_api_key() {
        let ctx = RuntimeContext::with_args(None, Some("test-key".to_string()), None, None)
            .expect("with_args should succeed");

        assert_eq!(ctx.api_key(), Some("test-key"));
    }

    #[test]
    fn test_normalize_credential_discards_blank_values() {
        assert_eq!(normalize_credential(None), None);
        assert_eq!(normalize_credential(Some(String::new())), None);
        assert_eq!(normalize_credential(Some("   ".to_string())), None);
        assert_eq!(normalize_credential(Some("\t\r\n".to_string())), None);
    }

    #[test]
    fn test_normalize_credential_keeps_a_real_key_verbatim() {
        assert_eq!(
            normalize_credential(Some("real-key".to_string())),
            Some("real-key".to_string())
        );
        // Surrounding whitespace decides only whether the value is blank; a key that
        // has any content is passed through exactly as supplied.
        assert_eq!(
            normalize_credential(Some(" real-key ".to_string())),
            Some(" real-key ".to_string())
        );
    }

    #[test]
    fn test_with_args_treats_an_empty_api_key_as_absent() {
        // Regression: `--api-key ""` used to be carried as Some("") -- it suppressed the
        // .env fallback and every authenticated request went out with a blank credential.
        // The resolved key may legitimately come from the environment here, so what is
        // asserted is that the blank flag itself is never what gets carried.
        for blank in ["", "   ", "\t"] {
            let ctx = RuntimeContext::with_args(None, Some(blank.to_string()), None, None)
                .expect("with_args should succeed");

            assert_ne!(
                ctx.api_key(),
                Some(blank),
                "an explicitly blank --api-key must not be carried verbatim"
            );
            assert!(
                ctx.api_key().is_none_or(|key| !key.trim().is_empty()),
                "a blank --api-key must never resolve to a blank credential"
            );
        }
    }

    /// A context whose app dir is an isolated temp dir, for the .env lookup tests.
    /// The `TempDir` must be kept alive for the test.
    fn create_test_context_with_app_dir() -> (RuntimeContext, TempDir) {
        let temp_dir = TempDir::new().expect("create temp dir");
        let mut ctx = create_test_context();
        ctx.app_dir = temp_dir.path().to_path_buf();

        (ctx, temp_dir)
    }

    /// Write one of the app dir's env files for a .env lookup test.
    fn write_env_file(dir: &TempDir, name: &str, contents: &str) {
        std::fs::write(dir.path().join(name), contents).expect("write env file");
    }

    #[test]
    fn test_load_api_key_from_env_files_returns_a_stored_key() {
        let (ctx, temp_dir) = create_test_context_with_app_dir();
        write_env_file(&temp_dir, ".env", "SPICE_API_KEY=real-key\n");

        assert_eq!(
            ctx.load_api_key_from_env_files(),
            Some("real-key".to_string())
        );
    }

    #[test]
    fn test_load_api_key_from_env_files_prefers_env_local() {
        let (ctx, temp_dir) = create_test_context_with_app_dir();
        write_env_file(&temp_dir, ".env.local", "SPICE_API_KEY=local-key\n");
        write_env_file(&temp_dir, ".env", "SPICE_API_KEY=plain-key\n");

        assert_eq!(
            ctx.load_api_key_from_env_files(),
            Some("local-key".to_string())
        );
    }

    #[test]
    fn test_load_api_key_from_env_files_treats_a_stored_blank_as_no_key() {
        let (ctx, temp_dir) = create_test_context_with_app_dir();
        // `spice login` writes SPICE_SPICEAI_API_KEY= for an app that has no key, so a
        // blank is a deliberate "no key" -- it must resolve to None rather than either
        // becoming a blank credential or resurrecting an older key from .env.
        write_env_file(&temp_dir, ".env.local", "SPICE_SPICEAI_API_KEY=\n");
        write_env_file(&temp_dir, ".env", "SPICE_API_KEY=older-key\n");

        assert_eq!(ctx.load_api_key_from_env_files(), None);
    }

    #[test]
    fn test_load_api_key_from_env_files_without_any_env_file() {
        let (ctx, _temp_dir) = create_test_context_with_app_dir();

        assert_eq!(ctx.load_api_key_from_env_files(), None);
    }

    /// An env lookup for the resolve tests: `name` is set to `value`, nothing else is.
    fn only_env(name: &'static str, value: &'static str) -> impl FnMut(&str) -> Option<String> {
        move |key| (key == name).then(|| value.to_string())
    }

    #[test]
    fn test_resolve_api_key_prefers_the_process_environment() {
        // --api-key is declared `env = "SPICE_API_KEY"`, so clap resolves that variable
        // itself when the flag is omitted. This fallback has to agree with clap:
        // otherwise a blank --api-key would resolve to the .env key while omitting the
        // flag resolved to the environment's, silently selecting a different credential.
        let (ctx, temp_dir) = create_test_context_with_app_dir();
        write_env_file(&temp_dir, ".env.local", "SPICE_API_KEY=file-key\n");

        let api_key = ctx.resolve_api_key(only_env("SPICE_API_KEY", "env-key"));

        assert_eq!(api_key, Some("env-key".to_string()));
    }

    #[test]
    fn test_resolve_api_key_prefers_files_over_the_legacy_variable() {
        let (ctx, temp_dir) = create_test_context_with_app_dir();
        write_env_file(&temp_dir, ".env", "SPICE_API_KEY=file-key\n");

        // A blank primary variable falls through to the files, and a stored key outranks
        // the legacy variable -- together with the two tests either side of this one,
        // that pins the whole order: SPICE_API_KEY > .env files > SPICE_SPICEAI_API_KEY.
        let api_key = ctx.resolve_api_key(|key| match key {
            "SPICE_API_KEY" => Some("   ".to_string()),
            "SPICE_SPICEAI_API_KEY" => Some("legacy-key".to_string()),
            _ => None,
        });

        assert_eq!(api_key, Some("file-key".to_string()));
    }

    #[test]
    fn test_resolve_api_key_uses_the_legacy_variable_last() {
        let (ctx, _temp_dir) = create_test_context_with_app_dir();

        let api_key = ctx.resolve_api_key(only_env("SPICE_SPICEAI_API_KEY", "legacy-key"));

        assert_eq!(api_key, Some("legacy-key".to_string()));
    }

    #[test]
    fn test_with_args_sets_cloud_mode_and_region() {
        let ctx = RuntimeContext::with_args(None, None, Some("us-east-1"), None)
            .expect("with_args should succeed");

        assert!(ctx.is_cloud());
        assert_eq!(
            ctx.http_endpoint(),
            "https://us-east-1-prod-aws-data.spiceai.io"
        );
        assert_eq!(ctx.cloud_region(), Some("us-east-1"));
    }

    #[test]
    fn test_with_args_sets_tls_certificate() {
        let ctx =
            RuntimeContext::with_args(None, None, None, Some("/path/to/cert.pem".to_string()))
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
            None,
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
            Some("us-west-2"), // Cloud mode enabled with region
            None,
        )
        .expect("with_args should succeed");

        assert!(ctx.is_cloud());
        assert_eq!(
            ctx.http_endpoint(),
            "https://us-west-2-prod-aws-data.spiceai.io"
        );
    }

    #[test]
    fn test_cloud_mode_with_api_key() {
        // Cloud mode with API key
        let ctx = RuntimeContext::with_args(
            None,
            Some("cloud-api-key-12345".to_string()),
            Some("us-west-2"),
            None,
        )
        .expect("with_args should succeed");

        assert!(ctx.is_cloud());
        assert_eq!(
            ctx.http_endpoint(),
            "https://us-west-2-prod-aws-data.spiceai.io"
        );
        assert_eq!(ctx.api_key(), Some("cloud-api-key-12345"));
    }

    #[test]
    fn test_local_mode_with_api_key() {
        // Local mode can also have an API key (for local runtime auth)
        let ctx = RuntimeContext::with_args(
            Some("http://localhost:8090".to_string()),
            Some("local-api-key".to_string()),
            None,
            None,
        )
        .expect("with_args should succeed");

        assert!(!ctx.is_cloud());
        assert_eq!(ctx.http_endpoint(), "http://localhost:8090");
        assert_eq!(ctx.api_key(), Some("local-api-key"));
    }

    #[test]
    fn test_cloud_mode_uses_https() {
        let ctx = RuntimeContext::with_args(None, None, Some("us-east-1"), None)
            .expect("with_args should succeed");

        assert!(
            ctx.http_endpoint().starts_with("https://"),
            "Cloud mode should use HTTPS, got: {}",
            ctx.http_endpoint()
        );
    }

    #[test]
    fn test_local_mode_socket_address() {
        let ctx =
            RuntimeContext::with_args(None, None, None, None).expect("with_args should succeed");

        // Local mode socket address should not have scheme prefix
        assert_eq!(ctx.http_socket_address(), "127.0.0.1:8090");
    }

    #[test]
    fn test_cloud_mode_socket_address() {
        let ctx = RuntimeContext::with_args(None, None, Some("us-east-1"), None)
            .expect("with_args should succeed");

        // Cloud mode socket address should strip https://
        assert_eq!(
            ctx.http_socket_address(),
            "us-east-1-prod-aws-data.spiceai.io"
        );
    }

    #[test]
    fn test_mode_reflected_in_headers() {
        // Both local and cloud modes should include user agent
        let local_ctx =
            RuntimeContext::with_args(None, None, None, None).expect("with_args should succeed");
        let cloud_ctx = RuntimeContext::with_args(None, None, Some("us-east-1"), None)
            .expect("with_args should succeed");

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

    /// How long a request that must not hang is given before the test fails it. Well under
    /// the context client's own 30-second timeout, so a regression fails fast instead of
    /// stalling.
    const TEST_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

    /// Read the request head so the client's write completes before we reply. Closing a
    /// socket with unread request data still buffered can surface as a reset rather than the
    /// response under test, which on Windows is packetisation dependent and so intermittent.
    fn drain_request_head(stream: &mut TcpStream) {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            if line == "\r\n" || line == "\n" {
                return;
            }
        }
    }

    fn serve_once(listener: &TcpListener, response: &str) {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        drain_request_head(&mut stream);
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }

    fn localhost_listener() -> TcpListener {
        TcpListener::bind("127.0.0.1:0").expect("test listener should bind")
    }

    fn local_port(listener: &TcpListener) -> u16 {
        listener
            .local_addr()
            .expect("listener should have a local address")
            .port()
    }

    /// Every request the CLI makes through this context carries the API key in an
    /// `X-API-Key` header, which `reqwest` does not strip on a cross-origin redirect. A
    /// runtime, proxy or ingress answering with an off-origin `Location` must therefore be
    /// refused rather than handed the key (#12495).
    ///
    /// Goes through `with_args` so the client under test is the one `RuntimeContext::new`
    /// builds — a test that assembled its own client would still pass if the policy were
    /// dropped from the constructor.
    #[tokio::test]
    async fn test_context_client_does_not_follow_a_cross_origin_redirect() {
        let runtime = localhost_listener();
        let elsewhere = localhost_listener();
        let elsewhere_port = local_port(&elsewhere);
        let runtime_port = local_port(&runtime);

        // Nothing should ever connect here; poll without blocking after the call returns.
        elsewhere
            .set_nonblocking(true)
            .expect("listener should go non-blocking");

        let response = format!(
            "HTTP/1.1 307 Temporary Redirect\r\n\
             Location: http://127.0.0.1:{elsewhere_port}/collect\r\n\
             Content-Length: 0\r\n\
             Connection: close\r\n\r\n"
        );
        let server = std::thread::spawn(move || serve_once(&runtime, &response));

        let ctx = RuntimeContext::with_args(
            Some(format!("http://127.0.0.1:{runtime_port}")),
            Some("SECRETKEY".to_string()),
            None,
            None,
        )
        .expect("context should build");

        // On the default policy the client follows the hop and then waits on a listener that
        // never answers, so without this bound the regression surfaces only as a stall.
        let got = tokio::time::timeout(TEST_REQUEST_TIMEOUT, ctx.get("/v1/status"))
            .await
            .expect("a refused redirect must return promptly, not hang")
            .expect("the 307 should come back as a response");

        // Stopped at the redirect rather than followed, and the 3xx is still diagnosable.
        assert_eq!(got.status().as_u16(), 307);

        // `WouldBlock` specifically: any other error would mean the listener itself failed,
        // which is not evidence that nothing ever connected to it.
        let contacted = elsewhere.accept();
        let refused_kind = contacted.as_ref().err().map(std::io::Error::kind);
        assert_eq!(
            refused_kind,
            Some(std::io::ErrorKind::WouldBlock),
            "the off-origin listener must never be contacted"
        );

        server.join().expect("server thread should not panic");
    }

    /// The policy must not break a legitimate same-origin redirect on a runtime endpoint.
    #[tokio::test]
    async fn test_context_client_follows_a_same_origin_redirect() {
        let listener = localhost_listener();
        let port = local_port(&listener);

        let redirect = format!(
            "HTTP/1.1 307 Temporary Redirect\r\n\
             Location: http://127.0.0.1:{port}/v1/status/retry\r\n\
             Content-Length: 0\r\n\
             Connection: close\r\n\r\n"
        );
        let ok = "HTTP/1.1 200 OK\r\n\
                  Content-Type: application/json\r\n\
                  Content-Length: 11\r\n\
                  Connection: close\r\n\r\n\
                  {\"ok\":true}";
        let server = std::thread::spawn(move || {
            serve_once(&listener, &redirect);
            serve_once(&listener, ok);
        });

        let ctx = RuntimeContext::with_args(
            Some(format!("http://127.0.0.1:{port}")),
            Some("SECRETKEY".to_string()),
            None,
            None,
        )
        .expect("context should build");

        let got = tokio::time::timeout(TEST_REQUEST_TIMEOUT, ctx.get("/v1/status"))
            .await
            .expect("the same-origin redirect chain must not hang")
            .expect("the followed redirect should return a response");

        assert_eq!(got.status().as_u16(), 200);

        server.join().expect("server thread should not panic");
    }
}
