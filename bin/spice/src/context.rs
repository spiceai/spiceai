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
    CreateDirectorySnafu, Error, HomeDirectoryNotFoundSnafu, HttpClientBuildSnafu, Result,
    RuntimeExecutionSnafu, RuntimeNotInstalledSnafu, RuntimeVersionSnafu,
    SpicedPathNotAnchorableSnafu, SpicedPathOverrideNotRunnableSnafu,
    WindowsNativeRuntimeUnsupportedSnafu,
};
use snafu::{OptionExt, ResultExt, ensure};
use spice_cloud_client::endpoints::data_endpoint as spice_cloud_data_endpoint;
use spice_cloud_client::redirect::same_origin_redirect_policy;
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
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

/// Environment variable that pins the runtime binary outright, ahead of every
/// other candidate. The escape hatch for testing one specific build without
/// moving files onto `PATH` or into the managed install directory.
const SPICED_PATH_ENV: &str = "SPICED_PATH";

/// The search path consulted after the sibling of the running CLI.
const PATH_ENV: &str = "PATH";
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
    /// never a credential, so it resolves to `None` rather than falling through to an
    /// older key in a lower-precedence file.
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

    /// Whether a runtime is present in the directory `spice install` writes to.
    ///
    /// Deliberately *not* "is a runtime available": `spice install` and
    /// `spice upgrade` own [`Self::spiced_path`] and nothing else, so asking
    /// the whole ladder would let a `spiced` on `PATH` convince them their own
    /// directory is already up to date. What will *run* is
    /// [`Self::resolve_spiced`]; the two are different questions and this is
    /// the one a writer of the managed install must ask.
    #[must_use]
    pub fn is_managed_runtime_installed(&self) -> bool {
        is_runnable_binary(&self.spiced_path())
    }

    /// Locate the `spiced` binary to run, and say where it came from.
    ///
    /// A CLI and a runtime are a pair, and the pair has to stay together: the
    /// managed install directory alone cannot express that, because a `spice`
    /// built from `trunk` and run off `PATH` would drive whatever release
    /// happened to be sitting in `~/.spice/bin`.
    ///
    /// `sudo` resets `HOME` to `/root` on most distributions, so
    /// [`Self::spiced_path`] — which is derived from `HOME` — points at
    /// `/root/.spice/bin/spiced` under `sudo` and misses the runtime the
    /// invoking user actually installed. That matters because
    /// `sudo spice cloud service install` is the documented way to install the
    /// service: without the last rung, every such run concludes the runtime is
    /// missing and downloads the latest *release*, which on a machine tracking
    /// `trunk` silently pairs a dev CLI with a released runtime.
    ///
    /// Preference order:
    /// 1. `$SPICED_PATH` — an explicit pin, honored ahead of everything.
    /// 2. `spiced` beside the running `spice`, via `current_exe()` — the
    ///    binary the user actually invoked, and its build partner.
    /// 3. `spiced` on `PATH`.
    /// 4. `$HOME/.spice/bin/spiced` — the managed install, and a genuine root
    ///    login's own install.
    /// 5. `~<$SUDO_USER>/.spice/bin/spiced` — what the operator installed
    ///    before elevating.
    ///
    /// Under `sudo`, every rung names a binary the invoking user already chose
    /// or installed, which is the trust rung 5 has always extended: a runtime
    /// in the invoker's own home has been executed as root since that rung was
    /// added.
    ///
    /// Rung 3 extends it furthest. What bounds it is that `PATH` is the
    /// caller's own configuration, that relative entries are refused so the
    /// working directory cannot become one, and that the rungs below are
    /// reached only when no entry holds a runtime at all. It does *not* rest on
    /// `sudo` sanitizing `PATH`: `secure_path` is set by stock `sudoers` on most
    /// Linux distributions but not on macOS, and `PATH` is searched in order, so
    /// the directory supplying `spiced` need not be the one that supplied
    /// `spice`. Narrowing the rung under elevation is tracked in #13316.
    /// The commands documented to run under `sudo` — `spice cloud service
    /// install` and `spice connect remove` — do not rely on it either; the
    /// former clears the environment and drops to the service account before it
    /// runs anything it resolved here.
    ///
    /// Returns `Ok(None)` when no candidate exists — the signal to install.
    ///
    /// # Errors
    ///
    /// Returns an error when `SPICED_PATH` is set but names nothing runnable —
    /// a mistyped path, or a binary that lost its execute bit — so the pin is
    /// reported instead of falling through to a different runtime than the one
    /// that was asked for.
    pub fn resolve_spiced(&self) -> Result<Option<ResolvedSpiced>> {
        resolve_spiced(&self.spiced_path(), &SpicedLookup::from_host())
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

    /// The version of the runtime that [`Self::resolve_spiced`] would launch.
    ///
    /// # Errors
    ///
    /// Returns an error if no runtime resolves, `SPICED_PATH` is unusable, or
    /// the version cannot be read.
    pub fn runtime_version(&self) -> Result<String> {
        let resolved = self.resolve_spiced()?.context(RuntimeNotInstalledSnafu)?;
        runtime_version_at(&resolved.path)
    }

    /// The version of the runtime in the directory `spice install` writes to.
    ///
    /// The counterpart of [`Self::is_managed_runtime_installed`], and what
    /// `spice install`/`spice upgrade` compare a release against: they replace
    /// that one file, so a `spiced` elsewhere on the machine must not decide
    /// whether they have work to do.
    ///
    /// # Errors
    ///
    /// Returns an error if that directory holds no runtime, or the version
    /// cannot be read.
    pub fn managed_runtime_version(&self) -> Result<String> {
        let managed = self.spiced_path();
        ensure!(
            self.is_managed_runtime_installed(),
            RuntimeNotInstalledSnafu
        );
        runtime_version_at(&managed)
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
        spiced: &ResolvedSpiced,
        args: &[String],
        http_endpoint_override: Option<&str>,
    ) -> Result<Command> {
        let mut cmd = Command::new(&spiced.path);
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
            let tls_cert = PathBuf::from(tls_cert);
            let tls_cert = if tls_cert.is_absolute() {
                tls_cert
            } else {
                // Preserve the CLI caller's path semantics even when a caller
                // selects a different child working directory before spawn.
                std::env::current_dir()
                    .context(RuntimeExecutionSnafu)?
                    .join(tls_cert)
            };
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

/// Warn when the runtime just written to the managed install directory is not
/// the one [`RuntimeContext::resolve_spiced`] will select.
///
/// `spice install` and `spice upgrade` only ever write to `$HOME/.spice/bin`,
/// but a `spiced` beside the CLI, on `PATH`, or pinned outranks it — so an
/// upgrade can report success while every later `spice run` keeps starting the
/// old binary. Silence there is the worst outcome: the user has done the thing
/// that was supposed to fix their problem.
pub fn warn_if_install_is_shadowed(ctx: &RuntimeContext) {
    if let Some(warning) = install_shadow_warning(&ctx.spiced_path(), &ctx.resolve_spiced()) {
        tracing::warn!("{warning}");
    }
}

/// The line [`warn_if_install_is_shadowed`] emits, or `None` when the managed
/// install is the file that will run.
///
/// Separated from the `tracing` call so each arm is assertable: reaching the
/// failed-resolution arm through [`warn_if_install_is_shadowed`] would mean
/// setting `SPICED_PATH` for the whole test process.
fn install_shadow_warning(
    installed: &Path,
    resolved: &Result<Option<ResolvedSpiced>>,
) -> Option<String> {
    match resolved {
        // Nothing outranks the managed install and nothing else exists, so the
        // file just written is the file that will run.
        Ok(None) => None,
        // Compared by path rather than by source: the question here is whether
        // the managed install is the file that will run, which is not the same
        // question as whether the source is worth announcing at launch.
        Ok(Some(resolved)) if same_file(&resolved.path, installed) => None,
        Ok(Some(resolved)) => Some(shadowed_install_warning(installed, resolved)),
        // The install stands — the bytes are on disk — but no `spice run` can
        // reach them while resolution fails, and the caller has just reported
        // success. This is the only place the user hears otherwise.
        Err(cause) => Some(unselectable_install_warning(installed, cause)),
    }
}

/// Whether two paths name the same file.
///
/// Resolves symlinks, so a `PATH` entry pointing at the managed install is
/// recognized as that install rather than as something shadowing it. Falls back
/// to a lexical comparison when either path cannot be canonicalized — a path
/// that no longer exists cannot be the file that is about to run, so the
/// fallback answering "different" is the safe direction.
fn same_file(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

/// The wording of [`warn_if_install_is_shadowed`].
///
/// A function so the text is asserted rather than eyeballed: it is the only
/// explanation a user gets for an upgrade that appears to succeed and changes
/// nothing, so it has to keep naming both binaries and what to do about it.
///
/// Worded without claiming a write, because the callers that most need it are
/// the ones that did not perform one — `spice install` reporting the version is
/// already present, and `spice upgrade` reporting it is already current.
fn shadowed_install_warning(installed: &Path, resolved: &ResolvedSpiced) -> String {
    let installed = installed.display();
    let selected = resolved.path.display();
    let source = resolved.source.describe();
    format!(
        "The managed runtime is at '{installed}', but 'spice run' will start '{selected}' ({source}) instead, so installing or upgrading the managed one has no effect. Remove that binary, or set `SPICED_PATH` to '{installed}' to pin the managed install. See: https://spiceai.org/docs/cli"
    )
}

/// The wording of [`warn_if_install_is_shadowed`]'s failed-resolution arm.
///
/// A function for the same reason as [`shadowed_install_warning`]: it is the
/// only account a user gets of an install that reported success and cannot be
/// started, so it has to keep naming the install and why it is unreachable.
///
/// Carries no remedy of its own — `cause` is a resolution error, and those
/// already name the fix and the docs page.
fn unselectable_install_warning(installed: &Path, cause: &Error) -> String {
    let installed = installed.display();
    format!(
        "The managed runtime is at '{installed}', but 'spice run' cannot work out which runtime to start, so it will fail rather than start this one. Cause: {cause}"
    )
}

/// Read a runtime's version by running it.
///
/// Public so a caller that has already resolved does not walk the ladder a
/// second time to reach the same binary — and cannot be told a version by one
/// binary while reporting the path of another.
///
/// # Errors
///
/// Returns an error if the binary cannot be executed or reports a failure.
pub fn runtime_version_at(spiced: &Path) -> Result<String> {
    let output = Command::new(spiced)
        .arg("--version")
        .output()
        .context(RuntimeExecutionSnafu)?;

    ensure!(
        output.status.success(),
        RuntimeVersionSnafu {
            message: String::from_utf8_lossy(&output.stderr).to_string(),
        }
    );

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Where the `spiced` that is about to run was found.
///
/// Carried alongside the path so the launcher can report a runtime that came
/// from anywhere other than the managed install — the case a user cannot
/// otherwise see, and the one that produces a version-skewed pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpicedSource {
    /// Pinned by `SPICED_PATH`.
    Pinned,
    /// Beside the running `spice`.
    Sibling,
    /// Found on `PATH`.
    OnPath,
    /// The managed install directory, `$HOME/.spice/bin`.
    ManagedInstall,
    /// The `sudo` invoker's managed install directory.
    SudoInvokerInstall,
}

impl SpicedSource {
    /// How this source reads in the line naming the runtime being launched.
    #[must_use]
    pub fn describe(self) -> &'static str {
        match self {
            Self::Pinned => "pinned by SPICED_PATH",
            Self::Sibling => "beside the spice CLI",
            Self::OnPath => "found on PATH",
            Self::ManagedInstall => "installed by spice install",
            Self::SudoInvokerInstall => "installed by spice install, by the sudo invoker",
        }
    }

    /// Whether this is the location `spice install` writes to for this user —
    /// the source that needs no announcing because it is what a user expects.
    #[must_use]
    pub fn is_expected_default(self) -> bool {
        matches!(self, Self::ManagedInstall)
    }
}

/// A located `spiced` binary and where it was found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSpiced {
    /// Path to the binary, anchored against the working directory in effect when
    /// it was resolved, so a caller that changes directory before spawning still
    /// launches the binary that was found.
    pub path: PathBuf,
    /// Which rung of the ladder supplied it.
    pub source: SpicedSource,
}

impl ResolvedSpiced {
    /// The constructor every rung goes through, so anchoring covers all of
    /// them rather than the two that happen to need it today. A struct literal
    /// skips that, which is why the rungs and the launcher's fixtures build
    /// through here.
    ///
    /// # Errors
    ///
    /// Returns an error when a relative candidate cannot be anchored, which
    /// means the working directory could not be read. Anchoring is what makes
    /// the binary that was validated the binary that runs, so a candidate that
    /// cannot be anchored is refused rather than handed on — see
    /// [`anchor_to_current_dir`].
    pub(crate) fn at(path: PathBuf, source: SpicedSource) -> Result<Self> {
        Ok(Self {
            path: anchor_to_current_dir(path)?,
            source,
        })
    }
}

/// The host facts [`resolve_spiced`] reads.
///
/// Injected rather than read inline so the ladder is testable: a developer
/// machine with `spiced` on `PATH`, or a suite running under `sudo`, would
/// otherwise decide the outcome of every test of the ordering.
struct SpicedLookup<'a> {
    /// `$SPICED_PATH`, as read from the environment.
    pinned: Option<OsString>,
    /// `$PATH`, unsplit. Not a `String`: a `PATH` entry need not be UTF-8.
    search_path: Option<OsString>,
    /// Path of the running executable, when the OS will say.
    current_exe: Option<PathBuf>,
    /// Home directory of the `sudo` invoker, when running under `sudo`.
    ///
    /// A callable rather than a value because answering it can cost a
    /// subprocess — `getent` on Linux, `dscl` on macOS, which queries Directory
    /// Services and is slow on a directory-bound host. Only the last rung needs
    /// it, and the last rung is the one almost never reached.
    sudo_invoker_home: &'a dyn Fn() -> Option<PathBuf>,
    /// Whether a candidate path names a runtime binary — see
    /// [`is_runnable_binary`].
    is_binary: &'a dyn Fn(&Path) -> bool,
}

/// Whether `path` names something that can be run as the runtime.
///
/// Stricter than `exists` in two ways that the ladder depends on, because a
/// candidate that wins a rung shadows every rung below it *and* suppresses the
/// auto-install: a `PATH` entry holding a *directory* called `spiced`, or a
/// half-written download left without its execute bit, must not become the
/// answer while a working runtime sits further down. Symlinks are followed, so
/// a symlinked install still resolves.
fn is_runnable_binary(path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(path) else {
        return false;
    };
    metadata.is_file() && grants_execute(&metadata)
}

/// Whether the mode grants execute to anyone.
///
/// Any of the three bits, not the one matching this process: `spice run` under
/// `sudo` is a different identity than the `spice install` that wrote the file,
/// and narrowing to the caller's own bit would make the answer depend on which
/// of them is asking.
#[cfg(unix)]
fn grants_execute(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

/// Windows carries no execute bit, so being a file is the whole test.
#[cfg(not(unix))]
fn grants_execute(_metadata: &std::fs::Metadata) -> bool {
    true
}

/// Resolve `path` against the working directory it was validated in.
///
/// The runtime is spawned with the child's working directory set to `--dir`,
/// and `Command::new` re-interprets a relative program path against *that*
/// directory — so a relative `SPICED_PATH` or `PATH` entry would be checked
/// against one directory and executed from another. Worse, a bare name with no
/// separator is not a path to `Command::new` at all: it is a fresh `PATH`
/// lookup, so the file that was validated need not be the file that runs.
/// Anchoring makes the binary that was checked the binary that runs.
///
/// # Errors
///
/// Returns [`Error::SpicedPathNotAnchorable`] when a relative path cannot be
/// made absolute, which happens when the working directory cannot be read.
/// Handing the relative path back instead would leave exactly the substitution
/// this function exists to prevent — and silently, since the candidate has
/// already passed its runnable check by then.
fn anchor_to_current_dir(path: PathBuf) -> Result<PathBuf> {
    // The closure only pins the lifetime `std::path::absolute`'s generic
    // parameter leaves open; it is still that function doing the work.
    anchor_with(path, |candidate: &Path| std::path::absolute(candidate))
}

/// [`anchor_to_current_dir`] with the resolver injected, so the failure arm is
/// reachable in a test.
///
/// `std::path::absolute` only fails when the working directory cannot be read,
/// which a test cannot arrange without changing the directory of the whole
/// process — shared with every other test in the binary.
fn anchor_with(
    path: PathBuf,
    absolute: impl Fn(&Path) -> std::io::Result<PathBuf>,
) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path);
    }
    absolute(&path).context(SpicedPathNotAnchorableSnafu {
        path: path.display().to_string(),
    })
}

impl SpicedLookup<'static> {
    /// The real host.
    fn from_host() -> Self {
        Self {
            pinned: std::env::var_os(SPICED_PATH_ENV),
            search_path: std::env::var_os(PATH_ENV),
            current_exe: std::env::current_exe().ok(),
            sudo_invoker_home: &sudo_invoker_home,
            is_binary: &is_runnable_binary,
        }
    }
}

/// Walk the resolution ladder documented on [`RuntimeContext::resolve_spiced`].
///
/// `managed_install` is the `$HOME/.spice/bin/spiced` of the calling context.
///
/// # Errors
///
/// Returns [`Error::SpicedPathOverrideNotRunnable`] when `SPICED_PATH` is set
/// to a path [`is_runnable_binary`] rejects.
fn resolve_spiced(
    managed_install: &Path,
    lookup: &SpicedLookup<'_>,
) -> Result<Option<ResolvedSpiced>> {
    if let Some(pinned) = lookup.pinned.as_ref().filter(|value| !value.is_empty()) {
        let pinned = PathBuf::from(pinned);
        // A pin that names nothing runnable is a mistake to report, never a
        // reason to start a different runtime than the one that was asked for.
        ensure!(
            (lookup.is_binary)(&pinned),
            SpicedPathOverrideNotRunnableSnafu {
                path: pinned.display().to_string(),
            }
        );
        return Ok(Some(ResolvedSpiced::at(pinned, SpicedSource::Pinned)?));
    }

    if let Some(dir) = lookup.current_exe.as_deref().and_then(Path::parent) {
        let sibling = dir.join(SPICED_FILENAME);
        if (lookup.is_binary)(&sibling) {
            // An ordinary install puts `spice` and `spiced` in the same managed
            // directory, so the sibling *is* the managed install. Reporting it
            // as `Sibling` would make [`SpicedSource::is_expected_default`]
            // false for the commonest layout there is, and `spice run` would
            // announce its runtime on every single run — the announcement means
            // "this is not the runtime you installed", so it has to stay rare
            // enough to be worth reading.
            let source = if same_file(&sibling, managed_install) {
                SpicedSource::ManagedInstall
            } else {
                SpicedSource::Sibling
            };
            return Ok(Some(ResolvedSpiced::at(sibling, source)?));
        }
    }

    if let Some(search_path) = lookup.search_path.as_ref() {
        for dir in std::env::split_paths(search_path) {
            // A relative entry — empty, `.`, `./bin` — resolves against the
            // working directory, and the child is spawned in `--dir` rather
            // than here. Honouring one would make `spice run` start a
            // different runtime depending on where it was run from, which is
            // the failure mode `.` on `PATH` is notorious for.
            if !dir.is_absolute() {
                continue;
            }
            let candidate = dir.join(SPICED_FILENAME);
            if (lookup.is_binary)(&candidate) {
                return Ok(Some(ResolvedSpiced::at(candidate, SpicedSource::OnPath)?));
            }
        }
    }

    if (lookup.is_binary)(managed_install) {
        return Ok(Some(ResolvedSpiced::at(
            managed_install.to_path_buf(),
            SpicedSource::ManagedInstall,
        )?));
    }

    if let Some(invoker_home) = (lookup.sudo_invoker_home)() {
        let invoker_install = invoker_home
            .join(DOT_SPICE)
            .join("bin")
            .join(SPICED_FILENAME);
        if (lookup.is_binary)(&invoker_install) {
            return Ok(Some(ResolvedSpiced::at(
                invoker_install,
                SpicedSource::SudoInvokerInstall,
            )?));
        }
    }

    Ok(None)
}

/// The home directory of the user who invoked `sudo`, or `None` when not
/// running under `sudo` (or the user cannot be resolved).
///
/// Only consulted by the last rung of [`RuntimeContext::resolve_spiced`].
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
/// under `sudo` on the documented `spice cloud service install` path, so a `PATH`
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

    /// The context's own managed install, as `get_run_cmd` now takes it.
    ///
    /// These tests are about the *arguments* the command carries, not about
    /// which binary was selected — that is what the ladder tests cover.
    fn test_resolved(ctx: &RuntimeContext) -> ResolvedSpiced {
        ResolvedSpiced::at(ctx.spiced_path(), SpicedSource::ManagedInstall)
            .expect("an absolute managed path anchors")
    }

    /// Write a stand-in `spiced` that [`is_runnable_binary`] will accept.
    ///
    /// The execute bit is the point: a runtime without one is a half-written
    /// download, and the ladder is required to walk past it.
    fn write_mock_runtime(path: &Path) {
        std::fs::write(path, "mock").expect("create mock spiced");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
                .expect("make the mock spiced executable");
        }
    }

    /// Create a test context with a mocked spiced binary in an isolated temp directory.
    /// Returns the context and the `TempDir` (which must be kept alive for the test).
    fn create_test_context_with_runtime() -> (RuntimeContext, TempDir) {
        let temp_dir = TempDir::new().expect("create temp dir");
        let bin_dir = temp_dir.path().join("bin");
        std::fs::create_dir_all(&bin_dir).expect("create bin dir");
        let spiced_path = bin_dir.join(SPICED_FILENAME);
        write_mock_runtime(&spiced_path);

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
        // without the `dscl` step `sudo spice cloud service install` cannot find the
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

    /// The runtime the CLI ships beside, on `PATH`, or in the managed install
    /// directory: whichever of them exists, the context must find one.
    ///
    /// Deliberately asserts *that* a runtime resolves rather than which rung
    /// supplied it — the host running this suite has a `PATH` and a
    /// `current_exe` of its own, and pinning the rung here would make the test
    /// a statement about the developer's machine. The ordering is tested
    /// against [`resolve_spiced`] directly, where every rung is injected.
    #[test]
    fn a_context_with_an_install_in_its_own_bin_dir_finds_a_runtime() {
        let (ctx, _temp) = create_test_context_with_runtime();
        assert!(
            ctx.resolve_spiced()
                .expect("no SPICED_PATH is set")
                .is_some(),
            "an install in this context's own bin dir must resolve"
        );
        assert!(ctx.is_managed_runtime_installed());
    }

    /// Build a [`SpicedLookup`] over an invented host: `present` is the set of
    /// paths that name a binary, and nothing else exists.
    ///
    /// Every rung is injected, so these tests describe the ladder and not the
    /// machine they run on — a developer with `spiced` on `PATH`, or a suite
    /// running under `sudo`, would otherwise decide the outcome.
    fn ladder(
        env: &[(&str, &str)],
        current_exe: Option<&str>,
        sudo_home: Option<&str>,
        present: &[&str],
        managed_install: &str,
    ) -> Result<Option<ResolvedSpiced>> {
        let present: Vec<PathBuf> = present.iter().map(PathBuf::from).collect();
        let is_binary = |path: &Path| present.iter().any(|known| known == path);
        ladder_with(env, current_exe, sudo_home, managed_install, &is_binary)
    }

    /// [`ladder`] with the real filesystem predicate, for the two behaviours
    /// only a real file can express: a directory named `spiced`, and a runtime
    /// with no execute bit.
    fn ladder_with(
        env: &[(&str, &str)],
        current_exe: Option<&str>,
        sudo_home: Option<&str>,
        managed_install: &str,
        is_binary: &dyn Fn(&Path) -> bool,
    ) -> Result<Option<ResolvedSpiced>> {
        let env: HashMap<&str, OsString> = env
            .iter()
            .map(|(key, value)| (*key, OsString::from(*value)))
            .collect();
        let sudo_home = sudo_home.map(PathBuf::from);
        let read_sudo_home = || sudo_home.clone();

        resolve_spiced(
            Path::new(managed_install),
            &SpicedLookup {
                pinned: env.get(SPICED_PATH_ENV).cloned(),
                search_path: env.get(PATH_ENV).cloned(),
                current_exe: current_exe.map(PathBuf::from),
                sudo_invoker_home: &read_sudo_home,
                is_binary,
            },
        )
    }

    /// Join directories the way the platform writes `PATH`, through the stdlib
    /// inverse of the `split_paths` the ladder itself uses — so the test's
    /// notion of how `PATH` is encoded cannot drift from the code's.
    fn search_path(dirs: &[&str]) -> String {
        std::env::join_paths(dirs)
            .expect("join a PATH")
            .into_string()
            .expect("the test PATH is UTF-8")
    }

    fn expect_resolved(found: Result<Option<ResolvedSpiced>>) -> ResolvedSpiced {
        found
            .expect("the ladder must not error")
            .expect("the ladder must resolve a runtime")
    }

    /// The pin exists so a specific build can be tested without moving files
    /// onto `PATH`, which means it has to outrank every rung below it.
    #[test]
    fn a_spiced_path_pin_outranks_every_other_candidate() {
        let path = search_path(&["/usr/local/bin"]);
        let found = expect_resolved(ladder(
            &[("SPICED_PATH", "/builds/pinned/spiced"), ("PATH", &path)],
            Some("/usr/local/bin/spice"),
            Some("/home/operator"),
            &[
                "/builds/pinned/spiced",
                "/usr/local/bin/spiced",
                "/home/me/.spice/bin/spiced",
                "/home/operator/.spice/bin/spiced",
            ],
            "/home/me/.spice/bin/spiced",
        ));
        assert_eq!(found.path, PathBuf::from("/builds/pinned/spiced"));
        assert_eq!(found.source, SpicedSource::Pinned);
    }

    /// A mistyped pin must not quietly start a *different* runtime — the whole
    /// point of pinning is that the binary is not in doubt. Every other rung is
    /// available here, so falling through would look like success.
    #[test]
    fn a_spiced_path_pin_that_names_nothing_is_an_error_not_a_fallthrough() {
        let path = search_path(&["/usr/local/bin"]);
        let error = ladder(
            &[("SPICED_PATH", "/builds/typo/spiced"), ("PATH", &path)],
            Some("/usr/local/bin/spice"),
            Some("/home/operator"),
            &[
                "/usr/local/bin/spiced",
                "/home/me/.spice/bin/spiced",
                "/home/operator/.spice/bin/spiced",
            ],
            "/home/me/.spice/bin/spiced",
        )
        .expect_err("a pin naming nothing must be reported");
        let message = error.to_string();
        assert!(
            message.contains("/builds/typo/spiced"),
            "the message must name the pin that failed: {message}"
        );
        assert!(
            message.contains("SPICED_PATH"),
            "the message must name the variable to fix: {message}"
        );
    }

    /// An unset variable and an empty one read the same from a shell.
    #[test]
    fn an_empty_spiced_path_is_not_a_pin() {
        let found = expect_resolved(ladder(
            &[("SPICED_PATH", ""), ("PATH", "")],
            None,
            None,
            &["/home/me/.spice/bin/spiced"],
            "/home/me/.spice/bin/spiced",
        ));
        assert_eq!(found.source, SpicedSource::ManagedInstall);
    }

    /// The reported bug: a `spice` and `spiced` built together and run off a
    /// build directory must pair with each other, not with whatever release is
    /// sitting in the managed install directory.
    #[test]
    fn the_sibling_of_the_running_cli_outranks_path_and_the_managed_install() {
        let path = search_path(&["/usr/local/bin"]);
        let found = expect_resolved(ladder(
            &[("PATH", &path)],
            Some("/builds/trunk/spice"),
            Some("/home/operator"),
            &[
                "/builds/trunk/spiced",
                "/usr/local/bin/spiced",
                "/home/me/.spice/bin/spiced",
                "/home/operator/.spice/bin/spiced",
            ],
            "/home/me/.spice/bin/spiced",
        ));
        assert_eq!(found.path, PathBuf::from("/builds/trunk/spiced"));
        assert_eq!(found.source, SpicedSource::Sibling);
    }

    /// The second half of the same bug: a pair installed into a prefix on
    /// `PATH` must not be shadowed by the managed install directory.
    #[test]
    fn path_outranks_the_managed_install() {
        let path = search_path(&["/opt/spice/bin", "/usr/local/bin"]);
        let found = expect_resolved(ladder(
            &[("PATH", &path)],
            Some("/builds/trunk/spice"),
            None,
            &[
                "/opt/spice/bin/spiced",
                "/usr/local/bin/spiced",
                "/home/me/.spice/bin/spiced",
            ],
            "/home/me/.spice/bin/spiced",
        ));
        assert_eq!(
            found.path,
            PathBuf::from("/opt/spice/bin/spiced"),
            "PATH is searched in order, and the CLI has no sibling here"
        );
        assert_eq!(found.source, SpicedSource::OnPath);
    }

    /// An entry that is not absolute resolves against the working directory,
    /// so honouring one would make `spice run` start a different runtime
    /// depending on where it was run from — the failure mode `.` on `PATH` is
    /// notorious for. Every spelling of it is refused, not just the empty one.
    #[test]
    fn a_relative_path_entry_does_not_supply_the_runtime() {
        for relative in ["", ".", "./bin", "../bin", "bin"] {
            let path = search_path(&[relative, "/usr/local/bin"]);
            let found = expect_resolved(ladder(
                &[("PATH", &path)],
                None,
                None,
                &[
                    "spiced",
                    "./spiced",
                    "./bin/spiced",
                    "../bin/spiced",
                    "bin/spiced",
                    "/usr/local/bin/spiced",
                ],
                "/home/me/.spice/bin/spiced",
            ));
            assert_eq!(
                found.path,
                PathBuf::from("/usr/local/bin/spiced"),
                "a '{relative}' entry on PATH must not supply the runtime"
            );
        }
    }

    /// A relative entry is refused rather than merely outranked: with nothing
    /// absolute on `PATH`, the ladder falls through to the managed install
    /// instead of running whatever the working directory holds.
    #[test]
    fn a_relative_path_entry_falls_through_to_the_managed_install() {
        let path = search_path(&[".", "./bin"]);
        let found = expect_resolved(ladder(
            &[("PATH", &path)],
            None,
            None,
            &["./spiced", "./bin/spiced", "/home/me/.spice/bin/spiced"],
            "/home/me/.spice/bin/spiced",
        ));
        assert_eq!(found.path, PathBuf::from("/home/me/.spice/bin/spiced"));
        assert_eq!(found.source, SpicedSource::ManagedInstall);
    }

    /// `sudo` rewrites `HOME`, so a runtime installed under the invoking user's
    /// home must still be found — otherwise `sudo spice cloud service install`
    /// concludes the runtime is missing and downloads a release over the
    /// operator's build. It stays the *last* rung: this context's own install
    /// is the one this user asked for.
    #[test]
    fn the_managed_install_outranks_the_sudo_invokers() {
        let found = expect_resolved(ladder(
            &[("PATH", "")],
            None,
            Some("/home/operator"),
            &[
                "/root/.spice/bin/spiced",
                "/home/operator/.spice/bin/spiced",
            ],
            "/root/.spice/bin/spiced",
        ));
        assert_eq!(found.path, PathBuf::from("/root/.spice/bin/spiced"));
        assert_eq!(found.source, SpicedSource::ManagedInstall);
    }

    #[test]
    fn the_sudo_invokers_install_is_the_last_resort() {
        let found = expect_resolved(ladder(
            &[("PATH", "")],
            None,
            Some("/home/operator"),
            &["/home/operator/.spice/bin/spiced"],
            "/root/.spice/bin/spiced",
        ));
        assert_eq!(
            found.path,
            PathBuf::from("/home/operator/.spice/bin/spiced")
        );
        assert_eq!(found.source, SpicedSource::SudoInvokerInstall);
    }

    /// `Ok(None)` — and not an error — is what tells the launcher to install.
    #[test]
    fn nothing_anywhere_resolves_to_none_so_the_launcher_installs() {
        let path = search_path(&["/usr/local/bin", "/usr/bin"]);
        assert_eq!(
            ladder(
                &[("PATH", &path)],
                Some("/builds/trunk/spice"),
                Some("/home/operator"),
                &[],
                "/home/me/.spice/bin/spiced",
            )
            .expect("an absent runtime is not an error"),
            None
        );
    }

    /// `current_exe()` is allowed to fail; the ladder must carry on down it.
    #[test]
    fn an_unknown_current_exe_falls_through_to_the_lower_rungs() {
        let path = search_path(&["/usr/local/bin"]);
        let found = expect_resolved(ladder(
            &[("PATH", &path)],
            None,
            None,
            &["/usr/local/bin/spiced"],
            "/home/me/.spice/bin/spiced",
        ));
        assert_eq!(found.source, SpicedSource::OnPath);
    }

    /// A directory named `spiced` on `PATH` must not shadow the real binary.
    /// Runs against the real filesystem predicate, which is the thing under
    /// test here — the injected one cannot tell a directory from a file.
    #[test]
    fn a_directory_named_spiced_on_path_does_not_shadow_the_runtime() {
        let temp = TempDir::new().expect("create temp dir");
        let decoy = temp.path().join("decoy");
        let real = temp.path().join("real");
        std::fs::create_dir_all(decoy.join(SPICED_FILENAME)).expect("create decoy directory");
        std::fs::create_dir_all(&real).expect("create real bin dir");
        write_mock_runtime(&real.join(SPICED_FILENAME));

        assert!(
            !is_runnable_binary(&decoy.join(SPICED_FILENAME)),
            "a directory does not name a runtime binary"
        );
        let path = search_path(&[&decoy.to_string_lossy(), &real.to_string_lossy()]);
        let found = ladder_with(
            &[("PATH", &path)],
            None,
            None,
            "/home/me/.spice/bin/spiced",
            &is_runnable_binary,
        )
        .expect("no SPICED_PATH is set")
        .expect("the real binary must still resolve");
        assert_eq!(found.path, real.join(SPICED_FILENAME));
    }

    /// Only the managed install is silent at launch. Every other source is a
    /// pairing the user cannot otherwise see, so it has to be announced.
    #[test]
    fn only_the_managed_install_launches_without_announcing_itself() {
        for source in [
            SpicedSource::Pinned,
            SpicedSource::Sibling,
            SpicedSource::OnPath,
            SpicedSource::SudoInvokerInstall,
        ] {
            assert!(
                !source.is_expected_default(),
                "{source:?} must be announced at launch"
            );
        }
        assert!(SpicedSource::ManagedInstall.is_expected_default());
    }

    /// The ordinary install puts `spice` and `spiced` in the same managed
    /// directory, so the sibling rung finds the managed install itself. It has
    /// to be reported as such: `is_expected_default` is what keeps `spice run`
    /// from announcing its runtime, and announcing on the commonest layout there
    /// is would train everyone to ignore the line that exists to say "this is
    /// not the runtime you installed".
    // POSIX path literals: on Windows these are rooted but not absolute, so
    // anchoring rewrites them and the equality below would not hold.
    #[cfg(unix)]
    #[test]
    fn the_sibling_that_is_the_managed_install_is_reported_as_managed() {
        let managed = "/home/me/.spice/bin/spiced";
        let is_binary = |candidate: &Path| candidate == Path::new(managed);
        let found = ladder_with(
            &[],
            Some("/home/me/.spice/bin/spice"),
            None,
            managed,
            &is_binary,
        )
        .expect("the ladder must not error")
        .expect("the managed install resolves");

        assert_eq!(found.path, PathBuf::from(managed));
        assert_eq!(
            found.source,
            SpicedSource::ManagedInstall,
            "the sibling and the managed install are one file here"
        );
        assert!(
            found.source.is_expected_default(),
            "the ordinary layout must not be announced on every run"
        );
    }

    /// A sibling that is *not* the managed install stays `Sibling` — that is the
    /// case the rung was added for, and the one worth announcing.
    // POSIX path literals: on Windows these are rooted but not absolute, so
    // anchoring rewrites them and the equality below would not hold.
    #[cfg(unix)]
    #[test]
    fn a_sibling_outside_the_managed_dir_is_still_reported_as_a_sibling() {
        let sibling = "/usr/local/bin/spiced";
        let is_binary = |candidate: &Path| candidate == Path::new(sibling);
        let found = ladder_with(
            &[],
            Some("/usr/local/bin/spice"),
            None,
            "/home/me/.spice/bin/spiced",
            &is_binary,
        )
        .expect("the ladder must not error")
        .expect("the sibling resolves");

        assert_eq!(found.source, SpicedSource::Sibling);
        assert!(
            !found.source.is_expected_default(),
            "a runtime from outside the managed install must be announced"
        );
    }

    /// A candidate that cannot be anchored is refused, not passed on. Returning
    /// it unanchored is what let `Command::new` re-resolve a bare name through
    /// `PATH` and start a different binary than the one just validated, and by
    /// this point the candidate has already passed its runnable check — so the
    /// substitution would be silent.
    ///
    /// The resolver is injected because the real one only fails when the working
    /// directory cannot be read, which cannot be arranged for one test without
    /// moving the whole process.
    #[test]
    fn a_candidate_that_cannot_be_anchored_is_refused() {
        let unreadable_cwd = |_: &Path| {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "working directory is gone",
            ))
        };
        let error = anchor_with(PathBuf::from("spiced"), unreadable_cwd)
            .expect_err("an unanchorable candidate must not resolve");
        let message = error.to_string();
        assert!(
            message.contains("spiced"),
            "the error must name the candidate: {message}"
        );
        assert!(
            message.contains("absolute path"),
            "the error must give a way out: {message}"
        );
    }

    /// An absolute candidate has nothing to anchor against, so it never consults
    /// the working directory — and so cannot fail when that directory is
    /// unreadable. Injecting a resolver that would fail is what proves the
    /// short-circuit is real rather than incidental.
    #[test]
    fn an_absolute_candidate_never_consults_the_working_directory() {
        let must_not_be_called = |_: &Path| {
            panic!("an absolute candidate must not be resolved against the working directory")
        };
        // Derived rather than written out: a POSIX-looking literal is rooted but
        // not absolute on Windows, which has no drive prefix on it, so the
        // short-circuit under test would not even be reached there.
        let absolute = std::env::current_dir()
            .expect("the test process has a working directory")
            .join("spiced");
        let anchored = anchor_with(absolute.clone(), must_not_be_called)
            .expect("an absolute path anchors to itself");
        assert_eq!(anchored, absolute);
    }

    /// A file without an execute bit is a half-written download, not a runtime.
    /// It must not win a rung — winning would both shadow a working binary
    /// further down and suppress the auto-install that would repair it.
    ///
    /// Unix only, because the premise is: Windows has no execute bit, so
    /// `grants_execute` accepts every file there and a "broken" candidate is
    /// indistinguishable from a working one.
    #[cfg(unix)]
    #[test]
    fn a_non_executable_file_does_not_win_the_ladder() {
        let temp = TempDir::new().expect("create temp dir");
        let broken = temp.path().join("broken");
        let working = temp.path().join("working");
        std::fs::create_dir_all(&broken).expect("create broken bin dir");
        std::fs::create_dir_all(&working).expect("create working bin dir");
        std::fs::write(broken.join(SPICED_FILENAME), "half a download")
            .expect("create the unusable runtime");
        write_mock_runtime(&working.join(SPICED_FILENAME));

        assert!(
            !is_runnable_binary(&broken.join(SPICED_FILENAME)),
            "a file with no execute bit is not a runnable runtime"
        );

        let path = search_path(&[&broken.to_string_lossy(), &working.to_string_lossy()]);
        let found = ladder_with(
            &[("PATH", &path)],
            None,
            None,
            "/home/me/.spice/bin/spiced",
            &is_runnable_binary,
        )
        .expect("no SPICED_PATH is set")
        .expect("the working runtime must still resolve");
        assert_eq!(found.path, working.join(SPICED_FILENAME));
    }

    /// The runtime is spawned with the child's working directory set to
    /// `--dir`, so a relative candidate checked here and handed to
    /// `Command::new` unchanged would be re-interpreted against a different
    /// directory — and a bare name would become a fresh `PATH` lookup, which is
    /// not the file that was checked at all.
    #[test]
    fn a_relative_candidate_is_anchored_to_the_directory_it_was_checked_in() {
        let is_binary = |candidate: &Path| candidate == Path::new("build/spiced");
        let found = ladder_with(
            &[("SPICED_PATH", "build/spiced")],
            None,
            None,
            "/home/me/.spice/bin/spiced",
            &is_binary,
        )
        .expect("the pin names a binary")
        .expect("the pin must resolve");
        assert!(
            found.path.is_absolute(),
            "a relative pin must be anchored, got {}",
            found.path.display()
        );
        assert!(
            found.path.ends_with("build/spiced"),
            "anchoring must not change which file was chosen, got {}",
            found.path.display()
        );
    }

    /// Exposing the managed install through a symlink on `PATH` is a normal
    /// layout. The two paths differ lexically while naming one file, and
    /// treating that as shadowing would warn that the install has no effect
    /// when it is the very file about to run.
    ///
    /// Unix only: this needs a symlink, and the fallback the non-Unix path takes
    /// is the lexical comparison the other case already covers.
    #[cfg(unix)]
    #[test]
    fn a_symlink_to_the_managed_install_is_not_shadowing_it() {
        let temp = TempDir::new().expect("create temp dir");
        let managed = temp.path().join("managed-spiced");
        let exposed = temp.path().join("exposed-spiced");
        write_mock_runtime(&managed);
        std::os::unix::fs::symlink(&managed, &exposed).expect("link the managed install onto PATH");

        assert!(
            same_file(&exposed, &managed),
            "a symlink and its target are one file, so neither shadows the other"
        );
        assert!(
            !same_file(&temp.path().join("other-spiced"), &managed),
            "two genuinely different paths must still compare as different"
        );
    }

    /// `spice install` and `spice upgrade` only ever write to the managed
    /// directory, so when something outranks it the warning is the only
    /// explanation a user gets for an install that changes nothing.
    #[test]
    fn the_shadowed_install_warning_names_both_binaries_and_the_way_out() {
        let warning = shadowed_install_warning(
            Path::new("/home/me/.spice/bin/spiced"),
            &ResolvedSpiced {
                path: PathBuf::from("/usr/local/bin/spiced"),
                source: SpicedSource::OnPath,
            },
        );
        assert!(
            warning.contains("/home/me/.spice/bin/spiced"),
            "the warning must name the managed install: {warning}"
        );
        assert!(
            warning.contains("/usr/local/bin/spiced"),
            "the warning must name what will actually run: {warning}"
        );
        assert!(
            warning.contains("found on PATH"),
            "the warning must say where the shadowing binary came from: {warning}"
        );
        assert!(
            warning.contains("SPICED_PATH"),
            "the warning must give a way out: {warning}"
        );
        assert!(
            warning.contains("https://spiceai.org/docs"),
            "the warning must link the docs: {warning}"
        );
    }

    /// A pin that names nothing runnable makes every rung unreachable, so the
    /// install the caller just reported cannot be started by anything. The
    /// caller keeps its success; this line is the only account of why it will
    /// not take effect.
    #[test]
    fn the_unselectable_install_warning_names_the_install_and_carries_its_cause() {
        let cause = SpicedPathOverrideNotRunnableSnafu {
            path: "/tmp/typo/spiced".to_string(),
        }
        .build();
        let warning = unselectable_install_warning(Path::new("/home/me/.spice/bin/spiced"), &cause);

        assert!(
            warning.contains("/home/me/.spice/bin/spiced"),
            "the warning must name the install it is about: {warning}"
        );
        assert!(
            warning.contains("fail rather than start this one"),
            "the warning must state the consequence, not just the failure: {warning}"
        );
        assert!(
            warning.contains("/tmp/typo/spiced"),
            "the cause must name the pin that cannot be resolved: {warning}"
        );
        assert!(
            warning.contains("SPICED_PATH"),
            "the cause must name the setting to change: {warning}"
        );
        assert!(
            warning.contains("https://spiceai.org/docs"),
            "the line a user sees must link the docs: {warning}"
        );
    }

    /// Which arm fires, asserted by equality against the wording each one must
    /// reach — the two wording tests above own the text itself. Asserted here
    /// rather than through [`warn_if_install_is_shadowed`], which reads
    /// `SPICED_PATH` and `PATH` from the process and so cannot be steered
    /// without mutating state every other test in this binary shares.
    #[test]
    fn a_resolution_that_fails_is_warned_about_rather_than_swallowed() {
        let installed = PathBuf::from("/home/me/.spice/bin/spiced");
        let cause = SpicedPathOverrideNotRunnableSnafu {
            path: "/tmp/typo/spiced".to_string(),
        }
        .build();

        assert_eq!(
            install_shadow_warning(&installed, &Err(cause)),
            Some(unselectable_install_warning(
                &installed,
                &SpicedPathOverrideNotRunnableSnafu {
                    path: "/tmp/typo/spiced".to_string(),
                }
                .build()
            )),
            "a resolution that failed must be reported, not returned from silently"
        );
    }

    /// No candidate at all means the install just written is the only runtime
    /// there is, so there is nothing to warn about.
    #[test]
    fn nothing_resolved_is_not_a_shadowed_install() {
        assert_eq!(
            install_shadow_warning(Path::new("/home/me/.spice/bin/spiced"), &Ok(None)),
            None
        );
    }

    /// The managed install resolving *is* the expected outcome, and warning
    /// there would fire on every ordinary `spice install`.
    #[test]
    fn the_managed_install_resolving_is_not_a_shadowed_install() {
        let installed = PathBuf::from("/home/me/.spice/bin/spiced");
        let resolved = ResolvedSpiced {
            path: installed.clone(),
            source: SpicedSource::ManagedInstall,
        };
        assert_eq!(
            install_shadow_warning(&installed, &Ok(Some(resolved))),
            None
        );
    }

    /// Something else resolving is the case the whole function exists for.
    #[test]
    fn another_runtime_resolving_is_a_shadowed_install() {
        let installed = PathBuf::from("/home/me/.spice/bin/spiced");
        let resolved = ResolvedSpiced {
            path: PathBuf::from("/usr/local/bin/spiced"),
            source: SpicedSource::OnPath,
        };

        assert_eq!(
            install_shadow_warning(&installed, &Ok(Some(resolved.clone()))),
            Some(shadowed_install_warning(&installed, &resolved)),
            "a runtime outranking the managed install must be reported"
        );
    }

    /// The public entry points over a real, empty install directory.
    ///
    /// Asserts the *managed* question, which is hermetic: whatever the host's
    /// `PATH` and `current_exe` hold, an empty bin dir holds no runtime. The
    /// ladder's own outcome depends on the machine, so it is asserted against
    /// [`resolve_spiced`] with every rung injected instead.
    #[test]
    fn an_empty_bin_dir_holds_no_managed_runtime() {
        let temp = TempDir::new().expect("create temp dir");
        let ctx = RuntimeContext::with_bin_dir_for_test(temp.path().join("bin"));
        assert!(!ctx.spiced_path().exists());
        assert!(!ctx.is_managed_runtime_installed());
        ctx.managed_runtime_version()
            .expect_err("an empty bin dir has no version to report");
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
            .get_run_cmd(&test_resolved(&ctx), &[], None)
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
            .get_run_cmd(&test_resolved(&ctx), &[], None)
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
            .get_run_cmd(&test_resolved(&ctx), &[], Some("http://0.0.0.0:9999"))
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
            .get_run_cmd(&test_resolved(&ctx), &[], Some("http://192.168.1.1:8080"))
            .expect("get_run_cmd should succeed");
        let args = get_cmd_args(&cmd);
        assert!(
            args.contains(&"192.168.1.1:8080".to_string()),
            "Should strip http:// prefix, got: {args:?}"
        );

        // Test with https:// prefix
        let cmd = ctx
            .get_run_cmd(
                &test_resolved(&ctx),
                &[],
                Some("https://secure.example.com:443"),
            )
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
            .get_run_cmd(&test_resolved(&ctx), &[], None)
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
            .get_run_cmd(&test_resolved(&ctx), &[], None)
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
            .get_run_cmd(&test_resolved(&ctx), &[], None)
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
            .get_run_cmd(&test_resolved(&ctx), &[], None)
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
            .get_run_cmd(&test_resolved(&ctx), &[], None)
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
            .get_run_cmd(&test_resolved(&ctx), &[], None)
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
            .get_run_cmd(&test_resolved(&ctx), &extra_args, None)
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
            .get_run_cmd(&test_resolved(&ctx), &extra_args, None)
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

    /// The command runs the binary it was handed, not one it re-derives.
    ///
    /// Resolution happens once, above this, and its answer is what the launcher
    /// reports — so re-resolving here could announce one runtime and start
    /// another. An absent runtime is now caught there, before the install
    /// decision, rather than by this function failing to find one.
    #[test]
    fn get_run_cmd_runs_the_runtime_it_was_given() {
        let (ctx, _temp) = create_test_context_with_runtime();
        let elsewhere =
            ResolvedSpiced::at(PathBuf::from("/opt/spice/bin/spiced"), SpicedSource::OnPath)
                .expect("an absolute path anchors");
        let cmd = ctx
            .get_run_cmd(&elsewhere, &[], None)
            .expect("building the command must not re-resolve");

        assert_eq!(
            Path::new(cmd.get_program()),
            elsewhere.path,
            "the command must run the resolved binary, not the managed install"
        );
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
