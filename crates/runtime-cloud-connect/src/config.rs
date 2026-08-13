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

//! Configuration for the Cloud Connect client.

use std::path::{Path, PathBuf};
use std::time::Duration;

use snafu::Snafu;

/// Default enroll endpoint for the Spice Cloud control plane (state
/// plane): the base URL the out-of-band HTTPS `/v1/cloud-connect/enroll`
/// and `/v1/cloud-connect/renew` requests are made against. The gateway
/// (stream) address is returned by the enroll response, not configured.
pub const DEFAULT_ENDPOINT: &str = "https://api.spice.ai";

#[derive(Debug, Snafu)]
#[snafu(display(
    "the control-plane endpoint must be an absolute HTTPS base URL without credentials, query, or fragment (plain HTTP is allowed only for a loopback fixture)"
))]
pub struct InvalidControlPlaneEndpoint;

#[derive(Debug, Snafu)]
pub enum EnrollmentEndpointOverrideError {
    #[snafu(display(
        "Failed to read the Cloud Connect endpoint override at {}: {source}",
        path.display()
    ))]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "The Cloud Connect endpoint override at {} is invalid: {source}",
        path.display()
    ))]
    Invalid {
        path: PathBuf,
        source: InvalidControlPlaneEndpoint,
    },
}

/// Validate and canonicalize a Cloud Connect control-plane base URL.
///
/// # Errors
///
/// Returns [`InvalidControlPlaneEndpoint`] for unsafe or non-base URLs.
pub fn normalize_control_plane_endpoint(
    endpoint: &str,
) -> std::result::Result<String, InvalidControlPlaneEndpoint> {
    let parsed = reqwest::Url::parse(endpoint).map_err(|_| InvalidControlPlaneEndpoint)?;
    if !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.host_str().is_none()
    {
        return Err(InvalidControlPlaneEndpoint);
    }
    let local_http = parsed.scheme() == "http" && parsed.host_str().is_some_and(is_loopback_host);
    if parsed.scheme() != "https" && !local_http {
        return Err(InvalidControlPlaneEndpoint);
    }
    Ok(parsed.to_string().trim_end_matches('/').to_string())
}

fn is_loopback_host(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// Default lead time before the identity cert's `not_after` at which the
/// client renews. The cloud issues 24h leaves, so a 12h lead yields the
/// ~12h renewal cadence of the BYOC operator.
pub const DEFAULT_RENEWAL_LEAD: Duration = Duration::from_hours(12);

/// Default cadence for `Heartbeat` frames on an established stream.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Default cadence for `Telemetry` frames on an established stream.
pub const DEFAULT_TELEMETRY_INTERVAL: Duration = Duration::from_mins(1);

/// Default cadence for `ExportMetrics` frames on an established stream.
///
/// Matches the heartbeat cadence: the payload carries cumulative totals, so the
/// interval sets chart resolution rather than what is or is not recorded.
pub const DEFAULT_METRICS_INTERVAL: Duration = Duration::from_secs(30);

/// Default ceiling on a single `ExecuteQuery`.
///
/// There is no cancellation command on this contract, and one query runs at a
/// time, so a query that never finishes would hold the only slot for the life
/// of the process and answer every later query as busy. The deadline is what
/// bounds that: it releases the slot and abandons the work.
///
/// It sits below the control plane's own command budget so the instance is the
/// layer that gives up first. The slot is then free by the time the caller is
/// told the query failed, and the retry it prompts is not answered busy by the
/// query it is replacing.
pub const DEFAULT_QUERY_DEADLINE: Duration = Duration::from_secs(25);

/// File name (relative to `$SPICE_CONFIG_DIR`) where the cloud-managed
/// spicepod is written when an `ApplySpicepod` command arrives.
pub const CLOUD_MANAGED_SPICEPOD_FILE: &str = "spicepod-cloud-managed.yml";

/// File name (relative to `$SPICE_CONFIG_DIR`) where the runtime identity
/// is persisted after enrollment.
pub const IDENTITY_FILE: &str = "identity.json";

/// Runtime config for the Cloud Connect client.
#[derive(Debug, Clone)]
pub struct CloudConnectConfig {
    /// Cloud enroll endpoint (state plane), e.g. `https://api.spice.ai`.
    /// Base URL for the out-of-band HTTPS `/v1/cloud-connect/enroll` and
    /// `/v1/cloud-connect/renew` requests. This is **not** the stream
    /// endpoint: the gateway address for the mTLS `CloudConnect` stream
    /// comes back in the enroll response and is persisted in the identity.
    pub enroll_endpoint: String,

    /// Optional explicit gateway (stream) endpoint override, e.g.
    /// `https://connect.aws.spiceai.io:443`. When `None` (the default),
    /// the stream connects to the `gateway_addr` returned by the enroll
    /// response and persisted in the identity. Mainly for tests and
    /// self-hosted control planes.
    pub gateway_endpoint: Option<String>,

    /// Optional PEM-encoded CA certificate to verify the server. When
    /// `None`, the system `WebPKI` roots are used. Mainly for self-hosted
    /// control planes during development.
    pub ca_cert_pem: Option<String>,

    /// When `true`, TLS is **disabled entirely** and the runtime connects
    /// to the gateway over a plaintext (h2c) channel — not merely with
    /// certificate verification turned off. For local development only;
    /// never enable against a real control plane. Defaults to `false`.
    pub insecure: bool,

    /// Path to the on-disk identity file (typically
    /// `$SPICE_CONFIG_DIR/identity.json`).
    pub identity_path: PathBuf,

    /// Directory where cloud-managed spicepod overlays should be written
    /// when an `ApplySpicepod` command arrives, and where the enrollment
    /// draft lives while an enrollment is in flight.
    pub config_dir: PathBuf,

    /// Where this instance runs (`--region`) — a customer-declared label,
    /// not a probed fact, so it rides the enroll request as a sibling of
    /// the host facts rather than as one of them. The cloud records it on
    /// the registry row and resolves the instance's gateway stamp from it.
    ///
    /// `None` means "leave the stored value alone": a re-enroll after the
    /// control plane rejects the old credential must not erase a region set in
    /// the portal.
    pub instance_region: Option<String>,

    /// Runtime semver-like string (`v2.0.0-build.deadbeef`). Sent in
    /// `Hello.runtime_version`.
    pub runtime_version: String,

    /// Cadence for `Heartbeat` frames once a stream is established.
    /// Defaults to [`DEFAULT_HEARTBEAT_INTERVAL`]; overridable so tests can
    /// exercise heartbeat cadence without waiting the production interval.
    pub heartbeat_interval: Duration,

    /// Cadence for `Telemetry` frames once a stream is established.
    /// Defaults to [`DEFAULT_TELEMETRY_INTERVAL`].
    pub telemetry_interval: Duration,

    /// Cadence for `ExportMetrics` frames once a stream is established.
    /// Defaults to [`DEFAULT_METRICS_INTERVAL`].
    pub metrics_interval: Duration,

    /// Lead time before the identity cert's `not_after` at which the
    /// client renews (fresh keypair + CSR against `/renew`). Defaults to
    /// [`DEFAULT_RENEWAL_LEAD`]; overridable so tests can exercise the
    /// renewal loop without waiting hours.
    pub renewal_lead: Duration,

    /// How long an `ExecuteQuery` may run before the instance abandons it and frees
    /// the query slot. Defaults to [`DEFAULT_QUERY_DEADLINE`]; overridable so
    /// tests can exercise the deadline without waiting it out.
    pub query_deadline: Duration,
}

impl CloudConnectConfig {
    /// Resolve the control-stream endpoint for a persisted identity.
    ///
    /// A configured override is already a complete URL. Otherwise the enroll
    /// response supplies `host:port`, and transport mode supplies the scheme.
    #[must_use]
    pub fn stream_endpoint(&self, identity: &crate::identity::Identity) -> Option<String> {
        if let Some(ref endpoint) = self.gateway_endpoint {
            return Some(endpoint.clone());
        }
        if identity.gateway_addr.trim().is_empty() {
            return None;
        }
        let scheme = if self.insecure { "http" } else { "https" };
        Some(format!("{scheme}://{}", identity.gateway_addr))
    }

    /// Resolve and validate the persisted control-stream endpoint shape.
    ///
    /// Durable activation deliberately ignores the process-local gateway
    /// override. The override may redirect the running client's reconnect
    /// attempts, but a typo or a service environment difference must not make
    /// consumers disagree about whether the persisted identity is usable.
    pub(crate) fn validated_persisted_gateway_endpoint(
        &self,
        identity: &crate::identity::Identity,
    ) -> Result<String, &'static str> {
        if identity.gateway_addr.trim().is_empty() {
            return Err("the gateway address is empty");
        }
        let expected_scheme = if self.insecure { "http" } else { "https" };
        let endpoint = format!("{expected_scheme}://{}", identity.gateway_addr);
        let uri = endpoint
            .parse::<http::Uri>()
            .map_err(|_| "the gateway endpoint is invalid")?;
        if uri.scheme_str() != Some(expected_scheme) {
            return Err("the gateway endpoint uses the wrong transport scheme");
        }
        if uri.host().is_none() {
            return Err("the gateway endpoint has no host");
        }
        match uri.port_u16() {
            None => return Err("the gateway endpoint has no explicit port"),
            Some(0) => return Err("the gateway endpoint port must be greater than zero"),
            Some(_) => {}
        }
        if uri
            .path_and_query()
            .is_some_and(|path_and_query| path_and_query.as_str() != "/")
        {
            return Err("the gateway endpoint must not contain a path or query");
        }
        Ok(endpoint)
    }

    /// Read the optional instance-local enrollment endpoint override without
    /// following symlinks or opening special files in blocking mode.
    ///
    /// # Errors
    ///
    /// Returns an error when the path exists but is unreadable or is not a
    /// regular file.
    pub fn read_enroll_endpoint_override(config_dir: &Path) -> std::io::Result<Option<String>> {
        let path = config_dir.join("cloud-endpoint");
        crate::identity::read_regular_file_optional(&path).map(|contents| {
            contents
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
    }

    /// Read and normalize the legacy instance-local control-plane endpoint.
    ///
    /// Enrollment and state-management front ends share this helper so a
    /// fresh or pre-binding instance cannot choose different control planes in
    /// `spice` and `spiced`. Durable identity/draft bindings still take
    /// precedence at each caller; this file is only the fallback before such a
    /// binding exists.
    ///
    /// # Errors
    ///
    /// Returns an error when the file exists but is unsafe/unreadable, or when
    /// its non-empty value is not a safe control-plane base URL.
    pub fn read_normalized_enroll_endpoint_override(
        config_dir: &Path,
    ) -> std::result::Result<Option<String>, EnrollmentEndpointOverrideError> {
        let path = config_dir.join("cloud-endpoint");
        let Some(endpoint) = Self::read_enroll_endpoint_override(config_dir).map_err(|source| {
            EnrollmentEndpointOverrideError::Read {
                path: path.clone(),
                source,
            }
        })?
        else {
            return Ok(None);
        };
        normalize_control_plane_endpoint(&endpoint)
            .map(Some)
            .map_err(|source| EnrollmentEndpointOverrideError::Invalid { path, source })
    }

    /// Resolve the Cloud Connect config directory to its canonical location.
    ///
    /// Precedence:
    /// 1. `$SPICE_CONFIG_DIR` env var (explicit override)
    /// 2. `./.spice` — a `.spice` directory in the current working
    ///    directory (the `spiced` instance's working directory)
    ///
    /// This deliberately does **not** fall back to the global `~/.spice`.
    /// Enrollment state (`identity.json`, the enrollment draft, and the
    /// `cloud-endpoint` override) is per-`spiced`-instance: several `spiced`
    /// processes can run on one machine and each must enroll into Spice Cloud
    /// independently. A shared `~/.spice` would make one machine present as a
    /// single runtime and let one instance's enrollment clobber another's, so
    /// the state is scoped to the working directory instead. The `spiced`
    /// binary itself still installs to the shared `~/.spice/bin` — only this
    /// per-instance state is local.
    #[must_use]
    pub fn default_config_dir() -> PathBuf {
        Self::resolve_config_dir(None)
    }

    /// Resolve the Cloud Connect config directory for an explicit instance
    /// directory (`spice connect --dir <path>`).
    ///
    /// Precedence:
    /// 1. `$SPICE_CONFIG_DIR` env var (explicit override, wins even over
    ///    `--dir` so a single knob controls every consumer of the config
    ///    dir)
    /// 2. `<instance_dir>/.spice` when an instance directory is given
    /// 3. `./.spice` (the current working directory)
    ///
    /// A relative `instance_dir` is resolved against the current working
    /// directory so the returned path is absolute whenever the cwd is
    /// available — the path is baked into installed services and must not
    /// depend on where a later process starts.
    #[must_use]
    pub fn resolve_config_dir(instance_dir: Option<&std::path::Path>) -> PathBuf {
        if let Ok(dir) = std::env::var("SPICE_CONFIG_DIR")
            && !dir.is_empty()
        {
            return PathBuf::from(dir);
        }
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        match instance_dir {
            Some(dir) if dir.is_absolute() => dir.join(".spice"),
            Some(dir) => cwd.join(dir).join(".spice"),
            None => cwd.join(".spice"),
        }
    }

    /// Resolve the canonical identity file path for the current
    /// environment.
    #[must_use]
    pub fn default_identity_path() -> PathBuf {
        Self::default_config_dir().join(IDENTITY_FILE)
    }

    /// Returns a config bootstrap from the environment.
    ///
    /// Enrollment is driven explicitly (a `--token` bootstrap, or a
    /// caller-provided authenticated-session authority); this config only
    /// locates state on disk and the endpoints. The enroll endpoint defaults to
    /// [`DEFAULT_ENDPOINT`]; override via the `SPICE_CLOUD_ENDPOINT` env
    /// var. The gateway (stream) endpoint normally comes from the enroll
    /// response; the `SPICE_CLOUD_GATEWAY_ENDPOINT` env var overrides it.
    #[must_use]
    pub fn from_env(runtime_version: impl Into<String>) -> Self {
        Self::from_env_at(runtime_version, Self::default_config_dir())
    }

    /// [`Self::from_env`] with the config directory pinned by the caller
    /// instead of resolved from the environment — used by `spice connect
    /// --dir <path>`, where the instance directory is an explicit argument.
    #[must_use]
    pub fn from_env_at(runtime_version: impl Into<String>, config_dir: PathBuf) -> Self {
        let identity_path = config_dir.join(IDENTITY_FILE);

        let enroll_endpoint = std::env::var("SPICE_CLOUD_ENDPOINT")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
        let gateway_endpoint = std::env::var("SPICE_CLOUD_GATEWAY_ENDPOINT")
            .ok()
            .filter(|v| !v.is_empty());

        Self {
            enroll_endpoint,
            gateway_endpoint,
            ca_cert_pem: None,
            insecure: false,
            identity_path,
            config_dir,
            instance_region: None,
            runtime_version: runtime_version.into(),
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            telemetry_interval: DEFAULT_TELEMETRY_INTERVAL,
            metrics_interval: DEFAULT_METRICS_INTERVAL,
            renewal_lead: DEFAULT_RENEWAL_LEAD,
            query_deadline: DEFAULT_QUERY_DEADLINE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// The instance must be the layer that gives up first, because it is the
    /// only one holding the query slot. If the control plane answers the caller
    /// while the query runs on, the retry that answer invites is refused as busy
    /// by the query it is replacing, and the deadline's own result code never
    /// reaches anyone.
    ///
    /// Serializing the result is inside the deadline, not inside the margin:
    /// the encode runs within the future the deadline wraps. The margin is what
    /// putting the finished payload on the control stream costs, so the answer
    /// lands inside the budget rather than exactly on it.
    #[test]
    fn the_query_deadline_expires_inside_the_control_plane_command_budget() {
        /// The control plane's shared `DEFAULT_COMMAND_TIMEOUT`, which bounds
        /// every command it dispatches rather than queries alone. Mirrored here
        /// because it is the number this deadline has to fit inside; if it moves
        /// there, it moves here, and this test is what notices.
        const CONTROL_PLANE_COMMAND_BUDGET: Duration = Duration::from_secs(30);
        const MARGIN: Duration = Duration::from_secs(5);

        assert!(
            DEFAULT_QUERY_DEADLINE.saturating_add(MARGIN) <= CONTROL_PLANE_COMMAND_BUDGET,
            "the {DEFAULT_QUERY_DEADLINE:?} query deadline leaves under {MARGIN:?} of the {CONTROL_PLANE_COMMAND_BUDGET:?} command budget to return the result; lower it, or raise the budget first"
        );
    }

    #[test]
    fn default_config_dir_respects_env_var() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        // SAFETY: tests gate env-var mutations behind a mutex.
        unsafe {
            std::env::set_var("SPICE_CONFIG_DIR", "/tmp/spice-test");
        }
        let dir = CloudConnectConfig::default_config_dir();
        assert_eq!(dir, PathBuf::from("/tmp/spice-test"));
        unsafe {
            std::env::remove_var("SPICE_CONFIG_DIR");
        }
    }

    #[test]
    fn default_config_dir_is_local_not_global_when_env_unset() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        // SAFETY: tests gate env-var mutations behind a mutex.
        unsafe {
            std::env::remove_var("SPICE_CONFIG_DIR");
        }
        let dir = CloudConnectConfig::default_config_dir();
        // Enrollment state must be scoped to the instance's working directory
        // (`./.spice`), never the machine-global `~/.spice`, so that multiple
        // spiced instances on one host enroll independently.
        assert_eq!(dir.file_name(), Some(std::ffi::OsStr::new(".spice")));
        let expected = std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(".spice");
        assert_eq!(dir, expected);
        if let Some(home) = dirs::home_dir() {
            assert_ne!(
                dir,
                home.join(".spice"),
                "enrollment config dir must not resolve to the global ~/.spice"
            );
        }
    }

    #[test]
    fn resolve_config_dir_env_var_wins_over_instance_dir() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        // SAFETY: tests gate env-var mutations behind a mutex.
        unsafe {
            std::env::set_var("SPICE_CONFIG_DIR", "/tmp/spice-env-dir");
        }
        let dir = CloudConnectConfig::resolve_config_dir(Some(std::path::Path::new("/opt/edge-1")));
        assert_eq!(
            dir,
            PathBuf::from("/tmp/spice-env-dir"),
            "SPICE_CONFIG_DIR must win over --dir"
        );
        unsafe {
            std::env::remove_var("SPICE_CONFIG_DIR");
        }
    }

    #[test]
    fn resolve_config_dir_anchors_at_instance_dir() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        // SAFETY: tests gate env-var mutations behind a mutex.
        unsafe {
            std::env::remove_var("SPICE_CONFIG_DIR");
        }
        let dir = CloudConnectConfig::resolve_config_dir(Some(std::path::Path::new("/opt/edge-1")));
        assert_eq!(dir, PathBuf::from("/opt/edge-1/.spice"));
    }

    #[test]
    fn resolve_config_dir_makes_relative_instance_dir_absolute() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        // SAFETY: tests gate env-var mutations behind a mutex.
        unsafe {
            std::env::remove_var("SPICE_CONFIG_DIR");
        }
        let dir = CloudConnectConfig::resolve_config_dir(Some(std::path::Path::new("edge-1")));
        let expected = std::env::current_dir()
            .expect("cwd available in tests")
            .join("edge-1")
            .join(".spice");
        assert_eq!(
            dir, expected,
            "a relative --dir must be resolved against the cwd at enroll time"
        );
    }

    #[test]
    fn from_env_uses_default_endpoint_when_unset() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        unsafe {
            std::env::remove_var("SPICE_CLOUD_ENDPOINT");
            std::env::remove_var("SPICE_CLOUD_GATEWAY_ENDPOINT");
        }
        let config = CloudConnectConfig::from_env("v0.0.0-test");
        assert_eq!(config.enroll_endpoint, DEFAULT_ENDPOINT);
        assert!(
            config.gateway_endpoint.is_none(),
            "gateway endpoint comes from the enroll response unless overridden"
        );
        assert!(
            config.instance_region.is_none(),
            "the region is an explicit argument, never read from the environment"
        );
    }

    #[test]
    fn persisted_gateway_rejects_port_zero() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        unsafe {
            std::env::remove_var("SPICE_CLOUD_ENDPOINT");
            std::env::remove_var("SPICE_CLOUD_GATEWAY_ENDPOINT");
        }
        let config = CloudConnectConfig::from_env("v0.0.0-test");
        let identity = crate::identity::Identity {
            identifier: "inst_test".to_string(),
            control_plane_endpoint: None,
            identity_cert_pem: String::new(),
            private_key_pem: String::new(),
            public_key_pem: String::new(),
            ca_bundle_pem: String::new(),
            gateway_addr: "gateway.example.test:0".to_string(),
            not_after_unix: None,
            app_id: None,
            org_name: None,
            app_name: None,
            monitor_url: None,
            enc_private_key_pem: String::new(),
            enc_public_key_pem: String::new(),
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
        };

        assert_eq!(
            config.validated_persisted_gateway_endpoint(&identity),
            Err("the gateway endpoint port must be greater than zero")
        );
    }

    #[test]
    fn enrollment_endpoint_override_is_trimmed_and_optional() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(
            CloudConnectConfig::read_enroll_endpoint_override(dir.path())
                .expect("missing override is ordinary absence")
                .is_none()
        );
        std::fs::write(
            dir.path().join("cloud-endpoint"),
            "  https://cloud.example.test  \n",
        )
        .expect("write override");
        assert_eq!(
            CloudConnectConfig::read_enroll_endpoint_override(dir.path())
                .expect("read override")
                .as_deref(),
            Some("https://cloud.example.test")
        );
        assert_eq!(
            CloudConnectConfig::read_normalized_enroll_endpoint_override(dir.path())
                .expect("normalize override")
                .as_deref(),
            Some("https://cloud.example.test")
        );

        std::fs::write(dir.path().join("cloud-endpoint"), "not an endpoint\n")
            .expect("write invalid override");
        CloudConnectConfig::read_normalized_enroll_endpoint_override(dir.path())
            .expect_err("an invalid configured endpoint must fail closed");
    }

    #[cfg(unix)]
    #[test]
    fn enrollment_endpoint_override_does_not_follow_symlinks() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("target");
        std::fs::write(&target, "https://redirected.example.test").expect("write symlink target");
        symlink(&target, dir.path().join("cloud-endpoint")).expect("create symlink");

        CloudConnectConfig::read_enroll_endpoint_override(dir.path())
            .expect_err("a state-file symlink must be rejected");
    }
}
