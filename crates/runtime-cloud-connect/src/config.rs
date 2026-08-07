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

use std::path::PathBuf;
use std::time::Duration;

/// Default enroll endpoint for the Spice Cloud control plane (state
/// plane): the base URL the out-of-band HTTPS `/v1/cloud-connect/enroll`
/// and `/v1/cloud-connect/renew` requests are made against. The gateway
/// (stream) address is returned by the enroll response, not configured.
pub const DEFAULT_ENDPOINT: &str = "https://api.spice.ai";

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

/// File name (relative to `$SPICE_CONFIG_DIR`) where the cloud-managed
/// spicepod is written when an `ApplySpicepod` command arrives.
pub const CLOUD_MANAGED_SPICEPOD_FILE: &str = "spicepod-cloud-managed.yml";

/// File name (relative to `$SPICE_CONFIG_DIR`) where a freshly-scheduled
/// adoption code is staged by `spice connect <code>` before `spiced`
/// starts and consumes it on first Hello.
pub const PENDING_ADOPT_CODE_FILE: &str = "pending-adopt-code";

/// File name (relative to `$SPICE_CONFIG_DIR`) where the runtime identity
/// is persisted after adoption.
pub const IDENTITY_FILE: &str = "identity.json";

/// Env var carrying a first-contact adoption code, for hosts where the
/// `spice` CLI is not available (containers, cloud-init).
pub const ADOPT_CODE_ENV: &str = "SPICE_CONNECT_ADOPT_CODE";

/// Env var carrying the org-scoped app name to attach the instance to at
/// enroll (mirrors `spice connect --app-name`), for hosts with no CLI.
pub const ADOPT_APP_NAME_ENV: &str = "SPICE_CONNECT_ADOPT_APP_NAME";

/// Env var mirroring `spice connect --create`: when truthy (`true`/`1`)
/// and the app named by [`ADOPT_APP_NAME_ENV`] does not exist, the cloud
/// creates it at enroll and attaches the instance.
pub const ADOPT_CREATE_APP_ENV: &str = "SPICE_CONNECT_ADOPT_CREATE";

/// Env var mirroring `spice connect --region`: where *this instance* runs
/// (`us-west-2`, `on-prem-syd`, …). A customer-declared label recorded on
/// the registry row, not a probed fact.
pub const ADOPT_REGION_ENV: &str = "SPICE_CONNECT_ADOPT_REGION";

/// Read the adoption code from [`ADOPT_CODE_ENV`]. An empty value is
/// treated as unset.
fn adoption_code_from_env() -> Option<String> {
    std::env::var(ADOPT_CODE_ENV).ok().filter(|c| !c.is_empty())
}

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
    /// when an `ApplySpicepod` command arrives.
    pub config_dir: PathBuf,

    /// First-contact adoption code, if any. May be `None` even when
    /// `CloudConnect` is enabled — that means the identity file is present
    /// and the client should reconnect with the stored identity.
    pub adoption_code: Option<String>,

    /// Path to the pending adoption-code file, if applicable. The driver
    /// intentionally retains this file across reconnects until adoption
    /// fully succeeds — it is deleted only once `handle_adopt` has
    /// persisted the issued identity to disk. Keeping it until then lets a
    /// dropped or retried connection re-send the code, while the post-adopt
    /// delete stops a restart from accidentally re-sending a consumed
    /// single-use code.
    pub pending_adopt_code_path: Option<PathBuf>,

    /// Org-scoped app name to attach the instance to at enroll
    /// (`spice connect --app-name` / [`ADOPT_APP_NAME_ENV`]). Sent in the
    /// enroll request; the cloud validates it before consuming the code.
    /// `None` enrolls unattached (or under the code's own app scope).
    pub adopt_app_name: Option<String>,

    /// When `true` and `adopt_app_name` names no existing app, the cloud
    /// creates the app at enroll and attaches the instance
    /// (`spice connect --create` / [`ADOPT_CREATE_APP_ENV`]).
    pub adopt_create_app: bool,

    /// Where this instance runs (`spice connect --region` /
    /// [`ADOPT_REGION_ENV`]) — a customer-declared label, not a probed fact,
    /// so it rides the enroll request as a sibling of the host facts rather
    /// than as one of them. The cloud records it on the registry row and
    /// resolves the instance's gateway stamp from it.
    ///
    /// `None` means "leave the stored value alone": a re-enrol (the recovery
    /// path once the renewal grace window has passed) must not erase a region
    /// set in the portal.
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
}

impl CloudConnectConfig {
    /// Resolve the Cloud Connect config directory to its canonical location.
    ///
    /// Precedence:
    /// 1. `$SPICE_CONFIG_DIR` env var (explicit override)
    /// 2. `./.spice` — a `.spice` directory in the current working
    ///    directory (the `spiced` instance's working directory)
    ///
    /// This deliberately does **not** fall back to the global `~/.spice`.
    /// Adoption state (the pending code, `identity.json`, and the
    /// `cloud-endpoint` override) is per-`spiced`-instance: several `spiced`
    /// processes can run on one machine and each must adopt into Spice Cloud
    /// independently. A shared `~/.spice` would make one machine present as a
    /// single runtime and let one instance's adoption clobber another's, so
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

    /// Resolve the canonical pending-adopt-code path.
    #[must_use]
    pub fn default_pending_adopt_code_path() -> PathBuf {
        Self::default_config_dir().join(PENDING_ADOPT_CODE_FILE)
    }

    /// Returns a config bootstrap that picks up adoption state from disk
    /// and the environment.
    ///
    /// Precedence for the adoption credential:
    /// 1. [`ADOPT_CODE_ENV`] env var.
    /// 2. `$SPICE_CONFIG_DIR/pending-adopt-code` file.
    /// 3. None (rely on identity at `$SPICE_CONFIG_DIR/identity.json`).
    ///
    /// The enroll endpoint defaults to [`DEFAULT_ENDPOINT`]; override via
    /// the `SPICE_CLOUD_ENDPOINT` env var. The gateway (stream) endpoint
    /// normally comes from the enroll response; the
    /// `SPICE_CLOUD_GATEWAY_ENDPOINT` env var overrides it.
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
        let pending_path = config_dir.join(PENDING_ADOPT_CODE_FILE);

        let enroll_endpoint = std::env::var("SPICE_CLOUD_ENDPOINT")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
        let gateway_endpoint = std::env::var("SPICE_CLOUD_GATEWAY_ENDPOINT")
            .ok()
            .filter(|v| !v.is_empty());

        let (adoption_code, pending_adopt_code_path) = if let Some(code) = adoption_code_from_env()
        {
            // Env var wins; we do not delete the pending file in this
            // branch because it's an out-of-band re-adopt signal.
            (Some(code), None)
        } else if pending_path.exists() {
            match std::fs::read_to_string(&pending_path) {
                Ok(s) => {
                    let trimmed = s.trim().to_string();
                    if trimmed.is_empty() {
                        (None, None)
                    } else {
                        (Some(trimmed), Some(pending_path))
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "Cloud Connect: failed to read pending adopt code at {}: {err}",
                        pending_path.display()
                    );
                    (None, None)
                }
            }
        } else {
            (None, None)
        };

        let adopt_app_name = std::env::var(ADOPT_APP_NAME_ENV)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let mut adopt_create_app = std::env::var(ADOPT_CREATE_APP_ENV)
            .is_ok_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1"));
        // "Create the app" needs an app to name. The `--create` flag is
        // guarded by clap (`requires = "app_name"`); the env pair has no
        // such guard, so enforce it here rather than sending a request the
        // cloud would reject.
        if adopt_create_app && adopt_app_name.is_none() {
            tracing::warn!(
                "{ADOPT_CREATE_APP_ENV} is set but {ADOPT_APP_NAME_ENV} is empty; ignoring it. Set {ADOPT_APP_NAME_ENV} to the app to attach this instance to, or attach it in the Spice Cloud portal after enrolling. See: https://spiceai.org/docs"
            );
            adopt_create_app = false;
        }

        // A malformed region is dropped rather than sent: the cloud rejects it
        // before consuming the adoption code, so enrolling anyway would only
        // turn a typo in the environment into a failed enrollment.
        let instance_region = std::env::var(ADOPT_REGION_ENV)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .and_then(|region| {
                if crate::is_valid_instance_region(&region) {
                    Some(region)
                } else {
                    tracing::warn!(
                        "{ADOPT_REGION_ENV} value {region:?} is not a valid region label; ignoring it. Expected 2-64 lowercase letters, digits, or hyphens (for example 'us-west-2' or 'on-prem-syd'). Set the region on the instance in the Spice Cloud portal instead. See: https://spiceai.org/docs"
                    );
                    None
                }
            });

        Self {
            enroll_endpoint,
            gateway_endpoint,
            ca_cert_pem: None,
            insecure: false,
            identity_path,
            config_dir,
            adoption_code,
            pending_adopt_code_path,
            adopt_app_name,
            adopt_create_app,
            instance_region,
            runtime_version: runtime_version.into(),
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            telemetry_interval: DEFAULT_TELEMETRY_INTERVAL,
            metrics_interval: DEFAULT_METRICS_INTERVAL,
            renewal_lead: DEFAULT_RENEWAL_LEAD,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

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
        // Adoption state must be scoped to the instance's working directory
        // (`./.spice`), never the machine-global `~/.spice`, so that multiple
        // spiced instances on one host adopt independently.
        assert_eq!(dir.file_name(), Some(std::ffi::OsStr::new(".spice")));
        let expected = std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(".spice");
        assert_eq!(dir, expected);
        if let Some(home) = dirs::home_dir() {
            assert_ne!(
                dir,
                home.join(".spice"),
                "adoption config dir must not resolve to the global ~/.spice"
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
    fn adoption_code_env_treats_empty_as_unset() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        // SAFETY: tests gate env-var mutations behind a mutex.
        unsafe {
            std::env::set_var(ADOPT_CODE_ENV, "SPICE-ADOPT-7K2PX-9XYZ2");
        }
        assert_eq!(
            adoption_code_from_env().as_deref(),
            Some("SPICE-ADOPT-7K2PX-9XYZ2")
        );

        // An exported-but-empty var is how a shell passes "no code"; it must
        // not be presented to the cloud as a credential.
        unsafe {
            std::env::set_var(ADOPT_CODE_ENV, "");
        }
        assert_eq!(adoption_code_from_env(), None);

        unsafe {
            std::env::remove_var(ADOPT_CODE_ENV);
        }
        assert_eq!(adoption_code_from_env(), None);
    }

    #[test]
    fn from_env_ignores_create_app_without_an_app_name() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        // SAFETY: tests gate env-var mutations behind a mutex.
        unsafe {
            std::env::remove_var(ADOPT_APP_NAME_ENV);
            std::env::set_var(ADOPT_CREATE_APP_ENV, "true");
        }
        let config = CloudConnectConfig::from_env("v0.0.0-test");
        assert!(
            !config.adopt_create_app,
            "creating an app needs an app to name"
        );

        // With a name it takes effect.
        unsafe {
            std::env::set_var(ADOPT_APP_NAME_ENV, "edge-fleet");
        }
        let config = CloudConnectConfig::from_env("v0.0.0-test");
        assert_eq!(config.adopt_app_name.as_deref(), Some("edge-fleet"));
        assert!(config.adopt_create_app);

        unsafe {
            std::env::remove_var(ADOPT_APP_NAME_ENV);
            std::env::remove_var(ADOPT_CREATE_APP_ENV);
        }
    }

    #[test]
    fn from_env_uses_default_endpoint_when_unset() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        unsafe {
            std::env::remove_var("SPICE_CLOUD_ENDPOINT");
            std::env::remove_var("SPICE_CLOUD_GATEWAY_ENDPOINT");
            std::env::remove_var(ADOPT_CODE_ENV);
        }
        let config = CloudConnectConfig::from_env("v0.0.0-test");
        assert_eq!(config.enroll_endpoint, DEFAULT_ENDPOINT);
        assert!(
            config.gateway_endpoint.is_none(),
            "gateway endpoint comes from the enroll response unless overridden"
        );
    }
}
