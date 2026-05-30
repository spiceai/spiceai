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

/// Default endpoint for the Spice Cloud control plane.
pub const DEFAULT_ENDPOINT: &str = "https://cloud.spice.ai";

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

/// Runtime config for the Cloud Connect client.
#[derive(Debug, Clone)]
pub struct CloudConnectConfig {
    /// gRPC endpoint, e.g. `https://cloud.spice.ai`. The path component
    /// is ignored — only scheme + host + port are used.
    pub endpoint: String,

    /// Optional PEM-encoded CA certificate to verify the server. When
    /// `None`, the system `WebPKI` roots are used. Mainly for self-hosted
    /// control planes during development.
    pub ca_cert_pem: Option<String>,

    /// When `true`, server certificate verification is **disabled** —
    /// for development only. Defaults to `false`.
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

    /// Runtime semver-like string (`v2.0.0-build.deadbeef`). Sent in
    /// `Hello.runtime_version`.
    pub runtime_version: String,
}

impl CloudConnectConfig {
    /// Resolve `$SPICE_CONFIG_DIR` to its canonical location.
    ///
    /// Precedence:
    /// 1. `$SPICE_CONFIG_DIR` env var
    /// 2. `~/.spice`
    /// 3. Current directory (fallback)
    #[must_use]
    pub fn default_config_dir() -> PathBuf {
        if let Ok(dir) = std::env::var("SPICE_CONFIG_DIR")
            && !dir.is_empty()
        {
            return PathBuf::from(dir);
        }
        if let Some(home) = dirs::home_dir() {
            return home.join(".spice");
        }
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
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
    /// 1. `SPICE_ADOPT_CODE` env var.
    /// 2. `$SPICE_CONFIG_DIR/pending-adopt-code` file.
    /// 3. None (rely on identity at `$SPICE_CONFIG_DIR/identity.json`).
    ///
    /// Endpoint defaults to [`DEFAULT_ENDPOINT`]; override via the
    /// `SPICE_CLOUD_ENDPOINT` env var.
    #[must_use]
    pub fn from_env(runtime_version: impl Into<String>) -> Self {
        let config_dir = Self::default_config_dir();
        let identity_path = config_dir.join(IDENTITY_FILE);
        let pending_path = config_dir.join(PENDING_ADOPT_CODE_FILE);

        let endpoint = std::env::var("SPICE_CLOUD_ENDPOINT")
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());

        let (adoption_code, pending_adopt_code_path) = if let Ok(code) =
            std::env::var("SPICE_ADOPT_CODE")
            && !code.is_empty()
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

        Self {
            endpoint,
            ca_cert_pem: None,
            insecure: false,
            identity_path,
            config_dir,
            adoption_code,
            pending_adopt_code_path,
            runtime_version: runtime_version.into(),
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
    fn from_env_uses_default_endpoint_when_unset() {
        let _guard = ENV_LOCK.lock().expect("env lock poisoned");
        unsafe {
            std::env::remove_var("SPICE_CLOUD_ENDPOINT");
            std::env::remove_var("SPICE_ADOPT_CODE");
        }
        let config = CloudConnectConfig::from_env("v0.0.0-test");
        assert_eq!(config.endpoint, DEFAULT_ENDPOINT);
    }
}
