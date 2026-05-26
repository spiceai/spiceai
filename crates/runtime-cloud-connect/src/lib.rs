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

//! # Spice Cloud Connect (standalone instance client)
//!
//! Outbound control-plane client that lets a standalone `spiced` runtime be
//! discovered and managed by Spice Cloud (or any compatible control plane)
//! using a UniFi-style adoption flow:
//!
//! 1. Admin in Spice Cloud generates a single-use adoption code.
//! 2. User runs `spice connect <code>` (or sets `SPICE_ADOPT_CODE`).
//! 3. `spiced` opens an outbound mTLS gRPC stream and sends `Hello`.
//! 4. Admin clicks "Adopt"; cloud sends an `Adopt` command.
//! 5. `spiced` generates an ed25519 keypair, stores identity at
//!    `$SPICE_CONFIG_DIR/identity.json`, and replies with `AdoptAck`.
//! 6. On future starts, identity is reused; no code needed.
//!
//! The client is **default off**: with no adoption code and no identity it
//! does nothing — existing OSS users see no change.
//!
//! ## Public entry point
//!
//! Call [`CloudConnect::start`] from the runtime bootstrap. It returns a
//! handle whose tokio task crashes/disconnects are isolated from the rest
//! of the process; the runtime stays up.

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

pub mod client;
pub mod config;
pub mod fingerprint;
pub mod handlers;
pub mod heartbeat;
pub mod identity;

/// Generated gRPC types for `spice.cloud.v1.CloudConnect`.
pub mod proto {
    #![allow(
        clippy::pedantic,
        clippy::clone_on_ref_ptr,
        clippy::doc_markdown,
        clippy::missing_errors_doc,
        clippy::default_trait_access,
        clippy::allow_attributes,
        clippy::mixed_attributes_style,
        clippy::large_enum_variant
    )]

    tonic::include_proto!("spice.cloud.v1");
}

use std::sync::Arc;

use snafu::Snafu;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

pub use config::CloudConnectConfig;
pub use handlers::RuntimeHandle;
pub use identity::{Identity, IdentityStore};

/// Errors that can occur while starting or running the Cloud Connect client.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to load identity from {}: {source}", path.display()))]
    IdentityLoad {
        path: std::path::PathBuf,
        source: identity::Error,
    },

    #[snafu(display("Failed to store identity at {}: {source}", path.display()))]
    IdentityStore {
        path: std::path::PathBuf,
        source: identity::Error,
    },

    #[snafu(display("Invalid Cloud Connect endpoint: {endpoint}: {source}"))]
    InvalidEndpoint {
        endpoint: String,
        source: tonic::transport::Error,
    },

    #[snafu(display("Failed to connect to Cloud Connect server: {source}"))]
    Transport { source: tonic::transport::Error },

    #[snafu(display("Cloud Connect stream error: {source}"))]
    Stream { source: tonic::Status },

    #[snafu(display("Cloud Connect is configured but no credentials are available"))]
    NoCredentials,

    #[snafu(display("Failed to read adoption code file at {}: {source}", path.display()))]
    AdoptCodeRead {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Handle for a running CloudConnect client.
///
/// Dropping the handle does **not** cancel the background task. Call
/// [`CloudConnect::shutdown`] to stop it cleanly.
pub struct CloudConnect {
    task: Mutex<Option<JoinHandle<()>>>,
    shutdown_tx: Arc<tokio::sync::Notify>,
}

impl CloudConnect {
    /// Start the Cloud Connect client.
    ///
    /// Behavior depends on the contents of [`CloudConnectConfig`] and on
    /// state on disk:
    ///
    /// - If `identity_path` contains a valid identity, the client reconnects
    ///   using the stored identity token.
    /// - Otherwise, if `adoption_code` is `Some`, the client sends the code
    ///   as the first-contact credential and waits to be adopted.
    /// - If both are absent, this function returns `Ok(None)` and the client
    ///   is **not started** — i.e. CloudConnect is disabled.
    pub async fn start(
        config: CloudConnectConfig,
        runtime: Arc<dyn RuntimeHandle>,
    ) -> Result<Option<Self>> {
        // Lazy import; keep ergonomics local.
        use snafu::ResultExt as _;

        let identity_path = config.identity_path.clone();
        let identity = IdentityStore::load_optional(&identity_path)
            .context(IdentityLoadSnafu {
                path: identity_path.clone(),
            })?;

        if identity.is_none() && config.adoption_code.is_none() {
            tracing::debug!(
                "Cloud Connect disabled: no identity at {} and no adoption code",
                identity_path.display()
            );
            return Ok(None);
        }

        let shutdown_tx = Arc::new(tokio::sync::Notify::new());
        let shutdown_rx = Arc::clone(&shutdown_tx);

        let runtime_for_task = Arc::clone(&runtime);
        let task = tokio::spawn(async move {
            let driver =
                client::ClientDriver::new(config, runtime_for_task, shutdown_rx, identity);
            if let Err(err) = driver.run().await {
                tracing::error!("Cloud Connect driver exited with error: {err}");
            }
        });

        Ok(Some(Self {
            task: Mutex::new(Some(task)),
            shutdown_tx,
        }))
    }

    /// Request graceful shutdown of the Cloud Connect task.
    ///
    /// Waits for the background task to exit (≤ 10s drain budget).
    pub async fn shutdown(&self) {
        self.shutdown_tx.notify_waiters();
        let Some(handle) = self.task.lock().await.take() else {
            return;
        };
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), handle).await;
    }
}

/// Validate an adoption code shape.
///
/// Format: `SPICE-ADOPT-XXXX-XXXX-...` where each `XXXX` segment is 4
/// uppercase ASCII alphanumeric characters, and there are between 2 and 5
/// segments. The actual code may be looser server-side; this is the
/// client-side sanity check.
#[must_use]
pub fn is_valid_adoption_code(code: &str) -> bool {
    let mut parts = code.split('-');
    if parts.next() != Some("SPICE") {
        return false;
    }
    if parts.next() != Some("ADOPT") {
        return false;
    }
    let mut segments = 0_usize;
    for part in parts {
        if part.len() != 4 || !part.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()) {
            return false;
        }
        segments += 1;
        if segments > 5 {
            return false;
        }
    }
    (2..=5).contains(&segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_codes_with_wrong_prefix() {
        assert!(!is_valid_adoption_code("FOO-BAR-AAAA-BBBB"));
        assert!(!is_valid_adoption_code("SPICE-CONNECT-AAAA-BBBB"));
    }

    #[test]
    fn accepts_canonical_adoption_code() {
        assert!(is_valid_adoption_code("SPICE-ADOPT-7K2P-9XYZ-A1B2"));
        assert!(is_valid_adoption_code("SPICE-ADOPT-AAAA-BBBB"));
        assert!(is_valid_adoption_code("SPICE-ADOPT-1111-2222-3333-4444-5555"));
    }

    #[test]
    fn rejects_codes_with_lowercase_or_wrong_length() {
        assert!(!is_valid_adoption_code("SPICE-ADOPT-aaaa-BBBB"));
        assert!(!is_valid_adoption_code("SPICE-ADOPT-AAA-BBBB"));
        assert!(!is_valid_adoption_code("SPICE-ADOPT-AAAAA-BBBB"));
        assert!(!is_valid_adoption_code("SPICE-ADOPT"));
        // 6 segments — too many.
        assert!(!is_valid_adoption_code(
            "SPICE-ADOPT-1111-2222-3333-4444-5555-6666"
        ));
    }
}
