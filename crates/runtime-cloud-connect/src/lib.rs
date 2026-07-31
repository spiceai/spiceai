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
//! using the enroll-first flow shipped for BYOC (DR-025):
//!
//! 1. Admin in Spice Cloud generates a single-use adoption code.
//! 2. User runs `spice connect <code>` (or sets `SPICE_CONNECT_ADOPT_CODE`).
//! 3. **State plane (out-of-band enroll)**: `spiced` generates an ECDSA
//!    P-256 keypair + PKCS#10 CSR and presents the adoption code, the
//!    CSR, and its host facts to the cloud enroll endpoint over plain
//!    HTTPS (no bearer — the code is the credential). The cloud atomically
//!    consumes the code, provisions the instance registry row, signs the
//!    CSR with the cloud KMS CA, and returns the leaf certificate, the
//!    CA bundle, the gateway address, and the stable `instance_id` —
//!    persisted at `$SPICE_CONFIG_DIR/identity.json`.
//! 4. **Control plane (mTLS stream)**: with the issued identity as its
//!    TLS client certificate, `spiced` connects to the stateless gateway
//!    and holds the long-lived `CloudConnect` stream. The gateway never
//!    signs and holds no state; certless connections are rejected.
//! 5. Admin clicks "Adopt"; cloud sends an `Adopt` command — a
//!    trust/marker message the client acknowledges with `AdoptAck`
//!    (the cert was already issued at enroll).
//! 6. On future starts, the identity is reused; no code needed. The
//!    identity is renewed on a ~12h cadence against the cloud `/renew`
//!    endpoint (dual proof-of-possession; every renewal rotates the
//!    keypair) and remains renewable up to 30 days past expiry.
//!
//! The client is **default off**: with no adoption code and no identity it
//! does nothing — existing OSS users see no change.
//!
//! ## Public entry point
//!
//! Call [`CloudConnect::start`] from the runtime bootstrap. It returns a
//! handle whose tokio task crashes/disconnects are isolated from the rest
//! of the process; the runtime stays up.

pub mod config;
pub mod enroll;
pub mod handlers;
pub mod identity;

mod client;
mod fingerprint;
mod heartbeat;
mod shutdown;

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

use shutdown::Shutdown;

/// Errors that can occur while starting or running the Cloud Connect client.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to load identity from {}: {source}", path.display()))]
    IdentityLoad {
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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Handle for a running `CloudConnect` client.
///
/// Dropping the handle does **not** cancel the background task. Call
/// [`CloudConnect::shutdown`] to stop it cleanly.
pub struct CloudConnect {
    task: Mutex<Option<JoinHandle<()>>>,
    shutdown: Arc<Shutdown>,
}

impl CloudConnect {
    /// Start the Cloud Connect client.
    ///
    /// Behavior depends on the contents of [`CloudConnectConfig`] and on
    /// state on disk:
    ///
    /// - If `identity_path` contains a valid identity, the client connects
    ///   to the gateway over mTLS using the stored identity (renewing it
    ///   first when due).
    /// - Otherwise, if `adoption_code` is `Some`, the client enrolls
    ///   out-of-band against the cloud enroll endpoint, then connects to
    ///   the gateway with the issued identity.
    /// - If both are absent, this function returns `Ok(None)` and the client
    ///   is **not started** — i.e. `CloudConnect` is disabled.
    ///
    /// # Errors
    ///
    /// Returns [`Error::IdentityLoad`] if an identity file exists at
    /// `config.identity_path` but cannot be read or parsed.
    #[expect(
        clippy::unused_async,
        reason = "spawns a background tokio task, so it must be called from within a tokio runtime context"
    )]
    pub async fn start(
        config: CloudConnectConfig,
        runtime: Arc<dyn RuntimeHandle>,
    ) -> Result<Option<Self>> {
        let identity_path = config.identity_path.clone();
        let identity = match IdentityStore::load_optional(&identity_path) {
            Ok(identity) => identity,
            // A corrupt/unreadable identity file must not wedge re-adoption:
            // when an adoption code is available, proceed without the stored
            // identity (a successful adoption rewrites identity.json). Only
            // surface the load error when there is no code to fall back to.
            Err(source) if config.adoption_code.is_some() => {
                tracing::warn!(
                    "Cloud Connect: identity at {} is unreadable ({source}); proceeding with the adoption code",
                    identity_path.display()
                );
                None
            }
            Err(source) => {
                return Err(Error::IdentityLoad {
                    path: identity_path,
                    source,
                });
            }
        };

        if identity.is_none() && config.adoption_code.is_none() {
            tracing::debug!(
                "Cloud Connect disabled: no identity at {} and no adoption code",
                identity_path.display()
            );
            return Ok(None);
        }

        let shutdown = Shutdown::new();
        let shutdown_for_task = Arc::clone(&shutdown);

        let runtime_for_task = Arc::clone(&runtime);
        let task = tokio::spawn(async move {
            let driver =
                client::ClientDriver::new(config, runtime_for_task, shutdown_for_task, identity);
            if let Err(err) = driver.run().await {
                tracing::error!("Cloud Connect driver exited with error: {err}");
            }
        });

        Ok(Some(Self {
            task: Mutex::new(Some(task)),
            shutdown,
        }))
    }

    /// Request graceful shutdown of the Cloud Connect task.
    ///
    /// Waits for the background task to exit (≤ 10s drain budget).
    pub async fn shutdown(&self) {
        self.shutdown.trigger();
        let Some(mut handle) = self.task.lock().await.take() else {
            return;
        };
        // Give the task its drain budget; if it doesn't exit in time, abort
        // it rather than dropping the JoinHandle (which would detach the task
        // and leave it running past shutdown).
        if tokio::time::timeout(std::time::Duration::from_secs(10), &mut handle)
            .await
            .is_err()
        {
            tracing::warn!("Cloud Connect: task did not exit within 10s; aborting");
            handle.abort();
        }
    }
}

/// Validate an adoption code shape.
///
/// Accepts either shape a portal may mint:
/// - a raw 32-byte hex string (64 hex digits) — what the cloud portal
///   currently mints (`randomBytes(32).toString('hex')`), or
/// - the grouped form `SPICE-ADOPT-XXXXX-XXXXX-...` where each `XXXXX` segment
///   is 5 uppercase ASCII alphanumerics, with 2–5 segments.
///
/// This is a client-side sanity check to tell an adoption code apart from a
/// Spicepod path (`<org>/<pod>`); the code is validated authoritatively
/// server-side at enroll.
#[must_use]
pub fn is_valid_adoption_code(code: &str) -> bool {
    // Raw 32-byte hex (64 hex digits) — the format the cloud portal mints today.
    if code.len() == 64 && code.bytes().all(|b| b.is_ascii_hexdigit()) {
        return true;
    }
    let mut parts = code.split('-');
    if parts.next() != Some("SPICE") {
        return false;
    }
    if parts.next() != Some("ADOPT") {
        return false;
    }
    let mut segments = 0_usize;
    for part in parts {
        if part.len() != 5
            || !part
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        {
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
        assert!(!is_valid_adoption_code("FOO-BAR-AAAAA-BBBBB"));
        assert!(!is_valid_adoption_code("SPICE-CONNECT-AAAAA-BBBBB"));
    }

    #[test]
    fn accepts_canonical_adoption_code() {
        // The portal mints four 5-char base32 segments.
        assert!(is_valid_adoption_code(
            "SPICE-ADOPT-7K2PX-9XYZ2-A1B2C-D3E4F"
        ));
        assert!(is_valid_adoption_code("SPICE-ADOPT-AAAAA-BBBBB"));
        assert!(is_valid_adoption_code(
            "SPICE-ADOPT-11111-22222-33333-44444-55555"
        ));
    }

    #[test]
    fn accepts_raw_hex_adoption_code() {
        // The cloud portal mints randomBytes(32).toString('hex') — 64 hex chars.
        assert!(is_valid_adoption_code(
            "9f500bdec2f2dcf06e50f255d6d8291603e9b10f5abf500a5de5ad6d2069837d"
        ));
        assert!(!is_valid_adoption_code("9f500bde")); // too short
        assert!(!is_valid_adoption_code(&"z".repeat(64))); // 64 chars but non-hex
    }

    #[test]
    fn rejects_codes_with_lowercase_or_wrong_length() {
        assert!(!is_valid_adoption_code("SPICE-ADOPT-aaaaa-BBBBB"));
        assert!(!is_valid_adoption_code("SPICE-ADOPT-AAAA-BBBBB")); // 4-char segment
        assert!(!is_valid_adoption_code("SPICE-ADOPT-AAAAAA-BBBBB")); // 6-char segment
        assert!(!is_valid_adoption_code("SPICE-ADOPT"));
        // 6 segments — too many.
        assert!(!is_valid_adoption_code(
            "SPICE-ADOPT-11111-22222-33333-44444-55555-66666"
        ));
    }
}
