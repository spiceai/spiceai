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
//! 1. Enrollment happens **before the runtime exists**, driven by exactly
//!    one [`enroll::EnrollmentAuthority`]: a one-time `spice-enroll-`
//!    enrollment key (`spiced --token`, parsed by
//!    [`enrollment_key::EnrollmentKey`]), or a logged-in session enrolling
//!    directly through a caller-provided authenticated-session authority.
//! 2. **State plane (out-of-band enroll)**: [`enroll::enroll_now`] loads or
//!    creates the per-directory [`draft::EnrollmentDraft`] — provisional
//!    ECDSA P-256 + X25519 key material and a stable enrollment operation
//!    ID — and presents the authority, the CSR, and the host facts to the
//!    cloud enroll endpoint over plain HTTPS under that operation's
//!    `Idempotency-Key`. The cloud consumes the authority, provisions the
//!    instance registry row, signs the CSR with the cloud KMS CA, and
//!    returns the leaf certificate, the CA bundle, the gateway address,
//!    and the stable `instance_id` — atomically promoted to
//!    `$SPICE_CONFIG_DIR/identity.json`. A lost response is safe: an exact
//!    operation replay returns the same instance instead of a sibling.
//! 3. **Control plane (mTLS stream)**: with the issued identity as its
//!    TLS client certificate, `spiced` connects to the stateless gateway
//!    and holds the long-lived `CloudConnect` stream. The gateway never
//!    signs and holds no state; certless connections are rejected.
//! 4. On future starts, the identity alone activates the client — no flag
//!    and no key. A valid identity always wins: a `--token` supplied
//!    beside one is not redeemed. The identity is renewed on a ~12h
//!    cadence against the cloud `/renew` endpoint (dual
//!    proof-of-possession; every renewal rotates the keypair) and remains
//!    renewable up to 30 days past expiry.
//!
//! The client is **default off**: with no identity on disk it does
//! nothing — existing OSS users see no change.
//!
//! ## Public entry points
//!
//! Call [`enroll::enroll_now`] before runtime construction to redeem an
//! enrollment authority, then [`CloudConnect::start`] from the runtime
//! bootstrap. `start` returns a handle whose tokio task
//! crashes/disconnects are isolated from the rest of the process; the
//! runtime stays up.

pub mod clock_skew;
pub mod config;
pub mod draft;
pub mod enroll;
pub mod enrollment_key;
pub mod handlers;
pub mod identity;
pub mod release;
pub mod sealed_secrets;
pub mod secret_cache;
pub mod supervisor;

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
pub use draft::EnrollmentDraft;
pub use enroll::{
    EnrollNowError, EnrollNowOutcome, EnrollmentAuthority, EnrollmentMetadata, RetryPolicy,
    SessionToken, enroll_now,
};
pub use enrollment_key::{EnrollmentKey, InvalidEnrollmentKey};
pub use handlers::{
    ApplyOutcome, Capability, CommandError, MAX_QUERY_RESULT_BYTES, MAX_QUERY_ROWS, PostApply,
    QueryOutcome, RestartMode, RuntimeHandle, RuntimePhase, SpicepodDeployment, StatusReport,
    effective_max_rows,
};
pub use identity::{Identity, IdentityStore};
pub use supervisor::Supervisor;

/// Revision of the `spice.cloud.v1` contract this client implements,
/// announced in `Hello.protocol_version`.
///
/// Bump it only for a change neither absent-oneof tolerance nor
/// `Hello.capabilities` can carry — a peer that would misread the stream
/// without knowing. A purely additive command does not qualify: an older peer
/// decodes it as an absent body and answers unsupported, and `capabilities`
/// already says self-descriptively what this instance answers. The package
/// name changes only for a break this number cannot bridge.
pub const PROTOCOL_VERSION: u32 = 1;

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
    /// Activation is driven purely by the per-directory identity:
    ///
    /// - If `identity_path` contains a valid identity, the client connects
    ///   to the gateway over mTLS using the stored identity (renewing it
    ///   first when due). No flag opts in — the identity is the signal.
    /// - Otherwise this function returns `Ok(None)` and the client is
    ///   **not started** — i.e. `CloudConnect` is disabled. Enrollment is
    ///   an explicit, pre-runtime step ([`enroll::enroll_now`]); the
    ///   running client never enrolls.
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
            Err(source) => {
                return Err(Error::IdentityLoad {
                    path: identity_path,
                    source,
                });
            }
        };

        let Some(identity) = identity else {
            tracing::debug!(
                "Cloud Connect disabled: no identity at {}",
                identity_path.display()
            );
            return Ok(None);
        };
        let identity = Some(identity);

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

/// Validate an instance region label (`--region`).
///
/// This is a **charset and length check only** — deliberately not a lookup
/// against the AWS region catalog. A standalone host may sit in a region newer
/// than any catalog the CLI shipped with, or in no cloud region at all
/// (`on-prem-syd`), and both must enroll. The catalog is used cloud-side for
/// display and gateway-stamp selection, which fall back to the home stamp for
/// a label it does not recognise.
///
/// The rule mirrors the cloud's own enroll validation (2–64 lowercase letters,
/// digits, and hyphens, starting and ending alphanumeric) so the CLI never
/// accepts a label the cloud would reject — and never rejects one it would
/// accept.
#[must_use]
pub fn is_valid_instance_region(region: &str) -> bool {
    if !(2..=64).contains(&region.len()) {
        return false;
    }
    let alnum = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    region.chars().all(|c| alnum(c) || c == '-')
        && region.chars().next().is_some_and(alnum)
        && region.chars().last().is_some_and(alnum)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_catalog_and_non_catalog_regions() {
        // A region the CLI's catalog knows nothing about must still enroll:
        // an on-prem label, and an AWS region newer than this build.
        for region in [
            "us-west-2",
            "eu-central-1",
            "on-prem-syd",
            "ap-southeast-7",
            "xx",
            "rack42",
        ] {
            assert!(is_valid_instance_region(region), "{region} must be valid");
        }
    }

    #[test]
    fn rejects_malformed_regions() {
        for region in [
            "",              // empty
            "u",             // shorter than the cloud's 2-char minimum
            "US-WEST-2",     // uppercase
            "us_west_2",     // underscore
            "-us-west-2",    // leading hyphen
            "us-west-2-",    // trailing hyphen
            "us west 2",     // whitespace
            "us-west-2\n",   // trailing newline (an unnoticed shell artifact)
            &"a".repeat(65), // past the cloud's 64-char ceiling
        ] {
            assert!(
                !is_valid_instance_region(region),
                "{region:?} must be rejected"
            );
        }
    }
}
