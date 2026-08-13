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
//! enrollment authority, load one startup snapshot with
//! [`load_reconnectable_identity_async`], and pass it to
//! [`CloudConnect::start_reconnectable`] after every optional facility has
//! followed that same decision. The returned handle owns a tokio task whose
//! crashes/disconnects are isolated from the rest of the process; the runtime
//! stays up.

pub mod clock_skew;
pub mod config;
pub mod draft;
pub mod enroll;
pub mod enrollment_key;
pub mod handlers;
pub mod identity;
pub mod mutation_lock;
pub mod release;
pub mod runtime_lock;
pub mod sealed_secrets;
pub mod secret_cache;
pub mod session;

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
pub use draft::{
    EnrollmentAuthorityBinding, EnrollmentDraft, EnrollmentRequestBinding,
    EnrollmentTransactionLock,
};
pub use enroll::{
    EnrollNowError, EnrollNowOutcome, EnrollmentAuthority, EnrollmentMetadata, RetryPolicy,
    SessionToken, enroll_now, enroll_now_with_token, enroll_now_with_transaction,
    sign_identity_proof,
};
pub use enrollment_key::{EnrollmentKey, InvalidEnrollmentKey};
pub use handlers::{
    Capability, CommandError, MAX_QUERY_RESULT_BYTES, MAX_QUERY_ROWS, QueryOutcome, RestartMode,
    RuntimeHandle, RuntimePhase, SpicepodDeployment, StatusReport, effective_max_rows,
};
pub use identity::{AppAttachment, AttachmentState, Identity, IdentityStore};
pub use mutation_lock::{MUTATION_LOCK_FILE, MutationLock};
pub use runtime_lock::{RuntimeLock, RuntimeLockOwner};
pub use session::{AcknowledgedSession, SessionAck};

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

    #[snafu(display("Identity load task failed for {}: {source}", path.display()))]
    IdentityLoadTaskPanicked {
        path: std::path::PathBuf,
        source: tokio::task::JoinError,
    },

    #[snafu(display("Identity validation task failed for {}: {source}", path.display()))]
    IdentityValidationTaskPanicked {
        path: std::path::PathBuf,
        source: tokio::task::JoinError,
    },

    #[snafu(display(
        "The Cloud Connect identity at {} cannot be used: {reason}. Stop spiced, run `spice connect remove --yes` from this instance directory, mint a new enrollment key in the Spice Cloud portal, and restart with `spiced --token <enrollment-key>`. See: https://spiceai.org/docs",
        path.display()
    ))]
    IdentityUnusable {
        path: std::path::PathBuf,
        reason: String,
    },

    #[snafu(display(
        "Failed to retire the completed Cloud Connect enrollment draft before startup: {source}. The durable identity remains authoritative; fix the config-directory permissions and restart without minting another enrollment key"
    ))]
    DraftCleanup { source: draft::Error },

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

/// A persisted identity that passed the complete local reconnectability gate.
///
/// The inner value is intentionally not constructible by callers. Carrying
/// this token from early bootstrap into [`CloudConnect`] prevents later startup
/// phases from making a second, potentially different activation decision.
#[derive(Clone, Debug)]
pub struct ReconnectableIdentity {
    identity: Identity,
    config: CloudConnectConfig,
}

impl ReconnectableIdentity {
    /// The stable cloud identifier, for diagnostics that do not need access to
    /// credential material.
    #[must_use]
    pub fn identifier(&self) -> &str {
        &self.identity.identifier
    }

    /// The application currently receiving this instance's metrics.
    #[must_use]
    pub fn app_id(&self) -> Option<&str> {
        self.identity.app_id.as_deref()
    }

    /// Decode the local delivered-secrets cache key without exposing the
    /// identity's mTLS or encryption private keys.
    #[must_use]
    pub fn cache_key(&self) -> Option<identity::CacheKey> {
        self.identity.cache_key()
    }

    /// The exact configuration used to validate this startup snapshot.
    #[must_use]
    pub fn config(&self) -> &CloudConnectConfig {
        &self.config
    }
}

/// Load the durable identity that may activate Cloud Connect.
///
/// This is the shared activation boundary for the runtime, early bootstrap
/// facilities, and service installation. A file being present or parseable is
/// insufficient: the credential, key relationships, signed validity interval,
/// and persisted endpoint shape must all be usable.
///
/// # Errors
///
/// Returns [`Error::IdentityLoad`] when the state path is unreadable, unsafe, or
/// malformed, and [`Error::IdentityUnusable`] when its durable contents cannot
/// establish or renew a control stream.
pub fn load_reconnectable_identity(
    config: &CloudConnectConfig,
) -> Result<Option<ReconnectableIdentity>> {
    let identity_path = config.identity_path.clone();
    let Some(identity) =
        IdentityStore::load_optional(&identity_path).map_err(|source| Error::IdentityLoad {
            path: identity_path.clone(),
            source,
        })?
    else {
        return Ok(None);
    };
    validate_reconnectable_identity(config, identity).map(Some)
}

/// Validate an already-loaded identity against the complete reconnectability
/// boundary.
///
/// # Errors
///
/// Returns [`Error::IdentityUnusable`] when the durable credential, signed
/// validity interval, or endpoint shape cannot establish or renew a control
/// stream. Host-local channel setup is retried by the driver and is not part of
/// this durable-state decision.
pub fn validate_reconnectable_identity(
    config: &CloudConnectConfig,
    identity: Identity,
) -> Result<ReconnectableIdentity> {
    let mut effective_config = config.clone();
    if let Some(bound) = identity.control_plane_endpoint.as_deref() {
        effective_config.enroll_endpoint = config::normalize_control_plane_endpoint(bound)
            .map_err(|_| Error::IdentityUnusable {
                path: config.identity_path.clone(),
                reason: "the bound control-plane endpoint is invalid".to_string(),
            })?;
    }
    let (identity, _endpoint) = validate_reconnectable_credential(&effective_config, identity)?;

    Ok(ReconnectableIdentity {
        identity,
        config: effective_config,
    })
}

/// Validate the durable credential independently of the current host's clock
/// and transport environment, returning its normalized identity and endpoint.
///
/// The control plane is authoritative for whether the signed interval includes
/// its current time. A host clock can schedule an early or late renewal, but it
/// must not reject a credential the cloud has already committed. Channel setup
/// is likewise retried by the driver, so a temporarily unavailable native trust
/// store cannot turn valid durable state into a process-lifetime disablement.
pub(crate) fn validate_reconnectable_credential(
    config: &CloudConnectConfig,
    mut identity: Identity,
) -> Result<(Identity, String)> {
    let identity_path = config.identity_path.clone();

    if let Some(reason) = identity.reconnect_validation_error() {
        return Err(Error::IdentityUnusable {
            path: identity_path,
            reason: reason.to_string(),
        });
    }
    let control_plane_endpoint = identity
        .control_plane_endpoint
        .as_deref()
        .unwrap_or(&config.enroll_endpoint);
    config::normalize_control_plane_endpoint(control_plane_endpoint).map_err(|_| {
        Error::IdentityUnusable {
            path: identity_path.clone(),
            reason: "the bound control-plane endpoint is invalid".to_string(),
        }
    })?;
    let (not_before, not_after) =
        identity
            .certificate_validity_unix()
            .map_err(|reason| Error::IdentityUnusable {
                path: identity_path.clone(),
                reason: reason.to_string(),
            })?;
    let signed_not_after = u64::try_from(not_after).map_err(|_| Error::IdentityUnusable {
        path: identity_path.clone(),
        reason: "the certificate expiration precedes the Unix epoch".to_string(),
    })?;
    if not_after <= not_before {
        return Err(Error::IdentityUnusable {
            path: identity_path,
            reason: "the certificate validity interval is empty or inverted".to_string(),
        });
    }
    // The signed certificate is authoritative. Normalize the unsigned cache
    // field before the driver uses it to schedule renewal.
    identity.not_after_unix = Some(signed_not_after);
    let endpoint = config
        .validated_persisted_gateway_endpoint(&identity)
        .map_err(|reason| Error::IdentityUnusable {
            path: identity_path,
            reason: reason.to_string(),
        })?;
    Ok((identity, endpoint))
}

/// Async variant of [`validate_reconnectable_identity`] for CLI and runtime
/// paths already executing on Tokio.
///
/// # Errors
///
/// Returns the same validation errors as [`validate_reconnectable_identity`],
/// or [`Error::IdentityValidationTaskPanicked`] if the blocking task fails.
pub async fn validate_reconnectable_identity_async(
    config: &CloudConnectConfig,
    identity: Identity,
) -> Result<ReconnectableIdentity> {
    let config = config.clone();
    let path = config.identity_path.clone();
    tokio::task::spawn_blocking(move || validate_reconnectable_identity(&config, identity))
        .await
        .map_err(|source| Error::IdentityValidationTaskPanicked { path, source })?
}

pub(crate) async fn validate_reconnectable_credential_async(
    config: &CloudConnectConfig,
    identity: Identity,
) -> Result<(Identity, String)> {
    let config = config.clone();
    let path = config.identity_path.clone();
    tokio::task::spawn_blocking(move || validate_reconnectable_credential(&config, identity))
        .await
        .map_err(|source| Error::IdentityValidationTaskPanicked { path, source })?
}

/// Async variant of [`load_reconnectable_identity`] for startup paths running
/// on Tokio. File and certificate parsing stay on the blocking pool.
///
/// # Errors
///
/// Returns the same errors as [`load_reconnectable_identity`], plus
/// [`Error::IdentityLoadTaskPanicked`] if the blocking task fails.
pub async fn load_reconnectable_identity_async(
    config: &CloudConnectConfig,
) -> Result<Option<ReconnectableIdentity>> {
    let config = config.clone();
    let path = config.identity_path.clone();
    tokio::task::spawn_blocking(move || load_reconnectable_identity(&config))
        .await
        .map_err(|source| Error::IdentityLoadTaskPanicked { path, source })?
}

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
    /// `config.identity_path` but cannot be read or parsed, or
    /// [`Error::IdentityUnusable`] if its reconnect credentials are
    /// internally inconsistent.
    pub async fn start(
        mut config: CloudConnectConfig,
        runtime: Arc<dyn RuntimeHandle>,
    ) -> Result<Option<Self>> {
        // Acquire the cross-process transaction before the authoritative read
        // and retain it until the driver task has captured the same identity.
        // Otherwise a concurrent release can clear the file after this read
        // and the stale in-memory credential can reconnect the removed host.
        let requested_config_dir = config.config_dir.clone();
        let transaction = tokio::task::spawn_blocking(move || {
            EnrollmentTransactionLock::acquire(&requested_config_dir)
        })
        .await
        .map_err(|source| Error::DraftCleanup {
            source: draft::Error::DeleteTaskPanicked { source },
        })?
        .map_err(|source| Error::DraftCleanup { source })?;
        let pinned_config_dir = transaction
            .config_dir()
            .map_err(|source| Error::DraftCleanup { source })?
            .to_path_buf();
        config.pin_config_dir(pinned_config_dir);
        let identity_path = config.identity_path.clone();

        let load_config = config.clone();
        let (transaction, identity) = tokio::task::spawn_blocking(move || {
            let identity = load_reconnectable_identity(&load_config);
            (transaction, identity)
        })
        .await
        .map_err(|source| Error::IdentityLoadTaskPanicked {
            path: identity_path.clone(),
            source,
        })?;
        let identity = identity?;

        let Some(identity) = identity else {
            tracing::debug!(
                "Cloud Connect disabled: no identity at {}",
                identity_path.display()
            );
            return Ok(None);
        };
        // Promotion is not complete until its retry operation ID is gone. A
        // previous cleanup may have failed after the identity became durable;
        // scrub that stale draft on every valid-identity startup so it can
        // never be replayed if the identity is later explicitly removed.
        let (transaction, deletion) = tokio::task::spawn_blocking(move || {
            let deletion = transaction.delete();
            (transaction, deletion)
        })
        .await
        .map_err(|source| Error::DraftCleanup {
            source: draft::Error::DeleteTaskPanicked { source },
        })?;
        deletion.map_err(|source| Error::DraftCleanup { source })?;
        let client = Self::start_reconnectable(runtime, identity, None);
        drop(transaction);
        Ok(Some(client))
    }

    /// Start from the exact identity snapshot validated during early startup.
    ///
    /// This does not reload on-disk state: metrics, logging, managed
    /// configuration, cached secrets, and the control client must all follow
    /// one activation decision for this process.
    ///
    /// `session_ack` receives the first control-plane acknowledgement, for a
    /// caller that reports the connection once the runtime is also serving.
    /// `None` for callers that report nothing.
    #[must_use]
    pub fn start_reconnectable(
        runtime: Arc<dyn RuntimeHandle>,
        identity: ReconnectableIdentity,
        session_ack: Option<Arc<SessionAck>>,
    ) -> Self {
        let ReconnectableIdentity { identity, config } = identity;
        let identity = Some(identity);

        let shutdown = Shutdown::new();
        let shutdown_for_task = Arc::clone(&shutdown);

        let runtime_for_task = runtime;
        let task = tokio::spawn(async move {
            let driver = client::ClientDriver::new(
                config,
                runtime_for_task,
                shutdown_for_task,
                identity,
                session_ack,
            );
            if let Err(err) = driver.run().await {
                tracing::error!("Cloud Connect driver exited with error: {err}");
            }
        });
        Self {
            task: Mutex::new(Some(task)),
            shutdown,
        }
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
/// a label it does not recognize.
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

    fn test_config(config_dir: &std::path::Path) -> CloudConnectConfig {
        CloudConnectConfig {
            enroll_endpoint: "https://api.spice.ai".to_string(),
            gateway_endpoint: None,
            ca_cert_pem: None,
            insecure: false,
            identity_path: config_dir.join(config::IDENTITY_FILE),
            config_dir: config_dir.to_path_buf(),
            instance_region: None,
            runtime_version: "v0-test".to_string(),
            heartbeat_interval: std::time::Duration::from_secs(30),
            telemetry_interval: std::time::Duration::from_mins(1),
            metrics_interval: std::time::Duration::from_secs(30),
            renewal_lead: std::time::Duration::from_hours(12),
            query_deadline: std::time::Duration::from_secs(25),
        }
    }

    fn test_identity() -> Identity {
        use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair};

        let material = IdentityStore::generate_enrollment().expect("generate enrollment material");
        let keypair = KeyPair::from_pem(&material.private_key_pem).expect("parse identity key");
        let mut params =
            CertificateParams::new(Vec::<String>::new()).expect("certificate parameters");
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let certificate = params.self_signed(&keypair).expect("sign certificate");
        let (_, parsed_certificate) =
            x509_parser::prelude::parse_x509_certificate(certificate.der().as_ref())
                .expect("parse test identity certificate");
        let not_after_unix = u64::try_from(parsed_certificate.validity().not_after.timestamp())
            .expect("test identity expiry after the Unix epoch");
        let certificate_pem = certificate.pem();
        Identity {
            identifier: "inst_test".to_string(),
            identity_cert_pem: certificate_pem.clone(),
            private_key_pem: material.private_key_pem,
            public_key_pem: material.public_key_pem,
            ca_bundle_pem: certificate_pem,
            gateway_addr: "gateway.example:443".to_string(),
            not_after_unix: Some(not_after_unix),
            control_plane_endpoint: None,
            new_project_url: None,
            enc_private_key_pem: material.enc_private_key_pem,
            enc_public_key_pem: material.enc_public_key_pem,
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
            app_id: None,
            org_name: None,
            app_name: None,
            monitor_url: None,
        }
    }

    fn set_certificate_validity(
        identity: &mut Identity,
        not_before: (i32, u8, u8),
        not_after: (i32, u8, u8),
    ) {
        use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair};

        let keypair = KeyPair::from_pem(&identity.private_key_pem).expect("parse identity key");
        let mut params =
            CertificateParams::new(Vec::<String>::new()).expect("certificate parameters");
        params.not_before = rcgen::date_time_ymd(not_before.0, not_before.1, not_before.2);
        params.not_after = rcgen::date_time_ymd(not_after.0, not_after.1, not_after.2);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let certificate = params.self_signed(&keypair).expect("sign certificate");
        let (_, parsed_certificate) =
            x509_parser::prelude::parse_x509_certificate(certificate.der().as_ref())
                .expect("parse test identity certificate");
        identity.not_after_unix = Some(
            u64::try_from(parsed_certificate.validity().not_after.timestamp())
                .expect("test identity expiry after the Unix epoch"),
        );
        let certificate_pem = certificate.pem();
        identity.identity_cert_pem.clone_from(&certificate_pem);
        identity.ca_bundle_pem = certificate_pem;
    }

    fn set_certificate_validity_from_now(
        identity: &mut Identity,
        not_before: std::time::Duration,
        not_after: std::time::Duration,
    ) {
        use rcgen::{CertificateParams, KeyPair};

        let keypair = KeyPair::from_pem(&identity.private_key_pem).expect("parse identity key");
        let now = std::time::SystemTime::now();
        let mut params =
            CertificateParams::new(Vec::<String>::new()).expect("certificate parameters");
        params.not_before = (now + not_before).into();
        params.not_after = (now + not_after).into();
        identity.identity_cert_pem = params
            .self_signed(&keypair)
            .expect("sign certificate")
            .pem();
    }

    #[test]
    fn durable_activation_requires_a_reconnectable_identity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());
        let identity = test_identity();
        IdentityStore::store(&config.identity_path, &identity).expect("store identity");

        let loaded = load_reconnectable_identity(&config)
            .expect("validate identity")
            .expect("identity is present");
        assert_eq!(loaded.identifier(), identity.identifier);
    }

    #[test]
    fn durable_activation_uses_signed_expiry_and_rejects_invalid_endpoints() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());
        let mut identity = test_identity();
        set_certificate_validity(&mut identity, (2019, 1, 1), (2020, 1, 1));
        IdentityStore::store(&config.identity_path, &identity).expect("store expired identity");
        let expired = load_reconnectable_identity(&config)
            .expect("the control plane decides whether the identity remains renewable")
            .expect("identity is present");
        assert_eq!(expired.identity.not_after_unix, Some(1_577_836_800));

        set_certificate_validity(&mut identity, (2025, 1, 1), (2099, 1, 1));
        identity.gateway_addr = "not a valid gateway".to_string();
        IdentityStore::store(&config.identity_path, &identity).expect("store invalid endpoint");
        let invalid = load_reconnectable_identity(&config).expect_err("endpoint must fail");
        assert!(
            invalid.to_string().contains("endpoint is invalid"),
            "{invalid}"
        );
    }

    #[test]
    fn durable_activation_uses_the_bound_control_plane() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = test_config(dir.path());
        let mut identity = test_identity();
        identity.control_plane_endpoint = Some(config.enroll_endpoint.clone());

        config.enroll_endpoint = "https://other-control-plane.example".to_string();
        let reconnectable = validate_reconnectable_identity(&config, identity)
            .expect("the durable identity endpoint is authoritative");
        assert_eq!(
            reconnectable.config().enroll_endpoint,
            "https://api.spice.ai"
        );
    }

    #[test]
    fn durable_activation_uses_signed_validity_and_exact_gateway_shape() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = test_config(dir.path());
        let mut identity = test_identity();
        set_certificate_validity(&mut identity, (2025, 1, 1), (2099, 1, 1));
        for gateway in [
            "gateway.example",
            "http://gateway.example:80",
            "gateway.example:443/path",
        ] {
            identity.gateway_addr = gateway.to_string();
            let error = validate_reconnectable_identity(&config, identity.clone())
                .expect_err("persisted gateway must be exactly host:port");
            assert!(error.to_string().contains("gateway endpoint"), "{error}");
        }

        identity.gateway_addr = "gateway.example:443".to_string();
        for override_endpoint in [
            "unix:///tmp/cloud-connect.sock",
            "http://gateway.example:443",
            "https://gateway.example:443/path",
        ] {
            config.gateway_endpoint = Some(override_endpoint.to_string());
            validate_reconnectable_identity(&config, identity.clone()).expect(
                "a process-local gateway override must not decide durable identity activation",
            );
        }

        identity.gateway_addr.clear();
        config.gateway_endpoint = Some("https://gateway.example:443".to_string());
        let error = validate_reconnectable_identity(&config, identity)
            .expect_err("an override must not replace missing durable gateway state");
        assert!(error.to_string().contains("gateway address is empty"));
    }

    #[test]
    fn durable_activation_does_not_use_the_host_clock_as_credential_authority() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = test_config(dir.path());
        let mut identity = test_identity();
        set_certificate_validity_from_now(
            &mut identity,
            std::time::Duration::from_hours(24),
            std::time::Duration::from_hours(48),
        );

        validate_reconnectable_credential(&config, identity)
            .expect("the cloud-committed credential must survive a slow host clock");
    }

    #[test]
    fn reconnectable_identity_is_a_stable_startup_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = test_config(dir.path());
        let identity = test_identity();
        IdentityStore::store(&config.identity_path, &identity).expect("store identity");
        let snapshot = load_reconnectable_identity(&config)
            .expect("validate identity")
            .expect("identity present");

        let mut replacement = identity;
        replacement.identifier = "inst_replaced_after_bootstrap".to_string();
        IdentityStore::store(&config.identity_path, &replacement).expect("replace identity");
        config.gateway_endpoint = Some("https://other.example:443".to_string());

        assert_eq!(snapshot.identifier(), "inst_test");
        assert_ne!(snapshot.identifier(), replacement.identifier);
        assert!(snapshot.config().gateway_endpoint.is_none());
        assert_ne!(snapshot.config().gateway_endpoint, config.gateway_endpoint);
    }

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
