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

//! Outbound client: mTLS gateway stream over a pre-obtained identity.
//!
//! Identity is obtained **before** any gRPC stream — and before this
//! driver even starts (see [`crate::enroll::enroll_now`]): enrollment is
//! an explicit pre-runtime step, and the driver only ever runs with a
//! stored identity. It connects to the stateless gateway over **mTLS**
//! (the leaf is the credential, which is why the `Hello` carries none) and
//! enters a long-running loop that processes `ControlMessage`s from the
//! server and emits `ClientMessage`s back (heartbeats, command results,
//! telemetry).
//!
//! Disconnects are tolerated: the driver reconnects with exponential
//! backoff (1s → 60s with jitter), always presenting the identity leaf.
//!
//! The identity is renewed on a ~12h cadence (see
//! [`crate::config::DEFAULT_RENEWAL_LEAD`]) against the cloud `/renew`
//! endpoint, both from the live stream loop and before reconnect attempts;
//! every renewal rotates the keypair. An expired leaf can still renew
//! within the 30-day grace window ([`crate::enroll::RENEWAL_GRACE`]);
//! past it a fresh enrollment key is required.
//!
//! `Adopt` over the stream is a trust/marker message (the portal admin
//! confirmed the instance) — the cert was already issued at enroll, so the
//! client just acknowledges against the identity it holds.
//!
//! If a `Remove` arrives, we clear the local identity from disk and, on
//! success, exit the cloud-connect task — spiced itself stays up and keeps
//! serving local spicepod traffic as before: a release stops management
//! but doesn't destroy the device. To re-enroll, the user starts the
//! runtime with a fresh enrollment key (`spiced --token`). If the on-disk
//! identity cannot be cleared, the `Remove` is reported as failed and the
//! driver stays connected with the still-valid identity rather than
//! falsely reporting the instance as released.

use std::sync::Arc;
use std::time::Duration;

use crate::TransportSnafu;
use snafu::ResultExt;
use tokio::sync::{RwLock, Semaphore, mpsc};
use tokio::time;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use crate::config::CloudConnectConfig;
use crate::enroll::EnrollClient;
use crate::handlers::{
    Capability, CommandError, MAX_QUERY_RESULT_BYTES, PostApply, RestartMode, RuntimeHandle,
    SpicepodDeployment, advertised_capabilities, effective_max_rows,
};
use crate::heartbeat::{build_heartbeat, build_telemetry, now_unix};
use crate::identity::{Identity, IdentityStore};
use crate::proto;
use crate::shutdown::Shutdown;
use crate::{Error, Result, enroll, fingerprint};

/// Minimum reconnect backoff.
const MIN_BACKOFF: Duration = Duration::from_secs(1);
/// Maximum reconnect backoff.
const MAX_BACKOFF: Duration = Duration::from_mins(1);

/// How long after a failed (transient) in-stream renewal attempt the next
/// attempt is made. Short enough to fit many retries into the grace
/// window, long enough not to hammer the cloud.
const RENEW_RETRY_INTERVAL: Duration = Duration::from_mins(5);

/// Outbound channel size: bounded to keep memory predictable.
const CLIENT_CHANNEL_SIZE: usize = 64;

/// How long the client waits for the outbound channel to drain before exiting
/// the process to apply a deployment, and how often it re-checks.
///
/// Best-effort by design: the spicepod is already persisted and the restart is
/// what makes it live, so a slow gateway delays the exit by at most this budget
/// rather than stalling the deployment on a result nobody is waiting for.
const APPLY_FLUSH_BUDGET: Duration = Duration::from_secs(5);
const APPLY_FLUSH_POLL: Duration = Duration::from_millis(25);

/// State held by the driver across reconnects.
pub(crate) struct ClientDriver {
    config: CloudConnectConfig,
    runtime: Arc<dyn RuntimeHandle>,
    shutdown: Arc<Shutdown>,
    /// Currently-effective identity, if any. Set by out-of-band enrollment,
    /// replaced on renewal (rotated keypair); cleared on `Remove` or when
    /// the cloud permanently refuses renewal (revocation).
    identity: Option<Identity>,
    /// Earliest instant the next in-stream renewal attempt may run — set
    /// after every renewal attempt (failed or successful) so attempts are
    /// paced by [`RENEW_RETRY_INTERVAL`] rather than spinning on a
    /// due-in-the-past `not_after`. Cleared on enrollment.
    renew_not_before: Option<time::Instant>,
    /// The single `ExecuteQuery` slot. Its one permit is taken by the spawned
    /// query task and released when that task ends, so a second query is
    /// refused as busy before it executes. Held on the driver rather than the
    /// stream so a reconnect cannot hand out a second slot while the first
    /// query is still running.
    query_slot: Arc<Semaphore>,
}

impl ClientDriver {
    pub(crate) fn new(
        config: CloudConnectConfig,
        runtime: Arc<dyn RuntimeHandle>,
        shutdown: Arc<Shutdown>,
        identity: Option<Identity>,
    ) -> Self {
        Self {
            config,
            runtime,
            shutdown,
            identity,
            renew_not_before: None,
            query_slot: Arc::new(Semaphore::new(1)),
        }
    }

    /// Run the driver until shutdown is requested.
    ///
    /// The driver is fully fault-tolerant: a transport, decode, or
    /// stream error triggers a reconnect with backoff; only an explicit
    /// shutdown notify, a `Remove`, or a non-recoverable credential state
    /// exits the loop.
    pub(crate) async fn run(mut self) -> Result<()> {
        let enroll_client = match EnrollClient::new(&self.config) {
            Ok(client) => client,
            Err(err) => {
                tracing::error!(
                    "Cloud Connect: failed to initialize the enrollment client for {}: {err}; exiting cloud-connect. Fix the configuration and restart spiced.",
                    self.config.enroll_endpoint
                );
                return Ok(());
            }
        };

        let mut backoff = MIN_BACKOFF;

        loop {
            // Honor shutdown before each attempt.
            if self.shutdown.is_triggered() {
                tracing::info!("Cloud Connect: shutdown requested; exiting driver");
                return Ok(());
            }

            // Ensure a usable identity: renew when the current leaf is due.
            match self.ensure_credentials(&enroll_client).await {
                CredentialStep::Ready => {}
                CredentialStep::Retry => {
                    if !self.sleep_backoff(&mut backoff).await {
                        return Ok(());
                    }
                    continue;
                }
                CredentialStep::Exit => return Ok(()),
            }

            // Connect the mTLS stream to the gateway.
            let Some(endpoint) = self.stream_endpoint() else {
                tracing::error!(
                    "Cloud Connect: the stored identity at {} has no gateway address (it predates the enroll-first flow); exiting cloud-connect. Stop spiced, remove this identity file, mint a new enrollment key in the Spice Cloud portal, and restart with `spiced --token <enrollment-key>`. The existing identity always wins, so merely supplying --token without removing the unusable file cannot re-enroll.",
                    self.config.identity_path.display()
                );
                return Ok(());
            };

            tracing::debug!(
                "Cloud Connect: attempting connect to gateway {} (identifier={})",
                endpoint,
                self.identity
                    .as_ref()
                    .map_or("<none>", |i| i.identifier.as_str()),
            );

            match self.connect_and_run(&enroll_client, &endpoint).await {
                Ok(ExitReason::Shutdown) => return Ok(()),
                Ok(ExitReason::Removed) => {
                    tracing::info!(
                        "Cloud Connect: Remove acknowledged; this instance was released and the cloud-connect task is exiting. spiced remains running and serving local spicepod traffic. To re-enroll, mint a new enrollment key in the Spice Cloud portal and restart spiced with `--token <enrollment-key>`."
                    );
                    return Ok(());
                }
                Ok(ExitReason::IdentityRevoked) => {
                    // The in-stream renewal was permanently refused and the
                    // identity was cleared; loop back — the no-credentials
                    // branch exits with the re-enrollment guidance.
                    tracing::warn!(
                        "Cloud Connect: identity renewal was refused by the control plane; reconnecting with remaining credentials"
                    );
                }
                Ok(ExitReason::Disconnected) => {
                    // Treat clean server-side disconnect as a transient
                    // event; back off and retry.
                    tracing::warn!("Cloud Connect: server closed stream; reconnecting");
                }
                Err(err) => {
                    tracing::warn!("Cloud Connect: connection error: {err}");
                }
            }

            if !self.sleep_backoff(&mut backoff).await {
                return Ok(());
            }
        }
    }

    /// Pre-connect credential phase: drop an unrenewable identity and renew
    /// a due one. Returns what the connect loop should do next.
    ///
    /// The driver never enrolls — enrollment is an explicit pre-runtime
    /// step ([`crate::enroll::enroll_now`]) — so running out of usable
    /// credentials exits the task with re-enrollment guidance.
    async fn ensure_credentials(&mut self, enroll_client: &EnrollClient) -> CredentialStep {
        // An identity past the renewal grace window can no longer be
        // renewed by the cloud — only a fresh enrollment key helps.
        if let Some(ref id) = self.identity
            && enroll::past_renewal_grace(id)
        {
            tracing::warn!(
                "Cloud Connect: the stored identity expired past the renewal grace window and can no longer be renewed"
            );
            self.identity = None;
        }

        if self.identity.is_none() {
            tracing::error!(
                "Cloud Connect: cannot connect (no usable identity); exiting cloud-connect. Mint a new enrollment key in the Spice Cloud portal and restart spiced with `--token <enrollment-key>` to re-enroll. See: https://spiceai.org/docs"
            );
            return CredentialStep::Exit;
        }

        // Renew before connecting when due — this covers the
        // expired-within-grace startup case, where the gateway would
        // reject the old leaf but /renew still accepts it.
        if self
            .identity
            .as_ref()
            .is_some_and(|id| renewal_due(id, self.config.renewal_lead))
        {
            match self.renew_once(enroll_client).await {
                Ok(()) => {}
                Err(err) if err.is_credential_rejection() => {
                    // A 401 is the cloud's credential revocation signal. It
                    // is the sole response that proves the local identity is
                    // dead; other terminal request errors must preserve it.
                    tracing::error!(
                        "Cloud Connect: identity renewal was refused: {err}; clearing the local identity"
                    );
                    self.clear_identity().await;
                    return CredentialStep::Retry;
                }
                Err(err) => {
                    // Transient, or a local key-material/PoP failure — the
                    // cloud did not reject us, so keep the identity: the
                    // existing leaf may still be usable to connect.
                    tracing::warn!("Cloud Connect: identity renewal failed (will retry): {err}");
                    if self.identity.as_ref().is_some_and(Identity::is_expired) {
                        // The leaf is expired: the gateway will reject it, so
                        // there is nothing to connect with until a renewal
                        // succeeds. Back off and retry.
                        return CredentialStep::Retry;
                    }
                    // Leaf still valid: connect now; the in-stream renewal
                    // timer keeps retrying.
                }
            }
        }

        CredentialStep::Ready
    }

    /// Sleep the current backoff (+ jitter), racing shutdown, then double
    /// the backoff up to [`MAX_BACKOFF`]. Returns `false` when shutdown
    /// fired during the sleep.
    async fn sleep_backoff(&self, backoff: &mut Duration) -> bool {
        let jitter_ms: u64 = rand::random::<u64>() % 500;
        let sleep_for = *backoff + Duration::from_millis(jitter_ms);
        tracing::debug!(
            "Cloud Connect: sleeping {} before retrying",
            humanize(sleep_for)
        );
        let proceed = tokio::select! {
            () = time::sleep(sleep_for) => true,
            () = self.shutdown.wait() => {
                tracing::info!("Cloud Connect: shutdown requested during backoff; exiting");
                false
            }
        };
        *backoff = next_backoff(*backoff);
        proceed
    }

    /// The gRPC endpoint the stream connects to: the configured override
    /// when present, otherwise the gateway address issued at enroll.
    /// `None` when the identity carries no gateway address (a pre-split
    /// identity file) — non-recoverable without re-enrolling.
    fn stream_endpoint(&self) -> Option<String> {
        if let Some(ref endpoint) = self.config.gateway_endpoint {
            return Some(endpoint.clone());
        }
        let identity = self.identity.as_ref()?;
        if identity.gateway_addr.is_empty() {
            return None;
        }
        let scheme = if self.config.insecure {
            "http"
        } else {
            "https"
        };
        Some(format!("{scheme}://{}", identity.gateway_addr))
    }

    /// Renew the identity against the cloud `/renew` endpoint with a fresh
    /// keypair (every renewal rotates the keypair) and persist the rotated
    /// identity.
    async fn renew_once(&mut self, client: &EnrollClient) -> Result<(), enroll::Error> {
        let Some(current) = self.identity.clone() else {
            return Ok(());
        };
        let material = IdentityStore::generate_enrollment().map_err(|source| {
            enroll::Error::ProofOfPossession {
                reason: format!("failed to generate renewal key material: {source}"),
            }
        })?;
        let outcome = client.renew(&current, &material).await?;

        let mut rotated = Identity {
            identifier: current.identifier,
            identity_cert_pem: outcome.identity_cert_pem,
            private_key_pem: material.private_key_pem,
            public_key_pem: material.public_key_pem,
            // The CA bundle and gateway address are not re-sent on renewal.
            ca_bundle_pem: current.ca_bundle_pem,
            gateway_addr: current.gateway_addr,
            not_after_unix: Some(outcome.not_after_unix),
            // The app attribution is not part of the credential and /renew
            // does not re-send it, so it rides across the rotation unchanged.
            app_id: current.app_id,
            // Seeded with the OUTGOING keypair and rotated by the call below,
            // which shifts it into `enc_previous_private_key_pem` — assigning
            // `material` here instead would leave the retained key equal to the
            // current one, retaining nothing.
            enc_private_key_pem: current.enc_private_key_pem,
            enc_public_key_pem: current.enc_public_key_pem,
            enc_previous_private_key_pem: current.enc_previous_private_key_pem,
            // Local and deliberately never rotated: the cache must stay
            // readable across every identity rotation.
            cache_key_b64: current.cache_key_b64,
        };
        // The encryption keypair rotates alongside the identity keypair on each
        // renewal: the renew request already carried this public key, so the
        // cloud pinned it in the same transaction that issued the new leaf and
        // seals to it from that commit on.
        //
        // The outgoing private key is retained for exactly one rotation, because
        // a payload sealed moments before this point is still addressed to it and
        // cannot be re-sealed in flight.
        rotated.rotate_encryption_key(material.enc_private_key_pem, material.enc_public_key_pem);
        // The cloud has already pinned the new public key: even if
        // persistence fails, the rotated identity must be used in memory
        // (the old key can no longer renew). Persistence logs the failure;
        // the next successful renewal re-attempts the write.
        rotated = self
            .persist_identity_preserving_attachment(rotated, "renewed identity")
            .await;
        tracing::info!(
            "Cloud Connect: identity renewed for {} (identity and encryption keypairs rotated, valid until {})",
            rotated.identifier,
            rotated
                .not_after_unix
                .map_or_else(|| "no expiry".to_string(), |secs| format!("unix={secs}")),
        );
        self.identity = Some(rotated);
        // Pace successful renewals too: if the cloud ever issues a leaf
        // already inside the renewal lead (validity <= lead, or heavy clock
        // skew), an unfloored timer would loop back-to-back renewals. The
        // floor is far below the ~12h cadence, so normal operation never
        // waits on it.
        self.renew_not_before = Some(time::Instant::now() + RENEW_RETRY_INTERVAL);
        Ok(())
    }

    /// Persist a full credential update while retaining the attachment most
    /// recently written by a command handler.
    async fn persist_identity_preserving_attachment(
        &self,
        identity: Identity,
        update: &'static str,
    ) -> Identity {
        let path = self.config.identity_path.clone();
        let fallback = identity.clone();
        let result = tokio::task::spawn_blocking(move || {
            IdentityStore::store_credential_update(&path, &identity)
        })
        .await;
        match result {
            Ok(Ok(Some(merged))) => merged,
            Ok(Ok(None)) => {
                tracing::error!(
                    "Cloud Connect: identity disappeared while the {update} was being persisted at {}; continuing with the updated in-memory identity",
                    self.config.identity_path.display()
                );
                fallback
            }
            Ok(Err(error)) => {
                tracing::error!(
                    "Cloud Connect: failed to persist the {update} at {}: {error}; continuing with the updated in-memory identity (it will be lost on restart)",
                    self.config.identity_path.display()
                );
                fallback
            }
            Err(error) => {
                tracing::error!(
                    "Cloud Connect: {update} persistence task failed at {}: {error}; continuing with the updated in-memory identity (it will be lost on restart)",
                    self.config.identity_path.display()
                );
                fallback
            }
        }
    }

    /// Clear the identity from disk and memory (best-effort on the disk
    /// side — unlike `Remove`, the cloud has already invalidated it, so a
    /// stale file only produces a failed renewal on the next start).
    async fn clear_identity(&mut self) {
        if let Err(err) = IdentityStore::clear_async(&self.config.identity_path).await {
            tracing::warn!(
                "Cloud Connect: failed to clear identity at {}: {err}",
                self.config.identity_path.display()
            );
        }
        self.identity = None;
    }

    /// Delay until the next renewal attempt, or `None` when the identity
    /// carries no expiry and renewal is moot.
    fn next_renewal_delay(&self) -> Option<Duration> {
        let not_after = self.identity.as_ref()?.not_after_unix?;
        let due_at = not_after.saturating_sub(self.config.renewal_lead.as_secs());
        let due_in = Duration::from_secs(due_at.saturating_sub(now_unix()));
        // After a transient failure, pace retries instead of spinning on a
        // due-in-the-past deadline.
        match self.renew_not_before {
            Some(not_before) => {
                Some(due_in.max(not_before.saturating_duration_since(time::Instant::now())))
            }
            None => Some(due_in),
        }
    }

    async fn connect_and_run(
        &mut self,
        enroll_client: &EnrollClient,
        endpoint: &str,
    ) -> Result<ExitReason> {
        let identity = self.identity.clone().ok_or(Error::NoCredentials)?;
        let channel = build_channel(&self.config, endpoint, &identity)?;
        let mut grpc = proto::cloud_connect_client::CloudConnectClient::new(channel)
            .max_decoding_message_size(16 * 1024 * 1024);

        // Outbound channel: we hand the receiver to tonic and keep the
        // sender to push ClientMessages from this task.
        let (tx, rx) = mpsc::channel::<proto::ClientMessage>(CLIENT_CHANNEL_SIZE);

        // Send Hello as the first frame. The client certificate is the
        // credential, so the Hello only names the instance.
        let hello = build_hello(&self.config, &identity, self.runtime.as_ref());
        tx.send(proto::ClientMessage {
            body: Some(proto::client_message::Body::Hello(hello)),
        })
        .await
        .map_err(|_| Error::NoCredentials)?;

        let request = tonic::Request::new(ReceiverStream::new(rx));
        // Keep shutdown bounded across a slow connect/handshake: if the
        // process is shutting down while `stream()` is still negotiating,
        // abort the connect instead of letting it run detached past the
        // shutdown timeout.
        let response = tokio::select! {
            res = grpc.stream(request) => res.map_err(|status| Error::Stream { source: status })?,
            () = self.shutdown.wait() => {
                tracing::info!("Cloud Connect: shutdown requested during handshake; aborting connect");
                return Ok(ExitReason::Shutdown);
            }
        };

        let mut server_stream: Streaming<proto::ControlMessage> = response.into_inner();
        tracing::info!("Cloud Connect: stream established to {endpoint}");

        // Announce a per-connection encryption key. The gateway seals the
        // control plane's already-sealed secret envelope to this key before
        // dispatching it, so recorded ciphertext stays undecryptable even if the
        // persisted enrolled key is later compromised. A session that announces
        // no key receives no secrets at all — so failing to generate one is a
        // "no secrets this session" degradation, not a reason to drop the stream
        // and lose every other command with it.
        let session_key = match cloud_connect_crypto::EncryptionKeypair::generate() {
            Ok(keypair) => {
                let announcement = proto::SecretsKey {
                    key_id: keypair.key_id().to_string(),
                    kem_id: cloud_connect_crypto::KEM_ID,
                    kdf_id: cloud_connect_crypto::KDF_ID,
                    aead_id: cloud_connect_crypto::AEAD_ID,
                    public_key: keypair.public_key().to_vec(),
                };
                if tx
                    .send(proto::ClientMessage {
                        body: Some(proto::client_message::Body::SecretsKey(announcement)),
                    })
                    .await
                    .is_err()
                {
                    return Ok(ExitReason::Disconnected);
                }
                tracing::debug!(
                    "Cloud Connect: announced per-connection secrets key {}",
                    keypair.key_id()
                );
                Some(Arc::new(keypair))
            }
            Err(err) => {
                tracing::warn!(
                    "Cloud Connect: could not generate a per-connection secrets key ({err}); \
                     this session cannot receive delivered secrets. Deployments still apply, and \
                     any cached secrets remain in effect."
                );
                None
            }
        };

        // Spawn the periodic heartbeat, metrics, and telemetry tasks. They emit
        // through the same outbound channel. The identifier is shared by RwLock
        // so a `Remove` can blank it for frames still in flight on the
        // draining stream.
        let runtime = Arc::clone(&self.runtime);
        let identifier = Arc::new(RwLock::new(identity.identifier.clone()));

        let hb_interval = self.config.heartbeat_interval;
        let tel_interval = self.config.telemetry_interval;

        let hb_tx = tx.clone();
        let hb_runtime = Arc::clone(&runtime);
        let hb_identifier = Arc::clone(&identifier);
        let hb_handle = tokio::spawn(async move {
            let mut seq: u64 = 0;
            let mut ticker = time::interval(hb_interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                seq = seq.wrapping_add(1);
                let id = hb_identifier.read().await.clone();
                let hb = build_heartbeat(&id, seq, &hb_runtime).await;
                let msg = proto::ClientMessage {
                    body: Some(proto::client_message::Body::Heartbeat(hb)),
                };
                if hb_tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // Metrics ride the same stream but not the same delivery guarantee. The
        // payload carries cumulative totals, so a dropped export costs a data
        // point and no data — whereas a late heartbeat is read as an instance
        // going away. This task therefore offers its message and moves on
        // instead of waiting for room behind one.
        let met_tx = tx.clone();
        let met_runtime = Arc::clone(&runtime);
        let met_identifier = Arc::clone(&identifier);
        let met_interval = self.config.metrics_interval;
        // Collecting metrics is CPU work the other periodic tasks do not do, so
        // this one races the shutdown signal rather than relying on the `abort()`
        // below: aborting resolves only at an await point, which can leave a
        // collection running past the shutdown it was supposed to end.
        let met_shutdown = Arc::clone(&self.shutdown);
        let met_handle = tokio::spawn(async move {
            let mut ticker = time::interval(met_interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = met_shutdown.wait() => break,
                    _ = ticker.tick() => {}
                }
                // Every interval accounts for itself: one line per tick, whatever
                // the outcome. An export path that silently stops is
                // indistinguishable from a runtime with nothing to report, which
                // is the failure this whole feature exists to remove.
                let id = met_identifier.read().await.clone();
                if id.is_empty() {
                    tracing::debug!(
                        "Cloud Connect: metrics export skipped — enrollment has not assigned an identifier yet, so nothing could attribute the payload"
                    );
                    continue;
                }
                let payload = match met_runtime.collect_metrics().await {
                    Ok(Some(payload)) => payload,
                    Ok(None) => {
                        tracing::debug!(
                            identifier = %id,
                            "Cloud Connect: metrics export skipped — the runtime reported no metrics to send"
                        );
                        continue;
                    }
                    Err(err) => {
                        tracing::warn!(
                            "Failed to export metrics to Spice Cloud: {err}. Runtime will retry the export on the next interval. Metrics may be delayed to appear in Spice Cloud"
                        );
                        continue;
                    }
                };
                let bytes = payload.len();
                let msg = proto::ClientMessage {
                    body: Some(proto::client_message::Body::ExportMetrics(
                        proto::ExportMetrics {
                            identifier: id.clone(),
                            otlp_request: payload,
                        },
                    )),
                };
                match met_tx.try_send(msg) {
                    Ok(()) => {
                        tracing::debug!(
                            identifier = %id,
                            bytes,
                            "Cloud Connect: metrics export queued to the gateway"
                        );
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        tracing::warn!(
                            identifier = %id,
                            bytes,
                            "Failed to export metrics to Spice Cloud: the outbound queue is full. Runtime will retry the export on the next interval. Metrics may be delayed to appear in Spice Cloud"
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
        });

        let tel_tx = tx.clone();
        let tel_identifier = Arc::clone(&identifier);
        let tel_handle = tokio::spawn(async move {
            let mut ticker = time::interval(tel_interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            let mut last_window = now_unix();
            loop {
                ticker.tick().await;
                let now = now_unix();
                let id = tel_identifier.read().await.clone();
                let t = build_telemetry(&id, last_window, now);
                last_window = now;
                let msg = proto::ClientMessage {
                    body: Some(proto::client_message::Body::Telemetry(t)),
                };
                if tel_tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // Main read loop. The renewal timer runs on the live stream so a
        // long-lived connection keeps its identity fresh — the leaf only
        // lives 24h, while streams can stay up for days.
        let exit_reason = loop {
            let renew_delay = self.next_renewal_delay();
            tokio::select! {
                () = self.shutdown.wait() => {
                    tracing::info!("Cloud Connect: shutdown requested; closing stream");
                    break ExitReason::Shutdown;
                }
                next = server_stream.message() => {
                    match next {
                        Ok(Some(msg)) => {
                            if let Some(reason) = self
                                .dispatch(&tx, msg, &identifier, session_key.as_deref())
                                .await
                            {
                                break reason;
                            }
                        }
                        Ok(None) => {
                            tracing::info!("Cloud Connect: server stream ended");
                            break ExitReason::Disconnected;
                        }
                        Err(status) => {
                            tracing::warn!("Cloud Connect: stream error: {status}");
                            break ExitReason::Disconnected;
                        }
                    }
                }
                () = sleep_or_never(renew_delay) => {
                    match self.renew_once(enroll_client).await {
                        Ok(()) => {}
                        Err(err) if err.is_authoritative_rejection() => {
                            // The cloud refusing renewal IS the revocation
                            // (DR-025): the identity is dead, so clear it and
                            // leave the stream — the outer loop decides whether
                            // a staged code allows re-enrollment.
                            tracing::error!(
                                "Cloud Connect: identity renewal was refused: {err}; clearing the local identity"
                            );
                            self.clear_identity().await;
                            break ExitReason::IdentityRevoked;
                        }
                        Err(err) => {
                            // Transient, or a local key-material/PoP failure —
                            // not a cloud rejection, so keep the identity and
                            // retry; the current leaf keeps the stream alive.
                            tracing::warn!(
                                "Cloud Connect: identity renewal failed (retrying in {}): {err}",
                                humanize(RENEW_RETRY_INTERVAL)
                            );
                            self.renew_not_before =
                                Some(time::Instant::now() + RENEW_RETRY_INTERVAL);
                        }
                    }
                }
            }
        };

        // Cancel background tasks; the channel close gives them an
        // out-of-band exit.
        hb_handle.abort();
        tel_handle.abort();
        met_handle.abort();
        drop(tx);

        Ok(exit_reason)
    }

    async fn dispatch(
        &mut self,
        tx: &mpsc::Sender<proto::ClientMessage>,
        msg: proto::ControlMessage,
        live_identifier: &Arc<RwLock<String>>,
        session_key: Option<&cloud_connect_crypto::EncryptionKeypair>,
    ) -> Option<ExitReason> {
        let command_id = msg.command_id;
        let Some(body) = msg.body else {
            // A control plane newer than this build dispatched a command whose
            // oneof arm prost does not know, which decodes to an absent body.
            // The envelope still carries the command_id, so answer with a NACK
            // instead of leaving the control plane to wait out its timeout.
            if command_id.is_empty() {
                tracing::debug!(
                    "Cloud Connect: received a ControlMessage with no body and no command_id; nothing to answer"
                );
            } else {
                tracing::warn!(
                    "Cloud Connect: received a command this build does not recognize (command_id={command_id}); answering unsupported"
                );
                send_unsupported(
                    tx,
                    &command_id,
                    &format!(
                        "This instance implements Cloud Connect protocol version {} and has no handler for the dispatched command. Upgrade spiced to a build that implements it. See: https://spiceai.org/docs",
                        crate::PROTOCOL_VERSION
                    ),
                )
                .await;
            }
            return None;
        };

        let name = command_name(&body);

        match body {
            proto::control_message::Body::Ack(_) => {
                tracing::debug!("Cloud Connect: ack for command_id={command_id}");
            }
            proto::control_message::Body::GetRuntimeInfo(_) => {
                let info = self.runtime.runtime_info_json().await;
                send_ok_json(tx, &command_id, &info).await;
            }
            proto::control_message::Body::Restart(cmd) => {
                if self
                    .supported(tx, &command_id, Capability::Restart, name)
                    .await
                {
                    let result = self.runtime.restart(restart_mode(cmd.mode)).await;
                    reply_with_json(tx, &command_id, result).await;
                }
            }
            proto::control_message::Body::ApplySpicepod(cmd) => {
                if self
                    .supported(tx, &command_id, Capability::ApplySpicepod, name)
                    .await
                {
                    self.handle_apply_spicepod(tx, &command_id, cmd, session_key)
                        .await;
                }
            }
            proto::control_message::Body::AttachApp(cmd) => {
                if self
                    .supported(tx, &command_id, Capability::AttachApp, name)
                    .await
                {
                    let app_id = cmd.app_id.as_deref();
                    let result = if app_id.is_some_and(str::is_empty) {
                        Err(CommandError::invalid_argument(
                            "AttachApp.app_id must be non-empty when present",
                        ))
                    } else {
                        self.runtime.attach_app(app_id).await
                    };
                    reply_with_json(tx, &command_id, result).await;
                }
            }
            proto::control_message::Body::UpgradeRuntime(cmd) => {
                if self
                    .supported(tx, &command_id, Capability::UpgradeRuntime, name)
                    .await
                {
                    let result = self.runtime.upgrade_runtime(&cmd.target_version).await;
                    reply_with_json(tx, &command_id, result).await;
                }
            }
            proto::control_message::Body::GetStatus(_) => {
                if self
                    .supported(tx, &command_id, Capability::GetStatus, name)
                    .await
                {
                    // A standalone instance *is* the workload, so the envelope
                    // carries no target and the whole runtime's readiness is
                    // what gets reported.
                    let result = self.runtime.status().await.map(|report| report.to_json());
                    reply_with_json(tx, &command_id, result).await;
                }
            }
            proto::control_message::Body::GetLogs(cmd) => {
                if self
                    .supported(tx, &command_id, Capability::GetLogs, name)
                    .await
                {
                    match self.runtime.get_logs(cmd.tail_lines).await {
                        Ok(logs) => {
                            send_ok(
                                tx,
                                &command_id,
                                Some(proto::command_result::Payload::Text(logs)),
                            )
                            .await;
                        }
                        Err(err) => send_command_error(tx, &command_id, &err).await,
                    }
                }
            }
            proto::control_message::Body::ExecuteQuery(cmd) => {
                if self
                    .supported(tx, &command_id, Capability::ExecuteQuery, name)
                    .await
                {
                    self.handle_execute_query(tx, &command_id, cmd).await;
                }
            }
            proto::control_message::Body::Adopt(cmd) => {
                self.handle_adopt(tx, &command_id, cmd, live_identifier)
                    .await;
            }
            proto::control_message::Body::Remove(_) => {
                // Only exit as removed if the identity was actually cleared; on
                // a clear failure stay connected with the still-valid identity
                // rather than falsely exiting as removed.
                if self.handle_remove(tx, &command_id, live_identifier).await {
                    return Some(ExitReason::Removed);
                }
            }
            // Operator-only commands. A standalone runtime has no cluster
            // workload to act on, no kube API to read, and no HPKE key to open
            // a sealed secret with (it neither enrolls an encryption key nor
            // announces a per-connection one). A classified NACK is the
            // fail-closed answer — the alternative is a dispatch the control
            // plane never hears back about.
            proto::control_message::Body::ApplyManifest(_)
            | proto::control_message::Body::DeleteManifest(_)
            | proto::control_message::Body::Drain(_)
            | proto::control_message::Body::Pause(_)
            | proto::control_message::Body::ApplySecrets(_)
            | proto::control_message::Body::DeleteSecrets(_) => {
                send_unsupported(
                    tx,
                    &command_id,
                    &format!("{name} is not supported on standalone instances"),
                )
                .await;
            }
        }

        None
    }

    /// Answer UNSUPPORTED when the runtime does not implement `capability`,
    /// returning whether the caller should go on to dispatch.
    ///
    /// This consults the same [`RuntimeHandle::supports`] the `Hello`
    /// capability list is built from, so what the instance advertises and what
    /// it actually answers cannot drift apart.
    async fn supported(
        &self,
        tx: &mpsc::Sender<proto::ClientMessage>,
        command_id: &str,
        capability: Capability,
        name: &str,
    ) -> bool {
        if self.runtime.supports(capability) {
            return true;
        }
        tracing::debug!(
            "Cloud Connect: {name} was dispatched but '{}' is not in this instance's announced capabilities",
            capability.wire_name()
        );
        send_unsupported(tx, command_id, &self.runtime.unsupported_reason(capability)).await;
        false
    }

    /// Take the query slot and run the query on its own task.
    ///
    /// The pump must stay free to answer heartbeats and other commands while a
    /// query executes, so nothing here awaits the query: the slot permit moves
    /// into the spawned task and is released when that task ends, which is
    /// also what makes a concurrent second query busy rather than queued.
    ///
    /// An empty statement is refused before the slot is taken, so a caller
    /// sending blank queries cannot keep a real one out.
    ///
    /// The query is bounded by `query_deadline` rather than left to run: this
    /// contract has no cancellation command, so a query that never returns
    /// would hold the only slot for the life of the process.
    async fn handle_execute_query(
        &self,
        tx: &mpsc::Sender<proto::ClientMessage>,
        command_id: &str,
        cmd: proto::ExecuteQuery,
    ) {
        if cmd.sql.trim().is_empty() {
            send_result(
                tx,
                command_id,
                proto::ResultCode::InvalidArgument,
                "The query is empty. Send a SQL statement to execute.",
                None,
            )
            .await;
            return;
        }

        let Ok(permit) = Arc::clone(&self.query_slot).try_acquire_owned() else {
            send_result(
                tx,
                command_id,
                proto::ResultCode::Busy,
                "This instance is already running a query. Cloud Connect runs one query at a time — retry once the running query finishes.",
                None,
            )
            .await;
            return;
        };

        let max_rows = effective_max_rows(cmd.max_rows);
        let sql = cmd.sql;
        let runtime = Arc::clone(&self.runtime);
        let tx = tx.clone();
        let command_id = command_id.to_string();
        let deadline = self.config.query_deadline;

        tokio::spawn(async move {
            // Held for the whole query so the slot frees only once the work is
            // genuinely finished, panic or not.
            let _permit = permit;

            let started = time::Instant::now();
            let outcome = time::timeout(deadline, runtime.execute_query(&sql, max_rows)).await;
            let elapsed_ms = started.elapsed().as_millis();

            let Ok(outcome) = outcome else {
                // Dropping the query future abandons the work and, with the
                // permit, frees the slot. Without this a query that never
                // returns would answer every later one as busy for the life of
                // the process — there is no cancellation command to rescue it.
                tracing::warn!(
                    "Cloud Connect: query {command_id} exceeded the {}s deadline; abandoning the waiter and freeing the query slot",
                    deadline.as_secs()
                );
                send_result(
                    &tx,
                    &command_id,
                    proto::ResultCode::Failed,
                    &format!(
                        "The query exceeded this instance's {}s Cloud Connect limit and was abandoned; no result will follow. Narrow it, or run it against the instance directly.",
                        deadline.as_secs()
                    ),
                    None,
                )
                .await;
                return;
            };

            match outcome {
                Ok(result) => {
                    let bytes = result.arrow_ipc.len();
                    // The handle serializes under the same caps, so these are
                    // the contract boundary refusing to put an out-of-bounds
                    // payload on the control stream rather than the primary
                    // enforcement. A handle that miscounts or ignores the caps
                    // is refused here instead of being forwarded.
                    if bytes > MAX_QUERY_RESULT_BYTES {
                        tracing::warn!(
                            "Cloud Connect: query {command_id} produced {bytes} bytes, over the {MAX_QUERY_RESULT_BYTES} byte limit; answering result-too-large"
                        );
                        send_result(
                            &tx,
                            &command_id,
                            proto::ResultCode::ResultTooLarge,
                            &result_too_large_message(),
                            None,
                        )
                        .await;
                        return;
                    }
                    if result.row_count > u64::from(max_rows) {
                        tracing::error!(
                            "Cloud Connect: query {command_id} returned {} rows against a {max_rows} row limit; refusing the result",
                            result.row_count
                        );
                        send_result(
                            &tx,
                            &command_id,
                            proto::ResultCode::Internal,
                            "The query result exceeded this instance's row limit and was not sent.",
                            None,
                        )
                        .await;
                        return;
                    }
                    tracing::debug!(
                        "Cloud Connect: query {command_id} returned {} rows ({bytes} bytes) in {elapsed_ms}ms",
                        result.row_count
                    );
                    send_ok(
                        &tx,
                        &command_id,
                        Some(proto::command_result::Payload::Binary(result.arrow_ipc)),
                    )
                    .await;
                }
                Err(err) => {
                    tracing::debug!(
                        "Cloud Connect: query {command_id} failed after {elapsed_ms}ms: {}",
                        result_code(&err).as_str_name()
                    );
                    send_command_error(&tx, &command_id, &err).await;
                }
            }
        });
    }

    /// Handle an `ApplySpicepod`, opening any secrets that rode with it.
    ///
    /// A payload that fails to open **fails the whole command**: the spicepod is
    /// not written and the components that referenced those secrets are left on
    /// the previous configuration rather than started without them. Applying the
    /// spicepod and dropping the secrets would report success and then fail
    /// every referencing component with a missing-parameter error naming
    /// nothing.
    ///
    /// A deployment applies by restart, so a successful apply usually ends with
    /// this process exiting: the result is sent and flushed first, then the
    /// runtime handle exits and the supervisor relaunches it on the persisted
    /// spicepod. Sending the result first is what lets a caller see the
    /// validation outcome at all — once the process is gone the stream is too,
    /// so a result that has not been flushed by then is lost.
    ///
    /// `command_id` comes from the `ControlMessage` envelope, not from the
    /// command body — and it is part of the outer AAD, so an envelope cannot be
    /// replayed onto a different dispatch.
    async fn handle_apply_spicepod(
        &mut self,
        tx: &mpsc::Sender<proto::ClientMessage>,
        command_id: &str,
        cmd: proto::ApplySpicepod,
        session_key: Option<&cloud_connect_crypto::EncryptionKeypair>,
    ) {
        // The app id is not reported here. Whether one arrived only matters
        // against what the handle already holds, which only the handle knows, so
        // it is the handle that says what the deployment did to the attribution.
        tracing::debug!(
            command_id,
            yaml_bytes = cmd.spicepod_yaml.len(),
            "Spice Cloud Connect: received a Spicepod deployment"
        );

        let delivered = match cmd.sealed_secret_payload.as_ref() {
            None => None,
            Some(payload) => match self
                .open_delivered_secrets(payload, command_id, session_key)
                .await
            {
                Ok(secrets) => Some(secrets),
                Err(err) => {
                    send_command_error(tx, command_id, &err).await;
                    return;
                }
            },
        };

        let outcome = self
            .runtime
            .apply_spicepod(SpicepodDeployment {
                config_dir: &self.config.config_dir,
                spicepod_yaml: &cmd.spicepod_yaml,
                delivered_secrets: delivered,
                // An empty app id on the wire means the control plane named no
                // app, which is a different thing from an app named "".
                app_id: Some(cmd.app_id.as_str()).filter(|id| !id.is_empty()),
            })
            .await;

        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(err) => {
                tracing::warn!(
                    command_id,
                    "Failed to apply the Spicepod deployed from Spice Cloud: {err}. This instance keeps serving its current configuration; correct the Spicepod and deploy it again. See: https://spiceai.org/docs"
                );
                send_command_error(tx, command_id, &err).await;
                return;
            }
        };
        tracing::debug!(
            command_id,
            "Spice Cloud Connect: applied the deployed Spicepod"
        );

        send_ok_json(tx, command_id, &outcome.document).await;

        if outcome.post_apply == PostApply::ExitToApply {
            tracing::info!(
                "Cloud Connect: the deployed spicepod is persisted; exiting so the supervisor restarts spiced on it"
            );
            flush_outbound(tx).await;
            self.runtime.exit_to_apply().await;
            // Reaching here means the handle did not exit. The spicepod stays
            // persisted and takes effect on the next start, so say what state
            // the instance is actually in rather than letting the control plane
            // infer it from a deployment that never goes live.
            tracing::error!(
                "Cloud Connect: the runtime did not exit to apply the deployment; it is persisted but NOT live, and takes effect the next time spiced starts. Restart it via your process manager. See: https://spiceai.org/docs"
            );
        }
    }

    /// Open a delivered payload against this instance's keys.
    ///
    /// Errors are [`CommandError`]s so the control plane learns whether a retry
    /// could help: a missing session key or an unknown enrolled key is
    /// `InvalidArgument` (this envelope will never open here — re-deploy to
    /// re-seal), while a local key-material fault is `Internal`. No variant
    /// carries key or secret material.
    async fn open_delivered_secrets(
        &mut self,
        payload: &proto::SealedSecretPayload,
        command_id: &str,
        session_key: Option<&cloud_connect_crypto::EncryptionKeypair>,
    ) -> std::result::Result<crate::sealed_secrets::DeliveredSecrets, CommandError> {
        let identity = self.identity.clone().ok_or_else(|| {
            CommandError::internal("no identity is held, so delivered secrets cannot be opened")
        })?;
        let keyring = identity.encryption_keyring().map_err(|err| {
            CommandError::invalid_argument(format!("delivered secrets could not be opened: {err}"))
        })?;

        let opened = crate::sealed_secrets::open_delivered(
            payload,
            &identity.identifier,
            command_id,
            session_key,
            &keyring,
        )
        .map_err(|err| CommandError::invalid_argument(err.to_string()))?;

        // The *current* key opening a payload proves the control plane is
        // sealing to the rotated key, so nothing in flight can still be
        // addressed to the retained one. Retiring it here is what stops a
        // superseded private key lingering on disk indefinitely. A payload that
        // opened with the previous key deliberately does not retire it.
        if crate::sealed_secrets::opened_with_current(&opened.inner_key_id, &keyring) {
            let mut updated = identity;
            if updated.retire_previous_enc_key() {
                self.identity = Some(
                    self.persist_identity_preserving_attachment(
                        updated,
                        "previous encryption-key retirement",
                    )
                    .await,
                );
            }
        }

        // Count only — a name is safe to log, a value never is, and the count is
        // enough to tell "secrets arrived" from "none did".
        tracing::info!(
            "Cloud Connect: opened {} delivered secret(s) for this deployment",
            opened.secrets.len()
        );
        Ok(opened.secrets)
    }

    async fn handle_adopt(
        &mut self,
        tx: &mpsc::Sender<proto::ClientMessage>,
        command_id: &str,
        cmd: proto::Adopt,
        live_identifier: &Arc<RwLock<String>>,
    ) {
        // Post-DR-025, `Adopt` over the stream is a trust/marker message —
        // the portal admin clicked Adopt — not the cert-delivery mechanism.
        // The leaf was issued at the out-of-band enroll, so acknowledge
        // against the identity we already hold.
        let Some(identity) = self.identity.clone() else {
            // Reaching the stream at all requires an identity (mTLS), so
            // this indicates a driver bug or a `Remove` racing the Adopt.
            tracing::error!("Cloud Connect: received Adopt while holding no identity; refusing");
            send_result(
                tx,
                command_id,
                proto::ResultCode::Internal,
                "no identity held: the instance has not completed enrollment",
                None,
            )
            .await;
            return;
        };

        // Presence, not emptiness: an absent `assigned_identifier` means the
        // control plane named no instance, which is not the same as naming one
        // whose identifier happens to be empty.
        if let Some(assigned) = cmd.assigned_identifier.as_deref()
            && assigned != identity.identifier
        {
            // A marker naming a different instance is a control-plane bug;
            // refuse rather than silently impersonate another identifier.
            tracing::error!(
                "Cloud Connect: Adopt names instance {assigned} but this instance enrolled as {}; refusing",
                identity.identifier
            );
            send_result(
                tx,
                command_id,
                proto::ResultCode::InvalidArgument,
                &format!(
                    "adopt marker names instance {assigned}, but this instance is {}",
                    identity.identifier
                ),
                None,
            )
            .await;
            return;
        }

        tracing::info!(
            "Cloud Connect: enrollment confirmed by the control plane for {}",
            identity.identifier
        );
        *live_identifier.write().await = identity.identifier.clone();

        // Echo the AdoptAck so the control plane can confirm the pinned
        // public key matches the one recorded at enroll.
        let ack = proto::AdoptAck {
            identifier: identity.identifier.clone(),
            identity_pubkey_pem: identity.public_key_pem.clone(),
        };
        if let Err(err) = tx
            .send(proto::ClientMessage {
                body: Some(proto::client_message::Body::AdoptAck(ack)),
            })
            .await
        {
            tracing::warn!("Cloud Connect: failed to send AdoptAck: {err}");
        }

        send_ok_json(
            tx,
            command_id,
            &serde_json::json!({
                "status": "adopted",
                "identifier": identity.identifier,
            }),
        )
        .await;
    }

    /// Handle a `Remove` command. Returns `true` only if the on-disk identity
    /// was actually removed (or was already absent) — i.e. the instance is
    /// genuinely released and the caller may exit as such.
    ///
    /// If clearing `identity.json` fails, the file would still be loaded on the
    /// next start and Cloud Connect would silently reconnect, so reporting
    /// success here would lie to the control plane. In that case we keep the
    /// in-memory identity, report the command as failed, and return `false`
    /// so the driver stays connected with the still-valid identity instead of
    /// exiting as removed.
    async fn handle_remove(
        &mut self,
        tx: &mpsc::Sender<proto::ClientMessage>,
        command_id: &str,
        live_identifier: &Arc<RwLock<String>>,
    ) -> bool {
        // Clear identity from disk first. Use the async clear so the remote
        // `Remove` path does not block a Tokio worker on `std::fs` I/O while
        // the Cloud Connect stream is active. `clear_async` treats a missing
        // file as success, so reaching the error branch means the file exists
        // but could not be removed.
        if let Err(err) = IdentityStore::clear_async(&self.config.identity_path).await {
            tracing::warn!(
                "Cloud Connect: failed to clear identity at {}: {err}; \
                 reporting Remove as failed and staying connected (the unchanged \
                 identity would otherwise reconnect on restart)",
                self.config.identity_path.display()
            );
            send_failed(
                tx,
                command_id,
                &format!(
                    "failed to clear identity at {}: {err}",
                    self.config.identity_path.display()
                ),
            )
            .await;
            return false;
        }

        // Disk identity is gone — drop it from memory too and report success.
        self.identity = None;
        live_identifier.write().await.clear();

        send_ok_json(tx, command_id, &serde_json::json!({ "status": "removed" })).await;
        true
    }
}

/// Outcome of the pre-connect credential phase
/// ([`ClientDriver::ensure_credentials`]).
enum CredentialStep {
    /// A usable identity is held; proceed to connect.
    Ready,
    /// A transient failure occurred; back off and re-run the phase.
    Retry,
    /// Non-recoverable without user action; exit the driver.
    Exit,
}

/// Reasons the inner loop can exit.
enum ExitReason {
    Shutdown,
    Disconnected,
    /// The cloud dispatched `Remove`: the local identity was cleared and the
    /// instance released. `spiced` keeps serving; only the cloud-connect task
    /// exits.
    Removed,
    /// The cloud permanently refused to renew the identity (revocation);
    /// the local identity was cleared before exiting the stream.
    IdentityRevoked,
}

/// Sleep for `delay`, or never resolve when `delay` is `None` (an
/// unbounded identity has no renewal deadline).
async fn sleep_or_never(delay: Option<Duration>) {
    match delay {
        Some(d) => time::sleep(d).await,
        None => std::future::pending::<()>().await,
    }
}

/// `true` when the identity should be renewed now: within `lead` of its
/// `not_after` (or already past it). An identity with no expiry never renews.
fn renewal_due(identity: &Identity, lead: Duration) -> bool {
    let Some(not_after) = identity.not_after_unix else {
        return false;
    };
    now_unix().saturating_add(lead.as_secs()) >= not_after
}

/// Trust anchors for verifying the gateway's SERVER certificate on the mTLS
/// channel.
struct ServerTrust<'a> {
    /// Whether to trust the host's native root store. Always `true`: per
    /// DR-025 (#7) the gateway serves a publicly-trusted (Let's Encrypt)
    /// server cert that chains to a public CA.
    native_roots: bool,
    /// Extra anchors trusted *on top of* the native roots — the enrollment CA
    /// bundle and any dev/self-hosted CA — for a gateway that instead serves
    /// an internally-signed cert.
    extra_cas: Vec<&'a str>,
}

/// Decide the trust anchors for verifying the gateway's SERVER certificate.
///
/// The host's native root store is **always** trusted — the gateway serves a
/// publicly-trusted (Let's Encrypt) server cert (DR-025 #7). The enrollment CA
/// bundle (`ca_bundle_pem`) and any dev/self-hosted CA (`dev_ca_pem`) are added
/// as *extra* anchors only, never as the exclusive root.
///
/// NB: the enrollment CA (`ca_bundle_pem`) is CA1 — it signs our own CLIENT
/// identity presented on the same channel — NOT the gateway's server cert,
/// which chains to a public CA (CA2). Pinning the server-cert trust to CA1
/// exclusively is wrong: it rejects the public server cert as `UnknownIssuer`.
fn server_trust<'a>(ca_bundle_pem: &'a str, dev_ca_pem: Option<&'a str>) -> ServerTrust<'a> {
    let mut extra_cas = Vec::new();
    if !ca_bundle_pem.is_empty() {
        extra_cas.push(ca_bundle_pem);
    }
    if let Some(dev_ca) = dev_ca_pem {
        extra_cas.push(dev_ca);
    }
    ServerTrust {
        native_roots: true,
        extra_cas,
    }
}

fn build_channel(
    config: &CloudConnectConfig,
    endpoint_url: &str,
    identity: &Identity,
) -> Result<Channel> {
    let mut endpoint = Endpoint::from_shared(endpoint_url.to_string())
        .map_err(|source| Error::InvalidEndpoint {
            endpoint: endpoint_url.to_string(),
            source,
        })?
        .timeout(Duration::from_mins(1))
        .connect_timeout(Duration::from_secs(10))
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .http2_keep_alive_interval(Duration::from_secs(15))
        .keep_alive_timeout(Duration::from_mins(1))
        .keep_alive_while_idle(true);

    if !config.insecure {
        let mut tls = ClientTlsConfig::new();

        // Trust roots for verifying the gateway's SERVER certificate — see
        // [`server_trust`] for the (subtle) rationale.
        let trust = server_trust(&identity.ca_bundle_pem, config.ca_cert_pem.as_deref());
        if trust.native_roots {
            tls = tls.with_native_roots();
        }
        for ca_pem in trust.extra_cas {
            tls = tls.ca_certificate(Certificate::from_pem(ca_pem.as_bytes()));
        }

        // Mutual TLS: present the cloud-issued leaf and its private key as
        // the client certificate. The gateway verifies the leaf chains to
        // the Cloud Connect CA root — this is the entire authN, which is
        // precisely why the `Hello` carries none. There is no certless
        // path: the post-DR-025 gateway rejects connections without a
        // client certificate.
        tls = tls.identity(tonic::transport::Identity::from_pem(
            identity.identity_cert_pem.as_bytes(),
            identity.private_key_pem.as_bytes(),
        ));
        endpoint = endpoint.tls_config(tls).context(TransportSnafu)?;
    }

    Ok(endpoint.connect_lazy())
}

/// Wait for everything queued on the outbound channel to be handed to tonic,
/// bounded by [`APPLY_FLUSH_BUDGET`].
///
/// Called before the process exits to apply a deployment. Full capacity means
/// the transport took every queued frame — including the `CommandResult` just
/// sent — which is as close to "it is on the wire" as a channel can report. The
/// budget is what keeps a stalled gateway from holding up the deployment, which
/// is already persisted and takes effect on the restart either way.
async fn flush_outbound(tx: &mpsc::Sender<proto::ClientMessage>) {
    let deadline = time::Instant::now() + APPLY_FLUSH_BUDGET;
    while tx.capacity() < tx.max_capacity() {
        if time::Instant::now() >= deadline {
            tracing::warn!(
                "Cloud Connect: the deployment result was still queued after {}; exiting anyway (the control plane reconciles the deployment from the version reported on reconnect)",
                humanize(APPLY_FLUSH_BUDGET)
            );
            return;
        }
        if tx.is_closed() {
            // The stream is gone, so nothing more will be sent from this queue.
            return;
        }
        time::sleep(APPLY_FLUSH_POLL).await;
    }
}

fn build_hello(
    config: &CloudConnectConfig,
    identity: &Identity,
    runtime: &dyn RuntimeHandle,
) -> proto::Hello {
    // The client certificate carries the identity, so the Hello only names
    // the instance and declares what it can do.
    proto::Hello {
        instance_kind: proto::InstanceKind::Standalone as i32,
        identifier: identity.identifier.clone(),
        runtime_version: config.runtime_version.clone(),
        hostname: gethostname::gethostname().to_string_lossy().into_owned(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        fingerprint: fingerprint::compute(),
        public_ip_hint: String::new(),
        operator_version: String::new(),
        runtime_versions: std::collections::HashMap::new(),
        protocol_version: crate::PROTOCOL_VERSION,
        capabilities: advertised_capabilities(runtime),
    }
}

/// Proto message name of a command — the label used in logs and in the
/// message of a NACK.
fn command_name(body: &proto::control_message::Body) -> &'static str {
    use proto::control_message::Body;
    match body {
        Body::Ack(_) => "Ack",
        Body::GetRuntimeInfo(_) => "GetRuntimeInfo",
        Body::Restart(_) => "Restart",
        Body::ApplySpicepod(_) => "ApplySpicepod",
        Body::AttachApp(_) => "AttachApp",
        Body::UpgradeRuntime(_) => "UpgradeRuntime",
        Body::Adopt(_) => "Adopt",
        Body::Remove(_) => "Remove",
        Body::ApplyManifest(_) => "ApplyManifest",
        Body::DeleteManifest(_) => "DeleteManifest",
        Body::GetStatus(_) => "GetStatus",
        Body::Drain(_) => "Drain",
        Body::Pause(_) => "Pause",
        Body::ApplySecrets(_) => "ApplySecrets",
        Body::DeleteSecrets(_) => "DeleteSecrets",
        Body::GetLogs(_) => "GetLogs",
        Body::ExecuteQuery(_) => "ExecuteQuery",
    }
}

/// What the control plane is told when a result will not fit on the control
/// stream. Names the limit and the way out, and repeats neither the query nor
/// any value from it.
fn result_too_large_message() -> String {
    format!(
        "The query result exceeds the {} MiB Cloud Connect limit and was not sent. Return fewer rows or columns (a smaller LIMIT, a narrower projection, or an aggregate) and run it again.",
        MAX_QUERY_RESULT_BYTES / (1024 * 1024)
    )
}

/// Map the wire restart mode onto the handler-level one. A value a newer
/// control plane knows and this build doesn't degrades to `Unspecified`,
/// which implementations treat as graceful.
fn restart_mode(mode: i32) -> RestartMode {
    match proto::RestartMode::try_from(mode) {
        Ok(proto::RestartMode::Graceful) => RestartMode::Graceful,
        Ok(proto::RestartMode::Immediate) => RestartMode::Immediate,
        Ok(proto::RestartMode::DrainThenRestart) => RestartMode::DrainThenRestart,
        Ok(proto::RestartMode::Unspecified) | Err(_) => RestartMode::Unspecified,
    }
}

/// Wire code for a handler failure. Exhaustive, so a new [`CommandError`]
/// variant cannot silently collapse into an existing code.
fn result_code(err: &CommandError) -> proto::ResultCode {
    match err {
        CommandError::Unsupported { .. } => proto::ResultCode::Unsupported,
        CommandError::InvalidArgument { .. } => proto::ResultCode::InvalidArgument,
        CommandError::Failed { .. } => proto::ResultCode::Failed,
        CommandError::Internal { .. } => proto::ResultCode::Internal,
        CommandError::Busy { .. } => proto::ResultCode::Busy,
        CommandError::ResultTooLarge { .. } => proto::ResultCode::ResultTooLarge,
    }
}

/// Encode a JSON document as the `json` payload arm. A `Null` document means
/// the command produced no payload at all, which is the absent arm.
///
/// An encoding failure is an error, never an absent payload: a result the
/// control plane reads as OK-with-no-payload is indistinguishable from a
/// command that legitimately returned nothing, so dropping the document would
/// silently lose the answer.
fn json_payload(
    value: &serde_json::Value,
) -> Result<Option<proto::command_result::Payload>, CommandError> {
    if value.is_null() {
        return Ok(None);
    }
    match serde_json::to_string(value) {
        Ok(json) => Ok(Some(proto::command_result::Payload::Json(json))),
        Err(err) => Err(CommandError::internal(format!(
            "the command succeeded but its result could not be encoded as JSON: {err}"
        ))),
    }
}

/// Send a `CommandResult`. The payload arm the caller picks is what declares
/// the encoding, so there is no raw-text path to reach for by mistake.
async fn send_result(
    tx: &mpsc::Sender<proto::ClientMessage>,
    command_id: &str,
    code: proto::ResultCode,
    message: &str,
    payload: Option<proto::command_result::Payload>,
) {
    let msg = proto::ClientMessage {
        body: Some(proto::client_message::Body::Result(proto::CommandResult {
            command_id: command_id.to_string(),
            code: code as i32,
            message: message.to_string(),
            payload,
        })),
    };
    if let Err(err) = tx.send(msg).await {
        tracing::warn!("Cloud Connect: failed to send CommandResult for {command_id}: {err}");
    }
}

async fn send_ok(
    tx: &mpsc::Sender<proto::ClientMessage>,
    command_id: &str,
    payload: Option<proto::command_result::Payload>,
) {
    send_result(tx, command_id, proto::ResultCode::Ok, "", payload).await;
}

/// Answer OK carrying `payload` as the JSON arm — or INTERNAL if the document
/// cannot be encoded, so the control plane never reads an unsendable result as
/// a success that returned nothing.
async fn send_ok_json(
    tx: &mpsc::Sender<proto::ClientMessage>,
    command_id: &str,
    payload: &serde_json::Value,
) {
    match json_payload(payload) {
        Ok(arm) => send_ok(tx, command_id, arm).await,
        Err(err) => {
            tracing::error!("Cloud Connect: {command_id}: {err}");
            send_command_error(tx, command_id, &err).await;
        }
    }
}

async fn send_command_error(
    tx: &mpsc::Sender<proto::ClientMessage>,
    command_id: &str,
    err: &CommandError,
) {
    send_result(tx, command_id, result_code(err), &err.to_string(), None).await;
}

async fn send_failed(tx: &mpsc::Sender<proto::ClientMessage>, command_id: &str, message: &str) {
    send_result(tx, command_id, proto::ResultCode::Failed, message, None).await;
}

async fn send_unsupported(
    tx: &mpsc::Sender<proto::ClientMessage>,
    command_id: &str,
    message: &str,
) {
    tracing::debug!("Cloud Connect: answering unsupported for command_id={command_id}: {message}");
    send_result(
        tx,
        command_id,
        proto::ResultCode::Unsupported,
        message,
        None,
    )
    .await;
}

/// Forward a handler's JSON result as a `CommandResult`.
async fn reply_with_json(
    tx: &mpsc::Sender<proto::ClientMessage>,
    command_id: &str,
    result: Result<serde_json::Value, CommandError>,
) {
    match result {
        Ok(payload) => send_ok_json(tx, command_id, &payload).await,
        Err(err) => send_command_error(tx, command_id, &err).await,
    }
}

fn humanize(d: Duration) -> String {
    if d >= Duration::from_secs(1) {
        format!("{:.1}s", d.as_secs_f64())
    } else {
        format!("{}ms", d.as_millis())
    }
}

/// Next reconnect backoff after a failure: doubles, capped at `MAX_BACKOFF`.
fn next_backoff(prev: Duration) -> Duration {
    (prev * 2).min(MAX_BACKOFF)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enroll::past_renewal_grace;

    #[test]
    fn backoff_doubles_until_cap() {
        let mut d = MIN_BACKOFF;
        let mut seen = Vec::new();
        for _ in 0..10 {
            seen.push(d);
            d = next_backoff(d);
        }
        assert_eq!(seen[0], MIN_BACKOFF);
        let mut last = Duration::ZERO;
        let mut hit_cap = false;
        for d in seen {
            assert!(d >= last);
            last = d;
            if d == MAX_BACKOFF {
                hit_cap = true;
            }
        }
        assert!(hit_cap, "backoff should hit the {MAX_BACKOFF:?} cap");
    }

    #[test]
    fn backoff_caps_at_max() {
        let d = next_backoff(Duration::from_mins(2));
        assert_eq!(d, MAX_BACKOFF);
    }

    fn identity_with_not_after(not_after_unix: Option<u64>) -> Identity {
        Identity {
            identifier: "inst_test".to_string(),
            identity_cert_pem: String::new(),
            private_key_pem: String::new(),
            public_key_pem: String::new(),
            ca_bundle_pem: String::new(),
            gateway_addr: "gateway.test:443".to_string(),
            not_after_unix,
            enc_private_key_pem: String::new(),
            enc_public_key_pem: String::new(),
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
            app_id: None,
        }
    }

    #[test]
    fn renewal_never_due_for_unbounded_identity() {
        let id = identity_with_not_after(None);
        assert!(!renewal_due(&id, Duration::from_hours(12)));
        assert!(!past_renewal_grace(&id));
    }

    #[test]
    fn renewal_due_within_lead_of_expiry() {
        let lead = Duration::from_hours(12);
        // Expires in 1h with a 12h lead: due now.
        let soon = identity_with_not_after(Some(now_unix() + 3600));
        assert!(renewal_due(&soon, lead));
        // Expires in 24h with a 12h lead: not yet due.
        let later = identity_with_not_after(Some(now_unix() + 24 * 60 * 60));
        assert!(!renewal_due(&later, lead));
        // Already expired: due (renewable within the grace window).
        let expired = identity_with_not_after(Some(now_unix().saturating_sub(60)));
        assert!(renewal_due(&expired, lead));
        assert!(!past_renewal_grace(&expired));
    }

    #[test]
    fn identity_past_grace_cannot_renew() {
        let long_dead = identity_with_not_after(Some(
            now_unix().saturating_sub(crate::enroll::RENEWAL_GRACE.as_secs() + 60),
        ));
        assert!(past_renewal_grace(&long_dead));
    }

    #[test]
    fn server_trust_always_trusts_native_roots_even_with_pinned_bundle() {
        // Regression guard: the gateway serves a public (Let's Encrypt) server
        // cert that chains to a public CA — NOT to the enrollment
        // `ca_bundle_pem`, which signs our own CLIENT identity. Pinning the
        // server-cert trust to `ca_bundle_pem` exclusively (dropping the native
        // roots) rejected the public cert as `UnknownIssuer`. Native roots must
        // ALWAYS be trusted, even when an enrollment bundle is present.
        let trust = server_trust("CA-BUNDLE-PEM", None);
        assert!(
            trust.native_roots,
            "native roots must be trusted even alongside a pinned CA bundle"
        );
        assert_eq!(trust.extra_cas, vec!["CA-BUNDLE-PEM"]);
    }

    #[test]
    fn server_trust_adds_bundle_and_dev_ca_as_extra_anchors() {
        // Both the enrollment bundle and a dev/self-hosted CA are added on top
        // of (not instead of) the native roots.
        let trust = server_trust("CA-BUNDLE-PEM", Some("DEV-CA-PEM"));
        assert!(trust.native_roots);
        assert_eq!(trust.extra_cas, vec!["CA-BUNDLE-PEM", "DEV-CA-PEM"]);
    }

    #[test]
    fn json_payload_absent_only_for_a_null_document() {
        // Null means "this command produced no payload", which is the absent
        // arm. Anything else must produce a payload — an absent arm alongside
        // an OK code reads to the control plane as a command that returned
        // nothing, so it must never be how an encoding failure surfaces.
        assert!(
            json_payload(&serde_json::Value::Null)
                .expect("null encodes")
                .is_none()
        );

        let arm = json_payload(&serde_json::json!({ "status": "adopted" }))
            .expect("object encodes")
            .expect("a non-null document must produce a payload arm");
        match arm {
            proto::command_result::Payload::Json(json) => {
                assert_eq!(json, r#"{"status":"adopted"}"#);
            }
            other => panic!("a JSON document must take the json arm, got {other:?}"),
        }
    }

    #[test]
    fn server_trust_native_roots_only_when_no_pins() {
        // No enrollment bundle and no dev CA: native roots alone.
        let trust = server_trust("", None);
        assert!(trust.native_roots);
        assert!(trust.extra_cas.is_empty());
    }
}
