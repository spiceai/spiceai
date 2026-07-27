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

//! Outbound client: out-of-band cloud enrollment + mTLS gateway stream.
//!
//! Identity is obtained **before** any gRPC stream (see [`crate::enroll`]):
//! the adoption code + CSR go to the cloud enroll endpoint over plain
//! HTTPS, and the issued leaf + CA bundle + gateway address are persisted
//! to `identity.json`. The driver then connects to the stateless gateway
//! over **mTLS** (the leaf is the credential — `Hello.credential` is
//! always empty) and enters a long-running loop that processes
//! `ControlMessage`s from the server and emits `ClientMessage`s back
//! (heartbeats, command results, telemetry).
//!
//! Disconnects are tolerated: the driver reconnects with exponential
//! backoff (1s → 60s with jitter), always presenting the identity leaf.
//!
//! The identity is renewed on a ~12h cadence (see
//! [`crate::config::DEFAULT_RENEWAL_LEAD`]) against the cloud `/renew`
//! endpoint, both from the live stream loop and before reconnect attempts;
//! every renewal rotates the keypair. An expired leaf can still renew
//! within the 30-day grace window ([`crate::enroll::RENEWAL_GRACE`]);
//! past it a fresh adoption code is required.
//!
//! `Adopt` over the stream is a trust/marker message (the portal admin
//! clicked Adopt) — the cert was already issued at enroll, so the client
//! just acknowledges against the identity it holds.
//!
//! If a Forget arrives, we clear the local identity from disk and, on
//! success, exit the cloud-connect task — spiced itself stays up and keeps
//! serving local spicepod traffic as before. This matches the adoption
//! semantics where "Forget" releases management but doesn't destroy the
//! device. To re-adopt, the user runs `spice connect <code>` and restarts
//! spiced. If the on-disk identity cannot be cleared, the Forget is
//! reported as failed and the driver stays connected with the still-valid
//! identity rather than falsely exiting as forgotten.

use std::sync::Arc;
use std::time::Duration;

use crate::TransportSnafu;
use snafu::ResultExt;
use tokio::sync::{RwLock, mpsc};
use tokio::time;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use crate::config::CloudConnectConfig;
use crate::enroll::{EnrollClient, InstanceFacts, RENEWAL_GRACE};
use crate::handlers::RuntimeHandle;
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

/// State held by the driver across reconnects.
pub(crate) struct ClientDriver {
    config: CloudConnectConfig,
    runtime: Arc<dyn RuntimeHandle>,
    shutdown: Arc<Shutdown>,
    /// Currently-effective identity, if any. Set by out-of-band enrollment,
    /// replaced on renewal (rotated keypair); cleared on Forget or when
    /// the cloud permanently refuses renewal (revocation).
    identity: Option<Identity>,
    /// Earliest instant the next in-stream renewal attempt may run — set
    /// after every renewal attempt (failed or successful) so attempts are
    /// paced by [`RENEW_RETRY_INTERVAL`] rather than spinning on a
    /// due-in-the-past `not_after`. Cleared on enrollment.
    renew_not_before: Option<time::Instant>,
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
        }
    }

    /// Run the driver until shutdown is requested.
    ///
    /// The driver is fully fault-tolerant: a transport, decode, or
    /// stream error triggers a reconnect with backoff; only an explicit
    /// shutdown notify, a Forget, or a non-recoverable credential state
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

            // Ensure a usable identity: enroll out-of-band when we only
            // hold an adoption code, renew when the current leaf is due.
            match self.ensure_credentials(&enroll_client, &mut backoff).await {
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
                    "Cloud Connect: the stored identity has no gateway address (it predates the enroll-first flow); exiting cloud-connect. Run `spice connect <code>` with a fresh adoption code and restart spiced to re-enroll."
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
                Ok(ExitReason::Forget) => {
                    tracing::info!(
                        "Cloud Connect: Forget acknowledged; cloud-connect task exiting. spiced remains running and serving local spicepod traffic. To re-adopt, run `spice connect <code>` and restart spiced."
                    );
                    return Ok(());
                }
                Ok(ExitReason::IdentityRevoked) => {
                    // The in-stream renewal was permanently refused and the
                    // identity was cleared; loop back — with an adoption code
                    // still staged we re-enroll, otherwise the no-credentials
                    // branch above exits with the re-adopt guidance.
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

    /// Pre-connect credential phase: drop an unrenewable identity, enroll
    /// out-of-band when only an adoption code is held, and renew a due
    /// identity. Returns what the connect loop should do next.
    async fn ensure_credentials(
        &mut self,
        enroll_client: &EnrollClient,
        backoff: &mut Duration,
    ) -> CredentialStep {
        // An identity past the renewal grace window can no longer be
        // renewed by the cloud — only a fresh adoption code helps.
        if let Some(ref id) = self.identity
            && past_renewal_grace(id)
        {
            tracing::warn!(
                "Cloud Connect: stored identity expired past the renewal grace window; falling back to pending-adoption state"
            );
            self.identity = None;
        }

        // Out-of-band enrollment: no identity yet, so present the
        // adoption code + CSR to the cloud enroll endpoint.
        if self.identity.is_none() {
            let Some(code) = self.config.adoption_code.clone() else {
                tracing::error!(
                    "Cloud Connect: cannot connect (no identity and no adoption code); exiting cloud-connect. Run `spice connect <code>` and restart spiced to re-adopt."
                );
                return CredentialStep::Exit;
            };
            match self.enroll_once(enroll_client, &code).await {
                Ok(()) => {
                    *backoff = MIN_BACKOFF;
                }
                Err(err) if err.is_authoritative_rejection() => {
                    // The cloud authoritatively rejected the code — it is dead
                    // (invalid, already consumed, or expired): discard the
                    // staged file so a restart does not re-send it, and exit
                    // with an actionable message.
                    self.discard_pending_code().await;
                    tracing::error!(
                        "Cloud Connect: enrollment with {} was rejected: {err}; exiting cloud-connect. Mint a new adoption code in the Spice Cloud portal, run `spice connect <code>`, and restart spiced. See: https://spiceai.org/docs",
                        self.config.enroll_endpoint
                    );
                    return CredentialStep::Exit;
                }
                Err(err) => {
                    // Transient (transport / 5xx) OR a local failure that never
                    // reached the cloud (e.g. key-material generation). Either
                    // way the code was NOT consumed, so keep the staged code
                    // and retry rather than burning it.
                    tracing::warn!(
                        "Cloud Connect: enrollment attempt against {} failed (will retry): {err}",
                        self.config.enroll_endpoint
                    );
                    return CredentialStep::Retry;
                }
            }
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
                Err(err) if err.is_authoritative_rejection() => {
                    // Renewal authoritatively refused by the cloud: the
                    // instance was forgotten/revoked cloud-side (refusing
                    // renewal IS the revocation, DR-025) or the pinned key no
                    // longer matches. The identity is dead; the next pass
                    // enrolls with a staged code or exits with re-adopt
                    // guidance.
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

    /// Perform the out-of-band cloud enrollment: generate a fresh keypair +
    /// CSR, present the adoption code + host facts, persist the issued
    /// identity, and consume the staged code.
    async fn enroll_once(
        &mut self,
        client: &EnrollClient,
        code: &str,
    ) -> Result<(), enroll::Error> {
        let material = IdentityStore::generate_enrollment().map_err(|source| {
            enroll::Error::ProofOfPossession {
                reason: format!("failed to generate enrollment key material: {source}"),
            }
        })?;
        let facts = InstanceFacts::gather(&self.config.runtime_version);
        let outcome = client.enroll(code, &material, &facts).await?;

        let identity = Identity {
            identifier: outcome.instance_id,
            identity_cert_pem: outcome.identity_cert_pem,
            private_key_pem: material.private_key_pem,
            public_key_pem: material.public_key_pem,
            ca_bundle_pem: outcome.ca_bundle_pem,
            gateway_addr: outcome.gateway_addr,
            not_after_unix: outcome.not_after_unix,
        };
        self.persist_identity(&identity).await;
        tracing::info!(
            "Cloud Connect: enrolled as {} (gateway {}); identity stored at {}",
            identity.identifier,
            identity.gateway_addr,
            self.config.identity_path.display()
        );
        self.identity = Some(identity);
        // A stale pacing mark from a previous identity's failed renewals
        // must not delay the fresh identity's first renewal.
        self.renew_not_before = None;

        // The code was atomically consumed by the cloud — it can never be
        // redeemed again, so drop the staged copy and the in-memory value.
        self.discard_pending_code().await;
        self.config.adoption_code = None;
        Ok(())
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

        let rotated = Identity {
            identifier: current.identifier,
            identity_cert_pem: outcome.identity_cert_pem,
            private_key_pem: material.private_key_pem,
            public_key_pem: material.public_key_pem,
            // The CA bundle and gateway address are not re-sent on renewal.
            ca_bundle_pem: current.ca_bundle_pem,
            gateway_addr: current.gateway_addr,
            not_after_unix: outcome.not_after_unix,
        };
        // The cloud has already pinned the new public key: even if
        // persistence fails, the rotated identity must be used in memory
        // (the old key can no longer renew). `persist_identity` logs the
        // failure; the next successful renewal re-attempts the write.
        self.persist_identity(&rotated).await;
        tracing::info!(
            "Cloud Connect: identity renewed for {} (keypair rotated, valid until unix={})",
            rotated.identifier,
            rotated.not_after_unix
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

    /// Persist an identity to disk on the blocking pool, logging (not
    /// failing on) errors: the in-memory identity stays authoritative, and
    /// a persistence failure must not wedge an otherwise-working
    /// connection — it only costs durability across a restart.
    async fn persist_identity(&self, identity: &Identity) {
        let path = self.config.identity_path.clone();
        let to_store = identity.clone();
        let result =
            tokio::task::spawn_blocking(move || IdentityStore::store(&path, &to_store)).await;
        let error = match result {
            Ok(Ok(())) => return,
            Ok(Err(err)) => err.to_string(),
            Err(join) => format!("identity persistence task panicked: {join}"),
        };
        tracing::error!(
            "Cloud Connect: failed to persist identity at {}: {error}; continuing with the in-memory identity (it will be lost on restart)",
            self.config.identity_path.display()
        );
    }

    /// Remove the staged pending-adoption-code file, if configured. A
    /// missing file is success.
    async fn discard_pending_code(&self) {
        if let Some(ref path) = self.config.pending_adopt_code_path {
            match tokio::fs::remove_file(path).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    tracing::warn!(
                        "Cloud Connect: failed to remove pending adoption code at {}: {err}",
                        path.display()
                    );
                }
            }
        }
    }

    /// Clear the identity from disk and memory (best-effort on the disk
    /// side — unlike Forget, the cloud has already invalidated it, so a
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
    /// never expires (`not_after_unix == 0`) and renewal is moot.
    fn next_renewal_delay(&self) -> Option<Duration> {
        let identity = self.identity.as_ref()?;
        if identity.not_after_unix == 0 {
            return None;
        }
        let due_at = identity
            .not_after_unix
            .saturating_sub(self.config.renewal_lead.as_secs());
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
        let hello = build_hello(&self.config, &identity);
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

        // Spawn periodic heartbeat + telemetry tasks. They emit through
        // the same outbound channel. The identifier is shared by RwLock
        // so a Forget can blank it for frames still in flight on the
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

        let tel_tx = tx.clone();
        let tel_runtime = Arc::clone(&runtime);
        let tel_identifier = Arc::clone(&identifier);
        let tel_handle = tokio::spawn(async move {
            let mut ticker = time::interval(tel_interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            let mut last_window = now_unix();
            loop {
                ticker.tick().await;
                let now = now_unix();
                let id = tel_identifier.read().await.clone();
                let t = build_telemetry(&id, last_window, now, &tel_runtime).await;
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
                                .dispatch(&tx, msg, &identifier)
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
        drop(tx);

        Ok(exit_reason)
    }

    async fn dispatch(
        &mut self,
        tx: &mpsc::Sender<proto::ClientMessage>,
        msg: proto::ControlMessage,
        live_identifier: &Arc<RwLock<String>>,
    ) -> Option<ExitReason> {
        let Some(body) = msg.body else {
            tracing::debug!("Cloud Connect: received empty ControlMessage");
            return None;
        };

        match body {
            proto::control_message::Body::Ack(ack) => {
                tracing::debug!("Cloud Connect: ack for command_id={}", ack.for_command_id);
            }
            proto::control_message::Body::GetRuntimeInfo(cmd) => {
                let info = self.runtime.runtime_info_json().await;
                send_result(tx, &cmd.command_id, true, "", info).await;
            }
            proto::control_message::Body::RunQuery(cmd) => {
                let sql_hash = sql_hash(&cmd.sql);
                tracing::info!(
                    target: "cloud_connect_audit",
                    command_id = %cmd.command_id,
                    max_rows = cmd.max_rows,
                    sql_len = cmd.sql.len(),
                    sql_hash = %sql_hash,
                    "RunQuery command received from cloud control plane"
                );
                let identifier = live_identifier.read().await.clone();
                let started = std::time::Instant::now();
                // Race the query against shutdown so a slow cloud-originated
                // query can't hold the driver task until the shutdown timeout
                // — abandon it and exit promptly if shutdown fires mid-query.
                let exec_outcome = tokio::select! {
                    r = self.runtime.execute_sql(&cmd.sql, cmd.max_rows) => r,
                    () = self.shutdown.wait() => {
                        tracing::info!(
                            command_id = %cmd.command_id,
                            "Cloud Connect: shutdown during RunQuery; abandoning command"
                        );
                        return Some(ExitReason::Shutdown);
                    }
                };
                match exec_outcome {
                    Ok(result) => {
                        let duration_ms =
                            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                        emit_run_query_audit(
                            tx,
                            &identifier,
                            &RunQueryAudit {
                                command_id: &cmd.command_id,
                                sql_hash: &sql_hash,
                                row_count: result.row_count,
                                truncated: result.truncated,
                                duration_ms,
                                success: true,
                            },
                        )
                        .await;
                        // Tabular data rides as native Arrow IPC; payload_json
                        // carries only the row-count / truncation metadata.
                        let meta = serde_json::json!({
                            "row_count": result.row_count,
                            "truncated": result.truncated,
                        });
                        send_query_result(tx, &cmd.command_id, result.arrow_ipc, meta).await;
                    }
                    Err(err) => {
                        let duration_ms =
                            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                        emit_run_query_audit(
                            tx,
                            &identifier,
                            &RunQueryAudit {
                                command_id: &cmd.command_id,
                                sql_hash: &sql_hash,
                                row_count: 0,
                                truncated: false,
                                duration_ms,
                                success: false,
                            },
                        )
                        .await;
                        // Safe error: never includes the SQL text.
                        send_result(
                            tx,
                            &cmd.command_id,
                            false,
                            &sanitize_error(&err, &cmd.sql),
                            serde_json::Value::Null,
                        )
                        .await;
                    }
                }
            }
            proto::control_message::Body::Restart(cmd) => {
                let r = self.runtime.restart(cmd.graceful).await;
                reply_with(tx, &cmd.command_id, r).await;
            }
            proto::control_message::Body::ApplySpicepod(cmd) => {
                let r = self
                    .runtime
                    .apply_spicepod(&self.config.config_dir, &cmd.spicepod_yaml)
                    .await;
                reply_with(tx, &cmd.command_id, r).await;
            }
            proto::control_message::Body::UpgradeRuntime(cmd) => {
                match self.runtime.upgrade_runtime(&cmd.target_version).await {
                    Ok(payload) => {
                        // The default impl returns "unsupported"; treat
                        // that as a soft failure so cloud sees the
                        // intent clearly.
                        let success = payload.get("status").and_then(serde_json::Value::as_str)
                            != Some("unsupported");
                        send_result(tx, &cmd.command_id, success, "", payload).await;
                    }
                    Err(err) => {
                        send_result(tx, &cmd.command_id, false, &err, serde_json::Value::Null)
                            .await;
                    }
                }
            }
            proto::control_message::Body::Adopt(cmd) => {
                self.handle_adopt(tx, cmd, live_identifier).await;
            }
            proto::control_message::Body::Forget(cmd) => {
                // Only exit as forgotten if the identity was actually cleared;
                // on a clear failure stay connected with the still-valid
                // identity rather than falsely exiting as forgotten.
                if self.handle_forget(tx, cmd, live_identifier).await {
                    return Some(ExitReason::Forget);
                }
            }
            // Operator-only commands: acknowledge with an error.
            proto::control_message::Body::ApplyManifest(cmd) => {
                send_unsupported(tx, &cmd.command_id, "ApplyManifest").await;
            }
            proto::control_message::Body::DeleteManifest(cmd) => {
                send_unsupported(tx, &cmd.command_id, "DeleteManifest").await;
            }
            proto::control_message::Body::GetStatus(cmd) => {
                // Standalone status probe: the namespace/kind/name targeting
                // fields are empty for standalone instances (they address a
                // workload in a cluster), so they're ignored here — the whole
                // runtime's readiness is reported. The status document is a
                // JSON object, so it's JSON-encoded into payload_json.
                let r = self.runtime.get_status().await;
                reply_with(tx, &cmd.command_id, r).await;
            }
            proto::control_message::Body::Drain(cmd) => {
                send_unsupported(tx, &cmd.command_id, "Drain").await;
            }
            proto::control_message::Body::Pause(cmd) => {
                send_unsupported(tx, &cmd.command_id, "Pause").await;
            }
            proto::control_message::Body::GetPodLogs(cmd) => {
                match self.runtime.get_pod_logs(cmd.tail_lines).await {
                    // The log text rides verbatim in payload_json (a raw
                    // string, not JSON-encoded) per the gateway contract.
                    Ok(logs) => send_result_text(tx, &cmd.command_id, logs).await,
                    Err(err) => {
                        send_result(tx, &cmd.command_id, false, &err, serde_json::Value::Null)
                            .await;
                    }
                }
            }
        }

        None
    }

    async fn handle_adopt(
        &mut self,
        tx: &mpsc::Sender<proto::ClientMessage>,
        cmd: proto::Adopt,
        live_identifier: &Arc<RwLock<String>>,
    ) {
        // Post-DR-025, `Adopt` over the stream is a trust/marker message —
        // the portal admin clicked Adopt — not the cert-delivery mechanism.
        // The leaf was issued at the out-of-band enroll, so acknowledge
        // against the identity we already hold and ignore any legacy
        // cert-delivery fields on the command.
        let Some(identity) = self.identity.clone() else {
            // Reaching the stream at all requires an identity (mTLS), so
            // this indicates a driver bug or a Forget racing the Adopt.
            tracing::error!("Cloud Connect: received Adopt while holding no identity; refusing");
            send_result(
                tx,
                &cmd.command_id,
                false,
                "no identity held: the instance has not completed enrollment",
                serde_json::Value::Null,
            )
            .await;
            return;
        };

        if !cmd.assigned_identifier.is_empty() && cmd.assigned_identifier != identity.identifier {
            // A marker naming a different instance is a control-plane bug;
            // refuse rather than silently impersonate another identifier.
            tracing::error!(
                "Cloud Connect: Adopt names instance {} but this instance enrolled as {}; refusing",
                cmd.assigned_identifier,
                identity.identifier
            );
            send_result(
                tx,
                &cmd.command_id,
                false,
                &format!(
                    "adopt marker names instance {}, but this instance is {}",
                    cmd.assigned_identifier, identity.identifier
                ),
                serde_json::Value::Null,
            )
            .await;
            return;
        }

        tracing::info!(
            "Cloud Connect: adoption confirmed by the control plane for {}",
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

        send_result(
            tx,
            &cmd.command_id,
            true,
            "",
            serde_json::json!({
                "status": "adopted",
                "identifier": identity.identifier,
            }),
        )
        .await;
    }

    /// Handle a `Forget` command. Returns `true` only if the on-disk identity
    /// was actually removed (or was already absent) — i.e. the instance is
    /// genuinely forgotten and the caller may exit as such.
    ///
    /// If clearing `identity.json` fails, the file would still be loaded on the
    /// next start and Cloud Connect would silently reconnect, so reporting
    /// success here would lie to the control plane. In that case we keep the
    /// in-memory identity, report the command as failed, and return `false`
    /// so the driver stays connected with the still-valid identity instead of
    /// exiting as forgotten.
    async fn handle_forget(
        &mut self,
        tx: &mpsc::Sender<proto::ClientMessage>,
        cmd: proto::Forget,
        live_identifier: &Arc<RwLock<String>>,
    ) -> bool {
        // Clear identity from disk first. Use the async clear so the remote
        // `Forget` path does not block a Tokio worker on `std::fs` I/O while
        // the Cloud Connect stream is active. `clear_async` treats a missing
        // file as success, so reaching the error branch means the file exists
        // but could not be removed.
        if let Err(err) = IdentityStore::clear_async(&self.config.identity_path).await {
            tracing::warn!(
                "Cloud Connect: failed to clear identity at {}: {err}; \
                 reporting Forget as failed and staying connected (the unchanged \
                 identity would otherwise reconnect on restart)",
                self.config.identity_path.display()
            );
            send_result(
                tx,
                &cmd.command_id,
                false,
                &format!(
                    "failed to clear identity at {}: {err}",
                    self.config.identity_path.display()
                ),
                serde_json::Value::Null,
            )
            .await;
            return false;
        }

        // Disk identity is gone — drop it from memory too and report success.
        self.identity = None;
        live_identifier.write().await.clear();

        send_result(
            tx,
            &cmd.command_id,
            true,
            "",
            serde_json::json!({ "status": "forgotten" }),
        )
        .await;
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
    Forget,
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
/// `not_after` (or already past it). An unbounded identity
/// (`not_after_unix == 0`) never renews.
fn renewal_due(identity: &Identity, lead: Duration) -> bool {
    if identity.not_after_unix == 0 {
        return false;
    }
    now_unix().saturating_add(lead.as_secs()) >= identity.not_after_unix
}

/// `true` when the identity expired longer than [`RENEWAL_GRACE`] ago —
/// the cloud refuses to renew it, so only a fresh adoption code helps.
fn past_renewal_grace(identity: &Identity) -> bool {
    identity.not_after_unix != 0
        && now_unix()
            >= identity
                .not_after_unix
                .saturating_add(RENEWAL_GRACE.as_secs())
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
        // precisely why `Hello.credential` is empty. There is no certless
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

fn build_hello(config: &CloudConnectConfig, identity: &Identity) -> proto::Hello {
    // The client certificate carries the identity, so the Hello only names
    // the instance: no credential, no CSR, no public key (all of those
    // moved to the out-of-band enroll).
    proto::Hello {
        kind: proto::InstanceKind::Standalone as i32,
        identifier: identity.identifier.clone(),
        credential: String::new(),
        runtime_version: config.runtime_version.clone(),
        extra_versions: std::collections::HashMap::new(),
        hostname: gethostname::gethostname().to_string_lossy().into_owned(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        fingerprint: fingerprint::compute(),
        public_ip_hint: String::new(),
        operator_version: String::new(),
        runtime_versions: std::collections::HashMap::new(),
        agent_pubkey_pem: String::new(),
        csr_pem: String::new(),
    }
}

async fn send_result(
    tx: &mpsc::Sender<proto::ClientMessage>,
    command_id: &str,
    success: bool,
    error: &str,
    payload_json: serde_json::Value,
) {
    let payload_json_str = if payload_json.is_null() {
        String::new()
    } else {
        serde_json::to_string(&payload_json).unwrap_or_default()
    };
    let msg = proto::ClientMessage {
        body: Some(proto::client_message::Body::Result(proto::CommandResult {
            command_id: command_id.to_string(),
            success,
            error: error.to_string(),
            payload_json: payload_json_str,
            result_arrow_ipc: Vec::new(),
        })),
    };
    if let Err(err) = tx.send(msg).await {
        tracing::warn!("Cloud Connect: failed to send CommandResult: {err}");
    }
}

/// Send a successful `CommandResult` whose `payload_json` is raw text rather
/// than a JSON value. `GetPodLogs` uses this: the log blob is returned
/// verbatim (the gateway relays `payload_json` straight through as text), so
/// it must NOT be JSON-encoded/quoted the way [`send_result`] would.
async fn send_result_text(tx: &mpsc::Sender<proto::ClientMessage>, command_id: &str, text: String) {
    let msg = proto::ClientMessage {
        body: Some(proto::client_message::Body::Result(proto::CommandResult {
            command_id: command_id.to_string(),
            success: true,
            error: String::new(),
            payload_json: text,
            result_arrow_ipc: Vec::new(),
        })),
    };
    if let Err(err) = tx.send(msg).await {
        tracing::warn!("Cloud Connect: failed to send GetPodLogs CommandResult: {err}");
    }
}

/// Send a successful tabular `CommandResult` whose data is a native Arrow IPC
/// stream (`arrow_ipc`) with row-count / truncation metadata in `meta`.
async fn send_query_result(
    tx: &mpsc::Sender<proto::ClientMessage>,
    command_id: &str,
    arrow_ipc: Vec<u8>,
    meta: serde_json::Value,
) {
    let msg = proto::ClientMessage {
        body: Some(proto::client_message::Body::Result(proto::CommandResult {
            command_id: command_id.to_string(),
            success: true,
            error: String::new(),
            payload_json: serde_json::to_string(&meta).unwrap_or_default(),
            result_arrow_ipc: arrow_ipc,
        })),
    };
    if let Err(err) = tx.send(msg).await {
        tracing::warn!("Cloud Connect: failed to send query CommandResult: {err}");
    }
}

/// Forward a `Result<Value, String>` from a runtime call as a `CommandResult`.
async fn reply_with(
    tx: &mpsc::Sender<proto::ClientMessage>,
    command_id: &str,
    result: Result<serde_json::Value, String>,
) {
    match result {
        Ok(payload) => send_result(tx, command_id, true, "", payload).await,
        Err(err) => send_result(tx, command_id, false, &err, serde_json::Value::Null).await,
    }
}

async fn send_unsupported(tx: &mpsc::Sender<proto::ClientMessage>, command_id: &str, kind: &str) {
    tracing::debug!("Cloud Connect: ignoring operator-only command {kind} ({command_id})");
    send_result(
        tx,
        command_id,
        false,
        &format!("{kind} is not supported on standalone instances"),
        serde_json::Value::Null,
    )
    .await;
}

/// Hash a SQL string with SHA-256 so the audit log carries a stable
/// identifier for the statement without leaking the statement itself.
fn sql_hash(sql: &str) -> String {
    use sha2::{Digest as _, Sha256};
    let digest = Sha256::digest(sql.as_bytes());
    // Hex is compact, log-friendly, and avoids any worry about base64
    // padding showing up in structured logs.
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Return the longest prefix of `s` no longer than `max` bytes that ends on
/// a UTF-8 char boundary. Used to bound work on the error-redaction path.
fn bounded_prefix(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Trim a runtime error string to a short, safe summary. We deliberately
/// avoid surfacing the full `DataFusion` error message because it can
/// echo back the SQL fragment, table contents, or row values that
/// triggered the failure.
///
/// Strategy:
/// 1. Take the first line only.
/// 2. Strip any literal occurrences of the original `sql` (and substrings
///    long enough to leak meaningful query content) — `DataFusion` errors
///    sometimes include the SQL without quoting.
/// 3. Replace any backtick-, single-quote-, or double-quote-delimited
///    spans with a `<redacted>` placeholder — these almost always carry
///    user data (table names, identifiers, column values).
/// 4. Cap at 256 chars.
fn sanitize_error(err: &str, sql: &str) -> String {
    const MAX_LEN: usize = 256;
    // Bound the work on the error path: inbound gRPC errors can be up to
    // 16 MiB and a pathological one may have no newlines, so cap the slice
    // we scan/redact. The result is truncated to MAX_LEN anyway, so a few
    // KiB of context is far more than enough.
    const MAX_SCAN: usize = 8 * 1024;
    let first_line = err.lines().next().unwrap_or("query failed");
    let first_line = bounded_prefix(first_line, MAX_SCAN);
    let no_sql = redact_sql_occurrences(first_line, sql);
    let redacted = redact_quoted_spans(&no_sql);
    if redacted.len() <= MAX_LEN {
        redacted
    } else {
        let mut s: String = redacted.chars().take(MAX_LEN).collect();
        s.push('…');
        s
    }
}

/// Replace occurrences of the original SQL — both the full text and any
/// substring of length >= [`MIN_SQL_FRAGMENT_LEN`] — with `<sql>` so an
/// error that quotes the query without backticks/quotes does not leak
/// back to the control plane.
fn redact_sql_occurrences(input: &str, sql: &str) -> String {
    /// Below this length a fragment is unlikely to carry meaningful user
    /// SQL (e.g. `SELECT`, `FROM`, `WHERE`) and replacing it would mangle
    /// every error message. Tuned empirically.
    const MIN_SQL_FRAGMENT_LEN: usize = 16;

    // Cap the SQL length the O(n²) fragment scan walks over. The input is
    // already bounded by the caller; bounding the SQL keeps the sliding
    // window cheap for very large queries. A prefix is enough — any leaked
    // fragment of the query is still redacted up to this length.
    const MAX_SQL_SCAN: usize = 4 * 1024;

    let sql_trim = bounded_prefix(sql.trim(), MAX_SQL_SCAN);
    if sql_trim.is_empty() {
        return input.to_string();
    }

    // First pass: remove the full SQL verbatim if it appears.
    let mut out = input.replace(sql_trim, "<sql>");

    // Second pass: scan for the longest substrings of `sql_trim` that
    // appear in the (already partially redacted) error. We only check
    // substrings >= MIN_SQL_FRAGMENT_LEN to avoid eating common keywords.
    if sql_trim.len() >= MIN_SQL_FRAGMENT_LEN {
        // Slide a window over the SQL; if a window appears in `out`,
        // replace it. Use byte indices and char boundaries to stay
        // UTF-8-safe.
        let bytes = sql_trim.as_bytes();
        let mut start = 0;
        while start + MIN_SQL_FRAGMENT_LEN <= bytes.len() {
            // Skip indices that are not on a char boundary.
            if !sql_trim.is_char_boundary(start) {
                start += 1;
                continue;
            }
            // Find the longest suffix starting at `start` that still
            // appears in `out`. Greedy: try the longest first, shorten.
            let mut end = bytes.len();
            while end > start + MIN_SQL_FRAGMENT_LEN {
                if !sql_trim.is_char_boundary(end) {
                    end -= 1;
                    continue;
                }
                let fragment = &sql_trim[start..end];
                if out.contains(fragment) {
                    out = out.replace(fragment, "<sql>");
                    break;
                }
                end -= 1;
            }
            start += 1;
        }
    }

    out
}

/// Walk the input and replace any text between matching backticks /
/// single quotes / double quotes with `<redacted>`. Unterminated
/// delimiters discard the rest of the input.
fn redact_quoted_spans(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(c) = chars.next() {
        if c == '`' || c == '\'' || c == '"' {
            // Skip until the matching delimiter or end of string.
            let mut found_close = false;
            for next in chars.by_ref() {
                if next == c {
                    found_close = true;
                    break;
                }
            }
            out.push_str("<redacted>");
            if !found_close {
                break;
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Fields describing a single `RunQuery` invocation for the audit log.
struct RunQueryAudit<'a> {
    command_id: &'a str,
    sql_hash: &'a str,
    row_count: u64,
    truncated: bool,
    duration_ms: u64,
    success: bool,
}

/// Emit a `kind: "audit"` `EventLog` describing a `RunQuery` invocation.
async fn emit_run_query_audit(
    tx: &mpsc::Sender<proto::ClientMessage>,
    identifier: &str,
    audit: &RunQueryAudit<'_>,
) {
    let RunQueryAudit {
        command_id,
        sql_hash,
        row_count,
        truncated,
        duration_ms,
        success,
    } = *audit;
    let event = serde_json::json!({
        "action": "run_query",
        "sql_hash": sql_hash,
        "row_count": row_count,
        "truncated": truncated,
        "duration_ms": duration_ms,
        "command_id": command_id,
        "success": success,
    });
    let event_json = serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string());
    tracing::info!(
        target: "cloud_connect_audit",
        command_id = %command_id,
        sql_hash = %sql_hash,
        row_count = row_count,
        truncated = truncated,
        duration_ms = duration_ms,
        success = success,
        "RunQuery audit event"
    );
    let msg = proto::ClientMessage {
        body: Some(proto::client_message::Body::Event(proto::EventLog {
            identifier: identifier.to_string(),
            kind: "audit".to_string(),
            event_json,
            timestamp_unix: crate::heartbeat::now_unix(),
        })),
    };
    if let Err(err) = tx.send(msg).await {
        tracing::warn!("Cloud Connect: failed to send audit EventLog: {err}");
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

    fn identity_with_not_after(not_after_unix: u64) -> Identity {
        Identity {
            identifier: "inst_test".to_string(),
            identity_cert_pem: String::new(),
            private_key_pem: String::new(),
            public_key_pem: String::new(),
            ca_bundle_pem: String::new(),
            gateway_addr: "gateway.test:7320".to_string(),
            not_after_unix,
        }
    }

    #[test]
    fn renewal_never_due_for_unbounded_identity() {
        let id = identity_with_not_after(0);
        assert!(!renewal_due(&id, Duration::from_hours(12)));
        assert!(!past_renewal_grace(&id));
    }

    #[test]
    fn renewal_due_within_lead_of_expiry() {
        let lead = Duration::from_hours(12);
        // Expires in 1h with a 12h lead: due now.
        let soon = identity_with_not_after(now_unix() + 3600);
        assert!(renewal_due(&soon, lead));
        // Expires in 24h with a 12h lead: not yet due.
        let later = identity_with_not_after(now_unix() + 24 * 60 * 60);
        assert!(!renewal_due(&later, lead));
        // Already expired: due (renewable within the grace window).
        let expired = identity_with_not_after(now_unix().saturating_sub(60));
        assert!(renewal_due(&expired, lead));
        assert!(!past_renewal_grace(&expired));
    }

    #[test]
    fn identity_past_grace_cannot_renew() {
        let long_dead =
            identity_with_not_after(now_unix().saturating_sub(RENEWAL_GRACE.as_secs() + 60));
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
    fn server_trust_native_roots_only_when_no_pins() {
        // No enrollment bundle and no dev CA: native roots alone.
        let trust = server_trust("", None);
        assert!(trust.native_roots);
        assert!(trust.extra_cas.is_empty());
    }
}
