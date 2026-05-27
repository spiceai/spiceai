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

//! Outbound bidirectional-stream client.
//!
//! Connects to the configured Cloud Connect endpoint over TLS, sends a
//! `Hello`, then enters a long-running loop that processes
//! `ControlMessage`s from the server and emits `ClientMessage`s back
//! (heartbeats, command results, telemetry, adopt ack).
//!
//! Disconnects are tolerated: the driver reconnects with exponential
//! backoff (1s → 60s with jitter). On reconnect:
//!
//! - If we have an identity, send it as `Hello.credential` with type
//!   `INSTANCE` and the assigned identifier.
//! - Otherwise, send the (pending) adoption code as `Hello.credential`
//!   and identifier empty.
//!
//! If a Forget arrives, we clear the local identity and exit the
//! cloud-connect task — spiced itself stays up and keeps serving local
//! spicepod traffic as before. This matches the UniFi semantics where
//! "Forget" releases management but doesn't destroy the device. To
//! re-adopt, the user runs `spice connect <code>` and restarts spiced.

use std::sync::Arc;
use std::time::Duration;

use snafu::ResultExt;
use crate::TransportSnafu;
use tokio::sync::{RwLock, mpsc};
use tokio::time;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};

use crate::config::CloudConnectConfig;
use crate::handlers::RuntimeHandle;
use crate::heartbeat::{HEARTBEAT_INTERVAL, TELEMETRY_INTERVAL, build_heartbeat, build_telemetry, now_unix};
use crate::identity::{Identity, IdentityStore};
use crate::proto;
use crate::shutdown::Shutdown;
use crate::{Error, Result, fingerprint};

/// Minimum reconnect backoff.
const MIN_BACKOFF: Duration = Duration::from_secs(1);
/// Maximum reconnect backoff.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Outbound channel size: bounded to keep memory predictable.
const CLIENT_CHANNEL_SIZE: usize = 64;

/// State held by the driver across reconnects.
pub(crate) struct ClientDriver {
    config: CloudConnectConfig,
    runtime: Arc<dyn RuntimeHandle>,
    shutdown: Arc<Shutdown>,
    /// Currently-effective identity, if any. Replaced on adoption; set
    /// to `None` on Forget or when the identity cert expires.
    identity: Option<Identity>,
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
        }
    }

    /// Run the driver until shutdown is requested.
    ///
    /// The driver is fully fault-tolerant: a transport, decode, or
    /// stream error triggers a reconnect with backoff; only an explicit
    /// shutdown notify exits the loop.
    pub(crate) async fn run(mut self) -> Result<()> {
        let mut backoff = MIN_BACKOFF;

        loop {
            // Honor shutdown before each reconnect.
            if self.shutdown.is_triggered() {
                tracing::info!("Cloud Connect: shutdown requested; exiting driver");
                return Ok(());
            }

            // If the stored identity is expired, fall back to "pending"
            // and require a fresh adoption code.
            if let Some(ref id) = self.identity
                && id.is_expired()
            {
                tracing::warn!(
                    "Cloud Connect: stored identity is expired; falling back to pending-adoption state"
                );
                self.identity = None;
            }

            // Determine the credential for this attempt.
            let (client_type, identifier, credential) = self.next_credential();

            if credential.is_empty() {
                tracing::warn!(
                    "Cloud Connect: no credentials available; sleeping {} before retry",
                    humanize(backoff)
                );
            } else {
                tracing::debug!(
                    "Cloud Connect: attempting connect to {} (identifier={})",
                    self.config.endpoint,
                    if identifier.is_empty() {
                        "<pending>"
                    } else {
                        identifier.as_str()
                    },
                );
            }

            match self.connect_and_run(client_type, identifier, credential).await {
                Ok(ExitReason::Shutdown) => return Ok(()),
                Ok(ExitReason::Forget) => {
                    tracing::info!(
                        "Cloud Connect: Forget acknowledged; cloud-connect task exiting. spiced remains running and serving local spicepod traffic. To re-adopt, run `spice connect <code>` and restart spiced."
                    );
                    return Ok(());
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

            // Sleep with jitter, then retry.
            let jitter_ms: u64 = rand::random::<u64>() % 500;
            let sleep_for = backoff + Duration::from_millis(jitter_ms);
            tracing::debug!("Cloud Connect: sleeping {} before reconnect", humanize(sleep_for));
            tokio::select! {
                () = time::sleep(sleep_for) => {},
                () = self.shutdown.wait() => {
                    tracing::info!("Cloud Connect: shutdown requested during backoff; exiting");
                    return Ok(());
                }
            }

            backoff = next_backoff(backoff);
        }
    }

    /// Determine the next-attempt credential triple.
    fn next_credential(&self) -> (proto::ClientType, String, String) {
        if let Some(ref id) = self.identity {
            return (
                proto::ClientType::Instance,
                id.identifier.clone(),
                id.identity_cert_pem.clone(),
            );
        }
        if let Some(ref code) = self.config.adoption_code {
            return (proto::ClientType::Instance, String::new(), code.clone());
        }
        (proto::ClientType::Instance, String::new(), String::new())
    }

    async fn connect_and_run(
        &mut self,
        client_type: proto::ClientType,
        identifier: String,
        credential: String,
    ) -> Result<ExitReason> {
        if credential.is_empty() {
            return Err(Error::NoCredentials);
        }

        let channel = build_channel(&self.config)?;
        let mut grpc =
            proto::cloud_connect_client::CloudConnectClient::new(channel).max_decoding_message_size(16 * 1024 * 1024);

        // Outbound channel: we hand the receiver to tonic and keep the
        // sender to push ClientMessages from this task.
        let (tx, rx) = mpsc::channel::<proto::ClientMessage>(CLIENT_CHANNEL_SIZE);

        // Send Hello as the first frame.
        let hello = build_hello(&self.config, client_type, identifier, credential);
        tx.send(proto::ClientMessage {
            body: Some(proto::client_message::Body::Hello(hello)),
        })
        .await
        .map_err(|_| Error::NoCredentials)?;

        let request = tonic::Request::new(ReceiverStream::new(rx));
        let response = grpc
            .stream(request)
            .await
            .map_err(|status| Error::Stream { source: status })?;

        let mut server_stream: Streaming<proto::ControlMessage> = response.into_inner();
        tracing::info!("Cloud Connect: stream established to {}", self.config.endpoint);

        // Spawn periodic heartbeat + telemetry tasks. They emit through
        // the same outbound channel. The identifier is shared by RwLock
        // so that frames sent *after* a first-contact adoption pick up
        // the assigned identifier without waiting for a reconnect.
        let runtime = Arc::clone(&self.runtime);
        let identifier = Arc::new(RwLock::new(
            self.identity
                .as_ref()
                .map(|i| i.identifier.clone())
                .unwrap_or_default(),
        ));

        let hb_tx = tx.clone();
        let hb_runtime = Arc::clone(&runtime);
        let hb_identifier = Arc::clone(&identifier);
        let hb_handle = tokio::spawn(async move {
            let mut seq: u64 = 0;
            let mut ticker = time::interval(HEARTBEAT_INTERVAL);
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
            let mut ticker = time::interval(TELEMETRY_INTERVAL);
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

        // Main read loop.
        let mut pending_code_consumed = false;
        let exit_reason = loop {
            tokio::select! {
                () = self.shutdown.wait() => {
                    tracing::info!("Cloud Connect: shutdown requested; closing stream");
                    break ExitReason::Shutdown;
                }
                next = server_stream.message() => {
                    match next {
                        Ok(Some(msg)) => {
                            // The first inbound after Hello is taken as
                            // implicit acceptance of the adoption code
                            // (server may also send a no-op Ack). Delete
                            // the pending file so we don't replay a
                            // consumed code on restart.
                            if !pending_code_consumed
                                && self.identity.is_none()
                                && let Some(ref path) = self.config.pending_adopt_code_path
                            {
                                if let Err(err) = std::fs::remove_file(path)
                                    && err.kind() != std::io::ErrorKind::NotFound
                                {
                                    tracing::warn!(
                                        "Cloud Connect: could not remove pending code at {}: {err}",
                                        path.display()
                                    );
                                }
                                pending_code_consumed = true;
                            }

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
                tracing::debug!(
                    "Cloud Connect: ack for command_id={}",
                    ack.for_command_id
                );
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
                match self.runtime.execute_sql(&cmd.sql, cmd.max_rows).await {
                    Ok(payload) => {
                        let duration_ms = started.elapsed().as_millis() as u64;
                        let row_count = payload
                            .get("row_count")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or(0);
                        let truncated = payload
                            .get("truncated")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false);
                        emit_run_query_audit(
                            tx,
                            &identifier,
                            &cmd.command_id,
                            &sql_hash,
                            row_count,
                            truncated,
                            duration_ms,
                            true,
                        )
                        .await;
                        send_result(tx, &cmd.command_id, true, "", payload).await;
                    }
                    Err(err) => {
                        let duration_ms = started.elapsed().as_millis() as u64;
                        emit_run_query_audit(
                            tx,
                            &identifier,
                            &cmd.command_id,
                            &sql_hash,
                            0,
                            false,
                            duration_ms,
                            false,
                        )
                        .await;
                        // Safe error: never includes the SQL text.
                        send_result(
                            tx,
                            &cmd.command_id,
                            false,
                            &sanitize_error(&err),
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
                        let success = payload
                            .get("status")
                            .and_then(serde_json::Value::as_str)
                            != Some("unsupported");
                        send_result(tx, &cmd.command_id, success, "", payload).await;
                    }
                    Err(err) => {
                        send_result(
                            tx,
                            &cmd.command_id,
                            false,
                            &err,
                            serde_json::Value::Null,
                        )
                        .await;
                    }
                }
            }
            proto::control_message::Body::Adopt(cmd) => {
                self.handle_adopt(tx, cmd, live_identifier).await;
            }
            proto::control_message::Body::Forget(cmd) => {
                self.handle_forget(tx, cmd, live_identifier).await;
                return Some(ExitReason::Forget);
            }
            // Operator-only commands: acknowledge with an error.
            proto::control_message::Body::ApplyManifest(cmd) => {
                send_unsupported(tx, &cmd.command_id, "ApplyManifest").await;
            }
            proto::control_message::Body::DeleteManifest(cmd) => {
                send_unsupported(tx, &cmd.command_id, "DeleteManifest").await;
            }
            proto::control_message::Body::GetStatus(cmd) => {
                send_unsupported(tx, &cmd.command_id, "GetStatus").await;
            }
            proto::control_message::Body::Drain(cmd) => {
                send_unsupported(tx, &cmd.command_id, "Drain").await;
            }
            proto::control_message::Body::Pause(cmd) => {
                send_unsupported(tx, &cmd.command_id, "Pause").await;
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
        // Generate keypair + persist identity.
        let pair = match IdentityStore::generate_keypair() {
            Ok(pair) => pair,
            Err(err) => {
                tracing::error!("Cloud Connect: failed to generate keypair for adoption: {err}");
                send_result(
                    tx,
                    &cmd.command_id,
                    false,
                    &format!("keypair generation failed: {err}"),
                    serde_json::Value::Null,
                )
                .await;
                return;
            }
        };

        let identity = Identity {
            identifier: cmd.assigned_identifier.clone(),
            identity_cert_pem: cmd.identity_cert_pem.clone(),
            private_key_pem: pair.private_key_pem,
            public_key_pem: pair.public_key_pem.clone(),
            not_after_unix: cmd.not_after_unix,
        };

        if let Err(err) = IdentityStore::store(&self.config.identity_path, &identity) {
            tracing::error!(
                "Cloud Connect: failed to persist identity at {}: {err}",
                self.config.identity_path.display()
            );
            send_result(
                tx,
                &cmd.command_id,
                false,
                &format!("persist identity failed: {err}"),
                serde_json::Value::Null,
            )
            .await;
            return;
        }

        tracing::info!(
            "Cloud Connect: adopted as {} (identity stored at {})",
            identity.identifier,
            self.config.identity_path.display()
        );
        self.identity = Some(identity.clone());
        // Push the assigned identifier into the shared cell so the
        // in-flight heartbeat / telemetry tasks pick it up on their
        // next tick (otherwise frames on the same stream would carry
        // an empty identifier until the next reconnect).
        *live_identifier.write().await = identity.identifier.clone();

        // Clear the pending code file — adoption succeeded.
        if let Some(ref path) = self.config.pending_adopt_code_path {
            let _ = std::fs::remove_file(path);
        }
        // Clear the adoption_code in-memory so future reconnects use
        // identity-only credentials.
        self.config.adoption_code = None;

        // Send AdoptAck (separate from CommandResult) so the cloud can
        // pin the public key.
        let ack = proto::AdoptAck {
            identifier: identity.identifier,
            identity_pubkey_pem: identity.public_key_pem,
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
                "identifier": cmd.assigned_identifier,
            }),
        )
        .await;
    }

    async fn handle_forget(
        &mut self,
        tx: &mpsc::Sender<proto::ClientMessage>,
        cmd: proto::Forget,
        live_identifier: &Arc<RwLock<String>>,
    ) {
        // Clear identity from disk and memory.
        if let Err(err) = IdentityStore::clear(&self.config.identity_path) {
            tracing::warn!(
                "Cloud Connect: failed to clear identity at {}: {err}",
                self.config.identity_path.display()
            );
        }
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
    }
}

/// Reasons the inner loop can exit.
enum ExitReason {
    Shutdown,
    Disconnected,
    Forget,
}

fn build_channel(config: &CloudConnectConfig) -> Result<Channel> {
    let mut endpoint = Endpoint::from_shared(config.endpoint.clone())
        .map_err(|source| Error::InvalidEndpoint {
            endpoint: config.endpoint.clone(),
            source,
        })?
        .timeout(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(10))
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .http2_keep_alive_interval(Duration::from_secs(15))
        .keep_alive_timeout(Duration::from_secs(60))
        .keep_alive_while_idle(true);

    if !config.insecure {
        let mut tls = ClientTlsConfig::new().with_native_roots();
        if let Some(ref ca_pem) = config.ca_cert_pem {
            tls = tls.ca_certificate(Certificate::from_pem(ca_pem.as_bytes()));
        }
        endpoint = endpoint.tls_config(tls).context(TransportSnafu)?;
    }

    Ok(endpoint.connect_lazy())
}

fn build_hello(
    config: &CloudConnectConfig,
    client_type: proto::ClientType,
    identifier: String,
    credential: String,
) -> proto::Hello {
    proto::Hello {
        client_type: client_type as i32,
        identifier,
        credential,
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
        })),
    };
    if let Err(err) = tx.send(msg).await {
        tracing::warn!("Cloud Connect: failed to send CommandResult: {err}");
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

/// Trim a runtime error string to a short, safe summary. We deliberately
/// avoid surfacing the full DataFusion error message because it can
/// echo back the SQL fragment, table contents, or row values that
/// triggered the failure.
///
/// Strategy:
/// 1. Take the first line only.
/// 2. Replace any backtick-, single-quote-, or double-quote-delimited
///    spans with a `<redacted>` placeholder — these almost always carry
///    user data (table names, identifiers, column values).
/// 3. Cap at 256 chars.
fn sanitize_error(err: &str) -> String {
    const MAX_LEN: usize = 256;
    let first_line = err.lines().next().unwrap_or("query failed");
    let redacted = redact_quoted_spans(first_line);
    if redacted.len() <= MAX_LEN {
        redacted
    } else {
        let mut s: String = redacted.chars().take(MAX_LEN).collect();
        s.push('…');
        s
    }
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

/// Emit a `kind: "audit"` EventLog describing a RunQuery invocation.
#[allow(clippy::too_many_arguments)]
async fn emit_run_query_audit(
    tx: &mpsc::Sender<proto::ClientMessage>,
    identifier: &str,
    command_id: &str,
    sql_hash: &str,
    row_count: u64,
    truncated: bool,
    duration_ms: u64,
    success: bool,
) {
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
        let d = next_backoff(Duration::from_secs(120));
        assert_eq!(d, MAX_BACKOFF);
    }
}
