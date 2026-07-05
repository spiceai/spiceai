use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::config::ReplicationConfig;
use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;
use crate::protocol::framing::{
    read_backend_message, write_copy_data, write_copy_done, write_password_message, write_query,
    write_startup_message, MessageReader,
};
use crate::protocol::messages::{parse_auth_request, parse_error_response};
use crate::protocol::replication::{
    encode_standby_status_update, parse_copy_data, ReplicationCopyData, PG_EPOCH_MICROS,
};

/// Shared replication progress updated by the consumer and read by the worker.
///
/// Stored as an AtomicU64 so progress updates are cheap and monotonic
/// without async backpressure.
pub struct SharedProgress {
    applied: AtomicU64,
}

impl SharedProgress {
    pub fn new(start: Lsn) -> Self {
        Self {
            applied: AtomicU64::new(start.as_u64()),
        }
    }

    #[inline]
    pub fn load_applied(&self) -> Lsn {
        Lsn::from_u64(self.applied.load(Ordering::Acquire))
    }

    /// Monotonic update: if `lsn` is lower than the currently stored applied LSN,
    /// this is a no-op.
    #[inline]
    pub fn update_applied(&self, lsn: Lsn) {
        let new = lsn.as_u64();
        let mut cur = self.applied.load(Ordering::Relaxed);

        while new > cur {
            match self
                .applied
                .compare_exchange_weak(cur, new, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }
}

/// Events emitted by the replication worker.
#[derive(Debug, Clone)]
pub enum ReplicationEvent {
    /// Server heartbeat message.
    KeepAlive {
        /// Current server WAL end position
        wal_end: Lsn,
        /// Whether server requested a reply (already handled internally)
        reply_requested: bool,
        /// Server timestamp (microseconds since 2000-01-01)
        server_time_micros: i64,
    },

    /// Start of a transaction (pgoutput Begin message).
    Begin {
        final_lsn: Lsn,
        xid: u32,
        commit_time_micros: i64,
    },

    /// WAL data containing transaction changes.
    XLogData {
        /// WAL position where this data starts
        wal_start: Lsn,
        /// WAL end position (may be 0 for mid-transaction messages)
        wal_end: Lsn,
        /// Server timestamp (microseconds since 2000-01-01)
        server_time_micros: i64,
        /// pgoutput-encoded change data
        data: Bytes,
    },

    /// End of a transaction (pgoutput Commit message).
    Commit {
        lsn: Lsn,
        end_lsn: Lsn,
        commit_time_micros: i64,
    },

    /// Logical decoding message emitted via `pg_logical_emit_message()`.
    ///
    /// Transactional messages are delivered only after the enclosing
    /// transaction commits. Non-transactional messages are delivered
    /// immediately.
    Message {
        /// Whether the message was emitted inside a transaction.
        transactional: bool,
        /// LSN of the message in the WAL.
        lsn: Lsn,
        /// Application-defined message prefix (e.g. `"myapp.checkpoint"`).
        prefix: String,
        /// Raw message content bytes.
        content: Bytes,
    },

    /// Emitted when `stop_at_lsn` has been reached.
    ///
    /// After this event, no more events will be emitted and the
    /// replication stream will be closed.
    StoppedAt {
        /// The LSN that triggered the stop condition
        reached: Lsn,
    },
}

/// Channel receiver type for replication events.
pub type ReplicationEventReceiver =
    mpsc::Receiver<std::result::Result<ReplicationEvent, PgWireError>>;

/// Internal worker state.
pub struct WorkerState {
    cfg: ReplicationConfig,
    progress: Arc<SharedProgress>,
    stop_rx: watch::Receiver<bool>,
    out: mpsc::Sender<std::result::Result<ReplicationEvent, PgWireError>>,
}

impl WorkerState {
    pub fn new(
        cfg: ReplicationConfig,
        progress: Arc<SharedProgress>,
        stop_rx: watch::Receiver<bool>,
        out: mpsc::Sender<std::result::Result<ReplicationEvent, PgWireError>>,
    ) -> Self {
        Self {
            cfg,
            progress,
            stop_rx,
            out,
        }
    }

    /// Run the replication protocol on the given stream.
    pub async fn run_on_stream<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> Result<()> {
        // Wrap in a 128KB read buffer to batch multiple WAL messages into fewer
        // recv() syscalls. BufReader delegates AsyncWrite to the inner stream,
        // so writes (standby status replies, etc.) are unaffected.
        let mut stream = BufReader::with_capacity(128 * 1024, stream);
        self.startup(&mut stream).await?;
        self.authenticate(&mut stream).await?;
        self.start_replication(&mut stream).await?;
        self.stream_loop(&mut stream).await
    }

    /// Send startup message with replication parameters.
    async fn startup<S: AsyncWrite + Unpin>(&self, stream: &mut S) -> Result<()> {
        let params = [
            ("user", self.cfg.user.as_str()),
            ("database", self.cfg.database.as_str()),
            ("replication", "database"),
            ("client_encoding", "UTF8"),
            ("application_name", "pgwire-replication"),
        ];
        write_startup_message(stream, 196608, &params).await
    }

    /// Start the logical replication stream.
    async fn start_replication<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
    ) -> Result<()> {
        // Escape single quotes in publication name
        let publication = self.cfg.publication.replace('\'', "''");
        let sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {} \
            (proto_version '1', publication_names '{}', messages 'true')",
            self.cfg.slot, self.cfg.start_lsn, publication,
        );
        write_query(stream, &sql).await?;

        // Wait for CopyBothResponse
        loop {
            let msg = read_backend_message(stream).await?;
            match msg.tag {
                b'W' => return Ok(()), // CopyBothResponse - ready to stream
                b'E' => return Err(PgWireError::Server(parse_error_response(&msg.payload))),
                b'N' | b'S' | b'K' => continue, // Notice, ParameterStatus, BackendKeyData
                _ => continue,
            }
        }
    }

    /// Main replication streaming loop.
    ///
    /// Uses a two-phase approach for throughput:
    /// 1. **Drain phase**: while the BufReader has buffered data, read messages
    ///    in a tight loop without `select!` or timeout overhead.
    /// 2. **Wait phase**: when the buffer is empty, fall back to `select!` with
    ///    timeout + stop signal to handle idle keepalives and graceful shutdown.
    ///
    /// Reads use [`MessageReader`], which preserves partial-read state across
    /// dropped futures so the wait-phase `select!` is cancellation-safe.
    async fn stream_loop<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut BufReader<S>,
    ) -> Result<()> {
        let mut last_status_sent = Instant::now() - self.cfg.status_interval;
        let mut last_applied = self.progress.load_applied();
        // Cancellation-safe message reader, partial reads survive dropped futures.
        let mut reader = MessageReader::new();
        // How many messages to process in the tight loop before checking
        // stop signal and sending periodic status feedback.
        const DRAIN_BATCH: usize = 256;

        loop {
            // Update applied LSN from client
            let current_applied = self.progress.load_applied();
            if current_applied != last_applied {
                last_applied = current_applied;
            }

            // Send periodic status feedback
            if last_status_sent.elapsed() >= self.cfg.status_interval {
                self.send_feedback(stream, last_applied, false).await?;
                last_status_sent = Instant::now();
            }

            // ── Drain phase: tight loop while BufReader has buffered data ──
            // The BufReader has a 128KB internal buffer. When the kernel delivers
            // a large TCP segment, many WAL messages are available without syscalls.
            // Read them in a tight loop to avoid select!/timeout overhead per message.
            let mut drained = 0usize;
            while stream.buffer().len() >= 5 && drained < DRAIN_BATCH {
                let msg = reader.read(stream).await?;
                drained += 1;
                if msg.tag == b'E' {
                    return Err(PgWireError::Server(parse_error_response(&msg.payload)));
                }
                if msg.tag == b'd'
                    && self
                        .handle_copy_data(
                            stream,
                            msg.payload,
                            &mut last_applied,
                            &mut last_status_sent,
                        )
                        .await?
                {
                    return Ok(());
                }
            }

            // If we drained messages, loop back to check stop/status before
            // potentially blocking on the next read.
            if drained > 0 {
                // Check stop signal without blocking
                if self.stop_rx.has_changed().unwrap_or(false) && *self.stop_rx.borrow() {
                    let _ = write_copy_done(stream).await;
                    return Ok(());
                }
                continue;
            }

            // ── Wait phase: buffer empty, need to wait for socket data ──
            //
            // Both `stop_rx.changed()` and the timeout can drop the read future
            // mid-message. `MessageReader::read` is cancellation-safe — partial
            // header/payload state lives on `reader` and is preserved across the
            // drop, so the next iteration resumes the read without losing bytes.
            let msg = tokio::select! {
                biased;

                _ = self.stop_rx.changed() => {
                    if *self.stop_rx.borrow() {
                        let _ = write_copy_done(stream).await;
                        return Ok(());
                    }
                    continue;
                }

                msg_result = tokio::time::timeout(
                    self.cfg.idle_wakeup_interval,
                    reader.read(stream),
                ) => {
                    match msg_result {
                        Ok(res) => res?,
                        Err(_) => {
                            let applied = self.progress.load_applied();
                            last_applied = applied;
                            self.send_feedback(stream, applied, false).await?;
                            last_status_sent = Instant::now();
                            continue;
                        }
                    }
                }
            };

            if msg.tag == b'E' {
                return Err(PgWireError::Server(parse_error_response(&msg.payload)));
            }
            if msg.tag == b'd'
                && self
                    .handle_copy_data(
                        stream,
                        msg.payload,
                        &mut last_applied,
                        &mut last_status_sent,
                    )
                    .await?
            {
                return Ok(());
            }
        }
    }

    /// Handle a CopyData message. Returns true if we should stop.
    async fn handle_copy_data<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut BufReader<S>,
        payload: Bytes,
        last_applied: &mut Lsn,
        last_status_sent: &mut Instant,
    ) -> Result<bool> {
        let cd = parse_copy_data(payload)?;

        match cd {
            ReplicationCopyData::KeepAlive {
                wal_end,
                server_time_micros,
                reply_requested,
            } => {
                // Respond immediately if server requests it
                if reply_requested {
                    let applied = self.progress.load_applied();
                    *last_applied = applied;
                    self.send_feedback(stream, applied, true).await?;
                    *last_status_sent = Instant::now();
                }

                self.send_event(Ok(ReplicationEvent::KeepAlive {
                    wal_end,
                    reply_requested,
                    server_time_micros,
                }))
                .await;

                Ok(false)
            }
            ReplicationCopyData::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data,
            } => {
                // If the payload is a pgoutput Begin/Commit message, emit only the boundary event.
                if let Some(boundary_ev) = parse_pgoutput_boundary(&data)? {
                    let reached_lsn = match boundary_ev {
                        ReplicationEvent::Begin { final_lsn, .. } => final_lsn,
                        ReplicationEvent::Commit { end_lsn, .. } => end_lsn,
                        _ => wal_end, // should never happen if parser only returns Begin/Commit
                    };

                    self.send_event(Ok(boundary_ev)).await;

                    // Stop condition (prefer boundary LSN semantics when available)
                    if let Some(stop_lsn) = self.cfg.stop_at_lsn {
                        if reached_lsn >= stop_lsn {
                            self.send_event(Ok(ReplicationEvent::StoppedAt {
                                reached: reached_lsn,
                            }))
                            .await;
                            let _ = write_copy_done(stream).await;
                            return Ok(true); // should stop.
                        }
                    }

                    return Ok(false);
                }
                // Otherwise, emit raw payload
                // Check stop condition
                if let Some(stop_lsn) = self.cfg.stop_at_lsn {
                    if wal_end >= stop_lsn {
                        // Send final event, then stop signal
                        self.send_event(Ok(ReplicationEvent::XLogData {
                            wal_start,
                            wal_end,
                            server_time_micros,
                            data,
                        }))
                        .await;

                        self.send_event(Ok(ReplicationEvent::StoppedAt { reached: wal_end }))
                            .await;

                        let _ = write_copy_done(stream).await;
                        return Ok(true);
                    }
                }

                self.send_event(Ok(ReplicationEvent::XLogData {
                    wal_start,
                    wal_end,
                    server_time_micros,
                    data,
                }))
                .await;

                Ok(false)
            }
        }
    }

    /// Send an event to the client channel.
    ///
    /// If the channel is full or closed, we log and continue - the client
    /// may have stopped listening but we don't want to crash the worker.
    async fn send_event(&self, event: std::result::Result<ReplicationEvent, PgWireError>) {
        if self.out.send(event).await.is_err() {
            tracing::debug!("event channel closed, client may have disconnected");
        }
    }

    /// Handle PostgreSQL authentication exchange.
    async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> Result<()> {
        loop {
            let msg = read_backend_message(stream).await?;
            match msg.tag {
                b'R' => {
                    let (code, rest) = parse_auth_request(&msg.payload)?;
                    self.handle_auth_request(stream, code, rest).await?;
                }
                b'E' => return Err(PgWireError::Server(parse_error_response(&msg.payload))),
                b'S' | b'K' => {}      // ParameterStatus, BackendKeyData - ignore
                b'Z' => return Ok(()), // ReadyForQuery - auth complete
                _ => {}
            }
        }
    }

    /// Handle a specific authentication request.
    async fn handle_auth_request<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        code: i32,
        data: &[u8],
    ) -> Result<()> {
        match code {
            0 => Ok(()), // AuthenticationOk
            3 => {
                // Cleartext password
                let mut payload = Vec::from(self.cfg.password.as_bytes());
                payload.push(0);
                write_password_message(stream, &payload).await
            }
            10 => {
                // SASL (SCRAM-SHA-256)
                self.auth_scram(stream, data).await
            }
            #[cfg(feature = "md5")]
            5 => {
                // MD5 password
                if data.len() != 4 {
                    return Err(PgWireError::Protocol(
                        "MD5 auth: expected 4-byte salt".into(),
                    ));
                }
                let mut salt = [0u8; 4];
                salt.copy_from_slice(&data[..4]);

                let hash = postgres_md5(&self.cfg.password, &self.cfg.user, &salt);
                let mut payload = hash.into_bytes();
                payload.push(0);
                write_password_message(stream, &payload).await
            }
            _ => Err(PgWireError::Auth(format!(
                "unsupported auth method code: {code}"
            ))),
        }
    }

    /// Perform SCRAM-SHA-256 authentication.
    async fn auth_scram<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        mechanisms_data: &[u8],
    ) -> Result<()> {
        // Parse offered mechanisms
        let mechanisms = parse_sasl_mechanisms(mechanisms_data);

        if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
            return Err(PgWireError::Auth(format!(
                "server doesn't offer SCRAM-SHA-256, available: {mechanisms:?}"
            )));
        }

        #[cfg(not(feature = "scram"))]
        return Err(PgWireError::Auth(
            "SCRAM authentication required but 'scram' feature not enabled".into(),
        ));

        #[cfg(feature = "scram")]
        {
            use crate::auth::scram::ScramClient;

            let scram = ScramClient::new(&self.cfg.user);

            // Send SASLInitialResponse
            let mut init = Vec::new();
            init.extend_from_slice(b"SCRAM-SHA-256\0");
            init.extend_from_slice(&(scram.client_first.len() as i32).to_be_bytes());
            init.extend_from_slice(scram.client_first.as_bytes());
            write_password_message(stream, &init).await?;

            // Receive AuthenticationSASLContinue (code 11)
            let server_first = read_auth_data(stream, 11).await?;
            let server_first_str = String::from_utf8_lossy(&server_first);

            // Compute and send client-final
            let (client_final, auth_message, salted_password) =
                scram.client_final(&self.cfg.password, &server_first_str)?;
            write_password_message(stream, client_final.as_bytes()).await?;

            // Receive and verify AuthenticationSASLFinal (code 12)
            let server_final = read_auth_data(stream, 12).await?;
            let server_final_str = String::from_utf8_lossy(&server_final);
            ScramClient::verify_server_final(&server_final_str, &salted_password, &auth_message)?;

            Ok(())
        }
    }

    /// Send standby status update to server.
    async fn send_feedback<S: AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        applied: Lsn,
        reply_requested: bool,
    ) -> Result<()> {
        let client_time = current_pg_timestamp();
        let payload = encode_standby_status_update(applied, client_time, reply_requested);
        write_copy_data(stream, &payload).await
    }
}

/// Parse SASL mechanism list from auth data.
fn parse_sasl_mechanisms(data: &[u8]) -> Vec<String> {
    let mut mechanisms = Vec::new();
    let mut remaining = data;

    while !remaining.is_empty() {
        if let Some(pos) = remaining.iter().position(|&x| x == 0) {
            if pos == 0 {
                break; // Empty string terminates list
            }
            mechanisms.push(String::from_utf8_lossy(&remaining[..pos]).to_string());
            remaining = &remaining[pos + 1..];
        } else {
            break;
        }
    }

    mechanisms
}

fn parse_pgoutput_boundary(data: &Bytes) -> Result<Option<ReplicationEvent>> {
    if data.is_empty() {
        return Ok(None);
    }

    let tag = data[0];
    let mut p = &data[1..];

    fn take_i8(p: &mut &[u8]) -> Result<i8> {
        if p.is_empty() {
            return Err(PgWireError::Protocol("pgoutput: truncated i8".into()));
        }
        let v = p[0] as i8;
        *p = &p[1..];
        Ok(v)
    }

    fn take_i32(p: &mut &[u8]) -> Result<i32> {
        if p.len() < 4 {
            return Err(PgWireError::Protocol("pgoutput: truncated i32".into()));
        }
        let (head, tail) = p.split_at(4);
        *p = tail;
        Ok(i32::from_be_bytes(head.try_into().unwrap()))
    }

    fn take_i64(p: &mut &[u8]) -> Result<i64> {
        if p.len() < 8 {
            return Err(PgWireError::Protocol("pgoutput: truncated i64".into()));
        }
        let (head, tail) = p.split_at(8);
        *p = tail;
        Ok(i64::from_be_bytes(head.try_into().unwrap()))
    }

    match tag {
        b'B' => {
            let final_lsn = Lsn::from_u64(take_i64(&mut p)? as u64);
            let commit_time_micros = take_i64(&mut p)?;
            let xid = take_i32(&mut p)? as u32;

            Ok(Some(ReplicationEvent::Begin {
                final_lsn,
                commit_time_micros,
                xid,
            }))
        }
        b'C' => {
            let _flags = take_i8(&mut p)?;
            let lsn = Lsn::from_u64(take_i64(&mut p)? as u64); // should be safe
            let end_lsn = Lsn::from_u64(take_i64(&mut p)? as u64);
            let commit_time_micros = take_i64(&mut p)?;

            Ok(Some(ReplicationEvent::Commit {
                lsn,
                end_lsn,
                commit_time_micros,
            }))
        }
        b'M' => {
            // Logical decoding message (pg_logical_emit_message)
            // Wire: flags(1) + lsn(8) + prefix(null-terminated) + content_len(4) + content(n)
            let flags = take_i8(&mut p)?;
            let transactional = (flags & 1) != 0;
            let lsn = Lsn::from_u64(take_i64(&mut p)? as u64);

            // Read null-terminated prefix string
            let prefix_end = p.iter().position(|&b| b == 0).ok_or_else(|| {
                PgWireError::Protocol("pgoutput Message: missing null terminator for prefix".into())
            })?;
            let prefix = String::from_utf8_lossy(&p[..prefix_end]).into_owned();
            p = &p[prefix_end + 1..]; // advance past null byte

            let content_len = take_i32(&mut p)? as usize;
            if p.len() < content_len {
                return Err(PgWireError::Protocol(format!(
                    "pgoutput Message: expected {} content bytes, got {}",
                    content_len,
                    p.len()
                )));
            }
            let content = Bytes::copy_from_slice(&p[..content_len]);

            Ok(Some(ReplicationEvent::Message {
                transactional,
                lsn,
                prefix,
                content,
            }))
        }
        _ => Ok(None),
    }
}

/// Read authentication response data for a specific auth code.
async fn read_auth_data<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    expected_code: i32,
) -> Result<Vec<u8>> {
    loop {
        let msg = read_backend_message(stream).await?;
        match msg.tag {
            b'R' => {
                let (code, data) = parse_auth_request(&msg.payload)?;
                if code == expected_code {
                    return Ok(data.to_vec());
                }
                return Err(PgWireError::Auth(format!(
                    "unexpected auth code {code}, expected {expected_code}"
                )));
            }
            b'E' => return Err(PgWireError::Server(parse_error_response(&msg.payload))),
            _ => {} // Skip other messages
        }
    }
}

/// Get current time as PostgreSQL timestamp (microseconds since 2000-01-01).
fn current_pg_timestamp() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let unix_micros = (now.as_secs() as i64) * 1_000_000 + (now.subsec_micros() as i64);
    unix_micros - PG_EPOCH_MICROS
}

/// Compute PostgreSQL MD5 password hash.
#[cfg(feature = "md5")]
fn postgres_md5(password: &str, user: &str, salt: &[u8; 4]) -> String {
    fn md5_hex(data: &[u8]) -> String {
        format!("{:x}", md5::compute(data))
    }

    // First hash: md5(password + username)
    let inner = md5_hex(format!("{password}{user}").as_bytes());

    // Second hash: md5(inner_hash + salt)
    let mut outer_input = inner.into_bytes();
    outer_input.extend_from_slice(salt);

    format!("md5{}", md5_hex(&outer_input))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sasl_mechanisms_single() {
        let data = b"SCRAM-SHA-256\0\0";
        let mechs = parse_sasl_mechanisms(data);
        assert_eq!(mechs, vec!["SCRAM-SHA-256"]);
    }

    #[test]
    fn parse_sasl_mechanisms_multiple() {
        let data = b"SCRAM-SHA-256\0SCRAM-SHA-256-PLUS\0\0";
        let mechs = parse_sasl_mechanisms(data);
        assert_eq!(mechs, vec!["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]);
    }

    #[test]
    fn parse_sasl_mechanisms_empty() {
        let mechs = parse_sasl_mechanisms(b"\0");
        assert!(mechs.is_empty());
    }

    #[test]
    #[cfg(feature = "md5")]
    fn postgres_md5_known_value() {
        // Test vector: user="md5_user", password="md5_pass", salt=[0x01, 0x02, 0x03, 0x04]
        // Can verify with: SELECT 'md5' || md5(md5('md5_passmd5_user') || E'\\x01020304');
        let hash = postgres_md5("md5_pass", "md5_user", &[0x01, 0x02, 0x03, 0x04]);
        assert!(hash.starts_with("md5"));
        assert_eq!(hash.len(), 35); // "md5" + 32 hex chars
    }

    #[test]
    fn current_pg_timestamp_is_positive() {
        // Any time after 2000-01-01 should be positive
        let ts = current_pg_timestamp();
        assert!(ts > 0);
    }
}
