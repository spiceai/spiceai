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

//! SMB2 client — manages TCP connections and speaks the protocol.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::auth;
use crate::crypto;
use crate::protocol::{
    CloseResponse, Command, CreateResponse, DIALECT_SMB3_1_1, DesiredAccess, DirectoryEntry,
    FILE_ID_BOTH_DIRECTORY_INFORMATION, Header, NT_STATUS_ERROR_MASK, NtStatus, SENTINEL_FILE_ID,
    SMB2_FLAGS_RELATED, SMB2_FLAGS_SIGNED, SMB2_HEADER_SIZE, STATUS_PENDING, build_request,
    decode_close_response, decode_create_response, decode_negotiate_response, decode_read_response,
    decode_read_response_owned, decode_session_setup_response, decode_write_response,
    encode_close_request, encode_close_request_ex, encode_create_request, encode_negotiate_request,
    encode_query_directory_request, encode_read_request, encode_session_setup_request,
    encode_set_info_rename, encode_tree_connect_request, encode_write_request,
    parse_directory_entries,
};

/// Default timeout for a single SMB response read. Prevents indefinite mutex
/// hold when the SMB server is slow or unresponsive under heavy load.
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Default I/O cap for standalone (non-compound) read/write operations.
/// Many NAS servers advertise multi-MB maximums in negotiate but fail at sizes
/// well below the advertised limit. 64 KB is the safe conservative default;
/// override via [`SmbConfig::max_io_size`] for servers that handle larger I/O.
const DEFAULT_MAX_IO: u32 = 65536;

/// Configuration for connecting to an SMB server.
#[derive(Debug, Clone)]
pub struct SmbConfig {
    pub server: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub domain: String,
    pub workstation: String,
    /// Cap for standalone read/write I/O (0 = use `DEFAULT_MAX_IO`).
    pub max_io_size: u32,
    /// Per-response read timeout (None = use `DEFAULT_READ_TIMEOUT`).
    pub read_timeout: Option<Duration>,
}

impl SmbConfig {
    #[must_use]
    pub fn share_path(&self, share: &str) -> String {
        format!(r"\\{}\{}", self.server, share)
    }
}

/// An authenticated SMB2 session.
pub struct SmbClient {
    stream: Mutex<TcpStream>,
    message_id: AtomicU64,
    session_id: u64,
    config: SmbConfig,
    /// Effective max read size for standalone (non-compound) reads.
    pub(crate) max_read_size: u32,
    /// Effective max write size for standalone (non-compound) writes.
    pub(crate) max_write_size: u32,
    /// Capped max for compound operations.
    pub(crate) compound_max_read_size: u32,
    pub(crate) compound_max_write_size: u32,
    /// Effective per-response read timeout.
    read_timeout: Duration,
    client_guid: [u8; 16],
    signing_key: Option<[u8; 16]>,
    /// Set on read timeout — connection framing is desynchronized.
    poisoned: AtomicBool,
}

impl SmbClient {
    /// Connect to the SMB server and authenticate.
    pub async fn connect(config: SmbConfig) -> io::Result<Arc<Self>> {
        let addr = format!("{}:{}", config.server, config.port);
        let stream = match TcpStream::connect(&addr).await {
            Ok(s) => {
                tracing::debug!(target: "smb", "tcp connected: {addr}");
                s
            }
            Err(e) => {
                tracing::warn!(target: "smb", "tcp connect failed: {addr}: {e}");
                return Err(e);
            }
        };
        stream.set_nodelay(true)?;

        let mut client_guid = [0u8; 16];
        crypto::random_bytes(&mut client_guid);

        let read_timeout = config.read_timeout.unwrap_or(DEFAULT_READ_TIMEOUT);
        let mut client = Self {
            stream: Mutex::new(stream),
            message_id: AtomicU64::new(0),
            session_id: 0,
            config,
            max_read_size: 65536,
            max_write_size: 65536,
            compound_max_read_size: 65536,
            compound_max_write_size: 65536,
            read_timeout,
            client_guid,
            signing_key: None,
            poisoned: AtomicBool::new(false),
        };

        client.negotiate_and_auth().await?;
        Ok(Arc::new(client))
    }

    /// Whether this connection has been poisoned by a timeout.
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    /// Mark the connection as poisoned. Used when an early-exit error path
    /// leaves unread response frames in the TCP stream — subsequent
    /// operations would otherwise read those stale frames and corrupt
    /// request/response framing.
    fn poison(&self) {
        self.poisoned.store(true, Ordering::Relaxed);
    }

    fn next_message_id(&self) -> u64 {
        self.message_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Read exactly `buf.len()` bytes from the stream with a timeout.
    ///
    /// On timeout the stream framing is desynchronized, so we poison the
    /// connection (all future operations fail fast) and drop the underlying
    /// socket to fully close both halves.
    async fn read_exact_timeout(&self, stream: &mut TcpStream, buf: &mut [u8]) -> io::Result<()> {
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "SMB connection poisoned by previous timeout",
            ));
        }
        if let Ok(result) = tokio::time::timeout(self.read_timeout, stream.read_exact(buf)).await {
            result.map(|_| ())
        } else {
            self.poisoned.store(true, Ordering::Relaxed);
            let _ = stream.shutdown().await;
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "SMB server read timed out; connection poisoned",
            ))
        }
    }

    async fn send_recv_raw(&self, packet: &mut [u8]) -> io::Result<(Header, Vec<u8>, Vec<u8>)> {
        self.send_recv_inner(packet).await
    }

    async fn send_recv(&self, packet: &mut [u8]) -> io::Result<(Header, Vec<u8>)> {
        let (header, body, _raw) = self.send_recv_inner(packet).await?;
        Ok((header, body))
    }

    async fn send_recv_inner(&self, packet: &mut [u8]) -> io::Result<(Header, Vec<u8>, Vec<u8>)> {
        let mut stream = self.stream.lock().await;

        if let Some(ref key) = self.signing_key {
            sign_packet(packet, key);
        }
        stream.write_all(packet).await?;
        stream.flush().await?;

        loop {
            let mut len_buf = [0u8; 4];
            self.read_exact_timeout(&mut stream, &mut len_buf).await?;
            let msg_len = u32::from_be_bytes(len_buf) as usize;

            if !(SMB2_HEADER_SIZE..=16 * 1024 * 1024).contains(&msg_len) {
                tracing::warn!(target: "smb", "invalid message length: {msg_len}");
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid SMB2 message length: {msg_len}"),
                ));
            }

            let mut msg = vec![0u8; msg_len];
            self.read_exact_timeout(&mut stream, &mut msg).await?;

            let header = Header::decode(&msg).ok_or_else(|| {
                tracing::warn!(target: "smb", "invalid header");
                io::Error::new(io::ErrorKind::InvalidData, "invalid SMB2 header")
            })?;

            // STATUS_PENDING — server still processing, wait for real response
            if header.status == STATUS_PENDING {
                continue;
            }

            // Verify the server-side signature once signing is established.
            // Per [MS-SMB2] §3.2.5.1.3, after auth completes every signed
            // request must be answered with a signed response, and the
            // client MUST verify the CMAC. Without this verification, a
            // MITM or corrupted proxy can tamper with reads, metadata, and
            // directory listings even after signing is negotiated. We poison
            // and tear down the connection on any mismatch so subsequent
            // operations can't read attacker-controlled bytes.
            if let Some(ref key) = self.signing_key {
                if header.flags & SMB2_FLAGS_SIGNED == 0 {
                    self.poison();
                    let _ = stream.shutdown().await;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "SMB2 response missing signature after signing established",
                    ));
                }
                if !verify_signature(&mut msg, key) {
                    self.poison();
                    let _ = stream.shutdown().await;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "SMB2 response signature verification failed",
                    ));
                }
            }

            let body = msg[SMB2_HEADER_SIZE..].to_vec();
            return Ok((header, body, msg));
        }
    }

    /// Perform negotiate + session setup (NTLM auth) with signing key derivation.
    async fn negotiate_and_auth(&mut self) -> io::Result<()> {
        let mut preauth_hash = [0u8; 64];

        // ── Step 1: Negotiate ──
        let msg_id = self.next_message_id();
        let hdr = Header::new(Command::Negotiate, msg_id);
        let mut packet = build_request(&hdr, |buf| {
            encode_negotiate_request(buf, &self.client_guid);
        });

        update_preauth_hash(&mut preauth_hash, &packet[4..]);

        let (resp_hdr, resp_body, resp_raw) = self.send_recv_raw(&mut packet).await?;
        if NtStatus::from_u32(resp_hdr.status).is_error() {
            tracing::warn!(target: "smb", "negotiate failed: 0x{:08X}", resp_hdr.status);
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("negotiate failed: status=0x{:08X}", resp_hdr.status),
            ));
        }

        update_preauth_hash(&mut preauth_hash, &resp_raw);

        let neg_resp = decode_negotiate_response(&resp_body).ok_or_else(|| {
            tracing::warn!(target: "smb", "invalid negotiate response");
            io::Error::new(io::ErrorKind::InvalidData, "invalid negotiate response")
        })?;

        // Subsequent signing-key derivation assumes the SMB 3.1.1 preauth
        // integrity hash as the KDF context (see [MS-SMB2] 3.1.4.5.1). If a
        // server selected an older dialect we'd derive the wrong key and every
        // signed request after auth would fail with STATUS_ACCESS_DENIED.
        if neg_resp.dialect_revision != DIALECT_SMB3_1_1 {
            tracing::warn!(
                target: "smb",
                "server selected unsupported dialect 0x{:04X}; only 3.1.1 is supported",
                neg_resp.dialect_revision,
            );
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "server selected unsupported SMB dialect 0x{:04X}; only 3.1.1 is supported",
                    neg_resp.dialect_revision
                ),
            ));
        }

        let io_cap = if self.config.max_io_size > 0 {
            self.config.max_io_size
        } else {
            DEFAULT_MAX_IO
        };
        tracing::debug!(
            target: "smb",
            "negotiated SMB 0x{:04X}, server_max={}K io_cap={}K",
            neg_resp.dialect_revision,
            neg_resp.max_read_size / 1024,
            io_cap / 1024,
        );

        // ── Step 2: Session Setup (NTLM Negotiate) ──
        let ntlm_negotiate = auth::build_negotiate_message();
        let spnego_negotiate = auth::wrap_spnego_negotiate(&ntlm_negotiate);

        let msg_id = self.next_message_id();
        let mut hdr = Header::new(Command::SessionSetup, msg_id);
        let mut packet = build_request(&hdr, |buf| {
            encode_session_setup_request(buf, &spnego_negotiate);
        });

        update_preauth_hash(&mut preauth_hash, &packet[4..]);

        let (resp_hdr, resp_body, resp_raw) = self.send_recv_raw(&mut packet).await?;

        update_preauth_hash(&mut preauth_hash, &resp_raw);

        let sess_resp = decode_session_setup_response(&resp_hdr, &resp_body).ok_or_else(|| {
            tracing::warn!(target: "smb", "invalid session setup response");
            io::Error::new(io::ErrorKind::InvalidData, "invalid session setup response")
        })?;

        let challenge_data = auth::unwrap_spnego(&sess_resp.security_buffer);
        let challenge = auth::parse_challenge_message(challenge_data).ok_or_else(|| {
            tracing::warn!(target: "smb", "invalid NTLM challenge");
            io::Error::new(io::ErrorKind::InvalidData, "invalid NTLM challenge")
        })?;

        // ── Step 3: Session Setup (NTLM Auth) ──
        let (ntlm_auth, session_base_key) = auth::build_authenticate_message(
            &challenge,
            &self.config.username,
            &self.config.password,
            &self.config.domain,
            &self.config.workstation,
        );
        let spnego_auth = auth::wrap_spnego_auth(&ntlm_auth);

        let msg_id = self.next_message_id();
        hdr = Header::new(Command::SessionSetup, msg_id);
        hdr.session_id = sess_resp.session_id;
        let mut packet = build_request(&hdr, |buf| {
            encode_session_setup_request(buf, &spnego_auth);
        });

        update_preauth_hash(&mut preauth_hash, &packet[4..]);

        let (resp_hdr, _resp_body, mut resp_raw) = self.send_recv_raw(&mut packet).await?;
        if NtStatus::from_u32(resp_hdr.status).is_error() {
            tracing::warn!(target: "smb", "auth failed: 0x{:08X}", resp_hdr.status);
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("authentication failed: status=0x{:08X}", resp_hdr.status),
            ));
        }

        // Per [MS-SMB2] §3.2.5.5.3, the preauth integrity hash used for key
        // derivation ends with the final SESSION_SETUP *request*; the
        // STATUS_SUCCESS response is excluded. Only intermediate responses
        // (STATUS_MORE_PROCESSING_REQUIRED) are hashed. The server signs the
        // final response with the derived signing key, so the key cannot
        // depend on that response's own bytes. Hashing it here derives a key
        // that disagrees with the server's and every subsequent signed
        // exchange fails verification.
        let signing_key = auth::derive_signing_key(&session_base_key, &preauth_hash);
        tracing::debug!(target: "smb", "authenticated, signing key derived");

        // The server signs the final SESSION_SETUP response with the key it
        // just derived. When the response is marked signed, verify it before
        // trusting the session — a mismatch means the two sides disagree on
        // the key or the auth-completing response was tampered with in
        // transit ([MS-SMB2] §3.2.5.5.3).
        if resp_hdr.flags & SMB2_FLAGS_SIGNED != 0 && !verify_signature(&mut resp_raw, &signing_key)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "final SESSION_SETUP response signature verification failed",
            ));
        }

        self.session_id = resp_hdr.session_id;
        let transact = neg_resp.max_transact_size;
        let io_cap = if self.config.max_io_size > 0 {
            self.config.max_io_size
        } else {
            DEFAULT_MAX_IO
        };
        self.max_read_size = neg_resp.max_read_size.min(transact).min(io_cap);
        self.max_write_size = neg_resp.max_write_size.min(transact).min(io_cap);
        self.compound_max_read_size = self.max_read_size.min(65536);
        self.compound_max_write_size = self.max_write_size.min(65536);
        self.signing_key = Some(signing_key);
        Ok(())
    }

    /// Connect to a share (Tree Connect).
    pub async fn tree_connect(&self, share: &str) -> io::Result<u32> {
        let path = self.config.share_path(share);
        let msg_id = self.next_message_id();
        let mut hdr = Header::new(Command::TreeConnect, msg_id);
        hdr.session_id = self.session_id;

        let mut packet = build_request(&hdr, |buf| {
            encode_tree_connect_request(buf, &path);
        });

        let (resp_hdr, _resp_body) = self.send_recv(&mut packet).await?;
        let status = NtStatus::from_u32(resp_hdr.status);
        if status.is_error() {
            tracing::warn!(
                target: "smb",
                "tree connect failed: '{share}': 0x{:08X}",
                resp_hdr.status
            );
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "tree connect to '{share}' failed: 0x{:08X}",
                    resp_hdr.status
                ),
            ));
        }

        tracing::debug!(
            target: "smb",
            r"tree connected: \\{}\{}",
            self.config.server,
            share
        );
        Ok(resp_hdr.tree_id)
    }

    /// Open a file or directory.
    pub async fn create(
        &self,
        tree_id: u32,
        path: &str,
        desired_access: u32,
        share_access: u32,
        create_disposition: u32,
        create_options: u32,
    ) -> io::Result<CreateResponse> {
        let msg_id = self.next_message_id();
        let mut hdr = Header::new(Command::Create, msg_id);
        hdr.session_id = self.session_id;
        hdr.tree_id = tree_id;

        let mut packet = build_request(&hdr, |buf| {
            encode_create_request(
                buf,
                path,
                desired_access,
                share_access,
                create_disposition,
                create_options,
            );
        });

        let (resp_hdr, resp_body) = self.send_recv(&mut packet).await?;
        let status = NtStatus::from_u32(resp_hdr.status);
        if status.is_error() {
            return Err(smb_status_to_io_error(resp_hdr.status, path));
        }

        decode_create_response(&resp_body).ok_or_else(|| {
            tracing::warn!(target: "smb", "invalid create response: {path}");
            io::Error::new(io::ErrorKind::InvalidData, "invalid create response")
        })
    }

    /// Close a file handle.
    pub async fn close(&self, tree_id: u32, file_id: &[u8; 16]) -> io::Result<()> {
        self.close_inner(tree_id, file_id, false).await.map(|_| ())
    }

    /// Close a file handle and request post-query attributes. When the server
    /// honors the request, the returned `CloseResponse` carries the final
    /// `last_write_time` and `file_size`, saving a follow-up `head_object`
    /// round trip.
    pub async fn close_with_attrs(
        &self,
        tree_id: u32,
        file_id: &[u8; 16],
    ) -> io::Result<Option<CloseResponse>> {
        self.close_inner(tree_id, file_id, true).await
    }

    async fn close_inner(
        &self,
        tree_id: u32,
        file_id: &[u8; 16],
        postquery: bool,
    ) -> io::Result<Option<CloseResponse>> {
        let msg_id = self.next_message_id();
        let mut hdr = Header::new(Command::Close, msg_id);
        hdr.session_id = self.session_id;
        hdr.tree_id = tree_id;

        let mut packet = build_request(&hdr, |buf| {
            encode_close_request_ex(buf, file_id, postquery);
        });

        let (resp_hdr, resp_body) = self.send_recv(&mut packet).await?;
        let status = NtStatus::from_u32(resp_hdr.status);
        if status.is_error() {
            tracing::warn!(target: "smb", "close failed: 0x{:08X}", resp_hdr.status);
            return Err(io::Error::other(format!(
                "close failed: 0x{:08X}",
                resp_hdr.status
            )));
        }
        Ok(if postquery {
            decode_close_response(&resp_body)
        } else {
            None
        })
    }

    /// Read from an open file.
    pub async fn read(
        &self,
        tree_id: u32,
        file_id: &[u8; 16],
        offset: u64,
        length: u32,
    ) -> io::Result<bytes::Bytes> {
        let msg_id = self.next_message_id();
        let mut hdr = Header::new(Command::Read, msg_id).with_credit_charge(length);
        hdr.session_id = self.session_id;
        hdr.tree_id = tree_id;

        let mut packet = build_request(&hdr, |buf| {
            encode_read_request(buf, file_id, offset, length);
        });

        let (resp_hdr, resp_body) = self.send_recv(&mut packet).await?;
        let status = NtStatus::from_u32(resp_hdr.status);
        if status == NtStatus::EndOfFile {
            return Ok(bytes::Bytes::new());
        }
        if status.is_error() {
            tracing::warn!(target: "smb", "read failed: 0x{:08X}", resp_hdr.status);
            return Err(io::Error::other(format!(
                "read failed: 0x{:08X}",
                resp_hdr.status
            )));
        }

        decode_read_response_owned(resp_body).ok_or_else(|| {
            tracing::warn!(target: "smb", "invalid read response");
            io::Error::new(io::ErrorKind::InvalidData, "invalid read response")
        })
    }

    /// Pipelined read: send `count` read requests, then receive all responses.
    ///
    /// Holds the stream lock for the entire batch, eliminating per-request
    /// round-trip latency. Returns chunks in offset order. Stops early on EOF.
    pub async fn pipelined_read(
        &self,
        tree_id: u32,
        file_id: &[u8; 16],
        start_offset: u64,
        chunk_size: u32,
        count: usize,
    ) -> io::Result<Vec<bytes::Bytes>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let base_msg_id = self.message_id.fetch_add(count as u64, Ordering::Relaxed);

        let mut packets = Vec::with_capacity(count);
        for i in 0..count {
            let offset = start_offset + (i as u64) * (chunk_size as u64);
            let msg_id = base_msg_id + i as u64;
            let mut hdr = Header::new(Command::Read, msg_id).with_credit_charge(chunk_size);
            hdr.session_id = self.session_id;
            hdr.tree_id = tree_id;
            let packet = build_request(&hdr, |buf| {
                encode_read_request(buf, file_id, offset, chunk_size);
            });
            packets.push(packet);
        }

        if let Some(ref key) = self.signing_key {
            for packet in &mut packets {
                sign_packet(packet, key);
            }
        }

        let mut stream = self.stream.lock().await;
        for packet in &packets {
            stream.write_all(packet).await?;
        }
        stream.flush().await?;

        // Any early exit from the receive loop leaves unread response frames
        // in the TCP stream, so the connection is poisoned and the socket
        // shut down to prevent future operations reading stale frames.
        let mut slots: Vec<Option<bytes::Bytes>> = (0..count).map(|_| None).collect();
        let mut received = 0usize;
        let mut eof_after = count;
        let recv_result: io::Result<()> = async {
            while received < count {
                let mut len_buf = [0u8; 4];
                self.read_exact_timeout(&mut stream, &mut len_buf).await?;
                let msg_len = u32::from_be_bytes(len_buf) as usize;

                if !(SMB2_HEADER_SIZE..=16 * 1024 * 1024).contains(&msg_len) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid SMB2 message length: {msg_len}"),
                    ));
                }

                let mut msg = vec![0u8; msg_len];
                self.read_exact_timeout(&mut stream, &mut msg).await?;

                let header = Header::decode(&msg).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid SMB2 header")
                })?;

                if header.status == STATUS_PENDING {
                    continue;
                }

                // Verify the per-response CMAC before honoring its body. A
                // tampered or unsigned read response could otherwise hand a
                // caller bytes the server never returned (silent data
                // corruption on signed sessions).
                if let Some(ref key) = self.signing_key {
                    if header.flags & SMB2_FLAGS_SIGNED == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "pipelined read response missing signature after signing established",
                        ));
                    }
                    if !verify_signature(&mut msg, key) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "pipelined read response signature verification failed",
                        ));
                    }
                }

                let slot = (header.message_id.wrapping_sub(base_msg_id)) as usize;
                if slot >= count {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "unexpected message_id {} (base={base_msg_id}, count={count})",
                            header.message_id
                        ),
                    ));
                }

                let status = NtStatus::from_u32(header.status);
                if status == NtStatus::EndOfFile {
                    eof_after = eof_after.min(slot);
                    received += 1;
                    continue;
                }
                if status.is_error() {
                    return Err(io::Error::other(format!(
                        "pipelined read failed: 0x{:08X}",
                        header.status
                    )));
                }

                // Move the body out of `msg` without copying, preserving the
                // zero-copy intent of `decode_read_response_owned`.
                let body = msg.split_off(SMB2_HEADER_SIZE);
                let data = decode_read_response_owned(body).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid read response")
                })?;
                slots[slot] = Some(data);
                received += 1;
            }
            Ok(())
        }
        .await;

        if let Err(e) = recv_result {
            self.poison();
            let _ = stream.shutdown().await;
            return Err(e);
        }

        Ok(slots
            .into_iter()
            .take(eof_after)
            .map(Option::unwrap_or_default)
            .collect())
    }

    /// Write to an open file.
    pub async fn write(
        &self,
        tree_id: u32,
        file_id: &[u8; 16],
        offset: u64,
        data: &[u8],
    ) -> io::Result<u32> {
        let msg_id = self.next_message_id();
        let data_len = u32::try_from(data.len()).unwrap_or(u32::MAX);
        let mut hdr = Header::new(Command::Write, msg_id).with_credit_charge(data_len);
        hdr.session_id = self.session_id;
        hdr.tree_id = tree_id;

        let mut packet = build_request(&hdr, |buf| {
            encode_write_request(buf, file_id, offset, data);
        });

        let (resp_hdr, resp_body) = self.send_recv(&mut packet).await?;
        if resp_hdr.status & NT_STATUS_ERROR_MASK == NT_STATUS_ERROR_MASK {
            tracing::warn!(
                target: "smb",
                "write failed: 0x{:08X} offset={} len={}",
                resp_hdr.status,
                offset,
                data.len()
            );
            return Err(io::Error::other(format!(
                "write failed: status=0x{:08X} offset={} len={}",
                resp_hdr.status,
                offset,
                data.len()
            )));
        }

        decode_write_response(&resp_body)
            .ok_or_else(|| {
                tracing::warn!(target: "smb", "invalid write response");
                io::Error::new(io::ErrorKind::InvalidData, "invalid write response")
            })
            .and_then(|written| {
                // SMB2 WRITE replies can legally report a short count. Higher
                // levels (put_object, WAL flush) rely on the full buffer being
                // written; treat any mismatch as an error to avoid silent data
                // corruption.
                if usize::try_from(written).ok() == Some(data.len()) {
                    Ok(written)
                } else {
                    tracing::warn!(
                        target: "smb",
                        "short write: offset={offset} expected={} written={written}",
                        data.len()
                    );
                    Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!(
                            "short SMB write: offset={offset} expected={} written={written}",
                            data.len()
                        ),
                    ))
                }
            })
    }

    /// Pipelined write: send `chunks` write requests in a batch, then receive
    /// all responses. Returns total bytes written.
    pub async fn pipelined_write(
        &self,
        tree_id: u32,
        file_id: &[u8; 16],
        start_offset: u64,
        chunks: &[&[u8]],
    ) -> io::Result<u64> {
        if chunks.is_empty() {
            return Ok(0);
        }

        let n = chunks.len();
        let base_msg_id = self.message_id.fetch_add(n as u64, Ordering::Relaxed);

        let mut packets = Vec::with_capacity(n);
        let mut offset = start_offset;
        for (i, chunk) in chunks.iter().enumerate() {
            let msg_id = base_msg_id + i as u64;
            let chunk_len = u32::try_from(chunk.len()).unwrap_or(u32::MAX);
            let mut hdr = Header::new(Command::Write, msg_id).with_credit_charge(chunk_len);
            hdr.session_id = self.session_id;
            hdr.tree_id = tree_id;
            let packet = build_request(&hdr, |buf| {
                encode_write_request(buf, file_id, offset, chunk);
            });
            packets.push(packet);
            offset += chunk.len() as u64;
        }

        if let Some(ref key) = self.signing_key {
            for packet in &mut packets {
                sign_packet(packet, key);
            }
        }

        let mut stream = self.stream.lock().await;
        for packet in &packets {
            stream.write_all(packet).await?;
        }
        stream.flush().await?;

        // Track per-slot expected length so we can detect short writes and
        // out-of-batch responses.  Any early exit poisons the connection
        // because remaining frames would otherwise desynchronize framing.
        let expected: Vec<u32> = chunks
            .iter()
            .map(|c| u32::try_from(c.len()).unwrap_or(u32::MAX))
            .collect();
        let mut received = vec![false; n];
        let mut total_written = 0u64;
        let mut count = 0usize;
        let recv_result: io::Result<()> = async {
            while count < n {
                let mut len_buf = [0u8; 4];
                self.read_exact_timeout(&mut stream, &mut len_buf).await?;
                let msg_len = u32::from_be_bytes(len_buf) as usize;

                if !(SMB2_HEADER_SIZE..=16 * 1024 * 1024).contains(&msg_len) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid SMB2 message length: {msg_len}"),
                    ));
                }

                let mut msg = vec![0u8; msg_len];
                self.read_exact_timeout(&mut stream, &mut msg).await?;

                let header = Header::decode(&msg).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid SMB2 header")
                })?;

                if header.status == STATUS_PENDING {
                    continue;
                }

                if let Some(ref key) = self.signing_key {
                    if header.flags & SMB2_FLAGS_SIGNED == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "pipelined write response missing signature after signing established",
                        ));
                    }
                    if !verify_signature(&mut msg, key) {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "pipelined write response signature verification failed",
                        ));
                    }
                }

                let slot = (header.message_id.wrapping_sub(base_msg_id)) as usize;
                if slot >= n || received[slot] {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "unexpected message_id {} (base={base_msg_id}, n={n})",
                            header.message_id
                        ),
                    ));
                }

                if header.status & NT_STATUS_ERROR_MASK == NT_STATUS_ERROR_MASK {
                    return Err(io::Error::other(format!(
                        "pipelined write failed: 0x{:08X}",
                        header.status
                    )));
                }

                let body = &msg[SMB2_HEADER_SIZE..];
                let written = decode_write_response(body).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid write response")
                })?;
                if written != expected[slot] {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!(
                            "short pipelined write: slot={slot} expected={} written={written}",
                            expected[slot]
                        ),
                    ));
                }
                total_written += u64::from(written);
                received[slot] = true;
                count += 1;
            }
            Ok(())
        }
        .await;

        if let Err(e) = recv_result {
            self.poison();
            let _ = stream.shutdown().await;
            return Err(e);
        }

        Ok(total_written)
    }

    /// Rename a file using `SET_INFO` with `FileRenameInformation`.
    pub async fn rename(
        &self,
        tree_id: u32,
        file_id: &[u8; 16],
        new_path: &str,
        replace_if_exists: bool,
    ) -> io::Result<()> {
        let msg_id = self.next_message_id();
        let mut hdr = Header::new(Command::SetInfo, msg_id);
        hdr.session_id = self.session_id;
        hdr.tree_id = tree_id;

        let mut packet = build_request(&hdr, |buf| {
            encode_set_info_rename(buf, file_id, new_path, replace_if_exists);
        });

        let (resp_hdr, _) = self.send_recv(&mut packet).await?;
        if resp_hdr.status & NT_STATUS_ERROR_MASK == NT_STATUS_ERROR_MASK {
            tracing::warn!(
                target: "smb",
                "rename failed: 0x{:08X} -> {new_path}",
                resp_hdr.status
            );
            return Err(io::Error::other(format!(
                "rename failed: status=0x{:08X} -> {new_path}",
                resp_hdr.status
            )));
        }
        Ok(())
    }

    /// List directory contents.
    pub async fn query_directory(
        &self,
        tree_id: u32,
        file_id: &[u8; 16],
        pattern: &str,
    ) -> io::Result<Vec<DirectoryEntry>> {
        let mut all_entries = Vec::new();
        let mut first = true;

        loop {
            let msg_id = self.next_message_id();
            let mut hdr = Header::new(Command::QueryDirectory, msg_id);
            hdr.session_id = self.session_id;
            hdr.tree_id = tree_id;

            let restart = first;
            first = false;

            let mut packet = build_request(&hdr, |buf| {
                encode_query_directory_request(
                    buf,
                    file_id,
                    pattern,
                    FILE_ID_BOTH_DIRECTORY_INFORMATION,
                    restart,
                );
            });

            let (resp_hdr, resp_body) = self.send_recv(&mut packet).await?;
            let status = NtStatus::from_u32(resp_hdr.status);

            if status == NtStatus::NoMoreFiles {
                break;
            }
            if status.is_error() {
                tracing::warn!(
                    target: "smb",
                    "query directory failed: 0x{:08X}",
                    resp_hdr.status
                );
                return Err(io::Error::other(format!(
                    "query directory failed: 0x{:08X}",
                    resp_hdr.status
                )));
            }

            if resp_body.len() >= 9 {
                let buf_offset = (&resp_body[2..4] as &[u8]).get_u16_le() as usize;
                let buf_length = (&resp_body[4..8] as &[u8]).get_u32_le() as usize;
                // `resp_body` is the payload after the 64-byte SMB2 header,
                // so a `buf_offset` smaller than `SMB2_HEADER_SIZE` is
                // malformed. Reject the frame instead of silently slicing
                // from byte 0 (which would re-interpret response metadata
                // bytes as directory entries and hide real entries).
                let Some(start) = buf_offset.checked_sub(SMB2_HEADER_SIZE) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "query directory: malformed buf_offset 0x{buf_offset:04X} < SMB2 header size"
                        ),
                    ));
                };
                let Some(end) = start.checked_add(buf_length) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "query directory: buf_offset + buf_length overflow",
                    ));
                };
                if end > resp_body.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "query directory: declared buffer extends past response body",
                    ));
                }
                if start < end {
                    let entries = parse_directory_entries(&resp_body[start..end]);
                    all_entries.extend(entries);
                }
            }
        }

        Ok(all_entries)
    }

    // ── Compound operations ─────────────────────────────────────────────

    async fn send_compound(
        &self,
        requests: Vec<(Header, BytesMut)>,
    ) -> io::Result<Vec<(Header, Vec<u8>)>> {
        let n = requests.len();

        let sizes: Vec<usize> = requests
            .iter()
            .enumerate()
            .map(|(i, (_, body))| {
                let raw = SMB2_HEADER_SIZE + body.len();
                if i < n - 1 {
                    raw + (8 - raw % 8) % 8
                } else {
                    raw
                }
            })
            .collect();

        let total: usize = sizes.iter().sum();
        let mut buf = BytesMut::with_capacity(4 + total);
        let netbios_len = u32::try_from(total).unwrap_or(u32::MAX) & 0x00FF_FFFF;
        buf.put_u32(netbios_len);

        for (i, (mut header, body)) in requests.into_iter().enumerate() {
            let body_len = body.len();
            header.next_command = if i < n - 1 {
                u32::try_from(sizes[i]).unwrap_or(u32::MAX)
            } else {
                0
            };

            let msg_start = buf.len();
            header.encode(&mut buf);
            buf.put_slice(&body);

            let pad = sizes[i] - SMB2_HEADER_SIZE - body_len;
            if pad > 0 {
                buf.extend_from_slice(&[0u8; 7][..pad]);
            }

            if let Some(ref key) = self.signing_key {
                sign_message(&mut buf[msg_start..msg_start + sizes[i]], key);
            }
        }

        let mut stream = self.stream.lock().await;
        stream.write_all(&buf).await?;
        stream.flush().await?;

        loop {
            let mut len_buf = [0u8; 4];
            self.read_exact_timeout(&mut stream, &mut len_buf).await?;
            let msg_len = u32::from_be_bytes(len_buf) as usize;

            if !(SMB2_HEADER_SIZE..=16 * 1024 * 1024).contains(&msg_len) {
                tracing::warn!(target: "smb", "invalid message length: {msg_len}");
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid SMB2 message length: {msg_len}"),
                ));
            }

            let mut msg = vec![0u8; msg_len];
            self.read_exact_timeout(&mut stream, &mut msg).await?;

            if let Some(h) = Header::decode(&msg)
                && h.status == STATUS_PENDING
                && h.next_command == 0
            {
                continue;
            }

            // Verify the per-sub-message CMAC signatures on the compound
            // response. The send path signs each sub-message individually
            // (`sign_message` in the loop above); the receive path must
            // mirror that and reject any tampered or unsigned reply once
            // signing is established.
            if let Some(ref key) = self.signing_key
                && let Err(e) = verify_compound_signatures(&mut msg, key)
            {
                self.poison();
                let _ = stream.shutdown().await;
                return Err(e);
            }

            return Ok(parse_compound_response(&msg));
        }
    }

    /// Compound Create + Close (1 round trip).
    pub async fn create_close(
        &self,
        tree_id: u32,
        path: &str,
        desired_access: u32,
        share_access: u32,
        create_disposition: u32,
        create_options: u32,
    ) -> io::Result<(CreateResponse, CloseResponse)> {
        let base = self.message_id.fetch_add(2, Ordering::Relaxed);

        let mut h1 = Header::new(Command::Create, base);
        h1.session_id = self.session_id;
        h1.tree_id = tree_id;
        let mut b1 = BytesMut::with_capacity(128);
        encode_create_request(
            &mut b1,
            path,
            desired_access,
            share_access,
            create_disposition,
            create_options,
        );

        let mut h2 = Header::new(Command::Close, base + 1);
        h2.session_id = self.session_id;
        h2.tree_id = tree_id;
        h2.flags |= SMB2_FLAGS_RELATED;
        let mut b2 = BytesMut::with_capacity(32);
        encode_close_request_ex(&mut b2, &SENTINEL_FILE_ID, true);

        let resp = self.send_compound(vec![(h1, b1), (h2, b2)]).await?;
        if resp.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compound response too short",
            ));
        }

        if NtStatus::from_u32(resp[0].0.status).is_error() {
            return Err(smb_status_to_io_error(resp[0].0.status, path));
        }
        let cr = decode_create_response(&resp[0].1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid create response"))?;
        // Surface CLOSE failures as errors. Critical for callers that rely on
        // `DELETE_ON_CLOSE` (e.g. `delete_object`, WAL temp cleanup) — a
        // successful CREATE followed by a failed CLOSE means the deletion
        // never happened, and we must NOT report success in that case.
        if NtStatus::from_u32(resp[1].0.status).is_error() {
            tracing::warn!(
                target: "smb",
                "compound close failed: 0x{:08X}",
                resp[1].0.status
            );
            return Err(smb_status_to_io_error(resp[1].0.status, path));
        }
        let cl = decode_close_response(&resp[1].1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid close response in create_close",
            )
        })?;

        Ok((cr, cl))
    }

    /// Compound Create + Read + Close (1 round trip). For small-file reads.
    pub async fn create_read_close(
        &self,
        tree_id: u32,
        path: &str,
        max_read: u32,
    ) -> io::Result<(CreateResponse, bytes::Bytes)> {
        let base = self.message_id.fetch_add(3, Ordering::Relaxed);

        let mut h1 = Header::new(Command::Create, base);
        h1.session_id = self.session_id;
        h1.tree_id = tree_id;
        let mut b1 = BytesMut::with_capacity(128);
        encode_create_request(
            &mut b1,
            path,
            DesiredAccess::GenericRead as u32,
            crate::protocol::ShareAccess::All as u32,
            crate::protocol::CreateDisposition::Open as u32,
            crate::protocol::CreateOptions::NonDirectoryFile as u32,
        );

        let mut h2 = Header::new(Command::Read, base + 1).with_credit_charge(max_read);
        h2.session_id = self.session_id;
        h2.tree_id = tree_id;
        h2.flags |= SMB2_FLAGS_RELATED;
        let mut b2 = BytesMut::with_capacity(64);
        encode_read_request(&mut b2, &SENTINEL_FILE_ID, 0, max_read);

        let mut h3 = Header::new(Command::Close, base + 2);
        h3.session_id = self.session_id;
        h3.tree_id = tree_id;
        h3.flags |= SMB2_FLAGS_RELATED;
        let mut b3 = BytesMut::with_capacity(32);
        encode_close_request(&mut b3, &SENTINEL_FILE_ID);

        let resp = self
            .send_compound(vec![(h1, b1), (h2, b2), (h3, b3)])
            .await?;
        if resp.len() < 3 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compound response too short",
            ));
        }

        if NtStatus::from_u32(resp[0].0.status).is_error() {
            return Err(smb_status_to_io_error(resp[0].0.status, path));
        }
        let cr = decode_create_response(&resp[0].1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid create response"))?;

        let data = if NtStatus::from_u32(resp[1].0.status) == NtStatus::EndOfFile {
            bytes::Bytes::new()
        } else if NtStatus::from_u32(resp[1].0.status).is_error() {
            return Err(io::Error::other(format!(
                "read failed: 0x{:08X}",
                resp[1].0.status
            )));
        } else {
            decode_read_response(&resp[1].1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid read response")
            })?
        };

        // Surface CLOSE failures the same way `create_close`/`create_write_close`
        // do — otherwise a successful CREATE+READ followed by a failed CLOSE
        // would return Ok and leak one server-side handle per call, which under
        // long-lived read-heavy workloads accumulates until the server runs
        // out of handles.
        if NtStatus::from_u32(resp[2].0.status).is_error() {
            tracing::warn!(
                target: "smb",
                "create_read_close CLOSE failed: 0x{:08X}",
                resp[2].0.status
            );
            return Err(smb_status_to_io_error(resp[2].0.status, path));
        }

        Ok((cr, data))
    }

    /// Compound Create + Write + Close (1 round trip). For small-file writes.
    pub async fn create_write_close(
        &self,
        tree_id: u32,
        path: &str,
        data: &[u8],
    ) -> io::Result<CloseResponse> {
        let base = self.message_id.fetch_add(3, Ordering::Relaxed);

        let mut h1 = Header::new(Command::Create, base);
        h1.session_id = self.session_id;
        h1.tree_id = tree_id;
        let mut b1 = BytesMut::with_capacity(128);
        encode_create_request(
            &mut b1,
            path,
            DesiredAccess::GenericWrite as u32,
            crate::protocol::ShareAccess::Read as u32,
            crate::protocol::CreateDisposition::OverwriteIf as u32,
            crate::protocol::CreateOptions::NonDirectoryFile as u32,
        );

        let data_len = u32::try_from(data.len()).unwrap_or(u32::MAX);
        let mut h2 = Header::new(Command::Write, base + 1).with_credit_charge(data_len);
        h2.session_id = self.session_id;
        h2.tree_id = tree_id;
        h2.flags |= SMB2_FLAGS_RELATED;
        let mut b2 = BytesMut::with_capacity(64 + data.len());
        encode_write_request(&mut b2, &SENTINEL_FILE_ID, 0, data);

        let mut h3 = Header::new(Command::Close, base + 2);
        h3.session_id = self.session_id;
        h3.tree_id = tree_id;
        h3.flags |= SMB2_FLAGS_RELATED;
        let mut b3 = BytesMut::with_capacity(32);
        encode_close_request_ex(&mut b3, &SENTINEL_FILE_ID, true);

        let resp = self
            .send_compound(vec![(h1, b1), (h2, b2), (h3, b3)])
            .await?;
        if resp.len() < 3 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compound response too short",
            ));
        }

        if NtStatus::from_u32(resp[0].0.status).is_error() {
            return Err(smb_status_to_io_error(resp[0].0.status, path));
        }
        if resp[1].0.status & NT_STATUS_ERROR_MASK == NT_STATUS_ERROR_MASK {
            return Err(io::Error::other(format!(
                "write failed: 0x{:08X}",
                resp[1].0.status
            )));
        }
        // Validate the server-reported Count: SMB2 servers may legally return
        // a successful WRITE with a short byte count, and silently treating
        // that as success would corrupt small `put_object` payloads.
        let written = decode_write_response(&resp[1].1)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid write response"))?;
        if usize::try_from(written).ok() != Some(data.len()) {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "create_write_close short write: expected {} bytes but wrote {written}",
                    data.len()
                ),
            ));
        }

        // Surface CLOSE failures and missing post-query attrs as errors.
        // Falling back to fabricated metadata here means a small put could
        // return `Ok` even though the server-side close failed (leaking the
        // handle) or returned a malformed response (giving the caller a
        // bogus `last_write_time` / `file_size`).
        if NtStatus::from_u32(resp[2].0.status).is_error() {
            tracing::warn!(
                target: "smb",
                "create_write_close CLOSE failed: 0x{:08X}",
                resp[2].0.status
            );
            return Err(smb_status_to_io_error(resp[2].0.status, path));
        }
        decode_close_response(&resp[2].1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid close response in create_write_close",
            )
        })
    }

    /// Compound batch of Create+Close pairs for directory creation (1 round trip).
    pub async fn ensure_dirs(&self, tree_id: u32, dirs: &[String]) -> io::Result<()> {
        if dirs.is_empty() {
            return Ok(());
        }

        let count = dirs.len() * 2;
        let base = self.message_id.fetch_add(count as u64, Ordering::Relaxed);
        let mut requests = Vec::with_capacity(count);

        for (i, dir) in dirs.iter().enumerate() {
            let mut h1 = Header::new(Command::Create, base + (i as u64) * 2);
            h1.session_id = self.session_id;
            h1.tree_id = tree_id;
            let mut b1 = BytesMut::with_capacity(128);
            encode_create_request(
                &mut b1,
                dir,
                DesiredAccess::ReadAttributes as u32,
                crate::protocol::ShareAccess::All as u32,
                crate::protocol::CreateDisposition::OpenIf as u32,
                crate::protocol::CreateOptions::DirectoryFile as u32,
            );
            requests.push((h1, b1));

            let mut h2 = Header::new(Command::Close, base + (i as u64) * 2 + 1);
            h2.session_id = self.session_id;
            h2.tree_id = tree_id;
            h2.flags |= SMB2_FLAGS_RELATED;
            let mut b2 = BytesMut::with_capacity(32);
            encode_close_request(&mut b2, &SENTINEL_FILE_ID);
            requests.push((h2, b2));
        }

        let responses = self.send_compound(requests).await?;

        // Verify that the server returned every response we asked for. If a
        // truncated or malformed compound frame caused `parse_compound_response`
        // to stop early, we'd otherwise validate only the prefix and silently
        // declare success on un-created tail directories.
        let expected = dirs.len() * 2;
        if responses.len() != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "ensure_dirs: expected {expected} compound responses, got {}",
                    responses.len()
                ),
            ));
        }

        for i in (0..responses.len()).step_by(2) {
            let dir = &dirs[i / 2];
            let create_status = NtStatus::from_u32(responses[i].0.status);
            if create_status.is_error() {
                return Err(smb_status_to_io_error(responses[i].0.status, dir));
            }
            // The CLOSE half of each pair must also be checked. A failed
            // related CLOSE here leaks one server-side handle per
            // path segment, which under long-lived write-heavy sessions
            // can accumulate until the server runs out of handles.
            let close_resp = &responses[i + 1];
            let close_status = NtStatus::from_u32(close_resp.0.status);
            if close_status.is_error() {
                tracing::warn!(
                    target: "smb",
                    "ensure_dirs CLOSE failed for {dir}: 0x{:08X}",
                    close_resp.0.status,
                );
                return Err(smb_status_to_io_error(close_resp.0.status, dir));
            }
        }

        Ok(())
    }
}

/// Sign an SMB2 packet in-place. `packet` includes the 4-byte NetBIOS header.
/// Sets the `SMB2_FLAGS_SIGNED` bit and computes AES-128-CMAC over the SMB2 message.
fn sign_packet(packet: &mut [u8], key: &[u8; 16]) {
    const NETBIOS_HEADER: usize = 4;
    sign_message(&mut packet[NETBIOS_HEADER..], key);
}

/// Update preauth integrity hash: `hash = SHA-512(hash || message_bytes)`.
fn update_preauth_hash(hash: &mut [u8; 64], message: &[u8]) {
    let mut input = Vec::with_capacity(64 + message.len());
    input.extend_from_slice(hash);
    input.extend_from_slice(message);
    *hash = crypto::sha512(&input);
}

fn smb_status_to_io_error(status: u32, path: &str) -> io::Error {
    tracing::warn!(target: "smb", "error 0x{status:08X}: {path}");
    match status {
        0xC000_000F // STATUS_NO_SUCH_FILE
        | 0xC000_0034 // STATUS_OBJECT_NAME_NOT_FOUND
        | 0xC000_003A // STATUS_OBJECT_PATH_NOT_FOUND
        | 0xC000_0033 // STATUS_OBJECT_NAME_INVALID
        => io::Error::new(io::ErrorKind::NotFound, format!("not found: {path}")),

        0xC000_0103 => io::Error::new( // STATUS_NOT_A_DIRECTORY
            io::ErrorKind::NotADirectory,
            format!("not a directory: {path}"),
        ),

        0xC000_0022 => io::Error::new( // STATUS_ACCESS_DENIED
            io::ErrorKind::PermissionDenied,
            format!("access denied: {path}"),
        ),

        0xC000_0035 => io::Error::new( // STATUS_OBJECT_NAME_COLLISION
            io::ErrorKind::AlreadyExists,
            format!("already exists: {path}"),
        ),

        _ => io::Error::other(format!("SMB error 0x{status:08X} for {path}")),
    }
}

/// Sign a single SMB2 message in-place (no `NetBIOS` header prefix).
fn sign_message(msg: &mut [u8], key: &[u8; 16]) {
    const FLAGS_OFFSET: usize = 16;
    const SIGNATURE_OFFSET: usize = 48;

    let mut flag_bytes = [0u8; 4];
    flag_bytes.copy_from_slice(&msg[FLAGS_OFFSET..FLAGS_OFFSET + 4]);
    let flags = u32::from_le_bytes(flag_bytes);
    msg[FLAGS_OFFSET..FLAGS_OFFSET + 4].copy_from_slice(&(flags | SMB2_FLAGS_SIGNED).to_le_bytes());

    msg[SIGNATURE_OFFSET..SIGNATURE_OFFSET + 16].fill(0);

    let signature = crypto::aes128_cmac(key, msg);
    msg[SIGNATURE_OFFSET..SIGNATURE_OFFSET + 16].copy_from_slice(&signature);
}

/// Verify the AES-128-CMAC signature on a received SMB2 message in place.
///
/// Per [MS-SMB2] §3.2.5.1.3, the client extracts the 16-byte signature from
/// the SMB2 header, zeroes that field, recomputes the CMAC over the entire
/// message, and compares. The original signature is restored before
/// returning so the caller can reuse `msg` for downstream parsing.
///
/// Returns `true` when the recomputed CMAC matches the received signature.
fn verify_signature(msg: &mut [u8], key: &[u8; 16]) -> bool {
    const SIGNATURE_OFFSET: usize = 48;
    if msg.len() < SIGNATURE_OFFSET + 16 {
        return false;
    }

    let mut received = [0u8; 16];
    received.copy_from_slice(&msg[SIGNATURE_OFFSET..SIGNATURE_OFFSET + 16]);

    msg[SIGNATURE_OFFSET..SIGNATURE_OFFSET + 16].fill(0);
    let computed = crypto::aes128_cmac(key, msg);
    msg[SIGNATURE_OFFSET..SIGNATURE_OFFSET + 16].copy_from_slice(&received);

    received.as_slice() == &computed[..16]
}

/// Verify each sub-message of a compound SMB2 response. The send path
/// signs every sub-message individually before transmission, so the
/// receive path must verify each in turn. Walks `next_command` offsets to
/// find sub-message boundaries; rejects any sub-message that is missing
/// the SIGNED flag or whose CMAC does not match the included signature.
fn verify_compound_signatures(msg: &mut [u8], key: &[u8; 16]) -> io::Result<()> {
    let mut offset = 0usize;
    loop {
        if offset + SMB2_HEADER_SIZE > msg.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compound response: sub-message truncated before SMB2 header",
            ));
        }
        let header = Header::decode(&msg[offset..]).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "compound response: invalid SMB2 header",
            )
        })?;

        let next = header.next_command as usize;
        let sub_end = if next > 0 {
            let end = offset.checked_add(next).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "compound response: next_command overflow",
                )
            })?;
            if end <= offset || end > msg.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "compound response: next_command points outside frame",
                ));
            }
            end
        } else {
            msg.len()
        };

        if header.flags & SMB2_FLAGS_SIGNED == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compound response: sub-message missing signature after signing established",
            ));
        }
        if !verify_signature(&mut msg[offset..sub_end], key) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "compound response: sub-message signature verification failed",
            ));
        }

        if next == 0 {
            return Ok(());
        }
        offset = sub_end;
    }
}

/// Parse a compound response (multiple SMB2 messages in one frame).
fn parse_compound_response(msg: &[u8]) -> Vec<(Header, Vec<u8>)> {
    let mut results = Vec::new();
    let mut offset = 0;

    loop {
        if offset + SMB2_HEADER_SIZE > msg.len() {
            break;
        }
        let Some(header) = Header::decode(&msg[offset..]) else {
            break;
        };

        let next = header.next_command as usize;
        let body_start = offset + SMB2_HEADER_SIZE;
        let body_end = if next > 0 {
            let end = offset + next;
            if end > msg.len() || end < body_start {
                break;
            }
            end
        } else {
            msg.len()
        };
        if body_start > body_end || body_end > msg.len() {
            break;
        }

        let body = msg[body_start..body_end].to_vec();
        results.push((header, body));

        if next == 0 {
            break;
        }
        offset += next;
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    const STATUS_MORE_PROCESSING_REQUIRED: u32 = 0xC000_0016;
    const SMB2_FLAGS_RESPONSE: u32 = 0x0000_0001;
    const TEST_USERNAME: &str = "spicetester";
    const TEST_PASSWORD: &str = "s3cret-pw!";
    const TEST_DOMAIN: &str = "WORKGROUP";
    const TEST_SESSION_ID: u64 = 0x0102_0304_0506_0708;
    const TEST_TREE_ID: u32 = 99;

    /// How the mock server deviates (or not) from a well-behaved server.
    #[derive(Clone, Copy)]
    enum TreeConnectBehavior {
        /// Sign every response with the spec-derived key (well-behaved server).
        SignCorrectly,
        /// Sign the TREE_CONNECT response, then flip a bit in its signature.
        CorruptSignature,
        /// Send the TREE_CONNECT response without the SIGNED flag or signature.
        OmitSignature,
        /// Corrupt the signature on the final (STATUS_SUCCESS) SESSION_SETUP
        /// response and stop — the client must abort before TREE_CONNECT.
        CorruptFinalSetupSignature,
    }

    async fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .expect("mock server: read frame length");
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut msg = vec![0u8; len];
        stream
            .read_exact(&mut msg)
            .await
            .expect("mock server: read frame body");
        msg
    }

    async fn write_frame(stream: &mut TcpStream, msg: &[u8]) {
        let len = u32::try_from(msg.len()).expect("mock server: frame fits in u32");
        stream
            .write_all(&len.to_be_bytes())
            .await
            .expect("mock server: write frame length");
        stream
            .write_all(msg)
            .await
            .expect("mock server: write frame body");
        stream.flush().await.expect("mock server: flush");
    }

    /// Build a response message (SMB2 header + body, no NetBIOS prefix).
    /// Echoes the request's command and message id, as a real server does.
    fn build_response(req: &Header, status: u32, session_id: u64, body: &[u8]) -> Vec<u8> {
        let mut hdr = req.clone();
        hdr.status = status;
        hdr.flags = SMB2_FLAGS_RESPONSE;
        hdr.session_id = session_id;
        hdr.tree_id = TEST_TREE_ID;
        let mut buf = BytesMut::with_capacity(SMB2_HEADER_SIZE + body.len());
        hdr.encode(&mut buf);
        buf.put_slice(body);
        buf.to_vec()
    }

    /// Minimal SMB 3.1.1 NEGOTIATE response body (fields the client reads).
    fn negotiate_response_body() -> Vec<u8> {
        let mut body = vec![0u8; 64];
        body[0] = 65; // StructureSize
        body[2] = 0x01; // SecurityMode: signing enabled
        body[4..6].copy_from_slice(&DIALECT_SMB3_1_1.to_le_bytes());
        body[28..32].copy_from_slice(&65536u32.to_le_bytes()); // MaxTransactSize
        body[32..36].copy_from_slice(&65536u32.to_le_bytes()); // MaxReadSize
        body[36..40].copy_from_slice(&65536u32.to_le_bytes()); // MaxWriteSize
        body
    }

    /// Raw NTLM CHALLENGE (Type 2) message with a minimal target info.
    fn ntlm_challenge_message(server_challenge: [u8; 8]) -> Vec<u8> {
        // UNICODE | REQUEST_TARGET | NTLM | EXTENDED_SESSIONSECURITY | TARGET_INFO
        let flags: u32 = 0x0088_0205;
        let target_info = [0u8; 4]; // single MsvAvEOL AvPair
        let ti_offset = 56u32; // right after the 8-byte Version field
        let mut msg = Vec::with_capacity(60);
        msg.extend_from_slice(b"NTLMSSP\0");
        msg.extend_from_slice(&2u32.to_le_bytes()); // MessageType
        msg.extend_from_slice(&[0u8; 8]); // TargetNameFields (empty)
        msg.extend_from_slice(&flags.to_le_bytes());
        msg.extend_from_slice(&server_challenge);
        msg.extend_from_slice(&[0u8; 8]); // Reserved
        let ti_len = u16::try_from(target_info.len()).expect("test fixture");
        msg.extend_from_slice(&ti_len.to_le_bytes());
        msg.extend_from_slice(&ti_len.to_le_bytes());
        msg.extend_from_slice(&ti_offset.to_le_bytes());
        msg.extend_from_slice(&[0u8; 8]); // Version
        msg.extend_from_slice(&target_info);
        msg
    }

    /// SESSION_SETUP response body carrying `blob` as the security buffer.
    fn session_setup_response_body(blob: &[u8]) -> Vec<u8> {
        let mut body = Vec::with_capacity(8 + blob.len());
        body.extend_from_slice(&9u16.to_le_bytes()); // StructureSize
        body.extend_from_slice(&0u16.to_le_bytes()); // SessionFlags
        // SecurityBufferOffset is from the start of the SMB2 header.
        let offset = u16::try_from(SMB2_HEADER_SIZE + 8).expect("test fixture");
        body.extend_from_slice(&offset.to_le_bytes());
        let blob_len = u16::try_from(blob.len()).expect("test fixture");
        body.extend_from_slice(&blob_len.to_le_bytes());
        body.extend_from_slice(blob);
        body
    }

    /// Extract the NTLMSSP security blob from a SESSION_SETUP request message.
    fn session_setup_security_blob(msg: &[u8]) -> &[u8] {
        let body = &msg[SMB2_HEADER_SIZE..];
        let offset = u16::from_le_bytes([body[12], body[13]]) as usize;
        let len = u16::from_le_bytes([body[14], body[15]]) as usize;
        &msg[offset..offset + len]
    }

    /// Recompute the NTLMv2 session base key the way a real server does:
    /// from the stored credentials plus the NTProofStr sent by the client.
    fn server_session_base_key(auth_blob: &[u8]) -> [u8; 16] {
        let ntlmssp = auth::unwrap_spnego(auth_blob);
        // NtChallengeResponseFields live at offset 20 (Len u16, MaxLen u16, Offset u32).
        let nt_offset =
            u32::from_le_bytes(ntlmssp[24..28].try_into().expect("test fixture")) as usize;
        let mut nt_proof = [0u8; 16];
        nt_proof.copy_from_slice(&ntlmssp[nt_offset..nt_offset + 16]);

        let password_utf16: Vec<u8> = TEST_PASSWORD
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        let nt_hash = crypto::md4(&password_utf16);
        let user_domain = format!("{}{}", TEST_USERNAME.to_uppercase(), TEST_DOMAIN);
        let ud_utf16: Vec<u8> = user_domain
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        let ntlmv2_hash = crypto::hmac_md5(&nt_hash, &ud_utf16);
        crypto::hmac_md5(&ntlmv2_hash, &nt_proof)
    }

    /// Drive a spec-compliant SMB 3.1.1 negotiate + NTLM session setup +
    /// TREE_CONNECT exchange. Maintains the preauth integrity hash exactly as
    /// [MS-SMB2] prescribes for a server: negotiate request/response and
    /// session-setup messages are hashed, but the final SESSION_SETUP
    /// response (STATUS_SUCCESS) is *not* — it is itself signed with the
    /// derived key, so the key cannot depend on it.
    ///
    /// Returns whether the client's signed TREE_CONNECT request verified
    /// under the spec-derived signing key.
    async fn run_mock_server(listener: TcpListener, behavior: TreeConnectBehavior) -> bool {
        let (mut stream, _) = listener.accept().await.expect("mock server: accept");
        let mut preauth = [0u8; 64];

        // ── NEGOTIATE ──
        let req = read_frame(&mut stream).await;
        let req_hdr = Header::decode(&req).expect("mock server: negotiate header");
        assert_eq!(req_hdr.command, Command::Negotiate as u16);
        update_preauth_hash(&mut preauth, &req);
        let resp = build_response(&req_hdr, 0, 0, &negotiate_response_body());
        update_preauth_hash(&mut preauth, &resp);
        write_frame(&mut stream, &resp).await;

        // ── SESSION_SETUP #1 (NTLM NEGOTIATE → CHALLENGE) ──
        let req = read_frame(&mut stream).await;
        let req_hdr = Header::decode(&req).expect("mock server: session setup 1 header");
        assert_eq!(req_hdr.command, Command::SessionSetup as u16);
        update_preauth_hash(&mut preauth, &req);
        let challenge = ntlm_challenge_message(*b"SrvChal8");
        let resp = build_response(
            &req_hdr,
            STATUS_MORE_PROCESSING_REQUIRED,
            TEST_SESSION_ID,
            &session_setup_response_body(&challenge),
        );
        update_preauth_hash(&mut preauth, &resp);
        write_frame(&mut stream, &resp).await;

        // ── SESSION_SETUP #2 (NTLM AUTH → SUCCESS) ──
        let req = read_frame(&mut stream).await;
        let req_hdr = Header::decode(&req).expect("mock server: session setup 2 header");
        assert_eq!(req_hdr.command, Command::SessionSetup as u16);
        update_preauth_hash(&mut preauth, &req);

        // Key derivation happens HERE: the transcript ends with the final
        // session-setup request. The success response below is excluded.
        let session_base_key = server_session_base_key(session_setup_security_blob(&req));
        let signing_key = auth::derive_signing_key(&session_base_key, &preauth);

        let mut resp = build_response(
            &req_hdr,
            0,
            TEST_SESSION_ID,
            &session_setup_response_body(&[]),
        );
        // A real server signs the final session-setup response with the key
        // it just derived — proof that the key cannot include this response.
        sign_message(&mut resp, &signing_key);
        if matches!(behavior, TreeConnectBehavior::CorruptFinalSetupSignature) {
            resp[48] ^= 0xFF; // flip bits in the first signature byte
            write_frame(&mut stream, &resp).await;
            // The client rejects this response and never sends TREE_CONNECT.
            return false;
        }
        write_frame(&mut stream, &resp).await;

        // ── TREE_CONNECT (first signed request/response exchange) ──
        let mut req = read_frame(&mut stream).await;
        let req_hdr = Header::decode(&req).expect("mock server: tree connect header");
        assert_eq!(req_hdr.command, Command::TreeConnect as u16);
        let client_signed = req_hdr.flags & SMB2_FLAGS_SIGNED != 0;
        let client_sig_ok = client_signed && verify_signature(&mut req, &signing_key);

        let mut tree_body = vec![0u8; 16];
        tree_body[0] = 16; // StructureSize
        tree_body[2] = 1; // ShareType: disk
        let mut resp = build_response(&req_hdr, 0, TEST_SESSION_ID, &tree_body);
        match behavior {
            // CorruptFinalSetupSignature returned above, before TREE_CONNECT.
            TreeConnectBehavior::SignCorrectly
            | TreeConnectBehavior::CorruptFinalSetupSignature => {
                sign_message(&mut resp, &signing_key);
            }
            TreeConnectBehavior::CorruptSignature => {
                sign_message(&mut resp, &signing_key);
                resp[48] ^= 0xFF; // flip bits in the first signature byte
            }
            TreeConnectBehavior::OmitSignature => {}
        }
        write_frame(&mut stream, &resp).await;

        client_sig_ok
    }

    async fn connect_client(port: u16) -> io::Result<Arc<SmbClient>> {
        SmbClient::connect(SmbConfig {
            server: "127.0.0.1".to_string(),
            port,
            username: TEST_USERNAME.to_string(),
            password: TEST_PASSWORD.to_string(),
            domain: TEST_DOMAIN.to_string(),
            workstation: "TESTWS".to_string(),
            max_io_size: 0,
            read_timeout: Some(Duration::from_secs(5)),
        })
        .await
    }

    /// Run the full handshake plus one TREE_CONNECT against the mock server.
    /// Returns the tree-connect result and whether the client's signed
    /// TREE_CONNECT request verified under the spec-derived signing key.
    async fn tree_connect_against(behavior: TreeConnectBehavior) -> (io::Result<u32>, bool) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let port = listener.local_addr().expect("mock server addr").port();
        let server = tokio::spawn(run_mock_server(listener, behavior));

        let client = connect_client(port)
            .await
            .expect("negotiate + NTLM session setup should succeed");
        let result = client.tree_connect("data").await;
        let client_sig_ok = server.await.expect("mock server task");
        (result, client_sig_ok)
    }

    /// Regression test for #11148: the signing key must be derived from a
    /// preauth integrity hash that ends with the final SESSION_SETUP
    /// *request*. Hashing the final (STATUS_SUCCESS) response too — as the
    /// original port of this client did — derives a key that disagrees with
    /// every spec-compliant server (Samba, Windows), and the first signed
    /// exchange after auth fails with "signature verification failed".
    #[tokio::test]
    async fn signing_key_matches_spec_compliant_server() {
        let (result, client_sig_ok) =
            tree_connect_against(TreeConnectBehavior::SignCorrectly).await;
        let tree_id =
            result.expect("signed TREE_CONNECT should verify against a spec-compliant server");
        assert_eq!(tree_id, TEST_TREE_ID);
        assert!(
            client_sig_ok,
            "client's signed TREE_CONNECT request must verify under the spec-derived signing key"
        );
    }

    /// Negative control: a tampered response signature must be rejected —
    /// proves the positive test above cannot pass vacuously (i.e. the client
    /// really does verify response signatures with its derived key).
    #[tokio::test]
    async fn tampered_response_signature_is_rejected() {
        let (result, client_sig_ok) =
            tree_connect_against(TreeConnectBehavior::CorruptSignature).await;
        let err = result.expect_err("tampered response signature must be rejected");
        assert!(
            err.to_string().contains("signature verification failed"),
            "unexpected error: {err}"
        );
        assert!(
            client_sig_ok,
            "client request signature should still verify"
        );
    }

    /// Negative control: the final (STATUS_SUCCESS) SESSION_SETUP response is
    /// signed by the server with the freshly derived key; a tampered
    /// signature there must abort the session before any operation runs.
    #[tokio::test]
    async fn tampered_final_session_setup_signature_is_rejected() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let port = listener.local_addr().expect("mock server addr").port();
        let server = tokio::spawn(run_mock_server(
            listener,
            TreeConnectBehavior::CorruptFinalSetupSignature,
        ));

        let Err(err) = connect_client(port).await else {
            panic!("tampered final SESSION_SETUP signature must abort the session")
        };
        assert!(
            err.to_string()
                .contains("final SESSION_SETUP response signature verification failed"),
            "unexpected error: {err}"
        );
        let _ = server.await.expect("mock server task");
    }

    /// Negative control: once signing is established, an unsigned response
    /// must be rejected.
    #[tokio::test]
    async fn unsigned_response_after_auth_is_rejected() {
        let (result, client_sig_ok) =
            tree_connect_against(TreeConnectBehavior::OmitSignature).await;
        let err = result.expect_err("unsigned response after signing established must be rejected");
        assert!(
            err.to_string().contains("missing signature"),
            "unexpected error: {err}"
        );
        assert!(
            client_sig_ok,
            "client request signature should still verify"
        );
    }
}
