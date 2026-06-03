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

//! SMB 3.1.1 wire protocol definitions.
//!
//! All structures are little-endian on the wire. Ported from spiceio.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::crypto;

// ── SMB2 magic ──────────────────────────────────────────────────────────────

pub const SMB2_MAGIC: &[u8; 4] = b"\xfeSMB";
pub const SMB2_HEADER_SIZE: usize = 64;

// Encoded buffer offsets within an SMB2 request message. Each is the fixed
// offset from the start of the message at which variable-length payload
// data begins.
const SESSION_SETUP_SEC_BUF_OFFSET: u16 = (SMB2_HEADER_SIZE + 24) as u16;
const TREE_CONNECT_PATH_OFFSET: u16 = (SMB2_HEADER_SIZE + 8) as u16;
const CREATE_NAME_OFFSET: u16 = (SMB2_HEADER_SIZE + 56) as u16;
const WRITE_DATA_OFFSET: u16 = (SMB2_HEADER_SIZE + 48) as u16;
const SET_INFO_BUFFER_OFFSET: u16 = (SMB2_HEADER_SIZE + 32) as u16;
const QUERY_DIR_NAME_OFFSET: u16 = (SMB2_HEADER_SIZE + 32) as u16;

// ── Commands ────────────────────────────────────────────────────────────────

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Negotiate = 0x0000,
    SessionSetup = 0x0001,
    TreeConnect = 0x0003,
    Create = 0x0005,
    Close = 0x0006,
    Read = 0x0008,
    Write = 0x0009,
    QueryDirectory = 0x000E,
    SetInfo = 0x0011,
}

// ── NT Status codes we care about ───────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NtStatus {
    Success,
    MoreProcessingRequired,
    NoSuchFile,
    ObjectNameNotFound,
    ObjectNameCollision,
    AccessDenied,
    EndOfFile,
    NoMoreFiles,
    ObjectPathNotFound,
    Unknown(u32),
}

impl NtStatus {
    #[must_use]
    pub fn from_u32(v: u32) -> Self {
        match v {
            0x0000_0000 => Self::Success,
            0xC000_0016 => Self::MoreProcessingRequired,
            0xC000_000F => Self::NoSuchFile,
            0xC000_0034 => Self::ObjectNameNotFound,
            0xC000_0035 => Self::ObjectNameCollision,
            0xC000_0022 => Self::AccessDenied,
            0xC000_0011 => Self::EndOfFile,
            0x8000_0006 => Self::NoMoreFiles,
            0xC000_003A => Self::ObjectPathNotFound,
            other => Self::Unknown(other),
        }
    }

    #[must_use]
    pub fn is_error(self) -> bool {
        let code = match self {
            Self::Success => 0x0000_0000,
            Self::MoreProcessingRequired => 0xC000_0016,
            Self::NoSuchFile => 0xC000_000F,
            Self::ObjectNameNotFound => 0xC000_0034,
            Self::ObjectNameCollision => 0xC000_0035,
            Self::AccessDenied => 0xC000_0022,
            Self::EndOfFile => 0xC000_0011,
            Self::NoMoreFiles => 0x8000_0006,
            Self::ObjectPathNotFound => 0xC000_003A,
            Self::Unknown(v) => v,
        };
        code & 0xC000_0000 == 0xC000_0000
    }
}

// ── SMB2 Header ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Header {
    pub command: u16,
    pub credit_charge: u16,
    pub status: u32,
    pub credits_requested: u16,
    pub flags: u32,
    pub next_command: u32,
    pub message_id: u64,
    pub tree_id: u32,
    pub session_id: u64,
}

impl Header {
    #[must_use]
    pub fn new(command: Command, message_id: u64) -> Self {
        Self {
            command: command as u16,
            credit_charge: 1,
            status: 0,
            credits_requested: 256,
            flags: 0,
            next_command: 0,
            message_id,
            tree_id: 0,
            session_id: 0,
        }
    }

    /// Set credit charge for operations transferring `payload_size` bytes.
    /// Required for Read/Write/QueryDirectory with payloads >64KB.
    #[must_use]
    pub fn with_credit_charge(mut self, payload_size: u32) -> Self {
        self.credit_charge = credit_charge_for(payload_size);
        self
    }

    /// Encode the 64-byte SMB2 header into a buffer.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(SMB2_MAGIC); // 0: ProtocolId
        buf.put_u16_le(64); // 4: StructureSize
        buf.put_u16_le(self.credit_charge); // 6: CreditCharge
        buf.put_u32_le(self.status); // 8: Status
        buf.put_u16_le(self.command); // 12: Command
        buf.put_u16_le(self.credits_requested); // 14: CreditRequest
        buf.put_u32_le(self.flags); // 16: Flags
        buf.put_u32_le(self.next_command); // 20: NextCommand
        buf.put_u64_le(self.message_id); // 24: MessageID
        buf.put_u32_le(0); // 32: Reserved (async: AsyncId high)
        buf.put_u32_le(self.tree_id); // 36: TreeId (sync)
        buf.put_u64_le(self.session_id); // 40: SessionId
        buf.put_slice(&[0u8; 16]); // 48: Signature
    }

    /// Decode a 64-byte SMB2 header from bytes.
    #[must_use]
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < SMB2_HEADER_SIZE {
            return None;
        }
        let magic = &buf[..4];
        if magic != SMB2_MAGIC {
            return None;
        }
        let buf = &buf[4..];
        let _structure_size = (&buf[..2]).get_u16_le();
        let buf = &buf[2..];
        let credit_charge = (&buf[..2]).get_u16_le();
        let status = (&buf[2..6]).get_u32_le();
        let command = (&buf[6..8]).get_u16_le();
        let credits_requested = (&buf[8..10]).get_u16_le();
        let flags = (&buf[10..14]).get_u32_le();
        let next_command = (&buf[14..18]).get_u32_le();
        let message_id = (&buf[18..26]).get_u64_le();
        let _reserved = (&buf[26..30]).get_u32_le();
        let tree_id = (&buf[30..34]).get_u32_le();
        let session_id = (&buf[34..42]).get_u64_le();
        // signature at 42..58 — skip for now

        Some(Self {
            command,
            credit_charge,
            status,
            credits_requested,
            flags,
            next_command,
            message_id,
            tree_id,
            session_id,
        })
    }
}

/// Compute the credit charge for a payload of the given size.
/// `CreditCharge = max(1, ceil(payload_size / 65536))`
#[must_use]
pub fn credit_charge_for(payload_size: u32) -> u16 {
    1.max(u16::try_from(payload_size.div_ceil(65536)).unwrap_or(u16::MAX))
}

// ── Negotiate ───────────────────────────────────────────────────────────────

/// SMB 3.1.1 dialect (the only dialect we implement: signing-key derivation
/// and the preauth integrity context shape are 3.1.1-specific).
pub const DIALECT_SMB3_1_1: u16 = 0x0311;

// Only 3.1.1 is offered. We must NOT advertise 3.0.x because subsequent
// signing-key derivation assumes the 3.1.1 preauth-integrity-hash context
// (see [MS-SMB2] 3.1.4.5.1). Mismatched dialects would produce incorrect
// signing keys, breaking the session immediately after auth.
const DIALECTS: [u16; 1] = [DIALECT_SMB3_1_1];

// Negotiate context types for SMB 3.1.1
const SMB2_PREAUTH_INTEGRITY_CAPABILITIES: u16 = 0x0001;
const SMB2_ENCRYPTION_CAPABILITIES: u16 = 0x0002;

// Hash algorithm: SHA-512
const SHA_512: u16 = 0x0001;
// Cipher preferences
const AES_128_GCM: u16 = 0x0002;
const AES_128_CCM: u16 = 0x0001;

pub fn encode_negotiate_request(buf: &mut BytesMut, client_guid: &[u8; 16]) {
    let dialect_count = u16::try_from(DIALECTS.len()).unwrap_or(u16::MAX);
    let dialects_len = DIALECTS.len() * 2;

    // Build negotiate contexts (required when offering 3.1.1)
    let mut contexts = BytesMut::new();

    // Preauth Integrity Capabilities context
    let preauth_data_len: u16 = 2 + 2 + 2 + 32;
    contexts.put_u16_le(SMB2_PREAUTH_INTEGRITY_CAPABILITIES);
    contexts.put_u16_le(preauth_data_len);
    contexts.put_u32_le(0); // Reserved
    contexts.put_u16_le(1); // HashAlgorithmCount
    contexts.put_u16_le(32); // SaltLength
    contexts.put_u16_le(SHA_512);
    let mut salt = [0u8; 32];
    crypto::random_bytes(&mut salt);
    contexts.put_slice(&salt);
    // Pad to 8-byte alignment
    let pad = (8 - (contexts.len() % 8)) % 8;
    contexts.put_bytes(0, pad);

    // Encryption Capabilities context
    let enc_data_len: u16 = 2 + 2 * 2;
    contexts.put_u16_le(SMB2_ENCRYPTION_CAPABILITIES);
    contexts.put_u16_le(enc_data_len);
    contexts.put_u32_le(0); // Reserved
    contexts.put_u16_le(2); // CipherCount
    contexts.put_u16_le(AES_128_GCM);
    contexts.put_u16_le(AES_128_CCM);

    let body_fixed_len = 36 + dialects_len;
    let ctx_padding = (8 - (body_fixed_len % 8)) % 8;
    // SMB2 header + fixed body (36 + 2*N dialects) + padding — always under u32 range.
    let ctx_offset = (SMB2_HEADER_SIZE + body_fixed_len + ctx_padding) as u32;

    buf.put_u16_le(36); // StructureSize
    buf.put_u16_le(dialect_count);
    buf.put_u16_le(0x0001); // SecurityMode: signing enabled
    buf.put_u16_le(0); // Reserved
    buf.put_u32_le(0x0000_0041); // Capabilities: DFS | Leasing
    buf.put_slice(client_guid);
    buf.put_u32_le(ctx_offset); // NegotiateContextOffset
    buf.put_u16_le(2); // NegotiateContextCount
    buf.put_u16_le(0); // Reserved2
    for &d in &DIALECTS {
        buf.put_u16_le(d);
    }
    buf.put_bytes(0, ctx_padding);
    buf.put_slice(&contexts);
}

#[derive(Debug)]
pub struct NegotiateResponse {
    pub security_mode: u16,
    pub dialect_revision: u16,
    pub max_transact_size: u32,
    pub max_read_size: u32,
    pub max_write_size: u32,
}

#[must_use]
pub fn decode_negotiate_response(body: &[u8]) -> Option<NegotiateResponse> {
    if body.len() < 40 {
        return None;
    }
    let security_mode = (&body[2..4]).get_u16_le();
    let dialect_revision = (&body[4..6]).get_u16_le();
    let max_transact_size = (&body[28..32]).get_u32_le();
    let max_read_size = (&body[32..36]).get_u32_le();
    let max_write_size = (&body[36..40]).get_u32_le();

    Some(NegotiateResponse {
        security_mode,
        dialect_revision,
        max_transact_size,
        max_read_size,
        max_write_size,
    })
}

// ── Session Setup ───────────────────────────────────────────────────────────

pub fn encode_session_setup_request(buf: &mut BytesMut, security_blob: &[u8]) {
    let offset = SESSION_SETUP_SEC_BUF_OFFSET;
    let blob_len = u16::try_from(security_blob.len()).unwrap_or(u16::MAX);
    buf.put_u16_le(25); // StructureSize
    buf.put_u8(0); // Flags
    buf.put_u8(0x01); // SecurityMode: signing enabled
    buf.put_u32_le(0); // Capabilities
    buf.put_u32_le(0); // Channel
    buf.put_u16_le(offset); // SecurityBufferOffset
    buf.put_u16_le(blob_len); // SecurityBufferLength
    buf.put_u64_le(0); // PreviousSessionId
    buf.put_slice(security_blob);
}

#[derive(Debug)]
pub struct SessionSetupResponse {
    pub session_id: u64,
    pub security_buffer: Bytes,
}

#[must_use]
pub fn decode_session_setup_response(header: &Header, body: &[u8]) -> Option<SessionSetupResponse> {
    if body.len() < 9 {
        return None;
    }
    let security_buffer_offset = (&body[4..6]).get_u16_le() as usize;
    let security_buffer_length = (&body[6..8]).get_u16_le() as usize;

    // Reject malformed responses where the declared security buffer extends
    // past the body. Returning Some with an empty buffer would push the
    // failure downstream (typically as an opaque NTLM parse error).
    let sec_start = security_buffer_offset.saturating_sub(SMB2_HEADER_SIZE);
    let sec_end = sec_start.checked_add(security_buffer_length)?;
    if sec_end > body.len() {
        return None;
    }
    let security_buffer = Bytes::copy_from_slice(&body[sec_start..sec_end]);

    Some(SessionSetupResponse {
        session_id: header.session_id,
        security_buffer,
    })
}

// ── Tree Connect ────────────────────────────────────────────────────────────

pub fn encode_tree_connect_request(buf: &mut BytesMut, path: &str) {
    let path_bytes: Vec<u8> = path.encode_utf16().flat_map(u16::to_le_bytes).collect();
    let offset = TREE_CONNECT_PATH_OFFSET;
    let path_len = u16::try_from(path_bytes.len()).unwrap_or(u16::MAX);
    buf.put_u16_le(9); // StructureSize
    buf.put_u16_le(0); // Reserved / Flags
    buf.put_u16_le(offset); // PathOffset
    buf.put_u16_le(path_len); // PathLength
    buf.put_slice(&path_bytes);
}

// ── Create (Open) ───────────────────────────────────────────────────────────

#[repr(u32)]
#[derive(Debug, Clone, Copy)]
pub enum DesiredAccess {
    GenericRead = 0x8000_0000,
    GenericWrite = 0x4000_0000,
    Delete = 0x0001_0000,
    ReadAttributes = 0x0000_0080,
}

#[repr(u32)]
#[derive(Debug, Clone, Copy)]
pub enum ShareAccess {
    Read = 0x0000_0001,
    Delete = 0x0000_0004,
    All = 0x0000_0007,
}

#[repr(u32)]
#[derive(Debug, Clone, Copy)]
pub enum CreateDisposition {
    Open = 0x0000_0001,
    /// FILE_CREATE — atomically fails with `STATUS_OBJECT_NAME_COLLISION`
    /// if the target already exists. Use for create-exclusive semantics
    /// (e.g. `PutMode::Create`) to avoid a TOCTOU between `head` and write.
    Create = 0x0000_0002,
    OpenIf = 0x0000_0003,
    OverwriteIf = 0x0000_0005,
}

#[repr(u32)]
#[derive(Debug, Clone, Copy)]
pub enum CreateOptions {
    DirectoryFile = 0x0000_0001,
    NonDirectoryFile = 0x0000_0040,
}

/// Bit that adds `DELETE_ON_CLOSE` to a `CreateOptions` value.
pub const CREATE_OPTION_DELETE_ON_CLOSE: u32 = 0x0000_1000;

pub fn encode_create_request(
    buf: &mut BytesMut,
    path: &str,
    desired_access: u32,
    share_access: u32,
    create_disposition: u32,
    create_options: u32,
) {
    let name_bytes: Vec<u8> = path.encode_utf16().flat_map(u16::to_le_bytes).collect();
    let name_offset = CREATE_NAME_OFFSET;
    let name_len = u16::try_from(name_bytes.len()).unwrap_or(u16::MAX);
    buf.put_u16_le(57); // StructureSize
    buf.put_u8(0); // SecurityFlags
    buf.put_u8(0x00); // RequestedOplockLevel: SMB2_OPLOCK_LEVEL_NONE
    buf.put_u32_le(0x0000_0002); // ImpersonationLevel: Impersonation
    buf.put_u64_le(0); // SmbCreateFlags
    buf.put_u64_le(0); // Reserved
    buf.put_u32_le(desired_access); // DesiredAccess
    buf.put_u32_le(0x0000_0080); // FileAttributes: NORMAL
    buf.put_u32_le(share_access); // ShareAccess
    buf.put_u32_le(create_disposition); // CreateDisposition
    buf.put_u32_le(create_options); // CreateOptions
    buf.put_u16_le(name_offset); // NameOffset
    buf.put_u16_le(name_len); // NameLength
    buf.put_u32_le(0); // CreateContextsOffset
    buf.put_u32_le(0); // CreateContextsLength
    // StructureSize is 57 = 56-byte fixed part + 1 mandatory buffer byte.
    // The variable-length buffer must always be present, so when opening the
    // share root (empty name) we still emit a single padding byte. Omitting
    // it yields a 56-byte body that servers reject with STATUS_INVALID_PARAMETER.
    if name_bytes.is_empty() {
        buf.put_u8(0);
    } else {
        buf.put_slice(&name_bytes);
    }
}

#[derive(Debug, Clone)]
pub struct CreateResponse {
    pub file_id: [u8; 16],
    pub last_write_time: u64,
    pub file_size: u64,
}

#[must_use]
pub fn decode_create_response(body: &[u8]) -> Option<CreateResponse> {
    if body.len() < 88 {
        return None;
    }
    let last_write_time = (&body[24..32]).get_u64_le();
    let file_size = (&body[48..56]).get_u64_le();
    let mut file_id = [0u8; 16];
    file_id.copy_from_slice(&body[64..80]);

    Some(CreateResponse {
        file_id,
        last_write_time,
        file_size,
    })
}

// ── Close ───────────────────────────────────────────────────────────────────

pub fn encode_close_request(buf: &mut BytesMut, file_id: &[u8; 16]) {
    buf.put_u16_le(24); // StructureSize
    buf.put_u16_le(0); // Flags
    buf.put_u32_le(0); // Reserved
    buf.put_slice(file_id);
}

/// Encode a Close request with optional post-query attribute retrieval.
/// When `postquery` is true, the server returns file metadata in the response.
pub fn encode_close_request_ex(buf: &mut BytesMut, file_id: &[u8; 16], postquery: bool) {
    buf.put_u16_le(24); // StructureSize
    buf.put_u16_le(u16::from(postquery)); // Flags: SMB2_CLOSE_FLAG_POSTQUERY_ATTRIB
    buf.put_u32_le(0); // Reserved
    buf.put_slice(file_id);
}

/// Parsed Close response (meaningful when postquery was requested).
#[derive(Debug, Clone)]
pub struct CloseResponse {
    pub last_write_time: u64,
    pub file_size: u64,
}

#[must_use]
pub fn decode_close_response(body: &[u8]) -> Option<CloseResponse> {
    if body.len() < 56 {
        return None;
    }
    let last_write_time = u64::from_le_bytes(body[24..32].try_into().ok()?);
    let file_size = u64::from_le_bytes(body[48..56].try_into().ok()?);
    Some(CloseResponse {
        last_write_time,
        file_size,
    })
}

// ── Read ────────────────────────────────────────────────────────────────────

pub fn encode_read_request(buf: &mut BytesMut, file_id: &[u8; 16], offset: u64, length: u32) {
    buf.put_u16_le(49); // StructureSize
    buf.put_u8(0); // Padding
    buf.put_u8(0); // Flags
    buf.put_u32_le(length); // Length
    buf.put_u64_le(offset); // Offset
    buf.put_slice(file_id); // FileId
    buf.put_u32_le(1); // MinimumCount
    buf.put_u32_le(0); // Channel
    buf.put_u32_le(0); // RemainingBytes
    buf.put_u16_le(0); // ReadChannelInfoOffset
    buf.put_u16_le(0); // ReadChannelInfoLength
    buf.put_u8(0); // Buffer (padding byte)
}

#[must_use]
pub fn decode_read_response(body: &[u8]) -> Option<Bytes> {
    if body.len() < 17 {
        return None;
    }
    let data_offset = u16::from_le_bytes(body[2..4].try_into().ok()?) as usize;
    let data_length = (&body[4..8]).get_u32_le() as usize;

    // `body` is the SMB2 message body (everything after the 64-byte SMB2
    // header), so a spec-conformant `data_offset` is always at least
    // `SMB2_HEADER_SIZE`. Reject smaller values rather than letting
    // `saturating_sub` quietly slice from byte 0 — that would surface
    // unrelated response bytes to the caller as file data.
    let start = data_offset.checked_sub(SMB2_HEADER_SIZE)?;
    let end = start.checked_add(data_length)?;
    if end > body.len() {
        return None;
    }
    Some(Bytes::copy_from_slice(&body[start..end]))
}

/// Zero-copy variant of `decode_read_response` — takes ownership of the
/// response body `Vec` and slices into it without copying the data.
#[must_use]
pub fn decode_read_response_owned(body: Vec<u8>) -> Option<Bytes> {
    if body.len() < 17 {
        return None;
    }
    let data_offset = u16::from_le_bytes(body[2..4].try_into().ok()?) as usize;
    let data_length = u32::from_le_bytes(body[4..8].try_into().ok()?) as usize;

    // Same malformed-frame guard as `decode_read_response`: a `data_offset`
    // smaller than the SMB2 header size cannot be valid for a response body
    // that's already been split from the header, so we reject the frame
    // instead of letting `saturating_sub` produce an aliased slice into
    // unrelated response bytes.
    let start = data_offset.checked_sub(SMB2_HEADER_SIZE)?;
    let end = start.checked_add(data_length)?;
    if end > body.len() {
        return None;
    }
    let mut bytes = Bytes::from(body);
    bytes = bytes.slice(start..end);
    Some(bytes)
}

// ── Write ───────────────────────────────────────────────────────────────────

pub fn encode_write_request(buf: &mut BytesMut, file_id: &[u8; 16], offset: u64, data: &[u8]) {
    let data_offset = WRITE_DATA_OFFSET;
    let data_len = u32::try_from(data.len()).unwrap_or(u32::MAX);
    buf.put_u16_le(49); // StructureSize
    buf.put_u16_le(data_offset); // DataOffset
    buf.put_u32_le(data_len); // Length
    buf.put_u64_le(offset); // Offset
    buf.put_slice(file_id); // FileId
    buf.put_u32_le(0); // Channel
    buf.put_u32_le(0); // RemainingBytes
    buf.put_u16_le(0); // WriteChannelInfoOffset
    buf.put_u16_le(0); // WriteChannelInfoLength
    buf.put_u32_le(0); // Flags
    // Spec mandates StructureSize=49 (one byte beyond the 48-byte fixed part).
    // When `data` is empty we still need to emit one zero byte so stricter
    // servers (and the SMB2 validator) accept the request.
    if data.is_empty() {
        buf.put_u8(0);
    } else {
        buf.put_slice(data);
    }
}

#[must_use]
pub fn decode_write_response(body: &[u8]) -> Option<u32> {
    if body.len() < 16 {
        return None;
    }
    Some((&body[4..8]).get_u32_le())
}

// ── Set Info (rename) ──────────────────────────────────────────────────────

const SMB2_0_INFO_FILE: u8 = 0x01;
const FILE_RENAME_INFORMATION: u8 = 0x0A;

/// Encode a `SET_INFO` request for `FileRenameInformation` (rename/move a file).
///
/// `new_name` is the destination path relative to the share root, using
/// backslash separators (SMB convention). `replace_if_exists` controls
/// whether an existing file at the destination is overwritten.
pub fn encode_set_info_rename(
    buf: &mut BytesMut,
    file_id: &[u8; 16],
    new_name: &str,
    replace_if_exists: bool,
) {
    let name_bytes: Vec<u8> = new_name.encode_utf16().flat_map(u16::to_le_bytes).collect();

    let info_len = 1 + 7 + 8 + 4 + name_bytes.len();

    let buffer_offset = SET_INFO_BUFFER_OFFSET;

    buf.put_u16_le(33); // StructureSize
    buf.put_u8(SMB2_0_INFO_FILE);
    buf.put_u8(FILE_RENAME_INFORMATION);
    buf.put_u32_le(u32::try_from(info_len).unwrap_or(u32::MAX)); // BufferLength
    buf.put_u16_le(buffer_offset); // BufferOffset
    buf.put_u16_le(0); // Reserved
    buf.put_u32_le(0); // AdditionalInformation
    buf.put_slice(file_id); // FileId (16 bytes)

    // FileRenameInformation structure
    buf.put_u8(u8::from(replace_if_exists)); // ReplaceIfExists
    buf.put_slice(&[0u8; 7]); // Reserved
    buf.put_u64_le(0); // RootDirectory
    buf.put_u32_le(u32::try_from(name_bytes.len()).unwrap_or(u32::MAX));
    buf.put_slice(&name_bytes);
}

// ── Query Directory ─────────────────────────────────────────────────────────

pub const FILE_ID_BOTH_DIRECTORY_INFORMATION: u8 = 0x25;

pub fn encode_query_directory_request(
    buf: &mut BytesMut,
    file_id: &[u8; 16],
    pattern: &str,
    info_class: u8,
    restart: bool,
) {
    let pattern_bytes: Vec<u8> = pattern.encode_utf16().flat_map(u16::to_le_bytes).collect();
    let name_offset = QUERY_DIR_NAME_OFFSET;
    let pattern_len = u16::try_from(pattern_bytes.len()).unwrap_or(u16::MAX);
    let mut flags: u8 = 0;
    if restart {
        flags |= 0x01; // SMB2_RESTART_SCANS
    }
    buf.put_u16_le(33); // StructureSize
    buf.put_u8(info_class);
    buf.put_u8(flags);
    buf.put_u32_le(0); // FileIndex
    buf.put_slice(file_id);
    buf.put_u16_le(name_offset);
    buf.put_u16_le(pattern_len);
    buf.put_u32_le(65536); // OutputBufferLength
    buf.put_slice(&pattern_bytes);
}

#[derive(Debug, Clone)]
pub struct DirectoryEntry {
    pub file_name: String,
    pub file_size: u64,
    pub file_attributes: u32,
    pub last_write_time: u64,
}

impl DirectoryEntry {
    #[must_use]
    pub fn is_directory(&self) -> bool {
        self.file_attributes & 0x10 != 0
    }
}

/// Parse `FILE_ID_BOTH_DIRECTORY_INFORMATION` entries from a query directory response.
#[must_use]
pub fn parse_directory_entries(data: &[u8]) -> Vec<DirectoryEntry> {
    let mut entries = Vec::new();
    let mut offset = 0usize;

    loop {
        if offset + 104 > data.len() {
            break;
        }
        let entry = &data[offset..];

        let next_entry_offset = (&entry[0..4]).get_u32_le() as usize;
        let _file_index = (&entry[4..8]).get_u32_le();
        let _creation_time = (&entry[8..16]).get_u64_le();
        let _last_access_time = (&entry[16..24]).get_u64_le();
        let last_write_time = (&entry[24..32]).get_u64_le();
        let _change_time = (&entry[32..40]).get_u64_le();
        let file_size = (&entry[40..48]).get_u64_le();
        let _allocation_size = (&entry[48..56]).get_u64_le();
        let file_attributes = (&entry[56..60]).get_u32_le();
        let file_name_length = (&entry[60..64]).get_u32_le() as usize;

        let name_start = 104;
        let name_end = name_start + file_name_length;
        if name_end > entry.len() {
            break;
        }
        let name_bytes = &entry[name_start..name_end];
        let file_name = String::from_utf16_lossy(
            &name_bytes
                .chunks_exact(2)
                .map(|c| u16::from_le_bytes([c[0], c[1]]))
                .collect::<Vec<_>>(),
        );

        if file_name != "." && file_name != ".." {
            entries.push(DirectoryEntry {
                file_name,
                file_size,
                file_attributes,
                last_write_time,
            });
        }

        if next_entry_offset == 0 {
            break;
        }
        offset += next_entry_offset;
    }

    entries
}

// ── Compound request support ───────────────────────────────────────────────

pub const SMB2_FLAGS_RELATED: u32 = 0x0000_0004;
pub const SMB2_FLAGS_SIGNED: u32 = 0x0000_0008;

/// `STATUS_PENDING` — server is still processing the request and will send
/// the real response later. Callers should loop and read the next frame.
pub const STATUS_PENDING: u32 = 0x0000_0103;

/// Mask over the two high bits of an `NT_STATUS` indicating an error severity.
pub const NT_STATUS_ERROR_MASK: u32 = 0xC000_0000;

/// Sentinel file ID — server substitutes the file ID from the preceding
/// Create response in a related compound chain.
pub const SENTINEL_FILE_ID: [u8; 16] = [0xFF; 16];

// ── Frame helpers ───────────────────────────────────────────────────────────

/// Prepend a 4-byte `NetBIOS` session length prefix to the packet.
fn frame_packet(header: &Header, body: &[u8]) -> BytesMut {
    let total = SMB2_HEADER_SIZE + body.len();
    let mut buf = BytesMut::with_capacity(4 + total);
    let netbios_len = u32::try_from(total).unwrap_or(u32::MAX) & 0x00FF_FFFF;
    buf.put_u32(netbios_len);
    header.encode(&mut buf);
    buf.put_slice(body);
    buf
}

/// Build a complete SMB2 request packet: [`NetBIOS` length][Header][Body]
pub fn build_request<F>(header: &Header, body_builder: F) -> BytesMut
where
    F: FnOnce(&mut BytesMut),
{
    let mut body = BytesMut::with_capacity(256);
    body_builder(&mut body);
    frame_packet(header, &body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let mut hdr = Header::new(Command::Create, 42);
        hdr.session_id = 0xDEAD;
        hdr.tree_id = 7;
        hdr.flags = 0x04;

        let mut buf = BytesMut::with_capacity(64);
        hdr.encode(&mut buf);

        let decoded = Header::decode(&buf).expect("test fixture");
        assert_eq!(decoded.command, Command::Create as u16);
        assert_eq!(decoded.message_id, 42);
        assert_eq!(decoded.session_id, 0xDEAD);
        assert_eq!(decoded.tree_id, 7);
        assert_eq!(decoded.flags, 0x04);
    }

    #[test]
    fn header_decode_too_short() {
        assert!(Header::decode(&[0u8; 32]).is_none());
    }

    #[test]
    fn header_decode_bad_magic() {
        let mut buf = [0u8; 64];
        buf[0..4].copy_from_slice(b"XXXX");
        assert!(Header::decode(&buf).is_none());
    }

    #[test]
    fn nt_status_success_not_error() {
        assert!(!NtStatus::Success.is_error());
    }

    #[test]
    fn nt_status_not_found_is_error() {
        assert!(NtStatus::ObjectNameNotFound.is_error());
    }

    #[test]
    fn nt_status_no_more_files_not_error() {
        assert!(!NtStatus::NoMoreFiles.is_error());
    }

    #[test]
    fn nt_status_known_codes() {
        assert_eq!(NtStatus::from_u32(0x0000_0000), NtStatus::Success);
        assert_eq!(NtStatus::from_u32(0xC000_000F), NtStatus::NoSuchFile);
        assert_eq!(NtStatus::from_u32(0x8000_0006), NtStatus::NoMoreFiles);
    }

    #[test]
    fn decode_create_response_valid() {
        let mut body = vec![0u8; 88];
        body[24..32].copy_from_slice(&100u64.to_le_bytes());
        body[48..56].copy_from_slice(&42u64.to_le_bytes());
        body[64..80].copy_from_slice(&[1u8; 16]);

        let resp = decode_create_response(&body).expect("test fixture");
        assert_eq!(resp.last_write_time, 100);
        assert_eq!(resp.file_size, 42);
        assert_eq!(resp.file_id, [1u8; 16]);
    }

    #[test]
    fn decode_create_response_too_short() {
        assert!(decode_create_response(&[0u8; 10]).is_none());
    }

    #[test]
    fn decode_read_response_valid() {
        let mut body = vec![0u8; 32];
        let data_offset: u16 = (SMB2_HEADER_SIZE + 16) as u16;
        body[2..4].copy_from_slice(&data_offset.to_le_bytes());
        body[4..8].copy_from_slice(&5u32.to_le_bytes());
        body[16..21].copy_from_slice(b"hello");

        let data = decode_read_response(&body).expect("test fixture");
        assert_eq!(&data[..], b"hello");
    }

    #[test]
    fn decode_read_response_too_short() {
        assert!(decode_read_response(&[0u8; 5]).is_none());
    }

    #[test]
    fn decode_write_response_valid() {
        let mut body = vec![0u8; 16];
        body[4..8].copy_from_slice(&1024u32.to_le_bytes());
        assert_eq!(decode_write_response(&body), Some(1024));
    }

    #[test]
    fn decode_write_response_too_short() {
        assert!(decode_write_response(&[0u8; 8]).is_none());
    }

    #[test]
    fn decode_close_response_valid() {
        let mut body = vec![0u8; 60];
        body[24..32].copy_from_slice(&999u64.to_le_bytes());
        body[48..56].copy_from_slice(&4096u64.to_le_bytes());
        let resp = decode_close_response(&body).expect("test fixture");
        assert_eq!(resp.last_write_time, 999);
        assert_eq!(resp.file_size, 4096);
    }

    #[test]
    fn decode_close_response_too_short() {
        assert!(decode_close_response(&[0u8; 20]).is_none());
    }

    #[test]
    fn parse_directory_entries_single() {
        let name = "test.txt";
        let name_utf16: Vec<u8> = name.encode_utf16().flat_map(u16::to_le_bytes).collect();
        let entry_size = 104 + name_utf16.len();
        let mut data = vec![0u8; entry_size];
        data[40..48].copy_from_slice(&512u64.to_le_bytes());
        data[56..60].copy_from_slice(&0x20u32.to_le_bytes());
        data[60..64].copy_from_slice(
            &u32::try_from(name_utf16.len())
                .expect("test fixture")
                .to_le_bytes(),
        );
        data[104..].copy_from_slice(&name_utf16);

        let entries = parse_directory_entries(&data);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_name, "test.txt");
        assert_eq!(entries[0].file_size, 512);
        assert!(!entries[0].is_directory());
    }

    #[test]
    fn parse_directory_entries_skips_dot() {
        let name = ".";
        let name_utf16: Vec<u8> = name.encode_utf16().flat_map(u16::to_le_bytes).collect();
        let entry_size = 104 + name_utf16.len();
        let mut data = vec![0u8; entry_size];
        data[56..60].copy_from_slice(&0x10u32.to_le_bytes());
        data[60..64].copy_from_slice(
            &u32::try_from(name_utf16.len())
                .expect("test fixture")
                .to_le_bytes(),
        );
        data[104..].copy_from_slice(&name_utf16);

        let entries = parse_directory_entries(&data);
        assert!(entries.is_empty());
    }

    #[test]
    fn parse_directory_entries_empty() {
        assert!(parse_directory_entries(&[]).is_empty());
    }

    #[test]
    fn directory_entry_is_directory() {
        let entry = DirectoryEntry {
            file_name: "dir".into(),
            file_size: 0,
            file_attributes: 0x10,
            last_write_time: 0,
        };
        assert!(entry.is_directory());

        let file = DirectoryEntry {
            file_name: "f".into(),
            file_size: 100,
            file_attributes: 0x20,
            last_write_time: 0,
        };
        assert!(!file.is_directory());
    }

    #[test]
    fn build_request_has_netbios_header() {
        let hdr = Header::new(Command::Close, 0);
        let packet = build_request(&hdr, |buf| {
            encode_close_request(buf, &[0u8; 16]);
        });
        let netbios_len = u32::from_be_bytes(packet[0..4].try_into().expect("test fixture"));
        assert_eq!(netbios_len as usize, packet.len() - 4);
        assert_eq!(&packet[4..8], SMB2_MAGIC);
    }

    #[test]
    fn encode_close_request_size() {
        let mut buf = BytesMut::new();
        encode_close_request(&mut buf, &[0u8; 16]);
        assert_eq!(buf.len(), 24);
    }

    #[test]
    fn encode_read_request_size() {
        let mut buf = BytesMut::new();
        encode_read_request(&mut buf, &[0u8; 16], 0, 65536);
        assert_eq!(buf.len(), 49);
    }

    #[test]
    fn encode_write_request_includes_data() {
        let mut buf = BytesMut::new();
        let data = b"hello";
        encode_write_request(&mut buf, &[0u8; 16], 0, data);
        assert_eq!(buf.len(), 48 + data.len());
    }

    #[test]
    fn set_info_rename_structure_size() {
        let mut buf = BytesMut::new();
        encode_set_info_rename(&mut buf, &[0u8; 16], "test", false);
        assert_eq!(
            u16::from_le_bytes(buf[0..2].try_into().expect("test fixture")),
            33
        );
    }

    #[test]
    fn set_info_rename_file_id() {
        let file_id = [0xAA; 16];
        let mut buf = BytesMut::new();
        encode_set_info_rename(&mut buf, &file_id, "x", false);
        assert_eq!(&buf[16..32], &file_id);
    }

    #[test]
    fn set_info_rename_replace_flag_false() {
        let mut buf = BytesMut::new();
        encode_set_info_rename(&mut buf, &[0u8; 16], "test", false);
        assert_eq!(buf[32], 0);
    }

    #[test]
    fn set_info_rename_replace_flag_true() {
        let mut buf = BytesMut::new();
        encode_set_info_rename(&mut buf, &[0u8; 16], "test", true);
        assert_eq!(buf[32], 1);
    }

    #[test]
    fn credit_charge_scales_with_payload() {
        assert_eq!(credit_charge_for(0), 1);
        assert_eq!(credit_charge_for(65_536), 1);
        assert_eq!(credit_charge_for(65_537), 2);
        assert_eq!(credit_charge_for(131_072), 2);
    }
}
