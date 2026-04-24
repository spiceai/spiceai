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

//! NTLMv2 authentication for SMB2 sessions.
//!
//! Implements the NTLM challenge-response protocol using the pure-Rust
//! `crypto` module.

use bytes::{BufMut, Bytes, BytesMut};

use crate::crypto;

// ── NTLMSSP message types ───────────────────────────────────────────────────

const NTLMSSP_SIGNATURE: &[u8; 8] = b"NTLMSSP\0";
const NTLMSSP_NEGOTIATE: u32 = 1;
const NTLMSSP_CHALLENGE: u32 = 2;
const NTLMSSP_AUTH: u32 = 3;

// NTLMv2 LM response is always 24 zero bytes.
const LM_RESPONSE_LEN: usize = 24;
const LM_LEN: u16 = LM_RESPONSE_LEN as u16;

// Negotiate flags
const NTLMSSP_NEGOTIATE_UNICODE: u32 = 0x0000_0001;
const NTLMSSP_NEGOTIATE_NTLM: u32 = 0x0000_0200;
const NTLMSSP_REQUEST_TARGET: u32 = 0x0000_0004;
const NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY: u32 = 0x0008_0000;
const NTLMSSP_NEGOTIATE_VERSION: u32 = 0x0200_0000;

/// Build the NTLMSSP Negotiate (Type 1) message.
#[must_use]
pub fn build_negotiate_message() -> Bytes {
    let mut buf = BytesMut::with_capacity(40);
    buf.put_slice(NTLMSSP_SIGNATURE);
    buf.put_u32_le(NTLMSSP_NEGOTIATE);
    let flags = NTLMSSP_NEGOTIATE_UNICODE
        | NTLMSSP_NEGOTIATE_NTLM
        | NTLMSSP_REQUEST_TARGET
        | NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY
        | NTLMSSP_NEGOTIATE_VERSION;
    buf.put_u32_le(flags);
    buf.put_u16_le(0);
    buf.put_u16_le(0);
    buf.put_u32_le(0);
    buf.put_u16_le(0);
    buf.put_u16_le(0);
    buf.put_u32_le(0);
    put_ntlm_version(&mut buf);
    buf.freeze()
}

fn put_ntlm_version(buf: &mut BytesMut) {
    buf.put_u8(10); // ProductMajorVersion
    buf.put_u8(0); // ProductMinorVersion
    buf.put_u16_le(0); // ProductBuild
    buf.put_slice(&[0u8; 3]); // Reserved
    buf.put_u8(0x0f); // NTLMRevisionCurrent
}

#[derive(Debug)]
pub struct ChallengeMessage {
    pub server_challenge: [u8; 8],
    pub negotiate_flags: u32,
    pub target_info: Vec<u8>,
}

/// Parse an NTLMSSP Challenge (Type 2) message.
#[must_use]
pub fn parse_challenge_message(data: &[u8]) -> Option<ChallengeMessage> {
    if data.len() < 32 || &data[0..8] != NTLMSSP_SIGNATURE {
        return None;
    }
    let msg_type = u32::from_le_bytes(data[8..12].try_into().ok()?);
    if msg_type != NTLMSSP_CHALLENGE {
        return None;
    }

    let negotiate_flags = u32::from_le_bytes(data[20..24].try_into().ok()?);

    let mut server_challenge = [0u8; 8];
    server_challenge.copy_from_slice(&data[24..32]);

    let target_info = if data.len() >= 48 {
        let ti_len = u16::from_le_bytes(data[40..42].try_into().ok()?) as usize;
        let ti_offset = u32::from_le_bytes(data[44..48].try_into().ok()?) as usize;
        if ti_offset + ti_len <= data.len() {
            data[ti_offset..ti_offset + ti_len].to_vec()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    Some(ChallengeMessage {
        server_challenge,
        negotiate_flags,
        target_info,
    })
}

/// Compute NTLMv2 hash: `HMAC_MD5(MD4(UTF16LE(password)), UTF16LE(UPPER(username) + domain))`.
fn ntlmv2_hash(username: &str, password: &str, domain: &str) -> [u8; 16] {
    let password_utf16: Vec<u8> = password.encode_utf16().flat_map(u16::to_le_bytes).collect();
    let nt_hash = crypto::md4(&password_utf16);

    let user_domain = format!("{}{}", username.to_uppercase(), domain);
    let ud_utf16: Vec<u8> = user_domain
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();
    crypto::hmac_md5(&nt_hash, &ud_utf16)
}

/// Build the NTLMSSP Authenticate (Type 3) message.
/// Returns `(message_bytes, session_base_key)`.
#[must_use]
pub fn build_authenticate_message(
    challenge: &ChallengeMessage,
    username: &str,
    password: &str,
    domain: &str,
    workstation: &str,
) -> (Bytes, [u8; 16]) {
    let ntlmv2_hash = ntlmv2_hash(username, password, domain);

    let client_challenge = generate_client_challenge();
    let blob = build_ntlmv2_blob(client_challenge, &challenge.target_info);

    let mut hmac_input = Vec::with_capacity(8 + blob.len());
    hmac_input.extend_from_slice(&challenge.server_challenge);
    hmac_input.extend_from_slice(&blob);

    let nt_proof_str = crypto::hmac_md5(&ntlmv2_hash, &hmac_input);

    let mut nt_response = Vec::with_capacity(16 + blob.len());
    nt_response.extend_from_slice(&nt_proof_str);
    nt_response.extend_from_slice(&blob);

    let session_base_key = crypto::hmac_md5(&ntlmv2_hash, &nt_proof_str);

    let domain_bytes: Vec<u8> = domain.encode_utf16().flat_map(u16::to_le_bytes).collect();
    let user_bytes: Vec<u8> = username.encode_utf16().flat_map(u16::to_le_bytes).collect();
    let ws_bytes: Vec<u8> = workstation
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();

    let lm_response = [0u8; LM_RESPONSE_LEN];

    let payload_offset = 72u32;
    let lm_offset = payload_offset;
    let nt_offset = lm_offset + LM_RESPONSE_LEN as u32;
    let domain_offset = nt_offset + u32::try_from(nt_response.len()).unwrap_or(u32::MAX);
    let user_offset = domain_offset + u32::try_from(domain_bytes.len()).unwrap_or(u32::MAX);
    let ws_offset = user_offset + u32::try_from(user_bytes.len()).unwrap_or(u32::MAX);

    let flags = challenge.negotiate_flags | NTLMSSP_NEGOTIATE_VERSION;

    let mut buf = BytesMut::with_capacity(ws_offset as usize + ws_bytes.len());
    buf.put_slice(NTLMSSP_SIGNATURE);
    buf.put_u32_le(NTLMSSP_AUTH);

    // LmChallengeResponse
    buf.put_u16_le(LM_LEN);
    buf.put_u16_le(LM_LEN);
    buf.put_u32_le(lm_offset);

    // NtChallengeResponse
    let nt_len = u16::try_from(nt_response.len()).unwrap_or(u16::MAX);
    buf.put_u16_le(nt_len);
    buf.put_u16_le(nt_len);
    buf.put_u32_le(nt_offset);

    // DomainName
    let dom_len = u16::try_from(domain_bytes.len()).unwrap_or(u16::MAX);
    buf.put_u16_le(dom_len);
    buf.put_u16_le(dom_len);
    buf.put_u32_le(domain_offset);

    // UserName
    let user_len = u16::try_from(user_bytes.len()).unwrap_or(u16::MAX);
    buf.put_u16_le(user_len);
    buf.put_u16_le(user_len);
    buf.put_u32_le(user_offset);

    // Workstation
    let ws_len = u16::try_from(ws_bytes.len()).unwrap_or(u16::MAX);
    buf.put_u16_le(ws_len);
    buf.put_u16_le(ws_len);
    buf.put_u32_le(ws_offset);

    // EncryptedRandomSessionKey (empty)
    let enc_key_offset = ws_offset + u32::try_from(ws_bytes.len()).unwrap_or(u32::MAX);
    buf.put_u16_le(0);
    buf.put_u16_le(0);
    buf.put_u32_le(enc_key_offset);

    // NegotiateFlags
    buf.put_u32_le(flags);

    // Version
    put_ntlm_version(&mut buf);

    // Payload
    buf.put_slice(&lm_response);
    buf.put_slice(&nt_response);
    buf.put_slice(&domain_bytes);
    buf.put_slice(&user_bytes);
    buf.put_slice(&ws_bytes);

    (buf.freeze(), session_base_key)
}

/// Derive the SMB 3.1.1 signing key using SP800-108 Counter Mode KDF.
#[must_use]
pub fn derive_signing_key(session_key: &[u8; 16], preauth_hash: &[u8; 64]) -> [u8; 16] {
    let label = b"SMBSigningKey\0";

    let mut input = Vec::with_capacity(4 + label.len() + 1 + 64 + 4);
    input.extend_from_slice(&1u32.to_be_bytes());
    input.extend_from_slice(label);
    input.push(0x00);
    input.extend_from_slice(preauth_hash);
    input.extend_from_slice(&128u32.to_be_bytes());

    let derived = crypto::hmac_sha256(session_key, &input);
    let mut key = [0u8; 16];
    key.copy_from_slice(&derived[..16]);
    key
}

fn build_ntlmv2_blob(client_challenge: [u8; 8], target_info: &[u8]) -> Vec<u8> {
    let mut blob = Vec::with_capacity(28 + target_info.len() + 4);
    blob.push(0x01); // RespType
    blob.push(0x01); // HiRespType
    blob.extend_from_slice(&[0u8; 2]);
    blob.extend_from_slice(&[0u8; 4]);
    let ts = windows_filetime_now();
    blob.extend_from_slice(&ts.to_le_bytes());
    blob.extend_from_slice(&client_challenge);
    blob.extend_from_slice(&[0u8; 4]);
    blob.extend_from_slice(target_info);
    blob.extend_from_slice(&[0u8; 4]);
    blob
}

/// Get current time as Windows FILETIME (100ns intervals since 1601-01-01).
fn windows_filetime_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    const EPOCH_DIFF: u64 = 116_444_736_000_000_000;
    let unix_ns = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    )
    .unwrap_or(u64::MAX);
    let filetime_100ns = unix_ns / 100;
    filetime_100ns.saturating_add(EPOCH_DIFF)
}

fn generate_client_challenge() -> [u8; 8] {
    let mut buf = [0u8; 8];
    crypto::random_bytes(&mut buf);
    buf
}

/// Extract an NTLMSSP token from a GSS-API / SPNEGO wrapper if present,
/// or return the data as-is if it's already raw NTLMSSP.
#[must_use]
pub fn unwrap_spnego(data: &[u8]) -> &[u8] {
    data.windows(8)
        .position(|w| w == NTLMSSP_SIGNATURE)
        .map_or(data, |pos| &data[pos..])
}

/// Wrap an NTLMSSP token in a minimal SPNEGO `NegTokenInit` for the first
/// message.
#[must_use]
pub fn wrap_spnego_negotiate(ntlmssp: &[u8]) -> Vec<u8> {
    let oid_spnego: &[u8] = &[0x06, 0x06, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x02];
    let oid_ntlmssp: &[u8] = &[
        0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a,
    ];

    let mech_list = der_wrap(0x30, oid_ntlmssp);
    let mech_types = der_wrap(0xa0, &mech_list);
    let mech_token_inner = der_wrap(0x04, ntlmssp);
    let mech_token = der_wrap(0xa2, &mech_token_inner);

    let mut inner = Vec::with_capacity(mech_types.len() + mech_token.len());
    inner.extend_from_slice(&mech_types);
    inner.extend_from_slice(&mech_token);

    let neg_token_init = der_wrap(0x30, &inner);
    let neg_token = der_wrap(0xa0, &neg_token_init);

    let mut app_content = Vec::with_capacity(oid_spnego.len() + neg_token.len());
    app_content.extend_from_slice(oid_spnego);
    app_content.extend_from_slice(&neg_token);
    der_wrap(0x60, &app_content)
}

/// Wrap an NTLMSSP auth token in SPNEGO `NegTokenResp`.
#[must_use]
pub fn wrap_spnego_auth(ntlmssp: &[u8]) -> Vec<u8> {
    let octet_string = der_wrap(0x04, ntlmssp);
    let resp_token = der_wrap(0xa2, &octet_string);
    let seq = der_wrap(0x30, &resp_token);
    der_wrap(0xa1, &seq)
}

fn der_wrap(tag: u8, data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 4 + data.len());
    buf.push(tag);
    push_der_length(&mut buf, data.len());
    buf.extend_from_slice(data);
    buf
}

fn push_der_length(buf: &mut Vec<u8>, len: usize) {
    // Each branch is guarded so the bits fit cleanly into a u8 after shifting/masking.
    if len < 0x80 {
        buf.push(len as u8);
    } else if len < 0x100 {
        buf.push(0x81);
        buf.push(len as u8);
    } else if len < 0x10000 {
        buf.push(0x82);
        buf.push((len >> 8) as u8);
        buf.push(len as u8);
    } else {
        buf.push(0x83);
        buf.push((len >> 16) as u8);
        buf.push((len >> 8) as u8);
        buf.push(len as u8);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_challenge_bytes(
        server_challenge: [u8; 8],
        negotiate_flags: u32,
        target_info: &[u8],
        target_info_offset: usize,
    ) -> Vec<u8> {
        let mut data = vec![0u8; target_info_offset + target_info.len()];
        data[..8].copy_from_slice(NTLMSSP_SIGNATURE);
        data[8..12].copy_from_slice(&NTLMSSP_CHALLENGE.to_le_bytes());
        data[20..24].copy_from_slice(&negotiate_flags.to_le_bytes());
        data[24..32].copy_from_slice(&server_challenge);
        data[40..42].copy_from_slice(
            &u16::try_from(target_info.len())
                .expect("test fixture")
                .to_le_bytes(),
        );
        data[42..44].copy_from_slice(
            &u16::try_from(target_info.len())
                .expect("test fixture")
                .to_le_bytes(),
        );
        data[44..48].copy_from_slice(
            &u32::try_from(target_info_offset)
                .expect("test fixture")
                .to_le_bytes(),
        );
        data[target_info_offset..target_info_offset + target_info.len()]
            .copy_from_slice(target_info);
        data
    }

    #[test]
    fn test_build_negotiate_message_layout() {
        let message = build_negotiate_message();
        assert_eq!(message.len(), 40);
        assert_eq!(&message[..8], NTLMSSP_SIGNATURE);
        assert_eq!(
            u32::from_le_bytes(message[8..12].try_into().expect("test fixture")),
            NTLMSSP_NEGOTIATE
        );

        let expected_flags = NTLMSSP_NEGOTIATE_UNICODE
            | NTLMSSP_NEGOTIATE_NTLM
            | NTLMSSP_REQUEST_TARGET
            | NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY
            | NTLMSSP_NEGOTIATE_VERSION;
        assert_eq!(
            u32::from_le_bytes(message[12..16].try_into().expect("test fixture")),
            expected_flags
        );
        assert_eq!(&message[16..32], &[0u8; 16]);
        assert_eq!(&message[32..40], &[10, 0, 0, 0, 0, 0, 0, 0x0f]);
    }

    #[test]
    fn test_parse_challenge_message_extracts_fields() {
        let server_challenge = *b"12345678";
        let negotiate_flags = 0xaabb_ccdd;
        let target_info = [0xde, 0xad, 0xbe, 0xef, 0x01, 0x02];
        let challenge = build_challenge_bytes(server_challenge, negotiate_flags, &target_info, 48);

        let parsed = parse_challenge_message(&challenge).expect("test fixture");
        assert_eq!(parsed.server_challenge, server_challenge);
        assert_eq!(parsed.negotiate_flags, negotiate_flags);
        assert_eq!(parsed.target_info, target_info);
    }

    #[test]
    fn test_parse_challenge_message_rejects_invalid_inputs() {
        assert!(parse_challenge_message(&[]).is_none());

        let mut wrong_signature = vec![0u8; 32];
        wrong_signature[..8].copy_from_slice(b"badtoken");
        assert!(parse_challenge_message(&wrong_signature).is_none());

        let mut wrong_type = vec![0u8; 32];
        wrong_type[..8].copy_from_slice(NTLMSSP_SIGNATURE);
        wrong_type[8..12].copy_from_slice(&NTLMSSP_NEGOTIATE.to_le_bytes());
        assert!(parse_challenge_message(&wrong_type).is_none());
    }

    #[test]
    fn test_derive_signing_key_known_vector() {
        let session_key = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];
        let preauth_hash = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29,
            0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
            0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f,
        ];

        let derived = derive_signing_key(&session_key, &preauth_hash);
        assert_eq!(
            crypto::hex_encode(&derived),
            "f7e5401ecc6e79ef9eab401b05004e4f"
        );
    }

    #[test]
    fn test_build_ntlmv2_blob_layout() {
        let client_challenge = *b"12345678";
        let target_info = [0x01, 0x02, 0x03, 0x04];
        let blob = build_ntlmv2_blob(client_challenge, &target_info);

        assert_eq!(
            &blob[..8],
            &[0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
        assert!(
            u64::from_le_bytes(blob[8..16].try_into().expect("test fixture"))
                >= 116_444_736_000_000_000
        );
        assert_eq!(&blob[16..24], client_challenge);
        assert_eq!(&blob[24..28], &[0u8; 4]);
        assert_eq!(&blob[28..32], &target_info);
        assert_eq!(&blob[32..36], &[0u8; 4]);
    }

    #[test]
    fn test_unwrap_spnego_returns_original_slice_without_signature() {
        let token = b"plain-token";
        assert_eq!(unwrap_spnego(token), token);
    }

    #[test]
    fn test_unwrap_spnego_finds_embedded_ntlmssp() {
        let wrapped = b"junkNTLMSSP\0payload";
        assert_eq!(unwrap_spnego(wrapped), b"NTLMSSP\0payload");
    }

    #[test]
    fn test_wrap_spnego_negotiate_round_trips_token() {
        let token = b"NTLMSSP\0\x01\x02\x03";
        let wrapped = wrap_spnego_negotiate(token);
        assert_eq!(wrapped[0], 0x60);
        assert_eq!(unwrap_spnego(&wrapped), token);
    }

    #[test]
    fn test_wrap_spnego_auth_round_trips_token() {
        let token = b"NTLMSSP\0\x03\x02\x01";
        let wrapped = wrap_spnego_auth(token);
        assert_eq!(wrapped[0], 0xa1);
        assert_eq!(unwrap_spnego(&wrapped), token);
    }

    #[test]
    fn test_der_wrap_adds_tag_and_length() {
        let wrapped = der_wrap(0x04, b"\xaa\xbb");
        assert_eq!(wrapped, vec![0x04, 0x02, 0xaa, 0xbb]);
    }

    #[test]
    fn test_push_der_length_boundaries() {
        let cases = [
            (0x7f, vec![0x7f]),
            (0x80, vec![0x81, 0x80]),
            (0xff, vec![0x81, 0xff]),
            (0x100, vec![0x82, 0x01, 0x00]),
            (0xffff, vec![0x82, 0xff, 0xff]),
            (0x10000, vec![0x83, 0x01, 0x00, 0x00]),
        ];

        for (len, expected) in cases {
            let mut out = Vec::new();
            push_der_length(&mut out, len);
            assert_eq!(out, expected);
        }
    }
}
