/*
Copyright 2026 The Spice.ai OSS Authors

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

//! On-disk integrity framing for the Cayenne staging WAL.
//!
//! Historically a staging WAL record (`_wal.json`) was written as plain compact
//! JSON: its only integrity check was "does `serde_json` parse it". That misses
//! the most dangerous corruption — a single flipped byte inside a JSON string
//! value (a staged filename, or the `target_snapshot` id) still parses as valid
//! JSON, so recovery would move files using corrupted instructions. Silent
//! corruption is the worst failure mode for a system of record.
//!
//! This module wraps the JSON payload in a small binary envelope carrying a
//! checksum, so a corrupt or truncated record is *detected* on read (and, for
//! the staging WAL, *discarded* on recovery rather than parsed as garbage). The
//! JSON payload is stored verbatim, so the inner record type keeps its existing
//! `serde` representation.
//!
//! # Wire format
//!
//! ```text
//! ┌─────────────┬─────────┬────────────────┬────────────────┬──────────────┐
//! │  magic (8)  │ ver (1) │ payload_len(8) │  checksum (8)  │  payload (N) │
//! │  MAGIC bytes│  0x01   │   u64 LE = N   │  u64 LE xxh3   │  JSON bytes  │
//! └─────────────┴─────────┴────────────────┴────────────────┴──────────────┘
//! ```
//!
//! `checksum` is the seeded XXH3-64 (via [`hash_index::hash_key_bytes`]) over
//! exactly the `payload` bytes. A fast non-cryptographic checksum is the right
//! tool here: the staging WAL sits on the ingestion hot path and the threat is
//! accidental corruption / torn writes, not an adversary (analogous to a
//! `RocksDB` WAL record CRC).
//!
//! # Backward / forward compatibility
//!
//! Reads that do not begin with [`MAGIC`] are treated as a **legacy**
//! pre-checksum pure-JSON record and returned verbatim (no integrity check), so
//! a WAL written by an older binary still recovers after upgrade. Framing is
//! only emitted when integrity checksums are enabled, so with the feature off
//! the on-disk format is byte-identical to before (and remains readable by an
//! older binary — a downgrade-safety property).

use hash_index::hash_key_bytes;

/// Magic prefix identifying a checksum-framed staging-WAL record. Chosen to be
/// invalid JSON (a bare `{`/whitespace can never start with these bytes) so
/// framed and legacy records are unambiguously distinguishable.
pub(crate) const MAGIC: [u8; 8] = *b"CAYWALv1";

/// Framing format version. Bumped only on an incompatible envelope change.
pub(crate) const FORMAT_VERSION: u8 = 1;

/// Envelope header length: magic(8) + version(1) + `payload_len`(8) + checksum(8).
const HEADER_LEN: usize = MAGIC.len() + 1 + 8 + 8;

/// Reasons a framed record failed its integrity check. Every variant means the
/// record is untrustworthy and must be treated as corrupt (discarded on staging
/// WAL recovery; a hard read error elsewhere).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalChecksumError {
    /// The stored checksum does not match the payload — bit-rot or a torn write.
    ChecksumMismatch { stored: u64, computed: u64 },
    /// The record starts with [`MAGIC`] but is shorter than a full header, or
    /// the declared payload length runs past the end of the bytes — a truncated
    /// tail.
    Truncated { have: usize, need: usize },
    /// The envelope version is newer/unknown; refuse rather than guess.
    UnsupportedVersion(u8),
}

impl std::fmt::Display for WalChecksumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChecksumMismatch { stored, computed } => write!(
                f,
                "staging WAL checksum mismatch (stored {stored:#018x}, computed {computed:#018x})"
            ),
            Self::Truncated { have, need } => {
                write!(
                    f,
                    "staging WAL record truncated (have {have} bytes, need {need})"
                )
            }
            Self::UnsupportedVersion(v) => {
                write!(f, "unsupported staging WAL framing version {v}")
            }
        }
    }
}

/// A payload extracted from a staging-WAL record on read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WalPayload<'a> {
    /// A checksum-framed payload whose checksum verified.
    Verified(&'a [u8]),
    /// A legacy pre-checksum record (no framing); the whole input is the
    /// payload and no integrity check was possible.
    Legacy(&'a [u8]),
}

impl<'a> WalPayload<'a> {
    /// The JSON payload bytes, regardless of framing.
    pub(crate) fn bytes(&self) -> &'a [u8] {
        match self {
            Self::Verified(b) | Self::Legacy(b) => b,
        }
    }
}

/// Wrap `payload` in a checksum-framed envelope (see the module wire format).
pub(crate) fn frame(payload: &[u8]) -> Vec<u8> {
    // usize -> u64 is a widening (lossless) conversion on every supported
    // platform, so no fallible cast is needed here.
    let payload_len = payload.len() as u64;
    let checksum = hash_key_bytes(&[payload]);

    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&MAGIC);
    out.push(FORMAT_VERSION);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Interpret raw staging-WAL bytes.
///
/// * If the bytes do not begin with [`MAGIC`], they are a legacy pure-JSON
///   record and returned as [`WalPayload::Legacy`] (verbatim).
/// * Otherwise the envelope is validated (version, length, checksum) and the
///   inner payload returned as [`WalPayload::Verified`], or a
///   [`WalChecksumError`] if the record is corrupt/truncated.
pub(crate) fn verify(bytes: &[u8]) -> Result<WalPayload<'_>, WalChecksumError> {
    // Not framed → legacy pure-JSON record. A legacy record can never be
    // mistaken for a framed one because MAGIC is not valid JSON.
    if !bytes.starts_with(&MAGIC) {
        return Ok(WalPayload::Legacy(bytes));
    }

    if bytes.len() < HEADER_LEN {
        return Err(WalChecksumError::Truncated {
            have: bytes.len(),
            need: HEADER_LEN,
        });
    }

    let version = bytes[MAGIC.len()];
    if version != FORMAT_VERSION {
        return Err(WalChecksumError::UnsupportedVersion(version));
    }

    let len_start = MAGIC.len() + 1;
    let checksum_start = len_start + 8;
    let payload_start = checksum_start + 8;

    // These fixed-size reads cannot panic: the `bytes.len() < HEADER_LEN` guard
    // above proved all header bytes are present. `copy_from_slice` (not
    // `.unwrap()`/`.expect()`) keeps the clippy `unwrap_used`/`expect_used`
    // denials satisfied.
    let mut len_buf = [0u8; 8];
    len_buf.copy_from_slice(&bytes[len_start..checksum_start]);
    let mut sum_buf = [0u8; 8];
    sum_buf.copy_from_slice(&bytes[checksum_start..payload_start]);
    let payload_len_u64 = u64::from_le_bytes(len_buf);
    let stored = u64::from_le_bytes(sum_buf);

    // On a 32-bit host a payload_len beyond usize::MAX cannot address any real
    // buffer, so it is a truncated/corrupt record by definition.
    let Ok(payload_len) = usize::try_from(payload_len_u64) else {
        return Err(WalChecksumError::Truncated {
            have: bytes.len(),
            need: usize::MAX,
        });
    };

    let need = payload_start
        .checked_add(payload_len)
        .ok_or(WalChecksumError::Truncated {
            have: bytes.len(),
            need: usize::MAX,
        })?;
    if bytes.len() < need {
        return Err(WalChecksumError::Truncated {
            have: bytes.len(),
            need,
        });
    }

    let payload = &bytes[payload_start..need];
    let computed = hash_key_bytes(&[payload]);
    if computed != stored {
        return Err(WalChecksumError::ChecksumMismatch { stored, computed });
    }

    Ok(WalPayload::Verified(payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_then_verify_round_trips() {
        let payload = br#"{"table_name":"t","staged_files":["a.vortex"]}"#;
        let framed = frame(payload);
        assert!(framed.starts_with(&MAGIC));
        assert_eq!(framed.len(), HEADER_LEN + payload.len());

        let verified = verify(&framed).expect("valid frame verifies");
        assert_eq!(verified, WalPayload::Verified(payload));
        assert_eq!(verified.bytes(), payload);
    }

    #[test]
    fn empty_payload_round_trips() {
        let framed = frame(b"");
        assert_eq!(verify(&framed).expect("empty verifies").bytes(), b"");
    }

    #[test]
    fn legacy_pure_json_is_returned_verbatim() {
        // A record written before this feature has no MAGIC prefix.
        let legacy = br#"{"table_name":"t","staged_files":[]}"#;
        let payload = verify(legacy).expect("legacy is accepted");
        assert_eq!(payload, WalPayload::Legacy(legacy));
        assert_eq!(payload.bytes(), legacy);
    }

    #[test]
    fn flipped_payload_byte_is_detected() {
        let payload = br#"{"target_snapshot":"snap-1"}"#;
        let mut framed = frame(payload);
        // Flip a byte inside the JSON payload (the kind of corruption that would
        // otherwise still parse as valid JSON with a wrong value).
        let last = framed.len() - 1;
        framed[last] ^= 0x20;
        match verify(&framed) {
            Err(WalChecksumError::ChecksumMismatch { .. }) => {}
            other => panic!("expected ChecksumMismatch, got {other:?}"),
        }
    }

    #[test]
    fn flipped_checksum_byte_is_detected() {
        let payload = br#"{"target_snapshot":"snap-1"}"#;
        let mut framed = frame(payload);
        // Corrupt the stored checksum itself (magic + version + 8-byte length).
        framed[MAGIC.len() + 1 + 8] ^= 0xff;
        assert!(matches!(
            verify(&framed),
            Err(WalChecksumError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn truncated_header_is_detected() {
        let framed = frame(br#"{"a":1}"#);
        // Keep the magic but cut into the header.
        let truncated = &framed[..MAGIC.len() + 2];
        assert!(matches!(
            verify(truncated),
            Err(WalChecksumError::Truncated { .. })
        ));
    }

    #[test]
    fn truncated_payload_tail_is_detected() {
        let framed = frame(br#"{"staged_files":["a.vortex","b.vortex"]}"#);
        // Drop the last few payload bytes: a classic torn/truncated tail.
        let truncated = &framed[..framed.len() - 5];
        assert!(matches!(
            verify(truncated),
            Err(WalChecksumError::Truncated { .. })
        ));
    }

    #[test]
    fn unknown_version_is_rejected() {
        let mut framed = frame(br#"{"a":1}"#);
        framed[MAGIC.len()] = FORMAT_VERSION + 7;
        assert_eq!(
            verify(&framed),
            Err(WalChecksumError::UnsupportedVersion(FORMAT_VERSION + 7))
        );
    }

    #[test]
    fn truncated_to_bare_magic_is_detected() {
        // Exactly the magic and nothing else: shorter than a header.
        assert!(matches!(
            verify(&MAGIC),
            Err(WalChecksumError::Truncated { .. })
        ));
    }
}
