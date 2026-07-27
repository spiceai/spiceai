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

//! Per-file end-to-end integrity digests for Cayenne Vortex data files.
//!
//! The Vortex file format has no format-level per-file checksum today (only
//! upstream TODOs), so Cayenne computes a digest of each published data file's
//! bytes at flush and stores it in the snapshot manifest
//! (`cayenne_snapshot_file.digest`), then verifies it before the file is first
//! scanned. On mismatch the read fails as a *detected fault* rather than
//! returning silently-wrong rows — the worst failure mode for a system of
//! record fed by CDC.
//!
//! The stored digest is **self-describing** — `"<algorithm>:<lowercase-hex>"`
//! (e.g. `"xxh3-128:1a2b…"`) — so the algorithm can evolve without a schema
//! migration and an older binary can tell "a digest it cannot recompute" apart
//! from "no digest".

use hash_index::hash_key_128;

/// Algorithm tag embedded in the `<algo>:<hex>` digest string. XXH3-128 is a
/// fast, well-distributed non-cryptographic hash (the threat here is accidental
/// bit-rot / a bad object-store round-trip, not an adversary) that is already a
/// Cayenne dependency via `hash_index`, so it adds no new crate.
const DIGEST_ALGORITHM: &str = "xxh3-128";

/// Compute the integrity digest of `bytes`, formatted as
/// `"xxh3-128:<32-lowercase-hex>"`.
#[must_use]
pub(crate) fn compute(bytes: &[u8]) -> String {
    format!("{DIGEST_ALGORITHM}:{:032x}", hash_key_128(bytes))
}

/// The result of checking file bytes against a stored digest string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DigestCheck {
    /// The recomputed digest matches the stored value.
    Match,
    /// The recomputed digest differs — corruption detected.
    Mismatch,
    /// The stored digest is malformed or uses an algorithm this build cannot
    /// recompute. Verification is skipped (fail-open) rather than reported as a
    /// false corruption, so version skew never turns into a spurious read fault.
    Unsupported,
}

/// Check `bytes` against a previously stored digest string.
#[must_use]
pub(crate) fn check(stored: &str, bytes: &[u8]) -> DigestCheck {
    let Some((algorithm, hex)) = stored.split_once(':') else {
        return DigestCheck::Unsupported;
    };
    if algorithm != DIGEST_ALGORITHM {
        return DigestCheck::Unsupported;
    }
    // A well-formed xxh3-128 digest is exactly 32 lowercase hex chars (the
    // canonical form `compute` emits). A value that is prefixed but malformed
    // (wrong length, non-hex, or uppercase) is treated as unverifiable
    // (fail-open) rather than reported as a false corruption — the stored digest
    // itself being garbled is not evidence the file's bytes are wrong.
    if hex.len() != 32 || !hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return DigestCheck::Unsupported;
    }
    if compute(bytes) == stored {
        DigestCheck::Match
    } else {
        DigestCheck::Mismatch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_is_deterministic_and_prefixed() {
        let a = compute(b"hello world");
        let b = compute(b"hello world");
        assert_eq!(a, b);
        assert!(a.starts_with("xxh3-128:"));
        // 9-char prefix ("xxh3-128:") + 32 hex chars.
        assert_eq!(a.len(), "xxh3-128:".len() + 32);
    }

    #[test]
    fn distinct_inputs_differ() {
        assert_ne!(compute(b"payload-a"), compute(b"payload-b"));
        // A single flipped bit changes the digest.
        assert_ne!(
            compute(&[0u8; 16]),
            compute(&[1u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        );
    }

    #[test]
    fn check_matches_own_digest() {
        let bytes = b"the quick brown fox";
        let digest = compute(bytes);
        assert_eq!(check(&digest, bytes), DigestCheck::Match);
    }

    #[test]
    fn check_detects_corruption() {
        let bytes = b"the quick brown fox";
        let digest = compute(bytes);
        assert_eq!(
            check(&digest, b"the quick brown FOX"),
            DigestCheck::Mismatch
        );
    }

    #[test]
    fn check_unknown_algorithm_is_unsupported() {
        // A digest we cannot recompute must not be reported as corruption.
        assert_eq!(
            check("sha256:deadbeef", b"anything"),
            DigestCheck::Unsupported
        );
        assert_eq!(check("not-a-digest", b"anything"), DigestCheck::Unsupported);
        assert_eq!(check("", b"anything"), DigestCheck::Unsupported);
    }

    #[test]
    fn check_malformed_prefixed_digest_is_unsupported() {
        // Our prefix but a malformed hex body (too short, non-hex, uppercase)
        // is unverifiable — fail-open, not a false corruption report.
        assert_eq!(check("xxh3-128:deadbeef", b"x"), DigestCheck::Unsupported); // too short
        assert_eq!(
            check("xxh3-128:ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ", b"x"),
            DigestCheck::Unsupported // non-hex
        );
        // A well-formed digest for different bytes is a real mismatch, not
        // unsupported.
        let digest = compute(b"real bytes");
        assert_eq!(check(&digest, b"other bytes"), DigestCheck::Mismatch);
    }
}
