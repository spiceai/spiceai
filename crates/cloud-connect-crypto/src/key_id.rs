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

//! `key_id` derivation.

use sha2::{Digest, Sha256};

/// Bytes of the SHA-256 digest that a `key_id` renders. 8 bytes → 16 hex
/// characters, which names the recipient key well enough for a keyring lookup
/// without pretending to be a collision-resistant identifier — the seal binds
/// to the key itself, not to this string.
pub const KEY_ID_DIGEST_BYTES: usize = 8;

/// Canonical key identifier: lowercase hex of the first
/// [`KEY_ID_DIGEST_BYTES`] bytes of SHA-256 over the **raw** KEM public key
/// (32 bytes for X25519 — not the SPKI DER, and not a PEM document).
///
/// Every implementation derives it the same way, so it is safe to send on the
/// wire and to recompute rather than trust: a recipient looks the key up by it,
/// and a sealer checks an announced `key_id` against the announced public key
/// before using either.
#[must_use]
pub fn derive_key_id(public_key: &[u8]) -> String {
    let digest = Sha256::digest(public_key);
    hex::encode(&digest[..KEY_ID_DIGEST_BYTES])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the derivation against a fixed public key. Every implementation
    /// must produce this string for these bytes; the same pair is the first
    /// entry of the conformance suite.
    #[test]
    fn key_id_is_the_first_eight_digest_bytes_in_hex() {
        let public_key =
            hex::decode("49e4874e25fe389ed3c9fa2fd09d077907ecc5809c8619e2127128b6a72c7c7a")
                .expect("fixture public key hex");
        let key_id = derive_key_id(&public_key);
        assert_eq!(key_id, "c8dea1881b739590");
        assert_eq!(key_id.len(), KEY_ID_DIGEST_BYTES * 2);
        assert_eq!(
            key_id,
            hex::encode(&Sha256::digest(&public_key)[..KEY_ID_DIGEST_BYTES])
        );
    }

    /// The digest is over the raw key bytes, so any re-encoding of the same key
    /// (SPKI DER, PEM) derives a different id — a mismatch a recipient reports
    /// as "sealed to a key I do not hold" with no hint at the cause. Pin the
    /// input form.
    #[test]
    fn key_id_is_derived_from_the_raw_key_not_an_encoding_of_it() {
        let public_key = [7u8; 32];
        let spki_wrapped = [b"\x30\x2a\x30\x05".as_slice(), &public_key].concat();
        assert_ne!(derive_key_id(&public_key), derive_key_id(&spki_wrapped));
    }
}
