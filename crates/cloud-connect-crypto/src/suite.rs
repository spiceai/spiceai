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

//! The fixed HPKE (RFC 9180) suite.
//!
//! Both seal layers use the same suite and the same `info` label; they are
//! domain-separated by their recipient keys and by the arity of their AAD (see
//! [`crate::SecretAddress`]). Every value here is part of the wire contract:
//! changing one makes previously sealed ciphertext un-openable, so a change
//! belongs in a new `info` label, not in an edit to this one.

/// HPKE `info` domain-separation label, as text. The version suffix is what a
/// future change to any of these parameters moves; the label is never edited in
/// place, because ciphertext already in flight is bound to it.
pub const HPKE_INFO_LABEL: &str = "spice-cloud-connect/secrets/v1";

/// [`HPKE_INFO_LABEL`] as the bytes HPKE takes. Every implementation must use
/// these bytes exactly.
pub const HPKE_INFO: &[u8] = HPKE_INFO_LABEL.as_bytes();

/// RFC 9180 registry id for the KEM: `DHKEM(X25519, HKDF-SHA256)`.
pub const KEM_ID: u32 = 0x0020;
/// RFC 9180 registry id for the KDF: `HKDF-SHA256`.
pub const KDF_ID: u32 = 0x0001;
/// RFC 9180 registry id for the AEAD: `ChaCha20-Poly1305`.
pub const AEAD_ID: u32 = 0x0003;

/// Bytes the AEAD appends to a ciphertext: the Poly1305 authentication tag.
pub const AEAD_TAG_LEN: usize = 16;

/// Cap on the **secret payload** — the inner seal's plaintext. Matches the
/// Kubernetes `Secret` size limit, which is the real constraint: the payload has
/// to fit in one once applied.
pub const MAX_SECRET_PLAINTEXT_SIZE: usize = 1 << 20;

/// Headroom over [`MAX_SECRET_PLAINTEXT_SIZE`] for what the two seal layers add
/// on top of the secret itself — an encapsulated key and an AEAD tag per layer,
/// plus the inner envelope's own framing.
pub const SEALED_OVERHEAD_HEADROOM: usize = 1024;

/// Cap a recipient applies to a sealed blob **as it arrives**, before spending
/// anything on it. Applied to the outer blob, which bounds the inner envelope
/// nested inside it.
///
/// The caps form a chain that has to close, or a secret at exactly the
/// Kubernetes limit becomes undeliverable: the outer seal's plaintext is the
/// serialized inner envelope, which is *necessarily larger* than the secret it
/// wraps, so it cannot be held to [`MAX_SECRET_PLAINTEXT_SIZE`]. See
/// [`crate::SealLayer`], which is what keeps each layer to the right cap, and
/// `the_size_caps_let_a_maximum_sized_secret_through_both_layers` for the
/// arithmetic.
pub const MAX_SEALED_SECRETS_SIZE: usize = MAX_SECRET_PLAINTEXT_SIZE + SEALED_OVERHEAD_HEADROOM;

/// Length of a raw X25519 key, public or private (RFC 7748).
pub const X25519_KEY_LEN: usize = 32;

pub(crate) type Kem = hpke::kem::X25519HkdfSha256;
pub(crate) type Kdf = hpke::kdf::HkdfSha256;
pub(crate) type Aead = hpke::aead::ChaCha20Poly1305;

#[cfg(test)]
mod tests {
    use super::*;

    /// The suite ids are the ones announced on the wire, and the `info` label
    /// is what both sides feed HPKE. A silent edit to any of them strands every
    /// ciphertext already in flight, so pin the literals.
    #[test]
    fn the_wire_contract_constants_are_pinned() {
        assert_eq!(HPKE_INFO_LABEL, "spice-cloud-connect/secrets/v1");
        assert_eq!(HPKE_INFO, b"spice-cloud-connect/secrets/v1");
        assert_eq!(KEM_ID, 32);
        assert_eq!(KDF_ID, 1);
        assert_eq!(AEAD_ID, 3);
        assert_eq!(MAX_SECRET_PLAINTEXT_SIZE, 1_048_576);
        assert_eq!(MAX_SEALED_SECRETS_SIZE, 1_049_600);
    }
}
