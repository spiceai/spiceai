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

//! Sealed-secret wire crypto for Spice Cloud Connect.
//!
//! A Cloud Connect secret payload is sealed **twice** with HPKE (RFC 9180) and
//! opened by the receiving instance:
//!
//! - **Inner** — the control plane seals the secret payload to the instance's
//!   *enrolled* X25519 encryption key, the public half of which the instance
//!   registers when it enrolls and rotates on every renewal. No component
//!   between the control plane and the instance can open it.
//! - **Outer** — the gateway seals that already-sealed envelope to the
//!   instance's *per-connection* key, announced on the stream and held in
//!   memory only. It is sealing opaque bytes it cannot read, which keeps
//!   recorded ciphertext undecryptable even if the persisted enrolled key is
//!   later compromised.
//!
//! There is no plaintext delivery path: a payload that fails to open at either
//! layer is rejected.
//!
//! # Why this is a crate
//!
//! Three implementations have to agree on these bytes *exactly* — the sealer of
//! each layer and the opener of both. When each keeps its own copy of ~20 lines
//! of byte concatenation, a component that starts canonicalising one field
//! differently produces ciphertext that opens nowhere, and HPKE reports only
//! "authentication failed": there is nothing in the failure that points at the
//! field that diverged. That failure mode has occurred more than once.
//!
//! This crate is the single Rust source for the parts every implementation must
//! agree on:
//!
//! - the HPKE suite and `info` label ([`suite`]),
//! - `key_id` derivation ([`derive_key_id`]),
//! - both AAD forms **and the canonicalisation of their inputs**
//!   ([`SecretAddress`]),
//! - the recipient keypair, its PEM encodings, and `open` ([`EncryptionKeypair`],
//!   [`EncryptionKeyring`]),
//! - the sealer side ([`RecipientKey::seal`]).
//!
//! [`SecretAddress`] is the part that closes the class of bug above by
//! construction rather than by convention: it is the only way to build either
//! AAD, it canonicalises its inputs on the way in, and both forms are derived
//! from the same canonical value, so the two layers cannot disagree.
//!
//! # Implementations that are not Rust
//!
//! A Rust crate cannot single-source an implementation written in another
//! language. For those, this crate is the *normative reference* and emits a
//! language-neutral conformance suite — see [`vectors`] and
//! `testdata/conformance_vectors.json`. An implementation in any language
//! asserts byte equality against that artifact in its own CI, so a divergence
//! fails there instead of surfacing as an undiagnosable open failure in a
//! customer's cluster.
//!
//! # Nothing here is environment-specific
//!
//! Key generation, key encoding, `key_id` derivation, the AADs, `seal`, and
//! `open` are usable by any sealer or recipient. Persistence, transport, and
//! the proto types are the caller's.

mod aad;
mod encoding;
mod error;
mod key_id;
mod keypair;
mod recipient;
pub mod suite;
pub mod vectors;

pub use aad::{
    AAD_SEPARATOR, STANDALONE_SECRETS_NAME, STANDALONE_SECRETS_NAMESPACE, SecretAddress,
};
pub use error::{Error, Result};
pub use key_id::{KEY_ID_DIGEST_BYTES, derive_key_id};
pub use keypair::{EncryptionKeypair, EncryptionKeyring};
pub use recipient::{RecipientKey, SealLayer, Sealed};
pub use suite::{
    AEAD_ID, AEAD_TAG_LEN, HPKE_INFO, KDF_ID, KEM_ID, MAX_SEALED_SECRETS_SIZE,
    MAX_SECRET_PLAINTEXT_SIZE,
};
