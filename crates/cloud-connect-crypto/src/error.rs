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

//! Errors for the sealed-secret wire crypto.
//!
//! Two rules hold for every variant:
//!
//! 1. **No message ever echoes key material or plaintext.** A parse error that
//!    quoted the input it choked on would put a private key in a log line.
//! 2. **Decrypt failures are indistinguishable.** [`Error::Open`] carries no
//!    cause, because which check failed is exactly what an attacker probing a
//!    recipient would want to learn.

use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display(
        "Failed to address a Cloud Connect sealed secret: {component} is empty. \
         Both layers bind the seal to it, so a payload addressed this way opens nowhere. \
         Set a non-empty {component} on the secret before sealing it."
    ))]
    EmptyComponent { component: &'static str },

    #[snafu(display(
        "Failed to address a Cloud Connect sealed secret: {component} contains a NUL byte. \
         NUL separates the fields of the authenticated data, so accepting one would let a \
         value forge a field boundary. Remove the NUL byte from {component}."
    ))]
    SeparatorInComponent { component: &'static str },

    #[snafu(display(
        "Failed to accept a Cloud Connect encryption key: no public key was announced."
    ))]
    MissingPublicKey,

    #[snafu(display(
        "Failed to accept a Cloud Connect encryption key: unsupported HPKE suite \
         (kem={kem:#06x} kdf={kdf:#06x} aead={aead:#06x}). \
         This build seals to kem={expected_kem:#06x} kdf={expected_kdf:#06x} \
         aead={expected_aead:#06x} only. Upgrade the peer to a build that announces it."
    ))]
    UnsupportedSuite {
        kem: u32,
        kdf: u32,
        aead: u32,
        expected_kem: u32,
        expected_kdf: u32,
        expected_aead: u32,
    },

    #[snafu(display(
        "Failed to accept a Cloud Connect encryption key: the announced public key is not a \
         valid X25519 public key."
    ))]
    InvalidPublicKey,

    #[snafu(display(
        "Failed to accept a Cloud Connect encryption key: the announced key_id does not match \
         the announced public key."
    ))]
    KeyIdMismatch,

    #[snafu(display(
        "Failed to seal a Cloud Connect secret payload: the payload is {len} bytes, over the \
         {limit}-byte limit. Reduce the number or the size of the values in the secret."
    ))]
    PayloadTooLarge { len: usize, limit: usize },

    // Separate from `Open` on purpose. This one is decided from the length
    // alone, before any key agreement or decryption happens, and the length is
    // something the sender already knows — so naming it leaks nothing and is
    // the difference between a diagnosable rejection and an unexplained one.
    #[snafu(display(
        "Failed to open a Cloud Connect sealed secret payload: the sealed payload is {len} bytes, \
         over the {limit}-byte limit, and was rejected without being decrypted. A payload this \
         size cannot have been produced by a conforming sealer."
    ))]
    SealedPayloadTooLarge { len: usize, limit: usize },

    #[snafu(display("Failed to seal a Cloud Connect secret payload: HPKE encryption failed."))]
    Seal,

    // Deliberately causeless: see the module docs.
    #[snafu(display("Failed to open a Cloud Connect sealed secret payload."))]
    Open,

    #[snafu(display(
        "Failed to load a Cloud Connect encryption private key: expected an RFC 8410 X25519 \
         PKCS#8 private key in PEM form."
    ))]
    InvalidPrivateKey,

    #[snafu(display(
        "Failed to generate a Cloud Connect encryption keypair: the operating system randomness \
         source is unavailable: {source}"
    ))]
    Randomness { source: getrandom::Error },
}
