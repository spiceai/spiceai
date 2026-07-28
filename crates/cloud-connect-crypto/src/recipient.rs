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

//! The sealer side: a validated recipient public key, and `seal`.

use hpke::{Deserializable, Kem as KemTrait, OpModeS, Serializable};
use snafu::ensure;

use crate::error::{
    InvalidPublicKeySnafu, KeyIdMismatchSnafu, MissingPublicKeySnafu, PayloadTooLargeSnafu, Result,
    UnsupportedSuiteSnafu,
};
use crate::key_id::derive_key_id;
use crate::suite::{AEAD_ID, Aead, HPKE_INFO, KDF_ID, KEM_ID, Kdf, Kem, MAX_SECRET_PLAINTEXT_SIZE};

/// One HPKE seal: the encapsulated key and the ciphertext, in the shape the
/// wire carries them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sealed {
    /// The encapsulated KEM key. Fresh for every seal, which is what makes each
    /// ciphertext unique even when the plaintext and the AAD repeat.
    pub enc: Vec<u8>,
    /// The AEAD ciphertext, tag included.
    pub ciphertext: Vec<u8>,
}

/// A recipient public key that has been checked against the fixed suite and
/// against its own `key_id`.
///
/// The type exists so those checks cannot be skipped: [`Self::seal`] is only
/// reachable through one of the constructors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecipientKey {
    key_id: String,
    public_key: Vec<u8>,
}

impl RecipientKey {
    /// Accept a public key on its own, deriving the `key_id` from it.
    ///
    /// Use this where the key did not arrive with an announcement — the
    /// enrolled encryption key read back from storage, say.
    ///
    /// # Errors
    /// Returns [`crate::Error::MissingPublicKey`] for an empty key and
    /// [`crate::Error::InvalidPublicKey`] when the bytes are not a valid X25519
    /// public key.
    pub fn from_public_key(public_key: &[u8]) -> Result<Self> {
        ensure!(!public_key.is_empty(), MissingPublicKeySnafu);
        ensure!(
            <Kem as KemTrait>::PublicKey::from_bytes(public_key).is_ok(),
            InvalidPublicKeySnafu
        );
        Ok(Self {
            key_id: derive_key_id(public_key),
            public_key: public_key.to_vec(),
        })
    }

    /// Accept a key a peer announced, validating the suite it names and the
    /// `key_id` it claims.
    ///
    /// The `key_id` is checked rather than trusted: it is what a recipient
    /// looks the private half up by, so an announcement whose id does not
    /// derive from its own key would seal to something the recipient cannot
    /// find, and fail closed at the far end with nothing to point at.
    ///
    /// # Errors
    /// Returns [`crate::Error::MissingPublicKey`],
    /// [`crate::Error::UnsupportedSuite`], [`crate::Error::InvalidPublicKey`],
    /// or [`crate::Error::KeyIdMismatch`].
    pub fn from_announcement(
        key_id: &str,
        kem: u32,
        kdf: u32,
        aead: u32,
        public_key: &[u8],
    ) -> Result<Self> {
        ensure!(!public_key.is_empty(), MissingPublicKeySnafu);
        ensure!(
            kem == KEM_ID && kdf == KDF_ID && aead == AEAD_ID,
            UnsupportedSuiteSnafu {
                kem,
                kdf,
                aead,
                expected_kem: KEM_ID,
                expected_kdf: KDF_ID,
                expected_aead: AEAD_ID,
            }
        );
        let key = Self::from_public_key(public_key)?;
        ensure!(key.key_id == key_id, KeyIdMismatchSnafu);
        Ok(key)
    }

    /// Build from parts already known to be valid — the public half of a
    /// keypair this process generated.
    pub(crate) fn from_validated_parts(key_id: String, public_key: Vec<u8>) -> Self {
        Self { key_id, public_key }
    }

    /// The `key_id` a sealed payload names so the recipient can select the
    /// matching private key.
    #[must_use]
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// The raw public key bytes.
    #[must_use]
    pub fn public_key(&self) -> &[u8] {
        &self.public_key
    }

    /// HPKE-seal `plaintext` to this key (base mode, single-shot: a fresh
    /// encapsulation per message, so there is no context state to carry across
    /// reconnects).
    ///
    /// `plaintext` is opaque here. The outer layer seals an envelope whose
    /// contents the sealer cannot read, and neither layer's sealer should ever
    /// log what it was handed.
    ///
    /// # Errors
    /// Returns [`crate::Error::PayloadTooLarge`] when `plaintext` exceeds
    /// [`MAX_SECRET_PLAINTEXT_SIZE`], or [`crate::Error::Seal`] when HPKE
    /// fails.
    pub fn seal(&self, plaintext: &[u8], aad: &[u8]) -> Result<Sealed> {
        ensure!(
            plaintext.len() <= MAX_SECRET_PLAINTEXT_SIZE,
            PayloadTooLargeSnafu {
                len: plaintext.len(),
                limit: MAX_SECRET_PLAINTEXT_SIZE,
            }
        );
        let public_key = <Kem as KemTrait>::PublicKey::from_bytes(&self.public_key)
            .map_err(|_| crate::Error::InvalidPublicKey)?;
        let (enc, ciphertext) = hpke::single_shot_seal::<Aead, Kdf, Kem>(
            &OpModeS::Base,
            &public_key,
            HPKE_INFO,
            plaintext,
            aad,
        )
        .map_err(|_| crate::Error::Seal)?;
        Ok(Sealed {
            enc: enc.to_bytes().to_vec(),
            ciphertext,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EncryptionKeypair;
    use crate::error::Error;

    fn fixture_public_key() -> Vec<u8> {
        hex::decode("49e4874e25fe389ed3c9fa2fd09d077907ecc5809c8619e2127128b6a72c7c7a")
            .expect("fixture public key hex")
    }

    #[test]
    fn from_public_key_derives_the_canonical_key_id() {
        let key = RecipientKey::from_public_key(&fixture_public_key()).expect("valid key");
        assert_eq!(key.key_id(), "c8dea1881b739590");
        assert_eq!(key.public_key(), fixture_public_key());
    }

    #[test]
    fn an_announcement_is_validated_not_trusted() {
        let public_key = fixture_public_key();
        let key_id = derive_key_id(&public_key);
        RecipientKey::from_announcement(&key_id, KEM_ID, KDF_ID, AEAD_ID, &public_key)
            .expect("a well-formed announcement is accepted");

        assert!(matches!(
            RecipientKey::from_announcement(&key_id, 0x0010, KDF_ID, AEAD_ID, &public_key),
            Err(Error::UnsupportedSuite { kem: 0x0010, .. })
        ));
        assert!(matches!(
            RecipientKey::from_announcement(&key_id, KEM_ID, 0x0002, AEAD_ID, &public_key),
            Err(Error::UnsupportedSuite { kdf: 0x0002, .. })
        ));
        assert!(matches!(
            RecipientKey::from_announcement(&key_id, KEM_ID, KDF_ID, 0x0001, &public_key),
            Err(Error::UnsupportedSuite { aead: 0x0001, .. })
        ));
        assert!(matches!(
            RecipientKey::from_announcement(
                "deadbeefdeadbeef",
                KEM_ID,
                KDF_ID,
                AEAD_ID,
                &public_key
            ),
            Err(Error::KeyIdMismatch)
        ));
        assert!(matches!(
            RecipientKey::from_announcement(&key_id, KEM_ID, KDF_ID, AEAD_ID, &[]),
            Err(Error::MissingPublicKey)
        ));
        assert!(matches!(
            RecipientKey::from_announcement(&key_id, KEM_ID, KDF_ID, AEAD_ID, &[7u8; 31]),
            Err(Error::InvalidPublicKey)
        ));
    }

    #[test]
    fn seal_round_trips_through_the_matching_private_key() {
        let keypair = EncryptionKeypair::derive(b"seal-round-trip");
        let key = RecipientKey::from_public_key(keypair.public_key()).expect("valid key");
        let sealed = key.seal(b"payload-bytes", b"aad-bytes").expect("seal");

        assert_eq!(key.key_id(), keypair.key_id());
        let opened = keypair
            .open(&sealed.enc, &sealed.ciphertext, b"aad-bytes")
            .expect("open");
        assert_eq!(opened.as_slice(), b"payload-bytes");
    }

    /// A fresh encapsulation per seal is what keeps two identical payloads from
    /// producing identical ciphertext.
    #[test]
    fn each_seal_encapsulates_a_fresh_key() {
        let keypair = EncryptionKeypair::derive(b"fresh-encapsulation");
        let key = keypair.recipient();
        let first = key.seal(b"same", b"same").expect("seal");
        let second = key.seal(b"same", b"same").expect("seal");
        assert_ne!(first.enc, second.enc);
        assert_ne!(first.ciphertext, second.ciphertext);
    }

    #[test]
    fn a_payload_over_the_cap_is_rejected_before_it_is_sealed() {
        let key = EncryptionKeypair::derive(b"size-cap").recipient();
        key.seal(&vec![0u8; MAX_SECRET_PLAINTEXT_SIZE], b"aad")
            .expect("the cap itself must be accepted");
        assert!(matches!(
            key.seal(&vec![0u8; MAX_SECRET_PLAINTEXT_SIZE + 1], b"aad"),
            Err(Error::PayloadTooLarge {
                limit: MAX_SECRET_PLAINTEXT_SIZE,
                ..
            })
        ));
    }
}
