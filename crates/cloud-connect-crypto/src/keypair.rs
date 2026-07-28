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

//! The recipient side: the X25519 keypair a payload is sealed to, its
//! encodings, and `open`.
//!
//! An instance holds two kinds of these. The **enrolled** keypair is persisted
//! and its public half registered with the control plane, which seals the inner
//! layer to it; it rotates on every credential renewal. The **per-connection**
//! keypair is generated at connect time, announced on the stream, and never
//! persisted; the gateway seals the outer layer to it. Both are the same type
//! — what differs is who holds the public half and for how long.
//!
//! The identity keypair is *not* one of these: it is ECDSA P-256, a signing key
//! that cannot decrypt. Keeping the two separate is deliberate — the signing
//! scalar never enters the key-agreement path, so a key-extraction flaw in
//! decryption cannot escalate into forging the renewal proof-of-possession,
//! which would survive the next rotation.

use hpke::{Deserializable, Kem as KemTrait, OpModeR, Serializable};
use snafu::{OptionExt as _, ResultExt as _, ensure};
use zeroize::Zeroizing;

use crate::encoding::{
    PRIVATE_KEY_PEM_TAG, PUBLIC_KEY_PEM_TAG, X25519_PKCS8_PREFIX, encode_pem, pem_len, spki_der,
    write_pem,
};
use crate::error::{InvalidPrivateKeySnafu, RandomnessSnafu, Result, SealedPayloadTooLargeSnafu};
use crate::key_id::derive_key_id;
use crate::recipient::RecipientKey;
use crate::suite::{Aead, HPKE_INFO, Kdf, Kem, MAX_SEALED_SECRETS_SIZE, X25519_KEY_LEN};

/// Bytes of input keying material fed to the KEM's deterministic key
/// derivation. 32 bytes matches the KEM's own secret size — more would not add
/// entropy the KEM can carry.
const KEYGEN_IKM_LEN: usize = 32;

/// An X25519 HPKE recipient keypair.
pub struct EncryptionKeypair {
    private_key: <Kem as KemTrait>::PrivateKey,
    public_key_bytes: Vec<u8>,
    key_id: String,
}

/// Prints the key id only. The private key must never reach a formatter.
impl std::fmt::Debug for EncryptionKeypair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionKeypair")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl EncryptionKeypair {
    /// Generate a fresh keypair from operating-system randomness.
    ///
    /// # Errors
    /// Returns [`crate::Error::Randomness`] when the OS randomness source
    /// fails. Deriving a keypair from a fallback is never correct here, so this
    /// is a hard failure rather than a degraded mode.
    pub fn generate() -> Result<Self> {
        let mut ikm = Zeroizing::new([0u8; KEYGEN_IKM_LEN]);
        getrandom::fill(ikm.as_mut()).context(RandomnessSnafu)?;
        Ok(Self::derive(ikm.as_ref()))
    }

    /// Derive a keypair deterministically from input keying material.
    ///
    /// Exposed for the conformance vectors and for tests, which need a keypair
    /// that is the same on every run so a failure is reproducible. Production
    /// key generation is [`Self::generate`]: `ikm` here is only as
    /// unpredictable as whatever produced it.
    #[must_use]
    pub fn derive(ikm: &[u8]) -> Self {
        let (private_key, public_key) = Kem::derive_keypair(ikm);
        Self::from_parts(private_key, &public_key)
    }

    /// Reconstruct a keypair from raw private key bytes (32 bytes for X25519).
    ///
    /// # Errors
    /// Returns [`crate::Error::InvalidPrivateKey`] when the bytes are not a
    /// valid X25519 private key. The message never echoes the input.
    pub fn from_private_key_bytes(bytes: &[u8]) -> Result<Self> {
        let private_key = <Kem as KemTrait>::PrivateKey::from_bytes(bytes)
            .ok()
            .context(InvalidPrivateKeySnafu)?;
        let public_key = Kem::sk_to_pk(&private_key);
        Ok(Self::from_parts(private_key, &public_key))
    }

    fn from_parts(
        private_key: <Kem as KemTrait>::PrivateKey,
        public_key: &<Kem as KemTrait>::PublicKey,
    ) -> Self {
        let public_key_bytes = public_key.to_bytes().to_vec();
        let key_id = derive_key_id(&public_key_bytes);
        Self {
            private_key,
            public_key_bytes,
            key_id,
        }
    }

    /// Restore a keypair from a PKCS#8 PEM private key.
    ///
    /// The decoded DER is moved into a zeroizing buffer rather than copied, but
    /// note the asymmetry with [`Self::to_pkcs8_pem`]: whatever the PEM parser
    /// allocates internally is outside this crate's reach, so this direction is
    /// best-effort where the emitting direction is exact.
    ///
    /// # Errors
    /// Returns [`crate::Error::InvalidPrivateKey`] when `pem` is not an
    /// RFC 8410 X25519 PKCS#8 private key. The message never echoes the input.
    pub fn from_pkcs8_pem(pem: &str) -> Result<Self> {
        let parsed = pem::parse(pem).ok().context(InvalidPrivateKeySnafu)?;
        ensure!(parsed.tag() == PRIVATE_KEY_PEM_TAG, InvalidPrivateKeySnafu);
        let der = Zeroizing::new(parsed.into_contents());
        ensure!(
            der.len() == X25519_PKCS8_PREFIX.len() + X25519_KEY_LEN
                && der.starts_with(&X25519_PKCS8_PREFIX),
            InvalidPrivateKeySnafu
        );
        Self::from_private_key_bytes(&der[X25519_PKCS8_PREFIX.len()..])
    }

    /// Serialize the private key as PKCS#8 PEM, for persistence.
    ///
    /// No buffer holding the encoded key is ever freed without being zeroized:
    /// the returned buffer zeroizes on drop, and the DER it is built from lives
    /// only in zeroizing buffers. A caller that wants a `secrecy::SecretString`
    /// should build one from `as_str()` — that allocates exactly the length it
    /// needs, so the conversion does not strand a copy in a grown allocation,
    /// and the buffer here is wiped when it drops.
    #[must_use]
    pub fn to_pkcs8_pem(&self) -> Zeroizing<String> {
        let raw = Zeroizing::new(self.private_key.to_bytes().to_vec());
        let mut der = Zeroizing::new(Vec::with_capacity(
            X25519_PKCS8_PREFIX.len() + X25519_KEY_LEN,
        ));
        der.extend_from_slice(&X25519_PKCS8_PREFIX);
        der.extend_from_slice(&raw);

        let mut pem = Zeroizing::new(String::with_capacity(pem_len(PRIVATE_KEY_PEM_TAG, &der)));
        write_pem(&mut pem, PRIVATE_KEY_PEM_TAG, &der);
        pem
    }

    /// Serialize the public key as SPKI PEM — the form the control plane stores
    /// at enrollment and seals the inner layer to.
    #[must_use]
    pub fn public_key_spki_pem(&self) -> String {
        encode_pem(PUBLIC_KEY_PEM_TAG, &spki_der(&self.public_key_bytes))
    }

    /// The raw public key bytes, as announced on the wire and as
    /// [`derive_key_id`] hashes them.
    #[must_use]
    pub fn public_key(&self) -> &[u8] {
        &self.public_key_bytes
    }

    /// The `key_id` a sealed payload names to select this key.
    #[must_use]
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// This keypair's public half, as a sealer would hold it. Used by tests and
    /// by any component that seals to itself; production sealers build a
    /// [`RecipientKey`] from what the recipient announced.
    #[must_use]
    pub fn recipient(&self) -> RecipientKey {
        RecipientKey::from_validated_parts(self.key_id.clone(), self.public_key_bytes.clone())
    }

    /// Open a sealed payload (HPKE base mode, single-shot). The returned buffer
    /// zeroizes on drop.
    ///
    /// `ciphertext` is bounded by [`MAX_SEALED_SECRETS_SIZE`] before anything is
    /// spent on it. The AEAD tag is at the *end*, so a peer that sends a
    /// gigabyte of noise would otherwise get a gigabyte allocated and decrypted
    /// before the tag check rejects it. The cap is applied here rather than left
    /// to the caller because every recipient needs it and this is the one place
    /// they all share; a caller that also bounds the message on arrival is not
    /// duplicating anything that hurts.
    ///
    /// # Errors
    /// Returns [`crate::Error::SealedPayloadTooLarge`] when `ciphertext` is over
    /// the cap, and [`crate::Error::Open`] on any other failure. `Open` carries
    /// no cause: which check failed is what an attacker probing a recipient
    /// would want to learn.
    pub fn open(&self, enc: &[u8], ciphertext: &[u8], aad: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        ensure!(
            ciphertext.len() <= MAX_SEALED_SECRETS_SIZE,
            SealedPayloadTooLargeSnafu {
                len: ciphertext.len(),
                limit: MAX_SEALED_SECRETS_SIZE,
            }
        );
        let encapped =
            <Kem as KemTrait>::EncappedKey::from_bytes(enc).map_err(|_| crate::Error::Open)?;
        hpke::single_shot_open::<Aead, Kdf, Kem>(
            &OpModeR::Base,
            &self.private_key,
            &encapped,
            HPKE_INFO,
            ciphertext,
            aad,
        )
        .map(Zeroizing::new)
        .map_err(|_| crate::Error::Open)
    }
}

/// The enrolled encryption keys an instance holds: the key the control plane
/// currently seals to, plus the one it superseded at the last renewal.
///
/// The control plane switches to a rotated key the moment it commits the
/// renewal, so a payload sealed moments earlier can arrive after the instance
/// has rotated. Keeping the superseded key for one renewal interval covers that
/// window; nothing older is retained.
pub struct EncryptionKeyring {
    current: EncryptionKeypair,
    previous: Option<EncryptionKeypair>,
}

/// Prints key ids only.
impl std::fmt::Debug for EncryptionKeyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionKeyring")
            .field("current", &self.current.key_id())
            .field(
                "previous",
                &self.previous.as_ref().map(EncryptionKeypair::key_id),
            )
            .finish()
    }
}

impl EncryptionKeyring {
    #[must_use]
    pub fn new(current: EncryptionKeypair, previous: Option<EncryptionKeypair>) -> Self {
        Self { current, previous }
    }

    /// Restore the keyring from the persisted PKCS#8 PEMs.
    ///
    /// # Errors
    /// Returns [`crate::Error::InvalidPrivateKey`] when either PEM is not a
    /// usable X25519 key — an unusable one is an error, not a silently smaller
    /// keyring, because a keyring missing its previous key fails closed on
    /// exactly the payloads the overlap exists to cover.
    pub fn from_pkcs8_pems(current: &str, previous: Option<&str>) -> Result<Self> {
        Ok(Self::new(
            EncryptionKeypair::from_pkcs8_pem(current)?,
            previous
                .map(EncryptionKeypair::from_pkcs8_pem)
                .transpose()?,
        ))
    }

    /// The key a sealed payload's `key_id` names — the current key first, then
    /// the superseded one. `None` when it names neither, which is a fail-closed
    /// rejection: the payload was sealed to a key this instance does not hold.
    #[must_use]
    pub fn select(&self, key_id: &str) -> Option<&EncryptionKeypair> {
        if self.current.key_id() == key_id {
            return Some(&self.current);
        }
        self.previous
            .as_ref()
            .filter(|previous| previous.key_id() == key_id)
    }

    /// The key currently enrolled with the control plane.
    #[must_use]
    pub fn current(&self) -> &EncryptionKeypair {
        &self.current
    }

    /// The `key_id` of [`Self::current`].
    #[must_use]
    pub fn current_key_id(&self) -> &str {
        self.current.key_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::suite::MAX_SEALED_SECRETS_SIZE;
    use crate::{SealLayer, SecretAddress};

    fn address(key_id: &str) -> SecretAddress {
        SecretAddress::new("cell_x", "publicorg-7", "spicepod-secrets", key_id)
            .expect("a well-formed address")
    }

    #[test]
    fn round_trip_and_aad_binding() {
        let keypair = EncryptionKeypair::generate().expect("keygen");
        assert_eq!(keypair.key_id(), derive_key_id(keypair.public_key()));

        let address = address(keypair.key_id());
        let aad = address.outer_aad("ctrl-1").expect("outer aad");
        let sealed = keypair
            .recipient()
            .seal(SealLayer::Inner, b"super-secret", &aad)
            .expect("seal");

        let opened = keypair
            .open(&sealed.enc, &sealed.ciphertext, &aad)
            .expect("open");
        assert_eq!(opened.as_slice(), b"super-secret");

        // A payload addressed to a different instance must not open.
        let elsewhere = SecretAddress::new(
            "cell_y",
            "publicorg-7",
            "spicepod-secrets",
            keypair.key_id(),
        )
        .expect("address")
        .outer_aad("ctrl-1")
        .expect("outer aad");
        assert!(matches!(
            keypair.open(&sealed.enc, &sealed.ciphertext, &elsewhere),
            Err(Error::Open)
        ));
    }

    /// The AEAD tag is at the end of the ciphertext, so without a length check
    /// first, a peer sending noise gets all of it allocated and decrypted before
    /// the tag rejects it. The cap has to be enforced from the length alone,
    /// before any of that work.
    #[test]
    fn open_rejects_an_oversized_ciphertext_without_decrypting_it() {
        let keypair = EncryptionKeypair::derive(b"arrival-cap");
        let enc = [0u8; 32];

        assert!(matches!(
            keypair.open(&enc, &vec![0u8; MAX_SEALED_SECRETS_SIZE + 1], b"aad"),
            Err(Error::SealedPayloadTooLarge {
                limit: MAX_SEALED_SECRETS_SIZE,
                ..
            })
        ));

        // At the cap it is a normal open attempt again — rejected on the tag,
        // not on the length, so the cap is not quietly cutting valid payloads
        // short.
        assert!(matches!(
            keypair.open(&enc, &vec![0u8; MAX_SEALED_SECRETS_SIZE], b"aad"),
            Err(Error::Open)
        ));
    }

    #[test]
    fn fresh_keypairs_differ() {
        let a = EncryptionKeypair::generate().expect("keygen");
        let b = EncryptionKeypair::generate().expect("keygen");
        assert_ne!(a.public_key(), b.public_key());
        assert_ne!(a.key_id(), b.key_id());
    }

    #[test]
    fn derive_is_deterministic_and_generate_is_not() {
        let a = EncryptionKeypair::derive(b"conformance-vector-ikm");
        let b = EncryptionKeypair::derive(b"conformance-vector-ikm");
        assert_eq!(a.public_key(), b.public_key());
        assert_ne!(
            EncryptionKeypair::derive(b"a different ikm").public_key(),
            a.public_key()
        );
        assert_ne!(
            EncryptionKeypair::generate().expect("keygen").public_key(),
            a.public_key()
        );
    }

    #[test]
    fn pkcs8_pem_round_trips_the_same_recipient_key() {
        let keypair = EncryptionKeypair::generate().expect("keygen");
        let pem = keypair.to_pkcs8_pem();
        assert!(
            pem.starts_with("-----BEGIN PRIVATE KEY-----\n")
                && pem.ends_with("-----END PRIVATE KEY-----\n"),
            "the persisted key must be PKCS#8 PEM with LF line endings"
        );

        // The restored keypair is the SAME recipient: same public key, same
        // key_id, and it opens a payload sealed to the original.
        let restored = EncryptionKeypair::from_pkcs8_pem(&pem).expect("restore from PEM");
        assert_eq!(restored.public_key(), keypair.public_key());
        assert_eq!(restored.key_id(), keypair.key_id());

        let aad = address(keypair.key_id()).inner_aad();
        let sealed = keypair
            .recipient()
            .seal(SealLayer::Inner, b"across-a-restart", &aad)
            .expect("seal");
        let opened = restored
            .open(&sealed.enc, &sealed.ciphertext, &aad)
            .expect("open after restore");
        assert_eq!(opened.as_slice(), b"across-a-restart");
    }

    #[test]
    fn public_key_spki_pem_is_rfc_8410_x25519() {
        use x509_parser::prelude::FromDer;

        let keypair = EncryptionKeypair::generate().expect("keygen");
        let spki_pem = keypair.public_key_spki_pem();
        assert!(spki_pem.starts_with("-----BEGIN PUBLIC KEY-----\n"));

        // Parse it the way the control plane does — as a SubjectPublicKeyInfo —
        // and confirm the algorithm OID is id-X25519 (1.3.101.110) and the key
        // is the raw 32 bytes `key_id` is derived from.
        let der = pem::parse(&spki_pem).expect("spki pem");
        let (rest, spki) =
            x509_parser::x509::SubjectPublicKeyInfo::from_der(der.contents()).expect("spki der");
        assert!(rest.is_empty(), "no trailing bytes");
        assert_eq!(spki.algorithm.algorithm.to_id_string(), "1.3.101.110");
        assert_eq!(spki.subject_public_key.data.as_ref(), keypair.public_key());
        assert_eq!(keypair.key_id(), derive_key_id(keypair.public_key()));
    }

    #[test]
    fn from_pkcs8_pem_rejects_anything_that_is_not_an_x25519_private_key() {
        // Not PEM at all.
        assert!(matches!(
            EncryptionKeypair::from_pkcs8_pem("not a pem"),
            Err(Error::InvalidPrivateKey)
        ));
        // Right shape, wrong tag.
        let keypair = EncryptionKeypair::generate().expect("keygen");
        EncryptionKeypair::from_pkcs8_pem(&keypair.public_key_spki_pem())
            .expect_err("an SPKI public key must not load as a private key");
        // A P-256 PKCS#8 key (the identity key's algorithm) must be rejected —
        // the encryption key is X25519 and nothing else.
        let p256 = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("p256");
        EncryptionKeypair::from_pkcs8_pem(&p256.serialize_pem())
            .expect_err("a P-256 key must not load as an X25519 encryption key");
        // Correct prefix, truncated key.
        let mut short = X25519_PKCS8_PREFIX.to_vec();
        short.extend_from_slice(&[0u8; X25519_KEY_LEN - 1]);
        EncryptionKeypair::from_pkcs8_pem(&encode_pem(PRIVATE_KEY_PEM_TAG, &short))
            .expect_err("a truncated key must be rejected");
        // Correct length, something other than the expected prefix.
        let mut wrong_prefix = vec![0u8; X25519_PKCS8_PREFIX.len()];
        wrong_prefix.extend_from_slice(&[7u8; X25519_KEY_LEN]);
        EncryptionKeypair::from_pkcs8_pem(&encode_pem(PRIVATE_KEY_PEM_TAG, &wrong_prefix))
            .expect_err("a key without the X25519 PKCS#8 prefix must be rejected");
    }

    #[test]
    fn keyring_selects_the_key_a_payload_names_and_nothing_else() {
        let current = EncryptionKeypair::generate().expect("current");
        let previous = EncryptionKeypair::generate().expect("previous");
        let current_id = current.key_id().to_string();
        let previous_id = previous.key_id().to_string();
        let keyring = EncryptionKeyring::new(current, Some(previous));

        assert_eq!(
            keyring.select(&current_id).map(EncryptionKeypair::key_id),
            Some(current_id.as_str())
        );
        assert_eq!(
            keyring.select(&previous_id).map(EncryptionKeypair::key_id),
            Some(previous_id.as_str())
        );
        assert_eq!(keyring.current_key_id(), current_id);

        // A key the instance does not hold selects nothing — the fail-closed
        // case.
        let stranger = EncryptionKeypair::generate().expect("stranger");
        assert!(keyring.select(stranger.key_id()).is_none());
        assert!(keyring.select("").is_none());

        // Before the first renewal there is no previous key to fall back to.
        let fresh = EncryptionKeypair::generate().expect("fresh");
        let fresh_id = fresh.key_id().to_string();
        let no_previous = EncryptionKeyring::new(fresh, None);
        assert!(no_previous.select(&fresh_id).is_some());
        assert!(no_previous.select(&previous_id).is_none());
    }

    /// The rotation overlap: a payload the control plane sealed to key N must
    /// still open after the instance has rotated to N+1.
    #[test]
    fn keyring_opens_a_payload_sealed_to_the_superseded_key() {
        let previous = EncryptionKeypair::generate().expect("previous");
        let previous_id = previous.key_id().to_string();
        let aad = address(&previous_id).inner_aad();
        let sealed = previous
            .recipient()
            .seal(SealLayer::Inner, b"sealed-before-the-rotation", &aad)
            .expect("seal");

        // The instance renews, so the key the payload was sealed to becomes the
        // superseded one.
        let keyring = EncryptionKeyring::new(
            EncryptionKeypair::generate().expect("current"),
            Some(previous),
        );
        assert_ne!(keyring.current_key_id(), previous_id);

        let opened = keyring
            .select(&previous_id)
            .expect("the superseded key is still held")
            .open(&sealed.enc, &sealed.ciphertext, &aad)
            .expect("open with the superseded key");
        assert_eq!(opened.as_slice(), b"sealed-before-the-rotation");
    }

    #[test]
    fn keyring_from_pkcs8_pems_restores_both_keys() {
        let current = EncryptionKeypair::generate().expect("current");
        let previous = EncryptionKeypair::generate().expect("previous");
        let current_pem = current.to_pkcs8_pem();
        let previous_pem = previous.to_pkcs8_pem();
        let keyring = EncryptionKeyring::from_pkcs8_pems(&current_pem, Some(&previous_pem))
            .expect("restore keyring");
        assert!(keyring.select(current.key_id()).is_some());
        assert!(keyring.select(previous.key_id()).is_some());

        // An unusable PEM on either side is an error, not a silently smaller
        // keyring.
        EncryptionKeyring::from_pkcs8_pems("garbage", None)
            .expect_err("an unusable current key is an error");
        EncryptionKeyring::from_pkcs8_pems(&current_pem, Some("garbage"))
            .expect_err("an unusable previous key is an error");
    }

    #[test]
    fn debug_never_leaks_the_private_key() {
        let keypair = EncryptionKeypair::generate().expect("keygen");
        let rendered = format!("{keypair:?}");
        let raw_private = hex::encode(keypair.private_key.to_bytes());
        assert!(!rendered.contains(&raw_private), "must not leak the key");
        assert!(
            rendered.contains(keypair.key_id()),
            "the key id is safe to show: {rendered}"
        );

        let keyring = EncryptionKeyring::new(
            EncryptionKeypair::generate().expect("current"),
            Some(EncryptionKeypair::generate().expect("previous")),
        );
        let rendered = format!("{keyring:?}");
        assert!(rendered.contains(keyring.current_key_id()));
        assert!(!rendered.contains(&hex::encode(keyring.current().private_key.to_bytes())));
    }
}
