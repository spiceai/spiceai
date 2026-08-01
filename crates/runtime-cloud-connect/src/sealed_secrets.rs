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

//! Opening a double-sealed secret payload delivered with a spicepod.
//!
//! Two layers, opened outermost first:
//!
//! 1. **Outer** — sealed by the gateway to the per-connection key this session
//!    announced in `SecretsKey`. It is held in memory only and never persisted,
//!    so ciphertext recorded off the wire stays undecryptable even if the
//!    instance's on-disk enrolled key is later compromised.
//! 2. **Inner** — sealed by the control plane to the instance's *enrolled*
//!    encryption key. Nothing between the control plane and this process can
//!    open it, including the gateway that carried it.
//!
//! Both layers' additional authenticated data is derived here, never read off
//! the wire: a standalone instance has no Kubernetes namespace or Secret to
//! name, so those AAD components are the pinned literals in
//! `cloud-connect-crypto`. A peer that disagrees about any of them produces
//! ciphertext that fails to open, which is the intended outcome — the
//! alternative is trusting an attacker-supplied address.
//!
//! There is no plaintext path: a payload that fails at either layer is rejected
//! rather than partially applied.

use std::collections::BTreeMap;

use cloud_connect_crypto::{EncryptionKeypair, EncryptionKeyring, SecretAddress};
use prost::Message as _;
use snafu::{OptionExt as _, ResultExt, Snafu};
use zeroize::Zeroizing;

use crate::proto;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "This session announced no secrets key, so the control plane could not seal secrets to \
         it. Reconnect the instance and deploy again."
    ))]
    NoSessionKey,

    #[snafu(display(
        "The delivered secrets were sealed to key {wanted}, but this session announced {held}. \
         The payload was addressed to a different connection; deploy the app again."
    ))]
    OuterKeyMismatch { wanted: String, held: String },

    #[snafu(display(
        "The delivered secrets were sealed to encryption key {wanted}, which this instance does \
         not hold. That key rotated more than once before the deployment arrived; deploy the app \
         again to re-seal them."
    ))]
    InnerKeyUnknown { wanted: String },

    #[snafu(display("Failed to build the {layer} additional authenticated data: {source}"))]
    Address {
        layer: &'static str,
        source: cloud_connect_crypto::Error,
    },

    #[snafu(display(
        "The delivered secrets failed to open at the {layer} layer: {source}. The payload was \
         modified in transit, or a party disagrees about how the envelope is addressed. Nothing \
         was applied."
    ))]
    Open {
        layer: &'static str,
        source: cloud_connect_crypto::Error,
    },

    #[snafu(display(
        "The delivered secrets opened but did not decode as a secret payload: {source}. Nothing \
         was applied."
    ))]
    Decode { source: prost::DecodeError },

    #[snafu(display(
        "The delivered secrets are {size} bytes, over the {limit}-byte limit. Nothing was \
         applied. Reduce the number or size of the app's secrets."
    ))]
    TooLarge { size: usize, limit: usize },

    #[snafu(display("This instance holds no usable encryption key: {source}"))]
    NoEncryptionKey { source: crate::identity::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Secret names and values as delivered. Values are [`Zeroizing`] so the
/// plaintext is scrubbed when dropped rather than left in freed heap.
pub type DeliveredSecrets = BTreeMap<String, Zeroizing<Vec<u8>>>;

/// A successfully opened payload.
pub struct Opened {
    pub secrets: DeliveredSecrets,
    /// The **enrolled** key id the inner layer was sealed to — the current key
    /// or the retained previous one. The caller needs it to decide whether the
    /// rotation is confirmed and the previous key can be retired; deriving that
    /// from anything else would guess.
    pub inner_key_id: String,
}

/// Names only. A derived `Debug` would put every delivered value into any log
/// line or panic message that formatted this.
impl std::fmt::Debug for Opened {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Opened")
            .field("names", &self.secrets.keys().collect::<Vec<_>>())
            .field("inner_key_id", &self.inner_key_id)
            .finish_non_exhaustive()
    }
}

/// Open a payload delivered on the `ApplySpicepod` path.
///
/// `command_id` binds the outer layer to this specific dispatch — it is part of
/// the outer AAD, so an envelope cannot be replayed onto a different command.
///
/// # Errors
///
/// Every variant of [`Error`] is a refusal to apply: none of them results in a
/// partially-applied or plaintext-substituted payload. The messages name the
/// recovery (usually "deploy again") and never echo a secret value.
pub fn open_delivered(
    payload: &proto::SealedSecretPayload,
    external_id: &str,
    command_id: &str,
    session_key: Option<&EncryptionKeypair>,
    enrolled: &EncryptionKeyring,
) -> Result<Opened> {
    let session_key = session_key.context(NoSessionKeySnafu)?;

    // Refuse an oversized envelope before doing any crypto on it.
    let limit = cloud_connect_crypto::MAX_SEALED_SECRETS_SIZE;
    snafu::ensure!(
        payload.ciphertext.len() <= limit,
        TooLargeSnafu {
            size: payload.ciphertext.len(),
            limit,
        }
    );

    // The outer layer must be addressed to the key this session announced. The
    // check is not redundant with the open failing: it turns "authentication
    // failed" into a message that says which key was expected.
    snafu::ensure!(
        payload.key_id == session_key.key_id(),
        OuterKeyMismatchSnafu {
            wanted: payload.key_id.clone(),
            held: session_key.key_id().to_string(),
        }
    );

    let outer_address = SecretAddress::standalone(external_id, session_key.key_id())
        .context(AddressSnafu { layer: "outer" })?;
    let outer_aad = outer_address
        .outer_aad(command_id)
        .context(AddressSnafu { layer: "outer" })?;
    let inner_bytes = session_key
        .open(&payload.enc, &payload.ciphertext, &outer_aad)
        .context(OpenSnafu { layer: "outer" })?;

    let inner = proto::SealedSecretPayload::decode(inner_bytes.as_slice()).context(DecodeSnafu)?;

    // The inner layer may be addressed to the current *or* the retained
    // previous enrolled key: a deployment that crossed a renewal was sealed
    // before the rotation and cannot be re-sealed in flight.
    let inner_key = enrolled
        .select(&inner.key_id)
        .context(InnerKeyUnknownSnafu {
            wanted: inner.key_id.clone(),
        })?;

    let inner_aad = SecretAddress::standalone(external_id, &inner.key_id)
        .context(AddressSnafu { layer: "inner" })?
        .inner_aad();
    let plaintext = inner_key
        .open(&inner.enc, &inner.ciphertext, &inner_aad)
        .context(OpenSnafu { layer: "inner" })?;

    let secrets = proto::SecretPayload::decode(plaintext.as_slice()).context(DecodeSnafu)?;

    Ok(Opened {
        secrets: secrets
            .string_data
            .into_iter()
            .map(|(name, value)| (name, Zeroizing::new(value)))
            .collect(),
        inner_key_id: inner.key_id,
    })
}

/// `true` when a payload sealed to `inner_key_id` was opened with the keyring's
/// **current** key, which is what licenses retiring the retained previous one.
///
/// A payload that opened with the *previous* key must not retire it: it proves
/// the control plane had not yet seen the rotation, so another in-flight
/// dispatch may still be addressed to the same key.
#[must_use]
pub fn opened_with_current(inner_key_id: &str, enrolled: &EncryptionKeyring) -> bool {
    inner_key_id == enrolled.current_key_id()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloud_connect_crypto::{RecipientKey, SealLayer};

    const EXTERNAL_ID: &str = "inst_test";
    const COMMAND_ID: &str = "cmd-apply-1";

    fn secret_payload(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let payload = proto::SecretPayload {
            string_data: entries
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.to_vec()))
                .collect(),
        };
        payload.encode_to_vec()
    }

    /// Seal a payload exactly as the control plane and the gateway do: inner to
    /// `enrolled`, then outer to `session`.
    fn seal(
        entries: &[(&str, &[u8])],
        enrolled: &EncryptionKeypair,
        session: &EncryptionKeypair,
        command_id: &str,
    ) -> proto::SealedSecretPayload {
        let inner_aad = SecretAddress::standalone(EXTERNAL_ID, enrolled.key_id())
            .expect("inner address")
            .inner_aad();
        let inner_sealed = RecipientKey::from_public_key(enrolled.public_key())
            .expect("inner recipient")
            .seal(SealLayer::Inner, &secret_payload(entries), &inner_aad)
            .expect("inner seal");
        let inner = proto::SealedSecretPayload {
            key_id: enrolled.key_id().to_string(),
            enc: inner_sealed.enc,
            ciphertext: inner_sealed.ciphertext,
        };

        let outer_aad = SecretAddress::standalone(EXTERNAL_ID, session.key_id())
            .expect("outer address")
            .outer_aad(command_id)
            .expect("outer aad");
        let outer_sealed = RecipientKey::from_public_key(session.public_key())
            .expect("outer recipient")
            .seal(SealLayer::Outer, &inner.encode_to_vec(), &outer_aad)
            .expect("outer seal");
        proto::SealedSecretPayload {
            key_id: session.key_id().to_string(),
            enc: outer_sealed.enc,
            ciphertext: outer_sealed.ciphertext,
        }
    }

    fn keypair(seed: u8) -> EncryptionKeypair {
        EncryptionKeypair::derive(&[seed; 32])
    }

    #[test]
    fn opens_a_double_sealed_payload() {
        let enrolled = keypair(1);
        let session = keypair(2);
        let sealed = seal(&[("openai_key", b"sk-1")], &enrolled, &session, COMMAND_ID);

        let keyring = EncryptionKeyring::new(keypair(1), None);
        let out = open_delivered(&sealed, EXTERNAL_ID, COMMAND_ID, Some(&session), &keyring)
            .expect("opens");

        assert_eq!(out.secrets.len(), 1);
        assert_eq!(out.secrets["openai_key"].to_vec(), b"sk-1".to_vec());
        assert_eq!(out.inner_key_id, keypair(1).key_id());
        let _ = enrolled;
    }

    #[test]
    fn opens_a_payload_sealed_to_the_previous_key_after_a_rotation() {
        // The case the retained key exists for: the control plane sealed to the
        // key it had pinned, then the instance renewed before the dispatch
        // arrived.
        let old = keypair(1);
        let session = keypair(2);
        let sealed = seal(&[("openai_key", b"sk-old")], &old, &session, COMMAND_ID);

        let rotated = EncryptionKeyring::new(keypair(9), Some(keypair(1)));
        let out = open_delivered(&sealed, EXTERNAL_ID, COMMAND_ID, Some(&session), &rotated)
            .expect("the retained previous key must open it");
        assert_eq!(out.secrets["openai_key"].to_vec(), b"sk-old".to_vec());
        assert_eq!(
            out.inner_key_id,
            old.key_id(),
            "opened with the retained key"
        );

        // ...and that is exactly the case where the previous key must NOT be
        // retired, since the current one did not open it.
        assert!(!opened_with_current(old.key_id(), &rotated));
    }

    #[test]
    fn refuses_a_payload_sealed_to_a_key_two_rotations_old() {
        let ancient = keypair(1);
        let session = keypair(2);
        let sealed = seal(&[("k", b"v")], &ancient, &session, COMMAND_ID);

        // Two rotations on: neither current nor previous is the sealed-to key.
        let keyring = EncryptionKeyring::new(keypair(8), Some(keypair(9)));
        let err = open_delivered(&sealed, EXTERNAL_ID, COMMAND_ID, Some(&session), &keyring)
            .expect_err("must refuse");
        assert!(matches!(err, Error::InnerKeyUnknown { .. }), "{err}");
        assert!(err.to_string().contains("deploy the app again"), "{err}");
    }

    #[test]
    fn refuses_a_session_that_announced_no_key() {
        let sealed = seal(&[("k", b"v")], &keypair(1), &keypair(2), COMMAND_ID);
        let keyring = EncryptionKeyring::new(keypair(1), None);
        let err = open_delivered(&sealed, EXTERNAL_ID, COMMAND_ID, None, &keyring)
            .expect_err("no session key means no secrets");
        assert!(matches!(err, Error::NoSessionKey), "{err}");
    }

    #[test]
    fn refuses_an_outer_layer_addressed_to_another_session() {
        let sealed = seal(&[("k", b"v")], &keypair(1), &keypair(2), COMMAND_ID);
        let keyring = EncryptionKeyring::new(keypair(1), None);
        // A different session key: the envelope belongs to another connection.
        let other_session = keypair(7);
        let err = open_delivered(
            &sealed,
            EXTERNAL_ID,
            COMMAND_ID,
            Some(&other_session),
            &keyring,
        )
        .expect_err("must refuse");
        assert!(matches!(err, Error::OuterKeyMismatch { .. }), "{err}");
    }

    #[test]
    fn refuses_a_replay_onto_a_different_command() {
        // `command_id` is in the outer AAD precisely so an envelope captured
        // from one dispatch cannot be replayed onto another.
        let session = keypair(2);
        let sealed = seal(&[("k", b"v")], &keypair(1), &session, COMMAND_ID);
        let keyring = EncryptionKeyring::new(keypair(1), None);

        let err = open_delivered(
            &sealed,
            EXTERNAL_ID,
            "cmd-different",
            Some(&session),
            &keyring,
        )
        .expect_err("a different command_id must not open it");
        assert!(matches!(err, Error::Open { layer: "outer", .. }), "{err}");
    }

    #[test]
    fn refuses_a_payload_addressed_to_another_instance() {
        let session = keypair(2);
        let sealed = seal(&[("k", b"v")], &keypair(1), &session, COMMAND_ID);
        let keyring = EncryptionKeyring::new(keypair(1), None);

        let err = open_delivered(&sealed, "inst_other", COMMAND_ID, Some(&session), &keyring)
            .expect_err("another instance's id must not open it");
        assert!(matches!(err, Error::Open { layer: "outer", .. }), "{err}");
    }

    #[test]
    fn refuses_tampered_ciphertext_without_echoing_a_value() {
        let session = keypair(2);
        let mut sealed = seal(
            &[("openai_key", b"sk-secret")],
            &keypair(1),
            &session,
            COMMAND_ID,
        );
        let last = sealed.ciphertext.len() - 1;
        sealed.ciphertext[last] ^= 0x01;

        let keyring = EncryptionKeyring::new(keypair(1), None);
        let err = open_delivered(&sealed, EXTERNAL_ID, COMMAND_ID, Some(&session), &keyring)
            .expect_err("a flipped bit must fail authentication");
        assert!(matches!(err, Error::Open { .. }), "{err}");
        let message = err.to_string();
        assert!(!message.contains("sk-secret"));
        assert!(message.contains("Nothing was applied"), "{message}");
    }

    #[test]
    fn refuses_an_oversized_envelope_before_any_crypto() {
        let session = keypair(2);
        let sealed = proto::SealedSecretPayload {
            key_id: session.key_id().to_string(),
            enc: vec![0; 32],
            ciphertext: vec![0; cloud_connect_crypto::MAX_SEALED_SECRETS_SIZE + 1],
        };
        let keyring = EncryptionKeyring::new(keypair(1), None);
        let err = open_delivered(&sealed, EXTERNAL_ID, COMMAND_ID, Some(&session), &keyring)
            .expect_err("over the cap must be refused");
        assert!(matches!(err, Error::TooLarge { .. }), "{err}");
    }

    #[test]
    fn an_empty_payload_opens_to_no_secrets() {
        // "This app has no secrets" is a legitimate delivery, not an error.
        let session = keypair(2);
        let sealed = seal(&[], &keypair(1), &session, COMMAND_ID);
        let keyring = EncryptionKeyring::new(keypair(1), None);
        let out = open_delivered(&sealed, EXTERNAL_ID, COMMAND_ID, Some(&session), &keyring)
            .expect("opens");
        assert!(out.secrets.is_empty());
    }

    #[test]
    fn opened_with_current_tracks_the_keyring() {
        let keyring = EncryptionKeyring::new(keypair(1), Some(keypair(2)));
        assert!(opened_with_current(keyring.current_key_id(), &keyring));
        assert!(!opened_with_current(keypair(2).key_id(), &keyring));
    }
}
