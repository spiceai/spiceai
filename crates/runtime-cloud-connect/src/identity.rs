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

//! Local persistence for the post-enrollment runtime identity.
//!
//! The identity file lives at `$SPICE_CONFIG_DIR/identity.json` with
//! `0600` perms on Unix. On first boot the client generates a keypair
//! (ECDSA P-256) and a PKCS#10 CSR, presents the enrollment authority + CSR to
//! the **cloud enroll endpoint** over plain HTTPS (out-of-band, before
//! any gRPC stream), and receives back the signed leaf certificate, the
//! issuing-CA bundle, and the gateway address. The leaf, the matching
//! private key, the CA bundle, and the gateway address are persisted
//! here for the mTLS stream to the gateway. Generating the key and
//! proving possession of it (via the CSR) *before* the cert is issued is
//! what makes the issued cert genuinely bind the keypair used for mTLS.
//!
//! Renewals (~12h cadence against the cloud `/renew` endpoint) rotate the
//! keypair: each renewal persists a fresh key + leaf over this file.
//!
//! The JSON layout is intentionally narrow — it is a private interface
//! between the runtime and the local filesystem. Other Spice tooling
//! should treat it as opaque.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use base64::Engine as _;
use rcgen::{
    CertificateParams, CertificateSigningRequestParams, DnType, ExtendedKeyUsagePurpose, KeyPair,
    PublicKeyData,
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use zeroize::Zeroizing;

/// Errors that can occur while reading or writing the identity file, or
/// generating enrollment key material.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Identity I/O error at {}: {source}", path.display()))]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "Failed to parse identity JSON at {} (line {}, column {})",
        path.display(),
        source.line(),
        source.column()
    ))]
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display("Failed to serialize identity JSON: {source}"))]
    Serialize { source: serde_json::Error },

    #[snafu(display("Failed to generate enrollment key material: {source}"))]
    Enrollment { source: rcgen::Error },

    #[snafu(display("Failed to generate enrollment encryption key material: {reason}"))]
    EncKeyGeneration { reason: String },

    #[snafu(display("Failed to remove the identity file: {source}"))]
    ClearTaskPanicked { source: tokio::task::JoinError },

    #[snafu(display(
        "Failed to acquire the identity update transaction for {}: {reason}",
        path.display()
    ))]
    UpdateTransaction { path: PathBuf, reason: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Result of a credential read-modify-write under the enrollment transaction.
#[derive(Debug)]
pub enum CredentialUpdateOutcome {
    /// The update matched the durable generation and was stored.
    Stored(Identity),
    /// A newer enrollment or renewal was already durable, so the stale update
    /// was rejected and the durable winner is returned to the caller.
    Superseded(Identity),
    /// Removal won the transaction and no identity remains.
    Missing,
}

/// Persisted runtime identity. Treat as opaque outside this crate.
#[derive(Clone, Serialize, Deserialize)]
pub struct Identity {
    /// Cloud-assigned stable instance identifier (`instance_id` from the
    /// enroll response, e.g. `inst_...`).
    pub identifier: String,
    /// PEM-encoded X.509 leaf certificate the cloud KMS CA signed from
    /// the client's CSR (enroll response `identity_cert_pem`). On every
    /// gateway connection this leaf (with `private_key_pem`) is presented
    /// as the TLS client certificate — it *is* the credential, which is why
    /// the `Hello` carries none.
    pub identity_cert_pem: String,
    /// PEM-encoded PKCS#8 private key for the current keypair. Kept
    /// local (never sent); pairs with `identity_cert_pem` for mTLS and
    /// signs the `/renew` proof-of-possession. Rotated on every renewal.
    pub private_key_pem: String,
    /// PEM-encoded SPKI public key. The cloud pins it at enroll/renew.
    pub public_key_pem: String,
    /// PEM-encoded issuing-CA chain from the enroll response
    /// (`ca_bundle_pem`). The client pins this to verify the gateway on
    /// mTLS connections. Empty when the server did not supply one (the
    /// client then falls back to public roots). Defaulted so identity
    /// files written before this field existed still load.
    #[serde(default)]
    pub ca_bundle_pem: String,
    /// Gateway `host:port` from the enroll response (`gateway_addr`) —
    /// the address the mTLS `CloudConnect` stream connects to. Defaulted
    /// so identity files written before this field existed still load;
    /// an empty value means the identity predates the enroll-first flow
    /// and cannot be used to reach the gateway (re-enroll with a fresh
    /// enrollment key).
    #[serde(default)]
    pub gateway_addr: String,
    /// Normalized state-plane base URL used to enroll this identity. Renewals
    /// and release stay bound to this authority even when a later process has
    /// different environment variables. `None` for identities written before
    /// the binding became part of durable state; those continue to use the
    /// legacy environment/`cloud-endpoint` resolution.
    #[serde(default)]
    pub control_plane_endpoint: Option<String>,
    /// Unix timestamp (seconds) after which the identity cert is no longer
    /// accepted by the server. `None` when the server issued no expiry —
    /// carried as presence rather than a `0` sentinel so "unbounded" and
    /// "expires at the epoch" stay distinguishable.
    #[serde(default, deserialize_with = "deserialize_not_after")]
    pub not_after_unix: Option<u64>,
    /// PEM-encoded PKCS#8 X25519 encryption private key. The control plane
    /// HPKE-seals secret payloads to the matching public key; this key
    /// unseals them. Kept local (never sent). Rotated alongside the
    /// identity keypair on every renewal so the cloud can begin sealing
    /// to the new key from that commit on; the outgoing key is retained in
    /// [`Identity::enc_previous_private_key_pem`] for exactly one rotation
    /// so a payload already in flight still opens. Defaulted (empty) so
    /// identity files written before this field existed still load.
    #[serde(default)]
    pub enc_private_key_pem: String,
    /// PEM-encoded SPKI (RFC 8410) X25519 encryption public key, as sent
    /// to the cloud in the enroll and renew requests (`enc_pubkey_pem`).
    /// Defaulted so older identity files still load.
    #[serde(default)]
    pub enc_public_key_pem: String,
    /// The encryption private key this identity held **before** the last
    /// rotation, retained for exactly one rotation.
    ///
    /// This is what makes a dispatch that crosses a renewal still open: the
    /// control plane may have sealed to the key it had pinned moments before
    /// the rotation, and a payload already in flight cannot be re-sealed. It
    /// is dropped once the current key has successfully opened an envelope
    /// (see [`Identity::retire_previous_enc_key`]), which is the point at
    /// which no in-flight payload can still be addressed to the old one.
    ///
    /// On disk rather than in memory only, because a restart between the
    /// rotation and the dispatch would otherwise reintroduce the race.
    /// Empty means there is no retained key.
    #[serde(default)]
    pub enc_previous_private_key_pem: String,
    /// Base64 (standard, padded) 32-byte AEAD key for the local
    /// delivered-secrets cache. Minted once at enrollment and **never
    /// rotated** — that is deliberate: the identity and encryption keys
    /// rotate about every 12 hours, and a cache key derived from or rotated
    /// with them would strand the cache on every renewal.
    ///
    /// Local only. It is never sent to the control plane, never logged, and
    /// appears in no request, response, or span. Defaulted (empty) so older
    /// identity files still load; an empty value means this instance has no
    /// cache key yet and the cache is simply unavailable until one is minted.
    #[serde(default)]
    pub cache_key_b64: String,
    /// The app this instance's telemetry is attributed to, as delivered by the
    /// control plane and stamped on exported metrics as `scp_app_id`.
    ///
    /// Portal metadata, not credential material. It lives alongside the
    /// credential because both are control-plane facts about this enrolled
    /// instance and leave disk together when the instance is released.
    ///
    /// Persisted rather than held only in memory so a restart does not silence
    /// the export before the next control-stream reconciliation.
    ///
    /// `None` means the instance is detached, which is also the state a freshly
    /// enrolled instance starts in.
    #[serde(default)]
    pub app_id: Option<String>,
    /// The Spice Cloud organization this instance belongs to, as the control
    /// plane last reported it. Portal metadata, not credential material: it is
    /// what lets local surfaces name the org destination without constructing
    /// portal routes themselves.
    ///
    /// Instance-level, not attachment-scoped: the org an instance is enrolled
    /// in owns every attachment it can ever have (the control plane never
    /// attaches one credential across organizations), so this is **updated
    /// only when a command names an org and never cleared by omission** — not
    /// by detach, and not by an attach that carries no org. That is what
    /// keeps the org's new-project page reachable as the recovery path for a
    /// detached instance, including under a control plane that still sends
    /// app-id-only attachments. It leaves disk only when the whole identity
    /// does.
    #[serde(default)]
    pub org_name: Option<String>,
    /// The attached app (project) name inside [`Identity::org_name`]. Scoped to
    /// the attachment, so a detach clears it along with the app id.
    #[serde(default)]
    pub app_name: Option<String>,
    /// Cloud-constructed portal URL for the attached app's monitor page,
    /// delivered rather than derived because the Cloud owns environment and
    /// route metadata. Scoped to the attachment; cleared on detach.
    #[serde(default)]
    pub monitor_url: Option<String>,
    /// Cloud-constructed portal URL for creating a project with this instance
    /// preselected, as the enrollment response reported it.
    ///
    /// Instance-level like [`Identity::org_name`], and for the same reason: it
    /// is the destination a *detached* instance is sent to, so an attach must
    /// not clear it and a detach must not lose it. Persisted rather than kept
    /// only in the enrolling process because every later start reports the same
    /// unattached state and must name the same page — the runtime never derives
    /// a portal route itself.
    ///
    /// `None` for an instance enrolled before this was recorded, or against a
    /// control plane that reported none.
    #[serde(default)]
    pub new_project_url: Option<String>,
}

/// The control plane's app attachment state for this instance, as one tuple:
/// the attached app plus the portal metadata that describes it. Delivered by
/// an `AttachApp` command and persisted into the [`Identity`] as a unit — the
/// pieces are only meaningful together, so they are applied together.
///
/// An absent optional member means the command did not name it. The org is
/// preserved; project metadata is preserved only for the same app and is
/// cleared on an app change — see [`IdentityStore::set_attachment`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppAttachment {
    /// The attached app. Always non-empty: a detached instance is represented
    /// by the absence of the whole tuple, not an empty id.
    pub app_id: String,
    /// The organization the instance belongs to, when the control plane named
    /// it.
    pub org_name: Option<String>,
    /// The app (project) name, when the control plane named it.
    pub app_name: Option<String>,
    /// Portal monitor URL for the app, when the control plane supplied it.
    pub monitor_url: Option<String>,
}

/// The attachment-related fields of the identity **as persisted** — what a
/// command handler reports back after an update, so the reply reflects the
/// state on disk rather than echoing the command that produced it (the two
/// differ exactly where absence preserves: a detach keeps the org, an
/// app-id-only attach keeps the org it already had).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AttachmentState {
    /// `None` means detached.
    pub app_id: Option<String>,
    pub org_name: Option<String>,
    pub app_name: Option<String>,
    pub monitor_url: Option<String>,
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity")
            .field("identifier", &self.identifier)
            .field("identity_cert_pem", &"[CERTIFICATE]")
            .field("private_key_pem", &"[REDACTED]")
            .field("public_key_pem", &"[PUBLIC KEY]")
            .field("ca_bundle_pem", &"[CERTIFICATE BUNDLE]")
            .field("gateway_addr", &self.gateway_addr)
            .field("control_plane_endpoint", &self.control_plane_endpoint)
            .field("not_after_unix", &self.not_after_unix)
            .field("enc_private_key_pem", &"[REDACTED]")
            .field("enc_public_key_pem", &"[PUBLIC KEY]")
            .field("enc_previous_private_key_pem", &"[REDACTED]")
            .field("cache_key_b64", &"[REDACTED]")
            .field("app_id", &self.app_id)
            .field("org_name", &self.org_name)
            .field("app_name", &self.app_name)
            .field("monitor_url", &self.monitor_url)
            .field("new_project_url", &self.new_project_url)
            .finish()
    }
}

/// Read the persisted `not_after_unix`, mapping a missing, null, or `0` value
/// to "no expiry" so identity files written before the field carried presence
/// keep their meaning.
fn deserialize_not_after<'de, D>(deserializer: D) -> std::result::Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<u64>::deserialize(deserializer)?.filter(|seconds| *seconds != 0))
}

impl Identity {
    /// Drop any portal page this runtime would not accept today.
    ///
    /// Validating at the writer stops an unusable link becoming durable, but it
    /// says nothing about links already on disk: these fields predate the rule,
    /// so an identity written by an older runtime can hold one, and every
    /// consumer — the startup report, `spice connect status`, a browser an
    /// operator opens — reads the stored value rather than a freshly delivered
    /// one. Applying the rule on the way in is what makes "nothing unusable
    /// reaches a log or a browser" true of existing state and not just of new
    /// writes. A dropped page is reduced to absent, never rewritten into
    /// something else, and the file is not modified by reading it: the next write
    /// that touches these fields persists the reduction.
    fn drop_unusable_portal_pages(&mut self) {
        self.monitor_url = self
            .monitor_url
            .take()
            .and_then(|url| crate::config::safe_portal_url(&url));
        self.new_project_url = self
            .new_project_url
            .take()
            .and_then(|url| crate::config::safe_portal_url(&url));
    }

    pub(crate) fn certificate_validity_unix(&self) -> Result<(i64, i64), &'static str> {
        let certificate_pem = pem::parse(&self.identity_cert_pem)
            .map_err(|_| "the client identity certificate is not valid PEM")?;
        if certificate_pem.tag() != "CERTIFICATE" {
            return Err("the client identity certificate has an invalid PEM label");
        }
        let (remaining, certificate) =
            x509_parser::prelude::parse_x509_certificate(certificate_pem.contents())
                .map_err(|_| "the client identity certificate is not valid X.509")?;
        if !remaining.is_empty() {
            return Err("the client identity certificate has trailing DER data");
        }
        Ok((
            certificate.validity().not_before.timestamp(),
            certificate.validity().not_after.timestamp(),
        ))
    }

    /// Why this identity cannot establish a control stream, if any.
    ///
    /// Enrollment uses this as a fail-closed gate before honoring the
    /// existing-identity precedence rule. Credentials, the cloud identifier,
    /// and a durable gateway address are always required; a process-local
    /// override may redirect a running client but cannot activate an identity.
    #[must_use]
    pub fn reconnect_validation_error(&self) -> Option<&'static str> {
        if self.identifier.trim().is_empty() {
            return Some("the cloud-assigned instance identifier is empty");
        }
        if self.identifier.chars().any(char::is_control) {
            return Some("the cloud-assigned instance identifier contains control characters");
        }
        if self.identity_cert_pem.trim().is_empty() {
            return Some("the client identity certificate is empty");
        }
        if self.private_key_pem.trim().is_empty() {
            return Some("the client identity private key is empty");
        }
        let Ok(certificate_pem) = pem::parse(&self.identity_cert_pem) else {
            return Some("the client identity certificate is not valid PEM");
        };
        if certificate_pem.tag() != "CERTIFICATE" {
            return Some("the client identity certificate has an invalid PEM label");
        }
        let Ok((remaining, certificate)) =
            x509_parser::prelude::parse_x509_certificate(certificate_pem.contents())
        else {
            return Some("the client identity certificate is not valid X.509");
        };
        if !remaining.is_empty() {
            return Some("the client identity certificate has trailing DER data");
        }
        let Ok(private_key) = KeyPair::from_pem(&self.private_key_pem) else {
            return Some("the client identity private key is not valid PKCS key material");
        };
        if certificate.public_key().raw != private_key.subject_public_key_info().as_slice() {
            return Some("the client identity certificate and private key do not match");
        }
        let Ok(public_key_pem) = pem::parse(&self.public_key_pem) else {
            return Some("the client identity public key is not valid PEM");
        };
        if public_key_pem.tag() != "PUBLIC KEY" {
            return Some("the client identity public key has an invalid PEM label");
        }
        if public_key_pem.contents() != private_key.subject_public_key_info().as_slice() {
            return Some("the client identity public and private keys do not match");
        }
        if self.gateway_addr.trim().is_empty() {
            return Some("the gateway address is empty");
        }
        if self.gateway_addr.chars().any(char::is_control) {
            return Some("the gateway address contains control characters");
        }
        match (
            self.enc_private_key_pem.trim().is_empty(),
            self.enc_public_key_pem.trim().is_empty(),
        ) {
            // Identities written before encrypted secret delivery carry
            // neither field. They remain reconnectable and gain a keypair on
            // their next scheduled renewal.
            (true, true) => {}
            (false, false) => {
                let Ok(keypair) = cloud_connect_crypto::EncryptionKeypair::from_pkcs8_pem(
                    &self.enc_private_key_pem,
                ) else {
                    return Some(
                        "the secret-delivery private key is not valid X25519 key material",
                    );
                };
                let Ok(public_key) = pem::parse(&self.enc_public_key_pem) else {
                    return Some("the secret-delivery public key is not valid PEM");
                };
                if public_key.tag() != "PUBLIC KEY" {
                    return Some("the secret-delivery public key has an invalid PEM label");
                }
                let Ok(expected_public_key) = pem::parse(keypair.public_key_spki_pem()) else {
                    return Some("the secret-delivery public key could not be derived");
                };
                if public_key.contents() != expected_public_key.contents() {
                    return Some("the secret-delivery public and private keys do not match");
                }
            }
            _ => return Some("the secret-delivery keypair is incomplete"),
        }
        if let Some(endpoint) = self.control_plane_endpoint.as_deref()
            && crate::config::normalize_control_plane_endpoint(endpoint).is_err()
        {
            return Some("the bound control-plane endpoint is invalid");
        }
        None
    }

    /// The signed certificate expiry, falling back to the cached response value
    /// only when a legacy certificate cannot be parsed.
    #[must_use]
    pub fn effective_not_after_unix(&self) -> Option<u64> {
        self.certificate_validity_unix()
            .ok()
            .and_then(|(_, not_after)| u64::try_from(not_after).ok())
            .or(self.not_after_unix)
    }

    /// Returns `true` if the identity has an expiry that is in the past
    /// relative to the system clock. An identity with no expiry never expires.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        let not_after = self.effective_not_after_unix().map(i128::from);
        let Some(not_after) = not_after else {
            return false;
        };
        // Treat the cert as expired *at* `not_after`, not only strictly after
        // it: the field is defined as the timestamp after which the server no
        // longer accepts the credential, so the boundary second should
        // already be considered past the NotAfter limit.
        i128::from(now) >= not_after
    }

    /// Returns `true` when the signed certificate validity interval starts in
    /// the future relative to the system clock.
    #[must_use]
    pub fn is_not_yet_valid(&self) -> bool {
        self.certificate_validity_unix()
            .is_ok_and(|(not_before, _)| {
                i128::from(crate::heartbeat::now_unix()) < i128::from(not_before)
            })
    }

    /// The encryption keys a sealed payload may be addressed to: the current
    /// key, plus the retained previous one when a rotation has not yet been
    /// confirmed by a successful open.
    ///
    /// A malformed retained key is dropped rather than failing the whole
    /// keyring — the current key is what almost every payload is sealed to, and
    /// refusing to build a keyring over a bad *previous* key would turn a
    /// recoverable state into a total delivery outage.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EncKeyGeneration`] when this identity holds no usable
    /// current encryption key — it predates the encryption key (empty field) or
    /// the stored PEM does not parse.
    pub fn encryption_keyring(&self) -> Result<cloud_connect_crypto::EncryptionKeyring> {
        snafu::ensure!(
            !self.enc_private_key_pem.is_empty(),
            EncKeyGenerationSnafu {
                reason:
                    "this identity holds no encryption key; it enrolled before encrypted secret \
                     delivery existed. If this identity has a certificate expiry, its scheduled \
                     renewal will re-key it when due. To recover immediately, or if it has no \
                     renewal deadline, stop spiced, run `spice connect remove --yes` from this \
                     instance directory, mint a new enrollment key in the Spice Cloud portal, \
                     and restart with `spiced --token <enrollment-key>`. The existing identity \
                     always wins, so supplying --token before removing it cannot re-enroll."
                        .to_string(),
            }
        );

        let current =
            cloud_connect_crypto::EncryptionKeypair::from_pkcs8_pem(&self.enc_private_key_pem)
                .map_err(|source| Error::EncKeyGeneration {
                    reason: format!("the stored encryption key could not be parsed: {source}"),
                })?;

        let previous = if self.enc_previous_private_key_pem.is_empty() {
            None
        } else {
            match cloud_connect_crypto::EncryptionKeypair::from_pkcs8_pem(
                &self.enc_previous_private_key_pem,
            ) {
                Ok(previous) => Some(previous),
                Err(source) => {
                    // No key material in the message — only that one could not
                    // be parsed.
                    tracing::warn!(
                        "Cloud Connect: the retained previous encryption key could not be parsed ({source}); \
                         continuing with the current key only. A secret payload sealed just before the last \
                         rotation will not open until the app is deployed again."
                    );
                    None
                }
            }
        };

        Ok(cloud_connect_crypto::EncryptionKeyring::new(
            current, previous,
        ))
    }

    /// Rotate the encryption key: `next` becomes current and the outgoing
    /// current key is retained as previous.
    ///
    /// Called on renewal, alongside the identity keypair rotation, so both keys
    /// move in the one request the cloud updates atomically.
    pub fn rotate_encryption_key(&mut self, next_private_pem: String, next_public_pem: String) {
        // Retain whatever was current — including nothing, for an identity that
        // predates the encryption key, where there is no previous key to keep.
        self.enc_previous_private_key_pem = std::mem::take(&mut self.enc_private_key_pem);
        self.enc_private_key_pem = next_private_pem;
        self.enc_public_key_pem = next_public_pem;
    }

    /// Drop the retained previous encryption key, if any. Returns `true` when
    /// there was one to drop, so the caller knows whether to persist.
    ///
    /// Called once the **current** key has successfully opened an envelope: at
    /// that point the control plane is demonstrably sealing to the rotated key,
    /// so no in-flight payload can still be addressed to the old one and
    /// retaining it only widens the window in which it is on disk.
    pub fn retire_previous_enc_key(&mut self) -> bool {
        if self.enc_previous_private_key_pem.is_empty() {
            return false;
        }
        self.enc_previous_private_key_pem.clear();
        true
    }

    /// The local delivered-secrets cache key, decoded to raw bytes.
    ///
    /// `None` when this identity has no cache key (it predates the field) or
    /// the stored value is not a well-formed key — in both cases the cache is
    /// unavailable, which is a degraded mode rather than an error: a deployment
    /// re-delivers the secrets.
    #[must_use]
    pub fn cache_key(&self) -> Option<CacheKey> {
        if self.cache_key_b64.is_empty() {
            return None;
        }
        let raw = base64::engine::general_purpose::STANDARD
            .decode(&self.cache_key_b64)
            .ok()?;
        (raw.len() == CACHE_KEY_LEN).then(|| Zeroizing::new(raw))
    }

    /// Mint a cache key if this identity has none, returning `true` when one
    /// was added (so the caller persists).
    ///
    /// Idempotent, and deliberately additive: an instance enrolled before the
    /// cache existed gains a key on its next start without needing to
    /// re-enroll, and an existing key is never replaced — replacing it would
    /// discard a cache that is still perfectly readable.
    ///
    /// A failed randomness draw leaves the identity without a cache key and
    /// returns `false`: the cache is then unavailable, which costs a redeploy
    /// after a restart, rather than failing an enrollment over it.
    pub fn ensure_cache_key(&mut self) -> bool {
        if self.cache_key().is_some() {
            return false;
        }
        match generate_cache_key_b64() {
            Ok(key) => {
                self.cache_key_b64 = key;
                true
            }
            Err(err) => {
                tracing::warn!(
                    "Cloud Connect: could not generate a local secrets-cache key ({err}); \
                     delivered secrets will not survive a restart until the next enrollment."
                );
                false
            }
        }
    }
}

/// Length of the delivered-secrets cache key: a 256-bit AEAD key.
pub const CACHE_KEY_LEN: usize = 32;

/// The local delivered-secrets cache key.
///
/// Aliased so callers can hold one without depending on `zeroize` themselves —
/// and so the zeroizing wrapper cannot be dropped from the type by accident at a
/// call site.
pub type CacheKey = Zeroizing<Vec<u8>>;

/// Mint a fresh random cache key, base64-encoded for JSON storage.
///
/// Drawn from OS randomness, matching how `cloud-connect-crypto` draws key
/// material — deriving this from anything else is what strands a cache when the
/// thing it was derived from rotates.
fn generate_cache_key_b64() -> std::result::Result<String, getrandom::Error> {
    let mut key = Zeroizing::new([0_u8; CACHE_KEY_LEN]);
    getrandom::fill(key.as_mut())?;
    Ok(base64::engine::general_purpose::STANDARD.encode(key.as_ref()))
}

/// On-disk identity store rooted at a single JSON file.
#[derive(Debug, Clone)]
pub struct IdentityStore;

/// Serializes writers of the identity file.
///
/// [`atomic_write`] already makes any single write all-or-nothing, so no reader
/// ever sees a half-file. What this adds is protection against a LOST UPDATE:
/// [`IdentityStore::set_app_id`] and [`IdentityStore::set_attachment`] read the
/// file, edit attachment fields, and write it back, while the renewal path
/// replaces the credential fields.
/// Interleave those and the rotated credential is silently replaced by the copy
/// the app-id update read a moment earlier — leaving on disk a key the cloud has
/// already stopped accepting, which surfaces much later as a renewal that cannot
/// authenticate.
///
/// The config directory's persistent enrollment transaction additionally
/// serializes these read-modify-writes against `spice connect remove` in a
/// separate process. The process-wide lock remains necessary for writers that
/// already own that transaction and for the runtime's in-process updates.
///
/// Poisoning is ignored: the guarded data is the file, not memory, and a panic
/// mid-write leaves it either fully old or fully new thanks to the atomic
/// rename. Refusing all later writes would turn a transient fault into a
/// permanently unrenewable instance.
fn write_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn acquire_update_transaction(
    config_dir: &Path,
    path: &Path,
) -> Result<crate::draft::EnrollmentTransactionLock> {
    crate::draft::EnrollmentTransactionLock::acquire(config_dir).map_err(|source| {
        Error::UpdateTransaction {
            path: path.to_path_buf(),
            reason: source.to_string(),
        }
    })
}

fn acquire_removal_transaction(path: &Path) -> Result<crate::draft::EnrollmentTransactionLock> {
    let config_dir = parent_directory(path);
    crate::draft::EnrollmentTransactionLock::acquire_for_removal(config_dir).map_err(|source| {
        Error::UpdateTransaction {
            path: path.to_path_buf(),
            reason: source.to_string(),
        }
    })
}

fn protected_identity_path(
    transaction: &crate::draft::EnrollmentTransactionLock,
    path: &Path,
) -> Result<PathBuf> {
    if let Some(protected) = transaction.protected_path(path) {
        return Ok(protected);
    }
    transaction
        .ensure_directory_stable()
        .map_err(|source| Error::UpdateTransaction {
            path: path.to_path_buf(),
            reason: source.to_string(),
        })?;

    // `identity_path` is independently configurable. It still participates in
    // the config directory's transaction, but its own secure open/write path
    // validates the external ancestor chain rather than relocating the file
    // beneath the config directory.
    Ok(path.to_path_buf())
}

/// Copy one existing regular state file into a destination that must not yet
/// exist, without following either leaf and with owner-only permissions.
///
/// The caller removes an earlier snapshot first. `create_new` makes a raced
/// replacement fail rather than truncate it; platform-specific no-follow
/// opens prevent a replacement symlink/reparse point from redirecting either
/// side of the copy. Paths may be rooted through a retained directory
/// descriptor (for example `/proc/self/fd/N`) so the parent stays pinned too.
///
/// # Errors
///
/// Returns an error when either path is unsafe or cannot be read, created,
/// copied, or durably synchronized.
pub fn snapshot_regular_file_create_new(
    source: &Path,
    destination: &Path,
) -> std::io::Result<bool> {
    use std::io::Write as _;

    #[cfg(unix)]
    let (mut source_file, mut destination_file) = {
        use std::os::unix::fs::MetadataExt as _;

        let Some(source_file) = open_regular_file_optional_unix(source)? else {
            return Ok(false);
        };
        let source_metadata = source_file.metadata()?;
        if !source_metadata.is_file() || source_metadata.nlink() != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{} must be a singly-linked regular file", source.display()),
            ));
        }

        let destination_file = create_regular_file_new_unix(destination)?;
        (source_file, destination_file)
    };

    #[cfg(not(unix))]
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "secure state-file snapshots are unsupported on this platform",
        ));
    }

    std::io::copy(&mut source_file, &mut destination_file)?;
    destination_file.flush()?;
    destination_file.sync_all()?;
    sync_parent_directory(destination)?;
    Ok(true)
}

/// Read a security-sensitive state file without following symlinks or opening
/// a FIFO/device in blocking mode.
///
/// A missing path is the only absence case. Every other file-system object is
/// rejected so a privileged bootstrap cannot be redirected outside its config
/// directory or held indefinitely by a special file.
pub(crate) fn read_regular_file_optional(path: &Path) -> std::io::Result<Option<String>> {
    const MAX_STATE_FILE_BYTES: u64 = 16 * 1024 * 1024;
    let Some(bytes) = read_regular_file_optional_bounded(path, MAX_STATE_FILE_BYTES)? else {
        return Ok(None);
    };
    String::from_utf8(bytes).map(Some).map_err(|source| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("the state file was not UTF-8: {source}"),
        )
    })
}

pub(crate) fn read_regular_file_optional_bounded(
    path: &Path,
    max_bytes: u64,
) -> std::io::Result<Option<Vec<u8>>> {
    use std::io::Read as _;

    #[cfg(unix)]
    let Some(file) = open_regular_file_optional_unix(path)? else {
        return Ok(None);
    };

    #[cfg(not(unix))]
    let mut file = {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "secure state-file reads are unsupported on this platform",
        ));
    };

    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the state path must be a bounded regular file",
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.nlink() != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "the state file must not be hard-linked",
            ));
        }
    }

    let mut contents = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut contents)?;
    if u64::try_from(contents.len()).unwrap_or(u64::MAX) > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the state file exceeded its size limit",
        ));
    }
    Ok(Some(contents))
}

/// Open a state file relative to verified directory descriptors, refusing a
/// symlink or non-directory at every component.
///
/// Unix has a small set of root-owned compatibility links in otherwise
/// immutable directories (`/var` and `/tmp` on macOS are common examples).
/// Those links cannot be replaced by the process running Spice and are safe to
/// resolve before descriptor traversal. Every other symlink is rejected,
/// including a root-owned link in a user-writable directory. `openat` with
/// `O_NOFOLLOW` then closes the rename race between inspecting and opening
/// every ordinary component.
#[cfg(unix)]
fn open_regular_file_optional_unix(path: &Path) -> std::io::Result<Option<std::fs::File>> {
    open_regular_file_optional_unix_with(path, || {})
}

#[cfg(unix)]
fn open_regular_file_optional_unix_with(
    path: &Path,
    before_descriptor_traversal: impl FnOnce(),
) -> std::io::Result<Option<std::fs::File>> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::ffi::OsStrExt as _;

    let (directory, file_name) =
        match open_verified_state_parent_unix(path, before_descriptor_traversal) {
            Ok(opened) => opened,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(source),
        };

    let file_name = CString::new(file_name.as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "the state path contains a NUL byte",
        )
    })?;
    // SAFETY: `file_name` is NUL-terminated, the directory descriptor is live
    // for this call, and no pointer is retained. The leaf cannot be followed.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            file_name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        let source = std::io::Error::last_os_error();
        if source.kind() == std::io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(source);
    }
    // SAFETY: `openat` returned a new owned descriptor on success.
    Ok(Some(unsafe { std::fs::File::from_raw_fd(descriptor) }))
}

/// Open the directory containing a state file without allowing a path lookup
/// to retarget it after validation.
///
/// A Linux removal may intentionally root the path at the live directory
/// descriptor retained by [`crate::mutation_lock::MutationLock`]. That exact
/// `/proc/self/fd/N` spelling is process-owned authority, so duplicate `N`
/// directly instead of following procfs's magic symlink. Every ordinary path
/// still uses component-by-component `openat` traversal with `O_NOFOLLOW`.
#[cfg(unix)]
pub(crate) fn open_verified_state_parent_unix(
    path: &Path,
    before_descriptor_traversal: impl FnOnce(),
) -> std::io::Result<(std::fs::File, std::ffi::OsString)> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::ffi::OsStrExt as _;

    let absolute = normalize_absolute_state_path(path)?;
    let file_name = absolute
        .file_name()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "the state path has no file name",
            )
        })?
        .to_os_string();
    let parent = absolute.parent().unwrap_or_else(|| Path::new("/"));

    #[cfg(target_os = "linux")]
    if let Some(retained) = retained_directory_descriptor(parent) {
        before_descriptor_traversal();
        // SAFETY: `retained` was parsed from this process's `/proc/self/fd`
        // namespace. `F_DUPFD_CLOEXEC` creates a new owned descriptor without
        // following the procfs magic symlink or retaining a borrowed lifetime.
        let descriptor = unsafe { libc::fcntl(retained, libc::F_DUPFD_CLOEXEC, 0) };
        if descriptor < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `fcntl` returned a new owned descriptor on success.
        let directory = unsafe { std::fs::File::from_raw_fd(descriptor) };
        if !directory.metadata()?.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "the retained Cloud Connect descriptor is not a directory",
            ));
        }
        return Ok((directory, file_name));
    }

    let parent = resolve_trusted_system_links(parent)?;
    before_descriptor_traversal();
    let mut directory = std::fs::File::open("/")?;

    for component in parent.components() {
        let std::path::Component::Normal(component) = component else {
            continue;
        };
        let component = CString::new(component.as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "the state path contains a NUL byte",
            )
        })?;
        // SAFETY: `component` is NUL-terminated, the directory descriptor is
        // live for this call, and no pointer is retained. `O_NOFOLLOW` and
        // `O_DIRECTORY` make a raced symlink/non-directory fail this step.
        let descriptor = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                component.as_ptr(),
                libc::O_RDONLY
                    | libc::O_DIRECTORY
                    | libc::O_NOFOLLOW
                    | libc::O_NONBLOCK
                    | libc::O_CLOEXEC,
            )
        };
        if descriptor < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `openat` returned a new owned descriptor on success.
        directory = unsafe { std::fs::File::from_raw_fd(descriptor) };
    }
    Ok((directory, file_name))
}

#[cfg(target_os = "linux")]
fn retained_directory_descriptor(path: &Path) -> Option<std::os::fd::RawFd> {
    use std::os::unix::ffi::OsStrExt as _;

    let relative = path.strip_prefix("/proc/self/fd").ok()?;
    let mut components = relative.components();
    let std::path::Component::Normal(descriptor) = components.next()? else {
        return None;
    };
    if components.next().is_some()
        || descriptor.as_bytes().is_empty()
        || !descriptor.as_bytes().iter().all(u8::is_ascii_digit)
    {
        return None;
    }
    std::str::from_utf8(descriptor.as_bytes())
        .ok()?
        .parse()
        .ok()
}

#[cfg(unix)]
fn create_regular_file_new_unix(path: &Path) -> std::io::Result<std::fs::File> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::MetadataExt as _;

    let (directory, file_name) = open_verified_state_parent_unix(path, || {})?;
    let file_name = CString::new(file_name.as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "the state path contains a NUL byte",
        )
    })?;
    // SAFETY: `file_name` is NUL-terminated and `directory` remains live.
    // `O_EXCL` and `O_NOFOLLOW` make an existing or redirected leaf fail.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            file_name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: `openat` returned a new owned descriptor on success.
    let file = unsafe { std::fs::File::from_raw_fd(descriptor) };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{} must be a singly-linked regular file", path.display()),
        ));
    }
    Ok(file)
}

#[cfg(unix)]
fn normalize_absolute_state_path(path: &Path) -> std::io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::from("/");
    for component in absolute.components() {
        match component {
            std::path::Component::RootDir | std::path::Component::CurDir => {}
            std::path::Component::Normal(component) => normalized.push(component),
            std::path::Component::ParentDir => {
                if !normalized.pop() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "the state path escapes the filesystem root",
                    ));
                }
            }
            std::path::Component::Prefix(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "the state path has a non-Unix prefix",
                ));
            }
        }
    }
    Ok(normalized)
}

#[cfg(unix)]
fn resolve_trusted_system_links(path: &Path) -> std::io::Result<PathBuf> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    let mut resolved = PathBuf::from("/");
    let mut components = path.components().filter_map(|component| match component {
        std::path::Component::Normal(component) => Some(component),
        _ => None,
    });
    while let Some(component) = components.next() {
        let candidate = resolved.join(component);
        let metadata = match std::fs::symlink_metadata(&candidate) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                resolved.push(component);
                for remaining in components {
                    resolved.push(remaining);
                }
                return Ok(resolved);
            }
            Err(source) => return Err(source),
        };
        if !metadata.file_type().is_symlink() {
            resolved.push(component);
            continue;
        }
        let containing = candidate.parent().unwrap_or_else(|| Path::new("/"));
        let containing_metadata = std::fs::metadata(containing)?;
        let trusted_system_link = metadata.uid() == 0
            && containing_metadata.uid() == 0
            && containing_metadata.permissions().mode() & 0o022 == 0;
        if !trusted_system_link {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "the state path has an untrusted symlink ancestor: {}",
                    candidate.display()
                ),
            ));
        }
        resolved = std::fs::canonicalize(candidate)?;
    }
    Ok(resolved)
}

impl IdentityStore {
    /// Read an identity file, returning `Ok(None)` if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read (for any reason
    /// other than not-found) or its contents fail to parse as an [`Identity`].
    pub fn load_optional(path: &Path) -> Result<Option<Identity>> {
        match read_regular_file_optional(path) {
            Ok(Some(s)) => {
                let mut identity: Identity = serde_json::from_str(&s).context(ParseSnafu {
                    path: path.to_path_buf(),
                })?;
                identity.drop_unusable_portal_pages();
                Ok(Some(identity))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(Error::Io {
                path: path.to_path_buf(),
                source: err,
            }),
        }
    }

    /// Async wrapper around [`Self::load_optional`] for Tokio call sites.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::load_optional`]. A failed blocking
    /// task is reported as an I/O error without exposing identity contents.
    pub async fn load_optional_async(path: PathBuf) -> Result<Option<Identity>> {
        let task_path = path.clone();
        tokio::task::spawn_blocking(move || Self::load_optional(&task_path))
            .await
            .unwrap_or_else(|join| {
                Err(Error::Io {
                    path,
                    source: std::io::Error::other(format!("identity load task panicked: {join}")),
                })
            })
    }

    /// Persist an identity to disk atomically with `0600` perms on Unix.
    ///
    /// This acquires the config directory's enrollment/removal transaction so
    /// every creating writer participates in the same resurrection boundary.
    /// Enrollment, which already owns that transaction, uses the internal
    /// `store_with_transaction` path instead of acquiring it recursively.
    ///
    /// # Errors
    ///
    /// Returns an error if the parent directory cannot be created, the
    /// identity cannot be serialized, or the file cannot be written.
    pub fn store(path: &Path, identity: &Identity) -> Result<()> {
        let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let transaction = acquire_update_transaction(config_dir, path)?;
        Self::store_with_transaction(path, identity, &transaction)
    }

    pub(crate) fn store_with_transaction(
        path: &Path,
        identity: &Identity,
        transaction: &crate::draft::EnrollmentTransactionLock,
    ) -> Result<()> {
        let path = protected_identity_path(transaction, path)?;
        let _guard = write_lock();
        Self::store_locked(&path, identity)
    }

    /// Record the app this instance's telemetry belongs to, preserving the org
    /// and credential fields. An app change clears the previous app's name and
    /// monitor URL because they cannot describe the new app.
    ///
    /// A read-modify-write, which is why it runs under [`write_lock`]. Complete
    /// credential updates use [`IdentityStore::store_credential_update`] under
    /// the same lock and merge this field from disk, preventing either update
    /// from reverting the other.
    ///
    /// `Ok(())` with nothing written when the file does not exist. The app id
    /// arrives over an established stream, which requires an identity, so this
    /// means the identity was cleared concurrently (a `Remove`) — and
    /// re-creating the file from a partial value would resurrect an instance the
    /// control plane just released.
    ///
    /// # Errors
    ///
    /// Returns an error if the existing file cannot be read or parsed, or if the
    /// updated identity cannot be written.
    pub fn store_app_id(config_dir: &Path, path: &Path, app_id: &str) -> Result<()> {
        Self::set_app_id(config_dir, path, Some(app_id)).map(|_| ())
    }

    /// Set or clear the app this instance's telemetry belongs to, leaving every
    /// credential field and the enrollment org as they are on disk. An app
    /// change clears the previous app's name and monitor URL.
    ///
    /// Returns `Ok(false)` when the identity file no longer exists. Callers
    /// handling a control command must not acknowledge a durable update in that
    /// case; the identity may have been removed concurrently.
    ///
    /// # Errors
    ///
    /// Returns an error if the existing file cannot be read or parsed, or if the
    /// updated identity cannot be written.
    pub fn set_app_id(config_dir: &Path, path: &Path, app_id: Option<&str>) -> Result<bool> {
        let transaction = acquire_update_transaction(config_dir, path)?;
        let path = protected_identity_path(&transaction, path)?;
        let _guard = write_lock();
        let Some(mut identity) = Self::load_optional(&path)? else {
            return Ok(false);
        };
        if identity.app_id.as_deref() == app_id {
            return Ok(true);
        }
        identity.app_id = app_id.map(str::to_string);
        // The stored app name and monitor URL describe the app this instance
        // was attached to a moment ago. Under a different app id they would
        // present one app's monitor page as another's, so they go with the old
        // id; the next `AttachApp` delivers the new app's metadata. The org is
        // not attachment-scoped and stays.
        identity.app_name = None;
        identity.monitor_url = None;
        Self::store_locked(&path, &identity)?;
        Ok(true)
    }

    /// Apply the control plane's app attachment state — the app id and the
    /// portal metadata that describes it — as one atomic update, leaving every
    /// credential field as it is on disk.
    ///
    /// The whole tuple is resolved, compared, and written together: an update
    /// that changes only one member (a renamed app, a moved monitor URL) must
    /// still persist, and a partial write could pair one app's id with
    /// another's metadata. Each member resolves by one rule — **presence
    /// updates, absence preserves, except that project-scoped metadata never
    /// survives an app change**:
    ///
    /// - [`Identity::org_name`] is instance-level (see its field docs): a
    ///   present `org_name` updates it, an absent one leaves it — on attach
    ///   *and* on detach — so a control plane that sends app-id-only
    ///   attachments cannot wipe the org a fuller one recorded.
    /// - [`Identity::app_name`] / [`Identity::monitor_url`] describe exactly
    ///   one app. Re-attaching the *same* app updates them when present and
    ///   preserves them when absent; attaching a *different* app takes the
    ///   command's values verbatim, so one app's monitor page can never be
    ///   presented as another's. `None` detaches and clears both along with
    ///   the app id.
    ///
    /// Returns the attachment state now on disk, or `Ok(None)` when the
    /// identity file no longer exists — nothing is written and no file is
    /// created. Callers handling a control command must not acknowledge a
    /// durable update in that case; the identity may have been removed
    /// concurrently, and re-creating it from attachment state would resurrect
    /// an instance the control plane just released.
    ///
    /// # Errors
    ///
    /// Returns an error if the existing file cannot be read or parsed, or if the
    /// updated identity cannot be written.
    pub fn set_attachment(
        config_dir: &Path,
        path: &Path,
        attachment: Option<&AppAttachment>,
    ) -> Result<Option<AttachmentState>> {
        let transaction = acquire_update_transaction(config_dir, path)?;
        let path = protected_identity_path(&transaction, path)?;
        let _guard = write_lock();
        let Some(mut identity) = Self::load_optional(&path)? else {
            return Ok(None);
        };
        let resolved = match attachment {
            Some(attachment) => {
                let same_app = identity.app_id.as_deref() == Some(attachment.app_id.as_str());
                // The monitor page is Cloud-constructed portal metadata that the
                // runtime prints and an operator opens, so it passes the one
                // portal-link rule here, at the writer — the same place the
                // enrollment validates its create-project page.
                //
                // The outer `Option` is what keeps presence-updates and
                // absence-preserves intact through that check: a command that
                // named a page the rule rejects has still *named* one, so it
                // clears the stale page rather than leaving the old one standing
                // behind an invalid delivery. Only an omitted page preserves.
                let delivered = attachment
                    .monitor_url
                    .as_deref()
                    .map(crate::config::safe_portal_url);
                let (app_name, monitor_url) = if same_app {
                    (
                        attachment
                            .app_name
                            .clone()
                            .or_else(|| identity.app_name.clone()),
                        delivered.unwrap_or_else(|| identity.monitor_url.clone()),
                    )
                } else {
                    (attachment.app_name.clone(), delivered.flatten())
                };
                AttachmentState {
                    app_id: Some(attachment.app_id.clone()),
                    org_name: attachment
                        .org_name
                        .clone()
                        .or_else(|| identity.org_name.clone()),
                    app_name,
                    monitor_url,
                }
            }
            None => AttachmentState {
                app_id: None,
                org_name: identity.org_name.clone(),
                app_name: None,
                monitor_url: None,
            },
        };
        if identity.app_id == resolved.app_id
            && identity.org_name == resolved.org_name
            && identity.app_name == resolved.app_name
            && identity.monitor_url == resolved.monitor_url
        {
            return Ok(Some(resolved));
        }
        identity.app_id.clone_from(&resolved.app_id);
        identity.org_name.clone_from(&resolved.org_name);
        identity.app_name.clone_from(&resolved.app_name);
        identity.monitor_url.clone_from(&resolved.monitor_url);
        Self::store_locked(&path, &identity)?;
        Ok(Some(resolved))
    }

    /// Persist a complete identity update without overwriting an attachment
    /// change made after the caller cloned its in-memory identity.
    ///
    /// Certificate renewal and encryption-key retirement both replace
    /// credential material from the client's in-memory clone. The attachment
    /// is owned by control commands and can be newer on disk, so every such
    /// full identity update must merge it while holding [`write_lock`].
    ///
    /// `expected_identifier` and `expected_public_key_pem` fence the durable
    /// credential generation the caller cloned before a long-running request.
    /// A removal followed by re-enrollment can therefore win the transaction
    /// without a queued stale update overwriting the replacement. Missing and
    /// superseded generations are reported without writing.
    ///
    /// # Errors
    ///
    /// Returns an error if the existing file cannot be read or parsed, or if the
    /// merged identity cannot be written.
    pub fn store_credential_update(
        config_dir: &Path,
        path: &Path,
        expected_identifier: &str,
        expected_public_key_pem: &str,
        credential_update: &Identity,
    ) -> Result<CredentialUpdateOutcome> {
        let transaction = acquire_update_transaction(config_dir, path)?;
        let path = protected_identity_path(&transaction, path)?;
        let _guard = write_lock();
        let Some(current) = Self::load_optional(&path)? else {
            return Ok(CredentialUpdateOutcome::Missing);
        };
        if current.identifier != expected_identifier
            || current.public_key_pem != expected_public_key_pem
        {
            return Ok(CredentialUpdateOutcome::Superseded(current));
        }
        let mut merged = credential_update.clone();
        // The whole attachment tuple, not just the app id: a command handler
        // may have written any of these after the caller cloned its identity,
        // and a credential update must not revert them. The enrollment's
        // new-project page travels with them: it is portal metadata this
        // process may never have loaded, and a renewal must not erase it.
        merged.app_id = current.app_id;
        merged.org_name = current.org_name;
        merged.app_name = current.app_name;
        merged.monitor_url = current.monitor_url;
        merged.new_project_url = current.new_project_url;
        // A durable binding already on disk wins over a stale renewal clone.
        // A legacy identity has no binding, so retain the endpoint the renewal
        // just proved by succeeding and promote it atomically with the rotated
        // credential.
        if current.control_plane_endpoint.is_some() {
            merged.control_plane_endpoint = current.control_plane_endpoint;
        }
        Self::store_locked(&path, &merged)?;
        Ok(CredentialUpdateOutcome::Stored(merged))
    }

    /// The write itself, with the caller already holding [`write_lock`].
    fn store_locked(path: &Path, identity: &Identity) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context(IoSnafu {
                path: parent.to_path_buf(),
            })?;
        }
        let bytes = serde_json::to_vec_pretty(identity).context(SerializeSnafu)?;
        atomic_write(path, &bytes)
    }

    /// Remove the identity file. No-op if it doesn't exist.
    ///
    /// Acquires the config directory's persistent enrollment transaction before
    /// [`write_lock`], the same order used by every read-modify-write. This
    /// serializes runtime revocation and remote removal with writers in another
    /// process, preventing a stale writer from resurrecting the identity.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be removed.
    pub fn clear(path: &Path) -> Result<()> {
        let transaction = acquire_removal_transaction(path)?;
        Self::clear_with_transaction(path, &transaction)
    }

    /// Remove the identity while the caller retains ownership of the config
    /// directory's enrollment transaction.
    ///
    /// This is the non-recursive removal path for `spice connect remove`, which
    /// owns one transaction across the identity, draft, endpoint, cached
    /// secrets, and installed service. The transaction must protect the same
    /// config directory as `path`.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction belongs to another directory or
    /// when the identity file exists but cannot be removed.
    pub fn clear_with_transaction(
        path: &Path,
        transaction: &crate::draft::EnrollmentTransactionLock,
    ) -> Result<()> {
        let path = protected_identity_path(transaction, path)?;
        let _guard = write_lock();
        Self::clear_locked(&path)
    }

    /// Async variant of [`Self::clear_with_transaction`] for a caller that
    /// already owns the config directory's enrollment transaction.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::clear_with_transaction`], or an error
    /// when the blocking task carrying the removal panics.
    pub async fn clear_with_transaction_async(
        path: PathBuf,
        transaction: Arc<crate::draft::EnrollmentTransactionLock>,
    ) -> Result<()> {
        tokio::task::spawn_blocking(move || Self::clear_with_transaction(&path, &transaction))
            .await
            .map_err(|source| Error::ClearTaskPanicked { source })?
    }

    /// Async variant of [`IdentityStore::clear`] for use on the Tokio driver
    /// task, where blocking on synchronous `std::fs` I/O would stall a worker
    /// thread. Same semantics: no-op if the file doesn't exist.
    ///
    /// Runs on the blocking pool rather than awaiting `tokio::fs` directly: the
    /// removal has to happen under the same [`write_lock`] every other writer
    /// takes, and a std mutex must never be held across an `.await`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be removed, or if the
    /// blocking task carrying the removal panicked.
    pub async fn clear_async(path: &Path) -> Result<()> {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || Self::clear(&path))
            .await
            .map_err(|source| Error::ClearTaskPanicked { source })?
    }

    /// The removal itself, with the caller already holding [`write_lock`].
    fn clear_locked(path: &Path) -> Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => sync_parent_directory(path).context(IoSnafu {
                path: path.to_path_buf(),
            }),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                match sync_parent_directory(path) {
                    Ok(()) => Ok(()),
                    Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(source) => Err(Error::Io {
                        path: path.to_path_buf(),
                        source,
                    }),
                }
            }
            Err(err) => Err(Error::Io {
                path: path.to_path_buf(),
                source: err,
            }),
        }
    }

    /// Generate fresh enrollment material: an ECDSA P-256 identity keypair
    /// with a PKCS#10 CSR for it, plus an X25519 encryption keypair, all
    /// PEM-encoded. Called before the cloud enroll request — and again
    /// before every renewal, since each renewal rotates both the identity
    /// keypair and the encryption keypair — so the client proves possession
    /// of its identity key (the CSR self-signature) before the cloud CA
    /// issues the leaf certificate. The fresh X25519 keypair is sent to
    /// the cloud on renew so it can begin sealing secrets to the new key.
    ///
    /// The CSR carries a stable common name and a `clientAuth` extended
    /// key usage so the issued leaf is directly usable as an mTLS client
    /// certificate. The encryption public key is sent at enroll and at
    /// renewal (`enc_pubkey_pem`, RFC 8410 SPKI) for the cloud to HPKE-seal
    /// secret payloads to.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Enrollment`] if key generation or CSR
    /// serialization fails, or [`Error::EncKeyGeneration`] if the
    /// encryption keypair cannot be generated or encoded.
    pub fn generate_enrollment() -> Result<EnrollmentMaterial> {
        let key_pair = KeyPair::generate().context(EnrollmentSnafu)?;
        let private_key_pem = key_pair.serialize_pem();
        let public_key_pem = key_pair.public_key_pem();

        // No SANs: this is a client identity, not a server, so it is
        // identified by its issued serial / subject, not a hostname.
        let mut params = CertificateParams::new(Vec::<String>::new()).context(EnrollmentSnafu)?;
        params
            .distinguished_name
            .push(DnType::CommonName, "spice-standalone-runtime");
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];

        let csr = params
            .serialize_request(&key_pair)
            .context(EnrollmentSnafu)?;
        let csr_pem = csr.pem().context(EnrollmentSnafu)?;

        let (enc_private_key_pem, enc_public_key_pem) = generate_enc_keypair_pem()?;

        Ok(EnrollmentMaterial {
            private_key_pem,
            public_key_pem,
            csr_pem,
            enc_private_key_pem,
            enc_public_key_pem,
        })
    }
}

/// Generate an X25519 encryption keypair, returning `(private PKCS#8 PEM,
/// public SPKI PEM)` — the RFC 8410 encodings the cloud expects in
/// `enc_pubkey_pem` and that later unseal HPKE payloads locally. Delegates
/// to `cloud-connect-crypto`, the single source of the sealed-secret wire
/// crypto, so the keypair enrolled here is byte-compatible with the suite
/// the cloud seals against.
fn generate_enc_keypair_pem() -> Result<(String, String)> {
    let keypair = cloud_connect_crypto::EncryptionKeypair::generate().map_err(|source| {
        Error::EncKeyGeneration {
            reason: source.to_string(),
        }
    })?;
    Ok((
        keypair.to_pkcs8_pem().to_string(),
        keypair.public_key_spki_pem(),
    ))
}

/// Freshly-generated enrollment material returned by
/// [`IdentityStore::generate_enrollment`]: the client keypair (PEM) plus a
/// PKCS#10 CSR built from it. The private key is retained locally and, on
/// successful enroll/renew, persisted into the [`Identity`] alongside the
/// signed leaf; the CSR is sent in the HTTP enroll (or renew) request.
#[derive(Clone)]
pub struct EnrollmentMaterial {
    pub private_key_pem: String,
    pub public_key_pem: String,
    pub csr_pem: String,
    /// X25519 encryption private key (PKCS#8 PEM); persisted into the
    /// [`Identity`] at enroll and on each renewal alongside the rotated
    /// identity keypair.
    pub enc_private_key_pem: String,
    /// X25519 encryption public key (RFC 8410 SPKI PEM); sent as the
    /// enroll request's `enc_pubkey_pem`.
    pub enc_public_key_pem: String,
}

impl EnrollmentMaterial {
    /// Why persisted enrollment material is not safe to replay, if any.
    ///
    /// Every reason is deliberately independent of the key bytes so a corrupt
    /// draft can be diagnosed without reproducing credential material.
    pub(crate) fn validation_error(&self) -> Option<&'static str> {
        let Ok(private_key) = KeyPair::from_pem(&self.private_key_pem) else {
            return Some("the identity private key is not valid PKCS key material");
        };
        let Ok(public_key) = pem::parse(&self.public_key_pem) else {
            return Some("the identity public key is not valid PEM");
        };
        if public_key.tag() != "PUBLIC KEY" {
            return Some("the identity public key has an invalid PEM label");
        }
        if public_key.contents() != private_key.subject_public_key_info().as_slice() {
            return Some("the identity public and private keys do not match");
        }

        let Ok(csr) = CertificateSigningRequestParams::from_pem(&self.csr_pem) else {
            return Some("the certificate request is invalid or has a bad self-signature");
        };
        if csr.public_key.der_bytes() != private_key.der_bytes() {
            return Some("the certificate request and identity private key do not match");
        }

        let Ok(enc_keypair) =
            cloud_connect_crypto::EncryptionKeypair::from_pkcs8_pem(&self.enc_private_key_pem)
        else {
            return Some("the secret-delivery private key is not valid X25519 key material");
        };
        let Ok(enc_public_key) = pem::parse(&self.enc_public_key_pem) else {
            return Some("the secret-delivery public key is not valid PEM");
        };
        if enc_public_key.tag() != "PUBLIC KEY" {
            return Some("the secret-delivery public key has an invalid PEM label");
        }
        let Ok(expected_enc_public_key) = pem::parse(enc_keypair.public_key_spki_pem()) else {
            return Some("the secret-delivery public key could not be derived");
        };
        if enc_public_key.contents() != expected_enc_public_key.contents() {
            return Some("the secret-delivery public and private keys do not match");
        }
        None
    }
}

impl std::fmt::Debug for EnrollmentMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnrollmentMaterial")
            .field("private_key_pem", &"[REDACTED]")
            .field("public_key_pem", &"[PUBLIC KEY]")
            .field("csr_pem", &"[CERTIFICATE REQUEST]")
            .field("enc_private_key_pem", &"[REDACTED]")
            .field("enc_public_key_pem", &"[PUBLIC KEY]")
            .finish()
    }
}

/// Write the identity file, mapping I/O failures onto the identity error type.
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_write_owner_only(path, bytes).context(IoSnafu {
        path: path.to_path_buf(),
    })
}

/// The directory a file lives in, as a path that can actually be opened.
///
/// `Path::parent` answers `Some("")` for a bare relative name like
/// `identity.json` — the current directory, spelled in a way no syscall accepts.
/// Left as-is it turns `read_dir` into a `NotFound` that reads as "no debris
/// here" and turns the directory sync into a failure after the file is already
/// unlinked.
pub(crate) fn parent_directory(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

/// Which writer's artifacts may appear beside a file.
///
/// Two writers, two shapes, and they do not overlap: the runtime writes the
/// cache, the draft and the identity through [`atomic_write_owner_only`], while
/// `spice connect` writes the operation journals and the endpoint override
/// through its own. Accepting both everywhere would delete a
/// `.identity.json.7.candidate` nothing here can create, and let a
/// `.cloud-endpoint.<uuid>.tmp` — equally impossible — fail every release.
/// Dot-prefixed runtime artifacts are only the v4 UUID temps emitted by the
/// current writer.
#[derive(Clone, Copy, Debug)]
pub enum ArtifactKinds {
    /// `.tmp` from [`atomic_write_owner_only`].
    Runtime,
    /// `.candidate` from `spice connect`'s writer.
    Connect,
}

/// The `(token, extension)` of a sibling `kinds` could have written beside
/// `file_name`, or `None` for anything else in the directory.
///
/// Prefix and extension alone are not enough. A sibling somebody named
/// themselves — `.cloud-endpoint.notes.candidate`, `.identity.json.manual.bak` —
/// is not ours to delete, and a stray `.tmp` would be worse than deleted: the
/// release reads an unreclaimable temp as a writer's in-flight file and fails, so
/// one sitting in the directory would fail every `Remove` for good.
///
/// Every comparison is exact, because every emitted name is: the extensions are
/// written lowercase, the UUID is `Uuid::new_v4`'s lowercase hyphenated
/// `Display`, and the candidate token is `u64::to_string`.
fn produced_artifact<'a>(
    entry_name: &'a str,
    prefix: &str,
    kinds: ArtifactKinds,
) -> Option<(&'a str, &'a str)> {
    let (token, extension) = entry_name
        .strip_prefix(prefix)
        .and_then(|rest| rest.rsplit_once('.'))?;
    let produced = match (kinds, extension) {
        (ArtifactKinds::Runtime, "tmp") => uuid::Uuid::parse_str(token).is_ok_and(|id| {
            id.get_version_num() == 4
                && id.get_variant() == uuid::Variant::RFC4122
                && id.hyphenated().to_string() == token
        }),
        (ArtifactKinds::Connect, "candidate") => token
            .parse::<u64>()
            .is_ok_and(|number| number.to_string() == token),
        _ => false,
    };
    produced.then_some((token, extension))
}

/// Whether `entry_name` is an exact sibling artifact the runtime atomic writer
/// can create for `path`: a canonical UUID-v4 `.tmp`.
#[must_use]
pub fn is_runtime_atomic_write_artifact(path: &Path, entry_name: &str) -> bool {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let prefix = format!(".{file_name}.");
    produced_artifact(entry_name, &prefix, ArtifactKinds::Runtime).is_some()
}

/// Whether an interrupted writer left an exact artifact beside `path`.
///
/// This is the non-mutating half of release discovery. Interactive removal
/// uses it before confirmation so it can distinguish a genuinely empty
/// directory from one that needs crash-debris cleanup without deleting
/// anything the operator has not yet approved.
///
/// # Errors
///
/// Returns an error when the artifact directory cannot be inspected safely.
pub fn release_artifacts_present(path: &Path, kinds: ArtifactKinds) -> std::io::Result<bool> {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(std::io::Error::other(format!(
            "cannot identify interrupted writes beside {}: its file name is not valid UTF-8",
            path.display()
        )));
    };
    let prefix = format!(".{file_name}.");
    let entries = match std::fs::read_dir(parent_directory(path)) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(source) => return Err(source),
    };
    for entry in entries {
        let entry = entry?;
        let entry_name = entry.file_name();
        if entry_name
            .to_str()
            .is_some_and(|name| produced_artifact(name, &prefix, kinds).is_some())
            && entry.file_type()?.is_file()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// A newly-created writer gets ample time to acquire its advisory lock before
/// another process may consider its temp file abandoned. The lock remains the
/// authoritative liveness signal after this age; time alone never authorizes
/// deletion of an active writer.
pub(crate) const ABANDONED_TEMP_MIN_AGE: std::time::Duration = std::time::Duration::from_hours(1);

/// Reclaim secret-bearing temp files left by a process that exited before
/// promotion.
///
/// Temp names are unique per writer. Each live writer holds an exclusive
/// advisory lock from creation through rename, so cleanup removes only an old
/// exact-name match whose lock can be acquired. This bounds credential debris
/// without allowing concurrent writers to delete one another's files.
fn cleanup_abandoned_atomic_temps(
    path: &Path,
    minimum_age: std::time::Duration,
) -> std::io::Result<()> {
    cleanup_abandoned_atomic_temps_with(path, minimum_age, ArtifactKinds::Runtime, |_, _| Ok(()))
}

fn cleanup_abandoned_atomic_temps_with<F>(
    path: &Path,
    minimum_age: std::time::Duration,
    kinds: ArtifactKinds,
    before_remove: F,
) -> std::io::Result<()>
where
    F: Fn(&std::fs::File, &Path) -> std::io::Result<()>,
{
    let dir = parent_directory(path);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("identity.json");
    let prefix = format!(".{file_name}.");
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };

    for entry in entries {
        let entry = entry?;
        let entry_name = entry.file_name();
        let Some(entry_name) = entry_name.to_str() else {
            continue;
        };
        let is_temp = produced_artifact(entry_name, &prefix, kinds)
            .is_some_and(|(_, extension)| extension == "tmp");
        if !is_temp || !entry.file_type()?.is_file() {
            continue;
        }

        let old_enough = entry
            .metadata()?
            .modified()?
            .elapsed()
            .is_ok_and(|age| age >= minimum_age);
        if !old_enough {
            continue;
        }

        let file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(entry.path())
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !fs4::fs_std::FileExt::try_lock_exclusive(&file)? {
            continue;
        }
        // Keep the lock through unlink: acquiring it establishes that no live
        // writer owns this inode, and releasing it before removal would split
        // that liveness decision from the destructive action.
        before_remove(&file, &entry.path())?;
        let removal = std::fs::remove_file(entry.path());
        drop(file);
        match removal {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// What a release could not reclaim, split by whether it can still be renamed
/// onto the canonical path.
#[derive(Debug, Default)]
pub(crate) struct RemainingArtifacts {
    /// Temps whose writer may still promote them, undoing the release.
    pub(crate) promotable: Vec<PathBuf>,
    /// Artifacts that remain on disk but cannot become the canonical file.
    pub(crate) inert: Vec<PathBuf>,
}

/// Reclaim the secret-bearing artifacts an interrupted atomic write leaves beside
/// `path`, for a release that is deleting `path` itself.
///
/// [`atomic_write_owner_only`] writes through a uniquely-named temp and unlinks
/// it best-effort on failure. A temp can outlive the process that made it,
/// holding the same credential the canonical file did — so a release that
/// removed only the canonical file would report a host clean while leaving a
/// complete copy of what it was supposed to destroy.
///
/// Temps are reclaimed on the same terms as anywhere else: a live writer holds an
/// exclusive advisory lock on its own temp, so *acquiring* that lock is what
/// establishes no writer owns the file, and only a temp older than `minimum_age`
/// whose lock the reclaim takes may be removed. A release does not relax that —
/// deleting a live writer's file to tidy up would be the worse outcome.
///
/// A temp that survives is therefore reported as **promotable**, not merely
/// retained: whoever owns it can still rename it onto the canonical path, which
/// would put back the very file the release is removing, after the release has
/// reported success. A caller that cannot tolerate that must fail rather than
/// acknowledge.
///
/// So are `.candidate` files, the artifact `spice connect` leaves for the state
/// it writes — the operation journals and the endpoint override. Those get no
/// per-file lock, so the temp rule cannot judge them; what authorizes removing
/// them is that a release holds `connect.lock` for its whole run, which is the
/// same lock their writer holds for its whole transaction. No `spice connect`
/// can be mid-write, so any candidate present has been abandoned.
///
/// Returns what is still present afterwards, split by whether it can still
/// become the canonical file.
pub(crate) fn release_atomic_write_artifacts(
    path: &Path,
    minimum_age: std::time::Duration,
    kinds: ArtifactKinds,
) -> std::io::Result<RemainingArtifacts> {
    // Fail closed on a name this cannot read, rather than scanning under the
    // fallback prefix the writer uses. That fallback is `identity.json`, so the
    // artifacts of an unreadable name are spelled exactly like a real
    // `identity.json`'s and the two are indistinguishable from the outside:
    // reclaiming them would delete the other file's debris, and finding one held
    // would fail this release over the other file's writer. Neither is a
    // judgement worth making blind. It stops the release before the identity is
    // cleared — the only step that cannot be retried — so the instance stays
    // connected and can be released once the path is one both sides can name.
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(std::io::Error::other(format!(
            "cannot identify interrupted writes beside {}: its file name is not valid UTF-8, so they cannot be told apart from another file's",
            path.display()
        )));
    };
    cleanup_abandoned_atomic_temps_with(path, minimum_age, kinds, |_, _| Ok(()))?;

    let mut remaining = RemainingArtifacts::default();
    let dir = parent_directory(path);
    let prefix = format!(".{file_name}.");
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RemainingArtifacts::default());
        }
        Err(error) => return Err(error),
    };

    for entry in entries {
        let entry = entry?;
        let entry_name = entry.file_name();
        let Some(entry_name) = entry_name.to_str() else {
            continue;
        };
        let Some((_, extension)) = produced_artifact(entry_name, &prefix, kinds) else {
            continue;
        };
        // Only regular files, as the temp cleanup requires: none of these writers
        // creates a directory or a symlink, so anything else wearing the name came
        // from somewhere else and is neither ours to delete nor evidence of a
        // writer.
        if !entry.file_type()?.is_file() {
            continue;
        }
        let is_temp = extension == "tmp";
        let is_abandoned = !is_temp;
        if is_abandoned {
            match std::fs::remove_file(entry.path()) {
                Ok(()) => continue,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => {}
            }
            // It could not be removed, but nothing renames a candidate into
            // place on its own, so it cannot undo the release.
            remaining.inert.push(entry.path());
            continue;
        }
        remaining.promotable.push(entry.path());
    }

    // Durable, like the canonical removals: reporting that no credential copy
    // remains is a claim about what survives a crash, and an unlink that is
    // acknowledged but not synced can bring one back. One sync covers every
    // unlink above, including the temps the reclaim removed.
    sync_parent_directory(path)?;
    Ok(remaining)
}

/// Prove that a release can scan every writer artifact and that no runtime temp
/// could still be promoted, without deleting anything. Callers run this for
/// every canonical release target before unlinking the first one; once their
/// mutation/enrollment locks are held, no conforming writer can appear between
/// this gate and cleanup.
#[expect(
    dead_code,
    reason = "release preflight primitive is staged before its authenticated orchestration caller"
)]
pub(crate) fn preflight_release_atomic_write_artifacts(
    path: &Path,
    minimum_age: std::time::Duration,
    kinds: ArtifactKinds,
) -> std::io::Result<Option<PathBuf>> {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(std::io::Error::other(format!(
            "cannot identify interrupted writes beside {}: its file name is not valid UTF-8",
            path.display()
        )));
    };

    let prefix = format!(".{file_name}.");
    let entries = match std::fs::read_dir(parent_directory(path)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let entry_name = entry.file_name();
        let Some((_, extension)) = entry_name
            .to_str()
            .and_then(|name| produced_artifact(name, &prefix, kinds))
        else {
            continue;
        };
        if !entry.file_type()?.is_file() || extension != "tmp" {
            continue;
        }

        let old_enough = entry
            .metadata()?
            .modified()?
            .elapsed()
            .is_ok_and(|age| age >= minimum_age);
        if !old_enough {
            return Ok(Some(entry.path()));
        }
        let file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(entry.path())
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !fs4::fs_std::FileExt::try_lock_exclusive(&file)? {
            return Ok(Some(entry.path()));
        }
        drop(file);
    }
    Ok(None)
}

/// Reclaim every interrupted-write artifact beside `path`, failing if any
/// writer-owned or inert copy remains.
///
/// The caller must already exclude runtime and CLI writers for the complete
/// operation. Under that exclusion, a zero age is safe: a live atomic writer
/// still owns an advisory lock, while an unlocked artifact cannot become live
/// again. This stricter form is used by local removal before it destroys the
/// canonical credential that would make a retry possible.
///
/// # Errors
///
/// Returns an error when the artifact directory cannot be scanned or synced,
/// an artifact cannot be reclaimed, or another writer still owns a promotable
/// temporary file.
pub fn reclaim_all_release_artifacts(path: &Path, kinds: ArtifactKinds) -> std::io::Result<()> {
    let remaining = release_atomic_write_artifacts(path, std::time::Duration::ZERO, kinds)?;
    if let Some(artifact) = remaining
        .promotable
        .first()
        .or_else(|| remaining.inert.first())
    {
        return Err(std::io::Error::other(format!(
            "interrupted-write artifact {} remains beside {}",
            artifact.display(),
            path.display()
        )));
    }
    Ok(())
}

/// Atomically write `bytes` to `path` with owner-only permissions.
///
/// Shared with [`crate::secret_cache`]: both files hold secret material and need
/// the same guarantees — never world-readable, never observed half-written.
/// The file contents and parent directory entry are both synchronized before
/// success is reported, so a successful enrollment remains durable across a
/// power loss after the atomic rename.
#[cfg(unix)]
pub(crate) fn atomic_write_owner_only(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    use std::os::unix::fs::PermissionsExt as _;

    cleanup_abandoned_atomic_temps(path, ABANDONED_TEMP_MIN_AGE)?;
    let dir = parent_directory(path);
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("identity.json");
    let tmp_path = dir.join(format!(".{file_name}.{}.tmp", uuid::Uuid::new_v4()));

    // Every writer owns a distinct temp inode. Concurrent processes may safely
    // promote complete files to the same destination without truncating or
    // deleting one another's in-progress write.
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&tmp_path)?;
        fs4::fs_std::FileExt::lock_exclusive(&file)?;
        // Re-assert mode in case umask/file-creation flags interfered.
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp_path, path)?;
        // The unique temp name no longer exists, so cleanup cannot target this
        // inode. Release the lock before publishing success.
        drop(file);
        sync_parent_directory(path)
    })();

    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    result
}

#[cfg(not(unix))]
pub(crate) fn atomic_write_owner_only(_path: &Path, _bytes: &[u8]) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "owner-only Cloud Connect state files are unsupported on this platform",
    ))
}

/// Synchronize the directory entry containing `path` after a rename, hard
/// link, or removal. Synchronizing only the file contents does not make the
/// directory metadata durable across power loss.
#[cfg(unix)]
pub(crate) fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
    let dir = parent_directory(path);
    std::fs::File::open(dir)?.sync_all()
}

/// Directory metadata synchronization is unavailable on non-Unix platforms.
/// File contents are still flushed before promotion.
#[cfg(not(unix))]
pub(crate) fn sync_parent_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_identity() -> Identity {
        let key_pair = KeyPair::generate().expect("generate sample identity key");
        let certificate = CertificateParams::new(Vec::<String>::new())
            .expect("build sample identity certificate parameters")
            .self_signed(&key_pair)
            .expect("sign sample identity certificate");
        let encryption_keypair = cloud_connect_crypto::EncryptionKeypair::generate()
            .expect("generate sample encryption key");
        Identity {
            identifier: "inst_test".to_string(),
            identity_cert_pem: certificate.pem(),
            private_key_pem: key_pair.serialize_pem(),
            public_key_pem: key_pair.public_key_pem(),
            ca_bundle_pem: "-----BEGIN CERTIFICATE-----\nMOCKCA\n-----END CERTIFICATE-----\n"
                .to_string(),
            gateway_addr: "gateway.test.spice.ai:443".to_string(),
            control_plane_endpoint: None,
            not_after_unix: None,
            app_id: None,
            org_name: None,
            app_name: None,
            monitor_url: None,
            new_project_url: None,
            enc_private_key_pem: encryption_keypair.to_pkcs8_pem().to_string(),
            enc_public_key_pem: encryption_keypair.public_key_spki_pem(),
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
        }
    }

    fn set_sample_certificate_validity(
        identity: &mut Identity,
        not_before: (i32, u8, u8),
        not_after: (i32, u8, u8),
    ) {
        let key_pair = KeyPair::from_pem(&identity.private_key_pem).expect("parse identity key");
        let mut params = CertificateParams::new(Vec::<String>::new())
            .expect("build sample identity certificate parameters");
        params.not_before = rcgen::date_time_ymd(not_before.0, not_before.1, not_before.2);
        params.not_after = rcgen::date_time_ymd(not_after.0, not_after.1, not_after.2);
        identity.identity_cert_pem = params
            .self_signed(&key_pair)
            .expect("sign sample identity certificate")
            .pem();
    }

    #[test]
    fn reconnect_validation_accepts_a_matching_certificate_and_private_key() {
        assert_eq!(sample_identity().reconnect_validation_error(), None);
    }

    #[test]
    fn reconnect_validation_rejects_malformed_certificate_and_private_key() {
        let mut identity = sample_identity();
        identity.identity_cert_pem =
            "-----BEGIN CERTIFICATE-----\nnot-a-certificate\n-----END CERTIFICATE-----\n"
                .to_string();
        assert_eq!(
            identity.reconnect_validation_error(),
            Some("the client identity certificate is not valid PEM")
        );

        let mut identity = sample_identity();
        identity.private_key_pem =
            "-----BEGIN PRIVATE KEY-----\nnot-a-private-key\n-----END PRIVATE KEY-----\n"
                .to_string();
        assert_eq!(
            identity.reconnect_validation_error(),
            Some("the client identity private key is not valid PKCS key material")
        );
    }

    #[test]
    fn reconnect_validation_rejects_a_mismatched_private_key() {
        let mut identity = sample_identity();
        identity.private_key_pem = KeyPair::generate()
            .expect("generate mismatched private key")
            .serialize_pem();
        assert_eq!(
            identity.reconnect_validation_error(),
            Some("the client identity certificate and private key do not match")
        );
    }

    #[test]
    fn reconnect_validation_rejects_a_malformed_or_mismatched_public_key() {
        let mut identity = sample_identity();
        identity.public_key_pem =
            "-----BEGIN PUBLIC KEY-----\nnot-a-public-key\n-----END PUBLIC KEY-----\n".to_string();
        assert_eq!(
            identity.reconnect_validation_error(),
            Some("the client identity public key is not valid PEM")
        );

        let mut identity = sample_identity();
        identity.public_key_pem = identity.private_key_pem.clone();
        assert_eq!(
            identity.reconnect_validation_error(),
            Some("the client identity public key has an invalid PEM label")
        );

        let mut identity = sample_identity();
        identity.public_key_pem = KeyPair::generate()
            .expect("generate mismatched public key")
            .public_key_pem();
        assert_eq!(
            identity.reconnect_validation_error(),
            Some("the client identity public and private keys do not match")
        );
    }

    #[test]
    fn reconnect_validation_rejects_a_mismatched_encryption_keypair() {
        let mut identity = sample_identity();
        identity.enc_public_key_pem = cloud_connect_crypto::EncryptionKeypair::generate()
            .expect("generate mismatched encryption key")
            .public_key_spki_pem();

        assert_eq!(
            identity.reconnect_validation_error(),
            Some("the secret-delivery public and private keys do not match")
        );
    }

    #[test]
    fn enrollment_material_validation_binds_both_keypairs_and_the_csr() {
        let material = IdentityStore::generate_enrollment().expect("generate material");
        assert_eq!(material.validation_error(), None);

        let mut mismatched_csr = material.clone();
        mismatched_csr.csr_pem = IdentityStore::generate_enrollment()
            .expect("generate mismatched CSR")
            .csr_pem;
        assert_eq!(
            mismatched_csr.validation_error(),
            Some("the certificate request and identity private key do not match")
        );

        let mut mismatched_encryption_key = material;
        mismatched_encryption_key.enc_public_key_pem = IdentityStore::generate_enrollment()
            .expect("generate mismatched encryption key")
            .enc_public_key_pem;
        assert_eq!(
            mismatched_encryption_key.validation_error(),
            Some("the secret-delivery public and private keys do not match")
        );
    }

    #[test]
    fn enrollment_enc_keypair_is_valid_rfc8410() {
        let material = IdentityStore::generate_enrollment().expect("generate material");

        // The PEMs must carry the standard RFC 8410 encodings.
        let private = pem::parse(&material.enc_private_key_pem).expect("private PEM parses");
        assert_eq!(private.tag(), "PRIVATE KEY");
        let public = pem::parse(&material.enc_public_key_pem).expect("public PEM parses");
        assert_eq!(public.tag(), "PUBLIC KEY");

        // Round-trip through the sealed-secret crypto crate (the consumer
        // of this key material): the persisted PKCS#8 must load and derive
        // the same public SPKI PEM that enrollment advertised to the cloud.
        let keypair =
            cloud_connect_crypto::EncryptionKeypair::from_pkcs8_pem(&material.enc_private_key_pem)
                .expect("persisted PKCS#8 must load in cloud-connect-crypto");
        assert_eq!(
            keypair.public_key_spki_pem(),
            material.enc_public_key_pem,
            "advertised SPKI must match the key derived from the private PKCS#8"
        );
    }

    #[test]
    fn round_trip_identity() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let identity = sample_identity();

        IdentityStore::store(&path, &identity).expect("store");
        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");

        assert_eq!(loaded.identifier, identity.identifier);
        assert_eq!(loaded.identity_cert_pem, identity.identity_cert_pem);
        assert_eq!(loaded.public_key_pem, identity.public_key_pem);
        assert_eq!(loaded.ca_bundle_pem, identity.ca_bundle_pem);
        assert_eq!(loaded.gateway_addr, identity.gateway_addr);
    }

    /// Exercise the write primitive directly, outside [`write_lock`], to model
    /// separate `spiced` processes sharing a persistent config directory.
    /// Each promotion must publish one complete payload and must not remove or
    /// truncate another writer's in-progress temp file.
    #[cfg(unix)]
    #[test]
    fn concurrent_atomic_writers_publish_one_complete_file() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let first = vec![b'a'; 128 * 1024];
        let second = vec![b'b'; 128 * 1024];
        let barrier = std::sync::Barrier::new(3);

        std::thread::scope(|scope| {
            let first_writer = scope.spawn(|| {
                barrier.wait();
                atomic_write_owner_only(&path, &first).expect("first atomic write");
            });
            let second_writer = scope.spawn(|| {
                barrier.wait();
                atomic_write_owner_only(&path, &second).expect("second atomic write");
            });
            barrier.wait();
            first_writer.join().expect("first writer thread");
            second_writer.join().expect("second writer thread");
        });

        let published = std::fs::read(&path).expect("read published file");
        assert!(
            published == first || published == second,
            "the destination must equal one complete writer payload"
        );
        let leftovers = std::fs::read_dir(dir.path())
            .expect("read tempdir")
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".identity.json.")
            })
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "temporary writes must be cleaned up");
    }

    #[test]
    fn abandoned_atomic_temp_cleanup_preserves_a_locked_writer() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let active_temp = dir
            .path()
            .join(".identity.json.aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.tmp");
        let abandoned_temp = dir
            .path()
            .join(".identity.json.11111111-2222-4333-8444-555555555555.tmp");
        let unrelated = dir
            .path()
            .join(".different.json.66666666-7777-4888-8999-aaaaaaaaaaaa.tmp");
        let active = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&active_temp)
            .expect("create active temp");
        fs4::fs_std::FileExt::lock_exclusive(&active).expect("lock active temp");
        std::fs::write(&abandoned_temp, "abandoned private credential")
            .expect("write abandoned temp");
        std::fs::write(&unrelated, "unrelated").expect("write unrelated temp");

        let observed_locked_removal = std::cell::Cell::new(false);
        cleanup_abandoned_atomic_temps_with(
            &path,
            std::time::Duration::ZERO,
            ArtifactKinds::Runtime,
            |_, candidate| {
                let contender = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(candidate)?;
                assert!(
                    !fs4::fs_std::FileExt::try_lock_exclusive(&contender)?,
                    "the cleanup lock must remain held through removal"
                );
                observed_locked_removal.set(true);
                Ok(())
            },
        )
        .expect("clean abandoned temps");

        assert!(active_temp.exists(), "a live writer must not be deleted");
        assert!(
            !abandoned_temp.exists(),
            "an unlocked abandoned credential must be reclaimed"
        );
        assert!(unrelated.exists(), "cleanup must stay scoped to one target");
        assert!(
            observed_locked_removal.get(),
            "the abandoned temp must be inspected while its cleanup lock is held"
        );

        drop(active);
        cleanup_abandoned_atomic_temps(&path, std::time::Duration::ZERO)
            .expect("clean released temp");
        assert!(
            !active_temp.exists(),
            "a writer temp becomes reclaimable after its process releases the lock"
        );
    }

    /// A bare relative name has `parent() == Some("")`, the current directory
    /// spelled in a way no syscall accepts. Every scan and directory sync in the
    /// release resolves through this, so leaving it unnormalized turns `read_dir`
    /// into a `NotFound` read as "no debris here" and turns the sync into a
    /// failure after the canonical file is already unlinked.
    #[test]
    fn a_name_without_a_directory_resolves_to_the_current_one() {
        assert_eq!(
            super::parent_directory(Path::new("identity.json")),
            Path::new("."),
            "an empty parent is the current directory"
        );
        assert_eq!(
            super::parent_directory(Path::new("/")),
            Path::new("."),
            "and so is no parent at all"
        );
        assert_eq!(
            super::parent_directory(Path::new("/etc/spice/identity.json")),
            Path::new("/etc/spice"),
            "while a real parent is left alone"
        );
    }

    #[cfg(unix)]
    #[test]
    fn directory_sync_surfaces_an_unopenable_parent() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("missing-parent/identity.json");

        let err = sync_parent_directory(&path).expect_err("missing parent cannot be synced");

        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn debug_redacts_all_private_identity_material() {
        let mut identity = sample_identity();
        identity.enc_previous_private_key_pem = "PREVIOUS-PRIVATE-KEY".to_string();
        identity.cache_key_b64 = "CACHE-KEY-SECRET".to_string();
        let private_values = [
            identity.private_key_pem.clone(),
            identity.enc_private_key_pem.clone(),
            identity.enc_previous_private_key_pem.clone(),
            identity.cache_key_b64.clone(),
        ];

        let debug = format!("{identity:?}");
        for private in private_values {
            assert!(!debug.contains(&private), "Debug leaked private material");
        }
        assert!(debug.contains("inst_test"));
        assert!(debug.contains("REDACTED"));
    }

    #[test]
    fn enrollment_material_debug_redacts_private_keys() {
        let material = IdentityStore::generate_enrollment().expect("generate material");
        let private_key = material.private_key_pem.clone();
        let enc_private_key = material.enc_private_key_pem.clone();

        let debug = format!("{material:?}");
        assert!(
            !debug.contains(&private_key),
            "Debug leaked the identity key"
        );
        assert!(
            !debug.contains(&enc_private_key),
            "Debug leaked the encryption key"
        );
        assert!(debug.contains("REDACTED"));
    }

    /// `store_app_id` is a read-modify-write, and everything it does not touch
    /// has to come back unchanged — most importantly the credential, since
    /// overwriting that with a stale copy leaves a key the cloud has stopped
    /// accepting.
    #[test]
    fn store_app_id_records_the_app_and_preserves_the_credential() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let identity = sample_identity();
        IdentityStore::store(&path, &identity).expect("store");

        IdentityStore::store_app_id(dir.path(), &path, "4002").expect("store app id");

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.app_id.as_deref(), Some("4002"));
        assert_eq!(loaded.identity_cert_pem, identity.identity_cert_pem);
        assert_eq!(loaded.private_key_pem, identity.private_key_pem);
        assert_eq!(loaded.enc_private_key_pem, identity.enc_private_key_pem);
        assert_eq!(loaded.not_after_unix, identity.not_after_unix);
    }

    #[test]
    fn store_app_id_replaces_a_previous_app() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");

        IdentityStore::store_app_id(dir.path(), &path, "4002").expect("first");
        IdentityStore::store_app_id(dir.path(), &path, "3387").expect("second");

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.app_id.as_deref(), Some("3387"));
    }

    #[test]
    fn set_app_id_clears_only_the_attachment() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let identity = sample_identity();
        IdentityStore::store(&path, &identity).expect("store");
        IdentityStore::set_app_id(dir.path(), &path, Some("4002")).expect("attach");

        let present = IdentityStore::set_app_id(dir.path(), &path, None).expect("detach");

        assert!(present, "the identity still exists");
        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.app_id, None);
        assert_eq!(loaded.identity_cert_pem, identity.identity_cert_pem);
        assert_eq!(loaded.private_key_pem, identity.private_key_pem);
        assert_eq!(loaded.enc_private_key_pem, identity.enc_private_key_pem);
    }

    fn sample_attachment() -> AppAttachment {
        AppAttachment {
            app_id: "4002".to_string(),
            org_name: Some("acme".to_string()),
            app_name: Some("retail-analytics".to_string()),
            monitor_url: Some("https://spice.ai/acme/retail-analytics/monitor".to_string()),
        }
    }

    #[test]
    fn attachment_state_json_reports_every_persisted_member() {
        let state = AttachmentState {
            app_id: Some("4002".to_string()),
            org_name: Some("acme".to_string()),
            app_name: Some("retail-analytics".to_string()),
            monitor_url: Some("https://spice.ai/acme/retail-analytics/monitor".to_string()),
        };

        assert_eq!(
            serde_json::json!(state),
            serde_json::json!({
                "app_id": "4002",
                "org_name": "acme",
                "app_name": "retail-analytics",
                "monitor_url": "https://spice.ai/acme/retail-analytics/monitor",
            })
        );
    }

    #[test]
    fn set_attachment_persists_the_full_tuple_and_preserves_the_credential() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let identity = sample_identity();
        IdentityStore::store(&path, &identity).expect("store");

        let attachment = sample_attachment();
        let persisted = IdentityStore::set_attachment(dir.path(), &path, Some(&attachment))
            .expect("attach")
            .expect("identity present");
        assert_eq!(persisted.app_id.as_deref(), Some("4002"));
        assert_eq!(persisted.org_name.as_deref(), Some("acme"));

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.app_id.as_deref(), Some("4002"));
        assert_eq!(loaded.org_name.as_deref(), Some("acme"));
        assert_eq!(loaded.app_name.as_deref(), Some("retail-analytics"));
        assert_eq!(
            loaded.monitor_url.as_deref(),
            Some("https://spice.ai/acme/retail-analytics/monitor")
        );
        assert_eq!(loaded.identity_cert_pem, identity.identity_cert_pem);
        assert_eq!(loaded.private_key_pem, identity.private_key_pem);
        assert_eq!(loaded.enc_private_key_pem, identity.enc_private_key_pem);
        assert_eq!(loaded.cache_key_b64, identity.cache_key_b64);
    }

    /// Attachment equality covers the whole tuple: an update that changes only
    /// one member (here the monitor URL) must still persist. Comparing only the
    /// app id would skip the write and leave stale metadata on disk.
    #[test]
    fn set_attachment_applies_a_change_to_any_tuple_member() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");
        let attachment = sample_attachment();
        IdentityStore::set_attachment(dir.path(), &path, Some(&attachment)).expect("attach");

        let moved = AppAttachment {
            monitor_url: Some("https://spice.ai/acme/retail-analytics-2/monitor".to_string()),
            ..attachment
        };
        IdentityStore::set_attachment(dir.path(), &path, Some(&moved))
            .expect("update")
            .expect("identity present");

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(
            loaded.monitor_url.as_deref(),
            Some("https://spice.ai/acme/retail-analytics-2/monitor")
        );
        assert_eq!(loaded.app_id.as_deref(), Some("4002"));
    }

    /// Attaching a *different* app takes the command's project metadata
    /// verbatim — absent members come out absent, so one app's monitor page is
    /// never carried under another app's id. The org is instance-level and an
    /// app-id-only attach must not wipe it: a control plane predating the
    /// metadata fields sends exactly this shape on every reconciliation.
    #[test]
    fn attaching_a_different_app_replaces_project_metadata_but_keeps_the_org() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");
        IdentityStore::set_attachment(dir.path(), &path, Some(&sample_attachment()))
            .expect("attach");

        let bare = AppAttachment {
            app_id: "3387".to_string(),
            org_name: None,
            app_name: None,
            monitor_url: None,
        };
        IdentityStore::set_attachment(dir.path(), &path, Some(&bare))
            .expect("re-attach")
            .expect("identity present");

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.app_id.as_deref(), Some("3387"));
        assert_eq!(
            loaded.org_name.as_deref(),
            Some("acme"),
            "an app-id-only attach must not clear the instance's org"
        );
        assert_eq!(loaded.app_name, None);
        assert_eq!(loaded.monitor_url, None);
    }

    /// Re-attaching the app the instance already holds updates the members the
    /// command names and preserves the ones it omits — so a mixed-version
    /// control plane re-asserting an attachment with fewer fields cannot strip
    /// metadata a fuller command recorded.
    #[test]
    fn re_attaching_the_same_app_preserves_omitted_members() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");
        let attachment = sample_attachment();
        IdentityStore::set_attachment(dir.path(), &path, Some(&attachment)).expect("attach");

        let sparse = AppAttachment {
            app_id: attachment.app_id,
            org_name: None,
            app_name: None,
            monitor_url: Some("https://spice.ai/acme/retail-analytics/monitor2".to_string()),
        };
        let persisted = IdentityStore::set_attachment(dir.path(), &path, Some(&sparse))
            .expect("re-attach")
            .expect("identity present");
        assert_eq!(persisted.org_name.as_deref(), Some("acme"));

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.org_name.as_deref(), Some("acme"));
        assert_eq!(loaded.app_name.as_deref(), Some("retail-analytics"));
        assert_eq!(
            loaded.monitor_url.as_deref(),
            Some("https://spice.ai/acme/retail-analytics/monitor2"),
            "a present member still updates"
        );
    }

    /// The org survives the detach → app-id-only re-attach cycle end to end:
    /// this is the mixed-version reconciliation sequence that must not lose
    /// the recovery pointer.
    #[test]
    fn the_org_survives_detach_and_a_bare_re_attach() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");
        IdentityStore::set_attachment(dir.path(), &path, Some(&sample_attachment()))
            .expect("attach");
        IdentityStore::set_attachment(dir.path(), &path, None).expect("detach");

        let bare = AppAttachment {
            app_id: "3387".to_string(),
            org_name: None,
            app_name: None,
            monitor_url: None,
        };
        IdentityStore::set_attachment(dir.path(), &path, Some(&bare)).expect("re-attach");

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.org_name.as_deref(), Some("acme"));
        assert_eq!(loaded.app_id.as_deref(), Some("3387"));
    }

    /// Detach clears the attachment-scoped fields but keeps the org: the org
    /// outlives the attachment, and is what lets a detached instance still
    /// point at the org's new-project page as its recovery path. The
    /// credential is untouched either way.
    #[test]
    fn detach_clears_project_and_monitor_but_preserves_the_org() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let identity = sample_identity();
        IdentityStore::store(&path, &identity).expect("store");
        IdentityStore::set_attachment(dir.path(), &path, Some(&sample_attachment()))
            .expect("attach");

        let persisted = IdentityStore::set_attachment(dir.path(), &path, None)
            .expect("detach")
            .expect("the identity still exists");
        // The returned state is what a command handler reports, and must match
        // the disk: detached, org retained.
        assert_eq!(persisted.app_id, None);
        assert_eq!(persisted.org_name.as_deref(), Some("acme"));

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.app_id, None);
        assert_eq!(loaded.app_name, None);
        assert_eq!(loaded.monitor_url, None);
        assert_eq!(loaded.org_name.as_deref(), Some("acme"));
        assert_eq!(loaded.identity_cert_pem, identity.identity_cert_pem);
        assert_eq!(loaded.private_key_pem, identity.private_key_pem);
    }

    /// The attachment arrives over an established stream, which requires an
    /// identity — so a missing file means one was cleared concurrently by a
    /// `Remove`, and the update must not resurrect it.
    #[test]
    fn set_attachment_does_not_create_an_identity() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");

        let persisted =
            IdentityStore::set_attachment(dir.path(), &path, Some(&sample_attachment()))
                .expect("no-op on a missing identity");

        assert!(
            persisted.is_none(),
            "the caller must not acknowledge a durable update"
        );
        assert!(
            IdentityStore::load_optional(&path).expect("load").is_none(),
            "a released instance must not be resurrected by attachment state"
        );
    }

    /// A release racing attachment updates must win, exactly as it does for
    /// app-id updates: both mutations take the same persistent transaction, so
    /// an update either lands before removal, observes the removed file after
    /// it, or is explicitly rejected while removal owns the directory.
    #[test]
    fn a_release_wins_over_concurrent_attachment_updates() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");

        std::thread::scope(|scope| {
            let updater = scope.spawn(|| {
                for i in 0..200 {
                    let attachment = AppAttachment {
                        app_id: format!("400{i}"),
                        ..sample_attachment()
                    };
                    if let Err(error) =
                        IdentityStore::set_attachment(dir.path(), &path, Some(&attachment))
                    {
                        assert!(
                            error.to_string().contains("Another live process"),
                            "{error}"
                        );
                        break;
                    }
                }
            });
            IdentityStore::clear(&path).expect("clear");
            updater.join().expect("updater thread");
        });

        assert!(
            IdentityStore::load_optional(&path).expect("load").is_none(),
            "a released instance must stay released"
        );
    }

    /// The identity file holds the mTLS private key, so every writer —
    /// including the attachment update — must leave it owner-only.
    #[cfg(unix)]
    #[test]
    fn set_attachment_keeps_owner_only_perms() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");

        IdentityStore::set_attachment(dir.path(), &path, Some(&sample_attachment()))
            .expect("attach");

        let mode = std::fs::metadata(&path)
            .expect("read metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    /// Switching the attributed app without an `AttachApp` (the `ApplySpicepod`
    /// path carries only the id) must not keep the previous app's name and
    /// monitor URL: under a different app id they would present one app's
    /// monitor page as another's. The org is not attachment-scoped and stays.
    #[test]
    fn changing_the_app_id_drops_the_stale_project_metadata() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");
        IdentityStore::set_attachment(dir.path(), &path, Some(&sample_attachment()))
            .expect("attach");

        IdentityStore::store_app_id(dir.path(), &path, "9999").expect("re-attribute");

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.app_id.as_deref(), Some("9999"));
        assert_eq!(loaded.app_name, None);
        assert_eq!(loaded.monitor_url, None);
        assert_eq!(loaded.org_name.as_deref(), Some("acme"));
    }

    /// Re-delivering the id the instance already holds is a no-op, so the
    /// project metadata delivered by the last `AttachApp` survives it.
    #[test]
    fn restating_the_same_app_id_keeps_the_project_metadata() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");
        let attachment = sample_attachment();
        IdentityStore::set_attachment(dir.path(), &path, Some(&attachment)).expect("attach");

        IdentityStore::store_app_id(dir.path(), &path, &attachment.app_id).expect("re-attribute");

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.app_name.as_deref(), Some("retail-analytics"));
        assert_eq!(
            loaded.monitor_url.as_deref(),
            Some("https://spice.ai/acme/retail-analytics/monitor")
        );
    }

    /// The app id arrives over an established stream, which requires an
    /// identity — so a missing file means one was cleared concurrently by a
    /// `Remove`. Writing a fresh file here would resurrect an instance the
    /// control plane just released.
    #[test]
    fn store_app_id_does_not_create_an_identity() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");

        IdentityStore::store_app_id(dir.path(), &path, "4002")
            .expect("no-op on a missing identity");

        assert!(
            IdentityStore::load_optional(&path).expect("load").is_none(),
            "a released instance must not be resurrected by a metrics label"
        );
    }

    /// A release racing app-id updates must win: `store_app_id` reads the file
    /// and writes it back, so a removal landing between the two would be undone
    /// and the instance would keep talking to a control plane that released it.
    /// Both sides take the same persistent transaction, so a racing update may
    /// be rejected, but no ordering can recreate the file after removal.
    #[test]
    fn a_release_wins_over_concurrent_app_id_updates() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");

        std::thread::scope(|scope| {
            let updater = scope.spawn(|| {
                for i in 0..200 {
                    if let Err(error) =
                        IdentityStore::store_app_id(dir.path(), &path, &format!("400{i}"))
                    {
                        assert!(
                            error.to_string().contains("Another live process"),
                            "{error}"
                        );
                        break;
                    }
                }
            });
            IdentityStore::clear(&path).expect("clear");
            updater.join().expect("updater thread");
        });

        // Updates that ran before the clear were removed with the rest of the
        // identity; those that ran after found no file and did nothing.
        assert!(
            IdentityStore::load_optional(&path).expect("load").is_none(),
            "a released instance must stay released"
        );
    }

    #[test]
    fn credential_update_merges_an_attachment_newer_than_its_identity_clone() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let mut identity = sample_identity();
        identity.enc_previous_private_key_pem = "PREVIOUS-ENCRYPTION-KEY".to_string();
        IdentityStore::store(&path, &identity).expect("store");
        let mut rotated = IdentityStore::load_optional(&path)
            .expect("load stale clone")
            .expect("present");
        IdentityStore::store_app_id(dir.path(), &path, "4002").expect("store app id");

        rotated.private_key_pem = "ROTATED-KEY".to_string();
        rotated.identity_cert_pem = "ROTATED-CERT".to_string();
        rotated.enc_previous_private_key_pem.clear();
        assert_eq!(
            rotated.app_id, None,
            "the renewal clone is stale by construction"
        );
        let CredentialUpdateOutcome::Stored(merged) = IdentityStore::store_credential_update(
            dir.path(),
            &path,
            &identity.identifier,
            &identity.public_key_pem,
            &rotated,
        )
        .expect("store rotated") else {
            panic!("the expected credential generation must still be present");
        };

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.private_key_pem, "ROTATED-KEY");
        assert!(loaded.enc_previous_private_key_pem.is_empty());
        assert_eq!(loaded.app_id.as_deref(), Some("4002"));
        assert_eq!(merged.app_id.as_deref(), Some("4002"));
    }

    /// The merge covers the whole attachment tuple, not just the app id: a
    /// credential rotation racing an `AttachApp` must not clobber the portal
    /// metadata that command just wrote.
    #[test]
    fn credential_update_merges_the_full_attachment_tuple() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");
        let stale = IdentityStore::load_optional(&path)
            .expect("load stale clone")
            .expect("present");
        IdentityStore::set_attachment(dir.path(), &path, Some(&sample_attachment()))
            .expect("attach");

        let mut rotated = stale.clone();
        rotated.private_key_pem = "ROTATED-KEY".to_string();
        let CredentialUpdateOutcome::Stored(merged) = IdentityStore::store_credential_update(
            dir.path(),
            &path,
            &stale.identifier,
            &stale.public_key_pem,
            &rotated,
        )
        .expect("store rotated") else {
            panic!("the expected credential generation must still be present");
        };

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.private_key_pem, "ROTATED-KEY");
        assert_eq!(loaded.app_id.as_deref(), Some("4002"));
        assert_eq!(loaded.org_name.as_deref(), Some("acme"));
        assert_eq!(loaded.app_name.as_deref(), Some("retail-analytics"));
        assert_eq!(
            loaded.monitor_url.as_deref(),
            Some("https://spice.ai/acme/retail-analytics/monitor")
        );
        assert_eq!(merged.org_name.as_deref(), Some("acme"));
    }

    #[test]
    fn credential_update_preserves_the_enrollment_portal_page() {
        // The new-project page is enrollment metadata, not credential material:
        // a renewal writing back a clone that predates it would strand a
        // detached instance with nowhere to send its operator.
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let mut enrolled = sample_identity();
        enrolled.new_project_url = Some("https://spice.ai/acme/new?instance=inst_1".to_string());
        IdentityStore::store(&path, &enrolled).expect("store");

        let mut rotated = enrolled.clone();
        rotated.new_project_url = None;
        rotated.private_key_pem = "ROTATED-KEY".to_string();
        let CredentialUpdateOutcome::Stored(_) = IdentityStore::store_credential_update(
            dir.path(),
            &path,
            &enrolled.identifier,
            &enrolled.public_key_pem,
            &rotated,
        )
        .expect("store rotated") else {
            panic!("the expected credential generation must still be present");
        };

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.private_key_pem, "ROTATED-KEY");
        assert_eq!(
            loaded.new_project_url.as_deref(),
            Some("https://spice.ai/acme/new?instance=inst_1")
        );
    }

    /// A monitor page the portal-link rule rejects is not stored, and never
    /// reaches the log line or the browser that would open it. The attachment
    /// itself still lands — losing the page is not losing the attachment.
    #[test]
    fn an_unsafe_monitor_page_is_not_stored() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");

        for unsafe_url in [
            "javascript:alert(1)",
            "http://attacker.example/acme/monitor",
            "https://user:secret@spice.ai/acme/monitor",
            "/acme/monitor",
        ] {
            let attachment = AppAttachment {
                monitor_url: Some(unsafe_url.to_string()),
                ..sample_attachment()
            };
            let persisted = IdentityStore::set_attachment(dir.path(), &path, Some(&attachment))
                .expect("attach")
                .expect("the identity still exists");
            assert_eq!(
                persisted.monitor_url, None,
                "{unsafe_url} must not be reported as this attachment's page"
            );
            let loaded = IdentityStore::load_optional(&path)
                .expect("load")
                .expect("present");
            assert_eq!(loaded.monitor_url, None, "{unsafe_url} must not be stored");
            assert_eq!(
                loaded.app_id.as_deref(),
                Some(attachment.app_id.as_str()),
                "the attachment itself must still land"
            );
            // Back to detached so the next iteration starts from the same state.
            IdentityStore::set_attachment(dir.path(), &path, None).expect("detach");
        }
    }

    /// Portal pages written before the rule existed are dropped when the
    /// identity is read, so nothing unusable reaches the startup report, a
    /// status output, or a browser — the writer-side check cannot speak for
    /// state that is already on disk.
    #[test]
    fn portal_pages_a_legacy_identity_holds_are_dropped_on_load() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let identity = sample_identity();
        IdentityStore::store(&path, &identity).expect("store");

        // Written straight into the file, the way a runtime without the rule
        // would have written them.
        let mut document: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).expect("read identity"))
                .expect("identity JSON");
        let object = document
            .as_object_mut()
            .expect("identity document is an object");
        object.insert(
            "monitor_url".to_string(),
            serde_json::Value::String("javascript:alert(1)".to_string()),
        );
        object.insert(
            "new_project_url".to_string(),
            serde_json::Value::String("https://user:secret@spice.ai/acme/new".to_string()),
        );
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&document).expect("serialize identity"),
        )
        .expect("write the legacy identity");

        let loaded = IdentityStore::load_optional(&path)
            .expect("a legacy identity remains loadable")
            .expect("present");
        assert_eq!(
            loaded.monitor_url, None,
            "an unusable monitor page must not reach a consumer"
        );
        assert_eq!(
            loaded.new_project_url, None,
            "an unusable create-project page must not reach a consumer"
        );
        assert_eq!(
            loaded.identifier, identity.identifier,
            "the rest of the identity is untouched"
        );
        assert_eq!(loaded.private_key_pem, identity.private_key_pem);
    }

    /// A rejected page is not the same as an omitted one. Naming a page the rule
    /// refuses clears the stale one — leaving the old page standing would point an
    /// operator at metadata this attachment never delivered — while omitting the
    /// field preserves what is stored.
    #[test]
    fn a_rejected_monitor_page_clears_a_stale_one_but_an_omitted_page_preserves_it() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");
        IdentityStore::set_attachment(dir.path(), &path, Some(&sample_attachment()))
            .expect("attach");
        let stored = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(
            stored.monitor_url.as_deref(),
            Some("https://spice.ai/acme/retail-analytics/monitor"),
            "the delivered page is the starting state"
        );

        // Omitted: the stored page survives.
        let omitted = AppAttachment {
            monitor_url: None,
            ..sample_attachment()
        };
        IdentityStore::set_attachment(dir.path(), &path, Some(&omitted)).expect("attach");
        assert_eq!(
            IdentityStore::load_optional(&path)
                .expect("load")
                .expect("present")
                .monitor_url
                .as_deref(),
            Some("https://spice.ai/acme/retail-analytics/monitor"),
            "an omitted page preserves what is stored"
        );

        // Named but rejected: the stale page goes.
        let rejected = AppAttachment {
            monitor_url: Some("javascript:alert(1)".to_string()),
            ..sample_attachment()
        };
        let persisted = IdentityStore::set_attachment(dir.path(), &path, Some(&rejected))
            .expect("attach")
            .expect("the identity still exists");
        assert_eq!(
            persisted.monitor_url, None,
            "a rejected page must not be reported as this attachment's page"
        );
        assert_eq!(
            IdentityStore::load_optional(&path)
                .expect("load")
                .expect("present")
                .monitor_url,
            None,
            "a rejected page must clear the stale one rather than leave it standing"
        );
    }

    /// An identity written before the enrollment portal page was recorded must
    /// still load, reporting no page rather than failing — and no page is
    /// invented for it.
    #[test]
    fn an_identity_without_the_enrollment_portal_page_still_loads() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let identity = sample_identity();
        IdentityStore::store(&path, &identity).expect("store");

        let mut document: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).expect("read identity"))
                .expect("identity JSON");
        document
            .as_object_mut()
            .expect("identity document is an object")
            .remove("new_project_url");
        std::fs::write(
            &path,
            serde_json::to_vec_pretty(&document).expect("serialize identity"),
        )
        .expect("write legacy identity");

        let legacy = IdentityStore::load_optional(&path)
            .expect("a legacy identity remains loadable")
            .expect("present");
        assert_eq!(legacy.new_project_url, None);
        assert_eq!(legacy.identifier, identity.identifier);
    }

    #[test]
    fn an_attachment_never_clears_the_enrollment_portal_page() {
        // Attaching and detaching are project-scoped; the org's new-project
        // page is the recovery destination for a detached instance and belongs
        // to the enrollment, so neither may touch it.
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let mut enrolled = sample_identity();
        enrolled.new_project_url = Some("https://spice.ai/acme/new?instance=inst_1".to_string());
        IdentityStore::store(&path, &enrolled).expect("store");

        IdentityStore::set_attachment(dir.path(), &path, Some(&sample_attachment()))
            .expect("attach");
        IdentityStore::set_attachment(dir.path(), &path, None).expect("detach");

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(
            loaded.new_project_url.as_deref(),
            Some("https://spice.ai/acme/new?instance=inst_1")
        );
    }

    #[test]
    fn credential_update_backfills_but_never_replaces_the_control_plane_binding() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let legacy = sample_identity();
        IdentityStore::store(&path, &legacy).expect("store legacy identity");

        let mut renewed = legacy.clone();
        renewed.control_plane_endpoint = Some("https://private.example".to_string());
        let CredentialUpdateOutcome::Stored(backfilled) = IdentityStore::store_credential_update(
            dir.path(),
            &path,
            &legacy.identifier,
            &legacy.public_key_pem,
            &renewed,
        )
        .expect("store endpoint backfill") else {
            panic!("the legacy credential generation must remain present");
        };
        assert_eq!(
            backfilled.control_plane_endpoint.as_deref(),
            Some("https://private.example")
        );

        let mut stale_renewal = backfilled.clone();
        stale_renewal.control_plane_endpoint = Some("https://wrong.example".to_string());
        let CredentialUpdateOutcome::Stored(preserved) = IdentityStore::store_credential_update(
            dir.path(),
            &path,
            &backfilled.identifier,
            &backfilled.public_key_pem,
            &stale_renewal,
        )
        .expect("store stale renewal") else {
            panic!("the bound credential generation must remain present");
        };
        assert_eq!(
            preserved.control_plane_endpoint.as_deref(),
            Some("https://private.example")
        );
    }

    #[test]
    fn credential_update_does_not_recreate_a_removed_identity() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let identity = sample_identity();
        IdentityStore::store(&path, &identity).expect("store");
        IdentityStore::clear(&path).expect("remove");

        let stored = IdentityStore::store_credential_update(
            dir.path(),
            &path,
            &identity.identifier,
            &identity.public_key_pem,
            &identity,
        )
        .expect("credential update is a no-op");

        assert!(matches!(stored, CredentialUpdateOutcome::Missing));
        assert!(IdentityStore::load_optional(&path).expect("load").is_none());
    }

    #[test]
    fn credential_update_cannot_overwrite_a_replacement_identity() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let stale = sample_identity();
        IdentityStore::store(&path, &stale).expect("store original identity");

        let mut stale_update = stale.clone();
        stale_update.private_key_pem = "STALE-ROTATED-KEY".to_string();
        let replacement = sample_identity();
        IdentityStore::store(&path, &replacement).expect("publish replacement identity");

        let outcome = IdentityStore::store_credential_update(
            dir.path(),
            &path,
            &stale.identifier,
            &stale.public_key_pem,
            &stale_update,
        )
        .expect("reject stale update without losing the replacement");
        let CredentialUpdateOutcome::Superseded(winner) = outcome else {
            panic!("a new credential generation must supersede the stale update");
        };

        assert_eq!(winner.public_key_pem, replacement.public_key_pem);
        let durable = IdentityStore::load_optional(&path)
            .expect("load replacement")
            .expect("replacement remains present");
        assert_eq!(durable.public_key_pem, replacement.public_key_pem);
        assert_eq!(durable.private_key_pem, replacement.private_key_pem);
    }

    #[test]
    fn identity_updates_do_not_overlap_a_removal_transaction() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        // The identity path is independently configurable. Its parent must not
        // choose the transaction: removal and enrollment own `config_dir`.
        let path = dir.path().join("credential-state/identity.json");
        let identity = sample_identity();
        IdentityStore::store(&path, &identity).expect("store");
        let removal = crate::draft::EnrollmentTransactionLock::try_acquire(&config_dir)
            .expect("hold the removal transaction");

        let mut rotated = identity.clone();
        rotated.private_key_pem = "ROTATED-KEY".to_string();
        let credential_error = IdentityStore::store_credential_update(
            &config_dir,
            &path,
            &identity.identifier,
            &identity.public_key_pem,
            &rotated,
        )
        .expect_err("credential update must not overlap removal");
        let app_id_error = IdentityStore::set_app_id(&config_dir, &path, Some("4002"))
            .expect_err("app id update must not overlap removal");
        let attachment = AppAttachment {
            app_id: "4002".to_string(),
            org_name: Some("acme".to_string()),
            app_name: Some("retail-analytics".to_string()),
            monitor_url: Some("https://spice.ai/acme/retail-analytics/monitor".to_string()),
        };
        let attachment_error = IdentityStore::set_attachment(&config_dir, &path, Some(&attachment))
            .expect_err("attachment tuple update must not overlap removal");

        assert!(
            credential_error
                .to_string()
                .contains("Another live process"),
            "{credential_error}"
        );
        assert!(
            app_id_error.to_string().contains("Another live process"),
            "{app_id_error}"
        );
        assert!(
            attachment_error
                .to_string()
                .contains("Another live process"),
            "{attachment_error}"
        );
        let stored = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("identity remains");
        assert_eq!(stored.private_key_pem, identity.private_key_pem);
        assert_eq!(stored.app_id, None);

        drop(removal);
        let CredentialUpdateOutcome::Stored(merged) = IdentityStore::store_credential_update(
            &config_dir,
            &path,
            &identity.identifier,
            &identity.public_key_pem,
            &rotated,
        )
        .expect("store after removal transaction") else {
            panic!("the original credential generation remains");
        };
        assert_eq!(merged.private_key_pem, "ROTATED-KEY");
        IdentityStore::clear(&path).expect("clear after transaction ownership is released");
        assert!(IdentityStore::load_optional(&path).expect("load").is_none());
    }

    #[test]
    fn load_tolerates_identity_without_an_app_id() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let legacy = r#"{
            "identifier": "inst_legacy",
            "identity_cert_pem": "CERT",
            "private_key_pem": "KEY",
            "public_key_pem": "PUB"
        }"#;
        std::fs::write(&path, legacy).expect("write legacy identity");

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.app_id, None);
        // The portal metadata defaults the same way: an identity written
        // before the fields existed loads as detached-with-no-metadata.
        assert_eq!(loaded.org_name, None);
        assert_eq!(loaded.app_name, None);
        assert_eq!(loaded.monitor_url, None);
    }

    #[test]
    fn load_tolerates_identity_without_ca_bundle() {
        // Identity files written before `ca_bundle_pem` / `gateway_addr`
        // existed must still load (the fields are `#[serde(default)]`).
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let legacy = r#"{
            "identifier": "inst_legacy",
            "identity_cert_pem": "CERT",
            "private_key_pem": "KEY",
            "public_key_pem": "PUB",
            "not_after_unix": 0
        }"#;
        std::fs::write(&path, legacy).expect("write legacy identity");
        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.identifier, "inst_legacy");
        assert!(loaded.ca_bundle_pem.is_empty());
        assert!(loaded.gateway_addr.is_empty());
    }

    #[test]
    fn load_optional_returns_none_when_missing() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("does-not-exist.json");
        let loaded = IdentityStore::load_optional(&path).expect("load");
        assert!(loaded.is_none());
    }

    #[test]
    fn identity_parse_errors_never_echo_persisted_values() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let secret = "credential-value-that-must-stay-redacted";
        std::fs::write(
            &path,
            format!(r#"{{"identifier":["{secret}"],"identity_cert_pem":"CERT","private_key_pem":"KEY","public_key_pem":"PUB"}}"#),
        )
        .expect("write malformed identity");

        let error = IdentityStore::load_optional(&path)
            .expect_err("the wrong identifier type must fail parsing");
        let rendered = error.to_string();
        assert!(rendered.contains("Failed to parse identity JSON"));
        assert!(
            !rendered.contains(secret),
            "parse error leaked persisted data"
        );
    }

    #[cfg(unix)]
    #[test]
    fn identity_reads_reject_symlinks_without_reading_the_target() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("create tempdir");
        let target = dir.path().join("outside.json");
        let path = dir.path().join("identity.json");
        std::fs::write(&target, "sensitive target").expect("write target");
        symlink(&target, &path).expect("create identity symlink");

        let error = IdentityStore::load_optional(&path).expect_err("symlink must be rejected");
        assert!(matches!(error, Error::Io { .. }), "{error}");
        assert_eq!(
            std::fs::read_to_string(target).expect("target remains readable"),
            "sensitive target"
        );
    }

    #[cfg(unix)]
    #[test]
    fn identity_reads_reject_an_untrusted_symlink_ancestor() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("create tempdir");
        let outside = dir.path().join("outside");
        std::fs::create_dir(&outside).expect("create target directory");
        let target = outside.join("identity.json");
        std::fs::write(&target, "sensitive target").expect("write target");
        let redirected_parent = dir.path().join("redirected-config");
        symlink(&outside, &redirected_parent).expect("create parent symlink");

        let error = IdentityStore::load_optional(&redirected_parent.join("identity.json"))
            .expect_err("a state path with an untrusted symlink ancestor must be rejected");

        assert!(matches!(error, Error::Io { .. }), "{error}");
        assert_eq!(
            std::fs::read_to_string(target).expect("target remains readable"),
            "sensitive target"
        );
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_traversal_rejects_a_parent_swapped_to_a_symlink_after_validation() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("create tempdir");
        let config = dir.path().join("config");
        std::fs::create_dir(&config).expect("create config directory");
        std::fs::write(config.join("identity.json"), "safe identity").expect("write safe file");
        let outside = dir.path().join("outside");
        std::fs::create_dir(&outside).expect("create target directory");
        std::fs::write(outside.join("identity.json"), "redirected identity")
            .expect("write redirected file");
        let checked_config = dir.path().join("checked-config");
        let identity_path = config.join("identity.json");

        open_regular_file_optional_unix_with(&identity_path, || {
            std::fs::rename(&config, &checked_config).expect("move checked directory");
            symlink(&outside, &config).expect("replace parent with symlink");
        })
        .expect_err("descriptor-relative traversal must reject the raced parent symlink");

        assert_eq!(
            std::fs::read_to_string(outside.join("identity.json"))
                .expect("redirected target remains readable"),
            "redirected identity"
        );
    }

    #[cfg(unix)]
    #[test]
    fn identity_reads_reject_fifos_without_blocking() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt as _;
        use std::os::unix::fs::FileTypeExt as _;

        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let path_c = CString::new(path.as_os_str().as_bytes()).expect("FIFO path has no NUL");
        // SAFETY: `path_c` is a valid NUL-terminated path and `0o600` is a
        // valid mode. `mkfifo` retains neither argument.
        let result = unsafe { libc::mkfifo(path_c.as_ptr(), 0o600) };
        assert_eq!(
            result,
            0,
            "create FIFO: {}",
            std::io::Error::last_os_error()
        );

        let error = IdentityStore::load_optional(&path).expect_err("FIFO must be rejected");
        assert!(matches!(error, Error::Io { .. }), "{error}");
        assert!(
            std::fs::metadata(path)
                .expect("FIFO metadata")
                .file_type()
                .is_fifo()
        );
    }

    /// An identity file written before the encryption keyring and cache key
    /// existed must still load, so upgrading a runtime does not brick an
    /// enrolled instance. Every added field is `#[serde(default)]` for exactly
    /// this; the test is what keeps that true.
    #[test]
    fn loads_an_identity_file_predating_the_new_fields() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        // The shape a pre-secrets-delivery runtime wrote: no
        // enc_previous_private_key_pem, no cache_key_b64.
        std::fs::write(
            &path,
            serde_json::json!({
                "identifier": "inst_old",
                "identity_cert_pem": "cert",
                "private_key_pem": "key",
                "public_key_pem": "pub",
                "ca_bundle_pem": "ca",
                "gateway_addr": "gateway:7320",
                "not_after_unix": 0,
                "enc_private_key_pem": "",
                "enc_public_key_pem": "",
            })
            .to_string(),
        )
        .expect("write legacy identity");

        let loaded = IdentityStore::load_optional(&path)
            .expect("a legacy identity must still parse")
            .expect("present");
        assert_eq!(loaded.identifier, "inst_old");
        assert!(loaded.enc_previous_private_key_pem.is_empty());
        assert!(loaded.cache_key_b64.is_empty());
        // No cache key means no cache — a degraded mode, not an error.
        assert!(loaded.cache_key().is_none());
        // And no encryption key means no keyring, with a message that names the
        // fix rather than a parse failure.
        let err = loaded
            .encryption_keyring()
            .expect_err("an identity with no encryption key cannot open secrets");
        let guidance = err.to_string();
        assert!(guidance.contains("scheduled renewal"), "{err}");
        assert!(guidance.contains("no renewal deadline"), "{err}");
        assert!(guidance.contains("stop spiced"), "{err}");
        assert!(guidance.contains("spice connect remove --yes"), "{err}");
        assert!(guidance.contains("existing identity always wins"), "{err}");
    }

    #[test]
    fn ensure_cache_key_is_idempotent_and_additive() {
        let mut identity = sample_identity();
        assert!(identity.cache_key().is_none());

        assert!(identity.ensure_cache_key(), "a missing key is minted");
        let first = identity.cache_key_b64.clone();
        assert_eq!(
            identity.cache_key().map(|k| k.len()),
            Some(CACHE_KEY_LEN),
            "the minted key must be a full-length AEAD key"
        );

        // Never replaced: replacing it would discard a still-readable cache.
        assert!(!identity.ensure_cache_key(), "an existing key is kept");
        assert_eq!(identity.cache_key_b64, first);
    }

    #[test]
    fn a_malformed_cache_key_reads_as_absent() {
        let mut identity = sample_identity();
        // Not base64.
        identity.cache_key_b64 = "!!!not base64!!!".to_string();
        assert!(identity.cache_key().is_none());
        // Valid base64 of the wrong length is equally unusable.
        identity.cache_key_b64 =
            base64::engine::general_purpose::STANDARD.encode([0_u8; CACHE_KEY_LEN - 1]);
        assert!(identity.cache_key().is_none());
        // ...and `ensure_cache_key` replaces an unusable one rather than
        // leaving the instance permanently unable to cache.
        assert!(identity.ensure_cache_key());
        assert!(identity.cache_key().is_some());
    }

    #[test]
    fn rotating_the_encryption_key_retains_exactly_one_predecessor() {
        let mut identity = sample_identity();
        let original = identity.enc_private_key_pem.clone();

        identity.rotate_encryption_key("second-priv".to_string(), "second-pub".to_string());
        assert_eq!(identity.enc_private_key_pem, "second-priv");
        assert_eq!(identity.enc_public_key_pem, "second-pub");
        assert_eq!(identity.enc_previous_private_key_pem, original);

        // A second rotation drops the first predecessor: exactly one is kept,
        // so a payload two rotations old is refused rather than opened.
        identity.rotate_encryption_key("third-priv".to_string(), "third-pub".to_string());
        assert_eq!(identity.enc_private_key_pem, "third-priv");
        assert_eq!(identity.enc_previous_private_key_pem, "second-priv");
    }

    #[test]
    fn retiring_the_previous_key_is_idempotent() {
        let mut identity = sample_identity();
        identity.rotate_encryption_key("next".to_string(), "next-pub".to_string());
        assert!(!identity.enc_previous_private_key_pem.is_empty());

        assert!(identity.retire_previous_enc_key(), "there was one to drop");
        assert!(identity.enc_previous_private_key_pem.is_empty());
        assert!(
            !identity.retire_previous_enc_key(),
            "nothing to drop the second time, so the caller need not persist"
        );
    }

    #[test]
    fn a_keyring_tolerates_an_unparseable_previous_key() {
        // A corrupt retained key must not take the current one down with it:
        // that would turn a recoverable state into a total delivery outage.
        let mut identity = sample_identity();
        let material = IdentityStore::generate_enrollment().expect("material");
        identity.enc_private_key_pem = material.enc_private_key_pem;
        identity.enc_previous_private_key_pem = "-----BEGIN PRIVATE KEY-----\nnope\n".to_string();

        let keyring = identity
            .encryption_keyring()
            .expect("the current key still yields a keyring");
        assert!(keyring.select(keyring.current_key_id()).is_some());
    }

    #[test]
    fn a_persisted_identity_round_trips_the_new_fields() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let mut identity = sample_identity();
        identity.rotate_encryption_key("rotated-priv".to_string(), "rotated-pub".to_string());
        identity.ensure_cache_key();

        IdentityStore::store(&path, &identity).expect("store");
        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(
            loaded.enc_previous_private_key_pem,
            identity.enc_previous_private_key_pem
        );
        assert_eq!(loaded.cache_key_b64, identity.cache_key_b64);
        assert_eq!(
            loaded.cache_key().map(|k| k.to_vec()),
            identity.cache_key().map(|k| k.to_vec())
        );
    }

    #[test]
    fn clear_removes_identity() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store identity");
        assert!(path.exists());
        IdentityStore::clear(&path).expect("clear identity");
        assert!(!path.exists());
        // Idempotent.
        IdentityStore::clear(&path).expect("clear identity");
    }

    #[cfg(unix)]
    #[test]
    fn store_writes_with_0600_perms() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store identity");
        let mode = std::fs::metadata(&path)
            .expect("read metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn generate_enrollment_produces_key_and_csr() {
        let mat = IdentityStore::generate_enrollment().expect("generate");
        assert!(mat.private_key_pem.contains("PRIVATE KEY"));
        assert!(mat.public_key_pem.contains("PUBLIC KEY"));
        assert!(
            mat.csr_pem.contains("CERTIFICATE REQUEST"),
            "CSR must be a PKCS#10 PEM, got: {}",
            mat.csr_pem.lines().next().unwrap_or_default()
        );
    }

    #[test]
    fn generate_enrollment_csr_is_signable() {
        // The CSR the client emits must be parseable and verifiable by the
        // control plane. `from_pem` verifies the CSR's self-signature — so a
        // successful parse already proves the client possesses the private
        // key — and signing it with a throwaway CA proves the leaf-issuance
        // round-trip the real dp performs.
        use rcgen::{
            BasicConstraints, CertificateParams, CertificateSigningRequestParams, IsCa, Issuer,
            KeyPair, KeyUsagePurpose,
        };

        let mat = IdentityStore::generate_enrollment().expect("generate");

        let ca_key = KeyPair::generate().expect("ca key");
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let issuer = Issuer::new(ca_params, ca_key);

        let csr = CertificateSigningRequestParams::from_pem(&mat.csr_pem)
            .expect("CSR parses and self-signature verifies");
        let leaf = csr.signed_by(&issuer).expect("sign csr");
        assert!(leaf.pem().contains("CERTIFICATE"));
    }

    #[test]
    fn is_expired_handles_absent_expiry_as_unbounded() {
        let identity = sample_identity();
        assert!(!identity.is_expired());
    }

    #[test]
    fn load_reads_the_legacy_zero_expiry_as_unbounded() {
        // `0` used to be the in-band "unknown / unbounded" sentinel; a file
        // still carrying it must not be read as "expired at the epoch".
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let legacy = r#"{
            "identifier": "inst_legacy",
            "identity_cert_pem": "CERT",
            "private_key_pem": "KEY",
            "public_key_pem": "PUB",
            "not_after_unix": 0
        }"#;
        std::fs::write(&path, legacy).expect("write legacy identity");
        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.not_after_unix, None);
        assert!(!loaded.is_expired());
    }

    #[test]
    fn is_expired_uses_the_signed_certificate_over_a_future_cached_timestamp() {
        let mut identity = sample_identity();
        set_sample_certificate_validity(&mut identity, (2019, 1, 1), (2020, 1, 1));
        identity.not_after_unix = Some(4_102_444_800);
        assert!(identity.is_expired());
    }

    #[test]
    fn is_expired_falls_back_to_the_cached_timestamp_for_an_unparseable_certificate() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .expect("system clock after unix epoch");
        let mut identity = sample_identity();
        identity.identity_cert_pem = "not a certificate".to_string();
        identity.not_after_unix = Some(now);
        assert!(identity.is_expired());
    }

    #[test]
    fn is_expired_uses_the_signed_certificate_over_a_past_cached_timestamp() {
        let mut identity = sample_identity();
        set_sample_certificate_validity(&mut identity, (2025, 1, 1), (2099, 1, 1));
        identity.not_after_unix = Some(1);
        assert!(!identity.is_expired());
    }
}
