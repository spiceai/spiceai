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

use std::path::{Path, PathBuf};

use base64::Engine as _;
use rcgen::{CertificateParams, DnType, ExtendedKeyUsagePurpose, KeyPair, PublicKeyData};
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

    #[snafu(display("Failed to parse identity JSON at {}: {source}", path.display()))]
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
    /// The one field here that is not credential material. It lives alongside
    /// the credential because it shares the credential's lifetime — both are
    /// control-plane facts about this enrolled instance, and both are cleared
    /// together when the instance is released.
    ///
    /// Persisted rather than held only in memory so a restart does not silence
    /// the export before the next control-stream reconciliation.
    ///
    /// `None` means the instance is detached, which is also the state a freshly
    /// enrolled instance starts in.
    #[serde(default)]
    pub app_id: Option<String>,
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
            .field("not_after_unix", &self.not_after_unix)
            .field("enc_private_key_pem", &"[REDACTED]")
            .field("enc_public_key_pem", &"[PUBLIC KEY]")
            .field("enc_previous_private_key_pem", &"[REDACTED]")
            .field("cache_key_b64", &"[REDACTED]")
            .field("app_id", &self.app_id)
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
    /// Why this identity cannot establish a control stream, if any.
    ///
    /// Enrollment uses this as a fail-closed gate before honoring the
    /// existing-identity precedence rule. An explicit gateway override makes
    /// a legacy identity with no stored gateway usable, but credentials and
    /// the cloud identifier are always required.
    pub(crate) fn reconnect_validation_error(
        &self,
        gateway_override: Option<&str>,
    ) -> Option<&'static str> {
        if self.identifier.trim().is_empty() {
            return Some("the cloud-assigned instance identifier is empty");
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
        if self.gateway_addr.trim().is_empty()
            && gateway_override.is_none_or(|endpoint| endpoint.trim().is_empty())
        {
            return Some("the gateway address is empty");
        }
        None
    }

    /// Returns `true` if the identity has an expiry that is in the past
    /// relative to the system clock. An identity with no expiry never expires.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        let Some(not_after) = self.not_after_unix else {
            return false;
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        // Treat the cert as expired *at* `not_after`, not only strictly after
        // it: the field is defined as the timestamp after which the server no
        // longer accepts the credential, so the boundary second should
        // already be considered past the NotAfter limit.
        now >= not_after
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
/// [`IdentityStore::set_app_id`] reads the file, edits one field, and writes it
/// back, while the renewal path replaces the credential fields.
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

fn acquire_update_transaction(path: &Path) -> Result<crate::draft::EnrollmentTransactionLock> {
    let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
    crate::draft::EnrollmentTransactionLock::try_acquire(config_dir).map_err(|source| {
        Error::UpdateTransaction {
            path: path.to_path_buf(),
            reason: source.to_string(),
        }
    })
}

impl IdentityStore {
    /// Read an identity file, returning `Ok(None)` if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read (for any reason
    /// other than not-found) or its contents fail to parse as an [`Identity`].
    pub fn load_optional(path: &Path) -> Result<Option<Identity>> {
        #[cfg(not(unix))]
        cleanup_stale_identity_backups(path).context(IoSnafu {
            path: path.to_path_buf(),
        })?;
        match std::fs::read_to_string(path) {
            Ok(s) => {
                let identity: Identity = serde_json::from_str(&s).context(ParseSnafu {
                    path: path.to_path_buf(),
                })?;
                Ok(Some(identity))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(Error::Io {
                path: path.to_path_buf(),
                source: err,
            }),
        }
    }

    /// Persist an identity to disk atomically with `0600` perms on Unix.
    ///
    /// # Errors
    ///
    /// Returns an error if the parent directory cannot be created, the
    /// identity cannot be serialized, or the file cannot be written.
    pub fn store(path: &Path, identity: &Identity) -> Result<()> {
        let _guard = write_lock();
        Self::store_locked(path, identity)
    }

    /// Record the app this instance's telemetry belongs to, leaving every other
    /// field as it is on disk.
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
    pub fn store_app_id(path: &Path, app_id: &str) -> Result<()> {
        Self::set_app_id(path, Some(app_id)).map(|_| ())
    }

    /// Set or clear the app this instance's telemetry belongs to, leaving every
    /// credential field as it is on disk.
    ///
    /// Returns `Ok(false)` when the identity file no longer exists. Callers
    /// handling a control command must not acknowledge a durable update in that
    /// case; the identity may have been removed concurrently.
    ///
    /// # Errors
    ///
    /// Returns an error if the existing file cannot be read or parsed, or if the
    /// updated identity cannot be written.
    pub fn set_app_id(path: &Path, app_id: Option<&str>) -> Result<bool> {
        let _transaction = acquire_update_transaction(path)?;
        let _guard = write_lock();
        let Some(mut identity) = Self::load_optional(path)? else {
            return Ok(false);
        };
        if identity.app_id.as_deref() == app_id {
            return Ok(true);
        }
        identity.app_id = app_id.map(str::to_string);
        Self::store_locked(path, &identity)?;
        Ok(true)
    }

    /// Persist a complete identity update without overwriting an attachment
    /// change made after the caller cloned its in-memory identity.
    ///
    /// Certificate renewal and encryption-key retirement both replace
    /// credential material from the client's in-memory clone. The attachment
    /// is owned by control commands and can be newer on disk, so every such
    /// full identity update must merge it while holding [`write_lock`].
    ///
    /// Returns `Ok(None)` when the identity was removed before the write and
    /// does not recreate it.
    ///
    /// # Errors
    ///
    /// Returns an error if the existing file cannot be read or parsed, or if the
    /// merged identity cannot be written.
    pub fn store_credential_update(
        path: &Path,
        credential_update: &Identity,
    ) -> Result<Option<Identity>> {
        let _transaction = acquire_update_transaction(path)?;
        let _guard = write_lock();
        let Some(current) = Self::load_optional(path)? else {
            return Ok(None);
        };
        let mut merged = credential_update.clone();
        merged.app_id = current.app_id;
        Self::store_locked(path, &merged)?;
        Ok(Some(merged))
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
    /// Takes [`write_lock`] to serialize with updates in this process. The
    /// caller performing `spice connect remove` owns the config directory's
    /// enrollment transaction before calling this method, which serializes the
    /// removal with updates from another process.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be removed.
    pub fn clear(path: &Path) -> Result<()> {
        let _guard = write_lock();
        Self::clear_locked(path)
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
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
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

/// A newly-created writer gets ample time to acquire its advisory lock before
/// another process may consider its temp file abandoned. The lock remains the
/// authoritative liveness signal after this age; time alone never authorizes
/// deletion of an active writer.
const ABANDONED_TEMP_MIN_AGE: std::time::Duration = std::time::Duration::from_hours(1);

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
    cleanup_abandoned_atomic_temps_with(path, minimum_age, |_, _| Ok(()))
}

fn cleanup_abandoned_atomic_temps_with<F>(
    path: &Path,
    minimum_age: std::time::Duration,
    before_remove: F,
) -> std::io::Result<()>
where
    F: Fn(&std::fs::File, &Path) -> std::io::Result<()>,
{
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
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
        let is_temp = Path::new(entry_name)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("tmp"));
        if !entry_name.starts_with(&prefix)
            || !is_temp
            || entry_name.len() <= prefix.len() + ".tmp".len()
            || !entry.file_type()?.is_file()
        {
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
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
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
        // inode. Release the lock before publishing success (especially on
        // Windows, where range locks are mandatory for readers).
        drop(file);
        sync_parent_directory(path)
    })();

    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    result
}

/// As the Unix variant, minus the permission enforcement: Windows ACLs are not
/// expressible through `PermissionsExt`, so the owner-only guarantee is scoped to
/// Unix hosts and documented as such.
#[cfg(not(unix))]
pub(crate) fn atomic_write_owner_only(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    cleanup_stale_identity_backups(path)?;
    cleanup_abandoned_atomic_temps(path, ABANDONED_TEMP_MIN_AGE)?;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("identity.json");
    let tmp_path = dir.join(format!(".{file_name}.{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&tmp_path)?;
        fs4::fs_std::FileExt::lock_exclusive(&file)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        promote_temp(&tmp_path, path)?;
        drop(file);
        sync_parent_directory(path)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_path);
    }
    result
}

/// Synchronize the directory entry containing `path` after a rename, hard
/// link, or removal. Synchronizing only the file contents does not make the
/// directory metadata durable across power loss.
#[cfg(unix)]
pub(crate) fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::File::open(dir)?.sync_all()
}

/// Windows does not expose a portable directory handle through `std::fs` that
/// can be synchronized. File contents are still flushed before promotion.
#[cfg(not(unix))]
pub(crate) fn sync_parent_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Remove stale non-Unix replacement backups once the canonical identity is
/// present. A failed cleanup is fail-closed: backups contain the complete old
/// credential, so silently accumulating them is not acceptable. If the
/// canonical identity is absent, preserve the backup and return an error — it
/// may be the only recoverable identity after an interrupted rollback.
#[cfg(any(not(unix), test))]
fn cleanup_stale_identity_backups(path: &Path) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("identity.json");
    let prefix = format!(".{file_name}.");
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };

    for entry in entries {
        let entry = entry?;
        let entry_name = entry.file_name();
        let Some(entry_name) = entry_name.to_str() else {
            continue;
        };
        let is_backup = Path::new(entry_name)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("bak"));
        if !entry_name.starts_with(&prefix) || !is_backup {
            continue;
        }
        if !path.exists() {
            return Err(std::io::Error::other(
                "A stale identity backup exists without the canonical identity; restore or remove the backup before retrying",
            ));
        }
        match std::fs::remove_file(entry.path()) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// Promote a freshly-written temp file into its final location on non-Unix
/// platforms, where `std::fs::rename` does **not** atomically replace an
/// existing destination (it errors if the target already exists). A rotated
/// or re-enrolled identity must be able to overwrite an existing
/// `identity.json`, so when the plain rename fails we move the existing file
/// to a backup, retry the rename, and roll the backup back if the retry
/// fails. The backup is removed on success.
#[cfg(not(unix))]
fn promote_temp(tmp_path: &Path, path: &Path) -> std::io::Result<()> {
    if let Err(err) = std::fs::rename(tmp_path, path) {
        // The most likely cause on non-Unix is that `path` already exists.
        // If the destination is genuinely absent, surface the original error.
        if !path.exists() {
            return Err(err);
        }

        let dir = path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("identity.json");
        let backup_path = dir.join(format!(".{file_name}.{}.bak", uuid::Uuid::new_v4()));
        std::fs::rename(path, &backup_path)?;
        match std::fs::rename(tmp_path, path) {
            Ok(()) => {
                // Promotion succeeded; the old credential must not remain on
                // disk under a backup name.
                std::fs::remove_file(&backup_path)?;
            }
            Err(promote_err) => {
                // Roll the original file back into place so we don't leave the
                // store without an identity, then report the failure.
                let _ = std::fs::rename(&backup_path, path);
                return Err(promote_err);
            }
        }
    }
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
        Identity {
            identifier: "inst_test".to_string(),
            identity_cert_pem: certificate.pem(),
            private_key_pem: key_pair.serialize_pem(),
            public_key_pem: key_pair.public_key_pem(),
            ca_bundle_pem: "-----BEGIN CERTIFICATE-----\nMOCKCA\n-----END CERTIFICATE-----\n"
                .to_string(),
            gateway_addr: "gateway.test.spice.ai:443".to_string(),
            not_after_unix: None,
            app_id: None,
            enc_private_key_pem:
                "-----BEGIN PRIVATE KEY-----\nMOCKENC\n-----END PRIVATE KEY-----\n".to_string(),
            enc_public_key_pem: "-----BEGIN PUBLIC KEY-----\nMOCKENC\n-----END PUBLIC KEY-----\n"
                .to_string(),
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
        }
    }

    #[test]
    fn reconnect_validation_accepts_a_matching_certificate_and_private_key() {
        assert_eq!(sample_identity().reconnect_validation_error(None), None);
    }

    #[test]
    fn reconnect_validation_rejects_malformed_certificate_and_private_key() {
        let mut identity = sample_identity();
        identity.identity_cert_pem =
            "-----BEGIN CERTIFICATE-----\nnot-a-certificate\n-----END CERTIFICATE-----\n"
                .to_string();
        assert_eq!(
            identity.reconnect_validation_error(None),
            Some("the client identity certificate is not valid PEM")
        );

        let mut identity = sample_identity();
        identity.private_key_pem =
            "-----BEGIN PRIVATE KEY-----\nnot-a-private-key\n-----END PRIVATE KEY-----\n"
                .to_string();
        assert_eq!(
            identity.reconnect_validation_error(None),
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
            identity.reconnect_validation_error(None),
            Some("the client identity certificate and private key do not match")
        );
    }

    #[test]
    fn reconnect_validation_rejects_a_malformed_or_mismatched_public_key() {
        let mut identity = sample_identity();
        identity.public_key_pem =
            "-----BEGIN PUBLIC KEY-----\nnot-a-public-key\n-----END PUBLIC KEY-----\n".to_string();
        assert_eq!(
            identity.reconnect_validation_error(None),
            Some("the client identity public key is not valid PEM")
        );

        let mut identity = sample_identity();
        identity.public_key_pem = identity.private_key_pem.clone();
        assert_eq!(
            identity.reconnect_validation_error(None),
            Some("the client identity public key has an invalid PEM label")
        );

        let mut identity = sample_identity();
        identity.public_key_pem = KeyPair::generate()
            .expect("generate mismatched public key")
            .public_key_pem();
        assert_eq!(
            identity.reconnect_validation_error(None),
            Some("the client identity public and private keys do not match")
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
        let active_temp = dir.path().join(".identity.json.active.tmp");
        let abandoned_temp = dir.path().join(".identity.json.abandoned.tmp");
        let unrelated = dir.path().join(".different.json.abandoned.tmp");
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
        cleanup_abandoned_atomic_temps_with(&path, std::time::Duration::ZERO, |_, candidate| {
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
        })
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

    #[test]
    fn stale_identity_backups_are_removed_only_when_the_identity_is_present() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let backup = dir.path().join(".identity.json.interrupted.bak");
        std::fs::write(&path, "current identity").expect("write identity");
        std::fs::write(&backup, "stale private credential").expect("write stale backup");

        cleanup_stale_identity_backups(&path).expect("remove stale backup");

        assert!(!backup.exists(), "stale credential backup must be removed");
    }

    #[test]
    fn an_orphaned_identity_backup_is_preserved_for_recovery() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let backup = dir.path().join(".identity.json.interrupted.bak");
        std::fs::write(&backup, "only recoverable identity").expect("write backup");

        cleanup_stale_identity_backups(&path)
            .expect_err("an orphaned backup must stop identity creation");

        assert!(backup.exists(), "the only recoverable identity must remain");
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

        IdentityStore::store_app_id(&path, "4002").expect("store app id");

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

        IdentityStore::store_app_id(&path, "4002").expect("first");
        IdentityStore::store_app_id(&path, "3387").expect("second");

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
        IdentityStore::set_app_id(&path, Some("4002")).expect("attach");

        let present = IdentityStore::set_app_id(&path, None).expect("detach");

        assert!(present, "the identity still exists");
        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.app_id, None);
        assert_eq!(loaded.identity_cert_pem, identity.identity_cert_pem);
        assert_eq!(loaded.private_key_pem, identity.private_key_pem);
        assert_eq!(loaded.enc_private_key_pem, identity.enc_private_key_pem);
    }

    /// The app id arrives over an established stream, which requires an
    /// identity — so a missing file means one was cleared concurrently by a
    /// `Remove`. Writing a fresh file here would resurrect an instance the
    /// control plane just released.
    #[test]
    fn store_app_id_does_not_create_an_identity() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");

        IdentityStore::store_app_id(&path, "4002").expect("no-op on a missing identity");

        assert!(
            IdentityStore::load_optional(&path).expect("load").is_none(),
            "a released instance must not be resurrected by a metrics label"
        );
    }

    /// A release racing app-id updates must win: `store_app_id` reads the file
    /// and writes it back, so a removal landing between the two would be undone
    /// and the instance would keep talking to a control plane that released it.
    /// Both sides take the same writer lock, which leaves only the two orderings
    /// where the file ends up gone.
    #[test]
    fn a_release_wins_over_concurrent_app_id_updates() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).expect("store");

        std::thread::scope(|scope| {
            let updater = scope.spawn(|| {
                for i in 0..200 {
                    IdentityStore::store_app_id(&path, &format!("400{i}")).expect("store app id");
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
        IdentityStore::store_app_id(&path, "4002").expect("store app id");

        rotated.private_key_pem = "ROTATED-KEY".to_string();
        rotated.identity_cert_pem = "ROTATED-CERT".to_string();
        rotated.enc_previous_private_key_pem.clear();
        assert_eq!(
            rotated.app_id, None,
            "the renewal clone is stale by construction"
        );
        let merged = IdentityStore::store_credential_update(&path, &rotated)
            .expect("store rotated")
            .expect("identity still present");

        let loaded = IdentityStore::load_optional(&path)
            .expect("load")
            .expect("present");
        assert_eq!(loaded.private_key_pem, "ROTATED-KEY");
        assert!(loaded.enc_previous_private_key_pem.is_empty());
        assert_eq!(loaded.app_id.as_deref(), Some("4002"));
        assert_eq!(merged.app_id.as_deref(), Some("4002"));
    }

    #[test]
    fn credential_update_does_not_recreate_a_removed_identity() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("identity.json");
        let identity = sample_identity();
        IdentityStore::store(&path, &identity).expect("store");
        IdentityStore::clear(&path).expect("remove");

        let stored = IdentityStore::store_credential_update(&path, &identity)
            .expect("credential update is a no-op");

        assert!(stored.is_none());
        assert!(IdentityStore::load_optional(&path).expect("load").is_none());
    }

    #[test]
    fn identity_updates_do_not_overlap_a_removal_transaction() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config_dir = dir.path().join(".spice");
        let path = config_dir.join("identity.json");
        let identity = sample_identity();
        IdentityStore::store(&path, &identity).expect("store");
        let removal = crate::draft::EnrollmentTransactionLock::try_acquire(&config_dir)
            .expect("hold the removal transaction");

        let mut rotated = identity.clone();
        rotated.private_key_pem = "ROTATED-KEY".to_string();
        let credential_error = IdentityStore::store_credential_update(&path, &rotated)
            .expect_err("credential update must not overlap removal");
        let attachment_error = IdentityStore::set_app_id(&path, Some("4002"))
            .expect_err("attachment update must not overlap removal");

        assert!(
            credential_error
                .to_string()
                .contains("Another live process"),
            "{credential_error}"
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
        let merged = IdentityStore::store_credential_update(&path, &rotated)
            .expect("store after removal transaction")
            .expect("identity remains");
        assert_eq!(merged.private_key_pem, "ROTATED-KEY");
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
    fn is_expired_detects_past_timestamp() {
        let mut identity = sample_identity();
        identity.not_after_unix = Some(1);
        assert!(identity.is_expired());
    }

    #[test]
    fn is_expired_treats_boundary_second_as_expired() {
        // A cert whose `not_after_unix` equals the current second is past the
        // NotAfter boundary and must be considered expired.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .expect("system clock after unix epoch");
        let mut identity = sample_identity();
        identity.not_after_unix = Some(now);
        assert!(identity.is_expired());
    }

    #[test]
    fn is_expired_accepts_future_timestamp() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .expect("system clock after unix epoch");
        let mut identity = sample_identity();
        identity.not_after_unix = Some(now + 3600);
        assert!(!identity.is_expired());
    }
}
