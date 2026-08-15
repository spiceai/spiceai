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

//! Encrypted at-rest cache for control-plane-delivered secrets.
//!
//! A deployment that delivers secrets applies by restart, so secrets that lived
//! only in process memory would be gone by the time the components that need
//! them come back up — that deploy would take two deploys. This cache closes it:
//! it is written when a deployment delivers secrets and opened locally at
//! startup, with **no control-plane round trip**, so a restart succeeds even
//! with the gateway unreachable.
//!
//! # Not the wire envelope
//!
//! The sealed envelope on the wire is a *transport* format keyed to the
//! enrolled encryption key, which rotates about every 12 hours. Persisting it
//! as-is would strand the cache on every renewal. This is a separate local
//! format under a separate, local, **non-rotating** key
//! ([`Identity::cache_key`](crate::Identity::cache_key)) for exactly that
//! reason — which is also why the crypto here is a plain AEAD rather than the
//! HPKE path in `cloud-connect-crypto`.
//!
//! # Format
//!
//! One JSON document. The header fields are plaintext so `spice connect status`
//! can report *which* secrets are cached — and diagnose a dangling reference —
//! without holding the key. The values are in the sealed body and nowhere else.
//!
//! ```json
//! {
//!   "format_version": 1,
//!   "suite": "xchacha20poly1305",
//!   "deployment_version": "42",
//!   "names": ["openai_key"],
//!   "nonce_b64": "…",
//!   "ciphertext_b64": "…"
//! }
//! ```
//!
//! The header is **authenticated**: it is canonically re-encoded into the AEAD's
//! additional data (see [`header_aad`]), so an edited `names` list or a swapped
//! `deployment_version` fails the open instead of silently misreporting what is
//! cached.
//!
//! A random 192-bit `XChaCha20` nonce is drawn per write, which is what makes a
//! random nonce safe under a key that never rotates.

use std::collections::BTreeMap;
use std::path::Path;

use base64::Engine as _;
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt as _, ResultExt, Snafu};
use zeroize::Zeroizing;

/// File name (relative to the config dir) of the delivered-secrets cache.
pub const SECRET_CACHE_FILE: &str = "secrets-cache.json";

/// The only format version this build writes, and the only one it opens.
pub const FORMAT_VERSION: u32 = 1;

/// AEAD suite label recorded in the header.
const SUITE: &str = "xchacha20poly1305";

/// XChaCha20-Poly1305 nonce length (192 bits — wide enough that random nonces
/// need no counter).
const NONCE_LEN: usize = 24;

/// Largest plaintext this cache will write or open.
///
/// Bounded so a malformed or hostile payload cannot drive an unbounded write, or
/// an unbounded allocation on the read side. Matches the wire format's plaintext
/// ceiling, since the same payload arrived through it.
pub const MAX_CACHE_PLAINTEXT: usize = cloud_connect_crypto::MAX_SECRET_PLAINTEXT_SIZE;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to read the secrets cache at {}: {source}", path.display()))]
    Read {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to write the secrets cache at {}: {source}", path.display()))]
    Write {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("The secrets cache at {} is not valid JSON: {source}", path.display()))]
    Malformed {
        path: std::path::PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display(
        "The secrets cache at {} is format version {found}, but this runtime writes version \
         {FORMAT_VERSION}. Discarding it; deploy the app to re-deliver its secrets.",
        path.display()
    ))]
    UnsupportedVersion {
        path: std::path::PathBuf,
        found: u32,
    },

    #[snafu(display(
        "The secrets cache at {} uses AEAD suite {found:?}, which this runtime does not \
         implement. Discarding it; deploy the app to re-deliver its secrets.",
        path.display()
    ))]
    UnsupportedSuite {
        path: std::path::PathBuf,
        found: String,
    },

    #[snafu(display(
        "The secrets cache at {} could not be decrypted — it was written under a different cache \
         key, or it has been modified. Discarding it; deploy the app to re-deliver its secrets.",
        path.display()
    ))]
    Undecryptable { path: std::path::PathBuf },

    #[snafu(display("The secrets cache key is malformed (expected {expected} bytes)"))]
    KeyLength { expected: usize },

    #[snafu(display(
        "Delivered secrets are {size} bytes, over the {MAX_CACHE_PLAINTEXT}-byte cache limit. \
         They are applied to this running instance but not cached, so a restart needs a \
         redeploy. Reduce the number or size of the app's secrets."
    ))]
    TooLarge { size: usize },

    #[snafu(display("Failed to encode the secrets cache: {source}"))]
    Encode { source: serde_json::Error },

    #[snafu(display("Failed to draw a nonce for the secrets cache: {source}"))]
    Randomness { source: getrandom::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// On-disk shape. `names` duplicates the sealed body's keys deliberately: it is
/// what lets status answer "which secrets do I have" without the key.
#[derive(Serialize, Deserialize)]
struct CacheFile {
    format_version: u32,
    suite: String,
    /// The deployment whose dispatch delivered these secrets, so a restart can
    /// report which deployment it came up on. Empty when unknown.
    #[serde(default)]
    deployment_version: String,
    /// Sorted secret names — never values.
    names: Vec<String>,
    nonce_b64: String,
    ciphertext_b64: String,
}

/// Delivered secrets, decrypted.
///
/// Values are held in [`Zeroizing`] so the buffers are scrubbed on drop rather
/// than left in freed heap.
pub struct CachedSecrets {
    /// The deployment version recorded when the cache was written.
    pub deployment_version: String,
    values: BTreeMap<String, Zeroizing<Vec<u8>>>,
}

/// Names and provenance only. A derived `Debug` would put every delivered
/// secret value into any log line or panic message that formatted this.
impl std::fmt::Debug for CachedSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedSecrets")
            .field("deployment_version", &self.deployment_version)
            .field("names", &self.names())
            .finish_non_exhaustive()
    }
}

impl CachedSecrets {
    #[must_use]
    pub fn new(deployment_version: String, values: BTreeMap<String, Zeroizing<Vec<u8>>>) -> Self {
        Self {
            deployment_version,
            values,
        }
    }

    /// The cached secret names, sorted. Safe to log and to report in status.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.values.keys().cloned().collect()
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Zeroizing<Vec<u8>>> {
        self.values.get(name)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    #[must_use]
    pub fn into_values(self) -> BTreeMap<String, Zeroizing<Vec<u8>>> {
        self.values
    }
}

/// The additional authenticated data for the sealed body: the header fields,
/// canonically encoded.
///
/// Built explicitly rather than by re-serializing the struct, because AEAD
/// additional data has to be byte-identical on write and open and `serde_json`
/// makes no such guarantee across versions or field reorderings. Components are
/// NUL-joined and lengths are prefixed on the name list, so no combination of
/// names can encode to the same bytes as a different combination.
fn header_aad(
    format_version: u32,
    suite: &str,
    deployment_version: &str,
    names: &[String],
) -> Vec<u8> {
    let mut aad = Vec::with_capacity(64 + names.iter().map(String::len).sum::<usize>());
    aad.extend_from_slice(&format_version.to_be_bytes());
    aad.push(0);
    aad.extend_from_slice(suite.as_bytes());
    aad.push(0);
    aad.extend_from_slice(deployment_version.as_bytes());
    aad.push(0);
    // Length-prefixed so `["ab","c"]` and `["a","bc"]` cannot collide.
    aad.extend_from_slice(&u32::try_from(names.len()).unwrap_or(u32::MAX).to_be_bytes());
    for name in names {
        aad.extend_from_slice(&u32::try_from(name.len()).unwrap_or(u32::MAX).to_be_bytes());
        aad.extend_from_slice(name.as_bytes());
    }
    aad
}

fn cipher(key: &[u8]) -> Result<XChaCha20Poly1305> {
    let key: &[u8; 32] = key.try_into().ok().context(KeyLengthSnafu {
        expected: crate::identity::CACHE_KEY_LEN,
    })?;
    Ok(XChaCha20Poly1305::new(key.into()))
}

/// Build a nonce from raw bytes whose length was already checked.
fn nonce_from(bytes: &[u8; NONCE_LEN]) -> XNonce {
    XNonce::from(*bytes)
}

/// Write `secrets` to the cache at `path`, sealed under `key`.
///
/// The file is written with owner-only permissions through the same atomic
/// rename the identity file uses, so a reader never sees a partial cache and the
/// values are not world-readable even briefly.
///
/// # Errors
///
/// Returns [`Error::TooLarge`] when the payload exceeds
/// [`MAX_CACHE_PLAINTEXT`], and the I/O and encoding variants on failure. A
/// caller treats every one as non-fatal: the secrets are already applied to the
/// running instance, and a failed cache only costs a redeploy after a restart.
pub fn write(
    path: &Path,
    key: &[u8],
    deployment_version: &str,
    secrets: &BTreeMap<String, Zeroizing<Vec<u8>>>,
) -> Result<()> {
    // `BTreeMap` iterates in key order, so `names` and the serialized body are
    // both canonical without a separate sort.
    let names: Vec<String> = secrets.keys().cloned().collect();

    let plaintext = encode_values(secrets)?;
    snafu::ensure!(
        plaintext.len() <= MAX_CACHE_PLAINTEXT,
        TooLargeSnafu {
            size: plaintext.len()
        }
    );

    let mut nonce = [0_u8; NONCE_LEN];
    getrandom::fill(&mut nonce).context(RandomnessSnafu)?;

    let aad = header_aad(FORMAT_VERSION, SUITE, deployment_version, &names);
    let ciphertext = cipher(key)?
        .encrypt(
            &nonce_from(&nonce),
            Payload {
                msg: plaintext.as_ref(),
                aad: &aad,
            },
        )
        .map_err(|_| Error::Undecryptable {
            path: path.to_path_buf(),
        })?;

    let file = CacheFile {
        format_version: FORMAT_VERSION,
        suite: SUITE.to_string(),
        deployment_version: deployment_version.to_string(),
        names,
        nonce_b64: base64::engine::general_purpose::STANDARD.encode(nonce),
        ciphertext_b64: base64::engine::general_purpose::STANDARD.encode(&ciphertext),
    };
    let bytes = serde_json::to_vec_pretty(&file).context(EncodeSnafu)?;
    crate::identity::atomic_write_owner_only(path, &bytes).context(WriteSnafu {
        path: path.to_path_buf(),
    })
}

/// Open the cache at `path` under `key`, returning `Ok(None)` when there is no
/// cache file.
///
/// # Errors
///
/// Every failure mode — unknown format version, corrupt JSON, wrong key,
/// tampered header — is an error the caller *discards the cache* on rather than
/// crashing over; the messages say so and name the recovery (deploy again).
pub fn read(path: &Path, key: &[u8]) -> Result<Option<CachedSecrets>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(Error::Read {
                path: path.to_path_buf(),
                source,
            });
        }
    };

    let file: CacheFile = serde_json::from_slice(&bytes).context(MalformedSnafu {
        path: path.to_path_buf(),
    })?;
    snafu::ensure!(
        file.format_version == FORMAT_VERSION,
        UnsupportedVersionSnafu {
            path: path.to_path_buf(),
            found: file.format_version,
        }
    );
    // Moved, not cloned: this arm returns, so nothing reads `file.suite` after
    // it. The `header_aad` call below is on the success path only.
    snafu::ensure!(
        file.suite == SUITE,
        UnsupportedSuiteSnafu {
            path: path.to_path_buf(),
            found: file.suite,
        }
    );

    let undecryptable = || Error::Undecryptable {
        path: path.to_path_buf(),
    };
    let nonce: [u8; NONCE_LEN] = base64::engine::general_purpose::STANDARD
        .decode(&file.nonce_b64)
        .map_err(|_| undecryptable())?
        .try_into()
        .map_err(|_| undecryptable())?;
    let ciphertext = base64::engine::general_purpose::STANDARD
        .decode(&file.ciphertext_b64)
        .map_err(|_| undecryptable())?;
    // Refuse an oversized ciphertext before decrypting it, so a hostile file
    // cannot drive a large allocation on the way in.
    snafu::ensure!(
        ciphertext.len() <= MAX_CACHE_PLAINTEXT + cloud_connect_crypto::AEAD_TAG_LEN,
        TooLargeSnafu {
            size: ciphertext.len()
        }
    );

    let aad = header_aad(
        file.format_version,
        &file.suite,
        &file.deployment_version,
        &file.names,
    );
    let plaintext = Zeroizing::new(
        cipher(key)?
            .decrypt(
                &nonce_from(&nonce),
                Payload {
                    msg: &ciphertext,
                    aad: &aad,
                },
            )
            .map_err(|_| undecryptable())?,
    );

    let values = decode_values(&plaintext).ok_or_else(undecryptable)?;
    Ok(Some(CachedSecrets {
        deployment_version: file.deployment_version,
        values,
    }))
}

/// Delete the cache file. A missing file is success.
///
/// Success means the file is absent and its directory entry has been
/// synchronized as far as the platform allows. A release deletes this cache
/// before the identity holding its key, so an unlink that is acknowledged but
/// not durable lets the entry come back after a crash, beside a durably-deleted
/// identity — exactly the stranded cache this function exists to prevent.
///
/// That includes the already-missing case, which is what a retry sees. Returning
/// success there without synchronizing would let a caller whose earlier unlink
/// went unsynced go on to clear the identity, and a crash could then roll the
/// cache back with no key left to open it.
///
/// **Unix only.** [`crate::identity::sync_parent_directory`] cannot flush a
/// directory entry through `std::fs` on other platforms and is a no-op there, so
/// on those the absence is only as durable as the filesystem's own metadata
/// ordering. Every removal in this crate shares that limit; it is stated here
/// because this is the one whose result a caller uses to decide it may clear the
/// identity.
///
/// # Errors
///
/// Returns [`Error::Write`] when the file exists but cannot be removed, or when
/// its absence cannot be made durable — the caller must know, since leaving it
/// behind leaves secrets on a host that was meant to be released.
pub fn remove(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_absence(path),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => sync_absence(path),
        Err(source) => Err(Error::Write {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Synchronize the directory that held `path`, so far as the platform allows, so
/// its absence survives a crash. A directory that is itself gone needs nothing:
/// the entry cannot come back.
fn sync_absence(path: &Path) -> Result<()> {
    match crate::identity::sync_parent_directory(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(Error::Write {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Read only the plaintext header, without the key.
///
/// This is what lets `spice connect status` say which secrets are cached, and
/// from which deployment, on a host where it holds no key at all. Returns `None`
/// when there is no cache or it cannot be parsed.
#[must_use]
pub fn read_header(path: &Path) -> Option<CacheHeader> {
    let bytes = std::fs::read(path).ok()?;
    let file: CacheFile = serde_json::from_slice(&bytes).ok()?;
    Some(CacheHeader {
        format_version: file.format_version,
        deployment_version: file.deployment_version,
        names: file.names,
    })
}

/// The plaintext half of the cache: what is cached, never any value.
#[derive(Debug, Clone)]
pub struct CacheHeader {
    pub format_version: u32,
    pub deployment_version: String,
    pub names: Vec<String>,
}

/// Encode the values as a length-prefixed sequence, in key order.
///
/// Deliberately not JSON: a secret value is arbitrary bytes, and round-tripping
/// it through JSON would force base64 (a second plaintext copy the encoder owns
/// and will not zeroize) or lossy UTF-8.
fn encode_values(secrets: &BTreeMap<String, Zeroizing<Vec<u8>>>) -> Result<Zeroizing<Vec<u8>>> {
    let size: usize = secrets
        .iter()
        .map(|(name, value)| 8 + name.len() + value.len())
        .sum();
    snafu::ensure!(size <= MAX_CACHE_PLAINTEXT, TooLargeSnafu { size });

    let mut out = Zeroizing::new(Vec::with_capacity(size));
    for (name, value) in secrets {
        let name_len = u32::try_from(name.len()).map_err(|_| Error::TooLarge { size })?;
        let value_len = u32::try_from(value.len()).map_err(|_| Error::TooLarge { size })?;
        out.extend_from_slice(&name_len.to_be_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&value_len.to_be_bytes());
        out.extend_from_slice(value);
    }
    Ok(out)
}

/// Inverse of [`encode_values`]. `None` on any truncation or non-UTF-8 name —
/// the caller reports the cache as undecryptable, since a body that decrypted
/// but does not parse means the format is not what this build writes.
fn decode_values(bytes: &[u8]) -> Option<BTreeMap<String, Zeroizing<Vec<u8>>>> {
    let mut out = BTreeMap::new();
    let mut cursor = 0_usize;
    while cursor < bytes.len() {
        let name_len = read_u32(bytes, &mut cursor)?;
        let name = std::str::from_utf8(bytes.get(cursor..cursor.checked_add(name_len)?)?).ok()?;
        cursor = cursor.checked_add(name_len)?;
        let value_len = read_u32(bytes, &mut cursor)?;
        let value = bytes.get(cursor..cursor.checked_add(value_len)?)?;
        cursor = cursor.checked_add(value_len)?;
        out.insert(name.to_string(), Zeroizing::new(value.to_vec()));
    }
    Some(out)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Option<usize> {
    let end = cursor.checked_add(4)?;
    let raw: [u8; 4] = bytes.get(*cursor..end)?.try_into().ok()?;
    *cursor = end;
    Some(u32::from_be_bytes(raw) as usize)
}

#[cfg(test)]
mod tests {

    /// A retry sees the file already gone. Reporting success there without
    /// synchronizing would let a caller whose earlier unlink went unsynced go on
    /// to clear the identity, and a crash could roll the cache back with no key
    /// left to open it — so removal has to establish durable absence, not just
    /// absence.
    #[test]
    fn removing_an_already_missing_cache_still_synchronizes_its_absence() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join(SECRET_CACHE_FILE);

        super::remove(&path).expect("a missing cache is success");

        // A directory that is gone too needs nothing: the entry cannot return.
        let vanished = dir.path().join("gone").join(SECRET_CACHE_FILE);
        super::remove(&vanished).expect("a missing directory is success as well");
    }

    /// That the sync actually happens, which a successful one cannot show. A
    /// directory the process may traverse and write but not open for reading
    /// lets the unlink report the file missing and the synchronization fail, so
    /// success here would mean the absence was never made durable.
    #[cfg(unix)]
    #[test]
    fn an_absence_that_cannot_be_synchronized_is_not_reported_as_removed() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("create tempdir");
        let holder = dir.path().join("write-only");
        std::fs::create_dir(&holder).expect("create the directory");
        let path = holder.join(SECRET_CACHE_FILE);
        std::fs::set_permissions(&holder, std::fs::Permissions::from_mode(0o311))
            .expect("make the directory traversable and writable but not readable");

        let removed = super::remove(&path);

        // Restore before asserting, so a failure cannot leave the tempdir
        // undeletable.
        std::fs::set_permissions(&holder, std::fs::Permissions::from_mode(0o755))
            .expect("restore the directory");
        removed.expect_err("an absence that cannot be made durable is not a removal");
    }
    use super::*;

    fn scratch(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("spice-cache-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    fn key() -> Vec<u8> {
        (0..32_u8).collect()
    }

    fn secrets(entries: &[(&str, &[u8])]) -> BTreeMap<String, Zeroizing<Vec<u8>>> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), Zeroizing::new(v.to_vec())))
            .collect()
    }

    #[test]
    fn round_trips_values_and_names() {
        let dir = scratch("round-trip");
        let path = dir.join(SECRET_CACHE_FILE);
        let input = secrets(&[("openai_key", b"sk-live-1"), ("pg_password", b"hunter2")]);

        write(&path, &key(), "42", &input).expect("write cache");
        let read_back = read(&path, &key()).expect("read cache").expect("present");

        assert_eq!(read_back.deployment_version, "42");
        assert_eq!(read_back.names(), vec!["openai_key", "pg_password"]);
        assert_eq!(
            read_back.get("openai_key").map(|v| v.to_vec()),
            Some(b"sk-live-1".to_vec())
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn values_never_appear_in_plaintext_on_disk() {
        let dir = scratch("no-plaintext");
        let path = dir.join(SECRET_CACHE_FILE);
        write(
            &path,
            &key(),
            "7",
            &secrets(&[("openai_key", b"sk-super-secret-value")]),
        )
        .expect("write cache");

        let raw = std::fs::read_to_string(&path).expect("read raw");
        assert!(
            !raw.contains("sk-super-secret-value"),
            "the cache must never hold a plaintext value"
        );
        // The name, by contrast, is deliberately readable so status can report
        // it without the key.
        assert!(raw.contains("openai_key"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_header_reports_names_without_the_key() {
        let dir = scratch("header");
        let path = dir.join(SECRET_CACHE_FILE);
        write(&path, &key(), "9", &secrets(&[("a", b"1"), ("b", b"2")])).expect("write");

        let header = read_header(&path).expect("header parses");
        assert_eq!(header.format_version, FORMAT_VERSION);
        assert_eq!(header.deployment_version, "9");
        assert_eq!(header.names, vec!["a", "b"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_cache_is_not_an_error() {
        let dir = scratch("missing");
        let path = dir.join(SECRET_CACHE_FILE);
        assert!(read(&path, &key()).expect("no cache is Ok").is_none());
        assert!(read_header(&path).is_none());
        // Removing an absent cache is also success.
        remove(&path).expect("remove absent");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wrong_key_is_rejected_and_names_no_value() {
        let dir = scratch("wrong-key");
        let path = dir.join(SECRET_CACHE_FILE);
        write(&path, &key(), "1", &secrets(&[("openai_key", b"sk-1")])).expect("write");

        let mut other = key();
        other[0] ^= 0xff;
        let err = read(&path, &other).expect_err("a different key must not open it");
        assert!(matches!(err, Error::Undecryptable { .. }), "{err}");
        assert!(!err.to_string().contains("sk-1"));
        assert!(
            err.to_string().contains("deploy"),
            "must name recovery: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_tampered_header_fails_the_open() {
        let dir = scratch("tampered");
        let path = dir.join(SECRET_CACHE_FILE);
        write(&path, &key(), "1", &secrets(&[("openai_key", b"sk-1")])).expect("write");

        // Editing the plaintext name list must not go unnoticed: the header is
        // in the AEAD's additional data precisely so status cannot be lied to.
        let raw = std::fs::read_to_string(&path).expect("read");
        let edited = raw.replace("openai_key", "other_name");
        std::fs::write(&path, edited).expect("write tampered");

        let err = read(&path, &key()).expect_err("a tampered header must fail");
        assert!(matches!(err, Error::Undecryptable { .. }), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_tampered_deployment_version_fails_the_open() {
        let dir = scratch("tampered-version");
        let path = dir.join(SECRET_CACHE_FILE);
        write(&path, &key(), "41", &secrets(&[("k", b"v")])).expect("write");

        let raw = std::fs::read_to_string(&path).expect("read");
        std::fs::write(&path, raw.replace("\"41\"", "\"99\"")).expect("write tampered");
        assert!(matches!(
            read(&path, &key()),
            Err(Error::Undecryptable { .. })
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_unknown_format_version_is_a_discard_not_a_crash() {
        let dir = scratch("version");
        let path = dir.join(SECRET_CACHE_FILE);
        write(&path, &key(), "1", &secrets(&[("k", b"v")])).expect("write");

        let raw = std::fs::read_to_string(&path).expect("read");
        let bumped = raw.replace(
            &format!("\"format_version\": {FORMAT_VERSION}"),
            "\"format_version\": 9999",
        );
        std::fs::write(&path, bumped).expect("write bumped");

        let err = read(&path, &key()).expect_err("an unknown version must not be opened");
        assert!(
            matches!(err, Error::UnsupportedVersion { found: 9999, .. }),
            "{err}"
        );
        assert!(err.to_string().contains("Discarding"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_json_is_a_discard_not_a_crash() {
        let dir = scratch("corrupt");
        let path = dir.join(SECRET_CACHE_FILE);
        std::fs::write(&path, b"{not json").expect("write garbage");
        assert!(matches!(read(&path, &key()), Err(Error::Malformed { .. })));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_oversized_payload_is_refused_rather_than_written() {
        let dir = scratch("oversize");
        let path = dir.join(SECRET_CACHE_FILE);
        let huge = vec![0_u8; MAX_CACHE_PLAINTEXT + 1];
        let err = write(&path, &key(), "1", &secrets(&[("big", &huge)]))
            .expect_err("over the cap must be refused");
        assert!(matches!(err, Error::TooLarge { .. }), "{err}");
        assert!(!path.exists(), "nothing should have been written");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_short_key_is_rejected() {
        let dir = scratch("short-key");
        let path = dir.join(SECRET_CACHE_FILE);
        let err = write(&path, &[0_u8; 16], "1", &secrets(&[("k", b"v")]))
            .expect_err("a 16-byte key is not a 32-byte key");
        assert!(matches!(err, Error::KeyLength { .. }), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_and_binary_values_round_trip() {
        let dir = scratch("edges");
        let path = dir.join(SECRET_CACHE_FILE);
        // An empty value, invalid UTF-8, and an embedded NUL: a secret is
        // arbitrary bytes, which is why the body is not JSON.
        let input = secrets(&[
            ("empty", b""),
            ("binary", &[0xff, 0x00, 0xfe]),
            ("nul", b"a\0b"),
        ]);
        write(&path, &key(), "", &input).expect("write");

        let out = read(&path, &key()).expect("read").expect("present");
        assert_eq!(out.get("empty").map(|v| v.to_vec()), Some(Vec::new()));
        assert_eq!(
            out.get("binary").map(|v| v.to_vec()),
            Some(vec![0xff, 0x00, 0xfe])
        );
        assert_eq!(out.get("nul").map(|v| v.to_vec()), Some(b"a\0b".to_vec()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn header_aad_length_prefixes_names_so_they_cannot_collide() {
        // `["ab","c"]` and `["a","bc"]` concatenate identically; the length
        // prefixes are what keep their AADs distinct.
        let a = header_aad(1, SUITE, "v", &["ab".to_string(), "c".to_string()]);
        let b = header_aad(1, SUITE, "v", &["a".to_string(), "bc".to_string()]);
        assert_ne!(a, b);
    }

    #[test]
    fn decode_rejects_a_truncated_body() {
        // A body that decrypts but does not parse means the format is not what
        // this build writes — never a partial map.
        assert!(decode_values(&[0, 0, 0, 8, b'a']).is_none());
        assert!(decode_values(&[0, 0, 0, 1, b'a', 0, 0, 0, 9]).is_none());
        assert!(
            decode_values(&[])
                .expect("empty body is an empty map")
                .is_empty()
        );
    }
}
