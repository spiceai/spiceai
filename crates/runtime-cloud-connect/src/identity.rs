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

//! Local persistence for the post-adoption runtime identity.
//!
//! The identity file lives at `$SPICE_CONFIG_DIR/identity.json` with
//! `0600` perms on Unix. On first adoption we generate an ed25519
//! keypair, send the public key to the server in `AdoptAck`, and
//! receive a signed identity certificate that we persist alongside the
//! private key for later reconnects.
//!
//! The JSON layout is intentionally narrow — it is a private interface
//! between the runtime and the local filesystem. Other Spice tooling
//! should treat it as opaque.

use std::path::{Path, PathBuf};

use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey};
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

/// Errors that can occur while reading or writing the identity file.
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

    #[snafu(display("Failed to encode private key in PKCS#8: {source}"))]
    EncodePrivateKey { source: ed25519_dalek::pkcs8::Error },

    #[snafu(display("Failed to decode private key from PKCS#8: {source}"))]
    DecodePrivateKey { source: ed25519_dalek::pkcs8::Error },

    #[snafu(display("Failed to encode public key in SPKI: {source}"))]
    EncodePublicKey {
        source: ed25519_dalek::pkcs8::spki::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Persisted runtime identity. Treat as opaque outside this crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    /// Server-assigned identifier (`inst_...`).
    pub identifier: String,
    /// PEM-encoded X.509 cert (or token) returned by the server in
    /// `Adopt.identity_cert_pem`. Verbatim — the client does not parse
    /// or validate it; it is sent back as `Hello.credential`.
    pub identity_cert_pem: String,
    /// PEM-encoded PKCS#8 ed25519 private key.
    pub private_key_pem: String,
    /// PEM-encoded SPKI ed25519 public key. Echoes back in `AdoptAck`
    /// so the server can pin it.
    pub public_key_pem: String,
    /// Unix timestamp (seconds) after which the identity cert is no
    /// longer accepted by the server. `0` means "unknown / unbounded".
    pub not_after_unix: u64,
}

impl Identity {
    /// Returns `true` if `not_after_unix` is in the past relative to the
    /// system clock. Returns `false` if `not_after_unix == 0`.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        if self.not_after_unix == 0 {
            return false;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        now > self.not_after_unix
    }
}

/// On-disk identity store rooted at a single JSON file.
#[derive(Debug, Clone)]
pub struct IdentityStore;

impl IdentityStore {
    /// Read an identity file, returning `Ok(None)` if it does not exist.
    pub fn load_optional(path: &Path) -> Result<Option<Identity>> {
        match std::fs::read_to_string(path) {
            Ok(s) => {
                let identity: Identity =
                    serde_json::from_str(&s).context(ParseSnafu { path: path.to_path_buf() })?;
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
    pub fn store(path: &Path, identity: &Identity) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context(IoSnafu {
                path: parent.to_path_buf(),
            })?;
        }
        let bytes = serde_json::to_vec_pretty(identity).context(SerializeSnafu)?;
        atomic_write(path, &bytes)
    }

    /// Remove the identity file. No-op if it doesn't exist.
    pub fn clear(path: &Path) -> Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(Error::Io {
                path: path.to_path_buf(),
                source: err,
            }),
        }
    }

    /// Generate a fresh ed25519 keypair as PEM-encoded PKCS#8 (private)
    /// and SPKI (public) strings. Used at adoption time before sending
    /// `AdoptAck`.
    pub fn generate_keypair() -> Result<KeyPairPem> {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        let private_key_pem = signing_key
            .to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
            .context(EncodePrivateKeySnafu)?
            .to_string();
        let public_key_pem = verifying_key
            .to_public_key_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
            .context(EncodePublicKeySnafu)?;
        Ok(KeyPairPem {
            private_key_pem,
            public_key_pem,
        })
    }

    /// Parse a PEM-encoded PKCS#8 ed25519 private key back into a
    /// `SigningKey`. Useful for future challenge-response work; not
    /// used today.
    pub fn signing_key_from_pem(pem: &str) -> Result<SigningKey> {
        SigningKey::from_pkcs8_pem(pem).context(DecodePrivateKeySnafu)
    }
}

/// Newly-generated keypair returned by [`IdentityStore::generate_keypair`].
#[derive(Debug, Clone)]
pub struct KeyPairPem {
    pub private_key_pem: String,
    pub public_key_pem: String,
}

#[cfg(unix)]
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;

    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("identity.json");
    let tmp_path = dir.join(format!(".{file_name}.tmp"));

    {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&tmp_path)
            .context(IoSnafu {
                path: tmp_path.clone(),
            })?;
        file.write_all(bytes).context(IoSnafu {
            path: tmp_path.clone(),
        })?;
        file.sync_all().context(IoSnafu {
            path: tmp_path.clone(),
        })?;
    }

    std::fs::rename(&tmp_path, path).context(IoSnafu {
        path: path.to_path_buf(),
    })?;
    Ok(())
}

#[cfg(not(unix))]
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("identity.json");
    let tmp_path = dir.join(format!(".{file_name}.tmp"));
    {
        let mut file = std::fs::File::create(&tmp_path).context(IoSnafu {
            path: tmp_path.clone(),
        })?;
        file.write_all(bytes).context(IoSnafu {
            path: tmp_path.clone(),
        })?;
        file.sync_all().context(IoSnafu {
            path: tmp_path.clone(),
        })?;
    }
    std::fs::rename(&tmp_path, path).context(IoSnafu {
        path: path.to_path_buf(),
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_identity() -> Identity {
        Identity {
            identifier: "inst_test".to_string(),
            identity_cert_pem: "-----BEGIN CERTIFICATE-----\nMOCK\n-----END CERTIFICATE-----\n"
                .to_string(),
            private_key_pem: "-----BEGIN PRIVATE KEY-----\nMOCK\n-----END PRIVATE KEY-----\n"
                .to_string(),
            public_key_pem: "-----BEGIN PUBLIC KEY-----\nMOCK\n-----END PUBLIC KEY-----\n"
                .to_string(),
            not_after_unix: 0,
        }
    }

    #[test]
    fn round_trip_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.json");
        let identity = sample_identity();

        IdentityStore::store(&path, &identity).expect("store");
        let loaded = IdentityStore::load_optional(&path).expect("load").expect("present");

        assert_eq!(loaded.identifier, identity.identifier);
        assert_eq!(loaded.identity_cert_pem, identity.identity_cert_pem);
        assert_eq!(loaded.public_key_pem, identity.public_key_pem);
    }

    #[test]
    fn load_optional_returns_none_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist.json");
        let loaded = IdentityStore::load_optional(&path).expect("load");
        assert!(loaded.is_none());
    }

    #[test]
    fn clear_removes_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).unwrap();
        assert!(path.exists());
        IdentityStore::clear(&path).unwrap();
        assert!(!path.exists());
        // Idempotent.
        IdentityStore::clear(&path).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn store_writes_with_0600_perms() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.json");
        IdentityStore::store(&path, &sample_identity()).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn generate_keypair_produces_pem_pair() {
        let pair = IdentityStore::generate_keypair().expect("generate");
        assert!(pair.private_key_pem.contains("PRIVATE KEY"));
        assert!(pair.public_key_pem.contains("PUBLIC KEY"));
        // Round-trip parse the private key.
        let _signing = IdentityStore::signing_key_from_pem(&pair.private_key_pem).unwrap();
    }

    #[test]
    fn is_expired_handles_zero_as_unbounded() {
        let identity = sample_identity();
        assert!(!identity.is_expired());
    }

    #[test]
    fn is_expired_detects_past_timestamp() {
        let mut identity = sample_identity();
        identity.not_after_unix = 1;
        assert!(identity.is_expired());
    }
}
