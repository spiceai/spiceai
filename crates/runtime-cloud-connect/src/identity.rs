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
//! `0600` perms on Unix. On first boot the client generates a keypair
//! (ECDSA P-256) and a PKCS#10 CSR *before* it says `Hello`, sends the
//! CSR (and the public key) in the enrollment `Hello`, and receives a
//! leaf certificate the control plane signed from that CSR in `Adopt`.
//! The leaf, the matching private key, and the issuing-CA bundle are
//! persisted here for later mTLS reconnects. Generating the key and
//! proving possession of it (via the CSR) *before* the cert is issued is
//! what makes the issued cert genuinely bind the keypair used for mTLS.
//!
//! The JSON layout is intentionally narrow — it is a private interface
//! between the runtime and the local filesystem. Other Spice tooling
//! should treat it as opaque.

use std::path::{Path, PathBuf};

use rcgen::{CertificateParams, DnType, ExtendedKeyUsagePurpose, KeyPair};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Persisted runtime identity. Treat as opaque outside this crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    /// Server-assigned identifier (`inst_...`).
    pub identifier: String,
    /// PEM-encoded X.509 leaf certificate the control plane signed from
    /// the client's CSR and returned in `Adopt.identity_cert_pem`. On
    /// every reconnect this leaf (with `private_key_pem`) is presented as
    /// the TLS client certificate — it *is* the reconnect credential, so
    /// the reconnect `Hello.credential` is empty.
    pub identity_cert_pem: String,
    /// PEM-encoded PKCS#8 private key for the enrollment keypair. Kept
    /// local (never sent); pairs with `identity_cert_pem` for mTLS.
    pub private_key_pem: String,
    /// PEM-encoded SPKI public key. Echoes back in `AdoptAck` so the
    /// server can pin it.
    pub public_key_pem: String,
    /// PEM-encoded issuing-CA chain returned in `Adopt.ca_bundle_pem`.
    /// The client pins this to verify the control plane on mTLS
    /// reconnects. Empty when the server did not supply one (the client
    /// then falls back to public roots). Defaulted so identity files
    /// written before this field existed still load.
    #[serde(default)]
    pub ca_bundle_pem: String,
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
            .map_or(0, |d| d.as_secs());
        // Treat the cert as expired *at* `not_after_unix`, not only strictly
        // after it: the field is defined as the timestamp after which the
        // server no longer accepts the credential, so the boundary second
        // should already be considered past the NotAfter limit.
        now >= self.not_after_unix
    }
}

/// On-disk identity store rooted at a single JSON file.
#[derive(Debug, Clone)]
pub struct IdentityStore;

impl IdentityStore {
    /// Read an identity file, returning `Ok(None)` if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be read (for any reason
    /// other than not-found) or its contents fail to parse as an [`Identity`].
    pub fn load_optional(path: &Path) -> Result<Option<Identity>> {
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
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be removed.
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

    /// Async variant of [`IdentityStore::clear`] for use on the Tokio driver
    /// task, where blocking on synchronous `std::fs` I/O would stall a worker
    /// thread. Same semantics: no-op if the file doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the file exists but cannot be removed.
    pub(crate) async fn clear_async(path: &Path) -> Result<()> {
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(Error::Io {
                path: path.to_path_buf(),
                source: err,
            }),
        }
    }

    /// Generate fresh enrollment material: an ECDSA P-256 keypair and a
    /// PKCS#10 CSR for it, all PEM-encoded. Called at startup *before*
    /// the first `Hello`, so the client can prove possession of its key
    /// (the CSR) before the control plane issues the leaf certificate.
    ///
    /// The CSR carries a stable common name and a `clientAuth` extended
    /// key usage so the issued leaf is directly usable as an mTLS client
    /// certificate.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Enrollment`] if key generation or CSR
    /// serialization fails.
    pub(crate) fn generate_enrollment() -> Result<EnrollmentMaterial> {
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

        Ok(EnrollmentMaterial {
            private_key_pem,
            public_key_pem,
            csr_pem,
        })
    }
}

/// Freshly-generated enrollment material returned by
/// [`IdentityStore::generate_enrollment`]: the client keypair (PEM) plus a
/// PKCS#10 CSR built from it. The private key is retained locally and, on
/// successful adoption, persisted into the [`Identity`] alongside the
/// signed leaf; the CSR and public key are sent in the enrollment `Hello`.
#[derive(Debug, Clone)]
pub struct EnrollmentMaterial {
    pub(crate) private_key_pem: String,
    pub(crate) public_key_pem: String,
    pub(crate) csr_pem: String,
}

#[cfg(unix)]
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    use std::os::unix::fs::PermissionsExt as _;

    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("identity.json");
    let tmp_path = dir.join(format!(".{file_name}.tmp"));

    // `OpenOptions::mode` only applies to *newly created* files. If a stale
    // `.<file>.tmp` from a previous crashed run still exists with broader
    // permissions, `create(true).truncate(true)` would reuse it and then
    // `rename` would publish the private key under those wider permissions.
    // Defend against that by removing any stale temp first, refusing to
    // reuse an existing inode (`create_new`), and explicitly enforcing
    // `0o600` on the opened file before writing the sensitive bytes.
    if let Err(err) = std::fs::remove_file(&tmp_path)
        && err.kind() != std::io::ErrorKind::NotFound
    {
        return Err(err).context(IoSnafu { path: tmp_path });
    }

    {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&tmp_path)
            .context(IoSnafu {
                path: tmp_path.clone(),
            })?;
        // Re-assert mode in case umask/file-creation flags interfered.
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
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
    promote_temp(&tmp_path, path)
}

/// Promote a freshly-written temp file into its final location on non-Unix
/// platforms, where `std::fs::rename` does **not** atomically replace an
/// existing destination (it errors if the target already exists). A rotated
/// or re-adopted identity must be able to overwrite an existing
/// `identity.json`, so when the plain rename fails we move the existing file
/// to a backup, retry the rename, and roll the backup back if the retry
/// fails. The backup is removed on success.
#[cfg(not(unix))]
fn promote_temp(tmp_path: &Path, path: &Path) -> Result<()> {
    if let Err(err) = std::fs::rename(tmp_path, path) {
        // The most likely cause on non-Unix is that `path` already exists.
        // If the destination is genuinely absent, surface the original error.
        if !path.exists() {
            return Err(err).context(IoSnafu {
                path: path.to_path_buf(),
            });
        }

        let backup_path = path.with_extension("bak");
        // Clear any stale backup so the rename below can't fail on a leftover.
        if let Err(rm_err) = std::fs::remove_file(&backup_path)
            && rm_err.kind() != std::io::ErrorKind::NotFound
        {
            return Err(rm_err).context(IoSnafu { path: backup_path });
        }
        std::fs::rename(path, &backup_path).context(IoSnafu {
            path: backup_path.clone(),
        })?;
        match std::fs::rename(tmp_path, path) {
            Ok(()) => {
                // Promotion succeeded; drop the backup (best-effort).
                let _ = std::fs::remove_file(&backup_path);
            }
            Err(promote_err) => {
                // Roll the original file back into place so we don't leave the
                // store without an identity, then report the failure.
                let _ = std::fs::rename(&backup_path, path);
                return Err(promote_err).context(IoSnafu {
                    path: path.to_path_buf(),
                });
            }
        }
    }
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
            ca_bundle_pem: "-----BEGIN CERTIFICATE-----\nMOCKCA\n-----END CERTIFICATE-----\n"
                .to_string(),
            not_after_unix: 0,
        }
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
    }

    #[test]
    fn load_tolerates_identity_without_ca_bundle() {
        // Identity files written before `ca_bundle_pem` existed must still
        // load (the field is `#[serde(default)]`).
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
    }

    #[test]
    fn load_optional_returns_none_when_missing() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let path = dir.path().join("does-not-exist.json");
        let loaded = IdentityStore::load_optional(&path).expect("load");
        assert!(loaded.is_none());
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

    #[test]
    fn is_expired_treats_boundary_second_as_expired() {
        // A cert whose `not_after_unix` equals the current second is past the
        // NotAfter boundary and must be considered expired.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .expect("system clock after unix epoch");
        let mut identity = sample_identity();
        identity.not_after_unix = now;
        assert!(identity.is_expired());
    }

    #[test]
    fn is_expired_accepts_future_timestamp() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .expect("system clock after unix epoch");
        let mut identity = sample_identity();
        identity.not_after_unix = now + 3600;
        assert!(!identity.is_expired());
    }
}
