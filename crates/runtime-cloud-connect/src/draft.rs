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

//! The persisted enrollment draft: what makes enrollment retry-safe.
//!
//! `<config-dir>/enrollment-draft.json` (owner-only, `0600` on Unix) holds
//! the provisional identity keypair, CSR, encryption keypair, the enrollment
//! operation ID that rides every attempt as `Idempotency-Key`, and the
//! non-secret request facts that must remain byte-for-byte stable on replay.
//! It **never** contains the enrollment key — the key is single-use bearer
//! material whose lifetime is one process's argument, while the draft is
//! what lets a *lost response* be replayed safely: the cloud stores the
//! operation with the request's canonical fingerprint/public-key hash, so
//! re-presenting the same operation and material returns (or reissues) the
//! same instance identity instead of creating a sibling.
//!
//! Lifecycle:
//! - Created (and persisted) before the first enrollment request.
//! - First publication is serialized by a persistent advisory lock file. The
//!   operating system releases lock ownership if a creator exits, so a later
//!   process can recover without replacing an in-progress operation.
//! - Reused verbatim by every retry — including a later process presenting
//!   a **new** key after the first one expired mid-retry, which the cloud
//!   consumes against the existing operation rather than a new instance.
//! - On success, atomically promoted into `identity.json` and deleted.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

use crate::enroll::InstanceFacts;
use crate::identity::{EnrollmentMaterial, IdentityStore};

/// File name (relative to `$SPICE_CONFIG_DIR`) of the enrollment draft.
pub const ENROLLMENT_DRAFT_FILE: &str = "enrollment-draft.json";

/// The stable advisory lock file that serializes first publication.
///
/// This inode is deliberately persistent. Unlinking an advisory lock file can
/// let a new process lock a replacement inode while another process still
/// owns the original, defeating serialization.
const ENROLLMENT_DRAFT_LOCK_FILE: &str = ".enrollment-draft.lock";

#[cfg(not(test))]
const LOCK_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);
#[cfg(test)]
const LOCK_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_millis(250);
const LOCK_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

/// Current schema of the draft file.
const DRAFT_SCHEMA_VERSION: u32 = 2;

/// Errors reading, writing, or generating an enrollment draft.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Enrollment draft I/O error at {}: {source}", path.display()))]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to serialize the enrollment draft: {source}"))]
    Serialize { source: serde_json::Error },

    #[snafu(display("Failed to parse the enrollment draft at {}: {source}", path.display()))]
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display(
        "The enrollment draft at {} uses unsupported schema {found} (this runtime requires \
         schema {DRAFT_SCHEMA_VERSION}). Upgrade spiced, or remove the file to start a fresh \
         enrollment. See: https://spiceai.org/docs",
        path.display()
    ))]
    UnsupportedSchema { path: PathBuf, found: u32 },

    #[snafu(display("Failed to generate enrollment key material: {source}"))]
    Material { source: crate::identity::Error },

    #[snafu(display(
        "Another live process is still publishing the enrollment draft protected by {}. Wait for that process to finish and retry",
        path.display()
    ))]
    CreationInProgress { path: PathBuf },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The provisional, retry-stable state of one enrollment operation.
///
/// Contains private key material — treat like `identity.json`. Never
/// contains the enrollment key.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrollmentDraft {
    /// Schema of this file, for forward compatibility.
    pub schema_version: u32,
    /// The operation this enrollment runs under, sent as `Idempotency-Key`
    /// on every attempt. Stable across retries and process restarts, which
    /// is what makes a replay return the same instance.
    pub enrollment_operation_id: String,
    /// PEM PKCS#8 ECDSA P-256 identity private key (provisional until
    /// promotion).
    pub private_key_pem: String,
    /// PEM SPKI public key for `private_key_pem`.
    pub public_key_pem: String,
    /// PKCS#10 CSR built from the identity keypair; sent as `csr_pem`.
    pub csr_pem: String,
    /// PEM PKCS#8 X25519 encryption private key (provisional until
    /// promotion).
    pub enc_private_key_pem: String,
    /// PEM SPKI (RFC 8410) X25519 public key; sent as `enc_pubkey_pem`.
    pub enc_public_key_pem: String,
    /// Non-secret host facts sent on the first request. Persisted so a
    /// container replacement or runtime upgrade replays the exact request
    /// instead of recomputing a different hostname, fingerprint, or version.
    pub instance: InstanceFacts,
    /// Operator-declared location label sent on the first request. A later
    /// process reuses this value even if its command-line configuration
    /// changed while the operation was pending.
    pub region: Option<String>,
}

impl std::fmt::Debug for EnrollmentDraft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnrollmentDraft")
            .field("schema_version", &self.schema_version)
            .field("enrollment_operation_id", &self.enrollment_operation_id)
            .field("private_key_pem", &"[REDACTED]")
            .field("public_key_pem", &"[PUBLIC KEY]")
            .field("csr_pem", &"[CERTIFICATE REQUEST]")
            .field("enc_private_key_pem", &"[REDACTED]")
            .field("enc_public_key_pem", &"[PUBLIC KEY]")
            .field("instance", &self.instance)
            .field("region", &self.region)
            .finish()
    }
}

impl EnrollmentDraft {
    /// The draft path inside `config_dir`.
    #[must_use]
    pub fn path_in(config_dir: &Path) -> PathBuf {
        config_dir.join(ENROLLMENT_DRAFT_FILE)
    }

    fn lock_path(path: &Path) -> PathBuf {
        path.with_file_name(ENROLLMENT_DRAFT_LOCK_FILE)
    }

    /// Load the persisted draft for `config_dir`, or create, persist, and
    /// return a fresh one when none exists.
    ///
    /// An unreadable or unparseable draft is an error. It may represent an
    /// operation whose response was lost after the cloud created an instance;
    /// replacing it would lose the only safe replay key and could create a
    /// sibling instance.
    ///
    /// # Errors
    ///
    /// Returns an error when an existing file cannot be read or parsed, when
    /// its schema is newer than this runtime understands, when key material
    /// cannot be generated, or when a fresh draft cannot be persisted.
    pub fn load_or_create(
        config_dir: &Path,
        instance: &InstanceFacts,
        region: Option<&str>,
    ) -> Result<Self> {
        let path = Self::path_in(config_dir);
        match std::fs::read_to_string(&path) {
            Ok(contents) => Self::load_published(&path, &contents),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                Self::create_at(&path, instance, region)
            }
            Err(source) => Err(Error::Io { path, source }),
        }
    }

    fn parse_at(path: &Path, contents: &str) -> Result<Self> {
        let draft = serde_json::from_str::<Self>(contents).context(ParseSnafu {
            path: path.to_path_buf(),
        })?;
        if draft.schema_version != DRAFT_SCHEMA_VERSION {
            return Err(Error::UnsupportedSchema {
                path: path.to_path_buf(),
                found: draft.schema_version,
            });
        }
        Ok(draft)
    }

    fn load_published(path: &Path, contents: &str) -> Result<Self> {
        Self::parse_at(path, contents)
    }

    /// Generate a fresh draft and persist it at `path` before returning it,
    /// so the operation ID is durable before the first request that uses it.
    fn create_at(path: &Path, instance: &InstanceFacts, region: Option<&str>) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context(IoSnafu {
                path: parent.to_path_buf(),
            })?;
        }

        let lock_path = Self::lock_path(path);
        let publication_lock = Self::acquire_publication_lock(&lock_path)?;
        Self::publish_locked(path, &publication_lock, instance, region)
    }

    fn acquire_publication_lock(lock_path: &Path) -> Result<std::fs::File> {
        #[cfg(unix)]
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

        let mut options = std::fs::OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options.open(lock_path).context(IoSnafu {
            path: lock_path.to_path_buf(),
        })?;
        #[cfg(unix)]
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .context(IoSnafu {
                path: lock_path.to_path_buf(),
            })?;
        file.sync_all().context(IoSnafu {
            path: lock_path.to_path_buf(),
        })?;
        crate::identity::sync_parent_directory(lock_path).context(IoSnafu {
            path: lock_path.to_path_buf(),
        })?;

        let deadline = std::time::Instant::now() + LOCK_WAIT_BUDGET;
        loop {
            if fs4::fs_std::FileExt::try_lock_exclusive(&file).context(IoSnafu {
                path: lock_path.to_path_buf(),
            })? {
                return Ok(file);
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::CreationInProgress {
                    path: lock_path.to_path_buf(),
                });
            }
            std::thread::sleep(LOCK_POLL_INTERVAL);
        }
    }

    fn publish_locked(
        path: &Path,
        _publication_lock: &std::fs::File,
        instance: &InstanceFacts,
        region: Option<&str>,
    ) -> Result<Self> {
        // `load_or_create` observed NotFound before entering `create_at`, but
        // another process may have published before this process acquired the
        // lock. Re-read while locked so a delayed creator cannot replace the
        // durable winner through the atomic rename below.
        match std::fs::read_to_string(path) {
            Ok(contents) => return Self::parse_at(path, &contents),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(Error::Io {
                    path: path.to_path_buf(),
                    source,
                });
            }
        }

        let material = IdentityStore::generate_enrollment().context(MaterialSnafu)?;
        let draft = Self {
            schema_version: DRAFT_SCHEMA_VERSION,
            enrollment_operation_id: uuid::Uuid::new_v4().to_string(),
            private_key_pem: material.private_key_pem,
            public_key_pem: material.public_key_pem,
            csr_pem: material.csr_pem,
            enc_private_key_pem: material.enc_private_key_pem,
            enc_public_key_pem: material.enc_public_key_pem,
            instance: instance.clone(),
            region: region.map(str::to_string),
        };
        let bytes = serde_json::to_vec_pretty(&draft).context(SerializeSnafu)?;
        crate::identity::atomic_write_owner_only(path, &bytes).context(IoSnafu {
            path: path.to_path_buf(),
        })?;
        Ok(draft)
    }

    /// Remove the draft for `config_dir`. A missing file is success.
    ///
    /// # Errors
    ///
    /// Returns an error when the file exists but cannot be removed.
    pub fn delete(config_dir: &Path) -> Result<()> {
        let path = Self::path_in(config_dir);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                crate::identity::sync_parent_directory(&path)
                    .context(IoSnafu { path: path.clone() })?;
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(Error::Io { path, source }),
        }
        Ok(())
    }

    /// This draft's key material, in the shape the enroll request and
    /// identity promotion consume.
    #[must_use]
    pub fn material(&self) -> EnrollmentMaterial {
        EnrollmentMaterial {
            private_key_pem: self.private_key_pem.clone(),
            public_key_pem: self.public_key_pem.clone(),
            csr_pem: self.csr_pem.clone(),
            enc_private_key_pem: self.enc_private_key_pem.clone(),
            enc_public_key_pem: self.enc_public_key_pem.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_instance(runtime_version: &str) -> InstanceFacts {
        InstanceFacts {
            fingerprint: "f".repeat(64),
            hostname: "draft-test".to_string(),
            os: "linux".to_string(),
            arch: "x86_64".to_string(),
            runtime_version: runtime_version.to_string(),
        }
    }

    fn load_or_create(config_dir: &Path) -> Result<EnrollmentDraft> {
        EnrollmentDraft::load_or_create(
            config_dir,
            &test_instance("v2.2.0-test"),
            Some("us-west-2"),
        )
    }

    #[test]
    fn creates_and_persists_a_fresh_draft() {
        let dir = tempfile::tempdir().expect("tempdir");
        let draft = load_or_create(dir.path()).expect("create");

        assert_eq!(draft.schema_version, DRAFT_SCHEMA_VERSION);
        assert!(!draft.enrollment_operation_id.is_empty());
        assert!(draft.csr_pem.contains("CERTIFICATE REQUEST"));
        assert_eq!(draft.instance.runtime_version, "v2.2.0-test");
        assert_eq!(draft.region.as_deref(), Some("us-west-2"));
        assert!(EnrollmentDraft::path_in(dir.path()).exists());
    }

    #[test]
    fn reloading_returns_the_same_operation_and_material() {
        // The whole point of the draft: a retry (or a new process presenting
        // a new key) must reuse the operation ID and keypair, so the cloud
        // can replay the operation instead of creating a sibling instance.
        let dir = tempfile::tempdir().expect("tempdir");
        let first = load_or_create(dir.path()).expect("create");
        let second = EnrollmentDraft::load_or_create(
            dir.path(),
            &test_instance("v9.9.9-replacement"),
            Some("eu-west-1"),
        )
        .expect("reload");

        assert_eq!(
            first.enrollment_operation_id,
            second.enrollment_operation_id
        );
        assert_eq!(first.public_key_pem, second.public_key_pem);
        assert_eq!(first.csr_pem, second.csr_pem);
        assert_eq!(first.instance, second.instance);
        assert_eq!(first.region, second.region);
    }

    #[test]
    fn distinct_directories_get_distinct_operations() {
        let a = tempfile::tempdir().expect("tempdir");
        let b = tempfile::tempdir().expect("tempdir");
        let draft_a = load_or_create(a.path()).expect("a");
        let draft_b = load_or_create(b.path()).expect("b");
        assert_ne!(
            draft_a.enrollment_operation_id,
            draft_b.enrollment_operation_id
        );
    }

    #[test]
    fn a_corrupt_draft_is_never_replaced() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = EnrollmentDraft::path_in(dir.path());
        std::fs::create_dir_all(dir.path()).expect("mkdir");
        std::fs::write(&path, "{not json").expect("write corrupt draft");

        let err = load_or_create(dir.path()).expect_err("must refuse");
        assert!(matches!(err, Error::Parse { .. }), "{err}");
        assert_eq!(
            std::fs::read_to_string(path).expect("corrupt draft remains"),
            "{not json",
            "ambiguous enrollment state must not be silently replaced"
        );
    }

    #[test]
    fn concurrent_creation_publishes_one_operation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let barrier = std::sync::Barrier::new(8);
        let drafts = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        load_or_create(dir.path()).expect("create or load")
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("creator thread"))
                .collect::<Vec<_>>()
        });

        let winner = &drafts[0];
        for draft in &drafts[1..] {
            assert_eq!(
                draft.enrollment_operation_id,
                winner.enrollment_operation_id
            );
            assert_eq!(draft.public_key_pem, winner.public_key_pem);
            assert_eq!(draft.enc_public_key_pem, winner.enc_public_key_pem);
            assert_eq!(draft.instance, winner.instance);
            assert_eq!(draft.region, winner.region);
        }
        let files = std::fs::read_dir(dir.path())
            .expect("read config dir")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("read entries");
        let mut file_names = files
            .into_iter()
            .map(|entry| entry.file_name())
            .collect::<Vec<_>>();
        file_names.sort_unstable();
        assert_eq!(
            file_names,
            vec![ENROLLMENT_DRAFT_LOCK_FILE, ENROLLMENT_DRAFT_FILE],
            "only the durable draft and stable publication lock should remain"
        );
    }

    #[test]
    fn a_delayed_creator_cannot_replace_a_published_operation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = EnrollmentDraft::path_in(dir.path());
        let winner = load_or_create(dir.path()).expect("publish winner");

        // `create_at` is the continuation after an earlier NotFound read.
        // Calling it after publication deterministically reproduces a creator
        // that was descheduled between that read and claim acquisition.
        let delayed = EnrollmentDraft::create_at(
            &path,
            &test_instance("v9.9.9-delayed"),
            Some("eu-central-1"),
        )
        .expect("delayed creator loads the winner");

        assert_eq!(
            delayed.enrollment_operation_id,
            winner.enrollment_operation_id
        );
        assert_eq!(delayed.instance, winner.instance);
        assert_eq!(delayed.region, winner.region);
        let persisted = load_or_create(dir.path()).expect("reload winner");
        assert_eq!(
            persisted.enrollment_operation_id,
            winner.enrollment_operation_id
        );
    }

    #[test]
    fn a_published_draft_reloads_with_a_persistent_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let winner = load_or_create(dir.path()).expect("create draft");
        let path = EnrollmentDraft::path_in(dir.path());
        let lock_path = EnrollmentDraft::lock_path(&path);

        let loaded = load_or_create(dir.path()).expect("load published draft");
        assert_eq!(
            loaded.enrollment_operation_id,
            winner.enrollment_operation_id
        );
        assert!(
            lock_path.exists(),
            "the stable lock inode must remain for later creators"
        );
    }

    #[test]
    fn an_abandoned_publication_lock_is_reclaimed_automatically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = EnrollmentDraft::path_in(dir.path());
        let lock_path = EnrollmentDraft::lock_path(&path);
        let abandoned =
            EnrollmentDraft::acquire_publication_lock(&lock_path).expect("acquire lock");
        drop(abandoned);

        let draft = load_or_create(dir.path()).expect("reclaim abandoned lock");
        assert!(!draft.enrollment_operation_id.is_empty());
        assert!(path.exists(), "the recovered creator publishes the draft");
        assert!(lock_path.exists(), "the stable lock inode remains in place");
    }

    #[test]
    fn a_live_publication_lock_serializes_creators() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = EnrollmentDraft::path_in(dir.path());
        let lock_path = EnrollmentDraft::lock_path(&path);
        let active = EnrollmentDraft::acquire_publication_lock(&lock_path).expect("acquire lock");

        let error = load_or_create(dir.path()).expect_err("live creator owns publication");
        assert!(matches!(error, Error::CreationInProgress { .. }), "{error}");
        assert!(error.to_string().contains("Another live process"));
        assert!(
            !path.exists(),
            "a contender must not publish a new operation"
        );

        drop(active);
        load_or_create(dir.path()).expect("publish after active creator exits");
    }

    #[test]
    fn a_newer_schema_is_an_error_not_a_silent_reset() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = EnrollmentDraft::path_in(dir.path());
        let newer = serde_json::json!({
            "schema_version": DRAFT_SCHEMA_VERSION + 1,
            "enrollment_operation_id": "op-from-the-future",
            "private_key_pem": "k",
            "public_key_pem": "p",
            "csr_pem": "c",
            "enc_private_key_pem": "ek",
            "enc_public_key_pem": "ep",
            "instance": test_instance("v3.0.0"),
            "region": null,
        });
        std::fs::write(&path, newer.to_string()).expect("write newer draft");

        let err = load_or_create(dir.path()).expect_err("must refuse");
        assert!(matches!(err, Error::UnsupportedSchema { .. }), "{err}");
    }

    #[test]
    fn delete_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        load_or_create(dir.path()).expect("create");
        assert!(EnrollmentDraft::path_in(dir.path()).exists());
        EnrollmentDraft::delete(dir.path()).expect("delete");
        assert!(!EnrollmentDraft::path_in(dir.path()).exists());
        assert!(
            EnrollmentDraft::lock_path(&EnrollmentDraft::path_in(dir.path())).exists(),
            "delete retains the stable lock inode"
        );
        EnrollmentDraft::delete(dir.path()).expect("second delete is a no-op");
        load_or_create(dir.path()).expect("the persistent lock permits re-enrollment");
    }

    #[cfg(unix)]
    #[test]
    fn the_draft_is_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        load_or_create(dir.path()).expect("create");
        let mode = std::fs::metadata(EnrollmentDraft::path_in(dir.path()))
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "the draft holds private key material");
    }

    #[cfg(unix)]
    #[test]
    fn the_publication_lock_is_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = EnrollmentDraft::path_in(dir.path());
        let lock_path = EnrollmentDraft::lock_path(&path);
        EnrollmentDraft::acquire_publication_lock(&lock_path).expect("acquire lock");
        let mode = std::fs::metadata(lock_path)
            .expect("lock metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn the_draft_never_contains_an_enrollment_key() {
        // Structural guarantee: serialize a draft and prove no field can
        // carry the key. The field list is the contract — a `token` field
        // appearing here must fail this test.
        let dir = tempfile::tempdir().expect("tempdir");
        load_or_create(dir.path()).expect("create");
        let raw = std::fs::read_to_string(EnrollmentDraft::path_in(dir.path())).expect("read");
        let value: serde_json::Value = serde_json::from_str(&raw).expect("valid json");
        let object = value.as_object().expect("draft is an object");
        let mut fields: Vec<&str> = object.keys().map(String::as_str).collect();
        fields.sort_unstable();
        assert_eq!(
            fields,
            vec![
                "csr_pem",
                "enc_private_key_pem",
                "enc_public_key_pem",
                "enrollment_operation_id",
                "instance",
                "private_key_pem",
                "public_key_pem",
                "region",
                "schema_version",
            ],
            "the draft's field list changed; verify no field can carry the enrollment key"
        );
        assert!(!raw.contains("spice-enroll-"));
    }

    #[test]
    fn debug_redacts_private_key_material() {
        let dir = tempfile::tempdir().expect("tempdir");
        let draft = load_or_create(dir.path()).expect("create");
        let private_key = draft.private_key_pem.clone();
        let enc_private_key = draft.enc_private_key_pem.clone();

        let debug = format!("{draft:?}");
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
}
