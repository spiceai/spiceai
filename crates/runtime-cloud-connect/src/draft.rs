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
//! - The full identity-check, request, promotion, and cleanup transaction is
//!   serialized by a persistent advisory lock file. The operating system
//!   releases lock ownership if an enrolling process exits, so a later process
//!   can recover without overlapping or replacing an in-progress operation.
//! - Reused verbatim by every retry — including a later process presenting
//!   a **new** key after the first one expired mid-retry, which the cloud
//!   consumes against the existing operation rather than a new instance.
//! - On success, atomically promoted into `identity.json` and deleted.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

use crate::enroll::InstanceFacts;
use crate::identity::{EnrollmentMaterial, IdentityStore};

fn same_directory(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

/// File name (relative to `$SPICE_CONFIG_DIR`) of the enrollment draft.
pub const ENROLLMENT_DRAFT_FILE: &str = "enrollment-draft.json";

/// The stable advisory lock file that serializes enrollment for one directory.
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
#[cfg(not(test))]
const REMOVAL_LOCK_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);
#[cfg(test)]
const REMOVAL_LOCK_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_DRAFT_BYTES: u64 = 1024 * 1024;

/// Current schema of the draft file.
const DRAFT_SCHEMA_VERSION: u32 = 3;

/// Non-secret enrollment authority facts for one operation. The actual
/// bearer/enrollment key is deliberately absent. Token assertions may change
/// during recovery; an authenticated operation remains bound to its org.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EnrollmentAuthorityBinding {
    Token { expected_org: Option<String> },
    AuthenticatedSession { organization: String },
}

/// Control-plane and authority provenance for one enrollment operation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EnrollmentRequestBinding {
    pub endpoint: String,
    pub authority: EnrollmentAuthorityBinding,
}

#[derive(Deserialize)]
struct DraftSchemaHeader {
    schema_version: u32,
}

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

    #[snafu(display(
        "Failed to parse the enrollment draft at {} (line {}, column {})",
        path.display(),
        source.line(),
        source.column()
    ))]
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display(
        "The enrollment draft at {} cannot be replayed safely: {reason}. Preserve the draft and contact Spice Cloud support before removing it or starting a new enrollment",
        path.display()
    ))]
    Invalid { path: PathBuf, reason: &'static str },

    #[snafu(display(
        "The enrollment draft at {} uses unsupported schema {found} (this runtime requires \
         schema {DRAFT_SCHEMA_VERSION}). Upgrade spiced to a version that supports this draft, \
         or contact Spice Cloud support to recover the pending enrollment. Do not delete the \
         draft because it may identify an enrollment already committed by Spice Cloud. \
         See: https://spiceai.org/docs",
        path.display()
    ))]
    UnsupportedSchema { path: PathBuf, found: u32 },

    #[snafu(display("Failed to generate enrollment key material: {source}"))]
    Material { source: crate::identity::Error },

    #[snafu(display(
        "The pending enrollment at {} belongs to a different control plane or authority. Retry with the original endpoint and organization; the exact-replay state was preserved",
        path.display()
    ))]
    RequestBindingMismatch { path: PathBuf },

    #[snafu(display(
        "Another live process is still enrolling this config directory under the lock at {}. Wait for that process to finish and retry",
        path.display()
    ))]
    CreationInProgress { path: PathBuf },

    #[snafu(display("Failed to remove the enrollment draft: {source}"))]
    DeleteTaskPanicked { source: tokio::task::JoinError },

    #[snafu(display("Failed to acquire the enrollment transaction: {source}"))]
    AcquireTaskPanicked { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Exclusive ownership of one config directory's enrollment transaction.
///
/// The file handle must remain alive from the first identity read through
/// draft cleanup. Releasing it earlier permits another process to observe no
/// identity after promotion has started and race a new operation against the
/// first process.
pub struct EnrollmentTransactionLock {
    config_dir: PathBuf,
    draft_path: PathBuf,
    file: std::fs::File,
    #[cfg(unix)]
    directory: std::fs::File,
}

impl EnrollmentTransactionLock {
    pub(crate) fn acquire(config_dir: &Path) -> Result<Self> {
        Self::acquire_with_budget(config_dir, LOCK_WAIT_BUDGET)
    }

    pub(crate) fn try_acquire(config_dir: &Path) -> Result<Self> {
        Self::acquire_with_budget(config_dir, std::time::Duration::ZERO)
    }

    pub(crate) fn acquire_for_removal(config_dir: &Path) -> Result<Self> {
        Self::acquire_with_budget(config_dir, REMOVAL_LOCK_WAIT_BUDGET)
    }

    fn acquire_with_budget(config_dir: &Path, wait_budget: std::time::Duration) -> Result<Self> {
        std::fs::create_dir_all(config_dir).context(IoSnafu {
            path: config_dir.to_path_buf(),
        })?;
        // Pin every later draft and identity operation to the directory named
        // at acquisition time. Retargeting a symlink ancestor after this point
        // must not redirect state while the lock stays on the original inode.
        let config_dir = std::fs::canonicalize(config_dir).context(IoSnafu {
            path: config_dir.to_path_buf(),
        })?;
        if !config_dir.is_dir() {
            return Err(Error::Io {
                path: config_dir,
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "the Cloud Connect config path must resolve to a directory",
                ),
            });
        }
        #[cfg(unix)]
        let directory = std::fs::File::open(&config_dir).context(IoSnafu {
            path: config_dir.clone(),
        })?;
        let draft_path = EnrollmentDraft::path_in(&config_dir);
        let lock_path = EnrollmentDraft::lock_path(&draft_path);
        let file = EnrollmentDraft::acquire_publication_lock_with_budget(&lock_path, wait_budget)?;
        let transaction = Self {
            config_dir,
            draft_path,
            file,
            #[cfg(unix)]
            directory,
        };
        transaction.ensure_directory_stable()?;
        Ok(transaction)
    }

    /// The canonical directory this transaction owns.
    pub(crate) fn config_dir(&self) -> Result<&Path> {
        self.ensure_directory_stable()?;
        Ok(&self.config_dir)
    }

    /// Verify that the canonical path still resolves to the directory inode
    /// retained for this transaction.
    pub(crate) fn ensure_directory_stable(&self) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;

            let retained = self.directory.metadata().context(IoSnafu {
                path: self.config_dir.clone(),
            })?;
            let named = std::fs::metadata(&self.config_dir).context(IoSnafu {
                path: self.config_dir.clone(),
            })?;
            let final_entry = std::fs::symlink_metadata(&self.config_dir).context(IoSnafu {
                path: self.config_dir.clone(),
            })?;
            if final_entry.file_type().is_symlink()
                || retained.dev() != named.dev()
                || retained.ino() != named.ino()
            {
                return Err(Error::Io {
                    path: self.config_dir.clone(),
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "the enrollment transaction directory was renamed or replaced",
                    ),
                });
            }
        }
        Ok(())
    }

    /// Try once to acquire exclusive ownership of a config directory's
    /// enrollment transaction without blocking a Tokio worker thread.
    ///
    /// This is the removal boundary: callers must acquire it before inspecting
    /// identity, draft, cache, or endpoint state and retain it until all those
    /// files have been removed.
    ///
    /// # Errors
    ///
    /// Returns an error if another process owns the transaction, the lock file
    /// cannot be opened, or the blocking task panics.
    pub async fn try_acquire_async(config_dir: &Path) -> Result<Self> {
        let config_dir = config_dir.to_path_buf();
        tokio::task::spawn_blocking(move || Self::try_acquire(&config_dir))
            .await
            .map_err(|source| Error::AcquireTaskPanicked { source })?
    }

    pub(crate) fn protects(&self, path: &Path) -> bool {
        path.parent()
            .is_some_and(|wanted| same_directory(&self.config_dir, wanted))
    }

    /// Resolve a state path into the directory this transaction pinned.
    /// Returns `None` when the caller's spelling no longer names that directory,
    /// which is the fail-closed result after an ancestor substitution.
    pub(crate) fn protected_path(&self, path: &Path) -> Option<PathBuf> {
        self.ensure_directory_stable().ok()?;
        let file_name = path.file_name()?;
        self.protects(path).then(|| self.config_dir.join(file_name))
    }

    /// Acquire exclusive ownership of a config directory's enrollment
    /// transaction without blocking a Tokio worker thread.
    ///
    /// Callers that need to publish additional retry state beside the
    /// enrollment draft retain this guard and pass it to
    /// `enroll_now_with_transaction`, closing the gap between the two durable
    /// writes.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be acquired or the blocking
    /// task panics.
    pub async fn acquire_async(config_dir: &Path) -> Result<Self> {
        let config_dir = config_dir.to_path_buf();
        tokio::task::spawn_blocking(move || Self::acquire(&config_dir))
            .await
            .map_err(|source| Error::AcquireTaskPanicked { source })?
    }

    /// Load the published draft, or durably create it while this transaction
    /// owns the directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the draft cannot be read, validated, or published.
    pub fn load_or_create(
        &self,
        instance: &InstanceFacts,
        region: Option<&str>,
        binding: &EnrollmentRequestBinding,
    ) -> Result<EnrollmentDraft> {
        self.ensure_directory_stable()?;
        let result = match read_bounded_regular_file(&self.draft_path, MAX_DRAFT_BYTES) {
            Ok(contents) => {
                let draft = EnrollmentDraft::load_published(&self.draft_path, &contents)?;
                draft.validate_request(&self.draft_path, binding)?;
                Ok(draft)
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                EnrollmentDraft::publish_locked(
                    &self.draft_path,
                    &self.file,
                    instance,
                    region,
                    binding,
                )
            }
            Err(source) => Err(Error::Io {
                path: self.draft_path.clone(),
                source,
            }),
        };
        self.ensure_directory_stable()?;
        result
    }

    pub(crate) fn delete(&self) -> Result<()> {
        self.ensure_directory_stable()?;
        let result = EnrollmentDraft::delete_at(&self.draft_path);
        self.ensure_directory_stable()?;
        result
    }

    /// Delete the enrollment draft while retaining this transaction lock.
    ///
    /// # Errors
    ///
    /// Returns an error if the draft cannot be removed or the blocking task
    /// panics.
    pub async fn delete_draft_async(self: &Arc<Self>) -> Result<()> {
        let transaction = Arc::clone(self);
        tokio::task::spawn_blocking(move || transaction.delete())
            .await
            .map_err(|source| Error::DeleteTaskPanicked { source })?
    }
}

/// Open the config directory one descriptor at a time, then create/open the
/// persistent transaction lock relative to that pinned directory. Neither an
/// ancestor symlink substitution nor a final-name symlink can redirect a
/// privileged enrollment between validation and open.
#[cfg(unix)]
fn open_unix_publication_lock(lock_path: &Path) -> std::io::Result<(std::fs::File, std::fs::File)> {
    use std::ffi::CString;
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    use std::os::unix::ffi::OsStrExt as _;

    fn open_directory_at(
        parent: &std::fs::File,
        name: &std::ffi::OsStr,
    ) -> std::io::Result<std::fs::File> {
        let name = CString::new(name.as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cloud Connect config paths cannot contain NUL bytes",
            )
        })?;
        let descriptor = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if descriptor < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(unsafe { std::fs::File::from_raw_fd(descriptor) })
    }

    let parent = lock_path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "the enrollment transaction lock has no config directory",
        )
    })?;
    let parent = std::fs::canonicalize(parent)?;
    let root = c"/";
    let root_descriptor = unsafe {
        libc::open(
            root.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if root_descriptor < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut directory = unsafe { std::fs::File::from_raw_fd(root_descriptor) };
    for component in parent.components() {
        match component {
            std::path::Component::RootDir | std::path::Component::CurDir => {}
            std::path::Component::Normal(name) => {
                directory = open_directory_at(&directory, name)?;
            }
            std::path::Component::ParentDir | std::path::Component::Prefix(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "the canonical Cloud Connect config path was not absolute",
                ));
            }
        }
    }

    let name = lock_path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "the enrollment transaction lock has no file name",
        )
    })?;
    let name = CString::new(name.as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "the enrollment transaction lock name contains a NUL byte",
        )
    })?;
    let lock_descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDWR | libc::O_CREAT | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            0o600,
        )
    };
    if lock_descriptor < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let lock = unsafe { std::fs::File::from_raw_fd(lock_descriptor) };
    Ok((lock, directory))
}

/// The provisional, retry-stable state of one enrollment operation.
///
/// Contains private key material — treat like `identity.json`. Never
/// contains the enrollment key.
#[expect(
    clippy::unsafe_derive_deserialize,
    reason = "the unsafe file operations do not rely on deserialized value invariants"
)]
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
    /// The normalized control-plane endpoint and non-secret authority facts.
    /// These prevent a lost response from being replayed to another cloud or
    /// under a different organization by a later process.
    pub binding: EnrollmentRequestBinding,
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
            .field("binding", &self.binding)
            .finish()
    }
}

impl EnrollmentDraft {
    /// The draft path inside `config_dir`.
    #[must_use]
    pub fn path_in(config_dir: &Path) -> PathBuf {
        config_dir.join(ENROLLMENT_DRAFT_FILE)
    }

    /// Load an existing draft without creating or mutating enrollment state.
    ///
    /// # Errors
    ///
    /// Returns an error for unreadable, malformed, or unsupported draft data.
    pub fn load_optional(config_dir: &Path) -> Result<Option<Self>> {
        let path = Self::path_in(config_dir);
        match read_bounded_regular_file(&path, MAX_DRAFT_BYTES) {
            Ok(contents) => Self::parse_at(&path, &contents).map(Some),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(Error::Io { path, source }),
        }
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
        binding: &EnrollmentRequestBinding,
    ) -> Result<Self> {
        EnrollmentTransactionLock::acquire(config_dir)?.load_or_create(instance, region, binding)
    }

    fn parse_at(path: &Path, contents: &[u8]) -> Result<Self> {
        // Read only the version first. A future schema may add fields that the
        // current strict draft parser deliberately rejects, but operators must
        // still receive the unsupported-schema recovery guidance rather than a
        // generic parse error that obscures the pending enrollment identity.
        let header = serde_json::from_slice::<DraftSchemaHeader>(contents).context(ParseSnafu {
            path: path.to_path_buf(),
        })?;
        if header.schema_version != DRAFT_SCHEMA_VERSION {
            return Err(Error::UnsupportedSchema {
                path: path.to_path_buf(),
                found: header.schema_version,
            });
        }
        let draft = serde_json::from_slice::<Self>(contents).context(ParseSnafu {
            path: path.to_path_buf(),
        })?;
        if let Some(reason) = draft.material().validation_error() {
            return Err(Error::Invalid {
                path: path.to_path_buf(),
                reason,
            });
        }
        Ok(draft)
    }

    fn load_published(path: &Path, contents: &[u8]) -> Result<Self> {
        Self::parse_at(path, contents)
    }

    /// Generate a fresh draft and persist it at `path` before returning it,
    /// so the operation ID is durable before the first request that uses it.
    #[cfg(test)]
    fn create_at(
        path: &Path,
        instance: &InstanceFacts,
        region: Option<&str>,
        binding: &EnrollmentRequestBinding,
    ) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context(IoSnafu {
                path: parent.to_path_buf(),
            })?;
        }

        let lock_path = Self::lock_path(path);
        let publication_lock = Self::acquire_publication_lock(&lock_path)?;
        Self::publish_locked(path, &publication_lock, instance, region, binding)
    }

    #[cfg(test)]
    fn acquire_publication_lock(lock_path: &Path) -> Result<std::fs::File> {
        Self::acquire_publication_lock_with_budget(lock_path, LOCK_WAIT_BUDGET)
    }

    fn acquire_publication_lock_with_budget(
        lock_path: &Path,
        wait_budget: std::time::Duration,
    ) -> Result<std::fs::File> {
        #[cfg(unix)]
        use std::os::fd::AsRawFd as _;
        #[cfg(unix)]
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        #[cfg(unix)]
        let (file, lock_directory) = open_unix_publication_lock(lock_path).context(IoSnafu {
            path: lock_path.to_path_buf(),
        })?;
        #[cfg(not(unix))]
        let mut options = std::fs::OpenOptions::new();
        #[cfg(not(unix))]
        options.create(true).read(true).write(true);
        #[cfg(not(unix))]
        let file = options.open(lock_path).context(IoSnafu {
            path: lock_path.to_path_buf(),
        })?;
        let metadata = file.metadata().context(IoSnafu {
            path: lock_path.to_path_buf(),
        })?;
        if !metadata.is_file() {
            return Err(Error::Io {
                path: lock_path.to_path_buf(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "the enrollment transaction lock must be a regular file",
                ),
            });
        }
        #[cfg(unix)]
        {
            if metadata.nlink() != 1 {
                return Err(Error::Io {
                    path: lock_path.to_path_buf(),
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "the enrollment transaction lock must not be hard-linked",
                    ),
                });
            }
            let directory_metadata = lock_directory.metadata().context(IoSnafu {
                path: lock_path.to_path_buf(),
            })?;
            let effective_uid = unsafe { libc::geteuid() };
            if effective_uid != 0 && effective_uid != directory_metadata.uid() {
                return Err(Error::Io {
                    path: lock_path.to_path_buf(),
                    source: std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "the enrollment transaction lock must be acquired by the config-directory owner",
                    ),
                });
            }
            if metadata.uid() != directory_metadata.uid() {
                if effective_uid != 0 {
                    return Err(Error::Io {
                        path: lock_path.to_path_buf(),
                        source: std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "the enrollment transaction lock is not owned by the config-directory owner",
                        ),
                    });
                }
                let result = unsafe {
                    libc::fchown(
                        file.as_raw_fd(),
                        directory_metadata.uid(),
                        directory_metadata.gid(),
                    )
                };
                if result != 0 {
                    return Err(Error::Io {
                        path: lock_path.to_path_buf(),
                        source: std::io::Error::last_os_error(),
                    });
                }
            }
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .context(IoSnafu {
                    path: lock_path.to_path_buf(),
                })?;
        }
        file.sync_all().context(IoSnafu {
            path: lock_path.to_path_buf(),
        })?;
        #[cfg(unix)]
        lock_directory.sync_all().context(IoSnafu {
            path: lock_path.to_path_buf(),
        })?;
        #[cfg(not(unix))]
        crate::identity::sync_parent_directory(lock_path).context(IoSnafu {
            path: lock_path.to_path_buf(),
        })?;

        let deadline = std::time::Instant::now() + wait_budget;
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
        binding: &EnrollmentRequestBinding,
    ) -> Result<Self> {
        // A caller may have observed NotFound before acquiring the lock, but
        // another process can publish before this process owns it. Re-read
        // while locked so a delayed creator cannot replace the durable winner
        // through the atomic rename below.
        match read_bounded_regular_file(path, MAX_DRAFT_BYTES) {
            Ok(contents) => {
                let draft = Self::parse_at(path, &contents)?;
                draft.validate_request(path, binding)?;
                return Ok(draft);
            }
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
            binding: binding.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&draft).context(SerializeSnafu)?;
        crate::identity::atomic_write_owner_only(path, &bytes).context(IoSnafu {
            path: path.to_path_buf(),
        })?;
        Ok(draft)
    }

    /// Confirm a retry targets the same control plane and authority as the
    /// durable draft.
    ///
    /// The declared region is deliberately not part of the replay key: a
    /// resumed enrollment keeps the region recorded when the draft was
    /// published, so a differing `--region` on a retry must not invalidate
    /// exact-replay state.
    fn validate_request(&self, path: &Path, binding: &EnrollmentRequestBinding) -> Result<()> {
        let authority_matches = match (&self.binding.authority, &binding.authority) {
            (
                EnrollmentAuthorityBinding::Token { .. },
                EnrollmentAuthorityBinding::Token { .. },
            ) => true,
            (
                EnrollmentAuthorityBinding::AuthenticatedSession {
                    organization: persisted,
                },
                EnrollmentAuthorityBinding::AuthenticatedSession {
                    organization: requested,
                },
            ) => persisted == requested,
            _ => false,
        };
        if self.binding.endpoint != binding.endpoint || !authority_matches {
            return Err(Error::RequestBindingMismatch {
                path: path.to_path_buf(),
            });
        }
        Ok(())
    }

    /// Remove the draft for `config_dir`. A missing file is success. Deletion
    /// acquires the same transaction lock as enrollment so an external remove
    /// cannot discard retry identity while another process is using it.
    ///
    /// # Errors
    ///
    /// Returns an error when another live enrollment owns the directory or
    /// when the file exists but cannot be removed.
    pub fn delete(config_dir: &Path) -> Result<()> {
        EnrollmentTransactionLock::acquire(config_dir)?.delete()
    }

    /// Async variant of [`EnrollmentDraft::delete`] for callers on a Tokio
    /// runtime. Lock acquisition can wait for another enrollment transaction,
    /// so it runs on the blocking pool rather than occupying a worker thread.
    ///
    /// # Errors
    ///
    /// Returns an error when another live enrollment owns the directory, when
    /// the file cannot be removed, or when the blocking task panics.
    pub async fn delete_async(config_dir: &Path) -> Result<()> {
        let config_dir = config_dir.to_path_buf();
        tokio::task::spawn_blocking(move || Self::delete(&config_dir))
            .await
            .map_err(|source| Error::DeleteTaskPanicked { source })?
    }

    fn delete_at(path: &Path) -> Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => {
                crate::identity::sync_parent_directory(path).context(IoSnafu {
                    path: path.to_path_buf(),
                })?;
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(Error::Io {
                    path: path.to_path_buf(),
                    source,
                });
            }
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

fn read_bounded_regular_file(path: &Path, max_bytes: u64) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;

    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the enrollment draft was not a bounded regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.nlink() != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "the enrollment draft must not be hard-linked",
            ));
        }
    }
    let mut bytes = Vec::with_capacity(usize::try_from(metadata.len()).unwrap_or(0));
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the enrollment draft exceeded its size limit",
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_binding() -> EnrollmentRequestBinding {
        EnrollmentRequestBinding {
            endpoint: "https://api.spice.ai".to_string(),
            authority: EnrollmentAuthorityBinding::Token {
                expected_org: Some("acme".to_string()),
            },
        }
    }

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
            &test_binding(),
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
            Some("us-west-2"),
            &test_binding(),
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
    fn token_recovery_reuses_the_draft_across_a_new_assertion_and_region() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = load_or_create(dir.path()).expect("create");
        let recovered = EnrollmentDraft::load_or_create(
            dir.path(),
            &test_instance("v9.9.9-replacement"),
            Some("eu-west-1"),
            &EnrollmentRequestBinding {
                endpoint: "https://api.spice.ai".to_string(),
                authority: EnrollmentAuthorityBinding::Token {
                    expected_org: Some("corrected-org".to_string()),
                },
            },
        )
        .expect("a new token assertion recovers the exact operation");

        assert_eq!(
            recovered.enrollment_operation_id,
            first.enrollment_operation_id
        );
        assert_eq!(recovered.region, first.region);
        assert_eq!(recovered.binding, first.binding);
    }

    #[test]
    fn recovery_rejects_another_endpoint_or_authenticated_org() {
        let endpoint_dir = tempfile::tempdir().expect("endpoint tempdir");
        load_or_create(endpoint_dir.path()).expect("create token draft");
        let endpoint_error = EnrollmentDraft::load_or_create(
            endpoint_dir.path(),
            &test_instance("v2.2.0-test"),
            Some("us-west-2"),
            &EnrollmentRequestBinding {
                endpoint: "https://other.example".to_string(),
                authority: EnrollmentAuthorityBinding::Token { expected_org: None },
            },
        )
        .expect_err("another endpoint cannot receive the persisted operation");
        assert!(
            matches!(endpoint_error, Error::RequestBindingMismatch { .. }),
            "{endpoint_error}"
        );

        let auth_dir = tempfile::tempdir().expect("auth tempdir");
        let original = EnrollmentRequestBinding {
            endpoint: "https://api.spice.ai".to_string(),
            authority: EnrollmentAuthorityBinding::AuthenticatedSession {
                organization: "acme".to_string(),
            },
        };
        EnrollmentDraft::load_or_create(
            auth_dir.path(),
            &test_instance("v2.2.0-test"),
            None,
            &original,
        )
        .expect("create authenticated draft");
        let org_error = EnrollmentDraft::load_or_create(
            auth_dir.path(),
            &test_instance("v2.2.0-test"),
            None,
            &EnrollmentRequestBinding {
                endpoint: original.endpoint,
                authority: EnrollmentAuthorityBinding::AuthenticatedSession {
                    organization: "other".to_string(),
                },
            },
        )
        .expect_err("another authenticated org cannot receive the operation");
        assert!(
            matches!(org_error, Error::RequestBindingMismatch { .. }),
            "{org_error}"
        );
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
    fn draft_parse_errors_never_echo_persisted_values() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = EnrollmentDraft::path_in(dir.path());
        let sensitive = "private-key-material-that-must-not-be-printed";
        std::fs::write(&path, format!(r#"{{"schema_version":"{sensitive}"}}"#))
            .expect("write malformed draft");

        let error = load_or_create(dir.path()).expect_err("must refuse malformed draft");
        let rendered = error.to_string();
        assert!(rendered.contains("line"), "{rendered}");
        assert!(!rendered.contains(sensitive), "{rendered}");
    }

    #[test]
    fn cryptographically_inconsistent_draft_is_never_replayed_or_replaced() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = EnrollmentDraft::path_in(dir.path());
        let mut corrupted = load_or_create(dir.path()).expect("create draft");
        corrupted.enc_public_key_pem = IdentityStore::generate_enrollment()
            .expect("generate mismatched material")
            .enc_public_key_pem;
        let serialized = serde_json::to_string_pretty(&corrupted).expect("serialize draft");
        std::fs::write(&path, &serialized).expect("write corrupt draft");

        let error = load_or_create(dir.path()).expect_err("must refuse unsafe replay");
        assert!(matches!(error, Error::Invalid { .. }), "{error}");
        assert!(error.to_string().contains("do not match"), "{error}");
        assert_eq!(
            std::fs::read_to_string(path).expect("corrupt draft remains"),
            serialized,
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
            Some("us-west-2"),
            &test_binding(),
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
            "future_retry_metadata": {
                "cloud_operation_generation": 3
            },
        });
        std::fs::write(&path, newer.to_string()).expect("write newer draft");

        let err = load_or_create(dir.path()).expect_err("must refuse");
        assert!(matches!(&err, Error::UnsupportedSchema { .. }), "{err}");
        let message = err.to_string();
        assert!(message.contains("contact Spice Cloud support"), "{message}");
        assert!(
            message.contains("Do not delete the draft"),
            "unsupported drafts must preserve retry identity: {message}"
        );
        assert!(
            path.exists(),
            "refusing an unsupported schema must preserve its retry identity"
        );
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

    #[test]
    fn external_delete_cannot_remove_an_active_enrollment_draft() {
        let dir = tempfile::tempdir().expect("tempdir");
        let transaction = EnrollmentTransactionLock::acquire(dir.path()).expect("acquire lock");
        transaction
            .load_or_create(
                &test_instance("v2.2.0-test"),
                Some("us-west-2"),
                &test_binding(),
            )
            .expect("publish draft while locked");

        let error = EnrollmentDraft::delete(dir.path()).expect_err("live enrollment owns draft");
        assert!(matches!(error, Error::CreationInProgress { .. }), "{error}");
        assert!(
            EnrollmentDraft::path_in(dir.path()).exists(),
            "external cleanup must preserve the active retry identity"
        );

        drop(transaction);
        EnrollmentDraft::delete(dir.path()).expect("delete after enrollment releases lock");
        assert!(!EnrollmentDraft::path_in(dir.path()).exists());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_delete_does_not_block_the_tokio_runtime_during_lock_contention() {
        let dir = tempfile::tempdir().expect("tempdir");
        let transaction = EnrollmentTransactionLock::acquire(dir.path()).expect("acquire lock");
        transaction
            .load_or_create(
                &test_instance("v2.2.0-test"),
                Some("us-west-2"),
                &test_binding(),
            )
            .expect("publish draft while locked");

        let started = std::time::Instant::now();
        let sentinel = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            started.elapsed()
        });
        let error = EnrollmentDraft::delete_async(dir.path())
            .await
            .expect_err("live enrollment owns draft");
        let sentinel_elapsed = sentinel.await.expect("sentinel task");

        assert!(matches!(error, Error::CreationInProgress { .. }), "{error}");
        assert!(
            sentinel_elapsed < std::time::Duration::from_millis(100),
            "lock contention blocked the Tokio runtime for {sentinel_elapsed:?}"
        );
        assert!(EnrollmentDraft::path_in(dir.path()).exists());
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
    fn draft_reads_reject_symlinks_without_reading_the_target() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let target = dir.path().join("outside.json");
        let path = EnrollmentDraft::path_in(dir.path());
        std::fs::write(&target, "sensitive target").expect("write target");
        symlink(&target, &path).expect("create draft symlink");

        let error = load_or_create(dir.path()).expect_err("symlink must be rejected");
        assert!(matches!(error, Error::Io { .. }), "{error}");
        assert_eq!(
            std::fs::read_to_string(target).expect("target remains readable"),
            "sensitive target"
        );
    }

    #[cfg(unix)]
    #[test]
    fn draft_reads_reject_fifos_without_blocking() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt as _;
        use std::os::unix::fs::FileTypeExt as _;

        let dir = tempfile::tempdir().expect("tempdir");
        let path = EnrollmentDraft::path_in(dir.path());
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

        let error = load_or_create(dir.path()).expect_err("FIFO must be rejected");
        assert!(matches!(error, Error::Io { .. }), "{error}");
        assert!(
            std::fs::metadata(path)
                .expect("FIFO metadata")
                .file_type()
                .is_fifo()
        );
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

    #[cfg(unix)]
    #[test]
    fn the_publication_lock_rejects_symlinks_without_touching_the_target() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let dir = tempfile::tempdir().expect("tempdir");
        let target_path = dir.path().join("unrelated");
        std::fs::write(&target_path, "must remain unchanged").expect("write target");
        std::fs::set_permissions(&target_path, std::fs::Permissions::from_mode(0o644))
            .expect("set target mode");

        let draft_path = EnrollmentDraft::path_in(dir.path());
        let lock_path = EnrollmentDraft::lock_path(&draft_path);
        symlink(&target_path, &lock_path).expect("create malicious lock symlink");

        EnrollmentDraft::acquire_publication_lock(&lock_path)
            .expect_err("a transaction lock must never follow a symlink");
        assert_eq!(
            std::fs::read_to_string(&target_path).expect("read target"),
            "must remain unchanged"
        );
        let mode = std::fs::metadata(&target_path)
            .expect("target metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o644, "the symlink target mode must not change");
    }

    #[cfg(unix)]
    #[test]
    fn the_publication_lock_rejects_hard_links_without_changing_the_inode() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("tempdir");
        let target_path = dir.path().join("unrelated");
        std::fs::write(&target_path, "must remain unchanged").expect("write target");
        std::fs::set_permissions(&target_path, std::fs::Permissions::from_mode(0o644))
            .expect("set target mode");

        let draft_path = EnrollmentDraft::path_in(dir.path());
        let lock_path = EnrollmentDraft::lock_path(&draft_path);
        std::fs::hard_link(&target_path, &lock_path).expect("create malicious hard link");

        let error = EnrollmentDraft::acquire_publication_lock(&lock_path)
            .expect_err("a transaction lock must never accept a hard link");
        assert!(error.to_string().contains("must not be hard-linked"));
        assert_eq!(
            std::fs::read_to_string(&target_path).expect("read target"),
            "must remain unchanged"
        );
        let mode = std::fs::metadata(&target_path)
            .expect("target metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o644, "the hard-linked target mode must not change");
    }

    #[cfg(unix)]
    #[test]
    fn the_publication_lock_rejects_a_fifo_without_blocking_or_chmod() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt as _;
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("tempdir");
        let draft_path = EnrollmentDraft::path_in(dir.path());
        let lock_path = EnrollmentDraft::lock_path(&draft_path);
        let lock_path_c =
            CString::new(lock_path.as_os_str().as_bytes()).expect("lock path CString");
        // SAFETY: `lock_path_c` is a live, NUL-terminated path and the mode is
        // a valid `mode_t`; `mkfifo` does not retain either argument.
        let result = unsafe { libc::mkfifo(lock_path_c.as_ptr(), 0o644) };
        assert_eq!(
            result,
            0,
            "create FIFO: {}",
            std::io::Error::last_os_error()
        );
        let original_mode = std::fs::metadata(&lock_path)
            .expect("FIFO metadata before acquisition")
            .permissions()
            .mode()
            & 0o777;

        EnrollmentDraft::acquire_publication_lock(&lock_path)
            .expect_err("a FIFO must not become an enrollment transaction lock");
        let metadata = std::fs::metadata(&lock_path).expect("FIFO metadata");
        assert!(!metadata.is_file(), "the lock path must remain a FIFO");
        assert_eq!(
            metadata.permissions().mode() & 0o777,
            original_mode,
            "rejecting a FIFO must not change its mode"
        );
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
                "binding",
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
