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

//! Stable instance fingerprint sent at enroll and in the first `Hello`, so the
//! control plane can tell two instances on one host apart and recognise one
//! that is re-installing itself.
//!
//! The fingerprint is the lowercase hex SHA-256 of:
//!
//! ```text
//! machine-id || ":" || hostname || ":" || os || ":" || arch || ":" || config-dir
//! ```
//!
//! ## Why the config directory is in it
//!
//! Everything else in that list is host-scoped, so without it every runtime on
//! one host produces the same value — and the registry, which identifies an
//! instance by its fingerprint, cannot then distinguish the instances this
//! fingerprint exists to distinguish.
//!
//! The config directory is the discriminator rather than the identity key
//! because the fingerprint's other job is recognising a *re-install*: deleting
//! `.spice/` regenerates the keypair, so keying on it would mint a new instance
//! every time an operator reinstalled one. The directory survives a re-install
//! in place, which separates co-located instances *and* keeps a reinstalled one
//! recognisable.
//!
//! It is hashed, not sent: the fingerprint remains a hash rather than a
//! disclosure of where an instance lives on disk.
//!
//! ## Why the path is canonicalized first
//!
//! Two spellings of one directory must not produce two fingerprints, or an
//! instance stops being recognisable as itself. So the path is resolved to an
//! absolute, symlink-free form before it is hashed, and a trailing separator is
//! dropped. On a case-insensitive filesystem — the macOS default —
//! canonicalization also settles case, because it returns the spelling the
//! filesystem stores rather than the one the caller typed. A directory that
//! cannot be resolved (it does not exist yet) is absolutized and normalized
//! lexically instead, which is the best available answer and is what the
//! directory will canonicalize to once it is created.
//!
//! ## Machine id
//!
//! `machine-id` is `/etc/machine-id` (or its `/var/lib/dbus/machine-id`
//! fallback) on Linux and the `IOPlatformUUID` of `IOPlatformExpertDevice` on
//! macOS. Windows still substitutes the literal `"unknown"`, which leaves its
//! fingerprint hostname-derived.
//!
//! Every one of those degrades to `"unknown"` rather than failing: an
//! enrollment that fails because a platform lookup did is a worse outcome than
//! a weaker fingerprint.
//!
//! This is a fingerprint, not a secret — leaking it is not a security issue,
//! and it intentionally does not include MAC addresses (reading them reliably
//! across platforms requires extra dependencies, and they drift more than an
//! instance's identity should).

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use sha2::{Digest, Sha256};

/// What a machine id degrades to when the platform will not give one up.
const UNKNOWN: &str = "unknown";

/// The host's machine id, resolved once.
///
/// Cached because resolving it can cost a subprocess on macOS and this is on
/// the path of every `Hello`, which is sent on every reconnect.
static MACHINE_ID: LazyLock<String> =
    LazyLock::new(|| read_machine_id().unwrap_or_else(|| UNKNOWN.to_string()));

/// Compute the fingerprint for the instance rooted at `config_dir`.
///
/// Deterministic for one instance across runs as long as the OS-supplied
/// machine id, the hostname, and the instance's directory are stable.
#[must_use]
pub(crate) fn compute(config_dir: &Path) -> String {
    let hostname = gethostname::gethostname().to_string_lossy().into_owned();

    let mut hasher = Sha256::new();
    hasher.update(MACHINE_ID.as_bytes());
    hasher.update(b":");
    hasher.update(hostname.as_bytes());
    hasher.update(b":");
    hasher.update(std::env::consts::OS.as_bytes());
    hasher.update(b":");
    hasher.update(std::env::consts::ARCH.as_bytes());
    hasher.update(b":");
    hasher.update(
        canonical_config_dir(config_dir)
            .as_os_str()
            .as_encoded_bytes(),
    );
    let bytes = hasher.finalize();

    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing into a `String` is infallible.
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

/// Resolve the machine id now, off whatever thread the caller is on.
///
/// The resolution is cached for the life of the process, and on macOS it runs
/// a short-lived subprocess — which must not happen on a Tokio worker, where
/// the first `Hello` would otherwise pay for it. Callers that are about to
/// start the client call this from a blocking context first.
pub(crate) fn prime_machine_id() {
    LazyLock::force(&MACHINE_ID);
}

/// The absolute, symlink-free form of an instance's config directory, with any
/// trailing separator and `.` component dropped.
///
/// A directory that cannot be resolved — most often because it does not exist
/// yet — is absolutized against the current directory and normalized
/// lexically. `..` is left in place there rather than resolved, because
/// resolving it lexically is wrong in the presence of symlinks and this value
/// only has to be stable, not minimal.
fn canonical_config_dir(config_dir: &Path) -> PathBuf {
    if let Ok(resolved) = std::fs::canonicalize(config_dir) {
        // Canonicalization already absolutizes, but it leaves macOS's `/private`
        // prefix and Windows's verbatim prefix as they are; re-collecting the
        // components is what drops a trailing separator either way.
        return resolved.components().collect();
    }

    // The directory does not exist yet, which enrollment reaches before it
    // creates one. Canonicalizing the longest ancestor that *does* exist and
    // re-appending the missing tail is what makes the answer survive creation:
    // resolving the whole path lexically instead would hash `/var/…` now and
    // `/private/var/…` the moment the directory appeared, giving one instance
    // two fingerprints across its own enrollment — the exact defect this
    // module exists to prevent.
    let absolute = normalize_lexically(config_dir);
    let mut unresolved = Vec::new();
    let mut ancestor = absolute.as_path();
    loop {
        if let Ok(resolved) = std::fs::canonicalize(ancestor) {
            let mut rebuilt: PathBuf = resolved.components().collect();
            rebuilt.extend(unresolved.iter().rev());
            return rebuilt;
        }
        match (ancestor.file_name(), ancestor.parent()) {
            (Some(name), Some(parent)) => {
                unresolved.push(name.to_os_string());
                ancestor = parent;
            }
            // Nothing on the path resolves — a relative path with no existing
            // root, or a root that cannot be read. The lexical form is then the
            // only stable answer available.
            _ => return absolute,
        }
    }
}

fn normalize_lexically(path: &Path) -> PathBuf {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    absolute.components().collect()
}

/// Best-effort host machine id. `None` when there is no stable identifier to
/// be had, in which case the caller substitutes [`UNKNOWN`].
///
/// Deliberately never falls back to boot time: it changes on every reboot,
/// which would defeat the whole point of a stable fingerprint.
fn read_machine_id() -> Option<String> {
    #[cfg(target_os = "linux")]
    {
        // /etc/machine-id is canonical on systemd-based distributions;
        // /var/lib/dbus/machine-id is the older D-Bus fallback.
        for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"] {
            if let Ok(contents) = std::fs::read_to_string(path) {
                let trimmed = contents.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
        None
    }
    #[cfg(target_os = "macos")]
    {
        read_platform_uuid()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// The Mac's `IOPlatformUUID`: the per-machine identifier macOS assigns to
/// `IOPlatformExpertDevice`.
///
/// Without it the fingerprint is hostname-derived, and default macOS hostnames
/// are not unique — two Macs both called `MacBook-Pro.local`, in one
/// organization, with an instance at the same path, would otherwise produce the
/// same fingerprint across two physical machines.
///
/// Read by running `ioreg` rather than by calling `IOKit` through FFI: the value
/// is wanted once per process, and a subprocess keeps this crate free of both a
/// platform framework dependency and the `unsafe` a C interface would need.
#[cfg(target_os = "macos")]
fn read_platform_uuid() -> Option<String> {
    /// Absolute, so a caller-controlled `PATH` cannot choose what answers.
    const IOREG: &str = "/usr/sbin/ioreg";

    let output = std::process::Command::new(IOREG)
        .args(["-rd1", "-c", "IOPlatformExpertDevice"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_platform_uuid(&String::from_utf8_lossy(&output.stdout))
}

/// Pull `IOPlatformUUID` out of `ioreg`'s property listing, whose lines look
/// like `"IOPlatformUUID" = "8A253738-…"`.
#[cfg(any(target_os = "macos", test))]
fn parse_platform_uuid(printed: &str) -> Option<String> {
    printed.lines().find_map(|line| {
        let (_, after) = line.split_once("\"IOPlatformUUID\"")?;
        let value = after.split_once('=')?.1.trim();
        let value = value.strip_prefix('"')?;
        let value = value.split('"').next()?.trim();
        (!value.is_empty()).then(|| value.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_dir(name: &str) -> PathBuf {
        PathBuf::from("/srv").join(name).join(".spice")
    }

    #[test]
    fn a_fingerprint_is_64_hex_characters() {
        let fingerprint = compute(&config_dir("edge-1"));
        assert_eq!(fingerprint.len(), 64);
        assert!(fingerprint.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn one_instance_fingerprints_the_same_way_every_time() {
        let dir = config_dir("edge-1");
        assert_eq!(compute(&dir), compute(&dir));
    }

    #[test]
    fn two_instances_on_one_host_do_not_share_a_fingerprint() {
        // The whole point: the registry identifies an instance by this value,
        // so two runtimes in one organization on one host that produced the
        // same one would be treated as the same instance — and enrolling the
        // second would overwrite the first's pinned key.
        assert_ne!(
            compute(&config_dir("edge-1")),
            compute(&config_dir("edge-2"))
        );
    }

    #[test]
    fn one_directory_spelled_two_ways_is_one_instance() {
        // An instance that fingerprinted differently depending on how its own
        // path was written would stop being recognisable as itself, and a
        // re-enrollment would mint a second registry row instead of reclaiming
        // the first.
        let dir = tempfile::tempdir().expect("create tempdir");
        let config = dir.path().join("instance").join(".spice");
        std::fs::create_dir_all(&config).expect("create the config directory");

        let canonical = compute(&config);
        assert_eq!(compute(&config.join("")), canonical, "trailing separator");
        assert_eq!(
            compute(&config.join(".")),
            canonical,
            "a `.` component is not part of the path"
        );

        // Unix-only: the assertion is about symlink resolution, and Windows has
        // no equivalent this test can create without elevation.
        #[cfg(unix)]
        {
            let indirect = dir.path().join("link");
            std::os::unix::fs::symlink(dir.path().join("instance"), &indirect)
                .expect("link to the instance directory");
            assert_eq!(
                compute(&indirect.join(".spice")),
                canonical,
                "a symlinked path names the same directory"
            );
        }
    }

    #[test]
    fn a_relative_path_fingerprints_as_the_directory_it_names() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let config = dir.path().join(".spice");
        std::fs::create_dir_all(&config).expect("create the config directory");
        let canonical = compute(&config);

        // `canonicalize` resolves a relative path against the process's
        // current directory, so both spellings have to land on one value.
        assert_eq!(
            compute(&config.join("..").join(".spice")),
            canonical,
            "a path that walks back through its own parent names the same directory"
        );
    }

    #[test]
    fn a_fingerprint_survives_the_directory_being_created() {
        // The pre-creation answer has to equal the post-creation one, or a
        // single instance gets two fingerprints across its own enrollment —
        // which is the defect this module exists to prevent, re-entering
        // through the fallback path. On macOS the tempdir lives under `/var`,
        // whose canonical form is `/private/var`, so a lexical fallback would
        // differ here.
        let dir = tempfile::tempdir().expect("create tempdir");
        let config = dir.path().join("instance").join(".spice");

        let before = compute(&config);
        std::fs::create_dir_all(&config).expect("create the config directory");
        let after = compute(&config);

        assert_eq!(
            before, after,
            "the fingerprint must not change when the directory it names is created"
        );
    }

    #[test]
    fn a_directory_that_does_not_exist_yet_still_fingerprints_stably() {
        // Enrollment computes this before the directory is necessarily on
        // disk, and it must not depend on whether it is.
        let missing = PathBuf::from("/srv/not-created-yet/.spice");
        assert_eq!(compute(&missing), compute(&missing));
        assert_eq!(compute(&missing).len(), 64);
        assert_ne!(compute(&missing), compute(&config_dir("edge-1")));
    }

    #[test]
    fn the_platform_uuid_is_read_out_of_ioregs_listing() {
        let printed = "+-o IOPlatformExpertDevice  <class IOPlatformExpertDevice>\n  {\n    \
                       \"IOPolledInterface\" = \"AppleARMWatchdogTimerHibernateHandler\"\n    \
                       \"IOPlatformUUID\" = \"8A253738-CE9B-5032-9D00-93CB32E6BDB1\"\n    \
                       \"IOPlatformSerialNumber\" = \"XXXXXXXXXX\"\n  }\n";
        assert_eq!(
            parse_platform_uuid(printed).as_deref(),
            Some("8A253738-CE9B-5032-9D00-93CB32E6BDB1")
        );
        // Nothing to read is nothing to claim: the caller degrades to a
        // hostname-derived fingerprint rather than failing enrollment.
        assert_eq!(parse_platform_uuid(""), None);
        assert_eq!(
            parse_platform_uuid("\"IOPlatformSerialNumber\" = \"X\""),
            None
        );
        assert_eq!(parse_platform_uuid("\"IOPlatformUUID\" = \"\""), None);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn this_mac_has_a_platform_uuid_and_it_is_what_the_fingerprint_uses() {
        let uuid = read_platform_uuid().expect("every Mac reports an IOPlatformUUID");
        assert!(uuid.len() >= 32, "{uuid}");
        assert_ne!(uuid, UNKNOWN);
        prime_machine_id();
        assert_eq!(*MACHINE_ID, uuid);
    }
}
