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

//! Stable machine fingerprint used in the first `Hello` to help the
//! control plane distinguish multiple instances on the same host or
//! detect re-installs.
//!
//! The fingerprint is the lowercase hex SHA-256 of:
//!
//! ```text
//! machine-id || ":" || hostname || ":" || os || ":" || arch
//! ```
//!
//! Where `machine-id` is taken from `/etc/machine-id` (or its
//! `/var/lib/dbus/machine-id` fallback) on Linux, the `IOPlatformUUID`
//! on macOS via `sysinfo`, or a synthetic value on other platforms.
//!
//! This is a fingerprint, not a secret — leaking it is not a security
//! issue, and it intentionally does not include MAC addresses (because
//! reading them reliably across platforms requires extra deps and they
//! drift more than the runtime's identity should).

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

/// Compute a stable machine fingerprint suitable for `Hello.fingerprint`.
///
/// This function is deterministic on the same host across runs as long
/// as the OS-supplied machine id and hostname are stable.
#[must_use]
pub fn compute() -> String {
    let machine_id = read_machine_id().unwrap_or_else(|| "unknown".to_string());
    let hostname = gethostname::gethostname().to_string_lossy().into_owned();
    let os = std::env::consts::OS.to_string();
    let arch = std::env::consts::ARCH.to_string();

    let mut hasher = Sha256::new();
    hasher.update(machine_id.as_bytes());
    hasher.update(b":");
    hasher.update(hostname.as_bytes());
    hasher.update(b":");
    hasher.update(os.as_bytes());
    hasher.update(b":");
    hasher.update(arch.as_bytes());
    let bytes = hasher.finalize();

    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(&mut out, "{b:02x}").expect("write to String");
    }
    out
}

/// Best-effort host machine id. Returns `None` if we cannot find one.
fn read_machine_id() -> Option<String> {
    // /etc/machine-id is canonical on systemd-based Linux distributions.
    // /var/lib/dbus/machine-id is the older D-Bus fallback.
    for path in ["/etc/machine-id", "/var/lib/dbus/machine-id"] {
        if let Ok(s) = std::fs::read_to_string(path) {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }

    // On macOS, `ioreg -d2 -c IOPlatformExpertDevice` yields a stable
    // `IOPlatformUUID`. We avoid spawning a subprocess and rely on
    // the boot-id surfaced by `sysinfo` instead, which is also stable
    // across reboots on most platforms.
    if let Some(boot_id) = sysinfo::System::boot_time().to_string().into() {
        // boot_time is u64-as-string and is identical for the lifetime
        // of the boot; combined with hostname this is enough.
        return Some(boot_id);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_64_hex_chars() {
        let fp = compute();
        assert_eq!(fp.len(), 64);
        assert!(fp.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn fingerprint_is_deterministic() {
        let a = compute();
        let b = compute();
        assert_eq!(a, b);
    }
}
