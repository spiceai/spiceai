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

//! The license Spice Cloud returns with an enrollment or a renewal.
//!
//! Spice Cloud includes a signed license in its enroll and renew responses for
//! an org entitled to one, or the reason it issued none. Cloud Connect does not
//! interpret either: it keeps the most recent license, and the most recent
//! reason, beside the identity in [`CLOUD_CONNECT_LICENSE_FILE`], so the process
//! that verifies licenses can read them, including after a restart without
//! network access. A response that carries neither field leaves the file as it
//! is, and clearing the identity removes it.
//!
//! [`CLOUD_CONNECT_LICENSE_FILE`]: crate::config::CLOUD_CONNECT_LICENSE_FILE

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config::CLOUD_CONNECT_LICENSE_FILE;

/// The largest license kept. A license is under a kilobyte; anything larger
/// is not one, and is not written to disk.
const MAX_LICENSE_BYTES: usize = 64 * 1024;

/// How much of the reason Spice Cloud gives for issuing no license is kept.
const MAX_REASON_CHARS: usize = 512;

/// What Cloud Connect keeps of the licenses Spice Cloud returned.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudLicense {
    /// The license Spice Cloud issued most recently, as the compact token it
    /// sent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    /// Why Spice Cloud issued no license with its most recent response, when
    /// it did not. A license issued later clears it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<LicenseUnavailable>,
}

/// Why Spice Cloud issued no license.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LicenseUnavailable {
    /// A stable code, such as `no_enterprise_subscription`.
    pub code: String,
    /// Spice Cloud's explanation.
    pub message: String,
}

/// The license fields of an enroll or renew response.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct LicenseFieldsWire {
    #[serde(default)]
    license: Option<String>,
    #[serde(default)]
    license_unavailable: Option<LicenseUnavailable>,
}

/// What one response delivered, after the checks a value written to disk must
/// pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LicenseDelivery {
    pub license: Option<String>,
    pub unavailable: Option<LicenseUnavailable>,
}

impl LicenseFieldsWire {
    /// Keep a license only when it looks like a compact token: bounded, and
    /// made only of base64url characters and dots. Verifying it is the
    /// consumer's job; this only keeps arbitrary peer bytes off the disk.
    pub(crate) fn into_delivery(self) -> LicenseDelivery {
        let unavailable = self.unavailable_reason();
        let license = self.license.filter(|token| {
            !token.is_empty()
                && token.len() <= MAX_LICENSE_BYTES
                && token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        });
        LicenseDelivery {
            license,
            unavailable,
        }
    }

    fn unavailable_reason(&self) -> Option<LicenseUnavailable> {
        let reason = self.license_unavailable.as_ref()?;
        let code: String = reason
            .code
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .take(64)
            .collect();
        let message: String = reason
            .message
            .chars()
            .filter(|c| !c.is_control())
            .take(MAX_REASON_CHARS)
            .collect();
        (!code.is_empty()).then_some(LicenseUnavailable { code, message })
    }
}

impl LicenseDelivery {
    /// Whether the response carried a license or a reason at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.license.is_none() && self.unavailable.is_none()
    }

    /// The state to keep after this delivery, given what was kept before: a
    /// new license replaces the old one and clears any reason; a reason alone
    /// keeps the old license, which stays usable until it expires.
    #[must_use]
    pub fn merged_into(&self, kept: Option<CloudLicense>) -> CloudLicense {
        let kept = kept.unwrap_or_default();
        match &self.license {
            Some(license) => CloudLicense {
                license: Some(license.clone()),
                unavailable: None,
            },
            None => CloudLicense {
                license: kept.license,
                unavailable: self.unavailable.clone().or(kept.unavailable),
            },
        }
    }
}

/// Where Cloud Connect keeps the license for `config_dir`.
#[must_use]
pub fn path_in(config_dir: &Path) -> PathBuf {
    config_dir.join(CLOUD_CONNECT_LICENSE_FILE)
}

/// Read what Cloud Connect kept, or `None` when it kept nothing.
///
/// # Errors
///
/// Returns an error when the file exists but cannot be read or parsed.
pub fn load(config_dir: &Path) -> std::io::Result<Option<CloudLicense>> {
    let path = path_in(config_dir);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(source),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|source| std::io::Error::new(std::io::ErrorKind::InvalidData, source))
}

/// Keep what `delivery` brought, owner-only and atomically. A delivery with
/// neither field changes nothing.
pub(crate) fn record(config_dir: &Path, delivery: &LicenseDelivery) -> std::io::Result<()> {
    if delivery.is_empty() {
        return Ok(());
    }
    // A file that cannot be parsed holds nothing worth keeping: replace it.
    let kept = load(config_dir).unwrap_or(None);
    let merged = delivery.merged_into(kept);
    let bytes = serde_json::to_vec_pretty(&merged)
        .map_err(|source| std::io::Error::new(std::io::ErrorKind::InvalidData, source))?;
    crate::identity::atomic_write_owner_only(&path_in(config_dir), &bytes)
}

/// [`record`], off the async runtime, logging rather than failing: an
/// enrollment or renewal that succeeded must not be undone by a license that
/// could not be written.
pub(crate) async fn record_logged(config_dir: &Path, delivery: LicenseDelivery) {
    if delivery.is_empty() {
        return;
    }
    let config_dir = config_dir.to_path_buf();
    let path = path_in(&config_dir);
    match tokio::task::spawn_blocking(move || record(&config_dir, &delivery)).await {
        Ok(Ok(())) => {
            tracing::debug!(
                "Cloud Connect: kept the license Spice Cloud returned at {}",
                path.display()
            );
        }
        Ok(Err(source)) => tracing::warn!(
            "Cloud Connect: failed to keep the license Spice Cloud returned at {}, so a restart without network access will not have it. Cause: {source}",
            path.display()
        ),
        Err(source) => tracing::warn!(
            "Cloud Connect: failed to keep the license Spice Cloud returned at {}, so a restart without network access will not have it. Cause: {source}",
            path.display()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(json: &str) -> LicenseFieldsWire {
        serde_json::from_str(json).expect("parse the response fields")
    }

    #[test]
    fn a_response_without_license_fields_changes_nothing() {
        let dir = tempfile::tempdir().expect("create a temp dir");
        let delivery = wire(r#"{"identity_cert_pem":"x"}"#).into_delivery();
        assert!(delivery.is_empty());
        record(dir.path(), &delivery).expect("record nothing");
        assert_eq!(load(dir.path()).expect("load"), None);
    }

    #[test]
    fn a_reason_keeps_the_license_and_a_new_license_clears_the_reason() {
        let dir = tempfile::tempdir().expect("create a temp dir");
        record(
            dir.path(),
            &wire(r#"{"license":"aGVhZGVy.cGF5bG9hZA.c2ln"}"#).into_delivery(),
        )
        .expect("record the license");
        record(
            dir.path(),
            &wire(r#"{"license_unavailable":{"code":"no_enterprise_subscription","message":"The org has no\nEnterprise subscription."}}"#)
                .into_delivery(),
        )
        .expect("record the reason");
        assert_eq!(
            load(dir.path()).expect("load"),
            Some(CloudLicense {
                license: Some("aGVhZGVy.cGF5bG9hZA.c2ln".to_string()),
                unavailable: Some(LicenseUnavailable {
                    code: "no_enterprise_subscription".to_string(),
                    message: "The org has noEnterprise subscription.".to_string(),
                }),
            })
        );
        record(
            dir.path(),
            &wire(r#"{"license":"bmV3.bGljZW5zZQ.c2ln"}"#).into_delivery(),
        )
        .expect("record the new license");
        assert_eq!(
            load(dir.path()).expect("load"),
            Some(CloudLicense {
                license: Some("bmV3.bGljZW5zZQ.c2ln".to_string()),
                unavailable: None,
            })
        );
    }

    #[test]
    fn a_value_that_is_not_a_compact_token_is_not_written() {
        let delivery = wire(r#"{"license":"../../etc/passwd\nERROR"}"#).into_delivery();
        assert_eq!(delivery.license, None);
        let oversized = format!(r#"{{"license":"{}"}}"#, "a".repeat(MAX_LICENSE_BYTES + 1));
        assert_eq!(wire(&oversized).into_delivery().license, None);
    }

    #[cfg(unix)]
    #[test]
    fn the_license_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().expect("create a temp dir");
        record(dir.path(), &wire(r#"{"license":"a.b.c"}"#).into_delivery()).expect("record");
        let mode = std::fs::metadata(path_in(dir.path()))
            .expect("stat the license file")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}
