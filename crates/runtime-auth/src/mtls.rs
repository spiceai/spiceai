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

//! mTLS-derived auth principals and channel identity.
//!
//! Two types live here:
//!
//! - [`MtlsPrincipal`] implements [`AuthPrincipal`] and is set on the
//!   request's [`AuthRequestContext`] **only** in mTLS-as-identity
//!   mode (the [`IdentitySource`] computed at startup is `Channel`).
//!   When the principal is set, the existing
//!   [`crate::layer::http::AuthMiddleware`] /
//!   [`crate::layer::flight::BasicAuthMiddleware`] short-circuits
//!   without invoking the underlying credential verifier (API-key
//!   validator).
//!
//! - [`ChannelIdentity`] is **always** populated when a verified peer
//!   cert was presented, regardless of mode. It lives on the request
//!   extensions for audit / task-history use; it never participates
//!   in authorization. In mTLS-as-channel mode it identifies the
//!   calling service while the auth principal identifies the human
//!   end-user.

use std::borrow::Cow;

use sha2::{Digest, Sha256};
use x509_certificate::X509Certificate;

use crate::AuthPrincipal;

/// Where the request's auth principal comes from. Computed once at
/// startup from `runtime.auth` and `runtime.tls.client_auth`, then
/// threaded into the HTTP and Flight middleware stacks.
///
/// The selection is deliberately done at startup (not per-request) so
/// the middleware ordering — does the mTLS layer set a principal that
/// the auth layer should respect, or does the auth layer set the only
/// principal? — is invariant for the process lifetime. Operators
/// flipping this require a restart, which matches how `runtime.auth`
/// and `client_auth` are themselves immutable for a given process.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum IdentitySource {
    /// No auth configured. Every request runs as the existing
    /// anonymous principal (`stable_id` returns `None`, `groups` empty).
    /// Reads work; writes are rejected by existing `groups()` checks.
    /// This is the out-of-the-box default and the only mode in which
    /// `spiced` runs without authentication.
    #[default]
    Anonymous,
    /// The TLS client cert is the auth principal. Selected when
    /// `runtime.auth` is unconfigured and `runtime.tls.client_auth` is
    /// `required`. The mTLS layer sets [`MtlsPrincipal`] on the request
    /// context; the auth layer's short-circuit fires and never calls
    /// the API-key verifier (which isn't configured anyway).
    /// "mTLS-as-identity" mode.
    Channel,
    /// The configured `runtime.auth` mechanism (API key today, OIDC
    /// later) is the auth principal. mTLS, if enabled, only protects
    /// the channel and contributes a [`ChannelIdentity`] to the
    /// request extensions for audit. "mTLS-as-channel" mode.
    RuntimeAuth,
}

/// Identity of the peer at the TLS layer. Always populated when the
/// per-protocol mTLS layer saw a verified peer cert, regardless of
/// [`IdentitySource`].
///
/// Independent of the auth principal: in mTLS-as-channel mode,
/// `ChannelIdentity` and `auth_principal` describe two different
/// actors (the calling service and the end user, respectively).
///
/// Cloning is cheap (small struct, owned strings) and safe to share
/// across threads.
#[derive(Debug, Clone)]
pub struct ChannelIdentity {
    /// The X.509 Subject DN, e.g. `CN=client-1`. Empty string when
    /// the cert leaf has no Subject (degenerate setup).
    pub identity: String,
    /// The X.509 Subject DN. Mirrors [`Self::identity`] today; kept
    /// as a separate field so the audit / task-history surface can
    /// rely on a stable "raw subject" slot independent of any
    /// future identity-promotion logic.
    pub subject_dn: String,
    /// SHA-256 of the leaf cert DER. Useful for audit ("which exact
    /// cert was used?") and as a last-resort cache namespace
    /// fallback.
    pub cert_fingerprint: [u8; 32],
}

/// The mTLS-as-identity auth principal. Set on the request's
/// [`AuthRequestContext`] **only** when the configured
/// [`IdentitySource`] is [`IdentitySource::Channel`] AND a verified
/// peer cert was presented.
///
/// Inert on the wire — never serialised, never logged in full
/// (fingerprint is 32-hex truncated to 16 in `stable_id` only).
#[derive(Debug, Clone)]
pub struct MtlsPrincipal {
    /// The X.509 Subject DN (e.g. `CN=client-1`). Used for
    /// [`AuthPrincipal::username`] and as the basis for
    /// [`AuthPrincipal::stable_id`].
    pub identity: String,
    /// X.509 Subject DN. Used for `stable_id` when `identity` is a
    /// fingerprint fallback. Empty string for cert-Subject-less
    /// leaves.
    pub subject_dn: String,
    /// SHA-256 of the leaf cert DER. Used for `stable_id` only as a
    /// last resort (cert with no Subject — a degenerate operator
    /// setup).
    pub cert_fingerprint: [u8; 32],
}

/// Always-`read_write` ACL group set returned by [`MtlsPrincipal`].
/// Matches the "verified mTLS principal" baseline: every cert that
/// passes verification is treated as a fully-trusted writer.
/// Per-cert group mapping is an out-of-scope follow-up.
const MTLS_GROUPS: &[&str] = &["read_write"];

impl AuthPrincipal for MtlsPrincipal {
    fn username(&self) -> &str {
        &self.identity
    }

    fn groups(&self) -> &[&str] {
        MTLS_GROUPS
    }

    /// Stable opaque id for cache-namespacing. **Must be stable
    /// across cert rotation for the same identity** — see the
    /// [`AuthPrincipal::stable_id`] contract.
    ///
    /// Resolution order:
    ///
    /// 1. Subject DN (non-empty) → `mtls:dn:<subject-dn>`. Stable
    ///    across CA-issued cert renewal for the same subject.
    /// 2. Fingerprint fallback → `mtls:fp:<32-hex>`. Only hit when
    ///    the cert has no Subject DN — a degenerate operator setup.
    ///    Cache *will* invalidate on rotation in this case; document
    ///    the trade-off.
    ///
    /// Two distinct prefixes (`mtls:dn:`, `mtls:fp:`) so the
    /// namespace can never accidentally collide across the two
    /// resolution paths.
    fn stable_id(&self) -> Option<Cow<'_, str>> {
        Some(if self.subject_dn.is_empty() {
            let mut id = String::with_capacity("mtls:fp:".len() + 32);
            id.push_str("mtls:fp:");
            for byte in &self.cert_fingerprint[..16] {
                id.push(HEX[usize::from(byte >> 4)] as char);
                id.push(HEX[usize::from(byte & 0x0f)] as char);
            }
            Cow::Owned(id)
        } else {
            Cow::Owned(format!("mtls:dn:{}", self.subject_dn))
        })
    }
}

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Build an [`MtlsPrincipal`] (and the parallel [`ChannelIdentity`])
/// from the DER bytes of a verified peer cert.
///
/// **The cert MUST already have been verified** by rustls's
/// [`rustls::server::danger::ClientCertVerifier`] — this function does
/// not re-validate the chain or the signature. It only parses the leaf
/// to extract the Subject DN and the cert fingerprint.
///
/// Returns `None` only when the leaf DER itself is unparseable, which
/// would be a rustls bug — we still return `None` rather than
/// panicking so a malformed-but-verified cert can't crash the server.
#[must_use]
pub fn principal_from_cert(leaf_der: &[u8]) -> Option<MtlsPrincipal> {
    let cert_fingerprint: [u8; 32] = Sha256::digest(leaf_der).into();
    let cert = X509Certificate::from_der(leaf_der).ok()?;
    // `user_friendly_str()` returns RFC 4514 form (e.g. `CN=client-1`)
    // when the subject is non-empty; cert leaves with an empty
    // subject are represented as the empty string.
    let subject_dn = cert.subject_name().user_friendly_str().unwrap_or_default();

    // The identity is the Subject DN, falling back to a
    // fingerprint-prefixed sentinel when the subject is empty.
    let identity = if subject_dn.is_empty() {
        let mut s = String::with_capacity("mtls:fp:".len() + 32);
        s.push_str("mtls:fp:");
        for byte in &cert_fingerprint[..16] {
            s.push(HEX[usize::from(byte >> 4)] as char);
            s.push(HEX[usize::from(byte & 0x0f)] as char);
        }
        s
    } else {
        subject_dn.clone()
    };

    Some(MtlsPrincipal {
        identity,
        subject_dn,
        cert_fingerprint,
    })
}

/// Companion to [`principal_from_cert`] that returns the
/// [`ChannelIdentity`] (always-on audit identity, regardless of
/// [`IdentitySource`]). The two are derived from the same cert so we
/// extract them via the same path.
#[must_use]
pub fn channel_identity_from_cert(leaf_der: &[u8]) -> Option<ChannelIdentity> {
    let p = principal_from_cert(leaf_der)?;
    Some(ChannelIdentity {
        identity: p.identity,
        subject_dn: p.subject_dn,
        cert_fingerprint: p.cert_fingerprint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `MtlsPrincipal` with a non-empty Subject DN should produce a
    /// `mtls:dn:` stable id.
    #[test]
    fn stable_id_uses_subject_dn() {
        let p = MtlsPrincipal {
            identity: "CN=client-1".to_string(),
            subject_dn: "CN=client-1".to_string(),
            cert_fingerprint: [0x42; 32],
        };
        assert_eq!(
            p.stable_id()
                .expect("stable_id is always Some for an MtlsPrincipal"),
            "mtls:dn:CN=client-1"
        );
        assert_eq!(p.username(), "CN=client-1");
        assert_eq!(p.groups(), &["read_write"]);
    }

    /// Two `MtlsPrincipal`s with the same Subject DN but different
    /// fingerprints (i.e. cert rotation — same identity, new cert)
    /// MUST produce the same `stable_id`. This is the cache-namespace
    /// stability contract: rotated certs of the same identity must
    /// keep the same per-principal cache namespace.
    #[test]
    fn stable_id_survives_cert_rotation() {
        let before = MtlsPrincipal {
            identity: "CN=client-1".to_string(),
            subject_dn: "CN=client-1".to_string(),
            cert_fingerprint: [0x11; 32],
        };
        let after = MtlsPrincipal {
            identity: "CN=client-1".to_string(),
            subject_dn: "CN=client-1".to_string(),
            cert_fingerprint: [0x22; 32], // rotated
        };
        assert_eq!(before.stable_id(), after.stable_id());
    }

    /// Empty Subject → fingerprint fallback. Cache will invalidate
    /// on rotation in this case (acceptable trade-off for a
    /// degenerate operator setup).
    #[test]
    fn stable_id_falls_back_to_fingerprint() {
        let p = MtlsPrincipal {
            identity: "mtls:fp:0011223344556677".to_string(),
            subject_dn: String::new(),
            cert_fingerprint: [
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
        };
        assert_eq!(
            p.stable_id()
                .expect("stable_id is always Some for an MtlsPrincipal"),
            "mtls:fp:00112233445566778899aabbccddeeff"
        );
    }
}
