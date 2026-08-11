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

//! Out-of-band HTTPS enrollment and renewal against the Spice Cloud
//! control plane (state plane).
//!
//! Post-DR-025 the gateway is stateless: it holds no CA, cannot sign, and
//! rejects certless connections. Identity is therefore obtained **before**
//! any gRPC stream, via two plain-HTTPS endpoints on the cloud:
//!
//! - `POST /v1/cloud-connect/enroll` — first contact, carrying exactly one
//!   [`EnrollmentAuthority`]: a one-time `spice-enroll-` key, or a
//!   logged-in session's bearer token with the selected organization. The
//!   request carries the PKCS#10 CSR and host facts from the persisted
//!   [`EnrollmentDraft`], plus an `Idempotency-Key` naming the enrollment
//!   operation. The cloud consumes the authority, provisions the
//!   `instances` registry row, signs the CSR with the KMS CA, and returns
//!   the leaf + CA bundle + gateway address + the stable `instance_id` +
//!   organization/portal metadata. An exact operation replay returns the
//!   same instance instead of creating a sibling — that is what makes a
//!   lost response safe to retry, and what lets a **new** key recover the
//!   same operation after the first key expires.
//! - `POST /v1/cloud-connect/renew` — ~12h cadence. Authenticated by dual
//!   proof-of-possession rather than mTLS (the presented cert may already
//!   be expired within the 30-day grace window): the CURRENT key signs the
//!   fresh CSR's DER bytes (`pop_sig`), and the NEW key proves itself via
//!   the CSR's self-signature. **Every renewal rotates the keypair.**
//!
//! HTTP status contract (both endpoints): 4xx responses other than 408/429
//! are authoritative rejections ([`Error::Denied`]) — retrying the same
//! request cannot succeed — while 408, 429, 5xx, and transport failures
//! are transient and retried with backoff. Successful response bodies that
//! cannot be read or decoded are also retryable because an unframed partial
//! body is indistinguishable from response loss; decoded-but-invalid response
//! fields are terminal under the operation's idempotency key.

use std::time::Duration;

use base64::Engine as _;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use snafu::{ResultExt, Snafu};
use zeroize::Zeroizing;

use crate::config::CloudConnectConfig;
use crate::draft::EnrollmentDraft;
use crate::enrollment_key::EnrollmentKey;
use crate::identity::{EnrollmentMaterial, Identity, IdentityStore};

/// Path of the cloud enroll endpoint, relative to the enroll base URL.
pub const ENROLL_PATH: &str = "/v1/cloud-connect/enroll";
/// Path of the cloud renew endpoint, relative to the enroll base URL.
pub const RENEW_PATH: &str = "/v1/cloud-connect/renew";

/// How long past the leaf's `not_after` a renewal is still accepted by the
/// cloud (mirrors the server-side grace). Past this the identity is dead
/// and a fresh enrollment key is required.
pub const RENEWAL_GRACE: Duration = Duration::from_hours(30 * 24);

/// The retry deadline for direct `spiced --token` (and service-install)
/// bootstrap: headless flows tolerate a long transient outage because
/// nobody is watching a prompt.
pub const HEADLESS_RETRY_DEADLINE: Duration = Duration::from_mins(10);

/// The retry deadline for interactive authenticated callers, where a person
/// is waiting on the terminal.
pub const INTERACTIVE_RETRY_DEADLINE: Duration = Duration::from_mins(2);

/// Full-jitter backoff base: the first retry window.
const RETRY_BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Full-jitter backoff ceiling: no retry window grows past this.
const RETRY_BACKOFF_CAP: Duration = Duration::from_secs(30);

/// `true` when the identity expired longer than [`RENEWAL_GRACE`] ago —
/// the cloud refuses to renew it, so only a fresh enrollment helps.
#[must_use]
pub fn past_renewal_grace(identity: &Identity) -> bool {
    identity.not_after_unix.is_some_and(|not_after| {
        crate::heartbeat::now_unix() >= not_after.saturating_add(RENEWAL_GRACE.as_secs())
    })
}

/// A logged-in Spice Cloud session's bearer token, wrapped so it cannot
/// leak through `Debug` and is wiped on drop. Constructed by callers that
/// own a login session; this crate only ever places it in the one
/// `Authorization` header that uses it.
#[derive(Clone)]
pub struct SessionToken(Zeroizing<String>);

impl SessionToken {
    #[must_use]
    pub fn new(token: String) -> Self {
        Self(Zeroizing::new(token))
    }

    /// The bearer token plaintext, for the `Authorization` header.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SessionToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SessionToken([REDACTED])")
    }
}

/// Exactly one authority enrolls an instance. The variants are mutually
/// exclusive by construction: a request carrying both a login session and
/// an enrollment key cannot be represented, mirroring the server contract
/// that rejects such a request.
#[derive(Debug, Clone)]
pub enum EnrollmentAuthority {
    /// A logged-in user enrolling directly: the session's bearer token plus
    /// the explicitly selected organization. No enrollment key is minted or
    /// sent on this path.
    AuthenticatedSession {
        access_token: SessionToken,
        /// The selected organization, sent as `X-Org-Name`.
        org: String,
    },
    /// A one-time `spice-enroll-` key, optionally asserting the
    /// organization it must belong to. A mismatch is rejected server-side
    /// before the key is consumed.
    Token {
        key: EnrollmentKey,
        expected_org: Option<String>,
    },
}

/// Machine-readable denial reason, parsed from the response body's `code`
/// field. Every variant is terminal for the request that provoked it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenialCode {
    /// The enrollment key is malformed or unknown (400/401 `invalid_token`).
    InvalidToken,
    /// The key expired before it was consumed (410 `expired_token`). A new
    /// key presented with the same operation recovers the same instance.
    ExpiredToken,
    /// The key was already consumed by a different operation
    /// (409 `consumed_token`).
    ConsumedToken,
    /// The key does not belong to the asserted `expected_org`
    /// (409 `org_mismatch`). The key was not consumed.
    OrgMismatch,
    /// The declared region label failed server validation
    /// (400 `invalid_region`).
    InvalidRegion,
    /// The operation exists with a different canonical request
    /// (409 `idempotency_mismatch`).
    IdempotencyMismatch,
    /// The request carried a field this endpoint no longer accepts
    /// (400 `unsupported_enrollment_field`).
    UnsupportedField,
    /// An authoritative rejection with no recognized code.
    Other,
}

impl DenialCode {
    fn parse(code: Option<&str>) -> Self {
        match code {
            Some("invalid_token") => Self::InvalidToken,
            Some("expired_token") => Self::ExpiredToken,
            Some("consumed_token") => Self::ConsumedToken,
            Some("org_mismatch") => Self::OrgMismatch,
            Some("invalid_region") => Self::InvalidRegion,
            Some("idempotency_mismatch") => Self::IdempotencyMismatch,
            Some("unsupported_enrollment_field") => Self::UnsupportedField,
            _ => Self::Other,
        }
    }

    /// One actionable next step for the operator, appended to terminal
    /// enrollment errors.
    #[must_use]
    pub fn remediation(self) -> &'static str {
        match self {
            Self::InvalidToken => {
                "Check the enrollment key was copied exactly, or mint a new one in the Spice Cloud portal"
            }
            Self::ExpiredToken => {
                "The enrollment key expired. Mint a new one in the Spice Cloud portal and retry; the retried enrollment resumes this instance's pending operation"
            }
            Self::ConsumedToken => {
                "The enrollment key was already used. Mint a new one in the Spice Cloud portal"
            }
            Self::OrgMismatch => {
                "The enrollment key belongs to a different organization than asserted; the key was not consumed. Re-check the organization or mint a key in the intended one"
            }
            Self::InvalidRegion => {
                "Use a region label of 2-64 lowercase letters, digits, or hyphens (for example 'us-west-2' or 'on-prem-syd'), or omit it"
            }
            Self::IdempotencyMismatch => {
                "This instance directory carries enrollment state from a different request. Restore the original enrollment draft for this directory or contact Spice Cloud support; deleting it may create a sibling instance"
            }
            Self::UnsupportedField | Self::Other => "Fix the reported problem and retry",
        }
    }
}

/// Errors from the out-of-band enroll/renew HTTP flow.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build the HTTPS client for Spice Cloud enrollment: {source}"))]
    ClientBuild { source: reqwest::Error },

    #[snafu(display("Invalid CA certificate PEM for Spice Cloud enrollment: {source}"))]
    CaCert { source: reqwest::Error },

    #[snafu(display("Failed to reach the Spice Cloud endpoint {url}: {source}"))]
    Http { url: String, source: reqwest::Error },

    #[snafu(display("Failed to read the Spice Cloud response from {url}: {source}"))]
    ResponseBody { url: String, source: reqwest::Error },

    #[snafu(display("Failed to decode the Spice Cloud response from {url}: {reason}"))]
    ResponseDecode { url: String, reason: String },

    #[snafu(display(
        "Failed to reach the Spice Cloud endpoint {url}: its TLS certificate was rejected as \
         outside its validity period, which almost always means this host's clock is wrong: \
         {advice}. See: https://spiceai.org/docs"
    ))]
    CertificateValidity {
        url: String,
        /// The measured skew and its fix, or the generic clock check when the
        /// offset could not be measured.
        advice: String,
    },

    #[snafu(display("Spice Cloud rejected the request ({status}): {message}"))]
    Denied {
        status: u16,
        code: DenialCode,
        message: String,
    },

    #[snafu(display("Spice Cloud is temporarily unavailable ({status}): {message}"))]
    Unavailable {
        status: u16,
        message: String,
        /// Server-provided `Retry-After`, when present.
        retry_after: Option<Duration>,
    },

    #[snafu(display("Unexpected response from the Spice Cloud endpoint {url}: {reason}"))]
    InvalidResponse { url: String, reason: String },

    #[snafu(display("Failed to sign the renewal proof-of-possession: {reason}"))]
    ProofOfPossession { reason: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// `true` only when the cloud *authoritatively rejected* the request
    /// ([`Error::Denied`]): retrying the same request cannot succeed.
    ///
    /// Everything else is retryable: transport failures and 408/429/5xx are
    /// transient, [`Error::CertificateValidity`] (a skewed host clock) is
    /// fixable and the request never reached the cloud, and a local
    /// proof-of-possession failure never left this host.
    #[must_use]
    pub fn is_authoritative_rejection(&self) -> bool {
        matches!(self, Error::Denied { .. })
    }

    /// `true` when retrying the same enrollment operation cannot produce a
    /// different outcome. A denied request is authoritative, while a malformed
    /// decoded successful response with invalid required fields is committed
    /// under the operation's idempotency key and will therefore replay the same
    /// unusable semantics. Body transport and JSON decode failures remain
    /// retryable because an unframed partial body is indistinguishable from
    /// response loss.
    #[must_use]
    pub fn is_terminal_enrollment_failure(&self) -> bool {
        matches!(self, Error::Denied { .. } | Error::InvalidResponse { .. })
    }

    /// `true` only when the *credential itself* was rejected (HTTP 401).
    /// For renewal this is the revocation signal — the sole condition under
    /// which the on-disk identity may be cleared.
    #[must_use]
    pub fn is_credential_rejection(&self) -> bool {
        matches!(self, Error::Denied { status: 401, .. })
    }

    /// The server's `Retry-After`, when this error carried one.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Error::Unavailable { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

/// Host facts a standalone `spiced` reports at enroll — recorded on the
/// cloud `instances` registry row. `fingerprint` is the stable machine
/// identity: re-enrolling the same host lands on its existing row instead
/// of minting a duplicate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstanceFacts {
    pub fingerprint: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub runtime_version: String,
}

impl InstanceFacts {
    /// Gather the local host facts. All fields are guaranteed non-empty
    /// (the enroll endpoint rejects empty strings): unknown values degrade
    /// to `"unknown"` rather than failing enrollment.
    #[must_use]
    pub fn gather(runtime_version: &str) -> Self {
        let hostname = gethostname::gethostname().to_string_lossy().into_owned();
        Self {
            fingerprint: crate::fingerprint::compute(),
            hostname: non_empty_or_unknown(hostname),
            os: non_empty_or_unknown(std::env::consts::OS.to_string()),
            arch: non_empty_or_unknown(std::env::consts::ARCH.to_string()),
            runtime_version: non_empty_or_unknown(runtime_version.to_string()),
        }
    }
}

fn non_empty_or_unknown(value: String) -> String {
    if value.trim().is_empty() {
        "unknown".to_string()
    } else {
        value
    }
}

/// Wire body of the standalone enroll request. Exactly one authority rides
/// it: `token` (+ optional `expected_org`), or none of those fields with
/// the login authorization carried in headers instead.
#[derive(Serialize)]
struct EnrollRequest<'a> {
    /// Always `standalone` from this crate; the Kubernetes operator sends
    /// `cluster` on the same endpoint.
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expected_org: Option<&'a str>,
    csr_pem: &'a str,
    /// The instance's X25519 encryption public key (RFC 8410 SPKI PEM).
    /// The cloud records it and HPKE-seals secret payloads to it.
    enc_pubkey_pem: &'a str,
    instance: &'a InstanceFacts,
    /// Where this instance runs. A **sibling of `instance`, not a member of
    /// it**: everything in [`InstanceFacts`] is probed from the host, while
    /// the region is whatever the operator declared (`--region`).
    ///
    /// Omitted (never `null`) when unset — the cloud reads absence as "leave
    /// the stored region alone", so a re-enroll cannot erase a region set in
    /// the portal.
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<&'a str>,
}

/// The organization an enrollment landed in, from the canonical response.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Organization {
    pub id: i64,
    pub name: String,
}

/// Wire shape of a successful enroll response (the canonical CLOUD-2
/// contract; the old flat `app_name`/`org` projection does not exist).
#[derive(Deserialize)]
struct EnrollResponseWire {
    instance_id: String,
    identity_cert_pem: String,
    ca_bundle_pem: String,
    gateway_addr: String,
    not_after: String,
    organization: Organization,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    portal: Option<PortalWire>,
}

#[derive(Deserialize)]
struct PortalWire {
    #[serde(default)]
    new_project_url: Option<String>,
}

/// Parsed result of a successful enrollment.
#[derive(Debug)]
pub struct EnrollOutcome {
    /// The instance's stable external id — becomes `Identity::identifier`.
    pub instance_id: String,
    pub identity_cert_pem: String,
    pub ca_bundle_pem: String,
    /// Gateway `host:port` the mTLS `CloudConnect` stream connects to.
    pub gateway_addr: String,
    /// Leaf expiry, Unix seconds.
    pub not_after_unix: u64,
    /// The canonical non-credential metadata the response carried.
    pub metadata: EnrollmentMetadata,
}

/// The non-credential facts an enrollment response reports: which
/// organization the instance landed in, the recorded host-location label,
/// and where to create a project for it.
#[derive(Debug, Clone)]
pub struct EnrollmentMetadata {
    pub organization: Organization,
    /// The region on the registry row after this enroll, when reported.
    pub region: Option<String>,
    /// Cloud-provided portal deep link for creating a project with this
    /// instance preselected. Contains stable identifiers only, never
    /// credentials.
    pub new_project_url: Option<String>,
}

#[derive(Serialize)]
struct RenewRequest<'a> {
    cert_pem: &'a str,
    csr_pem: &'a str,
    pop_sig: &'a str,
    /// The freshly-generated X25519 encryption public key (RFC 8410 SPKI
    /// PEM). The cloud records it in the same transaction that rotates the
    /// identity key, and seals to it from that commit on.
    ///
    /// Required, not optional: the endpoint rejects a renewal that omits it,
    /// and an encryption key that never rotated would outlive the identity it
    /// belongs to.
    ///
    /// Not covered by `pop_sig`, which signs the CSR DER alone — this field's
    /// integrity rests on the server-authenticated TLS to the cloud.
    enc_pubkey_pem: &'a str,
}

/// Wire shape of a successful renew response.
#[derive(Deserialize)]
struct RenewResponseWire {
    identity_cert_pem: String,
    not_after: String,
}

/// Parsed result of a successful renewal (the CA bundle and gateway address
/// are unchanged by renewal and are not re-sent).
#[derive(Debug)]
pub struct RenewOutcome {
    pub identity_cert_pem: String,
    /// New leaf expiry, Unix seconds.
    pub not_after_unix: u64,
}

/// Error body shape the cloud endpoints return:
/// `{ "code": "...", "error": "...", "retryable": bool }`, all optional so
/// older/simpler error bodies still surface their message.
#[derive(Deserialize)]
struct ErrorBody {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

/// HTTP client for the cloud enroll/renew endpoints.
pub(crate) struct EnrollClient {
    http: reqwest::Client,
    enroll_url: String,
    renew_url: String,
    /// Base URL and trust roots, retained so a TLS handshake failure can be
    /// diagnosed for clock skew (see [`crate::clock_skew::diagnose`]).
    base_url: String,
    ca_cert_pem: Option<String>,
}

impl EnrollClient {
    /// Build a client for the configured enroll endpoint. When
    /// `config.ca_cert_pem` is set (dev/self-hosted control planes), those
    /// roots are added to the trust store; otherwise the system roots are
    /// used — the production path, where the cloud serves a
    /// publicly-trusted certificate.
    pub(crate) fn new(config: &CloudConnectConfig) -> Result<Self> {
        let mut builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            // Enrollment authorities ride in either the request body or an
            // Authorization header. A 307/308 preserves the body, so header
            // sanitization alone cannot prevent a cross-origin disclosure.
            .redirect(spice_cloud_client::redirect::same_origin_redirect_policy());
        if let Some(ref ca_pem) = config.ca_cert_pem {
            for cert in
                reqwest::Certificate::from_pem_bundle(ca_pem.as_bytes()).context(CaCertSnafu)?
            {
                builder = builder.add_root_certificate(cert);
            }
        }
        let http = builder.build().context(ClientBuildSnafu)?;
        let base = config.enroll_endpoint.trim_end_matches('/');
        Ok(Self {
            http,
            enroll_url: format!("{base}{ENROLL_PATH}"),
            renew_url: format!("{base}{RENEW_PATH}"),
            base_url: base.to_string(),
            ca_cert_pem: config.ca_cert_pem.clone(),
        })
    }

    /// Measure this host's clock against the cloud for a certificate-validity
    /// failure, falling back to a generic clock check when no measurement can
    /// be made (the probe itself failed, or a proxy stripped `Date`).
    async fn clock_advice(&self) -> String {
        match crate::clock_skew::diagnose(&self.base_url, self.ca_cert_pem.as_deref()).await {
            Some(skew) if skew.is_significant() => skew.advice(),
            // A measured-but-small skew means the certificate really is
            // outside its window for another reason, so do not claim a clock
            // problem that is not there.
            Some(_) => format!(
                "this host's clock agrees with Spice Cloud, so the certificate is genuinely \
                 outside its validity period — check for a TLS-intercepting proxy on this \
                 network. Host time is {}",
                chrono::Utc::now().to_rfc3339()
            ),
            None => format!(
                "check this host's clock (currently {}) and enable NTP time synchronization \
                 (for example `sudo timedatectl set-ntp true`)",
                chrono::Utc::now().to_rfc3339()
            ),
        }
    }

    /// One enrollment attempt: present the authority, the draft's CSR and
    /// encryption public key, the host facts, and the optional declared
    /// region, all under the operation's `Idempotency-Key`.
    pub(crate) async fn enroll(
        &self,
        authority: &EnrollmentAuthority,
        operation_id: &str,
        material: &EnrollmentMaterial,
        facts: &InstanceFacts,
        region: Option<&str>,
    ) -> Result<EnrollOutcome> {
        let (token, expected_org) = match authority {
            EnrollmentAuthority::Token { key, expected_org } => {
                (Some(key.expose_secret()), expected_org.as_deref())
            }
            EnrollmentAuthority::AuthenticatedSession { .. } => (None, None),
        };
        let request = EnrollRequest {
            kind: "standalone",
            token,
            expected_org,
            csr_pem: &material.csr_pem,
            enc_pubkey_pem: &material.enc_public_key_pem,
            instance: facts,
            region,
        };

        let mut builder = self
            .http
            .post(&self.enroll_url)
            .header("Idempotency-Key", operation_id)
            .json(&request);
        // Exactly one enrollment authority: the token variant rides in the
        // body above and MUST NOT add an Authorization header; the
        // authenticated variant is headers-only and the body carries no
        // token fields.
        if let EnrollmentAuthority::AuthenticatedSession { access_token, org } = authority {
            builder = builder
                .bearer_auth(access_token.expose_secret())
                .header("X-Org-Name", org);
        }

        let sensitive = match authority {
            EnrollmentAuthority::Token { key, .. } => Some(key.expose_secret()),
            EnrollmentAuthority::AuthenticatedSession { access_token, .. } => {
                Some(access_token.expose_secret())
            }
        };
        let wire: EnrollResponseWire = self.send(&self.enroll_url, builder, sensitive).await?;
        let not_after_unix = parse_not_after(&self.enroll_url, &wire.not_after)?;
        // These fields become the durable identity after the one-time key is
        // consumed. Reject an unusable successful response before promotion;
        // the idempotency key makes this terminal rather than retryable.
        ensure_response_field(&self.enroll_url, "instance_id", &wire.instance_id)?;
        ensure_response_field(
            &self.enroll_url,
            "identity_cert_pem",
            &wire.identity_cert_pem,
        )?;
        ensure_response_field(&self.enroll_url, "gateway_addr", &wire.gateway_addr)?;
        Ok(EnrollOutcome {
            instance_id: wire.instance_id,
            identity_cert_pem: wire.identity_cert_pem,
            ca_bundle_pem: wire.ca_bundle_pem,
            gateway_addr: wire.gateway_addr,
            not_after_unix,
            metadata: EnrollmentMetadata {
                organization: wire.organization,
                region: wire.region,
                new_project_url: wire.portal.and_then(|p| p.new_project_url),
            },
        })
    }

    /// Renew the identity with a fresh keypair (`material`), presenting the
    /// current leaf and the current-key proof-of-possession signature over
    /// the new CSR. Works within the grace window even when the presented
    /// leaf is already expired.
    ///
    /// `material` also carries the freshly-generated X25519 encryption key: its
    /// public half rides this request so the cloud re-pins both keys in one
    /// atomic update, and the caller installs the private half as current while
    /// retaining the outgoing one for a single rotation.
    pub(crate) async fn renew(
        &self,
        current: &Identity,
        material: &EnrollmentMaterial,
    ) -> Result<RenewOutcome> {
        let pop_sig = sign_pop(&current.private_key_pem, &material.csr_pem)?;
        let request = RenewRequest {
            cert_pem: &current.identity_cert_pem,
            csr_pem: &material.csr_pem,
            pop_sig: &pop_sig,
            enc_pubkey_pem: &material.enc_public_key_pem,
        };
        let builder = self.http.post(&self.renew_url).json(&request);
        let wire: RenewResponseWire = self.send(&self.renew_url, builder, None).await?;
        let not_after_unix = parse_not_after(&self.renew_url, &wire.not_after)?;
        ensure_response_field(
            &self.renew_url,
            "identity_cert_pem",
            &wire.identity_cert_pem,
        )?;
        Ok(RenewOutcome {
            identity_cert_pem: wire.identity_cert_pem,
            not_after_unix,
        })
    }

    /// Classify a syntactically successful renewal response whose issued
    /// credential cannot be used with the key material sent in the request.
    pub(crate) fn invalid_renew_response(&self, reason: &'static str) -> Error {
        Error::InvalidResponse {
            url: self.renew_url.clone(),
            reason: format!("issued identity cannot reconnect: {reason}"),
        }
    }

    /// Send a prepared request and decode/classify the response.
    async fn send<Resp: DeserializeOwned>(
        &self,
        url: &str,
        builder: reqwest::RequestBuilder,
        sensitive: Option<&str>,
    ) -> Result<Resp> {
        let response = match builder.send().await {
            Ok(response) => response,
            Err(source) => {
                // A TLS validity rejection is the shape a wrong host clock
                // produces at every layer of this flow. Diagnose it here
                // rather than handing the operator a bare certificate error.
                if crate::clock_skew::looks_like_certificate_validity_failure(&source) {
                    return Err(Error::CertificateValidity {
                        url: url.to_string(),
                        advice: self.clock_advice().await,
                    });
                }
                return Err(Error::Http {
                    url: url.to_string(),
                    source,
                });
            }
        };

        // Every Spice Cloud response carries `Date`, so measure the host's
        // clock against it whether or not the request succeeded — the offset
        // is the difference between "the cloud is broken" and "this host's
        // clock is wrong", and it costs nothing to read.
        let skew = response
            .headers()
            .get(reqwest::header::DATE)
            .and_then(|value| value.to_str().ok())
            .and_then(crate::clock_skew::from_date_header)
            .filter(|skew| skew.is_significant());
        if let Some(ref skew) = skew {
            tracing::warn!("Cloud Connect: {}", skew.advice());
        }

        let status = response.status();
        if status.is_success() {
            let body = response
                .bytes()
                .await
                .map_err(|source| Error::ResponseBody {
                    url: url.to_string(),
                    source,
                })?;
            return serde_json::from_slice::<Resp>(&body).map_err(|source| {
                let reason = format!("failed to decode response body: {source}");
                Error::ResponseDecode {
                    url: url.to_string(),
                    reason: redact_sensitive(&reason, sensitive),
                }
            });
        }

        let retry_after = parse_retry_after(response.headers());

        // Non-2xx: surface the server's `{code, error}` body when present,
        // falling back to a bounded slice of the raw body. Append the
        // measured skew when there is one — a host hours out of step can
        // have its CSR refused, and the cloud's message alone would not say
        // why. A proxy or defensive server can echo a rejected credential,
        // so redact the authority before parsing or bounding the message and
        // again after JSON decoding (escapes can reconstruct it).
        let (code, message) = match response.text().await {
            Ok(text) => parse_error_body(&text, sensitive),
            Err(_) => (None, String::new()),
        };
        let message = match skew {
            Some(skew) => format!("{message} ({})", skew.advice()),
            None => message,
        };

        // 5xx is transient by definition; 429 (rate limit) and 408 (request
        // timeout) are the 4xx statuses that are also transient — treating
        // them as authoritative rejections would burn a single-use key or
        // clear a still-valid identity over a throttle.
        let transient = status.is_server_error()
            || status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::REQUEST_TIMEOUT;
        if transient {
            Err(Error::Unavailable {
                status: status.as_u16(),
                message,
                retry_after,
            })
        } else {
            Err(Error::Denied {
                status: status.as_u16(),
                code: DenialCode::parse(code.as_deref()),
                message,
            })
        }
    }
}

fn redact_sensitive(text: &str, sensitive: Option<&str>) -> String {
    match sensitive.filter(|value| !value.is_empty()) {
        Some(value) => text.replace(value, "[REDACTED]"),
        None => text.to_string(),
    }
}

fn parse_error_body(text: &str, sensitive: Option<&str>) -> (Option<String>, String) {
    let text = redact_sensitive(text, sensitive);
    match serde_json::from_str::<ErrorBody>(&text) {
        Ok(body) => (
            body.code.map(|code| redact_sensitive(&code, sensitive)),
            body.error.map_or_else(
                || error_body_fallback(&text, sensitive),
                |error| redact_sensitive(&error, sensitive),
            ),
        ),
        Err(_) => (None, error_body_fallback(&text, sensitive)),
    }
}

fn error_body_fallback(text: &str, sensitive: Option<&str>) -> String {
    if sensitive.is_some_and(|value| !value.is_empty()) {
        "the response contained no usable error message".to_string()
    } else {
        bounded(text, 256)
    }
}

fn ensure_response_field(url: &str, name: &str, value: &str) -> Result<()> {
    snafu::ensure!(
        !value.trim().is_empty(),
        InvalidResponseSnafu {
            url: url.to_string(),
            reason: format!("response carried an empty {name}"),
        }
    );
    Ok(())
}

/// Parse a `Retry-After` header: delta-seconds, or an HTTP-date converted
/// to a duration from now. `None` when absent or unparseable.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let raw = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = raw.trim().parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let date = chrono::DateTime::parse_from_rfc2822(raw.trim()).ok()?;
    let delta = date.timestamp() - chrono::Utc::now().timestamp();
    u64::try_from(delta).ok().map(Duration::from_secs)
}

/// How long a caller keeps retrying transient enrollment failures before
/// giving up. Both presets use full-jitter exponential backoff between
/// [`RETRY_BACKOFF_BASE`] and [`RETRY_BACKOFF_CAP`] and honor a shorter
/// server `Retry-After`.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub deadline: Duration,
}

impl RetryPolicy {
    /// The `spiced --token` / service-install bootstrap policy.
    pub const HEADLESS: Self = Self {
        deadline: HEADLESS_RETRY_DEADLINE,
    };
    /// The interactive authenticated-caller policy.
    pub const INTERACTIVE: Self = Self {
        deadline: INTERACTIVE_RETRY_DEADLINE,
    };
}

/// The window a retry sleep is drawn from: full jitter over
/// `min(cap, base * 2^attempt)`.
fn backoff_window(attempt: u32) -> Duration {
    let doubled = RETRY_BACKOFF_BASE.saturating_mul(1_u32.checked_shl(attempt).unwrap_or(u32::MAX));
    doubled.min(RETRY_BACKOFF_CAP)
}

/// The sleep before retry number `attempt` (0-based): uniform over the
/// backoff window. A shorter server `Retry-After` narrows the wait; a longer
/// one never expands the client's bounded backoff. Every path has a
/// one-millisecond floor so transient failures cannot busy-spin.
fn retry_sleep(attempt: u32, retry_after: Option<Duration>) -> Duration {
    let window = backoff_window(attempt);
    if let Some(after) = retry_after {
        after.min(window).max(Duration::from_millis(1))
    } else {
        let millis = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
        Duration::from_millis(rand::random_range(1..=millis))
    }
}

/// Drive `attempt` until it succeeds, is denied, or the deadline elapses.
///
/// Generic over the attempt so the pacing is testable with paused time and
/// injected outcomes; `enroll_now` passes the real HTTP attempt. Uses
/// `tokio::time` throughout, so `tokio::time::pause` governs it in tests.
async fn retry_until_deadline<T, F, Fut>(
    policy: RetryPolicy,
    mut attempt: F,
) -> std::result::Result<T, EnrollNowError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let started = tokio::time::Instant::now();
    let mut attempts: u32 = 0;
    loop {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) if error.is_terminal_enrollment_failure() => {
                return Err(EnrollNowError::Rejected { source: error });
            }
            Err(error) => error,
        };

        let sleep = retry_sleep(attempts, error.retry_after());
        let elapsed = started.elapsed();
        if elapsed.saturating_add(sleep) >= policy.deadline {
            return Err(EnrollNowError::DeadlineExceeded {
                deadline: policy.deadline,
                source: error,
            });
        }
        tracing::warn!(
            "Cloud Connect: enrollment attempt failed (retrying in {}ms, {}s of the retry budget left): {error}",
            sleep.as_millis(),
            policy.deadline.saturating_sub(elapsed).as_secs(),
        );
        tokio::time::sleep(sleep).await;
        attempts = attempts.saturating_add(1);
    }
}

/// Errors from the one-shot [`enroll_now`] flow.
#[derive(Debug, Snafu)]
pub enum EnrollNowError {
    #[snafu(display(
        "{source}. {}. See: https://spiceai.org/docs",
        denial_remediation(source)
    ))]
    Rejected { source: Error },

    #[snafu(display(
        "Enrollment did not succeed within the {}s retry budget; last failure: {source}. \
         Check connectivity to the Spice Cloud endpoint and retry. See: https://spiceai.org/docs",
        deadline.as_secs()
    ))]
    DeadlineExceeded { deadline: Duration, source: Error },

    #[snafu(display("{source}"))]
    Draft { source: crate::draft::Error },

    #[snafu(display("{source}"))]
    Client { source: Error },

    #[snafu(display(
        "The declared region {region:?} is not a valid region label. Expected 2-64 lowercase \
         letters, digits, or hyphens (for example 'us-west-2' or 'on-prem-syd'). \
         See: https://spiceai.org/docs"
    ))]
    InvalidRegion { region: String },

    #[snafu(display(
        "Failed to read the existing Cloud Connect identity at {}: {source}. \
         Fix or remove the file and retry. See: https://spiceai.org/docs",
        path.display()
    ))]
    IdentityUnreadable {
        path: std::path::PathBuf,
        source: crate::identity::Error,
    },

    #[snafu(display(
        "The existing Cloud Connect identity at {} cannot be used: {reason}. The supplied \
         enrollment authority was not redeemed. Stop spiced, remove this identity file, and \
         retry enrollment. See: https://spiceai.org/docs",
        path.display()
    ))]
    IdentityUnusable {
        path: std::path::PathBuf,
        reason: &'static str,
    },

    #[snafu(display(
        "Enrollment succeeded but the identity could not be persisted at {}: {source}. \
         Fix the directory (permissions/disk space) and retry with a new enrollment key; \
         the retried enrollment resumes this instance's pending operation instead of \
         creating a duplicate.",
        path.display()
    ))]
    Persist {
        path: std::path::PathBuf,
        source: crate::identity::Error,
    },
}

/// The remediation line for a denied enrollment, resolved from the denial
/// code when there is one.
fn denial_remediation(source: &Error) -> &'static str {
    match source {
        Error::Denied { code, .. } => code.remediation(),
        Error::InvalidResponse { .. } => {
            "Spice Cloud returned unusable data for this pending operation. Preserve enrollment-draft.json and contact Spice Cloud support before removing it or starting a new enrollment"
        }
        _ => "Fix the reported problem and retry",
    }
}

impl EnrollNowError {
    /// `true` when the cloud terminally rejected the enrollment: retrying
    /// the same request cannot succeed.
    #[must_use]
    pub fn is_terminal_rejection(&self) -> bool {
        matches!(
            self,
            Self::Rejected { .. }
                | Self::InvalidRegion { .. }
                | Self::IdentityUnreadable { .. }
                | Self::IdentityUnusable { .. }
        )
    }
}

/// Result of a successful [`enroll_now`].
#[derive(Debug)]
pub enum EnrollNowOutcome {
    /// A valid identity already existed for this directory, so the supplied
    /// authority was **not** redeemed and nothing about it was persisted.
    /// The identity is returned for the caller to report/reconnect with.
    AlreadyEnrolled { identity: Identity },
    /// Freshly enrolled: the identity is durable at `identity_path` and the
    /// enrollment draft has been promoted away.
    Enrolled {
        identity: Identity,
        metadata: EnrollmentMetadata,
    },
}

/// Operation-aware one-shot enrollment: the typed entry point `spiced
/// --token` uses directly.
///
/// 1. **Existing identity wins.** A readable identity that is not past the
///    renewal grace window short-circuits to
///    [`EnrollNowOutcome::AlreadyEnrolled`]; the supplied authority is not
///    redeemed and not persisted. (An expired-but-in-grace identity still
///    wins — the driver renews it.) A readable but unusable identity fails
///    closed with removal guidance; it never causes implicit re-enrollment.
/// 2. The per-directory [`EnrollmentDraft`] is loaded or created: the same
///    operation ID and key material back every retry, so a lost response —
///    or a fresh key presented after the first one expired — recovers the
///    same instance instead of enrolling a sibling.
/// 3. The enroll request is retried under `retry` (full-jitter 1–30s
///    backoff, honoring a shorter server `Retry-After`) until it succeeds,
///    is terminally denied, or the deadline elapses.
/// 4. On success the draft material and response are atomically promoted to
///    `identity.json` (owner-only, atomic rename) and the draft is deleted.
///    Only then is the enrollment durable — callers gate readiness on this
///    function returning.
///
/// The enrollment key is never persisted anywhere by this flow; only the
/// provisional key material, operation ID, and non-secret retry-stable request
/// facts are (in the draft).
///
/// # Errors
///
/// - [`EnrollNowError::Rejected`] — the cloud terminally rejected it.
/// - [`EnrollNowError::DeadlineExceeded`] — retryable failures outlasted
///   the policy deadline.
/// - [`EnrollNowError::InvalidRegion`] — the declared region label is
///   malformed (checked before any request).
/// - [`EnrollNowError::IdentityUnreadable`] / [`EnrollNowError::IdentityUnusable`]
///   — an identity file exists but cannot be safely reused; refusing to guess
///   beats silently re-enrolling over a live instance.
/// - [`EnrollNowError::Draft`] / [`EnrollNowError::Client`] /
///   [`EnrollNowError::Persist`] — local state or client construction
///   failures.
pub async fn enroll_now(
    config: &CloudConnectConfig,
    authority: &EnrollmentAuthority,
    retry: RetryPolicy,
) -> std::result::Result<EnrollNowOutcome, EnrollNowError> {
    // Existing identity wins without redeeming the supplied authority.
    let identity_path = config.identity_path.clone();
    let existing = tokio::task::spawn_blocking({
        let path = identity_path.clone();
        move || IdentityStore::load_optional(&path)
    })
    .await
    .unwrap_or_else(|join| {
        Err(crate::identity::Error::Io {
            path: identity_path.clone(),
            source: std::io::Error::other(format!("identity load task panicked: {join}")),
        })
    })
    .map_err(|source| EnrollNowError::IdentityUnreadable {
        path: identity_path.clone(),
        source,
    })?;
    if let Some(identity) = existing {
        if past_renewal_grace(&identity) {
            tracing::warn!(
                "Cloud Connect: the stored identity at {} expired past the renewal grace window; enrolling this instance again",
                identity_path.display()
            );
        } else {
            if let Some(reason) =
                identity.reconnect_validation_error(config.gateway_endpoint.as_deref())
            {
                return Err(EnrollNowError::IdentityUnusable {
                    path: identity_path,
                    reason,
                });
            }
            cleanup_enrollment_draft(config.config_dir.clone()).await;
            return Ok(EnrollNowOutcome::AlreadyEnrolled { identity });
        }
    }

    if let Some(ref region) = config.instance_region
        && !crate::is_valid_instance_region(region)
    {
        return Err(EnrollNowError::InvalidRegion {
            region: region.clone(),
        });
    }

    let facts = InstanceFacts::gather(&config.runtime_version);
    let config_dir = config.config_dir.clone();
    let draft_facts = facts.clone();
    let draft_region = config.instance_region.clone();
    let draft = tokio::task::spawn_blocking({
        let config_dir = config_dir.clone();
        move || EnrollmentDraft::load_or_create(&config_dir, &draft_facts, draft_region.as_deref())
    })
    .await
    .unwrap_or_else(|join| {
        Err(crate::draft::Error::Io {
            path: EnrollmentDraft::path_in(&config_dir),
            source: std::io::Error::other(format!("draft task panicked: {join}")),
        })
    })
    .context(DraftSnafu)?;

    let client = EnrollClient::new(config).context(ClientSnafu)?;
    let material = draft.material();

    let outcome = retry_until_deadline(retry, || {
        client.enroll(
            authority,
            &draft.enrollment_operation_id,
            &material,
            &draft.instance,
            draft.region.as_deref(),
        )
    })
    .await?;

    // Atomic promotion: the draft's provisional key material becomes the
    // identity, written owner-only via atomic rename; the draft is deleted
    // only after the identity is durable.
    let mut identity = Identity {
        identifier: outcome.instance_id,
        identity_cert_pem: outcome.identity_cert_pem,
        private_key_pem: material.private_key_pem,
        public_key_pem: material.public_key_pem,
        ca_bundle_pem: outcome.ca_bundle_pem,
        gateway_addr: outcome.gateway_addr,
        not_after_unix: Some(outcome.not_after_unix),
        app_id: None,
        enc_private_key_pem: material.enc_private_key_pem,
        enc_public_key_pem: material.enc_public_key_pem,
        // A fresh enrollment has no prior key to retain.
        enc_previous_private_key_pem: String::new(),
        // Minted below so an identity always leaves enrollment able to write
        // its delivered-secrets cache.
        cache_key_b64: String::new(),
    };
    identity.ensure_cache_key();

    // Required strings are checked while decoding the HTTP response, but a
    // non-empty certificate can still be malformed or belong to another key.
    // Fail before promotion so an unusable credential never replaces the
    // retryable draft. The operation is already committed under its
    // idempotency key, making this a terminal response error with support
    // guidance rather than an instruction to discard the draft.
    if let Some(reason) = identity.reconnect_validation_error(config.gateway_endpoint.as_deref()) {
        return Err(EnrollNowError::Rejected {
            source: Error::InvalidResponse {
                url: client.enroll_url.clone(),
                reason: format!("issued identity cannot reconnect: {reason}"),
            },
        });
    }

    let to_store = identity.clone();
    let store_path = config.identity_path.clone();
    let stored = tokio::task::spawn_blocking(move || IdentityStore::store(&store_path, &to_store))
        .await
        .unwrap_or_else(|join| {
            Err(crate::identity::Error::Io {
                path: config.identity_path.clone(),
                source: std::io::Error::other(format!(
                    "identity persistence task panicked: {join}"
                )),
            })
        });
    stored.context(PersistSnafu {
        path: config.identity_path.clone(),
    })?;

    // The identity is durable; retire the draft. A failure here only leaves
    // a stale draft behind — enrollment never runs again while the identity
    // exists, so it is a warning, not an error.
    cleanup_enrollment_draft(config.config_dir.clone()).await;

    Ok(EnrollNowOutcome::Enrolled {
        identity,
        metadata: outcome.metadata,
    })
}

async fn cleanup_enrollment_draft(config_dir: std::path::PathBuf) {
    let deleted = tokio::task::spawn_blocking(move || EnrollmentDraft::delete(&config_dir)).await;
    match deleted {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            tracing::warn!("Cloud Connect: could not remove the promoted enrollment draft: {err}");
        }
        Err(join) => {
            tracing::warn!("Cloud Connect: the enrollment draft cleanup task panicked: {join}");
        }
    }
}

/// Sign the renewal proof-of-possession: a DER-encoded ECDSA P-256/SHA-256
/// signature over the CSR's DER bytes, made with the **current** identity's
/// private key, base64-encoded. This authorizes the rotation — a leaked
/// certificate alone (which is not a secret) must not be able to renew.
pub(crate) fn sign_pop(current_private_key_pem: &str, csr_pem: &str) -> Result<String> {
    let csr = pem::parse(csr_pem).map_err(|source| Error::ProofOfPossession {
        reason: format!("CSR is not valid PEM: {source}"),
    })?;
    sign_pop_payload(current_private_key_pem, csr.contents())
        .map_err(|reason| Error::ProofOfPossession { reason })
}

/// Sign an arbitrary proof-of-possession payload with an identity's private
/// key: a DER-encoded ECDSA P-256/SHA-256 signature over `payload`,
/// base64-encoded. `/renew` signs a CSR's DER bytes ([`sign_pop`]);
/// `/release` signs its own domain-separated
/// `spice-cloud-connect/release/v1\n{instance_id}` payload.
///
/// The error is the bare reason rather than a typed error so each flow names
/// itself in the error it surfaces, instead of nesting one flow's message
/// inside another's.
pub(crate) fn sign_pop_payload(
    private_key_pem: &str,
    payload: &[u8],
) -> std::result::Result<String, String> {
    let key = pem::parse(private_key_pem)
        .map_err(|source| format!("current private key is not valid PEM: {source}"))?;

    // aws-lc-rs is the same backend rcgen generated the keypair with (see
    // Cargo.toml), so the persisted PKCS#8 always round-trips here.
    let key_pair = aws_lc_rs::signature::EcdsaKeyPair::from_pkcs8(
        &aws_lc_rs::signature::ECDSA_P256_SHA256_ASN1_SIGNING,
        key.contents(),
    )
    .map_err(|source| format!("current private key is not a PKCS#8 ECDSA P-256 key: {source}"))?;
    let rng = aws_lc_rs::rand::SystemRandom::new();
    let signature = key_pair
        .sign(&rng, payload)
        .map_err(|source| format!("signing failed: {source}"))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(signature.as_ref()))
}

/// Parse an RFC 3339 `not_after` timestamp from an enroll/renew response
/// into Unix seconds.
pub(crate) fn parse_not_after(url: &str, value: &str) -> Result<u64> {
    let parsed =
        chrono::DateTime::parse_from_rfc3339(value).map_err(|source| Error::InvalidResponse {
            url: url.to_string(),
            reason: format!("invalid not_after timestamp: {source}"),
        })?;
    u64::try_from(parsed.timestamp()).map_err(|_| Error::InvalidResponse {
        url: url.to_string(),
        reason: "not_after timestamp is before the Unix epoch".to_string(),
    })
}

/// Longest prefix of `s` no longer than `max` bytes on a char boundary.
fn bounded(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::IdentityStore;

    fn test_key() -> EnrollmentKey {
        EnrollmentKey::parse(&format!("spice-enroll-{}", "A".repeat(32))).expect("valid key")
    }

    /// Extract the uncompressed EC point from a P-256 SPKI DER blob: the
    /// final 65 bytes (0x04 || X || Y). Test-only shortcut — production
    /// verification happens server-side.
    fn p256_point_from_spki(spki_der: &[u8]) -> &[u8] {
        assert!(spki_der.len() > 65, "SPKI too short for a P-256 key");
        &spki_der[spki_der.len() - 65..]
    }

    #[test]
    fn pop_signature_verifies_against_current_public_key() {
        // The "current" identity keypair signs; a fresh CSR is what gets
        // signed — mirroring a real rotation.
        let current = IdentityStore::generate_enrollment().expect("current material");
        let next = IdentityStore::generate_enrollment().expect("next material");

        let sig_b64 = sign_pop(&current.private_key_pem, &next.csr_pem).expect("sign pop");
        let sig = base64::engine::general_purpose::STANDARD
            .decode(sig_b64)
            .expect("valid base64");

        let spki = pem::parse(&current.public_key_pem).expect("public key PEM");
        let csr = pem::parse(&next.csr_pem).expect("csr PEM");
        let key = aws_lc_rs::signature::UnparsedPublicKey::new(
            &aws_lc_rs::signature::ECDSA_P256_SHA256_ASN1,
            p256_point_from_spki(spki.contents()),
        );
        key.verify(csr.contents(), &sig)
            .expect("pop signature must verify against the current public key");
    }

    #[test]
    fn pop_signature_rejects_wrong_key() {
        let current = IdentityStore::generate_enrollment().expect("current material");
        let other = IdentityStore::generate_enrollment().expect("other material");
        let next = IdentityStore::generate_enrollment().expect("next material");

        let sig_b64 = sign_pop(&current.private_key_pem, &next.csr_pem).expect("sign pop");
        let sig = base64::engine::general_purpose::STANDARD
            .decode(sig_b64)
            .expect("valid base64");

        let spki = pem::parse(&other.public_key_pem).expect("public key PEM");
        let csr = pem::parse(&next.csr_pem).expect("csr PEM");
        let key = aws_lc_rs::signature::UnparsedPublicKey::new(
            &aws_lc_rs::signature::ECDSA_P256_SHA256_ASN1,
            p256_point_from_spki(spki.contents()),
        );
        key.verify(csr.contents(), &sig)
            .expect_err("signature must not verify against a different key");
    }

    #[test]
    fn sign_pop_rejects_garbage_key() {
        let material = IdentityStore::generate_enrollment().expect("material");
        let err = sign_pop("not a pem", &material.csr_pem).expect_err("must fail");
        assert!(
            matches!(err, Error::ProofOfPossession { .. }),
            "a local signing failure is a proof-of-possession error"
        );
        // A local PoP/crypto failure is NOT an authoritative cloud rejection:
        // it must never burn an un-consumed key or clear a valid identity —
        // the caller retries instead.
        assert!(
            !err.is_authoritative_rejection(),
            "a local signing failure must not be treated as a cloud rejection"
        );
    }

    #[test]
    fn parse_not_after_accepts_rfc3339() {
        let secs = parse_not_after("http://test", "2026-07-23T01:02:03.000Z").expect("parse");
        assert_eq!(secs, 1_784_768_523);
    }

    #[test]
    fn parse_not_after_rejects_garbage() {
        parse_not_after("http://test", "tomorrow-ish").expect_err("must fail");
    }

    #[test]
    fn parse_not_after_rejects_pre_epoch() {
        parse_not_after("http://test", "1899-01-01T00:00:00Z")
            .expect_err("pre-epoch timestamps cannot be a cert expiry");
    }

    #[test]
    fn required_response_fields_reject_empty_or_whitespace_values() {
        for (name, value) in [
            ("instance_id", ""),
            ("identity_cert_pem", " \n\t"),
            ("gateway_addr", "  "),
        ] {
            let err = ensure_response_field("http://test", name, value)
                .expect_err("empty required field must fail");
            assert!(
                matches!(err, Error::InvalidResponse { .. }),
                "{name}: {err}"
            );
        }
    }

    #[test]
    fn rejection_classification() {
        // Only an authoritative 4xx cloud rejection is terminal; everything
        // else is retryable.
        let denied = Error::Denied {
            status: 401,
            code: DenialCode::InvalidToken,
            message: "unknown enrollment key".to_string(),
        };
        assert!(denied.is_authoritative_rejection());
        assert!(denied.is_terminal_enrollment_failure());
        assert!(denied.is_credential_rejection());

        let unavailable = Error::Unavailable {
            status: 503,
            message: String::new(),
            retry_after: None,
        };
        assert!(!unavailable.is_authoritative_rejection());
        assert!(!unavailable.is_terminal_enrollment_failure());

        let invalid_response = Error::InvalidResponse {
            url: "https://api.spice.ai/v1/cloud-connect/enroll".to_string(),
            reason: "response carried an empty instance_id".to_string(),
        };
        assert!(
            invalid_response.is_terminal_enrollment_failure(),
            "a successful response is committed under the idempotency key and cannot improve on replay"
        );
        assert!(
            !invalid_response.is_authoritative_rejection(),
            "an invalid response is not a credential revocation signal"
        );

        let pop = Error::ProofOfPossession {
            reason: "key material generation failed".to_string(),
        };
        assert!(!pop.is_authoritative_rejection());

        let skew = Error::CertificateValidity {
            url: "https://api.spice.ai/v1/cloud-connect/enroll".to_string(),
            advice: "host clock is 42 minutes behind Spice Cloud".to_string(),
        };
        // A skewed clock never reached the cloud, so the key is still live:
        // classifying this as authoritative would burn it.
        assert!(!skew.is_authoritative_rejection());
        assert!(
            skew.to_string().contains("clock"),
            "the message must name the clock: {skew}"
        );
    }

    #[tokio::test]
    async fn a_truncated_success_body_is_retryable_response_loss() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await.expect("read request");
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 128\r\nConnection: close\r\n\r\n{\"instance_id\":\"partial",
                )
                .await
                .expect("write truncated response");
        });

        let base = format!("http://{address}");
        let url = format!("{base}/truncated");
        let client = EnrollClient::new(&test_config(&base)).expect("client");
        let request = client.http.get(&url);
        let err = client
            .send::<serde_json::Value>(&url, request, None)
            .await
            .expect_err("a short body must fail");
        server.await.expect("test server task");

        assert!(matches!(err, Error::ResponseBody { .. }), "{err}");
        assert!(
            !err.is_terminal_enrollment_failure(),
            "an incomplete response can succeed when the operation is replayed"
        );
    }

    #[tokio::test]
    async fn an_unframed_partial_success_body_is_retryable_response_loss() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await.expect("read request");
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"instance_id\":\"partial",
                )
                .await
                .expect("write unframed partial response");
        });

        let base = format!("http://{address}");
        let url = format!("{base}/unframed-partial");
        let client = EnrollClient::new(&test_config(&base)).expect("client");
        let request = client.http.get(&url);
        let Err(err) = client.send::<EnrollResponseWire>(&url, request, None).await else {
            panic!("an incomplete JSON body must fail");
        };
        server.await.expect("test server task");

        assert!(matches!(err, Error::ResponseDecode { .. }), "{err}");
        assert!(
            !err.is_terminal_enrollment_failure(),
            "an unframed partial response must replay within the retry budget"
        );
    }

    #[tokio::test]
    async fn a_success_decode_error_redacts_the_enrollment_authority() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let token = format!("spice-enroll-{}", "S".repeat(32));
        let body = serde_json::to_string(&token).expect("encode echoed token");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await.expect("read request");
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write echoed response");
        });

        let base = format!("http://{address}");
        let url = format!("{base}/echo");
        let client = EnrollClient::new(&test_config(&base)).expect("client");
        let request = client.http.get(&url);
        let Err(err) = client
            .send::<EnrollResponseWire>(&url, request, Some(&token))
            .await
        else {
            panic!("a string is not an enrollment response");
        };
        server.await.expect("test server task");

        let rendered = err.to_string();
        assert!(!rendered.contains(&token), "decode error leaked the key");
        assert!(rendered.contains("[REDACTED]"), "{rendered}");
        assert!(matches!(err, Error::ResponseDecode { .. }));
    }

    #[tokio::test]
    async fn a_success_semantic_error_never_reproduces_the_enrollment_authority() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let token = format!("spice-enroll-{}", "A".repeat(32));
        let body = serde_json::json!({
            "instance_id": "inst_test",
            "identity_cert_pem": "certificate",
            "ca_bundle_pem": "ca",
            "gateway_addr": "gateway.test:443",
            "not_after": token.clone(),
            "organization": {"id": 42, "name": "acme"}
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 4096];
            let _ = socket.read(&mut request).await.expect("read request");
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write semantic-error response");
        });

        let base = format!("http://{address}");
        let client = EnrollClient::new(&test_config(&base)).expect("client");
        let authority = EnrollmentAuthority::Token {
            key: EnrollmentKey::parse(&token).expect("valid enrollment key"),
            expected_org: None,
        };
        let material = IdentityStore::generate_enrollment().expect("enrollment material");
        let Err(err) = client
            .enroll(&authority, "test-operation", &material, &test_facts(), None)
            .await
        else {
            panic!("the echoed authority is not a timestamp");
        };
        server.await.expect("test server task");

        let rendered = EnrollNowError::Rejected { source: err }.to_string();
        assert!(
            !rendered.contains(&token),
            "semantic response error leaked the key: {rendered}"
        );
        assert!(
            rendered.contains("invalid not_after timestamp"),
            "{rendered}"
        );
    }

    #[test]
    fn denial_codes_parse_from_the_contract_strings() {
        for (raw, expected) in [
            ("invalid_token", DenialCode::InvalidToken),
            ("expired_token", DenialCode::ExpiredToken),
            ("consumed_token", DenialCode::ConsumedToken),
            ("org_mismatch", DenialCode::OrgMismatch),
            ("invalid_region", DenialCode::InvalidRegion),
            ("idempotency_mismatch", DenialCode::IdempotencyMismatch),
            ("unsupported_enrollment_field", DenialCode::UnsupportedField),
            ("something_else", DenialCode::Other),
        ] {
            assert_eq!(DenialCode::parse(Some(raw)), expected, "{raw}");
        }
        assert_eq!(DenialCode::parse(None), DenialCode::Other);
    }

    #[test]
    fn token_requests_carry_the_key_and_no_login_fields() {
        let facts = test_facts();
        let request = serde_json::to_value(EnrollRequest {
            kind: "standalone",
            token: Some("spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"),
            expected_org: Some("acme"),
            csr_pem: "csr",
            enc_pubkey_pem: "enc",
            instance: &facts,
            region: None,
        })
        .expect("serialize token request");
        assert_eq!(request["kind"], "standalone");
        assert_eq!(
            request["token"],
            "spice-enroll-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        );
        assert_eq!(request["expected_org"], "acme");
    }

    #[test]
    fn authenticated_requests_carry_no_token_fields() {
        // The authenticated variant's authorization is headers-only; a body
        // carrying both authorities is unrepresentable, and this pins the
        // wire shape: no `token`, no `expected_org`.
        let facts = test_facts();
        let request = serde_json::to_value(EnrollRequest {
            kind: "standalone",
            token: None,
            expected_org: None,
            csr_pem: "csr",
            enc_pubkey_pem: "enc",
            instance: &facts,
            region: None,
        })
        .expect("serialize authenticated request");
        assert!(request.get("token").is_none());
        assert!(request.get("expected_org").is_none());
    }

    #[test]
    fn enroll_request_carries_region_beside_the_host_facts() {
        let facts = test_facts();
        let declared = serde_json::to_value(EnrollRequest {
            kind: "standalone",
            token: None,
            expected_org: None,
            csr_pem: "csr",
            enc_pubkey_pem: "enc",
            instance: &facts,
            region: Some("on-prem-syd"),
        })
        .expect("serialize request with a region");

        // The region is customer-declared, not probed: it must be a sibling of
        // `instance`, never a member of it. The cloud reads it from the top
        // level and would ignore it nested.
        assert_eq!(declared["region"], "on-prem-syd");
        assert!(
            declared["instance"].get("region").is_none(),
            "the region must not be nested inside the probed host facts"
        );
    }

    #[test]
    fn enroll_request_omits_an_absent_region_rather_than_nulling_it() {
        let facts = test_facts();
        let omitted = serde_json::to_value(EnrollRequest {
            kind: "standalone",
            token: None,
            expected_org: None,
            csr_pem: "csr",
            enc_pubkey_pem: "enc",
            instance: &facts,
            region: None,
        })
        .expect("serialize request without a region");

        // Absence means "leave the stored region alone". Sending `null` would
        // make every re-enroll — the recovery path past the renewal grace
        // window — silently erase a region set in the portal.
        assert!(
            omitted.get("region").is_none(),
            "an omitted --region must not appear on the wire at all"
        );
    }

    #[test]
    fn the_canonical_response_decodes_with_and_without_optional_metadata() {
        let full: EnrollResponseWire = serde_json::from_value(serde_json::json!({
            "instance_id": "inst_1",
            "identity_cert_pem": "cert",
            "ca_bundle_pem": "ca",
            "gateway_addr": "gateway:7320",
            "not_after": "2030-01-01T00:00:00Z",
            "organization": {"id": 42, "name": "acme"},
            "region": "us-west-2",
            "portal": {"new_project_url": "https://spice.ai/acme/new?instance=inst_1"},
            "attachment": null,
        }))
        .expect("decode the full canonical response");
        assert_eq!(full.organization.name, "acme");
        assert_eq!(full.region.as_deref(), Some("us-west-2"));
        assert_eq!(
            full.portal.and_then(|p| p.new_project_url).as_deref(),
            Some("https://spice.ai/acme/new?instance=inst_1")
        );

        let minimal: EnrollResponseWire = serde_json::from_value(serde_json::json!({
            "instance_id": "inst_1",
            "identity_cert_pem": "cert",
            "ca_bundle_pem": "ca",
            "gateway_addr": "gateway:7320",
            "not_after": "2030-01-01T00:00:00Z",
            "organization": {"id": 42, "name": "acme"},
        }))
        .expect("decode a response without optional metadata");
        assert!(minimal.region.is_none());
        assert!(minimal.portal.is_none());
    }

    #[test]
    fn enrollment_authority_debug_never_prints_secrets() {
        let token = EnrollmentAuthority::Token {
            key: test_key(),
            expected_org: Some("acme".to_string()),
        };
        let debug = format!("{token:?}");
        assert!(
            !debug.contains(&"A".repeat(32)),
            "Debug leaked the enrollment key: {debug}"
        );

        let session = EnrollmentAuthority::AuthenticatedSession {
            access_token: SessionToken::new("sk-very-secret-session-token".to_string()),
            org: "acme".to_string(),
        };
        let debug = format!("{session:?}");
        assert!(
            !debug.contains("sk-very-secret-session-token"),
            "Debug leaked the session token: {debug}"
        );
        // The non-secret org is fine to show.
        assert!(debug.contains("acme"));
    }

    #[test]
    fn backoff_windows_grow_from_base_to_cap() {
        assert_eq!(backoff_window(0), Duration::from_secs(1));
        assert_eq!(backoff_window(1), Duration::from_secs(2));
        assert_eq!(backoff_window(4), Duration::from_secs(16));
        assert_eq!(backoff_window(5), Duration::from_secs(30), "capped");
        assert_eq!(backoff_window(20), Duration::from_secs(30));
        assert_eq!(backoff_window(u32::MAX), Duration::from_secs(30));
    }

    #[test]
    fn retry_sleep_honors_a_shorter_retry_after_and_caps_a_longer_one() {
        // A server-provided Retry-After is used exactly when shorter than
        // the local backoff window...
        assert_eq!(
            retry_sleep(10, Some(Duration::from_millis(250))),
            Duration::from_millis(250)
        );
        // ...and bounded by that window when longer, so a buggy header cannot
        // inflate the client's first backoff.
        assert_eq!(
            retry_sleep(0, Some(Duration::from_mins(10))),
            RETRY_BACKOFF_BASE
        );
        assert_eq!(
            retry_sleep(0, Some(Duration::ZERO)),
            Duration::from_millis(1),
            "Retry-After: 0 must not busy-spin"
        );
        // Without one, the sleep is drawn from the growing window.
        for attempt in 0..8 {
            let sleep = retry_sleep(attempt, None);
            assert!(
                !sleep.is_zero(),
                "attempt {attempt}: retry must not busy-spin"
            );
            assert!(
                sleep <= backoff_window(attempt),
                "attempt {attempt}: {sleep:?} exceeds its window"
            );
        }
    }

    #[test]
    fn retry_after_accepts_a_future_http_date() {
        let mut headers = reqwest::header::HeaderMap::new();
        let future = chrono::Utc::now() + chrono::TimeDelta::minutes(5);
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_str(&future.to_rfc2822())
                .expect("valid HTTP-date header"),
        );

        let parsed = parse_retry_after(&headers).expect("future date is retryable");
        assert!(
            (Duration::from_secs(295)..=Duration::from_mins(5)).contains(&parsed),
            "unexpected date delta: {parsed:?}"
        );
    }

    #[test]
    fn retry_after_ignores_a_past_http_date() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_static("Sat, 01 Jan 2000 00:00:00 +0000"),
        );

        assert!(parse_retry_after(&headers).is_none());
    }

    /// The retry loop, driven with paused time: transient failures back off
    /// (never busy-spin), a denial stops immediately, and the deadline
    /// bounds the whole thing.
    #[tokio::test(start_paused = true)]
    async fn retries_are_paced_and_bounded_by_the_deadline() {
        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let started = tokio::time::Instant::now();

        let result: std::result::Result<(), EnrollNowError> = retry_until_deadline(
            RetryPolicy {
                deadline: Duration::from_mins(2),
            },
            || {
                let attempts = std::sync::Arc::clone(&attempts);
                let observed = std::sync::Arc::clone(&observed);
                async move {
                    attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    observed
                        .lock()
                        .expect("observed lock")
                        .push(started.elapsed());
                    Err(Error::Unavailable {
                        status: 503,
                        message: "down".to_string(),
                        retry_after: None,
                    })
                }
            },
        )
        .await;

        let err = result.expect_err("must exhaust the deadline");
        assert!(
            matches!(err, EnrollNowError::DeadlineExceeded { .. }),
            "{err}"
        );

        let total = attempts.load(std::sync::atomic::Ordering::SeqCst);
        // With windows growing 1,2,4,8,16,30,30…s and full jitter, a 120s
        // deadline admits at least a handful of attempts and cannot admit
        // more than the zero-jitter-free floor would.
        assert!(total >= 5, "expected several attempts, got {total}");
        let times = observed.lock().expect("observed lock");
        // No attempt may start past the deadline.
        assert!(
            times.iter().all(|t| *t < Duration::from_mins(2)),
            "an attempt started past the deadline: {times:?}"
        );
        // Consecutive attempts never exceed the 30s window cap plus the
        // (zero, in paused time) attempt cost.
        for pair in times.windows(2) {
            let gap = pair[1]
                .checked_sub(pair[0])
                .expect("retry timestamps are ordered");
            assert!(
                gap <= RETRY_BACKOFF_CAP,
                "a retry gap exceeded the backoff cap: {gap:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_invalid_success_response_is_terminal_without_replay() {
        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let started = tokio::time::Instant::now();
        let result: std::result::Result<(), EnrollNowError> = retry_until_deadline(
            RetryPolicy {
                deadline: Duration::from_mins(2),
            },
            || {
                attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async {
                    Err(Error::InvalidResponse {
                        url: "http://test/v1/cloud-connect/enroll".to_string(),
                        reason: "response carried an empty instance_id".to_string(),
                    })
                }
            },
        )
        .await;

        let rendered = result
            .as_ref()
            .expect_err("semantic response failure is terminal")
            .to_string();
        assert!(matches!(&result, Err(EnrollNowError::Rejected { .. })));
        assert!(
            rendered.contains("Preserve enrollment-draft.json")
                && rendered.contains("contact Spice Cloud support"),
            "terminal semantic failures need operation-safe remediation: {rendered}"
        );
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the same idempotent success body must not be replayed"
        );
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn a_denial_stops_the_retry_loop_immediately() {
        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let result: std::result::Result<(), EnrollNowError> = retry_until_deadline(
            RetryPolicy {
                deadline: Duration::from_mins(10),
            },
            || {
                let attempts = std::sync::Arc::clone(&attempts);
                async move {
                    attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err(Error::Denied {
                        status: 410,
                        code: DenialCode::ExpiredToken,
                        message: "expired".to_string(),
                    })
                }
            },
        )
        .await;

        assert!(matches!(result, Err(EnrollNowError::Rejected { .. })));
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a terminal denial must not be retried"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_paces_the_next_attempt_exactly() {
        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let started = tokio::time::Instant::now();

        let result: std::result::Result<(), EnrollNowError> = retry_until_deadline(
            RetryPolicy {
                deadline: Duration::from_mins(1),
            },
            || {
                let attempts = std::sync::Arc::clone(&attempts);
                let observed = std::sync::Arc::clone(&observed);
                async move {
                    let n = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    observed
                        .lock()
                        .expect("observed lock")
                        .push(started.elapsed());
                    if n >= 3 {
                        Ok(())
                    } else {
                        Err(Error::Unavailable {
                            status: 429,
                            message: "throttled".to_string(),
                            retry_after: Some(Duration::from_millis(250)),
                        })
                    }
                }
            },
        )
        .await;

        result.expect("succeeds on the fourth attempt");
        let times = observed.lock().expect("observed lock");
        assert_eq!(times.len(), 4);
        // Paused time makes the pacing exact: each shorter Retry-After wins.
        for pair in times.windows(2) {
            assert_eq!(
                pair[1]
                    .checked_sub(pair[0])
                    .expect("retry timestamps are ordered"),
                Duration::from_millis(250)
            );
        }
    }

    #[test]
    fn echoed_authorities_are_redacted_before_becoming_errors() {
        let secret = "spice-enroll-secret-that-must-never-appear";
        let text = format!("credential {secret} was rejected; bearer={secret}");

        let redacted = redact_sensitive(&text, Some(secret));
        assert!(!redacted.contains(secret));
        assert_eq!(redacted.matches("[REDACTED]").count(), 2);
    }

    #[test]
    fn json_escaped_authorities_are_redacted_after_error_body_decoding() {
        let secret = "spice-enroll-secret-that-must-never-appear";
        let escaped = secret.replace('-', r"\u002d");
        let text =
            format!(r#"{{"code":"invalid_token","error":"rejected enrollment key {escaped}"}}"#);
        assert!(
            !text.contains(secret),
            "the wire response must exercise structural redaction"
        );

        let (code, message) = parse_error_body(&text, Some(secret));
        assert_eq!(code.as_deref(), Some("invalid_token"));
        assert!(!message.contains(secret));
        assert!(message.contains("[REDACTED]"));
    }

    #[test]
    fn sensitive_authorities_are_never_surfaced_from_unstructured_fallbacks() {
        let secret = "spice-enroll-secret-that-must-never-appear";
        let escaped = secret.replace('-', r"\u002d");
        let bodies = [
            format!(r#"{{"code":"invalid_token","note":"rejected {escaped}"}}"#),
            format!(r#"{{"code":"invalid_token","error":null,"note":"{escaped}"}}"#),
            format!("not JSON: rejected {escaped}"),
        ];

        for body in bodies {
            assert!(!body.contains(secret));
            let (_code, message) = parse_error_body(&body, Some(secret));
            assert_eq!(
                message, "the response contained no usable error message",
                "an unrecognized response must not become operator-visible"
            );
            assert!(!message.contains(secret));
            assert!(!message.contains(&escaped));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn success_after_transients_returns_the_value() {
        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let result = retry_until_deadline(
            RetryPolicy {
                deadline: Duration::from_mins(10),
            },
            || {
                let attempts = std::sync::Arc::clone(&attempts);
                async move {
                    if attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 2 {
                        Err(Error::Unavailable {
                            status: 500,
                            message: "flaky".to_string(),
                            retry_after: None,
                        })
                    } else {
                        Ok(42_u32)
                    }
                }
            },
        )
        .await;
        assert_eq!(result.expect("succeeds"), 42);
    }

    fn test_config(enroll_endpoint: &str) -> CloudConnectConfig {
        CloudConnectConfig {
            enroll_endpoint: enroll_endpoint.to_string(),
            gateway_endpoint: None,
            ca_cert_pem: None,
            insecure: false,
            identity_path: std::path::PathBuf::from("identity.json"),
            config_dir: std::path::PathBuf::from("."),
            instance_region: None,
            runtime_version: "v0-test".to_string(),
            heartbeat_interval: Duration::from_secs(30),
            telemetry_interval: Duration::from_mins(1),
            metrics_interval: Duration::from_secs(30),
            renewal_lead: Duration::from_hours(12),
            query_deadline: Duration::from_mins(1),
        }
    }

    fn test_facts() -> InstanceFacts {
        InstanceFacts {
            fingerprint: "f".to_string(),
            hostname: "h".to_string(),
            os: "o".to_string(),
            arch: "a".to_string(),
            runtime_version: "v".to_string(),
        }
    }

    async fn assert_cross_origin_enrollment_redirect_is_refused(status: &str) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let expected_status = status[..3].parse::<u16>().expect("HTTP status code");
        let collector = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind redirect collector");
        let collector_address = collector.local_addr().expect("collector address");
        let collector_task = tokio::spawn(async move {
            let accepted = tokio::time::timeout(Duration::from_secs(1), collector.accept()).await;
            let Ok(Ok((mut socket, _))) = accepted else {
                return None;
            };
            let mut request = vec![0_u8; 16 * 1024];
            let bytes = socket
                .read(&mut request)
                .await
                .expect("read redirected request");
            socket
                .write_all(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("answer redirected request");
            Some(String::from_utf8_lossy(&request[..bytes]).into_owned())
        });

        let origin = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind redirect origin");
        let origin_address = origin.local_addr().expect("origin address");
        let response = format!(
            "HTTP/1.1 {status}\r\nLocation: http://{collector_address}/collect\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let origin_task = tokio::spawn(async move {
            let (mut socket, _) = origin.accept().await.expect("accept enroll request");
            let mut request = vec![0_u8; 16 * 1024];
            let _ = socket
                .read(&mut request)
                .await
                .expect("read enroll request");
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write redirect");
        });

        let base = format!("http://{origin_address}");
        let client = EnrollClient::new(&test_config(&base)).expect("client");
        let authority = EnrollmentAuthority::Token {
            key: test_key(),
            expected_org: None,
        };
        let material = IdentityStore::generate_enrollment().expect("enrollment material");
        let error = client
            .enroll(
                &authority,
                "redirect-operation",
                &material,
                &test_facts(),
                None,
            )
            .await
            .expect_err("cross-origin redirect must remain a terminal 3xx response");
        origin_task.await.expect("redirect origin task");
        let leaked_request = collector_task.await.expect("redirect collector task");

        assert!(
            leaked_request.is_none(),
            "the enrollment authority crossed origins: {leaked_request:?}"
        );
        assert!(matches!(error, Error::Denied { status, .. } if status == expected_status));
    }

    #[tokio::test]
    async fn enrollment_authority_never_follows_cross_origin_redirects() {
        for status in [
            "301 Moved Permanently",
            "302 Found",
            "303 See Other",
            "307 Temporary Redirect",
            "308 Permanent Redirect",
        ] {
            assert_cross_origin_enrollment_redirect_is_refused(status).await;
        }
    }

    #[test]
    fn enroll_urls_join_with_and_without_trailing_slash() {
        for endpoint in ["https://api.spice.ai/", "https://api.spice.ai"] {
            let client = EnrollClient::new(&test_config(endpoint)).expect("client");
            assert_eq!(
                client.enroll_url,
                "https://api.spice.ai/v1/cloud-connect/enroll"
            );
            assert_eq!(
                client.renew_url,
                "https://api.spice.ai/v1/cloud-connect/renew"
            );
        }
    }

    #[tokio::test]
    async fn a_malformed_region_is_rejected_before_any_request() {
        // `enroll_now` with an invalid region must fail locally — the config
        // points at an unroutable endpoint, so reaching the network would
        // hang/fail differently than the typed error asserted here.
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = test_config("http://127.0.0.1:9");
        config.config_dir = dir.path().to_path_buf();
        config.identity_path = dir.path().join("identity.json");
        config.instance_region = Some("US_WEST_2".to_string());

        let err = enroll_now(
            &config,
            &EnrollmentAuthority::Token {
                key: test_key(),
                expected_org: None,
            },
            RetryPolicy {
                deadline: Duration::from_millis(10),
            },
        )
        .await
        .expect_err("must reject the region locally");
        assert!(matches!(err, EnrollNowError::InvalidRegion { .. }), "{err}");
        assert!(err.is_terminal_rejection());
    }

    #[test]
    fn renew_request_includes_enc_pubkey_pem() {
        // The cloud `/renew` endpoint requires `enc_pubkey_pem` (Zod schema
        // validation in spicehq/cloud). The request must serialize all four
        // fields — omitting it causes a 400 that blocks renewal.
        let renew_req = RenewRequest {
            cert_pem: "-----BEGIN CERTIFICATE-----\nMOCK\n-----END CERTIFICATE-----",
            csr_pem: "-----BEGIN CERTIFICATE REQUEST-----\nMOCK\n-----END CERTIFICATE REQUEST-----",
            pop_sig: "dGVzdC1zaWduYXR1cmU=",
            enc_pubkey_pem: "-----BEGIN PUBLIC KEY-----\nMOCKENC\n-----END PUBLIC KEY-----",
        };
        let value = serde_json::to_value(&renew_req).expect("serialize renew request");

        // All four fields must be present.
        assert!(value.get("cert_pem").is_some(), "cert_pem must be present");
        assert!(value.get("csr_pem").is_some(), "csr_pem must be present");
        assert!(value.get("pop_sig").is_some(), "pop_sig must be present");
        assert!(
            value.get("enc_pubkey_pem").is_some(),
            "enc_pubkey_pem must be present — the cloud schema requires it"
        );

        // Exactly four keys, no extras.
        assert_eq!(
            value
                .as_object()
                .expect("serialize produces an object")
                .len(),
            4,
            "renew request must carry exactly four fields"
        );
    }
}
