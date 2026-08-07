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
//! - `POST /v1/cloud-connect/enroll` — first contact. Authenticated by the
//!   one-time adoption code (no bearer token); carries the PKCS#10 CSR and
//!   the host facts nested under `instance`. The cloud atomically consumes
//!   the code, provisions the `instances` registry row, signs the CSR with
//!   the KMS CA, and returns the leaf + CA bundle + gateway address + the
//!   stable `instance_id`.
//! - `POST /v1/cloud-connect/renew` — ~12h cadence. Authenticated by dual
//!   proof-of-possession rather than mTLS (the presented cert may already
//!   be expired within the 30-day grace window): the CURRENT key signs the
//!   fresh CSR's DER bytes (`pop_sig`), and the NEW key proves itself via
//!   the CSR's self-signature. **Every renewal rotates the keypair.**
//!
//! HTTP status contract (both endpoints): 4xx responses are authoritative
//! rejections — retrying with the same inputs cannot succeed — while 5xx
//! and transport failures are transient and retried with backoff.

use std::time::Duration;

use base64::Engine as _;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use snafu::{ResultExt, Snafu};

use crate::config::CloudConnectConfig;
use crate::identity::{EnrollmentMaterial, Identity, IdentityStore};

/// Path of the cloud enroll endpoint, relative to the enroll base URL.
pub const ENROLL_PATH: &str = "/v1/cloud-connect/enroll";
/// Path of the cloud renew endpoint, relative to the enroll base URL.
pub const RENEW_PATH: &str = "/v1/cloud-connect/renew";

/// How long past the leaf's `not_after` a renewal is still accepted by the
/// cloud (mirrors the server-side grace). Past this the identity is dead
/// and a fresh adoption code is required.
pub const RENEWAL_GRACE: Duration = Duration::from_hours(30 * 24);

/// Errors from the out-of-band enroll/renew HTTP flow.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build the HTTPS client for Spice Cloud enrollment: {source}"))]
    ClientBuild { source: reqwest::Error },

    #[snafu(display("Invalid CA certificate PEM for Spice Cloud enrollment: {source}"))]
    CaCert { source: reqwest::Error },

    #[snafu(display("Failed to reach the Spice Cloud endpoint {url}: {source}"))]
    Http { url: String, source: reqwest::Error },

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
    Rejected { status: u16, message: String },

    #[snafu(display("Spice Cloud returned a server error ({status}): {message}"))]
    ServerError { status: u16, message: String },

    #[snafu(display("Unexpected response from the Spice Cloud endpoint {url}: {reason}"))]
    InvalidResponse { url: String, reason: String },

    #[snafu(display("Failed to sign the renewal proof-of-possession: {reason}"))]
    ProofOfPossession { reason: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// `true` only when the cloud *authoritatively rejected* the request
    /// (4xx `Rejected`): retrying the same request cannot succeed.
    ///
    /// Note this deliberately EXCLUDES [`Error::CertificateValidity`] — a
    /// skewed host clock is fixable and the request never reached the cloud,
    /// so the adoption code is still live and must not be burned.
    ///
    /// Note this deliberately EXCLUDES [`Error::ProofOfPossession`]. A
    /// proof-of-possession / key-material failure is *local* and never reaches
    /// the cloud (during enroll it fails before the HTTP request; during renew
    /// the code was never at stake), so it must NOT burn an un-consumed code or
    /// discard a still-valid identity — it is retried instead. Transport
    /// failures and 5xx responses are likewise transient (retryable).
    #[must_use]
    pub fn is_authoritative_rejection(&self) -> bool {
        matches!(self, Error::Rejected { .. })
    }

    /// `true` only when the *credential itself* was rejected (HTTP 401:
    /// unknown or already-consumed adoption code / revoked identity). This
    /// is the sole condition under which a staged adoption code may be
    /// burned or the on-disk identity cleared. Other authoritative 4xx
    /// rejections (app-attachment validation: 400/403/404/409) are checked
    /// by the cloud BEFORE the code is consumed, so the code remains
    /// redeemable once the request is corrected — discarding it would burn
    /// a live code over a typo in `--app-name`.
    #[must_use]
    pub fn is_credential_rejection(&self) -> bool {
        matches!(self, Error::Rejected { status: 401, .. })
    }
}

/// Host facts a standalone `spiced` reports at enroll — recorded on the
/// cloud `instances` registry row. `fingerprint` is the stable machine
/// identity: re-enrolling the same host lands on its existing row instead
/// of minting a duplicate.
#[derive(Debug, Clone, Serialize)]
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

#[derive(Serialize)]
struct EnrollRequest<'a> {
    adoption_code: &'a str,
    csr_pem: &'a str,
    /// The instance's X25519 encryption public key (RFC 8410 SPKI PEM).
    /// The cloud records it and HPKE-seals secret payloads to it.
    enc_pubkey_pem: &'a str,
    instance: &'a InstanceFacts,
    /// Attach-at-connect: the org-scoped app to attach the instance to.
    /// The cloud validates the attachment BEFORE consuming the code.
    #[serde(skip_serializing_if = "Option::is_none")]
    app_name: Option<&'a str>,
    /// With `app_name`: create the app when it does not exist. Omitted
    /// (never `false`) when unset — absence is the wire default.
    #[serde(skip_serializing_if = "Option::is_none")]
    create_app: Option<bool>,
    /// Where this instance runs. A **sibling of `instance`, not a member of
    /// it**: everything in [`InstanceFacts`] is probed from the host, while
    /// the region is whatever the operator declared with
    /// `spice connect --region`.
    ///
    /// Omitted (never `null`) when unset — the cloud reads absence as "leave
    /// the stored region alone", so a re-enrol cannot erase a region set in
    /// the portal.
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<&'a str>,
}

/// The customer-declared attributes an enrollment carries alongside the probed
/// host facts: which app to attach to, and where the instance runs. Borrowed
/// from a [`CloudConnectConfig`] by [`EnrollAttributes::from_config`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct EnrollAttributes<'a> {
    pub(crate) app_name: Option<&'a str>,
    pub(crate) create_app: bool,
    pub(crate) region: Option<&'a str>,
}

impl<'a> EnrollAttributes<'a> {
    pub(crate) fn from_config(config: &'a CloudConnectConfig) -> Self {
        Self {
            app_name: config.adopt_app_name.as_deref(),
            create_app: config.adopt_create_app,
            region: config.instance_region.as_deref(),
        }
    }
}

/// Wire shape of a successful enroll response.
#[derive(Deserialize)]
struct EnrollResponseWire {
    instance_id: String,
    identity_cert_pem: String,
    ca_bundle_pem: String,
    gateway_addr: String,
    not_after: String,
    /// The app the instance is attached to, when the enrollment requested
    /// or carried an attachment. Absent on older control planes.
    #[serde(default)]
    app_name: Option<String>,
    /// The org the adoption code was scoped to, so the CLI can name it in the
    /// enroll summary rather than making the customer look it up. Absent on
    /// control planes that do not report it.
    #[serde(default)]
    org: Option<String>,
    /// The region now stored on the registry row — the declared `region` when
    /// one was sent, otherwise whatever the row already held. Absent on
    /// control planes that do not report it.
    #[serde(default)]
    region: Option<String>,
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
    /// The app the instance was attached to at enroll, if any.
    pub app_name: Option<String>,
    /// The org the instance enrolled into, when the cloud reported it.
    pub org: Option<String>,
    /// The region stored on the registry row after this enroll, when the
    /// cloud reported it.
    pub region: Option<String>,
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

/// Error body shape the cloud endpoints return (`{ "error": "..." }`).
#[derive(Deserialize)]
struct ErrorBody {
    error: String,
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
            .connect_timeout(Duration::from_secs(10));
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

    /// First-contact enrollment: present the one-time adoption code, the
    /// CSR for a freshly-generated keypair, and the host facts — plus the
    /// optional app attachment and declared region (see
    /// [`EnrollAttributes`]). No bearer token — the code is the credential.
    pub(crate) async fn enroll(
        &self,
        adoption_code: &str,
        material: &EnrollmentMaterial,
        facts: &InstanceFacts,
        attributes: &EnrollAttributes<'_>,
    ) -> Result<EnrollOutcome> {
        let request = EnrollRequest {
            adoption_code,
            csr_pem: &material.csr_pem,
            enc_pubkey_pem: &material.enc_public_key_pem,
            instance: facts,
            app_name: attributes.app_name,
            // `create_app` is meaningless without an app to name, so it
            // rides only alongside `app_name` — the wire never carries the
            // orphaned combination even if a caller sets the flag alone.
            create_app: attributes
                .app_name
                .and(attributes.create_app.then_some(true)),
            region: attributes.region,
        };
        let wire: EnrollResponseWire = self.post_json(&self.enroll_url, &request).await?;
        let not_after_unix = parse_not_after(&self.enroll_url, &wire.not_after)?;
        // The gateway address is what the identity connects with — an empty
        // one would persist an unusable identity after the single-use code
        // was already consumed, so fail loudly instead.
        snafu::ensure!(
            !wire.gateway_addr.is_empty(),
            InvalidResponseSnafu {
                url: self.enroll_url.clone(),
                reason: "enroll response carried an empty gateway_addr".to_string(),
            }
        );
        Ok(EnrollOutcome {
            instance_id: wire.instance_id,
            identity_cert_pem: wire.identity_cert_pem,
            ca_bundle_pem: wire.ca_bundle_pem,
            gateway_addr: wire.gateway_addr,
            not_after_unix,
            app_name: wire.app_name,
            org: wire.org,
            region: wire.region,
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
        let wire: RenewResponseWire = self.post_json(&self.renew_url, &request).await?;
        let not_after_unix = parse_not_after(&self.renew_url, &wire.not_after)?;
        Ok(RenewOutcome {
            identity_cert_pem: wire.identity_cert_pem,
            not_after_unix,
        })
    }

    async fn post_json<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        url: &str,
        body: &Req,
    ) -> Result<Resp> {
        let response = match self.http.post(url).json(body).send().await {
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
        if let Some(skew) = skew {
            tracing::warn!("Cloud Connect: {}", skew.advice());
        }

        let status = response.status();
        if status.is_success() {
            return response
                .json::<Resp>()
                .await
                .map_err(|source| Error::InvalidResponse {
                    url: url.to_string(),
                    reason: format!("failed to decode response body: {source}"),
                });
        }

        // Non-2xx: surface the server's `{ "error": "..." }` message when
        // present, falling back to a bounded slice of the raw body. Append the
        // measured skew when there is one — a host hours out of step can have
        // its CSR or its renewal proof-of-possession refused, and the cloud's
        // message alone would not say why.
        let message = match response.text().await {
            Ok(text) => serde_json::from_str::<ErrorBody>(&text)
                .map_or_else(|_| bounded(&text, 256), |b| b.error),
            Err(_) => String::new(),
        };
        let message = match skew {
            Some(skew) => format!("{message} ({})", skew.advice()),
            None => message,
        };
        // 5xx is transient by definition; 429 (rate limit) and 408 (request
        // timeout) are the 4xx statuses that are also transient — treating
        // them as authoritative rejections would burn the single-use
        // adoption code or clear a still-valid identity over a throttle.
        let transient = status.is_server_error()
            || status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::REQUEST_TIMEOUT;
        if transient {
            Err(Error::ServerError {
                status: status.as_u16(),
                message,
            })
        } else {
            Err(Error::Rejected {
                status: status.as_u16(),
                message,
            })
        }
    }
}

/// Generate fresh key material, gather the host facts, and enroll against
/// the cloud: the shared core of the runtime driver's credential phase and
/// the CLI's one-shot [`enroll_now`] flow. Does not persist anything — the
/// two callers differ in how persistence failures are handled. Returns the
/// issued identity and the name of the app the instance was attached to at
/// enroll, if any.
pub(crate) async fn acquire_identity(
    client: &EnrollClient,
    adoption_code: &str,
    config: &CloudConnectConfig,
) -> Result<(Identity, EnrollRegistration)> {
    let material =
        IdentityStore::generate_enrollment().map_err(|source| Error::ProofOfPossession {
            reason: format!("failed to generate enrollment key material: {source}"),
        })?;
    let facts = InstanceFacts::gather(&config.runtime_version);
    let outcome = client
        .enroll(
            adoption_code,
            &material,
            &facts,
            &EnrollAttributes::from_config(config),
        )
        .await?;
    let registration = EnrollRegistration {
        app_name: outcome.app_name,
        org: outcome.org,
        region: outcome.region,
    };
    let identity = Identity {
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
    let mut identity = identity;
    identity.ensure_cache_key();
    Ok((identity, registration))
}

/// What the cloud recorded on the registry row for this enrollment — the parts
/// worth reporting to the operator, distinct from the identity itself.
#[derive(Debug, Clone, Default)]
pub struct EnrollRegistration {
    /// The app the instance was attached to at enroll, if any.
    pub app_name: Option<String>,
    /// The org the instance enrolled into, when the cloud reported it.
    pub org: Option<String>,
    /// The region on the registry row after this enroll, when the cloud
    /// reported it. Present even when this enroll declared no `--region`, since
    /// an omitted region leaves any previously-set value in place.
    pub region: Option<String>,
}

/// Errors from the one-shot [`enroll_now`] flow.
#[derive(Debug, Snafu)]
pub enum EnrollNowError {
    #[snafu(display("No adoption code is staged or configured"))]
    NoAdoptionCode,

    #[snafu(display("{source}"))]
    Enroll { source: Error },

    #[snafu(display(
        "Enrollment succeeded but the identity could not be persisted at {}: {source}. \
         The adoption code was already consumed by the cloud and cannot be reused; \
         fix the directory (permissions/disk space), mint a new adoption code, and re-run `spice connect <code>`.",
        path.display()
    ))]
    Persist {
        path: std::path::PathBuf,
        source: crate::identity::Error,
    },
}

impl EnrollNowError {
    /// `true` when the cloud authoritatively rejected the request (any 4xx
    /// — see [`Error::is_authoritative_rejection`]): retrying the same
    /// request cannot succeed.
    #[must_use]
    pub fn is_authoritative_rejection(&self) -> bool {
        matches!(self, Self::Enroll { source } if source.is_authoritative_rejection())
    }

    /// `true` when the adoption code itself was rejected (HTTP 401 — see
    /// [`Error::is_credential_rejection`]): the code is dead and the staged
    /// copy was discarded. Attachment rejections (400/403/404/409) return
    /// `false`: the code was not consumed and remains redeemable.
    #[must_use]
    pub fn is_credential_rejection(&self) -> bool {
        matches!(self, Self::Enroll { source } if source.is_credential_rejection())
    }
}

/// Result of a successful one-shot [`enroll_now`].
#[derive(Debug)]
pub struct EnrollNowOutcome {
    /// The issued (and persisted) identity.
    pub identity: Identity,
    /// What the cloud recorded on the registry row (app attachment, org,
    /// region).
    pub registration: EnrollRegistration,
}

/// One-shot out-of-band enrollment: present `config.adoption_code` to the
/// cloud enroll endpoint, persist the issued identity at
/// `config.identity_path`, and remove the staged pending-code file.
///
/// This is the `spice connect` (enroll-and-exit) entry point. Unlike the
/// long-running runtime client — which tolerates a persistence failure by
/// carrying the identity in memory — the calling process exits immediately,
/// so a persistence failure here is a hard error (the single-use code is
/// already consumed at that point; the error message says so).
///
/// The staged pending-code file is removed exactly when the code is spent:
/// on a successful enroll, on a credential rejection (HTTP 401:
/// invalid/consumed code), and on a persistence failure (the cloud consumed
/// the code to issue the identity that could not be written) — so a later
/// `spiced` start never retries a dead code. It is kept when the code
/// survives: other authoritative 4xx rejections (app-attachment validation,
/// which the cloud checks BEFORE consuming the code) and transient failures
/// (transport, 5xx), so a corrected or retried request can still redeem it.
///
/// # Errors
///
/// - [`EnrollNowError::NoAdoptionCode`] when `config.adoption_code` is `None`.
/// - [`EnrollNowError::Enroll`] when the HTTPS enroll fails.
/// - [`EnrollNowError::Persist`] when the issued identity cannot be written.
pub async fn enroll_now(config: &CloudConnectConfig) -> Result<EnrollNowOutcome, EnrollNowError> {
    let Some(ref code) = config.adoption_code else {
        return Err(EnrollNowError::NoAdoptionCode);
    };
    let client = EnrollClient::new(config).context(EnrollSnafu)?;

    let (identity, registration) = match acquire_identity(&client, code, config).await {
        Ok(enrolled) => enrolled,
        Err(source) => {
            if source.is_credential_rejection() {
                discard_pending_code_file(config);
            }
            return Err(EnrollNowError::Enroll { source });
        }
    };

    // The enroll succeeded, so the cloud has consumed the code: the staged
    // copy is spent regardless of whether the identity below persists.
    // Discard it here so a persistence failure can't leave a dead code that
    // `status` reports as redeemable and a later `spiced` start re-presents
    // for a 401.
    discard_pending_code_file(config);

    // spawn_blocking: identity persistence is file I/O with fsync inside an
    // async context.
    let path = config.identity_path.clone();
    let to_store = identity.clone();
    let stored = tokio::task::spawn_blocking(move || IdentityStore::store(&path, &to_store))
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

    Ok(EnrollNowOutcome {
        identity,
        registration,
    })
}

/// Best-effort removal of the staged pending-code file. A missing file is
/// success; other failures are logged (the file only risks re-sending an
/// already-consumed code, which the cloud rejects).
fn discard_pending_code_file(config: &CloudConnectConfig) {
    if let Some(ref path) = config.pending_adopt_code_path {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(
                    "Cloud Connect: failed to remove pending adoption code at {}: {err}",
                    path.display()
                );
            }
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
/// `/release` signs its own domain-separated `release\n{instance_id}` payload.
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
            reason: format!("invalid not_after timestamp {value:?}: {source}"),
        })?;
    u64::try_from(parsed.timestamp()).map_err(|_| Error::InvalidResponse {
        url: url.to_string(),
        reason: format!("not_after timestamp {value:?} is before the Unix epoch"),
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
        // it must never burn an un-consumed adoption code or clear a valid
        // identity — the caller retries instead.
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
    fn rejection_classification() {
        // Only an authoritative 4xx cloud rejection may trigger destructive
        // cleanup (burn the code / clear the identity).
        let rejected = Error::Rejected {
            status: 401,
            message: "Adoption code already used".to_string(),
        };
        assert!(rejected.is_authoritative_rejection());
        // 5xx is transient — retryable, never destructive.
        let server = Error::ServerError {
            status: 503,
            message: String::new(),
        };
        assert!(!server.is_authoritative_rejection());
        // A local proof-of-possession failure is not a cloud rejection.
        let pop = Error::ProofOfPossession {
            reason: "key material generation failed".to_string(),
        };
        assert!(!pop.is_authoritative_rejection());
    }

    fn test_config(enroll_endpoint: &str) -> CloudConnectConfig {
        CloudConnectConfig {
            enroll_endpoint: enroll_endpoint.to_string(),
            gateway_endpoint: None,
            ca_cert_pem: None,
            insecure: false,
            identity_path: std::path::PathBuf::from("identity.json"),
            config_dir: std::path::PathBuf::from("."),
            adoption_code: None,
            pending_adopt_code_path: None,
            adopt_app_name: None,
            adopt_create_app: false,
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

    #[test]
    fn enroll_request_omits_absent_app_attachment_fields() {
        let facts = test_facts();
        let bare = serde_json::to_value(EnrollRequest {
            adoption_code: "code",
            csr_pem: "csr",
            enc_pubkey_pem: "enc",
            instance: &facts,
            app_name: None,
            create_app: None,
            region: None,
        })
        .expect("serialize bare request");
        // Absent attachment fields must be omitted, not sent as null/false —
        // older control planes reject unknown-but-present fields loosely and
        // the wire default is absence.
        assert!(bare.get("app_name").is_none());
        assert!(bare.get("create_app").is_none());

        let attached = serde_json::to_value(EnrollRequest {
            adoption_code: "code",
            csr_pem: "csr",
            enc_pubkey_pem: "enc",
            instance: &facts,
            app_name: Some("my-app"),
            create_app: Some(true),
            region: None,
        })
        .expect("serialize attach request");
        assert_eq!(attached["app_name"], "my-app");
        assert_eq!(attached["create_app"], true);
    }

    #[test]
    fn enroll_request_carries_region_beside_the_host_facts() {
        let facts = test_facts();
        let declared = serde_json::to_value(EnrollRequest {
            adoption_code: "code",
            csr_pem: "csr",
            enc_pubkey_pem: "enc",
            instance: &facts,
            app_name: None,
            create_app: None,
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
            adoption_code: "code",
            csr_pem: "csr",
            enc_pubkey_pem: "enc",
            instance: &facts,
            app_name: None,
            create_app: None,
            region: None,
        })
        .expect("serialize request without a region");

        // Absence means "leave the stored region alone". Sending `null` would
        // make every re-enrol — the recovery path past the renewal grace
        // window — silently erase a region set in the portal.
        assert!(
            omitted.get("region").is_none(),
            "an omitted --region must not appear on the wire at all"
        );
    }

    #[test]
    fn enroll_attributes_read_the_config() {
        let mut config = test_config("https://api.spice.ai");
        config.adopt_app_name = Some("edge-fleet".to_string());
        config.adopt_create_app = true;
        config.instance_region = Some("us-west-2".to_string());

        let attributes = EnrollAttributes::from_config(&config);
        assert_eq!(attributes.app_name, Some("edge-fleet"));
        assert!(attributes.create_app);
        assert_eq!(attributes.region, Some("us-west-2"));
    }

    #[test]
    fn enroll_response_tolerates_a_control_plane_that_omits_org_and_region() {
        // Both fields are additive: an older control plane omits them and the
        // enroll must still succeed rather than failing to decode.
        let wire: EnrollResponseWire = serde_json::from_value(serde_json::json!({
            "instance_id": "inst_1",
            "identity_cert_pem": "cert",
            "ca_bundle_pem": "ca",
            "gateway_addr": "gateway:7320",
            "not_after": "2030-01-01T00:00:00Z",
        }))
        .expect("decode a response without org/region");
        assert!(wire.org.is_none());
        assert!(wire.region.is_none());

        let full: EnrollResponseWire = serde_json::from_value(serde_json::json!({
            "instance_id": "inst_1",
            "identity_cert_pem": "cert",
            "ca_bundle_pem": "ca",
            "gateway_addr": "gateway:7320",
            "not_after": "2030-01-01T00:00:00Z",
            "app_name": "my-app",
            "org": "my-org",
            "region": "us-west-2",
        }))
        .expect("decode a full response");
        assert_eq!(full.org.as_deref(), Some("my-org"));
        assert_eq!(full.region.as_deref(), Some("us-west-2"));
        assert_eq!(full.app_name.as_deref(), Some("my-app"));
    }

    #[test]
    fn certificate_validity_error_is_not_an_authoritative_rejection() {
        // A skewed clock never reached the cloud, so the adoption code is
        // still live: classifying this as authoritative would burn it.
        let err = Error::CertificateValidity {
            url: "https://api.spice.ai/v1/cloud-connect/enroll".to_string(),
            advice: "host clock is 42 minutes behind Spice Cloud".to_string(),
        };
        assert!(!err.is_authoritative_rejection());
        assert!(!err.is_credential_rejection());
        assert!(
            err.to_string().contains("clock"),
            "the message must name the clock: {err}"
        );
    }

    #[test]
    fn credential_rejection_is_401_only() {
        let dead_code = Error::Rejected {
            status: 401,
            message: "Adoption code already used".to_string(),
        };
        assert!(dead_code.is_credential_rejection());
        // Attachment validation failures (checked before the code is
        // consumed) must never burn the staged code.
        for status in [400_u16, 403, 404, 409] {
            let attach = Error::Rejected {
                status,
                message: "attachment rejected".to_string(),
            };
            assert!(attach.is_authoritative_rejection());
            assert!(
                !attach.is_credential_rejection(),
                "{status} must not be treated as a dead code"
            );
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
