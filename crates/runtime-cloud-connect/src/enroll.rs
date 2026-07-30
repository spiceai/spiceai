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
}

#[derive(Serialize)]
struct RenewRequest<'a> {
    cert_pem: &'a str,
    csr_pem: &'a str,
    pop_sig: &'a str,
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
        })
    }

    /// First-contact enrollment: present the one-time adoption code, the
    /// CSR for a freshly-generated keypair, and the host facts — plus the
    /// optional app attachment (`app_name`, `create_app`). No bearer
    /// token — the code is the credential.
    pub(crate) async fn enroll(
        &self,
        adoption_code: &str,
        material: &EnrollmentMaterial,
        facts: &InstanceFacts,
        app_name: Option<&str>,
        create_app: bool,
    ) -> Result<EnrollOutcome> {
        let request = EnrollRequest {
            adoption_code,
            csr_pem: &material.csr_pem,
            enc_pubkey_pem: &material.enc_public_key_pem,
            instance: facts,
            app_name,
            // `create_app` is meaningless without an app to name, so it
            // rides only alongside `app_name` — the wire never carries the
            // orphaned combination even if a caller sets the flag alone.
            create_app: app_name.and(create_app.then_some(true)),
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
        })
    }

    /// Renew the identity with a fresh keypair (`material`), presenting the
    /// current leaf and the current-key proof-of-possession signature over
    /// the new CSR. Works within the grace window even when the presented
    /// leaf is already expired.
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
        let response = self
            .http
            .post(url)
            .json(body)
            .send()
            .await
            .context(HttpSnafu {
                url: url.to_string(),
            })?;

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
        // present, falling back to a bounded slice of the raw body.
        let message = match response.text().await {
            Ok(text) => serde_json::from_str::<ErrorBody>(&text)
                .map_or_else(|_| bounded(&text, 256), |b| b.error),
            Err(_) => String::new(),
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
) -> Result<(Identity, Option<String>)> {
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
            config.adopt_app_name.as_deref(),
            config.adopt_create_app,
        )
        .await?;
    let identity = Identity {
        identifier: outcome.instance_id,
        identity_cert_pem: outcome.identity_cert_pem,
        private_key_pem: material.private_key_pem,
        public_key_pem: material.public_key_pem,
        ca_bundle_pem: outcome.ca_bundle_pem,
        gateway_addr: outcome.gateway_addr,
        not_after_unix: outcome.not_after_unix,
        enc_private_key_pem: material.enc_private_key_pem,
        enc_public_key_pem: material.enc_public_key_pem,
    };
    Ok((identity, outcome.app_name))
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
    /// The app the instance was attached to at enroll, if any.
    pub app_name: Option<String>,
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

    let (identity, app_name) = match acquire_identity(&client, code, config).await {
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

    Ok(EnrollNowOutcome { identity, app_name })
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
    let key = pem::parse(current_private_key_pem).map_err(|source| Error::ProofOfPossession {
        reason: format!("current private key is not valid PEM: {source}"),
    })?;
    let csr = pem::parse(csr_pem).map_err(|source| Error::ProofOfPossession {
        reason: format!("CSR is not valid PEM: {source}"),
    })?;

    // aws-lc-rs is the same backend rcgen generated the keypair with (see
    // Cargo.toml), so the persisted PKCS#8 always round-trips here.
    let key_pair = aws_lc_rs::signature::EcdsaKeyPair::from_pkcs8(
        &aws_lc_rs::signature::ECDSA_P256_SHA256_ASN1_SIGNING,
        key.contents(),
    )
    .map_err(|source| Error::ProofOfPossession {
        reason: format!("current private key is not a PKCS#8 ECDSA P-256 key: {source}"),
    })?;
    let rng = aws_lc_rs::rand::SystemRandom::new();
    let signature =
        key_pair
            .sign(&rng, csr.contents())
            .map_err(|source| Error::ProofOfPossession {
                reason: format!("signing failed: {source}"),
            })?;
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
            runtime_version: "v0-test".to_string(),
            heartbeat_interval: Duration::from_secs(30),
            telemetry_interval: Duration::from_mins(1),
            renewal_lead: Duration::from_hours(12),
        }
    }

    #[test]
    fn enroll_request_omits_absent_app_attachment_fields() {
        let facts = InstanceFacts {
            fingerprint: "f".to_string(),
            hostname: "h".to_string(),
            os: "o".to_string(),
            arch: "a".to_string(),
            runtime_version: "v".to_string(),
        };
        let bare = serde_json::to_value(EnrollRequest {
            adoption_code: "code",
            csr_pem: "csr",
            enc_pubkey_pem: "enc",
            instance: &facts,
            app_name: None,
            create_app: None,
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
        })
        .expect("serialize attach request");
        assert_eq!(attached["app_name"], "my-app");
        assert_eq!(attached["create_app"], true);
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
        for endpoint in ["https://cloud.spice.ai/", "https://cloud.spice.ai"] {
            let client = EnrollClient::new(&test_config(endpoint)).expect("client");
            assert_eq!(
                client.enroll_url,
                "https://cloud.spice.ai/v1/cloud-connect/enroll"
            );
            assert_eq!(
                client.renew_url,
                "https://cloud.spice.ai/v1/cloud-connect/renew"
            );
        }
    }
}
