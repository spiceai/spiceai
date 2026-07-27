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
use crate::identity::{EnrollmentMaterial, Identity};

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
    /// (4xx `Rejected`): the adoption code is invalid/consumed/expired, or the
    /// identity was revoked. This is the sole condition under which the caller
    /// may take a destructive action — burning the staged adoption code or
    /// clearing the on-disk identity.
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
    instance: &'a InstanceFacts,
}

/// Wire shape of a successful enroll response.
#[derive(Deserialize)]
struct EnrollResponseWire {
    instance_id: String,
    identity_cert_pem: String,
    ca_bundle_pem: String,
    gateway_addr: String,
    not_after: String,
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
    /// CSR for a freshly-generated keypair, and the host facts. No bearer
    /// token — the code is the credential.
    pub(crate) async fn enroll(
        &self,
        adoption_code: &str,
        material: &EnrollmentMaterial,
        facts: &InstanceFacts,
    ) -> Result<EnrollOutcome> {
        let request = EnrollRequest {
            adoption_code,
            csr_pem: &material.csr_pem,
            instance: facts,
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
            runtime_version: "v0-test".to_string(),
            heartbeat_interval: Duration::from_secs(30),
            telemetry_interval: Duration::from_mins(1),
            renewal_lead: Duration::from_hours(12),
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
