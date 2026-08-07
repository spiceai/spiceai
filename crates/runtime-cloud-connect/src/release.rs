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

//! Host-initiated release: `spice connect remove` telling the cloud that this
//! instance is gone.
//!
//! `POST /v1/cloud-connect/release` sits on the same state-plane surface as
//! `/enroll` and `/renew`, and is authorized the way `/renew` is: by
//! proof-of-possession carried in the request body. The request presents the
//! instance's current leaf (`cert_pem`), whose SPIFFE SAN names exactly one
//! instance, together with a signature made by that leaf's private key
//! (`pop_sig`) over `spice-cloud-connect/release/v1\n{instance_id}`. A
//! certificate is not a secret, so the signature — not the certificate — is
//! what proves the caller is the instance; the
//! `spice-cloud-connect/release/v1` domain prefix keeps the signature from
//! being replayable against another endpoint, and the instance id keeps it from
//! being replayable for another instance.
//!
//! The credential rides in the body rather than the TLS layer for the same
//! reason `/renew` does: the presented leaf may already be expired, which is
//! precisely the state a host being decommissioned can be in.
//!
//! It is an HTTPS call rather than a frame on the `CloudConnect` stream because
//! that stream is cloud→instance dispatch, and a release has to work at the
//! moment the customer is decommissioning the host, when the stream may already
//! be down.
//!
//! Release is best-effort by design: the caller clears local state either way.
//! Reachable, the registry row moves to the terminal `removed` status;
//! unreachable, the row reads `disconnected` until it is deleted in the portal.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

use crate::identity::Identity;

/// Path of the cloud release endpoint, relative to the enroll base URL.
pub const RELEASE_PATH: &str = "/v1/cloud-connect/release";

/// Domain-separation prefix of the release proof-of-possession payload.
const POP_DOMAIN: &str = "spice-cloud-connect/release/v1";

/// Errors from the host-initiated release call.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to build the mTLS client for the Spice Cloud release request: {source}"
    ))]
    ClientBuild { source: reqwest::Error },

    #[snafu(display(
        "Failed to release instance {instance_id} (Spice Cloud): the local identity file is \
         invalid ({source}). Delete this instance in the Spice Cloud portal to finish removing \
         it. See: https://spiceai.org/docs"
    ))]
    Identity {
        instance_id: String,
        source: reqwest::Error,
    },

    #[snafu(display("Invalid CA certificate PEM for the Spice Cloud release request: {source}"))]
    CaCert { source: reqwest::Error },

    #[snafu(display("Failed to reach the Spice Cloud endpoint {url}: {source}"))]
    Http { url: String, source: reqwest::Error },

    #[snafu(display(
        "Failed to reach the Spice Cloud endpoint {url}: its TLS certificate was rejected as \
         outside its validity period, which almost always means this host's clock is wrong: \
         {advice}. See: https://spiceai.org/docs"
    ))]
    CertificateValidity { url: String, advice: String },

    #[snafu(display("Spice Cloud rejected the release request ({status}): {message}"))]
    Rejected { status: u16, message: String },

    #[snafu(display(
        "Failed to release instance {instance_id} (Spice Cloud): the local identity file is \
         invalid ({reason}). Delete this instance in the Spice Cloud portal to finish removing \
         it. See: https://spiceai.org/docs"
    ))]
    ProofOfPossession { instance_id: String, reason: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Wire shape of the release request. Mirrors the `/renew` request's
/// credential half: the current leaf plus a proof-of-possession signature made
/// by its private key. There is no CSR — nothing is being issued — so the
/// signature covers [`pop_payload`] instead of a CSR's DER bytes.
///
/// The instance being released is **not** a field: the cloud reads it from the
/// certificate's SPIFFE SAN, which is the only statement of identity a caller
/// cannot choose for itself.
#[derive(Serialize)]
struct ReleaseRequest<'a> {
    cert_pem: &'a str,
    pop_sig: &'a str,
}

/// The exact bytes a release proof-of-possession signs: the
/// `spice-cloud-connect/release/v1` domain prefix, a newline, and the instance
/// id. The cloud rebuilds this string from
/// the presented certificate's SPIFFE SAN and verifies the signature against
/// that certificate's public key, so the separator must be a real newline
/// (0x0A) — an escaped `\n` would be two bytes the cloud never signs over.
fn pop_payload(instance_id: &str) -> String {
    format!("{POP_DOMAIN}\n{instance_id}")
}

/// Error body shape the cloud endpoints return (`{ "error": "..." }`).
#[derive(Deserialize)]
struct ErrorBody {
    error: String,
}

/// What the cloud reports back about the released instance.
#[derive(Debug, Clone, Default)]
pub struct ReleaseOutcome {
    /// The registry status after the release — `removed` on the current
    /// control plane. Empty when the response does not report one.
    pub status: String,
    /// The app the instance was attached to, when it had one: the caller names
    /// it so the customer learns the app was paused as part of this command.
    pub app_name: Option<String>,
}

#[derive(Deserialize)]
struct ReleaseResponseWire {
    #[serde(default)]
    status: String,
    #[serde(default)]
    app_name: Option<String>,
}

/// Report this instance's release to Spice Cloud, presenting its identity leaf
/// and a proof-of-possession signature made with that leaf's private key.
///
/// `enroll_endpoint` is the state-plane base URL (the same one `/enroll` and
/// `/renew` are reached on). `ca_cert_pem` overrides the trust roots for
/// self-hosted control planes; production uses the system roots.
///
/// # Errors
///
/// Returns [`Error::Rejected`] when the cloud refuses the release (including
/// the not-found a cross-org or already-deleted instance gets),
/// [`Error::ProofOfPossession`] when the stored private key cannot sign the
/// request, and the transport variants when the cloud cannot be reached. A
/// caller performing a `spice connect remove` treats **every** error as
/// non-fatal: local state is cleared regardless, and the portal-side delete
/// stays authoritative.
pub async fn release(
    enroll_endpoint: &str,
    identity: &Identity,
    ca_cert_pem: Option<&str>,
) -> Result<ReleaseOutcome> {
    let base = enroll_endpoint.trim_end_matches('/');
    let url = format!("{base}{RELEASE_PATH}");

    // The body carries the credential: the leaf names the instance (its SPIFFE
    // SAN) and the signature proves this host holds the leaf's private key, so
    // a copy of the certificate alone cannot release the instance. Signed
    // before the client is built so an unusable key is reported as what it is
    // rather than as a TLS setup failure.
    let pop_sig = crate::enroll::sign_pop_payload(
        &identity.private_key_pem,
        pop_payload(&identity.identifier).as_bytes(),
    )
    .map_err(|reason| Error::ProofOfPossession {
        instance_id: identity.identifier.clone(),
        reason,
    })?;
    let request = ReleaseRequest {
        cert_pem: &identity.identity_cert_pem,
        pop_sig: &pop_sig,
    };

    // The leaf is also presented as the TLS client identity, for a control
    // plane that fronts this surface with mTLS. reqwest wants the certificate
    // and its key in one PEM buffer, so concatenate rather than re-encoding
    // either. This is transport, not authorization — the release is authorized
    // by the proof-of-possession above.
    let mut client_pem = String::with_capacity(
        identity.identity_cert_pem.len() + identity.private_key_pem.len() + 2,
    );
    client_pem.push_str(identity.identity_cert_pem.trim_end());
    client_pem.push('\n');
    client_pem.push_str(identity.private_key_pem.trim_end());
    client_pem.push('\n');
    let client_identity =
        reqwest::Identity::from_pem(client_pem.as_bytes()).context(IdentitySnafu {
            instance_id: identity.identifier.clone(),
        })?;

    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .identity(client_identity);
    if let Some(ca_pem) = ca_cert_pem {
        for cert in reqwest::Certificate::from_pem_bundle(ca_pem.as_bytes()).context(CaCertSnafu)? {
            builder = builder.add_root_certificate(cert);
        }
    }
    let http = builder.build().context(ClientBuildSnafu)?;

    let response = match http.post(&url).json(&request).send().await {
        Ok(response) => response,
        Err(source) => {
            if crate::clock_skew::looks_like_certificate_validity_failure(&source) {
                let advice = match crate::clock_skew::diagnose(base, ca_cert_pem).await {
                    Some(skew) if skew.is_significant() => skew.advice(),
                    _ => format!(
                        "check this host's clock (currently {}) and enable NTP time \
                         synchronization (for example `sudo timedatectl set-ntp true`)",
                        chrono::Utc::now().to_rfc3339()
                    ),
                };
                return Err(Error::CertificateValidity { url, advice });
            }
            return Err(Error::Http { url, source });
        }
    };

    let status = response.status();
    if !status.is_success() {
        let message = match response.text().await {
            Ok(text) => serde_json::from_str::<ErrorBody>(&text)
                .map_or_else(|_| bounded(&text, 256), |body| body.error),
            Err(_) => String::new(),
        };
        return Err(Error::Rejected {
            status: status.as_u16(),
            message,
        });
    }

    // A control plane that answers 2xx with no body (or an unexpected one) has
    // still accepted the release, so treat an undecodable body as success with
    // nothing extra to report rather than failing a completed operation.
    let wire = response
        .json::<ReleaseResponseWire>()
        .await
        .unwrap_or(ReleaseResponseWire {
            status: String::new(),
            app_name: None,
        });
    Ok(ReleaseOutcome {
        status: wire.status,
        app_name: wire.app_name,
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
    use base64::Engine as _;

    use super::*;

    #[test]
    fn release_url_joins_with_and_without_trailing_slash() {
        for endpoint in ["https://api.spice.ai/", "https://api.spice.ai"] {
            let base = endpoint.trim_end_matches('/');
            assert_eq!(
                format!("{base}{RELEASE_PATH}"),
                "https://api.spice.ai/v1/cloud-connect/release"
            );
        }
    }

    #[test]
    fn identity_pem_concatenation_keeps_both_blocks_intact() {
        // The client PEM must contain the leaf and the key as two complete,
        // separately-delimited blocks: a missing newline between them would
        // make the buffer unparseable and the release unauthenticatable.
        let identity = Identity {
            identifier: "inst_1".to_string(),
            identity_cert_pem: "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----"
                .to_string(),
            private_key_pem: "-----BEGIN PRIVATE KEY-----\nBBBB\n-----END PRIVATE KEY-----\n\n"
                .to_string(),
            public_key_pem: String::new(),
            ca_bundle_pem: String::new(),
            gateway_addr: String::new(),
            not_after_unix: None,
            app_id: None,
            enc_private_key_pem: String::new(),
            enc_public_key_pem: String::new(),
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
        };
        let mut pem = String::new();
        pem.push_str(identity.identity_cert_pem.trim_end());
        pem.push('\n');
        pem.push_str(identity.private_key_pem.trim_end());
        pem.push('\n');

        assert!(pem.contains("-----END CERTIFICATE-----\n-----BEGIN PRIVATE KEY-----"));
        assert!(pem.ends_with("-----END PRIVATE KEY-----\n"));
    }

    #[test]
    fn bounded_truncates_on_char_boundaries() {
        assert_eq!(bounded("short", 256), "short");
        // A multi-byte char straddling the limit must not be split.
        let s = "aa\u{e9}bb";
        assert_eq!(bounded(s, 3), "aa");
    }

    #[test]
    fn pop_payload_separates_the_domain_from_the_instance_with_one_real_newline() {
        let payload = pop_payload("inst_1");
        assert_eq!(payload, "spice-cloud-connect/release/v1\ninst_1");
        // The cloud rebuilds these bytes and verifies the signature over them,
        // so the separator must be the single byte 0x0A. A literal backslash-n
        // would be two bytes the cloud never signs, and every release would
        // come back 401.
        assert_eq!(
            payload.as_bytes(),
            b"spice-cloud-connect/release/v1\x0Ainst_1",
            "the domain separator must be a real newline byte"
        );
        assert!(!payload.contains('\\'), "the payload must carry no escapes");
    }

    #[test]
    fn release_request_carries_only_the_certificate_and_the_pop_signature() {
        let value = serde_json::to_value(ReleaseRequest {
            cert_pem: "-----BEGIN CERTIFICATE-----\nMOCK\n-----END CERTIFICATE-----",
            pop_sig: "dGVzdC1zaWduYXR1cmU=",
        })
        .expect("serialize release request");

        assert_eq!(
            value["cert_pem"],
            "-----BEGIN CERTIFICATE-----\nMOCK\n-----END CERTIFICATE-----"
        );
        assert_eq!(value["pop_sig"], "dGVzdC1zaWduYXR1cmU=");
        // The instance is read from the certificate's SPIFFE SAN, so an
        // `instance_id` field is not part of the contract — sending one instead
        // of the credential is what left the release unauthenticatable.
        assert!(value.get("instance_id").is_none());
        assert_eq!(
            value
                .as_object()
                .expect("serialize produces an object")
                .len(),
            2,
            "the release request must carry exactly the two credential fields"
        );
    }

    /// Extract the uncompressed EC point from a P-256 SPKI DER blob: the final
    /// 65 bytes (0x04 || X || Y). Test-only shortcut — production verification
    /// happens server-side.
    fn p256_point_from_spki(spki_der: &[u8]) -> &[u8] {
        assert!(spki_der.len() > 65, "SPKI too short for a P-256 key");
        &spki_der[spki_der.len() - 65..]
    }

    /// A self-signed leaf and its key, standing in for an issued identity: the
    /// release only needs the certificate to be forwardable and its key to
    /// sign, neither of which depends on who signed the leaf.
    fn test_identity(identifier: &str) -> (Identity, String) {
        let key_pair = rcgen::KeyPair::generate().expect("generate keypair");
        let mut params =
            rcgen::CertificateParams::new(Vec::<String>::new()).expect("certificate params");
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "spice-release-test");
        let cert = params.self_signed(&key_pair).expect("self-signed leaf");

        let identity = Identity {
            identifier: identifier.to_string(),
            identity_cert_pem: cert.pem(),
            private_key_pem: key_pair.serialize_pem(),
            public_key_pem: key_pair.public_key_pem(),
            ca_bundle_pem: String::new(),
            gateway_addr: String::new(),
            not_after_unix: None,
            enc_private_key_pem: String::new(),
            enc_public_key_pem: String::new(),
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
            app_id: None,
        };
        let public_key_pem = key_pair.public_key_pem();
        (identity, public_key_pem)
    }

    /// The release request as it actually goes out over the wire, against an
    /// in-process mock of the cloud endpoint (plain HTTP: the production call
    /// is HTTPS, which is reqwest's standard path and not what this asserts).
    #[tokio::test]
    async fn release_posts_the_certificate_and_a_pop_signature_the_cloud_can_verify() {
        use std::sync::Arc;

        use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
        use tokio::sync::Mutex;

        type Captured = Arc<Mutex<Vec<serde_json::Value>>>;

        async fn release_handler(
            State(captured): State<Captured>,
            Json(body): Json<serde_json::Value>,
        ) -> (StatusCode, Json<serde_json::Value>) {
            captured.lock().await.push(body);
            (
                StatusCode::OK,
                Json(serde_json::json!({ "status": "removed", "app_name": "edge-fleet" })),
            )
        }

        let captured: Captured = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route(RELEASE_PATH, post(release_handler))
            .with_state(Arc::clone(&captured));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock release endpoint");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let (identity, public_key_pem) = test_identity("inst_release_1");
        let outcome = release(&format!("http://{addr}"), &identity, None)
            .await
            .expect("release must succeed against the mock endpoint");
        assert_eq!(outcome.status, "removed");
        assert_eq!(outcome.app_name.as_deref(), Some("edge-fleet"));

        let requests = captured.lock().await;
        assert_eq!(requests.len(), 1, "exactly one release request");
        let body = &requests[0];
        assert_eq!(
            body["cert_pem"], identity.identity_cert_pem,
            "the request must present the instance's own leaf"
        );
        assert!(
            body.get("instance_id").is_none(),
            "the instance is named by the certificate's SAN, not by the body"
        );

        // The signature must verify against the leaf's public key over exactly
        // the bytes the cloud rebuilds from the SAN — this is the check the
        // endpoint performs, and the reason a body without `pop_sig` is a 401.
        let sig = base64::engine::general_purpose::STANDARD
            .decode(body["pop_sig"].as_str().expect("pop_sig is a string"))
            .expect("pop_sig is base64");
        let spki = pem::parse(&public_key_pem).expect("public key PEM");
        let key = aws_lc_rs::signature::UnparsedPublicKey::new(
            &aws_lc_rs::signature::ECDSA_P256_SHA256_ASN1,
            p256_point_from_spki(spki.contents()),
        );
        key.verify(pop_payload(&identity.identifier).as_bytes(), &sig)
            .expect(
                "the release signature must verify over \
                 `spice-cloud-connect/release/v1\\n{instance_id}`",
            );
        // ...and must not verify over anything else, so a captured signature
        // cannot release a different instance.
        key.verify(pop_payload("inst_other").as_bytes(), &sig)
            .expect_err("the signature must be bound to this instance id");
    }
}
