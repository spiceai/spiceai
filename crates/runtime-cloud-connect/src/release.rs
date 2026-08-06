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
//! `/enroll` and `/renew` and is authorised by the instance's own mTLS leaf —
//! the leaf names exactly one instance, so a host can only release itself and
//! no new trust boundary is introduced.
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

use serde::Deserialize;
use snafu::{ResultExt, Snafu};

use crate::identity::Identity;

/// Path of the cloud release endpoint, relative to the enroll base URL.
pub const RELEASE_PATH: &str = "/v1/cloud-connect/release";

/// Errors from the host-initiated release call.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to build the mTLS client for the Spice Cloud release request: {source}"
    ))]
    ClientBuild { source: reqwest::Error },

    #[snafu(display(
        "The local identity cannot be presented as a TLS client certificate: {source}. \
         The identity file is corrupt — clear it with `spice connect remove` and delete the \
         instance in the Spice Cloud portal."
    ))]
    Identity { source: reqwest::Error },

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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

/// Report this instance's release to Spice Cloud, authenticating with its own
/// identity leaf.
///
/// `enroll_endpoint` is the state-plane base URL (the same one `/enroll` and
/// `/renew` are reached on). `ca_cert_pem` overrides the trust roots for
/// self-hosted control planes; production uses the system roots.
///
/// # Errors
///
/// Returns [`Error::Rejected`] when the cloud refuses the release (including
/// the not-found a cross-org or already-deleted instance gets), and the
/// transport variants when the cloud cannot be reached. A caller performing a
/// `spice connect remove` treats **every** error as non-fatal: local state is
/// cleared regardless, and the portal-side delete stays authoritative.
pub async fn release(
    enroll_endpoint: &str,
    identity: &Identity,
    ca_cert_pem: Option<&str>,
) -> Result<ReleaseOutcome> {
    let base = enroll_endpoint.trim_end_matches('/');
    let url = format!("{base}{RELEASE_PATH}");

    // The leaf + its private key are the credential. reqwest wants them in one
    // PEM buffer, so concatenate rather than re-encoding either.
    let mut client_pem = String::with_capacity(
        identity.identity_cert_pem.len() + identity.private_key_pem.len() + 2,
    );
    client_pem.push_str(identity.identity_cert_pem.trim_end());
    client_pem.push('\n');
    client_pem.push_str(identity.private_key_pem.trim_end());
    client_pem.push('\n');
    let client_identity =
        reqwest::Identity::from_pem(client_pem.as_bytes()).context(IdentitySnafu)?;

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

    // The body carries no credential — the mTLS leaf is the authorisation. The
    // instance id is sent so the cloud can reject a mismatch outright rather
    // than inferring the subject from the certificate alone.
    let response = match http
        .post(&url)
        .json(&serde_json::json!({ "instance_id": identity.identifier }))
        .send()
        .await
    {
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
}
