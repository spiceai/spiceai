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

//! Codeless connect: minting an adoption code on the customer's behalf.
//!
//! A host already authenticated with `spice login` holds an org-scoped
//! credential, so sending its operator to the portal to copy a code is a
//! browser round-trip that buys nothing. `spice connect` with no code mints a
//! single-use code through the management API and redeems it in the same
//! command.
//!
//! **The code is never displayed and never written to disk.** It exists only as
//! a value in this process, handed straight to the enroll request — which is
//! also why the mint asks for the endpoint's short default TTL rather than the
//! portal's hour: if the enroll fails after the mint, the code is a live org
//! credential nobody is holding, and a short life bounds that window.

use spice_cloud_client::types::MintAdoptionCodeRequest;

use crate::commands::cloud::CloudClient;
use crate::error::{Error, Result};

/// A minted, unredeemed adoption code plus the org it is scoped to.
pub(crate) struct MintedCode {
    /// The plaintext code. Held only long enough to enroll with.
    pub(crate) code: String,
    /// The org the cloud resolved from the caller's token, for the enroll
    /// summary.
    pub(crate) org: Option<String>,
}

/// Mint a single-use adoption code for this host using the `spice login`
/// credential.
///
/// `org` is passed through as an assertion, not a selection: the cloud resolves
/// the org from the token and answers not-found on a mismatch, so a token bound
/// to one org can never quietly mint into it while the operator asked for
/// another.
///
/// # Errors
///
/// - Not authenticated: names both fixes (`spice login`, or paste a
///   portal-minted code).
/// - Authenticated without mint rights (403): names the portal-minted-code
///   path, because the caller's next action is a different path, not a retry.
/// - Org assertion mismatch or unknown org (404): names the org and how to
///   correct it.
pub(crate) async fn mint_adoption_code(org: Option<&str>) -> Result<MintedCode> {
    if !CloudClient::is_authenticated() {
        return Err(Error::InvalidArgument {
            message: "Failed to enroll this host with Spice Cloud: no adoption code was given and \
                      this host is not logged in. Either run `spice login` and re-run \
                      `spice connect` (the CLI then mints a code for you), or mint a code in the \
                      Spice Cloud portal and run `spice connect SPICE-ADOPT-XXXXX-XXXXX-XXXXX-XXXXX`. \
                      See: https://spiceai.org/docs"
                .to_string(),
        });
    }

    let client = CloudClient::new()?;
    let request = MintAdoptionCodeRequest {
        label: Some(mint_label()),
        // Take the endpoint's short default rather than pinning a value here,
        // so the CLI and the cloud cannot disagree about how long a
        // mint-and-redeem code lives.
        ttl_seconds: None,
        org: org.map(ToString::to_string),
    };

    match client.mint_instance_adoption_code(&request).await {
        Ok(response) => Ok(MintedCode {
            code: response.code,
            org: response.org,
        }),
        Err(err) => Err(mint_error(&err, org)),
    }
}

/// Label recorded on the adoption-codes screen, so a mint is traceable to the
/// host it was made for. Carries no secret.
fn mint_label() -> String {
    let hostname = gethostname::gethostname().to_string_lossy().into_owned();
    let hostname = if hostname.trim().is_empty() {
        "unknown host".to_string()
    } else {
        hostname
    };
    // Bounded well inside the endpoint's 200-character label cap.
    let hostname: String = hostname.chars().take(120).collect();
    format!("spice connect ({hostname})")
}

/// Turn a mint failure into an error that names the alternative path, since a
/// denied mint is not something the caller can retry into success.
fn mint_error(err: &Error, org: Option<&str>) -> Error {
    let detail = err.to_string();

    // A 403 means the login is valid but lacks org admin/owner. The fix is a
    // different path, not a retry.
    if detail.contains("Forbidden") {
        return Error::InvalidArgument {
            message: format!(
                "Failed to mint an adoption code for this host: {detail}. Minting requires org \
                 admin or owner. Ask an org admin to mint a code in the Spice Cloud portal and \
                 run `spice connect SPICE-ADOPT-XXXXX-XXXXX-XXXXX-XXXXX` on this host. \
                 See: https://spiceai.org/docs"
            ),
        };
    }

    // A 404 is the org assertion failing: the token is not bound to the org the
    // caller named (the response deliberately does not enumerate orgs).
    if detail.contains("Not found") {
        let message = match org {
            Some(org) => format!(
                "Failed to mint an adoption code for org {org}: {detail}. This login is not a \
                 member of {org}, or no such org exists. Run `spice cloud whoami` to see which \
                 org this login belongs to, pass the right `--org <name>`, or mint a code in the \
                 Spice Cloud portal. See: https://spiceai.org/docs"
            ),
            None => format!(
                "Failed to mint an adoption code for this host: {detail}. Run \
                 `spice cloud whoami` to check which org this login belongs to, or mint a code in \
                 the Spice Cloud portal and run `spice connect <code>`. \
                 See: https://spiceai.org/docs"
            ),
        };
        return Error::InvalidArgument { message };
    }

    // An expired or revoked login.
    if detail.contains("Unauthorized") {
        return Error::InvalidArgument {
            message: format!(
                "Failed to mint an adoption code for this host: {detail}. Re-run `spice login` \
                 and try again, or mint a code in the Spice Cloud portal and run \
                 `spice connect SPICE-ADOPT-XXXXX-XXXXX-XXXXX-XXXXX`. \
                 See: https://spiceai.org/docs"
            ),
        };
    }

    Error::CloudConnectEnroll {
        message: format!(
            "Failed to mint an adoption code for this host: {detail}. Nothing was enrolled — \
             retry, or mint a code in the Spice Cloud portal and run `spice connect <code>`. \
             See: https://spiceai.org/docs"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_names_the_host_and_stays_within_the_cap() {
        let label = mint_label();
        assert!(label.starts_with("spice connect ("), "{label}");
        assert!(
            label.len() <= 200,
            "label must fit the endpoint cap: {label}"
        );
    }

    #[test]
    fn forbidden_points_at_the_portal_path() {
        let err = mint_error(
            &Error::InvalidArgument {
                message: "Forbidden: requires org admin".to_string(),
            },
            None,
        );
        let message = err.to_string();
        assert!(message.contains("org admin"), "{message}");
        assert!(
            message.contains("Spice Cloud portal"),
            "a denied mint must name the portal-minted-code path: {message}"
        );
        assert!(message.contains("spice connect SPICE-ADOPT"), "{message}");
    }

    #[test]
    fn not_found_names_the_asserted_org() {
        let err = mint_error(
            &Error::InvalidArgument {
                message: "Not found: org".to_string(),
            },
            Some("acme"),
        );
        let message = err.to_string();
        assert!(message.contains("acme"), "{message}");
        assert!(
            message.contains("--org"),
            "the fix is a corrected --org: {message}"
        );
    }

    #[test]
    fn not_found_without_an_org_still_actionable() {
        let err = mint_error(
            &Error::InvalidArgument {
                message: "Not found: org".to_string(),
            },
            None,
        );
        let message = err.to_string();
        assert!(message.contains("spice cloud whoami"), "{message}");
    }

    #[test]
    fn unauthorized_says_to_log_in_again() {
        let err = mint_error(
            &Error::InvalidArgument {
                message: "Unauthorized: token expired".to_string(),
            },
            None,
        );
        assert!(err.to_string().contains("spice login"), "{err}");
    }

    #[test]
    fn other_failures_do_not_claim_anything_was_enrolled() {
        let err = mint_error(
            &Error::InvalidArgument {
                message: "API error (503): upstream".to_string(),
            },
            None,
        );
        let message = err.to_string();
        assert!(message.contains("Nothing was enrolled"), "{message}");
    }
}
