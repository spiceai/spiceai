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

//! Azure Key Vault as a [`JwtSigner`]: the vault holds an RSA private key, such as a GitHub
//! App's, which cannot be exported, and signs with it.
//!
//! Spice authenticates to the vault as the workload's Microsoft Entra identity, found the way
//! the Azure SDKs' default credential finds it, and with no configuration of its own:
//!
//! 1. A workload identity: `AZURE_TENANT_ID` and `AZURE_CLIENT_ID`, with the identity token from
//!    `AZURE_FEDERATED_TOKEN_FILE` (the AKS workload identity webhook sets all three) or, in a
//!    GitHub Actions job, the job's OIDC token.
//! 2. A managed identity (an Azure VM, App Service, Container Apps or Functions), user-assigned
//!    when `AZURE_CLIENT_ID` names one.
//! 3. The Azure CLI's signed-in account, for local development.

use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use azure_core::credentials::TokenCredential;
use azure_identity::{
    AzureCliCredential, ManagedIdentityCredential, ManagedIdentityCredentialOptions, UserAssignedId,
};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserialize;
use serde_json::{Value, json};
use snafu::prelude::*;
use token_provider::TokenProvider;
use token_provider::jwt_signer::{JwtSigner, SignError};
use token_provider::workload_identity::{
    ENTRA_AUDIENCE, Exchange, SubjectTokenSource, WorkloadIdentityError,
    WorkloadIdentityTokenProvider,
};

/// The Key Vault REST API version the signer calls.
const API_VERSION: &str = "7.4";

const KEY_VAULT_RBAC_DOCS: &str = "https://learn.microsoft.com/azure/key-vault/general/rbac-guide";

/// How long the managed identity endpoint has to answer. Outside Azure there is none, and the
/// wait is the cost of looking for one.
const MANAGED_IDENTITY_BUDGET: Duration = Duration::from_secs(3);

/// How long the Azure CLI has to answer: it is a process that may refresh its own token.
const AZURE_CLI_BUDGET: Duration = Duration::from_secs(20);

/// Bounds the reason quoted from an error response or a credential's error.
const MAX_REASON_LEN: usize = 300;

/// How much of an error response is read for its reason.
const MAX_ERROR_BODY_BYTES: usize = 4096;

/// A Key Vault or Managed HSM DNS suffix, the Microsoft Entra authority of its cloud, and the
/// scope of an access token for it.
struct Cloud {
    suffix: &'static str,
    authority: &'static str,
    scope: &'static str,
}

const CLOUDS: [Cloud; 6] = [
    Cloud {
        suffix: ".vault.azure.net",
        authority: "https://login.microsoftonline.com",
        scope: "https://vault.azure.net/.default",
    },
    Cloud {
        suffix: ".managedhsm.azure.net",
        authority: "https://login.microsoftonline.com",
        scope: "https://managedhsm.azure.net/.default",
    },
    Cloud {
        suffix: ".vault.usgovcloudapi.net",
        authority: "https://login.microsoftonline.us",
        scope: "https://vault.usgovcloudapi.net/.default",
    },
    Cloud {
        suffix: ".managedhsm.usgovcloudapi.net",
        authority: "https://login.microsoftonline.us",
        scope: "https://managedhsm.usgovcloudapi.net/.default",
    },
    Cloud {
        suffix: ".vault.azure.cn",
        authority: "https://login.chinacloudapi.cn",
        scope: "https://vault.azure.cn/.default",
    },
    Cloud {
        suffix: ".managedhsm.azure.cn",
        authority: "https://login.chinacloudapi.cn",
        scope: "https://managedhsm.azure.cn/.default",
    },
];

#[derive(Debug, Snafu)]
pub enum KeyVaultError {
    #[snafu(display(
        "'{value}' is not an Azure Key Vault key URL, which looks like https://my-vault.vault.azure.net/keys/github-app, optionally followed by /<version>, in a vault or Managed HSM of the Azure public cloud, Azure Government or Azure China."
    ))]
    InvalidKeyUrl { value: String },

    #[snafu(display(
        "Signing with the Azure Key Vault key '{key}' as a Microsoft Entra workload identity needs `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` beside `AZURE_FEDERATED_TOKEN_FILE`, but {missing} not set. On AKS, label the pod `azure.workload.identity/use: \"true\"` and annotate its service account with `azure.workload.identity/client-id`, so the workload identity webhook sets all three."
    ))]
    IncompleteWorkloadIdentity { key: String, missing: String },

    #[snafu(display("Invalid `AZURE_TENANT_ID` or `AZURE_AUTHORITY_HOST`: {source}"))]
    InvalidIdentity { source: WorkloadIdentityError },

    #[snafu(display(
        "Failed to get a Microsoft Entra access token for the Azure Key Vault key '{key}': {source}"
    ))]
    AccessToken {
        key: String,
        source: WorkloadIdentityError,
    },

    #[snafu(display(
        "Found no Microsoft Entra identity to sign with the Azure Key Vault key '{key}'. Tried a workload identity (`AZURE_FEDERATED_TOKEN_FILE` is not set, and this is not a GitHub Actions job with `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `permissions: id-token: write`), {tried}. On AKS, label the pod `azure.workload.identity/use: \"true\"`; on an Azure VM, App Service or Container Apps, assign the workload a managed identity; locally, run `az login`."
    ))]
    NoIdentity { key: String, tried: String },

    #[snafu(display("Failed to create the HTTP client for Azure Key Vault: {source}"))]
    HttpClient { source: reqwest::Error },

    // The sign errors are worded as the cause a caller's message names after the key it was
    // signing with.
    #[snafu(display("Failed to get a Microsoft Entra access token for Azure Key Vault: {reason}"))]
    CredentialToken { reason: String },

    #[snafu(display("Failed to reach Azure Key Vault: {source}"))]
    SignRequest { source: reqwest::Error },

    #[snafu(display("Azure Key Vault refused to sign (HTTP {status}): {reason}.{hint}"))]
    SignRefused {
        status: reqwest::StatusCode,
        reason: String,
        hint: String,
    },

    #[snafu(display("Failed to read Azure Key Vault's signature: {reason}"))]
    SignResponse { reason: String },
}

impl KeyVaultError {
    /// Whether the error is a mistake in the configuration, which retrying cannot fix, as
    /// opposed to an identity that is not there yet, a refusal, or an unreachable service.
    #[must_use]
    pub fn is_configuration(&self) -> bool {
        matches!(
            self,
            Self::InvalidKeyUrl { .. }
                | Self::IncompleteWorkloadIdentity { .. }
                | Self::InvalidIdentity { .. }
        )
    }
}

/// A key in Azure Key Vault or a Managed HSM.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyVaultKey {
    /// `https://<vault>.<suffix>/keys/<name>[/<version>]`.
    url: String,
    authority: &'static str,
    scope: &'static str,
}

/// Whether `segment` is a vault name, key name or key version: letters, digits and hyphens.
fn is_key_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment.len() <= 127
        && segment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
}

impl KeyVaultKey {
    /// The key a Key Vault key URL names. Without a version, the vault signs with the key's
    /// current version, so a rotated key is picked up at the next signature.
    ///
    /// # Errors
    ///
    /// [`KeyVaultError::InvalidKeyUrl`] when `value` is not an `https://` URL of a key in a
    /// vault or Managed HSM of a known Azure cloud. The vault receives a Microsoft Entra access
    /// token, so no other host is accepted.
    pub fn parse(value: &str) -> Result<Self, KeyVaultError> {
        let invalid = || KeyVaultError::InvalidKeyUrl {
            value: value.to_string(),
        };
        let url = reqwest::Url::parse(value.trim()).map_err(|_| invalid())?;
        ensure!(
            url.scheme() == "https"
                && url.port().is_none()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            InvalidKeyUrlSnafu { value }
        );
        let host = url.host_str().ok_or_else(invalid)?;
        let cloud = CLOUDS
            .iter()
            .find(|cloud| host.strip_suffix(cloud.suffix).is_some_and(is_key_segment))
            .ok_or_else(invalid)?;
        let segments: Vec<&str> = url.path().trim_matches('/').split('/').collect();
        let path = match segments.as_slice() {
            ["keys", name] if is_key_segment(name) => format!("keys/{name}"),
            ["keys", name, version] if is_key_segment(name) && is_key_segment(version) => {
                format!("keys/{name}/{version}")
            }
            _ => return Err(invalid()),
        };
        Ok(Self {
            url: format!("https://{host}/{path}"),
            authority: cloud.authority,
            scope: cloud.scope,
        })
    }

    /// The key's URL, normalized.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl fmt::Display for KeyVaultKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.url)
    }
}

/// The workload identity the environment gives the process, in the variables the Azure SDKs
/// read, and the GitHub Actions job it runs in, if any. An empty variable counts as unset.
#[derive(Clone, Debug, Default)]
pub struct AzureIdentityEnvironment {
    /// `AZURE_TENANT_ID`.
    pub tenant_id: Option<String>,
    /// `AZURE_CLIENT_ID`: the workload identity's application, or a user-assigned managed
    /// identity.
    pub client_id: Option<String>,
    /// `AZURE_FEDERATED_TOKEN_FILE`.
    pub federated_token_file: Option<String>,
    /// `AZURE_AUTHORITY_HOST`, which replaces the authority of the vault's cloud.
    pub authority_host: Option<String>,
    /// `ACTIONS_ID_TOKEN_REQUEST_URL`.
    pub github_actions_request_url: Option<String>,
    /// `ACTIONS_ID_TOKEN_REQUEST_TOKEN`.
    pub github_actions_request_token: Option<String>,
}

impl AzureIdentityEnvironment {
    /// The variables of this process.
    #[must_use]
    pub fn from_process() -> Self {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        Self {
            tenant_id: var("AZURE_TENANT_ID"),
            client_id: var("AZURE_CLIENT_ID"),
            federated_token_file: var("AZURE_FEDERATED_TOKEN_FILE"),
            authority_host: var("AZURE_AUTHORITY_HOST"),
            github_actions_request_url: var("ACTIONS_ID_TOKEN_REQUEST_URL"),
            github_actions_request_token: var("ACTIONS_ID_TOKEN_REQUEST_TOKEN"),
        }
    }
}

/// The workload identity in `env` for the vault of `key`, if the environment configures one: the
/// identity token's source, the Microsoft Entra exchange, and where the token comes from, as a
/// message names it. `None` when it configures none, and another identity is looked for.
///
/// # Errors
///
/// When `AZURE_FEDERATED_TOKEN_FILE` is set without `AZURE_TENANT_ID` or `AZURE_CLIENT_ID`, or
/// the tenant or authority is invalid.
pub fn workload_identity(
    key: &KeyVaultKey,
    env: &AzureIdentityEnvironment,
) -> Result<Option<(SubjectTokenSource, Exchange, &'static str)>, KeyVaultError> {
    let (source, origin) = match &env.federated_token_file {
        Some(path) => (
            SubjectTokenSource::File(path.into()),
            "`AZURE_FEDERATED_TOKEN_FILE`",
        ),
        None if env.tenant_id.is_some() && env.client_id.is_some() => {
            match SubjectTokenSource::github_actions(
                env.github_actions_request_url.clone(),
                env.github_actions_request_token.clone(),
                ENTRA_AUDIENCE,
            ) {
                Ok(source) => (source, "this GitHub Actions job"),
                Err(_) => return Ok(None),
            }
        }
        None => return Ok(None),
    };
    let (tenant_id, client_id) = match (&env.tenant_id, &env.client_id) {
        (Some(tenant_id), Some(client_id)) => (tenant_id, client_id),
        (tenant_id, _) => {
            let missing = if tenant_id.is_none() {
                "`AZURE_TENANT_ID` is"
            } else {
                "`AZURE_CLIENT_ID` is"
            };
            return IncompleteWorkloadIdentitySnafu {
                key: key.url.clone(),
                missing,
            }
            .fail();
        }
    };
    let exchange = Exchange::entra(
        Some(env.authority_host.as_deref().unwrap_or(key.authority)),
        tenant_id,
        client_id.clone(),
        key.scope,
    )
    .context(InvalidIdentitySnafu)?;
    Ok(Some((source, exchange, origin)))
}

/// `text` as a clause of a longer message: on one line, without a trailing period, and bounded.
fn clause(text: &str) -> String {
    let text = util::single_line(text);
    let text = text.trim().trim_end_matches('.');
    match text.char_indices().nth(MAX_REASON_LEN) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text.to_string(),
    }
}

/// An Azure SDK credential to try, as a message names it, and how long it has to answer.
type Candidate = (
    &'static str,
    azure_core::Result<Arc<dyn TokenCredential>>,
    Duration,
);

/// The first of `candidates` that gets an access token for `scope` within its budget, and its
/// name; or, when none does, what each said, as a message lists them.
async fn first_answering(
    candidates: Vec<Candidate>,
    scope: &str,
) -> Result<(Arc<dyn TokenCredential>, &'static str), String> {
    let mut tried = Vec::new();
    for (name, credential, budget) in candidates {
        let credential = match credential {
            Ok(credential) => credential,
            Err(error) => {
                tried.push(format!("{name} ({})", clause(&error.to_string())));
                continue;
            }
        };
        match tokio::time::timeout(budget, credential.get_token(&[scope], None)).await {
            Ok(Ok(_)) => return Ok((credential, name)),
            Ok(Err(error)) => tried.push(format!("{name} ({})", clause(&error.to_string()))),
            Err(_) => tried.push(format!("{name} (no answer within {}s)", budget.as_secs())),
        }
    }
    Err(match tried.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    })
}

/// How Spice gets access tokens for the vault.
enum Identity {
    /// A workload identity, exchanged by Spice and refreshed in the background.
    Workload(Arc<dyn TokenProvider>),
    /// An Azure SDK credential: a managed identity or the Azure CLI.
    Credential {
        credential: Arc<dyn TokenCredential>,
        scope: &'static str,
    },
}

impl Identity {
    async fn access_token(&self) -> Result<String, KeyVaultError> {
        match self {
            Self::Workload(provider) => Ok(provider.get_token()),
            Self::Credential { credential, scope } => credential
                .get_token(&[scope], None)
                .await
                .map(|token| token.token.secret().to_string())
                .map_err(|error| KeyVaultError::CredentialToken {
                    reason: clause(&error.to_string()),
                }),
        }
    }
}

/// Key Vault's answer to a sign request.
#[derive(Deserialize)]
struct SignResult {
    value: String,
}

/// A [`JwtSigner`] that has Azure Key Vault sign with a key it holds.
pub struct KeyVaultSigner {
    key: KeyVaultKey,
    identity: Identity,
    http: reqwest::Client,
}

impl fmt::Debug for KeyVaultSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyVaultSigner")
            .field("key", &self.key.url)
            .finish_non_exhaustive()
    }
}

impl KeyVaultSigner {
    /// Signs with `key` as the workload's Microsoft Entra identity: its workload identity in
    /// `env`, else its managed identity, else the Azure CLI's account. The identity's first
    /// access token is obtained here, so a missing or refused identity fails here. `component`
    /// names what the signatures authenticate, as the logs show it. Also returns the identity,
    /// as a message names it.
    ///
    /// # Errors
    ///
    /// When the workload identity is incomplete ([`KeyVaultError::is_configuration`]), Microsoft
    /// Entra ID refuses it, or no identity answers.
    pub async fn try_new(
        component: &str,
        key: KeyVaultKey,
        env: AzureIdentityEnvironment,
    ) -> Result<(Self, String), KeyVaultError> {
        if let Some((source, exchange, origin)) = workload_identity(&key, &env)? {
            let provider = WorkloadIdentityTokenProvider::try_new(component, source, exchange)
                .await
                .context(AccessTokenSnafu {
                    key: key.url.clone(),
                })?;
            return Ok((
                Self::new(key, Identity::Workload(Arc::new(provider)))?,
                format!(
                    "the Microsoft Entra workload identity in `AZURE_TENANT_ID` and `AZURE_CLIENT_ID`, with the identity token from {origin}"
                ),
            ));
        }

        let managed_identity =
            ManagedIdentityCredential::new(Some(ManagedIdentityCredentialOptions {
                user_assigned_id: env.client_id.clone().map(UserAssignedId::ClientId),
                ..ManagedIdentityCredentialOptions::default()
            }))
            .map(|credential| credential as Arc<dyn TokenCredential>);
        let azure_cli =
            AzureCliCredential::new(None).map(|credential| credential as Arc<dyn TokenCredential>);
        let candidates = vec![
            (
                "a managed identity",
                managed_identity,
                MANAGED_IDENTITY_BUDGET,
            ),
            ("the Azure CLI", azure_cli, AZURE_CLI_BUDGET),
        ];
        match first_answering(candidates, key.scope).await {
            Ok((credential, name)) => {
                let scope = key.scope;
                Ok((
                    Self::new(key, Identity::Credential { credential, scope })?,
                    match name {
                        "the Azure CLI" => "the Azure CLI's signed-in account".to_string(),
                        _ => "the workload's managed identity".to_string(),
                    },
                ))
            }
            Err(tried) => NoIdentitySnafu {
                key: key.url.clone(),
                tried,
            }
            .fail(),
        }
    }

    fn new(key: KeyVaultKey, identity: Identity) -> Result<Self, KeyVaultError> {
        let http = reqwest::Client::builder()
            .user_agent(util::spiceai_user_agent())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            // A sign request carries an access token for the vault; following a redirect could
            // send it elsewhere.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context(HttpClientSnafu)?;
        Ok(Self {
            key,
            identity,
            http,
        })
    }

    async fn sign(&self, digest: &[u8; 32]) -> Result<Vec<u8>, KeyVaultError> {
        let access_token = self.identity.access_token().await?;
        let response = self
            .http
            .post(format!("{}/sign?api-version={API_VERSION}", self.key.url))
            .bearer_auth(access_token)
            .json(&json!({"alg": "RS256", "value": URL_SAFE_NO_PAD.encode(digest)}))
            .send()
            .await
            .context(SignRequestSnafu)?;
        let status = response.status();
        if !status.is_success() {
            let body = http_body_limit::read_prefix(response, MAX_ERROR_BODY_BYTES).await;
            return SignRefusedSnafu {
                status,
                reason: refusal_reason(&body),
                hint: refusal_hint(status),
            }
            .fail();
        }
        let unreadable = |reason: String| KeyVaultError::SignResponse { reason };
        let body = http_body_limit::buffer(response, http_body_limit::Limit::METADATA)
            .await
            .map_err(|error| unreadable(error.to_string()))?
            .bytes()
            .await
            .map_err(|error| unreadable(error.to_string()))?;
        let result: SignResult =
            serde_json::from_slice(&body).map_err(|error| unreadable(error.to_string()))?;
        URL_SAFE_NO_PAD
            .decode(result.value.trim_end_matches('='))
            .map_err(|error| unreadable(format!("the signature is not base64url: {error}")))
    }
}

#[async_trait]
impl JwtSigner for KeyVaultSigner {
    async fn sign_sha256_digest(&self, digest: &[u8; 32]) -> Result<Vec<u8>, SignError> {
        self.sign(digest)
            .await
            .map_err(|error| Box::new(error) as _)
    }

    fn key_id(&self) -> &str {
        &self.key.url
    }
}

/// Key Vault's `error.message`, or the body itself, on one bounded line.
fn refusal_reason(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body);
    let reason = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| value["error"]["message"].as_str().map(str::to_string))
        .unwrap_or_else(|| body.to_string());
    clause(&reason)
}

/// What to check when Key Vault refuses a sign request with `status`, as a sentence that
/// follows the reason, or nothing.
fn refusal_hint(status: reqwest::StatusCode) -> String {
    match status.as_u16() {
        401 => " Verify that the identity's tenant is the vault's.".to_string(),
        403 => format!(
            " Grant the identity the Key Vault Crypto User role on the key, or the `sign` key permission if the vault uses access policies: {KEY_VAULT_RBAC_DOCS}"
        ),
        404 => " Verify the key's name and version, and that the vault holds it.".to_string(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use aws_lc_rs::digest::{Digest as LcDigest, SHA256};
    use aws_lc_rs::rsa::{KeyPair, KeySize};
    use aws_lc_rs::signature::RSA_PKCS1_SHA256;
    use axum::{
        Json, Router,
        extract::{Query, State},
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::post,
    };
    use azure_core::credentials::{AccessToken, TokenRequestOptions};
    use azure_core::time::OffsetDateTime;
    use token_provider::StaticTokenProvider;

    use super::*;

    /// The RS256 signature of a message whose SHA-256 digest is `digest`, as a key service
    /// holding `key_pair` makes it.
    fn sign_digest(key_pair: &KeyPair, digest: &[u8]) -> Vec<u8> {
        let digest = LcDigest::import_less_safe(digest, &SHA256).expect("a SHA-256 digest");
        let mut signature = vec![0; key_pair.public_modulus_len()];
        key_pair
            .sign_digest(&RSA_PKCS1_SHA256, &digest, &mut signature)
            .expect("sign the digest");
        signature
    }

    #[test]
    fn a_key_url_names_a_key_of_a_vault_or_managed_hsm_in_a_known_cloud() {
        let parse = |value: &str| {
            KeyVaultKey::parse(value)
                .map(|key| (key.url, key.authority, key.scope))
                .map_err(|error| error.to_string())
        };
        assert_eq!(
            [
                parse("https://spice-ci.vault.azure.net/keys/github-app"),
                parse(
                    " https://Spice-CI.vault.azure.net/keys/github-app/0123456789abcdef0123456789abcdef/ "
                ),
                parse("https://spice.managedhsm.azure.net/keys/github-app"),
                parse("https://spice.vault.usgovcloudapi.net/keys/github-app"),
                parse("https://spice.vault.azure.cn/keys/github-app"),
            ],
            [
                Ok((
                    "https://spice-ci.vault.azure.net/keys/github-app".to_string(),
                    "https://login.microsoftonline.com",
                    "https://vault.azure.net/.default"
                )),
                Ok((
                    "https://spice-ci.vault.azure.net/keys/github-app/0123456789abcdef0123456789abcdef".to_string(),
                    "https://login.microsoftonline.com",
                    "https://vault.azure.net/.default"
                )),
                Ok((
                    "https://spice.managedhsm.azure.net/keys/github-app".to_string(),
                    "https://login.microsoftonline.com",
                    "https://managedhsm.azure.net/.default"
                )),
                Ok((
                    "https://spice.vault.usgovcloudapi.net/keys/github-app".to_string(),
                    "https://login.microsoftonline.us",
                    "https://vault.usgovcloudapi.net/.default"
                )),
                Ok((
                    "https://spice.vault.azure.cn/keys/github-app".to_string(),
                    "https://login.chinacloudapi.cn",
                    "https://vault.azure.cn/.default"
                )),
            ]
        );
        for value in [
            "http://spice.vault.azure.net/keys/github-app",
            "https://spice.vault.azure.net:8443/keys/github-app",
            "https://vault.azure.net/keys/github-app",
            "https://a.b.vault.azure.net/keys/github-app",
            "https://spice.vault.azure.net.example.com/keys/github-app",
            "https://example.com/keys/github-app",
            "https://spice.vault.azure.net/secrets/github-app",
            "https://spice.vault.azure.net/keys/",
            "https://spice.vault.azure.net/keys/github_app",
            "https://spice.vault.azure.net/keys/a/b/c",
            "https://spice.vault.azure.net/keys/github-app?x=1",
            "https://user@spice.vault.azure.net/keys/github-app",
            "spice.vault.azure.net/keys/github-app",
        ] {
            assert_eq!(
                parse(value),
                Err(format!(
                    "'{value}' is not an Azure Key Vault key URL, which looks like https://my-vault.vault.azure.net/keys/github-app, optionally followed by /<version>, in a vault or Managed HSM of the Azure public cloud, Azure Government or Azure China."
                )),
                "{value:?}"
            );
        }
    }

    fn key() -> KeyVaultKey {
        KeyVaultKey::parse("https://spice-ci.vault.azure.net/keys/github-app").expect("a key URL")
    }

    #[test]
    fn a_workload_identity_is_a_token_file_or_a_github_actions_job_with_the_azure_variables() {
        let aks = AzureIdentityEnvironment {
            tenant_id: Some("contoso.onmicrosoft.com".to_string()),
            client_id: Some("00000000-0000-0000-0000-000000000001".to_string()),
            federated_token_file: Some(
                "/var/run/secrets/azure/tokens/azure-identity-token".to_string(),
            ),
            ..AzureIdentityEnvironment::default()
        };
        let actions = AzureIdentityEnvironment {
            federated_token_file: None,
            authority_host: Some("https://login.microsoftonline.us".to_string()),
            github_actions_request_url: Some(
                "https://actions.example/id?api-version=2.0".to_string(),
            ),
            github_actions_request_token: Some("request-token".to_string()),
            ..aks.clone()
        };
        let describe = |env: AzureIdentityEnvironment| {
            workload_identity(&key(), &env)
                .map(|identity| {
                    identity.map(|(source, exchange, origin)| {
                        let Exchange::Entra {
                            token_url,
                            client_id,
                            scope,
                        } = exchange
                        else {
                            panic!("an Entra exchange, not {exchange:?}");
                        };
                        (format!("{source:?}"), token_url, client_id, scope, origin)
                    })
                })
                .map_err(|error| error.to_string())
        };
        assert_eq!(
            describe(aks.clone()),
            Ok(Some((
                r#"File("/var/run/secrets/azure/tokens/azure-identity-token")"#.to_string(),
                "https://login.microsoftonline.com/contoso.onmicrosoft.com/oauth2/v2.0/token"
                    .to_string(),
                "00000000-0000-0000-0000-000000000001".to_string(),
                "https://vault.azure.net/.default".to_string(),
                "`AZURE_FEDERATED_TOKEN_FILE`",
            )))
        );
        assert_eq!(
            describe(actions.clone()).map(|identity| identity.map(|d| (d.0, d.1, d.4))),
            Ok(Some((
                r#"GitHubActions { audience: "api://AzureADTokenExchange", .. }"#.to_string(),
                "https://login.microsoftonline.us/contoso.onmicrosoft.com/oauth2/v2.0/token"
                    .to_string(),
                "this GitHub Actions job",
            )))
        );

        // Without a token file or a GitHub Actions job, another identity is looked for.
        assert_eq!(
            describe(AzureIdentityEnvironment {
                github_actions_request_url: None,
                ..actions
            }),
            Ok(None)
        );
        assert_eq!(describe(AzureIdentityEnvironment::default()), Ok(None));

        assert_eq!(
            [
                describe(AzureIdentityEnvironment {
                    tenant_id: None,
                    ..aks.clone()
                }),
                describe(AzureIdentityEnvironment {
                    client_id: None,
                    ..aks.clone()
                }),
                describe(AzureIdentityEnvironment {
                    tenant_id: Some("../common".to_string()),
                    ..aks
                }),
            ],
            [
                Err("Signing with the Azure Key Vault key 'https://spice-ci.vault.azure.net/keys/github-app' as a Microsoft Entra workload identity needs `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` beside `AZURE_FEDERATED_TOKEN_FILE`, but `AZURE_TENANT_ID` is not set. On AKS, label the pod `azure.workload.identity/use: \"true\"` and annotate its service account with `azure.workload.identity/client-id`, so the workload identity webhook sets all three.".to_string()),
                Err("Signing with the Azure Key Vault key 'https://spice-ci.vault.azure.net/keys/github-app' as a Microsoft Entra workload identity needs `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` beside `AZURE_FEDERATED_TOKEN_FILE`, but `AZURE_CLIENT_ID` is not set. On AKS, label the pod `azure.workload.identity/use: \"true\"` and annotate its service account with `azure.workload.identity/client-id`, so the workload identity webhook sets all three.".to_string()),
                Err("Invalid `AZURE_TENANT_ID` or `AZURE_AUTHORITY_HOST`: The Microsoft Entra tenant must be a directory (tenant) ID, which is a GUID, or a domain name such as 'contoso.onmicrosoft.com'.".to_string()),
            ]
        );
    }

    /// A credential that answers with `outcome`, after `delay`, and records the scopes it was
    /// asked for.
    #[derive(Debug)]
    struct FakeCredential {
        outcome: Result<&'static str, &'static str>,
        delay: Duration,
        scopes: Mutex<Vec<String>>,
    }

    impl FakeCredential {
        fn new(outcome: Result<&'static str, &'static str>, delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                outcome,
                delay,
                scopes: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl TokenCredential for FakeCredential {
        async fn get_token(
            &self,
            scopes: &[&str],
            _options: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            self.scopes
                .lock()
                .expect("lock")
                .extend(scopes.iter().map(|scope| (*scope).to_string()));
            tokio::time::sleep(self.delay).await;
            match self.outcome {
                Ok(token) => Ok(AccessToken::new(
                    token,
                    OffsetDateTime::now_utc() + Duration::from_secs(3600),
                )),
                Err(reason) => Err(azure_core::Error::with_message(
                    azure_core::error::ErrorKind::Credential,
                    reason,
                )),
            }
        }
    }

    #[tokio::test]
    async fn the_first_identity_that_answers_within_its_budget_signs_and_the_others_are_listed() {
        let hanging = FakeCredential::new(Ok("never"), Duration::from_secs(60));
        let refusing = FakeCredential::new(
            Err("ERROR: Please run 'az login' to setup account.\n"),
            Duration::ZERO,
        );
        let answering = FakeCredential::new(Ok("vault-token"), Duration::ZERO);
        let candidates = |answering: Option<Arc<FakeCredential>>| {
            let mut candidates: Vec<Candidate> = vec![
                (
                    "a managed identity",
                    Ok(Arc::clone(&hanging) as Arc<dyn TokenCredential>),
                    Duration::from_millis(50),
                ),
                (
                    "a broken credential",
                    Err(azure_core::Error::with_message(
                        azure_core::error::ErrorKind::Credential,
                        "Azure Arc managed identity isn't supported",
                    )),
                    Duration::from_millis(50),
                ),
                (
                    "the Azure CLI",
                    Ok(Arc::clone(&refusing) as Arc<dyn TokenCredential>),
                    Duration::from_millis(50),
                ),
            ];
            if let Some(answering) = answering {
                candidates.push((
                    "the last resort",
                    Ok(answering as Arc<dyn TokenCredential>),
                    Duration::from_millis(50),
                ));
            }
            candidates
        };

        let (credential, name) =
            first_answering(candidates(Some(Arc::clone(&answering))), key().scope)
                .await
                .expect("the last candidate answers");
        assert_eq!(name, "the last resort");
        let token = credential
            .get_token(&[key().scope], None)
            .await
            .expect("a token");
        assert_eq!(token.token.secret(), "vault-token");
        assert_eq!(
            *answering.scopes.lock().expect("lock"),
            vec!["https://vault.azure.net/.default".to_string(); 2]
        );

        let tried = first_answering(candidates(None), key().scope)
            .await
            .expect_err("no candidate answers");
        assert_eq!(
            KeyVaultError::NoIdentity {
                key: key().url,
                tried
            }
            .to_string(),
            "Found no Microsoft Entra identity to sign with the Azure Key Vault key 'https://spice-ci.vault.azure.net/keys/github-app'. Tried a workload identity (`AZURE_FEDERATED_TOKEN_FILE` is not set, and this is not a GitHub Actions job with `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and `permissions: id-token: write`), a managed identity (no answer within 0s), a broken credential (Azure Arc managed identity isn't supported) and the Azure CLI (ERROR: Please run 'az login' to setup account). On AKS, label the pod `azure.workload.identity/use: \"true\"`; on an Azure VM, App Service or Container Apps, assign the workload a managed identity; locally, run `az login`."
        );
    }

    /// A sign request as the mock vault received it: its query, `Authorization` and body.
    type VaultRequest = (HashMap<String, String>, Option<String>, Value);

    /// What the mock vault received, and the key it signs with.
    struct Vault {
        key_pair: KeyPair,
        requests: Mutex<Vec<VaultRequest>>,
        refuse: bool,
    }

    async fn sign(
        State(vault): State<Arc<Vault>>,
        Query(query): Query<HashMap<String, String>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Response {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        vault
            .requests
            .lock()
            .expect("lock")
            .push((query, authorization, body.clone()));
        if vault.refuse {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": {"code": "Forbidden", "message": "Caller is not authorized to perform action on resource.\r\nIf role assignments, deny assignments or role definitions were changed recently, please observe propagation time.\r\nAction: 'Microsoft.KeyVault/vaults/keys/sign/action'", "innererror": {"code": "ForbiddenByRbac"}}})),
            )
                .into_response();
        }
        let digest = URL_SAFE_NO_PAD
            .decode(body["value"].as_str().expect("a digest"))
            .expect("base64url");
        Json(json!({
            "kid": "https://spice-ci.vault.azure.net/keys/github-app/0123456789abcdef0123456789abcdef",
            "value": URL_SAFE_NO_PAD.encode(sign_digest(&vault.key_pair, &digest)),
        }))
        .into_response()
    }

    /// A signer for a mock vault at `http://127.0.0.1:<port>/keys/github-app`, with `identity`.
    async fn start(refuse: bool, identity: Identity) -> (KeyVaultSigner, Arc<Vault>) {
        let vault = Arc::new(Vault {
            key_pair: KeyPair::generate(KeySize::Rsa2048).expect("generate an RSA key"),
            requests: Mutex::new(Vec::new()),
            refuse,
        });
        let app = Router::new()
            .route("/keys/github-app/sign", post(sign))
            .with_state(Arc::clone(&vault));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the mock vault");
        let address = listener.local_addr().expect("mock address");
        tokio::spawn(async move { axum::serve(listener, app).await });
        let vault_key = KeyVaultKey {
            url: format!("http://{address}/keys/github-app"),
            ..self::key()
        };
        let signer = KeyVaultSigner::new(vault_key, identity).expect("a signer");
        (signer, vault)
    }

    #[tokio::test]
    async fn the_vault_signs_the_digest_with_the_identitys_access_token() {
        for (identity, expected) in [
            (
                Identity::Workload(Arc::new(StaticTokenProvider::new(
                    "workload-access-token".into(),
                ))),
                "Bearer workload-access-token",
            ),
            (
                Identity::Credential {
                    credential: FakeCredential::new(Ok("managed-access-token"), Duration::ZERO),
                    scope: "https://vault.azure.net/.default",
                },
                "Bearer managed-access-token",
            ),
        ] {
            let (signer, vault) = start(false, identity).await;
            let digest = [7u8; 32];
            let signature = signer
                .sign_sha256_digest(&digest)
                .await
                .expect("the vault signs");
            assert_eq!(signature, sign_digest(&vault.key_pair, &digest));
            assert_eq!(
                vault.requests.lock().expect("lock").clone(),
                vec![(
                    HashMap::from([("api-version".to_string(), "7.4".to_string())]),
                    Some(expected.to_string()),
                    json!({"alg": "RS256", "value": URL_SAFE_NO_PAD.encode(digest)}),
                )]
            );
        }
    }

    #[tokio::test]
    async fn a_refused_signature_names_the_vaults_reason_and_the_role_to_grant() {
        let (signer, _vault) = start(
            true,
            Identity::Workload(Arc::new(StaticTokenProvider::new("token".into()))),
        )
        .await;
        let error = signer
            .sign_sha256_digest(&[7u8; 32])
            .await
            .expect_err("the vault refuses");
        assert_eq!(
            error.to_string(),
            "Azure Key Vault refused to sign (HTTP 403 Forbidden): Caller is not authorized to perform action on resource.  If role assignments, deny assignments or role definitions were changed recently, please observe propagation time.  Action: 'Microsoft.KeyVault/vaults/keys/sign/action'. Grant the identity the Key Vault Crypto User role on the key, or the `sign` key permission if the vault uses access policies: https://learn.microsoft.com/azure/key-vault/general/rbac-guide"
        );
    }
}
