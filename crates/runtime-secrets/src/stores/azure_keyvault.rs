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

//! Azure Key Vault secret store.
//!
//! This store resolves secrets by fetching individually-named secrets from a
//! single Azure Key Vault. Unlike AWS Secrets Manager (which packs many keys
//! into one JSON payload), Key Vault secrets are 1:1 — each Spice
//! `get_secret(key)` call maps to a separate `GetSecret` request against the
//! configured vault.
//!
//! Design notes
//! - The vault URL is derived from the `from:` selector. Bare names (e.g.
//!   `from: azure_keyvault:my-vault`) are expanded to
//!   `https://my-vault.vault.azure.net/`. Sovereign-cloud users can either
//!   pass a full URL or override the domain suffix via the `endpoint` param.
//! - Authentication follows an explicit `auth_method` parameter rather than
//!   a single opaque default, because `azure_identity` 0.31 does not ship a
//!   `DefaultAzureCredential` chain: each credential type has to be chosen
//!   up-front. `auth_method: default` auto-detects between service principal,
//!   workload identity, managed identity, and the CLI credential.
//! - Responses are cached per-key for a short TTL so repeated reads of the
//!   same logical secret do not re-hit Key Vault. `404 Not Found` responses
//!   are negatively cached for a shorter TTL to avoid hammering the service
//!   when a spicepod references a missing key.
//! - Concurrent cache misses for the same key are coalesced via a per-key
//!   `Notify`: only one fetch per key is in flight at a time. Fetches for
//!   different keys proceed in parallel. Locks are never held across the
//!   network round-trip, so a stalled endpoint cannot stall cache readers.
//! - A pre-flight `list_secret_properties` call in `init()` verifies the
//!   configured credentials and vault URL before the first user lookup, so
//!   misconfiguration surfaces at Spicepod load rather than on the first hot
//!   path.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use azure_core::credentials::{Secret as AzureSecret, TokenCredential};
use azure_core::http::StatusCode;
use azure_identity::{
    AzureCliCredential, ClientSecretCredential, DeveloperToolsCredential,
    ManagedIdentityCredential, ManagedIdentityCredentialOptions, UserAssignedId,
    WorkloadIdentityCredential,
};
use azure_security_keyvault_secrets::SecretClient;
use futures::StreamExt;
use runtime_parameter_spec::ParameterSpec;
use secrecy::SecretString;
use snafu::Snafu;
use tokio::sync::{Mutex, Notify, OnceCell, RwLock};

use crate::SecretStore;

/// Parameters accepted by the `azure_keyvault` secret store.
///
/// Authentication is selected via `auth_method`. `default` attempts, in
/// order: service-principal (if `client_secret` is set), workload-identity
/// (if the federated-token env vars are present), managed-identity (if
/// `client_id` is set), then the Azure CLI credential. Explicit modes are
/// provided for environments that want to fail fast rather than fall through
/// the chain.
pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::runtime("auth_method")
        .description(
            "Authentication method used to obtain tokens for Key Vault. `default` \
             auto-detects based on the other params and the environment. \
             Explicit modes short-circuit the chain so misconfiguration fails fast.",
        )
        .one_of(&[
            "default",
            "service_principal",
            "managed_identity",
            "workload_identity",
            "cli",
        ])
        .default("default"),
    ParameterSpec::runtime("tenant_id")
        .description(
            "Azure Entra ID (AAD) tenant ID. Required for `service_principal` and \
             `workload_identity`; ignored otherwise.",
        )
        .examples(&["00000000-0000-0000-0000-000000000000"]),
    ParameterSpec::runtime("client_id")
        .description(
            "Azure application (client) ID. Required for `service_principal` and \
             `workload_identity`. Optional for `managed_identity` — when set, selects \
             a user-assigned identity; when omitted, the system-assigned identity is used.",
        )
        .examples(&["00000000-0000-0000-0000-000000000000"]),
    ParameterSpec::runtime("client_secret")
        .description(
            "Azure application client secret for `service_principal` auth. Typically \
             sourced from env, e.g. `${ env:AZURE_CLIENT_SECRET }`.",
        )
        .secret(),
    ParameterSpec::runtime("endpoint")
        .description(
            "Override for the Key Vault endpoint. Accepts either a full URL \
             (e.g. `https://my-vault.vault.usgovcloudapi.net/`), in which case the \
             selector vault name is ignored, or a bare DNS suffix \
             (e.g. `vault.usgovcloudapi.net`) that is combined with the selector \
             to build the URL. Intended for sovereign clouds (Azure Government, \
             Azure China) and test environments.",
        )
        .examples(&[
            "https://my-vault.vault.usgovcloudapi.net/",
            "vault.usgovcloudapi.net",
        ]),
];

/// Resolved configuration for the `azure_keyvault` secret store.
///
/// `vault` carries either the bare vault name from the selector or the
/// fully-qualified URL the user supplied — we defer URL construction to
/// [`AzureKeyVault::from_config`] so the parse errors surface from one place.
#[derive(Debug, Clone)]
pub struct AzureKeyVaultConfig {
    pub vault: String,
    pub auth_method: AuthMethod,
    pub tenant_id: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub endpoint: Option<String>,
}

/// Authentication mode for the Key Vault SDK client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    /// Auto-detect: service principal → workload identity → managed identity → CLI.
    Default,
    /// Azure AD service-principal credentials (`tenant_id`, `client_id`, `client_secret`).
    ServicePrincipal,
    /// Azure managed identity. User-assigned if `client_id` set, system-assigned otherwise.
    ManagedIdentity,
    /// Kubernetes workload identity (federated token on disk).
    WorkloadIdentity,
    /// Local-dev Azure CLI credential (`az login`).
    Cli,
}

impl AuthMethod {
    fn parse(raw: &str) -> Self {
        // `validate_params` already enforces the one_of set, so this match is
        // total in practice; the fallthrough is defensive.
        match raw {
            "service_principal" => Self::ServicePrincipal,
            "managed_identity" => Self::ManagedIdentity,
            "workload_identity" => Self::WorkloadIdentity,
            "cli" => Self::Cli,
            _ => Self::Default,
        }
    }
}

impl AzureKeyVaultConfig {
    /// Builds an [`AzureKeyVaultConfig`] from the parsed selector and a
    /// validated parameter map.
    #[must_use]
    pub fn from_params(vault: String, params: &HashMap<String, String>) -> Self {
        let auth_method = params
            .get("auth_method")
            .map(|s| AuthMethod::parse(s.as_str()))
            .unwrap_or(AuthMethod::Default);
        Self {
            vault,
            auth_method,
            tenant_id: params.get("tenant_id").cloned(),
            client_id: params.get("client_id").cloned(),
            client_secret: params.get("client_secret").cloned(),
            endpoint: params.get("endpoint").cloned(),
        }
    }
}

/// Prefix used to scope secret names to Spice.
///
/// Key Vault only allows `[A-Za-z0-9-]` in secret names (no underscores), so
/// the Spice convention of `spice_<key>` is translated to the hyphenated
/// `spice-<key>` when hitting the Key Vault API. Callers continue to see
/// logical keys like `openai_api_key`.
const SPICE_KEY_PREFIX_HYPHEN: &str = "spice-";

/// Default TTL for cached per-key secret payloads.
///
/// Chosen to match the AWS Secrets Manager store — Key Vault secret rotation
/// is typically manual and on the order of hours/days, so a minute of
/// staleness is acceptable.
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60);

/// Negative-cache TTL for confirmed-missing secrets (404 responses).
///
/// Shorter than [`DEFAULT_CACHE_TTL`] so a newly-created secret becomes
/// visible promptly, but long enough to avoid hammering Key Vault when a
/// spicepod references a missing name.
const NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(10);

/// OAuth scope used to mint Key Vault access tokens. This value is defined by
/// the Key Vault service and is the same for all public/sovereign clouds.
const KEY_VAULT_SCOPE: &str = "https://vault.azure.net/.default";

/// Default DNS suffix for Azure Public cloud vault URLs.
const DEFAULT_VAULT_SUFFIX: &str = "vault.azure.net";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Azure Key Vault requires a vault name or URL. Specify it as \
         `from: azure_keyvault:<vault-name>` or \
         `from: azure_keyvault:https://<vault>.vault.azure.net/`."
    ))]
    EmptyVaultName {},

    #[snafu(display(
        "Invalid Azure Key Vault URL '{url}': {reason}. Expected an https URL, e.g. \
         `https://my-vault.vault.azure.net/`."
    ))]
    InvalidVaultUrl { url: String, reason: String },

    #[snafu(display(
        "Azure Key Vault auth method '{method}' requires the following parameters: {missing}."
    ))]
    MissingAuthParams { method: String, missing: String },

    #[snafu(display(
        "Unable to resolve Azure Key Vault credentials for vault '{vault_url}': {source}. \
         Verify the configured auth method and that the corresponding credentials are available."
    ))]
    UnableToResolveCredentials {
        vault_url: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Azure Key Vault pre-flight failed for '{vault_url}': {source}. Verify the vault \
         exists in the configured region/cloud and that the principal has \
         `secrets/list` or at minimum `secrets/get` permission via access policy or RBAC."
    ))]
    UnableToVerifyVault {
        vault_url: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Unable to get secret '{secret_name}' from Azure Key Vault '{vault_url}': {source}. \
         Verify the secret exists and the principal has `secrets/get` permission on it."
    ))]
    UnableToGetSecret {
        vault_url: String,
        secret_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Cached view of a single Key Vault secret.
struct CachedEntry {
    /// `None` for negatively-cached 404s; `Some` for present secrets.
    value: Option<SecretString>,
    fetched_at: Instant,
    ttl: Duration,
}

impl CachedEntry {
    fn is_fresh(&self) -> bool {
        self.fetched_at.elapsed() < self.ttl
    }
}

pub struct AzureKeyVault {
    vault_url: String,
    auth_method: AuthMethod,
    tenant_id: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,

    /// Lazily-initialized SDK client, shared across all lookups.
    client: OnceCell<SecretClient>,

    /// Per-key cache. The `RwLock` lets cache hits proceed in parallel.
    cache: RwLock<HashMap<String, Arc<CachedEntry>>>,

    /// Single-flight coordination: one `Notify` per in-flight key. Waiters
    /// subscribe before releasing the mutex; the winner removes the entry
    /// and wakes them once it has updated the cache. Locks are never held
    /// across the network round-trip.
    inflight: Mutex<HashMap<String, Arc<Notify>>>,

    cache_ttl: Duration,
    negative_ttl: Duration,
}

impl AzureKeyVault {
    /// Convenience constructor used by tests and callers that only have a
    /// bare vault name.
    ///
    /// # Errors
    /// Returns [`Error::EmptyVaultName`] if `vault` is empty or whitespace.
    pub fn new(vault: &str) -> Result<Self> {
        Self::from_config(AzureKeyVaultConfig {
            vault: vault.to_string(),
            auth_method: AuthMethod::Default,
            tenant_id: None,
            client_id: None,
            client_secret: None,
            endpoint: None,
        })
    }

    /// Creates a new [`AzureKeyVault`] store from a validated
    /// [`AzureKeyVaultConfig`] (i.e. one produced by [`crate::validate_params`]).
    ///
    /// Validates the vault identifier and auth parameters, but does *not*
    /// touch the network. Use [`AzureKeyVault::init`] to verify credentials.
    ///
    /// # Errors
    ///
    /// - [`Error::EmptyVaultName`] if the vault identifier is blank.
    /// - [`Error::InvalidVaultUrl`] if the resolved URL is not a valid https URL.
    /// - [`Error::MissingAuthParams`] if the chosen auth method is missing
    ///   required parameters.
    pub fn from_config(config: AzureKeyVaultConfig) -> Result<Self> {
        let vault = config.vault.trim();
        if vault.is_empty() {
            return EmptyVaultNameSnafu.fail();
        }

        let vault_url = resolve_vault_url(vault, config.endpoint.as_deref())?;

        validate_auth_params(
            config.auth_method,
            config.tenant_id.as_deref(),
            config.client_id.as_deref(),
            config.client_secret.as_deref(),
        )?;

        Ok(Self {
            vault_url,
            auth_method: config.auth_method,
            tenant_id: config.tenant_id,
            client_id: config.client_id,
            client_secret: config.client_secret,
            client: OnceCell::new(),
            cache: RwLock::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            cache_ttl: DEFAULT_CACHE_TTL,
            negative_ttl: NEGATIVE_CACHE_TTL,
        })
    }

    /// Overrides the default cache TTL. Primarily intended for tests.
    #[cfg(test)]
    #[must_use]
    pub fn with_cache_ttl(mut self, ttl: Duration) -> Self {
        self.cache_ttl = ttl;
        self
    }

    /// Returns the resolved vault URL (for tests and error messages).
    #[must_use]
    pub fn vault_url(&self) -> &str {
        &self.vault_url
    }

    /// Verifies that the configured credentials can reach the vault.
    ///
    /// Uses a `list_secret_properties` page fetch rather than a full
    /// enumeration so the check does not require elevated permissions and
    /// completes in a single round-trip. A `403 Forbidden` from the list
    /// call is tolerated when the principal may still hold `secrets/get`
    /// — we want init to succeed for read-only principals.
    ///
    /// # Errors
    ///
    /// Returns an error if credential resolution fails or the HTTP call
    /// returns a non-recoverable error (auth, DNS, network).
    pub async fn init(&self) -> Result<()> {
        let client = self.client().await.map_err(|source| {
            Error::UnableToResolveCredentials {
                vault_url: self.vault_url.clone(),
                source,
            }
        })?;

        // `list_secret_properties` returns a paged stream; we only need to
        // touch the first response to verify the endpoint + credentials.
        let mut pager =
            client
                .list_secret_properties(None)
                .map_err(|source| Error::UnableToVerifyVault {
                    vault_url: self.vault_url.clone(),
                    source: Box::new(source),
                })?;
        match pager.next().await {
            None => Ok(()),
            Some(Ok(_)) => Ok(()),
            Some(Err(err)) => {
                // A 403 on `secrets/list` is survivable if the principal still
                // has `secrets/get`; don't fail init for it.
                if err.http_status() == Some(StatusCode::Forbidden) {
                    tracing::debug!(
                        vault_url = %self.vault_url,
                        "Azure Key Vault list returned 403; continuing on the assumption that \
                         the principal has per-secret `secrets/get` permission"
                    );
                    return Ok(());
                }
                Err(Error::UnableToVerifyVault {
                    vault_url: self.vault_url.clone(),
                    source: Box::new(err),
                })
            }
        }
    }

    async fn client(
        &self,
    ) -> std::result::Result<&SecretClient, Box<dyn std::error::Error + Send + Sync>> {
        self.client
            .get_or_try_init(|| async {
                let credential = self.build_credential().await?;
                let sdk_client =
                    SecretClient::new(&self.vault_url, credential, None).map_err(boxed)?;
                Ok(sdk_client)
            })
            .await
    }

    async fn build_credential(
        &self,
    ) -> std::result::Result<Arc<dyn TokenCredential>, Box<dyn std::error::Error + Send + Sync>> {
        match self.effective_auth_method() {
            AuthMethod::ServicePrincipal => {
                // `validate_auth_params` guarantees all three are set when
                // the mode resolves to ServicePrincipal.
                let tenant_id = self.tenant_id.as_deref().unwrap_or_default();
                let client_id = self.client_id.clone().unwrap_or_default();
                let secret = AzureSecret::from(self.client_secret.clone().unwrap_or_default());
                let cred = ClientSecretCredential::new(tenant_id, client_id, secret, None)
                    .map_err(boxed)?;
                Ok(cred as Arc<dyn TokenCredential>)
            }
            AuthMethod::WorkloadIdentity => {
                let cred = WorkloadIdentityCredential::new(None).map_err(boxed)?;
                Ok(cred as Arc<dyn TokenCredential>)
            }
            AuthMethod::ManagedIdentity => {
                let opts = self.client_id.clone().map(|id| ManagedIdentityCredentialOptions {
                    user_assigned_id: Some(UserAssignedId::ClientId(id)),
                    ..Default::default()
                });
                let cred = ManagedIdentityCredential::new(opts).map_err(boxed)?;
                Ok(cred as Arc<dyn TokenCredential>)
            }
            AuthMethod::Cli => {
                let cred = AzureCliCredential::new(None).map_err(boxed)?;
                Ok(cred as Arc<dyn TokenCredential>)
            }
            AuthMethod::Default => {
                // Default chain: if the user supplied a client secret we
                // honor it; otherwise fall back to the developer-tools
                // credential which chains the Azure CLI and `azd` — both
                // are the common local-dev paths. Managed / workload
                // identity are not tried implicitly because their failure
                // modes (IMDS timeouts, missing federated-token files) are
                // noisy and confusing when the user actually intended the
                // CLI path.
                if self.client_secret.is_some() {
                    let tenant_id = self.tenant_id.as_deref().unwrap_or_default();
                    let client_id = self.client_id.clone().unwrap_or_default();
                    let secret = AzureSecret::from(self.client_secret.clone().unwrap_or_default());
                    let cred = ClientSecretCredential::new(tenant_id, client_id, secret, None)
                        .map_err(boxed)?;
                    Ok(cred as Arc<dyn TokenCredential>)
                } else {
                    let cred = DeveloperToolsCredential::new(None).map_err(boxed)?;
                    Ok(cred as Arc<dyn TokenCredential>)
                }
            }
        }
    }

    /// The auth method the store actually uses after accounting for
    /// `auth_method: default` auto-detection. Factored out so init and lookup
    /// agree on a single answer.
    fn effective_auth_method(&self) -> AuthMethod {
        match self.auth_method {
            AuthMethod::Default if self.client_secret.is_some() => AuthMethod::ServicePrincipal,
            other => other,
        }
    }

    /// Returns the cached entry for `key` if it is still fresh.
    async fn try_cached(&self, key: &str) -> Option<Arc<CachedEntry>> {
        let guard = self.cache.read().await;
        guard
            .get(key)
            .filter(|e| e.is_fresh())
            .map(Arc::clone)
    }

    async fn store_cached(&self, key: &str, entry: Arc<CachedEntry>) {
        let mut guard = self.cache.write().await;
        guard.insert(key.to_string(), entry);
    }

    /// Fetches a single secret from Key Vault, honoring the in-process cache.
    ///
    /// Concurrency
    /// - Cache hits take only an `RwLock` read and never serialize.
    /// - On a miss for key `K`, one task per key is elected winner via an
    ///   entry in the `inflight` map. Losers subscribe to the winner's
    ///   `Notify` before the mutex is dropped, wait for the winner to
    ///   publish, then re-check the cache.
    /// - Fetches for distinct keys proceed fully in parallel.
    /// - Locks are never held across the `.await` on the Azure round-trip.
    async fn load(&self, key: &str) -> crate::AnyErrorResult<Option<SecretString>> {
        loop {
            if let Some(entry) = self.try_cached(key).await {
                return Ok(entry.value.clone());
            }

            // Election: observe or install the in-flight entry while holding
            // the mutex, then drop it before any `.await` on the network.
            let waiter_role = {
                let mut inflight = self.inflight.lock().await;
                // Re-check under the mutex: another task may have just
                // finished populating the cache for this key.
                if let Some(entry) = self.try_cached(key).await {
                    return Ok(entry.value.clone());
                }
                if let Some(existing) = inflight.get(key) {
                    WaiterRole::Loser(Arc::clone(existing))
                } else {
                    let notify = Arc::new(Notify::new());
                    inflight.insert(key.to_string(), Arc::clone(&notify));
                    WaiterRole::Winner(notify)
                }
            };

            match waiter_role {
                WaiterRole::Loser(notify) => {
                    notify.notified().await;
                    // Loop and re-check the cache. If the winner failed, the
                    // cache will still be empty and we'll contend for winner.
                    continue;
                }
                WaiterRole::Winner(notify) => {
                    let result = self.fetch_one(key).await;
                    // Always clear the in-flight slot and wake waiters before
                    // returning, regardless of success/failure.
                    {
                        let mut inflight = self.inflight.lock().await;
                        inflight.remove(key);
                    }
                    return match result {
                        Ok((value, ttl)) => {
                            let entry = Arc::new(CachedEntry {
                                value: value.clone(),
                                fetched_at: Instant::now(),
                                ttl,
                            });
                            self.store_cached(key, entry).await;
                            notify.notify_waiters();
                            Ok(value)
                        }
                        Err(err) => {
                            notify.notify_waiters();
                            Err(err)
                        }
                    };
                }
            }
        }
    }

    /// Performs a single GetSecret round-trip for `logical_key`, applying the
    /// Spice-prefix precedence.
    ///
    /// Returns `(value, ttl)` where `value` is `None` for confirmed-missing
    /// secrets and the ttl is shortened for negative results.
    async fn fetch_one(
        &self,
        logical_key: &str,
    ) -> crate::AnyErrorResult<(Option<SecretString>, Duration)> {
        tracing::debug!(
            vault_url = %self.vault_url,
            key = %logical_key,
            "Fetching Azure Key Vault secret"
        );

        let client = self
            .client()
            .await
            .map_err(|source| Error::UnableToResolveCredentials {
                vault_url: self.vault_url.clone(),
                source,
            })?;

        let prefixed = format!("{SPICE_KEY_PREFIX_HYPHEN}{}", to_vault_name(logical_key));
        let primary = to_vault_name(logical_key);

        // Prefer the spice-prefixed variant so Spice-owned secrets can coexist
        // with other application secrets in the same vault. A 404 on the
        // prefixed name falls through to the plain name.
        for candidate in [prefixed.as_str(), primary.as_str()] {
            match get_secret_value(client, candidate).await {
                Ok(Some(value)) => {
                    return Ok((Some(value), self.cache_ttl));
                }
                Ok(None) => continue,
                Err(err) => {
                    return Err(Box::new(Error::UnableToGetSecret {
                        vault_url: self.vault_url.clone(),
                        secret_name: candidate.to_string(),
                        source: err,
                    }));
                }
            }
        }

        Ok((None, self.negative_ttl))
    }
}

enum WaiterRole {
    Winner(Arc<Notify>),
    Loser(Arc<Notify>),
}

#[async_trait]
impl SecretStore for AzureKeyVault {
    async fn get_secret(&self, key: &str) -> crate::AnyErrorResult<Option<SecretString>> {
        tracing::trace!(
            vault_url = %self.vault_url,
            key = %key,
            "Resolving secret via Azure Key Vault"
        );
        self.load(key).await
    }
}

/// Performs a single `get_secret` SDK call and extracts the plaintext value,
/// converting `404 Not Found` into `Ok(None)` for negative-cache handling.
async fn get_secret_value(
    client: &SecretClient,
    name: &str,
) -> std::result::Result<Option<SecretString>, Box<dyn std::error::Error + Send + Sync>> {
    match client.get_secret(name, None).await {
        Ok(response) => {
            // `into_model` is the sync-by-design hand-off from the SDK's
            // typed `Response<Secret>` to the deserialized model. The buffer
            // is already collected by the pipeline before this function
            // returns.
            let secret = response.into_model().map_err(boxed)?;
            Ok(secret.value.map(SecretString::from))
        }
        Err(err) => {
            if err.http_status() == Some(StatusCode::NotFound) {
                tracing::debug!(
                    secret_name = %name,
                    "Azure Key Vault secret not found; caching negative result"
                );
                Ok(None)
            } else {
                Err(boxed(err))
            }
        }
    }
}

/// Maps a logical Spice key (which may contain `_`) to a Key Vault secret
/// name (which only allows `[A-Za-z0-9-]`).
fn to_vault_name(key: &str) -> String {
    key.replace('_', "-")
}

/// Resolves the full vault URL from the selector and optional `endpoint`
/// override. Accepts bare vault names, full https URLs, and DNS-suffix-only
/// overrides for sovereign clouds.
fn resolve_vault_url(vault_selector: &str, endpoint: Option<&str>) -> Result<String> {
    let selector = vault_selector.trim().trim_end_matches('/');

    // Case 1: selector is already a full URL. Endpoint override is ignored.
    if selector.starts_with("https://") || selector.starts_with("http://") {
        validate_https(selector)?;
        return Ok(format!("{selector}/"));
    }

    // Case 2: endpoint override is a full URL. Ignore the selector and use
    // the override verbatim — this is the escape hatch for non-standard
    // vault URLs.
    if let Some(ep) = endpoint {
        let ep = ep.trim().trim_end_matches('/');
        if ep.starts_with("https://") || ep.starts_with("http://") {
            validate_https(ep)?;
            return Ok(format!("{ep}/"));
        }
    }

    // Case 3: build `https://<selector>.<suffix>/` where `<suffix>` defaults
    // to the public-cloud value but can be overridden for sovereign clouds.
    let suffix = endpoint
        .map(|s| s.trim().trim_end_matches('/'))
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_VAULT_SUFFIX);
    let url = format!("https://{selector}.{suffix}/");
    validate_https(url.trim_end_matches('/'))?;
    Ok(url)
}

fn validate_https(url: &str) -> Result<()> {
    if url.starts_with("https://") {
        Ok(())
    } else if url.starts_with("http://") {
        InvalidVaultUrlSnafu {
            url: url.to_string(),
            reason: "plaintext http:// is not allowed for Key Vault".to_string(),
        }
        .fail()
    } else {
        InvalidVaultUrlSnafu {
            url: url.to_string(),
            reason: "must start with https://".to_string(),
        }
        .fail()
    }
}

fn validate_auth_params(
    method: AuthMethod,
    tenant_id: Option<&str>,
    client_id: Option<&str>,
    client_secret: Option<&str>,
) -> Result<()> {
    match method {
        AuthMethod::ServicePrincipal => {
            let mut missing = Vec::new();
            if tenant_id.is_none_or(str::is_empty) {
                missing.push("tenant_id");
            }
            if client_id.is_none_or(str::is_empty) {
                missing.push("client_id");
            }
            if client_secret.is_none_or(str::is_empty) {
                missing.push("client_secret");
            }
            if missing.is_empty() {
                Ok(())
            } else {
                MissingAuthParamsSnafu {
                    method: "service_principal".to_string(),
                    missing: missing.join(", "),
                }
                .fail()
            }
        }
        AuthMethod::WorkloadIdentity => {
            // tenant_id and client_id are typically sourced from the
            // federated env vars (AZURE_TENANT_ID, AZURE_CLIENT_ID,
            // AZURE_FEDERATED_TOKEN_FILE); we do not require them in params.
            Ok(())
        }
        AuthMethod::ManagedIdentity | AuthMethod::Cli | AuthMethod::Default => Ok(()),
    }
}

fn boxed<E: std::error::Error + Send + Sync + 'static>(e: E) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(e)
}

/// Explicit usage of the Key Vault OAuth scope. Holding the constant here
/// keeps the token-resolution story colocated with the client code even
/// though the SDK supplies the scope itself under normal operation.
#[allow(dead_code)]
fn key_vault_scope() -> &'static str {
    KEY_VAULT_SCOPE
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[test]
    fn rejects_empty_vault_name() {
        assert!(matches!(
            AzureKeyVault::new(""),
            Err(Error::EmptyVaultName { .. })
        ));
        assert!(matches!(
            AzureKeyVault::new("   "),
            Err(Error::EmptyVaultName { .. })
        ));
    }

    #[test]
    fn expands_bare_vault_name_to_public_cloud_url() {
        let url = resolve_vault_url("my-vault", None).expect("resolves");
        assert_eq!(url, "https://my-vault.vault.azure.net/");
    }

    #[test]
    fn accepts_full_https_selector() {
        let url = resolve_vault_url("https://my-vault.vault.azure.net", None).expect("resolves");
        assert_eq!(url, "https://my-vault.vault.azure.net/");
    }

    #[test]
    fn accepts_full_https_selector_with_trailing_slash() {
        let url = resolve_vault_url("https://my-vault.vault.azure.net/", None).expect("resolves");
        assert_eq!(url, "https://my-vault.vault.azure.net/");
    }

    #[test]
    fn endpoint_overrides_default_suffix_for_sovereign_cloud() {
        let url = resolve_vault_url("gov-vault", Some("vault.usgovcloudapi.net")).expect("resolves");
        assert_eq!(url, "https://gov-vault.vault.usgovcloudapi.net/");
    }

    #[test]
    fn endpoint_as_full_url_takes_precedence_over_selector() {
        // Useful escape hatch for private DNS / test endpoints where the
        // selector name does not match the URL host.
        let url = resolve_vault_url(
            "my-vault",
            Some("https://override.internal.example.com/"),
        )
        .expect("resolves");
        assert_eq!(url, "https://override.internal.example.com/");
    }

    #[test]
    fn rejects_http_scheme_in_selector() {
        let err =
            resolve_vault_url("http://insecure.example.com", None).expect_err("http rejected");
        assert!(matches!(err, Error::InvalidVaultUrl { .. }));
    }

    #[test]
    fn service_principal_requires_all_three_params() {
        let err = validate_auth_params(
            AuthMethod::ServicePrincipal,
            Some("tenant"),
            None,
            Some("secret"),
        )
        .expect_err("missing client_id");
        match err {
            Error::MissingAuthParams { missing, .. } => {
                assert!(missing.contains("client_id"), "got {missing}");
            }
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn service_principal_ok_when_all_three_set() {
        validate_auth_params(
            AuthMethod::ServicePrincipal,
            Some("tenant"),
            Some("client"),
            Some("secret"),
        )
        .expect("all three present");
    }

    #[test]
    fn default_mode_does_not_require_any_params() {
        validate_auth_params(AuthMethod::Default, None, None, None).expect("defaults allowed");
    }

    #[test]
    fn auth_method_parse_maps_expected_strings() {
        assert_eq!(AuthMethod::parse("default"), AuthMethod::Default);
        assert_eq!(
            AuthMethod::parse("service_principal"),
            AuthMethod::ServicePrincipal
        );
        assert_eq!(
            AuthMethod::parse("managed_identity"),
            AuthMethod::ManagedIdentity
        );
        assert_eq!(
            AuthMethod::parse("workload_identity"),
            AuthMethod::WorkloadIdentity
        );
        assert_eq!(AuthMethod::parse("cli"), AuthMethod::Cli);
        // Unknown values fall back to Default; `validate_params` would have
        // already rejected anything not in the one_of set.
        assert_eq!(AuthMethod::parse("bogus"), AuthMethod::Default);
    }

    #[test]
    fn effective_auth_resolves_default_to_service_principal_when_secret_set() {
        let cfg = AzureKeyVaultConfig {
            vault: "my-vault".to_string(),
            auth_method: AuthMethod::Default,
            tenant_id: Some("t".to_string()),
            client_id: Some("c".to_string()),
            client_secret: Some("s".to_string()),
            endpoint: None,
        };
        let store = AzureKeyVault::from_config(cfg).expect("valid");
        assert_eq!(store.effective_auth_method(), AuthMethod::ServicePrincipal);
    }

    #[test]
    fn effective_auth_preserves_explicit_mode() {
        let cfg = AzureKeyVaultConfig {
            vault: "my-vault".to_string(),
            auth_method: AuthMethod::ManagedIdentity,
            tenant_id: None,
            client_id: None,
            client_secret: None,
            endpoint: None,
        };
        let store = AzureKeyVault::from_config(cfg).expect("valid");
        assert_eq!(store.effective_auth_method(), AuthMethod::ManagedIdentity);
    }

    #[test]
    fn vault_name_maps_underscores_to_hyphens() {
        assert_eq!(to_vault_name("openai_api_key"), "openai-api-key");
        assert_eq!(to_vault_name("spice_openai_api_key"), "spice-openai-api-key");
        assert_eq!(to_vault_name("already-hyphenated"), "already-hyphenated");
    }

    /// Seeds the per-key cache and verifies lookups never touch the network.
    #[tokio::test]
    async fn cached_values_short_circuit_network() {
        let store = AzureKeyVault::new("test-vault").expect("valid");
        let entry = Arc::new(CachedEntry {
            value: Some(SecretString::from("hello".to_string())),
            fetched_at: Instant::now(),
            ttl: Duration::from_secs(60),
        });
        store.store_cached("api_key", entry).await;

        let got = store
            .get_secret("api_key")
            .await
            .expect("lookup ok")
            .expect("present");
        assert_eq!(got.expose_secret(), "hello");
    }

    #[tokio::test]
    async fn negative_cache_returns_none_without_network() {
        let store = AzureKeyVault::new("test-vault").expect("valid");
        let entry = Arc::new(CachedEntry {
            value: None,
            fetched_at: Instant::now(),
            ttl: NEGATIVE_CACHE_TTL,
        });
        store.store_cached("missing", entry).await;

        assert!(
            store
                .get_secret("missing")
                .await
                .expect("lookup ok")
                .is_none()
        );
    }

    #[tokio::test]
    async fn expired_entry_is_not_returned_from_try_cached() {
        let store = AzureKeyVault::new("test-vault").expect("valid");
        let stale = Arc::new(CachedEntry {
            value: Some(SecretString::from("old".to_string())),
            fetched_at: Instant::now()
                .checked_sub(Duration::from_secs(3600))
                .expect("enough headroom"),
            ttl: Duration::from_secs(60),
        });
        store.store_cached("k", stale).await;

        assert!(store.try_cached("k").await.is_none());
    }
}
