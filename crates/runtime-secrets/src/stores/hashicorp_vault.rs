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

//! `HashiCorp` Vault secret store.
//!
//! This store resolves secrets by fetching a single Vault KV path whose
//! data field is a JSON object mapping keys to string values. Each
//! `get_secret(key)` lookup is satisfied from that map, mirroring the
//! semantics of the AWS Secrets Manager store (one selector → many keys).
//!
//! Design notes
//! - Supported auth methods: `token`, `approle`, `kubernetes`, `jwt`. AWS
//!   IAM auth is intentionally deferred — it requires sigv4-signing a
//!   `sts:GetCallerIdentity` request, which doubles the dependency
//!   surface, and most users on AWS reach for `aws_secrets_manager`
//!   instead. Adding `aws_iam` later is purely additive.
//! - Both KV v1 and v2 mounts are supported; the path layout differs
//!   (`/v1/{mount}/{path}` vs `/v1/{mount}/data/{path}`) and the response
//!   shape differs (`data.<key>` vs `data.data.<key>`).
//! - The `from:` selector is the path *under the mount*, e.g.
//!   `from: hashicorp_vault:myapp/config` with `hashicorp_vault_mount: secret`,
//!   `hashicorp_vault_kv_version: v2` reads `/v1/secret/data/myapp/config`.
//! - The Vault client token is cached and refreshed lazily: on a 403 the
//!   store re-authenticates once and retries the data read. We do not
//!   spawn a background renewal task — that would tie the store's
//!   lifetime to a runtime task and complicate shutdown for a feature
//!   most users will hit only at config-load time.
//! - Concurrent cache misses for the data payload are coalesced behind a
//!   single async `Mutex` so only one `GET` is in flight per store. The
//!   payload is wrapped in `Arc<HashMap<…>>` so readers share one
//!   allocation rather than cloning the (secret-bearing) map.
//! - URL validation goes through `url::Url` rather than a prefix check so
//!   invalid addresses fail at config load with a concrete message.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reqwest::{Client, ClientBuilder, StatusCode};
use runtime_parameter_spec::ParameterSpec;
use secrecy::{ExposeSecret, SecretString};
use snafu::{ResultExt, Snafu};
use tokio::sync::{Mutex, RwLock};

use crate::SecretStore;

/// Parameters accepted by the `hashicorp_vault` secret store.
///
/// Authentication is selected via `hashicorp_vault_auth_method`. The `token` method
/// requires `hashicorp_vault_token`; `AppRole` requires `hashicorp_vault_role_id` + `hashicorp_vault_secret_id`;
/// Kubernetes and JWT require `hashicorp_vault_role` and a JWT (Kubernetes can also
/// read it from `hashicorp_vault_kubernetes_token_path`, defaulting to the in-cluster
/// service-account token).
pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::runtime("hashicorp_vault_address")
        .description(
            "Vault server URL, e.g. `https://vault.example.com:8200`. \
             Plaintext `http://` is allowed only for `localhost` / `127.0.0.1` \
             so local dev with `vault server -dev` works without TLS.",
        )
        .required()
        .examples(&["https://vault.example.com:8200", "http://127.0.0.1:8200"]),
    ParameterSpec::runtime("hashicorp_vault_namespace")
        .description(
            "Vault Enterprise namespace, sent as the `X-Vault-Namespace` header. \
             Leave unset for OSS Vault.",
        )
        .examples(&["admin", "admin/team-a"]),
    ParameterSpec::runtime("hashicorp_vault_mount")
        .description(
            "Mount path of the KV secrets engine. Defaults to `secret`, the path \
             that `vault server -dev` provisions automatically.",
        )
        .default("secret"),
    ParameterSpec::runtime("hashicorp_vault_kv_version")
        .description(
            "KV engine version. v2 (the modern default) supports versioning and \
             rolls the read URL through `/data/`; v1 is the legacy layout.",
        )
        .one_of(&["v1", "v2"])
        .default("v2"),
    ParameterSpec::runtime("hashicorp_vault_auth_method")
        .description(
            "Authentication method used to obtain a Vault client token. \
             `token` uses the supplied `hashicorp_vault_token` directly. `approle`, \
             `kubernetes`, and `jwt` perform a login round-trip and cache the \
             returned client token until its lease expires.",
        )
        .one_of(&["token", "approle", "kubernetes", "jwt"])
        .default("token"),
    ParameterSpec::runtime("hashicorp_vault_token")
        .description(
            "Vault client token for `auth_method: token`. Typically sourced \
             from env, e.g. `${ env:VAULT_TOKEN }`.",
        )
        .secret(),
    ParameterSpec::runtime("hashicorp_vault_role_id")
        .description("AppRole `role_id`. Required for `auth_method: approle`."),
    ParameterSpec::runtime("hashicorp_vault_secret_id")
        .description(
            "AppRole `secret_id`. Required for `auth_method: approle`. Typically \
             sourced from env, e.g. `${ env:VAULT_SECRET_ID }`.",
        )
        .secret(),
    ParameterSpec::runtime("hashicorp_vault_role").description(
        "Vault role name. Required for `auth_method: kubernetes` and \
             `auth_method: jwt`.",
    ),
    ParameterSpec::runtime("hashicorp_vault_jwt")
        .description(
            "JWT/OIDC token presented for `auth_method: jwt`, or the Kubernetes \
             service-account JWT for `auth_method: kubernetes` when not reading \
             it from disk. Typically sourced from env.",
        )
        .secret(),
    ParameterSpec::runtime("hashicorp_vault_kubernetes_token_path")
        .description(
            "Filesystem path to the service-account JWT for `auth_method: kubernetes`. \
             Defaults to `/var/run/secrets/kubernetes.io/serviceaccount/token`. \
             Ignored unless `hashicorp_vault_jwt` is unset.",
        )
        .examples(&["/var/run/secrets/kubernetes.io/serviceaccount/token"]),
    ParameterSpec::runtime("hashicorp_vault_auth_mount").description(
        "Mount path of the auth backend, *without* the leading `auth/` segment. \
             Defaults to the auth method name (e.g. `approle`, `kubernetes`, `jwt`). \
             Override when the backend has been mounted at a non-default path \
             (e.g. `k8s-prod`). A leading `auth/` is tolerated and stripped.",
    ),
    ParameterSpec::runtime("hashicorp_vault_ca_cert")
        .description(
            "Filesystem path to a PEM-encoded CA certificate to add to the TLS \
             trust store. Use for self-signed Vault deployments.",
        )
        .examples(&["/etc/ssl/vault-ca.pem"]),
    ParameterSpec::runtime("hashicorp_vault_tls_skip_verify")
        .description(
            "Skip TLS certificate verification. Strongly discouraged outside \
             local development.",
        )
        .is_boolean()
        .default("false"),
    ParameterSpec::runtime("hashicorp_vault_request_timeout")
        .description("Per-request timeout in seconds for Vault HTTP calls. Defaults to 10.")
        .default("10"),
];

/// Default cache TTL applied when Vault does not return a `lease_duration`
/// for the data secret (typical for KV which has no lease itself).
const DEFAULT_DATA_TTL: Duration = Duration::from_secs(60);
/// Negative-cache TTL for confirmed-missing paths (404).
const NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(10);
/// Default request timeout when none is configured.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Default mount path for the Kubernetes service-account token, matching
/// Vault's own default expectation.
const DEFAULT_K8S_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Vault secret store requires a path selector. Specify it as \
         `from: hashicorp_vault:<kv-path>`, e.g. `from: hashicorp_vault:myapp/config`."
    ))]
    EmptyPath {},

    #[snafu(display(
        "Vault secret store requires `hashicorp_vault_address`. Set it in the store's `params:` block, \
         e.g. `hashicorp_vault_address: https://vault.example.com:8200`."
    ))]
    MissingAddress {},

    #[snafu(display(
        "Invalid Vault address '{address}': {reason}. Expected an https URL with a host, \
         e.g. `https://vault.example.com:8200`."
    ))]
    InvalidAddress { address: String, reason: String },

    #[snafu(display("Invalid value '{value}' for parameter '{parameter}': {reason}."))]
    InvalidNumericParameter {
        parameter: String,
        value: String,
        reason: String,
    },

    #[snafu(display("Vault auth method '{method}' requires the following parameters: {missing}."))]
    MissingAuthParams { method: String, missing: String },

    #[snafu(display("Unable to read Kubernetes service-account token at '{path}': {source}"))]
    UnableToReadKubernetesToken {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Unable to read Vault CA certificate at '{path}': {source}"))]
    UnableToReadCaCert {
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Vault CA certificate at '{path}' is not valid PEM: {source}"))]
    InvalidCaCert {
        path: String,
        source: reqwest::Error,
    },

    #[snafu(display("Failed to build Vault HTTP client: {source}"))]
    ClientBuild { source: reqwest::Error },

    #[snafu(display(
        "Vault login at '{address}' (auth method '{method}') failed: {source}. \
         Verify the auth backend is mounted and the supplied credentials are valid."
    ))]
    Login {
        address: String,
        method: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Vault returned status {status} from {url}: {body}. \
         Verify the path exists and the principal has `read` permission."
    ))]
    UnexpectedStatus {
        url: String,
        status: StatusCode,
        body: String,
    },

    #[snafu(display("Network error talking to Vault at '{url}': {source}"))]
    Http { url: String, source: reqwest::Error },

    #[snafu(display(
        "Vault response for '{url}' was not valid JSON: {source}. \
         Enable `runtime_secrets::stores::hashicorp_vault=debug` logging to inspect the body."
    ))]
    MalformedResponse {
        url: String,
        source: serde_json::Error,
    },

    #[snafu(display("Vault login response for '{address}' was missing `auth.client_token`."))]
    LoginMissingToken { address: String },

    #[snafu(display(
        "Vault response for '{url}' was missing the expected `data` field for KV {kv_version}."
    ))]
    MissingData { url: String, kv_version: KvVersion },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// KV engine version. The path layout and response shape differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvVersion {
    V1,
    V2,
}

impl KvVersion {
    fn parse(raw: &str) -> Self {
        // `validate_params` already enforces the one_of set, so `v1` is the
        // only non-default outcome; the fallthrough is defensive.
        match raw {
            "v1" => Self::V1,
            _ => Self::V2,
        }
    }
}

impl std::fmt::Display for KvVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        })
    }
}

/// Authentication method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    Token,
    AppRole,
    Kubernetes,
    Jwt,
}

impl AuthMethod {
    fn parse(raw: &str) -> Self {
        match raw {
            "approle" => Self::AppRole,
            "kubernetes" => Self::Kubernetes,
            "jwt" => Self::Jwt,
            _ => Self::Token,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Token => "token",
            Self::AppRole => "approle",
            Self::Kubernetes => "kubernetes",
            Self::Jwt => "jwt",
        }
    }
}

/// Resolved configuration for the `hashicorp_vault` secret store.
///
/// No `Debug` derive: `token`, `secret_id`, and `jwt` are sensitive. The
/// manual impl below redacts them.
#[derive(Clone)]
pub struct HashicorpVaultConfig {
    pub path: String,
    pub address: String,
    pub namespace: Option<String>,
    pub mount: String,
    pub kv_version: KvVersion,
    pub auth_method: AuthMethod,
    pub auth_mount: Option<String>,
    pub token: Option<SecretString>,
    pub role_id: Option<String>,
    pub secret_id: Option<SecretString>,
    pub role: Option<String>,
    pub jwt: Option<SecretString>,
    pub kubernetes_token_path: Option<String>,
    pub ca_cert: Option<String>,
    pub tls_skip_verify: bool,
    pub request_timeout: Duration,
}

impl std::fmt::Debug for HashicorpVaultConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashicorpVaultConfig")
            .field("path", &self.path)
            .field("address", &self.address)
            .field("namespace", &self.namespace)
            .field("mount", &self.mount)
            .field("kv_version", &self.kv_version)
            .field("auth_method", &self.auth_method)
            .field("auth_mount", &self.auth_mount)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("role_id", &self.role_id)
            .field("secret_id", &self.secret_id.as_ref().map(|_| "<redacted>"))
            .field("role", &self.role)
            .field("jwt", &self.jwt.as_ref().map(|_| "<redacted>"))
            .field("kubernetes_token_path", &self.kubernetes_token_path)
            .field("ca_cert", &self.ca_cert)
            .field("tls_skip_verify", &self.tls_skip_verify)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl HashicorpVaultConfig {
    /// Builds a [`HashicorpVaultConfig`] from the parsed selector and a validated
    /// parameter map.
    ///
    /// Empty / whitespace-only string params are normalized to `None` so
    /// users can leave keys defined-but-blank in templated configs without
    /// silently turning `auth_method: token` + `hashicorp_vault_token: ""` into a
    /// confusing 400 from Vault.
    ///
    /// # Errors
    ///
    /// - [`Error::InvalidNumericParameter`] if `hashicorp_vault_request_timeout` is
    ///   present but not a non-negative integer (in seconds). All other
    ///   semantic checks are deferred to [`HashicorpVault::from_config`] so the
    ///   spicepod can be loaded with partial configuration in tests.
    pub fn from_params(path: String, params: &HashMap<String, String>) -> Result<Self> {
        fn non_empty(s: Option<&String>) -> Option<String> {
            let trimmed = s.map(|v| v.trim())?;
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }

        let auth_method = params
            .get("hashicorp_vault_auth_method")
            .map_or(AuthMethod::Token, |s| AuthMethod::parse(s.as_str()));
        let kv_version = params
            .get("hashicorp_vault_kv_version")
            .map_or(KvVersion::V2, |s| KvVersion::parse(s.as_str()));
        let mount =
            non_empty(params.get("hashicorp_vault_mount")).unwrap_or_else(|| "secret".to_string());
        let tls_skip_verify = params
            .get("hashicorp_vault_tls_skip_verify")
            .is_some_and(|s| s.eq_ignore_ascii_case("true"));
        let request_timeout = match non_empty(params.get("hashicorp_vault_request_timeout")) {
            None => DEFAULT_REQUEST_TIMEOUT,
            Some(raw) => {
                let parsed = raw
                    .parse::<u64>()
                    .map_err(|e| Error::InvalidNumericParameter {
                        parameter: "hashicorp_vault_request_timeout".to_string(),
                        value: raw.clone(),
                        reason: format!("expected a non-negative integer in seconds: {e}"),
                    })?;
                Duration::from_secs(parsed)
            }
        };

        Ok(Self {
            path,
            // `hashicorp_vault_address` is required at the spec level so this is
            // populated in normal use; the empty default is checked in
            // `from_config` for a clean error.
            address: non_empty(params.get("hashicorp_vault_address")).unwrap_or_default(),
            namespace: non_empty(params.get("hashicorp_vault_namespace")),
            mount,
            kv_version,
            auth_method,
            auth_mount: non_empty(params.get("hashicorp_vault_auth_mount")),
            token: non_empty(params.get("hashicorp_vault_token")).map(SecretString::from),
            role_id: non_empty(params.get("hashicorp_vault_role_id")),
            secret_id: non_empty(params.get("hashicorp_vault_secret_id")).map(SecretString::from),
            role: non_empty(params.get("hashicorp_vault_role")),
            jwt: non_empty(params.get("hashicorp_vault_jwt")).map(SecretString::from),
            kubernetes_token_path: non_empty(params.get("hashicorp_vault_kubernetes_token_path")),
            ca_cert: non_empty(params.get("hashicorp_vault_ca_cert")),
            tls_skip_verify,
            request_timeout,
        })
    }
}

/// Cached Vault client token plus its expiry.
struct CachedToken {
    value: SecretString,
    expires_at: Option<Instant>,
}

impl CachedToken {
    fn is_fresh(&self) -> bool {
        match self.expires_at {
            // No expiry == static token (auth_method: token) or `lease_duration: 0`.
            None => true,
            // Renew slightly early so an in-flight request doesn't race the lease.
            Some(t) => Instant::now() + Duration::from_secs(5) < t,
        }
    }
}

/// Cached KV payload.
struct CachedPayload {
    data: Arc<HashMap<String, SecretString>>,
    fetched_at: Instant,
    ttl: Duration,
}

impl CachedPayload {
    fn is_fresh(&self) -> bool {
        self.fetched_at.elapsed() < self.ttl
    }
}

pub struct HashicorpVault {
    config: HashicorpVaultConfig,
    /// Pre-built base URL (`{address}/v1/`) so per-request URL construction
    /// can't drift from the validated form.
    base_url: String,
    http: Client,
    token_cache: RwLock<Option<CachedToken>>,
    /// Serializes login attempts so concurrent cache-misses don't all hit
    /// the auth backend.
    login_lock: Mutex<()>,
    payload_cache: RwLock<Option<CachedPayload>>,
    /// Serializes data-fetch attempts so concurrent cache-misses for the
    /// same path coalesce into a single request.
    fetch_lock: Mutex<()>,
}

impl HashicorpVault {
    /// Builds a [`HashicorpVault`] store from a validated [`HashicorpVaultConfig`].
    ///
    /// Validates the address, auth params, and TLS configuration but does
    /// **not** touch the network. Use [`HashicorpVault::init`] to verify
    /// reachability before the first user lookup.
    ///
    /// # Errors
    ///
    /// - [`Error::EmptyPath`] / [`Error::MissingAddress`] for blank required fields.
    /// - [`Error::InvalidAddress`] if the address is not a valid http(s) URL with a host.
    /// - [`Error::MissingAuthParams`] if the chosen auth method is missing required params.
    /// - [`Error::UnableToReadCaCert`] / [`Error::InvalidCaCert`] for TLS misconfig.
    /// - [`Error::ClientBuild`] if `reqwest` rejects the client builder.
    pub fn from_config(config: HashicorpVaultConfig) -> Result<Self> {
        let normalized_path = config.path.trim().trim_matches('/').to_string();
        if normalized_path.is_empty() {
            return EmptyPathSnafu.fail();
        }

        if config.address.trim().is_empty() {
            return MissingAddressSnafu.fail();
        }
        let base_url = build_base_url(&config.address)?;
        validate_auth_params(&config)?;

        let http = build_http_client(&config)?;

        // Re-write `path` into the canonical form we validated, so request
        // building never has to re-trim.
        let mut config = config;
        config.path = normalized_path;

        // Seed the token cache with the static token for `auth_method: token`
        // so the first lookup doesn't go through `login_lock`. The token is
        // guaranteed to be present by `validate_auth_params` above; the match
        // pattern keeps the panic-free property explicit instead of relying
        // on `expect`.
        let token_cache = match (config.auth_method, config.token.clone()) {
            (AuthMethod::Token, Some(token)) => RwLock::new(Some(CachedToken {
                value: token,
                expires_at: None,
            })),
            _ => RwLock::new(None),
        };

        Ok(Self {
            config,
            base_url,
            http,
            token_cache,
            login_lock: Mutex::new(()),
            payload_cache: RwLock::new(None),
            fetch_lock: Mutex::new(()),
        })
    }

    /// Returns the configured Vault address (for tests and error messages).
    #[must_use]
    pub fn address(&self) -> &str {
        &self.config.address
    }

    /// Verifies the Vault address is reachable.
    ///
    /// Hits the unauthenticated `sys/health` endpoint with `standbyok=true`
    /// and `sealedcode=200` so we get a 200 from primary, standby, and
    /// sealed Vaults alike — we only care that the address resolves and
    /// answers, not about cluster role. Auth credentials are deliberately
    /// not exercised here: an `init()` that pre-logs-in would burn an
    /// `AppRole` `secret_id` use even when the spicepod loads but never
    /// reads a secret.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Http`] for network failures or [`Error::UnexpectedStatus`]
    /// for non-2xx responses.
    pub async fn init(&self) -> Result<()> {
        let url = format!(
            "{base}sys/health?standbyok=true&sealedcode=200&uninitcode=200",
            base = self.base_url
        );
        // CodeQL [SM03878]: `init()` only hits Vault's unauthenticated
        // `sys/health` endpoint and never sends a token or secret payload.
        // The address is also pre-validated by `build_base_url` to require
        // `https://` for any non-loopback host, so cleartext is bounded to
        // local development against `vault server -dev`.
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|_| HttpSnafu { url: url.clone() })?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return UnexpectedStatusSnafu {
                url,
                status,
                body: truncate(&body, 256),
            }
            .fail();
        }
        Ok(())
    }

    /// Returns a fresh client token, performing a login round-trip if the
    /// cache is empty or expired.
    async fn token(&self) -> Result<SecretString> {
        if let Some(token) = self.try_cached_token().await {
            return Ok(token);
        }

        // Serialize logins; another waiter may have refreshed the cache by
        // the time we acquire the lock.
        let _guard = self.login_lock.lock().await;
        if let Some(token) = self.try_cached_token().await {
            return Ok(token);
        }

        let cached = self.login().await?;
        let value = cached.value.clone();
        let mut guard = self.token_cache.write().await;
        *guard = Some(cached);
        Ok(value)
    }

    async fn try_cached_token(&self) -> Option<SecretString> {
        let guard = self.token_cache.read().await;
        guard
            .as_ref()
            .filter(|t| t.is_fresh())
            .map(|t| t.value.clone())
    }

    /// Performs a login round-trip and returns the resulting cached token.
    async fn login(&self) -> Result<CachedToken> {
        match self.config.auth_method {
            AuthMethod::Token => {
                // `from_config` seeded the cache; reaching `login()` for
                // Token means the cached entry was evicted (e.g. by a
                // forced refresh in tests). Re-seed from config.
                let token = self.config.token.clone().ok_or_else(|| Error::Login {
                    address: self.config.address.clone(),
                    method: "token".to_string(),
                    source: "hashicorp_vault_token is unset".into(),
                })?;
                Ok(CachedToken {
                    value: token,
                    expires_at: None,
                })
            }
            AuthMethod::AppRole => {
                let role_id =
                    self.config
                        .role_id
                        .as_deref()
                        .ok_or_else(|| Error::MissingAuthParams {
                            method: "approle".to_string(),
                            missing: "hashicorp_vault_role_id".to_string(),
                        })?;
                let secret_id =
                    self.config
                        .secret_id
                        .as_ref()
                        .ok_or_else(|| Error::MissingAuthParams {
                            method: "approle".to_string(),
                            missing: "hashicorp_vault_secret_id".to_string(),
                        })?;
                let body = serde_json::json!({
                    "role_id": role_id,
                    "secret_id": secret_id.expose_secret(),
                });
                self.login_post("approle", &body).await
            }
            AuthMethod::Kubernetes => {
                let role = self
                    .config
                    .role
                    .as_deref()
                    .ok_or_else(|| Error::MissingAuthParams {
                        method: "kubernetes".to_string(),
                        missing: "hashicorp_vault_role".to_string(),
                    })?;
                let jwt = self.kubernetes_jwt().await?;
                let body = serde_json::json!({
                    "role": role,
                    "jwt": jwt.expose_secret(),
                });
                self.login_post("kubernetes", &body).await
            }
            AuthMethod::Jwt => {
                let role = self
                    .config
                    .role
                    .as_deref()
                    .ok_or_else(|| Error::MissingAuthParams {
                        method: "jwt".to_string(),
                        missing: "hashicorp_vault_role".to_string(),
                    })?;
                let jwt = self
                    .config
                    .jwt
                    .as_ref()
                    .ok_or_else(|| Error::MissingAuthParams {
                        method: "jwt".to_string(),
                        missing: "hashicorp_vault_jwt".to_string(),
                    })?;
                let body = serde_json::json!({
                    "role": role,
                    "jwt": jwt.expose_secret(),
                });
                self.login_post("jwt", &body).await
            }
        }
    }

    /// Reads the Kubernetes service-account JWT, preferring the explicit
    /// `hashicorp_vault_jwt` param if set so users can override it for testing.
    async fn kubernetes_jwt(&self) -> Result<SecretString> {
        if let Some(jwt) = self.config.jwt.clone() {
            return Ok(jwt);
        }
        let path = self
            .config
            .kubernetes_token_path
            .as_deref()
            .unwrap_or(DEFAULT_K8S_TOKEN_PATH)
            .to_string();
        // The SA JWT is a small (<2 KiB) file on disk; we use
        // `spawn_blocking` rather than `tokio::fs` to avoid pulling the
        // `fs` feature into `runtime-secrets`'s `tokio` dependency.
        let read_path = path.clone();
        let token = tokio::task::spawn_blocking(move || std::fs::read_to_string(&read_path))
            .await
            .map_err(|e| Error::UnableToReadKubernetesToken {
                path: path.clone(),
                source: std::io::Error::other(e.to_string()),
            })?
            .with_context(|_| UnableToReadKubernetesTokenSnafu { path: path.clone() })?;
        Ok(SecretString::from(token.trim().to_string()))
    }

    /// Issues the actual `POST /v1/auth/<mount>/login` and parses the
    /// `auth.client_token` / `auth.lease_duration` from the response.
    async fn login_post(
        &self,
        method: &'static str,
        body: &serde_json::Value,
    ) -> Result<CachedToken> {
        // Tolerate users supplying `auth/<mount>` as the value (matching
        // Vault's UI / CLI path layout) by stripping a leading `auth/`
        // segment before we re-prepend it. Trailing slashes are also
        // forgiving so `kubernetes/`, `auth/k8s-prod/`, and `k8s-prod`
        // all resolve to the same login URL.
        let raw_mount = self.config.auth_mount.as_deref().unwrap_or(method);
        let mount = raw_mount
            .trim_matches('/')
            .strip_prefix("auth/")
            .unwrap_or_else(|| raw_mount.trim_matches('/'));
        let url = format!("{base}auth/{mount}/login", base = self.base_url);

        let mut req = self.http.post(&url).json(body);
        if let Some(ns) = self.config.namespace.as_deref() {
            req = req.header("X-Vault-Namespace", ns);
        }

        let resp = req.send().await.map_err(|e| Error::Login {
            address: self.config.address.clone(),
            method: method.to_string(),
            source: Box::new(e),
        })?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| Error::Login {
            address: self.config.address.clone(),
            method: method.to_string(),
            source: Box::new(e),
        })?;
        if !status.is_success() {
            return Err(Error::Login {
                address: self.config.address.clone(),
                method: method.to_string(),
                source: format!("status {status}: {}", truncate(&text, 256)).into(),
            });
        }
        let parsed: serde_json::Value = serde_json::from_str(&text).map_err(|e| Error::Login {
            address: self.config.address.clone(),
            method: method.to_string(),
            source: Box::new(e),
        })?;
        let auth = parsed.get("auth").ok_or_else(|| Error::LoginMissingToken {
            address: self.config.address.clone(),
        })?;
        let token = auth
            .get("client_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::LoginMissingToken {
                address: self.config.address.clone(),
            })?
            .to_string();
        let lease_duration = auth
            .get("lease_duration")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let expires_at = if lease_duration > 0 {
            Some(Instant::now() + Duration::from_secs(lease_duration))
        } else {
            None
        };
        Ok(CachedToken {
            value: SecretString::from(token),
            expires_at,
        })
    }

    /// Returns the cached payload if still fresh.
    async fn try_cached_payload(&self) -> Option<Arc<HashMap<String, SecretString>>> {
        let guard = self.payload_cache.read().await;
        guard
            .as_ref()
            .filter(|p| p.is_fresh())
            .map(|p| Arc::clone(&p.data))
    }

    /// Coalesces concurrent payload misses behind `fetch_lock`.
    async fn payload(&self) -> crate::AnyErrorResult<Arc<HashMap<String, SecretString>>> {
        if let Some(data) = self.try_cached_payload().await {
            return Ok(data);
        }

        let _guard = self.fetch_lock.lock().await;
        if let Some(data) = self.try_cached_payload().await {
            return Ok(data);
        }

        let (data, ttl) = self.fetch_payload().await?;
        let mut guard = self.payload_cache.write().await;
        *guard = Some(CachedPayload {
            data: Arc::clone(&data),
            fetched_at: Instant::now(),
            ttl,
        });
        Ok(data)
    }

    /// Issues the KV `GET` and parses the response into a key/value map.
    /// Retries once after a forced re-login on 403.
    async fn fetch_payload(
        &self,
    ) -> crate::AnyErrorResult<(Arc<HashMap<String, SecretString>>, Duration)> {
        let url = self.read_url();
        tracing::debug!(url = %url, "Fetching Vault KV payload");

        match self.fetch_payload_once(&url).await {
            Ok(out) => Ok(out),
            Err(err) => {
                if let Some(Error::UnexpectedStatus { status, .. }) = err.downcast_ref::<Error>()
                    && (*status == StatusCode::FORBIDDEN || *status == StatusCode::UNAUTHORIZED)
                {
                    tracing::debug!(
                        url = %url,
                        "Vault returned {status}; forcing token refresh and retrying once"
                    );
                    {
                        let mut guard = self.token_cache.write().await;
                        *guard = None;
                    }
                    return self.fetch_payload_once(&url).await;
                }
                Err(err)
            }
        }
    }

    async fn fetch_payload_once(
        &self,
        url: &str,
    ) -> crate::AnyErrorResult<(Arc<HashMap<String, SecretString>>, Duration)> {
        let token = self.token().await?;
        let mut req = self
            .http
            .get(url)
            .header("X-Vault-Token", token.expose_secret());
        if let Some(ns) = self.config.namespace.as_deref() {
            req = req.header("X-Vault-Namespace", ns);
        }
        let resp = req.send().await.with_context(|_| HttpSnafu {
            url: url.to_string(),
        })?;
        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            tracing::debug!(url = %url, "Vault path not found; caching negative result");
            // Drain the response body so `reqwest` can return the
            // connection to the pool instead of dropping it mid-stream.
            // Vault's 404 body is small (a JSON envelope with `errors: []`)
            // and we don't need its content for the negative-cache path.
            let _ = resp.bytes().await;
            return Ok((Arc::new(HashMap::new()), NEGATIVE_CACHE_TTL));
        }
        let text = resp.text().await.with_context(|_| HttpSnafu {
            url: url.to_string(),
        })?;
        if !status.is_success() {
            return Err(Box::new(Error::UnexpectedStatus {
                url: url.to_string(),
                status,
                body: truncate(&text, 256),
            }));
        }
        let parsed: serde_json::Value = serde_json::from_str(&text).map_err(|source| {
            // The body is intentionally not embedded in the user-facing error
            // because for a 2xx KV response it may contain secret material.
            // Surface it only at debug level for operator-driven diagnosis.
            tracing::debug!(
                url = %url,
                body = %truncate(&text, 256),
                "Vault response was not valid JSON",
            );
            Error::MalformedResponse {
                url: url.to_string(),
                source,
            }
        })?;
        let (data_obj, ttl) = self.extract_data(&parsed, url)?;
        let map = json_object_to_secret_map(data_obj);
        Ok((Arc::new(map), ttl))
    }

    /// Pulls the right `data` slice out of the response, applying the KV
    /// version layout.
    fn extract_data<'a>(
        &self,
        body: &'a serde_json::Value,
        url: &str,
    ) -> Result<(&'a serde_json::Map<String, serde_json::Value>, Duration)> {
        let outer_data = body.get("data").ok_or_else(|| Error::MissingData {
            url: url.to_string(),
            kv_version: self.config.kv_version,
        })?;
        let (inner, lease_duration) = match self.config.kv_version {
            KvVersion::V2 => {
                let inner = outer_data.get("data").ok_or_else(|| Error::MissingData {
                    url: url.to_string(),
                    kv_version: KvVersion::V2,
                })?;
                let inner_map = inner.as_object().ok_or_else(|| Error::MissingData {
                    url: url.to_string(),
                    kv_version: KvVersion::V2,
                })?;
                let lease = body
                    .get("lease_duration")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                (inner_map, lease)
            }
            KvVersion::V1 => {
                let inner_map = outer_data.as_object().ok_or_else(|| Error::MissingData {
                    url: url.to_string(),
                    kv_version: KvVersion::V1,
                })?;
                let lease = body
                    .get("lease_duration")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                (inner_map, lease)
            }
        };
        let ttl = if lease_duration > 0 {
            Duration::from_secs(lease_duration)
        } else {
            DEFAULT_DATA_TTL
        };
        Ok((inner, ttl))
    }

    fn read_url(&self) -> String {
        let path_segment = match self.config.kv_version {
            KvVersion::V2 => format!("{}/data/{}", self.config.mount, self.config.path),
            KvVersion::V1 => format!("{}/{}", self.config.mount, self.config.path),
        };
        format!("{base}{path_segment}", base = self.base_url)
    }

    /// Builds the same login URL `login_post` would POST to. Test-only:
    /// keeps the auth-mount normalization rules (strip leading `auth/`,
    /// trim slashes) in one place so they can be exercised without
    /// standing up a Vault server.
    #[cfg(test)]
    fn login_url(&self, method: &str) -> String {
        let raw_mount = self.config.auth_mount.as_deref().unwrap_or(method);
        let mount = raw_mount
            .trim_matches('/')
            .strip_prefix("auth/")
            .unwrap_or_else(|| raw_mount.trim_matches('/'));
        format!("{base}auth/{mount}/login", base = self.base_url)
    }
}

#[async_trait]
impl SecretStore for HashicorpVault {
    async fn get_secret(&self, key: &str) -> crate::AnyErrorResult<Option<SecretString>> {
        tracing::trace!(
            address = %self.config.address,
            path = %self.config.path,
            key = %key,
            "Resolving secret via Vault"
        );
        let data = self.payload().await?;
        Ok(data.get(key).cloned())
    }
}

/// Coerces a JSON object into a `HashMap<String, SecretString>` of scalar
/// values. Mirrors the AWS Secrets Manager store: strings/numbers/booleans
/// become their string form; null/objects/arrays are skipped (logged at
/// debug) because they cannot be injected as `${ vault:KEY }` substitutions.
fn json_object_to_secret_map(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> HashMap<String, SecretString> {
    let mut out = HashMap::with_capacity(obj.len());
    for (k, v) in obj {
        match v {
            serde_json::Value::String(s) => {
                out.insert(k.clone(), SecretString::from(s.clone()));
            }
            serde_json::Value::Number(n) => {
                out.insert(k.clone(), SecretString::from(n.to_string()));
            }
            serde_json::Value::Bool(b) => {
                out.insert(k.clone(), SecretString::from(b.to_string()));
            }
            _ => {
                tracing::debug!(
                    key = %k,
                    "Skipping non-scalar value in Vault KV payload"
                );
            }
        }
    }
    out
}

fn build_http_client(config: &HashicorpVaultConfig) -> Result<Client> {
    let mut builder = ClientBuilder::new()
        .timeout(config.request_timeout)
        .user_agent(concat!("spice-vault/", env!("CARGO_PKG_VERSION")));
    if config.tls_skip_verify {
        // CodeQL [SM02351]: Opt-in only via the documented `hashicorp_vault_tls_skip_verify`
        // parameter. Intended for local development against `vault server -dev`
        // and self-signed test environments; loud parameter name surfaces the
        // risk to the operator. Production paths use the default (cert
        // validation enabled) plus `hashicorp_vault_ca_cert` for private CAs.
        builder = builder.danger_accept_invalid_certs(true);
    }
    if let Some(ca_path) = config.ca_cert.as_deref() {
        let pem = std::fs::read(ca_path).context(UnableToReadCaCertSnafu {
            path: ca_path.to_string(),
        })?;
        let cert = reqwest::tls::Certificate::from_pem(&pem).context(InvalidCaCertSnafu {
            path: ca_path.to_string(),
        })?;
        builder = builder.add_root_certificate(cert);
    }
    builder.build().context(ClientBuildSnafu)
}

/// Validates and normalizes the address into `https://host[:port]/v1/` form
/// (or `http://localhost:.../v1/` for dev).
fn build_base_url(address: &str) -> Result<String> {
    let raw = address.trim().trim_end_matches('/');
    let parsed = url::Url::parse(raw).map_err(|e| Error::InvalidAddress {
        address: raw.to_string(),
        reason: format!("not a valid URL: {e}"),
    })?;
    let scheme = parsed.scheme();
    if scheme != "https" && scheme != "http" {
        return InvalidAddressSnafu {
            address: raw.to_string(),
            reason: format!("scheme must be http or https, got {scheme}://"),
        }
        .fail();
    }
    let host = parsed.host_str().ok_or_else(|| Error::InvalidAddress {
        address: raw.to_string(),
        reason: "missing host; expected something like `https://vault.example.com:8200`"
            .to_string(),
    })?;
    if scheme == "http" && !is_loopback_host(host) {
        return InvalidAddressSnafu {
            address: raw.to_string(),
            reason: "plaintext http:// is only allowed for localhost / 127.0.0.1; use https \
                     for remote Vault addresses or set `hashicorp_vault_tls_skip_verify: true` \
                     against a loopback proxy"
                .to_string(),
        }
        .fail();
    }
    let path = parsed.path();
    if !path.is_empty() && path != "/" {
        return InvalidAddressSnafu {
            address: raw.to_string(),
            reason: format!(
                "unexpected path `{path}`; the address must point at the Vault root, e.g. \
                 `https://vault.example.com:8200`"
            ),
        }
        .fail();
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return InvalidAddressSnafu {
            address: raw.to_string(),
            reason: "unexpected query or fragment; the address must point at the Vault root"
                .to_string(),
        }
        .fail();
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return InvalidAddressSnafu {
            address: raw.to_string(),
            reason: "unexpected userinfo in URL; credentials must go through `params`".to_string(),
        }
        .fail();
    }
    let authority = &parsed[url::Position::BeforeScheme..url::Position::AfterPort];
    Ok(format!("{authority}/v1/"))
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1")
}

fn validate_auth_params(config: &HashicorpVaultConfig) -> Result<()> {
    let mut missing: Vec<&'static str> = Vec::new();
    match config.auth_method {
        AuthMethod::Token => {
            if config.token.is_none() {
                missing.push("hashicorp_vault_token");
            }
        }
        AuthMethod::AppRole => {
            if config.role_id.is_none() {
                missing.push("hashicorp_vault_role_id");
            }
            if config.secret_id.is_none() {
                missing.push("hashicorp_vault_secret_id");
            }
        }
        AuthMethod::Kubernetes => {
            if config.role.is_none() {
                missing.push("hashicorp_vault_role");
            }
            // The JWT can come from `hashicorp_vault_jwt` OR be read from disk at
            // login time, so it is not required at validation time.
        }
        AuthMethod::Jwt => {
            if config.role.is_none() {
                missing.push("hashicorp_vault_role");
            }
            if config.jwt.is_none() {
                missing.push("hashicorp_vault_jwt");
            }
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        MissingAuthParamsSnafu {
            method: config.auth_method.as_str().to_string(),
            missing: missing.join(", "),
        }
        .fail()
    }
}

/// Truncates `s` to at most `max` chars, appending an ellipsis if truncated.
/// Used to keep error bodies bounded (Vault sometimes returns multi-KB
/// HTML error pages from a fronting LB).
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut out = s.chars().take(max).collect::<String>();
        out.push('…');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_params() -> HashMap<String, String> {
        let mut p = HashMap::new();
        p.insert(
            "hashicorp_vault_address".into(),
            "https://vault.example.com:8200".into(),
        );
        p.insert("hashicorp_vault_token".into(), "root".into());
        p
    }

    #[test]
    fn from_params_normalizes_empty_strings_to_none() {
        let mut p = base_params();
        p.insert("hashicorp_vault_namespace".into(), String::new());
        p.insert("hashicorp_vault_role_id".into(), "  ".into());
        let cfg = HashicorpVaultConfig::from_params("myapp".into(), &p)
            .map_err(|e| e.to_string())
            .expect("from_params");
        assert!(cfg.namespace.is_none());
        assert!(cfg.role_id.is_none());
    }

    #[test]
    fn from_params_defaults_mount_and_kv_version() {
        let cfg = HashicorpVaultConfig::from_params("myapp".into(), &base_params())
            .map_err(|e| e.to_string())
            .expect("from_params");
        assert_eq!(cfg.mount, "secret");
        assert_eq!(cfg.kv_version, KvVersion::V2);
        assert_eq!(cfg.auth_method, AuthMethod::Token);
        assert_eq!(cfg.request_timeout, DEFAULT_REQUEST_TIMEOUT);
    }

    #[test]
    fn from_params_parses_kv_v1_and_approle() {
        let mut p = base_params();
        p.remove("hashicorp_vault_token");
        p.insert("hashicorp_vault_kv_version".into(), "v1".into());
        p.insert("hashicorp_vault_auth_method".into(), "approle".into());
        p.insert("hashicorp_vault_role_id".into(), "rid".into());
        p.insert("hashicorp_vault_secret_id".into(), "sid".into());
        let cfg = HashicorpVaultConfig::from_params("myapp".into(), &p)
            .map_err(|e| e.to_string())
            .expect("from_params");
        assert_eq!(cfg.kv_version, KvVersion::V1);
        assert_eq!(cfg.auth_method, AuthMethod::AppRole);
        assert_eq!(cfg.role_id.as_deref(), Some("rid"));
        assert_eq!(
            cfg.secret_id.as_ref().map(ExposeSecret::expose_secret),
            Some("sid")
        );
    }

    #[test]
    fn auth_method_parse() {
        assert_eq!(AuthMethod::parse("approle"), AuthMethod::AppRole);
        assert_eq!(AuthMethod::parse("kubernetes"), AuthMethod::Kubernetes);
        assert_eq!(AuthMethod::parse("jwt"), AuthMethod::Jwt);
        assert_eq!(AuthMethod::parse("token"), AuthMethod::Token);
        // Unknown defaults to Token (validate_params already rejects unknowns).
        assert_eq!(AuthMethod::parse("garbage"), AuthMethod::Token);
    }

    #[test]
    fn build_base_url_accepts_https() {
        let url = build_base_url("https://vault.example.com:8200").expect("ok");
        assert_eq!(url, "https://vault.example.com:8200/v1/");
    }

    #[test]
    fn build_base_url_strips_trailing_slash() {
        let url = build_base_url("https://vault.example.com:8200/").expect("ok");
        assert_eq!(url, "https://vault.example.com:8200/v1/");
    }

    #[test]
    fn build_base_url_allows_http_only_on_loopback() {
        let url = build_base_url("http://127.0.0.1:8200").expect("loopback ok");
        assert_eq!(url, "http://127.0.0.1:8200/v1/");
        let err = build_base_url("http://vault.example.com:8200").expect_err("remote http denied");
        assert!(matches!(err, Error::InvalidAddress { .. }));
    }

    #[test]
    fn build_base_url_rejects_path_query_userinfo() {
        for bad in [
            "https://vault.example.com:8200/v1",
            "https://vault.example.com:8200/?x=1",
            "https://user:pass@vault.example.com:8200",
        ] {
            let err = build_base_url(bad).expect_err(bad);
            assert!(matches!(err, Error::InvalidAddress { .. }), "{bad}");
        }
    }

    #[test]
    fn build_base_url_rejects_garbage() {
        for bad in ["not a url", "ftp://vault.example.com", "https://"] {
            let err = build_base_url(bad).expect_err(bad);
            assert!(matches!(err, Error::InvalidAddress { .. }), "{bad}");
        }
    }

    #[test]
    fn validate_auth_params_token() {
        let mut cfg = HashicorpVaultConfig::from_params("p".into(), &base_params())
            .map_err(|e| e.to_string())
            .expect("from_params");
        validate_auth_params(&cfg).expect("token ok");
        cfg.token = None;
        let err = validate_auth_params(&cfg).expect_err("missing token");
        let msg = format!("{err}");
        assert!(msg.contains("hashicorp_vault_token"), "{msg}");
    }

    #[test]
    fn validate_auth_params_approle() {
        let mut p = base_params();
        p.remove("hashicorp_vault_token");
        p.insert("hashicorp_vault_auth_method".into(), "approle".into());
        let cfg = HashicorpVaultConfig::from_params("p".into(), &p)
            .map_err(|e| e.to_string())
            .expect("from_params");
        let err = validate_auth_params(&cfg).expect_err("missing both");
        let msg = format!("{err}");
        assert!(
            msg.contains("hashicorp_vault_role_id") && msg.contains("hashicorp_vault_secret_id"),
            "{msg}"
        );
    }

    #[test]
    fn validate_auth_params_kubernetes_only_requires_role() {
        let mut p = base_params();
        p.remove("hashicorp_vault_token");
        p.insert("hashicorp_vault_auth_method".into(), "kubernetes".into());
        p.insert("hashicorp_vault_role".into(), "myrole".into());
        let cfg = HashicorpVaultConfig::from_params("p".into(), &p)
            .map_err(|e| e.to_string())
            .expect("from_params");
        validate_auth_params(&cfg).expect("k8s role ok; jwt resolved at login time");
    }

    #[test]
    fn validate_auth_params_jwt_requires_role_and_jwt() {
        let mut p = base_params();
        p.remove("hashicorp_vault_token");
        p.insert("hashicorp_vault_auth_method".into(), "jwt".into());
        let cfg = HashicorpVaultConfig::from_params("p".into(), &p)
            .map_err(|e| e.to_string())
            .expect("from_params");
        let err = validate_auth_params(&cfg).expect_err("missing");
        let msg = format!("{err}");
        assert!(
            msg.contains("hashicorp_vault_role") && msg.contains("hashicorp_vault_jwt"),
            "{msg}"
        );
    }

    #[test]
    fn json_object_to_secret_map_coerces_scalars_skips_complex() {
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"a":"x","b":42,"c":true,"d":null,"e":[1],"f":{"x":1}}"#)
                .expect("parse");
        let map = json_object_to_secret_map(&obj);
        assert_eq!(map.get("a").map(ExposeSecret::expose_secret), Some("x"));
        assert_eq!(map.get("b").map(ExposeSecret::expose_secret), Some("42"));
        assert_eq!(map.get("c").map(ExposeSecret::expose_secret), Some("true"));
        assert!(!map.contains_key("d"));
        assert!(!map.contains_key("e"));
        assert!(!map.contains_key("f"));
    }

    #[test]
    fn debug_redacts_secrets() {
        let mut p = base_params();
        p.insert("hashicorp_vault_secret_id".into(), "supersecret".into());
        p.insert("hashicorp_vault_jwt".into(), "jwt-value".into());
        let cfg = HashicorpVaultConfig::from_params("myapp".into(), &p)
            .map_err(|e| e.to_string())
            .expect("from_params");
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("root"), "token leaked: {dbg}");
        assert!(!dbg.contains("supersecret"), "secret_id leaked: {dbg}");
        assert!(!dbg.contains("jwt-value"), "jwt leaked: {dbg}");
        assert!(dbg.contains("<redacted>"), "{dbg}");
    }

    #[test]
    fn from_config_empty_path_rejected() {
        let cfg = HashicorpVaultConfig::from_params("   ".into(), &base_params())
            .map_err(|e| e.to_string())
            .expect("from_params");
        match HashicorpVault::from_config(cfg) {
            Err(Error::EmptyPath { .. }) => {}
            Err(e) => panic!("unexpected error: {e}"),
            Ok(_) => panic!("expected EmptyPath error"),
        }
    }

    #[test]
    fn from_config_normalizes_path_trims_slashes() {
        let cfg = HashicorpVaultConfig::from_params("/myapp/cfg/".into(), &base_params())
            .map_err(|e| e.to_string())
            .expect("from_params");
        let v = HashicorpVault::from_config(cfg)
            .map_err(|e| e.to_string())
            .expect("ok");
        assert_eq!(v.config.path, "myapp/cfg");
        assert_eq!(
            v.read_url(),
            "https://vault.example.com:8200/v1/secret/data/myapp/cfg"
        );
    }

    #[test]
    fn read_url_kv_v1() {
        let mut p = base_params();
        p.insert("hashicorp_vault_kv_version".into(), "v1".into());
        p.insert("hashicorp_vault_mount".into(), "kv".into());
        let cfg = HashicorpVaultConfig::from_params("a/b".into(), &p)
            .map_err(|e| e.to_string())
            .expect("from_params");
        let v = HashicorpVault::from_config(cfg)
            .map_err(|e| e.to_string())
            .expect("ok");
        assert_eq!(v.read_url(), "https://vault.example.com:8200/v1/kv/a/b");
    }

    #[test]
    fn from_params_invalid_request_timeout_is_rejected() {
        let mut p = base_params();
        p.insert("hashicorp_vault_request_timeout".into(), "ten".into());
        match HashicorpVaultConfig::from_params("myapp".into(), &p) {
            Err(Error::InvalidNumericParameter {
                parameter, value, ..
            }) => {
                assert_eq!(parameter, "hashicorp_vault_request_timeout");
                assert_eq!(value, "ten");
            }
            Err(e) => panic!("unexpected error: {e}"),
            Ok(_) => panic!("expected InvalidNumericParameter"),
        }
    }

    #[test]
    fn login_url_strips_leading_auth_prefix_in_mount() {
        // Both `k8s-prod` and `auth/k8s-prod` must produce the same
        // `/v1/auth/k8s-prod/login` URL so users following the Vault
        // CLI/UI path layout don't get a double-prefixed URL.
        let mk = |mount: &str| {
            let mut p = base_params();
            p.insert("hashicorp_vault_auth_method".into(), "approle".into());
            p.insert("hashicorp_vault_role_id".into(), "r".into());
            p.insert("hashicorp_vault_secret_id".into(), "s".into());
            p.insert("hashicorp_vault_auth_mount".into(), mount.into());
            let cfg = HashicorpVaultConfig::from_params("app".into(), &p)
                .map_err(|e| e.to_string())
                .expect("from_params");
            HashicorpVault::from_config(cfg)
                .map_err(|e| e.to_string())
                .expect("from_config")
                .login_url("approle")
        };
        let plain = mk("k8s-prod");
        let prefixed = mk("auth/k8s-prod");
        let trailing = mk("k8s-prod/");
        assert_eq!(
            plain,
            "https://vault.example.com:8200/v1/auth/k8s-prod/login"
        );
        assert_eq!(plain, prefixed);
        assert_eq!(plain, trailing);
    }
}
