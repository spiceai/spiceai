/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

pub use crate::stores::scheduler_rpc::ClusterSecretExpander;
use crate::stores::scheduler_rpc::SchedulerRPCSecretStore;
use async_trait::async_trait;
use indexmap::IndexMap;
use lexer::SecretReplacementMatcher;
pub use secrecy::ExposeSecret;
use secrecy::SecretString;
use snafu::prelude::*;
use spicepod::component::secret::Secret as SpicepodSecret;
use std::{collections::HashMap, sync::Arc};
use stores::env::EnvSecretStoreBuilder;
use tokio::sync::RwLock;

mod lexer;
mod params;
pub mod stores;

pub use params::{ParamError as SecretStoreParamError, expand_bootstrap_refs, validate_params};
pub use runtime_parameter_spec::{ParameterSpec, ParameterType};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load secrets: {source}"))]
    UnableToLoadSecrets { source: Box<dyn std::error::Error> },

    #[cfg(feature = "aws-secrets-manager")]
    #[snafu(display("Unable to initialize AWS Secrets Manager: {source}"))]
    UnableToInitializeAwsSecretsManager {
        source: Box<stores::aws_secrets_manager::Error>,
    },

    #[cfg(feature = "azure-keyvault")]
    #[snafu(display("Unable to initialize Azure Key Vault: {source}"))]
    UnableToInitializeAzureKeyVault {
        source: Box<stores::azure_keyvault::Error>,
    },

    #[cfg(feature = "hashicorp_vault")]
    #[snafu(display("Unable to initialize HashiCorp Vault: {source}"))]
    UnableToInitializeHashicorpVault {
        source: Box<stores::hashicorp_vault::Error>,
    },

    #[snafu(display("Unable to parse secret value"))]
    UnableToParseSecretValue,

    #[snafu(display(
        "Unknown secret store '{store}'. Available stores: {}. Docs: https://spiceai.org/docs/components/secret-stores",
        known_secret_stores().join(", ")
    ))]
    UnknownSecretStore { store: String },

    #[snafu(display(
        "The secret store {store} requires a secret selector. i.e. `from: {store}:my_secret_name`"
    ))]
    SecretStoreRequiresSecretSelector { store: String },

    #[snafu(display(
        "The secret store {store} should not specify a secret selector. i.e. `from: {store}`"
    ))]
    SecretStoreInvalidSecretSelector { store: String },

    #[snafu(display("Invalid secret store params: {source}"))]
    InvalidSecretStoreParams { source: Box<params::ParamError> },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type AnyErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Secret store types this binary was compiled with. Must match the cases in
/// `spicepod_secret_store_type` so unknown-store errors don't lie about what's
/// available in the current build (e.g. `keyring` is only present with the
/// `keyring-secret-store` feature).
#[must_use]
pub fn known_secret_stores() -> Vec<&'static str> {
    let mut stores = vec!["env"];
    #[cfg(feature = "keyring-secret-store")]
    stores.push("keyring");
    stores.push("kubernetes");
    #[cfg(feature = "aws-secrets-manager")]
    stores.push("aws_secrets_manager");
    #[cfg(feature = "azure-keyvault")]
    stores.push("azure_keyvault");
    #[cfg(feature = "hashicorp_vault")]
    stores.push("hashicorp_vault");
    stores.push("scheduler_rpc");
    stores
}

pub const SECRETS: &str = "secrets";

#[async_trait]
pub trait SecretStore: Send + Sync {
    /// `get_secret` will load a secret from the secret store with the given key.
    async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>>;
}

/// Stands in for a configured secret store that failed to initialize.
///
/// Keeping the name registered (instead of dropping the store) means a later
/// `${ <name>:KEY }` lookup surfaces the original initialization error at the
/// point of use, rather than a misleading "undefined store" error. In the
/// `${ secrets:KEY }` precedence walk the placeholder behaves like any other
/// erroring store: it is skipped with a warning and only surfaces when no
/// healthy store can answer.
struct FailedSecretStore {
    store_name: String,
    /// Display-formatted initialization error, captured at load time.
    init_error: String,
}

#[async_trait]
impl SecretStore for FailedSecretStore {
    async fn get_secret(&self, _key: &str) -> AnyErrorResult<Option<SecretString>> {
        Err(format!(
            "Secret store `{}` failed to initialize: {}",
            self.store_name, self.init_error
        )
        .into())
    }
}

pub struct Secrets {
    // Use an IndexMap to maintain the order of the secret stores.
    // This order is the reverse of the order in which the secret stores are defined in the SpicePod.
    // This maintains the precedence order we want, since we will search through the secret stores in their order here.
    stores: IndexMap<String, Arc<dyn SecretStore>>,
}

pub struct ParamStr<'a>(pub &'a str);

impl Secrets {
    #[must_use]
    pub fn new() -> Self {
        Self {
            stores: IndexMap::new(),
        }
    }

    #[must_use]
    pub fn new_for_cluster_executor(
        expander: Box<dyn crate::stores::scheduler_rpc::ClusterSecretExpander>,
        executor_id: String,
    ) -> Self {
        let expander: Arc<dyn crate::stores::scheduler_rpc::ClusterSecretExpander> =
            Arc::from(expander);
        let mut stores = IndexMap::new();
        stores.insert(
            "env".to_string(),
            Arc::new(SchedulerRPCSecretStore::new(
                Arc::clone(&expander),
                executor_id.clone(),
            )) as Arc<dyn SecretStore>,
        );
        stores.insert(
            "scheduler_rpc".to_string(),
            Arc::new(SchedulerRPCSecretStore::new(expander, executor_id)) as Arc<dyn SecretStore>,
        );

        Self { stores }
    }

    /// Initializes the runtime secrets based on the provided secret store configuration.
    ///
    /// If no secret stores are provided — or none of the configured stores
    /// initializes successfully — the default `env` store is (also) loaded, so
    /// `${ env:KEY }` and `${ secrets:KEY }` keep resolving from the
    /// environment.
    ///
    /// A configured store that fails to initialize stays registered as a
    /// placeholder (`FailedSecretStore`): lookups against it return the
    /// original initialization error instead of reporting the store as
    /// undefined.
    ///
    /// # Errors
    ///
    /// Returns an error when the `from` field references an unknown store or when a store
    /// requires (or disallows) a selector and the config is invalid.
    pub async fn load_from(&mut self, secrets: &[SpicepodSecret]) -> Result<()> {
        self.stores.clear();

        // Bootstrap env store used only to resolve `${ env:KEY }` /
        // `${ secrets:KEY }` references inside other stores' `params:`
        // blocks. Constructed once so `.env` files are loaded a single time
        // for the whole secrets section.
        let bootstrap_env: Arc<dyn SecretStore> = load_default_store();

        let mut any_healthy_store_loaded = false;
        for secret in secrets {
            let store_type = spicepod_secret_store_type(secret, bootstrap_env.as_ref()).await?;

            let secret_store = match load_secret_store(store_type).await {
                Ok(secret_store) => {
                    any_healthy_store_loaded = true;
                    secret_store
                }
                Err(e) => {
                    // Log a big actionable error and keep going so other stores
                    // can still be loaded. The store stays registered as a
                    // placeholder so a later `${ <name>:KEY }` lookup reports
                    // this initialization error (the root cause) instead of a
                    // misleading "undefined store".
                    tracing::error!(
                        "Failed to initialize secret store `{name}`: {e}. Secret references `${{ {name}:KEY }}` in spicepod.yaml will fail to resolve. Check the store's `params:` block (e.g. region/credentials for aws_secrets_manager) and retry. Docs: https://spiceai.org/docs/components/secret-stores",
                        name = secret.name
                    );
                    Arc::new(FailedSecretStore {
                        store_name: secret.name.clone(),
                        init_error: e.to_string(),
                    }) as Arc<dyn SecretStore>
                }
            };

            self.stores.insert(secret.name.clone(), secret_store);
        }

        if !any_healthy_store_loaded {
            // Long-standing fallback, now keyed on *healthy* stores rather
            // than registry emptiness (placeholders occupy the registry but
            // can't serve lookups): with no secrets section, or with every
            // configured store failing to initialize, `env` remains available
            // so `${ env:KEY }` / `${ secrets:KEY }` resolve as before.
            // `or_insert_with` keeps a user-configured store named `env` (or
            // its placeholder) authoritative over the implicit default.
            self.stores
                .entry("env".to_string())
                .or_insert_with(load_default_store);
        }

        // Reverse the order of the secret stores to maintain the expected precedence order.
        self.stores.reverse();

        Ok(())
    }

    pub async fn inject_secrets(&self, key: &str, param_str: ParamStr<'_>) -> SecretString {
        tracing::trace!("Injecting secrets for: {}", key);
        let input = param_str.0;
        // Preallocate so the builder rarely re-allocates. Any reallocation
        // would free the old buffer (containing partial secret bytes) without
        // zeroizing it — defense in depth, though the final wrap below does
        // guarantee the last live buffer is scrubbed when the `SecretString`
        // drops.
        let mut result = String::with_capacity(input.len().saturating_add(256));
        let mut last_end = 0;
        for secret_replacement in SecretReplacementMatcher::new(input) {
            // Log only the store name + key, never the resolved value.
            tracing::debug!(
                "Found secret replacement: Store name: {}, Key: {}, Span: {:?}",
                secret_replacement.store_name,
                secret_replacement.key,
                secret_replacement.span,
            );

            result.push_str(&input[last_end..secret_replacement.span.start]);

            // Keep the value inside `SecretString` until we splice its bytes
            // into `result`. Previously this path went through
            // `expose_secret().to_string()` which allocates a plain String
            // that is dropped without zeroizing — those bytes would linger
            // in freed heap slots.
            if let Some(secret) = self
                .lookup_for_injection(
                    input,
                    &secret_replacement.store_name,
                    &secret_replacement.key,
                )
                .await
            {
                result.push_str(secret.expose_secret());
            }

            last_end = secret_replacement.span.end;
        }

        result.push_str(&input[last_end..]);

        SecretString::from(result)
    }

    /// Gets a secret key from the connected secret stores in precedence order.
    ///
    /// A store that errors is logged and skipped so one unhealthy store (an
    /// expired Vault token, a network blip) cannot mask a key that a
    /// lower-precedence store can resolve:
    ///
    /// - A store returns the value → `Ok(Some(value))`, as before.
    /// - No value, but at least one store answered healthily (`Ok(None)`) →
    ///   `Ok(None)`; the key was not found in any healthy store. An erroring
    ///   store might still hold it — each skipped failure is logged so that
    ///   outcome stays diagnosable.
    /// - Every consulted store errored → the last error, so a total outage is
    ///   reported as an error rather than silently as "not found".
    ///
    /// # Errors
    ///
    /// Returns the last store error when every consulted store failed and none
    /// returned a healthy "not found".
    pub async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>> {
        let mut last_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;
        let mut any_healthy = false;
        for (store_name, store) in &self.stores {
            match store.get_secret(key).await {
                Ok(Some(secret)) => return Ok(Some(secret)),
                Ok(None) => any_healthy = true,
                Err(e) => {
                    // Include the failure cause: when a later store resolves
                    // the key, this warning is the only signal that a store is
                    // unhealthy (the returned-error path below never fires).
                    // Matches the `{e}` logging in `lookup_for_injection`'s
                    // error branches. Secret values are never logged.
                    tracing::warn!(
                        "Secret store `{store_name}` failed while looking up secret `{key}`: {e}. Trying the next store in precedence order."
                    );
                    last_err = Some(e);
                }
            }
        }

        match last_err {
            Some(e) if !any_healthy => Err(e),
            _ => Ok(None),
        }
    }

    /// Internal helper for [`Self::inject_secrets`]. Returns the value
    /// wrapped in a [`SecretString`] (never a plain `String`) so the caller
    /// can splice the bytes without an intermediate non-zeroizing allocation.
    async fn lookup_for_injection(
        &self,
        param_str: &str,
        store_name: &str,
        key: &str,
    ) -> Option<SecretString> {
        // Substitution failures leave the parameter as the empty string, which the
        // component layer then reports as "missing required parameter" without any
        // back-reference to the failed secret lookup. Include enough context in the
        // error branches (store, key, source of reference) that users can tie cause
        // to effect. The store-list is allocated lazily — `inject_secrets` calls this
        // in a tight loop for every `${...}` substitution, so we don't pay for it on
        // the hot (success) path.
        let configured_stores = || self.stores.keys().cloned().collect::<Vec<_>>().join(", ");

        // Special case: the `secrets:` sentinel means "walk the registry in
        // precedence order", which matches `get_secret`.
        if store_name == SECRETS {
            return match self.get_secret(key).await {
                Ok(Some(secret)) => Some(secret),
                Ok(None) => {
                    tracing::error!(
                        "Secret `${{ secrets:{key} }}` (referenced by `{}`) not found in any configured secret store (searched: [{}]). The parameter will be empty, which typically surfaces later as a missing/invalid parameter. Docs: https://spiceai.org/docs/components/secret-stores",
                        param_str,
                        configured_stores()
                    );
                    None
                }
                Err(e) => {
                    tracing::error!(
                        "Error looking up secret `${{ secrets:{key} }}` (referenced by `{}`): {e}",
                        param_str
                    );
                    None
                }
            };
        }

        let Some(store) = self.stores.get(store_name) else {
            tracing::error!(
                "Secret reference `${{ {store_name}:{key} }}` in `{}` uses an undefined store `{store_name}`. Configured stores: [{}]. Add a `secrets:` entry in spicepod.yaml with `from: {store_name}`, or use one of the configured stores. Docs: https://spiceai.org/docs/components/secret-stores",
                param_str,
                configured_stores()
            );
            return None;
        };

        match store.get_secret(key).await {
            Ok(Some(secret)) => Some(secret),
            Ok(None) => {
                tracing::error!(
                    "Secret `${{ {store_name}:{key} }}` (referenced by `{}`) not found in secret store `{store_name}`. The parameter will be empty. Docs: https://spiceai.org/docs/components/secret-stores",
                    param_str
                );
                None
            }
            Err(e) => {
                tracing::error!(
                    "Error looking up secret `${{ {store_name}:{key} }}` (referenced by `{}`): {e}",
                    param_str
                );
                None
            }
        }
    }
}

impl Default for Secrets {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract all secret references from a string (e.g., spicepod YAML content).
///
/// Returns a map where keys are secret keys and values are the store names they reference.
/// For example, `${ env:MY_VAR }` returns `("MY_VAR", "env")` and
/// `${ secrets:API_KEY }` returns `("API_KEY", "secrets")`.
///
/// # Example
/// ```
/// use runtime_secrets::extract_secret_references;
///
/// let yaml = r#"
/// params:
///   api_key: ${ secrets:OPENAI_KEY }
///   user: ${ env:DB_USER }
/// "#;
///
/// let refs = extract_secret_references(yaml);
/// assert_eq!(refs.get("OPENAI_KEY"), Some(&"secrets".to_string()));
/// assert_eq!(refs.get("DB_USER"), Some(&"env".to_string()));
/// ```
#[must_use]
pub fn extract_secret_references(content: &str) -> std::collections::HashMap<String, String> {
    let mut references = std::collections::HashMap::new();

    for secret_replacement in SecretReplacementMatcher::new(content) {
        references.insert(
            secret_replacement.key.clone(),
            secret_replacement.store_name.clone(),
        );
    }

    references
}

pub enum SecretStoreType {
    Env(stores::env::EnvConfig),
    #[cfg(feature = "keyring-secret-store")]
    Keyring,
    Kubernetes(stores::kubernetes::KubernetesConfig),
    #[cfg(feature = "aws-secrets-manager")]
    AwsSecretsManager(stores::aws_secrets_manager::AwsSecretsManagerConfig),
    #[cfg(feature = "azure-keyvault")]
    AzureKeyVault(stores::azure_keyvault::AzureKeyVaultConfig),
    #[cfg(feature = "hashicorp_vault")]
    HashicorpVault(stores::hashicorp_vault::HashicorpVaultConfig),
    SchedulerRPC,
}

#[expect(clippy::implicit_hasher)]
pub async fn get_params_with_secrets(
    secrets: Arc<RwLock<Secrets>>,
    params: &HashMap<String, String>,
) -> HashMap<String, SecretString> {
    let secrets = secrets.read().await;

    let mut params_with_secrets: HashMap<String, SecretString> = HashMap::new();

    // Inject secrets from the user-supplied params.
    // This will replace any instances of `${ store:key }` with the actual secret value.
    for (k, v) in params {
        let secret = secrets.inject_secrets(k, ParamStr(v)).await;
        params_with_secrets.insert(k.clone(), secret);
    }

    params_with_secrets
}

async fn spicepod_secret_store_type(
    store: &SpicepodSecret,
    bootstrap_env: &dyn SecretStore,
) -> Result<SecretStoreType> {
    let provider = secret_store_provider(&store.from);
    let selector = secret_selector(&store.from);
    let mut user_params = store
        .params
        .as_ref()
        .map(spicepod::param::Params::as_string_map)
        .unwrap_or_default();

    // Resolve `${ env:KEY }` / `${ secrets:KEY }` references in the
    // user-supplied params using the bootstrap env store *before* validating
    // against the spec. This lets users keep secrets like AWS regions /
    // endpoints out of the spicepod, while still failing fast on typos
    // (`regoin`, missing env vars, references to other stores) instead of
    // silently dropping them.
    expand_bootstrap_refs(provider, &mut user_params, bootstrap_env)
        .await
        .map_err(|source| Error::InvalidSecretStoreParams {
            source: Box::new(source),
        })?;

    // Validates user-provided params against the store's static
    // `ParameterSpec` list. Unknown params return an error rather than being
    // silently dropped.
    let validate = |spec: &'static [ParameterSpec]| {
        validate_params(provider, user_params.clone(), spec).map_err(|source| {
            Error::InvalidSecretStoreParams {
                source: Box::new(source),
            }
        })
    };

    match provider {
        "env" => {
            require_no_selector(provider, selector)?;
            let params = validate(stores::env::PARAMETERS)?;
            Ok(SecretStoreType::Env(stores::env::EnvConfig::from_params(
                &params,
            )))
        }
        #[cfg(feature = "keyring-secret-store")]
        "keyring" => {
            require_no_selector(provider, selector)?;
            let _ = validate(stores::keyring::PARAMETERS)?;
            Ok(SecretStoreType::Keyring)
        }
        "kubernetes" => {
            let secret_name = require_selector(provider, selector)?;
            let params = validate(stores::kubernetes::PARAMETERS)?;
            Ok(SecretStoreType::Kubernetes(
                stores::kubernetes::KubernetesConfig::from_params(secret_name, &params),
            ))
        }
        #[cfg(feature = "aws-secrets-manager")]
        "aws_secrets_manager" => {
            let secret_name = require_selector(provider, selector)?;
            let params = validate(stores::aws_secrets_manager::PARAMETERS)?;
            Ok(SecretStoreType::AwsSecretsManager(
                stores::aws_secrets_manager::AwsSecretsManagerConfig::from_params(
                    secret_name,
                    &params,
                ),
            ))
        }
        #[cfg(feature = "azure-keyvault")]
        "azure_keyvault" => {
            let vault = require_selector(provider, selector)?;
            let params = validate(stores::azure_keyvault::PARAMETERS)?;
            Ok(SecretStoreType::AzureKeyVault(
                stores::azure_keyvault::AzureKeyVaultConfig::from_params(vault, &params),
            ))
        }
        #[cfg(feature = "hashicorp_vault")]
        "hashicorp_vault" => {
            let path = require_selector(provider, selector)?;
            let params = validate(stores::hashicorp_vault::PARAMETERS)?;
            let cfg = stores::hashicorp_vault::HashicorpVaultConfig::from_params(path, &params)
                .map_err(|e| Error::UnableToInitializeHashicorpVault {
                    source: Box::new(e),
                })?;
            Ok(SecretStoreType::HashicorpVault(cfg))
        }
        "scheduler_rpc" => {
            require_no_selector(provider, selector)?;
            Ok(SecretStoreType::SchedulerRPC)
        }
        other => UnknownSecretStoreSnafu {
            store: other.to_string(),
        }
        .fail(),
    }
}

fn require_selector(provider: &str, selector: Option<&str>) -> Result<String> {
    let Some(selector) = selector else {
        return SecretStoreRequiresSecretSelectorSnafu {
            store: provider.to_string(),
        }
        .fail()?;
    };

    Ok(selector.to_string())
}

fn require_no_selector(provider: &str, selector: Option<&str>) -> Result<()> {
    if selector.is_some() {
        SecretStoreInvalidSecretSelectorSnafu {
            store: provider.to_string(),
        }
        .fail()?;
    }

    Ok(())
}

/// Returns the secret store provider - the first part of the `from` field before the first `:`.
#[must_use]
fn secret_store_provider(from: &str) -> &str {
    from.split(':').next().unwrap_or(from)
}

/// Returns the secret selector - the second part of the `from` field after the first `:`.
/// This is optional.
#[must_use]
fn secret_selector(from: &str) -> Option<&str> {
    match from.find(':') {
        Some(index) => Some(&from[index + 1..]),
        None => None,
    }
}

fn load_default_store() -> Arc<dyn SecretStore> {
    Arc::new(EnvSecretStoreBuilder::new().build())
}

/// Loads the secret store from the provided secret store type.
///
/// # Errors
///
/// Returns an error if the secrets cannot be loaded.
async fn load_secret_store(store_type: SecretStoreType) -> Result<Arc<dyn SecretStore>> {
    match store_type {
        SecretStoreType::Env(config) => {
            let mut builder = EnvSecretStoreBuilder::new();
            if let Some(path) = config.file_path {
                builder = builder.with_path(path.into());
            }
            Ok(Arc::new(builder.build()) as Arc<dyn SecretStore>)
        }
        #[cfg(feature = "keyring-secret-store")]
        SecretStoreType::Keyring => {
            Ok(Arc::new(stores::keyring::KeyringSecretStore::new()) as Arc<dyn SecretStore>)
        }
        SecretStoreType::Kubernetes(config) => {
            let mut kubernetes_secret_store = stores::kubernetes::KubernetesSecretStore::new(
                config.secret_name,
                config.namespace,
            );

            kubernetes_secret_store
                .init()
                .await
                .context(UnableToLoadSecretsSnafu)?;

            Ok(Arc::new(kubernetes_secret_store) as Arc<dyn SecretStore>)
        }
        #[cfg(feature = "aws-secrets-manager")]
        SecretStoreType::AwsSecretsManager(config) => {
            let secret_store = stores::aws_secrets_manager::AwsSecretsManager::from_config(config)
                .map_err(|e| Error::UnableToInitializeAwsSecretsManager {
                    source: Box::new(e),
                })?;

            secret_store
                .init()
                .await
                .map_err(|e| Error::UnableToInitializeAwsSecretsManager {
                    source: Box::new(e),
                })?;

            Ok(Arc::new(secret_store) as Arc<dyn SecretStore>)
        },
        #[cfg(feature = "azure-keyvault")]
        SecretStoreType::AzureKeyVault(config) => {
            let secret_store = stores::azure_keyvault::AzureKeyVault::from_config(config)
                .map_err(|e| Error::UnableToInitializeAzureKeyVault {
                    source: Box::new(e),
                })?;

            secret_store
                .init()
                .await
                .map_err(|e| Error::UnableToInitializeAzureKeyVault {
                    source: Box::new(e),
                })?;

            Ok(Arc::new(secret_store) as Arc<dyn SecretStore>)
        },
        #[cfg(feature = "hashicorp_vault")]
        SecretStoreType::HashicorpVault(config) => {
            let secret_store = stores::hashicorp_vault::HashicorpVault::from_config(config)
                .map_err(|e| Error::UnableToInitializeHashicorpVault {
                    source: Box::new(e),
                })?;

            secret_store
                .init()
                .await
                .map_err(|e| Error::UnableToInitializeHashicorpVault {
                    source: Box::new(e),
                })?;

            Ok(Arc::new(secret_store) as Arc<dyn SecretStore>)
        },
        SecretStoreType::SchedulerRPC => {
            Err(Error::UnableToLoadSecrets {
                source: "The `scheduler_rpc` is automatically configured for cluster mode, and should not be specified in the Spicepod.".into()
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use secrecy::ExposeSecret;
    use tokio::sync::{Mutex, MutexGuard};

    /// Global lock serializing any test that mutates process environment
    /// variables. Required because `std::env::set_var`/`remove_var` are
    /// `unsafe` on Rust 2024 — they are not sound to call concurrently
    /// with any other thread reading or writing env vars. `cargo test`
    /// runs tests in parallel by default, so without this lock two env
    /// tests could step on each other and trip the safety contract.
    ///
    /// `tokio::sync::Mutex` (rather than `std::sync::Mutex`) because the
    /// guard is held across `.await`s in the async test bodies — holding
    /// a sync mutex across await would block other tasks on the same
    /// runtime thread and clippy flags it as `await_holding_lock`.
    static ENV_LOCK: Mutex<()> = Mutex::const_new(());

    pub(super) async fn lock_env() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().await
    }

    struct MockClusterSecretExpander;

    #[async_trait]
    impl super::ClusterSecretExpander for MockClusterSecretExpander {
        async fn expand_secret(
            &self,
            executor_id: &str,
            key: &str,
        ) -> Result<secrecy::SecretString, String> {
            Ok(secrecy::SecretString::from(format!(
                "{executor_id}:{key}:expanded"
            )))
        }
    }

    /// A store that fails every lookup with the given message — simulates an
    /// unhealthy backend (expired token, network failure).
    struct ErroringStore(&'static str);

    #[async_trait]
    impl super::SecretStore for ErroringStore {
        async fn get_secret(
            &self,
            _key: &str,
        ) -> super::AnyErrorResult<Option<secrecy::SecretString>> {
            Err(self.0.into())
        }
    }

    /// A healthy in-memory store backed by a fixed key/value map.
    struct MapStore(std::collections::HashMap<String, String>);

    impl MapStore {
        fn new(entries: &[(&str, &str)]) -> Self {
            Self(
                entries
                    .iter()
                    .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                    .collect(),
            )
        }
    }

    #[async_trait]
    impl super::SecretStore for MapStore {
        async fn get_secret(
            &self,
            key: &str,
        ) -> super::AnyErrorResult<Option<secrecy::SecretString>> {
            Ok(self
                .0
                .get(key)
                .map(|v| secrecy::SecretString::from(v.clone())))
        }
    }

    /// Builds a `Secrets` registry directly from named stores, already in
    /// precedence order (first entry is consulted first).
    fn secrets_with_stores(
        stores: Vec<(&str, std::sync::Arc<dyn super::SecretStore>)>,
    ) -> super::Secrets {
        super::Secrets {
            stores: stores
                .into_iter()
                .map(|(name, store)| (name.to_string(), store))
                .collect(),
        }
    }

    #[tokio::test]
    async fn test_get_secret_skips_erroring_store_when_later_store_has_value() {
        let secrets = secrets_with_stores(vec![
            ("vault", std::sync::Arc::new(ErroringStore("vault is down"))),
            (
                "env",
                std::sync::Arc::new(MapStore::new(&[("MY_KEY", "value")])),
            ),
        ]);

        let secret = secrets
            .get_secret("MY_KEY")
            .await
            .expect("lookup should succeed via the healthy store")
            .expect("secret should be found");
        assert_eq!("value", secret.expose_secret());
    }

    #[tokio::test]
    async fn test_get_secret_returns_none_when_error_then_healthy_not_found() {
        let secrets = secrets_with_stores(vec![
            ("vault", std::sync::Arc::new(ErroringStore("vault is down"))),
            ("env", std::sync::Arc::new(MapStore::new(&[]))),
        ]);

        let result = secrets
            .get_secret("MISSING_KEY")
            .await
            .expect("a healthy 'not found' should not surface the store error");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_secret_returns_last_error_when_all_stores_error() {
        let secrets = secrets_with_stores(vec![
            ("vault", std::sync::Arc::new(ErroringStore("first error"))),
            ("aws", std::sync::Arc::new(ErroringStore("second error"))),
        ]);

        let Err(err) = secrets.get_secret("MY_KEY").await else {
            panic!("a total outage must surface an error, not 'not found'");
        };
        assert_eq!("second error", err.to_string());
    }

    #[tokio::test]
    async fn test_get_secret_precedence_order_preserved() {
        let secrets = secrets_with_stores(vec![
            (
                "high",
                std::sync::Arc::new(MapStore::new(&[("MY_KEY", "high_value")])),
            ),
            (
                "low",
                std::sync::Arc::new(MapStore::new(&[("MY_KEY", "low_value")])),
            ),
        ]);

        let secret = secrets
            .get_secret("MY_KEY")
            .await
            .expect("lookup should succeed")
            .expect("secret should be found");
        assert_eq!("high_value", secret.expose_secret());
    }

    #[tokio::test]
    async fn test_get_secret_empty_registry_returns_none() {
        let secrets = secrets_with_stores(vec![]);

        let result = secrets
            .get_secret("MY_KEY")
            .await
            .expect("empty registry should not error");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_inject_secrets_sentinel_resolves_through_erroring_store() {
        let secrets = secrets_with_stores(vec![
            ("vault", std::sync::Arc::new(ErroringStore("vault is down"))),
            (
                "env",
                std::sync::Arc::new(MapStore::new(&[("API_KEY", "s3cret")])),
            ),
        ]);

        let result = secrets
            .inject_secrets("api_key", super::ParamStr("key=${ secrets:API_KEY }"))
            .await;
        assert_eq!("key=s3cret", result.expose_secret());
    }

    /// A spicepod store entry that passes config validation but
    /// deterministically fails `load_secret_store` (the `scheduler_rpc` store
    /// is cluster-internal and rejects spicepod configuration) — a no-network
    /// stand-in for any store whose `init()` fails.
    fn failing_spicepod_secret(name: &str) -> spicepod::component::secret::Secret {
        spicepod::component::secret::Secret {
            from: "scheduler_rpc".to_string(),
            name: name.to_string(),
            description: None,
            params: None,
        }
    }

    fn env_spicepod_secret(name: &str) -> spicepod::component::secret::Secret {
        spicepod::component::secret::Secret {
            from: "env".to_string(),
            name: name.to_string(),
            description: None,
            params: None,
        }
    }

    #[tokio::test]
    async fn test_failed_store_registers_placeholder_with_init_error() {
        let mut secrets = super::Secrets::new();
        secrets
            .load_from(&[failing_spicepod_secret("rpc")])
            .await
            .expect("load_from should not abort on a store init failure");

        let store = secrets
            .stores
            .get("rpc")
            .expect("a failed store should stay registered as a placeholder");
        let Err(err) = store.get_secret("ANY_KEY").await else {
            panic!("placeholder lookups must return the initialization error");
        };
        let msg = err.to_string();
        assert!(msg.contains("`rpc` failed to initialize"), "got {msg}");
        assert!(
            msg.contains("scheduler_rpc"),
            "placeholder error must carry the init root cause; got {msg}"
        );
    }

    #[tokio::test]
    async fn test_all_stores_failed_keeps_env_fallback() {
        let _env_guard = lock_env().await;
        let var = format!("SPICE_TEST_FALLBACK_{}", rand::random::<u64>());
        unsafe { std::env::set_var(&var, "from_env") };

        let mut secrets = super::Secrets::new();
        secrets
            .load_from(&[failing_spicepod_secret("rpc")])
            .await
            .expect("load_from should not abort on a store init failure");

        // With zero healthy stores the implicit `env` fallback still loads,
        // preserving the long-standing `${ secrets:KEY }` behavior...
        let injected = secrets
            .inject_secrets(&var, super::ParamStr(&format!("v=${{ secrets:{var} }}")))
            .await;
        assert_eq!("v=from_env", injected.expose_secret());

        // ...while `${ rpc:KEY }` substitutes empty (the init root cause is
        // logged by the lookup error branch).
        let injected = secrets
            .inject_secrets(&var, super::ParamStr(&format!("v=${{ rpc:{var} }}")))
            .await;
        assert_eq!("v=", injected.expose_secret());

        unsafe { std::env::remove_var(&var) };
    }

    #[tokio::test]
    async fn test_failed_store_does_not_break_healthy_stores_or_force_fallback() {
        let _env_guard = lock_env().await;
        let var = format!("SPICE_TEST_HEALTHY_{}", rand::random::<u64>());
        unsafe { std::env::set_var(&var, "healthy_value") };

        let mut secrets = super::Secrets::new();
        secrets
            .load_from(&[env_spicepod_secret("myenv"), failing_spicepod_secret("rpc")])
            .await
            .expect("load_from should not abort on a store init failure");

        // A healthy store loaded, so no implicit `env` fallback is added.
        assert!(secrets.stores.get("env").is_none());

        // The placeholder sits earlier in the precedence walk (later spicepod
        // entries take precedence); its error is skipped and the healthy
        // store answers.
        let secret = secrets
            .get_secret(&var)
            .await
            .expect("the healthy store should answer despite the failed store")
            .expect("secret should be found");
        assert_eq!("healthy_value", secret.expose_secret());

        unsafe { std::env::remove_var(&var) };
    }

    #[test]
    fn test_secret_store_provider() {
        assert_eq!("foo", super::secret_store_provider("foo:bar"));
        assert_eq!("foo", super::secret_store_provider("foo"));
    }

    #[test]
    fn test_secret_selector() {
        assert_eq!(Some("bar"), super::secret_selector("foo:bar"));
        assert_eq!(None, super::secret_selector("foo"));
    }

    fn bootstrap_env() -> std::sync::Arc<dyn super::SecretStore> {
        super::load_default_store()
    }

    #[cfg(feature = "aws-secrets-manager")]
    #[tokio::test]
    async fn test_aws_secrets_manager_params_threaded_through() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        let mut p = HashMap::new();
        p.insert("region".to_string(), "eu-west-2".to_string());
        p.insert(
            "endpoint_url".to_string(),
            "https://localhost:4566".to_string(),
        );

        let store = SpicepodSecret {
            from: "aws_secrets_manager:my-secret".to_string(),
            name: "aws".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let resolved = super::spicepod_secret_store_type(&store, env.as_ref())
            .await
            .map_err(|e| e.to_string())
            .expect("validates");
        match resolved {
            super::SecretStoreType::AwsSecretsManager(cfg) => {
                assert_eq!(cfg.secret_name, "my-secret");
                assert_eq!(cfg.region.as_deref(), Some("eu-west-2"));
                assert_eq!(cfg.endpoint_url.as_deref(), Some("https://localhost:4566"));
            }
            _ => panic!("expected AwsSecretsManager variant"),
        }
    }

    #[cfg(feature = "aws-secrets-manager")]
    #[tokio::test]
    async fn test_aws_secrets_manager_unknown_param_is_rejected() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        // The classic typo path: a misspelled `regoin` parameter must be
        // rejected at load time rather than silently dropped (which is the
        // failure mode this whole feature exists to prevent).
        let mut p = HashMap::new();
        p.insert("regoin".to_string(), "us-east-1".to_string());

        let store = SpicepodSecret {
            from: "aws_secrets_manager:my-secret".to_string(),
            name: "aws".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let Err(err) = super::spicepod_secret_store_type(&store, env.as_ref()).await else {
            panic!("unknown param should have been rejected");
        };
        let msg = err.to_string();
        assert!(msg.contains("regoin"), "got {msg}");
        assert!(
            msg.contains("region"),
            "error must list supported params; got {msg}"
        );
    }

    #[tokio::test]
    async fn test_env_file_path_param_routed_through_validation() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        let mut p = HashMap::new();
        p.insert("file_path".to_string(), "/tmp/spice.env".to_string());

        let store = SpicepodSecret {
            from: "env".to_string(),
            name: "env".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let resolved = super::spicepod_secret_store_type(&store, env.as_ref())
            .await
            .map_err(|e| e.to_string())
            .expect("validates");
        match resolved {
            super::SecretStoreType::Env(cfg) => {
                assert_eq!(cfg.file_path.as_deref(), Some("/tmp/spice.env"));
            }
            _ => panic!("expected Env variant"),
        }
    }

    #[cfg(feature = "aws-secrets-manager")]
    #[tokio::test]
    async fn test_aws_secrets_manager_env_bootstrap_resolves_region() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        let _env_guard = lock_env().await;
        // Unique env-var name keeps tests isolated when run in parallel.
        let var = format!("SPICE_TEST_BOOTSTRAP_REGION_{}", rand::random::<u64>());
        unsafe { std::env::set_var(&var, "ap-south-1") };

        let mut p = HashMap::new();
        p.insert("region".to_string(), format!("${{ env:{var} }}"));
        // Also exercise `secrets:` syntax — at bootstrap it must resolve
        // against env (the only loaded store).
        let var2 = format!("SPICE_TEST_BOOTSTRAP_ENDPOINT_{}", rand::random::<u64>());
        unsafe { std::env::set_var(&var2, "https://localhost:4566") };
        p.insert("endpoint_url".to_string(), format!("${{ secrets:{var2} }}"));

        let store = SpicepodSecret {
            from: "aws_secrets_manager:my-secret".to_string(),
            name: "aws".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let resolved = super::spicepod_secret_store_type(&store, env.as_ref())
            .await
            .map_err(|e| e.to_string())
            .expect("validates");

        unsafe {
            std::env::remove_var(&var);
            std::env::remove_var(&var2);
        }

        match resolved {
            super::SecretStoreType::AwsSecretsManager(cfg) => {
                assert_eq!(cfg.region.as_deref(), Some("ap-south-1"));
                assert_eq!(cfg.endpoint_url.as_deref(), Some("https://localhost:4566"));
            }
            _ => panic!("expected AwsSecretsManager variant"),
        }
    }

    #[cfg(feature = "aws-secrets-manager")]
    #[tokio::test]
    async fn test_aws_secrets_manager_missing_env_var_fails_fast() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        let _env_guard = lock_env().await;
        let var = format!("SPICE_TEST_DEFINITELY_UNSET_{}", rand::random::<u64>());
        // Defensive: ensure it's not set in case of a prior leak.
        unsafe { std::env::remove_var(&var) };

        let mut p = HashMap::new();
        p.insert("region".to_string(), format!("${{ env:{var} }}"));

        let store = SpicepodSecret {
            from: "aws_secrets_manager:my-secret".to_string(),
            name: "aws".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let Err(err) = super::spicepod_secret_store_type(&store, env.as_ref()).await else {
            panic!("missing env var should have failed fast");
        };
        let msg = err.to_string();
        assert!(
            msg.contains(&var),
            "error must name the missing var; got {msg}"
        );
    }

    #[cfg(feature = "aws-secrets-manager")]
    #[cfg(feature = "aws-secrets-manager")]
    #[tokio::test]
    async fn test_aws_secrets_manager_static_credentials_threaded_through() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        // `key` / `secret` / `session_token` should land on the resolved
        // config so the store can hand them to the AWS SDK as static
        // credentials, instead of being silently dropped.
        let mut p = HashMap::new();
        p.insert("region".to_string(), "us-east-1".to_string());
        p.insert("key".to_string(), "AKIA_TEST".to_string());
        p.insert("secret".to_string(), "shh".to_string());
        p.insert("session_token".to_string(), "tok".to_string());

        let store = SpicepodSecret {
            from: "aws_secrets_manager:my-secret".to_string(),
            name: "aws".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let resolved = super::spicepod_secret_store_type(&store, env.as_ref())
            .await
            .map_err(|e| e.to_string())
            .expect("validates");
        match resolved {
            super::SecretStoreType::AwsSecretsManager(cfg) => {
                assert_eq!(cfg.access_key_id.as_deref(), Some("AKIA_TEST"));
                assert_eq!(
                    cfg.secret_access_key
                        .as_ref()
                        .map(|s| s.expose_secret().to_string()),
                    Some("shh".to_string())
                );
                assert_eq!(
                    cfg.session_token
                        .as_ref()
                        .map(|s| s.expose_secret().to_string()),
                    Some("tok".to_string())
                );
            }
            _ => panic!("expected AwsSecretsManager variant"),
        }
    }

    #[cfg(feature = "azure-keyvault")]
    #[tokio::test]
    async fn test_azure_keyvault_params_threaded_through() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        let mut p = HashMap::new();
        p.insert("auth_method".to_string(), "service_principal".to_string());
        p.insert(
            "tenant_id".to_string(),
            "00000000-0000-0000-0000-000000000001".to_string(),
        );
        p.insert(
            "client_id".to_string(),
            "00000000-0000-0000-0000-000000000002".to_string(),
        );
        p.insert("client_secret".to_string(), "shh".to_string());
        p.insert(
            "endpoint".to_string(),
            "vault.usgovcloudapi.net".to_string(),
        );

        let store = SpicepodSecret {
            from: "azure_keyvault:my-vault".to_string(),
            name: "azure".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let resolved = super::spicepod_secret_store_type(&store, env.as_ref())
            .await
            .map_err(|e| e.to_string())
            .expect("validates");
        match resolved {
            super::SecretStoreType::AzureKeyVault(cfg) => {
                assert_eq!(cfg.vault, "my-vault");
                assert_eq!(
                    cfg.auth_method,
                    super::stores::azure_keyvault::AuthMethod::ServicePrincipal
                );
                assert_eq!(
                    cfg.tenant_id.as_deref(),
                    Some("00000000-0000-0000-0000-000000000001")
                );
                assert_eq!(
                    cfg.client_id.as_deref(),
                    Some("00000000-0000-0000-0000-000000000002")
                );
                assert_eq!(
                    cfg.client_secret
                        .as_ref()
                        .map(|s| s.expose_secret().to_string()),
                    Some("shh".to_string())
                );
                assert_eq!(cfg.endpoint.as_deref(), Some("vault.usgovcloudapi.net"));
            }
            _ => panic!("expected AzureKeyVault variant"),
        }
    }

    #[cfg(feature = "azure-keyvault")]
    #[tokio::test]
    async fn test_azure_keyvault_unknown_param_is_rejected() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        // A misspelled `tennant_id` must be rejected at load time rather
        // than silently dropped — this is exactly the failure mode the
        // ParameterSpec validation is meant to prevent.
        let mut p = HashMap::new();
        p.insert(
            "tennant_id".to_string(),
            "00000000-0000-0000-0000-000000000000".to_string(),
        );

        let store = SpicepodSecret {
            from: "azure_keyvault:my-vault".to_string(),
            name: "azure".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let Err(err) = super::spicepod_secret_store_type(&store, env.as_ref()).await else {
            panic!("unknown param should have been rejected");
        };
        let msg = err.to_string();
        assert!(msg.contains("tennant_id"), "got {msg}");
        assert!(
            msg.contains("tenant_id"),
            "error must list supported params; got {msg}"
        );
    }

    #[cfg(feature = "azure-keyvault")]
    #[tokio::test]
    async fn test_azure_keyvault_env_bootstrap_resolves_client_secret() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        let _env_guard = lock_env().await;
        // The canonical path: client_secret (and tenant/client ids) are
        // sourced from environment via `${ env:... }` references so they
        // are never committed to the spicepod.
        let tenant_var = format!("SPICE_TEST_AZURE_TENANT_{}", rand::random::<u64>());
        let client_var = format!("SPICE_TEST_AZURE_CLIENT_{}", rand::random::<u64>());
        let secret_var = format!("SPICE_TEST_AZURE_SECRET_{}", rand::random::<u64>());
        unsafe {
            std::env::set_var(&tenant_var, "tenant-xyz");
            std::env::set_var(&client_var, "client-xyz");
            std::env::set_var(&secret_var, "s3cret");
        }

        let mut p = HashMap::new();
        p.insert("auth_method".to_string(), "service_principal".to_string());
        p.insert("tenant_id".to_string(), format!("${{ env:{tenant_var} }}"));
        p.insert("client_id".to_string(), format!("${{ env:{client_var} }}"));
        p.insert(
            "client_secret".to_string(),
            format!("${{ secrets:{secret_var} }}"),
        );

        let store = SpicepodSecret {
            from: "azure_keyvault:my-vault".to_string(),
            name: "azure".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let resolved = super::spicepod_secret_store_type(&store, env.as_ref())
            .await
            .map_err(|e| e.to_string())
            .expect("validates");

        unsafe {
            std::env::remove_var(&tenant_var);
            std::env::remove_var(&client_var);
            std::env::remove_var(&secret_var);
        }

        match resolved {
            super::SecretStoreType::AzureKeyVault(cfg) => {
                assert_eq!(cfg.tenant_id.as_deref(), Some("tenant-xyz"));
                assert_eq!(cfg.client_id.as_deref(), Some("client-xyz"));
                assert_eq!(
                    cfg.client_secret
                        .as_ref()
                        .map(|s| s.expose_secret().to_string()),
                    Some("s3cret".to_string())
                );
            }
            _ => panic!("expected AzureKeyVault variant"),
        }
    }

    #[cfg(feature = "azure-keyvault")]
    #[tokio::test]
    async fn test_azure_keyvault_rejects_invalid_auth_method() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        // `one_of` enforcement: only the five documented values are allowed.
        let mut p = HashMap::new();
        p.insert("auth_method".to_string(), "oauth".to_string());

        let store = SpicepodSecret {
            from: "azure_keyvault:my-vault".to_string(),
            name: "azure".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let Err(err) = super::spicepod_secret_store_type(&store, env.as_ref()).await else {
            panic!("invalid auth_method should have been rejected");
        };
        let msg = err.to_string();
        assert!(msg.contains("oauth"), "got {msg}");
        assert!(
            msg.contains("service_principal") && msg.contains("managed_identity"),
            "error must list the allowed values; got {msg}"
        );
    }

    #[cfg(feature = "azure-keyvault")]
    #[tokio::test]
    async fn test_azure_keyvault_selector_is_required() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        // `from: azure_keyvault` with no selector must fail — there is no
        // sensible default vault.
        let store = SpicepodSecret {
            from: "azure_keyvault".to_string(),
            name: "azure".to_string(),
            description: None,
            params: Some(Params::from_string_map(HashMap::new())),
        };

        let env = bootstrap_env();
        let Err(err) = super::spicepod_secret_store_type(&store, env.as_ref()).await else {
            panic!("missing selector should have been rejected");
        };
        let msg = err.to_string();
        assert!(msg.contains("azure_keyvault"), "got {msg}");
        assert!(
            msg.contains("secret selector"),
            "error should explain the selector requirement; got {msg}"
        );
    }

    #[cfg(feature = "hashicorp_vault")]
    #[tokio::test]
    async fn test_hashicorp_vault_params_threaded_through() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        let mut p = HashMap::new();
        p.insert(
            "hashicorp_vault_address".to_string(),
            "https://vault.example.com:8200".to_string(),
        );
        p.insert(
            "hashicorp_vault_namespace".to_string(),
            "admin/team-a".to_string(),
        );
        p.insert("hashicorp_vault_mount".to_string(), "kv".to_string());
        p.insert("hashicorp_vault_kv_version".to_string(), "v2".to_string());
        p.insert(
            "hashicorp_vault_auth_method".to_string(),
            "approle".to_string(),
        );
        p.insert("hashicorp_vault_role_id".to_string(), "rid".to_string());
        p.insert("hashicorp_vault_secret_id".to_string(), "sid".to_string());

        let store = SpicepodSecret {
            from: "hashicorp_vault:myapp/config".to_string(),
            name: "hashicorp_vault".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let resolved = super::spicepod_secret_store_type(&store, env.as_ref())
            .await
            .map_err(|e| e.to_string())
            .expect("validates");
        match resolved {
            super::SecretStoreType::HashicorpVault(cfg) => {
                assert_eq!(cfg.path, "myapp/config");
                assert_eq!(cfg.address, "https://vault.example.com:8200");
                assert_eq!(cfg.namespace.as_deref(), Some("admin/team-a"));
                assert_eq!(cfg.mount, "kv");
                assert_eq!(
                    cfg.kv_version,
                    super::stores::hashicorp_vault::KvVersion::V2
                );
                assert_eq!(
                    cfg.auth_method,
                    super::stores::hashicorp_vault::AuthMethod::AppRole
                );
                assert_eq!(cfg.role_id.as_deref(), Some("rid"));
                assert_eq!(
                    cfg.secret_id
                        .as_ref()
                        .map(|s| s.expose_secret().to_string()),
                    Some("sid".to_string())
                );
            }
            _ => panic!("expected Vault variant"),
        }
    }

    #[cfg(feature = "hashicorp_vault")]
    #[tokio::test]
    async fn test_hashicorp_vault_unknown_param_is_rejected() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        let mut p = HashMap::new();
        p.insert(
            "hashicorp_vault_addres".to_string(),
            "https://vault.example.com:8200".to_string(),
        );

        let store = SpicepodSecret {
            from: "hashicorp_vault:myapp".to_string(),
            name: "hashicorp_vault".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let Err(err) = super::spicepod_secret_store_type(&store, env.as_ref()).await else {
            panic!("unknown param should have been rejected");
        };
        let msg = err.to_string();
        assert!(msg.contains("hashicorp_vault_addres"), "got {msg}");
        assert!(
            msg.contains("hashicorp_vault_address"),
            "error must list supported params; got {msg}"
        );
    }

    #[cfg(feature = "hashicorp_vault")]
    #[tokio::test]
    async fn test_hashicorp_vault_env_bootstrap_resolves_token() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        let _env_guard = lock_env().await;
        let token_var = format!("SPICE_TEST_VAULT_TOKEN_{}", rand::random::<u64>());
        unsafe {
            std::env::set_var(&token_var, "hvs.dev-root");
        }

        let mut p = HashMap::new();
        p.insert(
            "hashicorp_vault_address".to_string(),
            "https://vault.example.com:8200".to_string(),
        );
        p.insert(
            "hashicorp_vault_token".to_string(),
            format!("${{ env:{token_var} }}"),
        );

        let store = SpicepodSecret {
            from: "hashicorp_vault:myapp".to_string(),
            name: "hashicorp_vault".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let resolved = super::spicepod_secret_store_type(&store, env.as_ref())
            .await
            .map_err(|e| e.to_string())
            .expect("validates");

        unsafe {
            std::env::remove_var(&token_var);
        }

        match resolved {
            super::SecretStoreType::HashicorpVault(cfg) => {
                assert_eq!(
                    cfg.token.as_ref().map(|s| s.expose_secret().to_string()),
                    Some("hvs.dev-root".to_string())
                );
            }
            _ => panic!("expected Vault variant"),
        }
    }

    #[cfg(feature = "hashicorp_vault")]
    #[tokio::test]
    async fn test_hashicorp_vault_rejects_invalid_auth_method() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        let mut p = HashMap::new();
        p.insert(
            "hashicorp_vault_address".to_string(),
            "https://vault.example.com:8200".to_string(),
        );
        p.insert(
            "hashicorp_vault_auth_method".to_string(),
            "oauth".to_string(),
        );

        let store = SpicepodSecret {
            from: "hashicorp_vault:myapp".to_string(),
            name: "hashicorp_vault".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let Err(err) = super::spicepod_secret_store_type(&store, env.as_ref()).await else {
            panic!("invalid hashicorp_vault_auth_method should have been rejected");
        };
        let msg = err.to_string();
        assert!(msg.contains("oauth"), "got {msg}");
        assert!(
            msg.contains("approle") && msg.contains("kubernetes"),
            "error must list the allowed values; got {msg}"
        );
    }

    #[cfg(feature = "hashicorp_vault")]
    #[tokio::test]
    async fn test_hashicorp_vault_selector_is_required() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        let store = SpicepodSecret {
            from: "hashicorp_vault".to_string(),
            name: "hashicorp_vault".to_string(),
            description: None,
            params: Some(Params::from_string_map(HashMap::new())),
        };

        let env = bootstrap_env();
        let Err(err) = super::spicepod_secret_store_type(&store, env.as_ref()).await else {
            panic!("missing selector should have been rejected");
        };
        let msg = err.to_string();
        assert!(msg.contains("hashicorp_vault"), "got {msg}");
        assert!(
            msg.contains("secret selector"),
            "error should explain the selector requirement; got {msg}"
        );
    }

    #[cfg(feature = "aws-secrets-manager")]
    #[tokio::test]
    async fn test_aws_secrets_manager_rejects_non_env_store_ref() {
        use spicepod::component::secret::Secret as SpicepodSecret;
        use spicepod::param::Params;
        use std::collections::HashMap;

        // References to other stores are a bootstrap cycle and must be
        // rejected with a clear error.
        let mut p = HashMap::new();
        p.insert(
            "region".to_string(),
            "${ kubernetes:my-region }".to_string(),
        );

        let store = SpicepodSecret {
            from: "aws_secrets_manager:my-secret".to_string(),
            name: "aws".to_string(),
            description: None,
            params: Some(Params::from_string_map(p)),
        };

        let env = bootstrap_env();
        let Err(err) = super::spicepod_secret_store_type(&store, env.as_ref()).await else {
            panic!("non-env store ref should have been rejected");
        };
        let msg = err.to_string();
        assert!(msg.contains("kubernetes"), "got {msg}");
    }

    #[tokio::test]
    async fn test_inject_secrets_env() {
        let _env_guard = lock_env().await;
        let mut secrets = super::Secrets::new();
        secrets.load_from(&[]).await.expect("to load successfully"); // Will automatically load `env` as the default

        let key = &format!("MY_SECRET_KEY_{}", rand::random::<u64>());
        unsafe { std::env::set_var(key, "super_secret") };

        let result = secrets
            .inject_secrets(
                key,
                super::ParamStr(&format!("This is a secret: ${{ env:{key} }}! 🫡")),
            )
            .await;
        assert_eq!("This is a secret: super_secret! 🫡", result.expose_secret());
    }

    #[tokio::test]
    async fn test_inject_secrets_case_sensitive() {
        let _env_guard = lock_env().await;
        let mut secrets = super::Secrets::new();
        secrets.load_from(&[]).await.expect("to load successfully"); // Will automatically load `env` as the default

        let upper_key = &format!("MY_UPPERCASE_SECRET_KEY_{}", rand::random::<u64>());
        let lower_key = &format!("MY_LOWERCASE_SECRET_KEY_{}", rand::random::<u64>());

        unsafe {
            std::env::set_var(upper_key, "UPPER_SECRET");
            std::env::set_var(lower_key, "lower_secret");
        }

        let result_upper = secrets
            .inject_secrets(
                upper_key,
                super::ParamStr(&format!("Upper: ${{ env:{upper_key} }}")),
            )
            .await;
        assert_eq!("Upper: UPPER_SECRET", result_upper.expose_secret());

        let result_lower = secrets
            .inject_secrets(
                lower_key,
                super::ParamStr(&format!("Lower: ${{ env:{lower_key} }}")),
            )
            .await;
        assert_eq!("Lower: lower_secret", result_lower.expose_secret());

        unsafe {
            std::env::remove_var(upper_key);
            std::env::remove_var(lower_key);
        }
    }

    #[tokio::test]
    async fn test_inject_secrets_original_key_takes_precedence() {
        let _env_guard = lock_env().await;
        let mut secrets = super::Secrets::new();
        secrets.load_from(&[]).await.expect("to load successfully"); // Will automatically load `env` as the default

        let lower_key = &format!("my_secret_key_{}", rand::random::<u64>());
        let upper_key = lower_key.to_uppercase();

        unsafe {
            std::env::set_var(&upper_key, "UPPER_SECRET");
            std::env::set_var(lower_key, "original_secret");
        }

        let result_upper = secrets
            .inject_secrets(
                &upper_key,
                super::ParamStr(&format!("Upper: ${{ env:{upper_key} }}")),
            )
            .await;
        assert_eq!("Upper: UPPER_SECRET", result_upper.expose_secret());

        let result_lower = secrets
            .inject_secrets(
                lower_key,
                super::ParamStr(&format!("Lower: ${{ env:{lower_key} }}")),
            )
            .await;
        assert_eq!("Lower: original_secret", result_lower.expose_secret());

        unsafe {
            std::env::remove_var(upper_key);
            std::env::remove_var(lower_key);
        }
    }

    #[tokio::test]
    async fn test_inject_secrets_no_env() {
        let _env_guard = lock_env().await;
        let mut secrets = super::Secrets::new();
        secrets.load_from(&[]).await.expect("to load successfully"); // Will automatically load `env` as the default

        let key = &format!("MY_SECRET_KEY_{}", rand::random::<u64>());

        // Ensure `MY_SECRET_KEY` is not set from other tests.
        unsafe { std::env::remove_var(key) };

        let result = secrets
            .inject_secrets(
                key,
                super::ParamStr(&format!("This is a secret: ${{ env:{key} }}! 🫡")),
            )
            .await;
        assert_eq!("This is a secret: ! 🫡", result.expose_secret());
    }

    #[tokio::test]
    async fn test_cluster_executor_env_references_expand_via_scheduler_rpc() {
        let secrets = super::Secrets::new_for_cluster_executor(
            Box::new(MockClusterSecretExpander),
            "executor-1".to_string(),
        );

        let result = secrets
            .inject_secrets(
                "aws_access_key_id",
                super::ParamStr("key=${ env:AWS_ACCESS_KEY_ID }"),
            )
            .await;

        assert_eq!(
            "key=executor-1:AWS_ACCESS_KEY_ID:expanded",
            result.expose_secret()
        );
    }

    #[test]
    fn test_extract_secret_references() {
        let yaml = r"
version: v1
kind: Spicepod
name: test

models:
  - from: openai:gpt-4o-mini
    name: openai-gpt
    params:
      openai_api_key: ${ secrets:SPICE_OPENAI_API_KEY }

datasets:
  - from: file:///path/to/data.jsonl
    name: qs
    params:
      schema_source_path: ${ env:QS_SCHEMA_PATH }
      pg_user: ${env:PG_USER}
      api_key: ${ secrets:ANOTHER_SECRET }
";

        let refs = super::extract_secret_references(yaml);
        assert_eq!(refs.len(), 4);
        assert_eq!(
            refs.get("SPICE_OPENAI_API_KEY"),
            Some(&"secrets".to_string())
        );
        assert_eq!(refs.get("QS_SCHEMA_PATH"), Some(&"env".to_string()));
        assert_eq!(refs.get("PG_USER"), Some(&"env".to_string()));
        assert_eq!(refs.get("ANOTHER_SECRET"), Some(&"secrets".to_string()));
    }

    #[test]
    fn test_extract_secret_references_empty() {
        let yaml = r"
version: v1
kind: Spicepod
name: test
";

        let refs = super::extract_secret_references(yaml);
        assert_eq!(refs.len(), 0);
    }

    #[test]
    fn test_extract_secret_references_duplicates() {
        let yaml = r"
param1: ${ env:MY_VAR }
param2: ${ env:MY_VAR }
param3: ${ secrets:MY_VAR }
";

        let refs = super::extract_secret_references(yaml);
        // MY_VAR appears with different stores, but since we use a HashMap keyed by secret key,
        // only the last occurrence is kept
        assert_eq!(refs.len(), 1);
        assert_eq!(refs.get("MY_VAR"), Some(&"secrets".to_string()));
    }
}
