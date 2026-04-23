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

    #[snafu(display("Unable to parse secret value"))]
    UnableToParseSecretValue,

    #[snafu(display("Unknown secret store: {store}"))]
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

pub const SECRETS: &str = "secrets";

#[async_trait]
pub trait SecretStore: Send + Sync {
    /// `get_secret` will load a secret from the secret store with the given key.
    async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>>;
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
    /// If no secret stores are provided, the default secret store is set to `env`.
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

        for secret in secrets {
            let store_type = spicepod_secret_store_type(secret, bootstrap_env.as_ref()).await?;

            let secret_store = match load_secret_store(store_type).await {
                Ok(secret_store) => secret_store,
                Err(e) => {
                    tracing::error!("Error loading secret store {}: {e}", secret.name);
                    continue;
                }
            };

            self.stores.insert(secret.name.clone(), secret_store);
        }

        if self.stores.is_empty() {
            let default_store = load_default_store();
            self.stores.insert("env".to_string(), default_store);
        }

        // Reverse the order of the secret stores to maintain the expected precedence order.
        self.stores.reverse();

        Ok(())
    }

    pub async fn inject_secrets(&self, key: &str, param_str: ParamStr<'_>) -> SecretString {
        tracing::trace!("Injecting secrets for: {}", key);
        let mut result = String::new();
        let mut last_end = 0;
        for secret_replacement in SecretReplacementMatcher::new(param_str.0) {
            tracing::debug!(
                "Found secret replacement: Store name: {}, Key: {}, Span: {:?}",
                secret_replacement.store_name,
                secret_replacement.key,
                secret_replacement.span,
            );

            // Append text from last match to the start of the current match
            result.push_str(&param_str.0[last_end..secret_replacement.span.start]);

            // Get the secret value from the store
            let secret = self
                .get_store_secret(
                    &param_str,
                    &secret_replacement.store_name,
                    &secret_replacement.key,
                )
                .await
                .unwrap_or_default();

            // Replace the token with the desired string
            result.push_str(&secret);

            // Update the last end to the end of the current match
            last_end = secret_replacement.span.end;
        }

        // Append the remaining text after the last match
        result.push_str(&param_str.0[last_end..]);

        SecretString::from(result)
    }

    /// Gets a secret key from the connected secret stores in precedence order.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by an underlying secret store implementation.
    pub async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>> {
        for store in self.stores.values() {
            match store.get_secret(key).await {
                Ok(Some(secret)) => return Ok(Some(secret)),
                Ok(None) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(None)
    }

    async fn get_store_secret(
        &self,
        param_str: &ParamStr<'_>,
        store_name: &str,
        key: &str,
    ) -> Option<String> {
        // This is a special case for loading secrets across stores in precedence order
        if store_name == SECRETS {
            match self.get_secret(key).await {
                Ok(Some(secret)) => return Some(secret.expose_secret().to_string()),
                Ok(None) => {
                    tracing::error!("Key '{key}' not found in any connected secrets.");
                    return None;
                }
                Err(e) => {
                    tracing::error!("Error getting secret: {}", e);
                    return None;
                }
            }
        }

        let secret = if let Some(store) = self.stores.get(store_name) {
            match store.get_secret(key).await {
                Ok(Some(secret)) => secret.expose_secret().to_string(),
                Ok(None) => {
                    tracing::error!("Key {key} not found in secret store: {store_name}");
                    return None;
                }
                Err(e) => {
                    tracing::error!("Error getting secret: {}", e);
                    return None;
                }
            }
        } else {
            tracing::error!(
                "Secret '{store_name}' referenced in {} not found.",
                param_str.0
            );
            return None;
        };

        Some(secret)
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

    struct MockClusterSecretExpander;

    #[async_trait]
    impl super::ClusterSecretExpander for MockClusterSecretExpander {
        async fn expand_secret(&self, executor_id: &str, key: &str) -> Result<String, String> {
            Ok(format!("{executor_id}:{key}:expanded"))
        }
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
                assert_eq!(cfg.secret_access_key.as_deref(), Some("shh"));
                assert_eq!(cfg.session_token.as_deref(), Some("tok"));
            }
            _ => panic!("expected AwsSecretsManager variant"),
        }
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
