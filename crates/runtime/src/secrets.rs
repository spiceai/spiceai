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

use async_trait::async_trait;
use indexmap::IndexMap;
use lexer::SecretReplacementMatcher;
pub use secrecy::ExposeSecret;
use secrecy::SecretString;
use snafu::prelude::*;
use spicepod::component::secret::Secret as SpicepodSecret;
use std::sync::Arc;
use stores::env::EnvSecretStoreBuilder;

mod lexer;
pub mod stores;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load secrets: {source}"))]
    UnableToLoadSecrets { source: Box<dyn std::error::Error> },

    #[snafu(display("Unable to initialize AWS Secrets Manager: {source}"))]
    UnableToInitializeAwsSecretsManager {
        source: stores::aws_secrets_manager::Error,
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

    /// Initializes the runtime secrets based on the provided secret store configuration.
    ///
    /// If no secret stores are provided, the default secret store is set to `env`.
    pub async fn load_from(&mut self, secrets: &[SpicepodSecret]) -> Result<()> {
        self.stores.clear();

        for secret in secrets {
            let store_type = spicepod_secret_store_type(secret)?;

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

        SecretString::new(result)
    }

    /// Gets a secret key from the connected secret stores in precedence order.
    pub async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>> {
        for store in self.stores.values() {
            match store.get_secret(key).await {
                Ok(Some(secret)) => return Ok(Some(secret)),
                Ok(None) => continue,
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

pub enum SecretStoreType {
    Env,
    EnvCustomPath(String),
    #[cfg(feature = "keyring-secret-store")]
    Keyring,
    Kubernetes(String),
    #[cfg(feature = "aws-secrets-manager")]
    AwsSecretsManager(String),
}

fn spicepod_secret_store_type(store: &SpicepodSecret) -> Result<SecretStoreType> {
    let provider = secret_store_provider(&store.from);
    let selector = secret_selector(&store.from);
    match provider {
        "env" => {
            require_no_selector(provider, selector)?;
            if let Some(params) = store.params.as_ref() {
                let params = params.as_string_map();
                if let Some(path) = params.get("file_path") {
                    return Ok(SecretStoreType::EnvCustomPath(path.to_string()));
                }
            }
            Ok(SecretStoreType::Env)
        }
        #[cfg(feature = "keyring-secret-store")]
        "keyring" => {
            require_no_selector(provider, selector)?;
            Ok(SecretStoreType::Keyring)
        }
        "kubernetes" => Ok(SecretStoreType::Kubernetes(require_selector(
            provider, selector,
        )?)),
        #[cfg(feature = "aws-secrets-manager")]
        "aws_secrets_manager" => Ok(SecretStoreType::AwsSecretsManager(require_selector(
            provider, selector,
        )?)),
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
        SecretStoreType::Env => {
            let env_secret_store = EnvSecretStoreBuilder::new().build();

            Ok(Arc::new(env_secret_store) as Arc<dyn SecretStore>)
        }
        SecretStoreType::EnvCustomPath(path) => {
            let env_secret_store = EnvSecretStoreBuilder::new().with_path(path.into()).build();

            Ok(Arc::new(env_secret_store) as Arc<dyn SecretStore>)
        }
        #[cfg(feature = "keyring-secret-store")]
        SecretStoreType::Keyring => {
            Ok(Arc::new(stores::keyring::KeyringSecretStore::new()) as Arc<dyn SecretStore>)
        }
        SecretStoreType::Kubernetes(secret_name) => {
            let mut kubernetes_secret_store =
                stores::kubernetes::KubernetesSecretStore::new(secret_name.clone());

            kubernetes_secret_store
                .init()
                .context(UnableToLoadSecretsSnafu)?;

            Ok(Arc::new(kubernetes_secret_store) as Arc<dyn SecretStore>)
        }
        #[cfg(feature = "aws-secrets-manager")]
        SecretStoreType::AwsSecretsManager(secret_name) => {
            let secret_store =
                stores::aws_secrets_manager::AwsSecretsManager::new(secret_name.clone());

            secret_store
                .init()
                .await
                .context(UnableToInitializeAwsSecretsManagerSnafu)?;

            Ok(Arc::new(secret_store) as Arc<dyn SecretStore>)
        }
    }
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

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

    #[tokio::test]
    async fn test_inject_secrets_env() {
        let mut secrets = super::Secrets::new();
        secrets.load_from(&[]).await.expect("to load successfully"); // Will automatically load `env` as the default

        let key = &format!("MY_SECRET_KEY_{}", rand::random::<u64>());
        std::env::set_var(key, "super_secret");

        let result = secrets
            .inject_secrets(
                key,
                super::ParamStr(&format!("This is a secret: ${{ env:{key} }}! 🫡")),
            )
            .await;
        assert_eq!(
            "This is a secret: super_secret! 🫡",
            result.expose_secret().as_str()
        );
    }

    #[tokio::test]
    async fn test_inject_secrets_no_env() {
        let mut secrets = super::Secrets::new();
        secrets.load_from(&[]).await.expect("to load successfully"); // Will automatically load `env` as the default

        let key = &format!("MY_SECRET_KEY_{}", rand::random::<u64>());

        // Ensure `MY_SECRET_KEY` is not set from other tests.
        std::env::remove_var(key);

        let result = secrets
            .inject_secrets(
                key,
                super::ParamStr(&format!("This is a secret: ${{ env:{key} }}! 🫡")),
            )
            .await;
        assert_eq!("This is a secret: ! 🫡", result.expose_secret().as_str());
    }
}
