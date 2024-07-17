/*
Copyright 2024 The Spice.ai OSS Authors

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

#[cfg(feature = "aws-secrets-manager")]
pub mod aws_secrets_manager;
pub mod env;
#[cfg(feature = "keyring-secret-store")]
pub mod keyring;
pub mod kubernetes;

use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use async_trait::async_trait;
use indexmap::IndexMap;
use secrecy::SecretString;
use snafu::prelude::*;

use spicepod::component::secrets::SpiceSecretStore;

pub use secrecy::ExposeSecret;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load secrets: {source}"))]
    UnableToLoadSecrets { source: Box<dyn std::error::Error> },

    #[snafu(display("Unable to initialize AWS Secrets Manager: {source}"))]
    UnableToInitializeAwsSecretsManager { source: aws_secrets_manager::Error },
    #[snafu(display("Unable to parse secret value"))]
    UnableToParseSecretValue {},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type AnyErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

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

pub struct ParamStr<'a>(&'a str);
pub struct SecretKey<'a>(&'a str);

impl Secrets {
    async fn inject_secrets(&self, param_str: ParamStr<'_>) -> SecretString {
        todo!();
    }

    /// Gets a secret key from the connected secret stores in precedence order.
    async fn get_secret(&self, key: SecretKey<'_>) -> AnyErrorResult<Option<SecretString>> {
        todo!();
    }
}

pub enum SecretStoreType {
    Env,
    #[cfg(feature = "keyring-secret-store")]
    Keyring,
    Kubernetes(String),
    #[cfg(feature = "aws-secrets-manager")]
    AwsSecretsManager(String),
}

#[must_use]
pub fn spicepod_secret_store_type(store: &SpiceSecretStore) -> Option<SecretStoreType> {
    match store {
        SpiceSecretStore::Env => Some(SecretStoreType::Env),
        #[cfg(feature = "keyring-secret-store")]
        SpiceSecretStore::Keyring => Some(SecretStoreType::Keyring),
        SpiceSecretStore::Kubernetes => todo!(),
        #[cfg(feature = "aws-secrets-manager")]
        SpiceSecretStore::AwsSecretsManager => todo!(),
        #[cfg(not(all(feature = "keyring-secret-store", feature = "aws-secrets-manager")))]
        _ => None,
    }
}

pub struct SecretsProvider {
    pub store: SecretStoreType,

    secret_store: Option<Box<dyn SecretStore + Send + Sync>>,
}

impl Default for SecretsProvider {
    fn default() -> Self {
        Self {
            store: SecretStoreType::Env,
            secret_store: None,
        }
    }
}

impl SecretsProvider {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads the secrets from the secret store.
    ///
    /// # Errors
    ///
    /// Returns an error if the secrets cannot be loaded.
    pub async fn load_secrets(&mut self) -> Result<()> {
        match &self.store {
            SecretStoreType::Env => {
                let env_secret_store = env::EnvSecretStore::new();

                self.secret_store = Some(Box::new(env_secret_store));
            }
            #[cfg(feature = "keyring-secret-store")]
            SecretStoreType::Keyring => {
                self.secret_store = Some(Box::new(keyring::KeyringSecretStore::new()));
            }
            SecretStoreType::Kubernetes(secret_name) => {
                let mut kubernetes_secret_store =
                    kubernetes::KubernetesSecretStore::new(secret_name.clone());

                kubernetes_secret_store
                    .init()
                    .context(UnableToLoadSecretsSnafu)?;

                self.secret_store = Some(Box::new(kubernetes_secret_store));
            }
            #[cfg(feature = "aws-secrets-manager")]
            SecretStoreType::AwsSecretsManager(secret_name) => {
                let secret_store = aws_secrets_manager::AwsSecretsManager::new(secret_name.clone());

                secret_store
                    .init()
                    .await
                    .context(UnableToInitializeAwsSecretsManagerSnafu)?;

                self.secret_store = Some(Box::new(secret_store));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::secrets::{get_secret_or_param, Secret};

    #[test]
    fn test_value_from_secret() {
        let mut params = HashMap::new();
        params.insert("secret_param".to_string(), "val".to_string());
        params.insert("anything".to_string(), "no_the_val".to_string());

        let secret = Secret::new(
            vec![("val".to_string(), "secret_value".to_string())]
                .into_iter()
                .collect(),
        );
        assert_eq!(
            Some("secret_value".to_string()),
            get_secret_or_param(&params, &Some(secret), "secret_param", "anything")
        );
    }

    #[test]
    fn test_value_from_fallback_to_secrets() {
        let mut params = HashMap::new();
        params.insert("secret_param".to_string(), "val_no_secret".to_string());
        params.insert("anything".to_string(), "value_from_params".to_string());

        let secret = Secret::new(
            vec![("val".to_string(), "secret_value".to_string())]
                .into_iter()
                .collect(),
        );
        assert_eq!(
            Some("value_from_params".to_string()),
            get_secret_or_param(&params, &Some(secret), "secret_param", "anything")
        );
    }
}
