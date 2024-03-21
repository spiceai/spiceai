pub mod env;
pub mod file;
#[cfg(feature = "keyring-secret-store")]
pub mod keyring;
pub mod kubernetes;

use std::collections::HashMap;

use async_trait::async_trait;
use secrecy::SecretString;
use snafu::prelude::*;

use crate::file::FileSecretStore;
use spicepod::component::secrets::SpiceSecretStore;

pub use secrecy::ExposeSecret;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load secrets for {store}"))]
    UnableToLoadSecrets { store: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait SecretStore {
    async fn get_secret(&self, secret_name: &str) -> Option<Secret>;
}

#[derive(Debug, Clone)]
pub struct Secret {
    data: HashMap<String, SecretString>,
}

impl Secret {
    #[must_use]
    pub fn new(data: HashMap<String, String>) -> Self {
        let data = data
            .into_iter()
            .map(|(key, value)| (key, SecretString::from(value)))
            .collect();

        Self { data }
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        let Some(secret_value): Option<&SecretString> = self.data.get(key) else {
            return None;
        };

        let exposed_secret = secret_value.expose_secret();
        Some(exposed_secret)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &SecretString)> {
        self.data.iter()
    }

    pub fn add(&mut self, key: String, value: String) {
        self.data.insert(key, SecretString::from(value));
    }
}

pub enum SecretStoreType {
    File,
    Env,
    #[cfg(feature = "keyring-secret-store")]
    Keyring,
    Kubernetes,
}

#[must_use]
pub fn spicepod_secret_store_type(store: &SpiceSecretStore) -> Option<SecretStoreType> {
    match store {
        SpiceSecretStore::File => Some(SecretStoreType::File),
        SpiceSecretStore::Env => Some(SecretStoreType::Env),
        #[cfg(feature = "keyring-secret-store")]
        SpiceSecretStore::Keyring => Some(SecretStoreType::Keyring),
        SpiceSecretStore::Kubernetes => Some(SecretStoreType::Kubernetes),
        #[cfg(not(feature = "keyring-secret-store"))]
        _ => None,
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct SecretsProvider {
    pub store: SecretStoreType,

    secret_store: Option<Box<dyn SecretStore + Send + Sync>>,
}

impl Default for SecretsProvider {
    fn default() -> Self {
        Self {
            store: SecretStoreType::File,
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
    pub fn load_secrets(&mut self) -> Result<()> {
        match self.store {
            SecretStoreType::File => {
                let mut file_secret_store = FileSecretStore::new();

                if file_secret_store.load_secrets().is_err() {
                    return Err(Error::UnableToLoadSecrets {
                        store: "file".to_string(),
                    });
                }

                self.secret_store = Some(Box::new(file_secret_store));
            }
            SecretStoreType::Env => {
                let mut env_secret_store = env::EnvSecretStore::new();

                env_secret_store.load_secrets();

                self.secret_store = Some(Box::new(env_secret_store));
            }
            #[cfg(feature = "keyring-secret-store")]
            SecretStoreType::Keyring => {
                self.secret_store = Some(Box::new(keyring::KeyringSecretStore::new()));
            }
            SecretStoreType::Kubernetes => {
                let mut kubernetes_secret_store = kubernetes::KubernetesSecretStore::new();

                if kubernetes_secret_store.init().is_err() {
                    return Err(Error::UnableToLoadSecrets {
                        store: "kubernetes".to_string(),
                    });
                };

                self.secret_store = Some(Box::new(kubernetes_secret_store));
            }
        }

        Ok(())
    }

    #[must_use]
    pub async fn get_secret(&self, secret_name: &str) -> Option<Secret> {
        if let Some(ref secret_store) = self.secret_store {
            secret_store.get_secret(secret_name).await
        } else {
            None
        }
    }
}
