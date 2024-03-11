pub mod env;
pub mod file;
pub mod keyring;
pub mod kubernetes;

use std::collections::HashMap;

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};

use super::Result;
use crate::{secrets::file::FileSecretStore, Error};
use spicepod::component::secrets::SpiceSecretStore;

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

    pub fn add(&mut self, key: String, value: String) {
        self.data.insert(key, SecretString::from(value));
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct SecretsProvider {
    pub store: SpiceSecretStore,

    secret_store: Option<Box<dyn SecretStore + Send + Sync>>,
}

impl Default for SecretsProvider {
    fn default() -> Self {
        Self {
            store: SpiceSecretStore::File,
            secret_store: None,
        }
    }
}

impl SecretsProvider {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load_secrets(&mut self) -> Result<()> {
        match self.store {
            SpiceSecretStore::File => {
                let mut file_secret_store = FileSecretStore::new();

                if file_secret_store.load_secrets().is_err() {
                    return Err(Error::UnableToLoadSecrets {
                        store: "file".to_string(),
                    });
                }

                self.secret_store = Some(Box::new(file_secret_store));
            }
            SpiceSecretStore::Env => {
                let mut env_secret_store = env::EnvSecretStore::new();

                env_secret_store.load_secrets();

                self.secret_store = Some(Box::new(env_secret_store));
            }
            SpiceSecretStore::Keyring => {
                self.secret_store = Some(Box::new(keyring::KeyringSecretStore::new()));
            }
            SpiceSecretStore::Kubernetes => {
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
