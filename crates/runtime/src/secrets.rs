pub mod file;
use std::collections::HashMap;

use super::Result;
use crate::{secrets::file::FileSecretStore, Error};
use spicepod::component::secrets::SpiceSecretStore;

pub trait SecretStore {
    fn get_secret(&self, secret_name: &str) -> Option<Secret>;
}

#[derive(Debug, Clone)]
pub struct Secret {
    data: HashMap<String, String>,
}

impl Secret {
    #[must_use]
    pub fn new(data: HashMap<String, String>) -> Self {
        Self { data }
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        if let Some(value) = self.data.get(key) {
            Some(value.as_str())
        } else {
            None
        }
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
                println!("Loading secrets from file");
                let mut file_secret_store = FileSecretStore::new();

                if file_secret_store.load_secrets().is_err() {
                    return Err(Error::UnableToLoadSecrets {
                        store: "file".to_string(),
                    });
                }

                self.secret_store = Some(Box::new(file_secret_store));
            }
            SpiceSecretStore::Keyring => {
                println!("Loading secrets from keyring");
            }
        }

        Ok(())
    }

    #[must_use]
    pub fn get_secret(&self, secret_name: &str) -> Option<Secret> {
        if let Some(ref secret_store) = self.secret_store {
            secret_store.get_secret(secret_name)
        } else {
            None
        }
    }
}
