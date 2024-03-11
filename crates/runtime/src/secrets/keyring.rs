use std::collections::HashMap;

use async_trait::async_trait;
use keyring::Entry;

use super::{Secret, SecretStore};

const KEYRING_SECRET_PREFIX: &str = "spiced_secret_";

#[allow(clippy::module_name_repetitions)]
pub struct KeyringSecretStore {}

impl Default for KeyringSecretStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyringSecretStore {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl SecretStore for KeyringSecretStore {
    #[must_use]
    async fn get_secret(&self, secret_name: &str) -> Option<Secret> {
        let entry_key = format!("{KEYRING_SECRET_PREFIX}{secret_name}");
        let Ok(entry) = Entry::new(entry_key.as_str(), "spiced") else {
            return None;
        };

        let Ok(secret) = entry.get_password() else {
            return None;
        };

        let Ok(parsed): serde_json::Result<serde_json::Value> =
            serde_json::from_str(secret.as_str())
        else {
            return None;
        };

        let Some(object) = parsed.as_object() else {
            return None;
        };

        let mut data = HashMap::new();

        object.iter().for_each(|(key, value)| {
            let Some(value) = value.as_str() else {
                return;
            };

            data.insert(key.clone(), value.to_string());
        });

        Some(Secret::new(data))
    }
}
