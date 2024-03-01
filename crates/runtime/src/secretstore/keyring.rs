use std::collections::HashMap;

use keyring::Entry;

use super::{Result, Secret, SecretStore};

#[allow(clippy::module_name_repetitions)]
#[derive(Default)]
pub struct KeyringSecretStore;

impl KeyringSecretStore {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl SecretStore for KeyringSecretStore {
    #[must_use]
    fn get_secret(&self, key: &str) -> Secret {
        let Ok(entry) = Entry::new(key, "spiced") else {
            return Secret::new(HashMap::new());
        };

        let Ok(secret) = entry.get_password() else {
            return Secret::new(HashMap::new());
        };

        let mut data: HashMap<String, String> = HashMap::new();
        data.insert("data".to_string(), secret);

        Secret::new(data)
    }

    fn init(&mut self) -> Result<()> {
        Ok(())
    }
}
