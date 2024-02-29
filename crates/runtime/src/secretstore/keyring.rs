use std::collections::HashMap;

use keyring::Entry;

use super::{Result, Secret, SecretStore};

pub struct KeyringSecretStore;

impl KeyringSecretStore {
    pub fn new() -> Self {
        Self {}
    }
}

impl SecretStore for KeyringSecretStore {
    #[must_use]
    fn get_secret(&self, key: &str) -> Secret {
        let entry = match Entry::new(key, "spiced") {
            Ok(entry) => entry,
            Err(e) => return Secret::new(HashMap::new()),
        };

        let secret = match entry.get_password() {
            Ok(secret) => secret,
            Err(e) => {
                return Secret::new(HashMap::new());
            }
        };

        let mut data: HashMap<String, String> = HashMap::new();
        data.insert("data".to_string(), secret);

        Secret::new(data)
    }

    fn init(&mut self) -> Result<()> {
        Ok(())
    }
}
