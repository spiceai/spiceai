/*
 Copyright 2024 Spice AI, Inc.

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

use std::collections::HashMap;

use async_trait::async_trait;
use keyring::Entry;

use super::{Secret, SecretStore};

const KEYRING_SECRET_PREFIX: &str = "spice_secret_";

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

        let object = parsed.as_object()?;

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
