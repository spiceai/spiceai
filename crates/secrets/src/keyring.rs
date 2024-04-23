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

use std::collections::HashMap;

use async_trait::async_trait;
use keyring::Entry;
use snafu::Snafu;

use super::{Secret, SecretStore};

const KEYRING_SECRET_PREFIX: &str = "spice_secret_";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get secret from keyring: {source}"))]
    UnableToGetSecret { source: keyring::Error },

    #[snafu(display("Unable to get keyring secret value: {source}"))]
    UnableToGetSecretValue { source: keyring::Error },

    #[snafu(display("Unable to parse keyring secret value: {source}"))]
    UnableToParseSecretValue { source: serde_json::Error },

    #[snafu(display("Invalid keyring secret value: JSON object is expected"))]
    InvalidJsonFormat {},
}

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
    async fn get_secret(&self, secret_name: &str) -> super::AnyErrorResult<Option<Secret>> {
        let entry_key = format!("{KEYRING_SECRET_PREFIX}{secret_name}");

        let entry = match Entry::new(entry_key.as_str(), "spiced") {
            Ok(entry) => entry,
            Err(keyring::Error::NoEntry) => {
                return Ok(None);
            }
            Err(err) => {
                return Err(Box::new(Error::UnableToGetSecret { source: err }));
            }
        };

        let secret = match entry.get_password() {
            Ok(secret) => secret,
            Err(keyring::Error::NoEntry) => {
                return Ok(None);
            }
            Err(err) => {
                return Err(Box::new(Error::UnableToGetSecretValue { source: err }));
            }
        };

        let parsed: serde_json::Result<serde_json::Value> = serde_json::from_str(secret.as_str());
        let parsed = match parsed {
            Ok(parsed) => parsed,
            Err(err) => {
                return Err(Box::new(Error::UnableToParseSecretValue { source: err }));
            }
        };

        let Some(object) = parsed.as_object() else {
            return Err(Box::new(Error::InvalidJsonFormat {}));
        };

        let mut data = HashMap::new();

        object.iter().for_each(|(key, value)| {
            let Some(value) = value.as_str() else {
                return;
            };

            data.insert(key.clone(), value.to_string());
        });

        Ok(Some(Secret::new(data)))
    }
}
