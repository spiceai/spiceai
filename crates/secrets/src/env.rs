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

use super::{Secret, SecretStore};

const ENV_SECRET_PREFIX: &str = "SPICE_SECRET_";

#[allow(clippy::module_name_repetitions)]
pub struct EnvSecretStore {
    secrets: HashMap<String, Secret>,
}

impl Default for EnvSecretStore {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvSecretStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            secrets: HashMap::new(),
        }
    }

    fn add_secret_value(&mut self, secret_name: &str, key: &str, value: &str) {
        if let Some(secret) = self.secrets.get_mut(secret_name) {
            secret.add(key.to_string(), value.to_string());
        } else {
            self.secrets.insert(
                secret_name.to_string(),
                Secret::new(
                    vec![(key.to_string(), value.to_string())]
                        .into_iter()
                        .collect(),
                ),
            );
        }
    }

    /// Load secrets from the environment.
    /// It will search for environment variables formatted as `SPICE_SECRET_<SECRET-NAME>_<SECRET-KEY>` and add them to the secret store.
    ///
    /// Example:
    /// ```shell
    /// SPICE_SECRET_SPICEAI_MY_KEY_1=my_value_1
    /// SPICE_SECRET_SPICEAI_MY_KEY_2=my_value_2
    /// ```
    /// will be compiled into
    /// ```json
    /// {
    ///     "spiceai": {
    ///         "my_key_1": "my_value_1",
    ///         "my_key_2": "my_value_2"
    ///     }
    /// }
    /// ```
    pub fn load_secrets(&mut self) {
        for (key, value) in std::env::vars() {
            if !key.starts_with(ENV_SECRET_PREFIX) {
                continue;
            }

            let Some((secret_name, key)) =
                key.trim_start_matches(ENV_SECRET_PREFIX).split_once('_')
            else {
                continue;
            };

            if secret_name.is_empty() || key.is_empty() {
                continue;
            }

            self.add_secret_value(
                secret_name.to_lowercase().as_str(),
                key.to_lowercase().as_str(),
                value.as_str(),
            );
        }
    }
}

#[async_trait]
impl SecretStore for EnvSecretStore {
    #[must_use]
    async fn get_secret(&self, secret_name: &str) -> super::AnyErrorResult<Option<Secret>> {
        if let Some(secret) = self.secrets.get(secret_name) {
            return Ok(Some(secret.clone()));
        }

        Ok(None)
    }
}
