use std::collections::HashMap;

use async_trait::async_trait;

use super::{Secret, SecretStore};

const ENV_SECRET_PREFIX: &str = "SPICED_SECRET_";

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
            secret.data.insert(key.to_string(), value.to_string());
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
    /// It will search for environment variables formatted as `SPICED_SECRET_<SECRET-NAME>_<SECRET-KEY>` and add them to the secret store.
    ///
    /// Example:
    /// ```shell
    /// SPICED_SECRET_SPICEAI_MY_KEY_1=my_value_1
    /// SPICED_SECRET_SPICEAI_MY_KEY_2=my_value_2
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
    async fn get_secret(&self, secret_name: &str) -> Option<Secret> {
        if let Some(secret) = self.secrets.get(secret_name) {
            return Some(secret.clone());
        }

        None
    }
}
