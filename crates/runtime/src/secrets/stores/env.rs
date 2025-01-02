/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::path::PathBuf;

use async_trait::async_trait;
use secrecy::SecretString;

use crate::secrets::SecretStore;

const ENV_SECRET_PREFIX: &str = "SPICE_";

pub struct EnvSecretStoreBuilder {
    path: Option<PathBuf>,
}

impl Default for EnvSecretStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvSecretStoreBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self { path: None }
    }

    #[must_use]
    pub fn with_path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    #[must_use]
    pub fn build(self) -> EnvSecretStore {
        let env = EnvSecretStore { path: self.path };
        env.load();
        env
    }
}

pub struct EnvSecretStore {
    path: Option<PathBuf>,
}

impl EnvSecretStore {
    fn load(&self) {
        if let Some(path) = &self.path {
            match dotenvy::from_path(path) {
                Ok(()) => return,
                Err(err) => {
                    if matches!(err, dotenvy::Error::LineParse(_, _)) {
                        tracing::warn!("{err}");
                    } else {
                        tracing::warn!("Error opening path {}: {err}", path.display());
                    }
                }
            };
        }
        if let Err(err) = dotenvy::from_filename(".env.local") {
            if matches!(err, dotenvy::Error::LineParse(_, _)) {
                tracing::warn!(".env.local: {err}");
            }
        };
        if let Err(err) = dotenvy::from_filename(".env") {
            if matches!(err, dotenvy::Error::LineParse(_, _)) {
                tracing::warn!(".env: {err}");
            }
        };
    }
}

#[async_trait]
impl SecretStore for EnvSecretStore {
    /// The key for std::env::var is case-sensitive. Calling `std::env::var("my_key")` is distinct from `std::env::var("MY_KEY")`.
    ///
    /// However, the convention is to use uppercase for environment variables - so to make the experience
    /// consistent across secret stores that don't have this convention we will uppercase the key before
    /// looking up the environment variable.
    #[must_use]
    async fn get_secret(&self, key: &str) -> crate::secrets::AnyErrorResult<Option<SecretString>> {
        let upper_key = key.to_uppercase();

        // First try looking for `SPICE_MY_KEY` and then `MY_KEY`
        let prefixed_key = format!("{ENV_SECRET_PREFIX}{upper_key}");
        match std::env::var(prefixed_key) {
            Ok(value) => Ok(Some(SecretString::new(value))),
            // If the prefixed key is not found, try the original key
            Err(std::env::VarError::NotPresent) => match std::env::var(upper_key) {
                Ok(value) => Ok(Some(SecretString::new(value))),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(err) => Err(Box::new(err)),
            },
            Err(err) => Err(Box::new(err)),
        }
    }
}
