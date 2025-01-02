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

use async_trait::async_trait;
use keyring::Entry;
use secrecy::SecretString;
use snafu::Snafu;

use crate::secrets::SecretStore;

const KEYRING_SECRET_PREFIX: &str = "spice_";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get secret from keyring: {source}"))]
    UnableToGetSecret { source: keyring::Error },

    #[snafu(display("Unable to get keyring secret value: {source}"))]
    UnableToGetSecretValue { source: keyring::Error },
}

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
    async fn get_secret(&self, key: &str) -> crate::secrets::AnyErrorResult<Option<SecretString>> {
        // Try with prefixed key first
        let prefixed_key = format!("{KEYRING_SECRET_PREFIX}{key}");

        // Try prefixed key
        match (
            Entry::new(&prefixed_key, "spiced"),
            Entry::new(key, "spiced"),
        ) {
            (Ok(prefixed_entry), Ok(fallback_entry)) => {
                // Try getting password with prefixed key first
                match prefixed_entry.get_password() {
                    Ok(secret) => return Ok(Some(SecretString::new(secret))),
                    Err(keyring::Error::NoEntry) => {
                        // Prefixed key failed, try fallback
                        match fallback_entry.get_password() {
                            Ok(secret) => Ok(Some(SecretString::new(secret))),
                            Err(keyring::Error::NoEntry) => Ok(None),
                            Err(err) => {
                                Err(Box::new(Error::UnableToGetSecretValue { source: err }))
                            }
                        }
                    }
                    Err(err) => Err(Box::new(Error::UnableToGetSecretValue { source: err })),
                }
            }
            (Err(keyring::Error::NoEntry), Ok(fallback_entry)) => {
                // Prefixed entry creation failed, try fallback
                match fallback_entry.get_password() {
                    Ok(secret) => Ok(Some(SecretString::new(secret))),
                    Err(keyring::Error::NoEntry) => Ok(None),
                    Err(err) => Err(Box::new(Error::UnableToGetSecretValue { source: err })),
                }
            }
            (Err(err), _) if !matches!(err, keyring::Error::NoEntry) => {
                Err(Box::new(Error::UnableToGetSecret { source: err }))
            }
            (_, Err(err)) => Err(Box::new(Error::UnableToGetSecret { source: err })),
            _ => Ok(None),
        }
    }
}
