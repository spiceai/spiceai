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

use async_trait::async_trait;
use secrecy::SecretString;

use super::SecretStore;

const ENV_SECRET_PREFIX: &str = "SPICE_SECRET_";

pub struct EnvSecretStore {}

impl Default for EnvSecretStore {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvSecretStore {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl SecretStore for EnvSecretStore {
    /// The key is case-sensitive. Calling `get_secret("my_key")` is distinct from `get_secret("MY_KEY")`.
    ///
    /// The convention is for environment variables to be uppercase, but this isn't required.
    ///
    /// <https://www.gnu.org/software/libc/manual/html_node/Environment-Variables.html>
    /// > Names of environment variables are case-sensitive and must not contain the character ‘=’. System-defined environment variables are invariably uppercase.
    #[must_use]
    async fn get_secret(&self, key: &str) -> super::AnyErrorResult<Option<SecretString>> {
        // TODO: Handle falling back to the spice generated prefix
        match std::env::var(key) {
            Ok(value) => Ok(Some(SecretString::new(value))),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(err) => Err(Box::new(err)),
        }
    }
}
