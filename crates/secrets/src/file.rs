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
use std::fs::File;
use std::io::Read;

use async_trait::async_trait;
use dirs;
use serde::Deserialize;
use snafu::prelude::*;

use super::{Secret, SecretStore};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to find home directory"))]
    UnableToFindHomeDir {},

    #[snafu(display("Unable to open auth file: {source}"))]
    UnableToOpenAuthFile { source: std::io::Error },

    #[snafu(display("Unable to read auth file: {source}"))]
    UnableToReadAuthFile { source: std::io::Error },

    #[snafu(display("Unable to parse auth file: {source}"))]
    UnableToParseAuthFile { source: toml::de::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(clippy::module_name_repetitions)]
pub type AuthConfigs = HashMap<String, AuthConfig>;

#[allow(clippy::module_name_repetitions)]
#[derive(Default, Deserialize, Clone)]
pub struct AuthConfig {
    pub params: HashMap<String, String>,
}

#[allow(clippy::module_name_repetitions)]
pub struct FileSecretStore {
    secrets: HashMap<String, Secret>,
}

impl FileSecretStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            secrets: HashMap::new(),
        }
    }
}

impl Default for FileSecretStore {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSecretStore {
    /// Loads the secrets.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a problem loading the secrets.
    pub fn load_secrets(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut auth_path = dirs::home_dir().context(UnableToFindHomeDirSnafu)?;
        auth_path.push(".spice/auth");

        let mut auth_file = File::open(auth_path).context(UnableToOpenAuthFileSnafu)?;
        let mut auth_contents = String::new();
        auth_file
            .read_to_string(&mut auth_contents)
            .context(UnableToReadAuthFileSnafu)?;

        let auth_configs =
            toml::from_str::<AuthConfigs>(&auth_contents).context(UnableToParseAuthFileSnafu)?;

        self.secrets = auth_configs
            .iter()
            .map(|(k, v)| (k.clone(), Secret::new(v.params.clone())))
            .collect();

        Ok(())
    }
}

#[async_trait]
impl SecretStore for FileSecretStore {
    #[must_use]
    async fn get_secret(&self, secret_name: &str) -> super::AnyErrorResult<Option<Secret>> {
        if let Some(secret) = self.secrets.get(secret_name) {
            return Ok(Some(secret.clone()));
        }

        Ok(None)
    }
}
