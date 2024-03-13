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
    pub fn load_secrets(&mut self) -> Result<()> {
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

<<<<<<< HEAD:crates/runtime/src/auth.rs
#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct AuthProvider {
    pub auth_config: AuthConfig,
}

impl AuthProvider {
=======
#[async_trait]
impl SecretStore for FileSecretStore {
>>>>>>> a219a58480356e5b0311ded3ebf1e654bc5f2b0a:crates/runtime/src/secrets/file.rs
    #[must_use]
    async fn get_secret(&self, secret_name: &str) -> Option<Secret> {
        if let Some(secret) = self.secrets.get(secret_name) {
            return Some(secret.clone());
        }

        None
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.auth_config.params.iter()
    }
}
