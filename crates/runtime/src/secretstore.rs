use std::collections::HashMap;

use serde::Deserialize;
use snafu::Snafu;

pub mod file;
pub mod keyring;

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

#[allow(clippy::module_name_repetitions)]
#[derive(Default)]
pub struct SecretStores {
    pub stores: HashMap<String, SecretStore>,
}

#[derive(Default, Clone)]
pub struct AuthConfig {
    pub params: HashMap<String, String>,
}

#[allow(clippy::module_name_repetitions)]
#[derive(Default, Deserialize, Clone)]
pub struct Secret {
    pub data: HashMap<String, String>,
}

#[allow(clippy::module_name_repetitions)]
pub struct SecretStore {
    pub secrets: HashMap<String, Secret>,
}

impl SecretStore {
    #[must_use]
    pub fn new() -> Self
    where
        Self: Sized,
    {
        Self {
            secrets: HashMap::new(),
        }
    }

    #[must_use]
    pub fn get_secret(&self, key: &str) -> Option<&str> {
        self.secrets
            .get(&key.to_string())
            .map(String::as_str)
    }
}
