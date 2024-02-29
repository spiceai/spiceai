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
pub struct AuthProviders {
    pub auth_configs: AuthConfigs,
}

#[derive(Default, Clone)]
pub struct AuthConfig {
    pub params: HashMap<String, String>,
}

#[allow(clippy::module_name_repetitions)]
pub type AuthConfigs = HashMap<String, AuthConfig>;

#[allow(clippy::module_name_repetitions)]
#[derive(Default, Deserialize, Clone)]
pub struct Secret {
    pub data: HashMap<String, String>,
}

#[allow(clippy::module_name_repetitions)]
pub struct SecretStore {
    pub secret: Secret,
}

#[derive(Clone)]
pub struct AuthProvider {
    auth_config: AuthConfig,
}

impl AuthProvider {
    #[must_use]
    pub fn new(auth_config: AuthConfig) -> Self
    where
        Self: Sized,
    {
        AuthProvider { auth_config }
    }

    #[must_use]
    pub fn get_param(&self, param: &str) -> Option<&str> {
        self.auth_config
            .params
            .get(&param.to_string())
            .map(String::as_str)
    }
}
