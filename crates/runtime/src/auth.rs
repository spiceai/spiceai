use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

use dirs;
use serde::Deserialize;
use snafu::prelude::*;

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

#[allow(clippy::module_name_repetitions)]
pub type AuthConfigs = HashMap<String, AuthConfig>;

#[allow(clippy::module_name_repetitions)]
#[derive(Default, Deserialize, Clone)]
pub struct AuthConfig {
    pub params: HashMap<String, String>,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl AuthProviders {
    #[must_use]
    pub fn get(&self, name: &str) -> AuthProvider {
        let auth = if let Some(auth) = self.auth_configs.get(name) {
            tracing::trace!("Using auth provider: {}", name);
            auth
        } else {
            tracing::trace!("No auth provider found for {}", name);
            return AuthProvider::new(AuthConfig::default());
        };

        AuthProvider::new(auth.clone())
    }

    pub fn parse_from_config(&mut self) -> Result<()> {
        let mut auth_path = dirs::home_dir().context(UnableToFindHomeDirSnafu)?;
        auth_path.push(".spice/auth");

        let mut auth_file = File::open(auth_path).context(UnableToOpenAuthFileSnafu)?;
        let mut auth_contents = String::new();
        auth_file
            .read_to_string(&mut auth_contents)
            .context(UnableToReadAuthFileSnafu)?;

        self.auth_configs =
            toml::from_str::<AuthConfigs>(&auth_contents).context(UnableToParseAuthFileSnafu)?;
        Ok(())
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct AuthProvider {
    pub auth_config: AuthConfig,
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

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.auth_config.params.iter()
    }
}
