use std::io::Read;
use std::{collections::HashMap, fs::File};

use dirs;
use serde::Deserialize;
use snafu::prelude::*;

use super::{
    Result, Secret, SecretStore, UnableToFindHomeDirSnafu, UnableToOpenAuthFileSnafu,
    UnableToParseAuthFileSnafu, UnableToReadAuthFileSnafu,
};

#[allow(clippy::module_name_repetitions)]
pub type AuthConfigs = HashMap<String, AuthConfig>;

#[allow(clippy::module_name_repetitions)]
#[derive(Default, Deserialize, Clone)]
pub struct AuthConfig {
    pub params: HashMap<String, String>,
}

pub struct FileSecretStore {
    secrets: HashMap<String, HashMap<String, String>>,
}

impl FileSecretStore {
    pub fn new() -> Self {
        Self {
            secrets: HashMap::new(),
        }
    }
}

impl SecretStore for FileSecretStore {
    #[must_use]
    fn get_secret(&self, key: &str) -> Secret {
        let secret = if let Some(auth) = self.secrets.get(key) {
            tracing::trace!("Using file auth provider secret key: {}", key);
            auth
        } else {
            tracing::trace!("No secret found for key {}", key);
            return Secret::new(HashMap::new());
        };

        Secret::new(secret.clone())
    }

    fn init(&mut self) -> Result<()> {
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
            .map(|(k, v)| (k.clone(), v.params.clone()))
            .collect();

        Ok(())
    }
}
