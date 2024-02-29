use std::io::Read;
use std::{collections::HashMap, fs::File};

use dirs;
use snafu::prelude::*;

use super::{
    Error, Secret, SecretStore, UnableToFindHomeDirSnafu, UnableToOpenAuthFileSnafu,
    UnableToParseAuthFileSnafu, UnableToReadAuthFileSnafu,
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct FileSecretStore {
    secrets: HashMap<String, String>,
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
            return Secret::new();
        };

        Secret::new()
    }

    fn init(&mut self) -> Result<()> {
        let mut auth_path = dirs::home_dir().context(UnableToFindHomeDirSnafu)?;
        auth_path.push(".spice/auth");

        let mut auth_file = File::open(auth_path).context(UnableToOpenAuthFileSnafu)?;
        let mut auth_contents = String::new();
        auth_file
            .read_to_string(&mut auth_contents)
            .context(UnableToReadAuthFileSnafu)?;

        self.secrets = toml::from_str::<HashMap<String, String>>(&auth_contents)
            .context(UnableToParseAuthFileSnafu)?;

        Ok(())
    }
}
