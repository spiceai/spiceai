use std::fs::File;
use std::io::Read;

use app::Result;
use dirs;
use snafu::prelude::*;

use super::{
    AuthConfigs, AuthProvider, UnableToFindHomeDirSnafu, UnableToOpenAuthFileSnafu,
    UnableToParseAuthFileSnafu, UnableToReadAuthFileSnafu,
};
use crate::secretstore::AuthConfig;

// pub type Result<T, E = dyn Error> = std::result::Result<T, E>;

pub struct FileSecretStore {
    auth_configs: AuthConfigs,
}

impl FileSecretStore {
    #[must_use]
    pub fn get_secret(&self, key: &str) -> Secret {
        let auth = if let Some(auth) = self.auth_configs.get(key) {
            tracing::trace!("Using file auth provider secret key: {}", key);
            auth
        } else {
            tracing::trace!("No secret found for key {}", key);
            return Secret::new(AuthConfig::default());
        };

        Secret::new(auth.clone())
    }

    pub fn init(&mut self) -> Result<()> {
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
