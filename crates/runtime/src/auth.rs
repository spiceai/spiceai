use dirs;
use snafu::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

use serde::Deserialize;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to find home directory"))]
    UnableToFindHomeDir {},

    #[snafu(display("Unable to open auth file"))]
    UnableToOpenAuthFile { source: std::io::Error },

    #[snafu(display("Unable to read auth file"))]
    UnableToReadAuthFile { source: std::io::Error },

    #[snafu(display("Unable to parse auth file"))]
    UnableToParseAuthFile { source: toml::de::Error },
}

pub trait Auth {
    fn get_token(&self) -> String;
}

#[allow(clippy::module_name_repetitions)]
#[derive(Default)]
pub struct AuthProviders {
    pub auth: AuthConfig,
}

#[allow(clippy::module_name_repetitions)]
#[derive(Deserialize)]
pub struct AuthProvider {
    pub provider_type: String,
    pub key: String,
}

#[allow(clippy::module_name_repetitions)]
pub type AuthConfig = HashMap<String, AuthProvider>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub mod none;
pub mod spiceai;

impl AuthProviders {
    #[must_use]
    pub fn get(&self, name: &str) -> Box<dyn Auth> {
        let auth_provider = if let Some(auth_provider) = self.auth.get(name) {
            tracing::info!("Using auth provider: {}", auth_provider.provider_type);
            auth_provider
        } else {
            tracing::info!("No auth provider found for {}", name);
            return Box::new(none::NoneAuth::new());
        };

        match auth_provider.provider_type.as_str() {
            "spice.ai" => Box::new(spiceai::SpiceAuth::new(auth_provider.key.to_string())),
            _ => Box::new(none::NoneAuth::new()),
        }
    }

    pub fn parse_from_config(&mut self) -> Result<()> {
        let mut auth_path = dirs::home_dir().context(UnableToFindHomeDirSnafu)?;
        auth_path.push(".spice/auth");

        let mut auth_file = File::open(auth_path).context(UnableToOpenAuthFileSnafu)?;
        let mut auth_contents = String::new();
        auth_file
            .read_to_string(&mut auth_contents)
            .context(UnableToReadAuthFileSnafu)?;

        self.auth =
            toml::from_str::<AuthConfig>(&auth_contents).context(UnableToParseAuthFileSnafu)?;
        Ok(())
    }
}
