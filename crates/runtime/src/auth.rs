use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

use dirs;
use serde::Deserialize;
use snafu::prelude::*;

pub mod dremio;
pub mod none;
pub mod spiceai;

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

#[allow(clippy::module_name_repetitions)]
pub trait AuthProvider {
    fn new(auth: &Auth) -> Self
    where
        Self: Sized;
    fn get_token(&self) -> String {
        String::new()
    }
    fn get_username(&self) -> String {
        String::new()
    }
    fn get_password(&self) -> String {
        String::new()
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Default)]
pub struct AuthProviders {
    pub auth: AuthConfig,
}

#[allow(clippy::module_name_repetitions)]
pub type AuthConfig = HashMap<String, Auth>;

#[derive(Deserialize, Default)]
pub struct Auth {
    pub provider_type: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl AuthProviders {
    #[must_use]
    pub fn get(&self, name: &str) -> Box<dyn AuthProvider> {
        let auth = if let Some(auth) = self.auth.get(name) {
            tracing::trace!("Using auth provider: {}", auth.provider_type);
            auth
        } else {
            tracing::trace!("No auth provider found for {}", name);
            return Box::new(none::NoneAuth::new(&Auth::default()));
        };

        match auth.provider_type.as_str() {
            "spice.ai" => Box::new(spiceai::SpiceAuth::new(auth)),
            "dremio" => Box::new(dremio::DremioAuth::new(auth)),
            _ => Box::new(none::NoneAuth::new(&Auth::default())),
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
