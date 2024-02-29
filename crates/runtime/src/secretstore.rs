use std::collections::HashMap;

use serde::Deserialize;
use snafu::Snafu;

pub mod file;
pub mod keyring;

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    pub stores: HashMap<String, Box<dyn SecretStore + Send + Sync>>,
}

impl SecretStores {
    #[must_use]
    pub fn new() -> Self
    where
        Self: Sized,
    {
        Self {
            stores: HashMap::new(),
        }
    }

    #[must_use]
    pub fn get_store(&self, name: &str) -> Option<&Box<dyn SecretStore + Send + Sync>> {
        self.stores.get(&name.to_string())
    }

    pub fn add_store(&mut self, name: String, store: Box<dyn SecretStore + Send + Sync>) {
        self.stores.insert(name, store);
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Default, Deserialize, Clone)]
pub struct Secret {
    data: HashMap<String, String>,
}

impl Secret {
    #[must_use]
    pub fn get_secret(&self, key: &str) -> Option<&str> {
        self.data.get(&key.to_string()).map(String::as_str)
    }

    pub fn new() -> Self
    where
        Self: Sized,
    {
        Self {
            data: HashMap::new(),
        }
    }
}

pub trait SecretStore {
    fn get_secret(&self, key: &str) -> Secret {
        Secret::new()
    }
    fn init(&mut self) -> Result<()> {
        Ok(())
    }
}

// #[allow(clippy::module_name_repetitions)]
// pub struct SecretStore {
//     pub secrets: HashMap<String, Secret>,
// }

// impl SecretStore {
//     #[must_use]
//     pub fn new() -> Self
//     where
//         Self: Sized,
//     {
//         Self {
//             secrets: HashMap::new(),
//         }
//     }

//     #[must_use]
//     pub fn get(&self, key: &str) -> Option<Secret> {
//         self.secrets.get(&key.to_string()).cloned()
//     }
// }
