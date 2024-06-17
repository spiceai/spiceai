/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#[cfg(feature = "aws-secrets-manager")]
pub mod aws_secrets_manager;
pub mod env;
pub mod file;
#[cfg(feature = "keyring-secret-store")]
pub mod keyring;
pub mod kubernetes;

use std::collections::HashMap;

use async_trait::async_trait;
use secrecy::SecretString;
use snafu::prelude::*;

pub use secrecy::ExposeSecret;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load secrets: {source}"))]
    UnableToLoadSecrets { source: Box<dyn std::error::Error> },

    #[snafu(display("Unable to initialize AWS Secrets Manager: {source}"))]
    UnableToInitializeAwsSecretsManager {
        source: crate::aws_secrets_manager::Error,
    },
    #[snafu(display("Unable to parse secret value"))]
    UnableToParseSecretValue {},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type AnyErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[async_trait]
pub trait SecretStore {
    async fn get_secret(&self, secret_name: &str) -> AnyErrorResult<Option<Secret>>;
}

#[derive(Debug, Clone)]
pub struct Secret {
    data: HashMap<String, SecretString>,
}

impl Secret {
    #[must_use]
    pub fn new(data: HashMap<String, String>) -> Self {
        let data = data
            .into_iter()
            .map(|(key, value)| (key, SecretString::from(value)))
            .collect();

        Self { data }
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        let secret_value = self.data.get(key)?;
        let exposed_secret = secret_value.expose_secret();
        Some(exposed_secret)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &SecretString)> {
        self.data.iter()
    }

    pub fn add(&mut self, key: String, value: String) {
        self.data.insert(key, SecretString::from(value));
    }
}

pub enum SecretStoreType {
    File,
    Env,
    #[cfg(feature = "keyring-secret-store")]
    Keyring,
    Kubernetes,
    #[cfg(feature = "aws-secrets-manager")]
    AwsSecretsManager,
}

#[must_use]
pub fn spicepod_secret_store_type(store: &SecretStoreType) -> Option<SecretStoreType> {
    match store {
        SecretStoreType::File => Some(SecretStoreType::File),
        SecretStoreType::Env => Some(SecretStoreType::Env),
        #[cfg(feature = "keyring-secret-store")]
        SecretStoreType::Keyring => Some(SecretStoreType::Keyring),
        SecretStoreType::Kubernetes => Some(SecretStoreType::Kubernetes),
        #[cfg(feature = "aws-secrets-manager")]
        SecretStoreType::AwsSecretsManager => Some(SecretStoreType::AwsSecretsManager),
        #[cfg(not(all(feature = "keyring-secret-store", feature = "aws-secrets-manager")))]
        _ => None,
    }
}

#[must_use]
#[allow(clippy::implicit_hasher)]
pub fn get_secret_or_param(
    params: &HashMap<String, String>,
    secret: &Option<Secret>,
    secret_param_key: &str,
    param_key: &str,
) -> Option<String> {
    let secret_param_val = match params.get(secret_param_key) {
        Some(val) => val,
        None => param_key,
    };

    if let Some(secrets) = secret {
        if let Some(secret_val) = secrets.get(secret_param_val) {
            return Some(secret_val.to_string());
        };
    };

    if let Some(param_val) = params.get(param_key) {
        return Some(param_val.to_string());
    };

    None
}
