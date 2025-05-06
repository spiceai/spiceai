/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use tokio::sync::watch;

pub mod databricks;
pub mod github_app_token;
pub mod registry;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get token.\n{source}"))]
    UnableToGetToken {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait TokenProvider: Send + Sync + Debug {
    async fn get_token(&self) -> Result<String>;

    /// Returns a `watch::Receiver` of new tokens, if the provider supports refresh.
    ///
    /// The default implementation gives no updates.
    fn subscribe(&self) -> Option<watch::Receiver<String>> {
        None
    }
}

pub struct StaticTokenProvider {
    token: Arc<SecretString>,
}

impl std::fmt::Debug for StaticTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticTokenProvider")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

impl StaticTokenProvider {
    #[must_use]
    pub fn new(token: SecretString) -> Self {
        Self {
            token: Arc::new(token),
        }
    }
}

#[async_trait]
impl TokenProvider for StaticTokenProvider {
    async fn get_token(&self) -> Result<String> {
        Ok(self.token.expose_secret().to_string())
    }
}
