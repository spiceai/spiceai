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

use std::path::PathBuf;

use async_trait::async_trait;
use secrecy::SecretString;

use crate::secrets::SecretStore;

const ENV_SECRET_PREFIX: &str = "SPICE_";

pub struct EnvSecretStoreBuilder {
    path: Option<PathBuf>,
}

impl Default for EnvSecretStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvSecretStoreBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self { path: None }
    }

    #[must_use]
    pub fn with_path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    #[must_use]
    pub fn build(self) -> EnvSecretStore {
        let mut env = EnvSecretStore {
            path: self.path,
            loaded: false,
        };
        env.load();
        env
    }
}

pub struct EnvSecretStore {
    path: Option<PathBuf>,
    loaded: bool,
}

impl EnvSecretStore {
    fn load(&mut self) {
        if let Some(path) = &self.path {
            if path.is_symlink() {
                tracing::warn!("Error opening path: {}: Path is a symlink", path.display());
                return;
            }

            match (path.canonicalize(), std::env::current_dir()) {
                (Ok(canonical_path), Ok(current_dir)) => {
                    if !canonical_path.starts_with(current_dir) {
                        tracing::warn!(
                            "Error opening path: {}: Path is not in current directory",
                            path.display()
                        );
                        return;
                    }
                }
                (Err(e), _) | (_, Err(e)) => {
                    tracing::warn!("Error opening path: {}: {e}", path.display());
                    return;
                }
            }

            match dotenvy::from_path(path) {
                Ok(()) => {
                    self.loaded = true;
                    return;
                }
                Err(err) => {
                    if matches!(err, dotenvy::Error::LineParse(_, _)) {
                        tracing::warn!("{err}");
                    } else {
                        tracing::warn!("Error opening path {}: {err}", path.display());
                    }
                }
            };
        }
        if let Err(err) = dotenvy::from_filename(".env.local") {
            if matches!(err, dotenvy::Error::LineParse(_, _)) {
                tracing::warn!(".env.local: {err}");
            }
        };
        if let Err(err) = dotenvy::from_filename(".env") {
            if matches!(err, dotenvy::Error::LineParse(_, _)) {
                tracing::warn!(".env: {err}");
            }
        };
    }
}

#[async_trait]
impl SecretStore for EnvSecretStore {
    /// The key for std::env::var is case-sensitive. Calling `std::env::var("my_key")` is distinct from `std::env::var("MY_KEY")`.
    ///
    /// However, the convention is to use uppercase for environment variables - so to make the experience
    /// consistent across secret stores that don't have this convention we will uppercase the key before
    /// looking up the environment variable.
    #[must_use]
    async fn get_secret(&self, key: &str) -> crate::secrets::AnyErrorResult<Option<SecretString>> {
        let upper_key = key.to_uppercase();

        // First try looking for `SPICE_MY_KEY` and then `MY_KEY`
        let prefixed_key = format!("{ENV_SECRET_PREFIX}{upper_key}");
        match std::env::var(prefixed_key) {
            Ok(value) => Ok(Some(SecretString::new(value))),
            // If the prefixed key is not found, try the original key
            Err(std::env::VarError::NotPresent) => match std::env::var(upper_key) {
                Ok(value) => Ok(Some(SecretString::new(value))),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(err) => Err(Box::new(err)),
            },
            Err(err) => Err(Box::new(err)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_loading_env_outside_of_current_dir() {
        let path = PathBuf::from("/tmp/.env");
        let mut env = EnvSecretStoreBuilder::new().with_path(path).build();
        env.load();

        assert!(!env.loaded); // env should not load because it's out of this directory
    }

    #[test]
    fn test_loading_env() {
        std::fs::File::create("test.env")
            .expect("file should be created")
            .write_all(b"TEST=1")
            .expect("file should be written");

        let path = PathBuf::from("test.env");
        let mut env = EnvSecretStoreBuilder::new().with_path(path).build();
        env.load();

        assert!(env.loaded); // env should load

        // check the env
        let value = std::env::var("TEST").expect("env var should be set");
        assert_eq!(value, "1");

        std::fs::remove_file("test.env").expect("file should be removed");
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_loading_env_with_symlink() {
        std::fs::File::create("symlinked.env")
            .expect("file should be created")
            .write_all(b"TEST=1")
            .expect("file should be written");

        std::os::unix::fs::symlink("symlinked.env", "symlink.env")
            .expect("symlink should be created");

        let path = PathBuf::from("symlink.env");
        let mut env = EnvSecretStoreBuilder::new().with_path(path).build();
        env.load();

        assert!(!env.loaded); // env should not load

        std::fs::remove_file("symlinked.env").expect("file should be removed");
        std::fs::remove_file("symlink.env").expect("file should be removed");
    }
}
