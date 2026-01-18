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

use std::path::PathBuf;

use async_trait::async_trait;
use secrecy::SecretString;

use crate::SecretStore;

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
        let env = EnvSecretStore { path: self.path };
        env.load();
        env
    }
}

pub struct EnvSecretStore {
    path: Option<PathBuf>,
}

impl EnvSecretStore {
    fn load(&self) {
        if let Some(path) = &self.path {
            match dotenvy::from_path(path) {
                Ok(()) => return,
                Err(err) => {
                    if matches!(err, dotenvy::Error::LineParse(_, _)) {
                        tracing::warn!("{err}");
                    } else {
                        tracing::warn!("Error opening path {}: {err}", path.display());
                    }
                }
            }
        }
        if let Err(err) = dotenvy::from_filename(".env.local")
            && matches!(err, dotenvy::Error::LineParse(_, _))
        {
            tracing::warn!(".env.local: {err}");
        }
        if let Err(err) = dotenvy::from_filename(".env")
            && matches!(err, dotenvy::Error::LineParse(_, _))
        {
            tracing::warn!(".env: {err}");
        }
    }
}

#[async_trait]
impl SecretStore for EnvSecretStore {
    /// The key for `std::env::var` is case-sensitive. Calling `std::env::var("my_key")` is distinct from `std::env::var("MY_KEY")`.
    ///
    /// However, the convention is to use uppercase for environment variables - so to make the experience
    /// consistent across secret stores that don't have this convention we will search for both original and uppercased keys.
    async fn get_secret(&self, key: &str) -> crate::AnyErrorResult<Option<SecretString>> {
        let uppercase_key = key.to_uppercase();

        [
            // First try looking for original prefixed `SPICE_my_key`
            format!("{ENV_SECRET_PREFIX}{key}"),
            // Then try looking for original `my_key`
            key.to_string(),
            // Then try looking for prefixed `SPICE_MY_KEY` in uppercase
            format!("{ENV_SECRET_PREFIX}{uppercase_key}"),
            // Then try looking for `MY_KEY` in uppercase
            uppercase_key,
        ]
        .iter()
        .find_map(|variant| match std::env::var(variant) {
            Ok(value) => Some(Ok(SecretString::from(value))),
            Err(std::env::VarError::NotPresent) => None,
            Err(err) => Some(Err(
                Box::new(err) as Box<dyn std::error::Error + Send + Sync>
            )),
        })
        .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    /// Test that verifies the dotenvy patch disables variable substitution.
    ///
    /// **Patch**: `dotenvy` (spiceai fork)
    /// **Purpose**: Disable shell-style variable substitution in .env files
    /// **Tracking Issue**: <https://github.com/allan2/dotenvy/issues/113>
    ///
    /// **What happens without this patch**: Values containing `$` characters like
    /// `API_KEY=sk-abc$123` would be incorrectly parsed as variable references,
    /// resulting in `sk-abc` (with `$123` treated as an undefined variable).
    ///
    /// This test creates a .env file with values containing `$` characters and verifies
    /// they are preserved literally.
    #[test]
    fn test_dotenvy_no_variable_substitution() {
        // Create a temp directory for the test .env file
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let env_file = temp_dir.path().join(".env.test");

        // Write a .env file with values that would be broken by variable substitution
        let env_content = r"# Test values with $ characters that should NOT be substituted
TEST_PATCH_API_KEY=sk-abc$123def
TEST_PATCH_PASSWORD=p@ss$word$123
TEST_PATCH_DOLLAR_SIGN=value_with_$_in_middle
TEST_PATCH_CURLY_BRACES=value_${NOT_A_VAR}_here
TEST_PATCH_MULTIPLE_DOLLARS=$$double$$dollars$$
";

        let mut file = std::fs::File::create(&env_file).expect("Failed to create test .env file");
        file.write_all(env_content.as_bytes())
            .expect("Failed to write test .env file");
        drop(file);

        // Load the .env file using dotenvy
        dotenvy::from_path(&env_file).expect("Failed to load .env file");

        // Verify each value is preserved literally (no variable substitution)
        let test_cases = [
            ("TEST_PATCH_API_KEY", "sk-abc$123def"),
            ("TEST_PATCH_PASSWORD", "p@ss$word$123"),
            ("TEST_PATCH_DOLLAR_SIGN", "value_with_$_in_middle"),
            ("TEST_PATCH_CURLY_BRACES", "value_${NOT_A_VAR}_here"),
            ("TEST_PATCH_MULTIPLE_DOLLARS", "$$double$$dollars$$"),
        ];

        for (key, expected_value) in test_cases {
            let actual_value =
                std::env::var(key).unwrap_or_else(|e| panic!("Failed to get {key}: {e}"));

            assert_eq!(
                actual_value, expected_value,
                "Dotenvy variable substitution FAILED for {key}: expected '{expected_value}', got '{actual_value}'. \
                 This indicates the dotenvy patch may be missing. \
                 See: https://github.com/allan2/dotenvy/issues/113"
            );

            // Clean up - remove_var is unsafe in Rust 2024 edition because modifying
            // environment variables while other threads may be reading them is UB.
            // SAFETY: This is a single-threaded unit test, no other threads are accessing env vars.
            unsafe {
                std::env::remove_var(key);
            }
        }
    }
}
