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

use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use runtime_parameter_spec::ParameterSpec;
use secrecy::SecretString;

use crate::SecretStore;

const ENV_SECRET_PREFIX: &str = "SPICE_";

/// Parameters accepted by the `env` secret store.
pub const PARAMETERS: &[ParameterSpec] = &[ParameterSpec::runtime("file_path")
    .description("Path to a `.env` file to load secrets from. Defaults to `.env`.")
    .examples(&[".env", "/etc/spice/secrets.env"])];

/// Resolved configuration for the `env` secret store.
#[derive(Debug, Clone, Default)]
pub struct EnvConfig {
    pub file_path: Option<String>,
}

impl EnvConfig {
    /// Builds an [`EnvConfig`] from a validated parameter map.
    #[must_use]
    pub fn from_params(params: &HashMap<String, String>) -> Self {
        Self {
            file_path: params.get("file_path").cloned(),
        }
    }
}

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
            if load_from_iter(path, dotenvy::from_path_iter(path)) {
                return;
            }
        }
        // `.env.local` is loaded before `.env` so that its values take priority:
        // dotenvy respects existing `std::env` vars and does not overwrite them,
        // so the first file loaded wins.
        let _ = dotenvy::from_filename_iter(".env.local").map(|iter| load_iter(iter, ".env.local"));
        let _ = dotenvy::from_filename_iter(".env").map(|iter| load_iter(iter, ".env"));
    }
}

/// Iterates parsed entries from a dotenvy iterator, setting each valid key-value
/// pair into the environment. Malformed lines are logged with their line number and
/// content so users can fix them. I/O-level errors (e.g. missing file) are reported
/// at debug level because the file is optional.
///
/// # Safety
///
/// `std::env::set_var` is called during store construction (single-threaded init)
/// before any other threads are spawned.
fn load_iter<I>(iter: I, label: &str)
where
    I: Iterator<Item = Result<(String, String), dotenvy::Error>>,
{
    for item in iter {
        match item {
            Ok((key, val)) => unsafe {
                std::env::set_var(key, val);
            },
            Err(dotenvy::Error::LineParse(line_num, content)) => {
                tracing::warn!(
                    "{label}: line {line_num} is malformed and was skipped: `{content}`"
                );
            }
            Err(err) => {
                tracing::debug!("{label}: {err}");
            }
        }
    }
}

/// Load entries from a `from_path_iter` / `from_filename_iter` result.
///
/// Returns `true` when the file was opened and iterated (even if some lines were
/// malformed). Returns `false` when the file could not be opened, so the caller
/// can fall back to the default `.env` / `.env.local` search.
fn load_from_iter<I, E>(path: &std::path::Path, result: Result<I, E>) -> bool
where
    I: Iterator<Item = Result<(String, String), dotenvy::Error>>,
    E: std::fmt::Display,
{
    match result {
        Ok(iter) => {
            load_iter(iter, &path.display().to_string());
            true
        }
        Err(err) => {
            tracing::debug!("Error opening path {}: {err}", path.display());
            false
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

    /// Valid entries that appear after a malformed line must still be loaded.
    /// Before the switch to `from_path_iter`, `dotenvy::from_path` stopped at
    /// the first parse error and discarded all subsequent lines.
    #[test]
    fn test_malformed_line_does_not_block_valid_entries() {
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let env_file = temp_dir.path().join(".env.malformed");

        let env_content = r"MALFORMED_VALID_FIRST=first_value
THIS LINE HAS NO EQUALS SIGN
MALFORMED_VALID_SECOND=second_value
";
        let mut file = std::fs::File::create(&env_file).expect("Failed to create test .env file");
        file.write_all(env_content.as_bytes())
            .expect("Failed to write test .env file");
        drop(file);

        let iter = dotenvy::from_path_iter(&env_file).expect("Failed to open .env file");
        super::load_iter(iter, ".env.malformed");

        assert_eq!(
            std::env::var("MALFORMED_VALID_FIRST").as_deref(),
            Ok("first_value"),
            "Valid entry before malformed line must be loaded"
        );
        assert_eq!(
            std::env::var("MALFORMED_VALID_SECOND").as_deref(),
            Ok("second_value"),
            "Valid entry after malformed line must still be loaded"
        );

        // Clean up
        unsafe {
            std::env::remove_var("MALFORMED_VALID_FIRST");
            std::env::remove_var("MALFORMED_VALID_SECOND");
        }
    }
}
