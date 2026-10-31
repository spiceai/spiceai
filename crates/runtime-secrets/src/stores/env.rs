/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Environment-variable secret store.
//!
//! # Precedence
//!
//! Keys are tried as `SPICE_{key}`, `{key}`, `SPICE_{KEY}`, then `{KEY}`.
//!
//! * **Default store** (no `file_path`): the process environment wins, then
//!   `.env.local`, then `.env`. That is the dotenv convention: a variable
//!   already in the environment is not overwritten by a file.
//! * **`file_path` store**: only that file is loaded. Its values win over the
//!   process environment. Project `.env` / `.env.local` are not read.
//!
//! When several secret stores are configured, the store defined last in the
//! spicepod wins for `${ secrets:KEY }` (the registry is searched last-to-first).
//!
//! # Process environment
//!
//! `Secrets::load_from` always exports project `.env` / `.env.local` into the
//! process environment without overwrite **before** any store initializes, so
//! libraries that read `std::env` directly (the AWS SDK default credential
//! chain, `AWS_REGION`, `GOOGLE_APPLICATION_CREDENTIALS`) keep working even
//! when `env` is not first — or is absent. A registered default env store
//! (no `file_path`) also exports the same way. A `file_path` store never
//! exports its own file. The bootstrap store used to expand other stores'
//! `params:` does not export again; the prelude already did.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use runtime_parameters_derive::TypedParams;
use secrecy::SecretString;

use crate::SecretStore;

const ENV_SECRET_PREFIX: &str = "SPICE_";

/// Typed `params:` for the `env` secret store.
#[derive(Debug, TypedParams)]
#[params(prefix = "env", deny_unknown)]
pub struct EnvParams {
    /// Path to a `.env` file to load secrets from. Defaults to `.env`.
    #[param(runtime)]
    pub file_path: Option<PathBuf>,
}

impl EnvParams {
    /// Builds the resolved [`EnvConfig`].
    #[must_use]
    pub fn into_config(self) -> EnvConfig {
        EnvConfig {
            file_path: self.file_path,
        }
    }
}

/// Resolved configuration for the `env` secret store.
#[derive(Debug, Clone, Default)]
pub struct EnvConfig {
    pub file_path: Option<PathBuf>,
}

pub struct EnvSecretStoreBuilder {
    path: Option<PathBuf>,
    export_to_process_env: bool,
}

impl Default for EnvSecretStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl EnvSecretStoreBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            path: None,
            export_to_process_env: false,
        }
    }

    #[must_use]
    pub fn with_path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    /// Export file-loaded values into the process environment without
    /// overwriting variables that are already set. Used only for the
    /// registered default env store (no `file_path`).
    #[must_use]
    pub fn export_to_process_env(mut self) -> Self {
        self.export_to_process_env = true;
        self
    }

    #[must_use]
    pub fn build(self) -> EnvSecretStore {
        let values = load_values(self.path.as_deref());
        if self.export_to_process_env {
            export_without_overwrite(&values);
        }
        EnvSecretStore {
            path: self.path,
            values,
        }
    }
}

/// An env secret store with a private key/value map.
///
/// See the module docs for precedence and process-environment rules.
pub struct EnvSecretStore {
    path: Option<PathBuf>,
    values: HashMap<String, String>,
}

impl EnvSecretStore {
    fn key_variants(key: &str) -> [String; 4] {
        let uppercase_key = key.to_uppercase();
        [
            format!("{ENV_SECRET_PREFIX}{key}"),
            key.to_string(),
            format!("{ENV_SECRET_PREFIX}{uppercase_key}"),
            uppercase_key,
        ]
    }

    fn lookup_map(&self, variants: &[String]) -> Option<SecretString> {
        variants.iter().find_map(|variant| {
            self.values
                .get(variant)
                .map(|value| SecretString::from(value.clone()))
        })
    }
}

/// Load this store's private map.
///
/// A `file_path` store reads only that file. The default store reads
/// `.env.local` then `.env` (first key wins).
fn load_values(path: Option<&Path>) -> HashMap<String, String> {
    let mut values = HashMap::new();
    if let Some(path) = path {
        load_path_into_map(path, &mut values);
        return values;
    }
    // `.env.local` is loaded before `.env` so that its values take priority:
    // `collect_iter` does not overwrite keys already present.
    load_filename_into_map(".env.local", &mut values);
    load_filename_into_map(".env", &mut values);
    values
}

fn load_path_into_map(path: &Path, values: &mut HashMap<String, String>) {
    match dotenv::from_path_iter(path) {
        Ok(iter) => collect_iter(iter, &path.display().to_string(), values),
        Err(err) => {
            tracing::debug!("Error opening path {}: {err}", path.display());
        }
    }
}

fn load_filename_into_map(filename: &str, values: &mut HashMap<String, String>) {
    match dotenv::from_filename_iter(filename) {
        Ok(iter) => collect_iter(iter, filename, values),
        Err(err) => {
            tracing::debug!("{filename}: {err}");
        }
    }
}

/// Iterates parsed entries from a [`dotenv`] iterator into `values`.
/// Keys already present are preserved, so earlier-loaded files win.
/// Malformed lines are logged with their line number and key so users can
/// fix them; the rest of the line can hold a secret value and is never
/// logged. I/O-level errors (e.g. missing file) are reported at debug
/// level because the file is optional.
fn collect_iter<I>(iter: I, label: &str, values: &mut HashMap<String, String>)
where
    I: Iterator<Item = Result<(String, String), dotenv::Error>>,
{
    for item in iter {
        match item {
            Ok((key, val)) => {
                values.entry(key).or_insert(val);
            }
            Err(dotenv::Error::LineParse(line_num, content)) => {
                tracing::warn!("{}", malformed_line_message(label, line_num, &content));
            }
            Err(err) => {
                tracing::debug!("{label}: {err}");
            }
        }
    }
}

/// Non-overwriting export of project `.env` / `.env.local` into the process
/// environment. Called once at the start of [`crate::Secrets::load_from`] so
/// stores that read `std::env` during init see those values even when no
/// `env` store is registered first — or at all.
pub(crate) fn export_default_dotenv_without_overwrite() {
    let values = load_values(None);
    export_without_overwrite(&values);
}

/// # Safety
///
/// `std::env::set_var` is unsafe in the 2024 edition because mutating the
/// environment while another thread reads it is undefined behavior. The store
/// is constructed during runtime startup, before request-serving workloads
/// run, which keeps the writes confined to initialization; do not call this
/// from steady-state code paths.
fn export_without_overwrite(values: &HashMap<String, String>) {
    for (key, val) in values {
        if std::env::var_os(key).is_none() {
            unsafe {
                std::env::set_var(key, val);
            }
        }
    }
}

/// Names the key of a malformed `KEY=value` line, never its value. Text
/// before `=` that is not a valid key (for example a line inside a multi-line
/// value) is not logged.
fn malformed_line_message(label: &str, line_num: usize, content: &str) -> String {
    let key = content.split_once('=').map(|(key, _)| {
        let key = key.trim();
        key.strip_prefix("export ").unwrap_or(key).trim()
    });
    match key {
        Some(key) if is_dotenv_key(key) => {
            format!("{label}: line {line_num} (key `{key}`) is malformed and was skipped")
        }
        _ => format!("{label}: line {line_num} is malformed and was skipped"),
    }
}

/// The key grammar of the `.env` parser: an ASCII letter or `_`, then ASCII
/// alphanumerics, `_` or `.`.
fn is_dotenv_key(key: &str) -> bool {
    let mut chars = key.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

/// Iterates parsed entries from a [`dotenv`] iterator, setting each valid
/// key-value pair into the environment. Variables already present in the
/// environment are preserved, so earlier-loaded files take priority over later
/// ones. Malformed lines are logged with their line number and key so users can
/// fix them. I/O-level errors (e.g. missing file) are reported at debug
/// level because the file is optional.
///
/// # Safety
///
/// See [`export_without_overwrite`].
#[cfg(test)]
fn load_iter<I>(iter: I, label: &str)
where
    I: Iterator<Item = Result<(String, String), dotenv::Error>>,
{
    let mut values = HashMap::new();
    collect_iter(iter, label, &mut values);
    export_without_overwrite(&values);
}

fn lookup_process_env(variants: &[String]) -> crate::AnyErrorResult<Option<SecretString>> {
    variants
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

#[async_trait]
impl SecretStore for EnvSecretStore {
    /// The key for `std::env::var` is case-sensitive. Calling `std::env::var("my_key")` is distinct from `std::env::var("MY_KEY")`.
    ///
    /// However, the convention is to use uppercase for environment variables - so to make the experience
    /// consistent across secret stores that don't have this convention we will search for both original and uppercased keys.
    async fn get_secret(&self, key: &str) -> crate::AnyErrorResult<Option<SecretString>> {
        let variants = Self::key_variants(key);
        if self.path.is_some() {
            if let Some(value) = self.lookup_map(&variants) {
                return Ok(Some(value));
            }
            return lookup_process_env(&variants);
        }
        if let Some(value) = lookup_process_env(&variants)? {
            return Ok(Some(value));
        }
        Ok(self.lookup_map(&variants))
    }
}

/// Serializes tests that mutate the process environment or the process cwd.
///
/// Tokio mutex so async tests can hold the guard across `.await` without
/// tripping `await_holding_lock`. Shared with `runtime-secrets` lib tests.
#[cfg(test)]
pub(crate) static ENV_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use secrecy::ExposeSecret;

    const C1_KEY: &str = "C1_PREC_KEY";

    #[test]
    fn a_malformed_line_is_reported_by_key_without_its_value() {
        for line in ["DB_PASS=O'Brien1", "export DB_PASS=correct horse"] {
            let message = malformed_line_message(".env", 3, line);
            assert_eq!(
                message,
                ".env: line 3 (key `DB_PASS`) is malformed and was skipped"
            );
        }
        for line in ["hunter2", r#"  \"private_key\": \"abc+/def==\""#] {
            assert_eq!(
                malformed_line_message(".env", 4, line),
                ".env: line 4 is malformed and was skipped"
            );
        }
    }

    fn write_env_file(path: &Path, contents: &str) {
        let mut file = std::fs::File::create(path).expect("Failed to create test .env file");
        file.write_all(contents.as_bytes())
            .expect("Failed to write test .env file");
    }

    /// Restores the process cwd when dropped so a failing assertion cannot
    /// leave later tests in a temp directory.
    struct CwdGuard {
        prev: PathBuf,
    }

    impl CwdGuard {
        fn enter(path: &Path) -> Self {
            let prev = std::env::current_dir().expect("cwd");
            std::env::set_current_dir(path).expect("chdir");
            Self { prev }
        }
    }

    impl Drop for CwdGuard {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.prev);
        }
    }

    /// Values containing `$` characters must be preserved literally.
    ///
    /// `dotenv` performs no shell-style variable substitution — with
    /// substitution (as in upstream `dotenvy`, see
    /// <https://github.com/allan2/dotenvy/issues/113>), values like
    /// `API_KEY=sk-abc$123` would be parsed as variable references and come
    /// back as `sk-abc` (with `$123` treated as an undefined variable).
    #[test]
    fn test_no_variable_substitution() {
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

        write_env_file(&env_file, env_content);

        let entries: std::collections::HashMap<String, String> = dotenv::from_path_iter(&env_file)
            .expect("Failed to open .env file")
            .collect::<Result<_, _>>()
            .expect("Failed to parse .env file");

        // Verify each value is preserved literally (no variable substitution)
        let test_cases = [
            ("TEST_PATCH_API_KEY", "sk-abc$123def"),
            ("TEST_PATCH_PASSWORD", "p@ss$word$123"),
            ("TEST_PATCH_DOLLAR_SIGN", "value_with_$_in_middle"),
            ("TEST_PATCH_CURLY_BRACES", "value_${NOT_A_VAR}_here"),
            ("TEST_PATCH_MULTIPLE_DOLLARS", "$$double$$dollars$$"),
        ];

        for (key, expected_value) in test_cases {
            assert_eq!(
                entries.get(key).map(String::as_str),
                Some(expected_value),
                "Variable substitution must not be applied to {key}"
            );
        }
    }

    /// Valid entries that appear after a malformed line must still be loaded.
    /// Before the switch to `from_path_iter`, loading stopped at the first
    /// parse error and discarded all subsequent lines.
    #[tokio::test]
    async fn test_malformed_line_does_not_block_valid_entries() {
        let _guard = ENV_TEST_MUTEX.lock().await;
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let env_file = temp_dir.path().join(".env.malformed");

        let env_content = r"MALFORMED_VALID_FIRST=first_value
THIS LINE HAS NO EQUALS SIGN
MALFORMED_VALID_SECOND=second_value
";
        write_env_file(&env_file, env_content);

        let iter = dotenv::from_path_iter(&env_file).expect("Failed to open .env file");
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

    /// Variables already present in the environment must not be overwritten by
    /// a `.env` file: real environment variables take priority, and `.env.local`
    /// (loaded first) takes priority over `.env`.
    #[tokio::test]
    async fn test_load_iter_preserves_existing_vars() {
        let _guard = ENV_TEST_MUTEX.lock().await;
        let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
        let env_file = temp_dir.path().join(".env.preserve");

        write_env_file(
            &env_file,
            "PRESERVE_EXISTING=from_file\nPRESERVE_NEW=from_file\n",
        );

        // SAFETY: unique variable names, mutated only by this test.
        unsafe {
            std::env::set_var("PRESERVE_EXISTING", "from_env");
        }

        let iter = dotenv::from_path_iter(&env_file).expect("Failed to open .env file");
        super::load_iter(iter, ".env.preserve");

        assert_eq!(
            std::env::var("PRESERVE_EXISTING").as_deref(),
            Ok("from_env"),
            "existing environment variables must not be overwritten"
        );
        assert_eq!(
            std::env::var("PRESERVE_NEW").as_deref(),
            Ok("from_file"),
            "new variables must be loaded from the file"
        );

        // Clean up
        unsafe {
            std::env::remove_var("PRESERVE_EXISTING");
            std::env::remove_var("PRESERVE_NEW");
        }
    }

    #[tokio::test]
    async fn file_path_store_returns_file_value_not_process_or_dotenv() {
        let _guard = ENV_TEST_MUTEX.lock().await;
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let custom = temp_dir.path().join("custom.env");
        let dotenv_local = temp_dir.path().join(".env.local");
        write_env_file(&custom, &format!("{C1_KEY}=from_custom\n"));
        write_env_file(&dotenv_local, &format!("{C1_KEY}=from_dotenv_local\n"));

        unsafe {
            std::env::set_var(C1_KEY, "from_process");
        }

        let store = EnvSecretStoreBuilder::new().with_path(custom).build();
        let value = store
            .get_secret(C1_KEY)
            .await
            .expect("lookup")
            .expect("present");
        assert_eq!(
            value.expose_secret(),
            "from_custom",
            "a file_path store must return its file value, not the process env or .env.local"
        );

        unsafe {
            std::env::remove_var(C1_KEY);
        }
    }

    #[tokio::test]
    async fn file_path_store_does_not_export_to_process_env() {
        let _guard = ENV_TEST_MUTEX.lock().await;
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let custom = temp_dir.path().join("custom.env");
        write_env_file(&custom, "C1_NO_EXPORT_KEY=from_custom\n");

        unsafe {
            std::env::remove_var("C1_NO_EXPORT_KEY");
        }

        let _store = EnvSecretStoreBuilder::new().with_path(custom).build();
        assert!(
            std::env::var_os("C1_NO_EXPORT_KEY").is_none(),
            "a file_path store must not export into the process environment"
        );
    }

    #[tokio::test]
    async fn default_store_without_export_does_not_pollute_process_env() {
        let _guard = ENV_TEST_MUTEX.lock().await;
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let _cwd = CwdGuard::enter(temp_dir.path());
        write_env_file(
            &temp_dir.path().join(".env.local"),
            "C1_BOOTSTRAP_KEY=from_dotenv_local\n",
        );

        unsafe {
            std::env::remove_var("C1_BOOTSTRAP_KEY");
        }

        let store = EnvSecretStoreBuilder::new().build();
        let value = store
            .get_secret("C1_BOOTSTRAP_KEY")
            .await
            .expect("lookup")
            .expect("present in the store map");
        assert_eq!(value.expose_secret(), "from_dotenv_local");
        assert!(
            std::env::var_os("C1_BOOTSTRAP_KEY").is_none(),
            "bootstrap / non-exporting default store must not write .env files into process env"
        );
    }

    #[tokio::test]
    async fn default_store_prefers_process_env_over_dotenv() {
        let _guard = ENV_TEST_MUTEX.lock().await;
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let _cwd = CwdGuard::enter(temp_dir.path());
        write_env_file(
            &temp_dir.path().join(".env.local"),
            &format!("{C1_KEY}=from_dotenv_local\n"),
        );

        unsafe {
            std::env::set_var(C1_KEY, "from_process");
        }

        let store = EnvSecretStoreBuilder::new().build();
        let value = store
            .get_secret(C1_KEY)
            .await
            .expect("lookup")
            .expect("present");
        assert_eq!(
            value.expose_secret(),
            "from_process",
            "the default env store must let the process environment beat .env files"
        );

        unsafe {
            std::env::remove_var(C1_KEY);
        }
    }

    #[tokio::test]
    async fn default_store_prefers_dotenv_local_over_dotenv() {
        let _guard = ENV_TEST_MUTEX.lock().await;
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let _cwd = CwdGuard::enter(temp_dir.path());
        write_env_file(
            &temp_dir.path().join(".env.local"),
            &format!("{C1_KEY}=from_dotenv_local\n"),
        );
        write_env_file(
            &temp_dir.path().join(".env"),
            &format!("{C1_KEY}=from_dotenv\n"),
        );

        unsafe {
            std::env::remove_var(C1_KEY);
        }

        let store = EnvSecretStoreBuilder::new().build();
        let value = store
            .get_secret(C1_KEY)
            .await
            .expect("lookup")
            .expect("present");
        assert_eq!(
            value.expose_secret(),
            "from_dotenv_local",
            ".env.local must beat .env when the process environment does not define the key"
        );
    }

    #[tokio::test]
    async fn file_path_store_does_not_load_project_dotenv_files() {
        let _guard = ENV_TEST_MUTEX.lock().await;
        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let _cwd = CwdGuard::enter(temp_dir.path());
        write_env_file(
            &temp_dir.path().join(".env.local"),
            "C1_ONLY_DOTENV=from_dotenv_local\n",
        );
        let custom = temp_dir.path().join("custom.env");
        write_env_file(&custom, "C1_ONLY_CUSTOM=from_custom\n");

        unsafe {
            std::env::remove_var("C1_ONLY_DOTENV");
            std::env::remove_var("C1_ONLY_CUSTOM");
        }

        let store = EnvSecretStoreBuilder::new().with_path(custom).build();
        assert!(
            store
                .get_secret("C1_ONLY_DOTENV")
                .await
                .expect("lookup")
                .is_none(),
            "a file_path store must not read project .env / .env.local"
        );
        let custom_value = store
            .get_secret("C1_ONLY_CUSTOM")
            .await
            .expect("lookup")
            .expect("present");
        assert_eq!(custom_value.expose_secret(), "from_custom");
    }
}
