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

//! Live Azure integration tests for the Azure Key Vault secret store.
//!
//! These tests require a real Azure Key Vault with a pre-provisioned test
//! secret and a principal that holds `secrets/get` permission on it. The
//! tests are skipped (with a message) when the required environment
//! variables are not set, so they are safe to run in CI environments that
//! do not have Azure credentials.
//!
//! ## Required environment variables
//!
//! - `SPICE_TEST_AZURE_KEYVAULT`: the vault name or full URL, e.g.
//!   `my-vault` or `https://my-vault.vault.azure.net/`.
//! - `SPICE_TEST_AZURE_SECRET_KEY`: the logical (Spice-facing) key name;
//!   this is translated to Key Vault's name rules (`_` → `-`) internally,
//!   so `openai_api_key` resolves to the Key Vault secret `openai-api-key`.
//! - `SPICE_TEST_AZURE_SECRET_VALUE`: the expected value for that secret.
//!
//! ## Authentication
//!
//! Credentials follow the store's usual resolution order:
//! - If `AZURE_CLIENT_SECRET`/`AZURE_CLIENT_ID`/`AZURE_TENANT_ID` are set,
//!   you can drive `ClientSecretCredential` by also setting
//!   `SPICE_TEST_AZURE_USE_SP=1`.
//! - Otherwise, the store falls back to [`DeveloperToolsCredential`], which
//!   chains the Azure CLI (`az login`) and Azure Developer CLI.
//!
//! ## Example
//!
//! ```sh
//! az login
//! az keyvault secret set \
//!     --vault-name my-vault \
//!     --name openai-api-key \
//!     --value hello-world
//!
//! export SPICE_TEST_AZURE_KEYVAULT=my-vault
//! export SPICE_TEST_AZURE_SECRET_KEY=openai_api_key
//! export SPICE_TEST_AZURE_SECRET_VALUE=hello-world
//!
//! cargo test -p runtime-secrets --features azure-keyvault \
//!     --test azure_keyvault_live -- --nocapture
//! ```

#![cfg(feature = "azure-keyvault")]

use runtime_secrets::stores::azure_keyvault::{AuthMethod, AzureKeyVault, AzureKeyVaultConfig};
use runtime_secrets::{ExposeSecret, SecretStore};
use secrecy::SecretString;

fn env_or_skip(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("Skipping live Azure Key Vault test: {var} is not set");
            None
        }
    }
}

struct TestConfig {
    vault: String,
    key: String,
    expected_value: String,
}

fn load_config() -> Option<TestConfig> {
    Some(TestConfig {
        vault: env_or_skip("SPICE_TEST_AZURE_KEYVAULT")?,
        key: env_or_skip("SPICE_TEST_AZURE_SECRET_KEY")?,
        expected_value: env_or_skip("SPICE_TEST_AZURE_SECRET_VALUE")?,
    })
}

/// Returns a fresh store configured from the environment. Honors an explicit
/// `SPICE_TEST_AZURE_USE_SP=1` flag that forces `ClientSecretCredential` from
/// the ambient AZURE_* env vars; otherwise falls back to the default chain.
fn build_store(vault: &str) -> AzureKeyVault {
    let cfg = if std::env::var("SPICE_TEST_AZURE_USE_SP").ok().as_deref() == Some("1") {
        AzureKeyVaultConfig {
            vault: vault.to_string(),
            auth_method: AuthMethod::ServicePrincipal,
            tenant_id: std::env::var("AZURE_TENANT_ID").ok(),
            client_id: std::env::var("AZURE_CLIENT_ID").ok(),
            client_secret: std::env::var("AZURE_CLIENT_SECRET")
                .ok()
                .map(SecretString::from),
            endpoint: None,
        }
    } else {
        AzureKeyVaultConfig {
            vault: vault.to_string(),
            auth_method: AuthMethod::Default,
            tenant_id: None,
            client_id: None,
            client_secret: None,
            endpoint: None,
        }
    };
    AzureKeyVault::from_config(cfg).expect("valid vault config")
}

#[tokio::test]
async fn live_init_verifies_credentials() {
    let Some(config) = load_config() else {
        return;
    };
    let store = build_store(&config.vault);
    store
        .init()
        .await
        .expect("init must succeed with valid Azure credentials and vault URL");
}

#[tokio::test]
async fn live_get_secret_returns_expected_value() {
    let Some(config) = load_config() else {
        return;
    };
    let store = build_store(&config.vault);

    let secret = store
        .get_secret(&config.key)
        .await
        .expect("get_secret must succeed against a real Key Vault secret")
        .expect("secret must be present");

    assert_eq!(
        secret.expose_secret(),
        config.expected_value,
        "value for key {} did not match expected",
        config.key,
    );
}

#[tokio::test]
async fn live_get_unknown_key_returns_none() {
    let Some(config) = load_config() else {
        return;
    };
    let store = build_store(&config.vault);

    let key = format!("spice-nonexistent-key-{}", rand::random::<u64>());
    let result = store
        .get_secret(&key)
        .await
        .expect("get_secret must succeed (as Ok(None)) even for an unknown key");
    assert!(
        result.is_none(),
        "lookup of an unknown key {key} returned a value"
    );
}

#[tokio::test]
async fn live_repeated_lookups_are_served_from_cache() {
    let Some(config) = load_config() else {
        return;
    };
    let store = build_store(&config.vault);

    // Deterministic cache-behavior assertion. The earlier implementation
    // compared wall-clock timings (second call ≥10× faster than first),
    // which flaked under network jitter on shared CI hosts. Instead, seed
    // the cache with one lookup, then assert the next lookup increments
    // the hit counter without incrementing the miss counter.
    let v1 = store
        .get_secret(&config.key)
        .await
        .expect("first lookup ok")
        .expect("present");

    let (hits_before, misses_before) = store.cache_stats();

    let v2 = store
        .get_secret(&config.key)
        .await
        .expect("second lookup ok")
        .expect("present");

    let (hits_after, misses_after) = store.cache_stats();

    assert_eq!(v1.expose_secret(), v2.expose_secret());
    assert_eq!(
        hits_after - hits_before,
        1,
        "second lookup should have been a cache hit (hits went {hits_before} → {hits_after})",
    );
    assert_eq!(
        misses_after - misses_before,
        0,
        "second lookup must not issue a network fetch (misses went {misses_before} → {misses_after})",
    );
}
