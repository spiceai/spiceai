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

//! Live `HashiCorp` Vault integration tests.
//!
//! These tests require a reachable Vault server with a pre-provisioned KV
//! v2 secret and credentials that can read it. The tests are skipped (with
//! a printed message) when the required environment variables are not set,
//! so they are safe to run in CI environments without Vault.
//!
//! ## Local quick start
//!
//! ```sh
//! vault server -dev -dev-root-token-id=root &
//! export VAULT_ADDR=http://127.0.0.1:8200
//! export VAULT_TOKEN=root
//! vault kv put secret/spice-test foo=bar baz=qux
//!
//! export SPICE_TEST_VAULT_ADDR=$VAULT_ADDR
//! export SPICE_TEST_VAULT_TOKEN=$VAULT_TOKEN
//! export SPICE_TEST_VAULT_PATH=spice-test
//! export SPICE_TEST_VAULT_KEY=foo
//! export SPICE_TEST_VAULT_VALUE=bar
//!
//! cargo test -p runtime-secrets --test hashicorp_vault_live -- --ignored
//! ```
//!
//! ## Required environment variables
//!
//! - `SPICE_TEST_VAULT_ADDR`: Vault address, e.g. `http://127.0.0.1:8200`.
//! - `SPICE_TEST_VAULT_TOKEN`: client token to authenticate with.
//! - `SPICE_TEST_VAULT_PATH`: the KV path under the mount (default mount
//!   is `secret`), e.g. `spice-test`.
//! - `SPICE_TEST_VAULT_KEY`: a key inside that secret to read.
//! - `SPICE_TEST_VAULT_VALUE`: the expected value for that key.
//!
//! Optional:
//! - `SPICE_TEST_VAULT_MOUNT` (default `secret`)
//! - `SPICE_TEST_VAULT_KV_VERSION` (default `v2`)

#![cfg(feature = "hashicorp_vault")]

use std::collections::HashMap;

use runtime_secrets::SecretStore;
use runtime_secrets::stores::hashicorp_vault::{HashicorpVault, HashicorpVaultConfig};
use secrecy::ExposeSecret;

fn require_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn build_params(
    address: &str,
    token: &str,
    mount: &str,
    kv_version: &str,
) -> HashMap<String, String> {
    let mut p = HashMap::new();
    p.insert("hashicorp_vault_address".to_string(), address.to_string());
    p.insert("hashicorp_vault_token".to_string(), token.to_string());
    p.insert("hashicorp_vault_mount".to_string(), mount.to_string());
    p.insert(
        "hashicorp_vault_kv_version".to_string(),
        kv_version.to_string(),
    );
    p
}

#[tokio::test]
#[ignore = "requires a live Vault server; set SPICE_TEST_VAULT_* env vars to enable"]
async fn vault_token_auth_reads_kv_value() {
    let Some(address) = require_env("SPICE_TEST_VAULT_ADDR") else {
        eprintln!("skipping: SPICE_TEST_VAULT_ADDR not set");
        return;
    };
    let Some(token) = require_env("SPICE_TEST_VAULT_TOKEN") else {
        eprintln!("skipping: SPICE_TEST_VAULT_TOKEN not set");
        return;
    };
    let Some(path) = require_env("SPICE_TEST_VAULT_PATH") else {
        eprintln!("skipping: SPICE_TEST_VAULT_PATH not set");
        return;
    };
    let Some(key) = require_env("SPICE_TEST_VAULT_KEY") else {
        eprintln!("skipping: SPICE_TEST_VAULT_KEY not set");
        return;
    };
    let Some(expected) = require_env("SPICE_TEST_VAULT_VALUE") else {
        eprintln!("skipping: SPICE_TEST_VAULT_VALUE not set");
        return;
    };
    let mount = require_env("SPICE_TEST_VAULT_MOUNT").unwrap_or_else(|| "secret".to_string());
    let kv_version = require_env("SPICE_TEST_VAULT_KV_VERSION").unwrap_or_else(|| "v2".to_string());

    let params = build_params(&address, &token, &mount, &kv_version);
    let cfg = HashicorpVaultConfig::from_params(path, &params)
        .map_err(|e| e.to_string())
        .expect("from_params");
    let vault = HashicorpVault::from_config(cfg)
        .map_err(|e| e.to_string())
        .expect("from_config");

    vault
        .init()
        .await
        .map_err(|e| e.to_string())
        .expect("init reaches sys/health");

    let value = vault
        .get_secret(&key)
        .await
        .map_err(|e| e.to_string())
        .expect("get_secret succeeds")
        .expect("key exists in payload");
    assert_eq!(value.expose_secret(), expected);

    // Second lookup hits the in-process payload cache; the value must
    // still match.
    let value2 = vault
        .get_secret(&key)
        .await
        .map_err(|e| e.to_string())
        .expect("cached get_secret succeeds")
        .expect("cached key exists");
    assert_eq!(value2.expose_secret(), expected);

    // A missing key resolves to None rather than an error so callers can
    // chain stores by precedence.
    let missing = vault
        .get_secret("definitely_not_a_real_key_zzz")
        .await
        .map_err(|e| e.to_string())
        .expect("get_secret on missing key returns Ok(None)");
    assert!(missing.is_none());
}
