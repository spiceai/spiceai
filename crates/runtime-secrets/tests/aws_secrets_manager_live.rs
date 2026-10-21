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

//! Live AWS integration tests for the AWS Secrets Manager secret store.
//!
//! These tests require a real AWS account with `secretsmanager:GetSecretValue`
//! permission on a pre-provisioned test secret. The tests are skipped (with a
//! message) when the required environment variables are not set, so they are
//! safe to run in CI environments that do not have AWS credentials.
//!
//! ## Required environment variables
//!
//! - `SPICE_TEST_AWS_SECRET_NAME`: name or ARN of an AWS secret whose value
//!   is a JSON object containing at least one key/value pair.
//! - `SPICE_TEST_AWS_SECRET_KEY`: a key that exists in the secret payload.
//! - `SPICE_TEST_AWS_SECRET_VALUE`: the expected value for that key.
//!
//! Standard AWS credential resolution applies: `AWS_PROFILE`,
//! `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, IMDS, etc. Set `AWS_REGION`
//! if the secret is not in the SDK's default region.
//!
//! ## Example
//!
//! ```sh
//! aws secretsmanager create-secret \
//!     --name spice-integration-test \
//!     --secret-string '{"api_key":"hello-world"}'
//!
//! export SPICE_TEST_AWS_SECRET_NAME=spice-integration-test
//! export SPICE_TEST_AWS_SECRET_KEY=api_key
//! export SPICE_TEST_AWS_SECRET_VALUE=hello-world
//!
//! cargo test -p runtime-secrets --features aws-secrets-manager \
//!     --test aws_secrets_manager_live -- --nocapture
//! ```

#![cfg(feature = "aws-secrets-manager")]

use std::time::Duration;

use runtime_secrets::stores::aws_secrets_manager::AwsSecretsManager;
use runtime_secrets::{ExposeSecret, SecretStore};

/// Reads a required environment variable. Returns `None` (with a logged
/// message) if the variable is not set, signalling the test should be
/// skipped rather than fail.
fn env_or_skip(var: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => {
            eprintln!("Skipping live AWS Secrets Manager test: {var} is not set");
            None
        }
    }
}

struct TestConfig {
    secret_name: String,
    key: String,
    expected_value: String,
}

fn load_config() -> Option<TestConfig> {
    Some(TestConfig {
        secret_name: env_or_skip("SPICE_TEST_AWS_SECRET_NAME")?,
        key: env_or_skip("SPICE_TEST_AWS_SECRET_KEY")?,
        expected_value: env_or_skip("SPICE_TEST_AWS_SECRET_VALUE")?,
    })
}

#[tokio::test]
async fn live_init_verifies_credentials() {
    let Some(config) = load_config() else {
        return;
    };

    let store = AwsSecretsManager::new(&config.secret_name).expect("valid secret name");
    store
        .init()
        .await
        .expect("STS get-caller-identity must succeed with valid AWS credentials");
}

#[tokio::test]
async fn live_get_secret_returns_expected_value() {
    let Some(config) = load_config() else {
        return;
    };

    let store = AwsSecretsManager::new(&config.secret_name).expect("valid secret name");

    let secret = store
        .get_secret(&config.key)
        .await
        .expect("get_secret must succeed against a real AWS secret")
        .expect("secret key must be present in the payload");

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

    let store = AwsSecretsManager::new(&config.secret_name).expect("valid secret name");

    // Use a key that is overwhelmingly unlikely to exist in the test secret.
    let key = format!("__spice_nonexistent_key_{}__", rand::random::<u64>());
    let result = store
        .get_secret(&key)
        .await
        .expect("get_secret must succeed even for an unknown key");
    assert!(
        result.is_none(),
        "lookup of an unknown key {key} returned a value"
    );
}

#[tokio::test]
async fn live_missing_secret_returns_none_without_error() {
    if env_or_skip("SPICE_TEST_AWS_SECRET_NAME").is_none() {
        // Re-uses the same skip signal as the other live tests so all live
        // tests are gated by the same env vars.
        return;
    }

    // A name that is overwhelmingly unlikely to exist in the test account.
    let nonexistent = format!(
        "spice-integration-test-nonexistent-{}",
        rand::random::<u64>()
    );
    let store = AwsSecretsManager::new(&nonexistent).expect("valid secret name");

    let result = store
        .get_secret("anything")
        .await
        .expect("ResourceNotFoundException must surface as Ok(None), not as an error");
    assert!(
        result.is_none(),
        "missing secret {nonexistent} resolved to a value"
    );
}

#[tokio::test]
async fn live_repeated_lookups_are_served_from_cache() {
    let Some(config) = load_config() else {
        return;
    };

    let store = AwsSecretsManager::new(&config.secret_name).expect("valid secret name");

    let first = std::time::Instant::now();
    let v1 = store
        .get_secret(&config.key)
        .await
        .expect("first lookup ok")
        .expect("present");
    let first_elapsed = first.elapsed();

    let second = std::time::Instant::now();
    let v2 = store
        .get_secret(&config.key)
        .await
        .expect("second lookup ok")
        .expect("present");
    let second_elapsed = second.elapsed();

    assert_eq!(v1.expose_secret(), v2.expose_secret());

    // The second lookup should be served from the in-process cache and be
    // dramatically faster than the network round-trip. A loose 10x margin
    // avoids flakiness on slow CI hosts.
    let upper_bound = first_elapsed.max(Duration::from_millis(10)) / 10;
    assert!(
        second_elapsed <= upper_bound.max(Duration::from_millis(5)),
        "expected cached lookup to be faster than {upper_bound:?}; got {second_elapsed:?}"
    );
}
