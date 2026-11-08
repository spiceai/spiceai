/*
Copyright 2025 The Spice.ai OSS Authors

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

// The credential tests need `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`.

use aws_sdk_credential_bridge::{S3CredentialProvider, get_or_init_sdk_config};
use object_store::CredentialProvider;
use reqsign_core::{Context, ProvideCredential};
use tempfile::TempDir;

#[tokio::test]
async fn s3_credential_provider_caches_calls_for_iceberg() {
    let (credential_provider, _) = S3CredentialProvider::from_env()
        .await
        .expect("To Create S3CredentialProvider");

    let first_credentials = credential_provider
        .provide_credential(&Context::new())
        .await
        .expect("To Fetch Credentials")
        .expect("To Find Valid Credentials");

    let second_credentials = credential_provider
        .provide_credential(&Context::new())
        .await
        .expect("To Fetch Credentials")
        .expect("To Find Valid Credentials");

    assert_eq!(
        first_credentials.access_key_id,
        second_credentials.access_key_id
    );
    assert_eq!(
        first_credentials.secret_access_key,
        second_credentials.secret_access_key
    );
    assert_eq!(
        first_credentials.session_token,
        second_credentials.session_token
    );
}

#[tokio::test]
async fn s3_credential_provider_caches_calls_for_object_store() {
    let (credential_provider, _) = S3CredentialProvider::from_env()
        .await
        .expect("To Create S3CredentialProvider");

    let first_credentials = credential_provider
        .get_credential()
        .await
        .expect("To Get Credentials");

    let second_credentials = credential_provider
        .get_credential()
        .await
        .expect("To Get Credentials");

    assert_eq!(first_credentials.key_id, second_credentials.key_id);
    assert_eq!(first_credentials.secret_key, second_credentials.secret_key);
    assert_eq!(first_credentials.token, second_credentials.token);
}

#[tokio::test]
async fn aws_sdk_config_allows_unauthenticated_access() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let credentials_file = temp_dir.path().join("fake_credentials");
    std::fs::write(&credentials_file, "").expect("write fake credentials file");
    let config_file = temp_dir.path().join("fake_config");
    std::fs::write(&config_file, "").expect("write fake config file");

    // Preserve existing environment variables so we can restore them after the test.
    let vars_to_clear = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_CONFIG_FILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_EC2_METADATA_DISABLED",
    ];

    let mut saved_vars = Vec::new();
    unsafe {
        for key in vars_to_clear {
            saved_vars.push((key, std::env::var(key).ok()));
            std::env::remove_var(key);
        }

        std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &credentials_file);
        std::env::set_var("AWS_CONFIG_FILE", &config_file);
        std::env::set_var("AWS_PROFILE", "spiceai-nonexistent-profile");
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }

    let config = get_or_init_sdk_config()
        .await
        .expect("AWS SDK initialization should not error for unauthenticated access");
    assert!(
        config.is_none(),
        "Expected AWS SDK config to be absent when no credentials are configured"
    );

    // Restore environment variables to avoid affecting other tests.
    unsafe {
        for (key, value) in saved_vars {
            if let Some(value) = value {
                std::env::set_var(key, value);
            } else {
                std::env::remove_var(key);
            }
        }
    }
}
