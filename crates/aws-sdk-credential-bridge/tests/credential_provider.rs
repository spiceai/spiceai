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

use aws_config::BehaviorVersion;

use aws_sdk_cognitoidentity as cognito_identity;
use aws_sdk_cognitoidentityprovider as cognito_idp;
use aws_sdk_cognitoidentityprovider::types::AuthFlowType;
use aws_sdk_credential_bridge::{S3CredentialProvider, get_or_init_sdk_config};
use iceberg::io::AwsCredentialLoad;
use object_store::CredentialProvider;
use std::io::Write;
use tempfile::{NamedTempFile, TempDir};

#[expect(clippy::expect_used)]
async fn setup(file: &mut NamedTempFile) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let client_id = std::env::var("AWS_S3_CLIENT_ID").expect("AWS_S3_CLIENT_ID must be set");
    let identity_pool_id =
        std::env::var("AWS_S3_IDENTITY_POOL_ID").expect("AWS_S3_IDENTITY_POOL_ID must be set");
    let username = std::env::var("AWS_S3_USERNAME").expect("AWS_S3_USERNAME must be set");
    let password = std::env::var("AWS_S3_PASSWORD").expect("AWS_S3_PASSWORD must be set");
    let cognito_idp_uri =
        std::env::var("AWS_COGNITO_IDP_URI").expect("AWS_COGNITO_IDP_URI must be set");

    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

    let cognito_idp_client = cognito_idp::Client::new(&config);
    let cognito_identity_client = cognito_identity::Client::new(&config);

    let auth_response = cognito_idp_client
        .initiate_auth()
        .auth_flow(AuthFlowType::UserPasswordAuth)
        .client_id(client_id)
        .auth_parameters("USERNAME", username)
        .auth_parameters("PASSWORD", password)
        .send()
        .await?;

    let id_token = auth_response
        .authentication_result()
        .as_ref()
        .and_then(|result| result.id_token())
        .ok_or("Failed to get ID token")?;

    let identity_id_response = cognito_identity_client
        .get_id()
        .identity_pool_id(identity_pool_id)
        .logins(&cognito_idp_uri, id_token)
        .send()
        .await?;

    let identity_id = identity_id_response
        .identity_id()
        .ok_or("Failed to get identity ID")?;

    let open_id_token_response = cognito_identity_client
        .get_open_id_token()
        .identity_id(identity_id)
        .logins(cognito_idp_uri, id_token)
        .send()
        .await?;

    let token = open_id_token_response
        .token()
        .ok_or("Failed to get OpenID token")?;

    writeln!(file, "{token}")?;

    unsafe {
        std::env::set_var(
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            file.path()
                .to_str()
                .ok_or("Failed to convert path to string")?,
        );
    }

    Ok(())
}

#[tokio::test]
async fn s3_credential_provider_caches_calls_for_iceberg() {
    let mut file = NamedTempFile::new().expect("To create temp file");
    setup(&mut file).await.expect("To setup properly");

    let (credential_provider, _) = S3CredentialProvider::from_env()
        .await
        .expect("To Create S3CredentialProvider");

    let client = reqwest::Client::new();

    let first_credentials = credential_provider
        .load_credential(client.clone())
        .await
        .expect("To Fetch Credentials")
        .expect("To Find Valid Credentials");

    let second_credentials = credential_provider
        .load_credential(client)
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
    let mut tempfile = NamedTempFile::new().expect("To create temp file");

    setup(&mut tempfile).await.expect("To setup properly");

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
