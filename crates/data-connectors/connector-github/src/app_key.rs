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

//! A GitHub App whose private key a key service holds: `github_private_key_kms` names an Azure
//! Key Vault key or an AWS KMS key, which signs the App JWT and never reveals the key, so no
//! private key is stored anywhere Spice reads configuration from.

use std::sync::Arc;

use aws_kms_signer::{AwsKmsSigner, KmsKey};
use azure_key_vault_signer::{
    AzureIdentityEnvironment, KeyVaultError, KeyVaultKey, KeyVaultSigner,
};
use snafu::prelude::*;
use token_provider::jwt_signer::JwtSigner;

const DOCS: &str =
    "https://spiceai.org/docs/components/data-connectors/github#github-app-installation";

/// The key service that holds a GitHub App's private key.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum KeyService {
    AzureKeyVault(KeyVaultKey),
    AwsKms(KmsKey),
}

/// A GitHub App installation whose private key `service` holds.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct KeyServiceApp {
    pub service: KeyService,
    pub client_id: String,
    pub installation_id: String,
}

/// A dataset's GitHub App params, as `github_private_key_kms` depends on them.
#[derive(Debug, Default)]
pub(crate) struct AppKeyParams<'a> {
    /// `github_private_key_kms`.
    pub private_key_kms: Option<&'a str>,
    /// Whether the Spicepod sets `github_private_key`.
    pub private_key_set: bool,
    /// Whether the Spicepod sets `github_token`.
    pub token_set: bool,
    /// `github_client_id`, from the Spicepod or a secret store.
    pub client_id: Option<&'a str>,
    /// `github_installation_id`, from the Spicepod or a secret store.
    pub installation_id: Option<&'a str>,
}

fn set(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// Why a GitHub App whose private key a key service holds cannot be set up.
#[derive(Debug, Snafu)]
pub(crate) enum AppKeyError {
    #[snafu(display(
        "Invalid value for parameter github_private_key_kms. A GitHub App's private key is either `github_private_key` or a key service's key in `github_private_key_kms`, and both are set. Remove one of them. See: {DOCS}"
    ))]
    BothPrivateKeys,

    #[snafu(display(
        "Invalid value for parameter github_token. A dataset authenticates with either `github_token` or a GitHub App, and both `github_token` and `github_private_key_kms` are set. Remove one of them. See: {DOCS}"
    ))]
    TokenAndApp,

    #[snafu(display(
        "Invalid value for parameter {param}. A GitHub App whose private key is in `github_private_key_kms` also needs `github_client_id` and `github_installation_id`, and `{param}` is not set. See: {DOCS}"
    ))]
    MissingAppParam { param: &'static str },

    #[snafu(display("Invalid value for parameter github_private_key_kms. {source} See: {DOCS}"))]
    InvalidKeyVaultKey { source: KeyVaultError },

    #[snafu(display("Invalid value for parameter github_private_key_kms. {source} See: {DOCS}"))]
    InvalidKmsKey { source: aws_kms_signer::Error },

    #[snafu(display(
        "Invalid value for parameter github_private_key_kms. `github_private_key_kms` names the key that holds the GitHub App's private key: an Azure Key Vault key URL, such as https://my-vault.vault.azure.net/keys/github-app, or an AWS KMS key or alias ARN, such as arn:aws:kms:us-east-1:111122223333:alias/github-app. Found '{value}'. See: {DOCS}"
    ))]
    UnrecognizedKey { value: String },

    /// The environment lacks the Azure identity the vault needs; retrying cannot fix it.
    #[snafu(display("{source}"))]
    IncompleteIdentity { source: KeyVaultError },

    /// The identity provider refused the identity, or could not be reached.
    #[snafu(display("{source}"))]
    IdentityUnavailable { source: KeyVaultError },
}

impl AppKeyError {
    /// Whether the error is a mistake in the configuration, which retrying cannot fix.
    pub(crate) fn is_configuration(&self) -> bool {
        !matches!(self, Self::IdentityUnavailable { .. })
    }
}

/// The App `params` describe when `github_private_key_kms` is set. Nothing here reaches a
/// network.
///
/// A `github_token` or `github_private_key` that a secret store supplies unasked is not counted:
/// the key service the Spicepod names takes precedence over it.
pub(crate) fn resolve(params: &AppKeyParams<'_>) -> Result<Option<KeyServiceApp>, AppKeyError> {
    let Some(value) = set(params.private_key_kms) else {
        return Ok(None);
    };
    ensure!(!params.private_key_set, BothPrivateKeysSnafu);
    ensure!(!params.token_set, TokenAndAppSnafu);
    let client_id = set(params.client_id).context(MissingAppParamSnafu {
        param: "github_client_id",
    })?;
    let installation_id = set(params.installation_id).context(MissingAppParamSnafu {
        param: "github_installation_id",
    })?;
    let service = if value.starts_with("https://") {
        KeyService::AzureKeyVault(KeyVaultKey::parse(value).context(InvalidKeyVaultKeySnafu)?)
    } else if value.starts_with("arn:") {
        KeyService::AwsKms(KmsKey::parse(value).context(InvalidKmsKeySnafu)?)
    } else {
        return UnrecognizedKeySnafu { value }.fail();
    };
    Ok(Some(KeyServiceApp {
        service,
        client_id: client_id.to_string(),
        installation_id: installation_id.to_string(),
    }))
}

/// The signer for `service`, and the `info` line to log once `component` has its first token.
/// For Azure Key Vault, the Microsoft Entra identity's first access token is obtained here; AWS
/// credentials are resolved by the first signature.
pub(crate) async fn signer(
    service: KeyService,
    component: &str,
) -> Result<(Arc<dyn JwtSigner>, String), AppKeyError> {
    match service {
        KeyService::AzureKeyVault(key) => {
            let url = key.url().to_string();
            let (signer, identity) =
                KeyVaultSigner::try_new(component, key, AzureIdentityEnvironment::from_process())
                    .await
                    .map_err(|source| {
                        if source.is_configuration() {
                            AppKeyError::IncompleteIdentity { source }
                        } else {
                            AppKeyError::IdentityUnavailable { source }
                        }
                    })?;
            Ok((
                Arc::new(signer),
                format!(
                    "Authenticated {component} as a GitHub App whose private key is the Azure Key Vault key '{url}', which signs as {identity}."
                ),
            ))
        }
        KeyService::AwsKms(key) => {
            let arn = key.arn().to_string();
            Ok((
                Arc::new(AwsKmsSigner::new(key).await),
                format!(
                    "Authenticated {component} as a GitHub App whose private key is the AWS KMS key '{arn}', which signs with the workload's AWS credentials."
                ),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app(private_key_kms: &str) -> AppKeyParams<'_> {
        AppKeyParams {
            private_key_kms: Some(private_key_kms),
            client_id: Some("Iv23liTest"),
            installation_id: Some("42"),
            ..AppKeyParams::default()
        }
    }

    #[test]
    fn the_key_service_is_named_by_the_form_of_the_key() {
        let resolve =
            |params: &AppKeyParams<'_>| resolve(params).map_err(|error| error.to_string());
        assert_eq!(resolve(&AppKeyParams::default()), Ok(None));
        assert_eq!(
            resolve(&AppKeyParams {
                private_key_kms: Some("  "),
                ..AppKeyParams::default()
            }),
            Ok(None)
        );
        assert_eq!(
            resolve(&app("https://spice-ci.vault.azure.net/keys/github-app/")),
            Ok(Some(KeyServiceApp {
                service: KeyService::AzureKeyVault(
                    KeyVaultKey::parse("https://spice-ci.vault.azure.net/keys/github-app")
                        .expect("a key URL")
                ),
                client_id: "Iv23liTest".to_string(),
                installation_id: "42".to_string(),
            }))
        );
        assert_eq!(
            resolve(&app("arn:aws:kms:us-east-1:111122223333:alias/github-app")),
            Ok(Some(KeyServiceApp {
                service: KeyService::AwsKms(
                    KmsKey::parse("arn:aws:kms:us-east-1:111122223333:alias/github-app")
                        .expect("an alias ARN")
                ),
                client_id: "Iv23liTest".to_string(),
                installation_id: "42".to_string(),
            }))
        );
    }

    #[test]
    fn each_configuration_mistake_names_the_param_to_fix() {
        let kms = "arn:aws:kms:us-east-1:111122223333:alias/github-app";
        let error = |params: AppKeyParams<'_>| {
            let error = resolve(&params).expect_err("a configuration error");
            assert!(error.is_configuration(), "{error}");
            error.to_string()
        };
        assert_eq!(
            [
                error(AppKeyParams {
                    private_key_set: true,
                    ..app(kms)
                }),
                error(AppKeyParams {
                    token_set: true,
                    ..app(kms)
                }),
                error(AppKeyParams {
                    client_id: None,
                    ..app(kms)
                }),
                error(AppKeyParams {
                    installation_id: Some(""),
                    ..app(kms)
                }),
                error(app("alias/github-app")),
                error(app("arn:aws:kms:us-east-1:111122223333:alias/")),
                error(app("https://example.com/keys/github-app")),
            ],
            [
                "Invalid value for parameter github_private_key_kms. A GitHub App's private key is either `github_private_key` or a key service's key in `github_private_key_kms`, and both are set. Remove one of them. See: https://spiceai.org/docs/components/data-connectors/github#github-app-installation".to_string(),
                "Invalid value for parameter github_token. A dataset authenticates with either `github_token` or a GitHub App, and both `github_token` and `github_private_key_kms` are set. Remove one of them. See: https://spiceai.org/docs/components/data-connectors/github#github-app-installation".to_string(),
                "Invalid value for parameter github_client_id. A GitHub App whose private key is in `github_private_key_kms` also needs `github_client_id` and `github_installation_id`, and `github_client_id` is not set. See: https://spiceai.org/docs/components/data-connectors/github#github-app-installation".to_string(),
                "Invalid value for parameter github_installation_id. A GitHub App whose private key is in `github_private_key_kms` also needs `github_client_id` and `github_installation_id`, and `github_installation_id` is not set. See: https://spiceai.org/docs/components/data-connectors/github#github-app-installation".to_string(),
                "Invalid value for parameter github_private_key_kms. `github_private_key_kms` names the key that holds the GitHub App's private key: an Azure Key Vault key URL, such as https://my-vault.vault.azure.net/keys/github-app, or an AWS KMS key or alias ARN, such as arn:aws:kms:us-east-1:111122223333:alias/github-app. Found 'alias/github-app'. See: https://spiceai.org/docs/components/data-connectors/github#github-app-installation".to_string(),
                "Invalid value for parameter github_private_key_kms. 'arn:aws:kms:us-east-1:111122223333:alias/' is not an AWS KMS key ARN or alias ARN, such as arn:aws:kms:us-east-1:111122223333:alias/github-app or arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab. The ARN names the key's region. See: https://spiceai.org/docs/components/data-connectors/github#github-app-installation".to_string(),
                "Invalid value for parameter github_private_key_kms. 'https://example.com/keys/github-app' is not an Azure Key Vault key URL, which looks like https://my-vault.vault.azure.net/keys/github-app, optionally followed by /<version>, in a vault or Managed HSM of the Azure public cloud, Azure Government or Azure China. See: https://spiceai.org/docs/components/data-connectors/github#github-app-installation".to_string(),
            ]
        );
    }
}
