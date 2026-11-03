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

//! Workload identity federation for `OpenAI`, Anthropic and Azure `OpenAI` models and embeddings:
//! the params that configure it, validated, and the refreshing token provider and client they
//! build.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::HostedModelConfig;
use crate::openai::{AzureTokenConfig, ChatBackend, Openai, UsageTier};
use secrecy::SecretString;
use token_provider::TokenProvider;
use token_provider::workload_identity::{
    ANTHROPIC_AUDIENCE, ENTRA_AUDIENCE, Exchange, SubjectTokenSource, WorkloadIdentityError,
    WorkloadIdentityTokenProvider,
};

const OPENAI_DOCS: &str = "https://spiceai.org/docs/components/models/openai";
const ANTHROPIC_DOCS: &str = "https://spiceai.org/docs/components/models/anthropic";
const AZURE_DOCS: &str = "https://spiceai.org/docs/components/models/azure";

/// The Microsoft Entra scope of Azure `OpenAI`.
const AZURE_OPENAI_SCOPE: &str = "https://cognitiveservices.azure.com/.default";

/// Why a model's workload identity federation could not be set up.
#[derive(Debug)]
pub enum FederationError {
    /// A mistake in the model's params: `param` is the one to change.
    Config {
        param: &'static str,
        message: String,
    },
    /// The provider refused the first exchange, or could not be reached.
    Exchange(WorkloadIdentityError),
    /// The client could not be built from the model's other params.
    Client(async_openai::error::OpenAIError),
}

impl FederationError {
    /// The model loading error this is: a configuration error for a mistake in the params.
    #[must_use]
    pub fn into_llm_error(self) -> crate::chat::Error {
        match self {
            Self::Config { param, message } => crate::chat::Error::InvalidParamValueError {
                param: param.to_string(),
                message,
            },
            Self::Exchange(source) => crate::chat::Error::FailedToAuthenticate {
                source: Box::new(source),
            },
            Self::Client(source) => crate::chat::Error::FailedToLoadModel {
                source: Box::new(source),
            },
        }
    }

    /// The embedding loading error this is, reading as the model error.
    #[must_use]
    pub fn into_embed_error(self) -> crate::embeddings::Error {
        crate::embeddings::Error::FailedToInstantiateEmbeddingModel {
            source: Box::new(self.into_llm_error()),
        }
    }
}

/// The `openai_*` federation params of a model or embedding, and the settings of the client
/// built with them.
pub struct OpenAiFederation<'a> {
    pub identity_provider_id: Option<&'a str>,
    pub service_account_id: Option<&'a str>,
    pub identity_token_audience: Option<&'a str>,
    pub identity_token_file: Option<&'a str>,
    pub endpoint: &'a str,
    pub org_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub usage_tier: UsageTier,
}

/// The `anthropic_*` federation params of a model.
pub struct AnthropicFederation<'a> {
    pub federation_rule_id: Option<&'a str>,
    pub organization_id: Option<&'a str>,
    pub service_account_id: Option<&'a str>,
    pub workspace_id: Option<&'a str>,
    pub identity_token_audience: Option<&'a str>,
    pub identity_token_file: Option<&'a str>,
}

/// The `azure_*` federation params of a model or embedding, and the settings of the client
/// built with them.
pub struct AzureFederation<'a> {
    pub tenant_id: Option<&'a str>,
    pub client_id: Option<&'a str>,
    pub identity_token_audience: Option<&'a str>,
    pub identity_token_file: Option<&'a str>,
    pub endpoint: Option<&'a str>,
    pub api_version: Option<&'a str>,
    pub deployment_name: Option<&'a str>,
}

/// The workload identity the platform gives the process in the environment variables the Azure
/// SDKs read. The AKS workload identity webhook sets all of them in a pod that uses it.
#[derive(Clone, Debug, Default)]
pub struct AzureEnvironment {
    /// `AZURE_TENANT_ID`.
    pub tenant_id: Option<String>,
    /// `AZURE_CLIENT_ID`.
    pub client_id: Option<String>,
    /// `AZURE_FEDERATED_TOKEN_FILE`.
    pub federated_token_file: Option<String>,
    /// `AZURE_AUTHORITY_HOST`, the Microsoft Entra authority of a sovereign cloud.
    pub authority_host: Option<String>,
}

impl AzureEnvironment {
    /// The variables of this process. An empty variable counts as unset.
    #[must_use]
    pub fn from_process() -> Self {
        let var = |name: &str| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        };
        Self {
            tenant_id: var("AZURE_TENANT_ID"),
            client_id: var("AZURE_CLIENT_ID"),
            federated_token_file: var("AZURE_FEDERATED_TOKEN_FILE"),
            authority_host: var("AZURE_AUTHORITY_HOST"),
        }
    }
}

/// The first of `names` that the Spicepod itself sets, as opposed to a key autoloaded from a
/// secret store: federation params win over an autoloaded key, but not over one the user wrote.
#[must_use]
pub fn key_in_spicepod<S: std::hash::BuildHasher>(
    raw_params: &HashMap<String, SecretString, S>,
    names: &[&'static str],
) -> Option<&'static str> {
    names
        .iter()
        .copied()
        .find(|name| raw_params.contains_key(*name))
}

/// `openai_api_key` when the Spicepod itself sets it; see [`key_in_spicepod`].
#[must_use]
pub fn openai_api_key_in_spicepod<S: std::hash::BuildHasher>(
    raw_params: &HashMap<String, SecretString, S>,
) -> Option<&'static str> {
    key_in_spicepod(raw_params, &["openai_api_key"])
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

/// Checks a provider's federation params. `None` when none of `required` or `optional` is set;
/// the `required` values when all are set; otherwise the param to fix. `key_param` names an API
/// key or auth token the model also sets, which federation cannot be combined with.
fn validate<'a, const N: usize>(
    required: [(&'static str, Option<&'a str>); N],
    optional: &[Option<&str>],
    key_param: Option<&'static str>,
    docs: &str,
) -> Result<Option<[&'a str; N]>, FederationError> {
    let configured =
        required.iter().any(|(_, value)| value.is_some()) || optional.iter().any(Option::is_some);
    if !configured {
        return Ok(None);
    }
    let federation_param = required.first().map_or("", |(name, _)| *name);
    if let Some(key_param) = key_param {
        return Err(FederationError::Config {
            param: key_param,
            message: format!(
                "A model authenticates with either `{key_param}` or workload identity federation (`{federation_param}`), and both are set. Remove one of them. See: {docs}"
            ),
        });
    }
    let mut values = [""; N];
    for (slot, (name, value)) in values.iter_mut().zip(required) {
        let Some(value) = value.filter(|value| !value.is_empty()) else {
            let names: Vec<String> = required
                .iter()
                .map(|(name, _)| format!("`{name}`"))
                .collect();
            return Err(FederationError::Config {
                param: name,
                message: format!(
                    "Workload identity federation needs {}, and `{name}` is not set. See: {docs}",
                    names.join(", ")
                ),
            });
        };
        *slot = value;
    }
    Ok(Some(values))
}

/// Where the identity token comes from: the file the model names, or the GitHub Actions job it
/// runs in.
fn subject_token_source(
    file: Option<&str>,
    audience: &str,
    file_param: &'static str,
    docs: &str,
) -> Result<SubjectTokenSource, FederationError> {
    if let Some(file) = file.filter(|file| !file.is_empty()) {
        return Ok(SubjectTokenSource::File(PathBuf::from(file)));
    }
    SubjectTokenSource::github_actions_from_env(audience).map_err(|_| FederationError::Config {
        param: file_param,
        message: format!(
            "Workload identity federation has no identity token: set `{file_param}` to the file the platform keeps it in, such as a Kubernetes projected service account token, or run in a GitHub Actions job with `permissions: id-token: write`. See: {docs}"
        ),
    })
}

async fn token_provider(
    component: &str,
    source: SubjectTokenSource,
    exchange: Exchange,
) -> Result<Arc<dyn TokenProvider>, FederationError> {
    let provider = WorkloadIdentityTokenProvider::try_new(component, source, exchange)
        .await
        .map_err(FederationError::Exchange)?;
    Ok(Arc::new(provider))
}

/// The token provider of an `OpenAI` model configured for workload identity federation, or `None`
/// when no federation param is set.
async fn openai_token_provider(
    component: &str,
    federation: &OpenAiFederation<'_>,
    key_param: Option<&'static str>,
) -> Result<Option<Arc<dyn TokenProvider>>, FederationError> {
    let Some([provider_id, service_account_id, audience]) = validate(
        [
            (
                "openai_identity_provider_id",
                federation.identity_provider_id,
            ),
            ("openai_service_account_id", federation.service_account_id),
            (
                "openai_identity_token_audience",
                federation.identity_token_audience,
            ),
        ],
        &[federation.identity_token_file],
        key_param,
        OPENAI_DOCS,
    )?
    else {
        return Ok(None);
    };
    let source = subject_token_source(
        federation.identity_token_file,
        audience,
        "openai_identity_token_file",
        OPENAI_DOCS,
    )?;
    token_provider(
        component,
        source,
        Exchange::openai(provider_id, service_account_id),
    )
    .await
    .map(Some)
}

/// The client of an `OpenAI` model or embedding configured for workload identity federation,
/// which sends the current token of a refreshing provider, or `None` when no federation param is
/// set. `component` names the model or embedding in the provider's logs, such as
/// `model 'gpt' (openai)`.
///
/// # Errors
///
/// A [`FederationError`] for a conflicting or missing param, a refused first exchange, or a
/// client the other params cannot build.
pub async fn openai_federated_client(
    component: &str,
    model: &str,
    federation: &OpenAiFederation<'_>,
    key_param: Option<&'static str>,
    chat_backend: ChatBackend,
) -> Result<Option<Openai<HostedModelConfig>>, FederationError> {
    let Some(token) = openai_token_provider(component, federation, key_param).await? else {
        return Ok(None);
    };
    crate::openai::new_openai_client_with_token_provider(
        model.to_string(),
        federation.endpoint,
        token,
        federation.org_id,
        federation.project_id,
        Some(federation.usage_tier),
        chat_backend,
    )
    .map(Some)
    .map_err(FederationError::Client)
}

/// The token provider of an Anthropic model configured for workload identity federation, or
/// `None` when no federation param is set. `component` names the model in the provider's logs,
/// such as `model 'claude' (anthropic)`.
///
/// # Errors
///
/// A [`FederationError`] for a conflicting or missing param, or a refused first exchange.
pub async fn anthropic_token_provider(
    component: &str,
    federation: &AnthropicFederation<'_>,
    key_param: Option<&'static str>,
) -> Result<Option<Arc<dyn TokenProvider>>, FederationError> {
    let Some([rule_id, organization_id, service_account_id]) = validate(
        [
            (
                "anthropic_federation_rule_id",
                federation.federation_rule_id,
            ),
            ("anthropic_organization_id", federation.organization_id),
            (
                "anthropic_service_account_id",
                federation.service_account_id,
            ),
        ],
        &[
            federation.identity_token_audience,
            federation.identity_token_file,
        ],
        key_param,
        ANTHROPIC_DOCS,
    )?
    else {
        return Ok(None);
    };
    let audience = federation
        .identity_token_audience
        .filter(|audience| !audience.is_empty())
        .unwrap_or(ANTHROPIC_AUDIENCE);
    let source = subject_token_source(
        federation.identity_token_file,
        audience,
        "anthropic_identity_token_file",
        ANTHROPIC_DOCS,
    )?;
    let workspace_id = federation
        .workspace_id
        .filter(|id| !id.is_empty())
        .map(str::to_string);
    token_provider(
        component,
        source,
        Exchange::anthropic(rule_id, organization_id, service_account_id, workspace_id),
    )
    .await
    .map(Some)
}

/// Where an Azure model's identity token comes from and the exchange it goes to, or `None` when
/// the model is not configured for workload identity federation.
///
/// - The `azure_*` federation params configure it, and conflict with an `azure_api_key` or
///   `azure_entra_token` that the Spicepod sets (`key_param`).
/// - A model with no credential at all (`has_key` is false, counting keys autoloaded from a
///   secret store) uses the workload identity of the environment, as the Azure SDKs do: the
///   tenant and client from `AZURE_TENANT_ID` and `AZURE_CLIENT_ID`, and the identity token from
///   `AZURE_FEDERATED_TOKEN_FILE` or the GitHub Actions job.
fn azure_exchange(
    federation: &AzureFederation<'_>,
    key_param: Option<&'static str>,
    has_key: bool,
    environment: &AzureEnvironment,
) -> Result<Option<(SubjectTokenSource, Exchange)>, FederationError> {
    let explicit = [
        federation.tenant_id,
        federation.client_id,
        federation.identity_token_audience,
        federation.identity_token_file,
    ]
    .into_iter()
    .any(|value| non_empty(value).is_some());
    if !explicit && has_key {
        return Ok(None);
    }
    let tenant = non_empty(federation.tenant_id).or(environment.tenant_id.as_deref());
    let client = non_empty(federation.client_id).or(environment.client_id.as_deref());
    let file =
        non_empty(federation.identity_token_file).or(environment.federated_token_file.as_deref());
    let audience = non_empty(federation.identity_token_audience).unwrap_or(ENTRA_AUDIENCE);
    if !explicit {
        // Only an environment that provides a whole workload identity configures a model that
        // sets no federation param; anything less leaves it a model without a credential.
        let identity_token =
            file.is_some() || SubjectTokenSource::github_actions_from_env(audience).is_ok();
        if tenant.is_none() || client.is_none() || !identity_token {
            return Ok(None);
        }
    }
    let Some([tenant, client]) = validate(
        [("azure_tenant_id", tenant), ("azure_client_id", client)],
        &[
            federation.identity_token_audience,
            federation.identity_token_file,
        ],
        key_param,
        AZURE_DOCS,
    )?
    else {
        return Ok(None);
    };
    let exchange = Exchange::entra(
        environment.authority_host.as_deref(),
        tenant,
        client,
        AZURE_OPENAI_SCOPE,
    )
    .map_err(|error| FederationError::Config {
        param: "azure_tenant_id",
        message: format!("{error} See: {AZURE_DOCS}"),
    })?;
    let source = subject_token_source(file, audience, "azure_identity_token_file", AZURE_DOCS)?;
    Ok(Some((source, exchange)))
}

/// The client of an Azure `OpenAI` model or embedding configured for workload identity
/// federation (see [`azure_exchange`]), which sends the current Microsoft Entra token of a
/// refreshing provider, or `None` when the model is not configured for it. `component` names the
/// model or embedding in the provider's logs, such as `model 'gpt' (azure)`.
///
/// # Errors
///
/// A [`FederationError`] for a conflicting, missing or invalid param, or a refused first
/// exchange.
pub async fn azure_federated_client(
    component: &str,
    model: &str,
    federation: &AzureFederation<'_>,
    key_param: Option<&'static str>,
    has_key: bool,
    environment: &AzureEnvironment,
    chat_backend: ChatBackend,
) -> Result<Option<Openai<AzureTokenConfig>>, FederationError> {
    let Some((source, exchange)) = azure_exchange(federation, key_param, has_key, environment)?
    else {
        return Ok(None);
    };
    let Some(endpoint) = non_empty(federation.endpoint) else {
        return Err(FederationError::Config {
            param: "endpoint",
            message: format!(
                "An Azure model needs `endpoint`, the endpoint of its Azure OpenAI resource, such as https://my-resource.openai.azure.com. See: {AZURE_DOCS}"
            ),
        });
    };
    let token = token_provider(component, source, exchange).await?;
    Ok(Some(crate::openai::new_azure_client_with_token_provider(
        model.to_string(),
        endpoint,
        federation.api_version,
        federation.deployment_name,
        token,
        chat_backend,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn openai(
        provider: Option<&'static str>,
        service_account: Option<&'static str>,
        audience: Option<&'static str>,
    ) -> OpenAiFederation<'static> {
        OpenAiFederation {
            identity_provider_id: provider,
            service_account_id: service_account,
            identity_token_audience: audience,
            identity_token_file: None,
            endpoint: "https://api.openai.com/v1",
            org_id: None,
            project_id: None,
            usage_tier: UsageTier::default(),
        }
    }

    fn anthropic(organization_id: Option<&'static str>) -> AnthropicFederation<'static> {
        AnthropicFederation {
            federation_rule_id: Some("fdrl_1"),
            organization_id,
            service_account_id: Some("svac_1"),
            workspace_id: None,
            identity_token_audience: None,
            identity_token_file: None,
        }
    }

    fn message(error: FederationError) -> String {
        error.into_llm_error().to_string()
    }

    #[tokio::test]
    async fn a_model_without_federation_params_has_no_token_provider() {
        let none = openai_token_provider(
            "model 'gpt' (openai)",
            &openai(None, None, None),
            Some("openai_api_key"),
        )
        .await
        .expect("no federation");
        assert!(none.is_none(), "an API key model stays an API key model");
    }

    #[tokio::test]
    async fn federation_and_an_api_key_in_the_spicepod_conflict() {
        let error = openai_token_provider(
            "model 'gpt' (openai)",
            &openai(Some("idp_1"), Some("user-1"), Some("aud")),
            Some("openai_api_key"),
        )
        .await
        .expect_err("a conflict");
        assert_eq!(
            message(error),
            "Invalid value for parameter openai_api_key. A model authenticates with either `openai_api_key` or workload identity federation (`openai_identity_provider_id`), and both are set. Remove one of them. See: https://spiceai.org/docs/components/models/openai"
        );
    }

    #[tokio::test]
    async fn a_missing_federation_param_is_named() {
        let error = openai_token_provider(
            "model 'gpt' (openai)",
            &openai(Some("idp_1"), None, Some("aud")),
            None,
        )
        .await
        .expect_err("a missing service account");
        assert_eq!(
            message(error),
            "Invalid value for parameter openai_service_account_id. Workload identity federation needs `openai_identity_provider_id`, `openai_service_account_id`, `openai_identity_token_audience`, and `openai_service_account_id` is not set. See: https://spiceai.org/docs/components/models/openai"
        );

        let error =
            anthropic_token_provider("model 'claude' (anthropic)", &anthropic(Some("")), None)
                .await
                .expect_err("an empty organization");
        assert_eq!(
            message(error),
            "Invalid value for parameter anthropic_organization_id. Workload identity federation needs `anthropic_federation_rule_id`, `anthropic_organization_id`, `anthropic_service_account_id`, and `anthropic_organization_id` is not set. See: https://spiceai.org/docs/components/models/anthropic"
        );
    }

    #[tokio::test]
    async fn an_anthropic_key_and_federation_conflict() {
        let error = anthropic_token_provider(
            "model 'claude' (anthropic)",
            &anthropic(Some("org")),
            Some("anthropic_auth_token"),
        )
        .await
        .expect_err("a conflict");
        assert_eq!(
            message(error),
            "Invalid value for parameter anthropic_auth_token. A model authenticates with either `anthropic_auth_token` or workload identity federation (`anthropic_federation_rule_id`), and both are set. Remove one of them. See: https://spiceai.org/docs/components/models/anthropic"
        );
    }

    #[test]
    fn an_embedding_error_reads_as_the_model_error() {
        let error = FederationError::Config {
            param: "openai_service_account_id",
            message: "It is not set.".to_string(),
        };
        assert_eq!(
            error.into_embed_error().to_string(),
            "Failed to load embedding model: Invalid value for parameter openai_service_account_id. It is not set."
        );
    }

    fn azure<'a>(
        tenant_id: Option<&'a str>,
        client_id: Option<&'a str>,
        identity_token_file: Option<&'a str>,
    ) -> AzureFederation<'a> {
        AzureFederation {
            tenant_id,
            client_id,
            identity_token_audience: None,
            identity_token_file,
            endpoint: Some("https://my-resource.openai.azure.com"),
            api_version: None,
            deployment_name: None,
        }
    }

    fn aks() -> AzureEnvironment {
        AzureEnvironment {
            tenant_id: Some("aks-tenant".to_string()),
            client_id: Some("aks-client".to_string()),
            federated_token_file: Some(
                "/var/run/secrets/azure/tokens/azure-identity-token".to_string(),
            ),
            authority_host: Some("https://login.microsoftonline.com/".to_string()),
        }
    }

    /// The identity token source and the token endpoint and client of the exchange an Azure
    /// model resolves to, or the message of the error it resolves to.
    fn azure_plan(
        federation: &AzureFederation<'_>,
        key_param: Option<&'static str>,
        has_key: bool,
        environment: &AzureEnvironment,
    ) -> Result<Option<(String, String, String)>, String> {
        match azure_exchange(federation, key_param, has_key, environment) {
            Ok(None) => Ok(None),
            Ok(Some((
                source,
                Exchange::Entra {
                    token_url,
                    client_id,
                    scope,
                },
            ))) => {
                assert_eq!(scope, "https://cognitiveservices.azure.com/.default");
                Ok(Some((format!("{source:?}"), token_url, client_id)))
            }
            Ok(Some((_, exchange))) => panic!("not an Entra exchange: {exchange:?}"),
            Err(error) => Err(message(error)),
        }
    }

    #[test]
    fn an_azure_model_with_a_key_ignores_the_environments_workload_identity() {
        assert_eq!(
            azure_plan(
                &azure(None, None, None),
                Some("azure_api_key"),
                true,
                &aks()
            ),
            Ok(None)
        );
        // A key autoloaded from a secret store is a credential too.
        assert_eq!(
            azure_plan(&azure(None, None, None), None, true, &aks()),
            Ok(None)
        );
    }

    #[test]
    fn an_azure_model_without_a_credential_uses_the_aks_workload_identity() {
        assert_eq!(
            azure_plan(&azure(None, None, None), None, false, &aks()),
            Ok(Some((
                r#"File("/var/run/secrets/azure/tokens/azure-identity-token")"#.to_string(),
                "https://login.microsoftonline.com/aks-tenant/oauth2/v2.0/token".to_string(),
                "aks-client".to_string()
            )))
        );
        // Without a tenant the environment holds no whole identity, so the model has no
        // credential.
        let no_tenant = AzureEnvironment {
            tenant_id: None,
            ..aks()
        };
        assert_eq!(
            azure_plan(&azure(None, None, None), None, false, &no_tenant),
            Ok(None)
        );
    }

    #[test]
    fn azure_federation_params_override_the_environment_and_an_autoloaded_key() {
        let federation = azure(
            Some("contoso.onmicrosoft.com"),
            Some("spicepod-client"),
            Some("/token"),
        );
        let sovereign = AzureEnvironment {
            authority_host: Some("https://login.microsoftonline.us".to_string()),
            ..aks()
        };
        assert_eq!(
            azure_plan(&federation, None, true, &sovereign),
            Ok(Some((
                r#"File("/token")"#.to_string(),
                "https://login.microsoftonline.us/contoso.onmicrosoft.com/oauth2/v2.0/token"
                    .to_string(),
                "spicepod-client".to_string()
            )))
        );
        // A param the Spicepod does not set comes from the environment.
        assert_eq!(
            azure_plan(
                &azure(None, Some("spicepod-client"), None),
                None,
                false,
                &aks()
            ),
            Ok(Some((
                r#"File("/var/run/secrets/azure/tokens/azure-identity-token")"#.to_string(),
                "https://login.microsoftonline.com/aks-tenant/oauth2/v2.0/token".to_string(),
                "spicepod-client".to_string()
            )))
        );
    }

    #[test]
    fn azure_federation_params_and_a_key_in_the_spicepod_conflict() {
        assert_eq!(
            azure_plan(
                &azure(Some("tenant"), Some("client"), None),
                Some("azure_entra_token"),
                true,
                &AzureEnvironment::default()
            ),
            Err("Invalid value for parameter azure_entra_token. A model authenticates with either `azure_entra_token` or workload identity federation (`azure_tenant_id`), and both are set. Remove one of them. See: https://spiceai.org/docs/components/models/azure".to_string())
        );
    }

    #[test]
    fn a_missing_or_invalid_azure_tenant_is_named() {
        assert_eq!(
            azure_plan(
                &azure(None, Some("client"), Some("/token")),
                None,
                false,
                &AzureEnvironment::default()
            ),
            Err("Invalid value for parameter azure_tenant_id. Workload identity federation needs `azure_tenant_id`, `azure_client_id`, and `azure_tenant_id` is not set. See: https://spiceai.org/docs/components/models/azure".to_string())
        );
        assert_eq!(
            azure_plan(
                &azure(Some("contoso/../common"), Some("client"), Some("/token")),
                None,
                false,
                &AzureEnvironment::default()
            ),
            Err("Invalid value for parameter azure_tenant_id. The Microsoft Entra tenant must be a directory (tenant) ID, which is a GUID, or a domain name such as 'contoso.onmicrosoft.com'. See: https://spiceai.org/docs/components/models/azure".to_string())
        );
    }

    #[test]
    fn an_api_key_counts_as_set_only_when_the_spicepod_writes_it() {
        let mut raw = HashMap::new();
        assert_eq!(openai_api_key_in_spicepod(&raw), None);
        raw.insert("openai_api_key".to_string(), SecretString::from("sk"));
        assert_eq!(openai_api_key_in_spicepod(&raw), Some("openai_api_key"));
    }
}
