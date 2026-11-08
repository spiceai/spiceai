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
use token_provider::google_metadata::GoogleMetadataIdentity;
use token_provider::workload_identity::{
    ANTHROPIC_AUDIENCE, ENTRA_AUDIENCE, Exchange, IdentityTokenSource, OPENAI_AUDIENCE,
    SubjectTokenSource, WorkloadIdentityError, WorkloadIdentityTokenProvider,
};

const OPENAI_DOCS: &str = "https://spiceai.org/docs/components/models/openai";
const ANTHROPIC_DOCS: &str = "https://spiceai.org/docs/components/models/anthropic";
const AZURE_DOCS: &str = "https://spiceai.org/docs/components/models/azure";

/// The Microsoft Entra authority and the Azure `OpenAI` token scope of each Azure cloud.
const AZURE_PUBLIC_CLOUD: (&str, &str) = (
    "https://login.microsoftonline.com",
    "https://cognitiveservices.azure.com/.default",
);
const AZURE_US_GOVERNMENT: (&str, &str) = (
    "https://login.microsoftonline.us",
    "https://cognitiveservices.azure.us/.default",
);
const AZURE_CHINA: (&str, &str) = (
    "https://login.chinacloudapi.cn",
    "https://cognitiveservices.azure.cn/.default",
);

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
    /// The environment's workload identity cannot be used: it lacks a part, or names an
    /// authority the identity token must not be sent to.
    IncompleteIdentity(String),
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
            Self::IncompleteIdentity(message) => crate::chat::Error::FailedToAuthenticate {
                source: message.into(),
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

/// The `anthropic_*` federation params of a model. Each defaults to the variable the Anthropic
/// SDKs read (`ANTHROPIC_FEDERATION_RULE_ID`, ...), which `settings_from` names when any of them
/// came from there.
pub struct AnthropicFederation<'a> {
    pub federation_rule_id: Option<&'a str>,
    pub organization_id: Option<&'a str>,
    pub service_account_id: Option<&'a str>,
    pub workspace_id: Option<&'a str>,
    pub identity_token_audience: Option<&'a str>,
    pub identity_token_file: Option<&'a str>,
    /// Where the federation settings came from, as the load line names it, such as
    /// ``"the `anthropic_*` params"``.
    pub settings_from: &'static str,
}

/// The `azure_*` federation params of a model or embedding, and the settings of the client
/// built with them.
pub struct AzureFederation<'a> {
    pub tenant_id: Option<&'a str>,
    pub client_id: Option<&'a str>,
    pub federated_token_audience: Option<&'a str>,
    pub federated_token_file: Option<&'a str>,
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

/// The error for federation params with no identity token to exchange, when there is no file
/// and no GitHub Actions job to get one from.
fn no_identity_token(file_param: &'static str, docs: &str) -> FederationError {
    FederationError::Config {
        param: file_param,
        message: format!(
            "Workload identity federation has no identity token: set `{file_param}` to the file the platform keeps it in, such as a Kubernetes projected service account token, or run in a GitHub Actions job with `permissions: id-token: write`. See: {docs}"
        ),
    }
}

/// The message for federation params with no identity token from a file, a GitHub Actions job or
/// the workload's cloud identity, which `tried` lists with what each said. It ends without a
/// period, so a caller can add where to read more.
#[must_use]
pub fn no_identity_token_message(file_param: &str, tried: &str) -> String {
    format!(
        "Workload identity federation has no identity token: `{file_param}` is not set, this is not a GitHub Actions job with `permissions: id-token: write`, and no cloud identity answered: {tried}. Set `{file_param}` to the file the platform keeps the token in, such as a Kubernetes projected service account token, or run where the workload has an AWS, Google Cloud or Azure identity"
    )
}

/// The error for an audience set beside a token file, whose audience is set where it is issued.
fn audience_beside_file(
    audience_param: &'static str,
    file_param: &'static str,
    docs: &str,
) -> FederationError {
    FederationError::Config {
        param: audience_param,
        message: format!(
            "`{audience_param}` applies only to an identity token Spice requests, from GitHub Actions or the workload's cloud identity, but the identity token comes from `{file_param}`, whose audience is set where it is issued. Remove one of them. See: {docs}"
        ),
    }
}

/// The workload's cloud identity as a source of identity tokens for `audience`, trying AWS, then
/// the Google Cloud metadata server, then the workload's Microsoft Entra identity. Each is chosen
/// only once it issues a token for `audience`, so credentials that cannot get one, such as AWS
/// credentials without `sts:GetWebIdentityToken`, pass to the next. `component` names what the
/// tokens authenticate.
///
/// # Errors
///
/// When none answers, with what each said, as a clause.
pub async fn cloud_identity(component: &str, audience: &str) -> Result<SubjectTokenSource, String> {
    let aws = match aws_identity::identity_token::AwsWebIdentityToken::detect(audience).await {
        Ok(source) => return Ok(SubjectTokenSource::Requested(Arc::new(source))),
        Err(reason) => reason
            .strip_prefix("found no AWS credentials. Tried ")
            .map_or(reason.clone(), |tried| {
                format!("no credentials from {tried}")
            }),
    };
    let google = match GoogleMetadataIdentity::from_env(audience) {
        Ok(source) => match source.fetch().await {
            Ok(_) => return Ok(SubjectTokenSource::Requested(Arc::new(source))),
            Err(reason) => reason,
        },
        Err(reason) => reason,
    };
    let azure = match azure_credential::AzureIdentityToken::detect(
        component,
        audience,
        &azure_credential::AzureIdentityEnvironment::from_process(),
    )
    .await
    {
        Ok(source) => return Ok(SubjectTokenSource::Requested(Arc::new(source))),
        Err(reason) => reason,
    };
    Err(format!(
        "AWS ({aws}), Google Cloud ({google}) and Azure ({azure})"
    ))
}

/// Where the identity token of an `OpenAI` or Anthropic model comes from: the file it names,
/// else the GitHub Actions job it runs in, else the workload's cloud identity.
async fn subject_token_source(
    component: &str,
    file: Option<&str>,
    audience: &str,
    file_param: &'static str,
    docs: &str,
) -> Result<SubjectTokenSource, FederationError> {
    if let Some(file) = file.filter(|file| !file.is_empty()) {
        return Ok(SubjectTokenSource::File(PathBuf::from(file)));
    }
    if let Ok(source) = SubjectTokenSource::github_actions_from_env(audience) {
        return Ok(source);
    }
    cloud_identity(component, audience)
        .await
        .map_err(|tried| FederationError::Config {
            param: file_param,
            message: format!(
                "{}. See: {docs}",
                no_identity_token_message(file_param, &tried)
            ),
        })
}

/// The info line logged when an `OpenAI` or Anthropic model has authenticated with workload
/// identity federation: where its settings and identity token came from, never the IDs.
#[must_use]
pub fn federation_message(
    component: &str,
    account: &str,
    settings_from: &str,
    source: &SubjectTokenSource,
) -> String {
    format!(
        "Authenticated {component} as {account} through workload identity federation, with the federation settings from {settings_from} and the identity token from {}.",
        source.describe()
    )
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
    let Some([provider_id, service_account_id]) = validate(
        [
            (
                "openai_identity_provider_id",
                federation.identity_provider_id,
            ),
            ("openai_service_account_id", federation.service_account_id),
        ],
        &[
            federation.identity_token_audience,
            federation.identity_token_file,
        ],
        key_param,
        OPENAI_DOCS,
    )?
    else {
        return Ok(None);
    };
    let file = non_empty(federation.identity_token_file);
    let audience = non_empty(federation.identity_token_audience);
    if file.is_some() && audience.is_some() {
        return Err(audience_beside_file(
            "openai_identity_token_audience",
            "openai_identity_token_file",
            OPENAI_DOCS,
        ));
    }
    let source = subject_token_source(
        component,
        file,
        audience.unwrap_or(OPENAI_AUDIENCE),
        "openai_identity_token_file",
        OPENAI_DOCS,
    )
    .await?;
    let message = federation_message(
        component,
        "an OpenAI service account",
        "the `openai_*` params",
        &source,
    );
    let provider = token_provider(
        component,
        source,
        Exchange::openai(provider_id, service_account_id),
    )
    .await?;
    tracing::info!("{message}");
    Ok(Some(provider))
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
    let file = non_empty(federation.identity_token_file);
    let audience = non_empty(federation.identity_token_audience);
    if file.is_some() && audience.is_some() {
        return Err(audience_beside_file(
            "anthropic_identity_token_audience",
            "anthropic_identity_token_file",
            ANTHROPIC_DOCS,
        ));
    }
    let source = subject_token_source(
        component,
        file,
        audience.unwrap_or(ANTHROPIC_AUDIENCE),
        "anthropic_identity_token_file",
        ANTHROPIC_DOCS,
    )
    .await?;
    let workspace_id = federation
        .workspace_id
        .filter(|id| !id.is_empty())
        .map(str::to_string);
    let message = federation_message(
        component,
        "an Anthropic service account",
        federation.settings_from,
        &source,
    );
    let provider = token_provider(
        component,
        source,
        Exchange::anthropic(rule_id, organization_id, service_account_id, workspace_id),
    )
    .await?;
    tracing::info!("{message}");
    Ok(Some(provider))
}

/// Where each part of an Azure model's workload identity came from: a param or an environment
/// variable, named as the load log names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AzureIdentityOrigin {
    tenant: &'static str,
    client: &'static str,
    token: &'static str,
}

/// How an Azure model authenticates with workload identity federation.
struct AzurePlan {
    source: SubjectTokenSource,
    exchange: Exchange,
    origin: AzureIdentityOrigin,
}

/// `param`, or else `env`, with the name of the one that supplied the value.
fn param_or_env<'a>(
    param: (Option<&'a str>, &'static str),
    env: (Option<&'a str>, &'static str),
) -> (Option<&'a str>, &'static str) {
    match non_empty(param.0) {
        Some(value) => (Some(value), param.1),
        None => (non_empty(env.0), env.1),
    }
}

/// The Microsoft Entra authority and the Azure `OpenAI` token scope of the Azure cloud the
/// endpoint is in: Azure Government for `*.azure.us`, Azure operated by 21Vianet for `*.azure.cn`,
/// otherwise the public cloud.
fn azure_cloud(endpoint: Option<&str>) -> (&'static str, &'static str) {
    let host = endpoint
        .and_then(|endpoint| reqwest::Url::parse(endpoint).ok())
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase));
    match host.as_deref() {
        Some(host) if host.ends_with(".azure.us") => AZURE_US_GOVERNMENT,
        Some(host) if host.ends_with(".azure.cn") => AZURE_CHINA,
        _ => AZURE_PUBLIC_CLOUD,
    }
}

/// The info line logged when an Azure model has authenticated with workload identity
/// federation. Part of the identity may come from the environment, which no Spicepod shows, so
/// the line names where each part came from (never the IDs themselves).
fn azure_identity_message(component: &str, origin: &AzureIdentityOrigin) -> String {
    format!(
        "Authenticated {component} as a Microsoft Entra workload identity, with the tenant from {}, the client from {} and the identity token from {}.",
        origin.tenant, origin.client, origin.token
    )
}

/// How an Azure model authenticates with workload identity federation, or `None` when it does not.
///
/// - The `azure_*` federation params configure it, and conflict with an `azure_api_key` or
///   `azure_entra_token` that the Spicepod sets (`key_param`).
/// - A model with no credential at all (`has_key` is false, counting keys autoloaded from a
///   secret store) uses the workload identity of the environment, as the Azure SDKs do: the
///   tenant and client from `AZURE_TENANT_ID` and `AZURE_CLIENT_ID`, and the identity token from
///   `AZURE_FEDERATED_TOKEN_FILE` or the GitHub Actions job (`github_actions`, given the
///   audience).
/// - The token endpoint and scope follow the endpoint's Azure cloud; `AZURE_AUTHORITY_HOST`
///   replaces the authority.
fn azure_exchange(
    federation: &AzureFederation<'_>,
    key_param: Option<&'static str>,
    has_key: bool,
    environment: &AzureEnvironment,
    github_actions: impl Fn(&str) -> Option<SubjectTokenSource>,
) -> Result<Option<AzurePlan>, FederationError> {
    let explicit = [
        federation.tenant_id,
        federation.client_id,
        federation.federated_token_audience,
        federation.federated_token_file,
    ]
    .into_iter()
    .any(|value| non_empty(value).is_some());
    if !explicit && has_key {
        return Ok(None);
    }
    let (tenant, tenant_from) = param_or_env(
        (federation.tenant_id, "`azure_tenant_id`"),
        (environment.tenant_id.as_deref(), "`AZURE_TENANT_ID`"),
    );
    let (client, client_from) = param_or_env(
        (federation.client_id, "`azure_client_id`"),
        (environment.client_id.as_deref(), "`AZURE_CLIENT_ID`"),
    );
    let (file, file_from) = param_or_env(
        (
            federation.federated_token_file,
            "`azure_federated_token_file`",
        ),
        (
            environment.federated_token_file.as_deref(),
            "`AZURE_FEDERATED_TOKEN_FILE`",
        ),
    );
    let audience = non_empty(federation.federated_token_audience);
    if let (Some(_), Some(_)) = (audience, file) {
        return Err(FederationError::Config {
            param: "azure_federated_token_audience",
            message: format!(
                "`azure_federated_token_audience` applies only to an identity token Spice requests from GitHub Actions, but the identity token comes from {file_from}, whose audience is set where it is issued. Remove one of them. See: {AZURE_DOCS}"
            ),
        });
    }
    let github = if file.is_none() {
        github_actions(audience.unwrap_or(ENTRA_AUDIENCE))
    } else {
        None
    };
    if !explicit && (tenant.is_none() || client.is_none() || (file.is_none() && github.is_none())) {
        // A model that sets no federation param uses only a whole workload identity from the
        // environment. Anything less is left to the Azure credential chain (a managed identity,
        // user-assigned when `AZURE_CLIENT_ID` names one, then the Azure CLI), as the Azure SDKs
        // do.
        return Ok(None);
    }
    let Some([tenant, client]) = validate(
        [("azure_tenant_id", tenant), ("azure_client_id", client)],
        &[
            federation.federated_token_audience,
            federation.federated_token_file,
        ],
        key_param,
        AZURE_DOCS,
    )?
    else {
        return Ok(None);
    };
    let (authority, scope) = azure_cloud(federation.endpoint);
    let exchange = Exchange::entra(
        Some(environment.authority_host.as_deref().unwrap_or(authority)),
        tenant,
        client,
        scope,
    )
    .map_err(|error| match error {
        WorkloadIdentityError::InsecureEntraAuthority => FederationError::IncompleteIdentity(
            format!("`AZURE_AUTHORITY_HOST` is not usable: {error} See: {AZURE_DOCS}"),
        ),
        error => FederationError::Config {
            param: "azure_tenant_id",
            message: format!("{error} See: {AZURE_DOCS}"),
        },
    })?;
    let (source, token_from) = match (file, github) {
        (Some(file), _) => (SubjectTokenSource::File(PathBuf::from(file)), file_from),
        (None, Some(github)) => (github, "this GitHub Actions job"),
        (None, None) => return Err(no_identity_token("azure_federated_token_file", AZURE_DOCS)),
    };
    Ok(Some(AzurePlan {
        source,
        exchange,
        origin: AzureIdentityOrigin {
            tenant: tenant_from,
            client: client_from,
            token: token_from,
        },
    }))
}

/// Whether `endpoint` is `OpenAI`'s own API, which a request without a credential cannot reach,
/// as opposed to a compatible server that may need none.
#[must_use]
fn is_openai_api(endpoint: &str) -> bool {
    reqwest::Url::parse(endpoint)
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
        .is_some_and(|host| host == "api.openai.com")
}

/// The error for an `OpenAI` model or embedding `component` that calls `OpenAI`'s own API with no
/// key (or a blank one, such as an empty `OPENAI_API_KEY`) and no federation params. `OpenAI`
/// refuses every such request, so Spice says so before the first. `None` for any other endpoint,
/// which may accept requests without a key.
#[must_use]
pub fn openai_missing_credential(
    component: &str,
    api_key: Option<&str>,
    endpoint: &str,
) -> Option<String> {
    (api_key.is_none_or(|key| key.trim().is_empty()) && is_openai_api(endpoint)).then(|| {
        format!(
            "Found no credential for {component}: set `openai_api_key`, or workload identity federation with `openai_identity_provider_id` and `openai_service_account_id`. See: {OPENAI_DOCS}"
        )
    })
}

/// The message for an Anthropic model with no key, auth token or federation settings.
#[must_use]
pub fn anthropic_no_credential_message(component: &str) -> String {
    format!(
        "Found no credential for {component}: set `anthropic_api_key` or `anthropic_auth_token`, or workload identity federation with `anthropic_federation_rule_id`, `anthropic_organization_id` and `anthropic_service_account_id`, or the `ANTHROPIC_FEDERATION_RULE_ID`, `ANTHROPIC_ORGANIZATION_ID` and `ANTHROPIC_SERVICE_ACCOUNT_ID` variables the Anthropic SDKs read. See: {ANTHROPIC_DOCS}"
    )
}

/// Where an Anthropic model's federation settings came from, as the load line names it: the
/// `anthropic_*` params the Spicepod sets (`in_spicepod`), the `ANTHROPIC_*` variables or secrets
/// autoloaded for the ones it leaves out (`autoloaded`), or both.
#[must_use]
pub fn anthropic_settings_from(in_spicepod: bool, autoloaded: bool) -> &'static str {
    match (in_spicepod, autoloaded) {
        (false, true) => "`ANTHROPIC_*` from the environment or a secret store",
        (true, true) => {
            "the `anthropic_*` params and `ANTHROPIC_*` from the environment or a secret store"
        }
        _ => "the `anthropic_*` params",
    }
}

/// The message for an Azure model that has no key and no Microsoft Entra identity that answered:
/// `tried` lists each identity tried and what it said.
#[must_use]
pub fn azure_no_credential_message(component: &str, tried: &str) -> String {
    format!(
        "Found no credential for {component}: it sets no `azure_api_key` or `azure_entra_token`, and no Microsoft Entra identity answered. Tried {tried}. Set `azure_api_key`, or give the workload an identity: on AKS, label the pod `azure.workload.identity/use: \"true\"`; on an Azure VM, App Service or Container Apps, assign it a managed identity; locally, run `az login`. See: {AZURE_DOCS}"
    )
}

/// `error`, finding an Azure model's identity in the Azure credential chain, as the model's
/// loading error.
fn azure_chain_error(component: &str, error: azure_credential::ResolveError) -> FederationError {
    use azure_credential::ResolveError;
    match error {
        ResolveError::IncompleteWorkloadIdentity { missing } => {
            FederationError::IncompleteIdentity(format!(
                "Found `AZURE_FEDERATED_TOKEN_FILE` for {component}, but {missing} not set. On AKS, label the pod `azure.workload.identity/use: \"true\"` and annotate its service account with `azure.workload.identity/client-id`, so the workload identity webhook sets all three. See: {AZURE_DOCS}"
            ))
        }
        ResolveError::InvalidIdentity { source } => FederationError::IncompleteIdentity(format!(
            "Invalid `AZURE_TENANT_ID` or `AZURE_AUTHORITY_HOST`: {source} See: {AZURE_DOCS}"
        )),
        ResolveError::AccessToken { source } => FederationError::Exchange(source),
        ResolveError::NoIdentity { tried } => {
            FederationError::IncompleteIdentity(azure_no_credential_message(component, &tried))
        }
    }
}

/// The client of an Azure `OpenAI` model or embedding configured for workload identity
/// federation (see [`azure_exchange`]), or, when it has no key, authenticating as the identity
/// the Azure credential chain finds, which sends the current Microsoft Entra token of a
/// refreshing provider, or `None` when the model is not configured for it. `component` names the
/// model or embedding in the logs, such as `model 'gpt' (azure)`.
///
/// # Errors
///
/// A [`FederationError`] for a conflicting, missing or invalid param, an incomplete identity in
/// the environment, or a refused first exchange.
pub async fn azure_federated_client(
    component: &str,
    model: &str,
    federation: &AzureFederation<'_>,
    key_param: Option<&'static str>,
    has_key: bool,
    environment: &AzureEnvironment,
    chat_backend: ChatBackend,
) -> Result<Option<Openai<AzureTokenConfig>>, FederationError> {
    let plan = azure_exchange(federation, key_param, has_key, environment, |audience| {
        SubjectTokenSource::github_actions_from_env(audience).ok()
    })?;
    if plan.is_none() && has_key {
        return Ok(None);
    }
    let Some(endpoint) = non_empty(federation.endpoint) else {
        return Err(FederationError::Config {
            param: "endpoint",
            message: format!(
                "An Azure model needs `endpoint`, the endpoint of its Azure OpenAI resource, such as https://my-resource.openai.azure.com. See: {AZURE_DOCS}"
            ),
        });
    };
    let token = if let Some(plan) = plan {
        let token = token_provider(component, plan.source, plan.exchange).await?;
        tracing::info!("{}", azure_identity_message(component, &plan.origin));
        token
    } else {
        let (authority, scope) = azure_cloud(federation.endpoint);
        let target = azure_credential::EntraTarget {
            authority: environment
                .authority_host
                .clone()
                .unwrap_or_else(|| authority.to_string()),
            scope: scope.to_string(),
        };
        let resolved = azure_credential::resolve(
            component,
            &target,
            &azure_credential::AzureIdentityEnvironment::from_process(),
        )
        .await
        .map_err(|error| azure_chain_error(component, error))?;
        tracing::info!("Authenticated {component} as {}.", resolved.description);
        resolved
            .identity
            .into_token_provider(component, resolved.first_token)
    };
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
            settings_from: "the `anthropic_*` params",
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
            "Invalid value for parameter openai_service_account_id. Workload identity federation needs `openai_identity_provider_id`, `openai_service_account_id`, and `openai_service_account_id` is not set. See: https://spiceai.org/docs/components/models/openai"
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
        federated_token_file: Option<&'a str>,
    ) -> AzureFederation<'a> {
        AzureFederation {
            tenant_id,
            client_id,
            federated_token_audience: None,
            federated_token_file,
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

    /// What an Azure model resolves to: the identity token source, the token endpoint, the
    /// client, the scope and the origin of each part.
    type Resolved = (String, String, String, String, AzureIdentityOrigin);

    /// What an Azure model resolves to, or the message of its error. GitHub Actions offers a
    /// token when `github` is set, whatever job the test runs in.
    fn azure_plan(
        federation: &AzureFederation<'_>,
        key_param: Option<&'static str>,
        has_key: bool,
        environment: &AzureEnvironment,
        github: bool,
    ) -> Result<Option<Resolved>, String> {
        let github_actions = |audience: &str| {
            github.then(|| SubjectTokenSource::GitHubActions {
                request_url: "https://github.invalid/token".to_string(),
                request_token: SecretString::from("request-token"),
                audience: audience.to_string(),
            })
        };
        match azure_exchange(federation, key_param, has_key, environment, github_actions) {
            Ok(None) => Ok(None),
            Ok(Some(AzurePlan {
                source,
                exchange:
                    Exchange::Entra {
                        token_url,
                        client_id,
                        scope,
                    },
                origin,
            })) => Ok(Some((
                format!("{source:?}"),
                token_url,
                client_id,
                scope,
                origin,
            ))),
            Ok(Some(plan)) => panic!("not an Entra exchange: {:?}", plan.exchange),
            Err(error) => Err(message(error)),
        }
    }

    const AKS_TOKEN: &str = r#"File("/var/run/secrets/azure/tokens/azure-identity-token")"#;
    const PUBLIC_SCOPE: &str = "https://cognitiveservices.azure.com/.default";

    #[test]
    fn an_azure_model_with_a_key_ignores_the_environments_workload_identity() {
        assert_eq!(
            azure_plan(
                &azure(None, None, None),
                Some("azure_api_key"),
                true,
                &aks(),
                true
            ),
            Ok(None)
        );
        // A key autoloaded from a secret store is a credential too.
        assert_eq!(
            azure_plan(&azure(None, None, None), None, true, &aks(), true),
            Ok(None)
        );
    }

    #[test]
    fn an_azure_model_without_a_credential_uses_the_aks_workload_identity() {
        assert_eq!(
            azure_plan(&azure(None, None, None), None, false, &aks(), false),
            Ok(Some((
                AKS_TOKEN.to_string(),
                "https://login.microsoftonline.com/aks-tenant/oauth2/v2.0/token".to_string(),
                "aks-client".to_string(),
                PUBLIC_SCOPE.to_string(),
                AzureIdentityOrigin {
                    tenant: "`AZURE_TENANT_ID`",
                    client: "`AZURE_CLIENT_ID`",
                    token: "`AZURE_FEDERATED_TOKEN_FILE`",
                }
            )))
        );
        // Without a tenant the environment holds no identity, so the model has no credential.
        let no_tenant = AzureEnvironment {
            tenant_id: None,
            ..aks()
        };
        assert_eq!(
            azure_plan(&azure(None, None, None), None, false, &no_tenant, true),
            Ok(None)
        );
    }

    #[test]
    fn an_aks_identity_without_its_token_is_left_to_the_azure_credential_chain() {
        let unlabeled_pod = AzureEnvironment {
            federated_token_file: None,
            ..aks()
        };
        // `AZURE_CLIENT_ID` without a token may name a user-assigned managed identity, which the
        // Azure credential chain tries next, as the Azure SDKs do.
        assert_eq!(
            azure_plan(&azure(None, None, None), None, false, &unlabeled_pod, false),
            Ok(None)
        );
        assert_eq!(
            azure_no_credential_message(
                "model 'gpt' (azure)",
                "a workload identity (`AZURE_FEDERATED_TOKEN_FILE` is not set), a managed identity (no answer within 3s) and the Azure CLI (az: command not found)"
            ),
            "Found no credential for model 'gpt' (azure): it sets no `azure_api_key` or `azure_entra_token`, and no Microsoft Entra identity answered. Tried a workload identity (`AZURE_FEDERATED_TOKEN_FILE` is not set), a managed identity (no answer within 3s) and the Azure CLI (az: command not found). Set `azure_api_key`, or give the workload an identity: on AKS, label the pod `azure.workload.identity/use: \"true\"`; on an Azure VM, App Service or Container Apps, assign it a managed identity; locally, run `az login`. See: https://spiceai.org/docs/components/models/azure"
        );
        // In a GitHub Actions job, the job's identity token completes it.
        let plan = azure_plan(&azure(None, None, None), None, false, &unlabeled_pod, true)
            .expect("a plan")
            .expect("federation");
        assert_eq!(
            (plan.0, plan.4.token),
            (
                r#"GitHubActions { audience: "api://AzureADTokenExchange", .. }"#.to_string(),
                "this GitHub Actions job"
            )
        );
    }

    #[test]
    fn azure_federation_params_override_the_environment_and_an_autoloaded_key() {
        let federation = azure(
            Some("contoso.onmicrosoft.com"),
            Some("spicepod-client"),
            Some("/token"),
        );
        assert_eq!(
            azure_plan(&federation, None, true, &aks(), false),
            Ok(Some((
                r#"File("/token")"#.to_string(),
                "https://login.microsoftonline.com/contoso.onmicrosoft.com/oauth2/v2.0/token"
                    .to_string(),
                "spicepod-client".to_string(),
                PUBLIC_SCOPE.to_string(),
                AzureIdentityOrigin {
                    tenant: "`azure_tenant_id`",
                    client: "`azure_client_id`",
                    token: "`azure_federated_token_file`",
                }
            )))
        );
        // A param the Spicepod does not set comes from the environment.
        let plan = azure_plan(
            &azure(None, Some("spicepod-client"), None),
            None,
            false,
            &aks(),
            false,
        )
        .expect("a plan")
        .expect("federation");
        assert_eq!(
            plan.4,
            AzureIdentityOrigin {
                tenant: "`AZURE_TENANT_ID`",
                client: "`azure_client_id`",
                token: "`AZURE_FEDERATED_TOKEN_FILE`",
            }
        );
    }

    #[test]
    fn the_entra_authority_and_scope_follow_the_endpoints_cloud() {
        let in_cloud = |endpoint: &'static str, authority_host: Option<&str>| {
            let federation = AzureFederation {
                endpoint: Some(endpoint),
                ..azure(Some("contoso"), Some("client"), Some("/token"))
            };
            let environment = AzureEnvironment {
                authority_host: authority_host.map(str::to_string),
                ..AzureEnvironment::default()
            };
            let plan = azure_plan(&federation, None, false, &environment, false)
                .expect("a plan")
                .expect("federation");
            (plan.1, plan.3)
        };
        assert_eq!(
            [
                in_cloud("https://my-resource.openai.azure.com", None),
                in_cloud("https://my-resource.cognitiveservices.azure.com/", None),
                in_cloud("https://my-resource.openai.azure.us", None),
                in_cloud("https://MY-RESOURCE.OPENAI.AZURE.CN", None),
                in_cloud(
                    "https://my-resource.openai.azure.us",
                    Some("https://login.example.test")
                ),
            ],
            [
                (
                    "https://login.microsoftonline.com/contoso/oauth2/v2.0/token".to_string(),
                    PUBLIC_SCOPE.to_string()
                ),
                (
                    "https://login.microsoftonline.com/contoso/oauth2/v2.0/token".to_string(),
                    PUBLIC_SCOPE.to_string()
                ),
                (
                    "https://login.microsoftonline.us/contoso/oauth2/v2.0/token".to_string(),
                    "https://cognitiveservices.azure.us/.default".to_string()
                ),
                (
                    "https://login.chinacloudapi.cn/contoso/oauth2/v2.0/token".to_string(),
                    "https://cognitiveservices.azure.cn/.default".to_string()
                ),
                (
                    "https://login.example.test/contoso/oauth2/v2.0/token".to_string(),
                    "https://cognitiveservices.azure.us/.default".to_string()
                ),
            ]
        );
    }

    #[test]
    fn an_http_authority_from_the_environment_is_refused() {
        let environment = AzureEnvironment {
            authority_host: Some("http://login.microsoftonline.com".to_string()),
            ..aks()
        };
        assert_eq!(
            azure_plan(&azure(None, None, None), None, false, &environment, false),
            Err("`AZURE_AUTHORITY_HOST` is not usable: The Microsoft Entra authority must be an https:// URL, such as https://login.microsoftonline.us, because the workload identity token is sent to it. See: https://spiceai.org/docs/components/models/azure".to_string())
        );
    }

    #[test]
    fn azure_federation_params_and_a_key_in_the_spicepod_conflict() {
        assert_eq!(
            azure_plan(
                &azure(Some("tenant"), Some("client"), None),
                Some("azure_entra_token"),
                true,
                &AzureEnvironment::default(),
                true
            ),
            Err("Invalid value for parameter azure_entra_token. A model authenticates with either `azure_entra_token` or workload identity federation (`azure_tenant_id`), and both are set. Remove one of them. See: https://spiceai.org/docs/components/models/azure".to_string())
        );
    }

    #[test]
    fn an_audience_beside_a_token_file_is_rejected() {
        for environment in [AzureEnvironment::default(), aks()] {
            let explicit_file = environment.federated_token_file.is_none();
            let federation = AzureFederation {
                federated_token_audience: Some("api://custom"),
                ..azure(
                    Some("tenant"),
                    Some("client"),
                    explicit_file.then_some("/token"),
                )
            };
            let source = if explicit_file {
                "`azure_federated_token_file`"
            } else {
                "`AZURE_FEDERATED_TOKEN_FILE`"
            };
            assert_eq!(
                azure_plan(&federation, None, false, &environment, true),
                Err(format!(
                    "Invalid value for parameter azure_federated_token_audience. `azure_federated_token_audience` applies only to an identity token Spice requests from GitHub Actions, but the identity token comes from {source}, whose audience is set where it is issued. Remove one of them. See: https://spiceai.org/docs/components/models/azure"
                ))
            );
        }
        // With no file, the audience is the one GitHub Actions is asked for.
        let federation = AzureFederation {
            federated_token_audience: Some("api://custom"),
            ..azure(Some("tenant"), Some("client"), None)
        };
        let plan = azure_plan(&federation, None, false, &AzureEnvironment::default(), true)
            .expect("a plan")
            .expect("federation");
        assert_eq!(plan.0, r#"GitHubActions { audience: "api://custom", .. }"#);
    }

    #[test]
    fn a_missing_or_invalid_azure_tenant_or_token_is_named() {
        assert_eq!(
            azure_plan(
                &azure(None, Some("client"), Some("/token")),
                None,
                false,
                &AzureEnvironment::default(),
                false
            ),
            Err("Invalid value for parameter azure_tenant_id. Workload identity federation needs `azure_tenant_id`, `azure_client_id`, and `azure_tenant_id` is not set. See: https://spiceai.org/docs/components/models/azure".to_string())
        );
        assert_eq!(
            azure_plan(
                &azure(Some("contoso/../common"), Some("client"), Some("/token")),
                None,
                false,
                &AzureEnvironment::default(),
                false
            ),
            Err("Invalid value for parameter azure_tenant_id. The Microsoft Entra tenant must be a directory (tenant) ID, which is a GUID, or a domain name such as 'contoso.onmicrosoft.com'. See: https://spiceai.org/docs/components/models/azure".to_string())
        );
        assert_eq!(
            azure_plan(
                &azure(Some("tenant"), Some("client"), None),
                None,
                false,
                &AzureEnvironment::default(),
                false
            ),
            Err("Invalid value for parameter azure_federated_token_file. Workload identity federation has no identity token: set `azure_federated_token_file` to the file the platform keeps it in, such as a Kubernetes projected service account token, or run in a GitHub Actions job with `permissions: id-token: write`. See: https://spiceai.org/docs/components/models/azure".to_string())
        );
    }

    #[test]
    fn the_load_log_names_where_the_identity_came_from() {
        assert_eq!(
            azure_identity_message(
                "model 'gpt' (azure)",
                &AzureIdentityOrigin {
                    tenant: "`AZURE_TENANT_ID`",
                    client: "`azure_client_id`",
                    token: "this GitHub Actions job",
                }
            ),
            "Authenticated model 'gpt' (azure) as a Microsoft Entra workload identity, with the tenant from `AZURE_TENANT_ID`, the client from `azure_client_id` and the identity token from this GitHub Actions job."
        );
    }

    #[test]
    fn an_api_key_counts_as_set_only_when_the_spicepod_writes_it() {
        let mut raw = HashMap::new();
        assert_eq!(openai_api_key_in_spicepod(&raw), None);
        raw.insert("openai_api_key".to_string(), SecretString::from("sk"));
        assert_eq!(openai_api_key_in_spicepod(&raw), Some("openai_api_key"));
    }

    #[tokio::test]
    async fn an_audience_beside_a_token_file_is_rejected_for_openai_and_anthropic() {
        let federation = OpenAiFederation {
            identity_token_file: Some("/var/run/secrets/tokens/openai"),
            ..openai(
                Some("idp_1"),
                Some("user-1"),
                Some("https://api.openai.com/v1"),
            )
        };
        assert_eq!(
            message(
                openai_token_provider("model 'gpt' (openai)", &federation, None)
                    .await
                    .expect_err("an audience beside a token file")
            ),
            "Invalid value for parameter openai_identity_token_audience. `openai_identity_token_audience` applies only to an identity token Spice requests, from GitHub Actions or the workload's cloud identity, but the identity token comes from `openai_identity_token_file`, whose audience is set where it is issued. Remove one of them. See: https://spiceai.org/docs/components/models/openai"
        );
        let federation = AnthropicFederation {
            identity_token_audience: Some("https://api.anthropic.com"),
            identity_token_file: Some("/var/run/secrets/tokens/anthropic"),
            ..anthropic(Some("00000000-0000-0000-0000-000000000000"))
        };
        assert_eq!(
            message(
                anthropic_token_provider("model 'claude' (anthropic)", &federation, None)
                    .await
                    .expect_err("an audience beside a token file")
            ),
            "Invalid value for parameter anthropic_identity_token_audience. `anthropic_identity_token_audience` applies only to an identity token Spice requests, from GitHub Actions or the workload's cloud identity, but the identity token comes from `anthropic_identity_token_file`, whose audience is set where it is issued. Remove one of them. See: https://spiceai.org/docs/components/models/anthropic"
        );
    }

    #[test]
    fn the_load_line_names_where_the_settings_and_the_token_came_from() {
        assert_eq!(
            federation_message(
                "model 'claude' (anthropic)",
                "an Anthropic service account",
                "the `ANTHROPIC_*` environment variables",
                &SubjectTokenSource::File(PathBuf::from("/var/run/secrets/tokens/anthropic")),
            ),
            "Authenticated model 'claude' (anthropic) as an Anthropic service account through workload identity federation, with the federation settings from the `ANTHROPIC_*` environment variables and the identity token from the file '/var/run/secrets/tokens/anthropic'."
        );
    }

    #[test]
    fn no_identity_token_names_every_source_tried() {
        assert_eq!(
            no_identity_token_message(
                "openai_identity_token_file",
                "AWS (no credentials from environment keys (not set)), Google Cloud (no metadata server answered within 1s) and Azure (a managed identity (no answer within 3s))",
            ),
            "Workload identity federation has no identity token: `openai_identity_token_file` is not set, this is not a GitHub Actions job with `permissions: id-token: write`, and no cloud identity answered: AWS (no credentials from environment keys (not set)), Google Cloud (no metadata server answered within 1s) and Azure (a managed identity (no answer within 3s)). Set `openai_identity_token_file` to the file the platform keeps the token in, such as a Kubernetes projected service account token, or run where the workload has an AWS, Google Cloud or Azure identity"
        );
    }
}
