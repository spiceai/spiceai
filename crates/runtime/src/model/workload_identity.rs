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

//! Workload identity federation for `from: openai` and `from: anthropic` models and embeddings:
//! the params that configure it, validated, and the refreshing token provider they build.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use llms::config::HostedModelConfig;
use llms::openai::{ChatBackend, Openai, UsageTier};
use secrecy::SecretString;
use token_provider::TokenProvider;
use token_provider::workload_identity::{
    ANTHROPIC_AUDIENCE, Exchange, SubjectTokenSource, WorkloadIdentityError,
    WorkloadIdentityTokenProvider,
};

const OPENAI_DOCS: &str = "https://spiceai.org/docs/components/models/openai";
const ANTHROPIC_DOCS: &str = "https://spiceai.org/docs/components/models/anthropic";

/// Why a model's workload identity federation could not be set up.
#[derive(Debug)]
pub(crate) enum FederationError {
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
    pub(crate) fn into_llm_error(self) -> llms::chat::Error {
        match self {
            Self::Config { param, message } => llms::chat::Error::InvalidParamValueError {
                param: param.to_string(),
                message,
            },
            Self::Exchange(source) => llms::chat::Error::FailedToAuthenticate {
                source: Box::new(source),
            },
            Self::Client(source) => llms::chat::Error::FailedToLoadModel {
                source: Box::new(source),
            },
        }
    }

    pub(crate) fn into_embed_error(self) -> llms::embeddings::Error {
        llms::embeddings::Error::FailedToInstantiateEmbeddingModel {
            source: Box::new(self.into_llm_error()),
        }
    }
}

/// The `openai_*` federation params of a model or embedding, and the settings of the client
/// built with them.
pub(crate) struct OpenAiFederation<'a> {
    pub(crate) identity_provider_id: Option<&'a str>,
    pub(crate) service_account_id: Option<&'a str>,
    pub(crate) identity_token_audience: Option<&'a str>,
    pub(crate) identity_token_file: Option<&'a str>,
    pub(crate) endpoint: &'a str,
    pub(crate) org_id: Option<&'a str>,
    pub(crate) project_id: Option<&'a str>,
    pub(crate) usage_tier: UsageTier,
}

/// The `anthropic_*` federation params of a model.
pub(crate) struct AnthropicFederation<'a> {
    pub(crate) federation_rule_id: Option<&'a str>,
    pub(crate) organization_id: Option<&'a str>,
    pub(crate) service_account_id: Option<&'a str>,
    pub(crate) workspace_id: Option<&'a str>,
    pub(crate) identity_token_audience: Option<&'a str>,
    pub(crate) identity_token_file: Option<&'a str>,
}

/// `openai_api_key` when the Spicepod itself sets it, as opposed to a key autoloaded from a secret
/// store: federation params win over an autoloaded key, but not over one the user wrote.
pub(crate) fn openai_api_key_in_spicepod(
    raw_params: &HashMap<String, SecretString>,
) -> Option<&'static str> {
    raw_params
        .contains_key("openai_api_key")
        .then_some("openai_api_key")
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
            let names: Vec<String> = required.iter().map(|(name, _)| format!("`{name}`")).collect();
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
    source: SubjectTokenSource,
    exchange: Exchange,
) -> Result<Arc<dyn TokenProvider>, FederationError> {
    let provider = WorkloadIdentityTokenProvider::try_new(source, exchange)
        .await
        .map_err(FederationError::Exchange)?;
    Ok(Arc::new(provider))
}

/// The token provider of an `OpenAI` model configured for workload identity federation, or `None`
/// when no federation param is set.
async fn openai_token_provider(
    federation: &OpenAiFederation<'_>,
    key_param: Option<&'static str>,
) -> Result<Option<Arc<dyn TokenProvider>>, FederationError> {
    let Some([provider_id, service_account_id, audience]) = validate(
        [
            ("openai_identity_provider_id", federation.identity_provider_id),
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
    token_provider(source, Exchange::openai(provider_id, service_account_id))
        .await
        .map(Some)
}

/// The client of an `OpenAI` model or embedding configured for workload identity federation,
/// which sends the current token of a refreshing provider, or `None` when no federation param is
/// set.
pub(crate) async fn openai_federated_client(
    model: &str,
    federation: &OpenAiFederation<'_>,
    key_param: Option<&'static str>,
    chat_backend: ChatBackend,
) -> Result<Option<Openai<HostedModelConfig>>, FederationError> {
    let Some(token) = openai_token_provider(federation, key_param).await? else {
        return Ok(None);
    };
    llms::openai::new_openai_client_with_token_provider(
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
/// `None` when no federation param is set.
pub(crate) async fn anthropic_token_provider(
    federation: &AnthropicFederation<'_>,
    key_param: Option<&'static str>,
) -> Result<Option<Arc<dyn TokenProvider>>, FederationError> {
    let Some([rule_id, organization_id, service_account_id]) = validate(
        [
            ("anthropic_federation_rule_id", federation.federation_rule_id),
            ("anthropic_organization_id", federation.organization_id),
            ("anthropic_service_account_id", federation.service_account_id),
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
        source,
        Exchange::anthropic(rule_id, organization_id, service_account_id, workspace_id),
    )
    .await
    .map(Some)
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
        let none = openai_token_provider(&openai(None, None, None), Some("openai_api_key"))
            .await
            .expect("no federation");
        assert!(none.is_none(), "an API key model stays an API key model");
    }

    #[tokio::test]
    async fn federation_and_an_api_key_in_the_spicepod_conflict() {
        let error = openai_token_provider(
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
        let error = openai_token_provider(&openai(Some("idp_1"), None, Some("aud")), None)
            .await
            .expect_err("a missing service account");
        assert_eq!(
            message(error),
            "Invalid value for parameter openai_service_account_id. Workload identity federation needs `openai_identity_provider_id`, `openai_service_account_id`, `openai_identity_token_audience`, and `openai_service_account_id` is not set. See: https://spiceai.org/docs/components/models/openai"
        );

        let error = anthropic_token_provider(&anthropic(Some("")), None)
            .await
            .expect_err("an empty organization");
        assert_eq!(
            message(error),
            "Invalid value for parameter anthropic_organization_id. Workload identity federation needs `anthropic_federation_rule_id`, `anthropic_organization_id`, `anthropic_service_account_id`, and `anthropic_organization_id` is not set. See: https://spiceai.org/docs/components/models/anthropic"
        );
    }

    #[tokio::test]
    async fn an_anthropic_key_and_federation_conflict() {
        let error = anthropic_token_provider(&anthropic(Some("org")), Some("anthropic_auth_token"))
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

    #[test]
    fn an_api_key_counts_as_set_only_when_the_spicepod_writes_it() {
        let mut raw = HashMap::new();
        assert_eq!(openai_api_key_in_spicepod(&raw), None);
        raw.insert("openai_api_key".to_string(), SecretString::from("sk"));
        assert_eq!(openai_api_key_in_spicepod(&raw), Some("openai_api_key"));
    }
}
