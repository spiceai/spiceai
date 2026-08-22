/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
#![allow(clippy::implicit_hasher)]
#[cfg(feature = "bedrock")]
use llms::bedrock::chat::{BedrockConverse, guardrail::GuardRail};
#[cfg(feature = "models")]
use llms::chat::DistributedBackendSetting;
use llms::{
    HealthCheck,
    anthropic::Anthropic,
    chat::{Chat, Error as LlmError},
    google::Google,
    xai::Xai,
};
use llms::{config::GenericAuthMechanism, openai::DEFAULT_LLM_MODEL};
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use snafu::ResultExt;
#[cfg(feature = "models")]
use spicepod::component::model::ModelFileType;
use spicepod::component::model::{Model, ModelSource};
#[cfg(feature = "models")]
use std::path::PathBuf;
#[cfg(feature = "models")]
use std::str::FromStr;
use std::{collections::HashMap, sync::Arc};
use token_provider::registry::TokenProviderRegistry;
use tokio::sync::RwLock;

use super::params::anthropic::AnthropicModelParams;
use super::params::azure::AzureModelParams;
#[cfg(feature = "bedrock")]
use super::params::bedrock::{BedrockModelParams, GuardrailTraceMode};
use super::params::databricks::DatabricksModelParams;
#[cfg(feature = "models")]
use super::params::file::FileModelParams;
use super::params::google::GoogleModelParams;
#[cfg(feature = "models")]
use super::params::huggingface::HuggingFaceModelParams;
use super::params::openai::OpenAiModelParams;
use super::params::orcarouter::OrcaRouterModelParams;
use super::params::spiceai::SpiceAiModelParams;
use super::params::xai::XaiModelParams;
use super::wrapper::OPENAI_DEFAULT_PARAM_KEYS;
use super::{tool_use::ToolUsingChat, wrapper::ChatWrapper};
use crate::token_providers::databricks::{DatabricksM2MTokenProvider, DatabricksU2MTokenProvider};
use crate::{
    Runtime,
    tools::{
        registry::{TOOL_EMBEDDING_MODEL_PARAM, prepare_model_tools},
        utils::{create_table_allowlist, get_tools_with_allowlist},
    },
};
use runtime_parameters_typed::TypedParams;
use runtime_secrets::Secrets;
use runtime_tools::options::SpiceToolsOptions;

pub type LLMChatCompletionsModelStore = HashMap<String, Arc<dyn Chat>>;

// Default recursion limit for tool usage to prevent infinite loops.
// This limit can be adjusted using the `tool_recursion_limit` model parameter.
const DEFAULT_SPICE_TOOL_RECURSION_LIMIT: usize = 10;

/// Extract a secret from a hashmap of secrets, if it exists.
macro_rules! extract_secret {
    ($params:expr, $key:expr) => {
        $params.get($key).map(secrecy::ExposeSecret::expose_secret)
    };
}

/// Attempt to derive a runnable Chat model from a given component from the Spicepod definition.
pub async fn try_to_chat_model(
    component: &Model,
    params: &HashMap<String, SecretString>,
    rt: Arc<Runtime>,
) -> Result<Arc<dyn Chat>, LlmError> {
    let secrets = rt.secrets();
    let model = construct_model(component, params, &secrets, rt.token_provider_registry()).await?;

    // Handle tool usage
    let spice_tool_opt: Option<SpiceToolsOptions> = extract_secret!(params, "tools")
        .or(extract_secret!(params, "spice_tools"))
        .map(str::parse)
        .transpose()
        .map_err(|_| unreachable!("SpiceToolsOptions::from_str has no error condition"))?;

    let spice_recursion_limit: Option<usize> = extract_secret!(params, "tool_recursion_limit")
        .map(|x| {
            x.parse().map_err(|e| LlmError::FailedToLoadModel {
                source: format!(
                    "Invalid value specified for `params.recursion_depth`: {x}. Error: {e}"
                )
                .into(),
            })
        })
        .transpose()?
        // Prevent infinite recursion in case of circular tool calls.
        .or(Some(DEFAULT_SPICE_TOOL_RECURSION_LIMIT));

    let tool_embedding_model = extract_secret!(params, TOOL_EMBEDDING_MODEL_PARAM);

    let tool_model = match spice_tool_opt {
        Some(opts) if opts.can_use_tools() => {
            let table_allowlist = create_table_allowlist(&component.datasets).map_err(|e| {
                LlmError::ModelParameterFailed {
                    model: component.name.clone(),
                    source: e,
                }
            })?;
            let tools = get_tools_with_allowlist(Arc::clone(&rt), &opts, table_allowlist).await;
            let tools = prepare_model_tools(Arc::clone(&rt), &opts, tools, tool_embedding_model)
                .await
                .map_err(|e| LlmError::FailedToLoadModel { source: e })?;
            Arc::new(ToolUsingChat::new(
                model,
                Arc::clone(&rt),
                tools,
                spice_recursion_limit,
            ))
        }
        Some(_) | None => model,
    };
    Ok(tool_model)
}

/// Deserializes the source's typed params from the (already secret-resolved)
/// spicepod params map, mapping a [`ParamsError`](runtime_parameters_typed::ParamsError)
/// to [`LlmError::ModelParameterFailed`].
async fn typed_params<P: TypedParams>(
    component: &Model,
    params: &HashMap<String, SecretString>,
    source: ModelSource,
    secrets: &Arc<RwLock<Secrets>>,
) -> Result<P, LlmError> {
    P::try_from_params(&format!("model {source}"), params.clone(), secrets)
        .await
        .map_err(|e| LlmError::ModelParameterFailed {
            model: component.name.clone(),
            source: Box::new(e),
        })
}

pub async fn construct_model(
    component: &spicepod::component::model::Model,
    params: &HashMap<String, SecretString>,
    secrets: &Arc<RwLock<Secrets>>,
    token_registry: Arc<TokenProviderRegistry>,
) -> Result<Arc<dyn Chat>, LlmError> {
    let model_id = component.get_model_id();
    let source = component.get_source().ok_or(LlmError::UnknownModelSource {
        from: component.from.clone(),
    })?;

    let model = match source {
        #[cfg(feature = "models")]
        ModelSource::HuggingFace => {
            let p =
                typed_params::<HuggingFaceModelParams>(component, params, source.clone(), secrets)
                    .await?;
            huggingface(model_id, component, &p).await
        }
        #[cfg(not(feature = "models"))]
        ModelSource::HuggingFace => Err(LlmError::UnknownModelSource {
            from: "huggingface".into(),
        }),
        #[cfg(feature = "models")]
        ModelSource::File => {
            let p =
                typed_params::<FileModelParams>(component, params, source.clone(), secrets).await?;
            file(component, &p).await
        }
        #[cfg(not(feature = "models"))]
        ModelSource::File => Err(LlmError::UnknownModelSource {
            from: "file".into(),
        }),
        ModelSource::Anthropic => {
            let p =
                typed_params::<AnthropicModelParams>(component, params, source.clone(), secrets)
                    .await?;
            anthropic(model_id.as_deref(), &p)
        }
        ModelSource::Google => {
            let p = typed_params::<GoogleModelParams>(component, params, source.clone(), secrets)
                .await?;
            google(model_id.as_deref(), &p)
        }
        ModelSource::Azure => {
            let p = typed_params::<AzureModelParams>(component, params, source.clone(), secrets)
                .await?;
            azure(model_id, component.name.as_str(), &p)
        }
        ModelSource::Xai => {
            let p =
                typed_params::<XaiModelParams>(component, params, source.clone(), secrets).await?;
            xai(model_id.as_deref(), &p)
        }
        ModelSource::OpenAi => {
            let p = typed_params::<OpenAiModelParams>(component, params, source.clone(), secrets)
                .await?;
            openai(model_id, params, &p)
        }
        ModelSource::Databricks => {
            let p =
                typed_params::<DatabricksModelParams>(component, params, source.clone(), secrets)
                    .await?;
            databricks(model_id, &p, Arc::clone(&token_registry)).await
        }
        #[cfg(feature = "bedrock")]
        ModelSource::Bedrock => {
            let p = typed_params::<BedrockModelParams>(component, params, source.clone(), secrets)
                .await?;
            bedrock(model_id, &p).await
        }
        #[cfg(not(feature = "bedrock"))]
        ModelSource::Bedrock => Err(LlmError::UnknownModelSource {
            from: "bedrock".into(),
        }),
        ModelSource::SpiceAI => {
            let p = typed_params::<SpiceAiModelParams>(component, params, source.clone(), secrets)
                .await?;
            spiceai(model_id, &p)
        }
        ModelSource::OrcaRouter => {
            let p =
                typed_params::<OrcaRouterModelParams>(component, params, source.clone(), secrets)
                    .await?;
            orcarouter(model_id, &p)
        }
    }?;

    let system_prompt = match component.params.get("system_prompt") {
        Some(Value::String(s)) => Some(s.as_str()),
        Some(v) => {
            return Err(LlmError::InvalidParamValueError {
                param: "system_prompt".to_string(),
                message: format!("Expected a string, got: {v:?}"),
            });
        }
        None => None,
    };
    let mut wrapper = ChatWrapper::new(
        model,
        component.name.as_str(),
        system_prompt,
        get_openai_request_overrides(component, source.short_name()),
    );

    if let Some(Value::String(s)) = component.params.get("parameterized_prompt")
        && matches!(s.as_str(), "enabled")
    {
        wrapper = wrapper.allowed_to_parameterise();
    }

    Ok(Arc::new(wrapper))
}

#[cfg(feature = "bedrock")]
async fn bedrock(
    model_id: Option<String>,
    params: &BedrockModelParams,
) -> Result<Arc<dyn Chat>, LlmError> {
    let Some(model_id) = model_id else {
        return Err(LlmError::ModelNotProvided {
            model_source: "bedrock".to_string(),
        });
    };

    let client = super::util::create_bedrock_client(&params.runtime_params(), "bedrock-chat")
        .await
        .map_err(|e| LlmError::FailedToLoadModel { source: e })?;

    let id = params.guardrail_identifier.as_deref();
    let version = params.guardrail_version.as_deref();
    let trace = params.trace.map(GuardrailTraceMode::as_str);
    let mut converse = BedrockConverse::new(client.into(), model_id);

    // Add Guardrail if added by user.
    if let (Some(id), Some(version)) = (id, version) {
        let g = GuardRail::try_new(id, version, trace)
            .boxed()
            .map_err(|e| LlmError::FailedToLoadModel { source: e })?;
        converse = converse.with_guardrail(g);
    }

    Ok(Arc::new(converse) as Arc<dyn Chat>)
}

fn xai(model_id: Option<&str>, params: &XaiModelParams) -> Result<Arc<dyn Chat>, LlmError> {
    let Some(api_key) = params.api_key.as_ref().map(ExposeSecret::expose_secret) else {
        return Err(LlmError::FailedToLoadModel {
            source: "No `xai_api_key` provided for xAI model.".into(),
        });
    };
    Ok(Arc::new(Xai::new(model_id, api_key)) as Arc<dyn Chat>)
}

fn anthropic(
    model_id: Option<&str>,
    params: &AnthropicModelParams,
) -> Result<Arc<dyn Chat>, LlmError> {
    let api_base = params.endpoint.as_deref();
    let api_key = params.api_key.as_ref().map(ExposeSecret::expose_secret);
    let auth_token = params.auth_token.as_ref().map(ExposeSecret::expose_secret);

    let auth = match (api_key, auth_token) {
        (Some(s), None) => GenericAuthMechanism::from_api_key(s),
        (None, Some(s)) => GenericAuthMechanism::from_bearer_token(s),
        (None, None) => return Err(LlmError::FailedToLoadModel {
            source: "One of following `model.params` is required: `anthropic_api_key` or `anthropic_auth_token`.".into(),
        }),
        (Some(_), Some(_)) => return Err(LlmError::FailedToLoadModel {
            source: "Only one of following `model.params` is allowed: `anthropic_api_key` or `anthropic_auth_token`.".into(),
        }),
    };

    let anthropic = Anthropic::new(auth, model_id, api_base, None).map_err(|_| {
        LlmError::FailedToLoadModel {
            source: format!("Unknown anthropic model: {:?}", model_id.clone()).into(),
        }
    })?;

    Ok(Arc::new(anthropic) as Arc<dyn Chat>)
}

fn google(model_id: Option<&str>, params: &GoogleModelParams) -> Result<Arc<dyn Chat>, LlmError> {
    let Some(model_id) = model_id else {
        return Err(LlmError::ModelNotProvided {
            model_source: "google".to_string(),
        });
    };
    let Some(api_key) = params.api_key.as_ref() else {
        return Err(LlmError::FailedToLoadModel {
            source: "`model.params.google_api_key` is required.".into(),
        });
    };

    let google = Google::new(api_key, model_id).map_err(|e| LlmError::FailedToLoadModel {
        source: format!("Failed to create Google client: {e}").into(),
    })?;

    Ok(Arc::new(google) as Arc<dyn Chat>)
}

#[cfg(feature = "models")]
async fn huggingface(
    model_id: Option<String>,
    component: &spicepod::component::model::Model,
    params: &HuggingFaceModelParams,
) -> Result<Arc<dyn Chat>, LlmError> {
    let Some(id) = model_id else {
        return Err(LlmError::FailedToLoadModel {
            source: "No model id for Huggingface model".to_string().into(),
        });
    };

    let model_type = params.model_type.as_deref();
    let hf_token = params.token.as_ref();

    // For GGUF models, we require user specify via `.files[].path`
    let gguf_path = component
        .find_all_file_path(ModelFileType::Weights)
        .iter()
        .find_map(|p| {
            let path = PathBuf::from_str(p.as_str());
            if let Ok(Some(ext)) = path.as_ref().map(|pp| pp.extension())
                && ext.eq_ignore_ascii_case("gguf")
            {
                return PathBuf::from_str(p.as_str()).ok();
            }
            None
        });

    if let Some(ref path) = gguf_path {
        tracing::debug!(
            "For Huggingface model {}, the GGUF model {} will be downloaded and used.",
            component.name,
            path.display()
        );
    }

    let chat_template_literal = params.chat_template.as_deref();
    let distributed = parse_distributed_config(
        params.distributed_backend,
        params.node_rank.as_deref(),
        params.nodes.as_deref(),
    )?;

    llms::chat::create_hf_model(
        &id,
        model_type,
        gguf_path,
        hf_token,
        chat_template_literal,
        distributed,
    )
    .await
}

/// Parse the optional multi-node distributed-inference params (`distributed_backend`,
/// `node_rank`, `nodes`) for a Huggingface model into a [`llms::chat::DistributedConfig`].
/// Returns `Ok(None)` when distributed mode is not requested.
#[cfg(feature = "models")]
fn parse_distributed_config(
    distributed_backend: DistributedBackendSetting,
    node_rank: Option<&str>,
    nodes: Option<&str>,
) -> Result<Option<llms::chat::DistributedConfig>, LlmError> {
    let backend = match distributed_backend {
        DistributedBackendSetting::None => {
            // Distributed is off: reject orphan topology params so forgetting (or
            // mistyping) `distributed_backend` doesn't silently run single-node
            // while `nodes`/`node_rank` look configured.
            if nodes.is_some() || node_rank.is_some() {
                return Err(LlmError::InvalidParamValueError {
                    param: "distributed_backend".to_string(),
                    message: "`nodes`/`node_rank` are set but `distributed_backend` is not `ring`; set `distributed_backend: ring` to enable multi-node inference, or remove `nodes`/`node_rank`.".to_string(),
                });
            }
            return Ok(None);
        }
        DistributedBackendSetting::Ring => llms::chat::DistributedBackend::Ring,
    };

    let node_rank = match node_rank.map(str::trim) {
        None | Some("") => 0,
        Some(raw) => raw
            .parse::<usize>()
            .map_err(|_| LlmError::InvalidParamValueError {
                param: "node_rank".to_string(),
                message: format!("Must be a non-negative integer, got '{raw}'"),
            })?,
    };

    let nodes: Vec<String> = nodes
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default();
    if nodes.is_empty() {
        return Err(LlmError::InvalidParamValueError {
            param: "nodes".to_string(),
            message:
                "`distributed_backend: ring` requires `nodes`: a comma-separated, rank-ordered list of node addresses (e.g. `10.0.0.1,10.0.0.2`)."
                    .to_string(),
        });
    }

    let config = llms::chat::DistributedConfig {
        backend,
        node_rank,
        nodes,
    };
    config
        .validate()
        .map_err(|(param, message)| LlmError::InvalidParamValueError {
            param: param.to_string(),
            message,
        })?;
    Ok(Some(config))
}

async fn databricks(
    model_id: Option<String>,
    params: &DatabricksModelParams,
    token_provider_registry: Arc<TokenProviderRegistry>,
) -> Result<Arc<dyn Chat>, LlmError> {
    // Required parameters
    let Some(endpoint) = params.endpoint.as_deref() else {
        return Err(LlmError::MissingParamError {
            param_key: "databricks_endpoint",
        });
    };
    let Some(model_id) = model_id else {
        return Err(LlmError::ModelNotProvided {
            model_source: "databricks".to_string(),
        });
    };

    // Optional parameters.
    let token_opt = params.token.as_ref().map(ExposeSecret::expose_secret);
    let client_id = params.client_id.as_deref();
    let client_secret = params
        .client_secret
        .as_ref()
        .map(ExposeSecret::expose_secret);

    #[cfg(feature = "databricks")]
    let user_agent = Some(data_components::databricks::user_agent());
    #[cfg(not(feature = "databricks"))]
    let user_agent: Option<&'static str> = None;

    match (token_opt, client_id, client_secret) {
        (Some(_), Some(_) | None, Some(_)) => {
            Err(LlmError::FailedToLoadModel {
                source: "Either `databricks_token` or `databricks_client_id` and `databricks_client_secret` should be provided, not both.".into(),
            })
        }
        (Some(_), Some(_), None) | (None, None, None) => {
            Err(LlmError::FailedToLoadModel {
                source: "Either `databricks_token` or `databricks_client_id` and `databricks_client_secret` should be provided.".into(),
            })
        }
        (None, None, Some(_client_secret)) => {
            Err(LlmError::FailedToLoadModel {
                source: "If `databricks_client_secret` is provided, `databricks_client_id` must also be provided.".into(),
            })
        }
        (Some(token), None, None) => Ok(Arc::new(llms::databricks::from_access_token(
            endpoint,
            model_id.as_str(),
            token,
            user_agent,
        )) as Arc<dyn Chat>),
        (None, Some(client_id), Some(client_secret)) => {
            let token_provider = token_provider_registry
                .get_or_create_provider(format!("databricks_m2m_{endpoint}_{client_id}"), || async {
                    DatabricksM2MTokenProvider::try_new(
                        endpoint.to_string(),
                        client_id.to_string(),
                        client_secret.into(),
                    )
                    .await
                })
                .await
            .map_err(|e| LlmError::FailedToLoadModel {
                source: Box::from(format!(
                    "Could not retrieve M2M tokens from Databricks. Error: {e}"
                )),
            })?;
            Ok(Arc::new(
                llms::databricks::from_token_provider(
                    endpoint,
                    model_id.as_str(),
                    token_provider,
                    user_agent,
                    HealthCheck::Required,
                )
            ) as Arc<dyn Chat>)
        }
        (None, Some(client_id), None) => {
            let token_provider = token_provider_registry
                .get_or_create_provider::<DatabricksU2MTokenProvider, std::convert::Infallible, _, _>(format!("databricks_u2m_{endpoint}_{client_id}"), || async {
                    Ok(DatabricksU2MTokenProvider::new(
                        endpoint.to_string(),
                        client_id.to_string(),
                    ))
                })
                .await.boxed().map_err(|e| LlmError::FailedToLoadModel {
                source: Box::from(format!(
                    "Could not retrieve U2M tokens from Databricks. Error: {e}"
                )),
            })?;

            Ok(Arc::new(
                llms::databricks::from_token_provider(
                    endpoint,
                    model_id.as_str(),
                    token_provider,
                    user_agent,
                    HealthCheck::Skip,
                ),
            ) as Arc<dyn Chat>)
        }
    }
}

/// Builds a chat model served by the Spice.ai Cloud Platform, or by another Spice runtime
/// (a Spice-to-Spice connection). Both expose an `OpenAI`-compatible API under `/v1`.
fn spiceai(
    model_id: Option<String>,
    params: &SpiceAiModelParams,
) -> Result<Arc<dyn Chat>, LlmError> {
    // Treat a blank id the same as a missing one: a client built with an empty model name fails
    // later with a far less obvious error.
    let Some(model_id) = model_id.filter(|id| !id.trim().is_empty()) else {
        return Err(LlmError::ModelNotProvided {
            model_source: "spiceai".to_string(),
        });
    };

    // The spec default guarantees `endpoint` is always set (the Spice.ai Cloud
    // Platform when the user leaves it unset).
    let endpoint = params.endpoint.as_str();
    let api_key = params.api_key.as_ref().map(ExposeSecret::expose_secret);

    // A self-hosted Spice runtime may not require authentication, but the Spice.ai Cloud Platform
    // always does — so an unset key there is a misconfiguration, not a valid anonymous setup.
    if api_key.is_none() && llms::spiceai::is_cloud_platform(Some(endpoint)) {
        return Err(LlmError::FailedToLoadModel {
            source: "Missing `spiceai_api_key`. Models served by the Spice.ai Cloud Platform require an API key. Set `spiceai_api_key`, or set `spiceai_endpoint` to the Spice runtime serving the model. See: https://spiceai.org/docs/components/models".into(),
        });
    }

    Ok(Arc::new(llms::spiceai::new_spiceai_client(
        model_id,
        Some(endpoint),
        api_key,
    )) as Arc<dyn Chat>)
}

/// Builds a chat model served by the OrcaRouter AI gateway, which exposes an
/// `OpenAI`-compatible API under `/v1`.
fn orcarouter(
    model_id: Option<String>,
    params: &OrcaRouterModelParams,
) -> Result<Arc<dyn Chat>, LlmError> {
    let Some(model_id) = model_id.filter(|id| !id.trim().is_empty()) else {
        return Err(LlmError::ModelNotProvided {
            model_source: "orcarouter".to_string(),
        });
    };

    // The gateway always authenticates, so an unset key is a misconfiguration.
    let api_key = params.api_key.as_ref().map(ExposeSecret::expose_secret);
    if api_key.is_none() {
        return Err(LlmError::FailedToLoadModel {
            source: "Missing `orcarouter_api_key`. Models served by the OrcaRouter AI gateway require an API key. Set `orcarouter_api_key`. See: https://www.orcarouter.ai".into(),
        });
    }

    Ok(Arc::new(llms::orcarouter::new_orcarouter_client(
        model_id,
        Some(params.endpoint.as_str()),
        api_key,
    )) as Arc<dyn Chat>)
}

fn openai(
    model_id: Option<String>,
    raw_params: &HashMap<String, SecretString>,
    params: &OpenAiModelParams,
) -> Result<Arc<dyn Chat>, LlmError> {
    let api_base = Some(params.endpoint.as_str());
    let api_key = params.api_key.as_ref().map(ExposeSecret::expose_secret);
    let org_id = params.org_id.as_deref();
    let project_id = params.project_id.as_deref();
    let usage_tier = Some(params.usage_tier);
    let chat_backend = params.responses_api;

    validate_temperature(raw_params, "openai")?;

    Ok(Arc::new(llms::openai::new_openai_client_with_chat_backend(
        model_id.unwrap_or(DEFAULT_LLM_MODEL.to_string()),
        api_base,
        api_key,
        org_id,
        project_id,
        usage_tier,
        chat_backend,
    )) as Arc<dyn Chat>)
}

/// Rejects a negative or unparseable `temperature` override at load time rather
/// than deferring to a request-time provider error. The value is read from the
/// raw params map because overrides are passthrough (see [`super::params::common`]),
/// accepting the unprefixed, `{prefix}_`-prefixed, and legacy `openai_` forms.
fn validate_temperature(
    raw_params: &HashMap<String, SecretString>,
    prefix: &str,
) -> Result<(), LlmError> {
    let temperature = raw_params
        .get("temperature")
        .or_else(|| raw_params.get(&format!("{prefix}_temperature")))
        .or_else(|| raw_params.get("openai_temperature"))
        .map(ExposeSecret::expose_secret);

    if let Some(temperature_str) = temperature
        && !matches!(temperature_str.parse::<f64>(), Ok(t) if t >= 0.0)
    {
        return Err(LlmError::InvalidParamValueError {
            param: "openai_temperature".to_string(),
            message: "Ensure it is a non-negative number.".to_string(),
        });
    }
    Ok(())
}

fn azure(
    model_id: Option<String>,
    model_name: &str,
    params: &AzureModelParams,
) -> Result<Arc<dyn Chat>, LlmError> {
    let Some(model_name) = model_id else {
        return Err(LlmError::FailedToLoadModel {
            source: format!(
    "Azure model '{model_name}' requires a model ID in the format `from:azure:<model_id>`. See https://spiceai.org/docs/components/models/azure for details."
).into(),
        });
    };
    let api_base = params.endpoint.as_deref();
    let api_version = params.api_version.as_deref();
    let deployment_name = params.deployment_name.as_deref();
    let api_key = params.api_key.as_ref().map(ExposeSecret::expose_secret);
    let entra_token = params.entra_token.as_ref().map(ExposeSecret::expose_secret);
    let chat_backend = params.responses_api;

    if api_base.is_none() {
        return Err(LlmError::FailedToLoadModel {
            source: format!(
    "Azure model '{model_name}' requires the 'endpoint' parameter. See https://spiceai.org/docs/components/models/azure for details."
).into(),
        });
    }

    if api_key.is_some() && entra_token.is_some() {
        return Err(LlmError::FailedToLoadModel {
            source: format!(
                "Azure model '{model_name}' allows only one of 'azure_api_key' or 'azure_entra_token'. See https://spiceai.org/docs/components/models/azure for details."
            )
            .into(),
        });
    }

    if api_key.is_none() && entra_token.is_none() {
        return Err(LlmError::FailedToLoadModel {
            source: format!(
                "Azure model '{model_name}' requires either 'azure_api_key' or 'azure_entra_token'. See https://spiceai.org/docs/components/models/azure for details."
            )
            .into(),
        });
    }

    Ok(Arc::new(llms::openai::new_azure_client_with_chat_backend(
        model_name,
        api_base,
        api_version,
        deployment_name,
        entra_token,
        api_key,
        chat_backend,
    )) as Arc<dyn Chat>)
}

#[cfg(feature = "models")]
async fn file(
    component: &spicepod::component::model::Model,
    params: &FileModelParams,
) -> Result<Arc<dyn Chat>, LlmError> {
    let model_weights = component.find_all_file_path(ModelFileType::Weights);
    if model_weights.is_empty() {
        return Err(LlmError::FailedToLoadModel {
            source: "No 'weights_path' parameter provided".into(),
        });
    }

    llms::chat::reject_unsafe_weight_formats(
        model_weights.as_slice(),
        params.trust_pickle.is_trusted(),
    )
    .map_err(|source| LlmError::FailedToLoadModel {
        source: Box::new(source),
    })?;

    let tokenizer_path = component.find_any_file_path(ModelFileType::Tokenizer);
    let tokenizer_config_path = component.find_any_file_path(ModelFileType::TokenizerConfig);
    let config_path = component.find_any_file_path(ModelFileType::Config);
    let generation_config = component.find_any_file_path(ModelFileType::GenerationConfig);
    let distributed = parse_distributed_config(
        params.distributed_backend,
        params.node_rank.as_deref(),
        params.nodes.as_deref(),
    )?;
    let context_length = parse_context_length(params)?;
    let paged_attention = params.paged_attention;

    let chat_template_literal = params.chat_template.as_deref();

    llms::chat::create_local_model(
        model_weights.as_slice(),
        config_path.as_deref(),
        tokenizer_path.as_deref(),
        tokenizer_config_path.as_deref(),
        generation_config.as_deref(),
        distributed,
        llms::chat::LocalModelOptions {
            chat_template_literal,
            context_length,
            paged_attention,
        },
    )
    .await
}

/// Parse the optional `context_length` model parameter (maximum sequence length,
/// in tokens) for locally served models. Returns `None` when unset or empty, so
/// the engine default applies. Rejects non-integer or zero values.
#[cfg(feature = "models")]
fn parse_context_length(params: &FileModelParams) -> Result<Option<usize>, LlmError> {
    let raw = params.context_length.as_deref().unwrap_or_default().trim();
    if raw.is_empty() {
        return Ok(None);
    }
    match raw.parse::<usize>() {
        Ok(n) if n > 0 => Ok(Some(n)),
        _ => Err(LlmError::InvalidParamValueError {
            param: "context_length".to_string(),
            message: format!("Must be a positive integer number of tokens, got '{raw}'"),
        }),
    }
}

// Get OpenAI compatible request parameter overrides.
// Prioritizes parameters without prefix, then model prefix (e.g., `hf_temperature`), then deprecated (e.g. `openai_temperature`) parameters.
pub fn get_openai_request_overrides(model: &Model, prefix: &str) -> Vec<(String, Value)> {
    let mut request_overrides: HashMap<String, Value> = HashMap::new();
    for &key in OPENAI_DEFAULT_PARAM_KEYS.iter() {
        if let Some(v) = model.params.get(key) {
            request_overrides.insert(key.to_string(), v.clone());
        } else if let Some(v) = model.params.get(&format!("{prefix}_{key}")) {
            request_overrides.insert(key.to_string(), v.clone());
        } else if let Some(v) = model.params.get(&format!("openai_{key}")) {
            request_overrides.insert(key.to_string(), v.clone());
        }
    }

    request_overrides.into_iter().collect()
}

#[cfg(test)]
mod test {
    use super::*;
    #[cfg(feature = "models")]
    use llms::chat::PagedAttentionMode;
    use serde_json::Number;
    use spicepod::component::model::Model;

    /// Builds a [`FileModelParams`] from key/value pairs for testing the local-model
    /// parsers in isolation from spicepod deserialization.
    #[cfg(feature = "models")]
    async fn file_params(pairs: &[(&str, &str)]) -> FileModelParams {
        let map: HashMap<String, SecretString> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), SecretString::from((*v).to_string())))
            .collect();
        FileModelParams::try_from_params("model file", map, &empty_secrets())
            .await
            .expect("file params should deserialize")
    }

    #[tokio::test]
    #[cfg(feature = "models")]
    async fn context_length_defaults_to_none() {
        assert_eq!(
            parse_context_length(&file_params(&[]).await).expect("absent context_length is valid"),
            None
        );
        // Whitespace-only is treated as unset rather than as a parse failure, so an empty
        // template value falls back to the engine default.
        assert_eq!(
            parse_context_length(&file_params(&[("context_length", "  ")]).await)
                .expect("blank context_length is valid"),
            None
        );
    }

    #[tokio::test]
    #[cfg(feature = "models")]
    async fn context_length_parses_positive_integers() {
        assert_eq!(
            parse_context_length(&file_params(&[("context_length", " 8192 ")]).await)
                .expect("positive context_length is valid"),
            Some(8192)
        );
    }

    #[tokio::test]
    #[cfg(feature = "models")]
    async fn context_length_rejects_zero_and_non_integers() {
        for bad in ["0", "-1", "4096.5", "many"] {
            let err = parse_context_length(&file_params(&[("context_length", bad)]).await)
                .expect_err("non-positive-integer context_length should be invalid");
            assert!(
                matches!(err, LlmError::InvalidParamValueError { ref param, .. } if param == "context_length"),
                "unexpected error for {bad:?}: {err}"
            );
        }
    }

    #[tokio::test]
    #[cfg(feature = "models")]
    async fn paged_attention_defaults_to_auto() {
        assert_eq!(
            file_params(&[]).await.paged_attention,
            PagedAttentionMode::Auto
        );
    }

    #[tokio::test]
    #[cfg(feature = "models")]
    async fn paged_attention_reads_the_configured_mode() {
        // The accepted vocabulary and its case-insensitivity are covered where `FromStr`
        // lives; what matters here is that the param reaches the struct at all.
        assert_eq!(
            file_params(&[("paged_attention", "disabled")])
                .await
                .paged_attention,
            PagedAttentionMode::Disabled
        );
    }

    #[tokio::test]
    #[cfg(feature = "models")]
    async fn paged_attention_rejects_values_outside_the_spec() {
        // A Spicepod that spells this `true`/`false` has to fail loudly, naming the values
        // it should have used, rather than have one of them quietly read as a mode.
        let map: HashMap<String, SecretString> =
            [("paged_attention".to_string(), SecretString::from("true"))].into();
        let err = FileModelParams::try_from_params("model file", map, &empty_secrets())
            .await
            .expect_err("a value outside the spec should be invalid");
        let message = err.to_string();
        assert!(message.contains("auto"), "{message}");
        assert!(message.contains("disabled"), "{message}");
    }

    #[test]
    fn test_get_openai_request_overrides_with_deprecated() {
        let mut model = Model::new("hf:test_model", "test_model");
        model.params.insert(
            "openai_temperature".to_string(),
            Value::Number(Number::from_f64(0.7).expect("valid number")),
        );
        let overrides = get_openai_request_overrides(&model, "hf");
        assert_eq!(overrides.len(), 1);
        assert!(overrides.iter().any(|(k, v)| k == "temperature"
            && v == &Value::Number(Number::from_f64(0.7).expect("valid number"))));
    }

    #[test]
    fn test_get_openai_request_overrides_with_model_prefix() {
        let mut model = Model::new("hf:test_model", "test_model");
        model.params.insert(
            "hf_temperature".to_string(),
            Value::Number(Number::from_f64(0.7).expect("valid number")),
        );
        model.params.insert(
            "hf_max_completion_tokens".to_string(),
            Value::Number(1.into()),
        );
        let overrides = get_openai_request_overrides(&model, "hf");
        assert_eq!(overrides.len(), 2);
        assert!(overrides.iter().any(|(k, v)| k == "temperature"
            && v == &Value::Number(Number::from_f64(0.7).expect("valid number"))));
        assert!(
            overrides
                .iter()
                .any(|(k, v)| k == "max_completion_tokens" && v == &Value::Number(1.into()))
        );
    }

    #[test]
    fn test_get_openai_request_overrides_with_prompt_cache_key() {
        let mut model = Model::new("hf:test_model", "test_model");
        model.params.insert(
            "hf_prompt_cache_key".to_string(),
            Value::String("schema-context".to_string()),
        );

        let overrides = get_openai_request_overrides(&model, "hf");

        assert_eq!(overrides.len(), 1);
        assert!(
            overrides
                .iter()
                .any(|(key, value)| key == "prompt_cache_key"
                    && value == &Value::String("schema-context".to_string()))
        );
    }

    #[test]
    // Param with <model-prefix> takes precedence over the deprecated openai_ prefix.
    fn test_get_openai_request_overrides_with_model_prefix_and_deprecated() {
        let mut model = Model::new("hf:test_model", "test_model");
        model.params.insert(
            "hf_temperature".to_string(),
            Value::Number(Number::from_f64(0.7).expect("valid number")),
        );
        model.params.insert(
            "hf_reasoning_effort".to_string(),
            Value::String("low".into()),
        );
        model.params.insert(
            "hf_max_completion_tokens".to_string(),
            Value::Number(1.into()),
        );
        model.params.insert(
            "openai_temperature".to_string(),
            Value::Number(Number::from_f64(0.6).expect("valid number")),
        );
        model.params.insert(
            "openai_max_completion_tokens".to_string(),
            Value::Number(2.into()),
        );
        let overrides = get_openai_request_overrides(&model, "hf");
        assert_eq!(overrides.len(), 3);
        assert!(overrides.iter().any(|(k, v)| k == "temperature"
            && v == &Value::Number(Number::from_f64(0.7).expect("valid number"))));
        assert!(
            overrides
                .iter()
                .any(|(k, v)| k == "reasoning_effort" && v == &Value::String("low".into()))
        );
        assert!(
            overrides
                .iter()
                .any(|(k, v)| k == "max_completion_tokens" && v == &Value::Number(1.into()))
        );
    }

    /// Runs `parse_distributed_config` from an already-parsed backend plus
    /// `(key, value)` pairs for `node_rank`/`nodes`. The accepted vocabulary for
    /// `distributed_backend` itself, and its case-insensitivity, are covered where
    /// `DistributedBackendSetting`'s `FromStr` lives; this exercises the cross-field
    /// validation between the backend and the topology params.
    #[cfg(feature = "models")]
    fn parse_dist(
        backend: DistributedBackendSetting,
        pairs: &[(&str, &str)],
    ) -> Result<Option<llms::chat::DistributedConfig>, LlmError> {
        let get = |k: &str| pairs.iter().find(|(pk, _)| *pk == k).map(|(_, v)| *v);
        parse_distributed_config(backend, get("node_rank"), get("nodes"))
    }

    #[cfg(feature = "models")]
    #[test]
    fn distributed_none_is_single_node() {
        assert!(
            parse_dist(DistributedBackendSetting::None, &[])
                .expect("`none` backend is valid")
                .is_none()
        );
    }

    #[cfg(feature = "models")]
    #[test]
    fn distributed_ring_parses_topology() {
        let cfg = parse_dist(
            DistributedBackendSetting::Ring,
            &[("nodes", "10.0.0.1, 10.0.0.2"), ("node_rank", "1")],
        )
        .expect("valid ring config")
        .expect("ring config is Some");
        assert_eq!(cfg.backend, llms::chat::DistributedBackend::Ring);
        assert_eq!(cfg.node_rank, 1);
        assert_eq!(
            cfg.nodes,
            vec!["10.0.0.1".to_string(), "10.0.0.2".to_string()]
        );
    }

    #[cfg(feature = "models")]
    #[test]
    fn distributed_ring_requires_nodes() {
        let err = parse_dist(DistributedBackendSetting::Ring, &[])
            .expect_err("ring without nodes is invalid");
        assert!(matches!(
            err,
            LlmError::InvalidParamValueError { ref param, .. } if param == "nodes"
        ));
    }

    #[cfg(feature = "models")]
    #[test]
    fn distributed_rejects_rank_out_of_range() {
        let err = parse_dist(
            DistributedBackendSetting::Ring,
            &[("nodes", "10.0.0.1,10.0.0.2"), ("node_rank", "2")],
        )
        .expect_err("rank >= world size is invalid");
        assert!(matches!(
            err,
            LlmError::InvalidParamValueError { ref param, .. } if param == "node_rank"
        ));
    }

    #[cfg(feature = "models")]
    #[test]
    fn distributed_rejects_orphan_nodes_without_backend() {
        let err = parse_dist(
            DistributedBackendSetting::None,
            &[("nodes", "10.0.0.1,10.0.0.2")],
        )
        .expect_err("nodes without ring backend is invalid");
        assert!(matches!(
            err,
            LlmError::InvalidParamValueError { ref param, .. } if param == "distributed_backend"
        ));
    }

    fn empty_secrets() -> Arc<RwLock<Secrets>> {
        Arc::new(RwLock::new(Secrets::new()))
    }

    async fn spiceai_params(entries: &[(&str, &str)]) -> SpiceAiModelParams {
        let map: HashMap<String, SecretString> = entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), SecretString::from((*v).to_string())))
            .collect();
        SpiceAiModelParams::try_from_params("model spiceai", map, &empty_secrets())
            .await
            .expect("spiceai params should deserialize")
    }

    #[tokio::test]
    async fn spiceai_builds_a_cloud_platform_model() {
        let params = spiceai_params(&[("spiceai_api_key", "test-key")]).await;

        spiceai(Some("openai/gpt-4o".to_string()), &params)
            .expect("a Spice.ai Cloud Platform model with an API key should load");
    }

    #[tokio::test]
    async fn spiceai_builds_a_spice_to_spice_model_without_a_key() {
        let params = spiceai_params(&[("spiceai_endpoint", "http://localhost:8090")]).await;

        spiceai(Some("local-llm".to_string()), &params)
            .expect("a Spice runtime endpoint should not require an API key");
    }

    #[tokio::test]
    async fn spiceai_requires_a_model_id() {
        let params = spiceai_params(&[("spiceai_api_key", "test-key")]).await;

        let Err(err) = spiceai(None, &params) else {
            panic!("a model id is required");
        };
        assert!(matches!(
            err,
            LlmError::ModelNotProvided { ref model_source } if model_source == "spiceai"
        ));
    }

    #[tokio::test]
    async fn spiceai_rejects_a_blank_model_id() {
        let params = spiceai_params(&[("spiceai_api_key", "test-key")]).await;

        for blank in ["", "   "] {
            let Err(err) = spiceai(Some(blank.to_string()), &params) else {
                panic!("a blank model id should be rejected, got a client for {blank:?}");
            };
            assert!(matches!(
                err,
                LlmError::ModelNotProvided { ref model_source } if model_source == "spiceai"
            ));
        }
    }

    #[tokio::test]
    async fn spiceai_cloud_platform_requires_an_api_key() {
        let params = spiceai_params(&[]).await;

        let Err(err) = spiceai(Some("openai/gpt-4o".to_string()), &params) else {
            panic!("the Spice.ai Cloud Platform requires an API key");
        };
        assert!(matches!(err, LlmError::FailedToLoadModel { .. }));
    }

    #[tokio::test]
    async fn spiceai_cloud_platform_requires_an_api_key_when_the_endpoint_is_spelled_out() {
        // The `endpoint` field default substitutes the Spice.ai Cloud Platform when unset, so
        // the API-key requirement has to hold for a present endpoint too, not just an absent one.
        let params = spiceai_params(&[("spiceai_endpoint", llms::spiceai::DEFAULT_ENDPOINT)]).await;

        let Err(err) = spiceai(Some("openai/gpt-4o".to_string()), &params) else {
            panic!("the Spice.ai Cloud Platform requires an API key");
        };
        assert!(matches!(err, LlmError::FailedToLoadModel { .. }));
    }
}
