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
use llms::{
    HealthCheck,
    anthropic::Anthropic,
    bedrock::chat::{BedrockConverse, guardrail::GuardRail},
    chat::{Chat, Error as LlmError},
    openai::UsageTier,
    perplexity::PerplexitySonar,
    xai::Xai,
};
use llms::{config::GenericAuthMechanism, openai::DEFAULT_LLM_MODEL};
use secrecy::SecretString;
use serde_json::Value;
use snafu::ResultExt;
use spicepod::component::model::{Model, ModelFileType, ModelSource};
use std::{collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};
use token_provider::registry::TokenProviderRegistry;

use super::wrapper::OPENAI_DEFAULT_PARAM_KEYS;
use super::{params::get_params_spec, tool_use::ToolUsingChat, wrapper::ChatWrapper};
use crate::token_providers::databricks::{DatabricksM2MTokenProvider, DatabricksU2MTokenProvider};
use crate::{
    Runtime,
    parameters::Parameters,
    tools::{options::SpiceToolsOptions, utils::get_tools},
};

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
    let source = component.get_source().ok_or(LlmError::UnknownModelSource {
        from: component.from.clone(),
    })?;

    let param_spec = get_params_spec(&source).ok_or(LlmError::UnsupportedTaskForModel {
        from: component.from.clone(),
        task: "llm".into(),
    })?;

    let params_struct = Parameters::try_new(
        &format!("model {source}"),
        params.clone().into_iter().collect::<Vec<_>>(),
        source.short_name(),
        rt.secrets(),
        param_spec,
    )
    .await
    .map_err(|e| LlmError::ModelParameterFailed {
        model: component.name.clone(),
        source: e,
    })?;

    let model = construct_model(component, &params_struct, rt.token_provider_registry()).await?;

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

    let tool_model = match spice_tool_opt {
        Some(opts) if opts.can_use_tools() => Arc::new(ToolUsingChat::new(
            model,
            Arc::clone(&rt),
            get_tools(Arc::clone(&rt), &opts).await,
            spice_recursion_limit,
        )),
        Some(_) | None => model,
    };
    Ok(tool_model)
}

pub async fn construct_model(
    component: &spicepod::component::model::Model,
    params: &Parameters,
    token_registry: Arc<TokenProviderRegistry>,
) -> Result<Arc<dyn Chat>, LlmError> {
    let model_id = component.get_model_id();
    let prefix = component.get_source().ok_or(LlmError::UnknownModelSource {
        from: component.from.clone(),
    })?;

    let model = match prefix {
        ModelSource::HuggingFace => huggingface(model_id, component, params).await,
        ModelSource::File => file(component, params).await,
        ModelSource::Anthropic => anthropic(model_id.as_deref(), params),
        ModelSource::Perplexity => perplexity(model_id.as_deref(), params),
        ModelSource::Azure => azure(model_id, component.name.as_str(), params),
        ModelSource::Xai => xai(model_id.as_deref(), params),
        ModelSource::OpenAi => openai(model_id, params),
        ModelSource::Databricks => databricks(model_id, params, Arc::clone(&token_registry)).await,
        #[cfg(feature = "bedrock")]
        ModelSource::Bedrock => bedrock(model_id, params).await,
        ModelSource::SpiceAI => Err(LlmError::UnsupportedTaskForModel {
            from: "spiceai".into(),
            task: "llm".into(),
        }),
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
        get_openai_request_overrides(component, params.prefix),
    );

    if let Some(Value::String(s)) = component.params.get("parameterized_prompt") {
        if matches!(s.as_str(), "enabled") {
            wrapper = wrapper.allowed_to_parameterise();
        }
    }

    Ok(Arc::new(wrapper))
}

#[cfg(feature = "bedrock")]
async fn bedrock(model_id: Option<String>, params: &Parameters) -> Result<Arc<dyn Chat>, LlmError> {
    let Some(model_id) = model_id else {
        return Err(LlmError::ModelNotProvided {
            model_source: "bedrock".to_string(),
        });
    };

    let client = super::util::create_bedrock_client(&params.get_runtime_params(), "bedrock-chat")
        .await
        .map_err(|e| LlmError::FailedToLoadModel { source: e })?;

    let id = params.get("guardrail_identifier").expose().ok();
    let version = params.get("guardrail_version").expose().ok();
    let trace = params.get("trace").expose().ok();
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

fn xai(model_id: Option<&str>, params: &Parameters) -> Result<Arc<dyn Chat>, LlmError> {
    let Some(api_key) = params.get("api_key").expose().ok() else {
        return Err(LlmError::FailedToLoadModel {
            source: "No `xai_api_key` provided for xAI model.".into(),
        });
    };
    Ok(Arc::new(Xai::new(model_id, api_key)) as Arc<dyn Chat>)
}

fn perplexity(model_id: Option<&str>, params: &Parameters) -> Result<Arc<dyn Chat>, LlmError> {
    // PerplexitySonar only requires prefixed parameters for constructing the model.
    let model = PerplexitySonar::from_unprefixed_params(model_id, &params.get_component_params())
        .map_err(|source| LlmError::FailedToLoadModel { source })?;

    Ok(Arc::new(model) as Arc<dyn Chat>)
}

fn anthropic(model_id: Option<&str>, params: &Parameters) -> Result<Arc<dyn Chat>, LlmError> {
    let api_base = params.get("endpoint").expose().ok();
    let api_key = params.get("api_key").expose().ok();
    let auth_token = params.get("auth_token").expose().ok();

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

async fn huggingface(
    model_id: Option<String>,
    component: &spicepod::component::model::Model,
    params: &Parameters,
) -> Result<Arc<dyn Chat>, LlmError> {
    let Some(id) = model_id else {
        return Err(LlmError::FailedToLoadModel {
            source: "No model id for Huggingface model".to_string().into(),
        });
    };

    let model_type = params.get("model_type").expose().ok();
    let hf_token = params.get("token").ok();

    // For GGUF models, we require user specify via `.files[].path`
    let gguf_path = component
        .find_all_file_path(ModelFileType::Weights)
        .iter()
        .find_map(|p| {
            let path = PathBuf::from_str(p.as_str());
            if let Ok(Some(ext)) = path.as_ref().map(|pp| pp.extension()) {
                if ext.eq_ignore_ascii_case("gguf") {
                    return PathBuf::from_str(p.as_str()).ok();
                }
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
    llms::chat::create_hf_model(&id, model_type, gguf_path, hf_token).await
}

async fn databricks(
    model_id: Option<String>,
    params: &Parameters,
    token_provider_registry: Arc<TokenProviderRegistry>,
) -> Result<Arc<dyn Chat>, LlmError> {
    // Required parameters
    let Some(endpoint) = params.get("endpoint").expose().ok() else {
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
    let token_opt = params.get("token").expose().ok();
    let client_id = params.get("client_id").expose().ok();
    let client_secret = params.get("client_secret").expose().ok();

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
                .get_or_create_provider(format!("databricks_m2m_{client_id}"), || async {
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
                .get_or_create_provider::<DatabricksU2MTokenProvider, std::convert::Infallible, _, _>(format!("databricks_u2m_{client_id}"), || async {
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

fn openai(model_id: Option<String>, params: &Parameters) -> Result<Arc<dyn Chat>, LlmError> {
    let api_base = params.get("endpoint").expose().ok();
    let api_key = params.get("api_key").expose().ok();
    let org_id = params.get("org_id").expose().ok();
    let project_id = params.get("project_id").expose().ok();
    let usage_tier = params
        .get("usage_tier")
        .expose()
        .ok()
        .map(UsageTier::from_str)
        .transpose()
        .map_err(|_| LlmError::InvalidParamValueError {
            param: "openai_usage_tier".to_string(),
            message: "Must be 'free', 'tier1', 'tier2', 'tier3', 'tier4', or 'tier5'".to_string(),
        })?;

    if let Some(temperature_str) = params.get("temperature").expose().ok() {
        match temperature_str.parse::<f64>() {
            Ok(temperature) => {
                if temperature < 0.0 {
                    return Err(LlmError::InvalidParamValueError {
                        param: "openai_temperature".to_string(),
                        message: "Ensure it is a non-negative number.".to_string(),
                    });
                }
            }
            Err(_) => {
                return Err(LlmError::InvalidParamValueError {
                    param: "openai_temperature".to_string(),
                    message: "Ensure it is a non-negative number.".to_string(),
                });
            }
        }
    }

    Ok(Arc::new(llms::openai::new_openai_client(
        model_id.unwrap_or(DEFAULT_LLM_MODEL.to_string()),
        api_base,
        api_key,
        org_id,
        project_id,
        usage_tier,
    )) as Arc<dyn Chat>)
}

fn azure(
    model_id: Option<String>,
    model_name: &str,
    params: &Parameters,
) -> Result<Arc<dyn Chat>, LlmError> {
    let Some(model_name) = model_id else {
        return Err(LlmError::FailedToLoadModel {
            source: format!(
    "Azure model '{model_name}' requires a model ID in the format `from:azure:<model_id>`. See https://spiceai.org/docs/components/models/azure for details."
).into(),
        });
    };
    let api_base = params.get("endpoint").expose().ok();
    let api_version = params.get("api_version").expose().ok();
    let deployment_name = params.get("deployment_name").expose().ok();
    let api_key = params.get("api_key").expose().ok();
    let entra_token = params.get("entra_token").expose().ok();

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

    Ok(Arc::new(llms::openai::new_azure_client(
        model_name,
        api_base,
        api_version,
        deployment_name,
        entra_token,
        api_key,
    )) as Arc<dyn Chat>)
}

async fn file(
    component: &spicepod::component::model::Model,
    params: &Parameters,
) -> Result<Arc<dyn Chat>, LlmError> {
    let model_weights = component.find_all_file_path(ModelFileType::Weights);
    if model_weights.is_empty() {
        return Err(LlmError::FailedToLoadModel {
            source: "No 'weights_path' parameter provided".into(),
        });
    }

    let tokenizer_path = component.find_any_file_path(ModelFileType::Tokenizer);
    let tokenizer_config_path = component.find_any_file_path(ModelFileType::TokenizerConfig);
    let config_path = component.find_any_file_path(ModelFileType::Config);
    let generation_config = component.find_any_file_path(ModelFileType::GenerationConfig);

    let chat_template_literal = params.get("chat_template").expose().ok();

    llms::chat::create_local_model(
        model_weights.as_slice(),
        config_path.as_deref(),
        tokenizer_path.as_deref(),
        tokenizer_config_path.as_deref(),
        generation_config.as_deref(),
        chat_template_literal,
    )
    .await
}

// Get OpenAI compatible request parameter overrides.
// Prioritizes parameters with the model prefix (e.g., `hf_temperature`) over deprecated (e.g. `openai_temperature`) parameters.
pub fn get_openai_request_overrides(model: &Model, prefix: &str) -> Vec<(String, Value)> {
    let prefix_str = format!("{prefix}_");
    let mut request_overrides: HashMap<String, Value> = HashMap::new();

    for (k, v) in &model.params {
        if k.starts_with(&prefix_str) {
            if let Some(new_k) = k.strip_prefix(&prefix_str) {
                if OPENAI_DEFAULT_PARAM_KEYS.contains(&new_k) {
                    request_overrides.insert(new_k.to_string(), v.clone());
                }
            }
        } else if k.starts_with("openai_") {
            if let Some(new_k) = k.strip_prefix("openai_") {
                if OPENAI_DEFAULT_PARAM_KEYS.contains(&new_k)
                    && !request_overrides.contains_key(new_k)
                {
                    request_overrides.insert(new_k.to_string(), v.clone());
                }
            }
        }
    }

    request_overrides.into_iter().collect()
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::Number;
    use spicepod::component::model::Model;

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
}
