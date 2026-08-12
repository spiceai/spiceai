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

use crate::Runtime;
use crate::model::ToolUsingResponses;
use crate::model::params::azure::AzureModelParams;
use crate::model::params::openai::{OpenAiAuthMode, OpenAiModelParams};
use crate::model::params::xai::XaiModelParams;
use crate::model::tool_use_responses::OpenAIResponsesTools;
use crate::model::wrapper::responses::ResponsesWrapper;
use crate::tools::registry::{TOOL_EMBEDDING_MODEL_PARAM, prepare_model_tools};
use crate::tools::utils::{create_table_allowlist, get_tools_with_allowlist};
use llms::chat::Error as LlmError;
use llms::openai::DEFAULT_LLM_MODEL;
use llms::responses::Responses;
use runtime_parameters_typed::TypedParams;
use runtime_secrets::Secrets;
use runtime_tools::options::SpiceToolsOptions;
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use spicepod::component::model::{Model, ModelSource};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, LazyLock},
};
use tokio::sync::RwLock;

pub type LLMResponsesModelStore = HashMap<String, Arc<dyn Responses>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponsesApiSupport {
    Supported,
    UnsupportedProvider { provider: String },
    Unavailable,
}

impl ResponsesApiSupport {
    #[must_use]
    pub fn supports_responses_api(&self) -> bool {
        matches!(self, ResponsesApiSupport::Supported)
    }
}

const DEFAULT_SPICE_TOOL_RECURSION_LIMIT: usize = 10;

static OPENAI_RESPONSES_DEFAULT_PARAM_KEYS: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| HashSet::from(["prompt_cache_key", "prompt_cache_retention"]));

macro_rules! extract_secret {
    ($params:expr, $key:expr) => {
        $params.get($key).map(secrecy::ExposeSecret::expose_secret)
    };
}

/// Attempt to derive a runnable Responses model from a given component from the Spicepod definition.
#[expect(clippy::implicit_hasher)]
pub async fn try_to_responses_model(
    component: &Model,
    params: &HashMap<String, SecretString>,
    rt: Arc<Runtime>,
) -> Result<Arc<dyn Responses>, LlmError> {
    let source = component.get_source().ok_or(LlmError::UnknownModelSource {
        from: component.from.clone(),
    })?;

    if !matches!(
        source,
        ModelSource::OpenAi | ModelSource::Azure | ModelSource::Xai
    ) {
        return Err(LlmError::ResponsesNotSupported { from: source });
    }

    let model = construct_model(component, params, &rt.secrets()).await?;

    let openai_responses_tools: Option<Vec<OpenAIResponsesTools>> =
        extract_secret!(params, "openai_responses_tools").and_then(|v| {
            Some(
                v.split(',')
                    .map(str::trim)
                    .map(OpenAIResponsesTools::try_from)
                    .filter_map(Result::ok)
                    .collect(),
            )
        });

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

    let spice_tool_opt: Option<SpiceToolsOptions> = extract_secret!(params, "tools")
        .or(extract_secret!(params, "spice_tools"))
        .map(str::parse)
        .transpose()
        .map_err(|_| unreachable!("SpiceToolsOptions::from_str has no error condition"))?;

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
            Arc::new(ToolUsingResponses::new(
                model,
                openai_responses_tools.unwrap_or_default(),
                tools,
                spice_recursion_limit,
            ))
        }
        Some(_) | None => model,
    };

    Ok(tool_model)
}

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

async fn construct_model(
    component: &spicepod::component::model::Model,
    params: &HashMap<String, SecretString>,
    secrets: &Arc<RwLock<Secrets>>,
) -> Result<Arc<dyn Responses>, LlmError> {
    let model_id = component.get_model_id();
    let source = component.get_source().ok_or(LlmError::UnknownModelSource {
        from: component.from.clone(),
    })?;

    let model = match source {
        ModelSource::OpenAi => {
            let p = typed_params::<OpenAiModelParams>(component, params, source.clone(), secrets)
                .await?;
            openai(model_id, params, &p)
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
        _ => Err(LlmError::ResponsesNotSupported {
            from: source.clone(),
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

    Ok(Arc::new(ResponsesWrapper::new(
        model,
        component.name.as_str(),
        system_prompt,
        get_openai_responses_request_overrides(component, source.short_name()),
    )))
}

pub fn get_openai_responses_request_overrides(model: &Model, prefix: &str) -> Vec<(String, Value)> {
    let mut request_overrides: HashMap<String, Value> = HashMap::new();
    for &key in OPENAI_RESPONSES_DEFAULT_PARAM_KEYS.iter() {
        if let Some(value) = model.params.get(key) {
            request_overrides.insert(key.to_string(), value.clone());
        } else if let Some(value) = model.params.get(&format!("{prefix}_{key}")) {
            request_overrides.insert(key.to_string(), value.clone());
        } else if let Some(value) = model.params.get(&format!("openai_{key}")) {
            request_overrides.insert(key.to_string(), value.clone());
        }
    }

    request_overrides.into_iter().collect()
}

fn openai(
    model_id: Option<String>,
    raw_params: &HashMap<String, SecretString>,
    params: &OpenAiModelParams,
) -> Result<Arc<dyn Responses>, LlmError> {
    if params.auth_mode == OpenAiAuthMode::Codex {
        super::chat::validate_codex_params(params)?;
        return Ok(Arc::new(llms::openai::new_codex_client(
            model_id.unwrap_or(DEFAULT_LLM_MODEL.to_string()),
            params.endpoint.clone(),
            Some(params.usage_tier),
        )) as Arc<dyn Responses>);
    }

    let api_base = Some(params.endpoint.as_str());
    let api_key = params.api_key.as_ref().map(ExposeSecret::expose_secret);
    let org_id = params.org_id.as_deref();
    let project_id = params.project_id.as_deref();
    let usage_tier = Some(params.usage_tier);

    // Reject a negative or unparseable `temperature` override at load time. The
    // value is read from the raw params map because overrides are passthrough
    // (see `crate::model::params::common`), accepting the unprefixed,
    // `openai_`-prefixed forms.
    let temperature = raw_params
        .get("temperature")
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

    Ok(Arc::new(llms::openai::new_openai_client(
        model_id.unwrap_or(DEFAULT_LLM_MODEL.to_string()),
        api_base,
        api_key,
        org_id,
        project_id,
        usage_tier,
    )) as Arc<dyn Responses>)
}

fn azure(
    model_id: Option<String>,
    model_name: &str,
    params: &AzureModelParams,
) -> Result<Arc<dyn Responses>, LlmError> {
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
    )) as Arc<dyn Responses>)
}

fn xai(model_id: Option<&str>, params: &XaiModelParams) -> Result<Arc<dyn Responses>, LlmError> {
    let Some(api_key) = params.api_key.as_ref().map(ExposeSecret::expose_secret) else {
        return Err(LlmError::FailedToLoadModel {
            source: "No `xai_api_key` provided for xAI model.".into(),
        });
    };
    Ok(Arc::new(llms::xai::Xai::new(model_id, api_key)) as Arc<dyn Responses>)
}

#[cfg(test)]
mod tests {
    use super::*;
    use spicepod::component::model::Model;

    #[test]
    fn test_get_openai_responses_request_overrides_with_prompt_cache() {
        let mut model = Model::new("openai:gpt-4o", "test_model");
        model.params.insert(
            "prompt_cache_key".to_string(),
            Value::String("default-key".to_string()),
        );
        model.params.insert(
            "openai_prompt_cache_retention".to_string(),
            Value::String("24h".to_string()),
        );

        let overrides = get_openai_responses_request_overrides(&model, "openai");

        assert_eq!(overrides.len(), 2);
        assert!(
            overrides
                .iter()
                .any(|(key, value)| key == "prompt_cache_key"
                    && value == &Value::String("default-key".to_string()))
        );
        assert!(
            overrides
                .iter()
                .any(|(key, value)| key == "prompt_cache_retention"
                    && value == &Value::String("24h".to_string()))
        );
    }

    #[tokio::test]
    async fn unsupported_provider_reports_responses_not_supported() {
        let runtime = Runtime::builder().build().await;
        let model = Model::new("anthropic:claude-3-5-sonnet", "anthropic_model");
        let params: HashMap<String, SecretString> = HashMap::new();

        assert!(matches!(
            try_to_responses_model(&model, &params, Arc::new(runtime)).await,
            Err(LlmError::ResponsesNotSupported { .. })
        ));
    }
}
