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
    anthropic::Anthropic,
    chat::{Chat, Error as LlmError},
    perplexity::PerplexitySonar,
    xai::Xai,
};
use llms::{config::GenericAuthMechanism, openai::DEFAULT_LLM_MODEL};
use secrecy::{ExposeSecret, SecretString};
use spicepod::component::model::{Model, ModelFileType, ModelSource};
use std::{collections::HashMap, path::PathBuf, str::FromStr, sync::Arc};

use super::{tool_use::ToolUsingChat, wrapper::ChatWrapper};
use crate::{
    tools::{options::SpiceToolsOptions, utils::get_tools},
    Runtime,
};

pub type LLMModelStore = HashMap<String, Box<dyn Chat>>;

/// Extract a secret from a hashmap of secrets, if it exists.
macro_rules! extract_secret {
    ($params:expr, $key:expr) => {
        $params.get($key).map(|s| s.expose_secret().as_str())
    };
}

/// Attempt to derive a runnable Chat model from a given component from the Spicepod definition.
pub async fn try_to_chat_model(
    component: &Model,
    params: &HashMap<String, SecretString>,
    rt: Arc<Runtime>,
) -> Result<Box<dyn Chat>, LlmError> {
    let model = construct_model(component, params)?;

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
        .transpose()?;

    let tool_model = match spice_tool_opt {
        Some(opts) if opts.can_use_tools() => Box::new(ToolUsingChat::new(
            Arc::new(model),
            Arc::clone(&rt),
            get_tools(Arc::clone(&rt), &opts).await,
            spice_recursion_limit,
        )),
        Some(_) | None => model,
    };
    Ok(tool_model)
}

pub fn construct_model(
    component: &spicepod::component::model::Model,
    params: &HashMap<String, SecretString>,
) -> Result<Box<dyn Chat>, LlmError> {
    let model_id = component.get_model_id();
    let prefix = component.get_source().ok_or(LlmError::UnknownModelSource {
        from: component.from.clone(),
    })?;

    let model = match prefix {
        ModelSource::HuggingFace => huggingface(model_id, component, params),
        ModelSource::File => file(component, params),
        ModelSource::Anthropic => anthropic(model_id.as_deref(), params),
        ModelSource::Perplexity => perplexity(model_id.as_deref(), params),
        ModelSource::Azure => azure(model_id, component.name.as_str(), params),
        ModelSource::Xai => xai(model_id.as_deref(), params),
        ModelSource::OpenAi => openai(model_id, params),
        ModelSource::SpiceAI => Err(LlmError::UnsupportedTaskForModel {
            from: "spiceai".into(),
            task: "llm".into(),
        }),
    }?;

    // Handle runtime wrapping
    let system_prompt = component
        .params
        .get("system_prompt")
        .cloned()
        .map(|s| s.to_string());
    let wrapper = ChatWrapper::new(
        model,
        component.name.as_str(),
        system_prompt,
        component.get_openai_request_overrides(),
    );
    Ok(Box::new(wrapper))
}

fn xai(
    model_id: Option<&str>,
    params: &HashMap<String, SecretString>,
) -> Result<Box<dyn Chat>, LlmError> {
    let Some(api_key) = extract_secret!(params, "xai_api_key") else {
        return Err(LlmError::FailedToLoadModel {
            source: "No `xai_api_key` provided for xAI model.".into(),
        });
    };
    Ok(Box::new(Xai::new(model_id, api_key)) as Box<dyn Chat>)
}

fn perplexity(
    model_id: Option<&str>,
    params: &HashMap<String, SecretString>,
) -> Result<Box<dyn Chat>, LlmError> {
    let model = PerplexitySonar::from_params(model_id, params)
        .map_err(|source| LlmError::FailedToLoadModel { source })?;

    Ok(Box::new(model) as Box<dyn Chat>)
}

fn anthropic(
    model_id: Option<&str>,
    params: &HashMap<String, SecretString>,
) -> Result<Box<dyn Chat>, LlmError> {
    let api_base = extract_secret!(params, "endpoint");
    let api_key = extract_secret!(params, "anthropic_api_key");
    let auth_token = extract_secret!(params, "anthropic_auth_token");

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

    Ok(Box::new(anthropic) as Box<dyn Chat>)
}

fn huggingface(
    model_id: Option<String>,
    component: &spicepod::component::model::Model,
    params: &HashMap<String, SecretString>,
) -> Result<Box<dyn Chat>, LlmError> {
    let Some(id) = model_id else {
        return Err(LlmError::FailedToLoadModel {
            source: "No model id for Huggingface model".to_string().into(),
        });
    };
    let model_type = extract_secret!(params, "model_type");
    let hf_token = params.get("hf_token");

    // For GGUF models, we require user specify via `.files[].path`
    let gguf_path = component
        .find_all_file_path(ModelFileType::Weights)
        .iter()
        .find_map(|p| {
            let path = PathBuf::from_str(p.as_str());
            if let Ok(Some(ext)) = path.as_ref().map(|pp| pp.extension()) {
                if ext.eq_ignore_ascii_case("gguf") {
                    return PathBuf::from_str(p.as_str()).ok();
                };
            }
            None
        });

    if let Some(ref path) = gguf_path {
        tracing::debug!(
            "For Huggingface model {}, the GGUF model {} will be downloaded and used.",
            component.name,
            path.display()
        );
    };
    llms::chat::create_hf_model(&id, model_type, gguf_path, hf_token)
}

fn openai(
    model_id: Option<String>,
    params: &HashMap<String, SecretString>,
) -> Result<Box<dyn Chat>, LlmError> {
    let api_base = extract_secret!(params, "endpoint");
    let api_key = extract_secret!(params, "openai_api_key");
    let org_id = extract_secret!(params, "openai_org_id");
    let project_id = extract_secret!(params, "openai_project_id");

    if let Some(temperature_str) = extract_secret!(params, "openai_temperature") {
        match temperature_str.parse::<f64>() {
            Ok(temperature) => {
                if temperature < 0.0 {
                    return Err(LlmError::InvalidParamError {
                        param: "openai_temperature".to_string(),
                        message: "Ensure it is a non-negative number.".to_string(),
                    });
                }
            }
            Err(_) => {
                return Err(LlmError::InvalidParamError {
                    param: "openai_temperature".to_string(),
                    message: "Ensure it is a non-negative number.".to_string(),
                })
            }
        }
    }

    Ok(Box::new(llms::openai::new_openai_client(
        model_id.unwrap_or(DEFAULT_LLM_MODEL.to_string()),
        api_base,
        api_key,
        org_id,
        project_id,
    )) as Box<dyn Chat>)
}

fn azure(
    model_id: Option<String>,
    model_name: &str,
    params: &HashMap<String, SecretString>,
) -> Result<Box<dyn Chat>, LlmError> {
    let Some(model_name) = model_id else {
        return Err(LlmError::FailedToLoadModel {
            source: format!(
    "Azure model '{model_name}' requires a model ID in the format `from:azure:<model_id>`. See https://spiceai.org/docs/components/models/azure for details."
).into(),
        });
    };
    let api_base = extract_secret!(params, "endpoint");
    let api_version = extract_secret!(params, "azure_api_version");
    let deployment_name = extract_secret!(params, "azure_deployment_name");
    let api_key = extract_secret!(params, "azure_api_key");
    let entra_token = extract_secret!(params, "azure_entra_token");

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

    Ok(Box::new(llms::openai::new_azure_client(
        model_name,
        api_base,
        api_version,
        deployment_name,
        entra_token,
        api_key,
    )) as Box<dyn Chat>)
}

fn file(
    component: &spicepod::component::model::Model,
    params: &HashMap<String, SecretString>,
) -> Result<Box<dyn Chat>, LlmError> {
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

    let chat_template_literal = params
        .get("chat_template")
        .map(|s| s.expose_secret().as_str());

    llms::chat::create_local_model(
        model_weights.as_slice(),
        config_path.as_deref(),
        tokenizer_path.as_deref(),
        tokenizer_config_path.as_deref(),
        generation_config.as_deref(),
        chat_template_literal,
    )
}
