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

use llms::chat::Error as LlmError;
use llms::openai::{DEFAULT_LLM_MODEL, UsageTier};
use llms::responses::Responses;
use secrecy::SecretString;
use spicepod::component::model::{Model, ModelSource};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use crate::Runtime;
use crate::model::ToolUsingResponses;
use crate::model::params::get_params_spec;
use crate::model::tool_use_responses::OpenAIResponsesTools;
use crate::parameters::Parameters;
use crate::tools::options::SpiceToolsOptions;
use crate::tools::utils::get_tools;

pub type LLMResponsesModelStore = HashMap<String, Arc<dyn Responses>>;

const DEFAULT_SPICE_TOOL_RECURSION_LIMIT: usize = 10;

macro_rules! extract_secret {
    ($params:expr, $key:expr) => {
        $params.get($key).map(secrecy::ExposeSecret::expose_secret)
    };
}

/// Attempt to derive a runnable Responses model from a given component from the Spicepod definition.
#[allow(clippy::implicit_hasher)]
pub async fn try_to_responses_model(
    component: &Model,
    params: &HashMap<String, SecretString>,
    rt: Arc<Runtime>,
) -> Result<Arc<dyn Responses>, LlmError> {
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

    let model = construct_model(component, &params_struct)?;

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

    let tool_model = match spice_tool_opt {
        Some(opts) if opts.can_use_tools() => Arc::new(ToolUsingResponses::new(
            model,
            openai_responses_tools.unwrap_or_default(),
            get_tools(Arc::clone(&rt), &opts).await,
            spice_recursion_limit,
        )),
        Some(_) | None => model,
    };

    Ok(tool_model)
}

fn construct_model(
    component: &spicepod::component::model::Model,
    params: &Parameters,
) -> Result<Arc<dyn Responses>, LlmError> {
    let model_id = component.get_model_id();
    let prefix = component.get_source().ok_or(LlmError::UnknownModelSource {
        from: component.from.clone(),
    })?;

    let model = match prefix {
        ModelSource::OpenAi => openai(model_id, params),
        _ => Err(LlmError::ResponsesNotSupported {
            from: component.get_source().ok_or(LlmError::UnknownModelSource {
                from: component.from.clone(),
            })?,
        }),
    }?;

    Ok(model)
}

fn openai(model_id: Option<String>, params: &Parameters) -> Result<Arc<dyn Responses>, LlmError> {
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
    )) as Arc<dyn Responses>)
}
