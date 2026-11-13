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

use crate::Runtime;
use crate::model::ToolUsingResponses;
use crate::model::params::azure::AzureModelParams;
use crate::model::params::darkbloom::DarkbloomModelParams;
use crate::model::params::openai::OpenAiModelParams;
use crate::model::params::xai::XaiModelParams;
use crate::model::tool_use_responses::OpenAIResponsesTools;
use crate::model::wrapper::responses::{ResponsesRequestDefaults, ResponsesWrapper};
use crate::tools::registry::{TOOL_EMBEDDING_MODEL_PARAM, prepare_model_tools};
use crate::tools::utils::{create_table_allowlist, get_tools_with_allowlist};
use llms::chat::Error as LlmError;
use llms::openai::DEFAULT_LLM_MODEL;
use llms::responses::Responses;
use runtime_parameters_typed::TypedParams;
use runtime_rate_control::RateController;
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

const DEFAULT_SPICE_TOOL_RECURSION_LIMIT: usize = 10;

static OPENAI_RESPONSES_DEFAULT_PARAM_KEYS: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| HashSet::from(["prompt_cache_key", "prompt_cache_retention"]));

macro_rules! extract_secret {
    ($params:expr, $key:expr) => {
        $params.get($key).map(secrecy::ExposeSecret::expose_secret)
    };
}

/// Attempt to derive a runnable Responses model from a given component from the Spicepod definition.
///
/// `provider_rate_controller` is as for [`super::chat::try_to_chat_model`]: a model load passes
/// the controller it gave the model's chat client, so both share the model's rate limits.
#[expect(clippy::implicit_hasher)]
pub async fn try_to_responses_model(
    component: &Model,
    params: &HashMap<String, SecretString>,
    rt: Arc<Runtime>,
    provider_rate_controller: Option<&Arc<RateController>>,
) -> Result<Arc<dyn Responses>, LlmError> {
    let model = construct_model(component, params, &rt.secrets(), provider_rate_controller).await?;

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

    // The Spicepod's request defaults go above the tool loop, so they apply once to each client
    // request rather than again to each round of the loop.
    let source = component.get_source().ok_or(LlmError::UnknownModelSource {
        from: component.from.clone(),
    })?;
    Ok(Arc::new(ResponsesRequestDefaults::new(
        tool_model,
        component.name.as_str(),
        get_openai_responses_request_overrides(component, source.short_name()),
    )))
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
    provider_rate_controller: Option<&Arc<RateController>>,
) -> Result<Arc<dyn Responses>, LlmError> {
    let model_id = component.get_model_id();
    let source = component.get_source().ok_or(LlmError::UnknownModelSource {
        from: component.from.clone(),
    })?;

    let model = match source {
        ModelSource::OpenAi => {
            let p = typed_params::<OpenAiModelParams>(component, params, source.clone(), secrets)
                .await?;
            openai(&component.name, model_id, params, &p).await
        }
        ModelSource::Azure => {
            let p = typed_params::<AzureModelParams>(component, params, source.clone(), secrets)
                .await?;
            azure(model_id, component.name.as_str(), &p).await
        }
        ModelSource::Xai => {
            let p =
                typed_params::<XaiModelParams>(component, params, source.clone(), secrets).await?;
            xai(model_id.as_deref(), &p)
        }
        ModelSource::Darkbloom => {
            let p =
                typed_params::<DarkbloomModelParams>(component, params, source.clone(), secrets)
                    .await?;
            super::chat::darkbloom_client(component, model_id, params, &p, provider_rate_controller)
                .map(|client| Arc::new(client) as Arc<dyn Responses>)
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
    )))
}

pub fn get_openai_responses_request_overrides(model: &Model, prefix: &str) -> Vec<(String, Value)> {
    let mut request_overrides: HashMap<String, Value> = HashMap::new();
    for &key in OPENAI_RESPONSES_DEFAULT_PARAM_KEYS.iter() {
        if let Some(value) = crate::model::chat::request_default(model, prefix, key) {
            request_overrides.insert(key.to_string(), value.clone());
        }
    }

    request_overrides.into_iter().collect()
}

async fn openai(
    model_name: &str,
    model_id: Option<String>,
    raw_params: &HashMap<String, SecretString>,
    params: &OpenAiModelParams,
) -> Result<Arc<dyn Responses>, LlmError> {
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

    let model = model_id.unwrap_or(DEFAULT_LLM_MODEL.to_string());
    let label = format!("model '{model_name}' (openai)");
    if let Some(client) = llms::workload_identity::openai_federated_client(
        &label,
        &model,
        &params.federation(),
        llms::workload_identity::openai_api_key_in_spicepod(raw_params),
        llms::openai::ChatBackend::ChatCompletions,
    )
    .await
    .map_err(llms::workload_identity::FederationError::into_llm_error)?
    {
        return Ok(Arc::new(client) as Arc<dyn Responses>);
    }
    if let Some(message) = llms::workload_identity::openai_missing_credential(
        &label,
        api_key,
        params.endpoint.as_str(),
    ) {
        return Err(LlmError::FailedToAuthenticate {
            source: message.into(),
        });
    }

    Ok(Arc::new(llms::openai::new_openai_client(
        model, api_base, api_key, org_id, project_id, usage_tier,
    )) as Arc<dyn Responses>)
}

async fn azure(
    model_id: Option<String>,
    component_name: &str,
    params: &AzureModelParams,
) -> Result<Arc<dyn Responses>, LlmError> {
    Ok(
        match super::chat::azure_client(model_id, component_name, params).await? {
            super::chat::AzureClient::Federated(client) => Arc::new(client) as Arc<dyn Responses>,
            super::chat::AzureClient::Key(client) => Arc::new(client) as Arc<dyn Responses>,
        },
    )
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
    fn a_prefixed_responses_override_takes_precedence_over_its_bare_alias() {
        let mut model = Model::new("azure:gpt-4o", "test_model");
        model.params.insert(
            "prompt_cache_key".to_string(),
            Value::String("bare".to_string()),
        );
        model.params.insert(
            "azure_prompt_cache_key".to_string(),
            Value::String("prefixed".to_string()),
        );
        assert_eq!(
            get_openai_responses_request_overrides(&model, "azure"),
            vec![(
                "prompt_cache_key".to_string(),
                Value::String("prefixed".to_string())
            )]
        );
    }

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

    /// A model load gives a Darkbloom model's chat and Responses clients one provider rate
    /// controller, so the two APIs share the model's `max_concurrency`: while that controller's
    /// only permit is held, neither sends a request, and once it is released both do.
    ///
    /// The endpoint is a closed local port, so a request that is sent fails at once with a
    /// connection error, and only one waiting for a permit outlasts the timeout.
    #[tokio::test]
    async fn darkbloom_chat_and_responses_take_permits_from_one_controller() {
        use async_openai::types::chat::CreateChatCompletionRequest;
        use async_openai::types::responses::CreateResponseArgs;
        use std::time::Duration;

        let runtime = Arc::new(Runtime::builder().build().await);
        let model = Model::new("darkbloom:gpt-oss-20b", "darkbloom_model");
        let params: HashMap<String, SecretString> = [
            ("darkbloom_api_key", "eigeninference-test"),
            ("darkbloom_endpoint", "http://127.0.0.1:1/v1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), SecretString::from(v)))
        .collect();
        let controller = RateController::builder()
            .with_max_concurrent_requests(1)
            .build();

        let chat = crate::model::try_to_chat_model(
            &model,
            &params,
            Arc::clone(&runtime),
            Some(&controller),
        )
        .await
        .expect("the chat client builds")
        .chat;
        let responses = try_to_responses_model(&model, &params, runtime, Some(&controller))
            .await
            .expect("the Responses client builds");

        let chat_request: CreateChatCompletionRequest = serde_json::from_value(serde_json::json!({
            "model": "darkbloom_model",
            "messages": [{"role": "user", "content": "ping"}]
        }))
        .expect("a chat completion request");
        let responses_request = CreateResponseArgs::default()
            .model("darkbloom_model")
            .input("ping")
            .build()
            .expect("a Responses API request");
        // Long enough that a request waiting for a permit is plainly waiting; a request that
        // is sent fails within `sent` instead.
        let wait = Duration::from_millis(300);
        let sent = Duration::from_secs(10);

        let held = controller
            .acquire()
            .await
            .expect("the controller's only permit");
        tokio::time::timeout(wait, chat.chat_request(chat_request.clone()))
            .await
            .expect_err("a chat request must wait for the permit the test holds");
        tokio::time::timeout(wait, responses.responses_request(responses_request.clone()))
            .await
            .expect_err("a Responses API request must wait for the permit the test holds");

        drop(held);
        tokio::time::timeout(sent, chat.chat_request(chat_request))
            .await
            .expect("with the permit free, the chat request is sent")
            .expect_err("nothing listens on the endpoint");
        tokio::time::timeout(sent, responses.responses_request(responses_request))
            .await
            .expect("with the permit free, the Responses API request is sent")
            .expect_err("nothing listens on the endpoint");
    }

    #[tokio::test]
    async fn unsupported_provider_reports_responses_not_supported() {
        let runtime = Runtime::builder().build().await;
        let model = Model::new("anthropic:claude-3-5-sonnet", "anthropic_model");
        let params: HashMap<String, SecretString> = HashMap::new();

        assert!(matches!(
            try_to_responses_model(&model, &params, Arc::new(runtime), None).await,
            Err(LlmError::ResponsesNotSupported { .. })
        ));
    }
}
