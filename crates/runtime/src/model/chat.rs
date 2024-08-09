/*
Copyright 2024 The Spice.ai OSS Authors

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
use llms::chat::{Chat, Error as LlmError};
use llms::openai::DEFAULT_LLM_MODEL;
use secrecy::{ExposeSecret, Secret, SecretString};
use spicepod::component::model::{Model, ModelFileType, ModelSource};
use std::collections::HashMap;
use std::result::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::tool_use::{SpiceToolsOptions, ToolUsingChat};
use crate::Runtime;
pub type LLMModelStore = HashMap<String, RwLock<Box<dyn Chat>>>;

/// Attempt to derive a runnable Chat model from a given component from the Spicepod definition.
pub fn try_to_chat_model<S: ::std::hash::BuildHasher>(
    component: &Model,
    params: &HashMap<String, SecretString, S>,
    rt: Arc<Runtime>,
) -> Result<Box<dyn Chat>, LlmError> {
    let model_id = component.get_model_id();
    let prefix = component.get_source().ok_or(LlmError::UnknownModelSource {
        source: format!(
            "Unknown model source for spicepod component from: {}",
            component.from.clone()
        )
        .into(),
    })?;

    let model = construct_model(&prefix, model_id, component, params)?;

    let spice_tool_opt: Option<SpiceToolsOptions> = params
        .get("spice_tools")
        .map(Secret::expose_secret)
        .map(|x| x.parse())
        .transpose()
        .map_err(|_| LlmError::UnsupportedSpiceToolUseParameterError {})?;

    match spice_tool_opt {
        Some(tools) if tools.can_use_tools() => {
            Ok(Box::new(ToolUsingChat::new(Arc::new(model), rt, &tools)))
        }
        Some(_) | None => Ok(model),
    }
}

pub fn construct_model<S: ::std::hash::BuildHasher>(
    prefix: &ModelSource,
    model_id: Option<String>,
    component: &spicepod::component::model::Model,
    params: &HashMap<String, SecretString, S>,
) -> Result<Box<dyn Chat>, LlmError> {
    match prefix {
        ModelSource::HuggingFace => {
            let model_type = params.get("model_type").map(Secret::expose_secret).cloned();

            let tokenizer_path = component.find_any_file_path(ModelFileType::Tokenizer);
            let tokenizer_config_path =
                component.find_any_file_path(ModelFileType::TokenizerConfig);
            let weights_path = model_id
                .clone()
                .or(component.find_any_file_path(ModelFileType::Weights));

            match model_id {
                Some(id) => {
                    llms::chat::create_hf_model(
                        &id,
                        model_type.map(|x| x.to_string()),
                        &weights_path,
                        &tokenizer_path,
                        &tokenizer_config_path, // TODO handle inline chat templates
                    )
                }
                None => Err(LlmError::FailedToLoadModel {
                    source: "No model id for Huggingface model".to_string().into(),
                }),
            }
        }
        ModelSource::File => {
            let weights_path = model_id
                .clone()
                .or(component.find_any_file_path(ModelFileType::Weights))
                .ok_or(LlmError::FailedToLoadModel {
                    source: "No 'weights_path' parameter provided".into(),
                })?
                .clone();
            let tokenizer_path = component.find_any_file_path(ModelFileType::Tokenizer);
            let tokenizer_config_path = component
                .find_any_file_path(ModelFileType::TokenizerConfig)
                .ok_or(LlmError::FailedToLoadTokenizer {
                    source: "No 'tokenizer_config_path' parameter provided".into(),
                })?;

            llms::chat::create_local_model(
                &weights_path,
                tokenizer_path.as_deref(),
                tokenizer_config_path.as_ref(),
            )
        }
        ModelSource::SpiceAI => Err(LlmError::UnsupportedTaskForModel {
            from: "spiceai".into(),
            task: "llm".into(),
        }),
        ModelSource::OpenAi => {
            let api_base = params.get("endpoint").map(Secret::expose_secret).cloned();
            let api_key = params
                .get("openai_api_key")
                .map(Secret::expose_secret)
                .cloned();
            let org_id = params
                .get("openai_org_id")
                .map(Secret::expose_secret)
                .cloned();
            let project_id = params
                .get("openai_project_id")
                .map(Secret::expose_secret)
                .cloned();

            Ok(Box::new(llms::openai::Openai::new(
                model_id.unwrap_or(DEFAULT_LLM_MODEL.to_string()),
                api_base,
                api_key,
                org_id,
                project_id,
            )))
        }
    }
}
