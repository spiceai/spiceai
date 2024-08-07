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
use arrow::record_batch::RecordBatch;
use llms::chat::{Chat, Error as LlmError};
use llms::embeddings::{candle::CandleEmbedding, Embed, Error as EmbedError};
use llms::openai::{DEFAULT_EMBEDDING_MODEL, DEFAULT_LLM_MODEL};
use model_components::model::{Error as ModelError, Model};
use secrecy::{ExposeSecret, Secret, SecretString};
use spicepod::component::model::ModelFileType;
use spicepod::component::{embeddings::EmbeddingPrefix, model::ModelSource};
use std::collections::HashMap;
use std::path::Path;
use std::result::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::tool_use::ToolUsingChat;
use crate::{DataFusion, Runtime};
pub type LLMModelStore = HashMap<String, RwLock<Box<dyn Chat>>>;

pub async fn run(m: &Model, df: Arc<DataFusion>) -> Result<RecordBatch, ModelError> {
    match df
        .ctx
        .sql(
            &(format!(
                "select * from datafusion.public.{} order by ts asc",
                m.model.datasets[0]
            )),
        )
        .await
    {
        Ok(data) => match data.collect().await {
            Ok(d) => m.run(d),
            Err(e) => Err(ModelError::UnableToRunModel {
                source: Box::new(e),
            }),
        },
        Err(e) => Err(ModelError::UnableToRunModel {
            source: Box::new(e),
        }),
    }
}

pub fn try_to_embedding<S: ::std::hash::BuildHasher>(
    component: &spicepod::component::embeddings::Embeddings,
    params: &HashMap<String, SecretString, S>,
) -> Result<Box<dyn Embed>, EmbedError> {
    let prefix = component
        .get_prefix()
        .ok_or(EmbedError::UnknownModelSource {
            source: format!(
                "Unknown model source for spicepod component from: {}",
                component.from.clone()
            )
            .into(),
        })?;

    let model_id = component.get_model_id();

    match prefix {
        EmbeddingPrefix::OpenAi => {
            // If parameter is from secret store, it will have `openai_` prefix
            Ok(Box::new(llms::openai::Openai::new(
                model_id.unwrap_or(DEFAULT_EMBEDDING_MODEL.to_string()),
                params.get("endpoint").map(Secret::expose_secret).cloned(),
                params
                    .get("api_key")
                    .or(params.get("openai_api_key"))
                    .map(Secret::expose_secret)
                    .cloned(),
                params
                    .get("org_id")
                    .or(params.get("openai_org_id"))
                    .map(Secret::expose_secret)
                    .cloned(),
                params
                    .get("project_id")
                    .or(params.get("openai_project_id"))
                    .map(Secret::expose_secret)
                    .cloned(),
            )))
        }
        EmbeddingPrefix::File => {
            let weights_path = model_id
                .clone()
                .or(component.find_any_file_path(ModelFileType::Weights))
                .ok_or(EmbedError::FailedToInstantiateEmbeddingModel {
                    source: "No 'weights_path' parameter provided".into(),
                })?
                .clone();
            let config_path = component
                .find_any_file_path(ModelFileType::Config)
                .ok_or(EmbedError::FailedToInstantiateEmbeddingModel {
                    source: "No 'config_path' parameter provided".into(),
                })?
                .clone();
            let tokenizer_path = component
                .find_any_file_path(ModelFileType::Tokenizer)
                .ok_or(EmbedError::FailedToInstantiateEmbeddingModel {
                    source: "No 'tokenizer_path' parameter provided".into(),
                })?
                .clone();
            Ok(Box::new(CandleEmbedding::from_local(
                Path::new(&weights_path),
                Path::new(&config_path),
                Path::new(&tokenizer_path),
            )?))
        }
        EmbeddingPrefix::HuggingFace => {
            if let Some(id) = model_id {
                Ok(Box::new(CandleEmbedding::from_hf(&id, None)?))
            } else {
                Err(EmbedError::FailedToInstantiateEmbeddingModel {
                    source: format!("Failed to load model from: {}", component.from).into(),
                })
            }
        }
    }
}

/// Attempt to derive a runnable Chat model from a given component from the Spicepod definition.
pub fn try_to_chat_model<S: ::std::hash::BuildHasher>(
    component: &spicepod::component::model::Model,
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

    let use_spiced_tools = params
        .get("use_spiced_tools")
        .map(Secret::expose_secret)
        .cloned();
    if use_spiced_tools.is_some_and(|x| x == "true") {
        Ok(Box::new(ToolUsingChat::new(model, rt)))
    } else {
        Ok(model)
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
