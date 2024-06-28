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
#![allow(clippy::module_name_repetitions)]
use arrow::record_batch::RecordBatch;
use llms::chat::{Chat, Error as LlmError};
use llms::embeddings::{candle::CandleEmbedding, Embed, Error as EmbedError};
use llms::openai::{DEFAULT_EMBEDDING_MODEL, DEFAULT_LLM_MODEL};
use model_components::model::{Error as ModelError, Model};
use secrecy::{ExposeSecret, Secret, SecretString};
use spicepod::component::model::{ModelFile, ModelFileType};
use spicepod::component::{
    embeddings::{EmbeddingParams, EmbeddingPrefix},
    model::ModelSource,
};
use std::collections::HashMap;
use std::path::Path;
use std::result::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::DataFusion;
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

    match construct_embedding_params(&prefix, &model_id, params) {
        Ok(EmbeddingParams::OpenAiParams {
            api_base,
            api_key,
            org_id,
            project_id,
        }) => Ok(Box::new(llms::openai::Openai::new(
            model_id.unwrap_or(DEFAULT_EMBEDDING_MODEL.to_string()),
            api_base,
            api_key,
            org_id,
            project_id,
        ))),
        Ok(EmbeddingParams::HuggingfaceParams {}) => {
            if let Some(id) = model_id {
                Ok(Box::new(CandleEmbedding::from_hf(&id, None)?))
            } else {
                Err(EmbedError::FailedToInstantiateEmbeddingModel {
                    source: format!("Failed to load model from: {}", component.from).into(),
                })
            }
        }
        Ok(EmbeddingParams::LocalModelParams {
            weights_path,
            config_path,
            tokenizer_path,
        }) => Ok(Box::new(CandleEmbedding::from_local(
            Path::new(&weights_path),
            Path::new(&config_path),
            Path::new(&tokenizer_path),
        )?)),
        Ok(EmbeddingParams::None) => Err(EmbedError::UnsupportedTaskForModel {
            from: component.from.clone(),
            task: "embedding".into(),
        }),
        Err(e) => Err(e),
    }
}

/// Construct the parameters needed to create an [`Embeddings`] based on its source (i.e. prefix).
/// If a `model_id` is provided (in the `from: `), it is provided.
fn construct_embedding_params<S: ::std::hash::BuildHasher>(
    from: &EmbeddingPrefix,
    model_id: &Option<String>,
    params: &HashMap<String, SecretString, S>,
) -> Result<EmbeddingParams, EmbedError> {
    match from {
        EmbeddingPrefix::OpenAi => Ok(EmbeddingParams::OpenAiParams {
            api_base: params.get("endpoint").map(Secret::expose_secret).cloned(),
            api_key: params
                .get("openai_api_key")
                .map(Secret::expose_secret)
                .cloned(),
            org_id: params
                .get("openai_org_id")
                .map(Secret::expose_secret)
                .cloned(),
            project_id: params
                .get("openai_project_id")
                .map(Secret::expose_secret)
                .cloned(),
        }),
        EmbeddingPrefix::File => {
            let weights_path = model_id
                .clone()
                .or(params
                    .get("weights_path")
                    .map(ExposeSecret::expose_secret)
                    .cloned())
                .ok_or(EmbedError::FailedToInstantiateEmbeddingModel {
                    source: "No 'weights_path' parameter provided".into(),
                })?
                .clone();
            let config_path = params
                .get("config_path")
                .map(Secret::expose_secret)
                .ok_or(EmbedError::FailedToInstantiateEmbeddingModel {
                    source: "No 'config_path' parameter provided".into(),
                })?
                .clone();

            let tokenizer_path = params
                .get("tokenizer_path")
                .map(Secret::expose_secret)
                .ok_or(EmbedError::FailedToInstantiateEmbeddingModel {
                    source: "No 'tokenizer_path' parameter provided".into(),
                })?
                .clone();
            Ok(EmbeddingParams::LocalModelParams {
                weights_path,
                config_path,
                tokenizer_path,
            })
        }
        EmbeddingPrefix::HuggingFace => Ok(EmbeddingParams::HuggingfaceParams {}),
    }
}

/// Attempt to derive a runnable Chat model from a given component from the Spicepod definition.
pub fn try_to_chat_model<S: ::std::hash::BuildHasher>(
    component: &spicepod::component::model::Model,
    params: &HashMap<String, SecretString, S>,
) -> Result<Box<dyn Chat>, LlmError> {
    let model_id = component.get_model_id();
    let prefix = component.get_source().ok_or(LlmError::UnknownModelSource {
        source: format!(
            "Unknown model source for spicepod component from: {}",
            component.from.clone()
        )
        .into(),
    })?;

    match prefix {
        ModelSource::HuggingFace => {
            let model_type = params.get("model_type").map(Secret::expose_secret).cloned();

            let weights_path = model_id
                .clone()
                .or(find_file_path(&component.files, ModelFileType::Weights));
            let tokenizer_path = find_file_path(&component.files, ModelFileType::Tokenizer);
            let tokenizer_config_path =
                find_file_path(&component.files, ModelFileType::TokenizerConfig);

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
                .or(find_file_path(&component.files, ModelFileType::Weights))
                .ok_or(LlmError::FailedToLoadModel {
                    source: "No 'weights_path' parameter provided".into(),
                })?
                .clone();
            let tokenizer_path = find_file_path(&component.files, ModelFileType::Tokenizer);
            let tokenizer_config_path =
                find_file_path(&component.files, ModelFileType::TokenizerConfig).ok_or(
                    LlmError::FailedToLoadTokenizer {
                        source: "No 'tokenizer_config_path' parameter provided".into(),
                    },
                )?;

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

fn find_file_path(files: &[ModelFile], file_type: ModelFileType) -> Option<String> {
    files
        .iter()
        .find(|f| f.r#type == Some(file_type))
        .map(|f| f.path.clone())
}
