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
use llms::embeddings::{candle::CandleEmbedding, Embed, Error as EmbedError};
use llms::openai::DEFAULT_EMBEDDING_MODEL;
use secrecy::{ExposeSecret, Secret, SecretString};
use spicepod::component::{embeddings::EmbeddingPrefix, model::ModelFileType};
use std::collections::HashMap;
use std::path::Path;
use std::result::Result;

pub type EmbeddingModelStore = HashMap<String, Box<dyn Embed>>;

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
