use bytes::Bytes;
use itertools::Itertools;
use llms::embeddings::candle::download_hf_file;
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
use llms::embeddings::{candle::tei::TeiEmbed, Embed, Error as EmbedError};
use llms::openai::embed::OpenaiEmbed;
use llms::openai::DEFAULT_EMBEDDING_MODEL;
use secrecy::{ExposeSecret, Secret, SecretString};
use snafu::ResultExt;
use spicepod::component::{embeddings::EmbeddingPrefix, model::ModelFileType};
use std::collections::HashMap;
use std::path::Path;
use std::result::Result;
use url::Url;

pub type EmbeddingModelStore = HashMap<String, Box<dyn Embed>>;

/// Extract a secret from a hashmap of secrets, if it exists.
macro_rules! extract_secret {
    ($params:expr, $key:expr) => {
        $params.get($key).map(Secret::expose_secret).cloned()
    };
}

/// Retrieves [`Bytes`] for a file/url path.
///
/// Supports:
///   - [`object_store`] compatible URLs.
///   - Huggingface URLs, e.g. `<https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/blob/main/tokenizer.json>`.
///   -
async fn get_bytes_for_file(url: &str) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    // hf://[<repo_type_prefix>]<repo_id>[@<revision>]/<path/in/repo>
    match url.split('/').collect_vec().as_slice() {
        ["https://huggingface.co/", repo_id, "blob", branch, file] | ["hf://", _] => {
            if let Ok(Some(path)) = download_hf_file(repo_id, Some(branch), None, file, None)
                .map(|s| s.to_str().map(ToString::to_string))
            {
                return Box::pin(get_bytes_for_file(path.as_str())).await;
            };
            Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
                "Downloaded HF url: {url}, but failed to get local path"
            )))
        }
        _ => {
            let url = Url::parse(url).boxed()?;
            let (store, path) = object_store::parse_url(&url).boxed()?;
            store.get(&path).await.boxed()?.bytes().await.boxed()
        }
    }
}

pub async fn try_to_embedding<S: ::std::hash::BuildHasher>(
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
            let mut embed = OpenaiEmbed::new(llms::openai::Openai::new(
                model_id.unwrap_or(DEFAULT_EMBEDDING_MODEL.to_string()),
                extract_secret!(params, "endpoint"),
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
            ));

            // For OpenAI compatible embedding models, we allow users to
            // specific the tokenizer being used, so that the model can chunk data properly.
            if let Some(tokenizer_file) = component.find_any_file_path(ModelFileType::Tokenizer) {
                tracing::debug!(
                    "Embedding model {} will use tokenizer from local file: {}.",
                    component.name,
                    tokenizer_file
                );
                let bytz = get_bytes_for_file(tokenizer_file.as_str())
                    .await
                    .map_err(|source| EmbedError::FailedToCreateTokenizer { source })?;

                embed = embed.try_with_tokenizer_bytes(&bytz)?;
            }
            Ok(Box::new(embed))
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
            let pooling = params.get("pooling").map(Secret::expose_secret).cloned();
            Ok(Box::new(TeiEmbed::from_local(
                Path::new(&weights_path),
                Path::new(&config_path),
                Path::new(&tokenizer_path),
                pooling,
            )?))
        }
        EmbeddingPrefix::HuggingFace => {
            let hf_token = extract_secret!(params, "hf_token");
            let pooling = extract_secret!(params, "pooling");

            if let Some(id) = model_id {
                Ok(Box::new(TeiEmbed::from_hf(&id, None, hf_token, pooling)?))
            } else {
                Err(EmbedError::FailedToInstantiateEmbeddingModel {
                    source: format!("Failed to load model from: {}", component.from).into(),
                })
            }
        }
    }
}
