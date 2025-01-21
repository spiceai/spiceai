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

use bytes::Bytes;
use itertools::Itertools;
use llms::embeddings::{
    candle::{download_hf_file, tei::TeiEmbed},
    Embed, Error as EmbedError,
};
use llms::openai::embed::OpenaiEmbed;
use llms::openai::DEFAULT_EMBEDDING_MODEL;
use secrecy::{ExposeSecret, Secret, SecretString};
use snafu::ResultExt;
use spicepod::component::{embeddings::EmbeddingPrefix, model::ModelFileType};
use std::path::{Path, PathBuf};
use std::result::Result;
use std::str::FromStr;
use std::{collections::HashMap, sync::Arc};
use tokio::fs;
use tokio::sync::RwLock;
use url::Url;

use crate::{get_params_with_secrets, secrets::Secrets};

pub type EmbeddingModelStore = HashMap<String, Box<dyn Embed>>;

/// Extract a secret from a hashmap of secrets, if it exists.
macro_rules! extract_secret {
    ($params:expr, $key:expr) => {
        $params.get($key).map(|s| s.expose_secret().as_str())
    };
}

pub async fn try_to_embedding(
    component: &spicepod::component::embeddings::Embeddings,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<Box<dyn Embed>, EmbedError> {
    let params = get_params_with_secrets(Arc::clone(&secrets), &component.params).await;
    let model_id = component.get_model_id();
    let prefix = component
        .get_prefix()
        .ok_or(EmbedError::UnknownModelSource {
            from: component.from.clone(),
        })?;

    match prefix {
        EmbeddingPrefix::Azure => azure(model_id, component.name.as_str(), &params),
        EmbeddingPrefix::OpenAi => openai(model_id, component, &params, secrets).await,
        EmbeddingPrefix::File => file(model_id.as_deref(), component, &params),
        EmbeddingPrefix::HuggingFace => huggingface(model_id, component, &params).await,
    }
}

async fn huggingface(
    model_id: Option<String>,
    component: &spicepod::component::embeddings::Embeddings,
    params: &HashMap<String, SecretString>,
) -> Result<Box<dyn Embed>, EmbedError> {
    let hf_token = extract_secret!(params, "hf_token");
    let pooling = extract_secret!(params, "pooling");
    let max_seq_len = max_seq_length_from_params(params)?;
    if let Some(id) = model_id {
        Ok(Box::new(
            TeiEmbed::from_hf(&id, None, hf_token, pooling, max_seq_len).await?,
        ))
    } else {
        Err(EmbedError::FailedToInstantiateEmbeddingModel {
            source: format!("Failed to load model from: {}", component.from).into(),
        })
    }
}

fn file(
    model_id: Option<&str>,
    component: &spicepod::component::embeddings::Embeddings,
    params: &HashMap<String, SecretString>,
) -> Result<Box<dyn Embed>, EmbedError> {
    let weights_path = model_id
        .map(ToString::to_string)
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
    let max_seq_len = max_seq_length_from_params(params)?;
    Ok(Box::new(TeiEmbed::from_local(
        Path::new(&weights_path),
        Path::new(&config_path),
        Path::new(&tokenizer_path),
        pooling,
        max_seq_len,
    )?))
}

fn azure(
    model_id: Option<String>,
    model_name: &str,
    params: &HashMap<String, SecretString>,
) -> Result<Box<dyn Embed>, EmbedError> {
    let Some(model_name) = model_id else {
        return Err(EmbedError::FailedToInstantiateEmbeddingModel {
            source: format!("For embedding model '{model_name}', model id must be specified in `from:azure:<model_id>`.").into(),
        });
    };
    let api_base = extract_secret!(params, "endpoint");
    let api_version = extract_secret!(params, "azure_api_version");
    let deployment_name = extract_secret!(params, "azure_deployment_name");
    let api_key = extract_secret!(params, "azure_api_key");
    let entra_token = extract_secret!(params, "azure_entra_token");
    if api_key.is_some() && entra_token.is_some() {
        return Err(EmbedError::FailedToInstantiateEmbeddingModel {
            source: format!(
                "Azure embedding model '{model_name}' can only use one of 'azure_api_key' or 'azure_entra_token'."
            )
            .into(),
        });
    }

    if api_key.is_none() && entra_token.is_none() {
        return Err(EmbedError::FailedToInstantiateEmbeddingModel {
            source: format!(
                "Azure embedding model '{model_name}' requires 'azure_api_key' or 'azure_entra_token'."
            )
            .into(),
        });
    }

    Ok(Box::new(OpenaiEmbed::new(llms::openai::new_azure_client(
        model_name,
        api_base,
        api_version,
        deployment_name,
        entra_token,
        api_key,
    ))))
}

async fn openai(
    model_id: Option<String>,
    component: &spicepod::component::embeddings::Embeddings,
    params: &HashMap<String, SecretString>,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<Box<dyn Embed>, EmbedError> {
    // If parameter is from secret store, it will have `openai_` prefix
    let mut embed = OpenaiEmbed::new(llms::openai::new_openai_client(
        model_id.unwrap_or(DEFAULT_EMBEDDING_MODEL.to_string()),
        extract_secret!(params, "endpoint"),
        params
            .get("api_key")
            .or(params.get("openai_api_key"))
            .map(|s| s.expose_secret().as_str()),
        params
            .get("org_id")
            .or(params.get("openai_org_id"))
            .map(|s| s.expose_secret().as_str()),
        params
            .get("project_id")
            .or(params.get("openai_project_id"))
            .map(|s| s.expose_secret().as_str()),
    ));

    // For OpenAI compatible embedding models, we allow users to
    // specific the tokenizer being used, so that the model can chunk data properly.
    if let Some(tokenizer_file) = component.find_any_file(ModelFileType::Tokenizer) {
        tracing::debug!(
            "Embedding model {} will use tokenizer from local file: {}.",
            component.name,
            &tokenizer_file.path
        );
        let file_params = if let Some(params) = tokenizer_file.params {
            get_params_with_secrets(Arc::clone(&secrets), &params).await
        } else {
            HashMap::default()
        };

        let bytz = get_bytes_for_file(tokenizer_file.path.as_str(), &file_params)
            .await
            .map_err(|source| EmbedError::FailedToCreateTokenizer { source })?;

        embed = embed.try_with_tokenizer_bytes(&bytz)?;
    }
    Ok(Box::new(embed))
}

/// Retrieves [`Bytes`] for a file/url path.
///
/// Supports:
///   - [`object_store`] compatible URLs.
///   - Huggingface URLs, e.g. `<https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/blob/main/tokenizer.json>`.
///   - Huggingface `FssSpec`: `hf://[<repo_type_prefix>]<repo_id>[@<revision>]/<path/in/repo>`.
async fn get_bytes_for_file(
    url: &str,
    params: &HashMap<String, Secret<String>>,
) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    match url.split('/').collect_vec().as_slice() {
        ["https:", "", "huggingface.co", org_id, model_id, "blob", branch, file @ ..] => {
            get_file_from_hf(
                None,
                org_id,
                model_id,
                Some(branch),
                file.join("/").as_str(),
                params.get("hf_token").map(|s| s.expose_secret().as_str()),
            )
            .await
        }
        ["hf:", "", "datasets", org_id, model_id_revision, file @ ..] => {
            let (model_id, branch) = parse_model_id_w_revision(model_id_revision);

            get_file_from_hf(
                Some("datasets"),
                org_id,
                model_id,
                branch,
                file.join("/").as_str(),
                params.get("hf_token").map(|s| s.expose_secret().as_str()),
            )
            .await
        }
        ["hf:", "", "spaces", org_id, model_id_revision, file @ ..] => {
            let (model_id, branch) = parse_model_id_w_revision(model_id_revision);
            get_file_from_hf(
                Some("spaces"),
                org_id,
                model_id,
                branch,
                file.join("/").as_str(),
                params.get("hf_token").map(|s| s.expose_secret().as_str()),
            )
            .await
        }
        ["hf:", "", "models", org_id, model_id_revision, file @ ..] => {
            let (model_id, branch) = parse_model_id_w_revision(model_id_revision);
            get_file_from_hf(
                Some("models"),
                org_id,
                model_id,
                branch,
                file.join("/").as_str(),
                params.get("hf_token").map(|s| s.expose_secret().as_str()),
            )
            .await
        }
        ["hf:", "", org_id, model_id_revision, file @ ..] => {
            let (model_id, branch) = parse_model_id_w_revision(model_id_revision);
            get_file_from_hf(
                Some("models"),
                org_id,
                model_id,
                branch,
                file.join("/").as_str(),
                params.get("hf_token").map(|s| s.expose_secret().as_str()),
            )
            .await
        }
        _ => {
            // Need to add `file://` for file paths
            let final_url = match PathBuf::from_str(url).map(|p| p.canonicalize()) {
                Ok(Ok(ref p)) if p.exists() => {
                    format!("file://{}", p.to_string_lossy())
                }
                _ => url.to_string(),
            };
            let url = Url::parse(final_url.as_str()).boxed()?;
            let (store, path) = object_store::parse_url(&url).boxed()?;
            store.get(&path).await.boxed()?.bytes().await.boxed()
        }
    }
}

/// From `hf://` spec, parse the `model_id` that may have a revision attached `all-MiniLM-L6-v2@main`.
///
/// `all-MiniLM-L6-v2` -> (`all-MiniLM-L6-v2`, None)
/// `all-MiniLM-L6-v2@main` -> (`all-MiniLM-L6-v2`, Some(`main`))
fn parse_model_id_w_revision(model_w_revision: &str) -> (&str, Option<&str>) {
    match model_w_revision.split_once('@') {
        Some((model_id, revision)) => (model_id, Some(revision)),
        None => (model_w_revision, None),
    }
}

async fn get_file_from_hf(
    repo_type: Option<&str>,
    org_id: &str,
    model_id: &str,
    branch: Option<&str>,
    file: &str,
    hf_token: Option<&str>,
) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    match download_hf_file(
        format!("{org_id}/{model_id}").as_str(),
        branch,
        repo_type,
        file,
        hf_token,
    )
    .await
    {
        Ok(path) => {
            let bytz = fs::read(path).await.boxed()?;
            Ok(bytz.into())
        }
        Err(e) => Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
            "Downloaded HF url, but failed to get local path. Error: {e:?}"
        ))),
    }
}

fn max_seq_length_from_params(
    params: &HashMap<String, SecretString>,
) -> Result<Option<usize>, EmbedError> {
    params
        .get("max_seq_length")
        .map(|s| {
            s.expose_secret().parse().boxed().map_err(|e| {
                EmbedError::FailedToInstantiateEmbeddingModel {
                    source: format!("Failed to parse 'max_seq_length' parameter: {e}").into(),
                }
            })
        })
        .transpose()
}
