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

use anyhow::Context;
use async_openai::error::OpenAIError;
use hf_hub::{api::sync::ApiBuilder, Repo, RepoType};
use llms::{
    anthropic::Anthropic,
    chat::{create_hf_model, create_local_model, Chat, Error as ChatError},
    config::GenericAuthMechanism,
    embeddings::candle::link_files_into_tmp_dir,
    openai::new_openai_client,
    perplexity::PerplexitySonar,
    xai::Xai,
};
use secrecy::{Secret, SecretString};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

pub(crate) fn create_xai(model_id: &str) -> Result<Arc<Box<dyn Chat>>, anyhow::Error> {
    let Ok(api_key) = std::env::var("SPICE_XAI_API_KEY") else {
        return Err(anyhow::anyhow!("SPICE_XAI_API_KEY not set"));
    };
    Ok(Arc::new(Box::new(Xai::new(
        Some(model_id),
        api_key.as_str(),
    ))))
}

pub(crate) fn create_openai(model_id: &str) -> Arc<Box<dyn Chat>> {
    let api_key = std::env::var("SPICE_OPENAI_API_KEY").ok();
    Arc::new(Box::new(new_openai_client(
        model_id.to_string(),
        None,
        api_key.as_deref(),
        None,
        None,
    )))
}

pub(crate) fn create_anthropic(model_id: Option<&str>) -> Result<Arc<Box<dyn Chat>>, OpenAIError> {
    let auth = match (
        std::env::var("SPICE_ANTHROPIC_API_KEY"),
        std::env::var("SPICE_ANTHROPIC_AUTH_TOKEN"),
    ) {
        (Ok(api_key), _) => GenericAuthMechanism::from_api_key(api_key),
        (_, Ok(auth_token)) => {
            GenericAuthMechanism::from_bearer_token(auth_token)
        }
        _ => return Err(OpenAIError::InvalidArgument("One and only one of 'SPICE_ANTHROPIC_API_KEY' or 'SPICE_ANTHROPIC_AUTH_TOKEN' must be set".to_string())),
    };
    Ok(Arc::new(Box::new(Anthropic::new(
        auth, model_id, None, None,
    )?)))
}

pub(crate) fn create_hf(model_id: &str) -> Result<Arc<Box<dyn Chat>>, ChatError> {
    Ok(Arc::new(create_hf_model(
        model_id,
        None,
        None,
        std::env::var("HF_TOKEN").ok().map(Secret::new).as_ref(),
    )?))
}

pub(crate) fn create_perplexity() -> Result<Arc<Box<dyn Chat>>, ChatError> {
    let mut params: HashMap<String, SecretString> = HashMap::new();
    if let Ok(api_key) = std::env::var("SPICE_PERPLEXITY_AUTH_TOKEN") {
        params.insert(
            "perplexity_auth_token".to_string(),
            SecretString::new(api_key),
        );
    }
    let sonar = PerplexitySonar::from_params(None, &params)
        .map_err(|e| ChatError::FailedToLoadModel { source: e })?;

    Ok(Arc::new(Box::new(sonar)))
}

pub(crate) fn create_local(model_id: &str) -> Result<Arc<Box<dyn Chat>>, anyhow::Error> {
    let (temp_dir, model_weights) =
        download_hf_model_artifacts(model_id, None, std::env::var("HF_TOKEN").ok())?;

    let model = create_local_model(
        &model_weights,
        temp_dir.join("config.json").to_str(),
        temp_dir.join("tokenizer.json").to_str(),
        temp_dir.join("tokenizer_config.json").to_str(),
        None,
        None,
    )
    .map_err(anyhow::Error::from)?;
    Ok(Arc::from(Box::new(model)))
}

/// For a given `HuggingFace` repo, downloads the specified file and save them into provided folder. Return folder, and which ones are model weights.
#[allow(clippy::case_sensitive_file_extension_comparisons)]
fn download_hf_model_artifacts(
    model_id: &str,
    revision: Option<&str>,
    hf_token: Option<String>,
) -> Result<(PathBuf, Vec<String>), anyhow::Error> {
    let api = ApiBuilder::new()
        .with_progress(false)
        .with_token(hf_token)
        .build()
        .context("Failed to instantiate API for downloading model artifacts")?;

    let repo = if let Some(revision) = revision {
        Repo::with_revision(model_id.to_string(), RepoType::Model, revision.to_string())
    } else {
        Repo::new(model_id.to_string(), RepoType::Model)
    };
    let api_repo = api.repo(repo.clone());

    let mut files = HashMap::<String, PathBuf>::new();
    let mut weights = vec![];
    for sibling in api_repo.info()?.siblings {
        if !(sibling.rfilename.ends_with(".py") || sibling.rfilename.ends_with(".md")) {
            let path = api_repo.get(sibling.rfilename.as_str())?;

            // `abs_path` will have symlinks and relative paths resolved, but will have a hash for a filename. This is fine after its symlinked in `link_files_into_tmp_dir`.
            // use `path` to get the original filename.
            let abs_path = fs::canonicalize(path.clone())?;

            if let Some(filename) = path.file_name() {
                files.insert(filename.to_string_lossy().to_string(), abs_path);
                if path_is_weights(&path) {
                    weights.push(filename.to_string_lossy().to_string());
                }
            }
        }
    }

    let dir =
        link_files_into_tmp_dir(files.clone()).context("Failed to link files into tmp dir")?;
    Ok((
        dir.clone(),
        weights // Reconstruct absolute model weights path based in the tmp dir.
            .iter()
            .map(|w| dir.join(w).display().to_string())
            .collect(),
    ))
}

/// Attempts to figure out if a given path is a model weights file.
///
/// This function is not perfect, but should cover all cases needed for testing.
fn path_is_weights(p: &Path) -> bool {
    // Get the file extension and convert to lowercase for case-insensitive comparison
    let extension = p
        .extension()
        .and_then(|ext| ext.to_str())
        .map(str::to_lowercase);

    // Get the file name as string for pattern matching
    let file_name = p
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_lowercase);

    // Common model weight file extensions
    let weight_extensions = ["bin", "pt", "gguf", "safetensors", "pth", "ckpt"];

    // Common weight file patterns
    let weight_patterns = [
        "weights",
        "model",
        "pytorch_model",
        "params",
        "parameters",
        "checkpoint",
        "ckpt",
    ];

    match (extension, file_name) {
        (Some(ext), Some(name)) => {
            // Check if extension matches known weight file extensions
            let has_weight_extension = weight_extensions.contains(&ext.as_str());

            // Check if filename contains common weight file patterns
            let has_weight_pattern = weight_patterns.iter().any(|pattern| name.contains(pattern));

            has_weight_extension && has_weight_pattern
        }
        _ => false,
    }
}
