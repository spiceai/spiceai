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
use async_openai::{config::OpenAIConfig, error::OpenAIError};
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_credential_bridge::default_aws_config;
use llms::{
    anthropic::Anthropic,
    bedrock::chat::BedrockConverse,
    chat::{Chat, Error as ChatError, create_hf_model, create_local_model},
    config::GenericAuthMechanism,
    google::{Google, auth},
    openai::{Openai, new_openai_client},
    responses::Responses,
    xai::Xai,
};
use secrecy::SecretString;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub(crate) async fn create_bedrock(model_id: &str) -> Result<Arc<dyn Chat>, anyhow::Error> {
    let mut config_builder = default_aws_config();

    if let Ok(region) = std::env::var("SPICE_BEDROCK_REGION") {
        config_builder = config_builder.region(Region::new(region));
    }

    match (
        std::env::var("SPICE_BEDROCK_ACCESS_KEY"),
        std::env::var("SPICE_BEDROCK_SECRET_KEY"),
    ) {
        (Ok(access_key), Ok(secret_key)) => {
            config_builder = config_builder.credentials_provider(Credentials::new(
                access_key,
                secret_key,
                std::env::var("SPICE_BEDROCK_SESSION_TOKEN").ok(),
                None,
                "bedrock-chat",
            ));
        }
        (Err(_), Ok(_)) => {
            return Err(anyhow::anyhow!("SPICE_BEDROCK_ACCESS_KEY not set"));
        }
        (Ok(_), Err(_)) => {
            return Err(anyhow::anyhow!("SPICE_BEDROCK_SECRET_KEY not set"));
        }
        (Err(_), Err(_)) => {
            return Err(anyhow::anyhow!(
                "SPICE_BEDROCK_ACCESS_KEY & SPICE_BEDROCK_SECRET_KEY not set"
            ));
        }
    }

    let config: aws_config::SdkConfig = config_builder.load().await;
    Ok(Arc::new(BedrockConverse::new(
        Arc::new((&config).into()),
        model_id.to_string(),
    )) as Arc<dyn Chat>)
}

fn xai(model_id: &str) -> Result<Xai, anyhow::Error> {
    let Ok(api_key) = std::env::var("SPICE_XAI_API_KEY") else {
        return Err(anyhow::anyhow!("SPICE_XAI_API_KEY not set"));
    };
    Ok(Xai::new(Some(model_id), api_key.as_str()))
}

pub(crate) fn create_xai(model_id: &str) -> Result<Arc<dyn Chat>, anyhow::Error> {
    Ok(Arc::new(xai(model_id)?))
}

pub(crate) fn create_xai_responses(model_id: &str) -> Result<Arc<dyn Responses>, anyhow::Error> {
    Ok(Arc::new(xai(model_id)?))
}

fn openai(model_id: &str) -> Openai<OpenAIConfig> {
    let api_key = std::env::var("SPICE_OPENAI_API_KEY").ok();
    new_openai_client(
        model_id.to_string(),
        None,
        api_key.as_deref(),
        None,
        None,
        None,
    )
}

pub(crate) fn create_openai(model_id: &str) -> Arc<dyn Chat> {
    Arc::new(openai(model_id))
}

pub(crate) fn create_openai_responses(model_id: &str) -> Arc<dyn Responses> {
    Arc::new(openai(model_id))
}

pub(crate) fn create_anthropic(model_id: Option<&str>) -> Result<Arc<dyn Chat>, OpenAIError> {
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
    Ok(Arc::new(Anthropic::new(auth, model_id, None, None)?))
}

pub(crate) async fn create_hf(model_id: &str) -> Result<Arc<dyn Chat>, ChatError> {
    let token = std::env::var("HF_TOKEN").ok().map(SecretString::from);
    create_hf_model(
        model_id,
        None,
        None,
        llms::hf::HubParams {
            token: token.as_ref(),
            token_in_spicepod: true,
            endpoint: None,
        },
        None,
        None,
        llms::chat::ReasoningDefault {
            model_name: model_id,
            effort: None,
        },
    )
    .await
}

pub(crate) async fn create_google(model_id: &str) -> Result<Arc<dyn Chat>, anyhow::Error> {
    let project = std::env::var("SPICE_GOOGLE_PROJECT").context("SPICE_GOOGLE_PROJECT not set")?;
    let location =
        std::env::var("SPICE_GOOGLE_LOCATION").context("SPICE_GOOGLE_LOCATION not set")?;

    // Authenticate the way `from: google` does — as a GCP service account against Vertex AI —
    // here through Application Default Credentials, so this needs
    // `GOOGLE_APPLICATION_CREDENTIALS` pointing at a service-account key.
    let client = auth::build_client(
        auth::VertexAuthParams {
            project: Some(&project),
            location: Some(&location),
            service_account_path: None,
            service_account_key: None,
            application_default_credentials: true,
        },
        "model.params",
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create Google client: {e}"))?;

    Ok(Arc::new(Google::from_client(client, model_id)))
}

pub(crate) async fn create_local(model_id: &str) -> Result<Arc<dyn Chat>, anyhow::Error> {
    let (model_dir, model_weights) =
        download_hf_model_artifacts(model_id, None, std::env::var("HF_TOKEN").ok()).await?;

    let model = create_local_model(
        &model_weights,
        model_dir.join("config.json").to_str(),
        model_dir.join("tokenizer.json").to_str(),
        model_dir.join("tokenizer_config.json").to_str(),
        None,
        None,
        llms::chat::LocalModelOptions::default(),
    )
    .await
    .map_err(anyhow::Error::from)?;
    Ok(model)
}

/// Downloads every file of a `HuggingFace` repo but its code and docs into the Hugging Face
/// cache. Returns the snapshot's folder, and the paths of the model weights in it.
#[expect(clippy::case_sensitive_file_extension_comparisons)]
async fn download_hf_model_artifacts(
    model_id: &str,
    revision: Option<&str>,
    hf_token: Option<String>,
) -> Result<(PathBuf, Vec<String>), anyhow::Error> {
    let token = hf_token.map(SecretString::from);
    let snapshot = llms::hf::open_model(
        llms::hf::Requester {
            kind: "model",
            name: model_id.to_string(),
            docs_url: "https://spiceai.org/docs/components/models/huggingface",
            retried: false,
        },
        model_id,
        revision,
        llms::hf::HubParams {
            token: token.as_ref(),
            token_in_spicepod: true,
            endpoint: None,
        },
    )
    .await?;
    let wanted: Vec<&str> = snapshot
        .files()
        .map(|file| file.path.as_str())
        .filter(|path| !(path.ends_with(".py") || path.ends_with(".md")))
        .collect();
    let weights = snapshot
        .download(&wanted)
        .await?
        .iter()
        .filter(|path| path_is_weights(path))
        .map(|path| path.display().to_string())
        .collect();
    Ok((snapshot.dir(), weights))
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
