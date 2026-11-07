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
use async_openai::config::OPENAI_API_BASE;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_credential_bridge::default_aws_config;
use llms::{
    anthropic::Anthropic,
    bedrock::chat::BedrockConverse,
    chat::{Chat, Error as ChatError, create_hf_model, create_local_model},
    config::GenericAuthMechanism,
    google::{Google, auth},
    openai::{ChatBackend, Openai, new_openai_client_with_token_provider},
    responses::Responses,
    xai::Xai,
};
use secrecy::SecretString;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use token_provider::workload_identity::{
    ANTHROPIC_AUDIENCE, Exchange, SubjectTokenSource, WorkloadIdentityTokenProvider,
};
use token_provider::{StaticTokenProvider, TokenProvider};

pub(crate) async fn create_bedrock(model_id: &str) -> Result<Arc<dyn Chat>, anyhow::Error> {
    let mut config_builder = default_aws_config();

    if let Some(region) = env_nonempty("SPICE_BEDROCK_REGION") {
        config_builder = config_builder.region(Region::new(region));
    }

    match (
        env_nonempty("SPICE_BEDROCK_ACCESS_KEY"),
        env_nonempty("SPICE_BEDROCK_SECRET_KEY"),
    ) {
        (Some(access_key), Some(secret_key)) => {
            config_builder = config_builder.credentials_provider(Credentials::new(
                access_key,
                secret_key,
                env_nonempty("SPICE_BEDROCK_SESSION_TOKEN"),
                None,
                "bedrock-chat",
            ));
        }
        (None, Some(_)) => {
            return Err(anyhow::anyhow!("SPICE_BEDROCK_ACCESS_KEY not set"));
        }
        (Some(_), None) => {
            return Err(anyhow::anyhow!("SPICE_BEDROCK_SECRET_KEY not set"));
        }
        // No key pair: the AWS credential chain, such as the role a GitHub Actions job assumes
        // with its OIDC token, or `AWS_ROLE_ARN` with `AWS_WEB_IDENTITY_TOKEN_FILE`.
        (None, None) => {}
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

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// The token CI authenticates `OpenAI` with: workload identity federation when
/// `OPENAI_IDENTITY_PROVIDER_ID` is set, exchanging the job's GitHub OIDC token and refreshing it;
/// otherwise `SPICE_OPENAI_API_KEY`, for a local run.
async fn openai_token() -> Result<Arc<dyn TokenProvider>, anyhow::Error> {
    let Some(provider_id) = env_nonempty("OPENAI_IDENTITY_PROVIDER_ID") else {
        let api_key = std::env::var("SPICE_OPENAI_API_KEY").unwrap_or_default();
        return Ok(Arc::new(StaticTokenProvider::new(SecretString::from(
            api_key,
        ))));
    };
    let service_account = env_nonempty("OPENAI_SERVICE_ACCOUNT_ID")
        .context("OPENAI_SERVICE_ACCOUNT_ID is not set")?;
    let audience =
        env_nonempty("OPENAI_WIF_AUDIENCE").unwrap_or_else(|| "https://api.openai.com".to_string());
    let source = SubjectTokenSource::github_actions_from_env(audience)?;
    let provider = WorkloadIdentityTokenProvider::try_new(
        "test model (openai)",
        source,
        Exchange::openai(provider_id, service_account),
    )
    .await?;
    Ok(Arc::new(provider))
}

async fn openai(model_id: &str) -> Result<Openai<llms::config::HostedModelConfig>, anyhow::Error> {
    Ok(new_openai_client_with_token_provider(
        model_id.to_string(),
        OPENAI_API_BASE,
        openai_token().await?,
        None,
        None,
        None,
        ChatBackend::ChatCompletions,
    )?)
}

pub(crate) async fn create_openai(model_id: &str) -> Result<Arc<dyn Chat>, anyhow::Error> {
    Ok(Arc::new(openai(model_id).await?))
}

pub(crate) async fn create_openai_responses(
    model_id: &str,
) -> Result<Arc<dyn Responses>, anyhow::Error> {
    Ok(Arc::new(openai(model_id).await?))
}

/// Anthropic with workload identity federation when `ANTHROPIC_FEDERATION_RULE_ID` is set, as in
/// CI; otherwise with `SPICE_ANTHROPIC_API_KEY` or `SPICE_ANTHROPIC_AUTH_TOKEN`.
pub(crate) async fn create_anthropic(
    model_id: Option<&str>,
) -> Result<Arc<dyn Chat>, anyhow::Error> {
    let auth = if let Some(rule_id) = env_nonempty("ANTHROPIC_FEDERATION_RULE_ID") {
        let source = SubjectTokenSource::github_actions_from_env(ANTHROPIC_AUDIENCE)?;
        let provider = WorkloadIdentityTokenProvider::try_new(
            "test model (anthropic)",
            source,
            Exchange::anthropic(
                rule_id,
                env_nonempty("ANTHROPIC_ORGANIZATION_ID")
                    .context("ANTHROPIC_ORGANIZATION_ID is not set")?,
                env_nonempty("ANTHROPIC_SERVICE_ACCOUNT_ID")
                    .context("ANTHROPIC_SERVICE_ACCOUNT_ID is not set")?,
                env_nonempty("ANTHROPIC_WORKSPACE_ID"),
            ),
        )
        .await?;
        GenericAuthMechanism::from_bearer_token_provider(Arc::new(provider))
    } else if let Some(api_key) = env_nonempty("SPICE_ANTHROPIC_API_KEY") {
        GenericAuthMechanism::from_api_key(api_key)
    } else if let Some(auth_token) = env_nonempty("SPICE_ANTHROPIC_AUTH_TOKEN") {
        GenericAuthMechanism::from_bearer_token(auth_token)
    } else {
        anyhow::bail!(
            "Set the `ANTHROPIC_*` workload identity federation IDs, 'SPICE_ANTHROPIC_API_KEY', or 'SPICE_ANTHROPIC_AUTH_TOKEN'"
        );
    };
    Ok(Arc::new(Anthropic::new(auth, model_id, None, None)?))
}

/// The Azure `OpenAI` model the suite runs against: deployment `AZURE_OPENAI_DEPLOYMENT` (default
/// `gpt-4o-mini`) of the resource at `AZURE_OPENAI_ENDPOINT`. It authenticates with Microsoft
/// Entra workload identity federation when `AZURE_OPENAI_CLIENT_ID` is set, as in CI, through the
/// same function a `spiced` Azure model uses; otherwise with `AZURE_OPENAI_API_KEY`.
pub(crate) async fn create_azure() -> Result<Arc<dyn Chat>, anyhow::Error> {
    let endpoint =
        env_nonempty("AZURE_OPENAI_ENDPOINT").context("AZURE_OPENAI_ENDPOINT is not set")?;
    let deployment =
        env_nonempty("AZURE_OPENAI_DEPLOYMENT").unwrap_or_else(|| "gpt-4o-mini".to_string());
    let api_version =
        env_nonempty("AZURE_OPENAI_API_VERSION").unwrap_or_else(|| "2024-10-21".to_string());

    if let Some(client_id) = env_nonempty("AZURE_OPENAI_CLIENT_ID") {
        let tenant_id =
            env_nonempty("AZURE_OPENAI_TENANT_ID").context("AZURE_OPENAI_TENANT_ID is not set")?;
        let federation = llms::workload_identity::AzureFederation {
            tenant_id: Some(&tenant_id),
            client_id: Some(&client_id),
            federated_token_audience: None,
            federated_token_file: None,
            endpoint: Some(&endpoint),
            api_version: Some(&api_version),
            deployment_name: Some(&deployment),
        };
        let client = llms::workload_identity::azure_federated_client(
            "test model (azure)",
            &deployment,
            &federation,
            None,
            false,
            // Only the IDs above, never this process's own `AZURE_*` identity.
            &llms::workload_identity::AzureEnvironment::default(),
            ChatBackend::ChatCompletions,
        )
        .await
        .map_err(|error| anyhow::anyhow!("{}", error.into_llm_error()))?
        .context("the Azure federation params configure no client")?;
        return Ok(Arc::new(client));
    }

    let api_key = env_nonempty("AZURE_OPENAI_API_KEY").context(
        "Set `AZURE_OPENAI_CLIENT_ID` and `AZURE_OPENAI_TENANT_ID` for workload identity federation, or `AZURE_OPENAI_API_KEY`",
    )?;
    Ok(Arc::new(llms::openai::new_azure_client(
        deployment.clone(),
        Some(&endpoint),
        Some(&api_version),
        Some(&deployment),
        None,
        Some(&api_key),
    )))
}

pub(crate) async fn create_hf(model_id: &str) -> Result<Arc<dyn Chat>, ChatError> {
    let token = std::env::var("HF_TOKEN").ok().map(SecretString::from);
    create_hf_model(
        model_id,
        None,
        &[],
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
