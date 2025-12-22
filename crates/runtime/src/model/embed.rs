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

use crate::token_providers::databricks::{DatabricksM2MTokenProvider, DatabricksU2MTokenProvider};
use bytes::Bytes;
use cache::CacheProvider;
use cache::result::embeddings::CachedEmbeddingResult;
use itertools::Itertools;
use llms::HealthCheck;
#[cfg(feature = "bedrock")]
use llms::bedrock::{
    self,
    embed::{
        cohere::{CohereEmbeddingInputType, CohereEmbeddingTruncate, CohereEmbeddingType},
        nova::{NovaEmbeddingPurpose, NovaTruncationMode},
    },
};
use runtime_secrets::{Secrets, get_params_with_secrets};

use llms::embeddings::{
    Embed, Error as EmbedError,
    candle::{download_hf_file, tei::TeiEmbed},
};
use llms::model2vec::Model2Vec;
use llms::openai::embed::OpenaiEmbed;
use llms::openai::{DEFAULT_EMBEDDING_MODEL, UsageTier};
use secrecy::{ExposeSecret, SecretBox, SecretString};
use snafu::ResultExt;
use spicepod::component::{embeddings::EmbeddingPrefix, model::ModelFileType};
use std::path::{Path, PathBuf};
use std::result::Result;
use std::str::FromStr;
use std::{collections::HashMap, sync::Arc};
use token_provider::registry::TokenProviderRegistry;
use tokio::fs;
use tokio::sync::RwLock;
use url::Url;

pub type EmbeddingModelStore = HashMap<String, Arc<dyn Embed>>;

/// Extract a secret from a hashmap of secrets, if it exists.
macro_rules! extract_secret {
    ($params:expr, $key:expr) => {
        $params.get($key).map(secrecy::ExposeSecret::expose_secret)
    };
}

pub async fn try_to_embedding(
    component: &spicepod::component::embeddings::Embeddings,
    secrets: Arc<RwLock<Secrets>>,
    token_provider_registry: Arc<TokenProviderRegistry>,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let string_params: HashMap<String, String> = component
        .params
        .iter()
        .map(|(k, v)| {
            (
                k.clone(),
                match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                },
            )
        })
        .collect();

    let params = get_params_with_secrets(Arc::clone(&secrets), &string_params).await;
    let model_id = component.get_model_id();
    let prefix = component
        .get_prefix()
        .ok_or(EmbedError::UnknownModelSource {
            from: component.from.clone(),
        })?;

    match prefix {
        EmbeddingPrefix::Azure => azure(
            model_id,
            component.name.as_str(),
            &params,
            embeddings_cache.clone(),
        ),
        EmbeddingPrefix::OpenAi => {
            openai(
                model_id,
                component,
                &params,
                secrets,
                embeddings_cache.clone(),
            )
            .await
        }
        EmbeddingPrefix::File => {
            file(
                model_id.as_deref(),
                component,
                &params,
                embeddings_cache.clone(),
            )
            .await
        }
        EmbeddingPrefix::HuggingFace => {
            huggingface(&component.name, model_id, &params, embeddings_cache.clone()).await
        }
        EmbeddingPrefix::Google => google(model_id, &params, embeddings_cache.clone()),
        EmbeddingPrefix::Databricks => {
            databricks(
                model_id,
                &params,
                Arc::clone(&token_provider_registry),
                embeddings_cache.clone(),
            )
            .await
        }
        #[cfg(feature = "bedrock")]
        EmbeddingPrefix::Bedrock => bedrock(model_id, &params, embeddings_cache.clone()).await,
        #[cfg(not(feature = "bedrock"))]
        EmbeddingPrefix::Bedrock => Err(EmbedError::UnknownModelSource {
            from: "bedrock".to_string(),
        }),
        EmbeddingPrefix::Model2Vec => model2vec(model_id, &params, embeddings_cache.clone()),
    }
}

fn model2vec(
    model_id: Option<String>,
    params: &HashMap<String, SecretString>,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let Some(model_id) = model_id else {
        return Err(EmbedError::ModelNotProvided {
            model_source: "model2vec".to_string(),
        });
    };

    let hf_token = params
        .get("hf_token")
        .map(secrecy::ExposeSecret::expose_secret);

    let subfolder = params
        .get("subfolder")
        .map(secrecy::ExposeSecret::expose_secret);

    let normalize = params
        .get("normalize")
        .and_then(|ss| ss.expose_secret().parse::<bool>().ok());

    let parallelism = params
        .get("parallelism")
        .and_then(|ss| ss.expose_secret().parse::<usize>().ok());

    let embed_max_token_length = params
        .get("embed_max_token_length")
        .and_then(|ss| ss.expose_secret().parse::<usize>().ok());

    let embed_custom_batch_size = params
        .get("embed_custom_batch_size")
        .and_then(|ss| ss.expose_secret().parse::<usize>().ok());

    Model2Vec::from_params(
        &model_id,
        hf_token,
        normalize,
        subfolder,
        parallelism,
        embed_max_token_length,
        embed_custom_batch_size,
    )
    .map(|m| Arc::new(m.set_cache(embeddings_cache)) as Arc<dyn Embed>)
    .map_err(|e| EmbedError::FailedToInstantiateEmbeddingModel {
        source: Box::new(e),
    })
}

fn google(
    model_id: Option<String>,
    params: &HashMap<String, SecretString>,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let Some(model_id) = model_id else {
        return Err(EmbedError::ModelNotProvided {
            model_source: "google".to_string(),
        });
    };
    let Some(api_key) = params.get("google_api_key") else {
        return Err(EmbedError::FailedToInstantiateEmbeddingModel {
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "`google_api_key` is required.",
            )),
        });
    };

    let dimensions: Option<u32> = params
        .get("google_dimensions")
        .map(|d| d.expose_secret().parse())
        .transpose()
        // Only error if user provided dimensions.
        .map_err(|e| EmbedError::FailedToInstantiateEmbeddingModel {
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Failed to parse 'dimensions' as u32 parameter: {e}"),
            )),
        })?;
    let google =
        llms::google::Google::new_embeddings(api_key, &model_id, dimensions, embeddings_cache)
            .map_err(|e| EmbedError::FailedToInstantiateEmbeddingModel {
                source: Box::new(std::io::Error::other(format!(
                    "Failed to create Google embeddings client: {e}"
                ))),
            })?;

    Ok(Arc::new(google) as Arc<dyn Embed>)
}

#[cfg(feature = "bedrock")]
async fn bedrock(
    model_id: Option<String>,
    params: &HashMap<String, SecretString>,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let Some(model_id) = model_id else {
        return Err(EmbedError::ModelNotProvided {
            model_source: "bedrock".to_string(),
        });
    };

    let client = super::util::create_bedrock_client(params, "bedrock-embed")
        .await
        .map_err(|e| EmbedError::FailedToInstantiateEmbeddingModel { source: e })?;

    if model_id.starts_with("amazon.titan-embed") {
        let normalize = params
            .get("normalize")
            .map(|s| s.expose_secret().parse::<bool>())
            .transpose()
            .map_err(|e| EmbedError::FailedToInstantiateEmbeddingModel {
                source: format!("Failed to parse 'normalize' parameter: {e}").into(),
            })?
            .unwrap_or(true);

        let Some(dimensions) = params
            .get("dimensions")
            .map(|s| s.expose_secret().parse::<u32>())
            .transpose()
            .map_err(|e| EmbedError::FailedToInstantiateEmbeddingModel {
                source: format!("Failed to parse 'dimensions' parameter: {e}").into(),
            })?
        else {
            return Err(EmbedError::MissingParamError {
                param_key: "dimensions",
            });
        };

        if !matches!(dimensions, 256 | 512 | 1024) {
            return Err(EmbedError::FailedToInstantiateEmbeddingModel {
                source: format!(
                    "Invalid dimensions '{dimensions}' for Titan model. Must be 256, 512, or 1024"
                )
                .into(),
            });
        }

        Ok(Arc::new(
            bedrock::embed::new_titan_v2(client, normalize, dimensions).set_cache(embeddings_cache),
        ) as Arc<dyn Embed>)
    } else if model_id.starts_with("cohere.embed") {
        let truncate = if let Some(truncate_str) =
            extract_secret!(params, "truncate_mode").or(extract_secret!(params, "truncate"))
        {
            CohereEmbeddingTruncate::from_str(truncate_str)
                .boxed()
                .map_err(|e| EmbedError::InvalidParamError {
                    param_key: "truncate_mode",
                    value: truncate_str.to_string(),
                    reason: e.to_string(),
                })?
        } else {
            CohereEmbeddingTruncate::default()
        };
        let input_type_str = extract_secret!(params, "input_type");
        let input_type = input_type_str
            .map(CohereEmbeddingInputType::from_str)
            .transpose()
            .map_err(|e| EmbedError::InvalidParamError {
                param_key: "input_type",
                value: input_type_str.unwrap_or_default().to_string(),
                reason: e.to_string(),
            })?
            .unwrap_or_default();
        Ok(Arc::new(
            bedrock::embed::new_cohere(
                client,
                model_id,
                truncate,
                input_type,
                CohereEmbeddingType::Float,
            )
            .set_cache(embeddings_cache),
        ) as Arc<dyn Embed>)
    } else if model_id.starts_with("amazon.nova-2-multimodal-embeddings") {
        let Some(dimensions) = params
            .get("dimensions")
            .map(|s| s.expose_secret().parse::<u32>())
            .transpose()
            .map_err(|e| EmbedError::FailedToInstantiateEmbeddingModel {
                source: format!("Failed to parse 'dimensions' parameter: {e}").into(),
            })?
        else {
            return Err(EmbedError::MissingParamError {
                param_key: "dimensions",
            });
        };

        if !matches!(dimensions, 256 | 384 | 1024 | 3072) {
            return Err(EmbedError::FailedToInstantiateEmbeddingModel {
                source: format!(
                    "Invalid dimensions '{dimensions}' for Nova model. Must be 256, 384, 1024, or 3072"
                )
                .into(),
            });
        }

        let embedding_purpose_str = params
            .get("embedding_purpose")
            .map(ExposeSecret::expose_secret);
        let embedding_purpose = embedding_purpose_str
            .map(NovaEmbeddingPurpose::from_str)
            .transpose()
            .map_err(|_| EmbedError::FailedToInstantiateEmbeddingModel {
                source: format!(
                    "Invalid 'embedding_purpose' parameter: '{}'",
                    embedding_purpose_str.unwrap_or_default()
                )
                .into(),
            })?
            .unwrap_or_default();

        let truncate = if let Some(truncate_str) =
            extract_secret!(params, "truncate_mode").or(extract_secret!(params, "truncate"))
        {
            NovaTruncationMode::from_str(truncate_str)
                .boxed()
                .map_err(|e| EmbedError::InvalidParamError {
                    param_key: "truncate_mode",
                    value: truncate_str.to_string(),
                    reason: e.to_string(),
                })?
        } else {
            NovaTruncationMode::default()
        };
        Ok(Arc::new(
            bedrock::embed::new_text_only_nova_multimodal(
                client,
                dimensions,
                embedding_purpose,
                truncate,
            )
            .set_cache(embeddings_cache),
        ) as Arc<dyn Embed>)
    } else {
        Err(EmbedError::ModelDoesNotExist {
            model_name: model_id,
        })
    }
}

async fn huggingface(
    name: &String,
    model_id: Option<String>,
    params: &HashMap<String, SecretString>,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let hf_token = extract_secret!(params, "hf_token");
    let pooling = extract_secret!(params, "pooling");
    let max_seq_len = max_seq_length_from_params(params)?;
    if let Some(id) = model_id {
        Ok(Arc::new(
            TeiEmbed::from_hf(&id, None, hf_token, pooling, max_seq_len)
                .await?
                .set_cache(embeddings_cache)
                .set_cache_model_id(name),
        ))
    } else {
        Err(EmbedError::ModelNotProvided {
            model_source: "huggingface".to_string(),
        })
    }
}

async fn databricks(
    model_id: Option<String>,
    params: &HashMap<String, SecretString>,
    token_provider_registry: Arc<TokenProviderRegistry>,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let Some(endpoint) = extract_secret!(params, "databricks_endpoint") else {
        return Err(EmbedError::MissingParamError {
            param_key: "databricks_endpoint",
        });
    };
    let Some(model_id) = model_id else {
        return Err(EmbedError::ModelNotProvided {
            model_source: "databricks".to_string(),
        });
    };

    let token_opt = extract_secret!(params, "databricks_token");
    let client_id = extract_secret!(params, "databricks_client_id");
    let client_secret = extract_secret!(params, "databricks_client_secret");

    #[cfg(feature = "databricks")]
    let user_agent = Some(data_components::databricks::user_agent());
    #[cfg(not(feature = "databricks"))]
    let user_agent: Option<&'static str> = None;

    match (token_opt, client_id, client_secret) {
        (Some(_), Some(_) | None, Some(_)) => {
            Err(EmbedError::FailedToInstantiateEmbeddingModel {
                source: "Either `databricks_token` or `databricks_client_id` and `databricks_client_secret` should be provided, not both.".into(),
            })
        }
        (Some(_), Some(_), None)|(None, None, None) => {
            Err(EmbedError::FailedToInstantiateEmbeddingModel {
                source: "Either `databricks_token` or `databricks_client_id` and `databricks_client_secret` should be provided.".into(),
            })
        }
        (None, None, Some(_client_secret)) => {
            Err(EmbedError::FailedToInstantiateEmbeddingModel {
                source: "If `databricks_client_secret` is provided, `databricks_client_id` must also be provided.".into(),
            })
        }
        (Some(token), None, None) => Ok(Arc::new(llms::databricks::from_access_token(
            endpoint,
            model_id.as_str(),
            token,
            user_agent,
        ).set_cache(embeddings_cache)) as Arc<dyn Embed>),

        (None, Some(client_id), Some(client_secret)) => {
            let token_provider = token_provider_registry
                .get_or_create_provider(format!("databricks_m2m_{client_id}"), || async {
                    DatabricksM2MTokenProvider::try_new(
                        endpoint.to_string(),
                        client_id.to_string(),
                        client_secret.into(),
                    )
                    .await
                })
                .await
            .map_err(|e| EmbedError::FailedToInstantiateEmbeddingModel {
                source: Box::from(format!(
                    "Could not retrieve M2M tokens from Databricks. Error: {e}"
                )),
            })?;
            Ok(Arc::new(
                llms::databricks::from_token_provider(
                    endpoint,
                    model_id.as_str(),
                    token_provider,
                    user_agent,
                    HealthCheck::Required,
                ).set_cache(embeddings_cache),
            ) as Arc<dyn Embed>)
        }
        (None, Some(client_id), None) => {
            let token_provider = token_provider_registry
                .get_or_create_provider::<DatabricksU2MTokenProvider, std::convert::Infallible, _, _>(format!("databricks_u2m_{client_id}"), || async {
                    Ok(DatabricksU2MTokenProvider::new(
                        endpoint.to_string(),
                        client_id.to_string(),
                    ))
                })
                .await
            .map_err(|e| EmbedError::FailedToInstantiateEmbeddingModel {
                source: Box::from(format!(
                    "Could not retrieve U2M tokens from Databricks. Error: {e}"
                )),
            })?;
            Ok(Arc::new(
                llms::databricks::from_token_provider(
                    endpoint,
                    model_id.as_str(),
                    token_provider,
                    user_agent,
                    HealthCheck::Skip,
                ).set_cache(embeddings_cache),
            ) as Arc<dyn Embed>)
        }
    }
}

async fn file(
    model_id: Option<&str>,
    component: &spicepod::component::embeddings::Embeddings,
    params: &HashMap<String, SecretString>,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
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
    let pooling = params
        .get("pooling")
        .map(SecretBox::expose_secret)
        .map(String::from);
    let max_seq_len = max_seq_length_from_params(params)?;
    Ok(Arc::new(
        TeiEmbed::from_local(
            Path::new(&weights_path),
            Path::new(&config_path),
            Path::new(&tokenizer_path),
            pooling,
            max_seq_len,
        )
        .await?
        .set_cache(embeddings_cache)
        .set_cache_model_id(&component.name),
    ))
}

fn azure(
    model_id: Option<String>,
    model_name: &str,
    params: &HashMap<String, SecretString>,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
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

    Ok(Arc::new(
        OpenaiEmbed::new(
            llms::openai::new_azure_client(
                model_name,
                api_base,
                api_version,
                deployment_name,
                entra_token,
                api_key,
            ),
            None,
        )
        .set_cache(embeddings_cache),
    ))
}

async fn openai(
    model_id: Option<String>,
    component: &spicepod::component::embeddings::Embeddings,
    params: &HashMap<String, SecretString>,
    secrets: Arc<RwLock<Secrets>>,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    // If parameter is from secret store, it will have `openai_` prefix
    let openai_usage_tier = params
        .get("usage_tier")
        .or(params.get("openai_usage_tier"))
        .map(secrecy::ExposeSecret::expose_secret)
        .map(UsageTier::from_str)
        .transpose()?;

    let mut embed = OpenaiEmbed::new(
        llms::openai::new_openai_client(
            model_id.unwrap_or(DEFAULT_EMBEDDING_MODEL.to_string()),
            extract_secret!(params, "endpoint"),
            params
                .get("api_key")
                .or(params.get("openai_api_key"))
                .map(secrecy::ExposeSecret::expose_secret),
            params
                .get("org_id")
                .or(params.get("openai_org_id"))
                .map(secrecy::ExposeSecret::expose_secret),
            params
                .get("project_id")
                .or(params.get("openai_project_id"))
                .map(secrecy::ExposeSecret::expose_secret),
            openai_usage_tier,
        ),
        openai_usage_tier.map(Into::into),
    )
    .set_cache(embeddings_cache);

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
    Ok(Arc::new(embed))
}

/// Retrieves [`Bytes`] for a file/url path.
///
/// Supports:
///   - [`object_store`] compatible URLs.
///   - Huggingface URLs, e.g. `<https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/blob/main/tokenizer.json>`.
///   - Huggingface `FssSpec`: `hf://[<repo_type_prefix>]<repo_id>[@<revision>]/<path/in/repo>`.
async fn get_bytes_for_file(
    url: &str,
    params: &HashMap<String, SecretString>,
) -> Result<Bytes, Box<dyn std::error::Error + Send + Sync>> {
    match url.split('/').collect_vec().as_slice() {
        [
            "https:",
            "",
            "huggingface.co",
            org_id,
            model_id,
            "blob",
            branch,
            file @ ..,
        ] => {
            get_file_from_hf(
                None,
                org_id,
                model_id,
                Some(branch),
                file.join("/").as_str(),
                params
                    .get("hf_token")
                    .map(secrecy::ExposeSecret::expose_secret),
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
                params
                    .get("hf_token")
                    .map(secrecy::ExposeSecret::expose_secret),
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
                params
                    .get("hf_token")
                    .map(secrecy::ExposeSecret::expose_secret),
            )
            .await
        }
        ["hf:", "", "models", org_id, model_id_revision, file @ ..]
        | ["hf:", "", org_id, model_id_revision, file @ ..] => {
            let (model_id, branch) = parse_model_id_w_revision(model_id_revision);
            get_file_from_hf(
                Some("models"),
                org_id,
                model_id,
                branch,
                file.join("/").as_str(),
                params
                    .get("hf_token")
                    .map(secrecy::ExposeSecret::expose_secret),
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
            secrecy::ExposeSecret::expose_secret(s)
                .parse()
                .boxed()
                .map_err(|e| EmbedError::FailedToInstantiateEmbeddingModel {
                    source: format!("Failed to parse 'max_seq_length' parameter: {e}").into(),
                })
        })
        .transpose()
}
