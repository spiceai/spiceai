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

use crate::embeddings::params as embedding_params;
use crate::token_providers::databricks::{DatabricksM2MTokenProvider, DatabricksU2MTokenProvider};
use bytes::Bytes;
use cache::CacheProvider;
use cache::result::embeddings::CachedEmbeddingResult;
#[cfg(feature = "models")]
use itertools::Itertools;
use llms::HealthCheck;
#[cfg(feature = "bedrock")]
use llms::bedrock::{
    self,
    embed::{
        cohere::{CohereEmbeddingTruncate, CohereEmbeddingType},
        nova::NovaTruncationMode,
    },
};
use runtime_parameters_typed::TypedParams;
use runtime_secrets::{Secrets, get_params_with_secrets};

use object_store::ObjectStoreExt;

#[cfg(feature = "models")]
use llms::embeddings::candle::{download_hf_file, tei::TeiEmbed};
use llms::embeddings::{Embed, Error as EmbedError};
#[cfg(feature = "models")]
use llms::model2vec::Model2Vec;
use llms::openai::DEFAULT_EMBEDDING_MODEL;
use llms::openai::embed::OpenaiEmbed;
use secrecy::{ExposeSecret, SecretString};
use snafu::ResultExt;
#[cfg(feature = "models")]
use spicepod::component::embeddings::pinned_revision;
use spicepod::component::{embeddings::EmbeddingPrefix, model::ModelFileType};
#[cfg(feature = "models")]
use std::path::Path;
use std::path::PathBuf;
use std::result::Result;
use std::str::FromStr;
use std::{collections::HashMap, sync::Arc};
use token_provider::registry::TokenProviderRegistry;
#[cfg(feature = "models")]
use tokio::fs;
use tokio::sync::RwLock;
use url::Url;

pub type EmbeddingModelStore = HashMap<String, Arc<dyn Embed>>;

/// Wraps a typed-params deserialization failure the same way `Parameters`
/// errors were surfaced for embeddings.
fn params_err(e: runtime_parameters_typed::ParamsError) -> EmbedError {
    EmbedError::FailedToInstantiateEmbeddingModel {
        source: Box::new(e),
    }
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

    let model_id = component.get_model_id();
    let prefix = component
        .get_prefix()
        .ok_or(EmbedError::UnknownModelSource {
            from: component.from.clone(),
        })?;
    let params = get_params_with_secrets(Arc::clone(&secrets), &string_params).await;
    let component_name = format!("embedding {}", component.name);

    match prefix {
        EmbeddingPrefix::Azure => {
            let typed = embedding_params::azure::AzureEmbeddingParams::try_from_params(
                &component_name,
                params,
                &secrets,
            )
            .await
            .map_err(params_err)?;
            azure(
                model_id,
                component.name.as_str(),
                &typed,
                embeddings_cache.clone(),
            )
        }
        EmbeddingPrefix::OpenAi => {
            let typed = embedding_params::openai::OpenAiEmbeddingParams::try_from_params(
                &component_name,
                params,
                &secrets,
            )
            .await
            .map_err(params_err)?;
            openai(
                model_id,
                component,
                &typed,
                secrets,
                embeddings_cache.clone(),
            )
            .await
        }
        #[cfg(feature = "models")]
        EmbeddingPrefix::File => {
            let typed = embedding_params::file::FileEmbeddingParams::try_from_params(
                &component_name,
                params,
                &secrets,
            )
            .await
            .map_err(params_err)?;
            file(
                model_id.as_deref(),
                component,
                &typed,
                embeddings_cache.clone(),
            )
            .await
        }
        #[cfg(not(feature = "models"))]
        EmbeddingPrefix::File => Err(EmbedError::UnknownModelSource {
            from: "file".to_string(),
        }),
        #[cfg(feature = "models")]
        EmbeddingPrefix::HuggingFace => {
            let typed = embedding_params::huggingface::HuggingFaceEmbeddingParams::try_from_params(
                &component_name,
                params,
                &secrets,
            )
            .await
            .map_err(params_err)?;
            huggingface(&component.name, model_id, &typed, embeddings_cache.clone()).await
        }
        #[cfg(not(feature = "models"))]
        EmbeddingPrefix::HuggingFace => Err(EmbedError::UnknownModelSource {
            from: "huggingface".to_string(),
        }),
        EmbeddingPrefix::Google => {
            let typed = embedding_params::google::GoogleEmbeddingParams::try_from_params(
                &component_name,
                params,
                &secrets,
            )
            .await
            .map_err(params_err)?;
            google(model_id, &typed, embeddings_cache.clone())
        }
        EmbeddingPrefix::Databricks => {
            let typed = embedding_params::databricks::DatabricksEmbeddingParams::try_from_params(
                &component_name,
                params,
                &secrets,
            )
            .await
            .map_err(params_err)?;
            databricks(
                model_id,
                &typed,
                Arc::clone(&token_provider_registry),
                embeddings_cache.clone(),
            )
            .await
        }
        #[cfg(feature = "bedrock")]
        EmbeddingPrefix::Bedrock => {
            let typed = embedding_params::bedrock::BedrockEmbeddingParams::try_from_params(
                &component_name,
                params,
                &secrets,
            )
            .await
            .map_err(params_err)?;
            bedrock(model_id, &typed, embeddings_cache.clone()).await
        }
        #[cfg(not(feature = "bedrock"))]
        EmbeddingPrefix::Bedrock => Err(EmbedError::UnknownModelSource {
            from: "bedrock".to_string(),
        }),
        #[cfg(feature = "models")]
        EmbeddingPrefix::Model2Vec => {
            let typed = embedding_params::model2vec::Model2VecEmbeddingParams::try_from_params(
                &component_name,
                params,
                &secrets,
            )
            .await
            .map_err(params_err)?;
            model2vec(model_id, &typed, embeddings_cache.clone())
        }
        #[cfg(not(feature = "models"))]
        EmbeddingPrefix::Model2Vec => Err(EmbedError::UnknownModelSource {
            from: "model2vec".to_string(),
        }),
    }
}

#[cfg(feature = "models")]
fn model2vec(
    model_id: Option<String>,
    params: &embedding_params::model2vec::Model2VecEmbeddingParams,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let Some(model_id) = model_id else {
        return Err(EmbedError::ModelNotProvided {
            model_source: "model2vec".to_string(),
        });
    };

    // `model2vec:` has no revision plumbing: `EmbeddingPrefix::Model2Vec` is a bare
    // `strip_prefix`, so a trailing `:rev` stays glued to the model id, and
    // `StaticModel::from_pretrained` takes no revision argument to hand it to. The id
    // therefore reaches the Hub as part of the *repository name*, which 401s — a bare
    // auth error for what is really an unsupported-configuration mistake (#12445).
    //
    // Say so instead. `pinned_revision` defers to the same regex the `huggingface:`
    // arm uses rather than splitting on the last ':', so the two agree about what a
    // revision is: only an `org/model:rev` shape has one, and a local path — the other
    // thing `from_pretrained` accepts, including a Windows `C:/…` — passes through.
    //
    // The `exists()` guard mirrors `from_pretrained`'s own precedence, which tries the
    // id as a local path before treating it as a repo name. Without it, a directory
    // that happened to match the `org/model:rev` shape would be rejected here even
    // though the loader would have opened it — so this only ever rejects ids that
    // really would have gone to the Hub and 401'd.
    if let Some(revision) = pinned_revision(&model_id)
        .filter(|_| !Path::new(&model_id).exists())
        .map(ToString::to_string)
    {
        return Err(EmbedError::RevisionPinningUnsupported {
            model_source: "model2vec".to_string(),
            model_id,
            revision,
        });
    }

    Model2Vec::from_params(
        &model_id,
        params.hf_token.as_ref().map(ExposeSecret::expose_secret),
        params.normalize,
        params.subfolder.as_deref(),
        params.parallelism,
        params.embed_max_token_length,
        params.embed_custom_batch_size,
    )
    .map(|m| Arc::new(m.set_cache(embeddings_cache)) as Arc<dyn Embed>)
    .map_err(|e| EmbedError::FailedToInstantiateEmbeddingModel {
        source: Box::new(e),
    })
}

fn google(
    model_id: Option<String>,
    params: &embedding_params::google::GoogleEmbeddingParams,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let Some(model_id) = model_id else {
        return Err(EmbedError::ModelNotProvided {
            model_source: "google".to_string(),
        });
    };
    let google = llms::google::Google::new_embeddings(
        &params.api_key,
        &model_id,
        params.dimensions,
        embeddings_cache,
    )
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
    params: &embedding_params::bedrock::BedrockEmbeddingParams,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let Some(model_id) = model_id else {
        return Err(EmbedError::ModelNotProvided {
            model_source: "bedrock".to_string(),
        });
    };

    let runtime_params = params.runtime_params();
    let client = super::util::create_bedrock_client(&runtime_params, "bedrock-embed")
        .await
        .map_err(|e| EmbedError::FailedToInstantiateEmbeddingModel { source: e })?;

    if model_id.starts_with("amazon.titan-embed") {
        let normalize = params.normalize.unwrap_or(true);

        let Some(dimensions) = params.dimensions else {
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
        let truncate = if let Some(truncate_str) = params.truncate_mode.as_deref() {
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
        let input_type = params.input_type.clone().unwrap_or_default();
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
        let Some(dimensions) = params.dimensions else {
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

        let embedding_purpose = params.embedding_purpose.clone().unwrap_or_default();

        let truncate = if let Some(truncate_str) = params.truncate_mode.as_deref() {
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

#[cfg(feature = "models")]
async fn huggingface(
    name: &String,
    model_id: Option<String>,
    params: &embedding_params::huggingface::HuggingFaceEmbeddingParams,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let hf_token = params.hf_token.as_ref().map(ExposeSecret::expose_secret);
    let pooling = params.pooling.map(embedding_params::Pooling::as_str);
    let truncation = params.truncate.unwrap_or_default();
    if let Some(id) = model_id {
        // `get_model_id` joins a pinned revision onto the repo id as `org/model:revision`, so
        // the two halves have to be recovered before the repo id reaches the Hub. Passing the
        // joined string through requests a repo named `org/model:revision`, which does not
        // exist, and leaves the revision defaulted to `main`. `chat` splits the same
        // convention back out; this path did not.
        let (repo_id, revision) = spicepod::component::model::split_hf_model_id(&id);
        Ok(Arc::new(
            TeiEmbed::from_hf(
                repo_id,
                revision,
                hf_token,
                pooling,
                params.max_seq_length,
                truncation.truncates(),
                truncation.direction(),
            )
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
    params: &embedding_params::databricks::DatabricksEmbeddingParams,
    token_provider_registry: Arc<TokenProviderRegistry>,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let endpoint = params.endpoint.as_str();
    let Some(model_id) = model_id else {
        return Err(EmbedError::ModelNotProvided {
            model_source: "databricks".to_string(),
        });
    };

    let token_opt = params.token.as_ref().map(ExposeSecret::expose_secret);
    let client_id = params.client_id.as_deref();
    let client_secret = params
        .client_secret
        .as_ref()
        .map(ExposeSecret::expose_secret);

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
                .get_or_create_provider(format!("databricks_m2m_{endpoint}_{client_id}"), || async {
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
                .get_or_create_provider::<DatabricksU2MTokenProvider, std::convert::Infallible, _, _>(format!("databricks_u2m_{endpoint}_{client_id}"), || async {
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

#[cfg(feature = "models")]
async fn file(
    model_id: Option<&str>,
    component: &spicepod::component::embeddings::Embeddings,
    params: &embedding_params::file::FileEmbeddingParams,
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
        .pooling
        .map(|p| embedding_params::Pooling::as_str(p).to_string());
    let truncation = params.truncate.unwrap_or_default();
    Ok(Arc::new(
        TeiEmbed::from_local(
            Path::new(&weights_path),
            Path::new(&config_path),
            Path::new(&tokenizer_path),
            pooling,
            params.max_seq_length,
            truncation.truncates(),
            truncation.direction(),
        )
        .await?
        .set_cache(embeddings_cache)
        .set_cache_model_id(&component.name),
    ))
}

fn azure(
    model_id: Option<String>,
    model_name: &str,
    params: &embedding_params::azure::AzureEmbeddingParams,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let Some(model_name) = model_id else {
        return Err(EmbedError::FailedToInstantiateEmbeddingModel {
            source: format!("For embedding model '{model_name}', model id must be specified in `from:azure:<model_id>`.").into(),
        });
    };
    let api_key = params.api_key.as_ref().map(ExposeSecret::expose_secret);
    let entra_token = params.entra_token.as_ref().map(ExposeSecret::expose_secret);
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
                params.endpoint.as_deref(),
                params.api_version.as_deref(),
                params.deployment_name.as_deref(),
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
    params: &embedding_params::openai::OpenAiEmbeddingParams,
    secrets: Arc<RwLock<Secrets>>,
    embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
) -> Result<Arc<dyn Embed>, EmbedError> {
    let usage_tier = params.usage_tier;

    let mut embed = OpenaiEmbed::new(
        llms::openai::new_openai_client(
            model_id.unwrap_or(DEFAULT_EMBEDDING_MODEL.to_string()),
            Some(params.endpoint.as_str()),
            params.api_key.as_ref().map(ExposeSecret::expose_secret),
            params.org_id.as_deref(),
            params.project_id.as_deref(),
            Some(usage_tier),
        ),
        Some(usage_tier.into()),
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
    #[cfg(not(feature = "models"))]
    let _ = params;

    #[cfg(feature = "models")]
    {
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
                return get_file_from_hf(
                    None,
                    org_id,
                    model_id,
                    Some(branch),
                    file.join("/").as_str(),
                    params
                        .get("hf_token")
                        .map(secrecy::ExposeSecret::expose_secret),
                )
                .await;
            }
            ["hf:", "", "datasets", org_id, model_id_revision, file @ ..] => {
                let (model_id, branch) = parse_model_id_w_revision(model_id_revision);

                return get_file_from_hf(
                    Some("datasets"),
                    org_id,
                    model_id,
                    branch,
                    file.join("/").as_str(),
                    params
                        .get("hf_token")
                        .map(secrecy::ExposeSecret::expose_secret),
                )
                .await;
            }
            ["hf:", "", "spaces", org_id, model_id_revision, file @ ..] => {
                let (model_id, branch) = parse_model_id_w_revision(model_id_revision);
                return get_file_from_hf(
                    Some("spaces"),
                    org_id,
                    model_id,
                    branch,
                    file.join("/").as_str(),
                    params
                        .get("hf_token")
                        .map(secrecy::ExposeSecret::expose_secret),
                )
                .await;
            }
            ["hf:", "", "models", org_id, model_id_revision, file @ ..]
            | ["hf:", "", org_id, model_id_revision, file @ ..] => {
                let (model_id, branch) = parse_model_id_w_revision(model_id_revision);
                return get_file_from_hf(
                    Some("models"),
                    org_id,
                    model_id,
                    branch,
                    file.join("/").as_str(),
                    params
                        .get("hf_token")
                        .map(secrecy::ExposeSecret::expose_secret),
                )
                .await;
            }
            _ => {}
        }
    }

    // Fallback: non-HuggingFace URLs or when models feature is disabled
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

/// From `hf://` spec, parse the `model_id` that may have a revision attached `all-MiniLM-L6-v2@main`.
///
/// `all-MiniLM-L6-v2` -> (`all-MiniLM-L6-v2`, None)
/// `all-MiniLM-L6-v2@main` -> (`all-MiniLM-L6-v2`, Some(`main`))
#[cfg(feature = "models")]
fn parse_model_id_w_revision(model_w_revision: &str) -> (&str, Option<&str>) {
    match model_w_revision.split_once('@') {
        Some((model_id, revision)) => (model_id, Some(revision)),
        None => (model_w_revision, None),
    }
}

#[cfg(feature = "models")]
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
