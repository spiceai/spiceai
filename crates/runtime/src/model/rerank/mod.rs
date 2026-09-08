/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Reranker loader: turns a [`spicepod::component::rerankers::Reranker`] spec
//! into a runtime [`llms::rerank::Rerank`] instance. Mirrors
//! [`crate::model::embed::try_to_embedding`] but targets the reranker trait.

pub mod params;

#[cfg(feature = "models")]
use llms::rerank::TeiRerank;
use llms::rerank::{CohereReranker, HttpReranker, JinaReranker, Rerank, VoyageReranker};
#[cfg(feature = "models")]
use params::file::FileRerankerParams;
#[cfg(feature = "models")]
use params::huggingface::HuggingFaceRerankerParams;
use params::{
    cohere::CohereRerankerParams, http::HttpRerankerParams, jina::JinaRerankerParams,
    voyage::VoyageRerankerParams,
};
use runtime_parameters_typed::{ParamsError, TypedParams};
use runtime_secrets::{Secrets, get_params_with_secrets};
use secrecy::ExposeSecret;
use snafu::{OptionExt, ResultExt, Snafu};
use spicepod::component::rerankers::{Reranker, RerankerPrefix};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

pub use llms::rerank::RerankerModelStore;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display(
        "Unknown reranker source in `from: {from}`. Supported prefixes: cohere:, voyage:, jina:, http://, https://."
    ))]
    UnknownSource { from: String },

    #[snafu(display(
        "Reranker '{name}' requires `params.{param_key}` (or the equivalent secret reference). Set it in your Spicepod and retry."
    ))]
    MissingParam { name: String, param_key: String },

    #[snafu(display("Reranker '{name}' failed health check: {source}"))]
    HealthFailed {
        name: String,
        source: llms::rerank::Error,
    },

    #[snafu(display("Failed to build reranker '{name}': {source}"))]
    BuildFailed {
        name: String,
        source: llms::rerank::Error,
    },

    #[snafu(display("Reranker '{name}' has an invalid `params.{param_key}`: {reason}"))]
    InvalidParam {
        name: String,
        param_key: String,
        reason: String,
    },

    #[snafu(display(
        "Reranker '{name}' uses a local model source (`{from}`), but this build was compiled without the `models` feature. Rebuild with `models` enabled or use a remote reranker (cohere:, voyage:, jina:, http://)."
    ))]
    LocalRerankerNotEnabled { name: String, from: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Wraps a typed-params deserialization failure with the reranker name, same
/// pattern `try_to_embedding` uses for embedding providers.
fn params_err(name: &str, e: ParamsError) -> Error {
    match e {
        ParamsError::MissingRequired { user_key, .. } => Error::MissingParam {
            name: name.to_string(),
            param_key: user_key,
        },
        ParamsError::InvalidValue { user_key, reason } => Error::InvalidParam {
            name: name.to_string(),
            param_key: user_key,
            reason,
        },
        ParamsError::UnknownParameter {
            user_key,
            supported,
        } => Error::InvalidParam {
            name: name.to_string(),
            param_key: user_key,
            reason: format!("unknown parameter. Supported: {supported}"),
        },
    }
}

/// Construct a `Rerank` instance from a Spicepod `Reranker` component.
///
/// Secret-bearing params (`api_key`, `endpoint`) are resolved through the
/// runtime's `Secrets` registry before the client is built, same pattern
/// `try_to_embedding` uses for OpenAI/Azure keys.
pub async fn try_to_rerank_model(
    component: &Reranker,
    secrets: Arc<RwLock<Secrets>>,
) -> Result<Arc<dyn Rerank>> {
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

    let prefix = component.get_prefix().context(UnknownSourceSnafu {
        from: component.from.clone(),
    })?;
    let model_id = component.get_model_id().context(UnknownSourceSnafu {
        from: component.from.clone(),
    })?;
    let component_name = format!("reranker {}", component.name);

    let reranker: Arc<dyn Rerank> = match prefix {
        RerankerPrefix::Cohere => {
            let typed = CohereRerankerParams::try_from_params(&component_name, params, &secrets)
                .await
                .map_err(|e| params_err(&component.name, e))?;
            let mut c =
                CohereReranker::try_new(component.name.clone(), typed.api_key.expose_secret())
                    .context(BuildFailedSnafu {
                        name: component.name.clone(),
                    })?
                    .with_model_id(model_id);
            if let Some(endpoint) = typed.endpoint {
                c = c.with_endpoint(endpoint);
            }
            Arc::new(c)
        }
        RerankerPrefix::Voyage => {
            let typed = VoyageRerankerParams::try_from_params(&component_name, params, &secrets)
                .await
                .map_err(|e| params_err(&component.name, e))?;
            let mut v =
                VoyageReranker::try_new(component.name.clone(), typed.api_key.expose_secret())
                    .context(BuildFailedSnafu {
                        name: component.name.clone(),
                    })?
                    .with_model_id(model_id);
            if let Some(endpoint) = typed.endpoint {
                v = v.with_endpoint(endpoint);
            }
            Arc::new(v)
        }
        RerankerPrefix::Jina => {
            let typed = JinaRerankerParams::try_from_params(&component_name, params, &secrets)
                .await
                .map_err(|e| params_err(&component.name, e))?;
            let mut j =
                JinaReranker::try_new(component.name.clone(), typed.api_key.expose_secret())
                    .context(BuildFailedSnafu {
                        name: component.name.clone(),
                    })?
                    .with_model_id(model_id);
            if let Some(endpoint) = typed.endpoint {
                j = j.with_endpoint(endpoint);
            }
            Arc::new(j)
        }
        RerankerPrefix::Http => {
            // For HTTP BYO the `from` *is* the endpoint URL. Model id +
            // auth header are both optional (some self-hosted services pin
            // the model and auth upstream).
            let typed = HttpRerankerParams::try_from_params(&component_name, params, &secrets)
                .await
                .map_err(|e| params_err(&component.name, e))?;
            let endpoint = model_id;
            let mut h = HttpReranker::try_new(component.name.clone(), endpoint).context(
                BuildFailedSnafu {
                    name: component.name.clone(),
                },
            )?;
            if let Some(api_key) = typed.api_key {
                h = h.with_api_key(Some(api_key.expose_secret().to_string()));
            }
            if let Some(id) = typed.model {
                h = h.with_model_id(Some(id));
            }
            Arc::new(h)
        }
        #[cfg(feature = "models")]
        RerankerPrefix::HuggingFace => {
            // `get_model_id` joins any pinned revision onto the repo id as
            // `org/model:revision`; recover the two halves before the repo id
            // reaches the Hub (same convention as embeddings/models).
            let (repo_id, revision) = spicepod::component::model::split_hf_model_id(&model_id);
            let typed =
                HuggingFaceRerankerParams::try_from_params(&component_name, params, &secrets)
                    .await
                    .map_err(|e| params_err(&component.name, e))?;
            let hf_token = typed.hf_token.as_ref().map(ExposeSecret::expose_secret);
            let truncation = typed.truncate.unwrap_or_default().direction();
            let r = TeiRerank::from_hf(
                component.name.clone(),
                repo_id,
                revision,
                hf_token,
                typed.max_seq_length,
                truncation,
            )
            .await
            .context(BuildFailedSnafu {
                name: component.name.clone(),
            })?;
            Arc::new(r)
        }
        #[cfg(feature = "models")]
        RerankerPrefix::File => {
            let typed = FileRerankerParams::try_from_params(&component_name, params, &secrets)
                .await
                .map_err(|e| params_err(&component.name, e))?;
            let truncation = typed.truncate.unwrap_or_default().direction();
            let r = TeiRerank::from_dir(
                component.name.clone(),
                std::path::Path::new(&model_id),
                typed.max_seq_length,
                truncation,
            )
            .await
            .context(BuildFailedSnafu {
                name: component.name.clone(),
            })?;
            Arc::new(r)
        }
        #[cfg(not(feature = "models"))]
        RerankerPrefix::HuggingFace | RerankerPrefix::File => {
            return Err(Error::LocalRerankerNotEnabled {
                name: component.name.clone(),
                from: component.from.clone(),
            });
        }
    };

    Ok(reranker)
}
