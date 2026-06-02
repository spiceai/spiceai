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

use llms::rerank::{CohereReranker, HttpReranker, JinaReranker, Rerank, VoyageReranker};
use runtime_secrets::{Secrets, get_params_with_secrets};
use secrecy::{ExposeSecret, SecretString};
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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    let params = get_params_with_secrets(secrets, &string_params).await;

    let prefix = component.get_prefix().context(UnknownSourceSnafu {
        from: component.from.clone(),
    })?;
    let model_id = component.get_model_id().context(UnknownSourceSnafu {
        from: component.from.clone(),
    })?;

    let reranker: Arc<dyn Rerank> = match prefix {
        RerankerPrefix::Cohere => {
            let api_key = extract_secret(&params, "api_key")
                .or_else(|| extract_secret(&params, "cohere_api_key"))
                .context(MissingParamSnafu {
                    name: component.name.clone(),
                    param_key: "api_key".to_string(),
                })?;
            let mut c = CohereReranker::try_new(component.name.clone(), api_key)
                .context(BuildFailedSnafu {
                    name: component.name.clone(),
                })?
                .with_model_id(model_id);
            if let Some(endpoint) = extract_secret(&params, "endpoint") {
                c = c.with_endpoint(endpoint);
            }
            Arc::new(c)
        }
        RerankerPrefix::Voyage => {
            let api_key = extract_secret(&params, "api_key")
                .or_else(|| extract_secret(&params, "voyage_api_key"))
                .context(MissingParamSnafu {
                    name: component.name.clone(),
                    param_key: "api_key".to_string(),
                })?;
            let mut v = VoyageReranker::try_new(component.name.clone(), api_key)
                .context(BuildFailedSnafu {
                    name: component.name.clone(),
                })?
                .with_model_id(model_id);
            if let Some(endpoint) = extract_secret(&params, "endpoint") {
                v = v.with_endpoint(endpoint);
            }
            Arc::new(v)
        }
        RerankerPrefix::Jina => {
            let api_key = extract_secret(&params, "api_key")
                .or_else(|| extract_secret(&params, "jina_api_key"))
                .context(MissingParamSnafu {
                    name: component.name.clone(),
                    param_key: "api_key".to_string(),
                })?;
            let mut j = JinaReranker::try_new(component.name.clone(), api_key)
                .context(BuildFailedSnafu {
                    name: component.name.clone(),
                })?
                .with_model_id(model_id);
            if let Some(endpoint) = extract_secret(&params, "endpoint") {
                j = j.with_endpoint(endpoint);
            }
            Arc::new(j)
        }
        RerankerPrefix::Http => {
            // For HTTP BYO the `from` *is* the endpoint URL. Model id +
            // auth header are both optional (some self-hosted services pin
            // the model and auth upstream).
            let endpoint = model_id;
            let mut h = HttpReranker::try_new(component.name.clone(), endpoint).context(
                BuildFailedSnafu {
                    name: component.name.clone(),
                },
            )?;
            if let Some(api_key) = extract_secret(&params, "api_key") {
                h = h.with_api_key(Some(api_key));
            }
            if let Some(id) = extract_secret(&params, "model") {
                h = h.with_model_id(Some(id));
            }
            Arc::new(h)
        }
    };

    Ok(reranker)
}

fn extract_secret(params: &HashMap<String, SecretString>, key: &str) -> Option<String> {
    params.get(key).map(|v| v.expose_secret().to_string())
}
