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

//! What a local model's files show it to be, read when it loads and before its capabilities
//! are decided: a Strands decider is found by the content of its files, never by a param, and
//! is served by a decider engine instead of a chat engine ([`llms::decider`]).

use std::collections::HashMap;
use std::sync::Arc;

use llms::evaluate::Evaluate;
use model_capabilities::FoundKind;
use runtime_rate_control::RateController;
use runtime_secrets::Secrets;
use secrecy::SecretString;
use spicepod::component::model::Model;
use tokio::sync::RwLock;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// What a model's files show it to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoundModel {
    /// Nothing beyond what its provider and model id say.
    AsConfigured,
    /// A Strands decider, with the files it loads from.
    #[cfg(feature = "models")]
    StrandsDecider(llms::decider::DeciderFiles),
}

impl FoundModel {
    #[must_use]
    pub fn kind(&self) -> FoundKind {
        match self {
            Self::AsConfigured => FoundKind::AsConfigured,
            #[cfg(feature = "models")]
            Self::StrandsDecider(_) => FoundKind::StrandsDecider,
        }
    }
}

/// Reads the files of `component`, a model served on this node, to find what it is. Only a
/// local model (`file:` or `hf:`) can be anything other than [`FoundModel::AsConfigured`]; a
/// model split across nodes is left as configured.
///
/// # Errors
///
/// Returns an error if a decider's files are incomplete or cannot be served, or a Hugging Face
/// repository cannot be read. The error is an [`llms::decider::Error`], or an
/// [`llms::chat::Error`] for params that do not parse.
#[cfg_attr(
    not(feature = "models"),
    expect(
        clippy::unused_async,
        unused_variables,
        reason = "only a build with local models reads a model's files"
    )
)]
pub async fn find(
    component: &Model,
    params: &HashMap<String, SecretString>,
    secrets: &Arc<RwLock<Secrets>>,
) -> Result<FoundModel, BoxError> {
    #[cfg(feature = "models")]
    {
        use llms::chat::DistributedBackendSetting;
        use spicepod::component::model::ModelSource;

        use super::chat::typed_params;
        use super::params::file::FileModelParams;
        use super::params::huggingface::HuggingFaceModelParams;

        let found = match component.get_source() {
            Some(ModelSource::File) => {
                let typed: FileModelParams =
                    typed_params(component, params, ModelSource::File, secrets).await?;
                if typed.distributed_backend != DistributedBackendSetting::None {
                    return Ok(FoundModel::AsConfigured);
                }
                let files: Vec<std::path::PathBuf> = component
                    .get_all_file_paths()
                    .into_iter()
                    .map(std::path::PathBuf::from)
                    .collect();
                llms::decider::detect_local(&component.name, &files).await?
            }
            Some(ModelSource::HuggingFace) => {
                let typed: HuggingFaceModelParams =
                    typed_params(component, params, ModelSource::HuggingFace, secrets).await?;
                let Some(model_id) = component.get_model_id() else {
                    return Ok(FoundModel::AsConfigured);
                };
                if typed.distributed_backend != DistributedBackendSetting::None {
                    return Ok(FoundModel::AsConfigured);
                }
                let hub = llms::hf::HubParams {
                    token: typed.hf_token.as_ref(),
                    // As a chat model's: its `hf_token` is never loaded from a secret store.
                    token_in_spicepod: true,
                    endpoint: typed.hf_endpoint.as_deref(),
                };
                let selectors = component.gguf_selectors();
                let selectors: Vec<&str> = selectors.iter().map(String::as_str).collect();
                llms::decider::detect_hf(&model_id, &selectors, hub, &component.name).await?
            }
            _ => None,
        };
        Ok(found.map_or(FoundModel::AsConfigured, FoundModel::StrandsDecider))
    }
    #[cfg(not(feature = "models"))]
    Ok(FoundModel::AsConfigured)
}

/// The evaluator of a Strands decider found in `component`'s files, whose requests each take
/// a permit from `rate_controller`.
///
/// # Errors
///
/// Returns an error if no decider engine is linked, or it fails to load the decider.
#[cfg(feature = "models")]
pub async fn load(
    component: &Model,
    files: &llms::decider::DeciderFiles,
    rate_controller: Arc<RateController>,
) -> Result<Arc<dyn Evaluate>, llms::decider::Error> {
    let model = llms::decider::load(llms::decider::DeciderLoad {
        name: &component.name,
        files,
        rate_controller,
    })
    .await?;
    Ok(super::evaluate::metered(&component.name, model))
}

/// Whether a model load error is a mistake in the Spicepod or the build, which no retry clears.
#[must_use]
pub fn is_configuration_error(error: &(dyn std::error::Error + 'static)) -> bool {
    #[cfg(feature = "models")]
    if let Some(error) = error.downcast_ref::<llms::decider::Error>() {
        return error.is_configuration_error();
    }
    #[cfg(not(feature = "models"))]
    let _ = error;
    false
}
