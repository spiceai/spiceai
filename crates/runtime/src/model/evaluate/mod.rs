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

//! System One evaluation model loader (TypeSafe Jev).

#![allow(clippy::implicit_hasher)]

use llms::chat::Error as LlmError;
use llms::evaluate::Evaluate;
use llms::typesafe::TypeSafe;
use runtime_parameters_typed::TypedParams;
use runtime_secrets::Secrets;
use secrecy::ExposeSecret;
use spicepod::component::model::{Model, ModelSource};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::params::typesafe::TypeSafeModelParams;

pub use llms::evaluate::EvaluateModelStore;

/// Construct an [`Evaluate`] model from a Spicepod model component.
///
/// Only [`ModelSource::TypeSafe`] is supported today. Other sources should use
/// chat / embeddings / responses loaders.
pub async fn try_to_evaluate_model(
    component: &Model,
    params: &HashMap<String, secrecy::SecretString>,
    secrets: &Arc<RwLock<Secrets>>,
) -> Result<Arc<dyn Evaluate>, LlmError> {
    let source = component.get_source().ok_or(LlmError::UnknownModelSource {
        from: component.from.clone(),
    })?;

    match source {
        ModelSource::TypeSafe => typesafe(component, params, secrets).await,
        other => Err(LlmError::UnsupportedTaskForModel {
            from: other.to_string(),
            task: "evaluate".to_string(),
        }),
    }
}

async fn typesafe(
    component: &Model,
    params: &HashMap<String, secrecy::SecretString>,
    secrets: &Arc<RwLock<Secrets>>,
) -> Result<Arc<dyn Evaluate>, LlmError> {
    let typed = TypeSafeModelParams::try_from_params(
        &format!("model {}", ModelSource::TypeSafe),
        params.clone(),
        secrets,
    )
    .await
    .map_err(|e| LlmError::ModelParameterFailed {
        model: component.name.clone(),
        source: Box::new(e),
    })?;

    let Some(api_key) = typed.api_key.as_ref().map(ExposeSecret::expose_secret) else {
        return Err(LlmError::FailedToLoadModel {
            source: "No `typesafe_api_key` (or `typesafe_ai_api_key`) provided for TypeSafe model. Set the param or export TYPESAFE_API_KEY.".into(),
        });
    };

    let model_id = component.get_model_id();
    let mut client = TypeSafe::try_new(component.name.clone(), model_id.as_deref(), api_key)
        .map_err(|e| LlmError::FailedToLoadModel {
            source: e.to_string().into(),
        })?;

    if typed.endpoint != llms::typesafe::DEFAULT_BASE_URL {
        client = client.with_base_url(typed.endpoint);
    }

    Ok(Arc::new(client) as Arc<dyn Evaluate>)
}

/// Whether this Spicepod model is an evaluation-only (non-chat) source.
#[must_use]
pub fn is_evaluate_only(component: &Model) -> bool {
    matches!(component.get_source(), Some(ModelSource::TypeSafe))
}
