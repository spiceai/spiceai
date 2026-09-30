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

//! Evaluation models: System One providers (`TypeSafe` Jev) loaded from their own
//! Spicepod source, and an evaluator over every chat model.

#![expect(clippy::implicit_hasher)]

use evaluate_chat::ChatEvaluator;
use llms::chat::{Chat, Error as LlmError};
use llms::evaluate::Evaluate;
use llms::typesafe::TypeSafe;
use runtime_rate_control::RateController;
use runtime_secrets::Secrets;
use secrecy::ExposeSecret;
use spicepod::component::model::{Model, ModelSource};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::chat::typed_params;
use super::params::typesafe::TypeSafeModelParams;
use super::rate_limit::build_model_rate_controller;

pub use llms::evaluate::EvaluateModelStore;

/// Construct an [`Evaluate`] model from a Spicepod model component.
///
/// Only [`ModelSource::TypeSafe`] is supported today. Other sources should use
/// chat / embeddings / responses loaders.
///
/// Returns the evaluation model and the rate controller that should also be
/// registered in the runtime's model rate-controller map.
pub async fn try_to_evaluate_model(
    component: &Model,
    params: &HashMap<String, secrecy::SecretString>,
    secrets: &Arc<RwLock<Secrets>>,
) -> Result<(Arc<dyn Evaluate>, Arc<RateController>), LlmError> {
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
) -> Result<(Arc<dyn Evaluate>, Arc<RateController>), LlmError> {
    let typed: TypeSafeModelParams =
        typed_params(component, params, ModelSource::TypeSafe, secrets).await?;

    let api_key = match typed.api_key.as_ref().map(ExposeSecret::expose_secret) {
        Some(key) => key.to_string(),
        None => {
            // TypedParams autoload covers `typesafe_api_key` only; also accept
            // the documented AI SDK env alias `TYPESAFE_AI_API_KEY`.
            runtime_parameters_typed::autoload_secret(
                secrets,
                &format!("model {}", ModelSource::TypeSafe),
                "typesafe_ai_api_key",
            )
            .await
            .map(|s| s.expose_secret().to_string())
            .ok_or_else(|| LlmError::FailedToLoadModel {
                source: "No TypeSafe API key provided. Set the `typesafe_api_key` param (alias `typesafe_ai_api_key`), or export one of TYPESAFE_API_KEY or TYPESAFE_AI_API_KEY. See: https://spiceai.org/docs/components/models".into(),
            })?
        }
    };

    let model_id = component.get_model_id();
    let rate_controller = build_model_rate_controller(component, params);
    let mut client = TypeSafe::try_new(component.name.clone(), model_id.as_deref(), api_key)
        .map_err(|e| LlmError::FailedToLoadModel {
            source: e.to_string().into(),
        })?
        .with_rate_controller(Arc::clone(&rate_controller));

    if typed.endpoint != llms::typesafe::DEFAULT_BASE_URL {
        client = client.with_base_url(typed.endpoint);
    }

    Ok((Arc::new(client) as Arc<dyn Evaluate>, rate_controller))
}

/// The evaluator `/v1/evaluate` uses for the chat model the Spicepod names `name`.
///
/// Pass the model without runtime tools ([`super::LoadedChatModel::without_tools`]): an
/// evaluation's `state` is untrusted input and must not be able to steer a tool call.
#[must_use]
pub fn chat_evaluator(name: &str, chat: Arc<dyn Chat>) -> Arc<dyn Evaluate> {
    Arc::new(ChatEvaluator::new(name, chat))
}

/// Whether this Spicepod model is an evaluation-only (non-chat) source.
#[must_use]
pub fn is_evaluate_only(component: &Model) -> bool {
    matches!(component.get_source(), Some(ModelSource::TypeSafe))
}
