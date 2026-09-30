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

//! System One evaluation model loader (`TypeSafe` Jev). Chat models get their
//! evaluator from [`super::LoadedChatModel::evaluator`].

#![expect(clippy::implicit_hasher)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use llms::chat::Error as LlmError;
use llms::evaluate::{Evaluate, EvaluateRequest, EvaluateResponse, Result as EvaluateResult};
use llms::typesafe::TypeSafe;
use opentelemetry::{Key, KeyValue, Value};
use runtime_rate_control::RateController;
use runtime_secrets::Secrets;
use secrecy::ExposeSecret;
use spicepod::component::model::{Model, ModelSource};
use tokio::sync::RwLock;

use super::chat::typed_params;
use super::metrics::{handle_metrics, handle_token_metrics};
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

    let metered = Metered {
        name: component.name.clone(),
        model: Arc::new(client),
    };
    Ok((Arc::new(metered) as Arc<dyn Evaluate>, rate_controller))
}

/// A System One model recorded in the LLM request, failure, duration and token metrics.
///
/// Evaluations are inference, so they belong in the same series as chat and responses.
/// A chat model's evaluator needs no such wrapper: every call it makes goes through the
/// chat model, which records it.
#[derive(Debug)]
struct Metered {
    name: String,
    model: Arc<dyn Evaluate>,
}

#[async_trait]
impl Evaluate for Metered {
    async fn evaluate(&self, request: EvaluateRequest) -> EvaluateResult<EvaluateResponse> {
        let labels = [KeyValue::new(
            Key::new("model"),
            Value::String(self.name.clone().into()),
        )];
        let start = Instant::now();
        let result = self.model.evaluate(request).await;
        handle_metrics(start.elapsed(), result.is_err(), &labels);
        if let Ok(EvaluateResponse {
            usage: Some(usage), ..
        }) = &result
        {
            handle_token_metrics(
                u32::try_from(usage.input_tokens).unwrap_or(u32::MAX),
                u32::try_from(usage.output_tokens).unwrap_or(u32::MAX),
                &labels,
            );
        }
        result
    }

    async fn health(&self) -> EvaluateResult<()> {
        self.model.health().await
    }
}

/// Whether this Spicepod model is an evaluation-only (non-chat) source.
#[must_use]
pub fn is_evaluate_only(component: &Model) -> bool {
    matches!(component.get_source(), Some(ModelSource::TypeSafe))
}
