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

//! Native decision model loader (`TypeSafe` Jev, `OpenAI` decision models). Models without
//! a native Decisions API get their evaluator from [`super::LoadedChatModel::evaluator`].

#![expect(clippy::implicit_hasher)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use llms::chat::Error as LlmError;
use llms::evaluate::{Evaluate, EvaluateRequest, EvaluateResponse, Result as EvaluateResult};
use llms::openai::decisions::OpenAiDecisions;
use llms::typesafe::TypeSafe;
use opentelemetry::{Key, KeyValue, Value};
use runtime_rate_control::RateController;
use runtime_secrets::Secrets;
use secrecy::ExposeSecret;
use spicepod::component::model::{Model, ModelSource};
use tokio::sync::RwLock;

use super::chat::typed_params;
use super::metrics::{handle_metrics, handle_token_metrics};
use super::params::openai::OpenAiModelParams;
use super::params::typesafe::TypeSafeModelParams;

pub use llms::evaluate::EvaluateModelStore;

/// Construct the native [`Evaluate`] model of a Spicepod model component, whose requests
/// each take a permit from `rate_controller`.
///
/// Supports the providers with a native decisions API: [`ModelSource::TypeSafe`] Jev and
/// `OpenAI` decision models such as `gpt-6-luna`, including models that also support chat.
/// Other models answer decisions through their chat evaluator.
pub async fn try_to_evaluate_model(
    component: &Model,
    params: &HashMap<String, secrecy::SecretString>,
    secrets: &Arc<RwLock<Secrets>>,
    rate_controller: Arc<RateController>,
) -> Result<Arc<dyn Evaluate>, LlmError> {
    let source = component.get_source().ok_or(LlmError::UnknownModelSource {
        from: component.from.clone(),
    })?;

    match source {
        ModelSource::TypeSafe => typesafe(component, params, secrets, rate_controller).await,
        ModelSource::OpenAi => openai_decisions(component, params, secrets, rate_controller).await,
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
    rate_controller: Arc<RateController>,
) -> Result<Arc<dyn Evaluate>, LlmError> {
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
    let mut client = TypeSafe::try_new(component.name.clone(), model_id.as_deref(), api_key)
        .map_err(|e| LlmError::FailedToLoadModel {
            source: e.to_string().into(),
        })?
        .with_rate_controller(rate_controller);

    if typed.endpoint != llms::typesafe::DEFAULT_BASE_URL {
        client = client.with_base_url(typed.endpoint);
    }

    let metered = Metered {
        name: component.name.clone(),
        model: Arc::new(client),
    };
    Ok(Arc::new(metered) as Arc<dyn Evaluate>)
}

/// An `OpenAI` decision model (`from: openai:gpt-6-luna`), answering through `OpenAI`'s
/// Decisions API with the same key, endpoint and organization params as `OpenAI` chat
/// models.
async fn openai_decisions(
    component: &Model,
    params: &HashMap<String, secrecy::SecretString>,
    secrets: &Arc<RwLock<Secrets>>,
    rate_controller: Arc<RateController>,
) -> Result<Arc<dyn Evaluate>, LlmError> {
    let typed: OpenAiModelParams =
        typed_params(component, params, ModelSource::OpenAi, secrets).await?;
    let api_key = typed
        .api_key
        .as_ref()
        .map(|key| key.expose_secret().to_string());
    // OpenAI's own API needs a key; an OpenAI-compatible endpoint may not, as with chat.
    if api_key.is_none() && llms::openai::decisions::requires_api_key(&typed.endpoint) {
        return Err(LlmError::FailedToLoadModel {
            source: "No OpenAI API key provided. Set the `openai_api_key` param, or export OPENAI_API_KEY. See: https://spiceai.org/docs/components/models".into(),
        });
    }
    let model_id = component.get_model_id().unwrap_or_default();
    let client =
        OpenAiDecisions::try_new(component.name.clone(), model_id, typed.endpoint, api_key)
            .map_err(|e| LlmError::FailedToLoadModel {
                source: e.to_string().into(),
            })?
            .with_organization(typed.org_id, typed.project_id)
            .with_rate_controller(rate_controller);

    let metered = Metered {
        name: component.name.clone(),
        model: Arc::new(client),
    };
    Ok(Arc::new(metered) as Arc<dyn Evaluate>)
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
