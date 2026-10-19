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

use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    Result, Runtime, UnableToInitializeLlmSnafu,
    model::{LoadedChatModel, try_to_chat_model, try_to_messages_model, try_to_responses_model},
};
use llms::{chat::try_map_boxed_error_to_box, responses::Responses};
use messages_api::Messages;
use model_capabilities::{Capability, ModelCapabilities};
use secrecy::SecretString;
use snafu::ResultExt;
use spicepod::component::model::{Model as SpicepodModel, ModelSource};

/// A chat model, and the endpoints of other APIs its provider serves natively.
pub(crate) struct LoadedLlm {
    pub(crate) chat: LoadedChatModel,
    pub(crate) responses: Option<Arc<dyn Responses>>,
    pub(crate) messages: Option<Arc<dyn Messages>>,
}

impl Runtime {
    /// Loads a chat model from the spicepod, and its Responses API and native Messages API
    /// endpoints when `capabilities` include them. If an error occurs, no retry attempt is made.
    ///
    /// A Responses or Messages endpoint that fails to load is left out, so the model is
    /// unavailable on `/v1/responses` or `/v1/messages` while its chat still serves.
    pub(crate) async fn load_llm(
        &self,
        m: SpicepodModel,
        params: HashMap<String, SecretString>,
        capabilities: &ModelCapabilities,
    ) -> Result<LoadedLlm> {
        let completions_model = try_to_chat_model(&m, &params, Arc::new(self.clone()))
            .await
            .boxed()
            .map_err(try_map_boxed_error_to_box)
            .context(UnableToInitializeLlmSnafu)?;

        completions_model
            .chat
            .health()
            .await
            .boxed()
            .map_err(try_map_boxed_error_to_box)
            .context(UnableToInitializeLlmSnafu)?;

        // A native endpoint is registered without a health check of its own: the chat model's
        // already sent the provider a Messages API request.
        let messages = match capabilities.messages() {
            Some(backend) => match try_to_messages_model(
                &m,
                &params,
                Arc::new(self.clone()),
                &completions_model,
                backend,
            )
            .await
            {
                Ok(model) => model,
                Err(e) => {
                    tracing::warn!("{}", messages_unavailable_warning(&m.name, &e));
                    None
                }
            },
            None => None,
        };

        if !capabilities.has(Capability::Responses) {
            return Ok(LoadedLlm {
                chat: completions_model,
                responses: None,
                messages,
            });
        }

        let mut responses_model = match try_to_responses_model(&m, &params, Arc::new(self.clone()))
            .await
        {
            Ok(model) => Some(model),
            Err(e) => {
                tracing::warn!(
                    "Failed to construct Responses API endpoint for model '{}': {e}. The model will not be available via /v1/responses.",
                    m.name
                );
                None
            }
        };

        // `OpenAI` and Azure chat models already probe the backend at load: `/v1/responses`
        // when `responses_api` is enabled, `/v1/chat/completions` otherwise. A second probe
        // sends a real LLM request for no extra signal, so the Responses model is
        // registered without one and any failure surfaces on first use.
        let already_probed = matches!(
            m.get_source(),
            Some(ModelSource::OpenAi | ModelSource::Azure)
        );
        if !already_probed
            && let Some(model) = &responses_model
            && let Err(e) = model.health().await
        {
            tracing::warn!("{}", responses_unavailable_warning(&m.name, &e));
            responses_model = None;
        }

        Ok(LoadedLlm {
            chat: completions_model,
            responses: responses_model,
            messages,
        })
    }
}

/// The warning for a model whose native Messages API endpoint failed to load.
fn messages_unavailable_warning(model: &str, cause: &llms::chat::Error) -> String {
    format!(
        "Failed to load the Messages API endpoint for model '{model}', so the model is not available on `/v1/messages`. Cause: {cause}"
    )
}

/// The warning for a model whose Responses API endpoint failed its health check. The
/// cause already says how to fix it, so the warning names the model and what is lost.
fn responses_unavailable_warning(model: &str, cause: &llms::responses::Error) -> String {
    format!(
        "Failed to load the Responses API endpoint for model '{model}', so the model is not available on `/v1/responses`. Cause: {cause}"
    )
}

#[cfg(test)]
mod tests {
    use super::responses_unavailable_warning;

    // regression test for #14910: the cause's fix sentence was followed by a second one.
    #[test]
    fn responses_unavailable_warning_states_the_fix_once() {
        let cause = llms::responses::Error::HealthCheckError {
            source: "404 Not Found".into(),
        };
        assert_eq!(
            responses_unavailable_warning("mock_ok", &cause),
            "Failed to load the Responses API endpoint for model 'mock_ok', so the model is not available on `/v1/responses`. Cause: Failed to invoke the model: 404 Not Found. Verify the model configuration and try again."
        );
    }
}
