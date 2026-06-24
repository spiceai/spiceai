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
    model::{ResponsesApiSupport, try_to_chat_model, try_to_responses_model},
};
use llms::{
    chat::{Chat, try_map_boxed_error_to_box},
    responses::Responses,
};
use secrecy::SecretString;
use snafu::ResultExt;
use spicepod::component::model::Model as SpicepodModel;

impl Runtime {
    /// Loads a specific LLM from the spicepod. If an error occurs, no retry attempt is made.
    pub(crate) async fn load_llm(
        &self,
        m: SpicepodModel,
        params: HashMap<String, SecretString>,
    ) -> Result<(
        Arc<dyn Chat>,
        Option<Arc<dyn Responses>>,
        ResponsesApiSupport,
    )> {
        let completions_model = try_to_chat_model(&m, &params, Arc::new(self.clone()))
            .await
            .boxed()
            .map_err(try_map_boxed_error_to_box)
            .context(UnableToInitializeLlmSnafu)?;

        completions_model
            .health()
            .await
            .boxed()
            .map_err(try_map_boxed_error_to_box)
            .context(UnableToInitializeLlmSnafu)?;

        let mut responses_support = ResponsesApiSupport::Unavailable;
        let mut responses_model = match try_to_responses_model(&m, &params, Arc::new(self.clone()))
            .await
        {
            Ok(model) => {
                responses_support = ResponsesApiSupport::Supported;
                Some(model)
            }
            Err(llms::chat::Error::ResponsesNotSupported { from }) => {
                responses_support = ResponsesApiSupport::UnsupportedProvider {
                    provider: from.short_name().to_string(),
                };
                None
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to construct Responses API endpoint for model '{}': {e}. The model will not be available via /v1/responses.",
                    m.name
                );
                None
            }
        };

        if let Some(model) = &responses_model
            && let Err(e) = model.health().await
        {
            tracing::warn!(
                "Failed to load Responses API endpoint for model '{}': {e}. Verify the Spicepod configuration and try again.",
                m.name.clone()
            );
            responses_model = None;
            responses_support = ResponsesApiSupport::Unavailable;
        }

        Ok((completions_model, responses_model, responses_support))
    }
}
