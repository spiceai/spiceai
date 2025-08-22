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
    model::{try_to_chat_model, try_to_responses_model},
};
use llms::{
    chat::{Chat, try_map_boxed_error_to_box},
    responses::Responses,
};
use secrecy::SecretString;
use serde_json::Value;
use snafu::ResultExt;
use spicepod::component::model::{Model as SpicepodModel, ModelSource};

static DEFAULT_OPENAI_ENDPOINT: &str = "https://api.openai.com/v1";

fn supports_responses_api(
    spicepod_model: &SpicepodModel,
    params: &HashMap<String, SecretString>,
) -> bool {
    if let Some(value) = params.get("responses_api") {
        return secrecy::ExposeSecret::expose_secret(value)
            .trim()
            .eq_ignore_ascii_case("enabled");
    }

    if !matches!(
        spicepod_model.get_source(),
        Some(ModelSource::OpenAi | ModelSource::Azure)
    ) {
        return false;
    }
    match spicepod_model.params.get("endpoint") {
        None => true,
        Some(Value::String(s)) => s == DEFAULT_OPENAI_ENDPOINT,
        _ => false,
    }
}

impl Runtime {
    /// Loads a specific LLM from the spicepod. If an error occurs, no retry attempt is made.
    pub(crate) async fn load_llm(
        &self,
        m: SpicepodModel,
        params: HashMap<String, SecretString>,
    ) -> Result<(Option<Arc<dyn Chat>>, Option<Arc<dyn Responses>>)> {
        let completions_model = try_to_chat_model(&m, &params, Arc::new(self.clone()))
            .await
            .ok();

        let responses_model = if supports_responses_api(&m, &params) {
            try_to_responses_model(&m, &params, Arc::new(self.clone()))
                .await
                .ok()
        } else {
            None
        };

        // Perform only one health check, preferring the Responses API to Chat Completions
        if let Some(model) = &responses_model {
            model
                .health()
                .await
                .boxed()
                .map_err(try_map_boxed_error_to_box)
                .context(UnableToInitializeLlmSnafu)?;
        } else if let Some(model) = &completions_model {
            model
                .health()
                .await
                .boxed()
                .map_err(try_map_boxed_error_to_box)
                .context(UnableToInitializeLlmSnafu)?;
        }

        Ok((completions_model, responses_model))
    }
}
