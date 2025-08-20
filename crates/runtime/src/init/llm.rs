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
use snafu::ResultExt;
use spicepod::component::model::Model as SpicepodModel;

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

        let mut responses_model = try_to_responses_model(&m, &params, Arc::new(self.clone()))
            .await
            .ok();

        if let Some(model) = &completions_model {
            model
                .health()
                .await
                .boxed()
                .map_err(try_map_boxed_error_to_box)
                .context(UnableToInitializeLlmSnafu)?;
        }

        if let Some(model) = responses_model {
            if model.health().await.is_ok() {
                responses_model = Some(model);
            } else {
                responses_model = None;
            }
        }

        Ok((completions_model, responses_model))
    }
}
