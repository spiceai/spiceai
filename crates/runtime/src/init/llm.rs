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
use spicepod::component::model::{Model as SpicepodModel, ModelSource};

fn supports_responses_api(model_type: Option<ModelSource>) -> bool {
    model_type.is_some_and(|t| matches!(t, ModelSource::OpenAi))
}

impl Runtime {
    /// Loads a specific LLM from the spicepod. If an error occurs, no retry attempt is made.
    pub(crate) async fn load_llm(
        &self,
        m: SpicepodModel,
        params: HashMap<String, SecretString>,
    ) -> Result<(Arc<dyn Chat>, Option<Arc<dyn Responses>>)> {
        let l = try_to_chat_model(&m, &params, Arc::new(self.clone()))
            .await
            .boxed()
            .map_err(try_map_boxed_error_to_box)
            .context(UnableToInitializeLlmSnafu)?;

        let responses_model = if supports_responses_api(m.get_source()) {
            Some(
                try_to_responses_model(&m, &params, Arc::new(self.clone()))
                    .await
                    .boxed()
                    .map_err(try_map_boxed_error_to_box)
                    .context(UnableToInitializeLlmSnafu)?,
            )
        } else {
            None
        };

        l.health()
            .await
            .boxed()
            .map_err(try_map_boxed_error_to_box)
            .context(UnableToInitializeLlmSnafu)?;

        if let Some(responses_model) = &responses_model {
            responses_model
                .health()
                .await
                .boxed()
                .map_err(try_map_boxed_error_to_box)
                .context(UnableToInitializeLlmSnafu)?;
        }

        Ok((l, responses_model))
    }
}
