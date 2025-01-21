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

use crate::{model::try_to_chat_model, Result, Runtime, UnableToInitializeLlmSnafu};
use llms::chat::{try_map_boxed_error_to_box, Chat};
use secrecy::SecretString;
use snafu::ResultExt;
use spicepod::component::model::Model as SpicepodModel;

impl Runtime {
    /// Loads a specific LLM from the spicepod. If an error occurs, no retry attempt is made.
    pub(crate) async fn load_llm(
        &self,
        m: SpicepodModel,
        params: HashMap<String, SecretString>,
    ) -> Result<Box<dyn Chat>> {
        let l = try_to_chat_model(&m, &params, Arc::new(self.clone()))
            .await
            .boxed()
            .map_err(try_map_boxed_error_to_box)
            .context(UnableToInitializeLlmSnafu)?;

        l.health()
            .await
            .boxed()
            .map_err(try_map_boxed_error_to_box)
            .context(UnableToInitializeLlmSnafu)?;
        Ok(l)
    }
}
