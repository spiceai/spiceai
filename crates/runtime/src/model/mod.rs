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
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::col;
use datafusion::sql::TableReference;
use model_components::model::{Error as ModelError, Model};
use std::result::Result;
use std::sync::Arc;
use std::{collections::HashMap, io};
use tokio::sync::RwLock;

mod chat;
mod embed;
mod metrics;
mod model_context;
pub mod params;
pub(crate) mod provider_models;
pub(crate) mod rate_limit;
pub mod rerank;
mod responses;
mod tool_use;
mod tool_use_responses;
mod util;
mod wrapper;

pub use chat::{LLMChatCompletionsModelStore, try_to_chat_model};
pub use embed::{EmbeddingModelStore, try_to_embedding};
pub use model_context::{
    ModelContextExtension, ModelContextLayer, add_tools_used, track_ai_inferences_with_spice_count,
};
pub use rerank::{RerankerModelStore, try_to_rerank_model};
pub use responses::{LLMResponsesModelStore, ResponsesApiSupport, try_to_responses_model};
pub use tool_use::ToolUsingChat;
pub use tool_use_responses::ToolUsingResponses;

#[derive(Clone)]
pub struct LlmRuntimeStores {
    completion_llms: Arc<RwLock<LLMChatCompletionsModelStore>>,
    responses_llms: Arc<RwLock<LLMResponsesModelStore>>,
    rate_controllers: Arc<RwLock<HashMap<String, Arc<runtime_rate_control::RateController>>>>,
    responses_api_support: Arc<RwLock<HashMap<String, ResponsesApiSupport>>>,
}

impl Default for LlmRuntimeStores {
    fn default() -> Self {
        Self {
            completion_llms: Arc::new(RwLock::new(HashMap::new())),
            responses_llms: Arc::new(RwLock::new(HashMap::new())),
            rate_controllers: Arc::new(RwLock::new(HashMap::new())),
            responses_api_support: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl LlmRuntimeStores {
    #[must_use]
    pub fn completion_llms(&self) -> Arc<RwLock<LLMChatCompletionsModelStore>> {
        Arc::clone(&self.completion_llms)
    }

    #[must_use]
    pub fn responses_llms(&self) -> Arc<RwLock<LLMResponsesModelStore>> {
        Arc::clone(&self.responses_llms)
    }

    #[must_use]
    pub fn rate_controllers(
        &self,
    ) -> Arc<RwLock<HashMap<String, Arc<runtime_rate_control::RateController>>>> {
        Arc::clone(&self.rate_controllers)
    }

    #[must_use]
    pub fn responses_api_support(&self) -> Arc<RwLock<HashMap<String, ResponsesApiSupport>>> {
        Arc::clone(&self.responses_api_support)
    }
}

use crate::DataFusion;

pub static ENABLE_MODEL_SUPPORT_MESSAGE: &str = "To enable model support, either: \n  1) `spice install ai` \n  2) Build spiced binary with flag `--features models`.";

pub async fn run(m: &Model, df: Arc<DataFusion>) -> Result<RecordBatch, ModelError> {
    let dataset = TableReference::parse_str(&m.model.datasets[0]);
    let dataset_name = dataset
        .clone()
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
        .to_string();

    let Some(provider) = df.get_table(&dataset).await else {
        return Err(ModelError::UnableToRunModel {
            source: Box::new(io::Error::other(format!(
                "Model dataset not found: {dataset_name}"
            ))),
        });
    };

    let batches = df
        .ctx
        .read_table(provider)
        .map_err(|e| ModelError::UnableToRunModel {
            source: Box::new(e),
        })?
        .sort(vec![col("ts").sort(true, false)])
        .map_err(|e| ModelError::UnableToRunModel {
            source: Box::new(e),
        })?
        .collect()
        .await
        .map_err(|e| ModelError::UnableToRunModel {
            source: Box::new(e),
        })?;

    m.run(batches)
}
