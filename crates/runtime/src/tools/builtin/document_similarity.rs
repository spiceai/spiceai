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
use async_trait::async_trait;
use serde_json::Value;
use snafu::ResultExt;
use std::{borrow::Cow, sync::Arc};
use tracing_futures::Instrument;

use crate::{
    Runtime,
    search::{
        request::{SearchRequest, SearchRequestAIJson},
        types::to_pretty,
        util::parse_explicit_primary_keys,
        vector_search::VectorSearch,
    },
    tools::{SpiceModelTool, utils::parameters},
};

pub struct DocumentSimilarityTool {
    name: String,
    description: String,
    rt: Arc<Runtime>,
}
impl DocumentSimilarityTool {
    #[must_use]
    pub fn new(rt: Arc<Runtime>, name: Option<&str>, description: Option<&str>) -> Self {
        Self {
            name: name.unwrap_or("document_similarity").to_string(),
            description: description
                .unwrap_or("Search and retrieve documents from available datasets")
                .to_string(),
            rt,
        }
    }
}

#[async_trait]
impl SpiceModelTool for DocumentSimilarityTool {
    fn name(&self) -> Cow<'_, str> {
        self.name.clone().into()
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self.description.as_str()))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<SearchRequestAIJson>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::document_similarity", tool = self.name().to_string(), input = arg);

        let tool_use_result = async {
            let req: SearchRequestAIJson = serde_json::from_str(arg)?;
            tracing::trace!("document_similarity tool use function call request: {req:?}");

            let vs = VectorSearch::new(
                self.rt.datafusion(),
                Arc::clone(&self.rt.embeds),
                parse_explicit_primary_keys(Arc::clone(&self.rt.app)).await,
            );

            let search_request = SearchRequest::try_from(req)?;

            let result = vs.search(&search_request).await.boxed()?;
            let mut formatted = serde_json::Map::with_capacity(result.len());
            for (tbl, result) in result {
                let displayed = to_pretty(result).await?;
                formatted.insert(tbl.to_string(), Value::String(displayed.to_string()));
            }
            Ok(Value::Object(formatted))
        }
        .instrument(span.clone())
        .await;

        match tool_use_result {
            Ok(value) => Ok(value),
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}
