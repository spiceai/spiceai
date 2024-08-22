/*
Copyright 2024 The Spice.ai OSS Authors

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
use std::sync::Arc;
use tracing_futures::Instrument;

use crate::{
    embeddings::vector_search::{
        parse_explicit_primary_keys, to_matches, SearchRequest, VectorSearch,
    },
    tools::{parameters, SpiceModelTool},
    Runtime,
};

pub struct DocumentSimilarityTool {}
impl DocumentSimilarityTool {}

#[async_trait]
impl SpiceModelTool for DocumentSimilarityTool {
    fn name(&self) -> &'static str {
        "document_similarity"
    }

    fn description(&self) -> Option<&'static str> {
        Some("Search and retrieve documents from available datasets")
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<SearchRequest>()
    }

    async fn call(
        &self,
        arg: &str,
        rt: Arc<Runtime>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::document_similarity", tool = self.name(), input = arg);

        let tool_use_result = async {
            let mut req: SearchRequest = serde_json::from_str(arg)?;

            let vs = VectorSearch::new(
                rt.datafusion(),
                Arc::clone(&rt.embeds),
                parse_explicit_primary_keys(Arc::clone(&rt.app)).await,
            );

            // If model provides a `where` keyword in their [`where_cond`] field, strip it.
            if let Some(cond) = &req.where_cond {
                if cond.to_lowercase().starts_with("where ") {
                    req.where_cond = Some(cond[5..].to_string());
                }
            }

            let result = vs.search(&req).await.boxed()?;

            let matches = to_matches(&result).boxed()?;
            serde_json::value::to_value(matches).boxed()
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
