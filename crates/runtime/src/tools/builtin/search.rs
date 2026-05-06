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
use datafusion::sql::TableReference;
use serde_json::Value;
use snafu::ResultExt;
use std::{borrow::Cow, sync::Arc};
use tracing_futures::Instrument;

use runtime_datafusion::allowlist::ResolvedTableAwareAllowlist;

use crate::search::search_engine::SearchEngine;
use crate::tools::builtin::list_datasets::get_dataset_elements;
use crate::{
    Runtime,
    search::{
        request::{SearchRequest, SearchRequestBaseJson},
        types::to_pretty,
        util::parse_explicit_primary_keys,
    },
    tools::{SpiceModelTool, utils::parameters},
};
use runtime_request_context::{AsyncMarker, RequestContext};

pub struct SearchTool {
    name: String,
    description: String,
    rt: Arc<Runtime>,
    table_allowlist: Option<ResolvedTableAwareAllowlist>,
}
impl SearchTool {
    #[must_use]
    pub fn new(rt: Arc<Runtime>, name: Option<&str>, description: Option<&str>) -> Self {
        Self {
            name: name.unwrap_or("search").to_string(),
            description: description
                .unwrap_or("Search across available, searchable datasets")
                .to_string(),
            rt,
            table_allowlist: None,
        }
    }
    #[must_use]
    pub fn with_table_allowlist(mut self, allowlist: Option<ResolvedTableAwareAllowlist>) -> Self {
        self.table_allowlist = allowlist;
        self
    }
}

#[async_trait]
impl SpiceModelTool for SearchTool {
    fn name(&self) -> Cow<'_, str> {
        self.name.clone().into()
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self.description.as_str()))
    }

    fn parameters(&self) -> Option<Value> {
        parameters::<SearchRequestBaseJson>()
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "tool_use::search", tool = self.name().to_string(), input = arg);

        let tool_use_result = async {
            let req: SearchRequestBaseJson = serde_json::from_str(arg)?;
            tracing::trace!("search tool use function call request: {req:?}");

            let vs = SearchEngine::new(
                self.rt.datafusion(),
                parse_explicit_primary_keys(Arc::clone(&self.rt.app)).await,
            );

            let mut search_request = SearchRequest::try_from(req)?;
            let allowed_tables = match (search_request.datasets, self.table_allowlist.as_ref()) {
                (tables, None) => tables,
                (Some(ds), Some(allowlist)) => Some(
                    ds.into_iter()
                        .filter(|d| allowlist.table_is_allowed(&TableReference::parse_str(d)))
                        .collect::<Vec<String>>(),
                ),
                (None, Some(allowlist)) => {
                    let tables = get_dataset_elements(Arc::clone(&self.rt), Some(allowlist))
                        .await
                        .into_iter()
                        .map(|d| d.table)
                        .collect::<Vec<String>>();
                    Some(tables)
                }
            };
            search_request.datasets = allowed_tables;
            let request_context = RequestContext::current(AsyncMarker::new().await);

            let (result, _) = vs
                .search_with_cache(
                    &search_request,
                    self.rt.datafusion().search_cache_provider(),
                    request_context,
                )
                .await
                .boxed()?;

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
            Ok(value) => {
                let captured_output_json = serde_json::to_string(&value).boxed()?;
                tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output_json);
                Ok(value)
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                Err(e)
            }
        }
    }
}
