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
use async_openai::{
    error::OpenAIError,
    types::chat::{
        ChatCompletionMessageToolCall, ChatCompletionMessageToolCalls,
        ChatCompletionRequestAssistantMessageArgs, ChatCompletionRequestMessage,
        ChatCompletionRequestToolMessageArgs, FunctionCall,
    },
};
use runtime_datafusion::allowlist::ResolvedTableAwareAllowlist;
use schemars::{JsonSchema, schema_for};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use crate::{Runtime, tools::catalog::SpiceToolCatalog};

use super::builtin::catalog::BuiltinToolCatalog;
use super::{Tooling, options::SpiceToolsOptions};
use tools::{SpiceModelTool, rename::with_name};

/// Creates the messages that would be sent and received if a language model were to request the `tool`
/// to be called (via an assistant message), with defined `arg`, and the response from running the
/// tool (via a tool message) also as a message.
///
/// Useful for constructing [`Vec<ChatCompletionRequestMessage>`], simulating a model already
/// having requested specific tools.
pub async fn create_tool_use_messages(
    tool: &dyn SpiceModelTool,
    id: &str,
    params: &impl serde::Serialize,
) -> Result<Vec<ChatCompletionRequestMessage>, OpenAIError> {
    let arg =
        serde_json::to_string(params).map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

    let resp = tool
        .call(arg.as_str())
        .await
        .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

    Ok(vec![
        ChatCompletionRequestAssistantMessageArgs::default()
            .tool_calls(vec![ChatCompletionMessageToolCalls::Function(
                ChatCompletionMessageToolCall {
                    id: id.to_string(),
                    function: FunctionCall {
                        name: tool.name().to_string(),
                        arguments: arg.clone(),
                    },
                },
            )])
            .build()?
            .into(),
        ChatCompletionRequestToolMessageArgs::default()
            .content(resp.to_string())
            .tool_call_id(id.to_string())
            .build()?
            .into(),
    ])
}

/// Construct a [`serde_json::Value`] from a [`JsonSchema`] type.
pub fn parameters<T: JsonSchema + Serialize>() -> Option<Value> {
    match serde_json::to_value(schema_for!(T)) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::error!("Unexpectedly cannot serialize schema: {e}",);
            None
        }
    }
}

/// Create a [`ResolvedTableAwareAllowlist`] from a list of dataset patterns.
///
/// Returns `None` if the list is empty.
pub fn create_table_allowlist(datasets: &[String]) -> Option<ResolvedTableAwareAllowlist> {
    if datasets.is_empty() {
        return None;
    }

    match ResolvedTableAwareAllowlist::with_defaults(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
        .with_table_patterns(datasets.to_vec())
    {
        Ok(allowlist) => Some(allowlist),
        Err(e) => {
            tracing::warn!("Failed to create table allowlist from model datasets: {e}");
            None
        }
    }
}

#[must_use]
pub async fn get_tools(rt: Arc<Runtime>, opts: &SpiceToolsOptions) -> Vec<Arc<dyn SpiceModelTool>> {
    get_tools_with_allowlist(rt, opts, None).await
}

#[must_use]
pub async fn get_tools_with_allowlist(
    rt: Arc<Runtime>,
    opts: &SpiceToolsOptions,
    table_allowlist: Option<ResolvedTableAwareAllowlist>,
) -> Vec<Arc<dyn SpiceModelTool>> {
    let all_tools = rt.tools.read().await;

    let mut tools = vec![];
    let mut missing_tools = vec![];

    for tt in opts.tools_by_name() {
        if let Some((catalog_name, catalog_tool)) = tt.split_once(':') {
            if let Some(Tooling::Catalog(catalog)) = all_tools.get(catalog_name) {
                let catalog = match (
                    catalog.as_any().downcast_ref::<BuiltinToolCatalog>(),
                    table_allowlist.clone(),
                ) {
                    (None, Some(_)) => {
                        tracing::info!(
                            "Table allowlist is only applicable to builtin catalog/tools. Allowlist will not be applied to '{catalog_name}'"
                        );
                        Arc::clone(catalog)
                    }
                    (Some(builtin_catalog), Some(allowlist)) => Arc::new(
                        builtin_catalog
                            .clone()
                            .with_table_allowlist(allowlist.clone()),
                    )
                        as Arc<dyn SpiceToolCatalog>,
                    _ => Arc::clone(catalog),
                };

                if let Some(t) = catalog.get(catalog_tool).await {
                    tools.push(with_name(
                        &t,
                        format!("{catalog_name}/{}", t.name()).as_str(),
                    ));
                } else {
                    tracing::warn!("Tool '{catalog_tool}' is not found in '{catalog_name}'.");
                    missing_tools.push(tt);
                }
            } else {
                missing_tools.push(tt);
            }
        } else if let Some(tool) = all_tools.get(tt) {
            if let Some(ref allowlist) = table_allowlist
                && BuiltinToolCatalog::is_builtin_tool(tt)
            {
                if let Ok(t) = BuiltinToolCatalog::new(Arc::clone(&rt))
                    .with_table_allowlist(allowlist.clone())
                    .construct_builtin(tt, None, None, &HashMap::new())
                {
                    tools.push(t);
                } else {
                    tracing::warn!("Failed to construct tool '{tt}' with table allowlist.");
                    missing_tools.push(tt);
                }
            } else {
                if table_allowlist.is_some() {
                    tracing::info!(
                        "Table allowlist is only applicable to builtin catalog/tools. Allowlist will not be applied to '{tt}'"
                    );
                }
                tools.extend(tool.tools().await);
            }
        } else {
            missing_tools.push(tt);
        }
    }

    if !missing_tools.is_empty() {
        let available_tools = all_tools
            .keys()
            .map(String::as_str)
            .collect::<Vec<&str>>()
            .join(", ");

        tracing::warn!(
            "The following tools were not found in the registry: {}.\nAvailable tools are: {available_tools}.\nFor details, visit https://spiceai.org/docs/features/large-language-models/tools",
            missing_tools.join(", ")
        );
    }

    tools
}
