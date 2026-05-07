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
use snafu::Snafu;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use crate::{Runtime, tools::catalog::SpiceToolCatalog};

use super::builtin::catalog::BuiltinToolCatalog;
use super::factory::default_catalog_names;
use super::{Tooling, options::SpiceToolsOptions};
use tools::{SpiceModelTool, rename::with_name};

#[derive(Debug, Snafu)]
enum ToolUtilsError {
    #[snafu(display("Failed to create table allowlist from model datasets: {source}"))]
    CreateTableAllowlist { source: globset::Error },
}

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

    let resp = match tool.call(arg.as_str()).await {
        Ok(resp) => resp,
        Err(e) => {
            let tool_name = tool.name();
            let error = e.to_string();
            tracing::warn!("Tool '{tool_name}' failed while creating tool-use messages: {error}");
            tool_call_error_response(tool_name.as_ref(), error)
        }
    };

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

pub fn tool_call_error_response(tool_name: &str, error: impl std::fmt::Display) -> Value {
    Value::String(format!(
        "Failed to call the tool {tool_name}. An error occurred: {error}"
    ))
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
/// Returns `Ok(None)` if the list is empty.
pub fn create_table_allowlist(
    datasets: &[String],
) -> Result<Option<ResolvedTableAwareAllowlist>, Box<dyn std::error::Error + Send + Sync>> {
    if datasets.is_empty() {
        return Ok(None);
    }

    ResolvedTableAwareAllowlist::with_defaults(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
        .with_table_patterns(datasets.to_vec())
        .map(Some)
        .map_err(|source| Box::new(ToolUtilsError::CreateTableAllowlist { source }).into())
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
    let configured_tool_names = configured_tool_names(&rt).await;
    let all_tools = rt.tools.read().await;

    let mut tools = vec![];
    let mut missing_tools = vec![];
    let mut seen_tool_names = HashSet::new();

    if opts.includes_all_available_tools() {
        let builtin_tool_names = opts.tools_by_name();
        extend_unique_tools(
            &mut tools,
            &mut seen_tool_names,
            all_available_tools(
                Arc::clone(&rt),
                &all_tools,
                &configured_tool_names,
                &builtin_tool_names,
                table_allowlist,
            )
            .await,
        );
        return tools;
    }

    if let SpiceToolsOptions::Specific(requested_tools) = opts {
        for tt in requested_tools {
            match tt.parse::<SpiceToolsOptions>() {
                Ok(SpiceToolsOptions::Auto) => {
                    let builtin_tool_names = SpiceToolsOptions::Auto.tools_by_name();
                    extend_unique_tools(
                        &mut tools,
                        &mut seen_tool_names,
                        all_available_tools(
                            Arc::clone(&rt),
                            &all_tools,
                            &configured_tool_names,
                            &builtin_tool_names,
                            table_allowlist.clone(),
                        )
                        .await,
                    );
                }
                Ok(SpiceToolsOptions::All | SpiceToolsOptions::SearchRegistry) => {
                    let builtin_tool_names = SpiceToolsOptions::All.tools_by_name();
                    extend_unique_tools(
                        &mut tools,
                        &mut seen_tool_names,
                        all_available_tools(
                            Arc::clone(&rt),
                            &all_tools,
                            &configured_tool_names,
                            &builtin_tool_names,
                            table_allowlist.clone(),
                        )
                        .await,
                    );
                }
                Ok(SpiceToolsOptions::Nsql) => {
                    for tool_name in SpiceToolsOptions::Nsql.tools_by_name() {
                        match get_tool_by_name(
                            Arc::clone(&rt),
                            &all_tools,
                            tool_name,
                            table_allowlist.clone(),
                        )
                        .await
                        {
                            Some(resolved_tools) => extend_unique_tools(
                                &mut tools,
                                &mut seen_tool_names,
                                resolved_tools,
                            ),
                            None => missing_tools.push(tool_name.to_string()),
                        }
                    }
                }
                Ok(SpiceToolsOptions::Disabled) => {}
                Ok(SpiceToolsOptions::Specific(_)) | Err(_) => {
                    match get_tool_by_name(Arc::clone(&rt), &all_tools, tt, table_allowlist.clone())
                        .await
                    {
                        Some(resolved_tools) => {
                            extend_unique_tools(&mut tools, &mut seen_tool_names, resolved_tools);
                        }
                        None => missing_tools.push(tt.clone()),
                    }
                }
            }
        }

        warn_missing_tools(&all_tools, &missing_tools);
        return tools;
    }

    for tt in opts.tools_by_name() {
        match get_tool_by_name(Arc::clone(&rt), &all_tools, tt, table_allowlist.clone()).await {
            Some(resolved_tools) => {
                extend_unique_tools(&mut tools, &mut seen_tool_names, resolved_tools);
            }
            None => missing_tools.push(tt.to_string()),
        }
    }

    warn_missing_tools(&all_tools, &missing_tools);

    tools
}

async fn all_available_tools(
    rt: Arc<Runtime>,
    all_tools: &HashMap<String, Tooling>,
    configured_tool_names: &HashSet<String>,
    builtin_tool_names: &[&str],
    table_allowlist: Option<ResolvedTableAwareAllowlist>,
) -> Vec<Arc<dyn SpiceModelTool>> {
    let mut tools = vec![];
    let mut seen_tool_names = HashSet::new();

    for tool_name in builtin_tool_names {
        if let Some(resolved_tools) = get_tool_by_name(
            Arc::clone(&rt),
            all_tools,
            tool_name,
            table_allowlist.clone(),
        )
        .await
        {
            extend_unique_tools(&mut tools, &mut seen_tool_names, resolved_tools);
        }
    }

    let default_catalog_names = default_catalog_names();
    let mut tool_entries = all_tools.iter().collect::<Vec<_>>();
    tool_entries.sort_by(|(left_name, _), (right_name, _)| left_name.cmp(right_name));

    for (tool_name, tooling) in tool_entries {
        if BuiltinToolCatalog::is_builtin_tool(tool_name) {
            continue;
        }

        if !configured_tool_names.contains(tool_name) {
            continue;
        }

        if let Tooling::Catalog(catalog) = tooling
            && default_catalog_names.contains(&catalog.name())
        {
            continue;
        }

        extend_unique_tools(&mut tools, &mut seen_tool_names, tooling.tools().await);
    }

    tools
}

async fn configured_tool_names(rt: &Arc<Runtime>) -> HashSet<String> {
    rt.read_app()
        .await
        .map(|app| app.tools.iter().map(|tool| tool.name.clone()).collect())
        .unwrap_or_default()
}

async fn get_tool_by_name(
    rt: Arc<Runtime>,
    all_tools: &HashMap<String, Tooling>,
    tool_name: &str,
    table_allowlist: Option<ResolvedTableAwareAllowlist>,
) -> Option<Vec<Arc<dyn SpiceModelTool>>> {
    if let Some((catalog_name, catalog_tool)) = tool_name.split_once(':') {
        let Some(Tooling::Catalog(catalog)) = all_tools.get(catalog_name) else {
            return None;
        };

        let catalog = match (
            catalog.as_any().downcast_ref::<BuiltinToolCatalog>(),
            table_allowlist,
        ) {
            (None, Some(_)) => {
                tracing::info!(
                    "Table allowlist is only applicable to builtin catalog/tools. Allowlist will not be applied to '{catalog_name}'"
                );
                Arc::clone(catalog)
            }
            (Some(builtin_catalog), Some(allowlist)) => {
                Arc::new(builtin_catalog.clone().with_table_allowlist(allowlist))
                    as Arc<dyn SpiceToolCatalog>
            }
            _ => Arc::clone(catalog),
        };

        if let Some(t) = catalog.get(catalog_tool).await {
            return Some(vec![with_name(
                &t,
                format!("{catalog_name}/{}", t.name()).as_str(),
            )]);
        }

        tracing::warn!("Tool '{catalog_tool}' is not found in '{catalog_name}'.");
        return None;
    }

    let tool = all_tools.get(tool_name)?;

    if let Some(ref allowlist) = table_allowlist
        && BuiltinToolCatalog::is_builtin_tool(tool_name)
    {
        if let Ok(t) = BuiltinToolCatalog::new(Arc::clone(&rt))
            .with_table_allowlist(allowlist.clone())
            .construct_builtin(tool_name, None, None, &HashMap::new())
        {
            return Some(vec![t]);
        }

        tracing::warn!("Failed to construct tool '{tool_name}' with table allowlist.");
        return None;
    }

    if table_allowlist.is_some() {
        tracing::info!(
            "Table allowlist is only applicable to builtin catalog/tools. Allowlist will not be applied to '{tool_name}'"
        );
    }

    Some(tool.tools().await)
}

fn extend_unique_tools(
    tools: &mut Vec<Arc<dyn SpiceModelTool>>,
    seen_tool_names: &mut HashSet<String>,
    new_tools: Vec<Arc<dyn SpiceModelTool>>,
) {
    for tool in new_tools {
        if seen_tool_names.insert(tool.name().to_string()) {
            tools.push(tool);
        }
    }
}

fn warn_missing_tools(all_tools: &HashMap<String, Tooling>, missing_tools: &[String]) {
    if missing_tools.is_empty() {
        return;
    }

    let available_tools = all_tools
        .keys()
        .map(String::as_str)
        .collect::<Vec<&str>>()
        .join(", ");

    tracing::warn!(
        "The following tools were not found in the registry: {}. Available tools are: {available_tools}. For details, visit https://spiceai.org/docs/features/large-language-models/tools",
        missing_tools.join(", ")
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::chat::ChatCompletionRequestToolMessageContent;
    use async_trait::async_trait;
    use std::borrow::Cow;

    struct FailingTool;

    #[async_trait]
    impl SpiceModelTool for FailingTool {
        fn name(&self) -> Cow<'_, str> {
            Cow::Borrowed("failing_tool")
        }

        fn description(&self) -> Option<Cow<'_, str>> {
            None
        }

        fn parameters(&self) -> Option<Value> {
            None
        }

        async fn call(&self, _: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
            Err("boom".into())
        }
    }

    #[tokio::test]
    async fn create_tool_use_messages_returns_tool_error_message() {
        let messages = create_tool_use_messages(&FailingTool, "tool-call", &serde_json::json!({}))
            .await
            .expect("tool-use messages should be created even when the tool fails");

        assert_eq!(messages.len(), 2);
        let ChatCompletionRequestMessage::Tool(tool_message) = &messages[1] else {
            panic!("second message should be a tool response");
        };

        let ChatCompletionRequestToolMessageContent::Text(content) = &tool_message.content else {
            panic!("tool response should be text");
        };

        assert!(content.contains("Failed to call the tool failing_tool"));
        assert!(content.contains("boom"));
    }
}
