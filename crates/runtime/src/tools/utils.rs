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
    types::{
        ChatCompletionMessageToolCall, ChatCompletionRequestAssistantMessageArgs,
        ChatCompletionRequestMessage, ChatCompletionRequestToolMessageArgs, ChatCompletionToolType,
        FunctionCall,
    },
};
use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;

use crate::Runtime;

use super::{options::SpiceToolsOptions, SpiceModelTool};

/// Creates the messages that would be sent and received if a language model were to request the `tool`
/// to be called (via an assistant message), with defined `arg`, and the response from running the
/// tool (via a tool message) also as a message.
///
/// Useful for constructing [`Vec<ChatCompletionRequestMessage>`], simulating a model already
/// having requested specific tools.
pub async fn create_tool_use_messages(
    rt: Arc<Runtime>,
    tool: &dyn SpiceModelTool,
    id: &str,
    params: &impl serde::Serialize,
) -> Result<Vec<ChatCompletionRequestMessage>, OpenAIError> {
    let arg =
        serde_json::to_string(params).map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

    let resp = tool
        .call(arg.as_str(), rt)
        .await
        .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;

    Ok(vec![
        ChatCompletionRequestAssistantMessageArgs::default()
            .tool_calls(vec![ChatCompletionMessageToolCall {
                id: id.to_string(),
                r#type: ChatCompletionToolType::Function,
                function: FunctionCall {
                    name: tool.name().to_string(),
                    arguments: arg.to_string(),
                },
            }])
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

#[must_use]
pub async fn get_tools(rt: Arc<Runtime>, opts: &SpiceToolsOptions) -> Vec<Arc<dyn SpiceModelTool>> {
    let all_tools = rt.tools.read().await;

    let mut tools = vec![];

    let mut missing_tools = vec![];

    for tt in opts.tools_by_name() {
        if let Some(tool) = all_tools.get(tt) {
            tools.extend(tool.tools().await);
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

        tracing::warn!("The following tools were not found in the registry: {}.\nAvailable tools are: {available_tools}.\nFor details, visit https://spiceai.org/docs/features/large-language-models/tools", missing_tools.join(", "));
    }

    tools
}
