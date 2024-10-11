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

use async_openai::{error::OpenAIError, types::{ChatCompletionMessageToolCallChunk, ChatCompletionStreamResponseDelta, ChatCompletionToolType, FunctionCallStream, Role}};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::{collections::HashMap, fmt, str::FromStr};

use super::types::{MessageRole, StopReason, Usage};

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum MessageCreateStreamResponse {
    #[serde(rename = "message_start")]
    MessageStart { message: MessageStartMessage },
    #[serde(rename = "content_block_start")]
    ContentBlockStart {
        index: u32,
        content_block: ContentBlock,
    },
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta { index: u32, delta: Delta },
    #[serde(rename = "content_block_stop")]
    ContentBlockStop { index: u32 },
    #[serde(rename = "message_delta")]
    MessageDelta { delta: MessageDelta, usage: Usage },
    #[serde(rename = "message_stop")]
    MessageStop,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MessageStartMessage {
    pub id: String,
    #[serde(rename = "type")]
    pub message_type: String,
    pub role: String,
    pub model: String,
    pub stop_sequence: Option<String>,
    pub usage: Usage,
    pub content: Vec<String>,
    pub stop_reason: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse(ContentBlockToolUse),
}
impl ContentBlock {
    pub fn into_completion(self) -> ChatCompletionStreamResponseDelta {
        match self {
            ContentBlock::Text { text } => ChatCompletionStreamResponseDelta{
                content: Some(text),
                function_call: None,
                tool_calls: None,
                refusal: None,
                role: None
            },
            ContentBlock::ToolUse(tool_use) => ChatCompletionStreamResponseDelta{
                content: None,
                function_call: None,
                tool_calls: Some(vec![
                    ChatCompletionMessageToolCallChunk{ index: 0, id: Some(tool_use.id), r#type: Some(ChatCompletionToolType::Function), function: Some(FunctionCallStream{name: Some(tool_use.name), arguments: None}) }
                ]),
                refusal: None,
                role: None
            }
        }
    }
}


#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ContentBlockToolUse {
    pub id: String,
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
enum Delta {
    #[serde(rename = "text_delta")]
    TextDelta { text: String },
    #[serde(rename = "input_json_delta")]
    InputJsonDelta { partial_json: String },
}
impl Delta {
    pub fn into_completion(self, role: MessageRole, tool_content: Option<(i32, ContentBlockToolUse)>) -> ChatCompletionStreamResponseDelta {
        match (self, tool_content) {
            (Delta::TextDelta { text }, _) => ChatCompletionStreamResponseDelta{
                content: Some(text),
                function_call: None,
                tool_calls: None,
                refusal: None,
                role: match role {
                    MessageRole::Assistant => Some(Role::Assistant),
                    MessageRole::User => Some(Role::User),
                }
            },
            (Delta::InputJsonDelta { partial_json }, Some((index, ContentBlockToolUse{id, name, input}))) => ChatCompletionStreamResponseDelta{
                content: None,
                function_call: None,
                tool_calls: Some(vec![
                    ChatCompletionMessageToolCallChunk{ index, id: Some(id), r#type: Some(ChatCompletionToolType::Function), function: Some(FunctionCallStream{name: Some(name), arguments: Some(partial_json)}) }
                ]),
                refusal: None,
                role: match role {
                    MessageRole::Assistant => Some(Role::Assistant),
                    MessageRole::User => Some(Role::User),
                }
            },

            // This should never happen, but we need to handle it as an 'empty' response.
            (Delta::InputJsonDelta{partial_json: _}, None) => ChatCompletionStreamResponseDelta{
                content: None,
                function_call: None,
                tool_calls: None,
                refusal: None,
                role: match role {
                    MessageRole::Assistant => Some(Role::Assistant),
                    MessageRole::User => Some(Role::User),
                }
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MessageDelta {
    pub stop_reason: Option<StopReason>,
    pub stop_sequence: Option<String>,
}