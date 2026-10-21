/*
Copyright 2026 The Spice.ai OSS Authors

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

//! The Messages API's request and response shapes, typed for translation.
//!
//! A request is read leniently where reading cannot lose meaning and exactly where it can: a
//! content block or tool of a type not named here is kept as [`ContentBlockParam::Other`] or
//! [`ToolParam::Other`], and a field not named here, at the top level or nested in a block, a
//! source, or a configuration, is kept in that shape's `other`, so that a translation can refuse
//! it by name.

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A `POST /v1/messages` request.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct CreateMessageRequest {
    pub model: String,
    pub max_tokens: u32,
    pub messages: Vec<MessageParam>,
    pub system: Option<SystemPrompt>,
    pub stream: Option<bool>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub top_k: Option<u32>,
    pub stop_sequences: Option<Vec<String>>,
    pub metadata: Option<Metadata>,
    pub tools: Option<Vec<ToolParam>>,
    pub tool_choice: Option<ToolChoice>,
    pub thinking: Option<ThinkingConfig>,
    pub output_config: Option<OutputConfig>,
    /// The deprecated form of `output_config.format`.
    pub output_format: Option<OutputFormat>,
    /// Every other field, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

/// The `system` prompt: a string, or text blocks.
#[derive(Clone, Debug, PartialEq)]
pub enum SystemPrompt {
    Text(String),
    Blocks(Vec<ContentBlockParam>),
}

/// Reads a field that is either a string or an array of content blocks, keeping a malformed
/// block's own error: an untagged enum would report only that neither form matched.
fn text_or_blocks<'de, D: Deserializer<'de>>(
    deserializer: D,
    field: &str,
) -> Result<Result<String, Vec<ContentBlockParam>>, D::Error> {
    match Value::deserialize(deserializer)? {
        Value::String(text) => Ok(Ok(text)),
        Value::Array(blocks) => blocks
            .into_iter()
            .map(|block| serde_json::from_value(block).map_err(de::Error::custom))
            .collect::<Result<Vec<_>, _>>()
            .map(Err),
        other => Err(de::Error::custom(format!(
            "`{field}` must be a string or an array of content blocks, not {other}"
        ))),
    }
}

impl<'de> Deserialize<'de> for SystemPrompt {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match text_or_blocks(deserializer, "system")? {
            Ok(text) => SystemPrompt::Text(text),
            Err(blocks) => SystemPrompt::Blocks(blocks),
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    User,
    Assistant,
    /// A system message in the middle of the conversation.
    System,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct MessageParam {
    pub role: Role,
    pub content: MessageContent,
}

/// A message's content: a string, or content blocks.
#[derive(Clone, Debug, PartialEq)]
pub enum MessageContent {
    Text(String),
    Blocks(Vec<ContentBlockParam>),
}

impl<'de> Deserialize<'de> for MessageContent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match text_or_blocks(deserializer, "content")? {
            Ok(text) => MessageContent::Text(text),
            Err(blocks) => MessageContent::Blocks(blocks),
        })
    }
}

/// A content block of a request, by its `type`.
#[derive(Clone, Debug, PartialEq)]
pub enum ContentBlockParam {
    Text(TextBlockParam),
    Image(ImageBlockParam),
    Document(DocumentBlockParam),
    ToolUse(ToolUseBlockParam),
    ToolResult(ToolResultBlockParam),
    Thinking(ThinkingBlockParam),
    RedactedThinking(RedactedThinkingBlockParam),
    McpToolUse(McpToolUseBlockParam),
    McpToolResult(McpToolResultBlockParam),
    /// A block of a type not named above.
    Other {
        block_type: String,
        block: Value,
    },
}

impl ContentBlockParam {
    /// The block's `type`.
    #[must_use]
    pub fn block_type(&self) -> &str {
        match self {
            ContentBlockParam::Text(_) => "text",
            ContentBlockParam::Image(_) => "image",
            ContentBlockParam::Document(_) => "document",
            ContentBlockParam::ToolUse(_) => "tool_use",
            ContentBlockParam::ToolResult(_) => "tool_result",
            ContentBlockParam::Thinking(_) => "thinking",
            ContentBlockParam::RedactedThinking(_) => "redacted_thinking",
            ContentBlockParam::McpToolUse(_) => "mcp_tool_use",
            ContentBlockParam::McpToolResult(_) => "mcp_tool_result",
            ContentBlockParam::Other { block_type, .. } => block_type,
        }
    }
}

/// A content block of type `block_type`, typed.
fn typed<T: serde::de::DeserializeOwned, E: de::Error>(
    block_type: &str,
    block: Value,
) -> Result<T, E> {
    serde_json::from_value(block)
        .map_err(|e| E::custom(format!("invalid `{block_type}` content block: {e}")))
}

impl<'de> Deserialize<'de> for ContentBlockParam {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let block = Value::deserialize(deserializer)?;
        let Some(block_type) = block.get("type").and_then(Value::as_str) else {
            return Err(de::Error::custom("content block has no `type`"));
        };
        Ok(match block_type {
            "text" => ContentBlockParam::Text(typed("text", block)?),
            "image" => ContentBlockParam::Image(typed("image", block)?),
            "document" => ContentBlockParam::Document(typed("document", block)?),
            "tool_use" => ContentBlockParam::ToolUse(typed("tool_use", block)?),
            "tool_result" => ContentBlockParam::ToolResult(typed("tool_result", block)?),
            "thinking" => ContentBlockParam::Thinking(typed("thinking", block)?),
            "redacted_thinking" => {
                ContentBlockParam::RedactedThinking(typed("redacted_thinking", block)?)
            }
            "mcp_tool_use" => ContentBlockParam::McpToolUse(typed("mcp_tool_use", block)?),
            "mcp_tool_result" => ContentBlockParam::McpToolResult(typed("mcp_tool_result", block)?),
            other => ContentBlockParam::Other {
                block_type: other.to_string(),
                block,
            },
        })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct TextBlockParam {
    pub text: String,
    /// Where an earlier answer's text came from, in the documents that answer was given.
    pub citations: Option<Value>,
    /// Every other field, `type` and `cache_control` included, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ImageBlockParam {
    pub source: ImageSource,
    /// Every other field, `type` and `cache_control` included, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ImageSource {
    Base64 {
        media_type: String,
        data: String,
        #[serde(flatten)]
        other: Map<String, Value>,
    },
    Url {
        url: String,
        #[serde(flatten)]
        other: Map<String, Value>,
    },
    File {
        file_id: String,
        #[serde(flatten)]
        other: Map<String, Value>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct DocumentBlockParam {
    pub source: DocumentSource,
    pub title: Option<String>,
    pub context: Option<String>,
    pub citations: Option<CitationsConfig>,
    /// Every other field, `type` and `cache_control` included, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DocumentSource {
    /// A PDF.
    Base64 {
        media_type: String,
        data: String,
        #[serde(flatten)]
        other: Map<String, Value>,
    },
    /// Plain text.
    Text {
        media_type: Option<String>,
        data: String,
        #[serde(flatten)]
        other: Map<String, Value>,
    },
    /// Content blocks.
    Content {
        content: MessageContent,
        #[serde(flatten)]
        other: Map<String, Value>,
    },
    Url {
        url: String,
        #[serde(flatten)]
        other: Map<String, Value>,
    },
    File {
        file_id: String,
        #[serde(flatten)]
        other: Map<String, Value>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct CitationsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Every other field, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ToolUseBlockParam {
    pub id: String,
    pub name: String,
    pub input: Value,
    /// Every other field, `type` and `cache_control` included, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ToolResultBlockParam {
    pub tool_use_id: String,
    pub content: Option<ToolResultContent>,
    pub is_error: Option<bool>,
    /// Every other field, `type` and `cache_control` included, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

/// A tool result's content: a string, or content blocks.
#[derive(Clone, Debug, PartialEq)]
pub enum ToolResultContent {
    Text(String),
    Blocks(Vec<ContentBlockParam>),
}

impl<'de> Deserialize<'de> for ToolResultContent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match text_or_blocks(deserializer, "content")? {
            Ok(text) => ToolResultContent::Text(text),
            Err(blocks) => ToolResultContent::Blocks(blocks),
        })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ThinkingBlockParam {
    pub thinking: String,
    #[serde(default)]
    pub signature: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct RedactedThinkingBlockParam {
    pub data: String,
}

/// A call to a tool of an MCP server that the server side ran.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct McpToolUseBlockParam {
    pub id: String,
    pub name: String,
    pub server_name: String,
    pub input: Value,
}

/// The result of an [`McpToolUseBlockParam`].
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct McpToolResultBlockParam {
    pub tool_use_id: String,
    pub is_error: Option<bool>,
    pub content: Option<ToolResultContent>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Metadata {
    pub user_id: Option<String>,
    /// Every other field, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

/// A tool the model may call, by its `type`.
#[derive(Clone, Debug, PartialEq)]
pub enum ToolParam {
    /// A tool the client defines and runs: no `type`, or `custom`.
    Custom(CustomToolParam),
    /// A tool Anthropic defines, such as `web_search_20250305` or `bash_20250124`.
    Other { tool_type: String, tool: Value },
}

impl<'de> Deserialize<'de> for ToolParam {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let tool = Value::deserialize(deserializer)?;
        match tool.get("type").and_then(Value::as_str) {
            None | Some("custom") => serde_json::from_value(tool)
                .map(ToolParam::Custom)
                .map_err(|e| de::Error::custom(format!("invalid tool: {e}"))),
            Some(other) => Ok(ToolParam::Other {
                tool_type: other.to_string(),
                tool,
            }),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct CustomToolParam {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
    pub strict: Option<bool>,
    /// Every other field, `type` and `cache_control` included, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolChoice {
    Auto {
        disable_parallel_tool_use: Option<bool>,
        #[serde(flatten)]
        other: Map<String, Value>,
    },
    Any {
        disable_parallel_tool_use: Option<bool>,
        #[serde(flatten)]
        other: Map<String, Value>,
    },
    Tool {
        name: String,
        disable_parallel_tool_use: Option<bool>,
        #[serde(flatten)]
        other: Map<String, Value>,
    },
    None {
        #[serde(flatten)]
        other: Map<String, Value>,
    },
}

impl ToolChoice {
    /// Whether the model is held to one tool call per turn.
    #[must_use]
    pub fn disables_parallel_tool_use(&self) -> bool {
        match self {
            ToolChoice::Auto {
                disable_parallel_tool_use,
                ..
            }
            | ToolChoice::Any {
                disable_parallel_tool_use,
                ..
            }
            | ToolChoice::Tool {
                disable_parallel_tool_use,
                ..
            } => disable_parallel_tool_use.unwrap_or(false),
            ToolChoice::None { .. } => false,
        }
    }
}

/// The `thinking` configuration. Its `type` is kept as a string: `enabled`, `adaptive`,
/// `disabled`, and the types later models add.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ThinkingConfig {
    #[serde(rename = "type")]
    pub thinking_type: String,
    pub budget_tokens: Option<u32>,
    /// How thinking is shown: `summarized`, or `omitted`, which leaves the text empty.
    pub display: Option<String>,
    /// Every other field, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

impl ThinkingConfig {
    /// Whether the response carries thinking blocks with text in them.
    #[must_use]
    pub fn shows_thinking(&self) -> bool {
        matches!(self.thinking_type.as_str(), "enabled" | "adaptive")
            && self.display.as_deref() != Some("omitted")
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OutputConfig {
    pub effort: Option<String>,
    pub format: Option<OutputFormat>,
    /// Every other field, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct OutputFormat {
    #[serde(rename = "type")]
    pub format_type: String,
    pub schema: Option<Value>,
    /// Every other field, by name.
    #[serde(flatten)]
    pub other: Map<String, Value>,
}

/// A Message, the response of `POST /v1/messages`.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Message {
    pub id: String,
    #[serde(rename = "type")]
    pub object_type: &'static str,
    pub role: &'static str,
    pub model: String,
    pub content: Vec<ContentBlock>,
    pub stop_reason: Option<StopReason>,
    pub stop_sequence: Option<String>,
    pub usage: Usage,
}

impl Message {
    /// An assistant message with no content yet, as a stream's `message_start` carries it.
    #[must_use]
    pub fn started(id: String, model: String) -> Self {
        Self {
            id,
            object_type: "message",
            role: "assistant",
            model,
            content: Vec::new(),
            stop_reason: None,
            stop_sequence: None,
            usage: Usage::default(),
        }
    }
}

/// A content block of a response.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    Thinking {
        thinking: String,
        signature: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    MaxTokens,
    StopSequence,
    ToolUse,
    PauseTurn,
    Refusal,
}

#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq, Eq)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(body: &Value) -> CreateMessageRequest {
        serde_json::from_value(body.clone()).expect("a valid request")
    }

    #[test]
    fn blocks_and_tools_of_unnamed_types_are_kept_by_type() {
        let parsed = request(&json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}},
                {"type": "search_result", "source": "s", "title": "t", "content": []},
            ]}],
            "tools": [
                {"name": "get_weather", "input_schema": {"type": "object"}},
                {"type": "custom", "name": "lookup", "input_schema": {"type": "object"}, "strict": true},
                {"type": "web_search_20250305", "name": "web_search"},
            ],
            "context_management": {"edits": []},
        }));
        let MessageContent::Blocks(blocks) = &parsed.messages[0].content else {
            panic!("blocks expected");
        };
        assert_eq!(
            blocks
                .iter()
                .map(ContentBlockParam::block_type)
                .collect::<Vec<_>>(),
            ["text", "search_result"]
        );
        let tools = parsed.tools.expect("tools");
        assert_eq!(
            tools
                .iter()
                .map(|tool| match tool {
                    ToolParam::Custom(custom) => format!("custom:{}", custom.name),
                    ToolParam::Other { tool_type, .. } => format!("other:{tool_type}"),
                })
                .collect::<Vec<_>>(),
            [
                "custom:get_weather",
                "custom:lookup",
                "other:web_search_20250305"
            ]
        );
        assert_eq!(
            parsed.other.keys().collect::<Vec<_>>(),
            ["context_management"]
        );
    }

    #[test]
    fn a_malformed_named_block_names_its_type() {
        let error = serde_json::from_value::<CreateMessageRequest>(json!({
            "model": "m",
            "max_tokens": 10,
            "messages": [{"role": "user", "content": [{"type": "tool_result"}]}],
        }))
        .expect_err("a tool_result without tool_use_id");
        assert!(
            error
                .to_string()
                .contains("invalid `tool_result` content block: missing field `tool_use_id`"),
            "{error}"
        );
    }

    #[test]
    fn thinking_is_shown_only_when_on_and_not_omitted() {
        let config = |thinking: Value| -> ThinkingConfig {
            serde_json::from_value(thinking).expect("a thinking config")
        };
        assert_eq!(
            [
                config(json!({"type": "enabled", "budget_tokens": 2048})),
                config(json!({"type": "adaptive"})),
                config(json!({"type": "adaptive", "display": "summarized"})),
                config(json!({"type": "adaptive", "display": "omitted"})),
                config(json!({"type": "disabled"})),
            ]
            .map(|c| c.shows_thinking()),
            [true, true, true, false, false]
        );
    }

    #[test]
    fn a_message_serializes_as_the_messages_api_returns_it() {
        let mut message = Message::started("msg_1".to_string(), "sonnet".to_string());
        message.content = vec![
            ContentBlock::Text {
                text: "hi".to_string(),
            },
            ContentBlock::ToolUse {
                id: "toolu_1".to_string(),
                name: "get_weather".to_string(),
                input: json!({"city": "Paris"}),
            },
        ];
        message.stop_reason = Some(StopReason::ToolUse);
        message.usage = Usage {
            input_tokens: 10,
            output_tokens: 5,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: Some(3),
        };
        assert_eq!(
            serde_json::to_value(&message).expect("serializable"),
            json!({
                "id": "msg_1",
                "type": "message",
                "role": "assistant",
                "model": "sonnet",
                "content": [
                    {"type": "text", "text": "hi"},
                    {"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {"city": "Paris"}},
                ],
                "stop_reason": "tool_use",
                "stop_sequence": null,
                "usage": {"input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": 3},
            })
        );
    }
}
