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

//! The events of a streamed answer.
//!
//! A stream is `message_start`; then, for each content block in order, `content_block_start`,
//! its deltas and `content_block_stop`; then `message_delta`, with the stop reason and the
//! usage of the whole message, and `message_stop`. `ping` may come anywhere.

use serde::Serialize;
use serde_json::Value;

use crate::types::{ContentBlock, Message, StopReason};

/// The `type` of an event, which is also the name it is sent under.
#[must_use]
pub fn event_type(event: &Value) -> Option<&str> {
    event.get("type").and_then(Value::as_str)
}

/// An event of a streamed answer.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEvent {
    MessageStart {
        message: Message,
    },
    ContentBlockStart {
        index: usize,
        content_block: ContentBlock,
    },
    ContentBlockDelta {
        index: usize,
        delta: ContentBlockDelta,
    },
    ContentBlockStop {
        index: usize,
    },
    MessageDelta {
        delta: MessageDelta,
        usage: DeltaUsage,
    },
    MessageStop,
    Ping,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlockDelta {
    TextDelta { text: String },
    InputJsonDelta { partial_json: String },
    ThinkingDelta { thinking: String },
    SignatureDelta { signature: String },
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct MessageDelta {
    pub stop_reason: Option<StopReason>,
    pub stop_sequence: Option<String>,
}

/// The usage of the whole message, as `message_delta` reports it.
#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq, Eq)]
pub struct DeltaUsage {
    pub output_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn events_serialize_with_their_type() {
        let events = [
            StreamEvent::ContentBlockStart {
                index: 1,
                content_block: ContentBlock::ToolUse {
                    id: "toolu_1".to_string(),
                    name: "get_weather".to_string(),
                    input: json!({}),
                },
            },
            StreamEvent::ContentBlockDelta {
                index: 1,
                delta: ContentBlockDelta::InputJsonDelta {
                    partial_json: "{\"city\"".to_string(),
                },
            },
            StreamEvent::MessageDelta {
                delta: MessageDelta {
                    stop_reason: Some(StopReason::ToolUse),
                    stop_sequence: None,
                },
                usage: DeltaUsage {
                    output_tokens: 7,
                    input_tokens: Some(20),
                    cache_creation_input_tokens: None,
                    cache_read_input_tokens: None,
                },
            },
            StreamEvent::MessageStop,
        ];
        let serialized: Vec<Value> = events
            .iter()
            .map(|event| serde_json::to_value(event).expect("serializable"))
            .collect();
        assert_eq!(
            serialized,
            [
                json!({"type": "content_block_start", "index": 1, "content_block": {"type": "tool_use", "id": "toolu_1", "name": "get_weather", "input": {}}}),
                json!({"type": "content_block_delta", "index": 1, "delta": {"type": "input_json_delta", "partial_json": "{\"city\""}}),
                json!({"type": "message_delta", "delta": {"stop_reason": "tool_use", "stop_sequence": null}, "usage": {"output_tokens": 7, "input_tokens": 20}}),
                json!({"type": "message_stop"}),
            ]
        );
        assert_eq!(
            serialized.iter().map(event_type).collect::<Vec<_>>(),
            [
                Some("content_block_start"),
                Some("content_block_delta"),
                Some("message_delta"),
                Some("message_stop")
            ]
        );
    }
}
