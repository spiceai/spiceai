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
#![allow(deprecated)] // `function_call` argument is deprecated but no builder pattern alternative is available.
#![allow(clippy::missing_errors_doc)]

use crate::chat::{Chat, nsql::SqlGeneration};
use crate::google::{openai_api_error, to_completion_usage};
use async_openai::error::OpenAIError;
use async_openai::types::{
    ChatChoiceStream, ChatCompletionMessageToolCall, ChatCompletionRequestAssistantMessageContent,
    ChatCompletionRequestAssistantMessageContentPart, ChatCompletionRequestMessage,
    ChatCompletionRequestSystemMessageContent, ChatCompletionRequestSystemMessageContentPart,
    ChatCompletionRequestToolMessage, ChatCompletionRequestToolMessageContent,
    ChatCompletionRequestToolMessageContentPart, ChatCompletionRequestUserMessageContent,
    ChatCompletionRequestUserMessageContentPart, ChatCompletionResponseMessage,
    ChatCompletionResponseStream, ChatCompletionStreamResponseDelta, ChatCompletionToolType,
    CompletionUsage, CreateChatCompletionRequest, CreateChatCompletionResponse,
    CreateChatCompletionStreamResponse, FinishReason, FunctionCall, FunctionCallStream, Role,
};
use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use google_genai::generate::{GenerateContentRequest, GenerateContentResponse};
use google_genai::types::{Content, FunctionDeclaration, FunctionResponse, Part};
use std::collections::HashMap;
use std::pin::Pin;
use std::time::SystemTime;

use super::Google;

#[async_trait]
impl Chat for Google {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let google_req = convert_to_google_request(req);

        let stream = self
            .client
            .stream_generate_content(&self.model, google_req)
            .await
            .map_err(|e| openai_api_error(e.to_string()))?;

        Ok(Box::pin(convert_google_stream_to_openai(stream)))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let google_req = convert_to_google_request(req);

        let response = self
            .client
            .generate_content(&self.model, google_req)
            .await
            .map_err(|e| openai_api_error(e.to_string()))?;

        convert_google_response_to_openai(response, &self.model)
    }
}

#[expect(clippy::too_many_lines)]
fn convert_to_google_request(req: CreateChatCompletionRequest) -> GenerateContentRequest {
    let mut contents = Vec::new();

    for message in req.messages {
        let content = match message {
            ChatCompletionRequestMessage::User(msg) => {
                let text = match msg.content {
                    ChatCompletionRequestUserMessageContent::Text(t) => t,
                    ChatCompletionRequestUserMessageContent::Array(parts) => parts
                        .into_iter()
                        .filter_map(|p| match p {
                            ChatCompletionRequestUserMessageContentPart::Text(t) => Some(t.text),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join("\n"),
                };
                Content::user(text)
            }
            ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
                content,
                tool_call_id,
            }) => {
                let response = match content {
                    ChatCompletionRequestToolMessageContent::Text(t) => t,
                    ChatCompletionRequestToolMessageContent::Array(parts) => parts
                        .into_iter()
                        .map(|p| match p {
                            ChatCompletionRequestToolMessageContentPart::Text(t) => t.text,
                        })
                        .collect::<String>(),
                };
                let response =
                    match serde_json::from_str::<serde_json::Value>(&response).map_err(|e| {
                        HashMap::from([(
                            "error".to_string(),
                            serde_json::Value::String(e.to_string()),
                        )])
                    }) {
                        Ok(map) => HashMap::from([("result".to_string(), map)]),
                        Err(err_map) => err_map,
                    };
                Content {
                    role: Some("user".to_string()),
                    parts: vec![Part::FunctionResponse {
                        function_response: FunctionResponse {
                            id: Some(tool_call_id.clone()),
                            name: tool_call_id, // Don't have access to name.
                            response,
                        },
                    }],
                }
            }
            ChatCompletionRequestMessage::Assistant(msg) => {
                // TODO: match tool call.
                let text = match msg.content {
                    Some(ChatCompletionRequestAssistantMessageContent::Text(t)) => t,
                    Some(ChatCompletionRequestAssistantMessageContent::Array(parts)) => parts
                        .into_iter()
                        .filter_map(|p| match p {
                            ChatCompletionRequestAssistantMessageContentPart::Text(t) => {
                                Some(t.text)
                            }
                            ChatCompletionRequestAssistantMessageContentPart::Refusal(_) => None,
                        })
                        .collect::<Vec<_>>()
                        .join("\n"),
                    None => String::new(),
                };
                if let Some(tools) = msg.tool_calls {
                    for ChatCompletionMessageToolCall {
                        id,
                        function: FunctionCall { name, arguments },
                        ..
                    } in tools
                    {
                        contents.push(Content {
                            role: Some("assistant".to_string()),
                            parts: vec![Part::FunctionCall {
                                function_call: google_genai::types::FunctionCall {
                                    id: Some(id),
                                    name,
                                    args:
                                        serde_json::from_str::<HashMap<String, serde_json::Value>>(
                                            &arguments,
                                        )
                                        .unwrap_or_default(),
                                },
                            }],
                        });
                    }
                }
                Content::model(text)
            }
            ChatCompletionRequestMessage::System(msg) => {
                let text = match msg.content {
                    ChatCompletionRequestSystemMessageContent::Text(t) => t,
                    ChatCompletionRequestSystemMessageContent::Array(parts) => parts
                        .into_iter()
                        .map(|p| match p {
                            ChatCompletionRequestSystemMessageContentPart::Text(t) => t.text,
                        })
                        .collect::<Vec<_>>()
                        .join("\n"),
                };
                Content::user(text)
            }

            _ => continue,
        };
        contents.push(content);
    }

    let mut google_req = GenerateContentRequest::new(contents);

    // Convert tools if present
    if let Some(openai_tools) = req.tools {
        let google_tools: Vec<google_genai::types::Tool> = openai_tools
            .into_iter()
            .map(|tool| {
                let func_decl = FunctionDeclaration {
                    name: tool.function.name,
                    description: tool.function.description.unwrap_or_default(),
                    parameters: tool.function.parameters.and_then(|params| {
                        serde_json::from_value::<google_genai::types::Schema>(params).ok()
                    }),
                };
                google_genai::types::Tool {
                    function_declarations: Some(vec![func_decl]),
                }
            })
            .collect();

        if !google_tools.is_empty() {
            google_req = google_req.with_tools(google_tools);
        }
    }

    google_req
}

#[expect(clippy::cast_possible_truncation)]
fn convert_google_response_to_openai(
    response: GenerateContentResponse,
    model: &str,
) -> Result<CreateChatCompletionResponse, OpenAIError> {
    use async_openai::types::ChatChoice;

    let choices = response
        .candidates
        .into_iter()
        .enumerate()
        .map(|(idx, candidate)| {
            let mut text_parts = Vec::new();
            let mut tool_calls = Vec::new();

            for part in candidate.content.parts {
                match part {
                    Part::Text { text } => {
                        text_parts.push(text);
                    }
                    Part::FunctionCall { function_call } => {
                        let args = serde_json::to_string(&function_call.args)
                            .unwrap_or_else(|_| "{}".to_string());

                        tool_calls.push(ChatCompletionMessageToolCall {
                            id: format!("call_{}", tool_calls.len()),
                            r#type: ChatCompletionToolType::Function,
                            function: FunctionCall {
                                name: function_call.name,
                                arguments: args,
                            },
                        });
                    }
                    _ => {}
                }
            }

            let content = if text_parts.is_empty() {
                None
            } else {
                Some(text_parts.join("\n"))
            };

            let tool_calls_opt = if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            };

            let finish_reason = candidate.finish_reason.map(|fr| match fr {
                google_genai::types::FinishReason::Stop => {
                    if tool_calls_opt.is_some() {
                        FinishReason::ToolCalls
                    } else {
                        FinishReason::Stop
                    }
                }
                google_genai::types::FinishReason::MaxTokens => FinishReason::Length,
                google_genai::types::FinishReason::Safety => FinishReason::ContentFilter,
                _ => FinishReason::Stop,
            });

            ChatChoice {
                index: idx as u32,
                message: ChatCompletionResponseMessage {
                    role: Role::Assistant,
                    content,
                    tool_calls: tool_calls_opt,
                    function_call: None,
                    refusal: None,
                    audio: None,
                },
                finish_reason,
                logprobs: None,
            }
        })
        .collect();

    Ok(CreateChatCompletionResponse {
        id: response
            .response_id
            .unwrap_or_else(|| "unknown".to_string()),
        model: model.to_string(),
        usage: response.usage_metadata.as_ref().map(to_completion_usage),
        created: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
            .as_secs() as u32,
        choices,
        system_fingerprint: None,
        object: "chat.completion".to_string(),
        service_tier: None,
    })
}

#[expect(clippy::cast_possible_truncation)]
fn convert_google_stream_response_to_openai(
    response: GenerateContentResponse,
) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
    let choices = response
        .candidates
        .into_iter()
        .enumerate()
        .map(|(idx, candidate)| {
            use async_openai::types::ChatCompletionMessageToolCallChunk;

            let mut text_parts = Vec::new();
            let mut tool_calls = Vec::new();

            for part in candidate.content.parts {
                match part {
                    Part::Text { text } => {
                        text_parts.push(text);
                    }
                    Part::FunctionCall { function_call } => {
                        let args = serde_json::to_string(&function_call.args)
                            .unwrap_or_else(|_| "{}".to_string());

                        tool_calls.push(ChatCompletionMessageToolCallChunk {
                            index: tool_calls.len() as u32,
                            id: Some(format!("call_{}", tool_calls.len())),
                            r#type: Some(ChatCompletionToolType::Function),
                            function: Some(FunctionCallStream {
                                name: Some(function_call.name),
                                arguments: Some(args),
                            }),
                        });
                    }
                    _ => {}
                }
            }

            let content = if text_parts.is_empty() {
                None
            } else {
                Some(text_parts.join("\n"))
            };

            let tool_calls_opt = if tool_calls.is_empty() {
                None
            } else {
                Some(tool_calls)
            };

            let finish_reason = candidate.finish_reason.map(|fr| match fr {
                google_genai::types::FinishReason::Stop => {
                    if tool_calls_opt.is_some() {
                        FinishReason::ToolCalls
                    } else {
                        FinishReason::Stop
                    }
                }
                google_genai::types::FinishReason::MaxTokens => FinishReason::Length,
                google_genai::types::FinishReason::Safety => FinishReason::ContentFilter,
                _ => FinishReason::Stop,
            });

            ChatChoiceStream {
                index: idx as u32,
                delta: ChatCompletionStreamResponseDelta {
                    role: Some(Role::Assistant),
                    content,
                    tool_calls: tool_calls_opt,
                    function_call: None,
                    refusal: None,
                },
                finish_reason,
                logprobs: None,
            }
        })
        .collect();

    let usage = response.usage_metadata.map(|usage| CompletionUsage {
        prompt_tokens: usage.prompt_token_count,
        completion_tokens: usage.candidates_token_count.unwrap_or(0),
        total_tokens: usage.total_token_count,
        prompt_tokens_details: None,
        completion_tokens_details: None,
    });

    Ok(CreateChatCompletionStreamResponse {
        id: response
            .response_id
            .unwrap_or_else(|| "unknown".to_string()),
        model: response.model_version.unwrap_or_default(),
        usage,
        created: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
            .as_secs() as u32,
        choices,
        system_fingerprint: None,
        object: "chat.completion.chunk".to_string(),
        service_tier: None,
    })
}

fn convert_google_stream_to_openai(
    stream: Pin<Box<dyn Stream<Item = google_genai::Result<GenerateContentResponse>> + Send>>,
) -> impl Stream<Item = Result<CreateChatCompletionStreamResponse, OpenAIError>> {
    stream.map(|pkt| {
        convert_google_stream_response_to_openai(pkt.map_err(|e| openai_api_error(e.to_string()))?)
            .map_err(|e| openai_api_error(e.to_string()))
    })
}
