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

use std::{collections::HashMap, time::SystemTime};

use async_openai::{
    error::{ApiError, OpenAIError},
    types::{
        Metadata,
        chat::{
            ChatChoice, ChatChoiceStream, ChatCompletionMessageCustomToolCall,
            ChatCompletionMessageToolCall, ChatCompletionMessageToolCallChunk,
            ChatCompletionMessageToolCalls, ChatCompletionRequestAssistantMessage,
            ChatCompletionRequestAssistantMessageContent,
            ChatCompletionRequestAssistantMessageContentPart,
            ChatCompletionRequestDeveloperMessageContent,
            ChatCompletionRequestDeveloperMessageContentPart, ChatCompletionRequestMessage,
            ChatCompletionRequestSystemMessageContent,
            ChatCompletionRequestSystemMessageContentPart, ChatCompletionRequestToolMessageContent,
            ChatCompletionRequestToolMessageContentPart, ChatCompletionRequestUserMessageContent,
            ChatCompletionRequestUserMessageContentPart, ChatCompletionResponseMessage,
            ChatCompletionResponseStream, ChatCompletionStreamResponseDelta,
            ChatCompletionToolChoiceOption, ChatCompletionTools, CompletionTokensDetails,
            CompletionUsage, CreateChatCompletionRequest, CreateChatCompletionResponse,
            CreateChatCompletionStreamResponse, CustomTool, FinishReason, FunctionCall,
            FunctionCallStream, FunctionType, PromptTokensDetails, ResponseFormat,
            Role as ChatRole, ToolChoiceAllowedMode as ChatToolChoiceAllowedMode,
            ToolChoiceOptions as ChatToolChoiceOptions,
        },
        responses::{
            CreateResponse, CustomToolCall, CustomToolParam, EasyInputContent, EasyInputMessage,
            FunctionCallOutput, FunctionCallOutputItemParam, FunctionTool, FunctionToolCall,
            InputContent, InputImageContent, InputItem, InputParam, InputTextContent, Item,
            MessageType, OutputItem, OutputMessage, OutputMessageContent, OutputStatus, Reasoning,
            Response, ResponseStream, ResponseStreamEvent, ResponseTextParam, ResponseUsage,
            Role as ResponsesRole, Status, TextResponseFormatConfiguration, Tool as ResponsesTool,
            ToolChoiceAllowed as ResponsesToolChoiceAllowed,
            ToolChoiceAllowedMode as ResponsesToolChoiceAllowedMode,
            ToolChoiceCustom as ResponsesToolChoiceCustom,
            ToolChoiceFunction as ResponsesToolChoiceFunction,
            ToolChoiceOptions as ResponsesToolChoiceOptions,
            ToolChoiceParam as ResponsesToolChoiceParam,
        },
    },
};
use futures::{StreamExt, future};
use serde::{Serialize, de::DeserializeOwned};

#[expect(deprecated)]
pub(super) fn responses_request_from_chat_completion_request(
    mut req: CreateChatCompletionRequest,
    backend_model: &str,
) -> Result<CreateResponse, OpenAIError> {
    let max_output_tokens = req.max_completion_tokens.take().or(req.max_tokens.take());

    Ok(CreateResponse {
        input: chat_messages_to_response_input(std::mem::take(&mut req.messages))?,
        model: Some(backend_model.to_string()),
        max_output_tokens,
        reasoning: req
            .reasoning_effort
            .map(|effort| convert_json(effort, "reasoning_effort"))
            .transpose()?
            .map(|effort| Reasoning {
                effort: Some(effort),
                summary: None,
            }),
        metadata: req
            .metadata
            .take()
            .as_ref()
            .map(metadata_to_hash_map)
            .transpose()?,
        parallel_tool_calls: req.parallel_tool_calls,
        prompt_cache_key: req.prompt_cache_key.take().or(req.user.take()),
        safety_identifier: req.safety_identifier.take(),
        service_tier: req
            .service_tier
            .map(|service_tier| convert_json(service_tier, "service_tier"))
            .transpose()?,
        store: req.store,
        stream: req.stream,
        stream_options: req.stream_options.map(|opts| {
            async_openai::types::responses::ResponseStreamOptions {
                include_obfuscation: opts.include_obfuscation,
            }
        }),
        temperature: req.temperature,
        text: response_text_param(req.response_format.take(), req.verbosity.take())?,
        tool_choice: req
            .tool_choice
            .map(response_tool_choice_from_chat_tool_choice)
            .transpose()?,
        tools: req.tools.map(response_tools_from_chat_tools).transpose()?,
        top_logprobs: req.top_logprobs,
        top_p: req.top_p,
        ..Default::default()
    })
}

#[expect(deprecated)]
pub(super) fn chat_completion_response_from_response(
    response: Response,
    model: String,
) -> Result<CreateChatCompletionResponse, OpenAIError> {
    if response.status == Status::Failed {
        return Err(response_failed_error(&response));
    }

    let created = created_at_to_u32(response.created_at)?;
    let has_tool_calls = response_has_tool_calls(&response);
    let message = chat_message_from_response_output(response.output);
    let finish_reason = if has_tool_calls {
        Some(FinishReason::ToolCalls)
    } else if response.status == Status::Incomplete {
        Some(FinishReason::Length)
    } else {
        Some(FinishReason::Stop)
    };

    Ok(CreateChatCompletionResponse {
        id: response.id,
        choices: vec![ChatChoice {
            index: 0,
            message,
            finish_reason,
            logprobs: None,
        }],
        created,
        model,
        service_tier: response
            .service_tier
            .map(|service_tier| convert_json(service_tier, "service_tier"))
            .transpose()?,
        system_fingerprint: None,
        object: "chat.completion".to_string(),
        usage: response
            .usage
            .as_ref()
            .map(completion_usage_from_response_usage),
    })
}

pub(super) fn chat_completion_stream_from_response_stream(
    stream: ResponseStream,
    model: String,
) -> ChatCompletionResponseStream {
    Box::pin(
        stream
            .scan(ChatCompletionStreamState::default(), move |state, event| {
                future::ready(Some(chat_completion_stream_event_from_response_event(
                    event, &model, state,
                )))
            })
            .filter_map(future::ready),
    )
}

fn chat_messages_to_response_input(
    messages: Vec<ChatCompletionRequestMessage>,
) -> Result<InputParam, OpenAIError> {
    let mut items = Vec::with_capacity(messages.len());
    for message in messages {
        items.extend(input_items_from_chat_message(message)?);
    }
    Ok(InputParam::Items(items))
}

fn input_items_from_chat_message(
    message: ChatCompletionRequestMessage,
) -> Result<Vec<InputItem>, OpenAIError> {
    match message {
        ChatCompletionRequestMessage::Developer(msg) => Ok(vec![easy_message(
            ResponsesRole::Developer,
            EasyInputContent::Text(developer_content_to_text(msg.content)),
        )]),
        ChatCompletionRequestMessage::System(msg) => Ok(vec![easy_message(
            ResponsesRole::System,
            EasyInputContent::Text(system_content_to_text(msg.content)),
        )]),
        ChatCompletionRequestMessage::User(msg) => Ok(vec![easy_message(
            ResponsesRole::User,
            user_content_to_easy_input_content(msg.content)?,
        )]),
        ChatCompletionRequestMessage::Assistant(msg) => input_items_from_assistant_message(msg),
        ChatCompletionRequestMessage::Tool(msg) => Ok(vec![InputItem::Item(
            Item::FunctionCallOutput(FunctionCallOutputItemParam {
                call_id: msg.tool_call_id,
                output: FunctionCallOutput::Text(tool_content_to_text(msg.content)),
                id: None,
                status: Some(OutputStatus::Completed),
            }),
        )]),
        ChatCompletionRequestMessage::Function(msg) => Ok(vec![InputItem::Item(
            Item::FunctionCallOutput(FunctionCallOutputItemParam {
                call_id: msg.name,
                output: FunctionCallOutput::Text(msg.content.unwrap_or_default()),
                id: None,
                status: Some(OutputStatus::Completed),
            }),
        )]),
    }
}

#[expect(deprecated)]
fn input_items_from_assistant_message(
    mut msg: ChatCompletionRequestAssistantMessage,
) -> Result<Vec<InputItem>, OpenAIError> {
    let mut items = Vec::new();

    if let Some(content) = msg.content.take() {
        items.push(easy_message(
            ResponsesRole::Assistant,
            EasyInputContent::Text(assistant_content_to_text(content)),
        ));
    } else if let Some(refusal) = msg.refusal.take() {
        items.push(easy_message(
            ResponsesRole::Assistant,
            EasyInputContent::Text(refusal),
        ));
    }

    if let Some(tool_calls) = msg.tool_calls.take() {
        for tool_call in tool_calls {
            items.push(input_item_from_chat_tool_call(tool_call)?);
        }
    }

    if let Some(function_call) = msg.function_call.take() {
        items.push(InputItem::Item(Item::FunctionCall(FunctionToolCall {
            call_id: function_call.name.clone(),
            name: function_call.name,
            arguments: function_call.arguments,
            id: None,
            status: Some(OutputStatus::Completed),
        })));
    }

    Ok(items)
}

fn input_item_from_chat_tool_call(
    tool_call: ChatCompletionMessageToolCalls,
) -> Result<InputItem, OpenAIError> {
    match tool_call {
        ChatCompletionMessageToolCalls::Function(function_call) => {
            Ok(InputItem::Item(Item::FunctionCall(FunctionToolCall {
                call_id: function_call.id,
                name: function_call.function.name,
                arguments: function_call.function.arguments,
                id: None,
                status: Some(OutputStatus::Completed),
            })))
        }
        ChatCompletionMessageToolCalls::Custom(custom_call) => {
            Ok(InputItem::Item(Item::CustomToolCall(custom_tool_call(
                &custom_call.id,
                &custom_call.custom_tool.input,
                &custom_call.custom_tool.name,
                &custom_call.id,
            )?)))
        }
    }
}

fn metadata_to_hash_map(metadata: &Metadata) -> Result<HashMap<String, String>, OpenAIError> {
    let Some(object) = metadata.as_value().as_object() else {
        return Err(OpenAIError::InvalidArgument(
            "Chat Completions metadata must be a JSON object when proxying to the Responses API"
                .to_string(),
        ));
    };

    Ok(object
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                value
                    .as_str()
                    .map_or_else(|| value.to_string(), ToString::to_string),
            )
        })
        .collect())
}

fn custom_tool_call(
    call_id: &str,
    input: &str,
    name: &str,
    id: &str,
) -> Result<CustomToolCall, OpenAIError> {
    serde_json::from_value(serde_json::json!({
        "type": "custom_tool_call",
        "call_id": call_id,
        "input": input,
        "name": name,
        "id": id,
    }))
    .map_err(|e| invalid_conversion("custom tool call", e))
}

fn easy_message(role: ResponsesRole, content: EasyInputContent) -> InputItem {
    InputItem::EasyMessage(EasyInputMessage {
        r#type: MessageType::Message,
        role,
        content,
    })
}

fn developer_content_to_text(content: ChatCompletionRequestDeveloperMessageContent) -> String {
    match content {
        ChatCompletionRequestDeveloperMessageContent::Text(text) => text,
        ChatCompletionRequestDeveloperMessageContent::Array(parts) => parts
            .into_iter()
            .map(|part| match part {
                ChatCompletionRequestDeveloperMessageContentPart::Text(text) => text.text,
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn system_content_to_text(content: ChatCompletionRequestSystemMessageContent) -> String {
    match content {
        ChatCompletionRequestSystemMessageContent::Text(text) => text,
        ChatCompletionRequestSystemMessageContent::Array(parts) => parts
            .into_iter()
            .map(|part| match part {
                ChatCompletionRequestSystemMessageContentPart::Text(text) => text.text,
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn user_content_to_easy_input_content(
    content: ChatCompletionRequestUserMessageContent,
) -> Result<EasyInputContent, OpenAIError> {
    match content {
        ChatCompletionRequestUserMessageContent::Text(text) => Ok(EasyInputContent::Text(text)),
        ChatCompletionRequestUserMessageContent::Array(parts) => parts
            .into_iter()
            .map(input_content_from_user_content_part)
            .collect::<Result<Vec<_>, _>>()
            .map(EasyInputContent::ContentList),
    }
}

fn input_content_from_user_content_part(
    part: ChatCompletionRequestUserMessageContentPart,
) -> Result<InputContent, OpenAIError> {
    match part {
        ChatCompletionRequestUserMessageContentPart::Text(text) => {
            Ok(InputContent::InputText(InputTextContent {
                text: text.text,
            }))
        }
        ChatCompletionRequestUserMessageContentPart::ImageUrl(image) => {
            Ok(InputContent::InputImage(InputImageContent {
                detail: image.image_url.detail.unwrap_or_default(),
                file_id: None,
                image_url: Some(image.image_url.url),
            }))
        }
        ChatCompletionRequestUserMessageContentPart::File(file) => {
            Ok(InputContent::InputFile(convert_json(file.file, "file")?))
        }
        ChatCompletionRequestUserMessageContentPart::InputAudio(_) => {
            Err(OpenAIError::InvalidArgument(
                "Audio chat input cannot be proxied to the Responses API".to_string(),
            ))
        }
    }
}

fn assistant_content_to_text(content: ChatCompletionRequestAssistantMessageContent) -> String {
    match content {
        ChatCompletionRequestAssistantMessageContent::Text(text) => text,
        ChatCompletionRequestAssistantMessageContent::Array(parts) => parts
            .into_iter()
            .map(|part| match part {
                ChatCompletionRequestAssistantMessageContentPart::Text(text) => text.text,
                ChatCompletionRequestAssistantMessageContentPart::Refusal(refusal) => {
                    refusal.refusal
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn tool_content_to_text(content: ChatCompletionRequestToolMessageContent) -> String {
    match content {
        ChatCompletionRequestToolMessageContent::Text(text) => text,
        ChatCompletionRequestToolMessageContent::Array(parts) => parts
            .into_iter()
            .map(|part| match part {
                ChatCompletionRequestToolMessageContentPart::Text(text) => text.text,
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn response_text_param(
    format: Option<ResponseFormat>,
    verbosity: Option<async_openai::types::chat::Verbosity>,
) -> Result<Option<ResponseTextParam>, OpenAIError> {
    match (format, verbosity) {
        (None, None) => Ok(None),
        (format, verbosity) => Ok(Some(ResponseTextParam {
            format: match format.unwrap_or(ResponseFormat::Text) {
                ResponseFormat::Text => TextResponseFormatConfiguration::Text,
                ResponseFormat::JsonObject => TextResponseFormatConfiguration::JsonObject,
                ResponseFormat::JsonSchema { json_schema } => {
                    TextResponseFormatConfiguration::JsonSchema(json_schema)
                }
            },
            verbosity: verbosity
                .map(|value| convert_json(value, "verbosity"))
                .transpose()?,
        })),
    }
}

fn response_tools_from_chat_tools(
    tools: Vec<ChatCompletionTools>,
) -> Result<Vec<ResponsesTool>, OpenAIError> {
    tools
        .into_iter()
        .map(response_tool_from_chat_tool)
        .collect()
}

fn response_tool_from_chat_tool(tool: ChatCompletionTools) -> Result<ResponsesTool, OpenAIError> {
    match tool {
        ChatCompletionTools::Function(function_tool) => Ok(ResponsesTool::Function(FunctionTool {
            name: function_tool.function.name,
            parameters: function_tool.function.parameters,
            strict: function_tool.function.strict,
            description: function_tool.function.description,
        })),
        ChatCompletionTools::Custom(custom_tool) => Ok(ResponsesTool::Custom(CustomToolParam {
            name: custom_tool.custom.name,
            description: custom_tool.custom.description,
            format: convert_json(custom_tool.custom.format, "custom tool format")?,
        })),
    }
}

fn response_tool_choice_from_chat_tool_choice(
    tool_choice: ChatCompletionToolChoiceOption,
) -> Result<ResponsesToolChoiceParam, OpenAIError> {
    match tool_choice {
        ChatCompletionToolChoiceOption::Mode(mode) => {
            Ok(ResponsesToolChoiceParam::Mode(match mode {
                ChatToolChoiceOptions::None => ResponsesToolChoiceOptions::None,
                ChatToolChoiceOptions::Auto => ResponsesToolChoiceOptions::Auto,
                ChatToolChoiceOptions::Required => ResponsesToolChoiceOptions::Required,
            }))
        }
        ChatCompletionToolChoiceOption::Function(function) => Ok(
            ResponsesToolChoiceParam::Function(ResponsesToolChoiceFunction {
                name: function.function.name,
            }),
        ),
        ChatCompletionToolChoiceOption::Custom(custom) => Ok(ResponsesToolChoiceParam::Custom(
            ResponsesToolChoiceCustom {
                name: custom.custom.name,
            },
        )),
        ChatCompletionToolChoiceOption::AllowedTools(allowed_tools) => {
            let tools = allowed_tools
                .allowed_tools
                .into_iter()
                .flat_map(|allowed| {
                    allowed
                        .tools
                        .into_iter()
                        .map(move |tool| (allowed.mode.clone(), tool))
                })
                .map(|(mode, tool)| {
                    let chat_tool = serde_json::from_value::<ChatCompletionTools>(tool)
                        .map_err(|e| invalid_conversion("allowed tool", e))?;
                    let response_tool = response_tool_from_chat_tool(chat_tool)?;
                    let tool_value = serde_json::to_value(response_tool)
                        .map_err(|e| invalid_conversion("allowed tool", e))?;
                    Ok((mode, tool_value))
                })
                .collect::<Result<Vec<_>, OpenAIError>>()?;

            let mode = tools
                .first()
                .map_or(ChatToolChoiceAllowedMode::Auto, |(mode, _)| mode.clone());

            Ok(ResponsesToolChoiceParam::AllowedTools(
                ResponsesToolChoiceAllowed {
                    mode: match mode {
                        ChatToolChoiceAllowedMode::Auto => ResponsesToolChoiceAllowedMode::Auto,
                        ChatToolChoiceAllowedMode::Required => {
                            ResponsesToolChoiceAllowedMode::Required
                        }
                    },
                    tools: tools.into_iter().map(|(_, tool)| tool).collect(),
                },
            ))
        }
    }
}

#[expect(deprecated)]
fn chat_message_from_response_output(output: Vec<OutputItem>) -> ChatCompletionResponseMessage {
    let mut text_parts = Vec::new();
    let mut refusal_parts = Vec::new();
    let mut tool_calls = Vec::new();

    for item in output {
        match item {
            OutputItem::Message(message) => {
                collect_output_message_content(message, &mut text_parts, &mut refusal_parts);
            }
            OutputItem::FunctionCall(function_call) => {
                tool_calls.push(ChatCompletionMessageToolCalls::Function(
                    chat_tool_call_from_response_function_call(function_call),
                ));
            }
            OutputItem::CustomToolCall(custom_call) => {
                tool_calls.push(ChatCompletionMessageToolCalls::Custom(
                    chat_custom_tool_call_from_response_custom_call(custom_call),
                ));
            }
            _ => {}
        }
    }

    ChatCompletionResponseMessage {
        content: (!text_parts.is_empty()).then(|| text_parts.join("\n")),
        refusal: (!refusal_parts.is_empty()).then(|| refusal_parts.join("\n")),
        tool_calls: (!tool_calls.is_empty()).then_some(tool_calls),
        annotations: None,
        role: ChatRole::Assistant,
        function_call: None,
        audio: None,
    }
}

fn collect_output_message_content(
    message: OutputMessage,
    text_parts: &mut Vec<String>,
    refusal_parts: &mut Vec<String>,
) {
    for content in message.content {
        match content {
            OutputMessageContent::OutputText(text) => text_parts.push(text.text),
            OutputMessageContent::Refusal(refusal) => refusal_parts.push(refusal.refusal),
        }
    }
}

fn response_has_tool_calls(response: &Response) -> bool {
    response.output.iter().any(|item| {
        matches!(
            item,
            OutputItem::FunctionCall(_) | OutputItem::CustomToolCall(_)
        )
    })
}

fn chat_tool_call_from_response_function_call(
    function_call: FunctionToolCall,
) -> ChatCompletionMessageToolCall {
    ChatCompletionMessageToolCall {
        id: function_call.id.unwrap_or(function_call.call_id),
        function: FunctionCall {
            name: function_call.name,
            arguments: function_call.arguments,
        },
    }
}

fn chat_custom_tool_call_from_response_custom_call(
    custom_call: CustomToolCall,
) -> ChatCompletionMessageCustomToolCall {
    ChatCompletionMessageCustomToolCall {
        id: custom_call.id,
        custom_tool: CustomTool {
            name: custom_call.name,
            input: custom_call.input,
        },
    }
}

#[derive(Default)]
struct ChatCompletionStreamState {
    response_id: Option<String>,
    created: Option<u32>,
}

impl ChatCompletionStreamState {
    fn update_response(&mut self, response: &Response) -> Result<(), OpenAIError> {
        self.response_id = Some(response.id.clone());
        self.created = Some(created_at_to_u32(response.created_at)?);
        Ok(())
    }

    fn chunk_id(&self, fallback: String) -> String {
        self.response_id.clone().unwrap_or(fallback)
    }

    fn created(&self) -> Result<u32, OpenAIError> {
        self.created.map_or_else(current_unix_timestamp, Ok)
    }
}

fn chat_completion_stream_event_from_response_event(
    event: Result<ResponseStreamEvent, OpenAIError>,
    model: &str,
    state: &mut ChatCompletionStreamState,
) -> Option<Result<CreateChatCompletionStreamResponse, OpenAIError>> {
    let event = match event {
        Ok(event) => event,
        Err(e) => return Some(Err(e)),
    };

    match event {
        ResponseStreamEvent::ResponseCreated(created) => {
            state.update_response(&created.response).err().map(Err)
        }
        ResponseStreamEvent::ResponseInProgress(in_progress) => {
            state.update_response(&in_progress.response).err().map(Err)
        }
        ResponseStreamEvent::ResponseOutputTextDelta(delta) => Some(chat_stream_chunk(
            state.chunk_id(delta.item_id),
            model,
            vec![chat_stream_choice(
                delta.output_index,
                Some(delta.delta),
                None,
                None,
                Some(ChatRole::Assistant),
                None,
            )],
            None,
            state,
        )),
        ResponseStreamEvent::ResponseRefusalDelta(delta) => Some(chat_stream_chunk(
            state.chunk_id(delta.item_id),
            model,
            vec![chat_stream_choice(
                delta.output_index,
                None,
                Some(delta.delta),
                None,
                Some(ChatRole::Assistant),
                None,
            )],
            None,
            state,
        )),
        ResponseStreamEvent::ResponseOutputItemDone(done) => match done.item {
            OutputItem::FunctionCall(function_call) => Some(chat_stream_chunk(
                state.chunk_id(
                    function_call
                        .id
                        .clone()
                        .unwrap_or_else(|| function_call.call_id.clone()),
                ),
                model,
                vec![chat_stream_choice(
                    done.output_index,
                    None,
                    None,
                    Some(vec![chat_tool_call_chunk_from_response_function_call(
                        function_call,
                        done.output_index,
                    )]),
                    Some(ChatRole::Assistant),
                    Some(FinishReason::ToolCalls),
                )],
                None,
                state,
            )),
            OutputItem::CustomToolCall(custom_call) => Some(chat_stream_chunk(
                state.chunk_id(custom_call.id.clone()),
                model,
                vec![chat_stream_choice(
                    done.output_index,
                    None,
                    None,
                    Some(vec![chat_custom_tool_call_chunk_from_response_custom_call(
                        custom_call,
                        done.output_index,
                    )]),
                    Some(ChatRole::Assistant),
                    Some(FinishReason::ToolCalls),
                )],
                None,
                state,
            )),
            _ => None,
        },
        ResponseStreamEvent::ResponseCompleted(completed) => {
            let response = completed.response;
            if let Err(e) = state.update_response(&response) {
                return Some(Err(e));
            }

            if response_has_tool_calls(&response) {
                Some(chat_stream_chunk(
                    response.id,
                    model,
                    vec![],
                    response
                        .usage
                        .as_ref()
                        .map(completion_usage_from_response_usage),
                    state,
                ))
            } else {
                Some(chat_stream_chunk(
                    response.id,
                    model,
                    vec![chat_stream_choice(
                        0,
                        None,
                        None,
                        None,
                        None,
                        Some(FinishReason::Stop),
                    )],
                    response
                        .usage
                        .as_ref()
                        .map(completion_usage_from_response_usage),
                    state,
                ))
            }
        }
        ResponseStreamEvent::ResponseIncomplete(incomplete) => {
            let response = incomplete.response;
            if let Err(e) = state.update_response(&response) {
                return Some(Err(e));
            }

            Some(chat_stream_chunk(
                response.id,
                model,
                vec![chat_stream_choice(
                    0,
                    None,
                    None,
                    None,
                    None,
                    Some(FinishReason::Length),
                )],
                response
                    .usage
                    .as_ref()
                    .map(completion_usage_from_response_usage),
                state,
            ))
        }
        ResponseStreamEvent::ResponseFailed(failed) => {
            Some(Err(response_failed_error(&failed.response)))
        }
        ResponseStreamEvent::ResponseError(error) => Some(Err(OpenAIError::ApiError(ApiError {
            message: error.message,
            r#type: Some("responses_api_error".to_string()),
            param: error.param,
            code: error.code,
        }))),
        _ => None,
    }
}

#[expect(deprecated)]
fn chat_stream_chunk(
    id: String,
    model: &str,
    choices: Vec<ChatChoiceStream>,
    usage: Option<CompletionUsage>,
    state: &ChatCompletionStreamState,
) -> Result<CreateChatCompletionStreamResponse, OpenAIError> {
    Ok(CreateChatCompletionStreamResponse {
        id,
        choices,
        created: state.created()?,
        model: model.to_string(),
        service_tier: None,
        system_fingerprint: None,
        object: "chat.completion.chunk".to_string(),
        usage,
    })
}

#[expect(deprecated)]
fn chat_stream_choice(
    index: u32,
    content: Option<String>,
    refusal: Option<String>,
    tool_calls: Option<Vec<ChatCompletionMessageToolCallChunk>>,
    role: Option<ChatRole>,
    finish_reason: Option<FinishReason>,
) -> ChatChoiceStream {
    ChatChoiceStream {
        index,
        delta: ChatCompletionStreamResponseDelta {
            content,
            function_call: None,
            tool_calls,
            role,
            refusal,
        },
        finish_reason,
        logprobs: None,
    }
}

fn chat_tool_call_chunk_from_response_function_call(
    function_call: FunctionToolCall,
    index: u32,
) -> ChatCompletionMessageToolCallChunk {
    ChatCompletionMessageToolCallChunk {
        index,
        id: Some(function_call.id.unwrap_or(function_call.call_id)),
        r#type: Some(FunctionType::Function),
        function: Some(FunctionCallStream {
            name: Some(function_call.name),
            arguments: Some(function_call.arguments),
        }),
    }
}

fn chat_custom_tool_call_chunk_from_response_custom_call(
    custom_call: CustomToolCall,
    index: u32,
) -> ChatCompletionMessageToolCallChunk {
    ChatCompletionMessageToolCallChunk {
        index,
        id: Some(custom_call.id),
        r#type: None,
        function: Some(FunctionCallStream {
            name: Some(custom_call.name),
            arguments: Some(custom_call.input),
        }),
    }
}

fn completion_usage_from_response_usage(usage: &ResponseUsage) -> CompletionUsage {
    CompletionUsage {
        prompt_tokens: usage.input_tokens,
        completion_tokens: usage.output_tokens,
        total_tokens: usage.total_tokens,
        prompt_tokens_details: Some(PromptTokensDetails {
            audio_tokens: None,
            cached_tokens: Some(usage.input_tokens_details.cached_tokens),
        }),
        completion_tokens_details: Some(CompletionTokensDetails {
            accepted_prediction_tokens: None,
            audio_tokens: None,
            reasoning_tokens: Some(usage.output_tokens_details.reasoning_tokens),
            rejected_prediction_tokens: None,
        }),
    }
}

fn created_at_to_u32(created_at: u64) -> Result<u32, OpenAIError> {
    u32::try_from(created_at).map_err(|e| OpenAIError::InvalidArgument(e.to_string()))
}

fn current_unix_timestamp() -> Result<u32, OpenAIError> {
    let seconds = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?
        .as_secs();
    created_at_to_u32(seconds)
}

fn response_failed_error(response: &Response) -> OpenAIError {
    let (message, code) = response.error.as_ref().map_or_else(
        || ("Responses API request failed".to_string(), None),
        |error| (error.message.clone(), Some(error.code.clone())),
    );

    OpenAIError::ApiError(ApiError {
        message,
        r#type: Some("responses_api_error".to_string()),
        param: None,
        code,
    })
}

fn convert_json<T, U>(value: T, context: &str) -> Result<U, OpenAIError>
where
    T: Serialize,
    U: DeserializeOwned,
{
    let value = serde_json::to_value(value).map_err(|e| invalid_conversion(context, e))?;
    serde_json::from_value(value).map_err(|e| invalid_conversion(context, e))
}

fn invalid_conversion(context: &str, err: impl std::fmt::Display) -> OpenAIError {
    OpenAIError::InvalidArgument(format!(
        "Failed to convert Chat Completions {context} to Responses API format: {err}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::types::chat::{
        ChatCompletionRequestMessage, ChatCompletionRequestUserMessage,
        ChatCompletionRequestUserMessageContent,
    };
    use futures::stream;
    use serde_json::json;

    #[test]
    fn maps_chat_request_to_responses_request() {
        let req: CreateChatCompletionRequest = serde_json::from_value(json!({
            "model": "public-model",
            "messages": [
                {"role": "system", "content": "Be terse."},
                {"role": "user", "content": "Hello"}
            ],
            "max_completion_tokens": 42,
            "temperature": 0.2,
            "tool_choice": "auto",
            "tools": [{
                "type": "function",
                "function": {
                    "name": "lookup",
                    "description": "Look something up",
                    "parameters": {"type": "object"},
                    "strict": true
                }
            }]
        }))
        .expect("chat request should deserialize");

        let response_req = responses_request_from_chat_completion_request(req, "backend-model")
            .expect("request should map to Responses API");

        assert_eq!(response_req.model.as_deref(), Some("backend-model"));
        assert_eq!(response_req.max_output_tokens, Some(42));
        assert_eq!(response_req.temperature, Some(0.2));
        assert_eq!(response_req.tools.as_ref().map(Vec::len), Some(1));
        assert!(matches!(
            response_req.tool_choice,
            Some(ResponsesToolChoiceParam::Mode(
                ResponsesToolChoiceOptions::Auto
            ))
        ));
    }

    #[test]
    fn maps_response_to_chat_completion_response() {
        let response: Response = serde_json::from_value(json!({
            "created_at": 1_755_639_134,
            "id": "resp_123",
            "model": "backend-model",
            "object": "response",
            "output": [{
                "type": "message",
                "id": "msg_123",
                "role": "assistant",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "annotations": [],
                    "logprobs": null,
                    "text": "hello"
                }]
            }],
            "status": "completed",
            "usage": {
                "input_tokens": 3,
                "input_tokens_details": {"cached_tokens": 1},
                "output_tokens": 2,
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": 5
            }
        }))
        .expect("response should deserialize");

        let chat_response =
            chat_completion_response_from_response(response, "public-model".to_string())
                .expect("response should map to chat completion");

        assert_eq!(chat_response.id, "resp_123");
        assert_eq!(chat_response.model, "public-model");
        assert_eq!(
            chat_response.choices[0].message.content.as_deref(),
            Some("hello")
        );
        assert_eq!(
            chat_response.usage.as_ref().map(|usage| usage.total_tokens),
            Some(5)
        );
    }

    #[test]
    #[expect(deprecated)]
    fn maps_chat_tool_messages_to_responses_function_items() {
        let req = CreateChatCompletionRequest {
            model: "public-model".to_string(),
            messages: vec![
                ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
                    content: None,
                    refusal: None,
                    name: None,
                    audio: None,
                    tool_calls: Some(vec![ChatCompletionMessageToolCalls::Function(
                        ChatCompletionMessageToolCall {
                            id: "call_123".to_string(),
                            function: FunctionCall {
                                name: "lookup".to_string(),
                                arguments: "{\"q\":\"spice\"}".to_string(),
                            },
                        },
                    )]),
                    function_call: None,
                }),
                ChatCompletionRequestMessage::Tool(
                    async_openai::types::chat::ChatCompletionRequestToolMessage {
                        content: ChatCompletionRequestToolMessageContent::Text(
                            "result".to_string(),
                        ),
                        tool_call_id: "call_123".to_string(),
                    },
                ),
                ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
                    content: ChatCompletionRequestUserMessageContent::Text("continue".to_string()),
                    name: None,
                }),
            ],
            ..Default::default()
        };

        let response_req = responses_request_from_chat_completion_request(req, "backend-model")
            .expect("request should map to Responses API");
        let InputParam::Items(items) = response_req.input else {
            panic!("chat messages should map to response input items");
        };

        let InputItem::Item(Item::FunctionCall(ref fc)) = items[0] else {
            panic!("first item should be a FunctionCall");
        };
        // id must be None: the Chat Completions tool call ID (e.g. 53-char fc_-prefixed string)
        // exceeds OpenAI's item id length constraint and is rejected if forwarded as the item id.
        // call_id is sufficient to link the call to its output.
        assert_eq!(
            fc.id, None,
            "FunctionToolCall item id must be None when converting from Chat Completions history"
        );
        assert_eq!(fc.call_id, "call_123");

        let InputItem::Item(Item::FunctionCallOutput(ref fco)) = items[1] else {
            panic!("second item should be a FunctionCallOutput");
        };
        assert_eq!(fco.id, None);
        assert_eq!(fco.call_id, "call_123");

        assert!(matches!(items[2], InputItem::EasyMessage(_)));
    }

    #[test]
    #[expect(deprecated)]
    fn multiple_tool_calls_in_one_turn_all_get_id_none() {
        let req = CreateChatCompletionRequest {
            model: "public-model".to_string(),
            messages: vec![ChatCompletionRequestMessage::Assistant(
                ChatCompletionRequestAssistantMessage {
                    content: None,
                    refusal: None,
                    name: None,
                    audio: None,
                    tool_calls: Some(vec![
                        ChatCompletionMessageToolCalls::Function(ChatCompletionMessageToolCall {
                            id: "fc_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                                .to_string(),
                            function: FunctionCall {
                                name: "tool_a".to_string(),
                                arguments: "{}".to_string(),
                            },
                        }),
                        ChatCompletionMessageToolCalls::Function(ChatCompletionMessageToolCall {
                            id: "fc_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                                .to_string(),
                            function: FunctionCall {
                                name: "tool_b".to_string(),
                                arguments: "{}".to_string(),
                            },
                        }),
                    ]),
                    function_call: None,
                },
            )],
            ..Default::default()
        };

        let response_req = responses_request_from_chat_completion_request(req, "backend-model")
            .expect("request should map to Responses API");
        let InputParam::Items(items) = response_req.input else {
            panic!("chat messages should map to response input items");
        };

        assert_eq!(items.len(), 2);
        for item in &items {
            let InputItem::Item(Item::FunctionCall(fc)) = item else {
                panic!("expected FunctionCall item");
            };
            assert_eq!(fc.id, None, "every tool call item must have id: None");
        }
        let InputItem::Item(Item::FunctionCall(ref fc_a)) = items[0] else {
            unreachable!()
        };
        let InputItem::Item(Item::FunctionCall(ref fc_b)) = items[1] else {
            unreachable!()
        };
        assert_eq!(
            fc_a.call_id,
            "fc_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            fc_b.call_id,
            "fc_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
    }

    #[test]
    fn tool_call_history_id_not_forwarded_as_item_id() {
        // Regression: OpenAI Responses API rejects item ids that exceed ~40 chars or use the
        // fc_-prefixed format from Chat Completions. Setting id: None avoids the rejection.
        let long_id = "fc_0bfce048f8124bc5006a068df33b348195ab7d85a4ab36380d".to_string();

        let tool_call = ChatCompletionMessageToolCalls::Function(ChatCompletionMessageToolCall {
            id: long_id.clone(),
            function: FunctionCall {
                name: "lookup".to_string(),
                arguments: "{}".to_string(),
            },
        });

        let item = input_item_from_chat_tool_call(tool_call).expect("should convert");
        let InputItem::Item(Item::FunctionCall(fc)) = item else {
            panic!("expected FunctionCall item");
        };
        assert_eq!(fc.id, None);
        assert_eq!(fc.call_id, long_id);
    }

    #[tokio::test]
    async fn maps_response_stream_deltas_to_response_id() {
        let created: ResponseStreamEvent = serde_json::from_value(json!({
            "type": "response.created",
            "sequence_number": 0,
            "response": minimal_response_json("resp_123", "in_progress")
        }))
        .expect("created stream event should deserialize");
        let delta: ResponseStreamEvent = serde_json::from_value(json!({
            "type": "response.output_text.delta",
            "sequence_number": 1,
            "item_id": "msg_123",
            "output_index": 0,
            "content_index": 0,
            "delta": "hel"
        }))
        .expect("text delta stream event should deserialize");
        let completed: ResponseStreamEvent = serde_json::from_value(json!({
            "type": "response.completed",
            "sequence_number": 2,
            "response": minimal_response_json("resp_123", "completed")
        }))
        .expect("completed stream event should deserialize");

        let response_stream: ResponseStream =
            Box::pin(stream::iter(vec![Ok(created), Ok(delta), Ok(completed)]));
        let mut chat_stream = chat_completion_stream_from_response_stream(
            response_stream,
            "public-model".to_string(),
        );

        let delta_chunk = chat_stream
            .next()
            .await
            .expect("stream should include delta chunk")
            .expect("delta chunk should be valid");
        assert_eq!(delta_chunk.id, "resp_123");
        assert_eq!(delta_chunk.created, 1_755_639_134);
        assert_eq!(delta_chunk.model, "public-model");
        assert_eq!(delta_chunk.choices[0].delta.content.as_deref(), Some("hel"));

        let completed_chunk = chat_stream
            .next()
            .await
            .expect("stream should include completion chunk")
            .expect("completion chunk should be valid");
        assert_eq!(completed_chunk.id, "resp_123");
        assert_eq!(completed_chunk.created, 1_755_639_134);
        assert_eq!(
            completed_chunk.choices[0].finish_reason,
            Some(FinishReason::Stop)
        );
    }

    fn minimal_response_json(id: &str, status: &str) -> serde_json::Value {
        json!({
            "created_at": 1_755_639_134,
            "id": id,
            "model": "backend-model",
            "object": "response",
            "output": [],
            "status": status
        })
    }
}
