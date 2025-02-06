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

use async_openai::types::{
    ChatChoice, ChatChoiceStream, ChatCompletionMessageToolCall, ChatCompletionResponseMessage,
    ChatCompletionResponseStream, ChatCompletionStreamResponseDelta, ChatCompletionToolType,
    CreateChatCompletionResponse, FunctionCall,
};
use futures::StreamExt;
/// Accumulate a [`ChatCompletionResponseStream`] into a single [`CreateChatCompletionResponse`].
///
/// This enables comparing the output from [`super::Chat::chat_stream`] as if it was a [`super::Chat::chat_request`].
#[allow(deprecated, clippy::cast_possible_truncation)]
pub(crate) async fn accumulate(
    stream: ChatCompletionResponseStream,
) -> CreateChatCompletionResponse {
    stream
        .fold(
            CreateChatCompletionResponse {
                id: String::new(),
                choices: vec![],
                created: 0,
                model: String::new(),
                service_tier: None,
                system_fingerprint: None,
                object: String::new(),
                usage: None,
            },
            |mut acc, item| async move {
                if let Ok(stream) = item {
                    // Update these fields on first iteration only.
                    if acc.model.is_empty() {
                        acc.id = stream.id;
                        acc.created = stream.created;
                        acc.model = stream.model;
                        acc.service_tier = stream.service_tier;
                        acc.system_fingerprint = stream.system_fingerprint;
                        acc.object = stream.object;
                    }
                    // Usage will be non-null on last iteration.
                    if let Some(usage) = stream.usage {
                        acc.usage = Some(usage);
                    }

                    // Update stat of [`ChatChoice`].
                    // On first, need to infer `n` choices, initialise "default" [`ChatChoice`].
                    if acc.choices.is_empty() && !stream.choices.is_empty() {
                        acc.choices = (0..stream.choices.len())
                            .map(|i| ChatChoice {
                                index: i as u32,
                                finish_reason: None,
                                logprobs: None,
                                message: ChatCompletionResponseMessage {
                                    content: None,
                                    refusal: None,
                                    tool_calls: None,
                                    function_call: None,
                                    role: async_openai::types::Role::User,
                                    audio: None,
                                },
                            })
                            .collect();
                    }
                    acc.choices
                        .iter_mut()
                        .zip(stream.choices.into_iter())
                        .for_each(|(c, s)| update_chat_choice(c, s));
                }
                acc
            },
        )
        .await
}

fn update_chat_choice(acc: &mut ChatChoice, update: ChatChoiceStream) {
    let ChatChoiceStream {
        index,
        finish_reason,
        logprobs,
        delta:
            ChatCompletionStreamResponseDelta {
                content,
                refusal,
                tool_calls,
                role,
                ..
            },
    } = update;
    acc.index = index;
    acc.finish_reason = finish_reason;
    acc.logprobs = logprobs;
    if let Some(role) = role {
        acc.message.role = role;
    }

    match (&mut acc.message.content, content) {
        (Some(ref mut a), Some(b)) => *a += &b,
        (None, Some(b)) => acc.message.content = Some(b),
        _ => (),
    }
    match (&mut acc.message.refusal, refusal) {
        (Some(ref mut a), Some(b)) => *a += &b,
        (None, Some(b)) => acc.message.refusal = Some(b),
        _ => (),
    }

    if let Some(tool_calls) = tool_calls {
        tool_calls.into_iter().enumerate().for_each(|(i, tool)| {
            if acc.message.tool_calls.is_none() {
                acc.message.tool_calls = Some(vec![]);
            }
            if let Some(acc_tools) = acc.message.tool_calls.as_mut() {
                if acc_tools.get(i).is_none() {
                    acc_tools.insert(
                        i,
                        ChatCompletionMessageToolCall {
                            id: String::new(),
                            r#type: ChatCompletionToolType::Function,
                            function: FunctionCall {
                                name: String::new(),
                                arguments: String::new(),
                            },
                        },
                    );
                }

                if let Some(id) = &tool.id {
                    acc_tools[i].id.clone_from(id);
                }
                if let Some(r#type) = tool.r#type {
                    acc_tools[i].r#type = r#type;
                }

                if let Some(fun) = tool.function {
                    if let Some(args) = fun.arguments {
                        acc_tools[i].function.arguments += &args;
                    }
                    if let Some(name) = fun.name {
                        acc_tools[i].function.name += &name;
                    }
                }
            }
        });
    }
}

pub mod tests {

    use async_openai::{error::OpenAIError, types::CreateChatCompletionStreamResponse};
    use serde_json::json;

    use super::*;

    #[tokio::test]
    pub async fn test_accumulate() {
        let parts = vec![
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":\"call_AGU5KhGhzAsH14iFbZcvHNzx\",\"type\":\"function\",\"function\":{\"name\":\"get_current_weather\",\"arguments\":\"\"}}],\"role\":\"assistant\",\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":null,\"type\":null,\"function\":{\"name\":null,\"arguments\":\"{\\\"\"}}],\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":null,\"type\":null,\"function\":{\"name\":null,\"arguments\":\"location\"}}],\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":null,\"type\":null,\"function\":{\"name\":null,\"arguments\":\"\\\":\\\"\"}}],\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":null,\"type\":null,\"function\":{\"name\":null,\"arguments\":\"Boston\"}}],\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":null,\"type\":null,\"function\":{\"name\":null,\"arguments\":\",\"}}],\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":null,\"type\":null,\"function\":{\"name\":null,\"arguments\":\" MA\"}}],\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":null,\"type\":null,\"function\":{\"name\":null,\"arguments\":\"\\\",\\\"\"}}],\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":null,\"type\":null,\"function\":{\"name\":null,\"arguments\":\"unit\"}}],\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":null,\"type\":null,\"function\":{\"name\":null,\"arguments\":\"\\\":\\\"\"}}],\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":null,\"type\":null,\"function\":{\"name\":null,\"arguments\":\"c\"}}],\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":null,\"type\":null,\"function\":{\"name\":null,\"arguments\":\"elsius\"}}],\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"tool_calls\":[{\"index\":0,\"id\":null,\"type\":null,\"function\":{\"name\":null,\"arguments\":\"\\\"}\"}}],\"refusal\":null},\"finish_reason\":null,\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
            "{\"id\":\"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd\",\"choices\":[{\"index\":0,\"delta\":{\"content\":null,\"refusal\":null},\"finish_reason\":\"stop\",\"logprobs\":null}],\"created\":1736724650,\"model\":\"not_needed\",\"service_tier\":\"default\",\"system_fingerprint\":\"fp_72ed7ab54c\",\"object\":\"chat.completion.chunk\"}",
        ];

        let stream: ChatCompletionResponseStream =
            Box::pin(futures::stream::iter(parts.into_iter().map(|s| {
                serde_json::from_str::<CreateChatCompletionStreamResponse>(s)
                    .map_err(OpenAIError::from)
            })));

        let resp = accumulate(stream).await;
        assert_eq!(
            serde_json::to_value(&resp)
                .expect("Output of `accumulate` should be serializable to serde_json::Value"),
            json!({
                "id":"chatcmpl-Ap1hqCfgxosk7rTVtDHee6aFff0wd",
                "choices":[
                    {
                        "index":0,
                        "message":{
                            "content":null,
                            "refusal":null,
                            "tool_calls":[
                                {
                                    "id":"call_AGU5KhGhzAsH14iFbZcvHNzx",
                                    "type":"function",
                                    "function":{
                                        "name":"get_current_weather",
                                        "arguments":"{\"location\":\"Boston, MA\",\"unit\":\"celsius\"}"
                                    }
                                }
                            ],
                            "role":"assistant",
                            "function_call":null
                        },
                        "finish_reason":"stop",
                        "logprobs":null
                    }
                ],
                "created":1_736_724_650,
                "model":"not_needed",
                "service_tier":"default",
                "system_fingerprint":"fp_72ed7ab54c",
                "object":"chat.completion.chunk"
            })
        );
    }
}
