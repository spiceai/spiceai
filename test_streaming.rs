// This is a test file to verify streaming behavior
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use async_openai::types::*;
use llms::chat::Chat;

// Mock streaming Chat implementation  
struct StreamingMockChat {
    name: String,
}

#[async_trait]
impl Chat for StreamingMockChat {
    fn as_sql(&self) -> Option<&dyn llms::chat::nsql::SqlGeneration> {
        None
    }

    async fn run(&self, prompt: String) -> llms::chat::Result<Option<String>> {
        Ok(Some(format!("Response from {}: {}", self.name, prompt)))
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, async_openai::error::OpenAIError> {
        use async_stream::stream;
        
        let model_name = self.name.clone();
        
        // Create a stream that yields multiple chunks
        let stream = stream! {
            println!("Streaming chunk 1");
            yield Ok(CreateChatCompletionStreamResponse {
                id: "stream-test".to_string(),
                model: model_name.clone(),
                object: "chat.completion.chunk".to_string(),
                created: 0,
                choices: vec![ChatChoiceStream {
                    index: 0,
                    delta: ChatCompletionStreamResponseDelta {
                        content: Some("Hello ".to_string()),
                        role: Some(Role::Assistant),
                        function_call: None,
                        tool_calls: None,
                        refusal: None,
                    },
                    finish_reason: None,
                    logprobs: None,
                }],
                usage: None,
                system_fingerprint: None,
                service_tier: None,
            });
            
            println!("Streaming chunk 2");
            yield Ok(CreateChatCompletionStreamResponse {
                id: "stream-test".to_string(),
                model: model_name.clone(),
                object: "chat.completion.chunk".to_string(),
                created: 0,
                choices: vec![ChatChoiceStream {
                    index: 0,
                    delta: ChatCompletionStreamResponseDelta {
                        content: Some("streaming ".to_string()),
                        role: None,
                        function_call: None,
                        tool_calls: None,
                        refusal: None,
                    },
                    finish_reason: None,
                    logprobs: None,
                }],
                usage: None,
                system_fingerprint: None,
                service_tier: None,
            });
            
            println!("Streaming chunk 3 (final)");
            yield Ok(CreateChatCompletionStreamResponse {
                id: "stream-test".to_string(),
                model: model_name.clone(),
                object: "chat.completion.chunk".to_string(),
                created: 0,
                choices: vec![ChatChoiceStream {
                    index: 0,
                    delta: ChatCompletionStreamResponseDelta {
                        content: Some("works!".to_string()),
                        role: None,
                        function_call: None,
                        tool_calls: None,
                        refusal: None,
                    },
                    finish_reason: Some(FinishReason::Stop),
                    logprobs: None,
                }],
                usage: Some(CompletionUsage {
                    prompt_tokens: 10,
                    completion_tokens: 20,
                    total_tokens: 30,
                    prompt_tokens_details: None,
                    completion_tokens_details: None,
                }),
                system_fingerprint: None,
                service_tier: None,
            });
        };
        
        Ok(Box::pin(stream))
    }

    async fn chat_request(
        &self,
        _req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, async_openai::error::OpenAIError> {
        unimplemented!("Use chat_stream instead")
    }
}

#[tokio::main]
async fn main() {
    println!("Testing streaming AI UDF behavior");
    
    // Create a mock model store
    let mut store = HashMap::new();
    store.insert("test-streaming".to_string(), Arc::new(StreamingMockChat {
        name: "test-streaming".to_string(),
    }) as Arc<dyn Chat>);
    
    let model_store = Arc::new(RwLock::new(store));
    let model_store_guard = model_store.read().await;
    let model = model_store_guard.get("test-streaming").unwrap();
    
    // Create a test request
    let request = CreateChatCompletionRequestArgs::default()
        .model("test-streaming".to_string())
        .messages(vec![
            ChatCompletionRequestSystemMessageArgs::default()
                .content("Hello, test streaming!".to_string())
                .build()
                .unwrap()
                .into(),
        ])
        .stream(true)
        .build()
        .unwrap();
    
    // Get the stream
    let mut stream = model.chat_stream(request).await.unwrap();
    
    let mut content_parts = Vec::new();
    let mut usage = None;
    
    println!("Starting to consume stream:");
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(response) => {
                if let Some(choice) = response.choices.first() {
                    if let Some(content) = &choice.delta.content {
                        println!("Received content chunk: '{}'", content);
                        content_parts.push(content.clone());
                    }
                }
                if response.usage.is_some() {
                    usage = response.usage;
                    println!("Received usage info: {:?}", usage);
                }
            }
            Err(e) => {
                println!("Stream error: {:?}", e);
                break;
            }
        }
    }
    
    let final_content = content_parts.join("");
    println!("Final combined content: '{}'", final_content);
    println!("Usage: {:?}", usage);
}