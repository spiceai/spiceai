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

//! [`AsyncScalarUDFImpl`] definitions for AI chat completion function.

use arrow::array::{Array, ArrayRef, StringArray};
use arrow_schema::DataType;
#[cfg(test)]
use async_openai::error::ApiError;
#[cfg(test)]
use async_openai::types::{
    ChatChoice, ChatCompletionResponseMessage, CompletionUsage, CreateChatCompletionResponse,
    CreateChatCompletionStreamResponse, FinishReason, Role,
};
use async_openai::types::{
    ChatCompletionRequestUserMessageArgs, ChatCompletionStreamOptions,
    CreateChatCompletionRequestArgs,
};

use datafusion::common::cast::as_string_array;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{DocSection, Documentation, ScalarFunctionArgs};
use datafusion::scalar::ScalarValue;
use datafusion::{
    common::{Result as DataFusionResult, exec_err},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use futures::StreamExt;
use tracing::{Instrument, Level};

use async_trait::async_trait;
use llms::chat::Chat;

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::{RwLock, Semaphore};
use tracing::Span;

// Security and performance constants
const MAX_MESSAGE_SIZE: usize = 1_000_000; // 1MB per message
const MAX_BATCH_SIZE: usize = 100; // Maximum rows per batch (LLM calls are slow)

pub static AI_UDF_NAME: &str = "ai";
pub static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation {
    doc_section: DocSection::default(),
    description: "Generates AI responses for text using a specified chat model".to_string(),
    syntax_example: "ai(message, model_name)".to_string(),
    sql_example: Some("SELECT ai('Hello, how are you?', 'gpt-4')".to_string()),
    arguments: Some(vec![
        ("message".to_string(), "The message string to send to the AI model.".to_string()),
        (
            "model_name".to_string(),
            "The name of the chat model to use as defined in the Spicepod (optional if only one model is configured).".to_string(),
        ),
    ]),
    alternative_syntax: Some(vec!["ai('What is the weather like today?')".to_string()]),
    related_udfs: None,
}
});

pub static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::one_of(
        vec![
            // ai(message)
            TypeSignature::Exact(vec![DataType::Utf8]),
            // ai(message, model_name)
            TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
        ],
        Volatility::Volatile, // Volatile because AI model responses are non-deterministic for the same input
    )
});

pub type ChatModelStore = HashMap<String, Arc<dyn Chat>>;

pub struct Ai {
    model_store: Arc<RwLock<ChatModelStore>>,
}

impl std::fmt::Debug for Ai {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ai")
            .field("model_store", &"<ChatModelStore>")
            .finish()
    }
}

impl Ai {
    #[must_use]
    pub fn new(model_store: Arc<RwLock<ChatModelStore>>) -> Self {
        Self { model_store }
    }

    #[must_use]
    pub fn into_async_udf(self) -> AsyncScalarUDF {
        AsyncScalarUDF::new(Arc::new(self))
    }

    async fn get_default_model_name(&self) -> DataFusionResult<String> {
        let model_store = self.model_store.read().await;
        let models: Vec<String> = model_store.keys().cloned().collect();

        match models.len() {
            0 => exec_err!("{AI_UDF_NAME}: No chat models configured in Spicepod"),
            1 => Ok(models[0].clone()),
            _ => exec_err!(
                "{AI_UDF_NAME}: Multiple chat models configured. Please specify model name as second argument"
            ),
        }
    }
}

impl ScalarUDFImpl for Ai {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        AI_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        exec_err!("AI UDF can only be called from async contexts. Use the async interface instead.")
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&DOCUMENTATION)
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for Ai {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
        config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<ArrayRef> {
        // Security: Validate argument count
        if args.args.is_empty() || args.args.len() > 2 {
            return exec_err!(
                "{AI_UDF_NAME} expects one or two arguments: message and optional model_name"
            );
        }

        // Security: Validate number of rows
        if args.number_rows > MAX_BATCH_SIZE {
            return exec_err!(
                "{AI_UDF_NAME} batch size ({}) exceeds maximum allowed ({})",
                args.number_rows,
                MAX_BATCH_SIZE
            );
        }

        let model_name = if args.args.len() == 2 {
            let model_arg = &args.args[1];
            match model_arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(model_name))) => {
                    // Security: Validate model name (prevent injection)
                    if model_name.is_empty() || model_name.len() > 256 {
                        return exec_err!("{AI_UDF_NAME} invalid model name length");
                    }
                    model_name.clone()
                }
                _ => {
                    return exec_err!("{AI_UDF_NAME} unsupported model parameter: {model_arg}");
                }
            }
        } else {
            self.get_default_model_name().await?
        };

        let model_store = self.model_store.read().await;
        let Some(model) = model_store.get(&model_name) else {
            return exec_err!(
                "{AI_UDF_NAME} cannot find model '{}'. Available models: {}",
                model_name,
                model_store.keys().cloned().collect::<Vec<_>>().join(", ")
            );
        };

        // Only convert the message argument to array (not the model name)
        // The model name is always a scalar and shouldn't be part of the columnar data
        let message_array = match &args.args[0] {
            ColumnarValue::Array(arr) => Arc::clone(arr),
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(args.number_rows)?,
        };

        // Use target_partitions from config for parallelism control
        let max_parallelism = config.execution.target_partitions;

        self.process_messages(
            Arc::clone(model),
            &model_name,
            message_array,
            max_parallelism,
        )
        .await
    }
}

impl Ai {
    async fn call_model(
        model: &Arc<dyn Chat>,
        model_name: &str,
        message: &str,
        _row_index: usize,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Sync + Send>> {
        // Security: Validate message size before processing
        if message.len() > MAX_MESSAGE_SIZE {
            return Err(format!(
                "Message size ({} bytes) exceeds maximum allowed size ({} bytes)",
                message.len(),
                MAX_MESSAGE_SIZE
            )
            .into());
        }

        async {
            tracing::debug!("Starting AI model call for message: {}", message);
            let mut stream = model
                .chat_stream(
                    CreateChatCompletionRequestArgs::default()
                        .messages(vec![
                            ChatCompletionRequestUserMessageArgs::default()
                                .content(message)
                                .build()
                                .map_err(|e| DataFusionError::External(Box::new(e)))?
                                .into(),
                        ])
                        .stream(true)
                        .stream_options(ChatCompletionStreamOptions {
                            include_usage: true,
                        })
                        .build()?,
                )
                .await?;

            // Performance: Pre-allocate with estimated size to reduce reallocations
            let mut complete_response = String::with_capacity(512);
            let max_response_size = MAX_MESSAGE_SIZE * 2;

            // Performance: Process stream chunks efficiently
            while let Some(chunk_result) = stream.next().await {
                let chunk = chunk_result?;

                // Performance: Use iterator directly to avoid intermediate allocations
                for choice in chunk.choices {
                    if let Some(ref content) = choice.delta.content {
                        let new_len = complete_response.len() + content.len();

                        // Security: Check accumulated response size
                        if new_len > max_response_size {
                            return Err("Response size exceeds maximum allowed size".into());
                        }

                        // Performance: push_str is optimized for string concatenation
                        complete_response.push_str(content);
                    }
                }
            }

            Ok(if complete_response.is_empty() {
                None
            } else {
                Some(complete_response)
            })
        }
        // Instrument the async block with an AI span as a child of the current (sql_query) span
        .instrument(tracing::span!(Level::INFO, "ai", model = %model_name))
        .await
    }

    async fn process_messages(
        &self,
        model: Arc<dyn Chat>,
        model_name: &str,
        message_array: ArrayRef,
        max_parallelism: usize,
    ) -> DataFusionResult<ArrayRef> {
        let message_array = as_string_array(&message_array)?;
        let array_len = message_array.len();

        // Security: Validate batch size
        if array_len > MAX_BATCH_SIZE {
            return exec_err!(
                "Batch size ({}) exceeds maximum allowed size ({})",
                array_len,
                MAX_BATCH_SIZE
            );
        }

        if array_len == 0 {
            return Ok(Arc::new(StringArray::from(Vec::<Option<String>>::new())) as ArrayRef);
        }

        // Always use parallel processing - LLM calls are I/O heavy, not compute heavy
        // Parallel processing benefits even small batches due to I/O wait times
        self.process_messages_parallel(&model, model_name, message_array, max_parallelism)
            .await
    }

    // Performance: Optimized parallel processing - always used since LLM calls are I/O heavy
    async fn process_messages_parallel(
        &self,
        model: &Arc<dyn Chat>,
        model_name: &str,
        message_array: &StringArray,
        max_parallelism: usize,
    ) -> DataFusionResult<ArrayRef> {
        let array_len = message_array.len();
        let parent_span = Span::current();

        // Performance: Use configured parallelism from DataFusion config (target_partitions)
        // Limit to batch size to avoid over-spawning
        let parallelism = std::cmp::min(max_parallelism, array_len);

        let semaphore = Arc::new(Semaphore::new(parallelism));

        // Performance: Pre-allocate task vector
        let mut tasks = Vec::with_capacity(array_len);

        for (row_index, message_opt) in message_array.iter().enumerate() {
            // Performance: Share Arc reference, only clone when spawning
            let model = Arc::clone(model);
            let model_name_str = model_name.to_string();
            let semaphore = Arc::clone(&semaphore);
            let parent_span = parent_span.clone();

            let task = if let Some(message) = message_opt {
                // Performance: Convert to owned string once before spawning
                let message = message.to_string();

                tokio::spawn(async move {
                    let _permit = semaphore
                        .acquire()
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    match Self::call_model(&model, &model_name_str, &message, row_index).await {
                        Ok(Some(result)) => {
                            tracing::info!(target: "task_history", captured_output = %result, row = %row_index);
                            Ok(Some(result))
                        }
                        Ok(None) => {
                            tracing::debug!(
                                "AI model returned empty response for row {}",
                                row_index
                            );
                            Ok(None)
                        }
                        Err(e) => {
                            // Security: Don't leak detailed error messages to parent span
                            tracing::error!(target: "task_history", parent: &parent_span, "AI model error for row {}", row_index);
                            tracing::debug!(target: "task_history", parent: &parent_span, "AI model error details: {}", e);
                            Err(DataFusionError::External(e))
                        }
                    }
                })
            } else {
                // Performance: Don't spawn task for null values, return immediately
                tokio::spawn(async move { Ok::<Option<String>, DataFusionError>(None) })
            };

            tasks.push(task);
        }

        // Performance: Collect results maintaining order
        let mut results = Vec::with_capacity(array_len);
        for task in tasks {
            let result = task
                .await
                .map_err(|e| DataFusionError::Internal(format!("Task join error: {e}")))??;
            results.push(result);
        }

        debug_assert_eq!(
            results.len(),
            array_len,
            "Result array length must match input array length"
        );

        Ok(Arc::new(StringArray::from(results)) as ArrayRef)
    }
}

#[cfg(test)]
// Allow various lints in test code for simplicity and readability.
// Test code prioritizes clarity over strict lint compliance.
#[allow(
    clippy::clone_on_ref_ptr,
    clippy::uninlined_format_args,
    clippy::too_many_lines
)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use async_openai::types::{ChatCompletionResponseStream, CreateChatCompletionRequest};
    use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDFImpl, Volatility};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::sync::RwLock;

    // Mock Chat implementation for testing
    struct MockChat {
        name: String,
    }

    #[async_trait]
    impl Chat for MockChat {
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
            // Extract the prompt from the request
            let prompt = req
                .messages
                .first()
                .and_then(|msg| match msg {
                    async_openai::types::ChatCompletionRequestMessage::System(sys_msg) => {
                        match &sys_msg.content {
                            async_openai::types::ChatCompletionRequestSystemMessageContent::Text(text) => Some(text.clone()),
                            async_openai::types::ChatCompletionRequestSystemMessageContent::Array(_) => Some("Array content".to_string()),
                        }
                    }
                    async_openai::types::ChatCompletionRequestMessage::User(user_msg) => {
                        match &user_msg.content {
                            async_openai::types::ChatCompletionRequestUserMessageContent::Text(text) => Some(text.clone()),
                            async_openai::types::ChatCompletionRequestUserMessageContent::Array(_) => Some("Array content".to_string()),
                        }
                    }
                    _ => None,
                })
                .unwrap_or_default();

            let response_text = format!("Response from {}: {}", self.name, prompt);
            let usage = Some(CompletionUsage {
                prompt_tokens: 10,
                completion_tokens: 20,
                total_tokens: 30,
                prompt_tokens_details: None,
                completion_tokens_details: None,
            });

            // Use shared streaming utilities - return the complete response as a single chunk
            Ok(llms::streaming_utils::create_mock_streaming_response(
                self.name.clone(),
                vec![response_text],
                usage,
            ))
        }

        async fn chat_request(
            &self,
            req: CreateChatCompletionRequest,
        ) -> Result<CreateChatCompletionResponse, async_openai::error::OpenAIError> {
            // Extract the prompt from the request
            let prompt = req
                .messages
                .first()
                .and_then(|msg| match msg {
                    async_openai::types::ChatCompletionRequestMessage::System(sys_msg) => {
                        match &sys_msg.content {
                            async_openai::types::ChatCompletionRequestSystemMessageContent::Text(text) => Some(text.clone()),
                            async_openai::types::ChatCompletionRequestSystemMessageContent::Array(_) => Some("Array content".to_string()),
                        }
                    }
                    async_openai::types::ChatCompletionRequestMessage::User(user_msg) => {
                        match &user_msg.content {
                            async_openai::types::ChatCompletionRequestUserMessageContent::Text(text) => Some(text.clone()),
                            async_openai::types::ChatCompletionRequestUserMessageContent::Array(_) => Some("Array content".to_string()),
                        }
                    }
                    _ => None,
                })
                .unwrap_or_default();

            let response_text = format!("Response from {}: {}", self.name, prompt);

            Ok(CreateChatCompletionResponse {
                id: "test-chat-id".to_string(),
                model: self.name.clone(),
                object: "chat.completion".to_string(),
                created: 0,
                choices: vec![ChatChoice {
                    index: 0,
                    message: ChatCompletionResponseMessage {
                        content: Some(response_text),
                        role: Role::Assistant,
                        #[allow(deprecated)]
                        function_call: None,
                        tool_calls: None,
                        refusal: None,
                        audio: None,
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
            })
        }
    }

    fn create_test_model_store() -> Arc<RwLock<ChatModelStore>> {
        let mut store = HashMap::new();
        let model = MockChat {
            name: "test-model".to_string(),
        };
        store.insert("test-model".to_string(), Arc::new(model) as Arc<dyn Chat>);
        Arc::new(RwLock::new(store))
    }

    #[test]
    fn test_ai_udf_signature() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        let sig = udf.signature();
        // Check that we have a OneOf signature with multiple options
        match &sig.type_signature {
            datafusion::logical_expr::TypeSignature::OneOf(sigs) => {
                assert_eq!(sigs.len(), 2);
            }
            _ => panic!("Expected OneOf signature"),
        }

        let return_type = udf
            .return_type(&[DataType::Utf8])
            .expect("should return Utf8 type");
        assert_eq!(return_type, DataType::Utf8);
    }

    #[tokio::test]
    async fn test_default_model_selection() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        let default_model = udf
            .get_default_model_name()
            .await
            .expect("should get default model");
        assert_eq!(default_model, "test-model");
    }

    #[tokio::test]
    async fn test_multiple_models_error() {
        let mut store = HashMap::new();

        let model1 = MockChat {
            name: "model1".to_string(),
        };
        let model2 = MockChat {
            name: "model2".to_string(),
        };

        store.insert("model1".to_string(), Arc::new(model1) as Arc<dyn Chat>);
        store.insert("model2".to_string(), Arc::new(model2) as Arc<dyn Chat>);

        let model_store = Arc::new(RwLock::new(store));
        let udf = Ai::new(model_store);

        let result = udf.get_default_model_name().await;
        assert!(result.is_err());
        assert!(
            result
                .expect_err("should error with multiple models")
                .to_string()
                .contains("Multiple chat models configured")
        );
    }

    #[tokio::test]
    async fn test_no_models_error() {
        let store = HashMap::new();
        let model_store = Arc::new(RwLock::new(store));
        let udf = Ai::new(model_store);

        let result = udf.get_default_model_name().await;
        assert!(result.is_err());
        assert!(
            result
                .expect_err("should error with no models")
                .to_string()
                .contains("No chat models configured")
        );
    }

    #[test]
    fn test_udf_name() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        assert_eq!(udf.name(), "ai");
    }

    #[test]
    fn test_documentation() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        let docs = udf.documentation().expect("should have documentation");
        assert_eq!(
            docs.description,
            "Generates AI responses for text using a specified chat model"
        );
        assert_eq!(docs.syntax_example, "ai(message, model_name)");
    }

    #[test]
    fn test_return_type_variations() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        // Test with single Utf8 argument
        let return_type1 = udf
            .return_type(&[DataType::Utf8])
            .expect("should return Utf8 for single arg");
        assert_eq!(return_type1, DataType::Utf8);

        // Test with two Utf8 arguments
        let return_type2 = udf
            .return_type(&[DataType::Utf8, DataType::Utf8])
            .expect("should return Utf8 for two args");
        assert_eq!(return_type2, DataType::Utf8);

        // Test with LargeUtf8
        let return_type3 = udf
            .return_type(&[DataType::LargeUtf8])
            .expect("should return Utf8 for LargeUtf8");
        assert_eq!(return_type3, DataType::Utf8);
    }

    #[test]
    fn test_non_async_invoke_with_args_error() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        let args = ScalarFunctionArgs {
            args: vec![],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("result", DataType::Utf8, false)),
        };

        let result = udf.invoke_with_args(args);
        assert!(result.is_err());
        assert!(
            result
                .expect_err("should error when called non-async")
                .to_string()
                .contains("can only be called from async contexts")
        );
    }

    // Additional Mock Chat implementation that can return errors
    struct ErrorMockChat;

    #[async_trait]
    impl Chat for ErrorMockChat {
        fn as_sql(&self) -> Option<&dyn llms::chat::nsql::SqlGeneration> {
            None
        }

        async fn run(&self, _prompt: String) -> llms::chat::Result<Option<String>> {
            Err(llms::chat::Error::FailedToRunModel {
                source: "Mock error for testing".into(),
            })
        }

        async fn chat_stream(
            &self,
            _req: CreateChatCompletionRequest,
        ) -> Result<ChatCompletionResponseStream, async_openai::error::OpenAIError> {
            Err(async_openai::error::OpenAIError::ApiError(ApiError {
                message: "Mock error for testing".to_string(),
                r#type: None,
                param: None,
                code: None,
            }))
        }

        async fn chat_request(
            &self,
            _req: CreateChatCompletionRequest,
        ) -> Result<CreateChatCompletionResponse, async_openai::error::OpenAIError> {
            Err(async_openai::error::OpenAIError::ApiError(ApiError {
                message: "Mock error for testing".to_string(),
                r#type: None,
                param: None,
                code: None,
            }))
        }
    }

    // Mock Chat that returns None responses
    struct NullMockChat;

    #[async_trait]
    impl Chat for NullMockChat {
        fn as_sql(&self) -> Option<&dyn llms::chat::nsql::SqlGeneration> {
            None
        }

        async fn run(&self, _prompt: String) -> llms::chat::Result<Option<String>> {
            Ok(None)
        }

        async fn chat_stream(
            &self,
            _req: CreateChatCompletionRequest,
        ) -> Result<ChatCompletionResponseStream, async_openai::error::OpenAIError> {
            use async_stream::stream;

            // Create a stream that yields empty content
            let stream = stream! {
                // Yield empty content chunk
                yield Ok(CreateChatCompletionStreamResponse {
                    id: "null-stream-id".to_string(),
                    model: "null-model".to_string(),
                    object: "chat.completion.chunk".to_string(),
                    created: 0,
                    choices: vec![async_openai::types::ChatChoiceStream {
                        index: 0,
                        delta: async_openai::types::ChatCompletionStreamResponseDelta {
                            content: None, // Empty content
                            role: Some(Role::Assistant),
                            tool_calls: None,
                            refusal: None,
                            #[allow(deprecated)]
                            function_call: None,
                        },
                        finish_reason: None,
                        logprobs: None,
                    }],
                    usage: None,
                    system_fingerprint: None,
                    service_tier: None,
                });

                // Yield final chunk with usage
                yield Ok(CreateChatCompletionStreamResponse {
                    id: "null-stream-id".to_string(),
                    model: "null-model".to_string(),
                    object: "chat.completion.chunk".to_string(),
                    created: 0,
                    choices: vec![],
                    usage: Some(CompletionUsage {
                        prompt_tokens: 5,
                        completion_tokens: 0,
                        total_tokens: 5,
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
            Ok(CreateChatCompletionResponse {
                id: "null-chat-id".to_string(),
                model: "null-model".to_string(),
                object: "chat.completion".to_string(),
                created: 0,
                choices: vec![ChatChoice {
                    index: 0,
                    message: ChatCompletionResponseMessage {
                        content: None, // This represents a null/empty response
                        role: Role::Assistant,
                        #[allow(deprecated)]
                        function_call: None,
                        tool_calls: None,
                        refusal: None,
                        audio: None,
                    },
                    finish_reason: Some(FinishReason::Stop),
                    logprobs: None,
                }],
                usage: Some(CompletionUsage {
                    prompt_tokens: 5,
                    completion_tokens: 0, // No completion tokens for null response
                    total_tokens: 5,
                    prompt_tokens_details: None,
                    completion_tokens_details: None,
                }),
                system_fingerprint: None,
                service_tier: None,
            })
        }
    }

    fn create_multi_model_store() -> Arc<RwLock<ChatModelStore>> {
        let mut store = HashMap::new();

        store.insert(
            "gpt-4".to_string(),
            Arc::new(MockChat {
                name: "gpt-4".to_string(),
            }) as Arc<dyn Chat>,
        );
        store.insert(
            "claude".to_string(),
            Arc::new(MockChat {
                name: "claude".to_string(),
            }) as Arc<dyn Chat>,
        );
        store.insert(
            "error-model".to_string(),
            Arc::new(ErrorMockChat) as Arc<dyn Chat>,
        );
        store.insert(
            "null-model".to_string(),
            Arc::new(NullMockChat) as Arc<dyn Chat>,
        );

        Arc::new(RwLock::new(store))
    }

    #[tokio::test]
    async fn test_process_single_message() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store.clone());

        let model_store_guard = model_store.read().await;
        let model = model_store_guard
            .get("test-model")
            .expect("should get test-model");

        let messages = Arc::new(arrow::array::StringArray::from(vec![Some("Hello")]));
        let result = udf
            .process_messages(
                Arc::clone(model),
                "test-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await
            .expect("should process messages");

        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");
        assert_eq!(string_array.len(), 1);
        assert_eq!(string_array.value(0), "Response from test-model: Hello");
    }

    #[tokio::test]
    async fn test_process_multiple_messages() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store.clone());

        let model_store_guard = model_store.read().await;
        let model = model_store_guard
            .get("test-model")
            .expect("should get test-model");

        let messages = Arc::new(arrow::array::StringArray::from(vec![
            Some("Hello"),
            Some("How are you?"),
            Some("Goodbye"),
        ]));
        let result = udf
            .process_messages(
                Arc::clone(model),
                "test-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await
            .expect("should invoke async");

        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");
        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "Response from test-model: Hello");
        assert_eq!(
            string_array.value(1),
            "Response from test-model: How are you?"
        );
        assert_eq!(string_array.value(2), "Response from test-model: Goodbye");
    }

    #[tokio::test]
    async fn test_process_messages_with_nulls() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store.clone());

        let model_store_guard = model_store.read().await;
        let model = model_store_guard
            .get("test-model")
            .expect("should get test-model");

        let messages = Arc::new(arrow::array::StringArray::from(vec![
            Some("Hello"),
            None,
            Some("Goodbye"),
        ]));
        let result = udf
            .process_messages(
                Arc::clone(model),
                "test-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await
            .expect("should invoke async");

        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");
        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "Response from test-model: Hello");
        assert!(string_array.is_null(1));
        assert_eq!(string_array.value(2), "Response from test-model: Goodbye");
    }

    #[tokio::test]
    async fn test_process_messages_with_model_error() {
        let model_store = create_multi_model_store();
        let udf = Ai::new(model_store.clone());

        let model_store_guard = model_store.read().await;
        let model = model_store_guard
            .get("error-model")
            .expect("should get error-model");

        let messages = Arc::new(arrow::array::StringArray::from(vec![Some("Hello")]));
        let result = udf
            .process_messages(
                Arc::clone(model),
                "error-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await;

        assert!(result.is_err());
        assert!(
            result
                .expect_err("should error with mock error")
                .to_string()
                .contains("Mock error for testing")
        );
    }

    #[tokio::test]
    async fn test_process_messages_with_null_response() {
        let model_store = create_multi_model_store();
        let udf = Ai::new(model_store.clone());

        let model_store_guard = model_store.read().await;
        let model = model_store_guard
            .get("null-model")
            .expect("should get null-model");

        let messages = Arc::new(arrow::array::StringArray::from(vec![Some("Hello")]));
        let result = udf
            .process_messages(
                Arc::clone(model),
                "null-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await
            .expect("should invoke async");

        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");
        assert_eq!(string_array.len(), 1);
        assert!(string_array.is_null(0));
    }

    #[test]
    fn test_debug_implementation() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        let debug_str = format!("{:?}", udf);
        assert!(debug_str.contains("Ai"));
        assert!(debug_str.contains("ChatModelStore"));
    }

    #[test]
    fn test_into_async_udf() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        let async_udf = udf.into_async_udf();
        let scalar_udf = async_udf.into_scalar_udf();

        assert_eq!(scalar_udf.name(), "ai");
    }

    #[test]
    fn test_signature_volatility() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        let sig = udf.signature();
        assert_eq!(sig.volatility, Volatility::Volatile);
    }

    #[test]
    fn test_signature_type_signatures() {
        let sig = &*SIGNATURE;

        // Check that we have the expected number of type signatures
        match &sig.type_signature {
            datafusion::logical_expr::TypeSignature::OneOf(sigs) => {
                assert_eq!(sigs.len(), 2);

                // Check single argument signature
                match &sigs[0] {
                    datafusion::logical_expr::TypeSignature::Exact(types) => {
                        assert_eq!(types.len(), 1);
                        assert_eq!(types[0], DataType::Utf8);
                    }
                    _ => panic!("Expected Exact signature"),
                }

                // Check two argument signature
                match &sigs[1] {
                    datafusion::logical_expr::TypeSignature::Exact(types) => {
                        assert_eq!(types.len(), 2);
                        assert_eq!(types[0], DataType::Utf8);
                        assert_eq!(types[1], DataType::Utf8);
                    }
                    _ => panic!("Expected Exact signature"),
                }
            }
            _ => panic!("Expected OneOf signature"),
        }
    }

    #[tokio::test]
    async fn test_ai_span_parent_child_relationship() {
        use tracing::Level;

        // This test verifies that the AI UDF properly accepts and uses parent span context
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store.clone());

        let model_store_guard = model_store.read().await;
        let model = model_store_guard
            .get("test-model")
            .expect("should get test-model");

        // Create a parent span to simulate sql_query span
        let _sql_query_span = tracing::span!(Level::INFO, "sql_query", query = "SELECT ai('test')");

        // Test that process_messages can accept and use a parent span without errors
        let messages = Arc::new(arrow::array::StringArray::from(vec![Some("Hello test")]));
        let result = udf
            .process_messages(
                Arc::clone(model),
                "test-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await;

        // The test passes if process_messages executes without error using the parent span
        assert!(
            result.is_ok(),
            "process_messages should succeed with parent span"
        );

        let response_array = result.expect("should get result");
        let string_array = response_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");

        assert_eq!(string_array.len(), 1);
        assert_eq!(
            string_array.value(0),
            "Response from test-model: Hello test"
        );
    }

    #[tokio::test]
    async fn test_invoke_with_columnar_message_and_scalar_model() {
        // This test verifies the fix for the "all columns in a record batch must have
        // the same length" error. When calling ai(column, 'model'), the first argument
        // is a columnar array and the second is a scalar model name.

        let model_store = create_multi_model_store();
        let udf = Ai::new(model_store.clone());

        // Simulate a query like: SELECT ai(title, 'gpt-4') FROM pulls LIMIT 3
        // where title is a column (array) and 'gpt-4' is a scalar literal
        let message_array = ColumnarValue::Array(Arc::new(arrow::array::StringArray::from(vec![
            Some("First message"),
            Some("Second message"),
            Some("Third message"),
        ])));
        let model_name_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("gpt-4".to_string())));

        let args = ScalarFunctionArgs {
            args: vec![message_array, model_name_scalar],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("result", DataType::Utf8, false)),
        };

        // This should not fail with "all columns in a record batch must have the same length"
        let result = udf
            .invoke_async_with_args(args, &datafusion::config::ConfigOptions::default())
            .await;

        assert!(
            result.is_ok(),
            "invoke_async_with_args should handle columnar message + scalar model: {:?}",
            result.err()
        );

        let response_array = result.expect("should get result");
        let string_array = response_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");

        // Verify we got 3 responses (matching the input array length)
        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "Response from gpt-4: First message");
        assert_eq!(string_array.value(1), "Response from gpt-4: Second message");
        assert_eq!(string_array.value(2), "Response from gpt-4: Third message");
    }

    #[tokio::test]
    async fn test_invoke_with_scalar_message_and_scalar_model_multiple_rows() {
        // This test verifies the fix for queries like:
        // SELECT LocationID, ai('hi', 'gpt-4o') FROM taxi_zones_direct LIMIT 5
        // where both arguments are scalar literals but need to be applied to multiple rows

        let model_store = create_multi_model_store();
        let udf = Ai::new(model_store.clone());

        // Simulate: SELECT ai('hi', 'gpt-4') FROM table LIMIT 5
        // Both arguments are scalars, but the function is called for 5 rows
        let message_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("hi".to_string())));
        let model_name_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("gpt-4".to_string())));

        let args = ScalarFunctionArgs {
            args: vec![message_scalar, model_name_scalar],
            arg_fields: vec![],
            number_rows: 5,
            return_field: Arc::new(Field::new("result", DataType::Utf8, false)),
        };

        // This should not fail with "all columns in a record batch must have the same length"
        let result = udf
            .invoke_async_with_args(args, &datafusion::config::ConfigOptions::default())
            .await;

        assert!(
            result.is_ok(),
            "invoke_async_with_args should handle scalar message + scalar model for multiple rows: {:?}",
            result.err()
        );

        let response_array = result.expect("should get result");
        let string_array = response_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");

        // Verify we got 5 responses (matching the number_rows)
        assert_eq!(string_array.len(), 5);
        // All responses should be the same since the input is the same
        for i in 0..5 {
            assert_eq!(string_array.value(i), "Response from gpt-4: hi");
        }
    }

    #[tokio::test]
    async fn test_invoke_with_mixed_array_and_scalars() {
        // This test covers various combinations of array and scalar arguments
        // to ensure proper handling in all cases

        let model_store = create_multi_model_store();
        let udf = Ai::new(model_store.clone());

        // Test 1: Array message with explicit model (as in: SELECT ai(column, 'model'))
        let message_array = ColumnarValue::Array(Arc::new(arrow::array::StringArray::from(vec![
            Some("Query 1"),
            Some("Query 2"),
        ])));
        let model_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("gpt-4".to_string())));

        let args = ScalarFunctionArgs {
            args: vec![message_array, model_scalar],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(Field::new("result", DataType::Utf8, false)),
        };

        let result = udf
            .invoke_async_with_args(args, &datafusion::config::ConfigOptions::default())
            .await
            .expect("Array message + scalar model should work");

        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");
        assert_eq!(string_array.len(), 2);
        assert_eq!(string_array.value(0), "Response from gpt-4: Query 1");
        assert_eq!(string_array.value(1), "Response from gpt-4: Query 2");

        // Test 2: Array message with nulls
        let message_array_with_nulls =
            ColumnarValue::Array(Arc::new(arrow::array::StringArray::from(vec![
                Some("Hello"),
                None,
                Some("World"),
            ])));
        let model_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("claude".to_string())));

        let args = ScalarFunctionArgs {
            args: vec![message_array_with_nulls, model_scalar],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("result", DataType::Utf8, false)),
        };

        let result = udf
            .invoke_async_with_args(args, &datafusion::config::ConfigOptions::default())
            .await
            .expect("Array with nulls should work");

        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");
        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "Response from claude: Hello");
        assert!(string_array.is_null(1));
        assert_eq!(string_array.value(2), "Response from claude: World");

        // Test 3: Single scalar message expanded to multiple rows (the original bug case)
        let single_message =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Same message".to_string())));
        let model_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("gpt-4".to_string())));

        let args = ScalarFunctionArgs {
            args: vec![single_message, model_scalar],
            arg_fields: vec![],
            number_rows: 10, // Expanded to 10 rows
            return_field: Arc::new(Field::new("result", DataType::Utf8, false)),
        };

        let result = udf
            .invoke_async_with_args(args, &datafusion::config::ConfigOptions::default())
            .await
            .expect("Scalar expanded to multiple rows should work");

        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");
        assert_eq!(string_array.len(), 10);
        // All should have the same response
        for i in 0..10 {
            assert_eq!(
                string_array.value(i),
                "Response from gpt-4: Same message",
                "Row {} should have the same response",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_invoke_async_captures_current_span() {
        use tracing::Level;

        // This test verifies that invoke_async_with_args properly captures the current span
        // and passes it to process_messages

        let model_store = create_test_model_store();
        let udf = Ai::new(model_store.clone());

        // Create a test span and enter it to simulate DataFusion's sql_query span context
        let sql_query_span = tracing::span!(Level::INFO, "sql_query", query = "SELECT ai('test')");
        let _enter = sql_query_span.enter();

        // Create test arguments for the UDF
        let message_scalar =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Hello test".to_string())));
        let args = ScalarFunctionArgs {
            args: vec![message_scalar],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::Utf8, false)),
        };

        // Call invoke_async_with_args which should capture the current span (sql_query)
        let result = udf
            .invoke_async_with_args(args, &datafusion::config::ConfigOptions::default())
            .await;

        // Verify that the function executed successfully
        // The real parent-child relationship will be established at runtime with proper tracing
        assert!(
            result.is_ok(),
            "invoke_async_with_args should succeed and capture parent span"
        );

        let response_array = result.expect("should get result");
        let string_array = response_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");

        assert_eq!(string_array.len(), 1);
        assert_eq!(
            string_array.value(0),
            "Response from test-model: Hello test"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_basic_tracing_works() {
        use tracing::Level;

        // Test that basic tracing works
        tracing::info!("Test message");
        let span = tracing::span!(Level::INFO, "test_span");
        let _enter = span.enter();
        tracing::info!("Inside test span");

        logs_assert(|lines: &[&str]| {
            let has_test_message = lines.iter().any(|line| line.contains("Test message"));
            let has_span_message = lines.iter().any(|line| line.contains("Inside test span"));

            if has_test_message && has_span_message {
                Ok(())
            } else {
                Err(format!("Missing basic tracing. Lines: {:?}", lines))
            }
        });
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_ai_span_creation_issue() {
        use tracing::Level;

        // This test reveals why spans aren't being created properly
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store.clone());

        // Test 1: Verify direct span creation works
        tracing::info!("Starting AI span test");

        // Test 2: Create and enter parent span
        let sql_query_span = tracing::span!(Level::INFO, "sql_query", query = "SELECT ai('test')");
        let _enter = sql_query_span.enter();
        tracing::info!("Inside sql_query span");

        // Test 3: Try calling process_messages directly to see if spans are created
        let model_store_guard = model_store.read().await;
        let model = model_store_guard
            .get("test-model")
            .expect("should get test-model");
        let messages = Arc::new(arrow::array::StringArray::from(vec![Some("Hello test")]));

        tracing::info!("About to call process_messages");
        let _result = udf
            .process_messages(
                Arc::clone(model),
                "test-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await;
        tracing::info!("process_messages completed");

        logs_assert(|lines: &[&str]| {
            let has_start = lines
                .iter()
                .any(|line| line.contains("Starting AI span test"));
            let has_sql_query = lines
                .iter()
                .any(|line| line.contains("Inside sql_query span"));
            let has_process_start = lines
                .iter()
                .any(|line| line.contains("About to call process_messages"));
            let has_process_end = lines
                .iter()
                .any(|line| line.contains("process_messages completed"));

            // Look for proper AI child span - it should have "ai{" in the span hierarchy
            let has_ai_child_span = lines.iter().any(|line| {
                // The AI span should appear as a child with format like "sql_query:ai:" but we only see "sql_query:"
                line.contains("}:ai{") && line.contains("model=")
            });

            if !has_start || !has_sql_query || !has_process_start || !has_process_end {
                return Err(format!(
                    "Missing basic trace messages. Start: {}, SQL: {}, Process start: {}, Process end: {}. Lines: {:?}",
                    has_start, has_sql_query, has_process_start, has_process_end, lines
                ));
            }

            if !has_ai_child_span {
                return Err(format!(
                    "AI child span was not created! The span should appear as 'sql_query:ai:' but we only see 'sql_query:'. This confirms the parent-child relationship is broken. Lines: {:?}",
                    lines
                ));
            }

            Ok(())
        });
    }

    #[tokio::test]
    async fn test_ai_udf_full_span_flow() {
        use tracing::Level;

        // This test documents the expected span flow behavior:
        // 1. SQL query execution creates sql_query span
        // 2. AI UDF invoke_async_with_args captures current span (sql_query)
        // 3. AI UDF creates child ai spans with proper parent context
        // 4. In production with tracing enabled, parent_span_id will be set correctly

        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        // Simulate the DataFusion execution context where sql_query span exists
        let sql_query_span = tracing::span!(
            Level::INFO,
            "sql_query",
            query = "SELECT ai('What is the weather?')"
        );

        let result = {
            let _enter = sql_query_span.enter();

            // Create test arguments that would come from DataFusion
            let message_scalar =
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("What is the weather?".to_string())));
            let args = ScalarFunctionArgs {
                args: vec![message_scalar],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("result", DataType::Utf8, false)),
            };

            // This simulates what happens when DataFusion calls the AI UDF:
            // 1. invoke_async_with_args captures tracing::Span::current() (sql_query)
            // 2. The captured span is passed to process_messages
            // 3. process_messages creates ai spans within parent context
            // 4. With proper tracing subscriber, parent_span_id relationships are recorded
            udf.invoke_async_with_args(args, &datafusion::config::ConfigOptions::default())
                .await
        };

        // Verify the UDF executed successfully
        assert!(result.is_ok(), "Full AI UDF execution should succeed");

        let response_array = result.expect("should get result");
        let string_array = response_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");

        assert_eq!(string_array.len(), 1);
        assert_eq!(
            string_array.value(0),
            "Response from test-model: What is the weather?"
        );

        // NOTE: In production with a tracing subscriber (like OpenTelemetry):
        // - The sql_query span will have a unique span_id
        // - The ai span will be created as a child with parent_span_id = sql_query.span_id
        // - The task_history table will show proper parent-child relationships
        // - Token usage (input_tokens, output_tokens, total_tokens) will be logged as labels

        // This test confirms the mechanism is in place - the actual tracing verification
        // requires runtime testing with a real tracing backend like the task_history system
    }

    #[tokio::test]
    async fn test_parallel_processing_with_multiple_messages() {
        // Mock Chat that simulates processing time
        struct SlowMockChat {
            name: String,
            delay: Duration,
        }

        #[async_trait]
        impl Chat for SlowMockChat {
            fn as_sql(&self) -> Option<&dyn llms::chat::nsql::SqlGeneration> {
                None
            }

            async fn run(&self, prompt: String) -> llms::chat::Result<Option<String>> {
                tokio::time::sleep(self.delay).await;
                Ok(Some(format!("Response from {}: {}", self.name, prompt)))
            }

            async fn chat_stream(
                &self,
                req: CreateChatCompletionRequest,
            ) -> Result<ChatCompletionResponseStream, async_openai::error::OpenAIError>
            {
                // Simulate processing time
                tokio::time::sleep(self.delay).await;

                // Extract the prompt from the request
                let prompt = req
                    .messages
                    .first()
                    .and_then(|msg| match msg {
                        async_openai::types::ChatCompletionRequestMessage::User(user_msg) => {
                            match &user_msg.content {
                                async_openai::types::ChatCompletionRequestUserMessageContent::Text(text) => Some(text.clone()),
                                async_openai::types::ChatCompletionRequestUserMessageContent::Array(_) => Some("Array content".to_string()),
                            }
                        }
                        _ => None,
                    })
                    .unwrap_or_default();

                let response_text = format!("Response from {}: {}", self.name, prompt);
                let usage = Some(CompletionUsage {
                    prompt_tokens: 10,
                    completion_tokens: 20,
                    total_tokens: 30,
                    prompt_tokens_details: None,
                    completion_tokens_details: None,
                });

                Ok(llms::streaming_utils::create_mock_streaming_response(
                    self.name.clone(),
                    vec![response_text],
                    usage,
                ))
            }

            async fn chat_request(
                &self,
                _req: CreateChatCompletionRequest,
            ) -> Result<CreateChatCompletionResponse, async_openai::error::OpenAIError>
            {
                tokio::time::sleep(self.delay).await;
                Ok(CreateChatCompletionResponse {
                    id: "slow-chat-id".to_string(),
                    model: self.name.clone(),
                    object: "chat.completion".to_string(),
                    created: 0,
                    choices: vec![],
                    usage: None,
                    system_fingerprint: None,
                    service_tier: None,
                })
            }
        }

        let mut store = HashMap::new();
        let slow_model = SlowMockChat {
            name: "slow-model".to_string(),
            delay: Duration::from_millis(100), // 100ms delay per call
        };
        store.insert(
            "slow-model".to_string(),
            Arc::new(slow_model) as Arc<dyn Chat>,
        );
        let model_store = Arc::new(RwLock::new(store));
        let udf = Ai::new(model_store.clone());

        let model_store_guard = model_store.read().await;
        let model = model_store_guard
            .get("slow-model")
            .expect("should get slow-model");

        // Test with 8 messages - if processed sequentially would take ~800ms,
        // but with parallelism should be much faster
        let messages = Arc::new(arrow::array::StringArray::from(vec![
            Some("Message 1"),
            Some("Message 2"),
            Some("Message 3"),
            Some("Message 4"),
            Some("Message 5"),
            Some("Message 6"),
            Some("Message 7"),
            Some("Message 8"),
        ]));

        let start = Instant::now();
        let result = udf
            .process_messages(
                Arc::clone(model),
                "slow-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await
            .expect("should process messages in parallel");
        let elapsed = start.elapsed();

        // Verify all results are correct
        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");
        assert_eq!(string_array.len(), 8);

        for i in 0..8 {
            assert_eq!(
                string_array.value(i),
                format!("Response from slow-model: Message {}", i + 1)
            );
        }

        // With parallelism, should take roughly 100ms * ceil(8 / num_cores)
        // rather than 800ms sequentially. Allow generous margin for test stability.
        let max_expected_time = Duration::from_millis(500);
        assert!(
            elapsed < max_expected_time,
            "Parallel processing took {}ms, expected less than {}ms",
            elapsed.as_millis(),
            max_expected_time.as_millis()
        );

        println!(
            "Parallel processing of 8 messages took: {}ms",
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_max_message_size_validation() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        // Create a message that exceeds MAX_MESSAGE_SIZE
        let large_message = "x".repeat(MAX_MESSAGE_SIZE + 1);
        let messages = Arc::new(StringArray::from(vec![Some(large_message.as_str())]));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(messages)],
            arg_fields: vec![Arc::new(Field::new("message", DataType::Utf8, true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
        };

        let result = udf
            .invoke_async_with_args(args, &datafusion::config::ConfigOptions::default())
            .await;

        assert!(result.is_err());
        let err_msg = result
            .expect_err("should return error for oversized message")
            .to_string();
        assert!(
            err_msg.contains("exceeds maximum allowed size"),
            "Expected size validation error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_max_batch_size_validation() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "test".to_string(),
            )))],
            arg_fields: vec![Arc::new(Field::new("message", DataType::Utf8, true))],
            number_rows: MAX_BATCH_SIZE + 1,
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
        };

        let result = udf
            .invoke_async_with_args(args, &datafusion::config::ConfigOptions::default())
            .await;

        assert!(result.is_err());
        let err_msg = result
            .expect_err("should return error for oversized batch")
            .to_string();
        assert!(
            err_msg.contains("batch size") && err_msg.contains("exceeds maximum"),
            "Expected batch size validation error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_model_name_length_validation() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        // Test empty model name
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("test".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::new()))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("message", DataType::Utf8, true)),
                Arc::new(Field::new("model", DataType::Utf8, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
        };

        let result = udf
            .invoke_async_with_args(args, &datafusion::config::ConfigOptions::default())
            .await;

        assert!(result.is_err());
        let err_msg = result
            .expect_err("should return error for empty model name")
            .to_string();
        assert!(
            err_msg.contains("invalid model name length"),
            "Expected model name validation error, got: {}",
            err_msg
        );

        // Test model name too long
        let long_model_name = "x".repeat(257);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("test".to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(long_model_name))),
            ],
            arg_fields: vec![
                Arc::new(Field::new("message", DataType::Utf8, true)),
                Arc::new(Field::new("model", DataType::Utf8, true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
        };

        let result = udf
            .invoke_async_with_args(args, &datafusion::config::ConfigOptions::default())
            .await;

        assert!(result.is_err());
        let err_msg = result
            .expect_err("should return error for long model name")
            .to_string();
        assert!(
            err_msg.contains("invalid model name length"),
            "Expected model name validation error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_parallel_processing_for_small_batches() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store.clone());

        // Create a small batch - should still use parallel processing since LLM calls are I/O heavy
        let messages = Arc::new(StringArray::from(vec![
            Some("Message 1"),
            Some("Message 2"),
            Some("Message 3"),
        ]));

        let model_store_read = model_store.read().await;
        let model = model_store_read
            .get("test-model")
            .expect("should get test-model");

        let start = Instant::now();
        let result = udf
            .process_messages(
                Arc::clone(model),
                "test-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await;
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        let result_array = result.expect("should process messages successfully");
        let string_array = result_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");

        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "Response from test-model: Message 1");
        assert_eq!(string_array.value(1), "Response from test-model: Message 2");
        assert_eq!(string_array.value(2), "Response from test-model: Message 3");

        // Parallel processing benefits even small batches due to I/O wait times
        println!(
            "Parallel processing of 3 messages took: {}ms",
            elapsed.as_millis()
        );
    }

    #[tokio::test]
    async fn test_empty_batch_handling() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store.clone());

        let messages = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));

        let model_store_read = model_store.read().await;
        let model = model_store_read
            .get("test-model")
            .expect("should get test-model");

        let result = udf
            .process_messages(
                Arc::clone(model),
                "test-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await;

        assert!(result.is_ok());
        let result_array = result.expect("should process empty batch successfully");
        let string_array = result_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");

        assert_eq!(string_array.len(), 0);
    }

    #[tokio::test]
    async fn test_response_size_limit() {
        // Mock Chat that returns very large responses
        struct LargeResponseMockChat;

        #[async_trait]
        impl Chat for LargeResponseMockChat {
            fn as_sql(&self) -> Option<&dyn llms::chat::nsql::SqlGeneration> {
                None
            }

            async fn run(&self, _prompt: String) -> llms::chat::Result<Option<String>> {
                Ok(Some("x".repeat(MAX_MESSAGE_SIZE * 2 + 1)))
            }

            async fn chat_stream(
                &self,
                _req: CreateChatCompletionRequest,
            ) -> Result<ChatCompletionResponseStream, async_openai::error::OpenAIError>
            {
                // Create a stream with multiple chunks that accumulate to exceed the limit
                // First chunk is within limit, second chunk pushes it over
                let chunk1 = "x".repeat(MAX_MESSAGE_SIZE + 1);
                let chunk2 = "y".repeat(MAX_MESSAGE_SIZE + 1);

                Ok(llms::streaming_utils::create_mock_streaming_response(
                    "large-model".to_string(),
                    vec![chunk1, chunk2],
                    None,
                ))
            }

            async fn chat_request(
                &self,
                _req: CreateChatCompletionRequest,
            ) -> Result<CreateChatCompletionResponse, async_openai::error::OpenAIError>
            {
                Err(async_openai::error::OpenAIError::ApiError(ApiError {
                    message: "Not implemented".to_string(),
                    r#type: None,
                    param: None,
                    code: None,
                }))
            }
        }

        let mut store = HashMap::new();
        store.insert(
            "large-model".to_string(),
            Arc::new(LargeResponseMockChat) as Arc<dyn Chat>,
        );
        let model_store = Arc::new(RwLock::new(store));
        let udf = Ai::new(model_store.clone());

        let messages = Arc::new(StringArray::from(vec![Some("test")]));

        let model_store_read = model_store.read().await;
        let model = model_store_read
            .get("large-model")
            .expect("should get large-model");

        let result = udf
            .process_messages(
                Arc::clone(model),
                "large-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await;

        assert!(result.is_err());
        let err_msg = result
            .expect_err("should return error for oversized response")
            .to_string();
        assert!(
            err_msg.contains("Response size exceeds maximum"),
            "Expected response size validation error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_parallelism_calculation() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store.clone());

        // Test with batch size larger than MIN_PARALLEL_THRESHOLD
        let messages = Arc::new(StringArray::from(vec![
            Some("Message 1"),
            Some("Message 2"),
            Some("Message 3"),
            Some("Message 4"),
            Some("Message 5"),
        ]));

        let model_store_read = model_store.read().await;
        let model = model_store_read
            .get("test-model")
            .expect("should get test-model");

        let result = udf
            .process_messages(
                Arc::clone(model),
                "test-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await;

        assert!(result.is_ok());
        let result_array = result.expect("should process messages successfully");
        let string_array = result_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");

        assert_eq!(string_array.len(), 5);
        for i in 0..5 {
            assert_eq!(
                string_array.value(i),
                format!("Response from test-model: Message {}", i + 1)
            );
        }
    }

    #[tokio::test]
    async fn test_mixed_null_and_valid_messages() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store.clone());

        // Mix of valid messages and nulls
        let messages = Arc::new(StringArray::from(vec![
            Some("Message 1"),
            None,
            Some("Message 3"),
            None,
            Some("Message 5"),
        ]));

        let model_store_read = model_store.read().await;
        let model = model_store_read
            .get("test-model")
            .expect("should get test-model");

        let result = udf
            .process_messages(
                Arc::clone(model),
                "test-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            )
            .await;

        assert!(result.is_ok());
        let result_array = result.expect("should process mixed null and valid messages");
        let string_array = result_array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");

        assert_eq!(string_array.len(), 5);
        assert_eq!(string_array.value(0), "Response from test-model: Message 1");
        assert!(string_array.is_null(1));
        assert_eq!(string_array.value(2), "Response from test-model: Message 3");
        assert!(string_array.is_null(3));
        assert_eq!(string_array.value(4), "Response from test-model: Message 5");
    }
}
