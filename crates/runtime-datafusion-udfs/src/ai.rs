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
use async_openai::types::chat::{
    ChatChoice, ChatCompletionResponseMessage, CompletionUsage, CreateChatCompletionResponse,
    CreateChatCompletionStreamResponse, FinishReason, Role,
};
use async_openai::types::chat::{
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
use runtime_request_context::{AsyncMarker, RequestContext};
use tracing::{Instrument, Level};

use async_trait::async_trait;
use llms::chat::Chat;

use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use tokio::sync::RwLock;

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
    // store a pointer to use for Hash/Eq since UDTF impls require this trait bound but we cannot feasibly make `RwLock<ChatModelStore>` implement them.
    ptr: u64,
}

impl std::fmt::Debug for Ai {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ai")
            .field("model_store", &"<ChatModelStore>")
            .finish()
    }
}

impl PartialEq for Ai {
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }
}

impl Eq for Ai {}

impl Hash for Ai {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ptr.hash(state);
    }
}

impl Ai {
    #[must_use]
    pub fn new(model_store: Arc<RwLock<ChatModelStore>>) -> Self {
        let ptr = Arc::as_ptr(&model_store).addr() as u64;
        Self { model_store, ptr }
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
    ) -> DataFusionResult<ColumnarValue> {
        // Start timing for explain analyze metrics
        let start_time = Instant::now();

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
        let max_parallelism = args.config_options.execution.target_partitions;

        // Format the input to show the full UDF call: ai('message', 'model')
        let input = if args.args.len() == 2 {
            format!(
                "ai({}, {})",
                Self::format_arg(&args.args[0]),
                Self::format_arg(&args.args[1])
            )
        } else {
            format!("ai({})", Self::format_arg(&args.args[0]))
        };

        // Create the 'ai' span that will contain all model_call operations
        // model_call internally emits ai_completion spans to task_history
        // Hierarchy: sql_query → ai → model_call (which emits ai_completion)
        let ai_span = tracing::span!(
            target: "task_history",
            Level::INFO,
            "ai",
            input = %input,
            model = %model_name,
            rows = %args.number_rows
        );

        let result = self
            .process_messages(
                Arc::clone(model),
                &model_name,
                message_array,
                max_parallelism,
            )
            .instrument(ai_span)
            .await;

        // Emit timing metrics for explain analyze
        let elapsed = start_time.elapsed();
        #[expect(clippy::cast_possible_truncation)]
        let elapsed_compute_ns = elapsed.as_nanos() as u64;

        // Log metrics in a format consistent with DataFusion explain analyze
        tracing::debug!(
            target: "datafusion::physical_plan::metrics",
            elapsed_compute = elapsed_compute_ns,
            rows_produced = args.number_rows,
            "ai UDF execution metrics"
        );

        Ok(ColumnarValue::from(result?))
    }
}

impl Ai {
    /// Formats a `ColumnarValue` argument as SQL syntax for tracing
    fn format_arg(arg: &ColumnarValue) -> String {
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => format!("'{s}'"),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => "NULL".to_string(),
            ColumnarValue::Array(_) => "<array>".to_string(),
            ColumnarValue::Scalar(_) => format!("{arg:?}"),
        }
    }

    async fn call_model(
        model: &Arc<dyn Chat>,
        _model_name: &str,
        message: &str,
        row_index: usize,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Sync + Send>> {
        use std::time::Instant;

        // Security: Validate message size before processing
        if message.len() > MAX_MESSAGE_SIZE {
            return Err(format!(
                "Message size ({} bytes) exceeds maximum allowed size ({} bytes)",
                message.len(),
                MAX_MESSAGE_SIZE
            )
            .into());
        }

        let model_call_start = Instant::now();
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
                        include_usage: Some(true),
                        include_obfuscation: None,
                    })
                    .build()?,
            )
            .await?;

        // Performance: Pre-allocate with estimated size to reduce reallocations
        let mut complete_response = String::with_capacity(512);
        let max_response_size = MAX_MESSAGE_SIZE * 2;

        // Performance: Process stream chunks efficiently with cancellation support
        while let Some(chunk_result) = stream.next().await {
            // Yield to allow tokio to cancel this task if needed (e.g., query timeout)
            tokio::task::yield_now().await;

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

        let model_call_elapsed = model_call_start.elapsed();

        // Emit per-row timing metrics for explain analyze
        #[expect(clippy::cast_possible_truncation)]
        let elapsed_ns = model_call_elapsed.as_nanos() as u64;
        tracing::debug!(
            target: "datafusion::physical_plan::metrics",
            row = row_index,
            elapsed_ns,
            response_len = complete_response.len(),
            "ai model call completed"
        );

        Ok(if complete_response.is_empty() {
            None
        } else {
            Some(complete_response)
        })
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
        use futures::stream::{self, StreamExt};

        let array_len = message_array.len();

        // Performance: Use configured parallelism from DataFusion config (target_partitions)
        // Limit to batch size to avoid over-spawning
        let parallelism = std::cmp::min(max_parallelism, array_len);

        // Collect messages into owned strings to avoid lifetime issues with async
        let messages: Vec<(usize, Option<String>)> = message_array
            .iter()
            .enumerate()
            .map(|(idx, msg_opt)| (idx, msg_opt.map(std::string::ToString::to_string)))
            .collect();

        // Clone model_name once outside the iterator to avoid repeated allocations
        let model_name_str = model_name.to_string();

        let ctx = RequestContext::current(AsyncMarker::new().await);
        let results: Result<Vec<(usize, Option<String>)>, DataFusionError> = stream::iter(messages)
            .map(|(row_index, message_str)| {
                let model = Arc::clone(model);
                let model_name_str = model_name_str.clone();

                Arc::clone(&ctx).scope(async move {
                    // Yield to allow tokio to cancel this task if needed (e.g., query timeout or user cancellation)
                    tokio::task::yield_now().await;

                    let result = if let Some(message) = message_str {
                        // call_model internally calls chat_stream, which emits ai_completion spans
                        // Hierarchy: sql_query → ai → model_call (emits ai_completion to task_history)
                        match Self::call_model(&model, &model_name_str, &message, row_index).await {
                            Ok(Some(result)) => Ok(Some(result)),
                            Ok(None) => {
                                tracing::debug!(
                                    "AI model returned empty response for row {}",
                                    row_index
                                );
                                Ok(None)
                            }
                            Err(e) => {
                                tracing::error!("AI model error for row {}: {}", row_index, e);
                                Err(DataFusionError::External(e))
                            }
                        }
                    } else {
                        Ok::<Option<String>, DataFusionError>(None)
                    };
                    result.map(|r| (row_index, r))
                })
            })
            .buffer_unordered(parallelism)
            .collect::<Vec<Result<(usize, Option<String>), DataFusionError>>>()
            .await
            .into_iter()
            .collect();

        let mut results = results?;

        // Restore original order by sorting by row_index
        results.sort_by_key(|(idx, _)| *idx);

        // Extract just the results, now in the correct order
        let ordered_results: Vec<Option<String>> =
            results.into_iter().map(|(_, result)| result).collect();

        // debug assertion only
        #[expect(clippy::disallowed_macros, clippy::allow_attributes)]
        #[allow(unfulfilled_lint_expectations)]
        {
            debug_assert_eq!(
                ordered_results.len(),
                array_len,
                "Result array length must match input array length"
            );
        }

        Ok(Arc::new(StringArray::from(ordered_results)) as ArrayRef)
    }
}

#[cfg(test)]
// Allow various lints in test code for simplicity and readability.
// Test code prioritizes clarity over strict lint compliance.
#[expect(clippy::clone_on_ref_ptr, clippy::uninlined_format_args)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use async_openai::types::chat::{
        ChatChoiceStream, ChatCompletionRequestMessage, ChatCompletionRequestSystemMessageContent,
        ChatCompletionRequestUserMessageContent, ChatCompletionResponseStream,
        ChatCompletionStreamResponseDelta, CreateChatCompletionRequest,
    };
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDFImpl, Volatility};
    use std::collections::HashMap;
    use std::sync::Arc;
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
                    ChatCompletionRequestMessage::System(sys_msg) => match &sys_msg.content {
                        ChatCompletionRequestSystemMessageContent::Text(text) => Some(text.clone()),
                        ChatCompletionRequestSystemMessageContent::Array(_) => {
                            Some("Array content".to_string())
                        }
                    },
                    ChatCompletionRequestMessage::User(user_msg) => match &user_msg.content {
                        ChatCompletionRequestUserMessageContent::Text(text) => Some(text.clone()),
                        ChatCompletionRequestUserMessageContent::Array(_) => {
                            Some("Array content".to_string())
                        }
                    },
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
                    ChatCompletionRequestMessage::System(sys_msg) => match &sys_msg.content {
                        ChatCompletionRequestSystemMessageContent::Text(text) => Some(text.clone()),
                        ChatCompletionRequestSystemMessageContent::Array(_) => {
                            Some("Array content".to_string())
                        }
                    },
                    ChatCompletionRequestMessage::User(user_msg) => match &user_msg.content {
                        ChatCompletionRequestUserMessageContent::Text(text) => Some(text.clone()),
                        ChatCompletionRequestUserMessageContent::Array(_) => {
                            Some("Array content".to_string())
                        }
                    },
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
                        #[expect(deprecated)]
                        function_call: None,
                        tool_calls: None,
                        refusal: None,
                        audio: None,
                        annotations: None,
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
                #[expect(deprecated)]
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
            config_options: Arc::new(ConfigOptions::default()),
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
                    choices: vec![ChatChoiceStream {
                        index: 0,
                        delta: ChatCompletionStreamResponseDelta {
                            content: None, // Empty content
                            role: Some(Role::Assistant),
                            tool_calls: None,
                            refusal: None,
                            #[expect(deprecated)]
                            function_call: None,
                        },
                        finish_reason: None,
                        logprobs: None,
                    }],
                    usage: None,
                    #[expect(deprecated)]
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
                    #[expect(deprecated)]
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
                        #[expect(deprecated)]
                        function_call: None,
                        tool_calls: None,
                        refusal: None,
                        audio: None,
                        annotations: None,
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
                #[expect(deprecated)]
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
            config_options: Arc::new(ConfigOptions::default()),
        };

        // This should not fail with "all columns in a record batch must have the same length"
        let result = udf.invoke_async_with_args(args).await;

        assert!(
            result.is_ok(),
            "invoke_async_with_args should handle columnar message + scalar model: {:?}",
            result.err()
        );

        let response_array = result.expect("should get result");
        let ColumnarValue::Array(response_array) = response_array else {
            panic!("expected Array result");
        };

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
            config_options: Arc::new(ConfigOptions::default()),
        };

        // This should not fail with "all columns in a record batch must have the same length"
        let result = udf.invoke_async_with_args(args).await;

        assert!(
            result.is_ok(),
            "invoke_async_with_args should handle scalar message + scalar model for multiple rows: {:?}",
            result.err()
        );

        let response_array = result.expect("should get result");
        let ColumnarValue::Array(response_array) = response_array else {
            panic!("expected Array result");
        };

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
    async fn test_parallel_processing_with_multiple_messages() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store.clone());

        let model_store_guard = model_store.read().await;
        let model = model_store_guard
            .get("test-model")
            .expect("should get test-model");

        let messages = Arc::new(arrow::array::StringArray::from(vec![
            Some("Message 1"),
            Some("Message 2"),
            Some("Message 3"),
            Some("Message 4"),
            Some("Message 5"),
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
            .expect("should process messages in parallel");

        let string_array = result
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
    async fn test_cancellation_support() {
        use tokio::time::{Duration, sleep, timeout};

        // Mock Chat that simulates long-running operations
        struct SlowMockChat;

        #[async_trait]
        impl Chat for SlowMockChat {
            fn as_sql(&self) -> Option<&dyn llms::chat::nsql::SqlGeneration> {
                None
            }

            async fn run(&self, _prompt: String) -> llms::chat::Result<Option<String>> {
                sleep(Duration::from_secs(10)).await;
                Ok(Some("Should be cancelled".to_string()))
            }

            async fn chat_stream(
                &self,
                _req: CreateChatCompletionRequest,
            ) -> Result<ChatCompletionResponseStream, async_openai::error::OpenAIError>
            {
                use async_stream::stream;

                let stream = stream! {
                    // Yield a chunk
                    yield Ok(CreateChatCompletionStreamResponse {
                        id: "slow-id".to_string(),
                        model: "slow-model".to_string(),
                        object: "chat.completion.chunk".to_string(),
                        created: 0,
                        choices: vec![ChatChoiceStream {
                            index: 0,
                            delta: ChatCompletionStreamResponseDelta {
                                content: Some("Starting...".to_string()),
                                role: Some(Role::Assistant),
                                tool_calls: None,
                                refusal: None,
                                #[expect(deprecated)]
                                function_call: None,
                            },
                            finish_reason: None,
                            logprobs: None,
                        }],
                        usage: None,
                        #[expect(deprecated)]
                        system_fingerprint: None,
                        service_tier: None,
                    });

                    // Simulate a long delay between chunks
                    sleep(Duration::from_secs(10)).await;

                    yield Ok(CreateChatCompletionStreamResponse {
                        id: "slow-id".to_string(),
                        model: "slow-model".to_string(),
                        object: "chat.completion.chunk".to_string(),
                        created: 0,
                        choices: vec![],
                        usage: None,
                        #[expect(deprecated)]
                        system_fingerprint: None,
                        service_tier: None,
                    });
                };

                Ok(Box::pin(stream))
            }

            async fn chat_request(
                &self,
                _req: CreateChatCompletionRequest,
            ) -> Result<CreateChatCompletionResponse, async_openai::error::OpenAIError>
            {
                sleep(Duration::from_secs(10)).await;
                Err(async_openai::error::OpenAIError::ApiError(ApiError {
                    message: "Should be cancelled".to_string(),
                    r#type: None,
                    param: None,
                    code: None,
                }))
            }
        }

        let mut store = HashMap::new();
        store.insert(
            "slow-model".to_string(),
            Arc::new(SlowMockChat) as Arc<dyn Chat>,
        );
        let model_store = Arc::new(RwLock::new(store));
        let udf = Ai::new(model_store.clone());

        let messages = Arc::new(StringArray::from(vec![
            Some("Message 1"),
            Some("Message 2"),
            Some("Message 3"),
        ]));

        let model_store_read = model_store.read().await;
        let model = model_store_read
            .get("slow-model")
            .expect("should get slow-model");

        // Start the processing task with a timeout
        let result = timeout(
            Duration::from_millis(500), // Short timeout to trigger cancellation
            udf.process_messages(
                Arc::clone(model),
                "slow-model",
                messages,
                std::thread::available_parallelism()
                    .map(std::num::NonZero::get)
                    .unwrap_or(4),
            ),
        )
        .await;

        // Should timeout, demonstrating that long-running operations can be cancelled
        assert!(
            result.is_err(),
            "Task should timeout (which is a form of cancellation)"
        );
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

    #[tokio::test]
    async fn test_metrics_emission() {
        // This test verifies that the AI UDF executes successfully and emits metrics.
        // The metrics are logged via tracing::debug! with target "datafusion::physical_plan::metrics"
        // In a real environment with proper tracing setup, these would be captured by monitoring systems.

        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        let message_scalar =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Test metrics".to_string())));
        let model_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("test-model".to_string())));

        let args = ScalarFunctionArgs {
            args: vec![message_scalar, model_scalar],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(arrow_schema::Field::new("result", DataType::Utf8, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = udf
            .invoke_async_with_args(args)
            .await
            .expect("UDF should execute successfully");

        // Verify we got a result back
        let ColumnarValue::Array(result) = result else {
            panic!("expected Array result");
        };

        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("should cast to StringArray");

        assert_eq!(string_array.len(), 1);
        assert!(string_array.value(0).contains("Response from test-model"));

        // Note: Metrics are emitted via tracing::debug! calls in invoke_async_with_args
        // and process_single_message_stream. These can be verified by enabling debug logging
        // and checking for events with target "datafusion::physical_plan::metrics"
    }
}
