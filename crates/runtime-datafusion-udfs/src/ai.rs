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
use async_openai::error::ApiError;
use async_openai::types::{
    ChatChoice, ChatCompletionRequestSystemMessageArgs, ChatCompletionResponseMessage,
    CompletionUsage, CreateChatCompletionRequest, CreateChatCompletionRequestArgs,
    CreateChatCompletionResponse, FinishReason, Role,
};
use async_trait::async_trait;
use datafusion::common::cast::as_string_array;
use datafusion::common::utils::take_function_args;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{DocSection, Documentation, ScalarFunctionArgs};
use datafusion::scalar::ScalarValue;
use datafusion::{
    common::{Result as DataFusionResult, exec_err, not_impl_err},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use llms::chat::Chat;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::RwLock;
use tracing::Instrument;

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
        Volatility::Volatile, // Changed to Volatile for async operations
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
        not_impl_err!("AI UDF can only be called from async contexts")
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
        _config: &datafusion::config::ConfigOptions,
    ) -> DataFusionResult<ArrayRef> {
        // Capture the current tracing context for direct parent-child relationships
        let parent_span = tracing::Span::current();

        if args.args.is_empty() || args.args.len() > 2 {
            return exec_err!(
                "{AI_UDF_NAME} expects one or two arguments: message and optional model_name"
            );
        }

        let model_name = if args.args.len() == 2 {
            let model_arg = &args.args[1];
            match model_arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(model_name))) => model_name.clone(),
                _ => {
                    return exec_err!("{AI_UDF_NAME} unsupported model parameter: {model_arg}");
                }
            }
        } else {
            self.get_default_model_name().await?
        };

        let model_store = self.model_store.read().await;
        let Some(model) = model_store.get(&model_name) else {
            return exec_err!("{AI_UDF_NAME} cannot find model '{model_name}'");
        };

        // Convert arguments to arrays for consistency
        let args_arrays = ColumnarValue::values_to_arrays(&args.args)?;

        match args_arrays.len() {
            1 => {
                let [message_array] = take_function_args(self.name(), args_arrays)?;
                self.process_messages(Arc::clone(model), message_array, &model_name)
                    .await
            }
            2 => {
                let [message_array, _model_array] = take_function_args(self.name(), args_arrays)?;
                self.process_messages(Arc::clone(model), message_array, &model_name)
                    .await
            }
            _ => exec_err!("{AI_UDF_NAME} unexpected number of arguments"),
        }
    }
}

impl Ai {
    async fn process_messages(
        &self,
        model: Arc<dyn Chat>,
        message_array: ArrayRef,
        model_name: &str,
    ) -> DataFusionResult<ArrayRef> {
        let message_array = as_string_array(&message_array)?;
        let mut results = Vec::with_capacity(message_array.len());

        for message_opt in message_array.iter() {
            let result = match message_opt {
                Some(message) => {
                    // Create span - the tracing framework automatically makes it a child of the current span (sql_query)
                    let span = tracing::span!(
                        target: "task_history",
                        tracing::Level::INFO,
                        "ai",
                        model = %model_name,
                        input = %message
                    );

                    // Create a proper chat completion request to get usage information
                    let chat_request = CreateChatCompletionRequestArgs::default()
                        .model(model_name.to_string())
                        .messages(vec![
                            ChatCompletionRequestSystemMessageArgs::default()
                                .content(message.to_string())
                                .build()
                                .map_err(|e| DataFusionError::External(Box::new(e)))?
                                .into(),
                        ])
                        .build()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    // Execute the AI call within the span context using .instrument()
                    let ai_result = model
                        .chat_request(chat_request)
                        .instrument(span.clone())
                        .await;

                    match ai_result {
                        Ok(response) => {
                            // Log token usage information if available
                            if let Some(usage) = &response.usage {
                                tracing::info!(
                                    target: "task_history",
                                    parent: &span,
                                    input_tokens = %usage.prompt_tokens,
                                    output_tokens = %usage.completion_tokens,
                                    total_tokens = %usage.total_tokens,
                                    "labels"
                                );
                            }

                            // Extract the response text
                            let response_text = response
                                .choices
                                .first()
                                .and_then(|choice| choice.message.content.clone());

                            match response_text {
                                Some(text) => {
                                    tracing::info!(target: "task_history", parent: &span, captured_output = %text, "labels");
                                    Some(text)
                                }
                                None => {
                                    tracing::info!(target: "task_history", parent: &span, captured_output = "", "labels");
                                    None
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(target: "task_history", parent: &span, "{e}");
                            return Err(DataFusionError::External(Box::new(e)));
                        }
                    }
                }
                None => None,
            };
            results.push(result);
        }

        Ok(Arc::new(StringArray::from(results)) as ArrayRef)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use datafusion::logical_expr::{ScalarUDFImpl, Volatility};
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
                    _ => None,
                })
                .unwrap_or_else(|| "".to_string());

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

        let return_type = udf.return_type(&[DataType::Utf8]).unwrap();
        assert_eq!(return_type, DataType::Utf8);
    }

    #[tokio::test]
    async fn test_default_model_selection() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        let default_model = udf.get_default_model_name().await.unwrap();
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
                .unwrap_err()
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
                .unwrap_err()
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

        let docs = udf.documentation().unwrap();
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
        let return_type1 = udf.return_type(&[DataType::Utf8]).unwrap();
        assert_eq!(return_type1, DataType::Utf8);

        // Test with two Utf8 arguments
        let return_type2 = udf.return_type(&[DataType::Utf8, DataType::Utf8]).unwrap();
        assert_eq!(return_type2, DataType::Utf8);

        // Test with LargeUtf8
        let return_type3 = udf.return_type(&[DataType::LargeUtf8]).unwrap();
        assert_eq!(return_type3, DataType::Utf8);
    }

    #[test]
    fn test_non_async_invoke_with_args_error() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store);

        use arrow_schema::Field;
        use datafusion::logical_expr::ScalarFunctionArgs;

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
                .unwrap_err()
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
        let model = model_store_guard.get("test-model").unwrap();

        let messages = Arc::new(arrow::array::StringArray::from(vec![Some("Hello")]));
        let result = udf
            .process_messages(Arc::clone(model), messages, "test-model")
            .await
            .unwrap();

        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 1);
        assert_eq!(string_array.value(0), "Response from test-model: Hello");
    }

    #[tokio::test]
    async fn test_process_multiple_messages() {
        let model_store = create_test_model_store();
        let udf = Ai::new(model_store.clone());

        let model_store_guard = model_store.read().await;
        let model = model_store_guard.get("test-model").unwrap();

        let messages = Arc::new(arrow::array::StringArray::from(vec![
            Some("Hello"),
            Some("How are you?"),
            Some("Goodbye"),
        ]));
        let result = udf
            .process_messages(Arc::clone(model), messages, "test-model")
            .await
            .unwrap();

        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
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
        let model = model_store_guard.get("test-model").unwrap();

        let messages = Arc::new(arrow::array::StringArray::from(vec![
            Some("Hello"),
            None,
            Some("Goodbye"),
        ]));
        let result = udf
            .process_messages(Arc::clone(model), messages, "test-model")
            .await
            .unwrap();

        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
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
        let model = model_store_guard.get("error-model").unwrap();

        let messages = Arc::new(arrow::array::StringArray::from(vec![Some("Hello")]));
        let result = udf
            .process_messages(Arc::clone(model), messages, "error-model")
            .await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Mock error for testing")
        );
    }

    #[tokio::test]
    async fn test_process_messages_with_null_response() {
        let model_store = create_multi_model_store();
        let udf = Ai::new(model_store.clone());

        let model_store_guard = model_store.read().await;
        let model = model_store_guard.get("null-model").unwrap();

        let messages = Arc::new(arrow::array::StringArray::from(vec![Some("Hello")]));
        let result = udf
            .process_messages(Arc::clone(model), messages, "null-model")
            .await
            .unwrap();

        let string_array = result
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
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
}
