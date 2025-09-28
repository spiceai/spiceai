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

pub static AI_UDF_NAME: &str = "ai";
pub static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| Documentation {
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
            _ => exec_err!("{AI_UDF_NAME}: Multiple chat models configured. Please specify model name as second argument"),
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
        if args.args.is_empty() || args.args.len() > 2 {
            return exec_err!(
                "{AI_UDF_NAME} expects one or two arguments: message and optional model_name"
            );
        }

        let model_name = if args.args.len() == 2 {
            let model_arg = &args.args[1];
            match model_arg {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(model_name))) => model_name.clone(),
                _ => return exec_err!("{AI_UDF_NAME} unsupported model parameter: {model_arg}"),
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
                self.process_messages(Arc::clone(model), message_array).await
            }
            2 => {
                let [message_array, _model_array] = take_function_args(self.name(), args_arrays)?;
                self.process_messages(Arc::clone(model), message_array).await
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
    ) -> DataFusionResult<ArrayRef> {
        let message_array = as_string_array(&message_array)?;
        let mut results = Vec::with_capacity(message_array.len());

        for message_opt in message_array.iter() {
            let result = match message_opt {
                Some(message) => {
                    match model.run(message.to_string()).await {
                        Ok(Some(response)) => Some(response),
                        Ok(None) => None,
                        Err(e) => return Err(DataFusionError::External(Box::new(e))),
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
    use std::collections::HashMap;
    use tokio::sync::RwLock;
    use datafusion::logical_expr::ScalarUDFImpl;
    use arrow_schema::DataType;
    use std::sync::Arc;

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
            _req: async_openai::types::CreateChatCompletionRequest,
        ) -> Result<async_openai::types::CreateChatCompletionResponse, async_openai::error::OpenAIError> {
            unimplemented!()
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
        assert_eq!(sig.type_signature.len(), 2);
        
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
        assert!(result.unwrap_err().to_string().contains("Multiple chat models configured"));
    }
}