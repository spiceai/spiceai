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

//! [`ScalarUDFImpl`] definitions for embedding function.

use arrow::array::{ListBuilder, PrimitiveBuilder};
use arrow::datatypes::Float32Type;
use arrow_schema::{DataType, Field};
use async_openai::types::EmbeddingInput;
use datafusion::common::cast::as_string_array;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Documentation, ScalarFunctionArgs};
use datafusion::scalar::ScalarValue;
use datafusion::{
    common::{Result as DataFusionResult, exec_err},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::RwLock;

pub static EMBED_UDF_NAME: &str = "embed";
pub static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| Documentation {
    doc_section: Default::default(),
    description: "Generates embeddings for text using a specified embedding model".to_string(),
    syntax_example: "embed(text, model_name)".to_string(),
    sql_example: Some("SELECT embed('hello world', 'potion_2m')".to_string()),
    arguments: Some(vec![
        ("text".to_string(), "The text string to embed.".to_string()),
        (
            "model_name".to_string(),
            "The name of the embedding model to use as defined in the Spicepod.".to_string(),
        ),
    ]),
    alternative_syntax: Some(vec!["embed(['foo', 'bar'], 'potion_2m')".to_string()]),
    related_udfs: None,
});

pub type EmbeddingModelStore = HashMap<String, Arc<dyn llms::embeddings::Embed>>;

#[derive(Debug)]
pub struct Embed {
    signature: Signature,
    model_store: Arc<RwLock<EmbeddingModelStore>>,
}

impl Embed {
    #[must_use]
    pub fn new(model_store: Arc<RwLock<EmbeddingModelStore>>) -> Self {
        Self {
            signature: Signature::one_of(
                // In order of least likely to auto-coerce, via logical_expr docs for OneOf:
                // > Coercion is attempted to match the signatures in order,
                // > and stops after the first success, if any.
                vec![
                    // embed(make_array(a, b, c), model_name)
                    TypeSignature::Exact(vec![
                        DataType::List(Arc::new(Field::new("sentence", DataType::Utf8, true))),
                        DataType::Utf8,
                    ]),
                    // embed(text, model_name)
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                ],
                Volatility::Stable,
            ),
            model_store,
        }
    }

    fn embed_single(
        &self,
        model: &Arc<dyn llms::embeddings::Embed>,
        sentence: &String,
    ) -> DataFusionResult<ColumnarValue> {
        let embedding = model
            .embed_sync(EmbeddingInput::String(sentence.clone()))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let vector_size = embedding[0].len();

        let mut builder = ListBuilder::with_capacity(
            PrimitiveBuilder::<Float32Type>::with_capacity(vector_size),
            1,
        );

        builder.values().append_slice(&embedding[0]);
        builder.append(true);

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn embed_multiple<'a>(
        &self,
        model: &Arc<dyn llms::embeddings::Embed>,
        sentences: impl Iterator<Item = Option<&'a str>>,
    ) -> DataFusionResult<ColumnarValue> {
        let mut builder =
            ListBuilder::new(ListBuilder::new(PrimitiveBuilder::<Float32Type>::new()));

        for maybe_string in sentences {
            let embedded = match maybe_string {
                Some(s) => model
                    .embed_sync(EmbeddingInput::String(s.to_string()))
                    .map_err(|e| DataFusionError::External(Box::new(e)))?,
                None => vec![vec![]],
            };

            builder.values().values().append_slice(&embedded[0]);
            builder.values().append(!embedded[0].is_empty());
        }

        builder.append(true);

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl ScalarUDFImpl for Embed {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        EMBED_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        match &arg_types[0] {
            // Embed single sentence
            DataType::Utf8 => Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Float32,
                true,
            )))),
            // Embed multiple sentences
            DataType::List(_) => Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("embedding", DataType::Float32, true))),
                true,
            )))),
            _ => exec_err!("{EMBED_UDF_NAME}: unsupported arg types {arg_types:?}"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!(
                "{EMBED_UDF_NAME} expects exactly two arguments: text and model_name"
            );
        }

        let text_arg = &args.args[0];
        let model_arg = &args.args[1];

        let model_name = match model_arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(model_name))) => model_name,
            _ => return exec_err!("{EMBED_UDF_NAME} unsupported model parameter: {model_arg}"),
        };

        let model_store = match self.model_store.try_read() {
            Ok(model_store) => model_store,
            _ => return exec_err!("{EMBED_UDF_NAME} cannot read model_store"),
        };

        let model = match model_store.get(model_name) {
            Some(model) => model,
            _ => return exec_err!("{EMBED_UDF_NAME} cannot mount {model_arg}"),
        };

        match text_arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))) => self.embed_single(model, text),
            ColumnarValue::Array(arr) => self.embed_multiple(model, as_string_array(arr)?.iter()),
            ColumnarValue::Scalar(ScalarValue::List(arr)) => {
                self.embed_multiple(model, as_string_array(arr.value(0).as_ref())?.iter())
            }
            unsupported_text_arg => exec_err!("Unsupported text argument: {unsupported_text_arg}"),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&DOCUMENTATION)
    }
}

#[cfg(test)]
mod tests {
    use crate::embed::{Embed, EmbeddingModelStore};
    use arrow_schema::{DataType, Field};
    use datafusion::common::cast::as_list_array;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use llms::model2vec::Model2Vec;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn create_fake_model_store() -> Arc<RwLock<EmbeddingModelStore>> {
        use std::collections::HashMap;

        let mut store = HashMap::new();
        store.insert(
            "potion_2m".to_string(),
            Arc::new(
                Model2Vec::from_params(
                    "minishlab/potion-base-2M",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap(),
            ) as Arc<dyn llms::embeddings::Embed>,
        );
        Arc::new(RwLock::new(store))
    }

    fn create_scalar_function_args(
        udf: &Embed,
        text_arg: ColumnarValue,
        model_arg: ColumnarValue,
        number_rows: usize,
    ) -> ScalarFunctionArgs {
        use datafusion::logical_expr::ColumnarValue;
        use datafusion::logical_expr::ScalarFunctionArgs;

        let arg_fields = match &text_arg {
            ColumnarValue::Scalar(_) => vec![
                Arc::new(Field::new("text", DataType::Utf8, false)),
                Arc::new(Field::new("model", DataType::Utf8, false)),
            ],
            ColumnarValue::Array(_) => vec![
                Arc::new(Field::new(
                    "texts",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    false,
                )),
                Arc::new(Field::new("model", DataType::Utf8, false)),
            ],
        };

        let return_type = udf
            .return_type(
                &arg_fields
                    .iter()
                    .map(|f| f.data_type().clone())
                    .collect::<Vec<_>>(),
            )
            .expect("Must determine return type");

        ScalarFunctionArgs {
            args: vec![text_arg, model_arg],
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("embed", return_type, false)),
        }
    }

    #[test]
    fn test_embed_single_sentence() {
        use arrow::array::{Array, ListArray};
        use datafusion::logical_expr::ColumnarValue;
        use datafusion::scalar::ScalarValue;

        let fake_model_store = create_fake_model_store();
        let udf = Embed::new(fake_model_store);

        let args = create_scalar_function_args(
            &udf,
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("hello world".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("potion_2m".to_string()))),
            1,
        );

        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(list_arr.len(), 1, "Expected 1 embedding vector");

                let inner_list = list_arr.value(0);
                let float_arr = inner_list
                    .as_any()
                    .downcast_ref::<arrow::array::Float32Array>()
                    .unwrap();
                assert_eq!(float_arr.len(), 64, "Expected embedding dimension of 64");
            }
            _ => panic!("Expected Array result for single sentence"),
        }
    }

    #[test]
    fn test_embed_multiple_sentences() {
        use arrow::array::{Array, ListArray, StringArray};
        use datafusion::logical_expr::ColumnarValue;
        use datafusion::scalar::ScalarValue;

        let fake_model_store = create_fake_model_store();
        let udf = Embed::new(fake_model_store);

        let args = create_scalar_function_args(
            &udf,
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("hello"),
                Some("world"),
            ]))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("potion_2m".to_string()))),
            2,
        );

        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(list_arr.len(), 1, "Expected 1 outer list");

                let inner_list = list_arr.value(0);
                let inner_list_arr = as_list_array(&inner_list).unwrap();
                assert_eq!(inner_list_arr.len(), 2, "Expected 2 embedding vectors");

                assert!(
                    inner_list_arr.iter().flatten().all(|a| a
                        .as_any()
                        .downcast_ref::<arrow::array::Float32Array>()
                        .unwrap()
                        .len()
                        == 64),
                    "All embedding vectors should be dimension 64"
                );
            }
            _ => panic!("Expected Array result for multiple sentences"),
        }
    }

    #[test]
    fn test_embed_multiple_sentences_with_null() {
        use arrow::array::{Array, ListArray, StringArray};
        use datafusion::logical_expr::ColumnarValue;
        use datafusion::scalar::ScalarValue;

        let fake_model_store = create_fake_model_store();
        let udf = Embed::new(fake_model_store);

        let args = create_scalar_function_args(
            &udf,
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("hello"),
                None,
                Some("world"),
            ]))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("potion_2m".to_string()))),
            3,
        );

        let result = udf.invoke_with_args(args).unwrap();
        match result {
            ColumnarValue::Array(arr) => {
                let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap();
                assert_eq!(list_arr.len(), 1, "Expected 1 outer list");

                let inner_list = list_arr.value(0);
                let inner_list_arr = as_list_array(&inner_list).unwrap();
                assert_eq!(
                    inner_list_arr.len(),
                    3,
                    "Expected 3 entries (2 embeddings + 1 null)"
                );

                assert!(inner_list_arr.is_null(1), "Expected null at index 1");

                assert!(
                    inner_list_arr.iter().flatten().all(|a| a
                        .as_any()
                        .downcast_ref::<arrow::array::Float32Array>()
                        .unwrap()
                        .len()
                        == 64),
                    "All non-null embedding vectors should be dimension 64"
                );
            }
            _ => panic!("Expected Array result for multiple sentences with null"),
        }
    }
}
