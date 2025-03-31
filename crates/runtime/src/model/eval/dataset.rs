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

use std::sync::Arc;

use crate::model::eval::FailedToParseColumnSnafu;
use crate::{
    component::validate_identifier,
    datafusion::{DataFusion, SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA},
    model::eval::FailedToQueryDatasetSnafu,
};

use super::{Error, Result};
use arrow::array::{
    Array, ArrayRef, ListArray, RecordBatch, StringArray, StringViewArray, StructArray,
};
use async_openai::{
    error::OpenAIError,
    types::{
        ChatChoice, ChatCompletionRequestMessage, ChatCompletionRequestUserMessageArgs,
        CreateChatCompletionRequest, CreateChatCompletionRequestArgs,
    },
};

use datafusion::sql::TableReference;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;
use spicepod::component::eval::Eval;

/// The possible representations of inputs into a model evaluation, at varying levels of detail for a [`CreateChatCompletionRequest`].
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetInput {
    Messages(Vec<ChatCompletionRequestMessage>),
    UserInput(String),
}

/// The possible representations of the correct/expected outputs from a [`Chat::chat_request`]  at varying levels of detail for a [`ChatCompletionResponse`].
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetOutput {
    Choices(Vec<ChatChoice>),
    AssistantResponse(String),
}

/// Retrieve and prepare the data needed to run a given [`Eval`].
pub async fn get_eval_data(
    df: Arc<DataFusion>,
    eval: &Eval,
) -> Result<(Vec<DatasetInput>, Vec<DatasetOutput>)> {
    validate_identifier(&eval.dataset)
        .boxed()
        .context(FailedToQueryDatasetSnafu {
            dataset_name: eval.dataset.to_string(),
        })?;

    let dataset = TableReference::parse_str(&eval.dataset)
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);

    let ds = df
        .query_builder(format!("SELECT input, ideal FROM {dataset}").as_str())
        .build()
        .run()
        .await
        .boxed()
        .context(FailedToQueryDatasetSnafu {
            dataset_name: dataset.to_string(),
        })?
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .boxed()
        .context(FailedToQueryDatasetSnafu {
            dataset_name: dataset.to_string(),
        })?;

    let (inputs, ideals): (Vec<&ArrayRef>, Vec<&ArrayRef>) =
        ds.iter().map(|rb| (rb.column(0), rb.column(1))).unzip();

    let inputs = inputs
        .iter()
        .map(|a| DatasetInput::try_from_array(a))
        .collect::<Result<Vec<_>, _>>()
        .boxed()
        .context(FailedToParseColumnSnafu {
            column: "input".to_string(),
            dataset: dataset.to_string(),
        })?;
    let input: Vec<DatasetInput> = inputs.into_iter().flatten().collect();

    tracing::debug!(
        "Eval '{}' dataset '{}' input (first): {:?}",
        eval.name.clone(),
        eval.dataset.clone(),
        input.first()
    );

    let ideals = ideals
        .iter()
        .map(|a| DatasetOutput::try_from_array(a))
        .collect::<Result<Vec<_>, _>>()
        .boxed()
        .context(FailedToParseColumnSnafu {
            column: "ideal".to_string(),
            dataset: dataset.to_string(),
        })?;
    let ideal: Vec<DatasetOutput> = ideals.into_iter().flatten().collect();

    tracing::debug!(
        "Eval '{}' dataset '{}' ideal (first): {:?}",
        eval.name.clone(),
        eval.dataset.clone(),
        ideal.first()
    );
    Ok((input, ideal))
}

impl TryFrom<&DatasetInput> for CreateChatCompletionRequest {
    type Error = OpenAIError;

    fn try_from(value: &DatasetInput) -> std::result::Result<Self, Self::Error> {
        match value {
            DatasetInput::Messages(m) => CreateChatCompletionRequestArgs::default()
                .messages(m.clone())
                .build(),
            DatasetInput::UserInput(content) => CreateChatCompletionRequestArgs::default()
                .messages(vec![ChatCompletionRequestUserMessageArgs::default()
                    .content(content.clone())
                    .build()?
                    .into()])
                .build(),
        }
    }
}

impl DatasetInput {
    #[must_use]
    pub fn from_raw(s: &str) -> Self {
        match serde_json::from_str(s) {
            Ok(m) => Self::Messages(m),
            Err(_) => Self::UserInput(s.to_string()),
        }
    }

    pub(crate) fn try_serialize(&self) -> Result<String> {
        match self {
            Self::Messages(m) => serde_json::to_string(m).map_err(|_| Error::InvalidInputFormat {
                reason: "Failed to serialize input messages.".to_string(),
            }),
            Self::UserInput(s) => Ok(s.clone()),
        }
    }

    pub fn try_from_value(v: Value) -> Result<Option<Self>, serde_json::Error> {
        match v {
            Value::String(s) => Ok(Some(Self::UserInput(s.to_string()))),
            Value::Array(values) => {
                let messages = values
                    .into_iter()
                    .map(serde_json::from_value)
                    .collect::<Result<Vec<ChatCompletionRequestMessage>, serde_json::Error>>()?;
                Ok(Some(Self::Messages(messages)))
            }
            v if matches!(v, Value::Object(_)) => (Some(serde_json::from_value(v))).transpose(),
            _ => Ok(None),
        }
    }
    /// Attempt to parse Arrow column values as a string ([`StringArray`] or [`StringViewArray`]), and failing that, as a [`ListArray`], into one of the valid [`DatasetInput`] formats.
    pub(crate) fn try_from_array(arr: &ArrayRef) -> Result<Vec<Self>> {
        // Try String inputs, as [`DatasetInput::UserInput`].
        let from_str_opt: Option<Vec<&str>> = {
            if let Some(arr_str) = arr.as_any().downcast_ref::<StringArray>() {
                Some(
                    arr_str
                        .iter()
                        .map(Option::unwrap_or_default)
                        .collect::<Vec<&str>>(),
                )
            } else {
                arr.as_any()
                    .downcast_ref::<StringViewArray>()
                    .map(|arr_str| {
                        arr_str
                            .iter()
                            .map(Option::unwrap_or_default)
                            .collect::<Vec<&str>>()
                    })
            }
        };
        if let Some(from_str) = from_str_opt {
            return Ok(from_str.into_iter().map(Self::from_raw).collect());
        }

        // Try as [`DatasetInput::Messages`].
        let list_arr =
            arr.as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::InvalidInputFormat {
                    reason: "The input must be a string or list, but was neither.".to_string(),
                })?;

        let mut result = Vec::with_capacity(list_arr.len());
        for i in 0..list_arr.len() {
            if list_arr.is_null(i) {
                return Err(Error::InvalidInputFormat {
                    reason: "Elements of the input list cannot be null.".to_string(),
                });
            }
            let arr = list_arr.value(i);
            let struct_arr = arr.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                Error::InvalidInputFormat {
                    reason: "The input must be a string or list, but was neither.".to_string(),
                }
            })?;

            let json_value = rb_to_json_value(&RecordBatch::from(struct_arr)).map_err(|e| {
                Error::InvalidInputFormatReport {
                    reason: format!(
                        "Failed to convert the input format into JSON representation.\n{e}"
                    ),
                }
            })?;

            match Self::try_from_value(json_value) {
                Ok(Some(v)) => result.push(v),
                Ok(None) => {
                    return Err(Error::InvalidInputFormatReport { reason: "Failed to convert valid list-type input element into a known model input format.".to_string() });
                }
                Err(e) => {
                    return Err(Error::InvalidInputFormatReport { reason: format!("Failed to convert JSON format of input into a known model input format.\n{e}") });
                }
            };
        }

        Ok(result)
    }
}

impl DatasetOutput {
    #[must_use]
    pub fn from_raw(s: &str) -> Self {
        match serde_json::from_str(s) {
            Ok(m) => Self::Choices(m),
            Err(_) => Self::AssistantResponse(s.to_string()),
        }
    }

    pub(crate) fn try_serialize(&self) -> Result<String> {
        match self {
            Self::Choices(c) => serde_json::to_string(c).map_err(|_| Error::InvalidOutputFormat {
                reason: "Failed to serialize output choices.".to_string(),
            }),
            Self::AssistantResponse(s) => Ok(s.clone()),
        }
    }

    pub fn try_from_value(v: Value) -> Result<Option<Self>, serde_json::Error> {
        match v {
            Value::String(s) => Ok(Some(Self::AssistantResponse(s.to_string()))),
            Value::Array(values) => {
                let choices = values
                    .into_iter()
                    .map(serde_json::from_value)
                    .collect::<Result<Vec<ChatChoice>, serde_json::Error>>()?;
                Ok(Some(Self::Choices(choices)))
            }
            v if matches!(v, Value::Object(_)) => (Some(serde_json::from_value(v))).transpose(),
            _ => Ok(None),
        }
    }

    pub(crate) fn try_from_array(arr: &ArrayRef) -> Result<Vec<Self>> {
        // Try String inputs, as [`DatasetOutput::AssistantResponse`].
        let from_str_opt: Option<Vec<&str>> = {
            if let Some(arr_str) = arr.as_any().downcast_ref::<StringArray>() {
                Some(
                    arr_str
                        .iter()
                        .map(Option::unwrap_or_default)
                        .collect::<Vec<&str>>(),
                )
            } else {
                arr.as_any()
                    .downcast_ref::<StringViewArray>()
                    .map(|arr_str| {
                        arr_str
                            .iter()
                            .map(Option::unwrap_or_default)
                            .collect::<Vec<&str>>()
                    })
            }
        };
        if let Some(from_str) = from_str_opt {
            return Ok(from_str.into_iter().map(Self::from_raw).collect());
        }

        // Try as [`DatasetOutput::Choices`].
        let list_arr =
            arr.as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::InvalidOutputFormat {
                    reason: "The output must be a string or list, but was neither.".to_string(),
                })?;

        let mut result = Vec::with_capacity(list_arr.len());
        for i in 0..list_arr.len() {
            if list_arr.is_null(i) {
                return Err(Error::InvalidOutputFormat {
                    reason: "Elements of the output list cannot be null.".to_string(),
                });
            }
            let arr = list_arr.value(i);
            let struct_arr = arr.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                Error::InvalidOutputFormat {
                    reason: "The output must be a string or list, but was neither.".to_string(),
                }
            })?;

            let json_value = rb_to_json_value(&RecordBatch::from(struct_arr)).map_err(|e| {
                Error::InvalidOutputFormatReport {
                    reason: format!(
                        "Failed to convert output element into JSON representation.\n{e}"
                    ),
                }
            })?;

            match Self::try_from_value(json_value) {
                Ok(Some(v)) => result.push(v),
                Ok(None) => {
                    return Err(Error::InvalidOutputFormatReport { reason: "Failed to convert valid list-type elements into a known model output format.".to_string() });
                }
                Err(e) => {
                    return Err(Error::InvalidOutputFormatReport { reason: format!("Failed to convert JSON format of an element into a known model output format.\n{e}") });
                }
            };
        }

        Ok(result)
    }
}

/// Convert a [`RecordBatch`] into its JSON [`Value`].
fn rb_to_json_value(data: &RecordBatch) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    writer.write_batches(&[data]).boxed()?;
    writer.finish().boxed()?;

    serde_json::from_str(String::from_utf8(writer.into_inner()).boxed()?.as_str()).boxed()
}
