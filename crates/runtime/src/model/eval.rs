/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::{
    component::validate_identifier,
    datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA},
};

use super::{eval_scorer::Scorer, DataFusion};
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
use llms::chat::Chat;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{ResultExt, Snafu};
use spicepod::component::eval::Eval;
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to query eval dataset '{dataset_name}': {source}. Ensure the dataset is available and has the correct schema."))]
    FailedToQueryDataset {
        dataset_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Column '{column}' in eval dataset '{dataset}' could not be parsed: {source}."
    ))]
    FailedToParseColumn {
        column: String,
        dataset: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "During evaluation '{eval_name}', an error occured when running the model: {source}"
    ))]
    FailedToRunModel {
        eval_name: String,
        source: OpenAIError,
    },

    #[snafu(display("Scorer '{scorer_name}' needed for eval '{eval_name}' is not available. Ensure '{scorer_name}' is defined in the spicepod and has been sucessfully loaded."))]
    EvalScorerUnavailable {
        eval_name: String,
        scorer_name: String,
    },

    #[snafu(display("Failed to parse the input column from the eval dataset because {reason}. Check that the values in the input column are of valid eval format."))]
    InvalidInputFormat { reason: String },

    #[snafu(display("Failed to parse the output column from the eval dataset because {reason}. Check that the values in the output column are of valid eval format."))]
    InvalidOutputFormat { reason: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The possible representations of inputs into a model evaluation, at varying levels of detail for a [`CreateChatCompletionRequest`].
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetInput {
    Messages(Vec<ChatCompletionRequestMessage>),
    UserInput(String),
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
    pub fn from_raw(s: &str) -> Self {
        match serde_json::from_str(s) {
            Ok(m) => Self::Messages(m),
            Err(_) => Self::UserInput(s.to_string()),
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
                    reason: "must be a string or list, but was neither".to_string(),
                })?;

        let mut result = Vec::with_capacity(list_arr.len());
        for i in 0..list_arr.len() {
            if list_arr.is_null(i) {
                return Err(Error::InvalidInputFormat {
                    reason: "elements cannot be null".to_string(),
                });
            }
            let arr = list_arr.value(i);
            let struct_arr = arr.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                Error::InvalidInputFormat {
                    reason: "must be a string or list, but was neither".to_string(),
                }
            })?;

            let json_value = rb_to_json_value(&RecordBatch::from(struct_arr)).map_err(|e| {
                Error::InvalidInputFormat {
                    reason: format!("could not convert input format into JSON representation: {e}"),
                }
            })?;

            match Self::try_from_value(json_value) {
                Ok(Some(v)) => result.push(v),
                Ok(None) => {
                    return Err(Error::InvalidInputFormat { reason: "could not convert valid list-type input element into a known model input format".to_string() });
                }
                Err(e) => {
                    return Err(Error::InvalidInputFormat { reason: format!("could not convert JSON format of input  into a known model input format: {e}") });
                }
            };
        }

        Ok(result)
    }
}

/// The possible representations of the correct/expected outputs from a [`Chat::chat_request`]  at varying levels of detail for a [`ChatCompletionResponse`].
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetOutput {
    Choices(Vec<ChatChoice>),
    AssistantResponse(String),
}

impl DatasetOutput {
    pub fn from_raw(s: &str) -> Self {
        match serde_json::from_str(s) {
            Ok(m) => Self::Choices(m),
            Err(_) => Self::AssistantResponse(s.to_string()),
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
                    reason: "must be a string or list, but was neither".to_string(),
                })?;

        let mut result = Vec::with_capacity(list_arr.len());
        for i in 0..list_arr.len() {
            if list_arr.is_null(i) {
                return Err(Error::InvalidOutputFormat {
                    reason: "elements cannot be null".to_string(),
                });
            }
            let arr = list_arr.value(i);
            let struct_arr = arr.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                Error::InvalidOutputFormat {
                    reason: "must be a string or list, but was neither".to_string(),
                }
            })?;

            let json_value = rb_to_json_value(&RecordBatch::from(struct_arr)).map_err(|e| {
                Error::InvalidOutputFormat {
                    reason: format!("could not convert element into JSON representation: {e}"),
                }
            })?;

            match Self::try_from_value(json_value) {
                Ok(Some(v)) => result.push(v),
                Ok(None) => {
                    return Err(Error::InvalidOutputFormat { reason: "could not convert valid list-type elements into a known model output format".to_string() });
                }
                Err(e) => {
                    return Err(Error::InvalidOutputFormat { reason: format!("could not convert JSON format of an element into a known model output format: {e}") });
                }
            };
        }

        Ok(result)
    }
}

#[allow(clippy::implicit_hasher)]
pub async fn run_eval(
    eval: &Eval,
    df: Arc<DataFusion>,
    model: &dyn Chat,
    scorers: &HashMap<String, Arc<dyn Scorer>>,
) -> Result<HashMap<String, Vec<(String, f32)>>> {
    let Eval {
        name: eval_name,
        scorers: scorer_names,
        dataset: dataset_str,
        ..
    } = eval;

    let mut scorers_subset = HashMap::with_capacity(scorer_names.len());
    for name in scorer_names {
        let Some(scorer) = scorers.get(name) else {
            return Err(Error::EvalScorerUnavailable {
                scorer_name: name.clone(),
                eval_name: eval_name.clone(),
            });
        };
        scorers_subset.insert(name, scorer);
    }

    validate_identifier(dataset_str)
        .boxed()
        .context(FailedToQueryDatasetSnafu {
            dataset_name: dataset_str.to_string(),
        })?;
    let dataset =
        TableReference::parse_str(dataset_str).resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);
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
    let input: Vec<&DatasetInput> = inputs.iter().flatten().collect();

    tracing::debug!(
        "Eval '{eval_name}' dataset '{dataset_str}' input (first): {:?}",
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
    let ideal: Vec<&DatasetOutput> = ideals.iter().flatten().collect();

    tracing::debug!(
        "Eval '{eval_name}' dataset '{dataset_str}' ideal (first): {:?}",
        ideal.first()
    );

    let actual: Vec<DatasetOutput> = if let Some(first_ideal) = ideal.first() {
        run_model(model, &input, first_ideal)
            .await
            .context(FailedToRunModelSnafu { eval_name })?
    } else {
        // Not error, no data in dataset
        vec![]
    };
    tracing::debug!(
        "Eval '{eval_name}' dataset '{dataset_str}' actual (first): {:?}",
        actual.first()
    );

    let mut aggregate: HashMap<String, Vec<f32>> = HashMap::with_capacity(actual.len());
    for ((actual, input), ideal) in actual.iter().zip(input.iter()).zip(ideal.iter()) {
        for (name, scorer) in &scorers_subset {
            let s = scorer.score(input, actual, ideal).await;
            if let Some(scorer_results) = aggregate.get_mut(*name) {
                scorer_results.push(s);
            } else {
                aggregate.insert((*name).to_string(), vec![s]);
            };
        }
    }

    Ok(scorers_subset
        .iter()
        .map(|(name, scorer)| ((*name).clone(), scorer.metrics(&aggregate[*name])))
        .collect())
}

/// Return format of [`DatasetOutput`] determined by `output_format`. `output_format` can be empty, is only used for its enum type.
async fn run_model(
    model: &dyn Chat,
    inputs: &[&DatasetInput],
    output_format: &DatasetOutput,
) -> Result<Vec<DatasetOutput>, OpenAIError> {
    let mut outputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let req: CreateChatCompletionRequest = (*input).try_into()?;
        let choices = model.chat_request(req).await?.choices;
        let output = match output_format {
            DatasetOutput::AssistantResponse(_) => DatasetOutput::AssistantResponse(
                choices
                    .first()
                    .and_then(|c| c.message.content.clone())
                    .unwrap_or_default(),
            ),
            DatasetOutput::Choices(_) => DatasetOutput::Choices(choices),
        };
        outputs.push(output);
    }
    Ok(outputs)
}

fn rb_to_json_value(data: &RecordBatch) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    writer.write_batches(&[data]).boxed()?;
    writer.finish().boxed()?;

    serde_json::from_str(String::from_utf8(writer.into_inner()).boxed()?.as_str()).boxed()
}
