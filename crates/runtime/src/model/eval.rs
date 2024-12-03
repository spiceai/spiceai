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

use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

use super::DataFusion;
use arrow::array::{
    Array, ArrayRef, Float32Array, RecordBatch, StringArray, StringViewArray, StructArray,
};
use arrow_schema::{ArrowError, Field, Schema};
use async_openai::{
    error::OpenAIError,
    types::{
        ChatChoice, ChatCompletionRequestMessage, ChatCompletionRequestUserMessageArgs,
        CreateChatCompletionRequest, CreateChatCompletionRequestArgs,
    },
};
use async_trait::async_trait;
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
    #[snafu(display("Failed to query eval dataset: {source}"))]
    FailedToQueryDataset {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Column '{column}' in eval dataset '{dataset}' could not be parsed: {source}"
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

    #[snafu(display("Scorer '{scorer_name}' needed for eval '{eval_name}' is not available"))]
    EvalScorerUnavailable {
        eval_name: String,
        scorer_name: String,
    },

    #[snafu(display("Failed to create score outputs: {source}"))]
    FailedToCreateScoreOutputs { source: ArrowError },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait Scorer: Sync + Send {
    async fn score(
        &self,
        input: &DatasetInput,
        actual: &DatasetOutput,
        ideal: &DatasetOutput,
    ) -> f32;
}

#[must_use]
pub fn builtin_scorer() -> Vec<(String, Arc<dyn Scorer>)> {
    vec![]
}

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
                let z = values
                    .into_iter()
                    .map(serde_json::from_value)
                    .collect::<Result<Vec<ChatCompletionRequestMessage>, serde_json::Error>>();
                z.map(|m| Some(Self::Messages(m)))
            }
            v if matches!(v, Value::Object(_)) => Ok(Some(serde_json::from_value(v)?)),
            _ => Ok(None),
        }
    }
    /// Attempt to parse Arrow column values as a string ([`StringArray`] or [`StringViewArray`]), and failing that, as a [`StructArray`], into one of the valid [`DatasetInput`] formats.
    pub(crate) fn try_from_array(
        arr: &ArrayRef,
    ) -> Result<Vec<Self>, Box<dyn std::error::Error + Send + Sync>> {
        // Try String inputs
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

        // Try [`StructArray`].
        if let Some(struct_arr) = arr.as_any().downcast_ref::<StructArray>() {
            let rb = RecordBatch::from(struct_arr.slice(0, struct_arr.len()));

            let raw = rb_to_json_value(&rb)?
                .into_iter()
                .map(Self::try_from_value)
                .collect::<Result<Vec<Option<Self>>, serde_json::Error>>()
                .boxed()?;

            let raw_count = raw.len();
            let filtered: Vec<Self> = raw.into_iter().flatten().collect();
            if filtered.len() == raw_count {
                Ok(filtered)
            } else {
                Err(Box::<dyn std::error::Error + Send + Sync>::from(
                    "Some values could not be parsed into DatasetInput".to_string(),
                ))
            }
        } else {
            Ok(vec![])
        }
    }
}

/// The possible representations of the correct/expected outputs from a [`Chat::chat_request`]  at varying levels of detail for a [`ChatCompletionResponse`].
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DatasetOutput {
    Messages(Vec<ChatChoice>),
    AssistantResponse(String),
}

impl DatasetOutput {
    pub fn from_raw(s: &str) -> Self {
        match serde_json::from_str(s) {
            Ok(m) => Self::Messages(m),
            Err(_) => Self::AssistantResponse(s.to_string()),
        }
    }

    pub fn try_from_value(v: Value) -> Result<Option<Self>, serde_json::Error> {
        match v {
            Value::String(s) => Ok(Some(Self::AssistantResponse(s.to_string()))),
            Value::Array(values) => {
                let z = values
                    .into_iter()
                    .map(serde_json::from_value)
                    .collect::<Result<Vec<ChatChoice>, serde_json::Error>>();
                z.map(|m| Some(Self::Messages(m)))
            }
            v if matches!(v, Value::Object(_)) => Ok(Some(serde_json::from_value(v)?)),
            _ => Ok(None),
        }
    }

    pub(crate) fn try_from_array(
        arr: &ArrayRef,
    ) -> Result<Vec<Self>, Box<dyn std::error::Error + Send + Sync>> {
        // Try String inputs
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
        if let Some(struct_arr) = arr.as_any().downcast_ref::<StructArray>() {
            let rb = RecordBatch::from(struct_arr.slice(0, struct_arr.len()));

            let raw = rb_to_json_value(&rb)?
                .into_iter()
                .map(Self::try_from_value)
                .collect::<Result<Vec<Option<Self>>, serde_json::Error>>()
                .boxed()?;

            let raw_count = raw.len();
            let filtered: Vec<Self> = raw.into_iter().flatten().collect();
            if filtered.len() == raw_count {
                Ok(filtered)
            } else {
                Err(Box::<dyn std::error::Error + Send + Sync>::from(
                    "Some values could not be parsed into DatasetInput".to_string(),
                ))
            }
        } else {
            Ok(vec![])
        }
    }
}

#[allow(clippy::borrowed_box, clippy::implicit_hasher)]
pub async fn run_eval(
    eval: &Eval,
    df: Arc<DataFusion>,
    model: &Box<dyn Chat>,
    scorers: &HashMap<String, Arc<Box<dyn Scorer>>>,
) -> Result<RecordBatch> {
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

    let dataset =
        TableReference::parse_str(dataset_str).resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA);

    let ds = df
        .query_builder(format!("SELECT input, ideal FROM {dataset}").as_str())
        .build()
        .run()
        .await
        .boxed()
        .context(FailedToQueryDatasetSnafu)?
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .boxed()
        .context(FailedToQueryDatasetSnafu)?;

    let (inputs, ideals): (Vec<&ArrayRef>, Vec<&ArrayRef>) =
        ds.iter().map(|rb| (rb.column(0), rb.column(1))).unzip();

    let inputs2 = inputs
        .iter()
        .map(|a| DatasetInput::try_from_array(a))
        .collect::<Result<Vec<_>, _>>()
        .context(FailedToParseColumnSnafu {
            column: "input".to_string(),
            dataset: dataset.to_string(),
        })?;
    let input: Vec<&DatasetInput> = inputs2.iter().flatten().collect();

    let ideally = ideals
        .iter()
        .map(|a| DatasetOutput::try_from_array(a))
        .collect::<Result<Vec<_>, _>>()
        .context(FailedToParseColumnSnafu {
            column: "ideal".to_string(),
            dataset: dataset.to_string(),
        })?;

    let ideal: Vec<&DatasetOutput> = ideally.iter().flatten().collect();

    let actual: Vec<DatasetOutput> = if let Some(first_ideal) = ideal.first() {
        run_model(model, &input, first_ideal)
            .await
            .context(FailedToRunModelSnafu { eval_name })?
    } else {
        vec![]
    };

    let mut result: HashMap<String, Vec<f32>> = HashMap::with_capacity(scorers_subset.len());
    for ((input, ideal), actual) in input.iter().zip(ideal.iter()).zip(actual.iter()) {
        for (name, scorer) in &scorers_subset {
            if let Some(scorer_results) = result.get_mut(*name) {
                scorer_results.push(scorer.score(input, actual, ideal).await);
            }
        }
    }
    to_record_batch(result).context(FailedToCreateScoreOutputsSnafu)
}

fn to_record_batch(x: HashMap<String, Vec<f32>>) -> Result<RecordBatch, ArrowError> {
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = x
        .into_iter()
        .map(|(k, v)| {
            (
                Field::new(k, arrow_schema::DataType::Float32, false),
                Arc::new(Float32Array::from(v)) as ArrayRef,
            )
        })
        .unzip();

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
}

/// Return format of [`DatasetOutput`] determined by `output_format`. `output_format` can be empty, is only used for its enum type.
#[allow(clippy::borrowed_box)]
async fn run_model(
    model: &Box<dyn Chat>,
    inputs: &[&DatasetInput],
    output_format: &DatasetOutput,
) -> Result<Vec<DatasetOutput>, OpenAIError> {
    let mut outputs = Vec::with_capacity(inputs.len());
    for (i, input) in inputs.iter().enumerate() {
        let req: CreateChatCompletionRequest = (*input).try_into()?;
        let choices = model.chat_request(req).await?.choices;
        let output = match output_format {
            DatasetOutput::AssistantResponse(_) => DatasetOutput::AssistantResponse(
                choices
                    .first()
                    .and_then(|c| c.message.content.clone())
                    .unwrap_or_default(),
            ),
            DatasetOutput::Messages(_) => DatasetOutput::Messages(choices),
        };
        outputs[i] = output;
    }
    Ok(outputs)
}

fn rb_to_json_value(
    data: &RecordBatch,
) -> Result<Vec<Value>, Box<dyn std::error::Error + Send + Sync>> {
    let mut writer = arrow_json::ArrayWriter::new(Vec::new());
    writer.write_batches(&[data]).boxed()?;
    writer.finish().boxed()?;

    serde_json::from_str(String::from_utf8(writer.into_inner()).boxed()?.as_str()).boxed()
}
