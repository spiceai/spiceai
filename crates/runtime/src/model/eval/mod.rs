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

use std::sync::Arc;

use arrow_schema::ArrowError;
use async_openai::{error::OpenAIError, types::CreateChatCompletionRequest};

use dataset::{DatasetInput, DatasetOutput};
use llms::chat::Chat;
use result::EVAL_RESULTS_TABLE_REFERENCE;
use runs::{EvalRunId, EvalRunStatus};
use snafu::{ResultExt, Snafu};

pub(crate) mod dataset;
pub(crate) mod result;
pub(crate) mod runs;
pub(crate) mod scorer;
pub(crate) mod worker;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to query eval dataset '{dataset_name}': {source}. Ensure the dataset is available and has the correct schema."))]
    FailedToQueryDataset {
        dataset_name: String,
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

    #[snafu(display(
        "During evaluation '{eval_name}', the model '{model_name}' could not be acquired"
    ))]
    FailedToGetModel {
        eval_name: String,
        model_name: String,
    },

    #[snafu(display("Scorer '{scorer_name}' needed for eval '{eval_name}' is not available. Ensure '{scorer_name}' is defined in the spicepod and has been sucessfully loaded."))]
    EvalScorerUnavailable {
        eval_name: String,
        scorer_name: String,
    },

    #[snafu(display("Failed to create score outputs: {source}"))]
    FailedToCreateScoreOutputs { source: ArrowError },

    #[snafu(display("Failed to write eval results to {} for '{eval_run_id}': {source}", EVAL_RESULTS_TABLE_REFERENCE.clone()))]
    FailedToWriteEvalResults {
        eval_run_id: EvalRunId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to start an eval run for {eval_name}: {source}"))]
    FailedToStartEvalRun {
        eval_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to update eval run table '{eval_run_id}': {source}"))]
    FailedToUpdateEvalRunTable {
        eval_run_id: EvalRunId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to send eval run '{eval_run_id}' to background workers: {source}"))]
    FailedToOffloadEvalRun {
        eval_run_id: EvalRunId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to update the status of an eval run '{eval_id}' to status '{status}': {source}"
    ))]
    FailedToUpdateEvalRunStatus {
        eval_id: EvalRunId,
        status: EvalRunStatus,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to parse the input column from the eval dataset because {reason}. Check that the values in the input column are of valid eval format."))]
    InvalidInputFormat { reason: String },

    #[snafu(display("Failed to parse the output column from the eval dataset because {reason}. Check that the values in the output column are of valid eval format."))]
    InvalidOutputFormat { reason: String },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Return format of [`DatasetOutput`] determined by `output_format`. `output_format` can be empty, is only used for its enum type.
#[allow(clippy::borrowed_box)]
async fn run_model(
    eval_name: String,
    model: Arc<Box<dyn Chat>>,
    inputs: &[DatasetInput],
    output_format: &DatasetOutput,
) -> Result<Vec<DatasetOutput>> {
    let mut outputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let req = TryInto::<CreateChatCompletionRequest>::try_into(input).context(
            FailedToRunModelSnafu {
                eval_name: eval_name.clone(),
            },
        )?;

        let choices = model
            .chat_request(req)
            .await
            .context(FailedToRunModelSnafu {
                eval_name: eval_name.clone(),
            })?
            .choices;

        let output = match output_format {
            DatasetOutput::AssistantResponse(_) => DatasetOutput::AssistantResponse(
                choices
                    .into_iter()
                    .next()
                    .and_then(|mut c| c.message.content.take())
                    .unwrap_or_default(),
            ),
            DatasetOutput::Choices(_) => DatasetOutput::Choices(choices),
        };
        outputs.push(output);
    }
    Ok(outputs)
}
