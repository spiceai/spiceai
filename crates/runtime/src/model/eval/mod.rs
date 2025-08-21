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

use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use arrow_schema::ArrowError;
use async_openai::{error::OpenAIError, types::CreateChatCompletionRequest};

use dataset::{DatasetInput, DatasetOutput, get_eval_data};
use llms::chat::Chat;
use result::{EVAL_RESULTS_TABLE_REFERENCE, ResultBuilder, write_result_to_table};
use runs::{
    EvalRunId, EvalRunStatus, add_metrics_to_eval_run, start_tracing_eval_run,
    update_eval_run_status,
};
use scorer::score_results;
use snafu::{ResultExt, Snafu, ensure};
use spicepod::component::eval::Eval;
use tracing_futures::Instrument;

use crate::datafusion::DataFusion;

use super::{EvalScorerRegistry, LLMChatCompletionsModelStore, Scorer};

pub(crate) mod dataset;
pub(crate) mod result;
pub(crate) mod runs;
pub(crate) mod scorer;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to query evaluation dataset '{dataset_name}'.\n{source}\nEnsure the dataset is available and has the correct schema."
    ))]
    FailedToQueryDataset {
        dataset_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to parse the column '{column}' in evaluation dataset '{dataset}'.\n{source}\nEnsure the column is available and has the correct schema."
    ))]
    FailedToParseColumn {
        column: String,
        dataset: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to prepare data for evaluation '{eval_name}'\n{source}\nVerify the dataset and model configuration, and try again."
    ))]
    FailedToPrepareData {
        eval_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to run the model during evaluation '{eval_name}'.\n{source}\nVerify the model configuration and try again."
    ))]
    FailedToRunModel {
        eval_name: String,
        source: OpenAIError,
    },

    #[snafu(display(
        "Failed to produce the number of expected rows from the model {model_name}, during evaluation '{eval_name}'.\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    ModelProducedFewerRows {
        model_name: String,
        eval_name: String,
    },

    #[snafu(display(
        "Failed to acquire the model '{model_name}' during evaluation '{eval_name}'.\nEnsure the model is available and has been successfully loaded."
    ))]
    FailedToGetModel {
        eval_name: String,
        model_name: String,
    },

    #[snafu(display(
        "Failed to load the scorer '{scorer_name}' needed for evaluation '{eval_name}'.\nVerify the scorer '{scorer_name}' is defined in the spicepod and has been sucessfully loaded."
    ))]
    EvalScorerUnavailable {
        eval_name: String,
        scorer_name: String,
    },

    #[snafu(display(
        "Failed to create score outputs.\n{source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToCreateScoreOutputs { source: ArrowError },

    #[snafu(display("Failed to write evaluation results to {} for '{eval_run_id}'.\n{source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues", EVAL_RESULTS_TABLE_REFERENCE.clone()))]
    FailedToWriteEvalResults {
        eval_run_id: EvalRunId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to start an evaluation run for {eval_name}.\n{source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToStartEvalRun {
        eval_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to update evaluation run table '{eval_run_id}'.\n{source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToUpdateEvalRunTable {
        eval_run_id: EvalRunId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to start the evaluation run '{eval_run_id}'.\n{source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToOffloadEvalRun {
        eval_run_id: EvalRunId,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to update the status of an evaluation run '{eval_id}' to status '{status}'.\n{source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    FailedToUpdateEvalRunStatus {
        eval_id: EvalRunId,
        status: EvalRunStatus,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to parse the input column from the evaluation dataset.\n{reason}\nCheck that the values in the input column are of valid evaluation format."
    ))]
    InvalidInputFormat { reason: String },

    #[snafu(display(
        "Failed to parse the input column from the evaluation dataset.\n{reason}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    InvalidInputFormatReport { reason: String },

    #[snafu(display(
        "Failed to parse the output column from the evaluation dataset.\n{reason}\nCheck that the values in the output column are of valid evaluation format."
    ))]
    InvalidOutputFormat { reason: String },

    #[snafu(display(
        "Failed to parse the output column from the evaluation dataset.\n{reason}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"
    ))]
    InvalidOutputFormatReport { reason: String },

    #[snafu(display("An error occured whilst scoring the results of the eval run. {source}"))]
    FailedToScoreEvalRun { source: scorer::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Handles both running the eval, tracking the `eval_run` `task_history`,  and updating the status of the evaluation run in `eval.runs`. Error is returned if the evaluation run fails or the evaluation run status/metrics could not be updated.
pub async fn handle_eval_run(
    eval: &Eval,
    model_name: String,
    df: Arc<DataFusion>,
    llms: Arc<RwLock<LLMChatCompletionsModelStore>>,
    scorer_registry: EvalScorerRegistry,
) -> Result<EvalRunId> {
    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        "eval_run",
        input = %serde_json::to_string(&eval).unwrap_or_default(),
    );
    let id = start_tracing_eval_run(eval, model_name.as_str(), Arc::clone(&df)).await?;

    span.in_scope(
        || tracing::info!(target: "task_history",trace_id = %id, model = %model_name.clone(), "labels"),
    );

    update_eval_run_status(Arc::clone(&df), &id, &EvalRunStatus::Running, None).await?;

    let (status, err_opt) = match run_eval(
        &id,
        Arc::clone(&llms),
        model_name,
        eval,
        Arc::clone(&df),
        Arc::clone(&scorer_registry),
    )
    .instrument(span.clone())
    .await
    {
        Err(e) => (EvalRunStatus::Failed, Some(e.to_string())),
        Ok(()) => (EvalRunStatus::Completed, None),
    };
    update_eval_run_status(Arc::clone(&df), &id, &status, err_opt).await?;

    Ok(id)
}

#[allow(clippy::implicit_hasher)]
async fn run_eval(
    id: &EvalRunId,
    llm_store: Arc<RwLock<LLMChatCompletionsModelStore>>,
    model_name: String,
    eval: &Eval,
    df: Arc<DataFusion>,
    scorer_registry: EvalScorerRegistry,
) -> Result<()> {
    // Get & prepare the evaluation dataset
    let (input, ideal) = get_eval_data(Arc::clone(&df), eval).await?;
    if input.len() != ideal.len() {
        return Err(Error::FailedToPrepareData {
            eval_name: eval.name.clone(),
            source: Box::<dyn std::error::Error + Send + Sync>::from(format!(
                "input ({}) and ideal ({}) in evaluation dataset '{}' do not have the same length",
                input.len(),
                ideal.len(),
                eval.dataset.clone()
            )),
        });
    }

    // Run the model against the evaluation dataset.
    let llms = llm_store.read().await;
    let model = llms
        .get(&model_name)
        .ok_or_else(|| Error::FailedToGetModel {
            model_name: model_name.clone(),
            eval_name: eval.name.clone(),
        })?;

    let actual: Vec<DatasetOutput> = if let Some(first_ideal) = ideal.first() {
        run_model(
            eval.name.as_str(),
            model_name.as_str(),
            &**model,
            &input,
            first_ideal,
        )
        .await?
    } else {
        // Not an error, no data in dataset
        vec![]
    };

    ensure!(
        actual.len() == ideal.len(),
        ModelProducedFewerRowsSnafu {
            eval_name: eval.name.clone(),
            model_name: model_name.clone()
        }
    );

    // Score the results
    let scorers_to_use = get_scorers_for_eval(eval, Arc::clone(&scorer_registry)).await?;

    let span = tracing::span!(
        target: "task_history",
        tracing::Level::INFO,
        "eval_scoring",
        scorers = %serde_json::to_string(&scorers_to_use.keys().collect::<Vec<_>>()).unwrap_or_default(),
    );

    let scores = match score_results(&input, &actual, &ideal, &scorers_to_use)
        .instrument(span.clone())
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            return Err(Error::FailedToScoreEvalRun { source: e });
        }
    };

    write_results(id, Arc::clone(&df), &input, &actual, &ideal, &scores).await?;

    // Compute metrics
    let metrics = scorers_to_use
        .iter()
        .map(|(name, scorer)| ((*name).clone(), scorer.metrics(&scores[name])))
        .collect();

    add_metrics_to_eval_run(Arc::clone(&df), id, &metrics).await?;
    Ok(())
}

async fn get_scorers_for_eval(
    eval: &Eval,
    scorers: Arc<RwLock<HashMap<String, Arc<dyn Scorer>>>>,
) -> Result<HashMap<String, Arc<dyn Scorer>>> {
    let mut scorer_subset = HashMap::with_capacity(eval.scorers.len());
    for name in &eval.scorers {
        let scorers_unlock = scorers.read().await;
        let scorer = scorers_unlock
            .get(name)
            .ok_or_else(|| Error::EvalScorerUnavailable {
                scorer_name: name.clone(),
                eval_name: eval.name.clone(),
            })?;
        scorer_subset.insert(name.clone(), Arc::clone(scorer));
    }
    Ok(scorer_subset)
}

async fn write_results(
    run_id: &EvalRunId,
    df: Arc<DataFusion>,
    input: &[DatasetInput],
    actual: &[DatasetOutput],
    expected: &[DatasetOutput],
    scores: &HashMap<String, Vec<f32>>,
) -> Result<()> {
    let mut bldr = ResultBuilder::new();
    for i in 0..input.len() {
        let input = &input[i];
        let actual = &actual[i];
        let expected = &expected[i];
        for (name, score) in scores {
            bldr.append(
                run_id,
                chrono::Utc::now(),
                input,
                actual,
                expected,
                name,
                score[i],
            )?;
        }
    }

    write_result_to_table(Arc::clone(&df), run_id, &mut bldr).await
}

/// Return format of [`DatasetOutput`] determined by `output_format`. `output_format` can be empty, is only used for its enum type.
async fn run_model(
    eval_name: &str,
    model_name: &str,
    model: &dyn Chat,
    inputs: &[DatasetInput],
    output_format: &DatasetOutput,
) -> Result<Vec<DatasetOutput>> {
    let mut outputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        let span = tracing::span!(
            target: "task_history",
            tracing::Level::INFO,
            "eval_step",
            input = %serde_json::to_string(&input).unwrap_or_default(),
        );
        match run_eval_step(eval_name, model_name, model, input, output_format)
            .instrument(span.clone())
            .await
        {
            Ok(output) => {
                if let Ok(captured_output) = serde_json::to_string(&output) {
                    tracing::info!(target: "task_history", parent: &span, captured_output = %captured_output);
                } else {
                    tracing::warn!("Failed to serialize output for logging");
                }
                outputs.push(output);
            }
            Err(e) => {
                tracing::error!(target: "task_history", parent: &span, "{e}");
                return Err(e);
            }
        }
    }
    Ok(outputs)
}

async fn run_eval_step(
    eval_name: &str,
    model_name: &str,
    model: &dyn Chat,
    input: &DatasetInput,
    output_format: &DatasetOutput,
) -> Result<DatasetOutput> {
    let mut req =
        TryInto::<CreateChatCompletionRequest>::try_into(input).context(FailedToRunModelSnafu {
            eval_name: eval_name.to_string(),
        })?;
    req.model.clone_from(&model_name.to_string());
    let resp = model
        .chat_request(req)
        .await
        .context(FailedToRunModelSnafu {
            eval_name: eval_name.to_string(),
        })?;

    let output = match output_format {
        DatasetOutput::AssistantResponse(_) => DatasetOutput::AssistantResponse(
            resp.choices
                .into_iter()
                .next()
                .and_then(|mut c| c.message.content.take())
                .unwrap_or_default(),
        ),
        DatasetOutput::Choices(_) => DatasetOutput::Choices(resp.choices),
    };
    Ok(output)
}
