/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use anyhow::{anyhow, Context, Result};
use async_openai::types::chat::{
    ChatCompletionRequestMessage, ChatCompletionRequestUserMessageArgs,
    CreateChatCompletionRequestArgs,
};
use serde_json::Value;
use spicepod::component::eval::Eval;
use std::collections::HashMap;
use uuid::Uuid;

use crate::client::SpiceClient;
use crate::scorer::{builtin_scorers, DatasetInput, DatasetOutput, Scorer};

/// Run an evaluation
pub async fn run_eval(client: &SpiceClient, eval: &Eval, model: &str) -> Result<String> {
    let run_id = Uuid::new_v4().to_string();

    tracing::info!("Starting eval run {}", run_id);

    // Initialize eval run record
    client
        .write_eval_run(&run_id, &eval.dataset, model, &eval.scorers)
        .await?;

    // Update status to Running
    client
        .update_eval_run_status(&run_id, "Running", None)
        .await?;

    // Execute the evaluation
    match run_eval_internal(client, eval, model, &run_id).await {
        Ok(()) => {
            client
                .update_eval_run_status(&run_id, "Completed", None)
                .await?;
            Ok(run_id)
        }
        Err(e) => {
            let error_msg = e.to_string();
            tracing::error!("Eval run {} failed: {}", run_id, error_msg);
            client
                .update_eval_run_status(&run_id, "Failed", Some(&error_msg))
                .await?;
            Err(e)
        }
    }
}

async fn run_eval_internal(
    client: &SpiceClient,
    eval: &Eval,
    model: &str,
    run_id: &str,
) -> Result<()> {
    // 1. Fetch evaluation dataset
    tracing::info!("Fetching evaluation dataset: {}", eval.dataset);
    let sql = format!("SELECT input, ideal FROM {}", eval.dataset);
    let rows = client.query_sql(&sql).await?;

    if rows.is_empty() {
        return Err(anyhow!("Evaluation dataset '{}' is empty", eval.dataset));
    }

    // 2. Parse inputs and expected outputs
    let mut inputs: Vec<DatasetInput> = Vec::new();
    let mut expected: Vec<DatasetOutput> = Vec::new();

    for row in &rows {
        let input = parse_dataset_input(
            row.get("input")
                .context("Missing 'input' column in dataset")?,
        )?;
        let ideal = parse_dataset_output(
            row.get("ideal")
                .context("Missing 'ideal' column in dataset")?,
        )?;

        inputs.push(input);
        expected.push(ideal);
    }

    tracing::info!("Loaded {} test cases from dataset", inputs.len());

    // 3. Run model on each input
    tracing::info!("Running model '{}' on test cases", model);
    let mut actual: Vec<DatasetOutput> = Vec::new();

    for (idx, input) in inputs.iter().enumerate() {
        tracing::debug!("Processing test case {}/{}", idx + 1, inputs.len());

        let messages = match input {
            DatasetInput::UserInput(content) => {
                vec![ChatCompletionRequestUserMessageArgs::default()
                    .content(content.clone())
                    .build()?
                    .into()]
            }
            DatasetInput::Messages(msgs) => msgs
                .iter()
                .map(|m| serde_json::from_value::<ChatCompletionRequestMessage>(m.clone()))
                .collect::<Result<Vec<_>, _>>()?,
        };

        let request = CreateChatCompletionRequestArgs::default()
            .model(model)
            .messages(messages)
            .build()?;

        let response = client.chat_completion(request).await?;

        let output = DatasetOutput::Choices(response.choices);
        actual.push(output);
    }

    tracing::info!("Model inference complete. Running scorers...");

    // 4. Load scorers
    let available_scorers = builtin_scorers();
    let mut scorers_to_use: HashMap<String, &Box<dyn Scorer>> = HashMap::new();

    for scorer_name in &eval.scorers {
        let scorer = available_scorers
            .get(scorer_name)
            .ok_or_else(|| anyhow!("Scorer '{scorer_name}' not found"))?;
        scorers_to_use.insert(scorer_name.clone(), scorer);
    }

    // 5. Score results
    let mut all_scores: HashMap<String, Vec<f32>> = HashMap::new();

    for ((input, actual_output), expected_output) in
        inputs.iter().zip(actual.iter()).zip(expected.iter())
    {
        for (scorer_name, scorer) in &scorers_to_use {
            let score = scorer.score(input, actual_output, expected_output)?;

            all_scores
                .entry((*scorer_name).clone())
                .or_default()
                .push(score);
        }
    }

    tracing::info!("Scoring complete. Writing results...");

    // 6. Write detailed results
    for i in 0..inputs.len() {
        let input_str = serde_json::to_string(&inputs[i])?;
        let actual_str = serde_json::to_string(&actual[i])?;
        let expected_str = serde_json::to_string(&expected[i])?;

        for (scorer_name, scores) in &all_scores {
            client
                .write_eval_results(
                    run_id,
                    &input_str,
                    &actual_str,
                    &expected_str,
                    scorer_name,
                    scores[i],
                )
                .await?;
        }
    }

    // 7. Compute and log metrics
    for (scorer_name, scorer) in &scorers_to_use {
        let scores = &all_scores[scorer_name];
        let metrics = scorer.metrics(scores);

        for (metric_name, metric_value) in metrics {
            tracing::info!(
                "Scorer '{}' - {}: {:.4}",
                scorer_name,
                metric_name,
                metric_value
            );
        }
    }

    Ok(())
}

/// Parse a JSON value into `DatasetInput`
fn parse_dataset_input(value: &Value) -> Result<DatasetInput> {
    match value {
        Value::String(s) => {
            // Try to parse as Messages JSON, fallback to UserInput
            match serde_json::from_str::<Vec<ChatCompletionRequestMessage>>(s) {
                Ok(messages) => {
                    let json_messages: Vec<Value> = messages
                        .into_iter()
                        .map(serde_json::to_value)
                        .collect::<Result<_, _>>()?;
                    Ok(DatasetInput::Messages(json_messages))
                }
                Err(_) => Ok(DatasetInput::UserInput(s.clone())),
            }
        }
        Value::Array(arr) => Ok(DatasetInput::Messages(arr.clone())),
        _ => Err(anyhow!("Invalid input format: expected string or array")),
    }
}

/// Parse a JSON value into `DatasetOutput`
fn parse_dataset_output(value: &Value) -> Result<DatasetOutput> {
    match value {
        Value::String(s) => {
            // Try to parse as Choices JSON, fallback to AssistantResponse
            match serde_json::from_str(s) {
                Ok(choices) => Ok(DatasetOutput::Choices(choices)),
                Err(_) => Ok(DatasetOutput::AssistantResponse(s.clone())),
            }
        }
        Value::Array(_) => {
            let choices = serde_json::from_value(value.clone())?;
            Ok(DatasetOutput::Choices(choices))
        }
        _ => Err(anyhow!("Invalid output format: expected string or array")),
    }
}
