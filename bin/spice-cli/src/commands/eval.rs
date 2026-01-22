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

//! `spice eval` command - Run model evaluation.

use crate::RuntimeContext;
use crate::error::{InvalidArgumentSnafu, InvalidResponseSnafu, Result};
use crate::output::TableOutput;
use clap::Args;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::HashMap;

/// Arguments for the `eval` command.
#[derive(Args, Debug)]
pub struct EvalArgs {
    /// Name of the eval to run
    pub eval_name: String,

    /// Model to evaluate
    #[arg(long, required = true)]
    pub model: String,
}

/// Request body for eval API.
#[derive(Debug, Serialize)]
struct EvalRequest {
    model: String,
}

/// Response from eval API.
#[derive(Debug, Deserialize)]
struct EvalResponse {
    id: String,
    created_at: String,
    dataset: String,
    model: String,
    status: String,
    scorers: Vec<String>,
    metrics: HashMap<String, f64>,
}

/// Execute the `eval` command.
///
/// # Errors
///
/// Returns an error if the eval name is empty, model is empty, or the API request fails.
pub async fn execute(ctx: &RuntimeContext, args: &EvalArgs) -> Result<()> {
    ensure!(
        !args.eval_name.is_empty(),
        InvalidArgumentSnafu {
            message: "eval name is required"
        }
    );

    ensure!(
        !args.model.is_empty(),
        InvalidArgumentSnafu {
            message: "model is required"
        }
    );

    let request = EvalRequest {
        model: args.model.clone(),
    };

    let response = ctx
        .post_json(&format!("/v1/evals/{}", args.eval_name), &request)
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(InvalidResponseSnafu {
            message: format!("eval request failed with status {status}: {text}"),
        }
        .build());
    }

    let results: Vec<EvalResponse> = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse eval response: {e}"),
        }
        .build()
    })?;

    if results.is_empty() {
        println!("No evaluation results.");
        return Ok(());
    }

    // Display results in a table
    let mut table = TableOutput::new(vec![
        "ID",
        "Created At",
        "Dataset",
        "Model",
        "Status",
        "Scorers",
        "Metrics",
    ]);

    for result in results {
        let scorers = result.scorers.join(", ");
        let metrics = result
            .metrics
            .iter()
            .map(|(k, v)| format!("{k}: {v:.4}"))
            .collect::<Vec<_>>()
            .join(", ");

        table.add_row(vec![
            result.id,
            result.created_at,
            result.dataset,
            result.model,
            result.status,
            scorers,
            metrics,
        ]);
    }

    println!("{table}");

    Ok(())
}
