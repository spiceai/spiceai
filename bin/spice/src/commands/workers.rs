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

//! Workers command implementation - lists workers loaded by the runtime.

use crate::context::RuntimeContext;
use crate::error::{self, InvalidResponseSnafu, Result, RuntimeUnavailableSnafu};
use crate::output::{OutputFormat, TableRow, write_json, write_table};
use clap::Args;
use runtime_api_types::v1::{WorkerInfo, WorkerListResponse};

/// Arguments for the workers command.
#[derive(Args, Debug)]
#[command(
    about = "Lists workers loaded by the Spice runtime",
    long_about = r#"Lists workers loaded by the Spice runtime

Examples:
  spice workers
  spice workers -o json

See more at: https://spiceai.org/docs/"#
)]
pub struct WorkersArgs {
    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

impl TableRow for WorkerInfo {
    fn headers() -> Vec<&'static str> {
        vec!["NAME", "IS_LLM", "DESCRIPTION"]
    }

    fn values(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.is_llm.to_string(),
            self.description.clone().unwrap_or_default(),
        ]
    }
}

/// Execute the workers command.
pub async fn execute(ctx: &RuntimeContext, args: &WorkersArgs) -> Result<()> {
    let response = ctx.get("/v1/workers").await.map_err(|_| {
        RuntimeUnavailableSnafu {
            endpoint: ctx.http_endpoint().to_string(),
        }
        .build()
    })?;

    let response = error::check_response(response, ctx.http_endpoint()).await?;

    let worker_response: WorkerListResponse = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse workers response: {e}"),
        }
        .build()
    })?;

    match args.output {
        OutputFormat::Table => write_table(&worker_response.data),
        OutputFormat::Json => write_json(&worker_response.data)?,
    }

    Ok(())
}
