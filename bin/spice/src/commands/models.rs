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

//! Models command implementation - lists models loaded by the runtime.

use crate::context::RuntimeContext;
use crate::error::{self, InvalidResponseSnafu, Result, RuntimeUnavailableSnafu};
use crate::output::{OutputFormat, TableRow, write_json, write_table};
use clap::Args;
use runtime_api_types::v1::{ModelInfo, ModelListResponse};

/// Arguments for the models command.
#[derive(Args, Debug)]
#[command(
    about = "Lists models loaded by the Spice runtime",
    long_about = r#"Lists models loaded by the Spice runtime

Examples:
  spice models
  spice models -o json

See more at: https://spiceai.org/docs/"#
)]
pub struct ModelsArgs {
    /// Output format
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
}

impl TableRow for ModelInfo {
    fn headers() -> Vec<&'static str> {
        vec!["ID", "OWNED_BY", "STATUS", "ERROR"]
    }

    fn values(&self) -> Vec<String> {
        vec![
            self.id.clone(),
            self.owned_by.clone(),
            self.status
                .as_ref()
                .map_or_else(String::new, ToString::to_string),
            self.error_message.clone().unwrap_or_default(),
        ]
    }
}

/// Execute the models command.
pub async fn execute(ctx: &RuntimeContext, args: &ModelsArgs) -> Result<()> {
    let response = ctx.get("/v1/models?status=true").await.map_err(|_| {
        RuntimeUnavailableSnafu {
            endpoint: ctx.http_endpoint().to_string(),
        }
        .build()
    })?;

    let response = error::check_response(response, ctx.http_endpoint()).await?;

    let model_response: ModelListResponse = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse models response: {e}"),
        }
        .build()
    })?;

    match args.output {
        OutputFormat::Table => write_table(&model_response.data),
        OutputFormat::Json => write_json(&model_response.data)?,
    }

    Ok(())
}
