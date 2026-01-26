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
use crate::error::{InvalidResponseSnafu, Result, RuntimeUnavailableSnafu};
use crate::output::{TableRow, write_table};
use clap::Args;
use serde::Deserialize;

/// Arguments for the models command.
#[derive(Args, Debug)]
#[command(
    about = "Lists models loaded by the Spice runtime",
    long_about = r#"Lists models loaded by the Spice runtime

Examples:
  spice models

See more at: https://spiceai.org/docs/"#
)]
pub struct ModelsArgs {}

/// Model information from the runtime API.
#[derive(Debug, Deserialize)]
pub struct Model {
    pub id: Option<String>,
    pub object: Option<String>,
    pub owned_by: Option<String>,
    pub status: Option<String>,
}

/// Response wrapper for models endpoint.
#[derive(Debug, Deserialize)]
pub struct ModelResponse {
    pub object: Option<String>,
    pub data: Vec<Model>,
}

impl TableRow for Model {
    fn headers() -> Vec<&'static str> {
        vec!["ID", "OWNED_BY", "STATUS"]
    }

    fn values(&self) -> Vec<String> {
        vec![
            self.id.clone().unwrap_or_default(),
            self.owned_by.clone().unwrap_or_default(),
            self.status.clone().unwrap_or_default(),
        ]
    }
}

/// Execute the models command.
pub async fn execute(ctx: &RuntimeContext, _args: &ModelsArgs) -> Result<()> {
    let response = ctx.get("/v1/models?status=true").await.map_err(|_| {
        RuntimeUnavailableSnafu {
            endpoint: ctx.http_endpoint().to_string(),
        }
        .build()
    })?;

    if !response.status().is_success() {
        return Err(RuntimeUnavailableSnafu {
            endpoint: ctx.http_endpoint().to_string(),
        }
        .build());
    }

    let model_response: ModelResponse = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse models response: {e}"),
        }
        .build()
    })?;

    write_table(&model_response.data);

    Ok(())
}
