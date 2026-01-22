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

//! Status command implementation.

use crate::context::RuntimeContext;
use crate::error::{InvalidResponseSnafu, Result, RuntimeUnavailableSnafu};
use clap::Args;
use serde::Deserialize;

/// Arguments for the status command.
#[derive(Args, Debug)]
pub struct StatusArgs {
    /// Output format (text or json)
    #[arg(long, default_value = "text")]
    format: OutputFormat,
}

/// Output format for status command.
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum OutputFormat {
    #[default]
    Text,
    Json,
}

/// Status response from the runtime.
#[derive(Debug, Deserialize)]
struct StatusResponse {
    status: String,
    #[serde(default)]
    application_name: Option<String>,
    #[serde(default)]
    version: Option<String>,
}

/// Execute the status command.
pub async fn execute(ctx: &RuntimeContext, args: &StatusArgs) -> Result<()> {
    let response = ctx.get("/v1/status").await.map_err(|_| {
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

    let status: StatusResponse = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse status response: {e}"),
        }
        .build()
    })?;

    match args.format {
        OutputFormat::Text => {
            println!("Status: {}", status.status);
            if let Some(name) = &status.application_name {
                println!("Application: {name}");
            }
            if let Some(version) = &status.version {
                println!("Version: {version}");
            }
        }
        OutputFormat::Json => {
            let json = serde_json::to_string_pretty(&serde_json::json!({
                "status": status.status,
                "application_name": status.application_name,
                "version": status.version,
                "endpoint": ctx.http_endpoint(),
            }))
            .map_err(|e| {
                InvalidResponseSnafu {
                    message: format!("Failed to serialize status: {e}"),
                }
                .build()
            })?;
            println!("{json}");
        }
    }

    Ok(())
}
