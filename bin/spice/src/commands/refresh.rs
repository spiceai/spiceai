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

//! Refresh command implementation - triggers a dataset refresh.

use crate::context::RuntimeContext;
use crate::error::{InvalidResponseSnafu, Result, RuntimeUnavailableSnafu};
use clap::Args;
use serde::{Deserialize, Serialize};

/// Arguments for the refresh command.
#[derive(Args, Debug)]
#[command(
    about = "Refresh a dataset",
    long_about = r#"Refresh a dataset

Examples:
  spice refresh taxi_trips
  spice refresh taxi_trips --refresh-mode full
  spice refresh taxi_trips --refresh-sql "SELECT * FROM source WHERE updated_at > now() - interval '1 hour'"

See more at: https://spiceai.org/docs/"#
)]
pub struct RefreshArgs {
    /// The dataset name to refresh
    #[arg(required = true)]
    pub dataset: String,

    /// SQL query to filter the data to refresh
    #[arg(long)]
    pub refresh_sql: Option<String>,

    /// Refresh mode: 'full' or 'append'
    #[arg(long)]
    pub refresh_mode: Option<String>,

    /// Maximum jitter for the refresh operation (e.g. '1m')
    #[arg(long)]
    pub refresh_jitter_max: Option<String>,
}

/// Request body for the refresh API.
#[derive(Debug, Serialize)]
#[expect(
    clippy::struct_field_names,
    reason = "API contract requires these field names"
)]
struct RefreshRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_jitter_max: Option<String>,
}

/// Response from the refresh API.
#[derive(Debug, Deserialize)]
struct RefreshResponse {
    message: Option<String>,
}

/// Execute the refresh command.
pub async fn execute(ctx: &RuntimeContext, args: &RefreshArgs) -> Result<()> {
    // Validate refresh mode if provided
    if let Some(ref mode) = args.refresh_mode
        && mode != "full"
        && mode != "append"
    {
        tracing::error!("Invalid refresh mode. Valid modes are 'full' or 'append'");
        return Ok(());
    }

    tracing::info!("Refreshing dataset {} ...", args.dataset);

    let url = format!("/v1/datasets/{}/acceleration/refresh", args.dataset);

    // Build the request body
    let request = RefreshRequest {
        refresh_sql: args.refresh_sql.clone(),
        refresh_mode: args.refresh_mode.clone(),
        refresh_jitter_max: args.refresh_jitter_max.clone(),
    };

    // Only send a body if at least one field is set
    let has_body = request.refresh_sql.is_some()
        || request.refresh_mode.is_some()
        || request.refresh_jitter_max.is_some();

    let response = if has_body {
        ctx.post_json(&url, &request).await
    } else {
        ctx.post(&url, None).await
    }
    .map_err(|_| {
        RuntimeUnavailableSnafu {
            endpoint: ctx.http_endpoint().to_string(),
        }
        .build()
    })?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        tracing::error!("Failed to refresh dataset: {} - {}", status, body);
        return Ok(());
    }

    let result: RefreshResponse = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse refresh response: {e}"),
        }
        .build()
    })?;

    if let Some(message) = result.message {
        tracing::info!("{message}");
    }

    Ok(())
}
