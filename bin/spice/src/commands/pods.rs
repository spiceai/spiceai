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

//! Pods command implementation - lists spicepods loaded by the runtime.

use crate::context::RuntimeContext;
use crate::error::{InvalidResponseSnafu, Result, RuntimeUnavailableSnafu};
use crate::output::{TableRow, write_table};
use clap::Args;
use serde::Deserialize;

/// Arguments for the pods command.
#[derive(Args, Debug)]
#[command(
    about = "Lists Spicepods loaded by the Spice runtime",
    long_about = r#"Lists Spicepods loaded by the Spice runtime

Examples:
  spice pods

See more at: https://spiceai.org/docs/"#
)]
pub struct PodsArgs {}

/// Spicepod status information from the runtime API.
#[derive(Debug, Deserialize)]
pub struct SpicepodStatus {
    pub version: Option<String>,
    pub name: Option<String>,
    pub datasets_count: Option<i32>,
    pub models_count: Option<i32>,
    pub dependencies_count: Option<i32>,
}

impl TableRow for SpicepodStatus {
    fn headers() -> Vec<&'static str> {
        vec!["NAME", "VERSION", "DATASETS", "MODELS", "DEPENDENCIES"]
    }

    fn values(&self) -> Vec<String> {
        vec![
            self.name.clone().unwrap_or_default(),
            self.version.clone().unwrap_or_default(),
            self.datasets_count
                .map_or("0".to_string(), |v| v.to_string()),
            self.models_count.map_or("0".to_string(), |v| v.to_string()),
            self.dependencies_count
                .map_or("0".to_string(), |v| v.to_string()),
        ]
    }
}

/// Execute the pods command.
pub async fn execute(ctx: &RuntimeContext, _args: &PodsArgs) -> Result<()> {
    let response = ctx.get("/v1/spicepods").await.map_err(|_| {
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

    let spicepods: Vec<SpicepodStatus> = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse spicepods response: {e}"),
        }
        .build()
    })?;

    write_table(&spicepods);

    Ok(())
}
