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

//! Datasets command implementation - lists datasets loaded by the runtime.

use crate::context::RuntimeContext;
use crate::error::{InvalidResponseSnafu, Result, RuntimeUnavailableSnafu};
use crate::output::{TableRow, write_table};
use clap::Args;
use runtime_api_types::v1::DatasetInfo;

/// Arguments for the datasets command.
#[derive(Args, Debug)]
#[command(
    about = "Lists datasets loaded by the Spice runtime",
    long_about = r#"Lists datasets loaded by the Spice runtime

Examples:
  spice datasets

See more at: https://spiceai.org/docs/"#
)]
pub struct DatasetsArgs {}

impl TableRow for DatasetInfo {
    fn headers() -> Vec<&'static str> {
        vec!["NAME", "FROM", "REPLICATION", "ACCELERATION", "STATUS"]
    }

    fn values(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.from.clone(),
            self.replication_enabled.to_string(),
            self.acceleration_enabled.to_string(),
            self.status
                .as_ref()
                .map_or_else(String::new, ToString::to_string),
        ]
    }
}

/// Execute the datasets command.
pub async fn execute(ctx: &RuntimeContext, _args: &DatasetsArgs) -> Result<()> {
    if ctx.is_cloud() {
        tracing::error!("`spice datasets` does not support `--cloud`.");
        return Ok(());
    }

    let response = ctx.get("/v1/datasets?status=true").await.map_err(|_| {
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

    let datasets: Vec<DatasetInfo> = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse datasets response: {e}"),
        }
        .build()
    })?;

    write_table(&datasets);

    Ok(())
}
