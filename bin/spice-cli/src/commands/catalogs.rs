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

//! Catalogs command implementation - lists catalogs configured by the runtime.

use crate::context::RuntimeContext;
use crate::error::{InvalidResponseSnafu, Result, RuntimeUnavailableSnafu};
use crate::output::{TableRow, write_table};
use clap::Args;
use serde::Deserialize;

/// Arguments for the catalogs command.
#[derive(Args, Debug)]
#[command(
    about = "Lists catalogs configured by the Spice runtime",
    long_about = r#"Lists catalogs configured by the Spice runtime

Examples:
  spice catalogs

See more at: https://spiceai.org/docs/"#
)]
pub struct CatalogsArgs {}

/// Catalog information from the runtime API.
#[derive(Debug, Deserialize)]
pub struct Catalog {
    pub from: Option<String>,
    pub name: Option<String>,
}

impl TableRow for Catalog {
    fn headers() -> Vec<&'static str> {
        vec!["NAME", "FROM"]
    }

    fn values(&self) -> Vec<String> {
        vec![
            self.name.clone().unwrap_or_default(),
            self.from.clone().unwrap_or_default(),
        ]
    }
}

/// Execute the catalogs command.
pub async fn execute(ctx: &RuntimeContext, _args: &CatalogsArgs) -> Result<()> {
    let response = ctx.get("/v1/catalogs").await.map_err(|_| {
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

    let catalogs: Vec<Catalog> = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse catalogs response: {e}"),
        }
        .build()
    })?;

    write_table(&catalogs);

    Ok(())
}
