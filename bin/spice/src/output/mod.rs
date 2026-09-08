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

//! Output formatting utilities for CLI commands.

mod table;

pub use table::{TableOutput, TableRow, write_table};

use crate::error::{InvalidResponseSnafu, Result};
use serde::Serialize;

/// Output format shared by all CLI commands that produce structured data.
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum, PartialEq, Eq)]
pub enum OutputFormat {
    /// Display results as a human-readable table (default)
    #[default]
    #[value(alias = "text")]
    Table,
    /// Output results as pretty-printed JSON
    Json,
}

/// Serialize `data` to pretty-printed JSON and print to stdout.
pub fn write_json<T: Serialize>(data: &T) -> Result<()> {
    let json = serde_json::to_string_pretty(data).map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to serialize to JSON: {e}"),
        }
        .build()
    })?;
    println!("{json}");
    Ok(())
}
