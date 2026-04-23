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

//! `spice validate` — check that a spicepod.yaml file is syntactically valid and
//! resolves its component references, without starting the runtime.
//!
//! Mirrors the behaviour of `tools/spicepod-validator` so it can be used in CI and
//! during authoring (e.g. `spice validate .` or `spice validate path/to/pod.yaml`).

use crate::error::{InvalidArgumentSnafu, Result};
use ansi_colors::Color;
use clap::Args;
use spicepod::Spicepod;
use std::path::PathBuf;

#[derive(Args, Debug)]
#[command(
    about = "Validate a spicepod.yaml without starting the runtime",
    long_about = r#"Validate a spicepod.yaml without starting the runtime.

Checks:
  - YAML syntax and schema
  - Component references (datasets/models/views/tools/...)
  - Duplicate component names
  - Reserved keywords
  - Nested pod includes (`dependsOn`, referenced pods)

Examples:
  spice validate                          # validate ./spicepod.yaml
  spice validate .                        # same as above
  spice validate ./my-app                 # validate my-app/spicepod.yaml
  spice validate path/to/spicepod.yaml    # validate a specific file
"#
)]
pub struct ValidateArgs {
    /// Path to a spicepod.yaml file, or a directory containing one. Defaults to ".".
    #[arg(default_value = ".")]
    pub path: PathBuf,
}

pub async fn execute(args: &ValidateArgs) -> Result<()> {
    let path = &args.path;

    let result = match tokio::fs::metadata(path).await {
        Ok(meta) if meta.is_file() => Spicepod::load_exact(path).await,
        Ok(_) => Spicepod::load(path).await,
        Err(_) => Spicepod::load_exact(path).await,
    };

    match result {
        Ok(pod) => {
            println!(
                "{} {} (datasets: {}, models: {}, views: {}, tools: {}, workers: {})",
                Color::Green.paint("OK"),
                pod.name,
                pod.datasets.len(),
                pod.models.len(),
                pod.views.len(),
                pod.tools.len(),
                pod.workers.len(),
            );
            Ok(())
        }
        Err(e) => {
            eprintln!("{} {e}", Color::Red.paint("Invalid:"));
            // Convert to our CLI's error type so the process exits with a failure code.
            InvalidArgumentSnafu {
                message: format!("spicepod validation failed: {e}"),
            }
            .fail()
        }
    }
}
