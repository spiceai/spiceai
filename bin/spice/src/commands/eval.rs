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

//! `spice eval` command - Run model evaluation using the standalone eval tool.

use crate::RuntimeContext;
use crate::error::{InvalidArgumentSnafu, InvalidResponseSnafu, Result};
use clap::Args;
use snafu::ensure;
use std::process::Command;

/// Arguments for the `eval` command.
#[derive(Args, Debug)]
pub struct EvalArgs {
    /// Name of the eval to run
    pub eval_name: String,

    /// Model to evaluate
    #[arg(long, required = true)]
    pub model: String,
}

/// Execute the `eval` command by invoking the standalone spice-eval tool.
///
/// # Errors
///
/// Returns an error if the eval name is empty, model is empty, or the tool execution fails.
pub async fn execute(ctx: &RuntimeContext, args: &EvalArgs) -> Result<()> {
    ensure!(
        !args.eval_name.is_empty(),
        InvalidArgumentSnafu {
            message: "eval name is required"
        }
    );

    ensure!(
        !args.model.is_empty(),
        InvalidArgumentSnafu {
            message: "model is required"
        }
    );

    // Invoke the spice-eval tool
    let mut cmd = Command::new("spice-eval");
    cmd.arg("--endpoint")
        .arg(&ctx.endpoint)
        .arg("run")
        .arg(&args.eval_name)
        .arg("--model")
        .arg(&args.model);

    let output = cmd.output().map_err(|e| {
        InvalidResponseSnafu {
            message: format!(
                "Failed to execute spice-eval tool: {}. Ensure spice-eval is installed and in PATH.",
                e
            ),
        }
        .build()
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(InvalidResponseSnafu {
            message: format!("spice-eval failed: {}", stderr),
        }
        .build());
    }

    // Print output from spice-eval
    let stdout = String::from_utf8_lossy(&output.stdout);
    print!("{}", stdout);

    Ok(())
}
