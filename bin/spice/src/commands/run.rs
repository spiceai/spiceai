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

//! Run command implementation - starts the Spice runtime.

use crate::context::RuntimeContext;
use crate::error::{ChildProcessIdSnafu, Result, RuntimeExecutionSnafu, SignalHandlerSnafu};
use clap::Args;
use snafu::{OptionExt, ResultExt};
use std::process::Stdio;

/// Arguments for the run command.
#[derive(Args, Debug)]
#[command(
    about = "Run Spice.ai - starts the Spice.ai runtime, installing if necessary",
    long_about = r#"Run Spice.ai - starts the Spice.ai runtime, installing if necessary

Examples:
  # Run with Spicepod in the current directory
  spice run

  # Run with Spicepod from a local file
  spice run /path/to/spicepod.yaml

  # Run with Spicepod from an S3 URL (requires AWS credentials)
  spice run s3://my-bucket/spicepod.yaml

  # Run with Spicepod from a remote HTTPS URL
  spice run https://host.com/spicepod.yaml

See more at: https://spiceai.org/docs/"#
)]
pub struct RunArgs {
    /// Specifies the runtime HTTP endpoint (overrides global --http-endpoint for binding)
    #[arg(long)]
    http_endpoint: Option<String>,

    /// Specifies the runtime Flight endpoint
    #[arg(long)]
    flight_endpoint: Option<String>,

    /// Specifies the runtime Prometheus metrics endpoint
    #[arg(long)]
    metrics_endpoint: Option<String>,

    /// Additional arguments passed to spiced
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<String>,
}

/// Execute the run command.
pub async fn execute(ctx: &RuntimeContext, args: &RunArgs, verbosity: u8) -> Result<()> {
    // Auto-install runtime if not present
    if !ctx.is_runtime_installed() {
        tracing::info!("Spice.ai runtime is not installed. Installing now...");
        crate::commands::install::execute(ctx, &crate::commands::install::InstallArgs::default())
            .await?;
    }

    tracing::info!("Spice.ai runtime starting...");

    let mut spiced_args = args.args.clone();

    // Add verbosity flags
    if verbosity > 0 {
        let v_flag = format!("-{}", "v".repeat(verbosity as usize));
        spiced_args.push(v_flag);
    }

    // Add endpoint flags if specified
    if let Some(flight) = &args.flight_endpoint {
        spiced_args.push("--flight".to_string());
        spiced_args.push(flight.clone());
    }

    if let Some(metrics) = &args.metrics_endpoint {
        spiced_args.push("--metrics".to_string());
        spiced_args.push(metrics.clone());
    }

    let std_cmd = ctx.get_run_cmd(&spiced_args, args.http_endpoint.as_deref())?;

    // Convert std::process::Command to tokio::process::Command
    let mut cmd = tokio::process::Command::from(std_cmd);

    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let mut child = cmd.spawn().context(RuntimeExecutionSnafu)?;

    let status = run_with_signal_forwarding(&mut child).await?;

    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }

    Ok(())
}

/// Run the child process and forward signals (SIGTERM, SIGINT) to it.
///
/// On Unix systems, this listens for SIGTERM and SIGINT and forwards them
/// to the child process so it can perform graceful shutdown.
#[cfg(unix)]
async fn run_with_signal_forwarding(
    child: &mut tokio::process::Child,
) -> Result<std::process::ExitStatus> {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    use tokio::signal::unix::{SignalKind, signal};

    let pid = child
        .id()
        .map(|id| Pid::from_raw(id as i32))
        .context(ChildProcessIdSnafu)?;

    let mut sigterm = signal(SignalKind::terminate()).context(SignalHandlerSnafu)?;
    let mut sigint = signal(SignalKind::interrupt()).context(SignalHandlerSnafu)?;

    tokio::select! {
        status = child.wait() => {
            status.context(RuntimeExecutionSnafu)
        }
        _ = sigterm.recv() => {
            tracing::debug!("Received SIGTERM, forwarding to child process");
            let _ = kill(pid, Signal::SIGTERM);
            child.wait().await.context(RuntimeExecutionSnafu)
        }
        _ = sigint.recv() => {
            tracing::debug!("Received SIGINT, forwarding to child process");
            let _ = kill(pid, Signal::SIGINT);
            child.wait().await.context(RuntimeExecutionSnafu)
        }
    }
}

/// On non-Unix systems (Windows), just wait for the child process.
/// Windows handles Ctrl+C differently and typically propagates it to child processes
/// in the same console automatically.
#[cfg(not(unix))]
async fn run_with_signal_forwarding(
    child: &mut tokio::process::Child,
) -> Result<std::process::ExitStatus> {
    child.wait().await.context(RuntimeExecutionSnafu)
}
