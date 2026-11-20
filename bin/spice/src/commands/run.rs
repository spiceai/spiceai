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

use crate::commands::build::FUNCTIONS_DOCS;
use crate::context::RuntimeContext;
use crate::error::Result;
use crate::runtime_launcher::{ConnectionReport, RunConfig, run_runtime};
use clap::Args;
use spiced_args::RuntimeSpicepod;

pub mod spiced_args;

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
    /// Specifies the runtime endpoint. The scheme determines the endpoint type:
    /// http:// or https:// sets the HTTP endpoint, grpc:// or grpc+tls:// sets the Flight endpoint.
    /// A scheme is required.
    #[arg(long)]
    endpoint: Option<String>,

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
    // The runtime runs in the app directory and resolves the Spicepod's
    // relative paths against it, a function's `params.module` among them,
    // wherever the Spicepod itself is. The modules are built and hashed
    // against the same directory, so the runtime loads what was just built.
    let runtime_dir = ctx.app_dir();

    // Build the function modules the runtime is about to load. The runtime
    // itself never builds anything.
    if let Some(spicepod) = local_spicepod(runtime_dir, &args.args) {
        crate::commands::build::build_before_run(&spicepod, runtime_dir).await?;
    }

    run_runtime(
        ctx,
        &RunConfig {
            endpoint: args.endpoint.clone(),
            http_endpoint: args.http_endpoint.clone(),
            flight_endpoint: args.flight_endpoint.clone(),
            metrics_endpoint: args.metrics_endpoint.clone(),
            verbosity,
            args: args.args.clone(),
            working_dir: Some(runtime_dir.clone()),
            // Nothing precedes this command, so the runtime it starts is the
            // only process that can report the Cloud connection.
            connection_report: ConnectionReport::Runtime,
        },
    )
    .await
}

/// The local Spicepod `spiced` will load, exactly as it is named: the path
/// argument passed through to it, wherever it stands among the runtime's
/// options (`spice run --tls-enabled false ./app/production.yaml`, a manifest
/// file or a directory holding `spicepod.yaml`), or the runtime directory.
/// A remote Spicepod (`s3://`, `https://`) has nothing local to build, and
/// neither has a runtime that loads none.
fn local_spicepod(runtime_dir: &std::path::Path, args: &[String]) -> Option<std::path::PathBuf> {
    match spiced_args::runtime_spicepod(args) {
        RuntimeSpicepod::WorkingDirectory => Some(runtime_dir.to_path_buf()),
        RuntimeSpicepod::Path(spicepod) if spicepod.contains("://") => None,
        RuntimeSpicepod::Path(spicepod) => Some(runtime_dir.join(spicepod)),
        RuntimeSpicepod::NotLoaded => None,
        RuntimeSpicepod::Unrecognized(argument) => {
            // Building a guess could run the commands of a Spicepod the
            // runtime never loads.
            tracing::warn!(
                "`spice run` does not recognize the runtime argument '{argument}', so it cannot tell which Spicepod the runtime will load and built no function module. Run `spice build` to build them, or `spice upgrade` if the runtime is newer than the CLI. See: {FUNCTIONS_DOCS}"
            );
            None
        }
    }
}
