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
use crate::error::Result;
use crate::runtime_launcher::{RunConfig, run_runtime};
use clap::Args;

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
    run_runtime(
        ctx,
        &RunConfig {
            endpoint: args.endpoint.clone(),
            http_endpoint: args.http_endpoint.clone(),
            flight_endpoint: args.flight_endpoint.clone(),
            metrics_endpoint: args.metrics_endpoint.clone(),
            verbosity,
            args: args.args.clone(),
            working_dir: None,
        },
    )
    .await
}
