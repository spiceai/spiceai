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

//! Connect command - adds a Spice.ai Cloud Platform app Spicepod for local use.

use crate::commands::add::{AddArgs, execute_add_or_connect};
use crate::context::RuntimeContext;
use crate::error::Result;
use clap::Args;

/// Arguments for the connect command.
#[derive(Args, Debug)]
pub struct ConnectArgs {
    /// Spicepod path from Spice.ai Cloud (e.g., spiceai/quickstart)
    pub pod_path: String,
}

/// Execute the connect command.
///
/// This is the same as the `add` command but includes Spice.ai Cloud authentication headers.
///
/// # Errors
///
/// Returns an error if the Spicepod cannot be fetched or added.
pub async fn execute(ctx: &RuntimeContext, args: ConnectArgs) -> Result<()> {
    let add_args = AddArgs {
        pod_path: args.pod_path,
    };
    execute_add_or_connect(ctx, add_args, true).await
}
