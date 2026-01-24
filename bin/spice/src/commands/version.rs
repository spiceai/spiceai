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

//! Version command implementation.

use crate::context::RuntimeContext;
use crate::error::Result;
use clap::Args;

/// Arguments for the version command.
#[derive(Args, Debug)]
pub struct VersionArgs {
    /// Show only the CLI version (no runtime version)
    #[arg(long)]
    cli_only: bool,
}

/// Get the CLI version string.
#[must_use]
pub fn cli_version() -> String {
    let version = env!("CARGO_PKG_VERSION");
    let git_hash = option_env!("GIT_COMMIT_HASH").unwrap_or("unknown");
    format!("v{version} ({git_hash})")
}

/// Execute the version command.
pub fn execute(ctx: &RuntimeContext, args: &VersionArgs) -> Result<()> {
    println!("CLI version:     {}", cli_version());

    if !args.cli_only {
        match ctx.runtime_version() {
            Ok(version) => {
                println!("Runtime version: {version}");
            }
            Err(_) => {
                println!("Runtime version: not installed");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_version_format() {
        let version = cli_version();
        assert!(version.starts_with('v'));
        assert!(version.contains('('));
        assert!(version.contains(')'));
    }
}
