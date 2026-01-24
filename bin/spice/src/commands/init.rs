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

//! Init command implementation - initializes a new Spice app.

use crate::error::{ConfigIoSnafu, CreateDirectorySnafu, Result};
use clap::Args;
use snafu::ResultExt;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Arguments for the init command.
#[derive(Args, Debug)]
#[command(
    about = "Initialize Spice app - initializes a new Spice app",
    long_about = r#"Initialize a new Spice app by creating a spicepod.yaml file

Examples:
  spice init
  spice init <spice app name>
  spice init my_app

See more at: https://spiceai.org/docs/"#
)]
pub struct InitArgs {
    /// Name of the Spice app (defaults to current directory name)
    #[arg(default_value = ".")]
    name: String,
}

/// Execute the init command.
pub fn execute(args: &InitArgs) -> Result<()> {
    let (spicepod_name, spicepod_dir) = determine_names(&args.name);

    let spicepod_path = Path::new(&spicepod_dir).join("spicepod.yaml");

    // Check if spicepod.yaml already exists
    if spicepod_path.exists() {
        print!("spicepod.yaml already exists. Replace (y/n)? ");
        io::stdout().flush().ok();

        let mut confirm = String::new();
        io::stdin().read_line(&mut confirm).ok();

        if confirm.trim().to_lowercase() != "y" {
            return Ok(());
        }
    }

    // Create directory if name is not "."
    if spicepod_dir != "." {
        std::fs::create_dir_all(&spicepod_dir).context(CreateDirectorySnafu {
            path: PathBuf::from(&spicepod_dir),
        })?;
    }

    // Create the spicepod.yaml file
    let spicepod_content = create_spicepod_yaml(&spicepod_name);
    std::fs::write(&spicepod_path, spicepod_content).context(ConfigIoSnafu {
        operation: "write",
        path: spicepod_path.clone(),
    })?;

    tracing::info!("Initialized {}", spicepod_path.display());
    Ok(())
}

/// Determine the spicepod name and directory from the provided argument.
fn determine_names(name_arg: &str) -> (String, String) {
    if name_arg == "." {
        // Interactive prompt for name based on current directory
        let current_dir = std::env::current_dir().unwrap_or_else(|_| ".".into());
        let dir_name = current_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("app")
            .to_string();

        print!("name: ({dir_name})? ");
        io::stdout().flush().ok();

        let mut input = String::new();
        io::stdin().read_line(&mut input).ok();
        let input = input.trim();

        let spicepod_name = if input.is_empty() {
            dir_name
        } else {
            input.to_string()
        };

        (spicepod_name, ".".to_string())
    } else {
        // Use provided name and create directory
        (name_arg.to_string(), name_arg.to_string())
    }
}

/// Create the spicepod.yaml content.
fn create_spicepod_yaml(name: &str) -> String {
    format!(
        r"version: v1
kind: Spicepod
name: {name}
"
    )
}
