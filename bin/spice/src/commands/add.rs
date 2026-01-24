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

//! Add command - adds a Spicepod to the project.

use crate::context::RuntimeContext;
use crate::error::{ConfigIoSnafu, Result};
use crate::registry;
use clap::Args;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::path::Path;

/// Arguments for the add command.
#[derive(Args, Debug)]
pub struct AddArgs {
    /// Spicepod path (e.g., spiceai/quickstart, ./local/path, or spiceai/quickstart@v1.0)
    pub pod_path: String,
}

/// Execute the add command.
///
/// # Errors
///
/// Returns an error if the Spicepod cannot be fetched or added.
pub async fn execute(ctx: &RuntimeContext, args: AddArgs) -> Result<()> {
    execute_add_or_connect(ctx, args, false).await
}

/// Execute the add or connect command with optional cloud authentication.
///
/// # Errors
///
/// Returns an error if the Spicepod cannot be fetched or added.
pub async fn execute_add_or_connect(
    ctx: &RuntimeContext,
    args: AddArgs,
    connect: bool,
) -> Result<()> {
    let pod_path = &args.pod_path;

    println!("Getting Spicepod {pod_path} ...");

    // Build headers
    let mut headers = ctx.get_headers();

    if connect {
        let api_key = ctx.api_key().ok_or_else(|| {
            crate::error::Error::InvalidArgument {
                message: "Missing or invalid Spice.ai Cloud API key. Run `spice login` to authenticate and continue.".to_string(),
            }
        })?;

        headers.insert("Spice-Target-Source".to_string(), "spice.ai".to_string());
        headers.insert("X-API-Key".to_string(), api_key.to_string());
    }

    // Fetch the Spicepod
    let download_path = registry::get_pod(pod_path, ctx.pods_dir(), &headers, ctx.http_client())
        .await
        .map_err(|e| crate::error::Error::InvalidArgument {
            message: e.to_string(),
        })?;

    // Get relative path for display
    let relative_path = get_relative_path(ctx.app_dir(), &download_path);

    // Read or create spicepod.yaml
    let spicepod_path = ctx.app_dir().join("spicepod.yaml");
    let mut spicepod = if spicepod_path.exists() {
        let contents = std::fs::read_to_string(&spicepod_path).context(ConfigIoSnafu {
            operation: "read",
            path: spicepod_path.clone(),
        })?;
        serde_yaml::from_str(&contents).map_err(|e| crate::error::Error::ConfigParse {
            message: e.to_string(),
        })?
    } else {
        // Create a new spicepod.yaml
        let name = ctx
            .app_dir()
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("app");
        println!("\x1b[32mspicepod.yaml initialized!\x1b[0m");
        SpicepodSpec::new(name)
    };

    // Add dependency if not already present
    if !spicepod.dependencies.contains(&pod_path.clone()) {
        spicepod.dependencies.push(pod_path.clone());

        // Write updated spicepod.yaml
        let yaml =
            serde_yaml::to_string(&spicepod).map_err(|e| crate::error::Error::ConfigParse {
                message: format!("Failed to serialize spicepod.yaml: {e}"),
            })?;
        std::fs::write(&spicepod_path, yaml).context(ConfigIoSnafu {
            operation: "write",
            path: spicepod_path,
        })?;
    }

    println!("added {relative_path}");

    Ok(())
}

/// Get a relative path from a base directory.
fn get_relative_path(base: &Path, path: &Path) -> String {
    path.strip_prefix(base)
        .map_or_else(|_| path.display().to_string(), |p| p.display().to_string())
}

/// Minimal Spicepod spec for reading/writing dependencies.
#[derive(Debug, Serialize, Deserialize)]
struct SpicepodSpec {
    version: String,
    kind: String,
    name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    dependencies: Vec<String>,
    #[serde(flatten)]
    other: serde_yaml::Mapping,
}

impl SpicepodSpec {
    fn new(name: &str) -> Self {
        Self {
            version: "v1beta1".to_string(),
            kind: "Spicepod".to_string(),
            name: name.to_string(),
            dependencies: Vec::new(),
            other: serde_yaml::Mapping::new(),
        }
    }
}
