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
use crate::error::Result;
use crate::manifest;
use crate::registry;
use clap::Args;
use std::path::Path;

/// Arguments for the add command.
#[derive(Args, Debug)]
#[command(
    about = "Add a Spicepod dependency to the current project",
    long_about = r#"Add a Spicepod dependency to the current project.

Fetches a Spicepod from a registry, Spice.ai Cloud, or a local path and writes
it into `./spicepods/<name>/`, then registers it under `dependencies:` in
`spicepod.yaml`.

EXAMPLES
  spice add spiceai/quickstart            # Add a registry Spicepod
  spice add spiceai/quickstart@v1.0       # Pin to a specific version
  spice add ./local/path                  # Add a Spicepod from a local directory

Use `spice connect <pod>` instead if the Spicepod is hosted on Spice.ai Cloud
and requires authentication.

Docs: https://spiceai.org/docs"#
)]
pub struct AddArgs {
    /// Spicepod path (e.g. `spiceai/quickstart`, `./local/path`, or `spiceai/quickstart@v1.0`).
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

    let name = ctx
        .app_dir()
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("app");
    let (spicepod_path, mut spicepod, created) =
        manifest::load_or_create_spicepod_value(ctx.app_dir(), name)?;

    if created {
        println!("\x1b[32m{} initialized!\x1b[0m", spicepod_path.display());
    }

    let dependency_path = dependency_reference(pod_path, ctx.app_dir(), &download_path)?;
    if manifest::ensure_string_sequence_item(&mut spicepod, "dependencies", &dependency_path)? {
        manifest::write_spicepod_value(&spicepod_path, &spicepod)?;
    }

    println!("added {relative_path}");

    Ok(())
}

/// Get a relative path from a base directory.
fn get_relative_path(base: &Path, path: &Path) -> String {
    path.strip_prefix(base).map_or_else(
        |_| manifest::path_to_spicepod_ref(path),
        manifest::path_to_spicepod_ref,
    )
}

fn get_relative_dependency_path(base: &Path, path: &Path) -> Result<String> {
    let relative_path = path.strip_prefix(base).map_err(|_| crate::error::Error::InvalidArgument {
        message: format!(
            "Downloaded Spicepod path '{}' is outside the app directory '{}'. Dependencies must be stored relative to the app manifest.",
            path.display(),
            base.display()
        ),
    })?;
    Ok(manifest::path_to_spicepod_ref(relative_path))
}

fn dependency_reference(pod_path: &str, base: &Path, download_path: &Path) -> Result<String> {
    if registry::is_local_path(pod_path) {
        return get_relative_dependency_path(base, download_path);
    }

    Ok(pod_path.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_reference_preserves_remote_version_pin() {
        let reference = dependency_reference(
            "spiceai/quickstart@v1.0",
            Path::new("/app"),
            Path::new("/app/spicepods/spiceai/quickstart"),
        )
        .expect("remote dependency reference should be built");

        assert_eq!(reference, "spiceai/quickstart@v1.0");
    }

    #[test]
    fn dependency_reference_uses_relative_path_for_local_sources() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let local_source = temp_dir.path().join("localpod");
        std::fs::create_dir_all(&local_source).expect("local source should be created");
        let download_path = temp_dir.path().join("spicepods/localpod");

        let reference = dependency_reference(
            local_source
                .to_str()
                .expect("local source path should be utf-8"),
            temp_dir.path(),
            &download_path,
        )
        .expect("local dependency reference should be built");

        assert_eq!(reference, "spicepods/localpod");
    }
}
