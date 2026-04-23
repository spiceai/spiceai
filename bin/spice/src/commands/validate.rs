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
    match load_pod(&args.path).await {
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

/// Resolve `path` into a loaded [`Spicepod`]:
/// - existing file → load that file directly
/// - existing directory → load `spicepod.yaml` (or `.yml`) from within
/// - non-existent path → let `Spicepod::load_exact` produce the canonical error
async fn load_pod(path: &std::path::Path) -> std::result::Result<Spicepod, spicepod::Error> {
    match tokio::fs::metadata(path).await {
        Ok(meta) if meta.is_file() => Spicepod::load_exact(path).await,
        Ok(_) => Spicepod::load(path).await,
        Err(_) => Spicepod::load_exact(path).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const VALID_POD: &str = "version: v2\nkind: Spicepod\nname: test_app\n";

    fn write_pod(dir: &tempfile::TempDir, filename: &str, body: &str) -> std::path::PathBuf {
        let path = dir.path().join(filename);
        let mut f = std::fs::File::create(&path).expect("create pod file");
        f.write_all(body.as_bytes()).expect("write pod file");
        path
    }

    #[tokio::test]
    async fn loads_file_path_directly() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_pod(&dir, "not_spicepod.yaml", VALID_POD);
        let pod = load_pod(&path).await.expect("should load from explicit file");
        assert_eq!(pod.name, "test_app");
    }

    #[tokio::test]
    async fn loads_directory_containing_spicepod() {
        let dir = tempfile::tempdir().expect("tempdir");
        let _ = write_pod(&dir, "spicepod.yaml", VALID_POD);
        let pod = load_pod(dir.path())
            .await
            .expect("should load spicepod.yaml from directory");
        assert_eq!(pod.name, "test_app");
    }

    #[tokio::test]
    async fn missing_path_produces_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("does_not_exist.yaml");
        assert!(load_pod(&missing).await.is_err());
    }

    #[tokio::test]
    async fn invalid_yaml_produces_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_pod(&dir, "spicepod.yaml", "not: [valid, yaml: for: a spicepod");
        assert!(load_pod(&path).await.is_err());
    }
}
