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

//! `spice validate` — check that a spicepod.yaml or spicepod.yml file is syntactically valid and
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
    about = "Validate a spicepod.yaml or spicepod.yml without starting the runtime",
    long_about = r#"Validate a spicepod.yaml or spicepod.yml without starting the runtime.

Checks:
  - YAML syntax and schema
  - Component references (datasets/models/views/tools/...)
  - Duplicate component names
  - Reserved keywords
  - Nested pod includes (`dependsOn`, referenced pods)

Examples:
    spice validate                          # validate ./spicepod.yaml
    spice validate .                        # same as above
    spice validate ./my-app                 # validate my-app/spicepod.yaml or spicepod.yml
    spice validate path/to/spicepod.yaml    # validate a specific file
    spice validate path/to/spicepod.yml     # validate a specific file
"#
)]
pub struct ValidateArgs {
    /// Path to a spicepod.yaml/spicepod.yml file, or a directory containing one. Defaults to ".".
    #[arg(default_value = ".")]
    pub path: PathBuf,
}

pub async fn execute(args: &ValidateArgs) -> Result<()> {
    match load_pod(&args.path).await {
        Ok(pod) => {
            let runtime = if pod.runtime == spicepod::component::runtime::Runtime::default() {
                "default"
            } else {
                "configured"
            };
            let management = if pod.management.is_some() {
                "configured"
            } else {
                "none"
            };
            let snapshots = if pod.snapshots.is_some() {
                "configured"
            } else {
                "none"
            };
            println!(
                "{} {}\n  components: catalogs={}, datasets={}, views={}, models={}, embeddings={}, rerankers={}, tools={}, workers={}, functions={}\n  resources: secrets={}, dependencies={}, extensions={}\n  configuration: runtime={runtime}, management={management}, snapshots={snapshots}",
                Color::Green.paint("OK"),
                pod.name,
                pod.catalogs.len(),
                pod.datasets.len(),
                pod.views.len(),
                pod.models.len(),
                pod.embeddings.len(),
                pod.rerankers.len(),
                pod.tools.len(),
                pod.workers.len(),
                pod.functions.len(),
                pod.secrets.len(),
                pod.dependencies.len(),
                pod.extensions.len(),
            );
            Ok(())
        }
        Err(e) => {
            // Return a single user-facing error so the top-level CLI logger emits it once
            // (main.rs logs `tracing::error!("{e}")` on CLI failures; emitting a separate
            // eprintln! here would duplicate the message on stderr).
            InvalidArgumentSnafu {
                message: format!(
                    "{} spicepod validation failed: {e}",
                    Color::Red.paint_err("Invalid:")
                ),
            }
            .fail()
        }
    }
}

/// Resolve `path` into a loaded [`Spicepod`]:
/// - metadata reports a file → load that file directly
/// - metadata reports a directory → load `spicepod.yaml` (or `.yml`) from within
/// - metadata fails (not-found, permission-denied, …) → fall through to
///   `Spicepod::load_exact`, which surfaces the underlying I/O error with the
///   path attached instead of us inventing a vague "not found" message here.
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
        let pod = load_pod(&path)
            .await
            .expect("should load from explicit file");
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
    async fn loads_directory_containing_spicepod_yml() {
        let dir = tempfile::tempdir().expect("tempdir");
        let _ = write_pod(&dir, "spicepod.yml", VALID_POD);
        let pod = load_pod(dir.path())
            .await
            .expect("should load spicepod.yml from directory");
        assert_eq!(pod.name, "test_app");
    }

    #[tokio::test]
    async fn missing_path_produces_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("does_not_exist.yaml");
        load_pod(&missing)
            .await
            .expect_err("missing path should fail to load");
    }

    #[tokio::test]
    async fn invalid_yaml_produces_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = write_pod(&dir, "spicepod.yaml", "not: [valid, yaml: for: a spicepod");
        load_pod(&path)
            .await
            .expect_err("invalid yaml should fail to load");
    }
}
