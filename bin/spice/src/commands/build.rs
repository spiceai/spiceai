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

//! `spice build`: build the modules of the functions that declare a `build:`
//! block.
//!
//! The runtime never builds anything: a `from: wasm` function loads the module
//! at `params.module`. This command runs each function's `build.command` on the
//! developer's machine, as the developer, and prints the module's SHA-256.
//! `spice run` runs it first.

use crate::context::RuntimeContext;
use crate::error::{Error, Result};
use clap::Args;
use sha2::{Digest, Sha256};
use spicepod::component::function::{Function, FunctionBuild};
use spicepod::dependency::{Dependency, DependencyTrust};
use std::path::{Path, PathBuf};

const FUNCTIONS_DOCS: &str = "https://spiceai.org/docs/components/functions/wasm";
const DEPENDENCIES_DOCS: &str = "https://spiceai.org/docs/reference/spicepod#dependencies";

/// Arguments for the build command.
#[derive(Args, Debug)]
#[command(
    about = "Build the modules of functions that declare a `build:` block",
    long_about = r#"Build the modules of functions that declare a `build:` block.

Runs each function's `build.command` in `build.workdir` (relative to the
Spicepod), in declaration order, as you and with your environment, then prints
the SHA-256 of the module at `params.module`. Stops at the first failed build.

Functions from a Spicepod dependency are built only when `spicepod.yaml` trusts
that dependency (`- { from: <name>, trust: full }` under `dependencies:`).
`spice build` never edits `spicepod.yaml`, and `spice run` runs it first.

EXAMPLE
  functions:
    - name: score
      from: wasm
      params:
        module: ./functions/score/target/wasm32-unknown-unknown/release/score.wasm
      build:
        command: cargo build --target wasm32-unknown-unknown --release
        workdir: ./functions/score

See: https://spiceai.org/docs/components/functions/wasm"#
)]
pub struct BuildArgs {}

/// One function to build, and where it was declared.
#[derive(Debug)]
struct BuildTarget {
    function: String,
    /// `None` for the root `spicepod.yaml`.
    dependency: Option<Dependency>,
    build: FunctionBuild,
    module: Option<String>,
}

/// Why `spice build` runs: on its own it fails on anything it cannot build;
/// before `spice run` it lets the runtime start on modules that already exist.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Build,
    BeforeRun,
}

/// Execute the build command.
///
/// # Errors
///
/// Returns an error when the Spicepod cannot be read, a build fails, or a
/// function from an untrusted dependency declares a build.
pub async fn execute(ctx: &RuntimeContext, _args: &BuildArgs) -> Result<()> {
    let app_dir = ctx.app_dir();
    let targets = build_targets(app_dir).await?;
    run(app_dir, &targets, Mode::Build)
}

/// Build what `spice run` is about to load. A Spicepod that cannot be read
/// is left for the runtime to report.
///
/// # Errors
///
/// Returns an error when a build command runs and fails.
pub async fn build_before_run(app_dir: &Path) -> Result<()> {
    let Ok(targets) = build_targets(app_dir).await else {
        return Ok(());
    };
    run(app_dir, &targets, Mode::BeforeRun)
}

/// Every function with a `build:` block in the root Spicepod and its
/// dependencies, in declaration order.
async fn build_targets(app_dir: &Path) -> Result<Vec<BuildTarget>> {
    let root =
        spicepod::Spicepod::load(app_dir)
            .await
            .map_err(|source| Error::InvalidArgument {
                message: format!(
                    "Failed to read the Spicepod in '{}': {source}",
                    app_dir.display()
                ),
            })?;
    let mut targets = targets_of(&root.functions, None);
    for dependency in &root.dependencies {
        let path = app_dir.join("spicepods").join(dependency.from());
        // The runtime reports a dependency it cannot load; there is nothing
        // here to build from it.
        if let Ok(spicepod) = spicepod::Spicepod::load(&path).await {
            targets.extend(targets_of(&spicepod.functions, Some(dependency)));
        }
    }
    Ok(targets)
}

fn targets_of(functions: &[Function], dependency: Option<&Dependency>) -> Vec<BuildTarget> {
    functions
        .iter()
        .filter_map(|function| {
            let build = function.build.clone()?;
            Some(BuildTarget {
                function: function.name.clone(),
                dependency: dependency.cloned(),
                build,
                module: function
                    .params
                    .get("module")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            })
        })
        .collect()
}

/// The refusal for a function from a dependency `spicepod.yaml` does not
/// trust to run programs on the host.
fn untrusted_build(target: &BuildTarget, dependency: &Dependency) -> String {
    let from = dependency.from();
    format!(
        "Failed to build function '{}' from Spicepod dependency '{from}': its `build:` command runs a program on this host, and a dependency may do that only when spicepod.yaml trusts it. To trust it, replace `- {from}` under `dependencies:` with `- {{ from: {from}, trust: full }}`. See: {DEPENDENCIES_DOCS}",
        target.function
    )
}

fn run(app_dir: &Path, targets: &[BuildTarget], mode: Mode) -> Result<()> {
    let mut refused: Vec<String> = Vec::new();
    for target in targets {
        if let Some(dependency) = &target.dependency
            && dependency.trust() != DependencyTrust::Full
        {
            let message = untrusted_build(target, dependency);
            match mode {
                Mode::BeforeRun => tracing::warn!("{message}"),
                Mode::Build => refused.push(message),
            }
            continue;
        }
        build_one(app_dir, target, mode)?;
    }

    // Every function that could be built was. Report each refusal, the last as
    // the command's error, so `spice build` exits 1 once at the end.
    let Some(last) = refused.pop() else {
        return Ok(());
    };
    for message in refused {
        tracing::error!("{message}");
    }
    Err(Error::BuildFailed { message: last })
}

fn build_one(app_dir: &Path, target: &BuildTarget, mode: Mode) -> Result<()> {
    let workdir_written = target.build.workdir.as_deref().unwrap_or(".");
    let workdir = app_dir.join(workdir_written);
    let command = &target.build.command;
    let failed = |reason: String| Error::BuildFailed {
        message: format!(
            "Failed to build function '{}': {reason}. See: {FUNCTIONS_DOCS}",
            target.function
        ),
    };

    let words = shell_words::split(command)
        .map_err(|source| failed(format!("its `build.command` cannot be read: {source}")))?;
    let Some((program, args)) = words.split_first() else {
        return Err(failed("its `build.command` is empty".to_string()));
    };

    println!(
        "Building function '{}' in {workdir_written}: {command}",
        target.function
    );
    let status = match std::process::Command::new(program)
        .args(args)
        .current_dir(&workdir)
        .status()
    {
        Ok(status) => status,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            let existing = target
                .module
                .as_deref()
                .filter(|module| app_dir.join(module).is_file());
            if mode == Mode::BeforeRun
                && let Some(module) = existing
            {
                let install = if program == "cargo" {
                    "Install the Rust toolchain".to_string()
                } else {
                    format!("Install `{program}`")
                };
                tracing::warn!(
                    "Could not build function '{}': `{program}` was not found, so the existing module '{module}' is used. {install} to rebuild it. See: {FUNCTIONS_DOCS}",
                    target.function
                );
                return Ok(());
            }
            return Err(failed(format!("`{program}` was not found")));
        }
        Err(source) => {
            return Err(failed(format!(
                "`{program}` could not be started in '{}': {source}",
                workdir.display()
            )));
        }
    };
    if !status.success() {
        return Err(failed(format!("`{command}` failed ({status})")));
    }

    let Some(module) = &target.module else {
        return Err(failed(
            "it has no `params.module` naming the file the build writes".to_string(),
        ));
    };
    let digest = sha256_of(&app_dir.join(module)).map_err(|source| {
        failed(format!(
            "the build finished, but its module '{module}' could not be read: {source}"
        ))
    })?;
    println!("Built '{}' -> {module} (sha256 {digest})", target.function);
    Ok(())
}

fn sha256_of(path: &PathBuf) -> std::io::Result<String> {
    use std::io::Read as _;

    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}
