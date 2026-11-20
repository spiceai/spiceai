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
use snafu::{ResultExt, Snafu};
use spicepod::component::function::{Function, FunctionBuild};
use spicepod::dependency::{Dependency, DependencyTrust};
use std::path::{Path, PathBuf};

pub(crate) const FUNCTIONS_DOCS: &str = "https://spiceai.org/docs/components/functions/wasm";
pub(crate) const DEPENDENCIES_DOCS: &str =
    "https://spiceai.org/docs/reference/spicepod#dependencies";

/// Why a function's module could not be built. Each is worded as the clause
/// that follows "Failed to build function '<name>': ".
#[derive(Debug, Snafu)]
pub enum BuildError {
    #[snafu(display("its `build.command` cannot be read: {source}"))]
    UnreadableCommand { source: shell_words::ParseError },

    #[snafu(display("its `build.command` is empty"))]
    EmptyCommand,

    #[snafu(display(
        "its `build.workdir` '{workdir}' is not a directory in '{}', where relative paths in the Spicepod resolve. Run `spice` from the directory those paths are written for, or correct `build.workdir`",
        base.display()
    ))]
    WorkdirNotADirectory { workdir: String, base: PathBuf },

    #[snafu(display("`{program}` was not found"))]
    ProgramNotFound { program: String },

    #[snafu(display("`{program}` could not be started in '{}': {source}", workdir.display()))]
    ProgramNotStarted {
        program: String,
        workdir: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("`{command}` failed ({status})"))]
    CommandFailed {
        command: String,
        status: std::process::ExitStatus,
    },

    #[snafu(display("it has no `params.module` naming the file the build writes"))]
    NoModule,

    #[snafu(display("the build finished, but its module '{module}' could not be read: {source}"))]
    UnreadableModule {
        module: String,
        source: std::io::Error,
    },
}

/// Arguments for the build command.
#[derive(Args, Debug)]
#[command(
    about = "Build the modules of functions that declare a `build:` block",
    long_about = r#"Build the modules of functions that declare a `build:` block.

Runs each function's `build.command` in `build.workdir`, in declaration order,
as you and with your environment, then prints the SHA-256 of the module at
`params.module`. Stops at the first failed build. Both paths are relative to
the app directory, where `spice run` starts the runtime, also for a function
from a dependency.

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
    // `spice run` starts the runtime in this directory, so the Spicepod here
    // is the one it loads and the directory its relative paths resolve
    // against.
    let app_dir = ctx.app_dir();
    let targets = build_targets(app_dir).await?;
    run(app_dir, &targets, Mode::Build)
}

/// Build what `spice run` is about to load: the functions of `spicepod`,
/// the exact manifest file or directory the runtime is given, and of its
/// dependencies. `runtime_dir` is the directory the runtime runs in: a
/// relative `params.module` is loaded from there, so each `build.workdir`
/// and `params.module` is resolved against it too, and what is built and
/// hashed is what the runtime loads. A Spicepod that cannot be read is left
/// for the runtime to report.
///
/// # Errors
///
/// Returns an error when a build command runs and fails.
pub async fn build_before_run(spicepod: &Path, runtime_dir: &Path) -> Result<()> {
    let Ok(targets) = build_targets(spicepod).await else {
        return Ok(());
    };
    run(runtime_dir, &targets, Mode::BeforeRun)
}

/// Every function with a `build:` block in the Spicepod at `spicepod` (a
/// manifest file, or a directory holding `spicepod.yaml`) and in its
/// dependencies, in declaration order.
async fn build_targets(spicepod: &Path) -> Result<Vec<BuildTarget>> {
    let root =
        spicepod::Spicepod::load(spicepod)
            .await
            .map_err(|source| Error::InvalidArgument {
                message: format!(
                    "Failed to read the Spicepod in '{}': {source}",
                    spicepod.display()
                ),
            })?;
    let mut targets = targets_of(&root.functions, None);
    // The runtime loads dependencies from `spicepods/` beside the manifest.
    let spicepods_dir = spicepod::Spicepod::base_path(spicepod).join("spicepods");
    for dependency in &root.dependencies {
        let path = spicepods_dir.join(dependency.from());
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

fn run(runtime_dir: &Path, targets: &[BuildTarget], mode: Mode) -> Result<()> {
    let mut refused: Vec<Error> = Vec::new();
    for target in targets {
        if let Some(dependency) = &target.dependency
            && dependency.trust() != DependencyTrust::Full
        {
            // A function from a dependency `spicepod.yaml` does not trust to
            // run programs on the host.
            let refusal = Error::BuildRefused {
                function: target.function.clone(),
                from: dependency.from().to_string(),
            };
            match mode {
                Mode::BeforeRun => tracing::warn!("{refusal}"),
                Mode::Build => refused.push(refusal),
            }
            continue;
        }
        build_one(runtime_dir, target, mode).map_err(|source| Error::BuildFailed {
            function: target.function.clone(),
            source,
        })?;
    }

    // Every function that could be built was. Report each refusal, the last as
    // the command's error, so `spice build` exits 1 once at the end.
    let Some(last) = refused.pop() else {
        return Ok(());
    };
    for refusal in refused {
        tracing::error!("{refusal}");
    }
    Err(last)
}

/// Run one function's build in `runtime_dir`/`build.workdir` and print the
/// digest of the module at `runtime_dir`/`params.module`.
fn build_one(
    runtime_dir: &Path,
    target: &BuildTarget,
    mode: Mode,
) -> std::result::Result<(), BuildError> {
    let workdir_written = target.build.workdir.as_deref().unwrap_or(".");
    let workdir = runtime_dir.join(workdir_written);
    let command = &target.build.command;

    let words = shell_words::split(command).context(UnreadableCommandSnafu)?;
    let Some((program, args)) = words.split_first() else {
        return EmptyCommandSnafu.fail();
    };

    println!(
        "Building function '{}' in {workdir_written}: {command}",
        target.function
    );
    // Starting a program in a directory that does not exist fails the same
    // way as a program that does not exist; tell them apart first, so a
    // missing directory is never mistaken for a missing build tool.
    if !workdir.is_dir() {
        return WorkdirNotADirectorySnafu {
            workdir: workdir_written,
            base: runtime_dir,
        }
        .fail();
    }
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
                .filter(|module| runtime_dir.join(module).is_file());
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
            return ProgramNotFoundSnafu { program }.fail();
        }
        Err(source) => {
            return Err(source).context(ProgramNotStartedSnafu { program, workdir });
        }
    };
    if !status.success() {
        return CommandFailedSnafu { command, status }.fail();
    }

    let Some(module) = &target.module else {
        return NoModuleSnafu.fail();
    };
    let digest = sha256_of(&runtime_dir.join(module)).context(UnreadableModuleSnafu { module })?;
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
